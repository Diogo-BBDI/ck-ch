#!/usr/bin/env python3
"""
Sistema de Verificação de Estoque Magento/Adobe Commerce
Versão adaptada para GitHub Actions (cron)
- Lê SKUs de uma planilha Google (aba EstoqueProdutos)
- Usa algoritmo otimizado para estimar estoque via carrinho convidado
- Escreve resultados de volta na planilha: cada execução cria/atualiza
  uma NOVA COLUNA com a data (YYYY-MM-DD) e preenche os estoques nas linhas.

Variáveis de ambiente exigidas:
- GOOGLE_APPLICATION_CREDENTIALS: caminho do JSON da service account (ex.: /tmp/google-credentials.json)
- SPREADSHEET_ID: ID da planilha
- MAGENTO_API_KEY: chave da API (X-Api-Key)
- MAGENTO_BASE_URL: base da loja (ex.: https://www.bringit.com.br)
Opcional:
- TEST_MODE: "true"/"false" (se true, limita processamento)
- BATCH_SIZE: inteiro (quantos SKUs processar no teste)
- RATE_LIMIT: segundos (default 0.3)
- MAX_WORKERS: inteiro (default 6)
- MAX_STOCK: teto da busca (default 5000)
"""

import os
import sys
import json
import time
import logging
import concurrent.futures
from datetime import datetime
from threading import Lock
from typing import List, Dict, Optional, Tuple

import requests
# pandas não é obrigatório aqui; pode ser removido se não usar em outro lugar
# import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'stock_check_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)

# ================== UTILS ==================
def get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Variável de ambiente obrigatória não encontrada: {name}")
    return val

def mask(value: Optional[str], keep: int = 4) -> str:
    if not value:
        return "<vazio>"
    v = str(value)
    return v if len(v) <= keep else (v[:keep] + "...")

def mask_email(email: str) -> str:
    try:
        user, dom = email.split("@", 1)
        return (user[:3] + "***@" + dom) if len(user) > 3 else ("***@" + dom)
    except Exception:
        return mask(email)

def now_ts_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

def col_letter(n: int) -> str:
    """1->A, 2->B, ..., 27->AA"""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# ================== GOOGLE SHEETS ==================
class GoogleSheetsUpdater:
    """
    Leitura e escrita no Google Sheets via Service Account.

    Guia esperada: "EstoqueProdutos"
      - Primeira linha = cabeçalho com colunas como: Família | SKU | Título | 2025-09-11 | ...
      - Lemos apenas a coluna "SKU" (case-insensitive).
      - A cada execução, criamos/atualizamos UMA coluna com a data (YYYY-MM-DD),
        preenchendo os estoques nas linhas (timeseries).
    """
    def __init__(self, spreadsheet_id: str, creds_path: str, input_sheet: str = "EstoqueProdutos", output_sheet: Optional[str] = None):
        self.spreadsheet_id = spreadsheet_id
        self.creds_path = creds_path
        self.input_sheet = input_sheet
        self.output_sheet = output_sheet or input_sheet

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = service_account.Credentials.from_service_account_file(self.creds_path, scopes=scopes)
        self.sa_email = ""
        try:
            with open(self.creds_path, "r") as fh:
                self.sa_email = json.load(fh).get("client_email", "")
        except Exception:
            pass
        self.service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    def read_products(self, header_row: int = 1) -> List[Dict]:
        """
        Lê produtos da aba definida (EstoqueProdutos), detectando a coluna 'SKU' na primeira linha.
        Retorna lista de dicts: {"sku": <str>, "hint": 1}
        """
        range_name = f"{self.input_sheet}!A:ZZ"
        try:
            resp = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueRenderOption="UNFORMATTED_VALUE",
                dateTimeRenderOption="FORMATTED_STRING",
            ).execute()
        except Exception:
            logger.exception("Erro ao acessar a planilha (talvez permissão faltando?)")
            if self.sa_email:
                logger.error(f"Compartilhe a planilha com a Service Account: {mask_email(self.sa_email)}")
            raise

        values = resp.get("values", [])
        if not values or len(values) <= (header_row - 1):
            logger.warning("Nenhum dado encontrado na planilha de entrada.")
            return []

        headers_raw = values[header_row - 1]
        headers = [str(h).strip() for h in headers_raw]
        headers_lower = [h.lower() for h in headers]

        # Localiza coluna SKU
        try:
            sku_idx = headers_lower.index("sku")
        except ValueError:
            raise RuntimeError("Cabeçalho 'SKU' não encontrado na primeira linha da guia.")

        data_rows = values[header_row:]
        products: List[Dict] = []
        for row in data_rows:
            row_extended = row + [""] * max(0, len(headers) - len(row))
            sku = str(row_extended[sku_idx]).strip() if len(row_extended) > sku_idx else ""
            if not sku:
                continue
            products.append({"sku": sku, "hint": 1})

        logger.info(f"🧾 Produtos lidos: {len(products)}")
        return products

    def write_timeseries_column(self, stocks_in_order: List[Optional[int]], date_header: str, header_row: int = 1) -> None:
        """
        Acrescenta (ou atualiza) UMA coluna com o cabeçalho = date_header (ex.: '2025-09-11')
        e preenche somente os estoques nas linhas (alinhadas aos SKUs lidos).
        """
        if not stocks_in_order:
            logger.info("Sem valores de estoque para escrever.")
            return

        # 1) Ler cabeçalho atual
        header_resp = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.output_sheet}!{header_row}:{header_row}",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
        header_values = header_resp.get("values", [[]])
        existing_headers = [str(h).strip() for h in (header_values[0] if header_values else [])]

        # 2) Verificar se a data já existe no cabeçalho
        try:
            col_index_1based = existing_headers.index(date_header) + 1
            creating_new = False
        except ValueError:
            # Não existe -> criar no fim
            col_index_1based = len(existing_headers) + 1
            creating_new = True

        col_letter_str = col_letter(col_index_1based)

        # 3) Se for nova, escrever o cabeçalho (somente a célula do cabeçalho)
        if creating_new:
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.output_sheet}!{col_letter_str}{header_row}",
                valueInputOption="RAW",
                body={"values": [[date_header]]},
            ).execute()

        # 4) Escrever os estoques na coluna (da linha 2 até N)
        values = [[s if s is not None else ""] for s in stocks_in_order]
        start_row = header_row + 1
        end_row = start_row + len(values) - 1
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.output_sheet}!{col_letter_str}{start_row}:{col_letter_str}{end_row}",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

        action = "criadas" if creating_new else "atualizadas"
        logger.info(f"🕒 Coluna '{date_header}' {action} em {self.output_sheet}!{col_letter_str}{header_row}:{col_letter_str}{end_row}")

# ================== MAGENTO STOCK CHECKER ==================
class MagentoStockChecker:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Accept": "*/*",
            "Content-Type": "application/json",
            "X-Api-Key": api_key,
            "Magento-Environment-Id": "5b161701-1558-4979-aebc-a80bbb012878",
            "Magento-Website-Code": "base",
            "Magento-Store-Code": "main_website_store",
            "Magento-Store-View-Code": "default",
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # Pool de carrinhos
        self.cart_pool: List[str] = []
        self.cart_lock = Lock()

        # Estatísticas
        self.stats = {
            "total_products": 0,
            "processed": 0,
            "errors": 0,
            "start_time": None,
            "requests_made": 0,
        }

        # Config de performance via env
        self.rate_limit = float(os.getenv("RATE_LIMIT", "0.3"))
        self.max_workers = int(os.getenv("MAX_WORKERS", "6"))
        # teto configurável (default 5000)
        self.max_stock = int(os.getenv("MAX_STOCK", "5000"))

        logger.info("✅ MagentoStockChecker inicializado")
        logger.info(f"🌐 URL: {self.base_url}")
        logger.info(f"⚡ Max workers: {self.max_workers}")
        logger.info(f"⏱️ Rate limit: {self.rate_limit}s")
        logger.info(f"📈 MAX_STOCK: {self.max_stock}")

    # ---------- Carrinho ----------
    def create_cart(self) -> Optional[str]:
        try:
            response = self.session.post(f"{self.base_url}/rest/V1/guest-carts", timeout=30)
            if response.status_code == 200:
                cart_id = response.text.strip('"')
                self.stats["requests_made"] += 1
                return cart_id
            else:
                logger.error(f"Erro criando carrinho: Status {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Erro ao criar carrinho: {e}")
            return None

    def get_cart(self) -> Optional[str]:
        with self.cart_lock:
            if self.cart_pool:
                return self.cart_pool.pop()
            return self.create_cart()

    def return_cart(self, cart_id: str):
        if cart_id:
            with self.cart_lock:
                if len(self.cart_pool) < 3:
                    self.cart_pool.append(cart_id)

    # ---------- Itens ----------
    def add_item_to_cart(self, cart_id: str, sku: str, qty: int) -> Tuple[bool, Optional[str]]:
        try:
            payload = {"cartItem": {"quoteId": cart_id, "sku": sku, "qty": qty}}
            response = self.session.post(
                f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items",
                json=payload,
                timeout=30
            )
            self.stats["requests_made"] += 1
            if response.status_code == 200:
                data = response.json()
                return True, data.get("item_id") or data.get("itemId")
            return False, None
        except Exception as e:
            logger.error(f"Erro ao adicionar {sku}: {e}")
            return False, None

    def update_item_qty(self, cart_id: str, item_id: str, sku: str, qty: int) -> bool:
        try:
            payload = {"cartItem": {"item_id": item_id, "quote_id": cart_id, "sku": sku, "qty": qty}}
            response = self.session.put(
                f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}",
                json=payload,
                timeout=30
            )
            self.stats["requests_made"] += 1
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Erro ao atualizar {sku} qty {qty}: {e}")
            return False

    def delete_item(self, cart_id: str, item_id: str):
        try:
            self.session.delete(
                f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}",
                timeout=30
            )
            self.stats["requests_made"] += 1
        except Exception:
            pass

    # ---------- Algoritmo de estoque ----------
    def check_stock_optimized(self, sku: str, hint: int = 1) -> int:
        cart_id = self.get_cart()
        if not cart_id:
            return 0

        try:
            # 1) adiciona 1 un
            success, item_id = self.add_item_to_cart(cart_id, sku, 1)
            if not success or not item_id:
                self.return_cart(cart_id)
                return 0

            # 2) busca exponencial dinâmica (multiplica por 4 até chegar perto do teto)
            valid_stock = 1
            test_values: List[int] = []
            v = max(int(hint) if hint else 1, 4)
            steps = 0
            while v <= self.max_stock and steps < 6:  # limita ~6 passos para não abusar da API
                test_values.append(v)
                v *= 4
                steps += 1

            for test_qty in test_values:
                time.sleep(self.rate_limit)
                if self.update_item_qty(cart_id, item_id, sku, test_qty):
                    valid_stock = test_qty
                else:
                    break

            # 3) busca binária entre o último válido e o próximo (ou até o teto)
            left, right = valid_stock, min(max(valid_stock * 4, valid_stock), self.max_stock)
            iterations = 0
            while left < right and iterations < 6:
                mid = (left + right + 1) // 2
                time.sleep(self.rate_limit)
                if self.update_item_qty(cart_id, item_id, sku, mid):
                    left = mid
                    valid_stock = mid
                else:
                    right = mid - 1
                iterations += 1

            # 4) teste final +1 (se ainda abaixo do teto)
            if valid_stock < self.max_stock:
                time.sleep(self.rate_limit)
                if self.update_item_qty(cart_id, item_id, sku, valid_stock + 1):
                    valid_stock += 1

            # 5) limpeza
            self.delete_item(cart_id, item_id)
            self.return_cart(cart_id)
            return valid_stock

        except Exception as e:
            logger.error(f"Erro verificando estoque {sku}: {e}")
            self.return_cart(cart_id)
            return 0

    # ---------- Processamento ----------
    def process_batch(self, products: List[Dict]) -> List[Dict]:
        """
        Processa lote de produtos em paralelo.
        Espera itens com chave: sku (e hint opcional).
        """
        results: List[Dict] = []
        self.stats["total_products"] = len(products)
        self.stats["processed"] = 0
        self.stats["errors"] = 0
        self.stats["start_time"] = time.time()

        def process_single(product: Dict) -> Dict:
            sku = product["sku"]
            hint = int(product.get("hint", 1) or 1)
            try:
                stock = self.check_stock_optimized(sku, hint)
                self.stats["processed"] += 1
                return {"sku": sku, "stock": stock}
            except Exception as e:
                self.stats["processed"] += 1
                self.stats["errors"] += 1
                logger.exception(f"Falha ao processar SKU {sku}")
                return {"sku": sku, "stock": ""}

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(process_single, p) for p in products]
            for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
                res = fut.result()
                results.append(res)

                if i % 20 == 0 or i == len(products):
                    elapsed = time.time() - self.stats["start_time"]
                    rate = (self.stats["processed"] / elapsed) if elapsed > 0 else 0
                    logger.info(
                        f"📦 Processados {self.stats['processed']}/{self.stats['total_products']} | "
                        f"Erros: {self.stats['errors']} | "
                        f"Reqs: {self.stats['requests_made']} | "
                        f"Velocidade: {rate:.2f} it/s"
                    )

        return results

# ================== MAIN ==================
def main():
    logger.info("🚀 Iniciando Sistema de Verificação de Estoque Magento")
    logger.info("============================================================")
    logger.info("🔍 Verificando variáveis de ambiente...")

    # Leia/valide env ANTES de usá-las
    spreadsheet_id = get_env("SPREADSHEET_ID")
    magento_api_key = get_env("MAGENTO_API_KEY")
    magento_base_url = get_env("MAGENTO_BASE_URL")
    google_credentials_path = get_env("GOOGLE_APPLICATION_CREDENTIALS")
    test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
    batch_size = int(os.getenv("BATCH_SIZE", "50"))

    # Logs seguros
    logger.info(f"SPREADSHEET_ID: {mask(spreadsheet_id)}")
    logger.info(f"MAGENTO_BASE_URL: {magento_base_url}")
    logger.info(f"TEST_MODE: {test_mode} | BATCH_SIZE: {batch_size}")

    # Inicializa integrações
    sheets = GoogleSheetsUpdater(
        spreadsheet_id=spreadsheet_id,
        creds_path=google_credentials_path,
        input_sheet="EstoqueProdutos",
        output_sheet="EstoqueProdutos"
    )
    if getattr(sheets, "sa_email", ""):
        logger.info(f"Service Account: {mask_email(sheets.sa_email)}")

    checker = MagentoStockChecker(
        base_url=magento_base_url,
        api_key=magento_api_key
    )

    # Lê produtos (ordem preservada)
    products = sheets.read_products()
    if not products:
        logger.error("❌ Nenhum produto encontrado na planilha. Encerrando.")
        sys.exit(1)

    # Modo teste (limita quantidade)
    if test_mode:
        products = products[:batch_size]
        logger.info(f"🧪 TEST_MODE ativo: processando {len(products)} produtos")

    # NÃO embaralhar — manter a ordem da planilha
    results = checker.process_batch(products)

    # Extrai apenas os estoques, na MESMA ORDEM dos produtos lidos
    sku_to_stock = {r["sku"]: r.get("stock", "") for r in results}
    stocks_in_order = [sku_to_stock.get(p["sku"], "") for p in products]

    # Cabeçalho da coluna = data da checagem (YYYY-MM-DD, UTC)
    date_header = datetime.utcnow().strftime("%Y-%m-%d")
    sheets.write_timeseries_column(stocks_in_order, date_header=date_header, header_row=1)

    # Resumo
    elapsed = time.time() - checker.stats["start_time"]
    logger.info("============================================================")
    logger.info("✅ Verificação concluída!")
    logger.info(f"📊 Produtos: {checker.stats['total_products']} | Processados: {checker.stats['processed']} | Erros: {checker.stats['errors']}")
    logger.info(f"⏱️ Tempo total: {elapsed:.1f}s | Requisições: {checker.stats['requests_made']}")
    logger.info("============================================================")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Falha na execução principal")
        sys.exit(1)
