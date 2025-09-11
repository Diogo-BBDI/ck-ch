#!/usr/bin/env python3
"""
Sistema de Verificação de Estoque Magento/Adobe Commerce
Versão adaptada para GitHub Actions (cron)
- Lê SKUs de uma planilha Google
- Usa algoritmo otimizado para estimar estoque via carrinho convidado
- Escreve resultados de volta na planilha

Requer variáveis de ambiente:
- GOOGLE_APPLICATION_CREDENTIALS: caminho do JSON da service account (ex.: /tmp/google-credentials.json)
- SPREADSHEET_ID: ID da planilha Google
- MAGENTO_API_KEY: Chave da API (X-Api-Key)
- MAGENTO_BASE_URL: Base da loja (ex.: https://www.bringit.com.br)
- TEST_MODE: "true"/"false" (se true limita processamento)
- BATCH_SIZE: inteiro (quantos SKUs processar no teste)
- RATE_LIMIT: segundos entre requests críticos (default 0.3)
- MAX_WORKERS: threads simultâneas (default 6)
"""

import os
import sys
import json
import time
import logging
import random
import concurrent.futures
from datetime import datetime
from threading import Lock
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
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

def now_ts_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

# ================== GOOGLE SHEETS ==================
class GoogleSheetsUpdater:
    """
    Leitura e escrita no Google Sheets via Service Account.
    Estrutura esperada (padrão, ajustável via parâmetros):
      - Guia de entrada: "Produtos" com colunas: sku, hint (opcional)
      - Escrita de saída: Colunas anexadas na mesma guia (ou em outra).
    """
    def __init__(self, spreadsheet_id: str, creds_path: str, input_sheet: str = "Produtos", output_sheet: Optional[str] = None):
        self.spreadsheet_id = spreadsheet_id
        self.creds_path = creds_path
        self.input_sheet = input_sheet
        # Se não definir output_sheet, escreve na mesma guia (append colunas)
        self.output_sheet = output_sheet or input_sheet

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = service_account.Credentials.from_service_account_file(self.creds_path, scopes=scopes)
        self.service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    def read_products(self, header_row: int = 1, range_start_col: str = "A", range_end_col: str = "B") -> List[Dict]:
        """
        Lê produtos do intervalo: <input_sheet>!A:B
        Espera cabeçalho na primeira linha: sku | hint (opcional)
        """
        range_name = f"{self.input_sheet}!{range_start_col}:{range_end_col}"
        resp = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=range_name,
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()

        values = resp.get("values", [])
        if not values or len(values) <= (header_row - 1):
            logger.warning("Nenhum dado encontrado na planilha de entrada.")
            return []

        headers = [h.strip().lower() for h in values[header_row - 1]]
        data_rows = values[header_row:]
        products = []
        for row in data_rows:
            row_extended = row + [""] * max(0, len(headers) - len(row))
            row_dict = dict(zip(headers, row_extended))
            sku = str(row_dict.get("sku", "")).strip()
            if not sku:
                continue
            hint_val = row_dict.get("hint", "")
            try:
                hint = int(hint_val) if str(hint_val).strip() != "" else 1
            except Exception:
                hint = 1
            products.append({"sku": sku, "hint": hint})

        logger.info(f"🧾 Produtos lidos: {len(products)}")
        return products

    def write_results_adjacent(self, results: List[Dict], header_row: int = 1, start_col: str = "D") -> None:
        """
        Escreve resultados em colunas adjacentes na mesma guia (output_sheet).
        Layout gerado a partir da coluna 'start_col':
          start_col     : status
          next          : stock
          next          : checked_at
          next          : notes
        """
        if not results:
            logger.info("Sem resultados para escrever.")
            return

        headers = ["status", "stock", "checked_at", "notes"]
        start_col_letter = start_col.upper()
        col_index = ord(start_col_letter) - ord("A") + 1  # 1-based

        # monta valores para escrita
        write_values = [headers]
        for r in results:
            row = [
                r.get("status", ""),
                r.get("stock", ""),
                r.get("checked_at", ""),
                r.get("notes", "")
            ]
            write_values.append(row)

        # Determina o range de escrita
        end_col_index = col_index + len(headers) - 1
        end_col_letter = chr(ord("A") + end_col_index - 1)
        start_range = f"{self.output_sheet}!{start_col_letter}{header_row}:{end_col_letter}"
        # Escreve cabeçalho na linha 1 (ou header_row)
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.output_sheet}!{start_col_letter}{header_row}:{end_col_letter}{header_row}",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()

        # Escreve linhas (a partir da 2)
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.output_sheet}!{start_col_letter}{header_row+1}:{end_col_letter}",
            valueInputOption="RAW",
            body={"values": write_values[1:]},
        ).execute()

        logger.info(f"📝 Resultados escritos em {self.output_sheet}!{start_col_letter}:{end_col_letter}")

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

        logger.info("✅ MagentoStockChecker inicializado")
        logger.info(f"🌐 URL: {self.base_url}")
        logger.info(f"⚡ Max workers: {self.max_workers}")
        logger.info(f"⏱️ Rate limit: {self.rate_limit}s")

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
            response = self.session.delete(
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

            # 2) busca exponencial
            valid_stock = 1
            test_values = [max(hint, 4), 16, 64, 256]
            for test_qty in test_values:
                time.sleep(self.rate_limit)
                if self.update_item_qty(cart_id, item_id, sku, test_qty):
                    valid_stock = test_qty
                else:
                    break

            # 3) busca binária
            left, right = valid_stock, min(valid_stock * 4, 9999)
            iterations = 0
            while left < right and iterations < 5:
                mid = (left + right + 1) // 2
                time.sleep(self.rate_limit)
                if self.update_item_qty(cart_id, item_id, sku, mid):
                    left = mid
                    valid_stock = mid
                else:
                    right = mid - 1
                iterations += 1

            # 4) teste final +1
            if valid_stock < 9999:
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
        Espera itens com chaves: sku, hint (opcional).
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
                return {
                    "sku": sku,
                    "stock": stock,
                    "status": "OK",
                    "checked_at": now_ts_iso(),
                    "notes": ""
                }
            except Exception as e:
                self.stats["processed"] += 1
                self.stats["errors"] += 1
                logger.exception(f"Falha ao processar SKU {sku}")
                return {
                    "sku": sku,
                    "stock": "",
                    "status": "ERROR",
                    "checked_at": now_ts_iso(),
                    "notes": str(e)
                }

        # thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(process_single, p) for p in products]
            for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
                res = fut.result()
                results.append(res)

                # métricas periódicas
                if i % 20 == 0 or i == len(products):
                    elapsed = time.time() - self.stats["start_time"]
                    rate = (self.stats["processed"] / elapsed) if elapsed > 0 else 0
                    logger.info(f"📦 Processados {self.stats['processed']}/{self.stats['total_products']} | "
                                f"Erros: {self.stats['errors']} | "
                                f"Reqs: {self.stats['requests_made']} | "
                                f"Velocidade: {rate:.2f} it/s")

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
        input_sheet="Produtos",           # ajuste se necessário
        output_sheet="Produtos"           # ou "Resultados" se preferir outra guia
    )
    checker = MagentoStockChecker(
        base_url=magento_base_url,
        api_key=magento_api_key
    )

    # Lê produtos
    products = sheets.read_products(input_sheet_range_hint())
    if not products:
        logger.error("❌ Nenhum produto encontrado na planilha. Encerrando.")
        sys.exit(1)

    # Modo teste (limita quantidade)
    if test_mode:
        products = products[:batch_size]
        logger.info(f"🧪 TEST_MODE ativo: processando {len(products)} produtos")

    # Embaralha levemente para evitar burst em SKUs consecutivos idênticos
    random.shuffle(products)

    # Processa
    results = checker.process_batch(products)

    # Ordena resultados para alinhar com a ordem lida (opcional)
    sku_to_result = {r["sku"]: r for r in results}
    aligned_results = []
    for p in products:
        aligned_results.append(sku_to_result.get(p["sku"], {
            "sku": p["sku"], "status": "MISSING", "stock": "", "checked_at": now_ts_iso(), "notes": "sem retorno"
        }))

    # Escreve na planilha (a partir da coluna D, ajustável)
    sheets.write_results_adjacent(aligned_results, header_row=1, start_col="D")

    # Resumo
    elapsed = time.time() - checker.stats["start_time"]
    logger.info("============================================================")
    logger.info("✅ Verificação concluída!")
    logger.info(f"📊 Produtos: {checker.stats['total_products']} | Processados: {checker.stats['processed']} | Erros: {checker.stats['errors']}")
    logger.info(f"⏱️ Tempo total: {elapsed:.1f}s | Requisições: {checker.stats['requests_made']}")
    logger.info("============================================================")

def input_sheet_range_hint() -> tuple:
    """
    Apenas um helper sem efeito no Sheets API: mantive para clareza de leitura acima.
    O método read_products define internamente A:B como padrão.
    Essa função existe só para compatibilidade com a chamada 'sheets.read_products(input_sheet_range_hint())'
    """
    return ("A", "B")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Falha na execução principal")
        sys.exit(1)
