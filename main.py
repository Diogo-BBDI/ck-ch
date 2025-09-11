#!/usr/bin/env python3
"""
Sistema de Verificação de Estoque Magento/Adobe Commerce (versão async turbo)
- Lê SKUs da aba EstoqueProdutos no Google Sheets
- Estima estoque via carrinho convidado Magento, de forma assíncrona
- Escreve os resultados em uma coluna com a data (YYYY-MM-DD)

Configuração via env (defina no GitHub Actions):
- GOOGLE_APPLICATION_CREDENTIALS: caminho do JSON da service account
- SPREADSHEET_ID: ID da planilha
- MAGENTO_API_KEY: chave da API
- MAGENTO_BASE_URL: base da loja
- MAX_WORKERS: concorrência máxima (default 20)
- RATE_LIMIT: intervalo mínimo entre chamadas (default 0.1s)
- MAX_STOCK: teto de busca (default 5000)
- TEST_MODE: "true"/"false"
- BATCH_SIZE: limite de SKUs no modo teste
"""

import os
import sys
import json
import time
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional

import aiohttp
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,  # <- cuidado: INFO (constante), não logging.info (função)
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"stock_check_{datetime.now().strftime('%Y%m%d')}.log"),
    ],
)
logger = logging.getLogger(__name__)

# ================== UTILS ==================
def get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Variável de ambiente obrigatória não encontrada: {name}")
    return val

def mask(s: Optional[str], keep: int = 4) -> str:
    if not s:
        return "<vazio>"
    s = str(s)
    return s if len(s) <= keep else s[:keep] + "..."

def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# ================== GOOGLE SHEETS ==================
class GoogleSheetsUpdater:
    def __init__(self, spreadsheet_id: str, creds_path: str, input_sheet: str = "EstoqueProdutos"):
        self.spreadsheet_id = spreadsheet_id
        self.input_sheet = input_sheet

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=scopes)
        self.service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    def read_products(self, header_row: int = 1) -> List[Dict]:
        resp = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.input_sheet}!A:ZZ",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
        values = resp.get("values", [])
        if not values or len(values) <= (header_row - 1):
            logger.warning("Nenhum dado encontrado na planilha.")
            return []
        headers = [str(h).strip().lower() for h in values[header_row - 1]]
        try:
            sku_idx = headers.index("sku")
        except ValueError:
            raise RuntimeError("Cabeçalho 'SKU' não encontrado na primeira linha.")
        products: List[Dict] = []
        for row in values[header_row:]:
            sku = str(row[sku_idx]).strip() if sku_idx < len(row) else ""
            if sku:
                products.append({"sku": sku})
        logger.info(f"🧾 Produtos lidos: {len(products)}")
        return products

    def write_timeseries_column(self, stocks: List[Optional[int]], date_header: str, header_row: int = 1) -> None:
        # Lê cabeçalho
        header_resp = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.input_sheet}!{header_row}:{header_row}",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
        existing_headers = header_resp.get("values", [[]])
        existing_headers = [str(h).strip() for h in (existing_headers[0] if existing_headers else [])]

        # Verifica se a data já existe no cabeçalho
        try:
            col_index = existing_headers.index(date_header) + 1
            creating = False
        except ValueError:
            col_index = len(existing_headers) + 1
            creating = True

        col = col_letter(col_index)

        # Se for nova, escreve o cabeçalho
        if creating:
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.input_sheet}!{col}{header_row}",
                valueInputOption="RAW",
                body={"values": [[date_header]]},
            ).execute()

        # Escreve linhas (da 2 até N)
        start_row = header_row + 1
        end_row = start_row + len(stocks) - 1
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.input_sheet}!{col}{start_row}:{col}{end_row}",
            valueInputOption="RAW",
            body={"values": [[s if s is not None else ""] for s in stocks]},
        ).execute()

        logger.info(f"🕒 Coluna '{date_header}' {'criada' if creating else 'atualizada'} ({len(stocks)} linhas)")

# ================== MAGENTO STOCK CHECKER (ASYNC) ==================
class AsyncMagentoStockChecker:
    def __init__(self, base_url: str, api_key: str, rate_limit: float = 0.1, max_stock: int = 5000, max_workers: int = 20):
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
        self.rate_limit = rate_limit
        self.max_stock = max_stock
        self._max_workers = max_workers  # semaphore será criado dentro do loop
        self.stats = {"processed": 0, "errors": 0, "start_time": None, "requests": 0}

    async def create_cart(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> Optional[str]:
        async with sem:
            async with session.post(f"{self.base_url}/rest/V1/guest-carts") as r:
                self.stats["requests"] += 1
                if r.status == 200:
                    return (await r.text()).strip('"')
                return None

    async def add_item(self, session: aiohttp.ClientSession, cart_id: str, sku: str, qty: int):
        payload = {"cartItem": {"quoteId": cart_id, "sku": sku, "qty": qty}}
        async with session.post(f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items", json=payload) as r:
            self.stats["requests"] += 1
            if r.status == 200:
                data = await r.json()
                return True, data.get("item_id") or data.get("itemId")
            return False, None

    async def update_item(self, session: aiohttp.ClientSession, cart_id: str, item_id: str, sku: str, qty: int) -> bool:
        payload = {"cartItem": {"item_id": item_id, "quote_id": cart_id, "sku": sku, "qty": qty}}
        async with session.put(f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}", json=payload) as r:
            self.stats["requests"] += 1
            return r.status == 200

    async def delete_item(self, session: aiohttp.ClientSession, cart_id: str, item_id: str):
        try:
            async with session.delete(f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}") as r:
                self.stats["requests"] += 1
        except:
            pass

    async def check_stock(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore, sku: str) -> int:
        try:
            cart_id = await self.create_cart(session, sem)
            if not cart_id:
                return 0

            ok, item_id = await self.add_item(session, cart_id, sku, 1)
            if not ok or not item_id:
                return 0

            valid = 1
            # Escalada exponencial dinâmica (x4) até o teto
            v = 4
            steps = 0
            while v <= self.max_stock and steps < 6:  # limita passos iniciais
                await asyncio.sleep(self.rate_limit)
                if await self.update_item(session, cart_id, item_id, sku, v):
                    valid = v
                    v *= 4
                else:
                    break
                steps += 1

            # Busca binária entre último válido e próximo (ou teto)
            left, right = valid, min(max(valid * 4, valid), self.max_stock)
            iterations = 0
            while left < right and iterations < 6:
                mid = (left + right + 1) // 2
                await asyncio.sleep(self.rate_limit)
                if await self.update_item(session, cart_id, item_id, sku, mid):
                    left = mid
                    valid = mid
                else:
                    right = mid - 1
                iterations += 1

            # Teste final +1
            if valid < self.max_stock:
                await asyncio.sleep(self.rate_limit)
                if await self.update_item(session, cart_id, item_id, sku, valid + 1):
                    valid += 1

            await self.delete_item(session, cart_id, item_id)
            return valid

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Erro {sku}: {e}")
            return 0
        finally:
            self.stats["processed"] += 1

    async def process_all(self, products: List[Dict]) -> List[int]:
        self.stats["start_time"] = time.time()
        sem = asyncio.Semaphore(self._max_workers)  # ✅ criar dentro do loop
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=60)
        connector = aiohttp.TCPConnector(limit=0)  # sem limite interno além do semáforo
        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout, connector=connector) as session:
            tasks = [self.check_stock(session, sem, p["sku"]) for p in products]
            return await asyncio.gather(*tasks, return_exceptions=False)

# ================== MAIN ==================
def main():
    spreadsheet_id = get_env("SPREADSHEET_ID")
    magento_api_key = get_env("MAGENTO_API_KEY")
    magento_base_url = get_env("MAGENTO_BASE_URL")
    creds_path = get_env("GOOGLE_APPLICATION_CREDENTIALS")

    test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
    batch_size = int(os.getenv("BATCH_SIZE", "50"))
    rate_limit = float(os.getenv("RATE_LIMIT", "0.1"))
    max_workers = int(os.getenv("MAX_WORKERS", "20"))
    max_stock = int(os.getenv("MAX_STOCK", "5000"))

    logger.info("🚀 Iniciando Sistema de Verificação de Estoque Magento")
    logger.info(f"SPREADSHEET_ID: {mask(spreadsheet_id)}")
    logger.info(f"MAGENTO_BASE_URL: {magento_base_url}")
    logger.info(f"TEST_MODE: {test_mode} | BATCH_SIZE: {batch_size}")
    logger.info(f"⚡ Max workers: {max_workers} | ⏱️ Rate limit: {rate_limit}s | 📈 MAX_STOCK: {max_stock}")

    sheets = GoogleSheetsUpdater(spreadsheet_id, creds_path)
    products = sheets.read_products()
    if not products:
        logger.error("❌ Nenhum produto encontrado.")
        sys.exit(1)

    if test_mode:
        products = products[:batch_size]
        logger.info(f"🧪 Test mode: {len(products)} produtos")

    checker = AsyncMagentoStockChecker(
        magento_base_url, magento_api_key,
        rate_limit=rate_limit, max_stock=max_stock, max_workers=max_workers
    )
    stocks = asyncio.run(checker.process_all(products))

    # Escreve no Google Sheets (coluna com a data de hoje em UTC)
    date_header = datetime.utcnow().strftime("%Y-%m-%d")
    sheets.write_timeseries_column(stocks, date_header)

    elapsed = time.time() - checker.stats["start_time"]
    speed = checker.stats["processed"] / elapsed if elapsed > 0 else 0
    logger.info("===================================================")
    logger.info(f"✅ Concluído! {checker.stats['processed']} SKUs em {elapsed:.1f}s")
    logger.info(f"🚀 Velocidade média: {speed:.2f} it/s | Erros: {checker.stats['errors']} | Reqs: {checker.stats['requests']}")
    logger.info("===================================================")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Falha na execução principal")
        sys.exit(1)
