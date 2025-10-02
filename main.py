#!/usr/bin/env python3
"""
Sistema de Verificação de Estoque Magento/Adobe Commerce
Entrada: aba Produtos (coluna B = SKU)
Saída: aba Estoque (SKU | Data | Estoque), sempre adicionando na última linha
"""

import os
import sys
import time
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ================== UTILS ==================
def get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Variável de ambiente obrigatória não encontrada: {name}")
    return val

# ================== TENACITY HELPERS ==================
GS_RETRIABLE_EXC = (HttpError, BrokenPipeError, OSError, ConnectionError, TimeoutError)
MG_RETRIABLE_EXC = (aiohttp.ClientError, asyncio.TimeoutError, OSError)

def _gs_retry():
    return retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(GS_RETRIABLE_EXC),
    )

def _mg_retry():
    return retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(MG_RETRIABLE_EXC),
    )

# ================== GOOGLE SHEETS ==================
class GoogleSheetsUpdater:
    def __init__(self, spreadsheet_id: str, creds_path: str, produtos_sheet="Produtos", estoque_sheet="Estoque"):
        self.spreadsheet_id = spreadsheet_id
        self.produtos_sheet = produtos_sheet
        self.estoque_sheet = estoque_sheet

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=scopes)
        self.service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    @_gs_retry()
    def _values_get(self, range_str: str, valueRenderOption="UNFORMATTED_VALUE", dateTimeRenderOption="FORMATTED_STRING"):
        return self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=range_str,
            valueRenderOption=valueRenderOption,
            dateTimeRenderOption=dateTimeRenderOption,
        ).execute(num_retries=3)

    @_gs_retry()
    def _values_update(self, range_str: str, values: List[List[object]], valueInputOption="RAW"):
        return self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range_str,
            valueInputOption=valueInputOption,
            body={"values": values},
        ).execute(num_retries=3)

    def get_last_row(self, sheet_name: str) -> int:
        resp = self._values_get(f"{sheet_name}!A:A")
        values = resp.get("values", [])
        return len(values)

    def read_products(self, header_row: int = 1) -> List[str]:
        resp = self._values_get(f"{self.produtos_sheet}!B:B")
        values = resp.get("values", [])
        if not values or len(values) <= (header_row - 1):
            logger.warning("Nenhum SKU encontrado na aba Produtos.")
            return []
        skus = [str(row[0]).strip() for row in values[header_row:] if row]
        logger.info(f"🧾 SKUs lidos da aba '{self.produtos_sheet}': {len(skus)}")
        return skus

    def append_stocks(self, skus: List[str], stocks: List[int]):
        """Adiciona SKU | Data | Estoque abaixo da última linha"""
        data_hoje = datetime.utcnow().strftime("%Y-%m-%d")
        linhas = [[sku, data_hoje, saldo] for sku, saldo in zip(skus, stocks)]

        last_row = self.get_last_row(self.estoque_sheet)
        start_row = last_row + 1 if last_row > 0 else 1

        self._values_update(
            f"{self.estoque_sheet}!A{start_row}:C{start_row+len(linhas)-1}",
            linhas
        )
        logger.info(f"📝 {len(linhas)} linhas adicionadas em '{self.estoque_sheet}' a partir da linha {start_row}")

# ================== MAGENTO STOCK CHECKER (ASYNC) ==================
class AsyncMagentoStockChecker:
    def __init__(self, base_url: str, api_key: str, rate_limit: float = 0.1, max_stock: int = 5000, max_workers: int = 20):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Accept": "*/*",
            "Content-Type": "application/json",
            "X-Api-Key": api_key,
        }
        self.rate_limit = rate_limit
        self.max_stock = max_stock
        self._max_workers = max_workers
        self.stats = {"processed": 0, "errors": 0, "start_time": None, "requests": 0}

    @_mg_retry()
    async def create_cart(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> Optional[str]:
        async with sem:
            async with session.post(f"{self.base_url}/rest/V1/guest-carts") as r:
                self.stats["requests"] += 1
                if r.status == 200:
                    return (await r.text()).strip('"')
                raise aiohttp.ClientError(f"Erro {r.status} em create_cart")

    @_mg_retry()
    async def add_item(self, session: aiohttp.ClientSession, cart_id: str, sku: str, qty: int):
        payload = {"cartItem": {"quoteId": cart_id, "sku": sku, "qty": qty}}
        async with session.post(f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items", json=payload) as r:
            self.stats["requests"] += 1
            if r.status == 200:
                data = await r.json()
                return True, data.get("item_id") or data.get("itemId")
            return False, None

    @_mg_retry()
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
            v = 4
            while v <= self.max_stock:
                await asyncio.sleep(self.rate_limit)
                if await self.update_item(session, cart_id, item_id, sku, v):
                    valid = v
                    v *= 2
                else:
                    break

            await self.delete_item(session, cart_id, item_id)
            return valid

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Erro {sku}: {e}")
            return 0
        finally:
            self.stats["processed"] += 1

    async def process_all(self, skus: List[str], sock_connect_timeout: float = 30, sock_read_timeout: float = 180) -> List[int]:
        self.stats["start_time"] = time.time()
        sem = asyncio.Semaphore(self._max_workers)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=sock_connect_timeout, sock_read=sock_read_timeout)
        connector = aiohttp.TCPConnector(limit=0)

        stocks: List[Optional[int]] = [None] * len(skus)

        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout, connector=connector) as session:
            async def runner(idx: int, sku: str):
                val = await self.check_stock(session, sem, sku)
                stocks[idx] = val

            tasks = [asyncio.create_task(runner(i, sku)) for i, sku in enumerate(skus)]
            await asyncio.gather(*tasks)

        return [(x if x is not None else 0) for x in stocks]

# ================== MAIN ==================
def main():
    spreadsheet_id = get_env("SPREADSHEET_ID")
    magento_api_key = get_env("MAGENTO_API_KEY")
    magento_base_url = get_env("MAGENTO_BASE_URL")
    creds_path = get_env("GOOGLE_APPLICATION_CREDENTIALS")

    rate_limit = float(os.getenv("RATE_LIMIT", "0.1"))
    max_workers = int(os.getenv("MAX_WORKERS", "20"))
    max_stock = int(os.getenv("MAX_STOCK", "5000"))
    sock_connect_timeout = float(os.getenv("SOCK_CONNECT_TIMEOUT", "30"))
    sock_read_timeout = float(os.getenv("SOCK_READ_TIMEOUT", "180"))

    logger.info("🚀 Iniciando Sistema de Verificação de Estoque Magento")

    sheets = GoogleSheetsUpdater(spreadsheet_id, creds_path)
    skus = sheets.read_products()
    if not skus:
        logger.error("❌ Nenhum SKU encontrado na aba Produtos.")
        sys.exit(1)

    checker = AsyncMagentoStockChecker(
        magento_base_url, magento_api_key,
        rate_limit=rate_limit, max_stock=max_stock, max_workers=max_workers
    )

    stocks = asyncio.run(checker.process_all(skus, sock_connect_timeout, sock_read_timeout))
    sheets.append_stocks(skus, stocks)

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
