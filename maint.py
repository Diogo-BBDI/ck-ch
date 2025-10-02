#!/usr/bin/env python3
"""
Sistema de Verificação de Estoque Magento/Adobe Commerce
- Async turbo (aiohttp) para máximo throughput
- Retries com tenacity (Google Sheets e Magento)
- Timeouts configuráveis (SOCK_CONNECT_TIMEOUT, SOCK_READ_TIMEOUT)
- Escrita incremental opcional no Google Sheets (INCREMENTAL_WRITE=true)
- Escrita final em chunks (CHUNK_SIZE, default 1000)
- Auto-expansão de colunas quando necessário

ENV no GitHub Actions:
- GOOGLE_APPLICATION_CREDENTIALS: caminho do JSON da service account
- SPREADSHEET_ID: ID da planilha (secrets)
- MAGENTO_API_KEY: chave da API (secrets)
- MAGENTO_BASE_URL: base da loja (secrets)
- MAX_WORKERS: concorrência (default 20)
- RATE_LIMIT: intervalo entre tentativas do algoritmo (default 0.1s)
- MAX_STOCK: teto de busca (default 5000)
- TEST_MODE: "true"/"false"
- BATCH_SIZE: limite de SKUs quando TEST_MODE=true
- INCREMENTAL_WRITE: "true"/"false" (default false)
- FLUSH_EVERY: int (default 500)
- FLUSH_SECONDS: int (default 120)
- CHUNK_SIZE: int (default 1000)
- SOCK_CONNECT_TIMEOUT: seg (default 30)
- SOCK_READ_TIMEOUT: seg (default 180)
- AUTO_EXPAND_COLUMNS: "true"/"false" (default true)
- EXPAND_BUFFER: número de colunas extras a adicionar (default 5)
"""

import os
import sys
import time
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
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

# ================== TENACITY HELPERS ==================
# Google Sheets retries
GS_RETRIABLE_EXC = (HttpError, BrokenPipeError, OSError, ConnectionError, TimeoutError)

def _gs_retry():
    return retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(GS_RETRIABLE_EXC),
        before_sleep=lambda rs: logger.warning(
            f"Sheets falhou: {type(rs.outcome.exception()).__name__}: {rs.outcome.exception()} "
            f"(tentativa {rs.attempt_number})"
        ),
    )

# Magento retries
MG_RETRIABLE_EXC = (aiohttp.ClientError, asyncio.TimeoutError, OSError)

def _mg_retry():
    return retry(
        reraise=True,
        stop=stop_after_attempt(4),  # 1 + 3 retries
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(MG_RETRIABLE_EXC),
    )

# ================== GOOGLE SHEETS ==================
class GoogleSheetsUpdater:
    def __init__(self, spreadsheet_id: str, creds_path: str, input_sheet: str = "EstoqueProdutos", auto_expand: bool = True, expand_buffer: int = 5):
        self.spreadsheet_id = spreadsheet_id
        self.input_sheet = input_sheet
        self.auto_expand = auto_expand
        self.expand_buffer = expand_buffer

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

    @_gs_retry()
    def _get_sheet_properties(self) -> Dict:
        """Obtém as propriedades da planilha incluindo dimensões."""
        response = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        for sheet in response.get('sheets', []):
            if sheet['properties']['title'] == self.input_sheet:
                return sheet['properties']
        raise RuntimeError(f"Sheet '{self.input_sheet}' não encontrada")

    @_gs_retry()
    def _expand_sheet_columns(self, new_column_count: int) -> None:
        """Expande a planilha para ter pelo menos new_column_count colunas."""
        sheet_props = self._get_sheet_properties()
        sheet_id = sheet_props['sheetId']
        current_cols = sheet_props.get('gridProperties', {}).get('columnCount', 26)
        
        if new_column_count > current_cols:
            request = {
                "requests": [{
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "columnCount": new_column_count
                            }
                        },
                        "fields": "gridProperties.columnCount"
                    }
                }]
            }
            
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=request
            ).execute()
            
            logger.info(f"🔧 Planilha expandida de {current_cols} para {new_column_count} colunas")

    def _ensure_column_exists(self, col_index: int) -> None:
        """Garante que a coluna especificada existe na planilha."""
        if not self.auto_expand:
            return
            
        try:
            sheet_props = self._get_sheet_properties()
            current_cols = sheet_props.get('gridProperties', {}).get('columnCount', 26)
            
            if col_index > current_cols:
                new_col_count = col_index + self.expand_buffer
                self._expand_sheet_columns(new_col_count)
        except Exception as e:
            logger.warning(f"Não foi possível verificar/expandir colunas: {e}")

    def read_products(self, header_row: int = 1) -> List[Dict]:
        resp = self._values_get(f"{self.input_sheet}!A:ZZ")
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

    def ensure_date_column(self, date_header: str, header_row: int = 1) -> Tuple[str, bool]:
        """Garante que a coluna de data existe, expandindo a planilha se necessário."""
        header_resp = self._values_get(f"{self.input_sheet}!{header_row}:{header_row}")
        existing_headers = header_resp.get("values", [[]])
        existing_headers = [str(h).strip() for h in (existing_headers[0] if existing_headers else [])]

        try:
            col_index = existing_headers.index(date_header) + 1
            created = False
        except ValueError:
            col_index = len(existing_headers) + 1
            created = True

        # Garante que a coluna existe antes de tentar escrever
        self._ensure_column_exists(col_index)
        
        col = col_letter(col_index)
        if created:
            try:
                self._values_update(f"{self.input_sheet}!{col}{header_row}", [[date_header]])
                logger.info(f"📅 Nova coluna criada: {col} ('{date_header}')")
            except HttpError as e:
                if "exceeds grid limits" in str(e):
                    logger.warning(f"Limite de colunas excedido, tentando expandir...")
                    # Força expansão com buffer maior
                    self._expand_sheet_columns(col_index + self.expand_buffer * 2)
                    self._values_update(f"{self.input_sheet}!{col}{header_row}", [[date_header]])
                    logger.info(f"📅 Nova coluna criada após expansão: {col} ('{date_header}')")
                else:
                    raise
        else:
            logger.info(f"📅 Usando coluna existente: {col} ('{date_header}')")
            
        return col, created

    def update_column_range_chunked(self, col: str, header_row: int, start_row: int, values_1col: List[object], chunk_size: int = 1000) -> None:
        total = len(values_1col)
        if total == 0:
            return
        for start in range(0, total, chunk_size):
            end = min(start + chunk_size, total)
            sub = values_1col[start:end]
            sr = start_row + start
            er = sr + (end - start) - 1
            self._values_update(
                f"{self.input_sheet}!{col}{sr}:{col}{er}",
                [[v if v is not None else ""] for v in sub]
            )

    def write_timeseries_column_all(self, stocks: List[Optional[int]], date_header: str, header_row: int = 1, chunk_size: int = 1000) -> None:
        col, created = self.ensure_date_column(date_header, header_row)
        start_row = header_row + 1
        self.update_column_range_chunked(col, header_row, start_row, stocks, chunk_size)
        logger.info(f"🕒 Coluna '{date_header}' {'criada' if created else 'atualizada'} ({len(stocks)} linhas em chunks)")

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
        self._max_workers = max_workers  # semaphore criado dentro do loop
        self.stats = {"processed": 0, "errors": 0, "start_time": None, "requests": 0}

    @_mg_retry()
    async def create_cart(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> Optional[str]:
        async with sem:
            async with session.post(f"{self.base_url}/rest/V1/guest-carts") as r:
                self.stats["requests"] += 1
                if r.status in (429, 500, 502, 503, 504):
                    raise aiohttp.ClientError(f"HTTP {r.status} em create_cart")
                if r.status == 200:
                    return (await r.text()).strip('"')
                return None

    @_mg_retry()
    async def add_item(self, session: aiohttp.ClientSession, cart_id: str, sku: str, qty: int):
        payload = {"cartItem": {"quoteId": cart_id, "sku": sku, "qty": qty}}
        async with session.post(f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items", json=payload) as r:
            self.stats["requests"] += 1
            if r.status in (429, 500, 502, 503, 504):
                raise aiohttp.ClientError(f"HTTP {r.status} em add_item")
            if r.status == 200:
                data = await r.json()
                return True, data.get("item_id") or data.get("itemId")
            return False, None

    @_mg_retry()
    async def update_item(self, session: aiohttp.ClientSession, cart_id: str, item_id: str, sku: str, qty: int) -> bool:
        payload = {"cartItem": {"item_id": item_id, "quote_id": cart_id, "sku": sku, "qty": qty}}
        async with session.put(f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}", json=payload) as r:
            self.stats["requests"] += 1
            if r.status in (429, 500, 502, 503, 504):
                raise aiohttp.ClientError(f"HTTP {r.status} em update_item")
            return r.status == 200

    @_mg_retry()
    async def delete_item(self, session: aiohttp.ClientSession, cart_id: str, item_id: str):
        try:
            async with session.delete(f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}") as r:
                self.stats["requests"] += 1
                if r.status in (429, 500, 502, 503, 504):
                    raise aiohttp.ClientError(f"HTTP {r.status} em delete_item")
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
            # Escalada exponencial (x4) limitada a alguns passos
            v = 4
            steps = 0
            while v <= self.max_stock and steps < 6:
                await asyncio.sleep(self.rate_limit)
                if await self.update_item(session, cart_id, item_id, sku, v):
                    valid = v
                    v *= 4
                else:
                    break
                steps += 1

            # Busca binária
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

    async def process_all_incremental(
        self,
        products: List[Dict],
        sheets: "GoogleSheetsUpdater",
        date_header: str,
        header_row: int,
        chunk_size: int,
        incremental: bool,
        flush_every: int,
        flush_seconds: int,
        sock_connect_timeout: float = 30,
        sock_read_timeout: float = 180,
    ) -> List[int]:
        """
        Processa todos os produtos com escrita incremental opcional.
        """
        self.stats["start_time"] = time.time()
        sem = asyncio.Semaphore(self._max_workers)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=sock_connect_timeout, sock_read=sock_read_timeout)
        connector = aiohttp.TCPConnector(limit=0)

        # Garante a coluna já criada e pega a letra
        col, _created = sheets.ensure_date_column(date_header, header_row)
        start_row_base = header_row + 1

        stocks: List[Optional[int]] = [None] * len(products)
        done_idx = set()
        last_flush_time = time.time()
        last_flushed_index = -1  # maior índice contíguo já escrito

        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout, connector=connector) as session:
            async def runner(idx: int, sku: str):
                val = await self.check_stock(session, sem, sku)
                stocks[idx] = val
                done_idx.add(idx)

            tasks = [asyncio.create_task(runner(i, p["sku"])) for i, p in enumerate(products)]

            async def contiguous_prefix_max() -> int:
                k = last_flushed_index
                while (k + 1) < len(stocks) and ((k + 1) in done_idx):
                    k += 1
                return k

            async def maybe_flush(force: bool = False):
                nonlocal last_flush_time, last_flushed_index
                if not incremental and not force:
                    return
                time_ok = (time.time() - last_flush_time) >= flush_seconds
                count_ok = (len(done_idx) - (last_flushed_index + 1)) >= flush_every
                if force or time_ok or count_ok:
                    k = await contiguous_prefix_max()
                    if k > last_flushed_index:
                        start_row = start_row_base + (last_flushed_index + 1)
                        block = stocks[(last_flushed_index + 1):(k + 1)]
                        sheets.update_column_range_chunked(col, header_row, start_row, block, chunk_size)
                        last_flushed_index = k
                        last_flush_time = time.time()
                        logger.info(f"📝 Flush incremental: linhas {start_row}..{start_row + len(block) - 1} (prefixo contíguo {k+1}/{len(stocks)})")

            for coro in asyncio.as_completed(tasks):
                await coro
                await maybe_flush(force=False)

            await maybe_flush(force=True)

        return [(x if x is not None else "") for x in stocks]

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

    incremental = os.getenv("INCREMENTAL_WRITE", "false").lower() == "true"
    flush_every = int(os.getenv("FLUSH_EVERY", "500"))
    flush_seconds = int(os.getenv("FLUSH_SECONDS", "120"))
    chunk_size = int(os.getenv("CHUNK_SIZE", "1000"))
    sock_connect_timeout = float(os.getenv("SOCK_CONNECT_TIMEOUT", "30"))
    sock_read_timeout = float(os.getenv("SOCK_READ_TIMEOUT", "180"))
    
    # Novas configurações para expansão de colunas
    auto_expand = os.getenv("AUTO_EXPAND_COLUMNS", "true").lower() == "true"
    expand_buffer = int(os.getenv("EXPAND_BUFFER", "5"))

    logger.info("🚀 Iniciando Sistema de Verificação de Estoque Magento")
    logger.info(f"SPREADSHEET_ID: {mask(spreadsheet_id)}")
    logger.info(f"MAGENTO_BASE_URL: {magento_base_url}")
    logger.info(f"TEST_MODE: {test_mode} | BATCH_SIZE: {batch_size}")
    logger.info(f"⚡ Max workers: {max_workers} | ⏱️ Rate limit: {rate_limit}s | 📈 MAX_STOCK: {max_stock}")
    logger.info(f"🧾 Incremental: {incremental} | FLUSH_EVERY={flush_every} | FLUSH_SECONDS={flush_seconds} | CHUNK_SIZE={chunk_size}")
    logger.info(f"⏳ Timeouts: connect={sock_connect_timeout}s, read={sock_read_timeout}s")
    logger.info(f"📊 Auto-expand: {auto_expand} | Buffer: {expand_buffer} colunas")

    sheets = GoogleSheetsUpdater(
        spreadsheet_id, 
        creds_path,
        auto_expand=auto_expand,
        expand_buffer=expand_buffer
    )
    
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

    date_header = datetime.utcnow().strftime("%Y-%m-%d")
    stocks = asyncio.run(
        checker.process_all_incremental(
            products, sheets, date_header,
            header_row=1,
            chunk_size=chunk_size,
            incremental=incremental,
            flush_every=flush_every,
            flush_seconds=flush_seconds,
            sock_connect_timeout=sock_connect_timeout,
            sock_read_timeout=sock_read_timeout,
        )
    )

    # Escrita final (garante consistência total)
    sheets.write_timeseries_column_all(stocks, date_header, header_row=1, chunk_size=chunk_size)

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
