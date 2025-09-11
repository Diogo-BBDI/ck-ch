#!/usr/bin/env python3
"""
Sistema de Verificação de Estoque Magento/Adobe Commerce
Versão adaptada para GitHub Actions
Executa automaticamente via cron jobs
"""

import os
import sys
import requests
import json
import time
import pandas as pd
from google.auth import default
from google.oauth2 import service_account
from googleapiclient.discovery import build
import concurrent.futures
from threading import Lock
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import random

# Configuração de logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'stock_check_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)

class MagentoStockChecker:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.headers = {
            'Accept': '*/*',
            'Content-Type': 'application/json',
            'X-Api-Key': api_key,
            'Magento-Environment-Id': '5b161701-1558-4979-aebc-a80bbb012878',
            'Magento-Website-Code': 'base',
            'Magento-Store-Code': 'main_website_store',
            'Magento-Store-View-Code': 'default'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Pool de carrinhos
        self.cart_pool = []
        self.cart_lock = Lock()
        
        # Estatísticas
        self.stats = {
            'total_products': 0,
            'processed': 0,
            'errors': 0,
            'start_time': None,
            'requests_made': 0
        }
        
        # Configurações de performance
        self.rate_limit = float(os.getenv('RATE_LIMIT', '0.3'))
        self.max_workers = int(os.getenv('MAX_WORKERS', '6'))
        
        logger.info("✅ MagentoStockChecker inicializado")
        logger.info(f"🌐 URL: {self.base_url}")
        logger.info(f"⚡ Max workers: {self.max_workers}")
        logger.info(f"⏱️ Rate limit: {self.rate_limit}s")
    
    def create_cart(self) -> Optional[str]:
        """Cria um carrinho guest"""
        try:
            response = self.session.post(f"{self.base_url}/rest/V1/guest-carts", timeout=30)
            if response.status_code == 200:
                cart_id = response.text.strip('"')
                self.stats['requests_made'] += 1
                return cart_id
            else:
                logger.error(f"Erro criando carrinho: Status {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Erro ao criar carrinho: {e}")
            return None
    
    def get_cart(self) -> Optional[str]:
        """Obtém carrinho da pool ou cria novo"""
        with self.cart_lock:
            if self.cart_pool:
                return self.cart_pool.pop()
            return self.create_cart()
    
    def return_cart(self, cart_id: str):
        """Retorna carrinho para a pool"""
        if cart_id:
            with self.cart_lock:
                if len(self.cart_pool) < 3:
                    self.cart_pool.append(cart_id)
    
    def add_item_to_cart(self, cart_id: str, sku: str, qty: int) -> Tuple[bool, Optional[str]]:
        """Adiciona item ao carrinho"""
        try:
            payload = {
                "cartItem": {
                    "quoteId": cart_id,
                    "sku": sku,
                    "qty": qty
                }
            }
            
            response = self.session.post(
                f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items",
                json=payload,
                timeout=30
            )
            self.stats['requests_made'] += 1
            
            if response.status_code == 200:
                data = response.json()
                return True, data.get('item_id') or data.get('itemId')
            return False, None
            
        except Exception as e:
            logger.error(f"Erro ao adicionar {sku}: {e}")
            return False, None
    
    def update_item_qty(self, cart_id: str, item_id: str, sku: str, qty: int) -> bool:
        """Atualiza quantidade do item"""
        try:
            payload = {
                "cartItem": {
                    "item_id": item_id,
                    "quote_id": cart_id,
                    "sku": sku,
                    "qty": qty
                }
            }
            
            response = self.session.put(
                f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}",
                json=payload,
                timeout=30
            )
            self.stats['requests_made'] += 1
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Erro ao atualizar {sku} qty {qty}: {e}")
            return False
    
    def delete_item(self, cart_id: str, item_id: str):
        """Remove item do carrinho"""
        try:
            response = self.session.delete(
                f"{self.base_url}/rest/V1/guest-carts/{cart_id}/items/{item_id}",
                timeout=30
            )
            self.stats['requests_made'] += 1
        except:
            pass
    
    def check_stock_optimized(self, sku: str, hint: int = 1) -> int:
        """Verifica estoque com algoritmo otimizado"""
        cart_id = self.get_cart()
        if not cart_id:
            return 0
        
        try:
            # 1. Adiciona item inicial
            success, item_id = self.add_item_to_cart(cart_id, sku, 1)
            if not success or not item_id:
                self.return_cart(cart_id)
                return 0
            
            # 2. Busca exponencial
            valid_stock = 1
            test_values = [max(hint, 4), 16, 64, 256]
            
            for test_qty in test_values:
                time.sleep(self.rate_limit)
                if self.update_item_qty(cart_id, item_id, sku, test_qty):
                    valid_stock = test_qty
                else:
                    break
            
            # 3. Busca binária
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
            
            # 4. Teste final +1
            if valid_stock < 9999:
                time.sleep(self.rate_limit)
                if self.update_item_qty(cart_id, item_id, sku, valid_stock + 1):
                    valid_stock += 1
            
            # 5. Cleanup
            self.delete_item(cart_id, item_id)
            self.return_cart(cart_id)
            
            return valid_stock
            
        except Exception as e:
            logger.error(f"Erro verificando estoque {sku}: {e}")
            self.return_cart(cart_id)
            return 0
    
    def process_batch(self, products: List[Dict]) -> List[Dict]:
        """Processa lote de produtos"""
        results = []
        
        def process_single(product):
            sku = product['sku']
            hint = product.get('hint', 1)
            
            try:
                stock = self.check_stock_optimized(sku, hint)
                self.stats['processed'] += 1
                
                if self.stats['processed'] % 50 == 0:
                    elapsed = time.time() - self.stats['start_time']
                    rate = self.stats['processed'] / elapsed * 60
                    eta = (self.stats['total_products'] - self.stats['processed']) / (rate/60)
                    
                    logger.info(f"📊 Progresso: {self.stats['processed']}/{self.stats['total_products']} "
                              f"({rate:.1f}/min) - ETA: {eta/60:.1f}min")
                
                return {
                    'sku': sku,
                    'stock': stock,
                    'familia': product.get('familia', ''),
                    'titulo': product.get('titulo', ''),
                    'status': 'success'
                }
                
            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"❌ Erro processando {sku}: {e}")
                return {
                    'sku': sku,
                    'stock': 0,
                    'familia': product.get('familia', ''),
                    'titulo': product.get('titulo', ''),
                    'status': 'error',
                    'error': str(e)
                }
        
        # Processa em paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(process_single, product) for product in products]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"❌ Erro no future: {e}")
                    self.stats['errors'] += 1
        
        return results

class GoogleSheetsUpdater:
    def __init__(self, spreadsheet_id: str):
        logger.info("🔐 Inicializando Google Sheets...")
        
        # Configura autenticação
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if credentials_path and os.path.exists(credentials_path):
            # Usando service account (para GitHub Actions)
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            logger.info("✅ Usando service account credentials")
        else:
            # Fallback para default credentials (para Colab)
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/spreadsheets'])
            logger.info("✅ Usando default credentials")
        
        self.service = build('sheets', 'v4', credentials=credentials)
        self.spreadsheet_id = spreadsheet_id
        
        logger.info("✅ Google Sheets configurado")
    
    def read_products(self, sheet_name: str = "EstoqueProdutos") -> List[Dict]:
        """Lê produtos da planilha"""
        try:
            logger.info(f"📖 Lendo produtos da aba '{sheet_name}'...")
            
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f'{sheet_name}!A:Z'
            ).execute()
            
            values = result.get('values', [])
            if not values:
                logger.warning("❌ Planilha vazia ou não encontrada")
                return []
            
            headers = values[0]
            products = []
            
            for row_idx, row in enumerate(values[1:], start=2):
                if len(row) >= 2 and row[1].strip():  # SKU não vazio
                    product = {
                        'familia': row[0] if len(row) > 0 else '',
                        'sku': row[1].strip(),
                        'titulo': row[2] if len(row) > 2 else '',
                        'hint': self._extract_hint(row) if len(row) > 3 else 1,
                        'row': row_idx
                    }
                    products.append(product)
            
            logger.info(f"✅ {len(products)} produtos carregados da planilha")
            return products
            
        except Exception as e:
            logger.error(f"❌ Erro lendo planilha: {e}")
            raise
    
    def _extract_hint(self, row: List) -> int:
        """Extrai hint das últimas colunas"""
        try:
            hints = []
            for i in range(len(row) - 1, max(2, len(row) - 6), -1):
                if i < len(row) and str(row[i]).replace('.', '').isdigit() and float(row[i]) > 0:
                    hints.append(int(float(row[i])))
            
            return int(sum(hints) / len(hints)) if hints else 1
        except:
            return 1
    
    def update_results(self, results: List[Dict], sheet_name: str = "EstoqueProdutos"):
        """Atualiza planilha com resultados"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            logger.info(f"📝 Atualizando planilha com {len(results)} resultados...")
            
            # Busca headers existentes
            headers_result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f'{sheet_name}!1:1'
            ).execute()
            
            headers = headers_result.get('values', [[]])[0]
            
            # Procura coluna de hoje
            today_col = None
            for i, header in enumerate(headers):
                if header == today:
                    today_col = i + 1
                    break
            
            # Se não existe, cria nova coluna
            if today_col is None:
                today_col = len(headers) + 1
                self.service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=f'{sheet_name}!{self._col_letter(today_col)}1',
                    valueInputOption='RAW',
                    body={'values': [[today]]}
                ).execute()
                logger.info(f"➕ Nova coluna criada: {today}")
            
            # Prepara updates
            updates = []
            sku_to_stock = {r['sku']: r['stock'] for r in results}
            
            # Lê SKUs para mapear linhas
            sku_data = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f'{sheet_name}!B:B'
            ).execute()
            
            sku_values = sku_data.get('values', [])
            
            for row_idx, sku_row in enumerate(sku_values[1:], start=2):
                if sku_row and sku_row[0].strip() in sku_to_stock:
                    sku = sku_row[0].strip()
                    stock = sku_to_stock[sku]
                    
                    updates.append({
                        'range': f'{sheet_name}!{self._col_letter(today_col)}{row_idx}',
                        'values': [[stock]]
                    })
            
            # Executa update em batches
            if updates:
                batch_size = 100
                for i in range(0, len(updates), batch_size):
                    batch = updates[i:i + batch_size]
                    self.service.spreadsheets().values().batchUpdate(
                        spreadsheetId=self.spreadsheet_id,
                        body={'valueInputOption': 'RAW', 'data': batch}
                    ).execute()
                    time.sleep(1)  # Pausa entre batches
                
                logger.info(f"✅ Planilha atualizada: {len(updates)} produtos")
            else:
                logger.warning("⚠️ Nenhum produto encontrado para atualizar")
            
        except Exception as e:
            logger.error(f"❌ Erro atualizando planilha: {e}")
            raise
    
    def _col_letter(self, col_num: int) -> str:
        """Converte número da coluna para letra"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(col_num % 26 + ord('A')) + result
            col_num //= 26
        return result

def main():
    """Função principal"""
    
    logger.info("🚀 Iniciando Sistema de Verificação de Estoque Magento")
    logger.info("="*60)
    
    # Lê configurações do ambiente
    spreadsheet_id = os.getenv('SPREADSHEET_ID')
    magento_base_url = os.getenv('MAGENTO_BASE_URL')
    magento_api_key = os.getenv('MAGENTO_API_KEY')
    test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    batch_size = int(os.getenv('BATCH_SIZE', '50'))
    
    if not all([spreadsheet_id, magento_base_url, magento_api_key]):
        logger.error("❌ Variáveis de ambiente obrigatórias não configuradas:")
        logger.error("   • SPREADSHEET_ID")
        logger.error("   • MAGENTO_BASE_URL")
        logger.error("   • MAGENTO_API_KEY")
        sys.exit(1)
    
    logger.info(f"📊 Planilha ID: {spreadsheet_id[:20]}...")
    logger.info(f"🌐 Magento URL: {magento_base_url}")
    logger.info(f"🧪 Modo teste: {'Sim' if test_mode else 'Não'}")
    logger.info(f"📦 Batch size: {batch_size}")
    
    try:
        # Inicializa componentes
        stock_checker = MagentoStockChecker(magento_base_url, magento_api_key)
        sheets_updater = GoogleSheetsUpdater(spreadsheet_id)
        
        # Lê produtos
        products = sheets_updater.read_products()
        if not products:
            logger.error("❌ Nenhum produto encontrado na planilha")
            sys.exit(1)
        
        # Modo teste: apenas primeiros produtos
        if test_mode:
            products = products[:10]
            logger.info(f"🧪 MODO TESTE: Processando apenas {len(products)} produtos")
        
        logger.info(f"📦 Total de produtos a processar: {len(products)}")
        estimated_time = len(products) * 15 / 60  # ~15s por produto
        logger.info(f"⏱️ Tempo estimado: {estimated_time:.0f} minutos")
        
        # Configurações
        stock_checker.stats['total_products'] = len(products)
        stock_checker.stats['start_time'] = time.time()
        
        # Processa em batches
        all_results = []
        
        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(products) + batch_size - 1) // batch_size
            
            logger.info(f"\n🔄 BATCH {batch_num}/{total_batches}")
            logger.info(f"📊 Produtos {i+1}-{min(i+batch_size, len(products))} de {len(products)}")
            
            # Processa batch
            batch_start = time.time()
            batch_results = stock_checker.process_batch(batch)
            batch_time = time.time() - batch_start
            
            all_results.extend(batch_results)
            
            # Estatísticas do batch
            batch_success = len([r for r in batch_results if r['status'] == 'success'])
            batch_errors = len([r for r in batch_results if r['status'] == 'error'])
            batch_rate = len(batch_results) / batch_time * 60
            
            logger.info(f"✅ Batch concluído em {batch_time/60:.1f}min")
            logger.info(f"📊 Sucessos: {batch_success} | Erros: {batch_errors}")
            logger.info(f"🚀 Velocidade: {batch_rate:.1f} produtos/min")
            
            # Atualiza planilha
            logger.info(f"📝 Atualizando planilha...")
            sheets_updater.update_results(batch_results)
            
            # Pausa entre batches
            if i + batch_size < len(products):
                logger.info("⏳ Pausa entre batches (60s)...")
                time.sleep(60)
        
        # Estatísticas finais
        total_time = time.time() - stock_checker.stats['start_time']
        success_count = len([r for r in all_results if r['status'] == 'success'])
        error_count = len([r for r in all_results if r['status'] == 'error'])
        success_rate = success_count / len(all_results) * 100 if all_results else 0
        
        logger.info(f"\n🎉 EXECUÇÃO CONCLUÍDA!")
        logger.info("="*60)
        logger.info(f"📊 ESTATÍSTICAS FINAIS:")
        logger.info(f"   • Total processado: {len(all_results)}")
        logger.info(f"   • ✅ Sucessos: {success_count}")
        logger.info(f"   • ❌ Erros: {error_count}")
        logger.info(f"   • 📈 Taxa de sucesso: {success_rate:.1f}%")
        logger.info(f"   • ⏱️ Tempo total: {total_time/60:.1f} minutos")
        logger.info(f"   • 🚀 Velocidade média: {len(all_results)/(total_time/60):.1f} produtos/min")
        logger.info(f"   • 📡 Total requests: {stock_checker.stats['requests_made']}")
        logger.info(f"   • 📊 Requests/produto: {stock_checker.stats['requests_made']/len(all_results):.1f}")
        
        # Salva log detalhado
        df = pd.DataFrame(all_results)
        filename = f'stock_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(filename, index=False)
        logger.info(f"💾 Log detalhado salvo: {filename}")
        
        # Verifica se há muitos erros
        if error_count / len(all_results) > 0.1:  # Mais de 10% de erros
            logger.warning(f"⚠️ Alta taxa de erros: {error_count}/{len(all_results)} ({error_count/len(all_results)*100:.1f}%)")
            sys.exit(1)
        
        logger.info("🎯 Execução finalizada com sucesso!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro crítico na execução: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
