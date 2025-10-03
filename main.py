async def check_stock(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore, sku: str) -> int:
    """Verifica estoque com busca exponencial + busca binária para precisão máxima"""
    try:
        cart_id = await self.create_cart(session, sem)
        if not cart_id:
            return 0

        await asyncio.sleep(self.rate_limit)  # Rate limit após criar carrinho
        ok, item_id = await self.add_item(session, cart_id, sku, 1)
        if not ok or not item_id:
            return 0

        # Fase 1: Busca exponencial para encontrar limite superior
        valid = 1
        v = 4
        while v <= self.max_stock:
            await asyncio.sleep(self.rate_limit)
            if await self.update_item(session, cart_id, item_id, sku, v):
                valid = v
                v *= 2
            else:
                break

        # Fase 2: Busca binária para refinar entre valid e v
        low = valid
        high = min(v, self.max_stock)
        
        while low < high:
            mid = (low + high + 1) // 2  # +1 para arredondar para cima
            await asyncio.sleep(self.rate_limit)
            
            if await self.update_item(session, cart_id, item_id, sku, mid):
                low = mid  # Conseguiu, tenta maior
            else:
                high = mid - 1  # Não conseguiu, limite é menor
        
        final_stock = low

        await self.delete_item(session, cart_id, item_id)
        return final_stock

    except Exception as e:
        self.stats["errors"] += 1
        logger.error(f"Erro {sku}: {e}")
        return 0
    finally:
        self.stats["processed"] += 1
