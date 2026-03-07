import ccxt
import ccxt.async_support as ccxt_async
import asyncio
import pandas as pd
import time
import sys

# Фикс для Windows и aiodns
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class BybitData:

    def __init__(self):
        # Добавляем заголовки, чтобы Bybit не блокировал запросы из облака
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        self.ex = ccxt.bybit({
            "enableRateLimit": True,
            "headers": headers,
            "urls": {
                "api": {
                    "public": "https://api.bytick.com", # Альтернативный домен Bybit (Bytick) для обхода банов Cloudflare
                    "private": "https://api.bytick.com"
                }
            }
        })
        self.ex.options["defaultType"]="swap"

    def load_markets(self):
        self.ex.load_markets()

    def top_symbols(self,top_n):
        self.load_markets()

        swaps=[
            s for s,m in self.ex.markets.items()
            if m.get("swap") and m.get("quote")=="USDT"
        ]

        tickers=self.ex.fetch_tickers(swaps)

        scored=[]

        for s in swaps:
            qv=tickers.get(s,{}).get("quoteVolume") or 0
            scored.append((s,qv))

        scored.sort(key=lambda x:x[1],reverse=True)

        return [s for s,_ in scored[:top_n]]

    def fetch_ohlcv(self,symbol,timeframe,limit, retries=5):
        for i in range(retries):
            try:
                data=self.ex.fetch_ohlcv(symbol,timeframe=timeframe,limit=limit)
                df=pd.DataFrame(data,columns=["ts","o","h","l","c","v"])
                return df
            except ccxt.RateLimitExceeded:
                print(f"Rate limit exceeded. Waiting {2**(i+1)}s before retry...")
                time.sleep(2**(i+1))
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                time.sleep(2)
        
        # Возвращаем пустой DataFrame, если ничего не получилось скачать
        return pd.DataFrame(columns=["ts","o","h","l","c","v"])

    async def _fetch_single_async(self, ex_async, symbol, timeframe, limit, retries=5):
        for i in range(retries):
            try:
                # CCXT внутри asyncio может иногда "давиться" блокировками, добавляем небольшой таймаут
                data = await asyncio.wait_for(ex_async.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit), timeout=10.0)
                df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
                return symbol, df
            except asyncio.TimeoutError:
                print(f"Timeout fetching {symbol}")
                await asyncio.sleep(2 ** (i + 1))
            except ccxt.RateLimitExceeded:
                await asyncio.sleep(2 ** (i + 1))
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                await asyncio.sleep(2)
        return symbol, pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    async def _fetch_all_async(self, symbols, timeframe, limit):
        # Используем асинхронный клиент для скорости
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        
        # Обход блокировок через использование альтернативного базового URL Bybit, если нужно
        ex_async = ccxt_async.bybit({
            "enableRateLimit": True,
            "headers": headers,
            "hostname": "api.bybit.com", # Явно задаем хост
            "urls": {
                "api": {
                    "public": "https://api.bytick.com", # Альтернативный домен Bybit (Bytick) для обхода банов Cloudflare
                    "private": "https://api.bytick.com"
                }
            }
        })
        ex_async.options["defaultType"] = "swap"
        
        # Ограничиваем количество одновременных запросов (семафор)
        sem = asyncio.Semaphore(15)
        
        async def fetch_with_sem(symbol):
            async with sem:
                return await self._fetch_single_async(ex_async, symbol, timeframe, limit)
        
        tasks = [fetch_with_sem(s) for s in symbols]
        results = await asyncio.gather(*tasks)
        
        await ex_async.close()
        return results

    def fetch_all_ohlcv(self, symbols, timeframe, limit):
        """
        Скачивает исторические данные для всех переданных тикеров асинхронно в разы быстрее.
        Возвращает словарь {symbol: DataFrame}
        """
        results = asyncio.run(self._fetch_all_async(symbols, timeframe, limit))
        return {sym: df for sym, df in results if not df.empty}