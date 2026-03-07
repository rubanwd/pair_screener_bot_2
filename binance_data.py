import ccxt
import ccxt.async_support as ccxt_async
import asyncio
import pandas as pd
import time
import sys

# Фикс для Windows и aiodns
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class BinanceData:

    def __init__(self):
        self.ex = ccxt.binance({
            "enableRateLimit": True,
            "urls": {
                "api": {
                    "public": "https://data-api.binance.vision/api/v3",
                }
            }
        })
        # Binance USDT-M Futures
        self.ex.options["defaultType"] = "swap"

    def load_markets(self):
        self.ex.load_markets()

    def top_symbols(self, top_n):
        self.load_markets()

        swaps = [
            s for s, m in self.ex.markets.items()
            if m.get("swap") and m.get("quote") == "USDT" and m.get("active")
        ]

        tickers = self.ex.fetch_tickers(swaps)

        scored = []
        for s in swaps:
            # Для Binance quoteVolume доступен
            qv = tickers.get(s, {}).get("quoteVolume") or 0
            scored.append((s, qv))

        scored.sort(key=lambda x: x[1], reverse=True)

        return [s for s, _ in scored[:top_n]]

    def fetch_ohlcv(self, symbol, timeframe, limit, retries=5):
        for i in range(retries):
            try:
                data = self.ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
                return df
            except ccxt.RateLimitExceeded:
                print(f"Rate limit exceeded. Waiting {2**(i+1)}s before retry...")
                time.sleep(2**(i+1))
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                time.sleep(2)
        
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    async def _fetch_single_async(self, ex_async, symbol, timeframe, limit, retries=5):
        for i in range(retries):
            try:
                data = await asyncio.wait_for(ex_async.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit), timeout=10.0)
                df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
                return symbol, df
            except asyncio.TimeoutError:
                print(f"Timeout fetching {symbol}, retrying ({i+1}/{retries})...")
                await asyncio.sleep(2 ** (i + 1))
            except ccxt.RateLimitExceeded:
                print(f"RateLimitExceeded for {symbol}, waiting...")
                await asyncio.sleep(2 ** (i + 1))
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                await asyncio.sleep(2)
        return symbol, pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    async def _fetch_all_async(self, symbols, timeframe, limit):
        ex_async = ccxt_async.binance({
            "enableRateLimit": True,
            "urls": {
                "api": {
                    "public": "https://data-api.binance.vision/api/v3",
                }
            }
        })
        ex_async.options["defaultType"] = "swap"
        
        sem = asyncio.Semaphore(15)
        
        async def fetch_with_sem(symbol):
            async with sem:
                return await self._fetch_single_async(ex_async, symbol, timeframe, limit)
        
        tasks = [fetch_with_sem(s) for s in symbols]
        results = await asyncio.gather(*tasks)
        
        await ex_async.close()
        return results

    def fetch_all_ohlcv(self, symbols, timeframe, limit):
        results = asyncio.run(self._fetch_all_async(symbols, timeframe, limit))
        return {sym: df for sym, df in results if not df.empty}