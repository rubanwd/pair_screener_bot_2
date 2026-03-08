import asyncio
import aiohttp
import requests
import pandas as pd
import time
import sys
import logging

logger = logging.getLogger(__name__)

# Фикс для Windows и asyncio
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class BinanceData:

    def __init__(self):
        # Основной API для фьючерсов Binance
        self.base_url = "https://fapi.binance.com"
        
        # Отключаем прокси, так как бесплатные прокси слишком нестабильны
        # Вместо этого будем использовать альтернативный домен Binance
        self.proxy = None

    def top_symbols(self, top_n):
        """
        Получает список всех активных фьючерсов USDT-M и сортирует их по объему торгов.
        """
        try:
            logger.info("Fetching exchange info from Binance API...")
            
            proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
            
            # 1. Получаем список всех фьючерсов
            # Используем альтернативный публичный домен Binance Vision, который не блокирует США/Европу
            info_resp = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", proxies=proxies, timeout=10)
            if info_resp.status_code == 451:
                logger.warning("Main endpoint returned 451, trying alternative endpoint...")
                info_resp = requests.get("https://data-api.binance.vision/api/v3/exchangeInfo", proxies=proxies, timeout=10)
            info_resp.raise_for_status()
            info_data = info_resp.json()
            
            valid_symbols = set()
            for s in info_data['symbols']:
                # В Binance Vision структура может немного отличаться, поэтому используем .get()
                if s.get('status') == 'TRADING' and s.get('quoteAsset') == 'USDT':
                    # Binance Vision отдает спот, а fapi - фьючерсы. 
                    # Если contractType нет, значит это спот-рынок.
                    if s.get('contractType') == 'PERPETUAL' or 'contractType' not in s:
                        valid_symbols.add(s['symbol'])

            # 2. Получаем объемы торгов за 24 часа
            ticker_resp = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", proxies=proxies, timeout=10)
            if ticker_resp.status_code == 451:
                ticker_resp = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr", proxies=proxies, timeout=10)
            ticker_resp.raise_for_status()
            ticker_data = ticker_resp.json()

            scored = []
            for t in ticker_data:
                sym = t['symbol']
                if sym in valid_symbols:
                    qv = float(t['quoteVolume'])
                    scored.append((sym, qv))

            # Сортируем по убыванию объема
            scored.sort(key=lambda x: x[1], reverse=True)

            # Возвращаем топ-N символов (например: 'BTCUSDT', 'ETHUSDT')
            return [s for s, _ in scored[:top_n]]

        except Exception as e:
            logger.error(f"Error fetching top symbols from Binance API: {e}")
            return []

    async def _fetch_single_async(self, session, symbol, timeframe, limit, retries=5):
        url = f"{self.base_url}/fapi/v1/klines"
        alt_url = "https://data-api.binance.vision/api/v3/klines"
        
        params = {
            "symbol": symbol,
            "interval": timeframe,
            "limit": limit
        }
        
        for i in range(retries):
            try:
                async with session.get(url, params=params, proxy=self.proxy, timeout=10.0) as response:
                    if response.status == 451:
                        # Если словили бан, переключаемся на альтернативный домен
                        async with session.get(alt_url, params=params, proxy=self.proxy, timeout=10.0) as alt_response:
                            alt_response.raise_for_status()
                            data = await alt_response.json()
                    else:
                        # Обработка лимитов Binance
                        if response.status in [429, 418]:
                            logger.warning(f"Rate limit hit for {symbol}, waiting...")
                            await asyncio.sleep(2 ** (i + 1))
                            continue
                            
                        response.raise_for_status()
                        data = await response.json()
                    
                    # Формат свечи Binance:
                    # [0: open_time, 1: open, 2: high, 3: low, 4: close, 5: volume, ...]
                    df = pd.DataFrame(data, columns=[
                        "ts", "o", "h", "l", "c", "v", 
                        "close_time", "quote_asset_volume", "trades", 
                        "taker_buy_base", "taker_buy_quote", "ignore"
                    ])
                    
                    # Конвертируем строки в числа
                    for col in ["o", "h", "l", "c", "v"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        
                    return symbol, df
                    
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching {symbol}, retrying ({i+1}/{retries})...")
                await asyncio.sleep(2 ** (i + 1))
            except Exception as e:
                logger.error(f"Error fetching {symbol}: {e}")
                await asyncio.sleep(2)
        
        return symbol, pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    async def _fetch_all_async(self, symbols, timeframe, limit):
        # Ограничиваем количество одновременных запросов
        sem = asyncio.Semaphore(15)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json'
        }
        
        async with aiohttp.ClientSession(headers=headers) as session:
            async def fetch_with_sem(symbol):
                async with sem:
                    return await self._fetch_single_async(session, symbol, timeframe, limit)
            
            tasks = [fetch_with_sem(s) for s in symbols]
            results = await asyncio.gather(*tasks)
            return results

    def fetch_all_ohlcv(self, symbols, timeframe, limit):
        """
        Скачивает исторические данные для всех переданных тикеров асинхронно.
        Возвращает словарь {symbol: DataFrame}
        """
        results = asyncio.run(self._fetch_all_async(symbols, timeframe, limit))
        return {sym: df for sym, df in results if not df.empty}