
import ccxt
import pandas as pd
import time

class BybitData:

    def __init__(self):
        self.ex = ccxt.bybit({"enableRateLimit": True})
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
