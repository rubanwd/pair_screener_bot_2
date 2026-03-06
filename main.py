
import time
import logging

from config import *
from bybit_data import BybitData
from stats_arb import find_pairs
from telegram_notify import send_telegram

logging.basicConfig(level=logging.INFO)

bybit=BybitData()

while True:

    try:

        symbols=bybit.top_symbols(TOP_N_SYMBOLS)

        price_map={}

        for s in symbols:
            df=bybit.fetch_ohlcv(s,TIMEFRAME,CANDLES_LIMIT)
            
            # Пропускаем монеты без данных
            if df.empty:
                continue
                
            price_map[s]=df["c"]
            time.sleep(0.5) # Увеличенная пауза, чтобы не спамить API Bybit

        pairs=find_pairs(price_map)

        if not pairs:
            msg="No pairs found"
        else:
            best=pairs[0]
            
            a_sym = best[0]
            b_sym = best[1]
            z_score = best[2]
            
            if z_score > 0:
                # Если спред положительный (выше среднего), значит первая монета переоценена относительно второй
                action = f"SHORT {a_sym} | LONG {b_sym}"
            else:
                # Если спред отрицательный (ниже среднего), значит первая монета недооценена относительно второй
                action = f"LONG {a_sym} | SHORT {b_sym}"

            msg=f"""BEST PAIR

{a_sym} vs {b_sym}

Action: {action}

zscore: {best[2]:.2f}
pvalue: {best[3]:.4f}
halflife: {best[4]:.2f}
"""

        print(msg)

        send_telegram(TELEGRAM_BOT_TOKEN,TELEGRAM_CHAT_ID,msg)

    except Exception as e:
        print("ERROR",e)

    time.sleep(600) # Задержка 10 минут (600 секунд)
