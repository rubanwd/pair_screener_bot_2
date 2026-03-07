
import time
import logging

from config import *
from binance_data import BinanceData
from stats_arb import find_pairs
from telegram_notify import send_telegram

logging.basicConfig(level=logging.INFO)

binance=BinanceData()

# Тестовое сообщение при запуске
startup_msg = f"🚀 Screener Bot started (Binance)!\nMonitoring top {TOP_N_SYMBOLS} pairs.\nInterval: {SLEEP_INTERVAL // 60} mins."
print(startup_msg)
send_telegram(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, startup_msg)

while True:

    try:

        symbols=binance.top_symbols(TOP_N_SYMBOLS)

        print(f"Fetching data for {len(symbols)} symbols...")
        data_map = binance.fetch_all_ohlcv(symbols, TIMEFRAME, CANDLES_LIMIT)
        
        price_map={}
        for s, df in data_map.items():
            price_map[s] = df["c"]

        pairs=find_pairs(
            price_map,
            min_corr=MIN_CORRELATION,
            p_max=COINT_PMAX,
            hl_min=HALFLIFE_MIN,
            hl_max=HALFLIFE_MAX
        )

        if not pairs:
            msg="No pairs found matching strategic criteria"
            print(msg)
        else:
            best=pairs[0]
            
            a_sym = best[0]
            b_sym = best[1]
            z_score = best[2]
            p_val = best[3]
            hl = best[4]
            corr = best[5]
            
            if abs(z_score) >= ZSCORE_THRESHOLD:
                if z_score > 0:
                    # Если спред положительный (выше среднего), значит первая монета переоценена относительно второй
                    action = f"SHORT {a_sym} | LONG {b_sym}"
                else:
                    # Если спред отрицательный (ниже среднего), значит первая монета недооценена относительно второй
                    action = f"LONG {a_sym} | SHORT {b_sym}"

                msg=f"""BEST PAIR SIGNAL 🚨

{a_sym} vs {b_sym}

Action: {action}

zscore: {z_score:.2f} (Threshold: {ZSCORE_THRESHOLD})
correlation: {corr:.2f}
pvalue: {p_val:.4f}
halflife: {hl:.2f}
"""
                print(msg)
                send_telegram(TELEGRAM_BOT_TOKEN,TELEGRAM_CHAT_ID,msg)
            else:
                msg=f"Best pair {a_sym} vs {b_sym} has Z-score {z_score:.2f} (Needs >= {ZSCORE_THRESHOLD}). No signal sent."
                print(msg)

    except Exception as e:
        print("ERROR",e)

    print(f"Sleeping for {SLEEP_INTERVAL // 60} minutes...")
    time.sleep(SLEEP_INTERVAL)
