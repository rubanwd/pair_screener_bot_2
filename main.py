
import time
import logging

from config import *
from binance_data import BinanceData
from stats_arb import find_pairs
from telegram_notify import send_telegram

import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

binance=BinanceData()

# Тестовое сообщение при запуске
startup_msg = f"🚀 Screener Bot started (Binance)!\nMonitoring top {TOP_N_SYMBOLS} pairs.\nInterval: {SLEEP_INTERVAL // 60} mins."
logger.info("Starting bot...")
logger.info(startup_msg.replace('\n', ' '))
send_telegram(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, startup_msg)

while True:

    try:
        logger.info("=========================================")
        logger.info(f"Starting new iteration. Fetching top {TOP_N_SYMBOLS} symbols...")
        symbols=binance.top_symbols(TOP_N_SYMBOLS)

        logger.info(f"Downloading OHLCV data for {len(symbols)} symbols...")
        data_map = binance.fetch_all_ohlcv(symbols, TIMEFRAME, CANDLES_LIMIT)
        
        price_map={}
        for s, df in data_map.items():
            price_map[s] = df["c"]
            
        logger.info(f"Successfully downloaded data for {len(price_map)} symbols.")
        logger.info("Calculating correlations, cointegration and Z-scores...")

        pairs=find_pairs(
            price_map,
            min_corr=MIN_CORRELATION,
            p_max=COINT_PMAX,
            hl_min=HALFLIFE_MIN,
            hl_max=HALFLIFE_MAX
        )

        if not pairs:
            logger.info("No pairs found matching strategic criteria.")
        else:
            logger.info(f"Found {len(pairs)} pairs matching preliminary criteria.")
            best=pairs[0]
            
            a_sym = best[0]
            b_sym = best[1]
            z_score = best[2]
            p_val = best[3]
            hl = best[4]
            corr = best[5]
            
            logger.info(f"🏆 BEST PAIR: {a_sym} vs {b_sym} | Z-score: {z_score:.2f} | Corr: {corr:.2f} | HL: {hl:.2f} | P-val: {p_val:.4f}")
            
            if abs(z_score) >= ZSCORE_THRESHOLD:
                logger.info(f"⚡ Z-Score {z_score:.2f} is beyond threshold {ZSCORE_THRESHOLD}! Sending Telegram signal...")
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
                send_telegram(TELEGRAM_BOT_TOKEN,TELEGRAM_CHAT_ID,msg)
            else:
                logger.info(f"💤 Threshold not reached (Needs >= {ZSCORE_THRESHOLD}). No signal sent.")

    except Exception as e:
        logger.error(f"ERROR during iteration: {e}", exc_info=True)

    logger.info(f"Sleeping for {SLEEP_INTERVAL // 60} minutes...\n")
    time.sleep(SLEEP_INTERVAL)
