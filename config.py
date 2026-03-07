
import os
from dotenv import load_dotenv

load_dotenv()

def _get_int(name, default):
    try:
        return int(os.getenv(name, default))
    except:
        return default

def _get_float(name, default):
    try:
        return float(os.getenv(name, default))
    except:
        return default

TIMEFRAME = os.getenv("TIMEFRAME","1h")
CANDLES_LIMIT = _get_int("CANDLES_LIMIT",200)
TOP_N_SYMBOLS = _get_int("TOP_N_SYMBOLS",200)

SLEEP_INTERVAL = _get_int("SLEEP_INTERVAL", 1800) # 1800 секунд = 30 минут
COINT_PMAX = _get_float("COINT_PMAX",0.05)

HALFLIFE_MIN = _get_float("HALFLIFE_MIN",2)
HALFLIFE_MAX = _get_float("HALFLIFE_MAX",72)
MIN_CORRELATION = _get_float("MIN_CORRELATION",0.7)
ZSCORE_THRESHOLD = _get_float("ZSCORE_THRESHOLD",2.0)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID","")
