import asyncio
import logging
import websockets
import aiohttp
import json, requests, pandas as pd
from datetime import datetime, timedelta, timezone
import os, logging, math
from dotenv import load_dotenv
load_dotenv()
import base64
import hmac
import hashlib
import time
import csv
from trading_db import TradingDB
from uuid import uuid4
import traceback

def read_secret(secret_name):
    path = f"/run/secrets/{secret_name}"
    if not os.path.isfile(path):
        logging.warning(f"⚠️ Secret path exists but is not a file: {path}")
        return None
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except Exception as e:
        logging.warning(f"⚠️ Failed to read Docker secret '{secret_name}': {e}")
        return None

# Init once
db = TradingDB()

# === File paths for logs ===
LOT_SIZE = 0.001  # 1 lot = 0.001 BTC (Delta spec)
DELTA_API_KEY = read_secret("DELTA_API_KEY") or os.getenv("DELTA_API_KEY")
DELTA_API_SECRET = read_secret("DELTA_API_SECRET") or os.getenv("DELTA_API_SECRET")
TELEGRAM_TOKEN = read_secret("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = read_secret("TELEGRAM_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
LIVE_MODE = os.getenv("LIVE_MODE", "0") == "1"
TEST_MODE = os.getenv("TEST_MODE", "0") == "1"
if not LIVE_MODE:
    MODE_LABEL = "SIMULATION (No real orders, shadow trading)"
elif LIVE_MODE and TEST_MODE:
    MODE_LABEL = "LIVE-TEST (API active, Order tested & cancelled immediately)"
else:
    MODE_LABEL = "LIVE (Real money trading enabled)"
SYMBOL = os.getenv("SYMBOL", "BTCUSD").upper()
BOT_NAME = f"{SYMBOL} - Local server"
TIMEFRAME = "15m"
ENTRY_BUFFER = float(os.getenv("ENTRY_BUFFER", "9.0"))
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "100"))
logging.info(f"🎯 ENTRY_BUFFER loaded: ±{ENTRY_BUFFER} points")
ENABLE_CANDLE_FALLBACK = True  # Set to False to disable fallback logic
MAX_SLIPPAGE_POINTS = float(os.getenv("MAX_SLIPPAGE_POINTS", "27"))
HEARTBEAT_FILE = "/app/data/heartbeat"
# TRADES_LOG_PATH = os.getenv("TRADES_LOG_PATH", "/app/data/live_trades.csv")
TRADES_LOG_PATH = f"data/{SYMBOL.lower()}_{TIMEFRAME}_trades.csv"
os.makedirs("logs", exist_ok=True)
latest_ws_position = {}

log_file = f"logs/{SYMBOL.lower()}_{TIMEFRAME}_bot.log"

class ExcludeVerboseFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return not ("Tick |" in msg or "RAW WS 15m Update" in msg or "🫀 Heartbeat" in msg)

# === Logging Setup ===
log_file = f"logs/{SYMBOL.lower()}_{TIMEFRAME}_bot.log"  # Ensure SYMBOL is defined before this line

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Avoid duplicate logs by clearing existing handlers
if logger.hasHandlers():
    logger.handlers.clear()

# Console handler (shows everything)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
logger.addHandler(console_handler)

# File handler (excludes noisy logs)
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_handler.addFilter(ExcludeVerboseFilter())  # Exclude Tick | and RAW WS logs
file_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
logger.addHandler(file_handler)



# === Global variables for {TIMEFRAME} candle aggregation ===
PRODUCT_ID = int(os.getenv("PRODUCT_ID", "3136"))
in_position = False
position_state = {}
ohlc_data = pd.DataFrame()
pending_setup = None
current_15m_candle = None
last_15m_candle_start_time = None
finalized_candle_timestamps = set()
trade_id_counter = 0
open_order_id = None
pullback_candle_sequence = []
# Cache for latest tick
latest_tick = {"price": None, "timestamp": None}


# === Config ===
# SYMBOL = "ETHUSD"
EMA_PERIOD = 5
EMERGENCY_MOVE = 300
EMERGENCY_LOCK = 100
MIN_CANDLE_POINTS = 10
MAX_SL_POINTS = 50
CAPITAL = 115
DAILY_RISK_PERCENT = 6
MAX_SL_PER_DAY = 3
MIN_QTY = 0.01
LEVERAGE = 25
UTC = timezone.utc
REQUIRED_CANDLES = EMA_PERIOD + 2
ohlc_data = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "ema", "volume"]) # Added volume
# current_bar = None # REMOVED: No longer needed, using current_{TIMEFRAME}_candle
in_position = False
position_state = {}
last_price = None # This is likely updated by a separate price feed, not directly tied to candle aggregation here
trade_id_counter = 0  # unique ID per trade
open_order_id = None

def is_kill_switch_active():
    return os.getenv("KILL_SWITCH", "false").lower() == "true"

# === Helpers ===
def convert_btc_to_lots(qty_btc):
    return int(qty_btc / LOT_SIZE) * (-1 if qty_btc < 0 else 1)

def floor_qty(q): return math.floor(q * 1000) / 1000

async def handle_private_ws_msg(msg):
    topic = msg.get("topic", "")
    data = msg.get("data", {})

    if topic.startswith("v2/fills"):
        await process_order_fill(data)
    elif topic.startswith("v2/orders"):
        await process_order_status(data)

async def process_order_status(data):
    """
    Handle order status updates from WebSocket.
    """
    try:
        order_id = data.get("id")
        status = data.get("status")
        symbol = data.get("symbol")

        logging.info(f"🔄 Order status update | Order ID: {order_id}, Status: {status}, Symbol: {symbol}")
        # Add further logic here if needed

    except Exception as e:
        error_type = f"{type(e).__name__}: {str(e)}"
        traceback_text = traceback.format_exc()
        mode = "LIVE" if LIVE_MODE else "TEST"
        symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

        context = "❌ Error in process_order_status"
        logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
        db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)

async def send_telegram(msg):
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}) as response:
                    await response.text()
        
        except Exception as e:
            error_type = f"{type(e).__name__}: {str(e)}"
            traceback_text = traceback.format_exc()
            mode = "LIVE" if LIVE_MODE else "TEST"
            symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

            context = "Telegram error"
            logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
            db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)

def write_heartbeat():
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(datetime.now(timezone.utc).isoformat() + "\n")
            f.flush()  # <-- force buffer to disk
            os.fsync(f.fileno())  # <-- ensure it's written even on Docker mounts
        logging.info(f"🫀 Heartbeat written successfully.")
    
    except Exception as e:
        error_type = f"{type(e).__name__}: {str(e)}"
        traceback_text = traceback.format_exc()
        mode = "LIVE" if LIVE_MODE else "TEST"
        symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

        context = "Heartbeat write failed"
        logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
        db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)

def get_authenticated_headers():
    api_key = os.getenv("DELTA_API_KEY")
    api_secret = os.getenv("DELTA_API_SECRET")

    if not api_key or not api_secret:
        raise Exception("Delta API credentials not set in environment variables.")

    timestamp = str(int(time.time() * 1000))
    signature_payload = f"{timestamp}GET/api/v2/orders/positions"
    signature = hmac.new(
        api_secret.encode(),
        signature_payload.encode(),
        hashlib.sha256
    ).hexdigest()

    return {
        "api-key": api_key,
        "timestamp": timestamp,
        "signature": signature
    }
# === Delta Exchange Order Placement (Add HMAC-SHA256 signature if required) ===
def generate_signature(secret_key, method, endpoint, body="", timestamp=None):
    if timestamp is None:
        timestamp = str(int(datetime.now(tz=UTC).timestamp()))
    message = f"{method.upper()}{timestamp}{endpoint}{body}"
    signature = hmac.new(secret_key.encode(), message.encode(), hashlib.sha256).hexdigest()
    return timestamp, signature

def get_available_capital(asset="USD"):
    mode = "LIVE" if LIVE_MODE else "TEST"

    # ✅ FORCE SIM CAPITAL if TEST_MODE or no LIVE_MODE
    if not LIVE_MODE or TEST_MODE:
        try:
            simulated_balance = db.get_last_wallet_balance(asset)
            balance = float(simulated_balance) if simulated_balance is not None else float(CAPITAL)
        except Exception as e:
            logging.warning(f"⚠️ Could not fetch balance from DB: {e}")
            balance = float(CAPITAL)

        try:
            db.log_wallet_balance(asset, balance, datetime.now(timezone.utc), mode)
            logging.info(f"📝 Simulated wallet balance logged to DB: {asset} = {balance}")
        except Exception as e:
            error_type = f"{type(e).__name__}: {str(e)}"
            logging.error(f"❌ Error logging simulated balance: {error_type}")
            db.log_error(SYMBOL, f"SimCapital DB Log Error | {error_type}", traceback.format_exc(), mode)

        return balance

    # ✅ REAL LIVE MODE starts here
    try:
        BASE_URL = "https://api.india.delta.exchange"
        endpoint = "/v2/wallet/balances"
        method = "GET"

        ts, signature = generate_signature(DELTA_API_SECRET, method, endpoint)

        headers = {
            "Accept": "application/json",
            "api-key": DELTA_API_KEY,
            "timestamp": str(ts),
            "signature": signature
        }

        response = requests.get(f"{BASE_URL}{endpoint}", headers=headers)
        response.raise_for_status()
        data = response.json()

        for item in data.get("result", []):
            if item.get("asset_symbol") == asset:
                available_balance = float(item.get("available_balance", 0))
                logging.info(f"💰 LIVE Delta Balance: {available_balance} {asset}")

                if available_balance > 0:
                    try:
                        db.log_wallet_balance(asset, available_balance, datetime.now(timezone.utc), mode)
                        logging.info(f"📝 LIVE balance logged to DB: {asset} = {available_balance}")
                    except Exception as e:
                        logging.error(f"❌ Error logging live balance: {e}")
                        db.log_error(SYMBOL, "Live balance log error", traceback.format_exc(), mode)
                    return available_balance
                else:
                    logging.warning("⚠️ LIVE balance is 0 — falling back to simulated capital.")
                    return float(CAPITAL)

        logging.warning(f"⚠️ Asset {asset} not found in Delta response — fallback to simulated capital.")
        return float(CAPITAL)

    except Exception as e:
        logging.error(f"❌ Error fetching Delta balance: {e}")
        db.log_error(SYMBOL, "Delta capital fetch error", traceback.format_exc(), mode)
        return float(CAPITAL)

async def fetch_open_position():
    endpoint = "/v2/positions"
    url = "https://api.india.delta.exchange" + endpoint
    method = "GET"

    ts, signature = generate_signature(DELTA_API_SECRET, method, endpoint, "")
    headers = {
        "api-key": DELTA_API_KEY,
        "timestamp": ts,
        "signature": signature,
        "Accept": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    resp_json = await response.json()
                    if resp_json["success"] and resp_json["result"]:
                        return resp_json["result"]
    except Exception as e:
        logging.error(f"❌ Failed to fetch open positions: {e}")
    return []

async def check_ws_position_consistency():
    delta_position = latest_ws_position.get(SYMBOL)
    db_trade = db.get_open_trade(SYMBOL, mode="LIVE" if LIVE_MODE else "TEST")

    size = delta_position.get("size") if delta_position else 0

    if size == 0 and db_trade:
        logging.warning("⚠️ DB shows trade open but Delta shows no position. Marking trade as exited.")
        db.update_trade_exit(
            trade_id=db_trade["id"],
            exit_price=db_trade["entry_price"],
            exit_time=datetime.now(timezone.utc),
            pnl=0,
            exit_reason="RECONCILED_MISSING",
            trailing_sl=None,
            mode="LIVE" if LIVE_MODE else "TEST"
        )
        await send_telegram(f"[{BOT_NAME}] ⚠️ Reconciled DB — trade marked exited (Delta shows flat)")
    
    elif size != 0 and not db_trade:
        logging.warning(f"⚠️ Delta shows open position of size {size} but DB has no open trade!")
        # Optional: log discrepancy, alert via Telegram
        await send_telegram(f"[{BOT_NAME}] ⚠️ WS shows open position but DB is empty! Size = {size}")

def finalize_candle(candle):
    global ohlc_data, in_position, position_state, pullback_candle_sequence

    if not isinstance(candle, dict) or any(k not in candle for k in ["timestamp", "open", "high", "low", "close", "volume"]):
        logging.error(f"❌ Invalid candle: {candle}")
        return

    candle_df = pd.DataFrame([candle])
    if candle_df[["open", "high", "low", "close", "volume"]].isnull().values.any():
        logging.warning(f"⚠️ Skipping due to NaN in candle: {candle}")
        return

    logging.info(f"🕒 Finalizing {TIMEFRAME} candle: {candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
    ohlc_data = pd.concat([ohlc_data, candle_df], ignore_index=True) if not ohlc_data.empty else candle_df.copy()

    global MAX_CANDLES
    if len(ohlc_data) > MAX_CANDLES:
        ohlc_data = ohlc_data.iloc[-MAX_CANDLES:].reset_index(drop=True)

    if len(ohlc_data) >= EMA_PERIOD:
        ohlc_data["ema"] = ohlc_data["close"].ewm(span=EMA_PERIOD, adjust=False).mean()
        logging.info(f"📊 Candle: {ohlc_data.iloc[-1].to_dict()}")
        logging.info(f"🔍 EMA: {ohlc_data.iloc[-1]['ema']:.2f}")
    else:
        logging.info("🔍 Not enough candles for EMA")

    if position_state.get("entry_buffer_zone") and not in_position:
        setup_index = position_state.get("setup_candle_index")
        current_index = len(ohlc_data) - 1

        if setup_index is not None and current_index >= setup_index + 2:
            setup_candle = ohlc_data.iloc[setup_index]
            buffer_low, buffer_high = position_state["entry_buffer_zone"]
            window_hits = []

            for offset in range(1, 4):
                if setup_index + offset < len(ohlc_data):
                    candle = ohlc_data.iloc[setup_index + offset]
                    if buffer_low <= candle["low"] <= buffer_high or buffer_low <= candle["high"] <= buffer_high:
                        window_hits.append(offset)

            hit_window = min(window_hits) if window_hits else None

            try:
                mode = "LIVE" if LIVE_MODE else "TEST"
                db.log_missed_entry_window(
                    symbol=SYMBOL,
                    setup_time=setup_candle["timestamp"],
                    entry_price=setup_candle["high"] if setup_candle["close"] < setup_candle["ema"] else setup_candle["low"],
                    sl_price=setup_candle["low"] if setup_candle["close"] < setup_candle["ema"] else setup_candle["high"],
                    hit_in_candle=hit_window,
                    mode=mode
                )
                logging.info(f"📊 Missed entry logged to DB | Hit in candle: {hit_window}")
            except Exception as e:
                error_type = f"{type(e).__name__}: {str(e)}"
                traceback_text = traceback.format_exc()
                mode = "LIVE" if LIVE_MODE else "TEST"
                symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

                context = "Failed to log missed entry window"  
                logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
                db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)

            logging.info("💤 No breakout on immediate next candle. Setup expired and buffer cleared.")
            position_state.pop("entry_buffer_zone", None)
            position_state.pop("setup_candle_index", None)
            position_state["has_trailed"] = False
            global pending_setup
            pending_setup = None

    if in_position:
        update_trailing_sl_from_candle(candle)

def update_trailing_sl_from_candle(latest_candle):
    global position_state

    direction = position_state.get("direction")
    current_trailing_sl = position_state.get("trailing_sl")
    sl_order_id = position_state.get("sl_order_id")
    trail_buffer = float(os.getenv("TRAIL_BUFFER", 9.0))
    symbol = position_state.get("symbol", SYMBOL)

    # Ensure directional pullback sequences exist
    position_state.setdefault("pullback_buy_sequence", [])
    position_state.setdefault("pullback_sell_sequence", [])

    pullback_buy_sequence = position_state["pullback_buy_sequence"]
    pullback_sell_sequence = position_state["pullback_sell_sequence"]

    o, h, l, c = latest_candle["open"], latest_candle["high"], latest_candle["low"], latest_candle["close"]
    new_sl = None

    if direction == "BUY":
        if c < o:  # red candle
            pullback_buy_sequence.append(latest_candle)
            logging.info(f"🔴 BUY: Red pullback candle added. Sequence length: {len(pullback_buy_sequence)}")
        else:
            if pullback_buy_sequence:
                pullback_high = max(x["high"] for x in pullback_buy_sequence)
                pullback_low = min(x["low"] for x in pullback_buy_sequence)

                if h > pullback_high:
                    new_sl = round(pullback_low - trail_buffer, 1)
                    if new_sl > current_trailing_sl:
                        position_state["trailing_sl"] = new_sl
                        position_state["has_trailed"] = True
                        logging.info(f"📈 BUY SL moved to {new_sl} after {len(pullback_buy_sequence)} red candles breakout!")
                        asyncio.create_task(send_telegram(f"[{BOT_NAME} | {MODE_LABEL}] 📈 SL moved to {new_sl} after red pullback."))

                        if LIVE_MODE and sl_order_id:
                            asyncio.create_task(update_stop_loss_order(sl_order_id, new_sl))

                        # Update to DB
                        trade_id = position_state.get("id")
                        if trade_id:
                            try:
                                db.update_trailing_sl(trade_id, new_sl)
                                logging.info(f"📄 Trailing SL updated in DB to {new_sl} for trade ID {trade_id}")
                            except Exception as e:
                                logging.error(f"❌ Failed to update trailing SL in DB: {e}", exc_info=True)
                    else:
                        logging.info(f"ℹ️ BUY SL not moved: proposed {new_sl} ≤ current {current_trailing_sl}")
                    pullback_buy_sequence.clear()
                else:
                    logging.info("⏸ BUY: No breakout yet. Keeping pullback sequence active.")

    elif direction == "SELL":
        if c > o:  # green candle
            pullback_sell_sequence.append(latest_candle)
            logging.info(f"🟢 SELL: Green pullback candle added. Sequence length: {len(pullback_sell_sequence)}")
        else:
            if pullback_sell_sequence:
                pullback_low = min(x["low"] for x in pullback_sell_sequence)
                pullback_high = max(x["high"] for x in pullback_sell_sequence)

                if l < pullback_low:
                    new_sl = round(pullback_high + trail_buffer, 1)
                    if new_sl < current_trailing_sl:
                        position_state["trailing_sl"] = new_sl
                        position_state["has_trailed"] = True
                        logging.info(f"📉 SELL SL moved to {new_sl} after {len(pullback_sell_sequence)} green candles breakout!")
                        asyncio.create_task(send_telegram(f"[{BOT_NAME} | {MODE_LABEL}] 📉 SL moved to {new_sl} after green pullback for {symbol}"))

                        if LIVE_MODE and sl_order_id:
                            asyncio.create_task(update_stop_loss_order(sl_order_id, new_sl))

                        # Update to DB
                        trade_id = position_state.get("id")
                        if trade_id:
                            try:
                                db.update_trailing_sl(trade_id, new_sl)
                                logging.info(f"📄 Trailing SL updated in DB to {new_sl} for trade ID {trade_id}")
                            except Exception as e:
                                logging.error(f"❌ Failed to update trailing SL in DB: {e}", exc_info=True)
                    else:
                        logging.info(f"ℹ️ SELL SL not moved: proposed {new_sl} ≥ current {current_trailing_sl}")
                    pullback_sell_sequence.clear()
                else:
                    logging.info("⏸ SELL: No breakout yet. Keeping pullback sequence active.")

# === Strategy ===
async def detect_trade():
    logging.info("📥 Entered detect_trade() for potential setup.")
    global in_position, position_state, last_price, trade_id_counter, open_order_id, pending_setup

    if LIVE_MODE and is_kill_switch_active():
        logging.warning("🛑 Kill switch active (env). Skipping trade execution.")
        return

    capital = get_available_capital("USD")
    if capital <= 0:
        logging.error("❌ Capital fetch failed or zero. Skipping trade.")
        return    

    logging.info(f"📏 Total candles available: {len(ohlc_data)} (Ready for detection)")
    if len(ohlc_data) < REQUIRED_CANDLES:
        logging.info(f"⏳ Waiting for {REQUIRED_CANDLES} candles before detecting setup. Currently have: {len(ohlc_data)}")
        return

    if in_position:
        logging.info("⚠️ Trade already in progress. Skipping new setup.")
        return

    setup_candle = ohlc_data.iloc[-1]
    ema = setup_candle["ema"]

    if pd.isna(ema):
        logging.warning("❌ EMA missing for setup candle. Skipping trade detection.")
        return

    logging.info(
        f"🧪 Checking setup at {setup_candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S+00:00')} "
        f"| close={setup_candle['close']}, high={setup_candle['high']}, low={setup_candle['low']}, EMA={ema:.2f}"
    )

    cond_sell = (setup_candle["close"] > ema and setup_candle["low"] > ema)
    cond_buy = (setup_candle["close"] < ema and setup_candle["high"] < ema)
    logging.info(f"🔍 Sell condition: {cond_sell}, Buy condition: {cond_buy}")

    trade_date = setup_candle["timestamp"].date()
    if position_state.get("date") != trade_date:
        position_state = {"sl_count": 0, "date": trade_date}

    if position_state.get("sl_count", 0) >= MAX_SL_PER_DAY:
        logging.info("❌ Max SLs reached for the day.")
        return

    direction, entry, sl, risk = None, None, None, None
    if cond_sell:
        direction = "SELL"
        entry = setup_candle["low"]
        sl = setup_candle["high"]
        risk = sl - entry
    elif cond_buy:
        direction = "BUY"
        entry = setup_candle["high"]
        sl = setup_candle["low"]
        risk = entry - sl

    if not direction:
        logging.info("⚠️ No trade direction detected.")
        return

    risk = round(risk, 2)
    logging.info(f"🧮 Calculated risk: {risk} points")
    logging.info(f"📏 Config Risk Bounds — MIN: {MIN_CANDLE_POINTS}, MAX: {MAX_SL_POINTS}")
    if not (MIN_CANDLE_POINTS <= risk <= MAX_SL_POINTS):
        logging.info(f"⚠️ Skipped setup — Risk: {risk:.1f} pts (outside limits)")

        setup_info = {
            "symbol": SYMBOL,
            "direction": direction,
            "setup_time": setup_candle["timestamp"],
            "entry_price": entry,
            "sl_price": sl,
            "risk_points": risk,
            "triggered": False
        }

        try:
            mode = "LIVE" if LIVE_MODE else "TEST"
            db.insert_setup(setup_info, mode)
            logging.info("📉 Skipped setup logged to DB.")
        
        except Exception as e:
            error_type = f"{type(e).__name__}: {str(e)}"
            traceback_text = traceback.format_exc()
            mode = "LIVE" if LIVE_MODE else "TEST"
            symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

            context = "Failed to log skipped setup to DB"  
            logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
            db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)

        trade_log = {
            "id": None,
            "entry_time": setup_candle["timestamp"],
            "direction": direction,
            "entry_price": entry,
            "sl_price": sl,
            "qty": 0,
            "status": "SKIPPED_RISK",
            "symbol": SYMBOL,
            "risk": risk
        }
        logging.info(f"🪧 Skipped trade (risk out of bounds): {trade_log}")
        # Optionally, save to a CSV or another DB table if you want a persistent log

        return

    
    capital_per_trade = (capital * DAILY_RISK_PERCENT / 100) / MAX_SL_PER_DAY
    expected_loss = capital_per_trade
    qty = round(floor_qty(expected_loss / risk), 6)
    logging.info(
        f"🧮 Risk points: {risk:.2f}, Capital per trade: {capital_per_trade:.2f}, Qty: {qty}, "
        f"Expected Loss: {qty * risk:.2f} USD"
    )

    # ✅ Skip if below min qty
    if qty < MIN_QTY:
        logging.info("❌ Quantity too small. Skipping trade.")
        return

    # ✅ Over-sized position protection (Margin check)
    MARK_PRICE = entry
    required_margin = qty * MARK_PRICE / LEVERAGE
    if required_margin > capital:
        logging.warning(f"⚠️ Skipped oversized position | Required margin: {required_margin:.2f}, Available: {capital:.2f}")
        return
    
    msg = (
        f"[{BOT_NAME}| {TIMEFRAME} | {MODE_LABEL}] 📢 {direction} SETUP DETECTED\n"
        f"🕒 Time: {setup_candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"📉 Entry: {entry}\n"
        f"🛑 SL: {sl}\n"
        f"🎯 Risk: {risk:.1f} pts\n"
        f"📦 Qty: {qty}\n"
        f"💸 Est. Loss: {qty * risk:.2f} USD"
    )
    await send_telegram(msg)
    logging.info(f"🎯 Setup: {msg}")
    logging.info(f"📏 Entry buffer zone: {entry - ENTRY_BUFFER:.2f} to {entry + ENTRY_BUFFER:.2f}")

    position_state["setup_candle_index"] = len(ohlc_data) - 1
    position_state["entry_buffer_zone"] = (entry - ENTRY_BUFFER, entry + ENTRY_BUFFER)
    position_state["in_position"] = False  # Still waiting for trigger

    pending_setup = {
        "direction": direction,
        "entry": entry,
        "sl_price": sl,
        "risk": risk,
        "qty": qty,
        "timestamp": setup_candle["timestamp"],
        "triggered": False
    }

    setup_info = {
        "symbol": SYMBOL,
        "direction": direction,
        "setup_time": setup_candle["timestamp"],
        "entry_price": entry,
        "sl_price": sl,
        "risk_points": risk,
        "reason_skipped": None,
        "triggered": False
    }

    try:
        mode = "LIVE" if LIVE_MODE else "TEST"
        db.insert_setup(setup_info, mode)
        logging.info("✅ Setup logged to DB.")
    except Exception as e:
        error_type = f"{type(e).__name__}: {str(e)}"
        traceback_text = traceback.format_exc()
        mode = "LIVE" if LIVE_MODE else "TEST"
        symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

        context = "Failed to log setup to DB"  
        logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
        db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)

async def execute_trade(direction, price, ts, fallback=False):
    global in_position, trade_id_counter, pending_setup, last_price, position_state

    if LIVE_MODE and is_kill_switch_active():
        logging.warning("🛑 Kill switch active (env). Skipping trade execution.")
        return

    entry = price
    setup_ts = pending_setup.get("timestamp") if pending_setup else ts
    sl = pending_setup["sl_price"]
    risk = pending_setup["risk"]
    qty_btc = pending_setup["qty"]

    current_ts = datetime.now(timezone.utc)
    if fallback and (current_ts - setup_ts) > timedelta(minutes=3):
        logging.warning("⚠️ Fallback trigger too delayed. Skipping execution.")
        return

    logging.info(f"📈 EXECUTING {direction} | Price: {entry}, SL: {sl}, Qty: {qty_btc}")

    local_trade_id = f"{SYMBOL}-{uuid4().hex[:12]}"
    last_price = entry
    sl_order_id = None

    if LIVE_MODE:
        # ✅ Place Entry Order
        entry_resp = await place_entry_order(direction, entry, qty_btc)

        if isinstance(entry_resp, dict) and entry_resp.get("success"):
            logging.info(f"✅ Entry order placed | Resp: {entry_resp}")
            in_position = True
        else:
            logging.error(f"❌ Entry order placement failed: {entry_resp}")
            await send_telegram(f"[{BOT_NAME} | {MODE_LABEL}] ❌ Entry order failed.\n{entry_resp}")
            return
    else:
        logging.info("🧪 SIM MODE | Entry order not sent. Simulating:")
        simulated_order = {
            "product_id": PRODUCT_ID,
            "side": direction.lower(),
            "order_type": "limit_order",
            "price": str(entry),
            "size": qty_btc
        }
        logging.info(json.dumps(simulated_order, indent=2))
        sl_order_id = f"SIM-{trade_id_counter}"
        position_state["filled"] = True

    # ✅ Log Trade in DB
    try:
        mode = "LIVE" if LIVE_MODE else "TEST"
        trade_id = db.log_trade(
            symbol=SYMBOL,
            direction=direction,
            entry_price=float(entry),
            sl_price=float(sl),
            qty=float(qty_btc),
            entry_time=ts,
            sl_order_id=None,  # will be added after SL placement (via WS)
            mode=mode
        )
        logging.info(f"✅ Trade logged to DB with ID: {trade_id}")
    except Exception as e:
        error_type = f"{type(e).__name__}: {str(e)}"
        traceback_text = traceback.format_exc()
        logging.error(f"❌ Failed to log trade to DB | {error_type}\n{traceback_text}")
        db.log_error(SYMBOL, f"DB Log Error | {error_type}", traceback_text, mode)
        trade_id = None

    # ✅ Update internal bot state
    position_state.update({
        "id": trade_id,
        "symbol": SYMBOL,
        "direction": direction,
        "entry": entry,
        "sl_price": sl,
        "qty": qty_btc,
        "risk": risk,
        "trailing_sl": sl,
        "emergency": False,
        "block_candles": [],
        "last_price": None,
        "start_ts": ts,
        "sl_order_id": None,
        "filled": TEST_MODE
    })

    # ✅ Log Trade Locally
    trade_log = {
        "id": trade_id or trade_id_counter,
        "entry_time": ts,
        "direction": direction,
        "entry_price": entry,
        "sl_price": sl,
        "qty": qty_btc,
        "status": "OPEN",
        "symbol": SYMBOL
    }
    save_trade_log(trade_log)

    # ✅ Alert
    msg = (
        f"[{BOT_NAME}  | {TIMEFRAME}|  {MODE_LABEL}] 📥 {direction} ORDER {'PLACED & CANCELLED' if TEST_MODE else 'PLACED'}\n"
        f"🎯 Entry: {entry}\n"
        f"🛑 SL: {sl}\n"
        f"📦 Qty: {qty_btc}"
    )
    await send_telegram(msg)

async def place_entry_order(side, price, qty_btc, order_type="limit_order"):
    endpoint = "/v2/orders"
    method = "POST"
    url = "https://api.india.delta.exchange" + endpoint

    order_payload = {
        "product_id": PRODUCT_ID,
        "side": side.lower(),
        "order_type": order_type,
        "limit_price": str(price) if order_type == "limit_order" else None,
        "size": convert_btc_to_lots(-qty_btc if side == "SELL" else qty_btc),
    }

    # Remove None values
    payload = {k: v for k, v in order_payload.items() if v is not None}

    body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    timestamp, signature = generate_signature(
        DELTA_API_SECRET,
        method,
        endpoint,
        body
    )

    headers = {
        "api-key": DELTA_API_KEY,
        "timestamp": timestamp,
        "signature": signature,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            logging.info(f"📤 Sending ENTRY order: {json.dumps(payload)}")
            async with session.post(url, headers=headers, data=body) as resp:
                text = await resp.text()
                if resp.status == 200:
                    response_json = await resp.json()
                    logging.info(f"✅ ENTRY order placed successfully: {response_json}")
                    return response_json
                else:
                    logging.warning(f"⚠️ ENTRY order failed: {resp.status} | {text}")
                    return {"error": f"{resp.status} | {text}"}
    except Exception as e:
        logging.error(f"❌ ENTRY order request failed: {e}")
        return {"error": str(e)}

async def place_stop_loss_order(side, stop_price, qty):
    endpoint = "/v2/orders"
    method = "POST"
    url = "https://api.india.delta.exchange" + endpoint

    sl_side = "sell" if side.lower() == "buy" else "buy"

    payload = {
        "product_id": PRODUCT_ID,
        "side": sl_side,
        "order_type": "stop_market_order",
        "stop_price": str(stop_price),
        "size": -qty if sl_side == "sell" else qty
    }

    body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    timestamp, signature = generate_signature(
        DELTA_API_SECRET,
        method,
        endpoint,
        body
    )

    headers = {
        "api-key": DELTA_API_KEY,
        "timestamp": timestamp,
        "signature": signature,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            logging.info(f"📤 Placing SL Order: {json.dumps(payload)}")
            async with session.post(url, headers=headers, data=body) as resp:
                text = await resp.text()
                if resp.status == 200:
                    response_json = await resp.json()
                    logging.info(f"✅ SL Order placed successfully: {response_json}")
                    return response_json
                else:
                    logging.warning(f"⚠️ SL order failed: {resp.status} | {text}")
                    return {"error": f"{resp.status} | {text}"}
    except Exception as e:
        logging.error(f"❌ SL order request failed: {e}")
        return {"error": str(e)}

async def update_stop_loss_order(order_id, new_stop_price):
    endpoint = f"/v2/orders/{order_id}"
    method = "PUT"
    url = "https://api.india.delta.exchange" + endpoint

    payload = {
        "stop_price": str(round(new_stop_price, 2))
    }

    body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    timestamp, signature = generate_signature(
        DELTA_API_SECRET,
        method,
        endpoint,
        body
    )

    headers = {
        "api-key": DELTA_API_KEY,
        "timestamp": timestamp,
        "signature": signature,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            logging.info(f"🔄 Updating SL Order ID: {order_id} → New Stop Price: {new_stop_price}")
            async with session.put(url, headers=headers, data=body) as resp:
                text = await resp.text()
                if resp.status == 200:
                    response_json = await resp.json()
                    logging.info(f"✅ SL Order updated successfully: {response_json}")
                    return response_json
                else:
                    logging.warning(f"⚠️ SL update failed: Status {resp.status} | Body: {text}")
                    return {"error": f"{resp.status} | {text}"}
    except Exception as e:
        error_type = f"{type(e).__name__}: {str(e)}"
        traceback_text = traceback.format_exc()
        mode = "LIVE" if LIVE_MODE else "TEST"
        symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

        context = "SL update request failed"
        logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
        db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)
        return {"error": str(e)}

async def process_order_fill(data):
    global in_position, position_state

    order_id = data.get("order_id")
    filled_qty = float(data.get("size", 0))
    side = data.get("side")
    fill_price = float(data.get("price"))

    if not order_id or filled_qty == 0:
        return

    logging.info(f"📬 Order Filled | ID: {order_id} | Side: {side} | Qty: {filled_qty} | Price: {fill_price}")

    position_state["filled"] = True
    position_state["entry"] = fill_price  # Optional override
    position_state["last_price"] = fill_price

    # ✅ Place Stop Loss after fill
    direction = position_state.get("direction")
    sl_price = position_state.get("sl_price")
    qty = position_state.get("qty")

    if LIVE_MODE and direction and sl_price and qty:
        sl_resp = await place_stop_loss_order(direction, sl_price, qty)
        if sl_resp.get("result") and "id" in sl_resp["result"]:
            sl_order_id = sl_resp["result"]["id"]
            position_state["sl_order_id"] = sl_order_id
            db.update_sl_order_id(position_state["id"], sl_order_id)
            logging.info(f"✅ SL Order placed after fill | SL Order ID: {sl_order_id}")
        else:
            logging.warning(f"⚠️ Failed to place SL after fill: {sl_resp}")

async def process_live_price(price, ts):
    global in_position, pending_setup
    logging.debug(f"🟡 Tick | Price: {price} | Time: {ts.strftime('%H:%M:%S')}")

    # Step 1: Check for Exit Logic if in trade
    await check_exit(price, ts)

    # Step 2: If a pending setup exists and not yet triggered
    if pending_setup and not pending_setup.get("triggered", False) and not in_position:
        direction = pending_setup["direction"]
        entry = pending_setup["entry"]

        buffer_low = entry - ENTRY_BUFFER
        buffer_high = entry + ENTRY_BUFFER

        # Debugging info
        logging.debug(
            f"🧪 Setup Active → Dir: {direction} | Price: {price} | "
            f"Entry: {entry} | Buffer: {buffer_low:.1f}–{buffer_high:.1f} | Triggered: {pending_setup.get('triggered')}"
        )

        if direction == "SELL" and buffer_low <= price <= buffer_high:
            logging.info(f"✅ SELL Triggered | Tick: {price} | Entry: {entry} | Buffer: {buffer_low:.1f}–{buffer_high:.1f}")
            asyncio.create_task(execute_trade("SELL", price, ts))
            pending_setup["triggered"] = True

        elif direction == "BUY" and buffer_low <= price <= buffer_high:
            logging.info(f"✅ BUY Triggered | Tick: {price} | Entry: {entry} | Buffer: {buffer_low:.1f}–{buffer_high:.1f}")
            asyncio.create_task(execute_trade("BUY", price, ts))
            pending_setup["triggered"] = True

async def check_exit(price, ts):
    global in_position, position_state, open_order_id

    if not in_position:
        return

    if price is None:
        logging.warning("⚠️ Exit check skipped – price is None.")
        return

    required_keys = ["direction", "entry", "trailing_sl", "qty"]
    missing_keys = [k for k in required_keys if k not in position_state]
    if missing_keys:
        logging.warning(f"⚠️ Exit check skipped – missing required keys in position_state: {missing_keys}")
        return

    if LIVE_MODE and not position_state.get("filled", TEST_MODE):
        logging.info("⏳ Waiting for order fill confirmation. Skipping exit logic.")
        return

    direction = position_state.get("direction")
    entry = position_state.get("entry")
    trailing_sl = position_state.get("trailing_sl")
    emergency = position_state.get("emergency", False)
    qty = position_state.get("qty")
    symbol = position_state.get("symbol", "UNKNOWN")

    prev_last_price = position_state.get("last_price")
    position_state["last_price"] = price

    #  ➤ Emergency SL Lock-in
    sl_updated = False
    # new_sl = None
    # Trigger emergency SL only if no trailing SL update has occurred due to pullback
    has_trailed = position_state.get("has_trailed", False)
    if not emergency and not has_trailed:
        if direction == "SELL" and entry - price >= EMERGENCY_MOVE:
            new_sl = entry - EMERGENCY_LOCK
            if new_sl < trailing_sl:
                position_state["trailing_sl"] = new_sl
                position_state["emergency"] = True
                sl_updated = True
                logging.info(f"🚨 Emergency SL activated. SL moved to {new_sl}")
                await send_telegram(f"[{BOT_NAME}  | {TIMEFRAME} |  {MODE_LABEL}] 🚨 Emergency SL activated for {symbol}. SL moved to {new_sl}")

        elif direction == "BUY" and price - entry >= EMERGENCY_MOVE:
            new_sl = entry + EMERGENCY_LOCK
            if new_sl > trailing_sl:
                position_state["trailing_sl"] = new_sl
                position_state["emergency"] = True
                sl_updated = True
                logging.info(f"🚨 Emergency SL activated. SL moved to {new_sl}")
                await send_telegram(f"[{BOT_NAME}  | {TIMEFRAME} |  {MODE_LABEL}] 🚨 Emergency SL activated for {symbol}. SL moved to {new_sl}")

    # ✅ Update SL on Exchange if changed
    if sl_updated and LIVE_MODE and position_state.get("sl_order_id"):
        response = await update_stop_loss_order(position_state["sl_order_id"], position_state["trailing_sl"])
        if response.get("error"):
            logging.warning(f"⚠️ SL update failed on Delta: {response}")
        else:
            logging.info(f"✅ SL successfully updated on Delta to {position_state['trailing_sl']}")

    # ➤ Final Exit Check (with buffer)
    required_keys = ['direction', 'entry', 'trailing_sl', 'qty']
    missing = [k for k in required_keys if position_state.get(k) is None]
    if missing:
        logging.warning(f"⚠️ Exit check skipped – missing required keys in position_state: {missing}")
        return

    exit_buffer = float(os.getenv("SL_EXIT_BUFFER", 2.5))
    logging.debug(f"🧠 SL Check: Price={price}, SL={trailing_sl}, Buffer={exit_buffer}")

    if direction == "BUY" and price <= trailing_sl + exit_buffer:
        logging.info(f"🛑 STOP LOSS HIT for BUY at price {price}")
        await execute_exit("SL_HIT", price, ts)

    elif direction == "SELL" and price >= trailing_sl - exit_buffer:
        logging.info(f"🛑 STOP LOSS HIT for SELL at price {price}")
        await execute_exit("SL_HIT", price, ts)
    
async def recover_position_from_db_and_delta():
    global in_position, position_state

    positions = await fetch_open_position()
    for pos in positions:
        if pos["symbol"] != SYMBOL:
            continue
        if float(pos["size"]) == 0:
            continue

        # Now match with DB using entry_price
        db_trade = db.get_latest_open_trade(SYMBOL, float(pos["entry_price"]))
        if not db_trade:
            logging.warning(f"⚠️ No DB match found for live position at {pos['entry_price']}")
            return

        # Restore state
        position_state.update({
            "id": db_trade["id"],
            "symbol": SYMBOL,
            "direction": db_trade["direction"],
            "entry": db_trade["entry_price"],
            "sl_price": db_trade["sl_price"],
            "qty": db_trade["qty"],
            "trailing_sl": db_trade.get("trailing_sl", db_trade["sl_price"]),
            "start_ts": db_trade["entry_time"],
            "filled": True,
            "emergency": db_trade.get("emergency", False),
            "sl_order_id": db_trade.get("sl_order_id")
        })

        in_position = True
        logging.info(f"✅ Recovered active position from Delta + DB | ID: {db_trade['id']}")

        return  # Only one position expected

    logging.info("ℹ️ No open positions found for reconciliation.")

def restore_pullback_sequence():
    direction = position_state.get("direction")
    if not direction:
        return

    # Initialize both to prevent KeyError
    position_state.setdefault("pullback_buy_sequence", [])
    position_state.setdefault("pullback_sell_sequence", [])

    logging.info("✅ Pullback sequences initialized after restart.")
    
# --- WebSocket 3m candle update handling (copied from create_3min_candle_ws.py) ---
async def process_websocket_candle_update(candle_data_from_ws):
    global current_15m_candle, last_15m_candle_start_time, ohlc_data, finalized_candle_timestamps
    
    # Use 'candle_start_time' which is present in your received data
    timestamp_us = candle_data_from_ws['candle_start_time'] 
    ws_candle_start_time = datetime.fromtimestamp(timestamp_us / 1_000_000, tz=timezone.utc)
    ws_open = float(candle_data_from_ws['open'])
    ws_high = float(candle_data_from_ws['high'])
    ws_low = float(candle_data_from_ws['low'])
    ws_close = float(candle_data_from_ws['close'])
    ws_volume = float(candle_data_from_ws.get('volume', 0)) 

    # Log the incoming raw WebSocket update (can make this DEBUG level later)
    logging.info(f"--- RAW WS {TIMEFRAME} Update --- | Time: {ws_candle_start_time.strftime('%Y-%m-%d %H:%M:%S+00:00')}, O: {ws_open}, H: {ws_high}, L: {ws_low}, C: {ws_close}, V: {ws_volume}")

    
    if last_15m_candle_start_time is None or (
        ws_candle_start_time >= last_15m_candle_start_time + timedelta(minutes=15)
    ):
        # Finalize the previous candle if it exists, prevent duplicate finalization
        if current_15m_candle:
            if current_15m_candle["timestamp"] not in finalized_candle_timestamps:
                finalize_candle(current_15m_candle)
                finalized_candle_timestamps.add(current_15m_candle["timestamp"])
                await detect_trade()
            else:
                logging.debug(f"🔁 Skipping duplicate finalization for {current_15m_candle['timestamp']}")

        # Initialize a new candle for the current period
        current_15m_candle = {
            'timestamp': ws_candle_start_time,
            'open': ws_open,
            'high': ws_high,
            'low': ws_low,
            'close': ws_close,
            'volume': ws_volume
        }
        last_15m_candle_start_time = ws_candle_start_time
        logging.info(f"🆕 Starting new {TIMEFRAME} candle aggregation at: {ws_candle_start_time.strftime('%Y-%m-%d %H:%M:%S+00:00')}")
    # If it's an update for the current candle (same 15-minute period)
    else:
        current_15m_candle['high'] = max(current_15m_candle['high'], ws_high)
        current_15m_candle['low'] = min(current_15m_candle['low'], ws_low)
        current_15m_candle['close'] = ws_close
        current_15m_candle['volume'] = ws_volume # Volume is cumulative

# Combined WebSocket Message Handler
def on_message(ws, msg):
    data = json.loads(msg)

    if data.get("type") == "candlestick_15m" and data.get("symbol") == SYMBOL:
        if 'candle_start_time' in data:
            asyncio.create_task(process_websocket_candle_update(data))
        else:
            logging.warning(f"Unexpected candlestick_{TIMEFRAME} data (missing 'candle_start_time'): {data}")

    elif data.get("type") == "subscribed":
        logging.info(f"Subscription successful: {data.get('payload', {})}")

    elif data.get("type") == "error":
        logging.error(f"WebSocket Error: {data.get('payload', {})}")

    elif data.get("type") == "trades" and data.get("symbol") == SYMBOL:
        for trade in data.get("data", []):
            price = float(trade["price"])
            ts = datetime.fromtimestamp(trade["time"] / 1_000_000, tz=timezone.utc)
            logging.debug(f"🟡 Tick received | Time: {ts}, Price: {price}")

            global pending_setup, in_position, trade_id_counter, open_order_id, position_state

            if pending_setup:
                logging.debug(f"🔁 Pending Setup: {pending_setup}")
            else:
                logging.debug("🚫 No pending setup in memory")

            check_exit(price, ts)

            if ENABLE_CANDLE_FALLBACK and pending_setup and not pending_setup.get("triggered", False) and not in_position:
                direction = pending_setup["direction"]
                entry = pending_setup["entry"]
                candle_low = float(data["low"])
                candle_high = float(data["high"])

                if direction == "SELL" and candle_low <= entry:
                    slippage = entry - candle_low
                    if slippage <= MAX_SLIPPAGE_POINTS:
                        logging.warning(f"⚠️ Fallback SELL triggered | Candle Low: {candle_low} | Entry: {entry} | Slippage: {slippage:.2f}")
                        asyncio.create_task(execute_trade("SELL", candle_low, datetime.now(timezone.utc)))
                        pending_setup["triggered"] = True
                    else:
                        logging.info(f"🚫 Fallback SELL skipped (slippage {slippage:.1f} > {MAX_SLIPPAGE_POINTS})")
                elif direction == "BUY" and candle_high >= entry:
                    slippage = candle_high - entry
                    if slippage <= MAX_SLIPPAGE_POINTS:
                        logging.warning(f"⚠️ Fallback BUY triggered | Candle High: {candle_high} | Entry: {entry} | Slippage: {slippage:.2f}")
                        asyncio.create_task(execute_trade("BUY", candle_high, datetime.now(timezone.utc)))
                        pending_setup["triggered"] = True
                    else:
                        logging.info(f"🚫 Fallback BUY skipped (slippage {slippage:.1f} > {MAX_SLIPPAGE_POINTS})")

    # === Live Ticks (entry execution based on price proximity) ===
    elif data.get("type") == "all_trades" and data.get("symbol") == SYMBOL:
        price = float(data["price"])
        ts = datetime.fromtimestamp(data["timestamp"] / 1_000_000, tz=timezone.utc)

        logging.info(f"🟡 Tick | {SYMBOL} | {ts.strftime('%H:%M:%S')} | Price={price} | Size={data['size']}")

        # Diagnostic buffer zone check
        if pending_setup and not pending_setup.get("triggered", False) and not in_position:
            direction = pending_setup["direction"]
            entry = pending_setup["entry"]

            if direction == "SELL" and entry - ENTRY_BUFFER <= price <= entry + ENTRY_BUFFER:
                logging.info(f"🎯 Tick in SELL buffer | Tick: {price} | Entry: {entry} ±{ENTRY_BUFFER}")
            elif direction == "BUY" and entry - ENTRY_BUFFER <= price <= entry + ENTRY_BUFFER:
                logging.info(f"🎯 Tick in BUY buffer | Tick: {price} | Entry: {entry} ±{ENTRY_BUFFER}")

        # Entry execution logic
        asyncio.create_task(process_live_price(price, ts))
    elif data.get("type") == "v2/orders":
        logging.info(f"📬 ORDER STATUS UPDATE: {json.dumps(data, indent=2)}")
    # You can optionally track open/closed order status here
    # Useful for debugging rejections or partial fills

    elif data.get("type") == "v2/fills":
        logging.info(f"📦 ORDER FILL CONFIRMED: {json.dumps(data, indent=2)}")

        # 🔁 Place SL order after entry fill
        asyncio.create_task(process_order_fill(data))

        # 🛑 Handle stop-loss fills (if any)
        if "order" in data:
            order = data["order"]
            if order.get("order_type") == "market_order" and order.get("reason") == "stop_loss":
                asyncio.create_task(execute_exit("SL_FILLED", float(order["price"]), datetime.now(timezone.utc)))

# === WebSocket Connection and Main Loop ===
async def connect_ws():
    retry_delay = 5
    max_retry = 60
    attempt = 0

    while True:
        try:
            async with websockets.connect("wss://socket.india.delta.exchange") as ws:
                logging.info("🟢 Async WebSocket connected")
                await send_telegram(f"[{BOT_NAME}  | {TIMEFRAME} |  {MODE_LABEL}]✅ WebSocket reconnected successfully.")
                
                # Subscribe to candlestick_{TIMEFRAME} and trades
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "candlestick_15m", "symbols": [SYMBOL]},
                            {"name": "all_trades", "symbols": [SYMBOL]}
                        ]
                    }
                }))
                logging.info(f"📡 Subscribed to candlestick_{TIMEFRAME} and trades")

                if LIVE_MODE:
                    # Subscribe to private channels if keys exist
                    logging.info("📡 Subscribing to private channels for LIVE_MODE")
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "payload": {
                            "channels": [
                                {"name": "v2/orders", "symbols": [SYMBOL]},
                                {"name": "v2/fills", "symbols": [SYMBOL]},
                                {"name": "positions", "symbols": [SYMBOL]}
                            ]
                        }
                    }))

                while True:
                    msg = await ws.recv()
                    # on_message is a synchronous function, but it creates async tasks within it.
                    on_message(ws, msg) 

        except Exception as e:
            error_type = f"{type(e).__name__}: {str(e)}"
            traceback_text = traceback.format_exc()
            mode = "LIVE" if LIVE_MODE else "TEST"
            symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

            context = "WebSocket error"  
            logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
            db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)
            await send_telegram(f"[{BOT_NAME}  | 3Min|  {MODE_LABEL}] ❌ WebSocket disconnected!\nError: {e}")
            await asyncio.sleep(5)
            if attempt > 20:
                logging.error("❌ Max reconnect attempts reached. Exiting...")
                break

async def handle_positions_ws(data):
    try:
        if data.get("type") != "positions":
            return  # skip unrelated

        action = data.get("action", "")
        symbol = data.get("symbol", "")
        size = int(data.get("size", 0))
        entry_price = float(data.get("entry_price", 0))
        margin = float(data.get("margin", 0))
        liquidation = float(data.get("liquidation_price", 0))
        timestamp = datetime.now(timezone.utc)
        direction = "BUY" if size > 0 else "SELL" if size < 0 else None

        if size == 0:
            logging.info(f"📉 WS Position closed for {symbol}")
            in_position = False
            position_state.clear()
        else:
            position_state.update({
                "symbol": symbol,
                "direction": direction,
                "entry": entry_price,
                "size": size,
                "margin": margin,
                "liquidation_price": liquidation,
                "last_update": timestamp
            })
            in_position = True
            logging.info(f"📈 WS Position Updated | {symbol} | Size: {size} | Entry: {entry_price}")

        # Optional DB logging
        db.log_position_snapshot(symbol, size, entry_price, margin, liquidation, timestamp)

    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        tb = traceback.format_exc()
        logging.error(f"❌ WS Position handler failed | {err}\n{tb}")
        db.log_error(symbol, f"WS_Position_Error | {err}", tb, mode="LIVE")

async def initialize_state_from_delta():
    global in_position, position_state

    try:
        response = await check_ws_position_consistency()
        positions = response.get("result", [])

        active_positions = [
            pos for pos in positions
            if pos.get("product_id") == PRODUCT_ID and float(pos.get("size", 0)) > 0
        ]

        if active_positions:
            pos = active_positions[0]
            in_position = True
            position_state = {
                "entry_time": datetime.now(tz=UTC),
                "entry": float(pos["entry_price"]),
                "qty": float(pos["size"]),
                "direction": pos["side"].upper(),
                "sl": None,
                "risk": None,
                "trailing_sl": None,
                "emergency": False,
                "expected_loss": None,
                "trade_id": 999,  # Placeholder
                "order_id": pos.get("order_id", "delta-live")
            }
            logging.info("🔄 Live position found. in_position set to True.")
            await send_telegram(f"[{BOT_NAME}  | {TIMEFRAME} |  {MODE_LABEL}] 🔄 Live position found on Delta. Resuming tracking.")
        else:
            in_position = False
            position_state = {}
            logging.info("✅ [DELTA INIT] No open positions found. Bot ready to trade.")
            await send_telegram(f"[{BOT_NAME}  | {TIMEFRAME} |  {MODE_LABEL}] ✅ No live position found. Bot state is clean.")
    
    except Exception as e:
        error_type = f"{type(e).__name__}: {str(e)}"
        traceback_text = traceback.format_exc()
        mode = "LIVE" if LIVE_MODE else "TEST"
        symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

        context = "Failed to initialize state"  
        logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
        db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)

async def heartbeat_loop():
    logging.info("🫀 Heartbeat loop started. Bot health will be monitored.")
    
    while True:
        try:
            mode = "LIVE" if LIVE_MODE else "TEST"
            db.log_heartbeat(SYMBOL, "alive", mode)
            write_heartbeat()

        except Exception as e:
            error_type = f"{type(e).__name__}: {str(e)}"
            traceback_text = traceback.format_exc()
            mode = "LIVE" if LIVE_MODE else "TEST"
            symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

            context = "Failed to log heartbeat"  
            logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
            db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)
        
        await asyncio.sleep(60)  # Ping every 60 seconds

async def execute_exit(reason, price, ts):

    global in_position, pending_setup

    logging.warning(f"📉 Exit Trade | Reason: {reason} | Price: {price}")

    try:
        trade_id = position_state.get("id")
        if trade_id:
            try:
                trailing_sl = float(position_state.get("trailing_sl"))
            except (TypeError, ValueError):
                trailing_sl = None

            entry = float(position_state.get("entry"))
            sl_price = float(position_state.get("sl_price"))
            qty = float(position_state.get("qty"))

            # Auto-assign reason
            if reason == "SL_HIT":
                reason = "TRAILING_SL_HIT" if trailing_sl and trailing_sl != sl_price else "INITIAL_SL_HIT"

            pnl = round((price - entry) * qty * (-1 if position_state.get("direction") == "SELL" else 1), 3)
            mode = "LIVE" if LIVE_MODE else "TEST"

            db.update_trade_exit(
                trade_id=trade_id,
                exit_price=float(price),
                exit_time=ts,
                pnl=pnl,
                exit_reason=reason,
                trailing_sl=trailing_sl,
                mode=mode
            )
            logging.info(f"✅ DB exit update successful for trade ID {trade_id}")

            # ✅ Fetch balance after trade exit (only in LIVE)
            if LIVE_MODE:
                balance = await get_available_capital()
                ts = datetime.now(timezone.utc)
                db.log_wallet_balance("USD", balance, ts, "LIVE")

    except Exception as e:
        error_type = f"{type(e).__name__}: {str(e)}"
        traceback_text = traceback.format_exc()
        mode = "LIVE" if LIVE_MODE else "TEST"
        symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

        context = "DB exit update failed"
        logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
        db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)

    in_position = False
    position_state.clear()
    pending_setup = None

    await send_telegram(
        f"[{BOT_NAME}  | {TIMEFRAME} |  {MODE_LABEL}] 📉 EXIT | Reason: {reason} | Price: {price} | Time: {ts.strftime('%H:%M:%S UTC')}"
    )

def save_trade_log(log_entry):
    try:
        # Ensure all required fields exist
        required_keys = ["symbol", "direction", "entry_price", "sl_price", "qty", "entry_time"]
        for key in required_keys:
            if key not in log_entry:
                raise KeyError(f"Missing required key: {key}")

        filename = f"data/{log_entry['symbol'].lower()}_trades.csv"
        file_exists = os.path.isfile(filename)

        # Append to CSV
        with open(filename, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=required_keys)
            if not file_exists:
                writer.writeheader()
            writer.writerow({k: log_entry[k] for k in required_keys})

        logging.info(f"📄 Trade saved to CSV: {filename}")
    
    except Exception as e:
        error_type = f"{type(e).__name__}: {str(e)}"
        traceback_text = traceback.format_exc()
        mode = "LIVE" if LIVE_MODE else "TEST"
        symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

        context = "Failed to save trade log to CSV"  
        logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
        db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)                       

if __name__ == "__main__":
    logging.info("🚀 Starting Hybrid Bot in SIMULATION mode" if not LIVE_MODE else "🚀 Starting Hybrid Bot in LIVE mode")
    logging.info("🚀 Hybrid Bot Startup Summary")
    logging.info(f"📈 Symbol       : {SYMBOL}")
    logging.info(f"🫀 Healthcheck  : ENABLED (heartbeat every 60s)")
    logging.info(f"📂 Data Path    : {TRADES_LOG_PATH}")
    logging.info("📡 Initializing WebSocket and strategy components...\n")
    logging.info(f"🎯 MAX_SLIPPAGE_POINTS loaded: {MAX_SLIPPAGE_POINTS}")
    logging.info(f"🔧 Mode : {MODE_LABEL}")
    mode = "LIVE" if LIVE_MODE else "TEST"
    db.log_event(SYMBOL, "BOT_STARTED", f"Bot started on {datetime.now(timezone.utc).isoformat()}")
   
    async def main():
        os.makedirs("data", exist_ok=True)
        get_available_capital("USD")
        asyncio.create_task(heartbeat_loop())
        await connect_ws()
        positions = await check_ws_position_consistency(product_id=PRODUCT_ID)
        if positions:
            print("Open Positions:", positions)
        else:
            print("No open positions found or failed to fetch.")
        if LIVE_MODE:
            await recover_position_from_db_and_delta()
        

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 Bot manually stopped.")
    
    except Exception as e:
        error_type = f"{type(e).__name__}: {str(e)}"
        traceback_text = traceback.format_exc()
        mode = "LIVE" if LIVE_MODE else "TEST"
        symbol = SYMBOL if 'SYMBOL' in globals() else "UNKNOWN"

        context = "Fatal error in main execution"  
        logging.error(f"❌ {context} | {error_type}\n{traceback_text}")
        db.log_error(symbol, f"{context} | {error_type}", traceback_text, mode)