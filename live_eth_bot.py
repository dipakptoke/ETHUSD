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

# === Logging ===
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.FileHandler("logs/eth_bot.log"),
        logging.StreamHandler()
    ])


# === File paths for logs ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
LIVE_MODE = os.getenv("LIVE_MODE", "0") == "1"
TEST_MODE = os.getenv("TEST_MODE", "0") == "1"
DELTA_API_KEY = os.getenv("DELTA_API_KEY")
DELTA_API_SECRET = os.getenv("DELTA_API_SECRET")
ENTRY_BUFFER = float(os.getenv("ENTRY_BUFFER", "9.0"))
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "100"))
logging.info(f"🎯 ENTRY_BUFFER loaded: ±{ENTRY_BUFFER} points")
ENABLE_CANDLE_FALLBACK = True  # Set to False to disable fallback logic
MAX_SLIPPAGE_POINTS = float(os.getenv("MAX_SLIPPAGE_POINTS", "27"))
HEARTBEAT_FILE = "/app/data/heartbeat"
TRADES_LOG_PATH = os.getenv("TRADES_LOG_PATH", "/app/data/live_trades.csv")

# === Global variables for 15m candle aggregation ===
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
# Cache for latest tick
latest_tick = {"price": None, "timestamp": None}

# === Config ===
SYMBOL = "ETHUSD"
EMA_PERIOD = 5
TRAIL_BUFFER = 5
EMERGENCY_MOVE = 500
EMERGENCY_LOCK = 150
MIN_CANDLE_POINTS = 10
MAX_SL_POINTS = 70
CAPITAL = 115
DAILY_RISK_PERCENT = 6
MAX_SL_PER_DAY = 3
MIN_QTY = 0.01
LEVERAGE = 25

UTC = timezone.utc

REQUIRED_CANDLES = EMA_PERIOD + 2
ohlc_data = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "ema", "volume"]) # Added volume
# current_bar = None # REMOVED: No longer needed, using current_15m_candle
in_position = False
position_state = {}
last_price = None # This is likely updated by a separate price feed, not directly tied to candle aggregation here
trade_id_counter = 0  # unique ID per trade
open_order_id = None

# === Helpers ===
def floor_qty(q): return math.floor(q * 1000) / 1000

async def handle_private_ws_msg(msg):
    topic = msg.get("topic", "")
    data = msg.get("data", {})

    if topic.startswith("v2/fills"):
        await process_order_fill(data)
    elif topic.startswith("v2/orders"):
        await process_order_status(data)

async def send_telegram(msg):
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}) as response:
                    await response.text()
        except Exception as e:
            logging.error(f"Telegram error: {e}")

def write_heartbeat():
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(datetime.now(timezone.utc).isoformat() + "\n")
            f.flush()  # <-- force buffer to disk
            os.fsync(f.fileno())  # <-- ensure it's written even on Docker mounts
        logging.info(f"🫀 Heartbeat written successfully.")
    except Exception as e:
        logging.error(f"Heartbeat write failed: {e}")

# === Delta Exchange Order Placement (Add HMAC-SHA256 signature if required) ===
def generate_signature(secret_key, method, endpoint, body=""):
    ts = str(int(datetime.now(tz=UTC).timestamp()))
    message = f"{method.upper()}{ts}{endpoint}{body}"  # ✅ Correct order
    signature = hmac.new(secret_key.encode(), message.encode(), hashlib.sha256).hexdigest()
    return ts, signature

async def place_market_order(side, qty, order_type="limit"):
    if not LIVE_MODE:
        logging.info(f"🧪 SIMULATION MODE: Skipping real order for {side} {qty}")
        return {"status": "simulated", "side": side, "qty": qty}

    endpoint = "/orders"
    url = "https://api.india.delta.exchange" + endpoint
    order = {
        "product_id": PRODUCT_ID,
        "size": qty,
        "side": side.lower(),
        "order_type": order_type,
        "time_in_force": "immediate_or_cancel" if order_type == "market" else "post_only"
    }
    body = json.dumps(order)
    ts, signature = generate_signature(DELTA_API_SECRET, "POST", endpoint, body)

    headers = {
        "api-key": DELTA_API_KEY,
        "timestamp": ts,
        "signature": signature,
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=body) as response:
                response_json = await response.json()
                logging.info(f"📤 LIVE ORDER SENT | {side.upper()} | Status: {response.status} | Response: {response_json}")

                # TEST_MODE: cancel the real order right after placement
                if TEST_MODE and "result" in response_json and response_json["result"]:
                    order_id = response_json["result"].get("id")
                    if order_id:
                        cancel_endpoint = f"/v2/orders/{order_id}/cancel"
                        cancel_url = "https://api.india.delta.exchange" + cancel_endpoint
                        cancel_ts, cancel_sig = generate_signature(DELTA_API_SECRET, "POST", cancel_endpoint)
                        cancel_headers = {
                            "api-key": DELTA_API_KEY,
                            "timestamp": cancel_ts,
                            "signature": cancel_sig,
                            "Content-Type": "application/json"
                        }
                        async with session.post(cancel_url, headers=cancel_headers) as cancel_response:
                            cancel_json = await cancel_response.json()
                            logging.info(f"🧪 TEST MODE: Order {order_id} cancelled. Response: {cancel_json}")
                            return {"status": "test-cancelled", "order_id": order_id}

                return response_json

    except Exception as e:
        logging.error(f"❌ Order placement failed: {e}")
        return {"status": "error", "message": str(e)}

async def place_bracket_order(side, price, qty, sl_price, tp_price=None):
    endpoint = "/v2/orders/bracket"
    url = "https://api.india.delta.exchange" + endpoint

    payload = {
        "product_id": PRODUCT_ID,
        "side": side.lower(),
        "order_type": "limit_order",
        "price": str(price),
        "size": qty,
        "stop_loss_order": {
            "order_type": "market_order",
            "stop_price": str(sl_price)
        },
        "bracket_stop_trigger_method": "last_traded_price"
    }

    if tp_price:
        payload["take_profit_order"] = {
            "order_type": "limit_order",
            "stop_price": str(tp_price),
            "limit_price": str(tp_price)
        }

    body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    ts, signature = generate_signature(DELTA_API_SECRET, "POST", endpoint, body)

    headers = {
        "api-key": DELTA_API_KEY,
        "timestamp": ts,
        "signature": signature,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    if not LIVE_MODE:
        logging.info("🧪 SIM MODE | Bracket order not sent. Would send:")
        logging.info(json.dumps(payload, indent=2))
        return {"simulated": True, "payload": payload}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=body) as resp:
                resp_json = await resp.json()
                if resp.status == 200:
                    logging.info(f"✅ Bracket Order Placed | Side: {side.upper()} | Price: {price} | Qty: {qty}")
                else:
                    logging.warning(f"⚠️ Bracket order status {resp.status} | Resp: {resp_json}")
                return resp_json
    except Exception as e:
        logging.error(f"❌ Bracket order failed: {e}")
        return {"error": str(e)}

async def process_order_fill(data):
    global in_position, position_state

    order_id = data.get("order_id")
    filled_qty = float(data.get("size", 0))
    side = data.get("side")
    price = data.get("price")

    if not order_id or filled_qty == 0:
        return

    logging.info(f"📬 Order Filled | ID: {order_id} | Side: {side} | Qty: {filled_qty} | Price: {price}")

    if in_position and position_state.get("bracket_order_id") == order_id:
        position_state["filled"] = True
        position_state["fill_price"] = float(price)
        logging.info(f"✅ Entry Fill Confirmed | Order ID: {order_id} | Fill Price: {price}")
    else:
        logging.warning(f"⚠️ Fill received for unmatched or inactive order ID: {order_id}")

# === Candle Management (Only finalize_candle is used now, as WS handles updates) ===
def finalize_candle(candle):
    global ohlc_data, in_position, position_state

    # Validate input
    if not isinstance(candle, dict) or any(k not in candle for k in ["timestamp", "open", "high", "low", "close", "volume"]):
        logging.error(f"❌ Invalid candle data provided to finalize_candle: {candle}")
        return

    candle_df = pd.DataFrame([candle])
    if candle_df[["open", "high", "low", "close", "volume"]].isnull().values.any():
        logging.warning(f"⚠️ Skipping finalization due to NaN values in candle: {candle}")
        return

    logging.info(f"🆕 Adding new candle with timestamp: {candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S+00:00')}")
    if not ohlc_data.empty:
        ohlc_data = pd.concat([ohlc_data, candle_df], ignore_index=True).copy()
    else:
        ohlc_data = candle_df.copy()

    # Trim candles
    if len(ohlc_data) > MAX_CANDLES:
        ohlc_data = ohlc_data.iloc[-MAX_CANDLES:].reset_index(drop=True)

    # EMA calculation
    if len(ohlc_data) >= EMA_PERIOD:
        ohlc_data["ema"] = ohlc_data["close"].ewm(span=EMA_PERIOD, adjust=False).mean()

    logging.info(f"📊 Finalized 15m candle: {ohlc_data.iloc[-1].to_dict()}")
    if len(ohlc_data) >= EMA_PERIOD:
        logging.info(f"🔍 EMA value calculated: {ohlc_data.iloc[-1]['ema']:.2f}")
    else:
        logging.info("🔍 EMA not yet calculated (not enough candles).")

      # ✅ Trailing SL via 15m candles
    if in_position:
        update_trailing_sl_from_candle(candle)

def update_trailing_sl_from_candle(candle):
    global position_state, in_position

    if not in_position or not candle:
        return

    direction = position_state.get("direction")
    trailing_sl = position_state.get("trailing_sl")
    entry = position_state.get("entry")
    symbol = position_state.get("symbol", "UNKNOWN")
    sl_order_id = position_state.get("sl_order_id")
    trail_buffer = float(os.getenv("TRAIL_BUFFER", 2.0))

    candle_open = candle["open"]
    candle_close = candle["close"]
    candle_high = candle["high"]
    candle_low = candle["low"]

    if direction == "SELL":
        is_green = candle_close > candle_open
        broke_low = candle_low < entry  # ✅ must break low of pullback
        if is_green and broke_low:
            new_sl = round(candle_high + trail_buffer, 1)
            if new_sl < trailing_sl:
                position_state["trailing_sl"] = new_sl
                logging.info(f"📉 Trailing SL moved to {new_sl} after GREEN candle break (SELL)")
                asyncio.create_task(send_telegram(
                    f"📉 Trailing SL moved to {new_sl} after green pullback for {symbol}"))
                if LIVE_MODE and sl_order_id:
                    asyncio.create_task(update_stop_loss_order(sl_order_id, new_sl))

    elif direction == "BUY":
        is_red = candle_close < candle_open
        broke_high = candle_high > entry  # ✅ must break high of red pullback
        if is_red and broke_high:
            new_sl = round(candle_low - trail_buffer, 1)
            if new_sl > trailing_sl:
                position_state["trailing_sl"] = new_sl
                logging.info(f"📈 Trailing SL moved to {new_sl} after RED candle break (BUY)")
                asyncio.create_task(send_telegram(
                    f"📈 Trailing SL moved to {new_sl} after red pullback for {symbol}"))
                if LIVE_MODE and sl_order_id:
                    asyncio.create_task(update_stop_loss_order(sl_order_id, new_sl))

# === Strategy ===
async def detect_trade():
    logging.info("📥 Entered detect_trade() for potential setup.")
    global in_position, position_state, last_price, trade_id_counter, open_order_id, pending_setup

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

    logging.info(f"🧮 Calculated risk: {risk} points")
    if risk < MIN_CANDLE_POINTS or risk > MAX_SL_POINTS:
        logging.info(f"⚠️ Skipped setup — Risk: {risk:.1f} pts (outside limits)")
        return

    capital_per_trade = (CAPITAL * DAILY_RISK_PERCENT / 100) / MAX_SL_PER_DAY
    expected_loss = capital_per_trade
    qty = floor_qty(expected_loss / risk)
    logging.info(
        f"🧮 Risk points: {risk:.2f}, Capital per trade: {capital_per_trade:.2f}, Qty: {qty}, "
        f"Expected Loss: {qty * risk:.2f} USDT"
    )

    if qty < MIN_QTY:
        logging.info("❌ Quantity too small. Skipping trade.")
        return

    msg = (
        f"📢 {direction} SETUP DETECTED IN {SYMBOL}\n"
        f"🕒 Time: {setup_candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"📉 Entry: {entry}\n"
        f"🛑 SL: {sl}\n"
        f"🎯 Risk: {risk:.1f} pts\n"
        f"📦 Qty: {qty}\n"
        f"💸 Est. Loss: {qty * risk:.2f} USDT"
    )
    await send_telegram(msg)
    logging.info(f"🎯 Setup: {msg}")
    logging.info(f"📏 Entry buffer zone: {entry - ENTRY_BUFFER:.2f} to {entry + ENTRY_BUFFER:.2f}")

    pending_setup = {
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "risk": risk,
        "qty": qty,
        "timestamp": setup_candle["timestamp"],
        "triggered": False
    }

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

async def execute_trade(direction, price, ts, fallback=False):
    global in_position, trade_id_counter, pending_setup, last_price, position_state

    entry = price
    setup_ts = pending_setup.get("timestamp") if pending_setup else ts
    sl = pending_setup["sl"]
    risk = pending_setup["risk"]
    qty = pending_setup["qty"]

    current_ts = datetime.now(timezone.utc)
    if fallback and (current_ts - setup_ts) > timedelta(minutes=15):
        logging.warning("⚠️ Fallback trigger too delayed. Skipping execution.")
        return

    logging.info(f"📈 EXECUTING {direction} | Price: {entry}, SL: {sl}, Qty: {qty}")

    sl_order_id = None

    if LIVE_MODE:
        response = await place_bracket_order(direction, entry, qty, sl)
        if isinstance(response, dict):
            if response.get("success") and "id" in response:
                sl_order_id = response["id"]
                logging.info(f"✅ Bracket order placed. SL Order ID: {sl_order_id}")
            elif response.get("result") and "id" in response["result"]:
                sl_order_id = response["result"]["id"]
                logging.info(f"✅ Bracket order placed (nested). SL Order ID: {sl_order_id}")
            else:
                logging.warning(f"⚠️ Could not find SL Order ID in response: {response}")
        else:
            logging.error(f"❌ Unexpected bracket order response: {response}")
    else:
        logging.info("🧪 SIM MODE | Bracket order not sent. Would send:")
        simulated_order = {
            "product_id": PRODUCT_ID,
            "side": direction.lower(),
            "order_type": "limit_order",
            "price": str(entry),
            "size": qty,
            "stop_loss_order": {
                "order_type": "market_order",
                "stop_price": str(sl)
            },
            "bracket_stop_trigger_method": "last_traded_price"
        }
        logging.info(json.dumps(simulated_order, indent=2))
        sl_order_id = f"SIM-{trade_id_counter}"

    # 🔁 Update bot state
    trade_id_counter += 1
    in_position = True
    last_price = entry

    position_state.update({
        "id": trade_id_counter,
        "symbol": SYMBOL,
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "qty": qty,
        "risk": risk,
        "trailing_sl": sl,
        "emergency": False,
        "block_candles": [],
        "last_price": None,
        "start_ts": ts,
        "sl_order_id": sl_order_id,
        "filled": False,
        "bracket_order_id": sl_order_id
    })

    trade_log = {
        "id": trade_id_counter,
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "direction": direction,
        "entry_price": entry,
        "sl": sl,
        "qty": qty,
        "status": "OPEN"
    }
    save_trade_log(trade_log)

    msg = (
        f"📥 {direction} ORDER PLACED\n"
        f"🎯 Entry: {entry}\n"
        f"🛑 SL: {sl}\n"
        f"📦 Qty: {qty}"
    )
    await send_telegram(msg)


async def check_exit(price, ts):
    global in_position, position_state, open_order_id

    if not in_position:
        return
    
    if not position_state.get("filled", False):
        logging.warning("⏳ Waiting for order fill confirmation. Skipping exit logic.")
        return

    direction = position_state.get("direction")
    entry = position_state.get("entry")
    trailing_sl = position_state.get("trailing_sl")
    emergency = position_state.get("emergency", False)
    qty = position_state.get("qty")
    symbol = position_state.get("symbol", "UNKNOWN")

    prev_last_price = position_state.get("last_price")
    position_state["last_price"] = price

    if "block_candles" not in position_state:
        position_state["block_candles"] = []
    block = position_state["block_candles"]

    # ➤ Trailing SL Logic (Block Pullbacks)
    sl_updated = False
    new_sl = None

    if direction == "SELL":
        if prev_last_price is not None and price > prev_last_price:
            block.append(price)
            if len(block) > 3:
                block.pop(0)
        elif block and price < min(block):
            new_sl = max(block) + TRAIL_BUFFER
            if new_sl < trailing_sl:
                position_state["trailing_sl"] = new_sl
                sl_updated = True
                block.clear()
                logging.info(f"📉 Trailing SL moved to {new_sl} after green pullback block")
                await send_telegram(f"📉 Trailing SL moved to {new_sl} after green pullback block for {symbol}")

    elif direction == "BUY":
        if prev_last_price is not None and price < prev_last_price:
            block.append(price)
            if len(block) > 3:
                block.pop(0)
        elif block and price > max(block):
            new_sl = min(block) - TRAIL_BUFFER
            if new_sl > trailing_sl:
                position_state["trailing_sl"] = new_sl
                sl_updated = True
                block.clear()
                logging.info(f"📈 Trailing SL moved to {new_sl} after red pullback block")
                await send_telegram(f"📈 Trailing SL moved to {new_sl} after red pullback block for {symbol}")

    # ➤ Emergency SL Lock-in
    if not emergency:
        if direction == "SELL" and entry - price >= EMERGENCY_MOVE:
            new_sl = entry - EMERGENCY_LOCK
            if new_sl < trailing_sl:
                position_state["trailing_sl"] = new_sl
                position_state["emergency"] = True
                sl_updated = True
                logging.info(f"🚨 Emergency SL activated. SL moved to {new_sl}")
                await send_telegram(f"🚨 Emergency SL activated for {symbol}. SL moved to {new_sl}")

        elif direction == "BUY" and price - entry >= EMERGENCY_MOVE:
            new_sl = entry + EMERGENCY_LOCK
            if new_sl > trailing_sl:
                position_state["trailing_sl"] = new_sl
                position_state["emergency"] = True
                sl_updated = True
                logging.info(f"🚨 Emergency SL activated. SL moved to {new_sl}")
                await send_telegram(f"🚨 Emergency SL activated for {symbol}. SL moved to {new_sl}")

    # ✅ Update SL on Exchange if changed
    if sl_updated and LIVE_MODE and position_state.get("sl_order_id"):
        response = await update_stop_loss_order(position_state["sl_order_id"], position_state["trailing_sl"])
        if response.get("error"):
            logging.warning(f"⚠️ SL update failed on Delta: {response}")
        else:
            logging.info(f"✅ SL successfully updated on Delta to {position_state['trailing_sl']}")

    # ➤ Final Exit Check (with buffer)
    trailing_sl = position_state.get("trailing_sl")
    if price is None or trailing_sl is None:
        logging.warning("⚠️ Exit check skipped – missing price or SL.")
        return

    exit_buffer = float(os.getenv("SL_EXIT_BUFFER", 2.5))
    logging.debug(f"🧠 SL Check: Price={price}, SL={trailing_sl}, Buffer={exit_buffer}")

    if direction == "BUY" and price <= trailing_sl + exit_buffer:
        logging.info(f"🛑 STOP LOSS HIT for BUY at price {price}")
        await execute_exit("SL_HIT", price, ts)

    elif direction == "SELL" and price >= trailing_sl - exit_buffer:
        logging.info(f"🛑 STOP LOSS HIT for SELL at price {price}")
        await execute_exit("SL_HIT", price, ts)

async def update_stop_loss_order(order_id, new_sl_price):
    url = "https://api.india.delta.exchange/v2/orders/edit-bracket"
    payload = {
        "order_id": order_id,
        "stop_loss_order": {
            "order_type": "market_order",
            "stop_price": str(new_sl_price)
        },
        "bracket_stop_trigger_method": "last_traded_price"
    }

    body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    ts, signature = generate_signature(
        DELTA_API_SECRET, "POST", "/v2/orders/edit-bracket", body
    )

    headers = {
        "api-key": DELTA_API_KEY,
        "timestamp": ts,
        "signature": signature,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=body) as resp:
                resp_json = await resp.json()
                if resp.status == 200 and resp_json.get("status") == "success":
                    logging.info(f"✅ SL Updated Successfully | New SL: {new_sl_price}")
                else:
                    logging.warning(f"⚠️ SL Update Failed | API Response: {resp_json}")
                return resp_json
    except Exception as e:
        logging.error(f"❌ SL Update Exception: {e}")
        return {"error": str(e)}
    
# --- WebSocket 15m candle update handling (copied from create_15min_candle_ws.py) ---
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
    logging.info(f"--- RAW WS 15m Update --- | Time: {ws_candle_start_time.strftime('%Y-%m-%d %H:%M:%S+00:00')}, O: {ws_open}, H: {ws_high}, L: {ws_low}, C: {ws_close}, V: {ws_volume}")

    # Check if a new 15-minute candle period has started
    if last_15m_candle_start_time is None or ws_candle_start_time > last_15m_candle_start_time:
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
        logging.info(f"🆕 Starting new 15m candle aggregation at: {ws_candle_start_time.strftime('%Y-%m-%d %H:%M:%S+00:00')}")
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
            logging.warning(f"Unexpected candlestick_15m data (missing 'candle_start_time'): {data}")

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
                await send_telegram(f"✅ WebSocket reconnected successfully for {SYMBOL}.")
                
                # Subscribe to candlestick_15m and trades
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "candlestick_15m", "symbols": [SYMBOL]},
                            {"name": "all_trades", "symbols": [SYMBOL]}
                            #{"name": "trades", "symbols": [SYMBOL]}
                        ]
                    }
                }))
                logging.info("📡 Subscribed to candlestick_15m and trades")

                if LIVE_MODE:
                    # Subscribe to private channels if keys exist
                    logging.info("📡 Subscribing to private channels for LIVE_MODE")
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "payload": {
                            "channels": [
                                {"name": "v2/orders", "symbols": [SYMBOL]},
                                {"name": "v2/fills", "symbols": [SYMBOL]}
                            ]
                        }
                    }))

                while True:
                    msg = await ws.recv()
                    # on_message is a synchronous function, but it creates async tasks within it.
                    on_message(ws, msg) 

        except Exception as e:
            logging.error(f"WebSocket error: {e}. Retrying in {retry_delay} sec...")
            await send_telegram(f"❌ WebSocket disconnected for BTCUSD!\nError: {e}")
            await asyncio.sleep(5)
            if attempt > 100:
                logging.error("❌ Max reconnect attempts reached. Exiting...")
                break


async def get_open_positions():
    endpoint = "/v2/positions/margined"
    url = "https://api.india.delta.exchange" + endpoint
    ts, signature = generate_signature(DELTA_API_SECRET, "GET", endpoint)
    headers = {
        "api-key": DELTA_API_KEY,
        "timestamp": ts,
        "signature": signature,
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    text = await response.text()
                    logging.error(f"❌ Delta API error: {response.status} | Body: {text}")
                    return {}
                return await response.json()
    except Exception as e:
        logging.error(f"Failed to fetch margined positions: {e}")
        return {}

async def initialize_state_from_delta():
    global in_position, position_state

    try:
        response = await get_open_positions()
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
            await send_telegram("🔄 Live position found on Delta. Resuming tracking.")
        else:
            in_position = False
            position_state = {}
            logging.info("✅ No open positions found. Bot ready to trade.")
            await send_telegram("✅ No live position found. Bot state is clean.")
    except Exception as e:
        logging.error(f"Failed to initialize state: {e}")

async def heartbeat_loop():
    logging.info("🫀 Heartbeat loop started. Bot health will be monitored.")
    while True:
        write_heartbeat()
        await asyncio.sleep(60)  # Update every 60 seconds

async def execute_exit(reason, price, ts):
    global in_position
    logging.warning(f"📉 Exit Trade | Reason: {reason} | Price: {price}")
    in_position = False
    position_state.clear()

    await send_telegram(f"📉 EXIT | Reason: {reason} | Price: {price} | Time: {ts.strftime('%H:%M:%S UTC')}")

def save_trade_log(log_entry):
    try:
        with open(TRADES_LOG_PATH, "a") as f:
            writer = csv.DictWriter(f, fieldnames=log_entry.keys())
            if f.tell() == 0:
                writer.writeheader()
            writer.writerow(log_entry)
        logging.info(f"📄 Trade logged successfully to {TRADES_LOG_PATH}.")
    except Exception as e:
        logging.error(f"❌ Failed to log trade: {e}")

if __name__ == "__main__":
    logging.info("🚀 Starting Hybrid Bot in SIMULATION mode" if not LIVE_MODE else "🚀 Starting Hybrid Bot in LIVE mode")
    logging.info("🚀 Hybrid Bot Startup Summary")
    logging.info(f"📈 Symbol       : {SYMBOL}")
    logging.info(f"🫀 Healthcheck  : ENABLED (heartbeat every 60s)")
    logging.info(f"📂 Data Path    : {TRADES_LOG_PATH}")
    logging.info("📡 Initializing WebSocket and strategy components...\n")
    logging.info(f"🎯 MAX_SLIPPAGE_POINTS loaded: {MAX_SLIPPAGE_POINTS}")
    if not LIVE_MODE:
        mode_label = "SIMULATION (No real orders, shadow trading)"
    elif LIVE_MODE and TEST_MODE:
        mode_label = "LIVE-TEST (API active, Order tested & cancelled immediately)"
    else:
        mode_label = "LIVE (Real money trading enabled)"

    logging.info(f"🔧 Mode         : {mode_label}")
    
    async def main():
        os.makedirs("data", exist_ok=True)
        asyncio.create_task(heartbeat_loop())
        await initialize_state_from_delta() 
        await connect_ws()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 Bot manually stopped.")
    except Exception as e:
        logging.error(f"❌ Fatal error in main execution: {e}")
