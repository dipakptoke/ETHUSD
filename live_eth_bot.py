import asyncio
import websockets
import aiohttp
import json, requests, pandas as pd
from datetime import datetime, timedelta, timezone
import os, logging, math

# === File paths for state and logs ===
STATE_FILE_PATH = "data/eth_state.pkl"
TRADES_LOG_PATH = "data/live_trades_etc.csv"
from dotenv import load_dotenv

# === Global variables for 15m candle aggregation ===
current_15m_candle = None
last_15m_candle_start_time = None
finalized_candle_timestamps = set()
pending_setup = None

# === Load secrets ===
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
LIVE_MODE = os.getenv("LIVE_MODE", "0") == "1"
DELTA_API_KEY = os.getenv("DELTA_API_KEY")
DELTA_API_SECRET = os.getenv("DELTA_API_SECRET")

# === Logging ===
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.FileHandler("logs/eth_bot.log"),
        logging.StreamHandler()
    ])

# === Config ===
SYMBOL = "ETHUSD"
EMA_PERIOD = 5
TRAIL_BUFFER = 50
EMERGENCY_MOVE = 1000
EMERGENCY_LOCK = 750
MIN_CANDLE_POINTS = 10
MAX_SL_POINTS = 350
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

async def send_telegram(msg):
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}) as response:
                    await response.text()
        except Exception as e:
            logging.error(f"Telegram error: {e}")

# === Delta Exchange Order Placement (Add HMAC-SHA256 signature if required) ===
async def place_market_order(side, qty, order_type="limit"):
    if not LIVE_MODE:
        logging.info(f"SIMULATION MODE: Skipping live order placement for {side} {qty}")
        return {"status": "simulated", "side": side, "qty": qty}
    url = "https://api.delta.exchange/orders"
    headers = {
        "api-key": DELTA_API_KEY,
        "Content-Type": "application/json"
        # !!! CRITICAL: Add HMAC-SHA256 signature here if required !!!
    }
    order = {
        "product_id": 3136,  # Ensure correct product ID
        "size": qty,
        "side": side.lower(),
        "order_type": order_type,
        "time_in_force": "post_only" if order_type == "limit" else "immediate_or_cancel"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=order, headers=headers) as response:
                response_json = await response.json()
                logging.info(f"📤 LIVE ORDER SENT | Status: {response.status} | Response: {response_json}")
                return response_json
    except Exception as e:
        logging.error(f"Order placement failed: {e}")
        return None

# === Candle Management (Only finalize_candle is used now, as WS handles updates) ===
def finalize_candle(candle):
    global ohlc_data
    # Ensure candle data is not missing critical values before adding
    if not isinstance(candle, dict) or any(k not in candle for k in ["timestamp", "open", "high", "low", "close", "volume"]):
        logging.error(f"❌ Invalid candle data provided to finalize_candle: {candle}")
        return

    candle_df = pd.DataFrame([candle])
    
    # Check for NaN values more robustly after converting to DataFrame
    if candle_df[["open", "high", "low", "close", "volume"]].isnull().values.any():
        logging.warning(f"⚠️ Skipping finalization due to NaN values in candle: {candle}")
        return

    logging.info(f"🆕 Adding new candle with timestamp: {candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S+00:00')}")
    if not ohlc_data.empty:
        ohlc_data = pd.concat([ohlc_data, candle_df], ignore_index=True).copy()
    else:
        ohlc_data = candle_df.copy()

    # Trim OHLC data to the last 100 rows for memory efficiency
    if len(ohlc_data) > 100:
        ohlc_data = ohlc_data.iloc[-100:].reset_index(drop=True)
    
    # Ensure EMA is only calculated when enough data is present
    if len(ohlc_data) >= EMA_PERIOD:
        # Calculate EMA for the entire series to avoid issues with new data
        ohlc_data.loc[:, "ema"] = ohlc_data["close"].ewm(span=EMA_PERIOD, adjust=False).mean()
        
    logging.info(f"📊 Finalized 15m candle: {ohlc_data.iloc[-1].to_dict()}")
    if not ohlc_data.empty and len(ohlc_data) >= EMA_PERIOD:
        logging.info(f"🔍 EMA value calculated: {ohlc_data.iloc[-1]['ema']:.2f}")
    else:
        logging.info("🔍 EMA not yet calculated (not enough candles).")

    # --- Persist state to disk ---
    import pickle
    try:
        with open(STATE_FILE_PATH, "wb") as f:
            pickle.dump({
                "in_position": in_position,
                "position_state": position_state,
                "ohlc_data": ohlc_data
            }, f)
        logging.info("💾 State saved to eth_state.pkl")
    except Exception as e:
        logging.error(f"❌ Failed to save state: {e}")

# === Strategy ===
async def detect_trade():
    logging.info("📥 Entered detect_trade() for potential setup.")
    global in_position, position_state, last_price, trade_id_counter, open_order_id
    
    # The candle aggregation ensures this is called only after a candle is finalized.
    # So, the explicit time check here is no longer needed and can be removed.
    # logging.info(f"⏰ Now: {now_ts}, Last candle timestamp: {last_ts}, Delta: {(now_ts - last_ts).total_seconds()} sec")
    # if now_ts < last_ts + timedelta(minutes=15):
    #     logging.info("⚠️ Skipping detection — last candle not yet closed.")
    #     return

    logging.info(f"📏 Total candles available: {len(ohlc_data)} (Ready for detection)")
    if len(ohlc_data) < REQUIRED_CANDLES:
        logging.info(f"⏳ Waiting for {REQUIRED_CANDLES} candles before detecting setup. Currently have: {len(ohlc_data)}")
        return

    # Check if a trade is already in progress before proceeding.
    if in_position:
        logging.info("⚠️ Trade already in progress. Skipping new setup.")
        return

    prev, curr = ohlc_data.iloc[-2], ohlc_data.iloc[-1]
    ema = prev["ema"]
    
    # Check if EMA is valid (might be NaN if not enough candles yet)
    if pd.isna(ema):
        logging.warning("❌ EMA missing for previous candle. Skipping trade detection.")
        return

    logging.info(f"🧪 Checking setup at {curr['timestamp'].strftime('%Y-%m-%d %H:%M:%S+00:00')} | prev.close={prev.close}, prev.low={prev.low}, curr.low={curr.low}, EMA={ema:.2f}")

    # Log condition checks
    cond_sell = (prev.close > ema and prev.low > ema and curr.low < prev.low)
    cond_buy = (prev.close < ema and prev.high < ema and curr.high > prev.high)
    logging.info(f"🔍 Sell condition: {cond_sell}, Buy condition: {cond_buy}")

    trade_date = curr["timestamp"].date()
    if position_state.get("date") != trade_date:
        position_state = {"sl_count": 0, "date": trade_date}

    if position_state.get("sl_count", 0) >= MAX_SL_PER_DAY:
        logging.info("❌ Max SLs reached for the day.")
        return

    direction, entry, sl, risk = None, None, None, None
    if cond_sell: # Condition for SELL, then check specific candle shape
        direction = "SELL"
        entry = prev.low
        sl = prev.high
        risk = sl - entry
    elif cond_buy: # Condition for BUY, then check specific candle shape
        direction = "BUY"
        entry = prev.high
        sl = prev.low
        risk = entry - sl

    if not direction:
        logging.info("⚠️ No trade direction detected.")
        return

    logging.info(f"🧮 Calculated risk: {risk} points")
    if risk < MIN_CANDLE_POINTS or risk > MAX_SL_POINTS:
        logging.info(f"⚠️ Skipped setup — Risk: {risk:.1f} points (outside limits)")
        return

    capital_per_trade = (CAPITAL * DAILY_RISK_PERCENT / 100) / MAX_SL_PER_DAY
    expected_loss = capital_per_trade
    qty = floor_qty(expected_loss / risk)
    logging.info(f"🧮 Risk points: {risk:.2f}, Capital per trade: {capital_per_trade:.2f}, Qty: {qty}, Expected Loss: {qty * risk:.2f} USDT")
    if qty < MIN_QTY:
        logging.info("❌ Quantity too small. Skipping trade.")
        return

    trade_id_counter += 1
    order_id = f"sim-{trade_id_counter}" # Use this for simulation tracking

    in_position = True
    position_state.update({
        "entry_time": curr["timestamp"], "entry": entry, "sl": sl, "direction": direction,
        "risk": risk, "qty": qty, "trailing_sl": sl, "emergency": False, # last_price might not be needed here
        "expected_loss": qty * risk,
        "trade_id": trade_id_counter,
        "order_id": order_id, # Store this for tracking
    })

    msg = (
        f"📢 {direction} SETUP DETECTED\n"    
        f"🕒 Time: {curr['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"📉 Entry: {entry}\n"
        f"🛑 SL: {sl}\n"
        f"🎯 Risk: {risk:.1f} pts\n"
        f"📦 Qty: {qty}\n"
        f"💸 Est. Loss: {qty * risk:.2f} USDT"
    )
    global pending_setup
    await send_telegram(msg)
    logging.info(f"🎯 Setup: {msg}")
    pending_setup = {
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "risk": risk,
        "qty": qty,
        "timestamp": curr["timestamp"],
        "triggered": False
        }

    # --- Breakout and order logging and Telegram ---
    now_ts = datetime.now(timezone.utc)
    breakout_log = f"{now_ts.strftime('[%Y-%m-%d %H:%M:%S.%f]')} *** BREAKOUT DETECTED! {direction} Entry at {entry} *** Signal Timestamp: {now_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}"
    order_log = f"{now_ts.strftime('[%Y-%m-%d %H:%M:%S.%f]')} 📤 Sending {'LIMIT' if not in_position else 'STOP-MARKET'} Order: {direction} {qty} @ {entry}"
    logging.info(breakout_log)
    logging.info(order_log)
    await send_telegram(f"{breakout_log}\n{order_log}")

    if LIVE_MODE:
        await place_market_order(direction, qty)
        # Note: In live mode, actual fill confirmation should come via v2/fills WS channel
    else:
        # Simulate a fill confirmation for simulation mode
        simulated_fill_msg = {
            "channel": "v2/fills", # This 'channel' key isn't in Delta's WS response top-level
            "data": {
                "order_id": order_id,
                "price": entry,
                "qty": qty
            }
        }
        # Call on_message with a stringified JSON to mimic WS reception
        await asyncio.to_thread(on_message, None, json.dumps(simulated_fill_msg))

async def execute_pending_trade():
    global in_position, position_state, trade_id_counter, open_order_id, last_price, pending_setup

    if not pending_setup or pending_setup.get("triggered"):
        return

    direction = pending_setup["direction"]
    entry = pending_setup["entry"]
    sl = pending_setup["sl"]
    risk = pending_setup["risk"]
    qty = pending_setup["qty"]
    timestamp = pending_setup["timestamp"]

    # Check breakout based on current 15m candle
    curr = ohlc_data.iloc[-1]
    breakout = False

    if direction == "BUY" and curr["high"] >= entry:
        breakout = True
    elif direction == "SELL" and curr["low"] <= entry:
        breakout = True

    if not breakout:
        return

    # Mark setup as triggered
    pending_setup["triggered"] = True

    trade_id_counter += 1
    order_id = f"sim-{trade_id_counter}"

    in_position = True
    position_state = {
        "entry_time": timestamp,
        "entry": entry,
        "sl": sl,
        "direction": direction,
        "risk": risk,
        "qty": qty,
        "trailing_sl": sl,
        "emergency": False,
        "expected_loss": qty * risk,
        "trade_id": trade_id_counter,
        "order_id": order_id
    }

    now_ts = datetime.now(timezone.utc)
    breakout_log = f"{now_ts.strftime('[%Y-%m-%d %H:%M:%S.%f]')} *** BREAKOUT TRIGGERED! {direction} Entry at {entry} ***"
    order_log = f"{now_ts.strftime('[%Y-%m-%d %H:%M:%S.%f]')} 📤 Sending STOP-MARKET Order: {direction} {qty} @ {entry}"
    logging.info(breakout_log)
    logging.info(order_log)
    await send_telegram(f"{breakout_log}\n{order_log}")

    if LIVE_MODE:
        await place_market_order(direction, qty)
    else:
        simulated_fill_msg = {
            "channel": "v2/fills",
            "data": {
                "order_id": order_id,
                "price": entry,
                "qty": qty
            }
        }
        await asyncio.to_thread(on_message, None, json.dumps(simulated_fill_msg))
        
def check_exit(price, ts): # `price` likely comes from a separate WebSocket price stream (e.g., trades)
    global in_position, position_state, open_order_id
    if not in_position:
        return
    # Block-based trailing stop-loss logic
    direction = position_state.get("direction")
    entry = position_state.get("entry")
    trailing_sl = position_state.get("trailing_sl")
    emergency = position_state.get("emergency", False)
    qty = position_state.get("qty")
    risk = position_state.get("risk")

    prev_last_price = position_state.get("last_price")
    position_state["last_price"] = price

    if "block_candles" not in position_state:
        position_state["block_candles"] = []

    block = position_state["block_candles"]

    if direction == "SELL":
        if prev_last_price is not None and price > prev_last_price:
            block.append(price)
            if len(block) > 3:
                block.pop(0)
        elif block and price < min(block):
            new_sl = max(block) + TRAIL_BUFFER
            if new_sl < trailing_sl:
                position_state["trailing_sl"] = new_sl
                logging.info(f"📉 Trailing SL moved to {new_sl} after green pullback block")
                asyncio.create_task(send_telegram(f"📉 Trailing SL moved to {new_sl} after green pullback block for ETHUSD"))
                block.clear()
    elif direction == "BUY":
        if prev_last_price is not None and price < prev_last_price:
            block.append(price)
            if len(block) > 3:
                block.pop(0)
        elif block and price > max(block):
            new_sl = min(block) - TRAIL_BUFFER
            if new_sl > trailing_sl:
                position_state["trailing_sl"] = new_sl
                logging.info(f"📈 Trailing SL moved to {new_sl} after red pullback block")
                asyncio.create_task(send_telegram(f"📈 Trailing SL moved to {new_sl} after red pullback block for ETHUSD"))
                block.clear()

    position_state["block_candles"] = block

    if not emergency:
        if direction == "SELL" and entry - price >= EMERGENCY_MOVE:
            new_sl = entry - EMERGENCY_LOCK
            if new_sl < trailing_sl:
                position_state["trailing_sl"] = new_sl
                position_state["emergency"] = True
                logging.info(f"🚨 Emergency SL activated. SL moved to {new_sl}")
                asyncio.create_task(send_telegram(f"🚨 Emergency SL activated for ETHUSD. SL moved to {new_sl}"))
        elif direction == "BUY" and price - entry >= EMERGENCY_MOVE:
            new_sl = entry + EMERGENCY_LOCK
            if new_sl > trailing_sl:
                position_state["trailing_sl"] = new_sl
                position_state["emergency"] = True
                logging.info(f"🚨 Emergency SL activated. SL moved to {new_sl}")
                asyncio.create_task(send_telegram(f"🚨 Emergency SL activated for ETHUSD. SL moved to {new_sl}"))

    trailing_sl = position_state.get("trailing_sl")  # Refresh in case it changed above
    # Make sure price is available and valid
    if price is None:
        logging.warning("⚠️ No price available for exit check.")
        return

    if direction == "BUY" and price <= trailing_sl:
        logging.info(f"🛑 STOP LOSS HIT for BUY at price {price}")
        in_position = False
        exit_msg = (
            f"🛑 STOP LOSS HIT for BUY at {price} on {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"📤 Sending STOP-MARKET Order: SELL {qty} @ {trailing_sl}"
        )
        asyncio.create_task(send_telegram(exit_msg))
        logging.info(exit_msg)
        if LIVE_MODE:
            asyncio.create_task(place_market_order("SELL", qty, order_type="stop_market"))
        open_order_id = None
        return
    elif direction == "SELL" and price >= trailing_sl:
        logging.info(f"🛑 STOP LOSS HIT for SELL at price {price}")
        in_position = False
        exit_msg = (
            f"🛑 STOP LOSS HIT for SELL at {price} on {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"📤 Sending STOP-MARKET Order: BUY {qty} @ {trailing_sl}"
        )
        asyncio.create_task(send_telegram(exit_msg))
        logging.info(exit_msg)
        if LIVE_MODE:
            asyncio.create_task(place_market_order("BUY", qty, order_type="stop_market"))
        open_order_id = None
        return

# === WebSocket Events ===

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
    
    # Process 15m candlestick updates
    if data.get("type") == "candlestick_15m" and data.get("symbol") == SYMBOL:
        if 'candle_start_time' in data: # Ensure it's a valid candle update
            asyncio.create_task(process_websocket_candle_update(data)) # Schedule async processing
        else:
            logging.warning(f"Unexpected candlestick_15m data (missing 'candle_start_time'): {data}")

    # Process other channels (e.g., simulated fills, real fills, trades, order book)
    elif data.get("type") == "subscribed":
        logging.info(f"Subscription successful: {data.get('payload', {})}")
    elif data.get("type") == "error":
        logging.error(f"WebSocket Error: {data.get('payload', {})}")
    # Example for other data types if you subscribe to them
    elif data.get("type") == "trades" and data.get("symbol") == SYMBOL:
        for trade in data.get("data", []):
            price = float(trade["price"])
            ts = datetime.fromtimestamp(trade["time"] / 1_000_000, tz=timezone.utc)
            check_exit(price, ts)
    if "price" in locals():  # Check if price is available
        if pending_setup and not pending_setup.get("triggered", False):
            if pending_setup["direction"] == "BUY" and price >= pending_setup["entry"]:
                pending_setup["triggered"] = True
                asyncio.create_task(send_telegram(f"✅ Executing BUY at {price} for ETHUSD"))
                asyncio.create_task(place_market_order("BUY", pending_setup["qty"]))
            elif pending_setup["direction"] == "SELL" and price <= pending_setup["entry"]:
                pending_setup["triggered"] = True
                asyncio.create_task(send_telegram(f"✅ Executing SELL at {price} for ETHUSD"))
                asyncio.create_task(place_market_order("SELL", pending_setup["qty"]))
    elif data.get("channel") == "v2/fills" and "data" in data: # This is for your simulated fill
        fill_info = data.get("data", {})
        fill_order_id = fill_info.get("order_id")
        logging.info(f"📥 Received fill for order ID: {fill_order_id}")
        if fill_order_id == open_order_id:
            logging.info(f"✅ Confirmed entry fill for Trade #{position_state.get('trade_id')} | Order ID: {fill_order_id}")
            # Potentially update position_state with fill details like actual fill price
    # Add other channel handlers as needed (e.g., v2/orders for real order updates)

# === WebSocket Connection and Main Loop ===
async def connect_ws():
    retry_delay = 5
    max_retry = 60
    attempt = 0

    while True:
        try:
            async with websockets.connect("wss://socket.india.delta.exchange") as ws:
                logging.info("🟢 Async WebSocket connected")
                await send_telegram("✅ WebSocket reconnected successfully for ETHUSD.")
                
                # Subscribe to candlestick_15m and trades
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "candlestick_15m", "symbols": [SYMBOL]},
                            {"name": "trades", "symbols": [SYMBOL]}
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
            await send_telegram(f"❌ WebSocket disconnected for ETHUSD!\nError: {e}")
            await asyncio.sleep(min(retry_delay, max_retry))
            attempt += 1
            retry_delay = min(max_retry, retry_delay * 2)  # Exponential backoff


if __name__ == "__main__":
    logging.info("🚀 Starting Hybrid Bot in SIMULATION mode" if not LIVE_MODE else "🚀 Starting Hybrid Bot in LIVE mode")
    # --- Restore state from disk if present ---
    import pickle
    try:
        with open(STATE_FILE_PATH, "rb") as f:
            saved = pickle.load(f)
            in_position = saved.get("in_position", False)
            position_state = saved.get("position_state", {})
            ohlc_data = saved.get("ohlc_data", ohlc_data)
            logging.info("✅ Restored persistent state from disk.")
    except Exception:
        logging.info("ℹ️ No saved state found. Starting fresh.")
    try:
        asyncio.run(connect_ws())
    except KeyboardInterrupt:
        logging.info("🛑 Bot manually stopped.")
    except Exception as e:
        logging.error(f"Error running bot: {e}")