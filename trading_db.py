import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

class TradingDB:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        self.conn.autocommit = True

    def log_trade(self, symbol, direction, entry_price, sl_price, qty, entry_time, sl_order_id=None, local_trade_id=None, mode=None):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trades (symbol, direction, entry_price, sl_price, qty, entry_time, sl_order_id, local_trade_id, mode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (
                str(symbol),
                str(direction),
                float(entry_price),
                float(sl_price),
                float(qty),
                entry_time,
                str(sl_order_id) if sl_order_id else None,
                str(local_trade_id) if local_trade_id else None,
                mode
            ))
            result = cur.fetchone()
            if result is None:
                raise Exception("❌ INSERT failed — no ID returned from DB.")
            return result["id"] # Return the trade ID

    def log_position_snapshot(self, symbol, size, entry_price, margin, liquidation_price, ts, mode):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO position_snapshot (symbol, size, entry_price, margin, liquidation_price, timestamp, mode)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (symbol, size, entry_price, margin, liquidation_price, ts, mode))


    
    def log_error(self, symbol, error_type, traceback_text, mode):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bot_errors (symbol, error_time, error_type, traceback, mode)
                VALUES (%s, %s, %s, %s, %s);
            """, (
                symbol,
                datetime.now(timezone.utc),
                error_type,
                traceback_text,
                mode
            ))

    def get_open_trade(self, symbol, mode="LIVE"):
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM trades
                WHERE symbol = %s AND exit_time IS NULL AND mode = %s
                ORDER BY entry_time DESC
                LIMIT 1;
            """, (symbol, mode))
            return cur.fetchone()

    def log_wallet_balance(self, asset, balance, ts, mode):
        """
        Log wallet balance for a given asset and timestamp.
        Table: wallet_balances(asset TEXT, balance REAL, ts TIMESTAMP, mode TEXT)
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO wallet_balances (asset, balance, ts, mode)
                VALUES (%s, %s, %s, %s)
                """,
                (asset, balance, ts, mode)
            )
    


    def update_trade_exit(self, trade_id, exit_price, exit_time, pnl, exit_reason, trailing_sl=None, mode=None):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE trades
                SET exit_price = %s,
                    exit_time = %s,
                    pnl = %s,
                    exit_reason = %s,
                    trailing_sl = %s,
                    mode = %s
                WHERE id = %s;
            """, (
                float(exit_price),
                exit_time,
                float(pnl),
                exit_reason,
                float(trailing_sl) if trailing_sl is not None else None,
                mode,
                trade_id
            ))
    
    def update_trailing_sl(self, trade_id, trailing_sl, mode=None):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE trades
                SET trailing_sl = %s,
                mode = %s
                WHERE id = %s;
            """, (trailing_sl, trade_id))
    


    def log_setup(self, symbol, direction, setup_time, entry_price, sl_price, risk_points, reason_skipped=None, triggered=False, mode=None):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO setups (symbol, direction, setup_time, entry_price, sl_price, risk_points, reason_skipped, triggered, mode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (symbol, direction, setup_time, entry_price, sl_price, risk_points, reason_skipped, triggered, mode))

    def log_pullback(self, symbol, trade_id, direction, candle_time, o, h, l, c, v, mode):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pullbacks (symbol, trade_id, direction, candle_time, open, high, low, close, volume, mode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (symbol, trade_id, direction, candle_time, o, h, l, c, v, mode))
    def insert_setup(self, setup_info, mode):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO setups (
                    symbol, direction, setup_time,
                    entry_price, sl_price, risk_points,
                    reason_skipped, triggered, mode
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                str(setup_info["symbol"]),
                str(setup_info["direction"]),
                setup_info["setup_time"],
                float(setup_info["entry_price"]),
                float(setup_info["sl_price"]),
                float(setup_info["risk_points"]),
                str(setup_info.get("reason_skipped")) if setup_info.get("reason_skipped") else None,
                bool(setup_info.get("triggered", False)),
                mode
            ))
    
    def log_event(self, symbol, event_type, message=None, ts=None, mode=None):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bot_events (symbol, event_time, event_type, message, mode)
                VALUES (%s, %s, %s, %s, %s);
            """, (
                symbol,
                ts or datetime.now(timezone.utc),
                event_type,
                message,
                mode
            ))
    
    def log_order_fill(self, symbol, trade_id, order_id, side, fill_price, qty, fill_time, mode):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO fills (symbol, trade_id, order_id, side, fill_price, qty, fill_time, mode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                symbol,
                trade_id,
                order_id,
                side,
                float(fill_price),
                float(qty),
                fill_time,
                mode
            ))

    def get_last_n_trades(self, symbol, mode, n=10):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id, direction, entry_price, sl_price, exit_price, qty, pnl, entry_time, exit_time, exit_reason
                FROM trades
                WHERE symbol = %s AND mode = %s
                ORDER BY entry_time DESC
                LIMIT %s;
            """, (symbol, mode, n))
            return cur.fetchall()
    
    def log_heartbeat(self, symbol, status, mode):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bot_heartbeat (symbol, status, mode)
                    VALUES (%s, %s, %s);
                """, (symbol, status, mode))
        except Exception as e:
            print(f"[DB ERROR] Failed to write heartbeat: {e}")

    def get_open_trade(self, symbol, mode):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM trades
                WHERE symbol = %s AND exit_time IS NULL AND mode = %s
                ORDER BY entry_time DESC
                LIMIT 1;
            """, (symbol, mode))
            return cur.fetchone()
    def get_last_wallet_balance(self, asset):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT balance FROM wallet_balances
                WHERE asset = %s
                ORDER BY ts DESC LIMIT 1;
            """, (asset,))
            result = cur.fetchone()
            return result[0] if result else None
    
    def create_wallet_balances_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_balances (
                    id SERIAL PRIMARY KEY,
                    asset TEXT,
                    balance REAL,
                    ts TIMESTAMP,
                    mode TEXT
                );
            """)
    
    def log_missed_entry_window(self, symbol, setup_time, entry_price, sl_price, hit_in_candle, mode):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO missed_entry_windows (symbol, setup_time, entry_price, sl_price, hit_in_candle, mode)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                symbol,
                setup_time,
                float(entry_price),
                float(sl_price),
                hit_in_candle,
                mode
            ))