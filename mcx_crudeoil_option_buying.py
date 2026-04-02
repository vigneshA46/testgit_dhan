import time
import pytz
import requests
from datetime import datetime, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import marketfeed
from dhanhq import dhanhq
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from find_security import load_fno_master, find_option_security
import threading
from dispatcher import subscribe
from queue import Queue
import pandas as pd
from io import StringIO

# =========================
# CONFIG
# =========================
trade_log_queue = Queue()
def trade_log_worker():
    while True:
        payload = trade_log_queue.get()
        try:
            requests.post(TRADE_LOG_URL, json=payload, timeout=2)
        except Exception as e:
            print("TRADE EVENT LOG ERROR:", e)
        finally:
            trade_log_queue.task_done()

load_dotenv()

TRADE_LOG_URL = "https://dreaminalgo-backend-production.up.railway.app/api/paperlogger/event"
EVENT_LOG_URL = "https://dreaminalgo-backend-production.up.railway.app/api/paperlogger/paperlogger"

MCX_MASTER_URL = "https://api.dhan.co/v2/instrument/MCX_COMM"

STRATEGY_NAME = "MCX CRUDE OIL OPTION BUYING"

client_id = os.getenv("CLIENT_ID")
access_token = get_access_token()


IST = pytz.timezone("Asia/Kolkata")

COMMON_ID = "7f4993c0-bc6b-4f42-a6ce-afcbb5709bae"
SYMBOL = "CRUDEOIL"

TRADE_START = dtime(15, 30)
TRADE_END   = dtime(22, 30)

LOTSIZE = 100

today = datetime.now(IST).strftime("%Y-%m-%d")

# =========================
# LOGIN
# =========================

dhan = dhanhq(client_id, access_token)
builder = OneMinuteCandleBuilder()



# =========================
# HELPERS
# =========================

def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)


def calculate_atm(price, step=50):
    return int(round(price / step) * step)


def get_future_close(security_id):

    today = datetime.now(IST).strftime("%Y-%m-%d")

    idx = dhan.intraday_minute_data(
        security_id=security_id,
        exchange_segment="MCX_COMM",
        instrument_type="FUTCOM",
        from_date=today,
        to_date=today
    )

   

    data = idx.get("data", {})
    closes = data.get("close", [])
    timestamps = data.get("timestamp", [])

    last_price = None

    for i in range(len(timestamps)):
        ts = datetime.fromtimestamp(timestamps[i], IST)

        if ts.hour == 15 and 15 <= ts.minute < 30:
            last_price = float(closes[i])   

    if last_price:
        print(f"📍 3:15–3:30 CLOSE @ {last_price}")
        return last_price

    print("❌ FUT CLOSE NOT FOUND")
    return None

def get_first_candle_mark_rest(security_id, access_token):

    url = "https://api.dhan.co/v2/charts/intraday"

    headers = {
        "access-token": access_token,
        "Content-Type": "application/json"
    }

    payload = {
        "securityId": int(security_id),
        "exchangeSegment": "MCX_COMM",
        "instrument": "OPTFUT",
        "interval": "1",
        "fromDate": today,
        "toDate": today
    }

    response = requests.post(url, json=payload, headers=headers)

    print("RAW RESPONSE:", response.text)  # 🔥 debug

    if response.status_code != 200:
        print("❌ API FAILED")
        return None

    res = response.json()

    if res.get("status") != "success":
        print("❌ API ERROR:", res)
        return None

    data = res.get("data", {})
    closes = data.get("close", [])
    timestamps = data.get("timestamp", [])

    mark = None

    for i in range(len(timestamps)):
        ts = datetime.fromtimestamp(timestamps[i], IST)

        if ts.hour == 15 and ts.minute == 30:
            mark = float(closes[i])

    if mark is not None:
        print(f"📍 15:30 MARK @ {mark}")
        return mark

    print("❌ 15:30 candle not found")
    return None

def log_event(leg_name, token, action, price, remark=""):
    payload = {
        "run_id": COMMON_ID,
        "strategy_id": COMMON_ID,
        "leg_name": leg_name,
        "token": int(token),
        "symbol": SYMBOL,
        "action": action,
        "price": price,
        "log_type": "TRADE_EVENT",
        "remark": remark
    }

    try:
        requests.post(EVENT_LOG_URL, json=payload, timeout=3)
    except Exception as e:
        print("EVENT LOG ERROR:", e)


def log_trade_event(
    event_type,   # ENTRY / EXIT
    leg_name,
    token,
    symbol,
    side,
    lot,
    price,
    reason,
    pnl,
    cum_pnl
        ):
    payload = {
        "run_id": COMMON_ID,
        "strategy_id": COMMON_ID,

        "trade_id": COMMON_ID,         # 🔥 VERY IMPORTANT
        "event_type": event_type,     # ENTRY / EXIT

        "leg_name": leg_name,
        "token": int(token),
        "symbol": symbol,

        "side": side,
        "lots": lot,
        "quantity": lot * LOTSIZE,

        "price": price,

        "reason": reason,
        "deployed_by": COMMON_ID,

        "pnl": str(pnl),
        "cum_pnl":str(cum_pnl)
    }
   
    trade_log_queue.put(payload)

def logtradeleg(strategyid, leg, symbol, strike_price, date, token):
    url = "https://dreaminalgo-backend-production.up.railway.app/api/tradelegs/create"
    
    payload = {
        "strategy_id": strategyid,
        "leg": leg,
        "symbol": symbol,
        "strike_price": strike_price,
        "date": date,
        "token":str(token)
    }

    try:
        response = requests.post(url, json=payload)

        if response.status_code == 200 or response.status_code == 201:
            print("✅ Trade leg logged successfully")
            return response.json()
        else:
            print(f"❌ Failed to log trade leg: {response.status_code}")
            print(response.text)
            return None

    except Exception as e:
        print(f"⚠️ Error while calling API: {e}")
        return None

telemetry = {
    "strategy_id": COMMON_ID,
    "run_id": COMMON_ID,
    "status": "RUNNING",
    "pnl": 0.0,
    "pnl_percentage": 0.0,
    "ce_ltp": 0.0,
    "pe_ltp": 0.0,
    "ce_pnl": 0.0,
    "pe_pnl": 0.0
}


def telemetry_broadcaster():
    while True:
        try:
            # 🔥 COPY to avoid mutation issues
            payload = telemetry.copy()

            # 🔥 optional: sanitize (prevents TypeError)
            def safe_number(x):
                try:
                    return float(x)
                except:
                    return 0

            payload = {k: safe_number(v) if k in ["pnl","ce_pnl","pe_pnl","ce_ltp","pe_ltp","pnl_percentage"] else v
                for k, v in payload.items()}


            res = requests.post(
                "https://dreaminalgo-backend-production.up.railway.app/api/telemetry",
                json=payload,
                timeout=0.5   # 🔥 keep it LOW
            )

            # optional debug
            if res.status_code != 200:
                print("Telemetry failed:", res.status_code)

        except Exception as e:
            print("Telemetry error:", e)

        time.sleep(1)


t = threading.Thread(target=telemetry_broadcaster, daemon=True)
t.start()



def init_state():
    return {
        "marked": None,
        "position": False,
        "trading_disabled": False,
        "entry_price": None,
        "entry_time": None,
        "lot": 1,
        "pnl": 0.0,
        "symbol": None,
        "rearm_required": False
    }

# =========================
# START
# =========================

wait_for_start()

print("\n🚀 CRUDEOIL OPTION BUYING STARTED\n")

threading.Thread(target=trade_log_worker, daemon=True).start()



# =========================
# STRATEGY ENGINE 
# =========================

class MCXCrudeOptionPaperEngine:

    def __init__(self, ce_token, pe_token, ce_base, pe_base):

        self.ce_token = ce_token
        self.pe_token = pe_token

        self.ce_base = ce_base
        self.pe_base = pe_base

        self.ce_buffer = ce_base + 8
        self.pe_buffer = pe_base + 8

        self.state = {
            ce_token: self._empty_leg(),
            pe_token: self._empty_leg()
        }

        self.realized_pnl = 0
        self.target_hit = False
        self.restricted_mode = False

        print("---- ENGINE INITIALIZED ----")
        print("CE BUFFER:", self.ce_buffer)
        print("PE BUFFER:", self.pe_buffer)

    def _empty_leg(self):
        return {
            "active": False,
            "entry": None,
            "tsl_active": False,
            "tsl": None,
            "buffer_reset": False
        }

    def on_new_candle(self, token, time, candle):

        if self.target_hit:
            return

        o, h, l, c = candle["open"], candle["high"], candle["low"], candle["close"]
        avg = (o + h + l + c) / 4

        leg = self.state[token]
        base = self.ce_base if token == self.ce_token else self.pe_base
        buffer = self.ce_buffer if token == self.ce_token else self.pe_buffer

        if leg["active"]:
            self._manage_position(token, c)
            return

        if self.restricted_mode:
            if c < buffer:
                leg["buffer_reset"] = True

            if leg["buffer_reset"] and self._breakout_condition(c, avg, buffer):
                self._enter(token, c)
                leg["buffer_reset"] = False
        else:
            if self._breakout_condition(c, avg, buffer):
                self._enter(token, c)

    def _breakout_condition(self, close, avg, buffer):
        return close > buffer and avg > buffer and avg < close

    def _enter(self, token, price):
        leg = self.state[token]
        leg["active"] = True
        leg["entry"] = price
        leg["tsl_active"] = False
        leg["tsl"] = None
        print(f"🚀 ENTRY {token} @ {price}")

    def _manage_position(self, token, close_price):

        leg = self.state[token]
        entry = leg["entry"]
        pnl = close_price - entry

        base = self.ce_base if token == self.ce_token else self.pe_base
        if close_price < base:
            print(f"❌ MARKED EXIT {token}")
            self._exit(token, close_price)
            return

        if not leg["tsl_active"] and pnl >= 15:
            leg["tsl_active"] = True
            leg["tsl"] = close_price - 10
            print(f"🔁 TSL ACTIVATED {token} @ {leg['tsl']}")

        if leg["tsl_active"]:
            new_tsl = close_price - 10
            if new_tsl > leg["tsl"]:
                leg["tsl"] = new_tsl

            if close_price <= leg["tsl"]:
                print(f"❌ TSL EXIT {token}")
                self._exit(token, close_price)
                return

        self._check_global_target(close_price, token)

    def _exit(self, token, price):

        leg = self.state[token]
        pnl = price - leg["entry"]
        self.realized_pnl += pnl

        print(f"🏁 EXIT {token} @ {price} | PnL: {pnl}")
        print(f"📊 CUMULATIVE PnL: {self.realized_pnl}")

        self.state[token] = self._empty_leg()

        if self.realized_pnl <= -25:
            self.restricted_mode = True
            print("⚠ RESTRICTED MODE ENABLED")

    def _check_global_target(self, current_price, token):

        unrealized = 0

        for t, leg in self.state.items():
            if leg["active"]:
                unrealized += current_price - leg["entry"]

        combined = self.realized_pnl + unrealized

        if combined >= 50:
            print("🎯 TARGET HIT 50 POINTS")
            self._exit_all(current_price)

    def _exit_all(self, price):

        for token, leg in self.state.items():
            if leg["active"]:
                self._exit(token, price)

        self.target_hit = True
        print("🛑 TRADING DISABLED FOR DAY")


# =========================
# MAIN
# =========================

def find_current_month_future(df, today):

    r = requests.get(MCX_MASTER_URL, headers={"access-token": access_token})
    r.raise_for_status()

    # ✅ Use header from API (IMPORTANT)
    df = pd.read_csv(StringIO(r.text), low_memory=False)
    
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")
    
    trade_date = pd.to_datetime(today)

    fut = df[
        (df["INSTRUMENT"] == "FUTCOM") &
        (df["UNDERLYING_SYMBOL"] == SYMBOL) &
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ]

    if fut.empty:
        raise ValueError("❌ No CRUDEOIL future found")

    return fut.sort_values("SM_EXPIRY_DATE").iloc[0]

def find_option_security(df,strike, option_type, today, target_symbol):

    trade_date = pd.to_datetime(today)
    df=df.copy()

    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")
    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    print("COLUMNS:", fno_df.columns.tolist())

    opt = df[
        (df["INSTRUMENT"] == "OPTFUT") & 
        (df["UNDERLYING_SYMBOL"] == target_symbol) &
        (df["STRIKE_PRICE"] == strike) &  
        (df["OPTION_TYPE"] == option_type) &   
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ]

    if opt.empty:
        raise ValueError(f"❌ No {option_type} found for strike {strike}")

    return opt.sort_values("SM_EXPIRY_DATE").iloc[0]

#fno_df = load_fno_master()

fno_df = load_fno_master()

#today_date = datetime.now().date()

currentfut = find_current_month_future(fno_df, today)

currfuttoken = str(currentfut["SECURITY_ID"])

print("future token", currfuttoken)

FutClose = get_future_close(currfuttoken)

ATM = calculate_atm(FutClose)
print("ATM strike price", ATM)


ce_row = find_option_security(fno_df, ATM, "CE", today, "CRUDEOIL")
pe_row = find_option_security(fno_df, ATM, "PE", today, "CRUDEOIL")

CE_TOKEN = str(ce_row["SECURITY_ID"])
PE_TOKEN = str(pe_row["SECURITY_ID"])


print("CE TOKEN", CE_TOKEN)
print("PE TOKEN", PE_TOKEN)

CE_CLOSE = get_first_candle_mark(CE_TOKEN)
PE_CLOSE = get_first_candle_mark(PE_TOKEN)

print("CE 15:30 candle close", CE_CLOSE)
print("PE 15:30 candle close", PE_CLOSE)


engine = MCXCrudeOptionPaperEngine(
    CE_TOKEN,
    PE_TOKEN,
    CE_CLOSE,
    PE_CLOSE
)


def on_candle(token, time, candle):
    engine.on_new_candle(token, time, candle)


def on_tick(msg):
    builder.process_tick(msg, on_candle)


# ======================
# START WS
# ======================


instruments = [
    (marketfeed.MCX_COMM, CE_TOKEN),
    (marketfeed.MCX_COMM, PE_TOKEN)
]


feed = marketfeed.DhanFeed(
    client_id=client_id,
    access_token=access_token,
    instruments=instruments
)


while True:
    data = feed.get_data()

    if data:
        on_tick(data)