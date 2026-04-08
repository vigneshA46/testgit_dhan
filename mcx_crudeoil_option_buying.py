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

TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"

MCX_MASTER_URL = "https://api.dhan.co/v2/instrument/MCX_COMM"
INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"






STRATEGY_NAME = "MCX CRUDE OIL OPTION BUYING"

client_id = os.getenv("CLIENT_ID")
access_token = get_access_token()

HEADERS = {
    "Content-Type": "application/json",
    "access-token": access_token
}

UNDERLYING_SYMBOL = "CRUDEOIL"
STRIKE_STEP = 50
INTERVAL_1M = "1"
INTERVAL_15M = "15"

IST = pytz.timezone("Asia/Kolkata")

COMMON_ID = "617126ad-4197-4272-a08f-cc2ad43b3858"
SYMBOL = "CRUDEOIL"

TRADE_START = dtime(15, 31)
TRADE_END   = dtime(22, 30)

LOTSIZE = 100

today = datetime.now(IST).strftime("%Y-%m-%d")
#today = "2026-04-01"
# =========================
# LOGIN
# =========================

dhan = dhanhq(client_id, access_token)




# =========================
# HELPERS
# =========================



def load_fno_master() -> pd.DataFrame:
    print("...downloading FNO master")

    r = requests.get(MCX_MASTER_URL, headers={"access-token": access_token})
    r.raise_for_status()

    # ✅ Use header from API (IMPORTANT)
    df = pd.read_csv(StringIO(r.text), low_memory=False)

    # ✅ Drop unwanted column
    if "Unnamed: 31" in df.columns:
        df = df.drop(columns=["Unnamed: 31"])

    # ✅ Type conversions
    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")

    return df





def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)


def calculate_atm(price, step=50):
    return int(round(price / step) * step)

# =====================================================
# STEP 3: FETCH INTRADAY DATA
# =====================================================


def fetch_intraday(security_id, instrument, interval, trade_date):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "MCX_COMM",
        "instrument": instrument,
        "interval": "15",
        "fromDate": f"{trade_date} {TRADE_START}",
        "toDate": f"{trade_date} {TRADE_END}"
    }

    r = requests.post(INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"]
    })

    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)

    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(df)

    return df

# =====================================================
# STEP 4: GET 3:15–3:30 FUT CANDLE
# =====================================================

def get_315_candle(df):
    candle = df[
        df["datetime"].dt.strftime("%H:%M:%S") == "15:30:00"
    ]

    if candle.empty:
        raise ValueError("❌ 3:15–3:30 candle not found")

    return candle.iloc[0]



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

    #print("RAW RESPONSE:", response.text) 

    if response.status_code != 200:
        print("❌ API FAILED")
        return None

    res = response.json()
    if "data" in res:
            data = res["data"]
    else:
            data = res

    #data = res.get("data", {})
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

        "pnl": str(pnl * 100),
        "cum_pnl":str(cum_pnl *100)
    }
   
    trade_log_queue.put(payload)

def logtradeleg(strategyid, leg, symbol, strike_price, date, token):
    url = "https://algoapi.dreamintraders.in/api/tradelegs/create"
    
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
    "status": "ACTIVE",
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
                "https://algoapi.dreamintraders.in/api/telemetry",
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

    def __init__(self, ce_token, pe_token, ce_base, pe_base,fut_base):

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
        self.fut_base = fut_base

        print("---- ENGINE INITIALIZED ----")
        print("CE BUFFER:", self.ce_buffer)
        print("PE BUFFER:", self.pe_buffer)

    def _empty_leg(self):
        return {
            "active": False,
            "entry": None,
            "tsl_active": False,
            "tsl": None,
            "buffer_reset": False,
            "last_price": None,
            "pending_entry": False
        }

    def on_new_candle(self, token, time, candle):

        print(candle , time , token)

        if self.target_hit:
            return

        o, h, l, c = candle["open"], candle["high"], candle["low"], candle["close"]
        avg = (o + h + l + c) / 4

        leg = self.state[token]
        base = self.ce_base if token == self.ce_token else self.pe_base
        buffer = self.ce_buffer if token == self.ce_token else self.pe_buffer

       
        if self.restricted_mode:

            if self.realized_pnl > -25:
                return

            if c < buffer:
                leg["buffer_reset"] = True
                return

            if not leg["buffer_reset"]:
                return

            if self._breakout_condition(c, avg, buffer):
                leg["pending_entry"] = True
                leg["buffer_reset"] = False
        else:
            if self._breakout_condition(c, avg, buffer):
                    leg["pending_entry"] = True
                    

    def _breakout_condition(self, close, avg, buffer):
        return close > buffer and avg > buffer and avg < close

    def _enter(self, token, price):
        leg = self.state[token]
        leg["active"] = True
        leg["entry"] = price
        leg["tsl_active"] = False
        leg["tsl"] = None
        leg["last_price"] = price
        leg["pending_entry"] = False
        name = "CE" if token == self.ce_token else "PE"
        print(f"🚀 ENTRY {token} @ {price}")

        telemetry["status"] = "RUNNING"

        log_trade_event(
            event_type="ENTRY",
            leg_name=name,
            token=token,
            symbol=SYMBOL,
            side="BUY",
            lot=1,
            price=price,
            reason="BREAKOUT",
            pnl=0,
            cum_pnl=self.realized_pnl
            )

    def on_tick(self, token, price):

        if token == self.ce_token:
            telemetry["ce_ltp"] = price
        else:
            telemetry["pe_ltp"] = price


        if self.target_hit:
            return

        leg = self.state[token]

        if not leg["active"]:

            if leg["pending_entry"] and not leg["active"]:
                print(f"⚡ ENTRY {token} @ {price}")
                self._enter(token, price)
                leg["pending_entry"] = False
                return
            return

        leg["last_price"] = price

        entry = leg["entry"]
        pnl = price - entry

        ce_pnl = 0
        pe_pnl = 0

        for t, leg in self.state.items():
            if leg["active"]:
                current_price = leg["last_price"] or leg["entry"]
                pnl = current_price - leg["entry"]

                if t == self.ce_token:
                    ce_pnl = pnl
                else:
                    pe_pnl = pnl

        telemetry["ce_pnl"] = ce_pnl
        telemetry["pe_pnl"] = pe_pnl

        combined = self.realized_pnl + ce_pnl + pe_pnl
        telemetry["pnl"] = combined

        base = self.ce_base if token == self.ce_token else self.pe_base


        # 🔥 HARD SL (tick-based)
        if price <= base:
            print(f"❌ SL EXIT {token} @ {price}")
            self._exit(token, price)
            return

        if not leg["tsl_active"] and pnl >= 15:
            leg["tsl_active"] = True
            leg["tsl"] = price - 10
            print(f"🔁 TSL ACTIVATED {token} @ {leg['tsl']}")

        if leg["tsl_active"]:
            new_tsl = price - 10

            if new_tsl > leg["tsl"]:
                leg["tsl"] = new_tsl

            if price <= leg["tsl"]:
                print(f"❌ TSL EXIT {token}")
                self._exit(token, price)
                return

        self._check_global_target()

    def _exit(self, token, price):

        leg = self.state[token]
        pnl = price - leg["entry"]
        self.realized_pnl += pnl

        name = "CE" if token == self.ce_token else "PE"

        print(f"🏁 EXIT {token} @ {price} | PnL: {pnl}")
        print(f"📊 CUMULATIVE PnL: {self.realized_pnl}")
        telemetry["pnl"] = self.realized_pnl

        log_trade_event(
            event_type="EXIT",
            leg_name=name,
            token=token,
            symbol=SYMBOL,
            side="SELL",
            lot=1,
            price=price,
            reason="EXIT",   # we improve this below 👇
            pnl=pnl,
            cum_pnl=self.realized_pnl
        )

        self.state[token] = self._empty_leg()

    def _check_global_target(self):

        unrealized = 0

        for t, leg in self.state.items():
            if leg["active"]:
                current_price = leg["last_price"] if leg["last_price"] else leg["entry"]
                unrealized += current_price - leg["entry"]

        combined = self.realized_pnl + unrealized

        if combined <= -25:
            self.restricted_mode = True
            print("⚠ RESTRICTED MODE ENABLED (COMBINED LOSS)")
            telemetry["status"] = "CLOSED"
            self._exit_all()
            

        if combined >= 50:
            print("🎯 TARGET HIT 50 POINTS")
            telemetry["status"] = "CLOSED"
            self._exit_all()

    def _exit_all(self):

        for token, leg in self.state.items():
            if leg["active"]:
                price = leg["last_price"] if leg["last_price"] else leg["entry"]
                self._exit(token, price)

        self.target_hit = True
        print("🛑 TRADING DISABLED FOR DAY")
        telemetry["status"] = "CLOSED"


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



def find_option_security(df , strike , option_type, today, target_symbol):

    trade_date = pd.to_datetime(today)
    df=df.copy()

    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")
    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    #print("COLUMNS:", fno_df.columns.tolist())

    opt = df[
        (df["INSTRUMENT"] == "OPTFUT") & 
        (df["UNDERLYING_SYMBOL"] == SYMBOL) &
        (df["STRIKE_PRICE"] == strike) &  
        (df["OPTION_TYPE"] == option_type) &   
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ]

    #print("OPT",opt)

    if opt.empty:
        raise ValueError(f"❌ No {option_type} found for strike {strike}")
        

    return opt.sort_values("SM_EXPIRY_DATE").iloc[0]

#fno_df = load_fno_master()

fno_df = load_fno_master()
#print(fno_df.iloc[0])

#today_date = datetime.now().date()

currentfut = find_current_month_future(fno_df, today)

currfuttoken = str(currentfut["SECURITY_ID"])

print("future token", currfuttoken)

fut_df = fetch_intraday(
        currfuttoken,
        instrument="FUTCOM",
        interval=INTERVAL_15M,
        trade_date=today
    )

candle_315 = get_315_candle(fut_df)
marked_price = candle_315["close"]

ATM = calculate_atm(marked_price)
print("ATM strike price", ATM)


ce_row = find_option_security(fno_df, ATM, "CE", today, "CRUDEOIL")
pe_row = find_option_security(fno_df, ATM, "PE", today, "CRUDEOIL")

CE_TOKEN = str(ce_row["SECURITY_ID"])
PE_TOKEN = str(pe_row["SECURITY_ID"])

builders = {
    str(CE_TOKEN): OneMinuteCandleBuilder(),
    str(PE_TOKEN): OneMinuteCandleBuilder()
}


# Log CE leg
logtradeleg(
    COMMON_ID,
    "CE",
    f"CRUDEOIL CE {ATM}",
    ATM,
    str(today),
    CE_TOKEN
)


logtradeleg(
    COMMON_ID,
    "PE",
    f"CRUDEOIL PE {ATM}",
    ATM,
    str(today),
    PE_TOKEN
)

print("trade leg logged")


print("CE TOKEN", CE_TOKEN)
print("PE TOKEN", PE_TOKEN)

CE_CLOSE = get_first_candle_mark_rest(CE_TOKEN,access_token)
PE_CLOSE = get_first_candle_mark_rest(PE_TOKEN,access_token)

print("CE 15:30 candle close", CE_CLOSE)
print("PE 15:30 candle close", PE_CLOSE)


engine = MCXCrudeOptionPaperEngine(
    CE_TOKEN,
    PE_TOKEN,
    CE_CLOSE,
    PE_CLOSE,
    fut_base=candle_315["close"]
)


def on_candle(token, candle):
    engine.on_new_candle(token, candle["timestamp"], candle)


def on_tick(msg):

    if msg["type"] != 'Quote Data':
        return

    token = str(msg["security_id"])
    ltp = msg.get("LTP")

    if not ltp:
        return

    ltp = float(ltp)  

    builder = builders.get(token)

    if not builder:
        return
    
    engine.on_tick(token, ltp)
    candle = builder.process_tick(msg)

    if candle:
        on_candle(token, candle)


# ======================
# START WS
# ======================


instruments = [
    (marketfeed.MCX, CE_TOKEN , marketfeed.Quote),
    (marketfeed.MCX, PE_TOKEN, marketfeed.Quote)
]


feed = marketfeed.DhanFeed(client_id,access_token,instruments,"v2")


while True:
    try:
        feed.run_forever()
        data = feed.get_data()

        if data:
            on_tick(data)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()