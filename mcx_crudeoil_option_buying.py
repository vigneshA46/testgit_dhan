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

ce_buffer = CE_CLOSE + 8
pe_buffer = PE_CLOSE + 8

# =========================
# GLOBAL STATE
# =========================

ce_state = {
    "active": False,
    "entry": None,
    "tsl_active": False,
    "tsl": None,
    "sl": None,
    "buffer_reset": False,
    "pending_entry": False,
    "last_price": None
}

pe_state = {
    "active": False,
    "entry": None,
    "tsl_active": False,
    "tsl": None,
    "sl": None,
    "buffer_reset": False,
    "pending_entry": False,
    "last_price": None
}

realized_pnl = 0
restricted_mode = False
target_hit = False

# =========================
# TELEMETRY
# =========================

telemetry = {
    "status": "ACTIVE",
    "pnl": 0.0,
    "ce_ltp": 0.0,
    "pe_ltp": 0.0,
    "ce_pnl": 0.0,
    "pe_pnl": 0.0
}

# =========================
# HANDLE LEG
# =========================

def handle_leg(token, candle, state, buffer, base, ltp, name):

    global realized_pnl, restricted_mode, target_hit

    if target_hit:
        return

    o,h,l,c = candle["open"],candle["high"],candle["low"],candle["close"]
    avg = (o+h+l+c)/4

    # =========================
    # TELEMETRY LTP
    # =========================
    if name == "CE":
        telemetry["ce_ltp"] = ltp
    else:
        telemetry["pe_ltp"] = ltp

    # =========================
    # ENTRY LOGIC
    # =========================

    if not state["active"]:

        if restricted_mode:

            if realized_pnl > -25:
                return

            if c < buffer:
                state["buffer_reset"] = True
                return

            if not state["buffer_reset"]:
                return

            if c > buffer and avg > buffer and avg < c:
                state["pending_entry"] = True
                state["buffer_reset"] = False

        else:
            if c > buffer and avg > buffer and avg < c:
                state["pending_entry"] = True

    # =========================
    # ENTRY EXECUTION
    # =========================

    if state["pending_entry"] and not state["active"]:

        state["active"] = True
        state["entry"] = ltp
        state["pending_entry"] = False
        state["tsl_active"] = False
        state["tsl"] = None
        state["last_price"] = ltp

        print(f"🚀 ENTRY {name} @ {ltp}")

        log_trade_event("ENTRY", name, token, SYMBOL, "BUY", 1, ltp, "BREAKOUT", 0, realized_pnl)

        telemetry["status"] = "RUNNING"
        return

    # =========================
    # POSITION MANAGEMENT
    # =========================

    if state["active"]:

        state["last_price"] = ltp

        pnl = ltp - state["entry"]

        if name == "CE":
            telemetry["ce_pnl"] = pnl
        else:
            telemetry["pe_pnl"] = pnl


        if ltp <= base:
            realized_pnl += pnl

            print(f"❌ SL EXIT {name} @ {ltp}")

            log_trade_event(
                "EXIT", 
                name, 
                token, 
                SYMBOL, 
                "SELL", 
                1, ltp, 
                "SL", 
                pnl, 
                realized_pnl
                )

            state["active"] = False
            return

        # 🔥 TSL
        if not state["tsl_active"]:
            if ltp >= state["entry"] + 15:
                state["tsl_active"] = True
                state["tsl"] = state["entry"] + 15
                state["sl"] = state["entry"] + 10

        if state["tsl_active"]:

            if ltp >= state["tsl"] + 10:
                state["tsl"] += 10
                state["sl"] += 10

            if ltp <= state["sl"]:
                realized_pnl += pnl

                print(f"❌ TSL EXIT {name} @ {ltp}")

                log_trade_event("EXIT", name, token, SYMBOL, "SELL", 1, ltp, "TSL", pnl, realized_pnl)

                state["active"] = False
                state["buffer_reset"] = True
                return


# =========================
# UNIVERSAL EXIT
# =========================

def universal_exit_check():

    global realized_pnl, restricted_mode, target_hit

    ce_run = (ce_state["last_price"] - ce_state["entry"]) if ce_state["active"] and ce_state["last_price"] else 0
    pe_run = (pe_state["last_price"] - pe_state["entry"]) if pe_state["active"] and pe_state["last_price"] else 0

    combined = realized_pnl + ce_run + pe_run

    telemetry["pnl"] = combined

    if combined <= -25:
        restricted_mode = True
        telemetry["status"] = "CLOSED"
        print("⚠ RESTRICTED MODE")

        ce_state["active"] = False
        pe_state["active"] = False

    if combined >= 50:
        target_hit = True
        telemetry["status"] = "CLOSED"
        print("🎯 TARGET HIT")

        ce_state["active"] = False
        pe_state["active"] = False


# =========================
# WS HANDLER
# =========================

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

    candle = builder.process_tick(msg)

    if candle:

        if token == CE_TOKEN:
            handle_leg(token, candle, ce_state, ce_buffer, CE_CLOSE, ltp, "CE")

        elif token == PE_TOKEN:
            handle_leg(token, candle, pe_state, pe_buffer, PE_CLOSE, ltp, "PE")

    universal_exit_check()


instruments=[
    (marketfeed.MCX,CE_TOKEN,marketfeed.Quote),
    (marketfeed.MCX,PE_TOKEN,marketfeed.Quote)
]

feed=marketfeed.DhanFeed(client_id,access_token,instruments,"v2")

while True:
    try:
        feed.run_forever()
        data=feed.get_data()
        if data:
            on_tick(data)
    except Exception as e:
        print("WS ERROR:",e)
        feed.run_forever()