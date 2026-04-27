import time
import pytz
import requests
from datetime import datetime, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
from dhanhq import dhanhq,DhanContext
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


access_token = get_access_token()
client_id = os.getenv("CLIENT_ID")


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

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)

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

        "pnl": str(pnl),
        "cum_pnl":str(cum_pnl)
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
        "buffer": None,
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



# =========================
# GLOBAL STATE
# =========================



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

def on_message(msg):

    global combined_pnl 

    if msg.get("type") != "Quote Data":
        return

    token = str(msg["security_id"])
    ltp = float(msg.get("LTP", 0)or 0)

    builder = builders.get(token)

    if not builder:
        return

    candle = builder.process_tick(msg)

    token = str(msg["security_id"])

    # store LTP
    if token == CE_ID:
        telemetry["ce_ltp"] = float(ltp or 0)

    if token == PE_ID:
        telemetry["pe_ltp"] = float(ltp or 0)

    # =========================
    # Entry +8 Breakout
    # =========================

    if token == CE_ID:
        state = ce_state
        leg_name = "CE"
    elif token == PE_ID:
        state = pe_state
        leg_name = "PE"
    else:
        state = None

    if state and state["marked"] is None:
        return

    # =========================
    # -8 EXIT (TICK LEVEL)
    # =========================
    if state and state["position"]:

        # =========================
        # TSL ACTIVATION (TICK)
        # =========================
        # TSL ACTIVATE
        if not state.get("tsl_active") and ltp >= state["entry_price"] + 15:
            state["tsl_active"] = True
            state["tsl"] = state["entry_price"] + 15
            state["sl"] = state["entry_price"] + 10

        # TSL TRAIL
        if state.get("tsl_active"):

            if ltp >= state["tsl"] + 10:
                state["tsl"] += 10
                state["sl"] += 10

            if ltp <= state["sl"]:

                pnl = (ltp - state["entry_price"]) * LOTSIZE * state["lot"]
                state["pnl"] += pnl
                combined_pnl += pnl

                print("🔴 TSL EXIT MCX", leg_name, ltp)

                log_trade_event(
                    event_type="EXIT",
                    leg_name=leg_name,
                    token=token,
                    symbol=SYMBOL,
                    side="SELL",
                    lot=state["lot"],
                    price=ltp,
                    reason="MCX TSL EXIT",
                    pnl=state["pnl"],
                    cum_pnl=combined_pnl
                )

                state["position"] = False
                state["rearm_required"] = True
                return

    # =========================
    # RUN UNIVERSAL EXIT (TICK LEVEL)
    # =========================
    if "ce_ltp" in telemetry and "pe_ltp" in telemetry:
        universal_exit_check(telemetry["ce_ltp"], telemetry["pe_ltp"])

    # =========================
    # CANDLE LOGIC
    # =========================
    if candle:

        if token == CE_ID:
            print("CE",token)
            print(candle)
            handle_leg("CE", token, candle, ce_state, ltp)

        if token == PE_ID:
            print("PE",token)
            print(candle)
            handle_leg("PE", token, candle, pe_state, ltp)

    # =========================
    # TELEMETRY (REAL-TIME PnL)
    # =========================
    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (telemetry["ce_ltp"] - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]

    if pe_state["position"]:
        pe_running = (telemetry["pe_ltp"] - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

    telemetry["ce_pnl"] = ce_state["pnl"] + ce_running
    telemetry["pe_pnl"] = pe_state["pnl"] + pe_running
    telemetry["pnl"] = telemetry["ce_pnl"] + telemetry["pe_pnl"]


def handle_leg(name, token, candle, state, ltp):

    global combined_pnl

    now = datetime.now(IST).time()

    close = candle["close"]
    avg = (candle["open"] + candle["high"] +
           candle["low"] + candle["close"]) / 4
    buffer = state["marked"] + 8

    timestamp = candle["timestamp"]

    # =========================
    # TIME EXIT (15:20)
    # =========================
    if now >= TRADE_END:

        if state["position"]:
            exit_price = ltp 

            pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

            state["pnl"] += pnl
            combined_pnl += pnl
            

            #run_async(emit_signal(build_payload(name, "SELL", token, "exit", "EXIT", ltp, state["pnl"], combined_pnl)))
            log_trade_event(
                event_type="EXIT",
                leg_name=name,
                token=token,
                symbol=SYMBOL,
                side="SELL",
                lot=state["lot"],
                price=exit_price,
                reason="TIME EXIT",
                pnl= state["pnl"],
                cum_pnl=combined_pnl
                )

            state["position"] = False


        state["trading_disabled"] = True
        return

    # =========================
    # STOP TRADING
    # =========================
    if state["trading_disabled"]:
        return

    if state["rearm_required"]:
        if close < state["marked"]:
            state["rearm_required"] = False
        else:
            return



    # =============================
    # ENTRY SIGNAL AND EXECUTION
    # =============================
    if not state["position"]:

        if close > state["buffer"] and avg > state["buffer"] and avg < close:
        

            entry_price = ltp   

            state["entry_price"] = entry_price
            state["entry_time"] = datetime.now(IST).isoformat()

            state["position"] = True
            state["tsl_active"] = False
            state["tsl"] = None
            state["sl"] = None

            print("🟢 BUY", name, entry_price)
            #run_async(emit_signal(build_payload(name, "BUY", token, "entry", "ENTRY", ltp, state["pnl"], combined_pnl)))

            log_trade_event(
                event_type="ENTRY",
                leg_name=name,
                token=token,
                symbol=SYMBOL,
                side="BUY",
                lot=state["lot"],
                price=entry_price,
                reason="Trade opened",
                pnl= state["pnl"],
                cum_pnl= combined_pnl
                )

            log_event(f"{name} BUY", token, "ENTRY_EXECUTED", entry_price, "Trade opened")



def universal_exit_check(ce_ltp, pe_ltp):

    global combined_pnl, target_hit, restricted_mode

    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (ce_ltp - ce_state["entry_price"]) * LOTSIZE *ce_state["lot"]

    if pe_state["position"]:
        pe_running = (pe_ltp - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

    total = ce_state["pnl"] + pe_state["pnl"] + ce_running + pe_running

    if target_hit:
        return


    # TARGET EXIT
    if total >= 50:

        target_hit = True

        print("🎯 MCX TARGET HIT — EXIT ALL")

        if ce_state["position"]:
            pnl = (ce_ltp - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]
            ce_state["pnl"] += pnl
            combined_pnl += pnl
            ce_state["position"] = False

            log_trade_event(
                event_type="EXIT",
                leg_name="CE",
                token=CE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=ce_state["lot"],
                price=ce_ltp,
                reason="MCX TARGET EXIT",
                pnl=ce_state["pnl"],
                cum_pnl=combined_pnl
            )

        if pe_state["position"]:
            pnl = (pe_ltp - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]
            pe_state["pnl"] += pnl
            combined_pnl += pnl
            pe_state["position"] = False

            log_trade_event(
                event_type="EXIT",
                leg_name="PE",
                token=PE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=pe_state["lot"],
                price=pe_ltp,
                reason="MCX TARGET EXIT",
                pnl=pe_state["pnl"],
                cum_pnl=combined_pnl
            )
    if total <= -25:

        target_hit = True

        print("OVERALL STOPLOSS HIT — EXIT ALL")

        if ce_state["position"]:
            pnl = (ce_ltp - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]
            ce_state["pnl"] += pnl
            combined_pnl += pnl
            ce_state["position"] = False

            log_trade_event(
                event_type="EXIT",
                leg_name="CE",
                token=CE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=ce_state["lot"],
                price=ce_ltp,
                reason="MCX TARGET EXIT",
                pnl=ce_state["pnl"],
                cum_pnl=combined_pnl
            )

        if pe_state["position"]:
            pnl = (pe_ltp - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]
            pe_state["pnl"] += pnl
            combined_pnl += pnl
            pe_state["position"] = False

            log_trade_event(
                event_type="EXIT",
                leg_name="PE",
                token=PE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=pe_state["lot"],
                price=pe_ltp,
                reason="MCX TARGET EXIT",
                pnl=pe_state["pnl"],
                cum_pnl=combined_pnl
            )






today = datetime.now(IST).strftime("%Y-%m-%d")


ATM = calculate_atm(marked_price)
print("ATM strike price :", ATM)

ce_row = find_option_security(fno_df, ATM, "CE", today, "CRUDEOIL")
pe_row = find_option_security(fno_df, ATM, "PE", today, "CRUDEOIL")

CE_ID = str(ce_row["SECURITY_ID"])
PE_ID = str(pe_row["SECURITY_ID"])   # <-- FIXED


print("security ids")
print(CE_ID, PE_ID)



""" builders = {
    CE_ID: OneMinuteCandleBuilder(),
    PE_ID: OneMinuteCandleBuilder()
        } """

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

    
# =========================
# STATE
# =========================

ce_state = init_state()
pe_state = init_state()

combined_pnl = 0

ce_state["marked"] = get_first_candle_mark_rest(CE_TOKEN,access_token)
pe_state["marked"] = get_first_candle_mark_rest(PE_TOKEN,access_token)

print("CE 15:30 candle close", ce_state["marked"])
print("PE 15:30 candle close", pe_state["marked"])

ce_state["buffer"] = ce_state["marked"] + 8
pe_state["buffer"] = pe_state["marked"] + 8


instruments = [
    (MarketFeed.MCX, str(CE_ID), MarketFeed.Quote),
    (MarketFeed.MCX, str(PE_ID), MarketFeed.Quote)
    ]


feed = MarketFeed(dhan_context, instruments, "v2")
 
while True:
    try:
        feed.run_forever()
        data = feed.get_data()

        if data:
                
            on_message(data)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
