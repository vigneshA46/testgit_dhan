import time
import pytz
import requests
from datetime import datetime, time as dtime
from datetime import timedelta
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


ATM = None 
TRADE_LOG_URL = "https://dreaminalgo-backend-production.up.railway.app/api/paperlogger/event"
EVENT_LOG_URL = "https://dreaminalgo-backend-production.up.railway.app/api/paperlogger/paperlogger"

COMMON_ID = "a6bbea5c-ee7e-41c2-b39a-fb4703422d36"
SYMBOL = "NIFTY"

load_dotenv()

STRATEGY_NAME = "DELTA_OPTION_BUYING_100"

CLIENT_ID = os.getenv("CLIENT_ID")

IST = pytz.timezone("Asia/Kolkata")
DHAN_OPTION_CHAIN_URL = "https://api.dhan.co/v2/optionchain"


TRADE_START = dtime(9, 16)
TRADE_END   = dtime(15, 20)

TARGET_POINTS = 100
LOTSIZE = 65

today = datetime.now(IST).strftime("%Y-%m-%d")

telemetry = {
    "strategy_id": COMMON_ID,
    "run_id": COMMON_ID,
    "status": "RUNNING",
    "pnl": 0,
    "pnl_percentage": 0,
    "ce_ltp": 0,
    "pe_ltp": 0,
    "ce_pnl": 0,
    "pe_pnl": 0
}

def get_next_tuesday():
    today = datetime.now(IST)

    days_ahead = 1 - today.weekday()  # Tuesday = 1

    if days_ahead < 0:
        days_ahead += 7

    # If today is Tuesday after market hours → next expiry
    if days_ahead == 0 and today.hour >= 15:
        days_ahead = 7

    next_tuesday = today + timedelta(days=days_ahead)
    return next_tuesday.strftime("%Y-%m-%d")


def get_high_delta_strikes(access_token, client_id):
    payload = {
        "UnderlyingScrip": 13,   # NIFTY
        "UnderlyingSeg": "IDX_I",
        "Expiry": get_next_tuesday()
    }

    headers = {
        "access-token": access_token,
        "client-id": client_id,
        "Content-Type": "application/json"
    }

    response = requests.post(DHAN_OPTION_CHAIN_URL, json=payload, headers=headers)
    data = response.json()

    if data.get("status") != "success":
        raise Exception(f"Option chain fetch failed: {data}")

    oc = data["data"]["oc"]

    best_ce_strike = None
    best_pe_strike = None

    min_ce_diff = float("inf")
    min_pe_diff = float("inf")

    for strike, strike_data in oc.items():
        strike = float(strike)

        # ---------- CE ----------
        ce = strike_data.get("ce")
        if ce and "greeks" in ce:
            delta = ce["greeks"].get("delta")

            if delta is not None and delta >= 0.85:
                diff = abs(delta - 0.85)

                if diff < min_ce_diff:
                    min_ce_diff = diff
                    best_ce_strike = strike

        # ---------- PE ----------
        pe = strike_data.get("pe")
        if pe and "greeks" in pe:
            delta = pe["greeks"].get("delta")

            if delta is not None and delta <= -0.85:
                diff = abs(abs(delta) - 0.85)

                if diff < min_pe_diff:
                    min_pe_diff = diff
                    best_pe_strike = strike

    if best_ce_strike is None or best_pe_strike is None:
        raise Exception("No suitable strikes found with delta ≥ 0.85")

    return best_ce_strike, best_pe_strike


# =========================
# LOGIN
# =========================

access_token = get_access_token()
dhan = dhanhq(CLIENT_ID, access_token)


fno_df=load_fno_master()


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


def logtradeleg(strategyid, leg, symbol, strike_price, date):
    url = "https://dreaminalgo-backend-production.up.railway.app/api/tradelegs/create"
    
    payload = {
        "strategy_id": strategyid,
        "leg": leg,
        "symbol": symbol,
        "strike_price": strike_price,
        "date": date
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

def log_trade_event(
    event_type,
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
        "trade_id": COMMON_ID,

        "event_type": event_type,
        "leg_name": leg_name,
        "token": int(token),
        "symbol": symbol,

        "side": side,
        "lots": lot,
        "quantity": lot * LOTSIZE,

        "price": float(price),  # 🔥 safety

        "reason": reason,
        "deployed_by": COMMON_ID,
        "pnl": str(pnl),
        "cum_pnl": str(cum_pnl),
    }

    # 🔥 NON-BLOCKING
    trade_log_queue.put(payload)

        
def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)


def calculate_atm(price, step=50):
    return int(round(price / step) * step)


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
        "rearm_required": False,
        "tsl": None,
        "sl": None,
        "trailing_active": False
    }

wait_for_start()

ce_strike, pe_strike = get_high_delta_strikes(access_token, CLIENT_ID)


CE_STRIKE = int(ce_strike)
PE_STRIKE = int(pe_strike)

print(CE_STRIKE)
print(PE_STRIKE)

# =========================
# OPTION SELECTION
# =========================
today_date = datetime.now().date()

ce_row = find_option_security(fno_df, CE_STRIKE, "CE", today_date, "NIFTY")
pe_row = find_option_security(fno_df, PE_STRIKE, "PE", today_date, "NIFTY")

CE_ID = str(ce_row["SECURITY_ID"])
PE_ID = str(pe_row["SECURITY_ID"])
print("CE :", CE_ID)
print("PE :", PE_ID)

builders = {
    CE_ID: OneMinuteCandleBuilder(),
    PE_ID: OneMinuteCandleBuilder()
}


# Log CE leg
logtradeleg(
    COMMON_ID,
    "CE",
    f"NIFTY CE {ATM}",
    ATM,
    str(today)
)

# Log PE leg
logtradeleg(
    COMMON_ID,
    "PE",
    f"NIFTY PE {ATM}",
    ATM,
    str(today)
)


def get_first_candle_mark(security_id):

    today = datetime.now(IST).strftime("%Y-%m-%d")
   

    idx= dhan.intraday_minute_data(
        security_id=security_id,
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=today,
        to_date=today
    )
    print("Today :",type(today),today)

    data = idx.get("data", {})
    closes = data.get("close", [])
    timestamps = data.get("timestamp", [])

    for i in range(len(timestamps)):
        ts = datetime.fromtimestamp(timestamps[i], IST)  

        if ts.hour == 9 and ts.minute == 15:
            mark = float(closes[i])
            print(f"📍 HIST MARK {security_id} @ {mark}")
            return mark

    print("❌ 09:15 candle not found")
    return None


# =========================
# STATE
# =========================

ce_state = init_state()
pe_state = init_state()

combined_pnl = 0

ce_state["marked"] = get_first_candle_mark(CE_ID)
pe_state["marked"] = get_first_candle_mark(PE_ID)


# =========================
# STRATEGY ENGINE
# =========================

def handle_leg(name, token, candle, state, ltp):

    global combined_pnl

    now = datetime.now(IST).time()

    close = candle["close"]
    avg = (candle["open"] + candle["high"] +
           candle["low"] + candle["close"]) / 4

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

        if close > state["marked"] and avg > state["marked"] and avg < close:

            entry_price = ltp   

            state["entry_price"] = entry_price
            state["entry_time"] = datetime.now(IST).isoformat()

            state["position"] = True
            state["tsl"] = entry_price + 30
            state["sl"] = None
            state["trailing_active"] = False

            print("🟢 BUY", name, entry_price)

            log_trade_event(
                event_type="ENTRY",
                leg_name=name,
                token=token,
                symbol="NIFTY",
                side="BUY",
                lot=state["lot"],
                price=entry_price,
                reason="Trade opened",
                pnl= state["pnl"],
                cum_pnl= combined_pnl
                )

            log_event(f"{name} BUY", token, "ENTRY_EXECUTED", entry_price, "Trade opened")

    if state["position"] and not state["trailing_active"]:
        if ltp >= state["tsl"]:
            state["trailing_active"] = True
            state["sl"] = state["tsl"] - 10

            print("⚡ TSL ACTIVATED", name, state["tsl"], "SL:", state["sl"])

    if state["position"] and state["trailing_active"]:
        if ltp >= state["tsl"] + 10:
            state["tsl"] += 10
            state["sl"] += 10

            print("📈 TSL MOVED", name, state["tsl"], "SL:", state["sl"])

    if state["position"] and state["trailing_active"]:
        if close < state["sl"]:   # IMPORTANT → candle close
            exit_price = ltp

            pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

            state["pnl"] += pnl
            combined_pnl += pnl

            print("🔴 TSL EXIT", name, exit_price)

            log_trade_event(
                event_type="EXIT",
                leg_name=name,
                token=token,
                symbol=SYMBOL,
                side="SELL",
                lot= state["lot"],
                price=exit_price,
                reason="TSL HIT",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )

            state["position"] = False
            state["lot"] = 1   # reset lot after TSL/target
            state["rearm_required"] = True

            
    # =========================
    # EXIT CONDITION (STRUCTURE BREAK)
    # =========================
    if state["position"] and ltp < state["marked"]:

        exit_price = ltp

        pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

        state["pnl"] += pnl
        combined_pnl += pnl

        print("🔴 EXIT", name, exit_price)

        log_trade_event(
            
            event_type="EXIT",
            leg_name=name,
            token=token,
            symbol=SYMBOL,
            side="SELL",
            lot=state["lot"],
            price=exit_price,
            reason="Below Mark",
            pnl=state["pnl"],
            cum_pnl=combined_pnl
                )

        state["position"] = False

        state["lot"] += 1


def universal_exit_check(ce_ltp, pe_ltp):

    global combined_pnl , pnl

    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (ce_ltp - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]

    if pe_state["position"]:
        pe_running = (pe_ltp - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

    total = ce_state["pnl"] + pe_state["pnl"] + ce_running + pe_running
    combined_pnl = total

    if total >= TARGET_POINTS*65:

        print("🏁 TARGET HIT", total)

        # FORCE EXIT CE
        if ce_state["position"]:
            exit_price = ce_ltp
            pnl = (exit_price - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]

            ce_state["pnl"] += pnl

            log_trade_event(
                event_type="EXIT",
                leg_name="CE",
                token=CE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=ce_state["lot"],
                price=exit_price,
                reason="UNIVERSAL EXIT",
                pnl= ce_state["pnl"],
                cum_pnl=combined_pnl
                )   

            ce_state["position"] = False

        # FORCE EXIT PE
        if pe_state["position"]:
            exit_price = pe_ltp
            pnl = (exit_price - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

            pe_state["pnl"] += pnl

            log_trade_event(
                event_type="EXIT",
                leg_name="PE",
                token=PE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=pe_state["lot"],
                price=exit_price,
                reason="UNIVERSAL EXIT",
                pnl=pe_state["pnl"],
                cum_pnl=combined_pnl
                )

            pe_state["position"] = False

        # STOP EVERYTHING
        ce_state["trading_disabled"] = True
        pe_state["trading_disabled"] = True



# =========================
# CALLBACKS
# =========================


def on_message(msg):

    if msg.get("type") != "Quote Data":
        return

    token = str(msg["security_id"])
    ltp = msg.get("LTP", 0)

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

# ========================= 
# START WEBSOCKET
# =========================

instruments = [
    (marketfeed.NSE_FNO, CE_ID, marketfeed.Quote),
    (marketfeed.NSE_FNO, PE_ID, marketfeed.Quote)
]

TOKENS = [
  CE_ID , PE_ID
]

MY_TOKENS = [CE_ID , PE_ID]
""" 
def on_tick(token, msg):

    if token not in MY_TOKENS:
        return  
    on_message(msg)

    
for t in TOKENS:
    subscribe(t, on_tick)
 """

feed = marketfeed.DhanFeed(CLIENT_ID, access_token, instruments, "v2")
 
while True:
    try:
        feed.run_forever()
        data = feed.get_data()

        if data:
            on_message(data)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()