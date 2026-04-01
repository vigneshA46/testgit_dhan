import time
import pytz
import requests
import pandas as pd
from datetime import datetime, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import marketfeed
from dhanhq import dhanhq
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from find_security import load_fno_master, find_option_security

from queue import Queue
import threading
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

COMMON_ID = "bbfe888c-60f9-4968-acf1-2320ce69ce8d"
SYMBOL = "NIFTY"
symbol=SYMBOL

load_dotenv()

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = get_access_token()

IST = pytz.timezone("Asia/Kolkata")


INDEX_TOKEN = "13"

TRADE_START = dtime(10, 1)
TRADE_END   = dtime(15, 20)

LOT_QTY = 1
DAY_TARGET = -38
LOT = 1
LOTSIZE= 65

#today = datetime.now(IST).strftime("%Y-%m-%d")
today = "2024-09-11"

# =========================
# LOGIN
# =========================

dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

builder = OneMinuteCandleBuilder()
fno_df = load_fno_master()



idx_builder = OneMinuteCandleBuilder()
#opt_builder = OneMinuteCandleBuilder()

# =========================
# STATE
# =========================

top_line = None
bottom_line = None

CE_ID = None
PE_ID = None
ce_strike = None
pe_strike = None

ce_pos = None
pe_pos = None

pending_ce = False
pending_pe = False

total_pnl = 0
stop_trading = False

allow_ce = True
allow_pe = True


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



def logtradeleg(strategyid, leg, symbol, strike_price, date, token):
    url = "https://dreaminalgo-backend-production.up.railway.app/api/tradelegs/create"
    
    payload = {
        "strategy_id": strategyid,
        "leg": leg,
        "symbol": symbol,
        "strike_price": strike_price,
        "date": date,
        "token": token
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

def mark_range():
    global top_line, bottom_line, CE_ID, PE_ID, ce_strike, pe_strike,today

    #today = datetime.now(IST).strftime("%Y-%m-%d")
    idx = dhan.intraday_minute_data(
        security_id=13,   
        exchange_segment="IDX_I",
        instrument_type="INDEX",
        from_date=f"{today} 9:50:00",
        to_date=f"{today} 10:15:00",
        interval=5
    )

    data = idx.get("data", {})
    opens = data.get("open", [])
    highs = data.get("high", [])
    lows = data.get("low", [])
    closes = data.get("close", [])
    timestamps = data.get("timestamp", [])

    candle = None

    for i in range(len(timestamps)):
        ts = datetime.fromtimestamp(timestamps[i], IST)
        print(ts)

        if ts.hour == 9 and ts.minute == 55:
            candle = {
                "open": opens[i],
                "high": highs[i],
                "low": lows[i],
                "close": closes[i]
            }
            break

    if candle:
        c = candle

        top_line = max(c["open"], c["close"])
        bottom_line = min(c["open"], c["close"])

        ATM = calculate_atm(c["close"])

    else:
        print("Waiting for 09:55 candle...")

    print(candle)

    ce_strike = ATM - 400
    pe_strike = ATM + 400

    ce_row = find_option_security(fno_df, ATM, "CE", today, "NIFTY")
    pe_row = find_option_security(fno_df, ATM, "PE", today, "NIFTY")

    CE_ID = str(ce_row["SECURITY_ID"])
    PE_ID = str(pe_row["SECURITY_ID"])

    
    # Log CE leg
    logtradeleg(
        COMMON_ID,
        "CE",
        f"NIFTY CE {ATM}",
        ATM,
        str(today),
        CE_ID
    )

    # Log PE leg
    logtradeleg(
        COMMON_ID,
        "PE",
        f"NIFTY PE {ATM}",
        ATM,
        str(today),
        PE_ID
    )   

    print("legs logged successfully")



    print("\n📏 RANGE MARKED")
    print("TOP    :", top_line)
    print("BOTTOM :", bottom_line)
    print("ATM    :", ATM)
    print("CE     :", ce_strike, CE_ID)
    print("PE     :", pe_strike, PE_ID)




# =========================
# ENGINE
# =========================

def on_index_candle(token, t, row):
    global pending_ce, pending_pe, stop_trading

    if stop_trading:
        return

    if t.time() < TRADE_START or t.time() > TRADE_END:
        return

    avg_price = (row["open"] + row["high"] + row["low"] + row["close"]) / 4
    # ---- REARM LOGIC ----
    if not allow_pe and row["close"] < top_line:
        allow_pe = True
        print("🔓 PE REARMED @", t)

    if not allow_ce and row["close"] > bottom_line:
        allow_ce = True
        print("🔓 CE REARMED @", t)

    # ---- CE SIGNAL ----
    if ce_pos is None and allow_ce:
        if row["close"] < bottom_line and avg_price < bottom_line and avg_price > row["close"]:
            pending_ce = True
            print("📉 CE SIGNAL @", t)

    # ---- PE SIGNAL ----
    if pe_pos is None and allow_pe:
        if row["close"] > top_line and avg_price > top_line and avg_price < row["close"]:
            pending_pe = True
            print("📈 PE SIGNAL @", t)

    # ---- INDEX EXIT ----
    if ce_pos and row["close"] > bottom_line:
        exit_position("CE", last_ce_ltp, t,"INDEX")

    if pe_pos and row["close"] < top_line:
        exit_position("PE", last_pe_ltp, t,"INDEX")




def manage_position(side, price, t):
    global ce_pos, pe_pos

    pos = ce_pos if side == "CE" else pe_pos

    if pos is None:
        return

    pos["best"] = min(pos["best"], price)

    if not pos["active"] and price <= pos["entry_price"] - 30:
        pos["active"] = True
        print(f"🔐 {side} TRAILING ACTIVATED @ {price}")

    if pos["active"]:
        # step-based trailing (every 10 points)
        if pos["best"] <= pos["trail"] - 10:
            pos["trail"] = pos["best"] - 30
            pos["sl"] = pos["trail"] - 15
            print(f"🔁 {side} TRAIL UPDATED → SL: {pos['sl']} | TRAIL: {pos['trail']}")

        if price >= pos["sl"]:
            exit_position(side, price, t, "SL")


def exit_position(side, price, t, reason):
    global ce_pos, pe_pos, total_pnl, stop_trading ,allow_ce, allow_pe

    pos = ce_pos if side == "CE" else pe_pos
    if stop_trading:
        telemetry["status"] = "STOPPED"
    if pos is None:
        return

    pnl = pos["entry_price"] - price
    total_pnl += pnl

    print(f"❌ {side} EXIT [{reason}] @ {price} | PNL {round(pnl,2)} | TOTAL {round(total_pnl,2)}")

    log_trade_event(
        event_type="EXIT",
        leg_name=side,
        token=CE_ID if side == "CE" else PE_ID,
        symbol=SYMBOL,
        side="BUY",  # exit of SELL = BUY
        lot=LOT,
        price=price,
        reason=reason,
        pnl=pnl,
        cum_pnl=total_pnl
    )


    if side == "CE":
        ce_pos = None
        if reason in ("SL", "TSL"): 
            allow_ce = False
    else:
        pe_pos = None
        if reason in ("SL", "TSL"):
            allow_pe = False


    if total_pnl <= DAY_TARGET:
        print("🛑 DAY TARGET HIT")
        stop_trading = True

# =========================
# WS HANDLERS
# =========================


def on_tick_index(msg):
    candle = idx_builder.process_tick(msg)

    if candle:
        on_index_candle(msg["security_id"], datetime.now(IST), candle)


def on_tick_option(msg):
    global ce_pos, pe_pos, pending_ce, pending_pe, last_ce_ltp, last_pe_ltp
    

    token = str(msg["security_id"])
    ltp = msg.get("LTP")
    t = datetime.now(IST)
    if token == CE_ID:
        last_ce_ltp = ltp
        telemetry["ce_ltp"] = ltp
    if token == PE_ID:
        last_pe_ltp = ltp
        telemetry["pe_ltp"] = ltp

    ce_running_pnl = 0
    pe_running_pnl = 0

    if ce_pos and last_ce_ltp:
        ce_running_pnl = ce_pos["entry_price"] - last_ce_ltp

    if pe_pos and last_pe_ltp:
        pe_running_pnl = pe_pos["entry_price"] - last_pe_ltp

    # ---- LTP ENTRY ----
    if token == CE_ID and pending_ce and ce_pos is None:
        ce_pos = {
            "entry_time": t,
            "entry_price": ltp,
            "best": ltp,
            "sl": ltp - 15,
            "trail": ltp - 30,
            "active": False,
        }
        pending_ce = False
        print("⚡ CE SELL (LTP) @", ltp, t)

        log_trade_event(
        event_type="ENTRY",
        leg_name="CE",
        token=CE_ID,
        symbol=SYMBOL,   
        side="SELL",
        lot=LOT,
        price=ltp,
        reason="SIGNAL",
        pnl=0,
        cum_pnl=total_pnl
    )

    if token == PE_ID and pending_pe and pe_pos is None:
        pe_pos = {
            "entry_time": t,
            "entry_price": ltp,
            "best": ltp,
            "sl": ltp - 15,
            "trail": ltp - 30,
            "active": False,
        }
        pending_pe = False
        print("⚡ PE SELL (LTP) @", ltp, t)
        log_trade_event(
        event_type="ENTRY",
        leg_name="PE",
        token=PE_ID,
        symbol=SYMBOL,
        side="SELL",
        lot=LOT,
        price=ltp,
        reason="SIGNAL",
        pnl=0,
        cum_pnl=total_pnl
    )

    if token == CE_ID and ce_pos:
        manage_position("CE", ltp, t)

    if token == PE_ID and pe_pos:
        manage_position("PE", ltp, t)

    


    telemetry["ce_pnl"] = round(ce_running_pnl, 2)
    telemetry["pe_pnl"] = round(pe_running_pnl, 2)

    # total = realized + running
    telemetry["pnl"] = round(total_pnl + ce_running_pnl + pe_running_pnl, 2)







# =========================
# MAIN
# =========================

if __name__ == "__main__":

    load_fno_master()

    wait_for_start()
    mark_range()

    instruments = [
        (marketfeed.NSE, INDEX_TOKEN),
        (marketfeed.NSE_FNO, CE_ID),
        (marketfeed.NSE_FNO, PE_ID)
    ]

    feed = marketfeed.DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments, "v2")

    print("\n🚀 Range Breakout Paper Engine Running...\n")

    while True:
        try:

            feed.run_forever()
            msg = feed.get_data()

            if msg:

                if str(msg["security_id"]) == INDEX_TOKEN:
                    on_tick_index(msg)

                elif str(msg["security_id"]) in (CE_ID, PE_ID):
                    on_tick_option(msg)
        except Exception as e:
            print("WS ERROR:", e)
            feed.run_forever()