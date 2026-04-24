import time
import pytz
import requests
import os
from dhanhq import marketfeed
from datetime import datetime, time as dtime
from collections import defaultdict
from dhanhq import dhanhq
from dotenv import load_dotenv
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from find_security import load_fno_master
import pandas as pd

load_dotenv()
client_id = os.getenv("CLIENT_ID")
access_token = get_access_token()

OI_ENTRY = 6000000  

IST = pytz.timezone("Asia/Kolkata")
today = datetime.now(IST).strftime("%Y-%m-%d")

TRADE_START = dtime(10,0)

atm_strike = None

combined_pnl = 0

# ================= GLOBAL =================
ltp_data = {}
oi_data = {}
positions = {}
security_map = {}   # sec_id → symbo


dhan = dhanhq(client_id, access_token)

# ================== HELPER FUNCTIONS ==================

def get_opposite_symbol(strike, target_opt):
    for data in security_map.values():
        if data["strike"] == strike and data["opt"] == target_opt:
            return data["symbol"]
    return None


def calculate_atm(price, step=50):
    return int(round(price / step) * step)


def mark_range():
    global atm_strike, today

    idx = dhan.intraday_minute_data(
        security_id=13,
        exchange_segment="IDX_I",
        instrument_type="INDEX",
        from_date=today,
        to_date=today,
        interval=1
    )

    data = idx.get("data", {})

    opens = data.get("open", [])
    highs=data.get("high",[])
    lows=data.get("low",[])
    closes = data.get("close", [])
    timestamps = data.get("timestamp", [])

    candle = None

    for i in range(len(timestamps)):
        ts_raw = timestamps[i]

        ts = datetime.fromtimestamp(timestamps[i], IST)

        if ts.hour == 10 and ts.minute == 0:
            candle = {
                "open": opens[i],
                "high": highs[i],
                "low": lows[i],
                "close": closes[i]
            }
            break

    if candle:
        close_price = candle["close"]

        atm_strike = calculate_atm(close_price)

        print(f"✅ 10:00 Candle Close: {close_price}")
        print(f"🎯 ATM FIXED: {atm_strike}")

        return atm_strike

    else:
        print("⏳ Waiting for 10:00 candle...")
        return None
def generate_strikes(atm):
    strikes = []

    for i in range(-10, 11):
        strike = atm + (i * 50)
        strikes.append(strike)

    return strikes

def generate_option_symbols(atm):
    symbols = []

    strikes = generate_strikes(atm)

    for strike in strikes:
        symbols.append((strike, "CE"))
        symbols.append((strike, "PE"))

    return symbols

df = load_fno_master()

atm = mark_range()

def get_40_security_ids(df, atm):
    today = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))

    base = df[
        (df["INSTRUMENT"] == "OPTIDX") &
        (df["UNDERLYING_SYMBOL"] == "NIFTY") &
        (df["SM_EXPIRY_DATE"] >= today)
    ]

    nearest_expiry = base["SM_EXPIRY_DATE"].min()
    base = base[base["SM_EXPIRY_DATE"] == nearest_expiry]

    security_ids = []
    for i in range(-10, 11):
        strike = atm + (i * 50)

        for opt in ["CE", "PE"]:
            row = base[
                (base["STRIKE_PRICE"] == strike) &
                (base["OPTION_TYPE"] == opt)
            ]

            if not row.empty:
                row_data = row.iloc[0]

                sec_id = str(row_data["SECURITY_ID"])

                underlying = row_data["UNDERLYING_SYMBOL"]
                strike = int(row_data["STRIKE_PRICE"])
                opt = row_data["OPTION_TYPE"]

                expiry = row_data["SM_EXPIRY_DATE"]
                expiry_str = expiry.strftime("%d%b%y").upper()

                symbol = f"{underlying}{expiry_str}{strike}{opt}"

                security_ids.append(sec_id)

                security_map[sec_id] = {
                    "symbol": symbol,
                    "strike": strike,
                    "opt": opt
                }


    print(f"Subscribing the {len(security_ids)} instruments")
    print(security_ids)
    return security_ids


def init_state():
    return {
        "position": False,
        "entry_price": None,
        "tsl": None,
        "sl": None,
        "tsl_active": False,
        "lot": 1,
        "pnl": 0.0
    }

global_state = {
    "position": False,
    "side": None,   # "CE" or "PE"
    "entry_price": None,
    "symbol": None,
    "tsl": None,
    "sl": None,
    "tsl_active": False
}

def handle_global_logic():
    global combined_pnl

    ce_total, pe_total = calculate_total_oi()
    print("CE_total",ce_total)
    print("PE_total",pe_total)

    diff = abs(ce_total - pe_total)
    
    print("DIFF" , diff)


    # ENTRY
    if not global_state["position"] and diff >= OI_ENTRY:

        atm_ce = get_opposite_symbol(atm_strike, "CE")
        atm_pe = get_opposite_symbol(atm_strike, "PE")

        # PE dominant → BUY CE
        if pe_total > ce_total:
            price = ltp_data.get(atm_ce)

            if price:
                global_state.update({
                    "position": True,
                    "side": "CE",
                    "entry_price": price,
                    "symbol": atm_ce,
                    "tsl_active": False
                })
                print("🟢 ENTRY CE", atm_ce, price)

        # CE dominant → BUY PE
        else:
            price = ltp_data.get(atm_pe)

            if price:
                global_state.update({
                    "position": True,
                    "side": "PE",
                    "entry_price": price,
                    "symbol": atm_pe,
                    "tsl_active": False
                })
                print("🟢 ENTRY PE", atm_pe, price)

    # MANAGEMENT
    if global_state["position"]:
        symbol = global_state["symbol"]
        price = ltp_data.get(symbol)

        if price is None:
            return

        # TSL activate
        if not global_state["tsl_active"] and price >= global_state["entry_price"] + 2000:
            global_state["tsl_active"] = True
            global_state["tsl"] = price
            global_state["sl"] = price - 1000

        # trail
        if global_state["tsl_active"]:
            if price >= global_state["tsl"] + 500:
                global_state["tsl"] += 500
                global_state["sl"] += 500

            if price <= global_state["sl"]:
                pnl = price - global_state["entry_price"]
                combined_pnl += pnl

                print(f"🔴 EXIT SL {symbol} | PnL: {pnl} | CumPnL: {combined_pnl}")
                global_state["position"] = False

        # OI EXIT
        if diff < OI_ENTRY:
            pnl = price - global_state["entry_price"]
            combined_pnl += pnl

            print(f"🔴 EXIT OI {symbol} | PnL: {pnl} | CumPnL: {combined_pnl}")
            global_state["position"] = False

def calculate_total_oi():
    ce_total = 0
    pe_total = 0

    for sec_id, data in security_map.items():
        symbol = data["symbol"]
        opt = data["opt"]

        oi = oi_data.get(symbol)
        if oi is None:
            continue

        if opt == "CE":
            ce_total += oi
        else:
            pe_total += oi

    return ce_total, pe_total

security_ids = get_40_security_ids(df, atm)

builders = {
    sec_id: OneMinuteCandleBuilder()
    for sec_id in security_ids
}
def on_message(msg):

    token = str(msg.get("security_id"))

    data = security_map.get(token)
    if not data:
        return

    symbol = data["symbol"]
    strike = data["strike"]
    opt = data["opt"]

    # ================= LTP =================
    if msg.get("type") == "Quote Data":
        ltp = float(msg.get("LTP") or msg.get("last_traded_price") or 0)
        ltp_data[symbol] = ltp

    # ================= OI =================
    elif msg.get("type") == "OI Data":
        oi = msg.get("OI")
        if oi is not None:
            oi_data[symbol] = oi

    if len(oi_data) >= len(security_ids) * 0.8:
        handle_global_logic()


instruments = [
    (marketfeed.NSE_FNO, sec_id, marketfeed.Quote)
    for sec_id in security_ids
    ]



feed = marketfeed.DhanFeed(client_id, access_token, instruments, "v2")
 
while True:
    try:
        feed.run_forever()
        data = feed.get_data()
        
        

        if data:
                
            on_message(data)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()