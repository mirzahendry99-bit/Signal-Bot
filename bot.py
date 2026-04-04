import os
import gate_api
import requests
import pandas as pd
import numpy as np
from supabase import create_client

# ENV
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg})

def setup_client():
    config = gate_api.Configuration(host="https://api.gateio.ws/api/v4")
    return gate_api.SpotApi(gate_api.ApiClient(config))

def get_data(client, pair, tf):
    candles = client.list_candlesticks(currency_pair=pair, interval=tf, limit=100)
    closes = np.array([float(c[2]) for c in candles])
    volumes = np.array([float(c[5]) for c in candles])
    return closes, volumes

def rsi(data, period=14):
    s = pd.Series(data)
    delta = s.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / (loss + 1e-9)
    return float((100 - (100 / (1 + rs))).iloc[-1])

def ema(data, period):
    return float(pd.Series(data).ewm(span=period).mean().iloc[-1])

# =========================

def generate_signal(pair, price, tp, sl, tf, score, typ):
    signal = {
        "pair": pair,
        "timeframe": tf,
        "entry": price,
        "tp": tp,
        "sl": sl,
        "confidence": score,
        "type": typ
    }

    supabase.table("signals").insert(signal).execute()

    msg = f"""
🔥 SIGNAL {typ.upper()}
Pair: {pair}
TF: {tf}
Entry: {price}
TP: {tp}
SL: {sl}
Confidence: {score}/6
"""

    send_telegram(msg)

# =========================

def scan_market():
    client = setup_client()

    for t in client.list_tickers():
        pair = t.currency_pair

        if not pair.endswith("_USDT"):
            continue

        try:
            closes, volumes = get_data(client, pair, "5m")

            r = rsi(closes)
            e20 = ema(closes, 20)
            e50 = ema(closes, 50)

            price = float(t.last or 0)
            change = float(t.change_percentage or 0)

            score = 0

            if r < 30: score += 2
            if e20 > e50: score += 2
            if change > 0: score += 1
            if volumes[-1] > np.mean(volumes[-20:]) * 1.5: score += 1

            if score >= 4:
                tp = price * 1.03
                sl = price * 0.98

                generate_signal(pair, price, tp, sl, "5m", score, "scalp")

        except:
            continue

# =========================

if __name__ == "__main__":
    print("🚀 SIGNAL ENGINE RUNNING")
    scan_market()
