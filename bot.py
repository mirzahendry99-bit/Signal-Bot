import os
import json
import time
import urllib.request
import urllib.parse
import gate_api
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from supabase import create_client

print("🚀 SIGNAL BOT RUNNING")

API_KEY      = os.environ.get("GATE_API_KEY")
SECRET_KEY   = os.environ.get("GATE_SECRET_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TG_TOKEN     = os.environ.get("TELEGRAM_TOKEN")
TG_CHAT_ID   = os.environ.get("CHAT_ID")

MIN_VOLUME   = 500000

BLACKLIST = [
    "3S","3L","5S","5L",
    "TUSD","USDC","BUSD","DAI","FDUSD","USD1",
    "USDP","USDD","USDJ","ZUSD","GUSD","CUSD","SUSD",
    "STBL","FRAX","LUSD","USDN","STABLE","BARD"
]

WIB = timezone(timedelta(hours=7))

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# =====================
# TELEGRAM
# =====================
def tg(msg):
    try:
        if not TG_TOKEN or not TG_CHAT_ID:
            print("⚠️ Telegram config missing")
            return
        url  = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        body = json.dumps({
            "chat_id": TG_CHAT_ID,
            "text": msg,
            "parse_mode": "HTML"
        }).encode()
        req = urllib.request.Request(
            url, data=body,
            headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"⚠️ Telegram error: {e}")


# =====================
# GATE CLIENT
# =====================
def get_client():
    cfg = gate_api.Configuration(
        host="https://api.gateio.ws/api/v4",
        key=API_KEY,
        secret=SECRET_KEY
    )
    return gate_api.SpotApi(gate_api.ApiClient(cfg))


# =====================
# HELPERS
# =====================
def is_valid(pair):
    if not pair.endswith("_USDT"):
        return False
    for b in BLACKLIST:
        if b in pair:
            return False
    return True


def get_candles(client, pair, interval, limit):
    try:
        candles = client.list_candlesticks(
            currency_pair=pair, interval=interval, limit=limit
        )
        if not candles or len(candles) < 20:
            return None, None, None
        closes  = np.array([float(c[2]) for c in candles])
        highs   = np.array([float(c[3]) for c in candles])
        lows    = np.array([float(c[4]) for c in candles])
        volumes = np.array([float(c[5]) for c in candles])
        return closes, highs, lows, volumes
    except Exception:
        return None, None, None, None


def calc_rsi(closes, period=14):
    s     = pd.Series(closes)
    delta = s.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / (loss + 1e-9)
    return float((100 - 100 / (1 + rs)).iloc[-1])


def calc_ema(closes, period):
    return float(pd.Series(closes).ewm(span=period).mean().iloc[-1])


def calc_macd(closes):
    s      = pd.Series(closes)
    ema12  = s.ewm(span=12).mean()
    ema26  = s.ewm(span=26).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    return float(macd.iloc[-1]), float(signal.iloc[-1])


def calc_bb(closes, period=20):
    s    = pd.Series(closes)
    mid  = s.rolling(period).mean().iloc[-1]
    std  = s.rolling(period).std().iloc[-1]
    return float(mid - 2*std), float(mid), float(mid + 2*std)


def calc_atr(closes, highs, lows, period=14):
    tr_list = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        )
        tr_list.append(tr)
    atr = pd.Series(tr_list).rolling(period).mean().iloc[-1]
    return float(atr)


def already_sent(pair, signal_type, timeframe):
    """Cek apakah signal pair+type+timeframe sudah dikirim dalam periode valid."""
    now = datetime.now(WIB)
    res = supabase.table("signals").select("id").eq("pair", pair).eq("type", signal_type).eq("timeframe", timeframe).gt("valid_until", now.isoformat()).execute()
    return len(res.data) > 0


def save_signal(pair, signal_type, side, entry, tp1, tp2, sl, strength, timeframe, valid_until):
    now = datetime.now(WIB)
    supabase.table("signals").insert({
        "pair":        pair,
        "type":        signal_type,
        "side":        side,
        "entry_price": entry,
        "tp1":         tp1,
        "tp2":         tp2,
        "sl":          sl,
        "strength":    strength,
        "timeframe":   timeframe,
        "valid_from":  now.isoformat(),
        "valid_until": valid_until.isoformat()
    }).execute()


def stars(strength):
    return "⭐" * strength + "☆" * (5 - strength)


def fmt_time(dt):
    return dt.strftime("%H:%M WIB")


def send_signal(pair, signal_type, side, entry, tp1, tp2, sl, strength, timeframe, valid_minutes):
    now         = datetime.now(WIB)
    valid_until = now + timedelta(minutes=valid_minutes)

    if already_sent(pair, signal_type, timeframe):
        return

    emoji_side = "🟢 BUY" if side == "BUY" else "🔴 SELL"
    emoji_type = {
        "SCALPING":  "⚡",
        "INTRADAY":  "📈",
        "SWING":     "🌊"
    }.get(signal_type, "🎯")

    pct_tp1 = ((tp1 - entry) / entry * 100) if side == "BUY" else ((entry - tp1) / entry * 100)
    pct_tp2 = ((tp2 - entry) / entry * 100) if side == "BUY" else ((entry - tp2) / entry * 100)
    pct_sl  = ((entry - sl) / entry * 100)  if side == "BUY" else ((sl - entry) / entry * 100)

    msg = (
        f"{emoji_type} <b>SIGNAL {emoji_side} — {signal_type}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair:     <b>{pair.replace('_USDT', '/USDT')}</b>\n"
        f"Entry:    <b>${entry:.6f}</b>\n"
        f"⏰ Valid:  {fmt_time(now)} → {fmt_time(valid_until)}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"TP1:      ${tp1:.6f} <i>(+{pct_tp1:.1f}%)</i>\n"
        f"TP2:      ${tp2:.6f} <i>(+{pct_tp2:.1f}%)</i>\n"
        f"SL:       ${sl:.6f} <i>(-{abs(pct_sl):.1f}%)</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Strength: {stars(strength)}\n"
        f"TF:       {timeframe}"
    )

    tg(msg)
    save_signal(pair, signal_type, side, entry, tp1, tp2, sl, strength, timeframe, valid_until)
    print(f"✅ Signal {signal_type} {side} → {pair}")


# =====================
# FEAR & GREED
# =====================
def get_fear_greed():
    try:
        req  = urllib.request.Request("https://api.alternative.me/fng/?limit=1")
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
            return int(data["data"][0]["value"])
    except Exception:
        return 50  # neutral fallback


# =====================
# SIGNAL ENGINES
# =====================

def check_scalping(client, pair, price, vol_24h):
    """5m — RSI + EMA cross + Volume spike."""
    result = get_candles(client, pair, "5m", 80)
    if result[0] is None:
        return
    closes, highs, lows, volumes = result

    rsi   = calc_rsi(closes)
    ema9  = calc_ema(closes, 9)
    ema21 = calc_ema(closes, 21)
    atr   = calc_atr(closes, highs, lows)

    vol_spike = volumes[-1] > np.mean(volumes[-20:]) * 1.8

    # BUY signal
    if rsi < 35 and ema9 > ema21 and vol_spike:
        strength = 3
        if rsi < 28: strength += 1
        if vol_24h > 1000000: strength += 1
        strength = min(strength, 5)

        tp1 = price * 1.02
        tp2 = price * 1.04
        sl  = price - (atr * 1.5)

        send_signal(pair, "SCALPING", "BUY", price, tp1, tp2, sl, strength, "5m", 15)

    # SELL signal
    elif rsi > 70 and ema9 < ema21 and vol_spike:
        strength = 3
        if rsi > 78: strength += 1
        if vol_24h > 1000000: strength += 1
        strength = min(strength, 5)

        tp1 = price * 0.98
        tp2 = price * 0.96
        sl  = price + (atr * 1.5)

        send_signal(pair, "SCALPING", "SELL", price, tp1, tp2, sl, strength, "5m", 15)


def check_intraday(client, pair, price, vol_24h, fg):
    """1h — RSI + MACD + Bollinger Band + Fear & Greed."""
    result = get_candles(client, pair, "1h", 100)
    if result[0] is None:
        return
    closes, highs, lows, volumes = result

    rsi          = calc_rsi(closes)
    macd, signal = calc_macd(closes)
    bb_low, bb_mid, bb_high = calc_bb(closes)
    atr          = calc_atr(closes, highs, lows)

    # BUY signal
    if (rsi < 40 and macd > signal and price < bb_mid and fg < 45):
        strength = 2
        if rsi < 35:    strength += 1
        if price < bb_low: strength += 1
        if fg < 30:     strength += 1
        strength = min(strength, 5)

        tp1 = price * 1.03
        tp2 = price * 1.06
        sl  = price - (atr * 2)

        send_signal(pair, "INTRADAY", "BUY", price, tp1, tp2, sl, strength, "1h", 120)

    # SELL signal
    elif (rsi > 60 and macd < signal and price > bb_mid and fg > 55):
        strength = 2
        if rsi > 65:     strength += 1
        if price > bb_high: strength += 1
        if fg > 70:      strength += 1
        strength = min(strength, 5)

        tp1 = price * 0.97
        tp2 = price * 0.94
        sl  = price + (atr * 2)

        send_signal(pair, "INTRADAY", "SELL", price, tp1, tp2, sl, strength, "1h", 120)


def check_swing(client, pair, price, vol_24h, fg):
    """4h — EMA 50/200 + RSI + Volume trend + Fear & Greed."""
    result = get_candles(client, pair, "4h", 210)
    if result[0] is None:
        return
    closes, highs, lows, volumes = result

    rsi    = calc_rsi(closes)
    ema50  = calc_ema(closes, 50)
    ema200 = calc_ema(closes, 200)
    atr    = calc_atr(closes, highs, lows)

    vol_trend = np.mean(volumes[-5:]) > np.mean(volumes[-20:]) * 1.2

    # BUY — Golden cross zone
    if (ema50 > ema200 and rsi < 50 and rsi > 30 and vol_trend and fg < 60):
        strength = 2
        if rsi < 40:    strength += 1
        if vol_trend:   strength += 1
        if fg < 35:     strength += 1
        strength = min(strength, 5)

        tp1 = price * 1.05
        tp2 = price * 1.10
        sl  = price - (atr * 3)

        send_signal(pair, "SWING", "BUY", price, tp1, tp2, sl, strength, "4h", 720)

    # SELL — Death cross zone
    elif (ema50 < ema200 and rsi > 50 and rsi < 70 and vol_trend and fg > 40):
        strength = 2
        if rsi > 60:    strength += 1
        if vol_trend:   strength += 1
        if fg > 65:     strength += 1
        strength = min(strength, 5)

        tp1 = price * 0.95
        tp2 = price * 0.90
        sl  = price + (atr * 3)

        send_signal(pair, "SWING", "SELL", price, tp1, tp2, sl, strength, "4h", 720)


# =====================
# MARKET FILTER
# =====================
def market_ok(client):
    try:
        btc    = client.list_tickers(currency_pair="BTC_USDT")[0]
        change = float(btc.change_percentage or 0)
        print(f"BTC 24h: {change:.2f}%")
        return change > -5
    except Exception as e:
        print(f"⚠️ Market check error: {e}")
        return False


# =====================
# MAIN
# =====================
def run():
    client = get_client()
    print("=== SIGNAL SCAN START ===")

    if not market_ok(client):
        print("❌ Market crash, skip scan")
        tg("⚠️ <b>Market Alert</b>\nBTC turun >5%, scan signal ditunda.")
        return

    fg = get_fear_greed()
    print(f"Fear & Greed: {fg}")

    tickers = client.list_tickers()
    total   = 0

    for t in tickers:
        pair    = t.currency_pair
        if not is_valid(pair):
            continue

        price   = float(t.last or 0)
        vol_24h = float(t.quote_volume or 0)

        if price <= 0 or vol_24h < MIN_VOLUME:
            continue

        try:
            check_scalping(client, pair, price, vol_24h)
            check_intraday(client, pair, price, vol_24h, fg)
            check_swing(client, pair, price, vol_24h, fg)
            total += 1
        except Exception as e:
            print(f"⚠️ Error {pair}: {e}")
            continue

    print(f"=== SCAN DONE | {total} pairs scanned ===")


if __name__ == "__main__":
    run()
