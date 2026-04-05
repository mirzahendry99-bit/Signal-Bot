import os
import json
import urllib.request
import gate_api
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from supabase import create_client

print("🚀 SIGNAL BOT — FINAL EDITION")

API_KEY      = os.environ.get("GATE_API_KEY")
SECRET_KEY   = os.environ.get("GATE_SECRET_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TG_TOKEN     = os.environ.get("TELEGRAM_TOKEN")
TG_CHAT_ID   = os.environ.get("CHAT_ID")

MIN_VOLUME   = 300000
MIN_STRENGTH = 3

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


def http_get(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"⚠️ HTTP {url[:50]}: {e}")
        return None


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
# EXTERNAL DATA
# =====================
def get_fear_greed():
    try:
        data = http_get("https://api.alternative.me/fng/?limit=1")
        if data:
            val = int(data["data"][0]["value"])
            lbl = data["data"][0]["value_classification"]
            print(f"Fear & Greed: {val} ({lbl})")
            return val, lbl
    except Exception:
        pass
    return 50, "Neutral"


def get_coingecko_market():
    data = http_get("https://api.coingecko.com/api/v3/global")
    if not data:
        return None
    try:
        d = data["data"]
        return {
            "btc_dominance": float(d.get("market_cap_percentage", {}).get("btc", 50)),
            "market_cap_change_24h": float(d.get("market_cap_change_percentage_24h_usd", 0))
        }
    except Exception:
        return None


def get_coingecko_trending():
    data = http_get("https://api.coingecko.com/api/v3/search/trending")
    if not data:
        return []
    try:
        return [item.get("item", {}).get("symbol", "").upper()
                for item in data.get("coins", [])]
    except Exception:
        return []


def get_funding_rate(pair):
    try:
        data = http_get(f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{pair}")
        if data and "funding_rate" in data:
            return float(data["funding_rate"])
    except Exception:
        pass
    return None


def get_order_book_pressure(client, pair):
    try:
        ob      = client.list_order_book(currency_pair=pair, limit=20)
        bid_vol = sum(float(b[1]) for b in ob.bids)
        ask_vol = sum(float(a[1]) for a in ob.asks)
        return bid_vol / ask_vol if ask_vol > 0 else 1.0
    except Exception:
        return 1.0


# =====================
# INDICATORS
# =====================
def get_candles(client, pair, interval, limit):
    try:
        candles = client.list_candlesticks(
            currency_pair=pair, interval=interval, limit=limit
        )
        if not candles or len(candles) < 20:
            return None, None, None, None
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
    s   = pd.Series(closes)
    mid = s.rolling(period).mean().iloc[-1]
    std = s.rolling(period).std().iloc[-1]
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
    return float(pd.Series(tr_list).rolling(period).mean().iloc[-1])


def calc_stoch_rsi(closes, period=14):
    """Stochastic RSI — konfirmasi oversold/overbought lebih akurat."""
    rsi_series = []
    s = pd.Series(closes)
    delta = s.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs   = gain / (loss + 1e-9)
    rsi  = 100 - (100 / (1 + rs))
    rsi_min = rsi.rolling(period).min()
    rsi_max = rsi.rolling(period).max()
    stoch_rsi = (rsi - rsi_min) / (rsi_max - rsi_min + 1e-9)
    return float(stoch_rsi.iloc[-1])


def calc_support_resistance(highs, lows, closes, lookback=20):
    """Deteksi level support dan resistance terdekat."""
    recent_highs = highs[-lookback:]
    recent_lows  = lows[-lookback:]
    resistance   = float(np.percentile(recent_highs, 85))
    support      = float(np.percentile(recent_lows, 15))
    return support, resistance


def calc_rsi_divergence(closes, highs, lows, period=14):
    """
    Deteksi RSI divergence — salah satu signal reversal paling kuat.
    Bullish divergence: harga lower low tapi RSI higher low → BUY
    Bearish divergence: harga higher high tapi RSI lower high → SELL
    """
    if len(closes) < 30:
        return None

    s     = pd.Series(closes)
    delta = s.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / (loss + 1e-9)
    rsi   = 100 - (100 / (1 + rs))

    # Ambil 2 titik terakhir untuk bandingkan
    price_now  = closes[-1]
    price_prev = closes[-15]
    rsi_now    = float(rsi.iloc[-1])
    rsi_prev   = float(rsi.iloc[-15])

    # Bullish divergence
    if price_now < price_prev and rsi_now > rsi_prev and rsi_now < 45:
        return "BULLISH"

    # Bearish divergence
    if price_now > price_prev and rsi_now < rsi_prev and rsi_now > 55:
        return "BEARISH"

    return None


def calc_volume_profile(closes, volumes, bins=10):
    """
    Deteksi Point of Control (POC) — harga dengan volume tertinggi.
    Harga mendekati POC = zona akumulasi/distribusi kuat.
    """
    if len(closes) < 10:
        return None
    price_min = min(closes)
    price_max = max(closes)
    if price_max == price_min:
        return None
    bin_size  = (price_max - price_min) / bins
    vol_bins  = [0.0] * bins
    for i, price in enumerate(closes):
        idx = int((price - price_min) / bin_size)
        idx = min(idx, bins - 1)
        vol_bins[idx] += volumes[i]
    poc_idx = vol_bins.index(max(vol_bins))
    poc     = price_min + (poc_idx + 0.5) * bin_size
    return float(poc)


# =====================
# SIGNAL TRACKING
# =====================
def already_sent(pair, signal_type, timeframe):
    now = datetime.now(WIB)
    res = supabase.table("signals").select("id") \
        .eq("pair", pair) \
        .eq("type", signal_type) \
        .eq("timeframe", timeframe) \
        .gt("valid_until", now.isoformat()) \
        .execute()
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


def get_win_rate(signal_type):
    """Ambil win rate historis dari Supabase untuk ditampilkan di signal."""
    try:
        res = supabase.table("signals").select("result") \
            .eq("type", signal_type) \
            .not_.is_("result", "null") \
            .execute()
        if not res.data or len(res.data) < 5:
            return None
        total = len(res.data)
        wins  = sum(1 for r in res.data if r.get("result") == "TP")
        return round((wins / total) * 100, 1)
    except Exception:
        return None


# =====================
# SIGNAL SENDER
# =====================
def stars(n):
    return "⭐" * n + "☆" * (5 - n)


def fmt_time(dt):
    return dt.strftime("%H:%M WIB")


def send_signal(pair, signal_type, side, entry, tp1, tp2, sl,
                strength, timeframe, valid_minutes, sources="", extra=""):
    if strength < MIN_STRENGTH:
        return
    if already_sent(pair, signal_type, timeframe):
        return

    now         = datetime.now(WIB)
    valid_until = now + timedelta(minutes=valid_minutes)

    emoji_side = "🟢 BUY" if side == "BUY" else "🔴 SELL"
    emoji_type = {
        "SCALPING": "⚡",
        "INTRADAY": "📈",
        "SWING":    "🌊",
        "MOONSHOT": "🚀"
    }.get(signal_type, "🎯")

    pct_tp1 = abs((tp1 - entry) / entry * 100)
    pct_tp2 = abs((tp2 - entry) / entry * 100)
    pct_sl  = abs((sl  - entry) / entry * 100)

    # Risk/Reward ratio
    rr = pct_tp1 / pct_sl if pct_sl > 0 else 0

    win_rate = get_win_rate(signal_type)
    wr_str   = f"{win_rate}%" if win_rate else "N/A (data belum cukup)"

    msg = (
        f"{emoji_type} <b>SIGNAL {emoji_side} — {signal_type}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair:     <b>{pair.replace('_USDT', '/USDT')}</b>\n"
        f"Entry:    <b>${entry:.6f}</b>\n"
        f"⏰ Valid:  {fmt_time(now)} → {fmt_time(valid_until)}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"TP1:      ${tp1:.6f} <i>(+{pct_tp1:.1f}%)</i>\n"
        f"TP2:      ${tp2:.6f} <i>(+{pct_tp2:.1f}%)</i>\n"
        f"SL:       ${sl:.6f} <i>(-{pct_sl:.1f}%)</i>\n"
        f"R/R:      <b>1:{rr:.1f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Strength: {stars(strength)}\n"
        f"Win Rate: <i>{wr_str}</i>\n"
        f"TF:       {timeframe}\n"
        f"Data:     {sources}"
    )

    if extra:
        msg += f"\n{extra}"

    tg(msg)
    save_signal(pair, signal_type, side, entry, tp1, tp2, sl, strength, timeframe, valid_until)
    print(f"✅ {signal_type} {side} → {pair} | Strength:{strength} | R/R:1:{rr:.1f}")


def is_valid(pair):
    if not pair.endswith("_USDT"):
        return False
    for b in BLACKLIST:
        if b in pair:
            return False
    return True


# =====================
# SCALPING 5m
# =====================
def check_scalping(client, pair, price, fg, ob_ratio, funding):
    closes, highs, lows, volumes = get_candles(client, pair, "5m", 80)
    if closes is None:
        return

    rsi        = calc_rsi(closes)
    stoch_rsi  = calc_stoch_rsi(closes)
    ema9       = calc_ema(closes, 9)
    ema21      = calc_ema(closes, 21)
    atr        = calc_atr(closes, highs, lows)
    support, resistance = calc_support_resistance(highs, lows, closes)
    divergence = calc_rsi_divergence(closes, highs, lows)
    vol_avg    = np.mean(volumes[-20:])
    vol_ratio  = volumes[-1] / vol_avg if vol_avg > 0 else 0
    vol_spike  = vol_ratio > 1.5

    sources = "Gate.io + F&G + OB + StochRSI + S/R"

    # BUY
    if rsi < 35 and ema9 > ema21 and vol_spike:
        strength = 2
        if rsi < 28:              strength += 1
        if stoch_rsi < 0.2:       strength += 1
        if ob_ratio > 1.2:        strength += 1
        if fg < 25:               strength += 1
        if vol_ratio > 2.5:       strength += 1
        if divergence == "BULLISH": strength += 1
        if price <= support * 1.02: strength += 1
        if funding and funding < -0.001: strength += 1
        strength = min(strength, 5)

        tp1 = price * 1.05
        tp2 = min(price * 1.08, resistance * 0.99)
        sl  = max(price - (atr * 2.0), price * 0.96)

        extra = ""
        if divergence == "BULLISH":
            extra += "\n🔀 <i>Bullish RSI Divergence terdeteksi!</i>"
        if price <= support * 1.02:
            extra += f"\n🛡️ <i>Harga di zona Support ${support:.6f}</i>"

        send_signal(pair, "SCALPING", "BUY", price, tp1, tp2, sl,
                    strength, "5m", 15, sources, extra)

    # SELL
    elif rsi > 68 and ema9 < ema21 and vol_spike:
        strength = 2
        if rsi > 75:              strength += 1
        if stoch_rsi > 0.8:       strength += 1
        if ob_ratio < 0.8:        strength += 1
        if fg > 65:               strength += 1
        if vol_ratio > 2.5:       strength += 1
        if divergence == "BEARISH": strength += 1
        if price >= resistance * 0.98: strength += 1
        if funding and funding > 0.001: strength += 1
        strength = min(strength, 5)

        tp1 = price * 0.95
        tp2 = max(price * 0.92, support * 1.01)
        sl  = min(price + (atr * 2.0), price * 1.04)

        extra = ""
        if divergence == "BEARISH":
            extra += "\n🔀 <i>Bearish RSI Divergence terdeteksi!</i>"
        if price >= resistance * 0.98:
            extra += f"\n🚧 <i>Harga di zona Resistance ${resistance:.6f}</i>"

        send_signal(pair, "SCALPING", "SELL", price, tp1, tp2, sl,
                    strength, "5m", 15, sources, extra)


# =====================
# INTRADAY 1h
# =====================
def check_intraday(client, pair, price, fg, ob_ratio, funding, trending, market_data):
    closes, highs, lows, volumes = get_candles(client, pair, "1h", 100)
    if closes is None:
        return

    rsi            = calc_rsi(closes)
    stoch_rsi      = calc_stoch_rsi(closes)
    macd, msig     = calc_macd(closes)
    bb_low, bb_mid, bb_high = calc_bb(closes)
    atr            = calc_atr(closes, highs, lows)
    ema20          = calc_ema(closes, 20)
    ema50          = calc_ema(closes, 50)
    support, resistance = calc_support_resistance(highs, lows, closes)
    divergence     = calc_rsi_divergence(closes, highs, lows)
    poc            = calc_volume_profile(closes, volumes)

    symbol      = pair.replace("_USDT", "")
    is_trending = symbol in trending
    btc_dom     = market_data.get("btc_dominance", 50) if market_data else 50

    sources = "Gate.io + F&G + CoinGecko + StochRSI + Divergence + S/R + POC"

    # BUY
    if rsi < 42 and macd > msig and price <= bb_mid:
        strength = 2
        if rsi < 35:              strength += 1
        if stoch_rsi < 0.25:      strength += 1
        if price <= bb_low:       strength += 1
        if fg < 30:               strength += 1
        if ob_ratio > 1.1:        strength += 1
        if is_trending:           strength += 1
        if ema20 > ema50:         strength += 1
        if divergence == "BULLISH": strength += 1
        if poc and price <= poc:  strength += 1
        if price <= support * 1.03: strength += 1
        if funding and funding < -0.001: strength += 1
        strength = min(strength, 5)

        tp1 = min(price * 1.05, resistance * 0.99)
        tp2 = min(price * 1.10, resistance * 1.05)
        sl  = max(price - (atr * 2.5), price * 0.94)

        extra = ""
        if divergence == "BULLISH":
            extra += "\n🔀 <i>Bullish RSI Divergence!</i>"
        if poc and price <= poc:
            extra += f"\n📊 <i>Harga di bawah POC ${poc:.6f} — zona akumulasi</i>"
        if is_trending:
            extra += "\n🔥 <i>Trending di CoinGecko!</i>"
        if fg < 20:
            extra += f"\n😱 <i>Extreme Fear {fg} — zona akumulasi institusi</i>"

        send_signal(pair, "INTRADAY", "BUY", price, tp1, tp2, sl,
                    strength, "1h", 120, sources, extra)

    # SELL
    elif rsi > 60 and macd < msig and price >= bb_mid:
        strength = 2
        if rsi > 65:              strength += 1
        if stoch_rsi > 0.75:      strength += 1
        if price >= bb_high:      strength += 1
        if fg > 60:               strength += 1
        if ob_ratio < 0.9:        strength += 1
        if divergence == "BEARISH": strength += 1
        if poc and price >= poc:  strength += 1
        if price >= resistance * 0.97: strength += 1
        if funding and funding > 0.001: strength += 1
        strength = min(strength, 5)

        tp1 = max(price * 0.95, support * 1.01)
        tp2 = max(price * 0.90, support * 0.95)
        sl  = min(price + (atr * 2.5), price * 1.06)

        extra = ""
        if divergence == "BEARISH":
            extra += "\n🔀 <i>Bearish RSI Divergence!</i>"
        if poc and price >= poc:
            extra += f"\n📊 <i>Harga di atas POC ${poc:.6f} — zona distribusi</i>"

        send_signal(pair, "INTRADAY", "SELL", price, tp1, tp2, sl,
                    strength, "1h", 120, sources, extra)


# =====================
# SWING 4h
# =====================
def check_swing(client, pair, price, fg, ob_ratio, funding, trending, market_data):
    closes, highs, lows, volumes = get_candles(client, pair, "4h", 210)
    if closes is None:
        return

    rsi        = calc_rsi(closes)
    stoch_rsi  = calc_stoch_rsi(closes)
    ema50      = calc_ema(closes, 50)
    ema200     = calc_ema(closes, 200)
    atr        = calc_atr(closes, highs, lows)
    macd, msig = calc_macd(closes)
    vol_trend  = np.mean(volumes[-5:]) > np.mean(volumes[-20:]) * 1.2
    support, resistance = calc_support_resistance(highs, lows, closes, lookback=50)
    divergence = calc_rsi_divergence(closes, highs, lows)
    poc        = calc_volume_profile(closes, volumes, bins=20)

    symbol      = pair.replace("_USDT", "")
    is_trending = symbol in trending

    sources = "Gate.io + F&G + CoinGecko + EMA50/200 + Divergence + POC"

    # BUY
    if rsi < 50 and rsi > 20 and macd > msig:
        strength = 2
        if ema50 > ema200:        strength += 1
        if rsi < 35:              strength += 1
        if stoch_rsi < 0.3:       strength += 1
        if fg < 25:               strength += 1
        if vol_trend:             strength += 1
        if is_trending:           strength += 1
        if ob_ratio > 1.1:        strength += 1
        if divergence == "BULLISH": strength += 1
        if poc and price <= poc:  strength += 1
        if price <= support * 1.05: strength += 1
        if funding and funding < -0.002: strength += 1
        strength = min(strength, 5)

        tp1 = min(price * 1.10, resistance * 0.98)
        tp2 = min(price * 1.20, resistance * 1.10)
        sl  = max(price - (atr * 3.0), price * 0.88)

        extra = ""
        if ema50 > ema200:
            extra += "\n✨ <i>Golden Cross EMA 50/200 aktif</i>"
        if divergence == "BULLISH":
            extra += "\n🔀 <i>Bullish RSI Divergence!</i>"
        if fg < 20:
            extra += f"\n😱 <i>Extreme Fear {fg} — historis zona bottom</i>"
        if poc:
            extra += f"\n📊 <i>POC: ${poc:.6f}</i>"
        if is_trending:
            extra += "\n🔥 <i>Trending di CoinGecko!</i>"

        send_signal(pair, "SWING", "BUY", price, tp1, tp2, sl,
                    strength, "4h", 720, sources, extra)

    # SELL
    elif rsi > 60 and rsi < 80 and macd < msig:
        strength = 2
        if ema50 < ema200:        strength += 1
        if rsi > 65:              strength += 1
        if stoch_rsi > 0.7:       strength += 1
        if fg > 60:               strength += 1
        if vol_trend:             strength += 1
        if divergence == "BEARISH": strength += 1
        if funding and funding > 0.002: strength += 1
        strength = min(strength, 5)

        tp1 = max(price * 0.90, support * 1.02)
        tp2 = max(price * 0.80, support * 0.90)
        sl  = min(price + (atr * 3.0), price * 1.12)

        extra = ""
        if ema50 < ema200:
            extra += "\n💀 <i>Death Cross EMA 50/200 aktif</i>"
        if divergence == "BEARISH":
            extra += "\n🔀 <i>Bearish RSI Divergence!</i>"

        send_signal(pair, "SWING", "SELL", price, tp1, tp2, sl,
                    strength, "4h", 720, sources, extra)


# =====================
# MOONSHOT
# =====================
def check_moonshot(client, pair, price, change_24h, trending):
    closes, highs, lows, volumes = get_candles(client, pair, "1h", 48)
    if closes is None:
        return

    vol_avg   = np.mean(volumes[:-6])
    vol_now   = np.mean(volumes[-6:])
    vol_ratio = vol_now / vol_avg if vol_avg > 0 else 0
    rsi       = calc_rsi(closes)
    atr       = calc_atr(closes, highs, lows)
    support, resistance = calc_support_resistance(highs, lows, closes)

    symbol      = pair.replace("_USDT", "")
    is_trending = symbol in trending

    if vol_ratio > 4.0 and 25 < rsi < 70 and change_24h > 3.0:
        strength = 2
        if vol_ratio > 7.0:   strength += 1
        if is_trending:       strength += 1
        if change_24h > 8.0:  strength += 1
        if rsi < 55:          strength += 1
        if price > resistance: strength += 1  # breakout konfirmasi
        strength = min(strength, 5)

        tp1 = price * 1.20
        tp2 = price * 1.50
        sl  = max(price * 0.88, support * 0.98)

        extra = (
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔥 Vol Spike:  <b>{vol_ratio:.1f}x</b> normal\n"
            f"📊 Change 24h: <b>+{change_24h:.1f}%</b>\n"
        )
        if price > resistance:
            extra += f"🚀 <i>Breakout Resistance ${resistance:.6f}!</i>\n"
        if is_trending:
            extra += "🌟 <i>Trending di CoinGecko!</i>\n"
        extra += "⚠️ <i>High Risk — Max 5% modal</i>"

        send_signal(pair, "MOONSHOT", "BUY", price, tp1, tp2, sl,
                    strength, "1h", 360, "Gate.io + CoinGecko + S/R", extra)


# =====================
# MARKET SUMMARY
# =====================
def send_market_summary(fg, fg_label, market_data, trending):
    now = datetime.now(WIB)
    if not market_data:
        return

    btc_dom    = market_data.get("btc_dominance", 0)
    mcap_chg   = market_data.get("market_cap_change_24h", 0)
    mcap_emoji = "📈" if mcap_chg > 0 else "📉"
    trend_str  = ", ".join(trending[:5]) if trending else "N/A"

    if fg < 20:
        saran = "😱 <b>Extreme Fear</b> — Zona akumulasi, potensi reversal"
    elif fg < 40:
        saran = "😨 <b>Fear</b> — Hati-hati, ada peluang beli selektif"
    elif fg < 60:
        saran = "😐 <b>Neutral</b> — Tunggu konfirmasi arah"
    elif fg < 80:
        saran = "😊 <b>Greed</b> — Mulai waspada, kurangi posisi"
    else:
        saran = "🤑 <b>Extreme Greed</b> — Bahaya, potensi koreksi besar"

    alt_note = "🟢 Alt Season" if btc_dom < 48 else "🔴 BTC Season"

    msg = (
        f"📊 <b>MARKET SUMMARY — {now.strftime('%H:%M WIB')}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Fear & Greed:  <b>{fg} — {fg_label}</b>\n"
        f"BTC Dominance: <b>{btc_dom:.1f}%</b> {alt_note}\n"
        f"Market Cap:    {mcap_emoji} <b>{mcap_chg:+.2f}%</b> (24h)\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔥 Trending: <b>{trend_str}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💡 {saran}"
    )
    tg(msg)


# =====================
# MAIN
# =====================
def run():
    client = get_client()
    print("=== FINAL SIGNAL SCAN ===")

    fg, fg_label = get_fear_greed()
    market_data  = get_coingecko_market()
    trending     = get_coingecko_trending()

    print(f"BTC Dom: {market_data.get('btc_dominance', 'N/A') if market_data else 'N/A'}%")
    print(f"Trending: {trending[:3]}")

    send_market_summary(fg, fg_label, market_data, trending)

    try:
        btc    = client.list_tickers(currency_pair="BTC_USDT")[0]
        change = float(btc.change_percentage or 0)
        print(f"BTC 24h: {change:.2f}%")
        if change < -12:
            print("❌ Market crash ekstrem, skip")
            tg("⚠️ <b>Market Crash Alert</b>\nBTC turun >12%, scan signal ditunda.")
            return
    except Exception as e:
        print(f"⚠️ BTC check: {e}")

    tickers = client.list_tickers()
    total   = 0

    for t in tickers:
        pair       = t.currency_pair
        if not is_valid(pair):
            continue

        price      = float(t.last or 0)
        vol_24h    = float(t.quote_volume or 0)
        change_24h = float(t.change_percentage or 0)

        if price <= 0 or vol_24h < MIN_VOLUME:
            continue

        try:
            ob_ratio = get_order_book_pressure(client, pair)
            funding  = get_funding_rate(pair)

            check_scalping(client, pair, price, fg, ob_ratio, funding)
            check_intraday(client, pair, price, fg, ob_ratio, funding, trending, market_data)
            check_swing(client, pair, price, fg, ob_ratio, funding, trending, market_data)
            check_moonshot(client, pair, price, change_24h, trending)

            total += 1

        except Exception as e:
            print(f"⚠️ {pair}: {e}")
            continue

    print(f"=== DONE | {total} pairs scanned ===")


if __name__ == "__main__":
    run()
