import os
import json
import time
import urllib.request
import urllib.parse
import urllib.error
import gate_api
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from supabase import create_client

print("🚀 SIGNAL BOT — INSTITUTIONAL LEVEL")

API_KEY      = os.environ.get("GATE_API_KEY")
SECRET_KEY   = os.environ.get("GATE_SECRET_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TG_TOKEN     = os.environ.get("TELEGRAM_TOKEN")
TG_CHAT_ID   = os.environ.get("CHAT_ID")

MIN_VOLUME   = 500000
MIN_STRENGTH = 4

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


# =====================
# HTTP HELPER
# =====================
def http_get(url, headers=None, timeout=10):
    try:
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"⚠️ HTTP error {url[:50]}: {e}")
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
# DATA SOURCE 1
# FEAR & GREED INDEX
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


# =====================
# DATA SOURCE 2
# COINGECKO — Market data
# =====================
def get_coingecko_market():
    """Ambil BTC dominance dan total market cap."""
    data = http_get("https://api.coingecko.com/api/v3/global")
    if not data:
        return None
    try:
        d = data["data"]
        return {
            "btc_dominance":   float(d.get("market_cap_percentage", {}).get("btc", 50)),
            "eth_dominance":   float(d.get("market_cap_percentage", {}).get("eth", 15)),
            "total_market_cap": float(d.get("total_market_cap", {}).get("usd", 0)),
            "market_cap_change_24h": float(d.get("market_cap_change_percentage_24h_usd", 0))
        }
    except Exception:
        return None


def get_coingecko_trending():
    """Ambil top trending coins di CoinGecko."""
    data = http_get("https://api.coingecko.com/api/v3/search/trending")
    if not data:
        return []
    try:
        trending = []
        for item in data.get("coins", []):
            coin = item.get("item", {})
            trending.append(coin.get("symbol", "").upper())
        return trending
    except Exception:
        return []


def get_coingecko_coin(coin_id):
    """Ambil data fundamental koin dari CoinGecko."""
    url  = f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&community_data=false&developer_data=false"
    data = http_get(url)
    if not data:
        return None
    try:
        md = data.get("market_data", {})
        return {
            "market_cap_rank": data.get("market_cap_rank", 999),
            "market_cap":      float(md.get("market_cap", {}).get("usd", 0)),
            "ath_change_pct":  float(md.get("ath_change_percentage", {}).get("usd", 0)),
            "atl_change_pct":  float(md.get("atl_change_percentage", {}).get("usd", 0)),
            "price_change_7d": float(md.get("price_change_percentage_7d", 0)),
            "price_change_30d": float(md.get("price_change_percentage_30d", 0)),
        }
    except Exception:
        return None


# Mapping pair ke CoinGecko ID untuk top coins
GECKO_ID_MAP = {
    "BTC_USDT": "bitcoin", "ETH_USDT": "ethereum",
    "SOL_USDT": "solana", "BNB_USDT": "binancecoin",
    "XRP_USDT": "ripple", "ADA_USDT": "cardano",
    "DOGE_USDT": "dogecoin", "AVAX_USDT": "avalanche-2",
    "DOT_USDT": "polkadot", "LINK_USDT": "chainlink",
    "UNI_USDT": "uniswap", "LTC_USDT": "litecoin",
    "NEAR_USDT": "near", "ALGO_USDT": "algorand",
    "PEPE_USDT": "pepe", "SUI_USDT": "sui",
    "TRX_USDT": "tron", "ENA_USDT": "ethena",
    "AAVE_USDT": "aave", "FET_USDT": "fetch-ai"
}


# =====================
# DATA SOURCE 3
# GATE.IO FUNDING RATE
# =====================
def get_funding_rate(pair):
    """Ambil funding rate dari Gate.io Futures."""
    try:
        contract = pair.replace("_USDT", "_USDT")
        url  = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{contract}"
        data = http_get(url)
        if data and "funding_rate" in data:
            return float(data["funding_rate"])
    except Exception:
        pass
    return None


# =====================
# DATA SOURCE 4
# GATE.IO ORDER BOOK
# =====================
def get_order_book_pressure(client, pair):
    """
    Cek tekanan buy vs sell dari order book.
    Return: ratio > 1 = tekanan buy lebih kuat
    """
    try:
        ob = client.list_order_book(currency_pair=pair, limit=20)
        bid_vol = sum(float(b[1]) for b in ob.bids)
        ask_vol = sum(float(a[1]) for a in ob.asks)
        if ask_vol == 0:
            return 1.0
        return bid_vol / ask_vol
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


# =====================
# SUPABASE
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


# =====================
# SIGNAL SENDER
# =====================
def stars(n):
    return "⭐" * n + "☆" * (5 - n)


def fmt_time(dt):
    return dt.strftime("%H:%M WIB")


def send_signal(pair, signal_type, side, entry, tp1, tp2, sl,
                strength, timeframe, valid_minutes, data_sources="", extra=""):
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
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Strength: {stars(strength)}\n"
        f"TF:       {timeframe}\n"
        f"Data:     {data_sources}"
    )

    if extra:
        msg += f"\n{extra}"

    tg(msg)
    save_signal(pair, signal_type, side, entry, tp1, tp2, sl, strength, timeframe, valid_until)
    print(f"✅ {signal_type} {side} → {pair} | Strength:{strength}")


# =====================
# IS VALID PAIR
# =====================
def is_valid(pair):
    if not pair.endswith("_USDT"):
        return False
    for b in BLACKLIST:
        if b in pair:
            return False
    return True


# =====================
# SIGNAL: SCALPING 5m
# Target: 5-8%
# =====================
def check_scalping(client, pair, price, vol_24h, fg, ob_ratio, funding):
    closes, highs, lows, volumes = get_candles(client, pair, "5m", 80)
    if closes is None:
        return

    rsi       = calc_rsi(closes)
    ema9      = calc_ema(closes, 9)
    ema21     = calc_ema(closes, 21)
    atr       = calc_atr(closes, highs, lows)
    vol_avg   = np.mean(volumes[-20:])
    vol_ratio = volumes[-1] / vol_avg if vol_avg > 0 else 0
    vol_spike = vol_ratio > 2.0

    sources = "Gate.io + F&G + OrderBook"
    if funding is not None:
        sources += " + FundingRate"

    # BUY
    if rsi < 32 and ema9 > ema21 and vol_spike and ob_ratio > 1.2:
        strength = 3
        if rsi < 25:            strength += 1
        if vol_ratio > 3.0:     strength += 1
        if fg < 30:             strength += 1
        if ob_ratio > 1.5:      strength += 1
        if funding and funding < -0.001: strength += 1
        strength = min(strength, 5)

        tp1 = price * 1.05
        tp2 = price * 1.08
        sl  = price - (atr * 2.0)

        send_signal(pair, "SCALPING", "BUY", price, tp1, tp2, sl,
                    strength, "5m", 15, sources)

    # SELL
    elif rsi > 72 and ema9 < ema21 and vol_spike and ob_ratio < 0.8:
        strength = 3
        if rsi > 80:            strength += 1
        if vol_ratio > 3.0:     strength += 1
        if fg > 70:             strength += 1
        if ob_ratio < 0.6:      strength += 1
        if funding and funding > 0.001: strength += 1
        strength = min(strength, 5)

        tp1 = price * 0.95
        tp2 = price * 0.92
        sl  = price + (atr * 2.0)

        send_signal(pair, "SCALPING", "SELL", price, tp1, tp2, sl,
                    strength, "5m", 15, sources)


# =====================
# SIGNAL: INTRADAY 1h
# Target: 5-10%
# =====================
def check_intraday(client, pair, price, vol_24h, fg, ob_ratio,
                   funding, gecko_data, trending, market_data):
    closes, highs, lows, volumes = get_candles(client, pair, "1h", 100)
    if closes is None:
        return

    rsi          = calc_rsi(closes)
    macd, msig   = calc_macd(closes)
    bb_low, bb_mid, bb_high = calc_bb(closes)
    atr          = calc_atr(closes, highs, lows)
    ema20        = calc_ema(closes, 20)
    ema50        = calc_ema(closes, 50)

    # Cek apakah pair ada di trending CoinGecko
    symbol     = pair.replace("_USDT", "")
    is_trending = symbol in trending

    # Cek BTC dominance — kalau dominance turun, altcoin biasanya naik
    btc_dom     = market_data.get("btc_dominance", 50) if market_data else 50
    alt_season  = btc_dom < 50

    sources = "Gate.io + F&G + OrderBook + CoinGecko"

    # BUY
    if (rsi < 38 and macd > msig and price <= bb_mid
            and ema20 > ema50 and fg < 50 and ob_ratio > 1.1):
        strength = 2
        if rsi < 32:            strength += 1
        if price <= bb_low:     strength += 1
        if fg < 30:             strength += 1
        if ob_ratio > 1.3:      strength += 1
        if is_trending:         strength += 1
        if alt_season:          strength += 1
        if funding and funding < -0.001: strength += 1
        if gecko_data and gecko_data.get("ath_change_pct", 0) < -70: strength += 1
        strength = min(strength, 5)

        tp1 = price * 1.05
        tp2 = price * 1.10
        sl  = price - (atr * 2.5)

        extra = ""
        if is_trending:
            extra += f"\n🔥 <i>Trending di CoinGecko!</i>"
        if gecko_data and gecko_data.get("ath_change_pct", 0) < -70:
            extra += f"\n📉 <i>Harga {abs(gecko_data['ath_change_pct']):.0f}% dari ATH — zona akumulasi</i>"

        send_signal(pair, "INTRADAY", "BUY", price, tp1, tp2, sl,
                    strength, "1h", 120, sources, extra)

    # SELL
    elif (rsi > 62 and macd < msig and price >= bb_mid
              and ema20 < ema50 and fg > 50 and ob_ratio < 0.9):
        strength = 2
        if rsi > 68:            strength += 1
        if price >= bb_high:    strength += 1
        if fg > 70:             strength += 1
        if ob_ratio < 0.7:      strength += 1
        if funding and funding > 0.001: strength += 1
        strength = min(strength, 5)

        tp1 = price * 0.95
        tp2 = price * 0.90
        sl  = price + (atr * 2.5)

        send_signal(pair, "INTRADAY", "SELL", price, tp1, tp2, sl,
                    strength, "1h", 120, sources)


# =====================
# SIGNAL: SWING 4h
# Target: 10-20%
# =====================
def check_swing(client, pair, price, vol_24h, fg, ob_ratio,
                funding, gecko_data, trending, market_data):
    closes, highs, lows, volumes = get_candles(client, pair, "4h", 210)
    if closes is None:
        return

    rsi        = calc_rsi(closes)
    ema50      = calc_ema(closes, 50)
    ema200     = calc_ema(closes, 200)
    atr        = calc_atr(closes, highs, lows)
    macd, msig = calc_macd(closes)
    vol_trend  = np.mean(volumes[-5:]) > np.mean(volumes[-20:]) * 1.3

    symbol      = pair.replace("_USDT", "")
    is_trending = symbol in trending
    btc_dom     = market_data.get("btc_dominance", 50) if market_data else 50
    alt_season  = btc_dom < 48

    sources = "Gate.io + F&G + CoinGecko + FundingRate"

    # BUY — Golden cross zone
    if (ema50 > ema200 and rsi < 45 and rsi > 25
            and vol_trend and macd > msig and fg < 55):
        strength = 2
        if rsi < 35:            strength += 1
        if fg < 30:             strength += 1
        if vol_trend:           strength += 1
        if alt_season:          strength += 1
        if is_trending:         strength += 1
        if ob_ratio > 1.2:      strength += 1
        if funding and funding < -0.002: strength += 1
        if gecko_data:
            if gecko_data.get("price_change_7d", 0) > 0: strength += 1
            if gecko_data.get("ath_change_pct", 0) < -60: strength += 1
        strength = min(strength, 5)

        tp1 = price * 1.10
        tp2 = price * 1.20
        sl  = price - (atr * 3.0)

        extra = ""
        if gecko_data:
            rank = gecko_data.get("market_cap_rank", 0)
            if rank:
                extra += f"\n📊 <i>Market Cap Rank: #{rank}</i>"
        if is_trending:
            extra += f"\n🔥 <i>Trending di CoinGecko!</i>"

        send_signal(pair, "SWING", "BUY", price, tp1, tp2, sl,
                    strength, "4h", 720, sources, extra)

    # SELL — Death cross zone
    elif (ema50 < ema200 and rsi > 55 and rsi < 75
              and vol_trend and macd < msig and fg > 45):
        strength = 2
        if rsi > 65:            strength += 1
        if fg > 70:             strength += 1
        if vol_trend:           strength += 1
        if funding and funding > 0.002: strength += 1
        strength = min(strength, 5)

        tp1 = price * 0.90
        tp2 = price * 0.80
        sl  = price + (atr * 3.0)

        send_signal(pair, "SWING", "SELL", price, tp1, tp2, sl,
                    strength, "4h", 720, sources)


# =====================
# SIGNAL: MOONSHOT
# Target: 20-100%+
# =====================
def check_moonshot(client, pair, price, vol_24h, change_24h,
                   trending, gecko_data):
    closes, highs, lows, volumes = get_candles(client, pair, "1h", 48)
    if closes is None:
        return

    vol_avg    = np.mean(volumes[:-6])
    vol_now    = np.mean(volumes[-6:])
    vol_ratio  = vol_now / vol_avg if vol_avg > 0 else 0

    rsi        = calc_rsi(closes)
    atr        = calc_atr(closes, highs, lows)
    price_avg  = np.mean(closes[-48:])
    price_low  = price < price_avg * 0.95

    symbol      = pair.replace("_USDT", "")
    is_trending = symbol in trending

    vol_explode  = vol_ratio > 5.0
    rsi_recover  = 25 < rsi < 65
    change_pos   = change_24h > 5.0

    if vol_explode and rsi_recover and change_pos:
        strength = 3
        if vol_ratio > 8.0:     strength += 1
        if price_low:           strength += 1
        if is_trending:         strength += 1
        if gecko_data:
            rank = gecko_data.get("market_cap_rank", 999)
            if rank and rank > 200: strength += 1  # low cap lebih potensial
        strength = min(strength, 5)

        tp1 = price * 1.20
        tp2 = price * 1.50
        sl  = price * 0.88

        extra = (
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔥 Vol Spike:  <b>{vol_ratio:.1f}x</b> dari normal\n"
            f"📊 Change 24h: <b>+{change_24h:.1f}%</b>\n"
        )
        if is_trending:
            extra += f"🌟 Trending:   <b>Ya (CoinGecko)</b>\n"
        if gecko_data:
            rank = gecko_data.get("market_cap_rank", 0)
            if rank:
                extra += f"📈 MCap Rank:  <b>#{rank}</b>\n"
        extra += f"⚠️ <i>High Risk — Gunakan max 5% modal</i>"

        sources = "Gate.io + CoinGecko Trending"
        send_signal(pair, "MOONSHOT", "BUY", price, tp1, tp2, sl,
                    strength, "1h", 360, sources, extra)


# =====================
# MARKET SUMMARY
# =====================
def send_market_summary(fg, fg_label, market_data, trending):
    now = datetime.now(WIB)

    if not market_data:
        return

    btc_dom   = market_data.get("btc_dominance", 0)
    mcap_chg  = market_data.get("market_cap_change_24h", 0)
    mcap_chg_emoji = "📈" if mcap_chg > 0 else "📉"

    trend_str = ", ".join(trending[:5]) if trending else "N/A"

    msg = (
        f"📊 <b>MARKET SUMMARY — {now.strftime('%H:%M WIB')}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Fear & Greed:  <b>{fg} — {fg_label}</b>\n"
        f"BTC Dominance: <b>{btc_dom:.1f}%</b>\n"
        f"Market Cap:    {mcap_chg_emoji} <b>{mcap_chg:+.2f}%</b> (24h)\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔥 Trending: <b>{trend_str}</b>"
    )
    tg(msg)


# =====================
# MARKET FILTER
# =====================
def market_ok(client):
    try:
        btc    = client.list_tickers(currency_pair="BTC_USDT")[0]
        change = float(btc.change_percentage or 0)
        print(f"BTC 24h: {change:.2f}%")
        return change > -10
    except Exception as e:
        print(f"⚠️ Market check error: {e}")
        return False


# =====================
# MAIN
# =====================
def run():
    client = get_client()
    print("=== INSTITUTIONAL SIGNAL SCAN ===")

    if not market_ok(client):
        print("❌ Market crash, skip")
        tg("⚠️ <b>Market Alert</b>\nBTC turun >10%, scan signal ditunda.")
        return

    # Ambil semua data eksternal dulu
    print("📡 Fetching external data...")
    fg, fg_label  = get_fear_greed()
    market_data   = get_coingecko_market()
    trending      = get_coingecko_trending()

    print(f"📊 BTC Dom: {market_data.get('btc_dominance', 'N/A') if market_data else 'N/A'}%")
    print(f"🔥 Trending: {trending[:5]}")

    # Kirim market summary setiap run
    send_market_summary(fg, fg_label, market_data, trending)

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
            # Data per pair
            ob_ratio   = get_order_book_pressure(client, pair)
            funding    = get_funding_rate(pair)
            gecko_data = get_coingecko_coin(GECKO_ID_MAP[pair]) if pair in GECKO_ID_MAP else None

            check_scalping(client, pair, price, vol_24h, fg, ob_ratio, funding)
            check_intraday(client, pair, price, vol_24h, fg, ob_ratio, funding, gecko_data, trending, market_data)
            check_swing(client, pair, price, vol_24h, fg, ob_ratio, funding, gecko_data, trending, market_data)
            check_moonshot(client, pair, price, vol_24h, change_24h, trending, gecko_data)

            total += 1

        except Exception as e:
            print(f"⚠️ Error {pair}: {e}")
            continue

    print(f"=== SCAN DONE | {total} pairs scanned ===")


if __name__ == "__main__":
    run()
