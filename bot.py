import os
import json
import time
import urllib.request
import gate_api
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from supabase import create_client

print("🚀 SIGNAL BOT — FINAL EDITION v2")

# ═══════════════════════════════════════════════
#  ENV
# ═══════════════════════════════════════════════
API_KEY      = os.environ.get("GATE_API_KEY")
SECRET_KEY   = os.environ.get("GATE_SECRET_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TG_TOKEN     = os.environ.get("TELEGRAM_TOKEN")
TG_CHAT_ID   = os.environ.get("CHAT_ID")

_missing = [k for k, v in {
    "SUPABASE_URL": SUPABASE_URL, "SUPABASE_KEY": SUPABASE_KEY,
    "TELEGRAM_TOKEN": TG_TOKEN,   "CHAT_ID": TG_CHAT_ID,
}.items() if not v]
if _missing:
    raise EnvironmentError(f"ENV belum diset: {', '.join(_missing)}")

# ═══════════════════════════════════════════════
#  KONFIGURASI
# ═══════════════════════════════════════════════
MIN_VOLUME   = 300_000
MIN_STRENGTH = 3
SUMMARY_HOUR_WIB = 8

FUTURES_PAIRS = {
    "BTC_USDT","ETH_USDT","SOL_USDT","BNB_USDT","XRP_USDT",
    "DOGE_USDT","ADA_USDT","AVAX_USDT","DOT_USDT","MATIC_USDT",
    "LINK_USDT","UNI_USDT","ATOM_USDT","LTC_USDT","NEAR_USDT",
    "ARB_USDT","OP_USDT","APT_USDT","SUI_USDT","TRX_USDT",
}

BLACKLIST = {
    "3S","3L","5S","5L",
    "TUSD","USDC","BUSD","DAI","FDUSD","USD1",
    "USDP","USDD","USDJ","ZUSD","GUSD","CUSD","SUSD",
    "STBL","FRAX","LUSD","USDN","STABLE","BARD"
}

WIB      = timezone(timedelta(hours=7))
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ═══════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════
def tg(msg):
    try:
        if not TG_TOKEN or not TG_CHAT_ID:
            return
        url  = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        body = json.dumps({
            "chat_id": TG_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True
        }).encode()
        req = urllib.request.Request(url, data=body,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        time.sleep(0.5)
    except Exception as e:
        print(f"⚠️ Telegram error: {e}")


def http_get(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"⚠️ HTTP error {url[:60]}: {e}")
        return None


# ═══════════════════════════════════════════════
#  GATE.IO CLIENT
# ═══════════════════════════════════════════════
def get_client():
    cfg = gate_api.Configuration(
        host="https://api.gateio.ws/api/v4",
        key=API_KEY, secret=SECRET_KEY
    )
    return gate_api.SpotApi(gate_api.ApiClient(cfg))


# ═══════════════════════════════════════════════
#  LAYER 1 — FEAR & GREED
# ═══════════════════════════════════════════════
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


# ═══════════════════════════════════════════════
#  LAYER 2 — COINGECKO
# ═══════════════════════════════════════════════
def get_coingecko_market():
    data = http_get("https://api.coingecko.com/api/v3/global")
    if not data:
        return None
    try:
        d = data["data"]
        return {
            "btc_dominance":         float(d.get("market_cap_percentage", {}).get("btc", 50)),
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


# ═══════════════════════════════════════════════
#  LAYER 3 — COINGLASS: LIQUIDATION + OI
# ═══════════════════════════════════════════════
def get_liquidation_data(symbol):
    default = {"liq_short_above": 0, "liq_long_below": 0, "liq_bias": "NEUTRAL"}
    try:
        url  = f"https://open-api.coinglass.com/public/v2/liquidation_map?symbol={symbol}&interval=12h"
        data = http_get(url, timeout=8)
        if not data or data.get("code") != "0":
            return default
        items  = data.get("data", {})
        shorts = items.get("shorts", [])
        longs  = items.get("longs",  [])
        liq_short = sum(float(s.get("liquidationAmount", 0)) for s in shorts[:10])
        liq_long  = sum(float(l.get("liquidationAmount", 0)) for l in longs[:10])
        if liq_short > liq_long * 1.5:
            bias = "BUY"
        elif liq_long > liq_short * 1.5:
            bias = "SELL"
        else:
            bias = "NEUTRAL"
        return {
            "liq_short_above": round(liq_short / 1_000_000, 2),
            "liq_long_below":  round(liq_long  / 1_000_000, 2),
            "liq_bias":        bias
        }
    except Exception as e:
        print(f"⚠️ Coinglass liq [{symbol}]: {e}")
        return default


def get_open_interest(symbol):
    default = {"oi_usd": 0, "oi_change_pct": 0}
    try:
        url  = f"https://open-api.coinglass.com/public/v2/open_interest?symbol={symbol}"
        data = http_get(url, timeout=8)
        if not data or data.get("code") != "0":
            return default
        items    = data.get("data", [])
        total_oi = sum(float(e.get("openInterest", 0)) for e in items)
        oi_chg   = float(items[0].get("openInterestChangePercent24h", 0)) if items else 0
        return {"oi_usd": round(total_oi / 1_000_000, 2), "oi_change_pct": round(oi_chg, 2)}
    except Exception as e:
        print(f"⚠️ Coinglass OI [{symbol}]: {e}")
        return default


def interpret_oi(oi_data, price_change):
    oi_chg = oi_data.get("oi_change_pct", 0)
    if oi_chg > 5 and price_change > 1:   return "STRONG_BUY"
    if oi_chg > 5 and price_change < -1:  return "SQUEEZE"
    if oi_chg < -5 and price_change > 1:  return "WEAK_RALLY"
    if oi_chg < -5 and price_change < -1: return "STRONG_SELL"
    return "NEUTRAL"


# ═══════════════════════════════════════════════
#  LAYER 4 — GATE.IO FUTURES
# ═══════════════════════════════════════════════
def get_funding_rate(pair):
    if pair not in FUTURES_PAIRS:
        return None
    try:
        data = http_get(f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{pair}", timeout=8)
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


# ═══════════════════════════════════════════════
#  CANDLE DATA
# ═══════════════════════════════════════════════
def get_candles(client, pair, interval, limit):
    try:
        candles = client.list_candlesticks(
            currency_pair=pair, interval=interval, limit=limit)
        if not candles or len(candles) < 20:
            return None, None, None, None
        closes  = np.array([float(c[2]) for c in candles])
        highs   = np.array([float(c[3]) for c in candles])
        lows    = np.array([float(c[4]) for c in candles])
        volumes = np.array([float(c[5]) for c in candles])
        return closes, highs, lows, volumes
    except Exception as e:
        print(f"⚠️ get_candles [{pair}|{interval}]: {e}")
        return None, None, None, None


# ═══════════════════════════════════════════════
#  INDIKATOR
# ═══════════════════════════════════════════════
def calc_rsi(closes, period=14):
    s     = pd.Series(closes)
    delta = s.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / (loss + 1e-9)
    return float((100 - 100 / (1 + rs)).iloc[-1])


def calc_ema(closes, period):
    return float(pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1])


def calc_macd(closes):
    s      = pd.Series(closes)
    ema12  = s.ewm(span=12, adjust=False).mean()
    ema26  = s.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return float(macd.iloc[-1]), float(signal.iloc[-1])


def calc_bb(closes, period=20):
    s   = pd.Series(closes)
    mid = s.rolling(period).mean().iloc[-1]
    std = s.rolling(period).std().iloc[-1]
    return float(mid - 2*std), float(mid), float(mid + 2*std)


def calc_atr(closes, highs, lows, period=14):
    tr_list = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
               for i in range(1, len(closes))]
    return float(pd.Series(tr_list).rolling(period).mean().iloc[-1])


def calc_stoch_rsi(closes, period=14):
    s         = pd.Series(closes)
    delta     = s.diff()
    gain      = delta.clip(lower=0).rolling(period).mean()
    loss      = (-delta.clip(upper=0)).rolling(period).mean()
    rsi       = 100 - (100 / (1 + gain / (loss + 1e-9)))
    rsi_min   = rsi.rolling(period).min()
    rsi_max   = rsi.rolling(period).max()
    stoch_rsi = (rsi - rsi_min) / (rsi_max - rsi_min + 1e-9)
    return float(stoch_rsi.iloc[-1])


def calc_vwap(closes, highs, lows, volumes):
    typical_price = (highs + lows + closes) / 3
    vwap = np.cumsum(typical_price * volumes) / (np.cumsum(volumes) + 1e-9)
    return float(vwap[-1])


def calc_ichimoku(closes, highs, lows):
    def hl(h, l, p):
        if len(h) < p: return None, None
        return float(np.max(h[-p:])), float(np.min(l[-p:]))

    th, tl = hl(highs, lows, 9)
    kh, kl = hl(highs, lows, 26)
    if None in (th, tl, kh, kl):
        return {"valid": False}

    tenkan   = (th + tl) / 2
    kijun    = (kh + kl) / 2
    senkou_a = (tenkan + kijun) / 2
    sh, sl2  = hl(highs, lows, 52)
    senkou_b = (sh + sl2) / 2 if sh else senkou_a

    price      = float(closes[-1])
    cloud_top  = max(senkou_a, senkou_b)
    cloud_bot  = min(senkou_a, senkou_b)

    return {
        "valid":       True,
        "above_cloud": price > cloud_top,
        "below_cloud": price < cloud_bot,
        "in_cloud":    cloud_bot <= price <= cloud_top,
        "tk_bull":     tenkan > kijun,
        "tk_bear":     tenkan < kijun,
        "cloud_top":   cloud_top,
        "cloud_bot":   cloud_bot,
    }


def calc_support_resistance(highs, lows, closes, lookback=20):
    return (float(np.percentile(lows[-lookback:],  15)),
            float(np.percentile(highs[-lookback:], 85)))


def calc_rsi_divergence(closes, highs, lows, period=14, lookback=20):
    if len(closes) < lookback + period:
        return None
    s     = pd.Series(closes)
    delta = s.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rsi   = (100 - (100 / (1 + gain / (loss + 1e-9)))).values

    wl = lows[-lookback:]
    wh = highs[-lookback:]
    wr = rsi[-lookback:]

    swing_lows, swing_highs = [], []
    for i in range(2, len(wl) - 2):
        if wl[i] < wl[i-1] and wl[i] < wl[i-2] and wl[i] < wl[i+1] and wl[i] < wl[i+2]:
            swing_lows.append(i)
        if wh[i] > wh[i-1] and wh[i] > wh[i-2] and wh[i] > wh[i+1] and wh[i] > wh[i+2]:
            swing_highs.append(i)

    if len(swing_lows) >= 2:
        i1, i2 = swing_lows[-2], swing_lows[-1]
        if wl[i2] < wl[i1] and wr[i2] > wr[i1] and wr[i2] < 45:
            return "BULLISH"

    if len(swing_highs) >= 2:
        i1, i2 = swing_highs[-2], swing_highs[-1]
        if wh[i2] > wh[i1] and wr[i2] < wr[i1] and wr[i2] > 55:
            return "BEARISH"

    return None


def calc_volume_profile(closes, volumes, bins=10):
    if len(closes) < 10: return None
    pmin, pmax = float(min(closes)), float(max(closes))
    if pmax == pmin: return None
    bsize    = (pmax - pmin) / bins
    vbins    = [0.0] * bins
    for i, p in enumerate(closes):
        vbins[min(int((p - pmin) / bsize), bins-1)] += float(volumes[i])
    return float(pmin + (vbins.index(max(vbins)) + 0.5) * bsize)


# ═══════════════════════════════════════════════
#  SUPABASE
# ═══════════════════════════════════════════════
def already_sent(pair, signal_type, timeframe):
    try:
        now = datetime.now(WIB)
        res = supabase.table("signals").select("id") \
            .eq("pair", pair).eq("type", signal_type).eq("timeframe", timeframe) \
            .gt("valid_until", now.isoformat()).execute()
        return len(res.data) > 0
    except Exception as e:
        print(f"⚠️ already_sent [{pair}]: {e}")
        return False


def save_signal(pair, signal_type, side, entry, tp1, tp2, sl,
                strength, timeframe, valid_until):
    try:
        now = datetime.now(WIB)
        supabase.table("signals").insert({
            "pair": pair, "type": signal_type, "side": side,
            "entry_price": entry, "tp1": tp1, "tp2": tp2, "sl": sl,
            "strength": strength, "timeframe": timeframe,
            "valid_from": now.isoformat(), "valid_until": valid_until.isoformat()
        }).execute()
    except Exception as e:
        print(f"⚠️ save_signal [{pair}]: {e}")


def get_win_rate(signal_type):
    try:
        res = supabase.table("signals").select("result") \
            .eq("type", signal_type).not_.is_("result", "null").execute()
        if not res.data or len(res.data) < 5: return None
        wins = sum(1 for r in res.data if r.get("result") == "TP")
        return round((wins / len(res.data)) * 100, 1)
    except Exception:
        return None


# ═══════════════════════════════════════════════
#  SIGNAL SENDER
# ═══════════════════════════════════════════════
def stars(n):
    n = max(1, min(n, 5))
    return "⭐" * n + "☆" * (5 - n)

def fmt_time(dt):
    return dt.strftime("%H:%M WIB")


def send_signal(pair, signal_type, side, entry, tp1, tp2, sl,
                strength, timeframe, valid_minutes, sources="", extra=""):
    if strength < MIN_STRENGTH: return
    if already_sent(pair, signal_type, timeframe): return

    now         = datetime.now(WIB)
    valid_until = now + timedelta(minutes=valid_minutes)

    emoji_side = "🟢 BUY" if side == "BUY" else "🔴 SELL"
    emoji_type = {"SCALPING":"⚡","INTRADAY":"📈","SWING":"🌊","MOONSHOT":"🚀"}.get(signal_type,"🎯")

    pct_tp1 = abs((tp1 - entry) / entry * 100)
    pct_tp2 = abs((tp2 - entry) / entry * 100)
    pct_sl  = abs((sl  - entry) / entry * 100)
    rr      = round(pct_tp1 / pct_sl, 1) if pct_sl > 0 else 0

    win_rate = get_win_rate(signal_type)
    wr_str   = f"{win_rate}%" if win_rate else "N/A (data belum cukup)"

    msg = (
        f"{emoji_type} <b>SIGNAL {emoji_side} — {signal_type}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair:     <b>{pair.replace('_USDT','/USDT')}</b>\n"
        f"Entry:    <b>${entry:.6f}</b>\n"
        f"⏰ Valid:  {fmt_time(now)} → {fmt_time(valid_until)}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"TP1:      ${tp1:.6f} <i>(+{pct_tp1:.1f}%)</i>\n"
        f"TP2:      ${tp2:.6f} <i>(+{pct_tp2:.1f}%)</i>\n"
        f"SL:       ${sl:.6f} <i>(-{pct_sl:.1f}%)</i>\n"
        f"R/R:      <b>1:{rr}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Strength: {stars(strength)}\n"
        f"Win Rate: <i>{wr_str}</i>\n"
        f"TF:       {timeframe}\n"
        f"Data:     {sources}"
    )
    if extra:
        msg += f"\n{extra}"

    tg(msg)
    save_signal(pair, signal_type, side, entry, tp1, tp2, sl,
                strength, timeframe, valid_until)
    print(f"✅ {signal_type} {side} → {pair} | Strength:{strength} | R/R:1:{rr}")


def is_valid(pair):
    if not pair.endswith("_USDT"): return False
    base = pair.replace("_USDT", "")
    return not any(b in base for b in BLACKLIST)


# ═══════════════════════════════════════════════
#  SCALPING — 5m
# ═══════════════════════════════════════════════
def check_scalping(client, pair, price, fg, ob_ratio, funding, liq, oi_signal):
    closes, highs, lows, volumes = get_candles(client, pair, "5m", 80)
    if closes is None: return

    rsi       = calc_rsi(closes)
    stoch_rsi = calc_stoch_rsi(closes)
    ema9      = calc_ema(closes, 9)
    ema21     = calc_ema(closes, 21)
    atr       = calc_atr(closes, highs, lows)
    vwap      = calc_vwap(closes, highs, lows, volumes)
    support, resistance = calc_support_resistance(highs, lows, closes)
    divergence = calc_rsi_divergence(closes, highs, lows)
    vol_avg   = np.mean(volumes[-20:])
    vol_ratio = volumes[-1] / vol_avg if vol_avg > 0 else 0
    vol_spike = vol_ratio > 1.5

    sources = "Gate.io · F&G · OB · StochRSI · VWAP · Liq · OI"

    # BUY
    if rsi < 35 and ema9 > ema21 and vol_spike:
        s = 2
        if rsi < 28:                              s += 1
        if stoch_rsi < 0.2:                       s += 1
        if ob_ratio > 1.2:                        s += 1
        if fg < 25:                               s += 1
        if vol_ratio > 2.5:                       s += 1
        if divergence == "BULLISH":               s += 1
        if price <= support * 1.02:               s += 1
        if funding and funding < -0.001:          s += 1
        if price > vwap:                          s += 1
        if liq.get("liq_bias") == "BUY":          s += 1
        if oi_signal in ("STRONG_BUY","SQUEEZE"): s += 1
        s = min(s, 5)

        tp1 = price * 1.05
        tp2 = min(price * 1.08, resistance * 0.99)
        sl  = max(price - atr * 2.0, price * 0.96)

        extra = ""
        if divergence == "BULLISH": extra += "\n🔀 <i>Bullish RSI Divergence!</i>"
        if price <= support * 1.02: extra += f"\n🛡️ <i>Support ${support:.6f}</i>"
        if price > vwap:            extra += f"\n📐 <i>Di atas VWAP ${vwap:.6f} — bullish bias</i>"
        if liq.get("liq_bias") == "BUY":
            extra += f"\n💥 <i>Short liq cluster ${liq['liq_short_above']}M — magnet ke atas</i>"
        if oi_signal == "SQUEEZE":  extra += "\n🔥 <i>OI naik + harga turun — short squeeze!</i>"

        send_signal(pair,"SCALPING","BUY",price,tp1,tp2,sl,s,"5m",15,sources,extra)

    # SELL
    elif rsi > 68 and ema9 < ema21 and vol_spike:
        s = 2
        if rsi > 75:                              s += 1
        if stoch_rsi > 0.8:                       s += 1
        if ob_ratio < 0.8:                        s += 1
        if fg > 65:                               s += 1
        if vol_ratio > 2.5:                       s += 1
        if divergence == "BEARISH":               s += 1
        if price >= resistance * 0.98:            s += 1
        if funding and funding > 0.001:           s += 1
        if price < vwap:                          s += 1
        if liq.get("liq_bias") == "SELL":         s += 1
        if oi_signal == "STRONG_SELL":            s += 1
        s = min(s, 5)

        tp1 = price * 0.95
        tp2 = max(price * 0.92, support * 1.01)
        sl  = min(price + atr * 2.0, price * 1.04)

        extra = ""
        if divergence == "BEARISH":       extra += "\n🔀 <i>Bearish RSI Divergence!</i>"
        if price >= resistance * 0.98:    extra += f"\n🚧 <i>Resistance ${resistance:.6f}</i>"
        if price < vwap:                  extra += f"\n📐 <i>Di bawah VWAP ${vwap:.6f} — bearish bias</i>"
        if liq.get("liq_bias") == "SELL":
            extra += f"\n💥 <i>Long liq cluster ${liq['liq_long_below']}M — magnet ke bawah</i>"

        send_signal(pair,"SCALPING","SELL",price,tp1,tp2,sl,s,"5m",15,sources,extra)


# ═══════════════════════════════════════════════
#  INTRADAY — 1h
# ═══════════════════════════════════════════════
def check_intraday(client, pair, price, fg, ob_ratio, funding,
                   trending, market_data, liq, oi_signal):
    closes, highs, lows, volumes = get_candles(client, pair, "1h", 100)
    if closes is None: return

    rsi                     = calc_rsi(closes)
    stoch_rsi               = calc_stoch_rsi(closes)
    macd, msig              = calc_macd(closes)
    bb_low, bb_mid, bb_high = calc_bb(closes)
    atr                     = calc_atr(closes, highs, lows)
    ema20                   = calc_ema(closes, 20)
    ema50                   = calc_ema(closes, 50)
    vwap                    = calc_vwap(closes, highs, lows, volumes)
    ichi                    = calc_ichimoku(closes, highs, lows)
    support, resistance     = calc_support_resistance(highs, lows, closes)
    divergence              = calc_rsi_divergence(closes, highs, lows)
    poc                     = calc_volume_profile(closes, volumes)

    symbol      = pair.replace("_USDT","")
    is_trending = symbol in trending
    sources     = "Gate.io · F&G · CoinGecko · StochRSI · VWAP · Ichimoku · Liq · OI"

    # BUY
    if rsi < 42 and macd > msig and price <= bb_mid:
        s = 2
        if rsi < 35:                              s += 1
        if stoch_rsi < 0.25:                      s += 1
        if price <= bb_low:                       s += 1
        if fg < 30:                               s += 1
        if ob_ratio > 1.1:                        s += 1
        if is_trending:                           s += 1
        if ema20 > ema50:                         s += 1
        if divergence == "BULLISH":               s += 1
        if poc and price <= poc:                  s += 1
        if price <= support * 1.03:               s += 1
        if funding and funding < -0.001:          s += 1
        if price > vwap:                          s += 1
        if ichi.get("above_cloud"):               s += 1
        if ichi.get("tk_bull"):                   s += 1
        if liq.get("liq_bias") == "BUY":          s += 1
        if oi_signal in ("STRONG_BUY","SQUEEZE"): s += 1
        s = min(s, 5)

        tp1 = min(price * 1.05, resistance * 0.99)
        tp2 = min(price * 1.10, resistance * 1.05)
        sl  = max(price - atr * 2.5, price * 0.94)

        extra = ""
        if divergence == "BULLISH":    extra += "\n🔀 <i>Bullish RSI Divergence!</i>"
        if poc and price <= poc:       extra += f"\n📊 <i>Di bawah POC ${poc:.6f} — zona akumulasi</i>"
        if is_trending:                extra += "\n🔥 <i>Trending di CoinGecko!</i>"
        if fg < 20:                    extra += f"\n😱 <i>Extreme Fear {fg} — zona akumulasi institusi</i>"
        if ichi.get("above_cloud"):    extra += "\n☁️ <i>Ichimoku: Di atas cloud — bullish kuat</i>"
        if liq.get("liq_bias")=="BUY": extra += f"\n💥 <i>Short liq ${liq['liq_short_above']}M — magnet ke atas</i>"
        if oi_signal == "SQUEEZE":     extra += "\n🔥 <i>Short squeeze incoming!</i>"

        send_signal(pair,"INTRADAY","BUY",price,tp1,tp2,sl,s,"1h",120,sources,extra)

    # SELL
    elif rsi > 60 and macd < msig and price >= bb_mid:
        s = 2
        if rsi > 65:                              s += 1
        if stoch_rsi > 0.75:                      s += 1
        if price >= bb_high:                      s += 1
        if fg > 60:                               s += 1
        if ob_ratio < 0.9:                        s += 1
        if divergence == "BEARISH":               s += 1
        if poc and price >= poc:                  s += 1
        if price >= resistance * 0.97:            s += 1
        if funding and funding > 0.001:           s += 1
        if price < vwap:                          s += 1
        if ichi.get("below_cloud"):               s += 1
        if ichi.get("tk_bear"):                   s += 1
        if liq.get("liq_bias") == "SELL":         s += 1
        if oi_signal == "STRONG_SELL":            s += 1
        s = min(s, 5)

        tp1 = max(price * 0.95, support * 1.01)
        tp2 = max(price * 0.90, support * 0.95)
        sl  = min(price + atr * 2.5, price * 1.06)

        extra = ""
        if divergence == "BEARISH":     extra += "\n🔀 <i>Bearish RSI Divergence!</i>"
        if poc and price >= poc:        extra += f"\n📊 <i>Di atas POC ${poc:.6f} — zona distribusi</i>"
        if ichi.get("below_cloud"):     extra += "\n☁️ <i>Ichimoku: Di bawah cloud — bearish kuat</i>"
        if liq.get("liq_bias")=="SELL": extra += f"\n💥 <i>Long liq ${liq['liq_long_below']}M — magnet ke bawah</i>"

        send_signal(pair,"INTRADAY","SELL",price,tp1,tp2,sl,s,"1h",120,sources,extra)


# ═══════════════════════════════════════════════
#  SWING — 4h
# ═══════════════════════════════════════════════
def check_swing(client, pair, price, fg, ob_ratio, funding,
                trending, market_data, liq, oi_signal):
    closes, highs, lows, volumes = get_candles(client, pair, "4h", 210)
    if closes is None: return

    rsi        = calc_rsi(closes)
    stoch_rsi  = calc_stoch_rsi(closes)
    ema50      = calc_ema(closes, 50)
    ema200     = calc_ema(closes, 200)
    atr        = calc_atr(closes, highs, lows)
    macd, msig = calc_macd(closes)
    vwap       = calc_vwap(closes, highs, lows, volumes)
    ichi       = calc_ichimoku(closes, highs, lows)
    vol_trend  = np.mean(volumes[-5:]) > np.mean(volumes[-20:]) * 1.2
    support, resistance = calc_support_resistance(highs, lows, closes, lookback=50)
    divergence = calc_rsi_divergence(closes, highs, lows, lookback=40)
    poc        = calc_volume_profile(closes, volumes, bins=20)

    symbol      = pair.replace("_USDT","")
    is_trending = symbol in trending
    sources     = "Gate.io · F&G · CoinGecko · EMA50/200 · VWAP · Ichimoku · Liq · OI"

    # BUY
    if rsi < 50 and rsi > 20 and macd > msig:
        s = 2
        if ema50 > ema200:                        s += 1
        if rsi < 35:                              s += 1
        if stoch_rsi < 0.3:                       s += 1
        if fg < 25:                               s += 1
        if vol_trend:                             s += 1
        if is_trending:                           s += 1
        if ob_ratio > 1.1:                        s += 1
        if divergence == "BULLISH":               s += 1
        if poc and price <= poc:                  s += 1
        if price <= support * 1.05:               s += 1
        if funding and funding < -0.002:          s += 1
        if price > vwap:                          s += 1
        if ichi.get("above_cloud"):               s += 1
        if ichi.get("tk_bull"):                   s += 1
        if liq.get("liq_bias") == "BUY":          s += 1
        if oi_signal in ("STRONG_BUY","SQUEEZE"): s += 1
        s = min(s, 5)

        tp1 = min(price * 1.10, resistance * 0.98)
        tp2 = min(price * 1.20, resistance * 1.10)
        sl  = max(price - atr * 3.0, price * 0.88)

        extra = ""
        if ema50 > ema200:             extra += "\n✨ <i>Golden Cross EMA 50/200</i>"
        if divergence == "BULLISH":    extra += "\n🔀 <i>Bullish RSI Divergence!</i>"
        if fg < 20:                    extra += f"\n😱 <i>Extreme Fear {fg} — historis zona bottom</i>"
        if poc:                        extra += f"\n📊 <i>POC: ${poc:.6f}</i>"
        if is_trending:                extra += "\n🔥 <i>Trending di CoinGecko!</i>"
        if ichi.get("above_cloud"):    extra += "\n☁️ <i>Ichimoku: Di atas cloud — trend bullish</i>"
        if liq.get("liq_bias")=="BUY": extra += f"\n💥 <i>Short liq ${liq['liq_short_above']}M</i>"
        if oi_signal == "SQUEEZE":     extra += "\n🔥 <i>Short squeeze incoming!</i>"

        send_signal(pair,"SWING","BUY",price,tp1,tp2,sl,s,"4h",720,sources,extra)

    # SELL
    elif rsi > 60 and rsi < 80 and macd < msig:
        s = 2
        if ema50 < ema200:                        s += 1
        if rsi > 65:                              s += 1
        if stoch_rsi > 0.7:                       s += 1
        if fg > 60:                               s += 1
        if vol_trend:                             s += 1
        if divergence == "BEARISH":               s += 1
        if funding and funding > 0.002:           s += 1
        if price < vwap:                          s += 1
        if ichi.get("below_cloud"):               s += 1
        if liq.get("liq_bias") == "SELL":         s += 1
        if oi_signal == "STRONG_SELL":            s += 1
        s = min(s, 5)

        tp1 = max(price * 0.90, support * 1.02)
        tp2 = min(tp1 * 0.95, max(price * 0.80, support * 0.90))  # fix: tp2 selalu lebih jauh
        sl  = min(price + atr * 3.0, price * 1.12)

        extra = ""
        if ema50 < ema200:              extra += "\n💀 <i>Death Cross EMA 50/200</i>"
        if divergence == "BEARISH":     extra += "\n🔀 <i>Bearish RSI Divergence!</i>"
        if ichi.get("below_cloud"):     extra += "\n☁️ <i>Ichimoku: Di bawah cloud — trend bearish</i>"
        if liq.get("liq_bias")=="SELL": extra += f"\n💥 <i>Long liq ${liq['liq_long_below']}M — magnet ke bawah</i>"

        send_signal(pair,"SWING","SELL",price,tp1,tp2,sl,s,"4h",720,sources,extra)


# ═══════════════════════════════════════════════
#  MOONSHOT — Early Pump
# ═══════════════════════════════════════════════
def check_moonshot(client, pair, price, change_24h, trending, liq):
    closes, highs, lows, volumes = get_candles(client, pair, "1h", 48)
    if closes is None: return

    vol_avg   = np.mean(volumes[:-6])
    vol_now   = np.mean(volumes[-6:])
    vol_ratio = vol_now / vol_avg if vol_avg > 0 else 0
    rsi       = calc_rsi(closes)
    atr       = calc_atr(closes, highs, lows)
    vwap      = calc_vwap(closes, highs, lows, volumes)
    support, resistance = calc_support_resistance(highs, lows, closes)

    symbol      = pair.replace("_USDT","")
    is_trending = symbol in trending

    if vol_ratio > 4.0 and 25 < rsi < 70 and change_24h > 3.0:
        s = 2
        if vol_ratio > 7.0:             s += 1
        if is_trending:                 s += 1
        if change_24h > 8.0:            s += 1
        if rsi < 55:                    s += 1
        if price > resistance:          s += 1
        if price > vwap:                s += 1
        if liq.get("liq_bias")=="BUY":  s += 1
        s = min(s, 5)

        tp1 = price * 1.20
        tp2 = price * 1.50
        sl  = max(price * 0.88, support * 0.98)

        extra = (
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔥 Vol Spike:  <b>{vol_ratio:.1f}x</b> normal\n"
            f"📊 Change 24h: <b>+{change_24h:.1f}%</b>\n"
        )
        if price > resistance: extra += f"🚀 <i>Breakout Resistance ${resistance:.6f}!</i>\n"
        if price > vwap:       extra += f"📐 <i>Di atas VWAP ${vwap:.6f}</i>\n"
        if is_trending:        extra += "🌟 <i>Trending di CoinGecko!</i>\n"
        if liq.get("liq_bias")=="BUY":
            extra += f"💥 <i>Short liq ${liq['liq_short_above']}M — magnet ke atas</i>\n"
        extra += "⚠️ <i>High Risk — Max 5% modal. SL wajib.</i>"

        send_signal(pair,"MOONSHOT","BUY",price,tp1,tp2,sl,s,"1h",360,
                    "Gate.io · CoinGecko · VWAP · Liq",extra)


# ═══════════════════════════════════════════════
#  MARKET SUMMARY (1x/hari)
# ═══════════════════════════════════════════════
def should_send_summary():
    now = datetime.now(WIB)
    return now.hour == SUMMARY_HOUR_WIB and now.minute < 15


def send_market_summary(fg, fg_label, market_data, trending):
    if not market_data: return
    now        = datetime.now(WIB)
    btc_dom    = market_data.get("btc_dominance", 0)
    mcap_chg   = market_data.get("market_cap_change_24h", 0)
    mcap_emoji = "📈" if mcap_chg > 0 else "📉"
    trend_str  = ", ".join(trending[:5]) if trending else "N/A"

    saran_map = [
        (20,  "😱 <b>Extreme Fear</b> — Zona akumulasi, potensi reversal"),
        (40,  "😨 <b>Fear</b> — Hati-hati, ada peluang beli selektif"),
        (60,  "😐 <b>Neutral</b> — Tunggu konfirmasi arah"),
        (80,  "😊 <b>Greed</b> — Mulai waspada, kurangi posisi"),
        (101, "🤑 <b>Extreme Greed</b> — Bahaya, potensi koreksi besar"),
    ]
    saran    = next(s for threshold, s in saran_map if fg < threshold)
    alt_note = "🟢 Alt Season" if btc_dom < 48 else "🔴 BTC Season"

    tg(
        f"📊 <b>MARKET SUMMARY — {now.strftime('%d %b %Y %H:%M WIB')}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Fear & Greed:  <b>{fg} — {fg_label}</b>\n"
        f"BTC Dominance: <b>{btc_dom:.1f}%</b> {alt_note}\n"
        f"Market Cap:    {mcap_emoji} <b>{mcap_chg:+.2f}%</b> (24h)\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔥 Trending: <b>{trend_str}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💡 {saran}"
    )


# ═══════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════
def run():
    client = get_client()
    print("=== SIGNAL SCAN STARTED ===")

    fg, fg_label = get_fear_greed()
    market_data  = get_coingecko_market()
    trending     = get_coingecko_trending()

    btc_dom = market_data.get("btc_dominance","N/A") if market_data else "N/A"
    print(f"BTC Dom: {btc_dom}% | F&G: {fg} | Trending: {trending[:3]}")

    if should_send_summary():
        send_market_summary(fg, fg_label, market_data, trending)

    # Guard: stop jika BTC crash >12%
    try:
        btc    = client.list_tickers(currency_pair="BTC_USDT")[0]
        change = float(btc.change_percentage or 0)
        print(f"BTC 24h: {change:.2f}%")
        if change < -12:
            print("❌ Market crash, scan dihentikan")
            tg("⚠️ <b>Market Crash Alert</b>\nBTC turun &gt;12% dalam 24j. Scan ditunda.")
            return
    except Exception as e:
        print(f"⚠️ BTC check: {e}")

    tickers = client.list_tickers()
    total   = 0

    for t in tickers:
        pair = t.currency_pair
        if not is_valid(pair): continue

        try:
            price      = float(t.last or 0)
            vol_24h    = float(t.quote_volume or 0)
            change_24h = float(t.change_percentage or 0)

            if price <= 0 or vol_24h < MIN_VOLUME: continue

            symbol = pair.replace("_USDT","")

            # Futures data hanya untuk FUTURES_PAIRS
            funding = get_funding_rate(pair)
            if pair in FUTURES_PAIRS:
                liq       = get_liquidation_data(symbol)
                oi_data   = get_open_interest(symbol)
                oi_signal = interpret_oi(oi_data, change_24h)
                time.sleep(0.2)
            else:
                liq       = {"liq_short_above":0,"liq_long_below":0,"liq_bias":"NEUTRAL"}
                oi_signal = "NEUTRAL"

            ob_ratio = get_order_book_pressure(client, pair)

            check_scalping(client, pair, price, fg, ob_ratio, funding, liq, oi_signal)
            check_intraday(client, pair, price, fg, ob_ratio, funding,
                           trending, market_data, liq, oi_signal)
            check_swing(client, pair, price, fg, ob_ratio, funding,
                        trending, market_data, liq, oi_signal)
            check_moonshot(client, pair, price, change_24h, trending, liq)

            total += 1
            time.sleep(0.1)

        except Exception as e:
            print(f"⚠️ Error [{pair}]: {e}")
            continue

    print(f"=== DONE | {total} pairs scanned ===")


if __name__ == "__main__":
    run()
