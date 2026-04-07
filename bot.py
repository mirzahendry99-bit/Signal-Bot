"""
╔══════════════════════════════════════════════════════════════════╗
║        SIGNAL BOT — DECISION ENGINE v10                         ║
║                                                                  ║
║  9 ENGINE LAYERS:                                                ║
║  1. Structure Engine   — BOS, CHoCH, Trend Phase                ║
║  2. Liquidity Engine   — Equal H/L, Stop Cluster, Fake BO       ║
║  3. Entry Precision    — Pullback, Rejection, Confirmation       ║
║  4. Signal Ranking     — Tier S/A+/A/B, Confluence scoring      ║
║  5. Position Sizing    — Risk-based dynamic sizing per tier      ║
║  6. Portfolio Brain    — Exposure, Correlation, Sector rotation  ║
║  7. Self-Optimizer     — Kill bad strats, Scale good strats      ║
║  8. Expectancy Tracker — E(x) = (WR×avgW) − (LR×avgL)          ║
║  9. Kill Switch        — Auto-halt on loss streak / chaos        ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os
import json
import time
import urllib.request
import gate_api
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from supabase import create_client

print("🚀 SIGNAL BOT — DECISION ENGINE v10")

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
#  KONFIGURASI GLOBAL
# ═══════════════════════════════════════════════
MIN_VOLUME       = 300_000
SUMMARY_HOUR_WIB = 8
SIGNAL_MODE      = "AGGRESSIVE"   # AGGRESSIVE | CONSERVATIVE
WIB              = timezone(timedelta(hours=7))
supabase         = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Account & Risk ────────────────────────────
ACCOUNT_BALANCE  = float(os.environ.get("ACCOUNT_BALANCE", "1000"))
MAX_PORTFOLIO_RISK_PCT = 10.0  # v9: naik 6→10 agar 5 sinyal muat (5×2%=10%)

# ── Signal Tier Thresholds ───────────────────
TIER_THRESHOLDS = {
    "S":  16,   # perfect setup — structure + liq + entry semua valid (turun dari 18)
    "A+": 11,   # high confluence + structure valid (turun dari 14, Coinglass sering NEUTRAL)
    "A":   7,   # valid, normal (turun dari 9)
    "B":   4,   # spekulatif, size kecil (turun dari 5)
    # < 4 = SKIP
    # Catatan: Coinglass API sering return NEUTRAL → liq_cluster(2)+oi_signal(2)=4 sering hilang
    # Threshold disesuaikan agar tetap ada sinyal berkualitas tanpa Coinglass
}

# ── Position Sizing per Tier ─────────────────
TIER_RISK_PCT = {
    "S":  2.0,   # v9: turun 3→2% agar 5 sinyal S bisa fit (5×2=10%)
    "A+": 1.5,   # v9: turun 2→1.5%
    "A":  1.0,
    "B":  0.5,
}

# ── Kill Switch ───────────────────────────────
KILL_SWITCH_LOSS_STREAK  = 3    # halt jika N loss berturut-turut dalam window
KILL_SWITCH_WINDOW_HOURS = 6    # jam lookback
KILL_SWITCH_ENABLED      = True

# ── Min R/R per Tier ─────────────────────────
# v7: Tier B dilonggarkan agar sinyal tidak terlalu sering di-skip
MIN_RR = {
    "SCALPING":  {"S": 1.8, "A+": 1.5, "A": 1.2, "B": 0.8},
    "INTRADAY":  {"S": 2.5, "A+": 2.0, "A": 1.5, "B": 1.0},
    "SWING":     {"S": 3.0, "A+": 2.5, "A": 2.0, "B": 1.5},
    "MOONSHOT":  {"S": 2.5, "A+": 2.0, "A": 1.5, "B": 1.0},
}

# ── Max Signals per Cycle ────────────────────
MAX_SIGNALS_PER_CYCLE = 5   # v7: naik dari 3 → 5

# ── Weighted Scoring ─────────────────────────
# Structure signals paling tinggi bobotnya
W = {
    # Structure (highest weight — institutional core)
    "bos":         5,
    "choch":       5,
    "liq_sweep":   4,
    "order_block": 4,
    "equal_hl":    3,
    "stop_cluster":3,
    "fake_bo":     3,
    # Confirmation signals
    "divergence":  3,
    "liq_cluster": 2,
    "oi_signal":   2,
    "ichimoku":    2,
    "vwap":        2,
    "bb_extreme":  2,
    "funding":     2,
    "poc":         2,
    # Indicator signals (lowest — confirmation only)
    "rsi_extreme": 1,
    "stoch_rsi":   1,
    "ema_cross":   1,
    "ob_ratio":    1,
    "trending":    1,
    "fg_extreme":  1,
    "vol_spike":   1,
    "support_res": 1,
    "pullback":    2,   # entry precision bonus
    "rejection":   2,   # entry precision bonus
    "candle_conf": 2,   # candle confirmation bonus
}

# ── Volatility Filter ─────────────────────────
ATR_MIN_PCT = {"SCALPING": 0.2, "INTRADAY": 0.3, "SWING": 0.5}
ATR_MAX_PCT = {"SCALPING": 6.0, "INTRADAY": 10.0, "SWING": 15.0}

# ── BTC Correlation Filter ────────────────────
BTC_1H_DROP_BLOCK = -3.0
BTC_1H_PUMP_BLOCK =  3.0

# ── Safe pairs saat RISK_OFF ─────────────────
SAFE_PAIRS_RISK_OFF = {"BTC_USDT", "ETH_USDT"}

FUTURES_PAIRS = {
    "BTC_USDT","ETH_USDT","SOL_USDT","BNB_USDT","XRP_USDT",
    "DOGE_USDT","ADA_USDT","AVAX_USDT","DOT_USDT","LINK_USDT",
    "UNI_USDT","ATOM_USDT","LTC_USDT","NEAR_USDT","ARB_USDT",
    "OP_USDT","APT_USDT","SUI_USDT","TRX_USDT",
}

TOP_PAIRS_OB = {
    "BTC_USDT","ETH_USDT","SOL_USDT","BNB_USDT","XRP_USDT",
    "DOGE_USDT","ADA_USDT","AVAX_USDT","LINK_USDT","SUI_USDT",
    "NEAR_USDT","UNI_USDT","LTC_USDT","DOT_USDT","TRX_USDT",
}

BLACKLIST = {
    "3S","3L","5S","5L",
    "TUSD","USDC","BUSD","DAI","FDUSD","USD1",
    "USDP","USDD","USDJ","ZUSD","GUSD","CUSD","SUSD",
    "STBL","FRAX","LUSD","USDN","STABLE","BARD"
}

# ── Signal queue (global per cycle) ──────────
_signal_queue = []

# ── Portfolio state (global per cycle) ───────
# Diisi dari Supabase saat run() dimulai agar posisi lama tetap dihitung
_portfolio_state = {
    "total_risk_pct": 0.0,
    "open_positions": {},    # pair → side
    "sector_exposure": defaultdict(float),
}


def load_portfolio_state() -> dict:
    """
    FIX 3: Load portfolio state dari sinyal yang MASIH AKTIF di Supabase.
    Sebelumnya: state direset tiap cycle → bisa double entry pair yang sama.
    Sekarang: baca sinyal valid (belum expired, result masih null) dari DB.

    Return: portfolio_state dict dengan posisi aktif yang sudah diperhitungkan.
    """
    state = {
        "total_risk_pct":  0.0,
        "open_positions":  {},
        "sector_exposure": defaultdict(float),
    }
    try:
        now = datetime.now(WIB)
        res = supabase.table("signals").select("pair,side,risk_pct,result") \
            .gt("valid_until", now.isoformat()) \
            .is_("result", "null") \
            .execute()

        if not res.data:
            return state

        for sig in res.data:
            pair     = sig.get("pair", "")
            side     = sig.get("side", "")
            risk_pct = float(sig.get("risk_pct") or 0)
            if not pair or not side:
                continue

            # Akumulasikan risk dan posisi
            state["total_risk_pct"] += risk_pct
            state["open_positions"][pair] = side
            sector = get_pair_sector(pair)
            state["sector_exposure"][sector] += 1

        print(f"📂 Portfolio loaded: {len(state['open_positions'])} open positions "
              f"| total risk: {state['total_risk_pct']:.1f}%")

    except Exception as e:
        print(f"⚠️ load_portfolio_state: {e}")

    return state


# ═══════════════════════════════════════════════
#  UTIL — HTTP & TELEGRAM
# ═══════════════════════════════════════════════
def tg(msg: str):
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


def http_get(url: str, timeout: int = 10):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"⚠️ HTTP {url[:60]}: {e}")
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
#  EXTERNAL DATA
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


def get_coingecko_market():
    data = http_get("https://api.coingecko.com/api/v3/global")
    if not data:
        return None
    try:
        d = data["data"]
        return {
            "btc_dominance":         float(d.get("market_cap_percentage", {}).get("btc", 50)),
            "eth_dominance":         float(d.get("market_cap_percentage", {}).get("eth", 15)),
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


def get_liquidation_data(symbol: str):
    default = {"liq_short_above": 0, "liq_long_below": 0, "liq_bias": "NEUTRAL"}
    try:
        url  = f"https://open-api.coinglass.com/public/v2/liquidation_map?symbol={symbol}&interval=12h"
        data = http_get(url, timeout=8)
        if not data or data.get("code") != "0":
            return default
        items     = data.get("data", {})
        liq_short = sum(float(s.get("liquidationAmount", 0))
                        for s in items.get("shorts", [])[:10])
        liq_long  = sum(float(l.get("liquidationAmount", 0))
                        for l in items.get("longs",  [])[:10])
        bias = "BUY" if liq_short > liq_long*1.5 else "SELL" if liq_long > liq_short*1.5 else "NEUTRAL"
        return {
            "liq_short_above": round(liq_short / 1_000_000, 2),
            "liq_long_below":  round(liq_long  / 1_000_000, 2),
            "liq_bias":        bias
        }
    except Exception:
        return default


def get_open_interest(symbol: str):
    default = {"oi_usd": 0, "oi_change_pct": 0}
    try:
        url  = f"https://open-api.coinglass.com/public/v2/open_interest?symbol={symbol}"
        data = http_get(url, timeout=8)
        if not data or data.get("code") != "0":
            return default
        items    = data.get("data", [])
        total_oi = sum(float(e.get("openInterest", 0)) for e in items)
        oi_chg   = float(items[0].get("openInterestChangePercent24h", 0)) if items else 0
        return {"oi_usd": round(total_oi/1_000_000, 2), "oi_change_pct": round(oi_chg, 2)}
    except Exception:
        return default


def get_open_interest_gate(pair: str) -> dict:
    """
    Fallback OI langsung dari Gate.io Futures jika Coinglass error.
    Gunakan position_size × mark_price sebagai proxy total OI.
    oi_change_pct di-set 0 karena tidak ada data historis dari endpoint ini.
    """
    default = {"oi_usd": 0, "oi_change_pct": 0}
    try:
        data = http_get(
            f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{pair}", timeout=8)
        if data:
            position_size = float(data.get("position_size", 0) or 0)
            mark_price    = float(data.get("mark_price",    0) or 0)
            quanto_mult   = float(data.get("quanto_multiplier", 1) or 1)
            oi_usd = position_size * mark_price * quanto_mult
            if oi_usd > 0:
                return {"oi_usd": round(oi_usd / 1_000_000, 2), "oi_change_pct": 0}
    except Exception:
        pass
    return default


def interpret_oi(oi_data: dict, price_change: float) -> str:
    oi_chg = oi_data.get("oi_change_pct", 0)

    # [v10 FIX] Jika oi_change_pct == 0 (Gate.io fallback tidak punya historis),
    # gunakan price_change_24h sebagai proxy OI signal.
    # Logika: price naik kuat + OI ada (oi_usd > 0) → asumsi STRONG_BUY.
    # Ini lebih baik daripada selalu return NEUTRAL yang membuang 4 poin score.
    if oi_chg == 0 and oi_data.get("oi_usd", 0) > 0:
        if price_change > 3:   return "STRONG_BUY"
        if price_change < -3:  return "STRONG_SELL"
        if price_change > 1:   return "WEAK_RALLY"
        return "NEUTRAL"

    if oi_chg > 5  and price_change > 1:  return "STRONG_BUY"
    if oi_chg > 5  and price_change < -1: return "SQUEEZE"
    if oi_chg < -5 and price_change > 1:  return "WEAK_RALLY"
    if oi_chg < -5 and price_change < -1: return "STRONG_SELL"
    return "NEUTRAL"


def get_funding_rate(pair: str):
    if pair not in FUTURES_PAIRS:
        return None
    try:
        data = http_get(
            f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{pair}", timeout=8)
        if data and "funding_rate" in data:
            return float(data["funding_rate"])
    except Exception:
        pass
    return None


def get_order_book_pressure(client, pair: str) -> float:
    if pair not in TOP_PAIRS_OB:
        return 1.0
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
def get_candles(client, pair: str, interval: str, limit: int):
    """Gate.io candlestick: [0]=ts [1]=volume [2]=close [3]=high [4]=low [5]=open"""
    try:
        candles = client.list_candlesticks(
            currency_pair=pair, interval=interval, limit=limit)
        if not candles or len(candles) < 20:
            return None, None, None, None
        closes  = np.array([float(c[2]) for c in candles])
        highs   = np.array([float(c[3]) for c in candles])
        lows    = np.array([float(c[4]) for c in candles])
        volumes = np.array([float(c[1]) for c in candles])   # FIX v6: c[1]=vol bukan c[5]=open
        return closes, highs, lows, volumes
    except Exception as e:
        print(f"⚠️ candles [{pair}|{interval}]: {e}")
        return None, None, None, None


# ═══════════════════════════════════════════════
#  INDIKATOR (unchanged from v4)
# ═══════════════════════════════════════════════
def calc_rsi(closes, period=14):
    s     = pd.Series(closes)
    delta = s.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / (loss + 1e-9)
    return float((100 - 100/(1+rs)).iloc[-1])


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
    return float(mid-2*std), float(mid), float(mid+2*std)


def calc_atr(closes, highs, lows, period=14):
    tr_list = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
               for i in range(1, len(closes))]
    return float(pd.Series(tr_list).rolling(period).mean().iloc[-1])


def calc_stoch_rsi(closes, period=14):
    s         = pd.Series(closes)
    delta     = s.diff()
    gain      = delta.clip(lower=0).rolling(period).mean()
    loss      = (-delta.clip(upper=0)).rolling(period).mean()
    rsi       = 100 - (100/(1+gain/(loss+1e-9)))
    rsi_min   = rsi.rolling(period).min()
    rsi_max   = rsi.rolling(period).max()
    stoch_rsi = (rsi-rsi_min)/(rsi_max-rsi_min+1e-9)
    return float(stoch_rsi.iloc[-1])


def calc_vwap(closes, highs, lows, volumes):
    tp   = (highs+lows+closes)/3
    vwap = np.cumsum(tp*volumes)/(np.cumsum(volumes)+1e-9)
    return float(vwap[-1])


def calc_ichimoku(closes, highs, lows):
    def hl(h, l, p):
        if len(h)<p: return None, None
        return float(np.max(h[-p:])), float(np.min(l[-p:]))
    th, tl = hl(highs, lows, 9)
    kh, kl = hl(highs, lows, 26)
    if None in (th, tl, kh, kl):
        return {"valid":False,"above_cloud":False,"below_cloud":False,
                "tk_bull":False,"tk_bear":False}
    tenkan   = (th+tl)/2;  kijun = (kh+kl)/2
    senkou_a = (tenkan+kijun)/2
    sh, sl2  = hl(highs, lows, 52)
    senkou_b = (sh+sl2)/2 if sh else senkou_a
    price    = float(closes[-1])
    cloud_top, cloud_bot = max(senkou_a,senkou_b), min(senkou_a,senkou_b)
    return {
        "valid":True, "above_cloud":price>cloud_top, "below_cloud":price<cloud_bot,
        "in_cloud":cloud_bot<=price<=cloud_top, "tk_bull":tenkan>kijun,
        "tk_bear":tenkan<kijun, "cloud_top":cloud_top, "cloud_bot":cloud_bot,
    }


def calc_support_resistance(highs, lows, closes, lookback=20):
    return (float(np.percentile(lows[-lookback:],  15)),
            float(np.percentile(highs[-lookback:], 85)))


def calc_rsi_divergence(closes, highs, lows, period=14, lookback=20):
    if len(closes)<lookback+period: return None
    s     = pd.Series(closes)
    delta = s.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rsi   = (100-(100/(1+gain/(loss+1e-9)))).values
    wl, wh, wr = lows[-lookback:], highs[-lookback:], rsi[-lookback:]
    swing_lows, swing_highs = [], []
    for i in range(2, len(wl)-2):
        if wl[i]<wl[i-1] and wl[i]<wl[i-2] and wl[i]<wl[i+1] and wl[i]<wl[i+2]:
            swing_lows.append(i)
        if wh[i]>wh[i-1] and wh[i]>wh[i-2] and wh[i]>wh[i+1] and wh[i]>wh[i+2]:
            swing_highs.append(i)
    if len(swing_lows)>=2:
        i1,i2 = swing_lows[-2], swing_lows[-1]
        if wl[i2]<wl[i1] and wr[i2]>wr[i1] and wr[i2]<45: return "BULLISH"
    if len(swing_highs)>=2:
        i1,i2 = swing_highs[-2], swing_highs[-1]
        if wh[i2]>wh[i1] and wr[i2]<wr[i1] and wr[i2]>55: return "BEARISH"
    return None


def calc_volume_profile(closes, volumes, bins=10):
    if len(closes)<10: return None
    pmin, pmax = float(min(closes)), float(max(closes))
    if pmax==pmin: return None
    bsize = (pmax-pmin)/bins
    vbins = [0.0]*bins
    for i,p in enumerate(closes):
        vbins[min(int((p-pmin)/bsize),bins-1)] += float(volumes[i])
    return float(pmin+(vbins.index(max(vbins))+0.5)*bsize)


# ═══════════════════════════════════════════════
#  ENGINE 1: STRUCTURE ENGINE
#  BOS, CHoCH, Trend Phase Detection
# ═══════════════════════════════════════════════
def detect_swing_points(highs, lows, lookback=60, strength=2):
    points = []
    n = min(len(highs), lookback)
    start = len(highs)-n
    for i in range(start+strength, len(highs)-strength):
        if all(highs[i]>highs[i-j] for j in range(1,strength+1)) and \
           all(highs[i]>highs[i+j] for j in range(1,strength+1)):
            points.append((i, highs[i], "SH"))
        if all(lows[i]<lows[i-j] for j in range(1,strength+1)) and \
           all(lows[i]<lows[i+j] for j in range(1,strength+1)):
            points.append((i, lows[i], "SL"))
    return sorted(points, key=lambda x: x[0])


def detect_structure(closes, highs, lows, lookback=80) -> dict:
    """
    Deteksi BOS, CHoCH, dan trend phase.

    Trend Phase:
    - ACCUMULATION: ranging setelah downtrend, swing lows mulai naik
    - MARKUP:       HH+HL — uptrend sehat
    - DISTRIBUTION: ranging setelah uptrend, swing highs mulai turun
    - MARKDOWN:     LH+LL — downtrend sehat

    BOS: break sesuai arah trend (continuation)
    CHoCH: break MELAWAN trend (reversal signal)
    """
    result = {
        "bos": None, "choch": None,
        "trend_phase": "UNKNOWN",
        "structure_bias": "NEUTRAL",  # BULLISH | BEARISH | NEUTRAL
        "last_sh": None, "last_sl": None,
        "prev_sh": None, "prev_sl": None,
        "valid": False,
    }

    if len(closes) < lookback:
        return result

    points      = detect_swing_points(highs, lows, lookback=lookback, strength=2)
    swing_highs = [(i, p) for i, p, t in points if t == "SH"]
    swing_lows  = [(i, p) for i, p, t in points if t == "SL"]

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return result

    last_sh_idx, last_sh = swing_highs[-1]
    last_sl_idx, last_sl = swing_lows[-1]
    prev_sh              = swing_highs[-2][1]
    prev_sl              = swing_lows[-2][1]
    current              = float(closes[-1])

    result.update({
        "last_sh": last_sh, "last_sl": last_sl,
        "prev_sh": prev_sh, "prev_sl": prev_sl,
        "valid": True,
    })

    # Tentukan trend phase dari Higher High/Low pattern
    hh = last_sh > prev_sh
    hl = last_sl > prev_sl
    lh = last_sh < prev_sh
    ll = last_sl < prev_sl

    if hh and hl:
        phase  = "MARKUP"
        bias   = "BULLISH"
    elif lh and ll:
        phase  = "MARKDOWN"
        bias   = "BEARISH"
    elif hh and ll:
        phase  = "DISTRIBUTION"   # diverging — indikasi awal distribusi
        bias   = "NEUTRAL"
    elif lh and hl:
        phase  = "ACCUMULATION"   # converging — indikasi awal akumulasi
        bias   = "NEUTRAL"
    else:
        phase  = "RANGING"
        bias   = "NEUTRAL"

    result["trend_phase"]    = phase
    result["structure_bias"] = bias

    # ── FIX 1: BOS & CHoCH ───────────────────────────────────────────
    # BOS/CHoCH harus dikonfirmasi dengan CANDLE CLOSE di atas/bawah level,
    # bukan hanya harga saat ini menyentuh level (menghindari false trigger
    # di sideways/ranging market karena wick saja sudah cukup sebelumnya).
    #
    # Syarat:
    # - Bullish BOS/CHoCH: closes[-1] > last_sh DAN closes[-2] < last_sh
    #   (candle terakhir CLOSE di atas, candle sebelumnya CLOSE di bawah)
    # - Bearish BOS/CHoCH: closes[-1] < last_sl DAN closes[-2] > last_sl
    #   (candle terakhir CLOSE di bawah, candle sebelumnya CLOSE di atas)
    #
    # Ini memastikan kita hanya masuk setelah breakout dikonfirmasi,
    # bukan saat harga sedang dalam proses menyentuh level (bisa balik).

    prev_close = float(closes[-2]) if len(closes) >= 2 else current

    # Bullish break: close terbaru di atas last_sh, close sebelumnya di bawah
    bullish_break = (current > last_sh and prev_close <= last_sh)

    # Bearish break: close terbaru di bawah last_sl, close sebelumnya di atas
    bearish_break = (current < last_sl and prev_close >= last_sl)

    if bullish_break:
        if bias == "BULLISH":
            result["bos"]   = "BULLISH"   # konfirmasi continuation markup
        elif bias == "BEARISH":
            result["choch"] = "BULLISH"   # reversal — harga balik arah dari markdown
        else:
            result["bos"]   = "BULLISH"   # neutral/ranging, anggap BOS

    elif bearish_break:
        if bias == "BEARISH":
            result["bos"]   = "BEARISH"   # konfirmasi continuation markdown
        elif bias == "BULLISH":
            result["choch"] = "BEARISH"   # reversal — harga balik arah dari markup
        else:
            result["bos"]   = "BEARISH"

    return result


# ═══════════════════════════════════════════════
#  ENGINE 2: LIQUIDITY ENGINE
#  Equal Highs/Lows, Stop Cluster, Fake Breakout
# ═══════════════════════════════════════════════
def detect_liquidity_map(closes, highs, lows, volumes, lookback=50) -> dict:
    """
    Peta likuiditas — di mana stop loss ritel tersimpan.

    1. Equal Highs/Lows: price menyentuh level yang sama 2x+ = stop cluster
    2. Stop Hunt Zone: spike tipis melewati level lalu kembali
    3. Fake Breakout: close di atas level lalu balik = distributor menjebak buyer
    4. Liquidity Sweep: spike keluar range dengan volume tinggi lalu balik

    Output: dict dengan semua zone yang terdeteksi.
    """
    result = {
        "equal_highs":    None,   # price level
        "equal_lows":     None,
        "stop_cluster_above": None,
        "stop_cluster_below": None,
        "fake_bo_bull":   False,  # fake bullish breakout (jebakan buyer)
        "fake_bo_bear":   False,  # fake bearish breakdown (jebakan seller)
        "sweep_bull":     False,  # sweep low lalu kembali naik (bullish)
        "sweep_bear":     False,  # sweep high lalu kembali turun (bearish)
        "sweep_level":    None,
        "liq_above":      None,   # estimasi harga di mana stop buy terkumpul
        "liq_below":      None,   # estimasi harga di mana stop sell terkumpul
    }

    if len(closes) < lookback:
        return result

    n = lookback
    h_slice = highs[-n:]
    l_slice = lows[-n:]
    c_slice = closes[-n:]
    v_slice = volumes[-n:]

    # ── 1. Equal Highs/Lows — VECTORIZED (FIX 2) ────────────────────
    # Sebelumnya O(n²) double loop → sekarang numpy broadcasting O(n²) tapi
    # tanpa Python loop overhead → ~50x lebih cepat untuk n=50
    #
    # Approach: buat matrix perbandingan antar semua pasang candle,
    # ambil hanya pasang yang price-nya dalam toleransi 0.3%,
    # lalu ambil median dari level-level tersebut.
    tol = 0.003

    # Equal Highs
    h_arr = h_slice.reshape(-1, 1)                          # (n,1)
    h_mat = np.abs(h_arr - h_slice) / (h_arr + 1e-9)       # (n,n) pairwise diff %
    np.fill_diagonal(h_mat, 1.0)                            # ignore self-comparison
    eq_high_mask = h_mat < tol
    if eq_high_mask.any():
        # Ambil level dari pasangan yang match (rata-rata dari dua nilai)
        rows, cols = np.where(eq_high_mask & (np.arange(len(h_slice))[:,None] < np.arange(len(h_slice))))
        if len(rows) > 0:
            eq_high_levels = (h_slice[rows] + h_slice[cols]) / 2
            result["equal_highs"]        = float(np.median(eq_high_levels))
            result["stop_cluster_above"] = result["equal_highs"] * 1.002

    # Equal Lows
    l_arr = l_slice.reshape(-1, 1)
    l_mat = np.abs(l_arr - l_slice) / (l_arr + 1e-9)
    np.fill_diagonal(l_mat, 1.0)
    eq_low_mask = l_mat < tol
    if eq_low_mask.any():
        rows, cols = np.where(eq_low_mask & (np.arange(len(l_slice))[:,None] < np.arange(len(l_slice))))
        if len(rows) > 0:
            eq_low_levels = (l_slice[rows] + l_slice[cols]) / 2
            result["equal_lows"]         = float(np.median(eq_low_levels))
            result["stop_cluster_below"] = result["equal_lows"] * 0.998

    # ── 2. Likuiditas di atas/bawah ──────────────────
    result["liq_above"] = float(np.max(h_slice) * 1.001)
    result["liq_below"] = float(np.min(l_slice) * 0.999)

    # ── 3. Liquidity Sweep (5 candle terakhir) ───────
    ref_high = float(np.max(h_slice[:-5]))
    ref_low  = float(np.min(l_slice[:-5]))
    avg_vol  = float(np.mean(v_slice[:-5]))

    for i in range(-5, 0):
        lo, cl = lows[i],  closes[i]
        hi, ch = highs[i], closes[i]
        vol    = volumes[i]

        # Sweep Low: spike di bawah ref_low, close balik di atas → bullish
        if lo < ref_low and cl > ref_low:
            result["sweep_bull"]  = True
            result["sweep_level"] = ref_low
        # Sweep High: spike di atas ref_high, close balik di bawah → bearish
        if hi > ref_high and ch < ref_high:
            result["sweep_bear"]  = True
            result["sweep_level"] = ref_high

    # ── 4. Fake Breakout Detection ───────────────────
    # Fake Bull BO: close 2+ candle lalu di atas ref_high, candle terakhir close di bawah
    recent_3  = closes[-4:-1]
    current_c = closes[-1]
    if any(c > ref_high for c in recent_3) and current_c < ref_high:
        result["fake_bo_bull"] = True
    if any(c < ref_low for c in recent_3) and current_c > ref_low:
        result["fake_bo_bear"] = True

    return result


def detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=30) -> dict:
    """
    Order Block: candle besar terakhir sebelum impulsive move.
    Smart money meninggalkan footprint di sini.

    BUY OB: last bearish candle sebelum strong bullish impulse
    SELL OB: last bullish candle sebelum strong bearish impulse
    """
    result = {"valid": False, "ob_high": None, "ob_low": None, "ob_mid": None}

    if len(closes) < lookback:
        return result

    c = closes[-lookback:]
    h = highs[-lookback:]
    l = lows[-lookback:]
    v = volumes[-lookback:]

    avg_body = np.mean([abs(c[i]-c[i-1]) for i in range(1, len(c))])

    for i in range(len(c)-4, 0, -1):
        body_now  = abs(c[i]-c[i-1])
        body_next = abs(c[i+1]-c[i])

        if side == "BUY":
            # Cari bearish candle (c[i] < c[i-1]) diikuti 2+ strong bullish candles
            is_bearish = c[i] < c[i-1]
            is_impulse = c[i+1] > c[i] and body_next > avg_body * 1.5
            if is_bearish and is_impulse:
                result = {
                    "valid":  True,
                    "ob_high": float(h[i]),
                    "ob_low":  float(l[i]),
                    "ob_mid":  float((h[i]+l[i])/2),
                }
                break

        elif side == "SELL":
            # Cari bullish candle diikuti 2+ strong bearish candles
            is_bullish = c[i] > c[i-1]
            is_impulse = c[i+1] < c[i] and body_next > avg_body * 1.5
            if is_bullish and is_impulse:
                result = {
                    "valid":  True,
                    "ob_high": float(h[i]),
                    "ob_low":  float(l[i]),
                    "ob_mid":  float((h[i]+l[i])/2),
                }
                break

    return result


# ═══════════════════════════════════════════════
#  ENGINE 3: ENTRY PRECISION LAYER
#  Pullback, Rejection, Candle Confirmation
# ═══════════════════════════════════════════════
def check_entry_precision(closes, highs, lows, volumes,
                          side: str, structure: dict, liq_map: dict,
                          ob: dict) -> dict:
    """
    Bukan langsung entry — tunggu konfirmasi.

    Checks:
    1. Pullback entry: harga sudah pullback ke OB / structure level
    2. Rejection candle: pin bar / hammer / shooting star
    3. Candle confirmation: close di atas/bawah level kunci
    4. Volume confirmation: volume naik saat konfirmasi

    Return: precision score dan detail
    """
    result = {
        "pullback":    False,
        "rejection":   False,
        "candle_conf": False,
        "precision_score": 0,
        "entry_quality": "WAIT",   # READY | WAIT | SKIP
        "detail": [],
    }

    if len(closes) < 5:
        return result

    price     = float(closes[-1])
    prev_close= float(closes[-2])
    hi        = float(highs[-1])
    lo        = float(lows[-1])
    body      = abs(price - prev_close)
    full_range= hi - lo + 1e-9
    upper_wick= hi - max(price, prev_close)
    lower_wick= min(price, prev_close) - lo
    avg_vol   = float(np.mean(volumes[-10:-1]))
    curr_vol  = float(volumes[-1])

    # ── 1. Pullback Check ─────────────────────
    # Harga sudah kembali ke zona OB atau antara last_sl dan last_sh
    if side == "BUY":
        last_sl = structure.get("last_sl")
        ob_high = ob.get("ob_high") if ob.get("valid") else None
        if last_sl and price <= last_sl * 1.03:
            result["pullback"] = True
            result["detail"].append("Pullback ke structure support")
        elif ob_high and price <= ob_high * 1.01:
            result["pullback"] = True
            result["detail"].append("Pullback ke Order Block")
        elif liq_map.get("stop_cluster_below") and price <= liq_map["stop_cluster_below"] * 1.015:
            result["pullback"] = True
            result["detail"].append("Pullback ke stop cluster")

    elif side == "SELL":
        last_sh = structure.get("last_sh")
        ob_low  = ob.get("ob_low") if ob.get("valid") else None
        if last_sh and price >= last_sh * 0.97:
            result["pullback"] = True
            result["detail"].append("Pullback ke structure resistance")
        elif ob_low and price >= ob_low * 0.99:
            result["pullback"] = True
            result["detail"].append("Pullback ke Order Block")
        elif liq_map.get("stop_cluster_above") and price >= liq_map["stop_cluster_above"] * 0.985:
            result["pullback"] = True
            result["detail"].append("Pullback ke stop cluster")

    # ── 2. Rejection Candle ───────────────────
    # Pin bar: wick >> body
    if side == "BUY":
        # Bullish pin bar / hammer: lower wick > 2x body, upper wick kecil
        if lower_wick > body * 2 and upper_wick < body * 0.5:
            result["rejection"] = True
            result["detail"].append("Hammer / Bullish Pin Bar")
        # Bullish engulfing approximation: close jauh di atas open, body besar
        elif price > prev_close and body/full_range > 0.7:
            result["rejection"] = True
            result["detail"].append("Bullish Engulfing")

    elif side == "SELL":
        # Shooting star: upper wick > 2x body, lower wick kecil
        if upper_wick > body * 2 and lower_wick < body * 0.5:
            result["rejection"] = True
            result["detail"].append("Shooting Star / Bearish Pin Bar")
        elif price < prev_close and body/full_range > 0.7:
            result["rejection"] = True
            result["detail"].append("Bearish Engulfing")

    # ── 3. Candle Confirmation — FIX 4 ───────────────────────────────
    # SEBELUMNYA: hanya cek price > last_sl — hampir selalu True dan tidak bermakna.
    # SEKARANG: konfirmasi spesifik terhadap level kunci yang relevan:
    #   BUY  → close di atas OB high, ATAU di atas BOS level, ATAU candle bullish
    #           dengan volume tinggi setelah sweep
    #   SELL → close di bawah OB low, ATAU di bawah BOS level, ATAU candle bearish
    #           dengan volume tinggi setelah sweep
    #
    # Setidaknya 1 dari kondisi ini harus terpenuhi (bukan hanya "price > swing low").
    last_sh = structure.get("last_sh")
    last_sl = structure.get("last_sl")
    ob_high = ob.get("ob_high") if ob.get("valid") else None
    ob_low  = ob.get("ob_low")  if ob.get("valid") else None
    bos_bull = structure.get("bos") == "BULLISH" or structure.get("choch") == "BULLISH"
    bos_bear = structure.get("bos") == "BEARISH" or structure.get("choch") == "BEARISH"

    if side == "BUY":
        # Konfirmasi 1: Close di atas OB high (harga konfirmasi masuk OB zone)
        if ob_high and price > ob_high and prev_close <= ob_high:
            result["candle_conf"] = True
            result["detail"].append(f"Close konfirmasi di atas OB ${ob_high:.6f}")
        # Konfirmasi 2: Close di atas last_sh setelah BOS/CHoCH (break konfirmasi)
        elif bos_bull and last_sh and price > last_sh and prev_close <= last_sh:
            result["candle_conf"] = True
            result["detail"].append(f"Close BOS konfirmasi di atas ${last_sh:.6f}")
        # Konfirmasi 3: Bullish candle kuat + volume tinggi setelah sweep
        elif (liq_map.get("sweep_bull") and
              price > prev_close and
              body / full_range > 0.6 and
              curr_vol > avg_vol * 1.3):
            result["candle_conf"] = True
            result["detail"].append("Bullish close + volume konfirmasi post-sweep")

    elif side == "SELL":
        # Konfirmasi 1: Close di bawah OB low
        if ob_low and price < ob_low and prev_close >= ob_low:
            result["candle_conf"] = True
            result["detail"].append(f"Close konfirmasi di bawah OB ${ob_low:.6f}")
        # Konfirmasi 2: Close di bawah last_sl setelah BOS/CHoCH
        elif bos_bear and last_sl and price < last_sl and prev_close >= last_sl:
            result["candle_conf"] = True
            result["detail"].append(f"Close BOS konfirmasi di bawah ${last_sl:.6f}")
        # Konfirmasi 3: Bearish candle kuat + volume tinggi setelah sweep
        elif (liq_map.get("sweep_bear") and
              price < prev_close and
              body / full_range > 0.6 and
              curr_vol > avg_vol * 1.3):
            result["candle_conf"] = True
            result["detail"].append("Bearish close + volume konfirmasi post-sweep")

    # Bonus: volume spike (tanpa mengubah candle_conf boolean)
    if curr_vol > avg_vol * 1.5:
        result["detail"].append(f"Volume spike {curr_vol/avg_vol:.1f}x")

    # ── 4. Precision Score ────────────────────
    score = (2 if result["pullback"] else 0) + \
            (2 if result["rejection"] else 0) + \
            (2 if result["candle_conf"] else 0)
    result["precision_score"] = score

    # Entry quality decision
    if score >= 4:
        result["entry_quality"] = "READY"
    elif score >= 2:
        result["entry_quality"] = "WAIT"
    else:
        result["entry_quality"] = "SKIP"

    return result


# ═══════════════════════════════════════════════
#  ENGINE 4: SIGNAL RANKING SYSTEM
#  Tier A+/A/B based on confluence
# ═══════════════════════════════════════════════
def assign_tier(score: int, structure: dict, precision: dict, liq_map: dict) -> str:
    """
    Tier S:  score sangat tinggi DAN semua komponen valid (struktur+liq+entry)
    Tier A+: score tinggi DAN structure valid DAN entry precision ready
    Tier A:  score cukup DAN ada minimal satu structure signal
    Tier B:  score di bawah A — spekulatif
    SKIP:    score < threshold B

    FIX 5: Pisahkan liq_signal menjadi STRONG vs WEAK.
    - STRONG: sweep (real liquidity grab) atau stop_cluster yang teridentifikasi
    - WEAK:   hanya equal_highs/lows (hampir selalu ada, terlalu mudah trigger)

    Tier S membutuhkan STRONG liq signal. Equal H/L saja tidak cukup untuk S.
    Tier A bisa dengan WEAK liq signal (lebih toleran).
    """
    has_structure = (
        structure.get("bos") is not None or
        structure.get("choch") is not None
    )

    # STRONG: sweep nyata atau stop cluster teridentifikasi — ini yang benar-benar bermakna
    has_liq_strong = (
        liq_map.get("sweep_bull") or
        liq_map.get("sweep_bear") or
        (liq_map.get("stop_cluster_above") and liq_map.get("stop_cluster_below"))
    )

    # WEAK: hanya equal levels — sering muncul, tidak spesifik
    has_liq_weak = (
        liq_map.get("equal_highs") is not None or
        liq_map.get("equal_lows") is not None
    )

    has_any_liq  = has_liq_strong or has_liq_weak
    entry_ready  = precision.get("entry_quality") == "READY"

    # Tier S: semua komponen valid DAN liq signal harus STRONG (sweep/cluster)
    if (score >= TIER_THRESHOLDS["S"] and
            has_structure and has_liq_strong and entry_ready):
        return "S"

    # Tier A+: structure + entry ready (liq tidak wajib tapi score sudah tinggi)
    elif score >= TIER_THRESHOLDS["A+"] and has_structure and entry_ready:
        return "A+"

    # Tier A: structure ATAU liq signal (termasuk weak)
    elif score >= TIER_THRESHOLDS["A"] and (has_structure or has_any_liq):
        return "A"

    elif score >= TIER_THRESHOLDS["B"]:
        return "B"

    else:
        return "SKIP"


# ═══════════════════════════════════════════════
#  ENGINE 5: POSITION SIZING
#  Dynamic per tier, risk-based
# ═══════════════════════════════════════════════
def calc_position_size(entry: float, sl: float, tier: str) -> dict:
    """
    Risk-based sizing per tier:
    A+ → 2% risk
    A  → 1% risk
    B  → 0.5% risk

    Formula: units = risk_usd / sl_distance
    """
    if entry <= 0 or sl <= 0 or abs(entry-sl) < 1e-9:
        return {"size_usdt": 0, "risk_usdt": 0, "risk_pct": 0, "units": 0}

    risk_pct  = TIER_RISK_PCT.get(tier, 0.5)
    risk_usdt = ACCOUNT_BALANCE * (risk_pct / 100)
    sl_dist   = abs(entry - sl)
    units     = risk_usdt / sl_dist
    size_usdt = units * entry

    return {
        "size_usdt": round(size_usdt, 2),
        "risk_usdt": round(risk_usdt, 2),
        "risk_pct":  round(risk_pct, 2),
        "units":     round(units, 6),
    }


# ═══════════════════════════════════════════════
#  ENGINE 6: PORTFOLIO BRAIN
#  Exposure control, correlation, sector rotation
# ═══════════════════════════════════════════════

# Sector mapping — v9: diperluas agar proteksi diversifikasi lebih efektif
SECTOR_MAP = {
    # Store of Value
    "BTC": "store_of_value", "LTC": "store_of_value",
    # Smart Contract L1
    "ETH": "smart_contract", "SOL": "smart_contract", "ADA": "smart_contract",
    "AVAX": "smart_contract","NEAR": "smart_contract", "APT": "smart_contract",
    "SUI": "smart_contract", "TRX": "smart_contract",  "TON": "smart_contract",
    "SEI": "smart_contract", "HBAR": "infra",           "ALGO": "infra",
    "ICP": "infra",
    # L2 / Scaling
    "ARB": "l2", "OP": "l2", "MATIC": "l2", "STRK": "l2", "ZK": "l2",
    "IMX": "l2",
    # Exchange tokens
    "BNB": "exchange", "OKB": "exchange",
    # Payment / Remittance
    "XRP": "payment", "XLM": "payment",
    # DeFi
    "UNI": "defi",  "AAVE": "defi", "CRV": "defi",  "GMX": "defi",
    "INJ": "defi",  "DYDX": "defi", "JUP": "defi",  "PENDLE": "defi",
    "MKR": "defi",  "LDO": "defi",
    # Oracle / Data
    "LINK": "oracle", "BAND": "oracle",
    # Interop / Cross-chain
    "DOT": "interop", "ATOM": "interop", "RUNE": "interop",
    # Meme
    "DOGE": "meme", "SHIB": "meme", "PEPE": "meme",
    "FLOKI": "meme","WIF": "meme",  "BONK": "meme",
    # AI / Data
    "RNDR": "ai",  "FET": "ai",  "TAO": "ai",  "AGIX": "ai",
    "WLD": "ai",   "GRT": "ai",
    # Gaming / Metaverse
    "AXS": "gaming", "SAND": "gaming", "MANA": "gaming", "GALA": "gaming",
    # Storage / Compute
    "FIL": "storage", "AR": "storage", "STORJ": "storage",
}

MAX_SECTOR_EXPOSURE = 2   # max 2 sinyal dari sektor yang sama per cycle


def get_pair_sector(pair: str) -> str:
    base = pair.replace("_USDT", "")
    return SECTOR_MAP.get(base, "altcoin")


def portfolio_allows(pair: str, side: str, tier: str, risk_pct: float,
                     regime: dict) -> tuple:
    """
    [P6] Guard portfolio sebelum sinyal dikirim.

    Checks:
    1. Max total risk tidak terlampaui
    2. Tidak double entry di pair yang sama
    3. Sector exposure tidak berlebihan
    4. Correlation check: RISK_OFF → hanya BTC/ETH

    Return: (allowed: bool, reason: str)
    """
    global _portfolio_state

    # Guard 1: RISK_OFF regime
    if regime.get("regime") == "RISK_OFF" and pair not in SAFE_PAIRS_RISK_OFF:
        return False, "RISK_OFF: hanya BTC/ETH"

    # Guard 2: Block sesuai BTC direction
    if side == "BUY" and regime.get("block_buy"):
        return False, f"BUY diblokir: BTC 1h {regime['btc_1h_chg']:+.1f}%"
    if side == "SELL" and regime.get("block_sell"):
        return False, f"SELL diblokir: BTC 1h {regime['btc_1h_chg']:+.1f}%"

    # Guard 3: Max portfolio risk
    if _portfolio_state["total_risk_pct"] + risk_pct > MAX_PORTFOLIO_RISK_PCT:
        return False, f"Portfolio risk limit: {_portfolio_state['total_risk_pct']:.1f}% + {risk_pct}% > {MAX_PORTFOLIO_RISK_PCT}%"

    # Guard 4: No double entry same pair
    existing = _portfolio_state["open_positions"].get(pair)
    if existing == side:
        return False, f"Sudah ada posisi {side} di {pair}"

    # Guard 5: Sector exposure limit
    sector = get_pair_sector(pair)
    if _portfolio_state["sector_exposure"][sector] >= MAX_SECTOR_EXPOSURE:
        return False, f"Sector {sector} sudah {MAX_SECTOR_EXPOSURE} posisi"

    return True, ""


def register_signal_to_portfolio(pair: str, side: str, risk_pct: float):
    """Catat sinyal ke portfolio state."""
    global _portfolio_state
    _portfolio_state["total_risk_pct"] += risk_pct
    _portfolio_state["open_positions"][pair] = side
    sector = get_pair_sector(pair)
    _portfolio_state["sector_exposure"][sector] += 1


# ═══════════════════════════════════════════════
#  ENGINE 7: SELF-OPTIMIZER
#  Kill bad strategies, scale good ones
# ═══════════════════════════════════════════════
def load_strategy_performance() -> dict:
    """
    Load win rate, profit factor, drawdown per strategi dari Supabase.
    Digunakan untuk adaptive parameter tuning.
    """
    perf = {}
    try:
        res = supabase.table("signals").select("*") \
            .not_.is_("result", "null") \
            .execute()

        if not res.data or len(res.data) < 15:
            return {}

        grouped = defaultdict(list)
        for sig in res.data:
            key    = f"{sig.get('type','?')}_{sig.get('side','?')}"
            result = sig.get("result", "EXPIRED")
            entry  = float(sig.get("entry_price") or 0)
            tp1    = float(sig.get("tp1") or 0)
            sl     = float(sig.get("sl") or 0)

            if entry > 0 and tp1 > 0 and sl > 0:
                tp_dist = abs(tp1-entry)
                sl_dist = abs(sl-entry)
                rr      = tp_dist/sl_dist if sl_dist > 0 else 1.0
                pnl     = rr if result == "TP" else (-1.0 if result == "SL" else 0.0)
                grouped[key].append(pnl)

        for key, pnl_list in grouped.items():
            if len(pnl_list) < 5:
                continue
            wins      = sum(1 for p in pnl_list if p > 0)
            losses    = sum(1 for p in pnl_list if p < 0)
            total     = len(pnl_list)
            wr        = wins/total
            gross_win = sum(p for p in pnl_list if p > 0)
            gross_loss= abs(sum(p for p in pnl_list if p < 0)) + 1e-9
            pf        = gross_win/gross_loss  # Profit Factor

            equity    = np.cumsum(pnl_list)
            peak      = np.maximum.accumulate(equity)
            dd        = float(np.min(equity-peak))

            avg_win  = float(np.mean([p for p in pnl_list if p > 0])) if wins  > 0 else 0.0
            avg_loss = float(np.mean([p for p in pnl_list if p < 0])) if losses > 0 else 0.0
            expectancy = round((wr * avg_win) + ((1 - wr) * avg_loss), 3)  # E(x) per trade in R

            perf[key] = {
                "win_rate":     round(wr*100, 1),
                "profit_factor":round(pf, 2),
                "max_drawdown": round(dd, 2),
                "avg_win_r":    round(avg_win, 2),
                "avg_loss_r":   round(avg_loss, 2),
                "expectancy":   expectancy,   # positif = edge ada, negatif = hentikan
                "total":        total,
                "status":       _classify_strategy(wr, pf, dd, expectancy),
            }

    except Exception as e:
        print(f"⚠️ load_strategy_performance: {e}")

    return perf


def _classify_strategy(wr: float, pf: float, dd: float, expectancy: float = 0.0) -> str:
    """
    Klasifikasi otomatis berdasarkan statistik.
    SCALE → naikkan size
    NORMAL → jalan biasa
    CAUTION → kurangi size
    KILL → hentikan sementara
    """
    if wr >= 0.55 and pf >= 1.5 and dd > -3.0 and expectancy > 0.3:
        return "SCALE"
    elif wr >= 0.45 and pf >= 1.0 and expectancy > 0.0:
        return "NORMAL"
    elif wr >= 0.35 or pf >= 0.8:
        return "CAUTION"
    else:
        return "KILL"


def get_adaptive_risk_multiplier(strategy_key: str, perf: dict) -> float:
    """
    Adjust risk multiplier berdasarkan historical performance.
    SCALE   → 1.5x
    NORMAL  → 1.0x
    CAUTION → 0.5x
    KILL    → 0.0x (skip)
    """
    if not perf or strategy_key not in perf:
        return 1.0  # default — tidak ada data cukup
    status = perf[strategy_key]["status"]
    return {"SCALE": 1.5, "NORMAL": 1.0, "CAUTION": 0.5, "KILL": 0.0}.get(status, 1.0)




# ═══════════════════════════════════════════════
#  ENGINE 8: TRADE NARRATIVE
#  "Kenapa trade ini harus terjadi?" — wajib per sinyal
# ═══════════════════════════════════════════════
def build_trade_narrative(side: str, structure: dict, liq_map: dict,
                           precision: dict, ob: dict, tier: str) -> str:
    """
    Bangun reasoning singkat yang menjawab:
    1. Likuiditas di mana?
    2. Siapa yang terjebak? (retail trap)
    3. Smart money kemungkinan push ke mana?

    Output: string HTML untuk dikirim via Telegram.
    """
    lines = []
    is_bull = (side == "BUY")

    # 1. Liquidity thesis
    if is_bull:
        if liq_map.get("sweep_bull"):
            lvl = liq_map.get("sweep_level", 0)
            lines.append(f"🧲 <i>Stop hunt low @ ${lvl:.6f} — liquidity terkuras, reversal potensial</i>")
        if liq_map.get("equal_lows"):
            lines.append(f"📌 <i>Equal lows @ ${liq_map['equal_lows']:.6f} — stop sell cluster di bawah level ini</i>")
        if liq_map.get("liq_below"):
            lines.append(f"⚡ <i>Liq pool bawah @ ${liq_map['liq_below']:.6f} — sudah di-sweep</i>")
    else:
        if liq_map.get("sweep_bear"):
            lvl = liq_map.get("sweep_level", 0)
            lines.append(f"🧲 <i>Stop hunt high @ ${lvl:.6f} — liquidity terkuras, reversal potensial</i>")
        if liq_map.get("equal_highs"):
            lines.append(f"📌 <i>Equal highs @ ${liq_map['equal_highs']:.6f} — stop buy cluster di atas level ini</i>")
        if liq_map.get("liq_above"):
            lines.append(f"⚡ <i>Liq pool atas @ ${liq_map['liq_above']:.6f} — sudah di-sweep</i>")

    # 2. Retail trap thesis
    if is_bull and liq_map.get("fake_bo_bear"):
        lines.append("🪤 <i>Fake bearish breakdown — seller retail terjebak, reversal squeeze BUY</i>")
    elif not is_bull and liq_map.get("fake_bo_bull"):
        lines.append("🪤 <i>Fake bullish breakout — buyer retail terjebak, reversal squeeze SELL</i>")

    # 3. Structure thesis
    bos   = structure.get("bos")
    choch = structure.get("choch")
    phase = structure.get("trend_phase", "UNKNOWN")
    if bos == ("BULLISH" if is_bull else "BEARISH"):
        lines.append(f"🏗️ <i>BOS {bos} — struktur trend dikonfirmasi ({phase})</i>")
    if choch == ("BULLISH" if is_bull else "BEARISH"):
        lines.append(f"🔄 <i>CHoCH {choch} — early reversal signal, masuk sebelum crowd</i>")

    # 4. Order Block thesis
    if ob.get("valid"):
        lines.append(f"📦 <i>OB zone ${ob['ob_low']:.6f}–${ob['ob_high']:.6f} — smart money footprint</i>")

    # 5. Entry confirmation
    ep = precision.get("entry_quality", "WAIT")
    detail = precision.get("detail", [])
    if ep == "READY":
        conf_str = " | ".join(detail[:2]) if detail else "Konfirmasi terpenuhi"
        lines.append(f"✅ <i>Entry READY — {conf_str}</i>")
    elif ep == "WAIT":
        lines.append("⏳ <i>Entry WAIT — konfirmasi belum penuh, perhatikan candle berikutnya</i>")

    # 6. Tier justification
    tier_desc = {
        "S":  "💎 TIER S — Full confluence: structure + liquidity + entry semua valid",
        "A+": "🏆 TIER A+ — High confluence, signal kuat",
        "A":  "🥇 TIER A — Valid setup, size normal",
        "B":  "🥈 TIER B — Spekulatif, size kecil, SL ketat",
    }
    if tier in tier_desc:
        lines.append(tier_desc[tier])

    if not lines:
        return ""
    return "━━━━━━━━━━━━━━━━━━\n📖 <b>Trade Narrative</b>\n" + "\n".join(lines)



# ═══════════════════════════════════════════════
#  ENGINE 9: KILL SWITCH
#  Auto-halt jika loss streak atau market chaos
# ═══════════════════════════════════════════════
def check_kill_switch() -> tuple:
    """
    Cek apakah bot harus di-halt sementara.

    Kondisi halt:
    - Loss streak >= KILL_SWITCH_LOSS_STREAK dalam KILL_SWITCH_WINDOW_HOURS terakhir

    Return: (halted: bool, reason: str)
    """
    if not KILL_SWITCH_ENABLED:
        return False, ""
    try:
        now        = datetime.now(WIB)
        since      = (now - timedelta(hours=KILL_SWITCH_WINDOW_HOURS)).isoformat()
        res        = supabase.table("signals").select("result,valid_until") \
                        .not_.is_("result", "null") \
                        .gt("valid_until", since) \
                        .order("valid_until", desc=True) \
                        .limit(20).execute()
        if not res.data:
            return False, ""

        # Ambil urutan hasil terbaru
        recent_results = [r.get("result") for r in res.data
                          if r.get("result") in ("TP", "SL")]
        if len(recent_results) < KILL_SWITCH_LOSS_STREAK:
            return False, ""

        # Cek loss streak N berturut-turut
        streak = 0
        for r in recent_results:
            if r == "SL":
                streak += 1
                if streak >= KILL_SWITCH_LOSS_STREAK:
                    reason = (f"🛑 Loss streak {streak}x dalam {KILL_SWITCH_WINDOW_HOURS}h terakhir. "
                              f"Bot di-halt sementara — evaluasi manual diperlukan.")
                    return True, reason
            else:
                break  # streak putus jika ada TP

        return False, ""
    except Exception as e:
        print(f"⚠️ check_kill_switch: {e}")
        return False, ""

# ═══════════════════════════════════════════════
#  MARKET REGIME ENGINE
# ═══════════════════════════════════════════════
def get_market_regime(client) -> dict:
    default = {
        "regime": "NEUTRAL", "btc_trend": "SIDEWAYS",
        "btc_1h_chg": 0.0, "btc_4h_chg": 0.0,
        "alt_season": False, "block_buy": False,
        "block_sell": False, "reason": "Default",
        "aggressiveness": "NORMAL",
    }
    try:
        closes_4h, _, _, _ = get_candles(client, "BTC_USDT", "4h", 210)
        closes_1h, _, _, _ = get_candles(client, "BTC_USDT", "1h", 10)
        if closes_4h is None or closes_1h is None:
            return default

        btc_price  = float(closes_4h[-1])
        ema200_btc = float(pd.Series(closes_4h).ewm(span=200, adjust=False).mean().iloc[-1])
        ema50_btc  = float(pd.Series(closes_4h).ewm(span=50,  adjust=False).mean().iloc[-1])
        btc_1h_chg = ((closes_1h[-1]-closes_1h[-2])/closes_1h[-2])*100
        btc_4h_chg = ((closes_4h[-1]-closes_4h[-5])/closes_4h[-5])*100

        btc_trend = ("BULL" if btc_price > ema200_btc and ema50_btc > ema200_btc
                     else "BEAR" if btc_price < ema200_btc and ema50_btc < ema200_btc
                     else "SIDEWAYS")

        market_data = get_coingecko_market()
        btc_dom     = market_data.get("btc_dominance", 50) if market_data else 50
        alt_season  = btc_dom < 48

        block_buy, block_sell, reason = False, False, ""

        if btc_1h_chg < BTC_1H_DROP_BLOCK:
            block_buy = True
            reason    = f"BTC turun {btc_1h_chg:.1f}% (1h)"
        elif btc_1h_chg > BTC_1H_PUMP_BLOCK:
            block_sell = True
            reason     = f"BTC naik {btc_1h_chg:.1f}% (1h)"

        if btc_trend == "BULL" and btc_4h_chg > 1.0 and not block_buy:
            regime = "RISK_ON";  aggressiveness = "HIGH"
            reason = reason or "BTC bull + 4h momentum positif"
        elif btc_trend == "BEAR" or btc_4h_chg < -4.0:
            regime = "RISK_OFF"; aggressiveness = "LOW"; block_buy = True
            reason = reason or "BTC bear — hanya BTC/ETH"
        else:
            regime = "NEUTRAL";  aggressiveness = "NORMAL"
            reason = reason or "BTC sideways"

        print(f"📡 Regime: {regime} | BTC: {btc_trend} | "
              f"1h: {btc_1h_chg:+.1f}% | 4h: {btc_4h_chg:+.1f}% | "
              f"Dom: {btc_dom:.1f}% | Mode: {aggressiveness}")

        return {
            "regime": regime, "btc_trend": btc_trend,
            "btc_1h_chg": round(btc_1h_chg, 2), "btc_4h_chg": round(btc_4h_chg, 2),
            "alt_season": alt_season, "block_buy": block_buy,
            "block_sell": block_sell, "reason": reason,
            "aggressiveness": aggressiveness,
        }
    except Exception as e:
        print(f"⚠️ Regime error: {e}")
        return default


def get_btc_volatility_state(client) -> str:
    try:
        closes, highs, lows, _ = get_candles(client, "BTC_USDT", "1h", 24)
        if closes is None: return "NORMAL"
        atr_pct = (calc_atr(closes, highs, lows)/closes[-1])*100
        if atr_pct < 0.3: return "LOW"
        if atr_pct > 4.0: return "SPIKE"
        return "NORMAL"
    except Exception:
        return "NORMAL"


# ═══════════════════════════════════════════════
#  SUPABASE — PERSISTENCE
# ═══════════════════════════════════════════════
def already_sent(pair: str, signal_type: str, timeframe: str) -> bool:
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
                strength, timeframe, valid_until,
                tier="B", size_usdt=0, risk_pct=0,
                bos=None, choch=None, sweep=None,
                trend_phase=None, ob_valid=False,
                entry_quality="WAIT"):
    try:
        now = datetime.now(WIB)
        supabase.table("signals").insert({
            "pair": pair, "type": signal_type, "side": side,
            "entry_price": entry, "tp1": tp1, "tp2": tp2, "sl": sl,
            "strength": strength, "timeframe": timeframe,
            "valid_from": now.isoformat(), "valid_until": valid_until.isoformat(),
            "tier": tier,
            "size_usdt": size_usdt, "risk_pct": risk_pct,
            "bos": bos, "choch": choch, "sweep": sweep,
            "trend_phase": trend_phase,
            "ob_valid": ob_valid,
            "entry_quality": entry_quality,
        }).execute()
    except Exception as e:
        print(f"⚠️ save_signal [{pair}]: {e}")


def update_expired_signals(client):
    try:
        now = datetime.now(WIB)
        res = supabase.table("signals").select("*") \
            .lt("valid_until", now.isoformat()) \
            .is_("result", "null") \
            .limit(20).execute()
        if not res.data: return

        tickers = {t.currency_pair: t for t in client.list_tickers()}
        updated = 0

        for sig in res.data:
            pair   = sig.get("pair"); side = sig.get("side")
            entry  = float(sig.get("entry_price") or 0)
            tp1    = float(sig.get("tp1") or 0)
            sl     = float(sig.get("sl") or 0)
            if not pair or entry <= 0: continue
            ticker  = tickers.get(pair)
            current = float(ticker.last or 0) if ticker else 0
            if current <= 0: continue

            result = ("TP" if (side=="BUY" and current>=tp1) or (side=="SELL" and current<=tp1)
                      else "SL" if (side=="BUY" and current<=sl) or (side=="SELL" and current>=sl)
                      else "EXPIRED")

            supabase.table("signals").update({"result": result}).eq("id", sig["id"]).execute()
            print(f"📝 [{pair}] {side} → {result} | Entry:{entry:.4f} Now:{current:.4f}")
            updated += 1

        if updated:
            print(f"📝 Updated {updated} expired signals")
    except Exception as e:
        print(f"⚠️ update_expired_signals: {e}")


def get_win_rate(signal_type: str):
    try:
        res = supabase.table("signals").select("result") \
            .eq("type", signal_type).not_.is_("result", "null").execute()
        if not res.data or len(res.data) < 5:
            return None
        wins = sum(1 for r in res.data if r.get("result") == "TP")
        return round((wins/len(res.data))*100, 1)
    except Exception:
        return None


# ═══════════════════════════════════════════════
#  BACKTEST / PERFORMANCE REPORT
# ═══════════════════════════════════════════════
def send_performance_report(perf: dict):
    """Kirim performance report ke Telegram."""
    if not perf:
        tg("📊 <b>Performance</b>\n<i>Data belum cukup (min 15 sinyal)</i>")
        return

    lines = ["📊 <b>PERFORMANCE REPORT — v10 Decision Engine</b>", "━━━━━━━━━━━━━━━━━━"]
    for key, s in sorted(perf.items()):
        status   = s["status"]
        emoji    = {"SCALE":"🚀","NORMAL":"✅","CAUTION":"⚠️","KILL":"❌"}.get(status,"❓")
        exp    = s.get("expectancy", 0)
        exp_em = "✅" if exp > 0 else "❌"
        lines.append(
            f"{emoji} <b>{key}</b> ({s['total']} trades) [{status}]\n"
            f"   WR: <b>{s['win_rate']}%</b> | PF: <b>{s['profit_factor']}</b> | "
            f"DD: <b>{s['max_drawdown']:.1f}R</b>\n"
            f"   E(x): {exp_em} <b>{exp:+.3f}R</b> | "
            f"AvgW: {s.get('avg_win_r',0):.2f}R | AvgL: {s.get('avg_loss_r',0):.2f}R"
        )
    lines += ["━━━━━━━━━━━━━━━━━━",
              "<i>SCALE=naikkan size | KILL=strategi dihentikan</i>"]
    tg("\n".join(lines))


# ═══════════════════════════════════════════════
#  SIGNAL QUEUE & FLUSH
# ═══════════════════════════════════════════════
def queue_signal(**kwargs):
    """Tambah sinyal ke queue global."""
    _signal_queue.append(kwargs)


def flush_signal_queue(perf: dict, regime: dict = None):
    """
    Kirim top MAX_SIGNALS_PER_CYCLE signals.
    Sort: Tier S > A+ > A > B, kemudian score dalam tier.
    regime: live market regime dari run() — FIX v6 (v5 hardcode NEUTRAL)
    """
    if regime is None:
        regime = {"regime": "NEUTRAL", "block_buy": False, "block_sell": False, "btc_1h_chg": 0.0}
    if not _signal_queue:
        print("  📭 Tidak ada sinyal dalam queue")
        return

    tier_order = {"S": 0, "A+": 1, "A": 2, "B": 3}
    sorted_q   = sorted(
        _signal_queue,
        key=lambda x: (tier_order.get(x.get("tier","B"), 9), -x.get("score", 0))
    )

    print(f"\n📨 Queue: {len(sorted_q)} signals → top {MAX_SIGNALS_PER_CYCLE}")
    # Diagnostic: print semua candidates dan alasan skip
    for _s in sorted_q[:10]:
        print(f"  candidate: {_s['pair']} [{_s.get('signal_type')}|{_s.get('tier')}] score={_s.get('score')}")
    sent = 0

    for sig in sorted_q:
        if sent >= MAX_SIGNALS_PER_CYCLE:
            break

        pair         = sig["pair"]
        signal_type  = sig["signal_type"]
        side         = sig["side"]
        entry        = sig["entry"]
        tp1          = sig["tp1"]
        tp2          = sig["tp2"]
        sl           = sig["sl"]
        strength     = sig["strength"]
        timeframe    = sig["timeframe"]
        valid_minutes= sig["valid_minutes"]
        tier         = sig["tier"]
        score        = sig["score"]
        sources      = sig.get("sources", "")
        extra        = sig.get("extra", "")
        structure    = sig.get("structure", {})
        liq_map      = sig.get("liq_map", {})
        precision    = sig.get("precision", {})
        ob           = sig.get("ob", {})
        entry_info   = sig.get("entry_info", {})   # v7: entry method

        if already_sent(pair, signal_type, timeframe):
            print(f"  ⏭️ SKIP already_sent: {pair} [{signal_type}|{timeframe}]")
            continue

        # ── Portfolio guard ───────────────────
        strategy_key = f"{signal_type}_{side}"
        risk_mult    = get_adaptive_risk_multiplier(strategy_key, perf)
        if risk_mult == 0.0:
            print(f"  🚫 {pair} [{signal_type}] KILL — strategi dimatikan oleh optimizer")
            continue

        sizing    = calc_position_size(entry, sl, tier)
        adj_risk  = round(sizing["risk_pct"] * risk_mult, 2)
        adj_size  = round(sizing["size_usdt"] * risk_mult, 2)

        portfolio_ok, port_reason = portfolio_allows(
            pair, side, tier, adj_risk, regime   # FIX v6: live regime, bukan hardcode NEUTRAL
        )
        if not portfolio_ok:
            print(f"  ⏭️ {pair} portfolio guard: {port_reason}")
            continue

        now         = datetime.now(WIB)
        # v7: PULLBACK dan BREAKOUT butuh waktu lebih lama (harga harus sampai dulu)
        e_method_flush = entry_info.get("method", "MARKET")
        if e_method_flush in ("PULLBACK", "BREAKOUT"):
            effective_minutes = valid_minutes * 2   # 2x lipat waktu
        else:
            effective_minutes = valid_minutes
        valid_until = now + timedelta(minutes=effective_minutes)

        # ── R/R Check ─────────────────────────
        pct_tp1 = abs((tp1-entry)/entry*100)
        pct_tp2 = abs((tp2-entry)/entry*100)
        pct_sl  = abs((sl-entry)/entry*100)
        rr      = round(pct_tp1/pct_sl, 1) if pct_sl > 0 else 0

        min_rr_needed = MIN_RR.get(signal_type, {}).get(tier, 1.2)
        if rr < min_rr_needed:
            print(f"  ⏭️ SKIP R/R {pair} [{signal_type}|{tier}] rr={rr} < min={min_rr_needed} | entry={entry:.4f} tp1={tp1:.4f} sl={sl:.4f}")
            continue

        # ── SANITY CHECK: arah TP dan SL harus benar ─────────
        # BUY: TP harus di ATAS entry, SL harus di BAWAH entry
        # SELL: TP harus di BAWAH entry, SL harus di ATAS entry
        if side == "BUY":
            if tp1 <= entry:
                print(f"  🚫 {pair} [{signal_type}] INVALID: TP1 ({tp1:.6f}) <= Entry ({entry:.6f}) — discarded")
                continue
            if sl >= entry:
                print(f"  🚫 {pair} [{signal_type}] INVALID: SL ({sl:.6f}) >= Entry ({entry:.6f}) — discarded")
                continue
        elif side == "SELL":
            if tp1 >= entry:
                print(f"  🚫 {pair} [{signal_type}] INVALID: TP1 ({tp1:.6f}) >= Entry ({entry:.6f}) — discarded")
                continue
            if sl <= entry:
                print(f"  🚫 {pair} [{signal_type}] INVALID: SL ({sl:.6f}) <= Entry ({entry:.6f}) — discarded")
                continue

        win_rate  = get_win_rate(signal_type)
        wr_str    = f"{win_rate}%" if win_rate else "Akumulasi..."
        tier_emoji= {"S":"💎","A+":"🏆","A":"🥇","B":"🥈"}.get(tier,"🎯")

        # ── Adaptive size display ─────────────
        size_note = ""
        if risk_mult != 1.0:
            st_status = perf.get(strategy_key, {}).get("status", "")
            size_note = f" [x{risk_mult} — {st_status}]"

        # ── Structure summary ─────────────────
        struct_lines = []
        if structure.get("bos"):
            struct_lines.append(f"🏗️ BOS {structure['bos']} | Phase: {structure.get('trend_phase')}")
        if structure.get("choch"):
            struct_lines.append(f"🔄 CHoCH {structure['choch']} — Early Reversal")
        if liq_map.get("sweep_bull") or liq_map.get("sweep_bear"):
            lvl = liq_map.get("sweep_level", 0)
            struct_lines.append(f"🧲 Liq Sweep @ ${lvl:.6f}")
        if liq_map.get("equal_highs"):
            struct_lines.append(f"📌 Equal Highs: ${liq_map['equal_highs']:.6f}")
        if liq_map.get("equal_lows"):
            struct_lines.append(f"📌 Equal Lows: ${liq_map['equal_lows']:.6f}")
        if liq_map.get("fake_bo_bull"):
            struct_lines.append("⚠️ Fake Bullish Breakout terdeteksi")
        if liq_map.get("fake_bo_bear"):
            struct_lines.append("⚠️ Fake Bearish Breakdown terdeteksi")
        if ob.get("valid"):
            struct_lines.append(f"📦 Order Block: ${ob['ob_low']:.6f}–${ob['ob_high']:.6f}")

        # ── Trade Narrative ───────────────────
        # Gunakan Engine 8: build_trade_narrative() — narrative_lines manual dihapus (duplikasi)
        narrative       = build_trade_narrative(side, structure, liq_map, precision, ob, tier)
        precision_lines = precision.get("detail", [])
        emoji_side = "🟢 BUY" if side == "BUY" else "🔴 SELL"
        emoji_type = {"SCALPING":"⚡","INTRADAY":"📈","SWING":"🌊","MOONSHOT":"🚀"}.get(signal_type,"🎯")

        # ── Entry Method Block (v7) ──────────────
        e_method = entry_info.get("method", "MARKET")
        e_emoji  = entry_info.get("emoji", "✅")
        e_trigger= entry_info.get("trigger", "")
        e_inval  = entry_info.get("invalidate", "")
        e_zone   = entry_info.get("entry_zone")

        method_label = {
            "MARKET":   "✅ MARKET — Entry Sekarang",
            "PULLBACK": "⏬ LIMIT PULLBACK — Tunggu Retrace",
            "BREAKOUT": "🚀 LIMIT BREAKOUT — Tunggu Konfirmasi",
        }.get(e_method, e_method)

        # Untuk PULLBACK/BREAKOUT: tunjukkan entry zone jika ada
        zone_str = ""
        if e_zone:
            zone_str = f"\nZona:      <b>${e_zone[0]:.6f} – ${e_zone[1]:.6f}</b>"

        msg = (
            f"{emoji_type} <b>{tier_emoji} [{tier}] SIGNAL {emoji_side} — {signal_type}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Pair:      <b>{pair.replace('_USDT','/USDT')}</b>\n"
            f"⏰ Valid:   {now.strftime('%H:%M')} → {valid_until.strftime('%H:%M')} WIB\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📌 <b>CARA ENTRY</b>\n"
            f"Metode:    <b>{method_label}</b>\n"
            f"Entry:     <b>${entry:.6f}</b>{zone_str}\n"
            f"Trigger:   <i>{e_trigger}</i>\n"
            f"Batal jika: <i>{e_inval}</i>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"TP1:       ${tp1:.6f} <i>(+{pct_tp1:.1f}%)</i>\n"
            f"TP2:       ${tp2:.6f} <i>(+{pct_tp2:.1f}%)</i>\n"
            f"SL:        ${sl:.6f} <i>(-{pct_sl:.1f}%)</i>\n"
            f"R/R:       <b>1:{rr}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Tier:      <b>{tier}</b> | Score: {score}\n"
            f"Strength:  {'⭐'*strength}{'☆'*(5-strength)}\n"
            f"Win Rate:  <i>{wr_str}</i>\n"
            f"TF:        {timeframe}\n"
            f"💰 Size:   ${adj_size}{size_note}\n"
            f"   Risk:   {adj_risk}% = ${round(sizing['risk_usdt']*risk_mult,2)}\n"
            f"Data:      {sources}"
        )

        if struct_lines:
            msg += "\n━━━━━━━━━━━━━━━━━━\n🏗️ <b>Structure</b>\n" + "\n".join(struct_lines)
        if precision_lines:
            msg += "\n🎯 <b>Entry Precision</b>\n" + "\n".join(precision_lines)
        if narrative:
            msg += f"\n{narrative}"
        if extra:
            msg += f"\n{extra}"

        tg(msg)
        save_signal(
            pair, signal_type, side, entry, tp1, tp2, sl,
            strength, timeframe, valid_until,
            tier=tier, size_usdt=adj_size, risk_pct=adj_risk,
            bos=structure.get("bos"), choch=structure.get("choch"),
            sweep=("BULL" if liq_map.get("sweep_bull") else
                   "BEAR" if liq_map.get("sweep_bear") else None),
            trend_phase=structure.get("trend_phase"),
            ob_valid=ob.get("valid", False),
            entry_quality=precision.get("entry_quality","WAIT"),
        )
        register_signal_to_portfolio(pair, side, adj_risk)
        print(f"  ✅ [{sent+1}] [{tier}] {signal_type} {side} → {pair} | "
              f"R/R:1:{rr} | Score:{score} | Size:${adj_size}{size_note}")
        sent += 1

    _signal_queue.clear()


# ═══════════════════════════════════════════════
#  HELPER
# ═══════════════════════════════════════════════
# Suffix yang mengindikasikan tokenized stock / ETF / leveraged product
# Gate.io memakai suffix seperti HON (UnitedHealth), NVDAX, dll
ETF_STOCK_SUFFIXES = {
    # Tokenized US stocks (Ondo, backed by real shares)
    "HON","NVDAX","TSLAX","AAPLX","AMZNX","MSFX","METAX","GOOGX",
    "COINX","MSTRX","NVDAX","AMZX","GOOQX","MSLX","NFLXX","ARKX",
    # Spot ETF tokens
    "BTCB","ETHB","SOLB",
    # Other tokenized / synthetic
    "PAXG","XAUT",
}

def is_valid(pair: str) -> bool:
    if not pair.endswith("_USDT"): return False
    base = pair.replace("_USDT","")
    # Filter stablecoin & leveraged
    if any(b in base for b in BLACKLIST): return False
    # Filter tokenized stocks / ETF (akhiran dikenal)
    for suffix in ETF_STOCK_SUFFIXES:
        if base.endswith(suffix) and base != suffix:
            return False
    # Filter pair yang namanya terlalu panjang (>10 char) — biasanya synthetic/niche
    # tapi tidak terlalu ketat karena ada JELLYJELLY dll
    return True


def wscore(conditions: list) -> int:
    return sum(W.get(key, 0) for cond, key in conditions if cond)


def volatility_ok(atr: float, price: float, mode: str) -> tuple:
    atr_pct = (atr/price)*100 if price > 0 else 0
    mn, mx  = ATR_MIN_PCT.get(mode, 0.3), ATR_MAX_PCT.get(mode, 8.0)
    if atr_pct < mn: return False, f"Vol rendah ({atr_pct:.2f}%)"
    if atr_pct > mx: return False, f"Vol terlalu tinggi ({atr_pct:.2f}%)"
    return True, ""


def resolve_entry(price: float, side: str, atr: float,
                  ob: dict, liq_map: dict,
                  structure: dict, precision: dict) -> dict:
    """
    [v7] Tentukan entry method dan zone secara eksplisit.

    Prioritas:
    1. MARKET NOW     — entry precision READY, price sudah di zona ideal
    2. LIMIT PULLBACK — harga belum turun ke OB/stop-cluster (BUY) atau
                        belum naik ke OB/stop-cluster (SELL)
    3. LIMIT BREAKOUT — harga belum break struktur, tunggu konfirmasi close

    Output: dict {
        method:       "MARKET" | "PULLBACK" | "BREAKOUT"
        entry_price:  float (harga ideal)
        entry_zone:   (float, float) atau None  (range zona entry)
        trigger:      str  (kondisi yang harus terpenuhi)
        invalidate:   str  (kondisi yang membatalkan setup)
        emoji:        str
    }
    """
    is_bull      = (side == "BUY")
    entry_ready  = precision.get("entry_quality") == "READY"
    ob_valid     = ob.get("valid", False)
    last_sh      = structure.get("last_sh")
    last_sl      = structure.get("last_sl")

    # ── BREAKOUT entry ────────────────────────────────────
    # Price belum break last_sh (BUY) atau last_sl (SELL)
    if is_bull and last_sh and price < last_sh * 0.998:
        bo_level = last_sh
        return {
            "method":      "BREAKOUT",
            "entry_price": round(bo_level * 1.001, 8),   # sedikit di atas level
            "entry_zone":  (round(bo_level * 1.000, 8), round(bo_level * 1.003, 8)),
            "trigger":     f"Tunggu candle CLOSE di atas ${bo_level:.6f} (last SH)",
            "invalidate":  f"Batal jika harga turun di bawah ${last_sl:.6f}" if last_sl else "Batal jika SL tembus",
            "emoji":       "🚀",
        }
    if not is_bull and last_sl and price > last_sl * 1.002:
        bo_level = last_sl
        return {
            "method":      "BREAKOUT",
            "entry_price": round(bo_level * 0.999, 8),
            "entry_zone":  (round(bo_level * 0.997, 8), round(bo_level * 1.000, 8)),
            "trigger":     f"Tunggu candle CLOSE di bawah ${bo_level:.6f} (last SL)",
            "invalidate":  f"Batal jika harga naik di atas ${last_sh:.6f}" if last_sh else "Batal jika SL tembus",
            "emoji":       "🚀",
        }

    # ── PULLBACK entry ────────────────────────────────────
    # Price sudah melewati level, tapi belum pullback ke zona ideal
    if is_bull:
        # Zona ideal: OB mid atau stop-cluster bawah
        if ob_valid and price > ob["ob_high"] * 1.005:
            pb_level = ob["ob_mid"]
            pb_zone  = (ob["ob_low"], ob["ob_high"])
            trigger  = f"Tunggu pullback ke OB ${ob['ob_low']:.6f}–${ob['ob_high']:.6f}"
            inval    = f"Batal jika candle close di bawah ${ob['ob_low'] * 0.998:.6f}"
        elif liq_map.get("stop_cluster_below") and price > liq_map["stop_cluster_below"] * 1.015:
            pb_level = liq_map["stop_cluster_below"] * 1.003
            pb_zone  = (liq_map["stop_cluster_below"] * 0.999, liq_map["stop_cluster_below"] * 1.006)
            trigger  = f"Tunggu pullback ke stop cluster ${liq_map['stop_cluster_below']:.6f}"
            inval    = f"Batal jika price break di bawah ${liq_map['stop_cluster_below'] * 0.995:.6f}"
        elif last_sl and price > last_sl * 1.03:
            pb_level = last_sl * 1.005
            pb_zone  = (last_sl, last_sl * 1.015)
            trigger  = f"Tunggu pullback ke support ${last_sl:.6f}"
            inval    = f"Batal jika close di bawah ${last_sl * 0.997:.6f}"
        else:
            pb_level = None
            pb_zone  = None
            trigger  = ""
            inval    = ""

        if pb_level and not entry_ready:
            return {
                "method":      "PULLBACK",
                "entry_price": round(pb_level, 8),
                "entry_zone":  (round(pb_zone[0], 8), round(pb_zone[1], 8)) if pb_zone else None,
                "trigger":     trigger,
                "invalidate":  inval,
                "emoji":       "⏬",
            }
    else:
        # SELL pullback: tunggu retrace ke OB atau stop-cluster atas
        if ob_valid and price < ob["ob_low"] * 0.995:
            pb_level = ob["ob_mid"]
            pb_zone  = (ob["ob_low"], ob["ob_high"])
            trigger  = f"Tunggu rally ke OB ${ob['ob_low']:.6f}–${ob['ob_high']:.6f}"
            inval    = f"Batal jika candle close di atas ${ob['ob_high'] * 1.002:.6f}"
        elif liq_map.get("stop_cluster_above") and price < liq_map["stop_cluster_above"] * 0.985:
            pb_level = liq_map["stop_cluster_above"] * 0.997
            pb_zone  = (liq_map["stop_cluster_above"] * 0.994, liq_map["stop_cluster_above"] * 1.001)
            trigger  = f"Tunggu rally ke stop cluster ${liq_map['stop_cluster_above']:.6f}"
            inval    = f"Batal jika price break di atas ${liq_map['stop_cluster_above'] * 1.005:.6f}"
        elif last_sh and price < last_sh * 0.97:
            pb_level = last_sh * 0.995
            pb_zone  = (last_sh * 0.985, last_sh)
            trigger  = f"Tunggu rally ke resistance ${last_sh:.6f}"
            inval    = f"Batal jika close di atas ${last_sh * 1.003:.6f}"
        else:
            pb_level = None
            pb_zone  = None
            trigger  = ""
            inval    = ""

        if pb_level and not entry_ready:
            return {
                "method":      "PULLBACK",
                "entry_price": round(pb_level, 8),
                "entry_zone":  (round(pb_zone[0], 8), round(pb_zone[1], 8)) if pb_zone else None,
                "trigger":     trigger,
                "invalidate":  inval,
                "emoji":       "⏫",
            }

    # ── MARKET entry ─────────────────────────────────────
    # Entry sekarang — harga sudah di zona + precision ready
    if is_bull:
        trigger = "Entry sekarang — semua konfirmasi terpenuhi"
        inval   = f"Batal jika harga turun di bawah ${last_sl:.6f}" if last_sl else "Pantau SL ketat"
    else:
        trigger = "Entry sekarang — semua konfirmasi terpenuhi"
        inval   = f"Batal jika harga naik di atas ${last_sh:.6f}" if last_sh else "Pantau SL ketat"

    return {
        "method":      "MARKET",
        "entry_price": price,
        "entry_zone":  None,
        "trigger":     trigger,
        "invalidate":  inval,
        "emoji":       "✅",
    }


# ═══════════════════════════════════════════════
#  MULTI-TIMEFRAME FILTER
# ═══════════════════════════════════════════════
def get_htf_bias(client, pair: str) -> str:
    """
    [v9] Cek bias 1h untuk filter scalping 5m.
    Scalping BUY hanya jika 1h BULLISH atau NEUTRAL.
    Scalping SELL hanya jika 1h BEARISH atau NEUTRAL.

    Kriteria: price > EMA20 > EMA50 = BULLISH
              price < EMA20 < EMA50 = BEARISH
              else = NEUTRAL
    """
    try:
        closes, highs, lows, _ = get_candles(client, pair, "1h", 55)
        if closes is None:
            return "NEUTRAL"
        ema20 = calc_ema(closes, 20)
        ema50 = calc_ema(closes, 50)
        price = float(closes[-1])
        if price > ema20 and ema20 > ema50:
            return "BULLISH"
        if price < ema20 and ema20 < ema50:
            return "BEARISH"
        return "NEUTRAL"
    except Exception:
        return "NEUTRAL"


# ═══════════════════════════════════════════════
#  STRATEGY: SCALPING — 5m
# ═══════════════════════════════════════════════
def check_scalping(client, pair, price, fg, ob_ratio, funding,
                   liq, oi_signal, regime):
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
    vol_ratio = volumes[-1]/vol_avg if vol_avg > 0 else 0

    # Engine 1: Structure
    structure  = detect_structure(closes, highs, lows, lookback=40)
    # Engine 2: Liquidity
    liq_map    = detect_liquidity_map(closes, highs, lows, volumes, lookback=20)

    vol_ok, _ = volatility_ok(atr, price, "SCALPING")
    if not vol_ok: return

    # [v9] MTF Filter: ambil bias 1h sekali untuk pair ini
    htf_bias = get_htf_bias(client, pair)

    for side in ["BUY", "SELL"]:
        is_bull = (side == "BUY")

        # [v9] MTF Filter: scalping HARUS sejalan atau netral dengan trend 1h
        # BUY skip jika 1h BEARISH, SELL skip jika 1h BULLISH
        if is_bull  and htf_bias == "BEARISH":
            print(f"  ↩️ {pair} [SCALP BUY] skip — HTF 1h BEARISH")
            continue
        if not is_bull and htf_bias == "BULLISH":
            print(f"  ↩️ {pair} [SCALP SELL] skip — HTF 1h BULLISH")
            continue

        # Engine 1: Harus ada minimal satu structure signal
        has_structure_signal = (
            (is_bull and (structure.get("bos")=="BULLISH" or structure.get("choch")=="BULLISH")) or
            (not is_bull and (structure.get("bos")=="BEARISH" or structure.get("choch")=="BEARISH"))
        )
        has_liq_signal = (
            (is_bull and liq_map.get("sweep_bull")) or
            (not is_bull and liq_map.get("sweep_bear"))
        )

        if not has_structure_signal and not has_liq_signal:
            continue  # SKIP — tidak ada structure trigger

        # Engine 2: Fake BO filter
        if is_bull and liq_map.get("fake_bo_bull"):
            continue   # jebakan buyer — skip BUY
        if not is_bull and liq_map.get("fake_bo_bear"):
            continue   # jebakan seller — skip SELL

        # Engine 2: Order Block
        ob = detect_order_block(closes, highs, lows, volumes, side=side, lookback=20)

        # Engine 3: Entry Precision
        precision = check_entry_precision(
            closes, highs, lows, volumes, side, structure, liq_map, ob)
        if precision["entry_quality"] == "SKIP":
            continue  # tidak cukup konfirmasi

        # Build condition list
        if is_bull:
            conditions = [
                (structure.get("bos")  == "BULLISH",          "bos"),
                (structure.get("choch")== "BULLISH",          "choch"),
                (liq_map.get("sweep_bull"),                   "liq_sweep"),
                (liq_map.get("equal_lows") is not None,       "equal_hl"),
                (ob.get("valid"),                             "order_block"),
                (precision.get("pullback"),                   "pullback"),
                (precision.get("rejection"),                  "rejection"),
                (precision.get("candle_conf"),                "candle_conf"),
                (rsi < 35,                                    "rsi_extreme"),
                (stoch_rsi < 0.2,                            "stoch_rsi"),
                (ob_ratio > 1.2,                              "ob_ratio"),
                (fg < 25,                                     "fg_extreme"),
                (vol_ratio > 2.5,                            "vol_spike"),
                (divergence == "BULLISH",                    "divergence"),
                (price <= support*1.02,                      "support_res"),
                (funding and funding < -0.001,               "funding"),
                (price > vwap,                               "vwap"),
                (liq.get("liq_bias") == "BUY",               "liq_cluster"),
                (oi_signal in ("STRONG_BUY","SQUEEZE"),      "oi_signal"),
            ]
            tp1 = price + atr * 2.5   # [v10 FIX] ATR-based, bukan fixed 5% (terlalu jauh untuk 5m)
            tp2 = price + atr * 4.5   # [v10 FIX] ATR-based TP2
            sl  = max(price - atr*1.8, price*0.97)   # [v10 FIX] SL lebih ketat (1.8x vs 2.0x)
            if liq_map.get("sweep_level"):
                sl = min(sl, liq_map["sweep_level"] * 0.998)
        else:
            conditions = [
                (structure.get("bos")  == "BEARISH",          "bos"),
                (structure.get("choch")== "BEARISH",          "choch"),
                (liq_map.get("sweep_bear"),                   "liq_sweep"),
                (liq_map.get("equal_highs") is not None,      "equal_hl"),
                (ob.get("valid"),                             "order_block"),
                (precision.get("pullback"),                   "pullback"),
                (precision.get("rejection"),                  "rejection"),
                (precision.get("candle_conf"),                "candle_conf"),
                (rsi > 68,                                    "rsi_extreme"),
                (stoch_rsi > 0.8,                            "stoch_rsi"),
                (ob_ratio < 0.8,                              "ob_ratio"),
                (fg > 65,                                     "fg_extreme"),
                (vol_ratio > 2.5,                            "vol_spike"),
                (divergence == "BEARISH",                    "divergence"),
                (price >= resistance*0.98,                   "support_res"),
                (funding and funding > 0.001,                "funding"),
                (price < vwap,                               "vwap"),
                (liq.get("liq_bias") == "SELL",              "liq_cluster"),
                (oi_signal == "STRONG_SELL",                 "oi_signal"),
            ]
            tp1 = price - atr * 2.5   # [v10 FIX] ATR-based
            tp2 = price - atr * 4.5   # [v10 FIX] ATR-based TP2
            sl  = min(price + atr*1.8, price*1.03)   # [v10 FIX] SL lebih ketat
            if liq_map.get("sweep_level"):
                sl = max(sl, liq_map["sweep_level"] * 1.002)

        score     = wscore(conditions)
        tier      = assign_tier(score, structure, precision, liq_map)
        stars_val = min(5, max(1, score//2))

        print(f"  📊 {pair} [SCALP {side}] score={score} tier={tier} "
              f"bos={structure.get('bos')} entry={precision['entry_quality']}")

        if tier == "SKIP":
            continue

        extra = f"📡 <i>Regime: {regime['regime']} | BTC 1h: {regime['btc_1h_chg']:+.1f}%</i>"
        if divergence == ("BULLISH" if is_bull else "BEARISH"):
            extra += f"\n🔀 <i>{'Bullish' if is_bull else 'Bearish'} RSI Divergence!</i>"
        if fg < 20 and is_bull:
            extra += f"\n😱 <i>Extreme Fear {fg}</i>"

        entry_info = resolve_entry(price, side, atr, ob, liq_map, structure, precision)
        # Gunakan entry_price dari resolve_entry
        final_entry = entry_info["entry_price"]
        # [v10 FIX] Recalculate TP/SL dari final_entry pakai ATR-based (bukan fixed %)
        if side == "BUY":
            tp1_f = final_entry + atr * 2.5
            tp2_f = final_entry + atr * 4.5
            sl_f  = max(final_entry - atr*1.8, final_entry*0.97)
        else:
            tp1_f = final_entry - atr * 2.5
            tp2_f = final_entry - atr * 4.5
            sl_f  = min(final_entry + atr*1.8, final_entry*1.03)
        queue_signal(
            pair=pair, signal_type="SCALPING", side=side,
            entry=final_entry, tp1=tp1_f, tp2=tp2_f, sl=sl_f,
            strength=stars_val, timeframe="5m", valid_minutes=30,  # [v10 FIX] 15→30 menit
            tier=tier, score=score,
            sources="Gate.io · Structure · Liq Engine · Entry Precision",
            extra=extra, structure=structure, liq_map=liq_map,
            precision=precision, ob=ob, entry_info=entry_info,
        )


# ═══════════════════════════════════════════════
#  STRATEGY: INTRADAY — 1h
# ═══════════════════════════════════════════════
def check_intraday(client, pair, price, fg, ob_ratio, funding,
                   trending, market_data, liq, oi_signal, regime):
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

    structure  = detect_structure(closes, highs, lows, lookback=60)
    liq_map    = detect_liquidity_map(closes, highs, lows, volumes, lookback=30)

    symbol      = pair.replace("_USDT","")
    is_trending = symbol in trending

    vol_ok, _ = volatility_ok(atr, price, "INTRADAY")
    if not vol_ok: return

    for side in ["BUY", "SELL"]:
        is_bull = (side == "BUY")

        # Structure must trigger first
        has_struct = (
            (is_bull and (structure.get("bos")=="BULLISH" or structure.get("choch")=="BULLISH"
                          or liq_map.get("sweep_bull"))) or
            (not is_bull and (structure.get("bos")=="BEARISH" or structure.get("choch")=="BEARISH"
                               or liq_map.get("sweep_bear")))
        )
        if not has_struct: continue

        # Macro filter: intraday tidak boleh jalan RISK_OFF (kecuali safe pair)
        if regime.get("regime") == "RISK_OFF" and pair not in SAFE_PAIRS_RISK_OFF:
            continue
        if is_bull and regime.get("block_buy"): continue
        if not is_bull and regime.get("block_sell"): continue

        # Fake BO filter
        if is_bull and liq_map.get("fake_bo_bull"): continue
        if not is_bull and liq_map.get("fake_bo_bear"): continue

        # MACD confirmation (sebagai filter, bukan trigger)
        if is_bull and macd <= msig: continue
        if not is_bull and macd >= msig: continue

        ob        = detect_order_block(closes, highs, lows, volumes, side=side, lookback=30)
        precision = check_entry_precision(closes, highs, lows, volumes,
                                          side, structure, liq_map, ob)
        if precision["entry_quality"] == "SKIP": continue

        if is_bull:
            conditions = [
                (structure.get("bos")  == "BULLISH",          "bos"),
                (structure.get("choch")== "BULLISH",          "choch"),
                (liq_map.get("sweep_bull"),                   "liq_sweep"),
                (liq_map.get("equal_lows") is not None,       "equal_hl"),
                (liq_map.get("stop_cluster_below") is not None,"stop_cluster"),
                (ob.get("valid"),                             "order_block"),
                (precision.get("pullback"),                   "pullback"),
                (precision.get("rejection"),                  "rejection"),
                (precision.get("candle_conf"),                "candle_conf"),
                (rsi < 35,                                    "rsi_extreme"),
                (stoch_rsi < 0.25,                           "stoch_rsi"),
                (price <= bb_low,                            "bb_extreme"),
                (fg < 25,                                     "fg_extreme"),
                (ob_ratio > 1.1,                              "ob_ratio"),
                (is_trending,                                 "trending"),
                (ema20 > ema50,                               "ema_cross"),
                (divergence == "BULLISH",                    "divergence"),
                (poc and price <= poc,                       "poc"),
                (price <= support*1.03,                      "support_res"),
                (funding and funding < -0.001,               "funding"),
                (price > vwap,                               "vwap"),
                (ichi.get("above_cloud"),                    "ichimoku"),
                (liq.get("liq_bias") == "BUY",               "liq_cluster"),
                (oi_signal in ("STRONG_BUY","SQUEEZE"),      "oi_signal"),
            ]
            tp1 = price * 1.07  # FIX: tidak di-cap ke resistance (resistance bisa < entry)
            tp2 = price * 1.15  # FIX: tidak di-cap ke resistance
            sl  = max(price - atr*2.5, price*0.94)
            if liq_map.get("sweep_level"): sl = min(sl, liq_map["sweep_level"]*0.997)

            # PREDICTIVE ENTRY: entry di OB/stop-cluster zone, bukan market price
            if ob.get("valid") and price > ob["ob_high"] * 1.005:
                entry_price = ob["ob_mid"]
                entry_note  = f"Limit @ OB ${ob['ob_mid']:.6f}"
                tp1 = entry_price * 1.07
                tp2 = entry_price * 1.15
                sl  = max(entry_price - atr*2.5, entry_price*0.94)
            elif liq_map.get("stop_cluster_below") and price > liq_map["stop_cluster_below"] * 1.01:
                entry_price = liq_map["stop_cluster_below"] * 1.003
                entry_note  = f"Limit @ ${entry_price:.6f}"
                tp1 = entry_price * 1.07
                tp2 = entry_price * 1.15
                sl  = max(entry_price - atr*2.5, entry_price*0.94)
            else:
                entry_price = price
                entry_note  = "Market entry"
        else:
            conditions = [
                (structure.get("bos")  == "BEARISH",          "bos"),
                (structure.get("choch")== "BEARISH",          "choch"),
                (liq_map.get("sweep_bear"),                   "liq_sweep"),
                (liq_map.get("equal_highs") is not None,      "equal_hl"),
                (liq_map.get("stop_cluster_above") is not None,"stop_cluster"),
                (ob.get("valid"),                             "order_block"),
                (precision.get("pullback"),                   "pullback"),
                (precision.get("rejection"),                  "rejection"),
                (precision.get("candle_conf"),                "candle_conf"),
                (rsi > 65,                                    "rsi_extreme"),
                (stoch_rsi > 0.75,                           "stoch_rsi"),
                (price >= bb_high,                           "bb_extreme"),
                (fg > 60 or fg < 20,                         "fg_extreme"),
                (ob_ratio < 0.9,                              "ob_ratio"),
                (divergence == "BEARISH",                    "divergence"),
                (poc and price >= poc,                       "poc"),
                (price >= resistance*0.97,                   "support_res"),
                (funding and funding > 0.001,                "funding"),
                (price < vwap,                               "vwap"),
                (ichi.get("below_cloud"),                    "ichimoku"),
                (liq.get("liq_bias") == "SELL",              "liq_cluster"),
                (oi_signal == "STRONG_SELL",                 "oi_signal"),
            ]
            tp1 = price * 0.93  # FIX: tidak di-cap ke support (support bisa > entry SELL)
            tp2 = price * 0.87  # FIX: tidak di-cap ke support
            sl  = min(price + atr*2.5, price*1.06)

            # PREDICTIVE ENTRY SELL
            if ob.get("valid") and price < ob["ob_low"] * 0.995:
                entry_price = ob["ob_mid"]
                entry_note  = f"Limit @ OB ${ob['ob_mid']:.6f}"
                tp1 = entry_price * 0.93
                tp2 = entry_price * 0.87
                sl  = min(entry_price + atr*2.5, entry_price*1.06)
            else:
                entry_price = price
                entry_note  = "Market entry"

        score     = wscore(conditions)
        tier      = assign_tier(score, structure, precision, liq_map)
        stars_val = min(5, max(1, score//3))

        print(f"  📊 {pair} [INTRA {side}] score={score} tier={tier} "
              f"phase={structure.get('trend_phase')} entry={precision['entry_quality']}")

        if tier == "SKIP": continue

        extra = f"📡 <i>Regime: {regime['regime']} | BTC 1h: {regime['btc_1h_chg']:+.1f}%</i>"
        if divergence == ("BULLISH" if is_bull else "BEARISH"):
            extra += f"\n🔀 <i>{'Bullish' if is_bull else 'Bearish'} Divergence!</i>"
        if poc and is_bull and price <= poc: extra += f"\n📊 <i>Di bawah POC ${poc:.6f}</i>"
        if is_trending:    extra += "\n🔥 <i>Trending CoinGecko!</i>"
        if fg < 20 and is_bull: extra += f"\n😱 <i>Extreme Fear {fg}</i>"
        if ichi.get("above_cloud" if is_bull else "below_cloud"):
            extra += f"\n☁️ <i>Ichimoku: {'di atas' if is_bull else 'di bawah'} cloud</i>"
        if entry_note != "Market entry":
            extra += f"\n🎯 <i>Entry Type: {entry_note}</i>"

        entry_info  = resolve_entry(price, side, atr, ob, liq_map, structure, precision)
        final_entry = entry_info["entry_price"]
        if side == "BUY":
            tp1 = final_entry * 1.07
            tp2 = final_entry * 1.15
            sl  = max(final_entry - atr*2.5, final_entry*0.94)
        else:
            tp1 = final_entry * 0.93
            tp2 = final_entry * 0.87
            sl  = min(final_entry + atr*2.5, final_entry*1.06)
        queue_signal(
            pair=pair, signal_type="INTRADAY", side=side,
            entry=final_entry, tp1=tp1, tp2=tp2, sl=sl,
            strength=stars_val, timeframe="1h", valid_minutes=120,
            tier=tier, score=score,
            sources="Gate.io · Structure · Liq Engine · OI · Ichimoku",
            extra=extra, structure=structure, liq_map=liq_map,
            precision=precision, ob=ob, entry_info=entry_info,
        )


# ═══════════════════════════════════════════════
#  STRATEGY: SWING — 4h
# ═══════════════════════════════════════════════
def check_swing(client, pair, price, fg, ob_ratio, funding,
                trending, market_data, liq, oi_signal, regime):
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
    vol_trend  = np.mean(volumes[-5:]) > np.mean(volumes[-20:])*1.2
    support, resistance = calc_support_resistance(highs, lows, closes, lookback=50)
    divergence = calc_rsi_divergence(closes, highs, lows, lookback=40)
    poc        = calc_volume_profile(closes, volumes, bins=20)

    structure  = detect_structure(closes, highs, lows, lookback=100)
    liq_map    = detect_liquidity_map(closes, highs, lows, volumes, lookback=50)

    symbol      = pair.replace("_USDT","")
    is_trending = symbol in trending

    vol_ok, _ = volatility_ok(atr, price, "SWING")
    if not vol_ok: return

    for side in ["BUY", "SELL"]:
        is_bull = (side == "BUY")

        has_struct = (
            (is_bull and (structure.get("bos")=="BULLISH" or structure.get("choch")=="BULLISH"
                          or liq_map.get("sweep_bull"))) or
            (not is_bull and (structure.get("bos")=="BEARISH" or structure.get("choch")=="BEARISH"
                               or liq_map.get("sweep_bear")))
        )
        if not has_struct: continue

        # Swing: RISK_OFF → hanya safe pairs
        if regime.get("regime") == "RISK_OFF" and pair not in SAFE_PAIRS_RISK_OFF: continue
        if is_bull and regime.get("block_buy"): continue
        if not is_bull and regime.get("block_sell"): continue
        if is_bull and liq_map.get("fake_bo_bull"): continue
        if not is_bull and liq_map.get("fake_bo_bear"): continue
        if is_bull and macd <= msig: continue
        if not is_bull and macd >= msig: continue

        ob        = detect_order_block(closes, highs, lows, volumes, side=side, lookback=40)
        precision = check_entry_precision(closes, highs, lows, volumes,
                                          side, structure, liq_map, ob)
        if precision["entry_quality"] == "SKIP": continue

        if is_bull:
            conditions = [
                (structure.get("bos")  == "BULLISH",          "bos"),
                (structure.get("choch")== "BULLISH",          "choch"),
                (liq_map.get("sweep_bull"),                   "liq_sweep"),
                (liq_map.get("equal_lows") is not None,       "equal_hl"),
                (liq_map.get("stop_cluster_below") is not None,"stop_cluster"),
                (ob.get("valid"),                             "order_block"),
                (precision.get("pullback"),                   "pullback"),
                (precision.get("rejection"),                  "rejection"),
                (precision.get("candle_conf"),                "candle_conf"),
                (ema50 > ema200,                              "ema_cross"),
                (rsi < 35,                                    "rsi_extreme"),
                (stoch_rsi < 0.3,                            "stoch_rsi"),
                (fg < 25,                                     "fg_extreme"),
                (vol_trend,                                   "vol_spike"),
                (is_trending,                                 "trending"),
                (ob_ratio > 1.1,                              "ob_ratio"),
                (divergence == "BULLISH",                    "divergence"),
                (poc and price <= poc,                       "poc"),
                (price <= support*1.05,                      "support_res"),
                (funding and funding < -0.002,               "funding"),
                (price > vwap,                               "vwap"),
                (ichi.get("above_cloud"),                    "ichimoku"),
                (liq.get("liq_bias") == "BUY",               "liq_cluster"),
                (oi_signal in ("STRONG_BUY","SQUEEZE"),      "oi_signal"),
            ]

            # ── PREDICTIVE ENTRY: gunakan OB zone sebagai entry, bukan harga pasar ──
            # Kalau OB valid dan harga di atas OB → entry di OB mid (lebih baik dari market)
            # Kalau harga sudah di OB atau bawah → entry di harga pasar
            if ob.get("valid") and price > ob["ob_high"] * 1.005:
                # Harga sudah di atas OB → entry di OB mid (limit order zone)
                entry_price = ob["ob_mid"]
                entry_note  = f"Limit @ OB ${ob['ob_mid']:.6f}–${ob['ob_high']:.6f}"
            elif liq_map.get("stop_cluster_below") and price > liq_map["stop_cluster_below"] * 1.01:
                # Entry di stop cluster zone
                entry_price = liq_map["stop_cluster_below"] * 1.003
                entry_note  = f"Limit @ Stop Cluster ${entry_price:.6f}"
            else:
                entry_price = price
                entry_note  = "Market entry"

            tp1 = entry_price * 1.12
            tp2 = entry_price * 1.25
            sl  = max(entry_price - atr*3.0, entry_price*0.88)
            if liq_map.get("sweep_level"): sl = min(sl, liq_map["sweep_level"]*0.996)
        else:
            conditions = [
                (structure.get("bos")  == "BEARISH",          "bos"),
                (structure.get("choch")== "BEARISH",          "choch"),
                (liq_map.get("sweep_bear"),                   "liq_sweep"),
                (liq_map.get("equal_highs") is not None,      "equal_hl"),
                (liq_map.get("stop_cluster_above") is not None,"stop_cluster"),
                (ob.get("valid"),                             "order_block"),
                (precision.get("pullback"),                   "pullback"),
                (precision.get("rejection"),                  "rejection"),
                (precision.get("candle_conf"),                "candle_conf"),
                (ema50 < ema200,                              "ema_cross"),
                (rsi > 65,                                    "rsi_extreme"),
                (stoch_rsi > 0.7,                            "stoch_rsi"),
                (fg > 60,                                     "fg_extreme"),
                (vol_trend,                                   "vol_spike"),
                (divergence == "BEARISH",                    "divergence"),
                (funding and funding > 0.002,                "funding"),
                (price < vwap,                               "vwap"),
                (ichi.get("below_cloud"),                    "ichimoku"),
                (liq.get("liq_bias") == "SELL",              "liq_cluster"),
                (oi_signal == "STRONG_SELL",                 "oi_signal"),
            ]

            # PREDICTIVE ENTRY SELL: entry di OB sell zone
            if ob.get("valid") and price < ob["ob_low"] * 0.995:
                entry_price = ob["ob_mid"]
                entry_note  = f"Limit @ OB ${ob['ob_low']:.6f}–${ob['ob_high']:.6f}"
            elif liq_map.get("stop_cluster_above") and price < liq_map["stop_cluster_above"] * 0.99:
                entry_price = liq_map["stop_cluster_above"] * 0.997
                entry_note  = f"Limit @ Stop Cluster ${entry_price:.6f}"
            else:
                entry_price = price
                entry_note  = "Market entry"

            tp1 = entry_price * 0.88
            tp2 = entry_price * 0.78
            sl  = min(entry_price + atr*3.0, entry_price*1.12)

        score     = wscore(conditions)
        tier      = assign_tier(score, structure, precision, liq_map)
        stars_val = min(5, max(1, score//4))

        print(f"  📊 {pair} [SWING {side}] score={score} tier={tier} "
              f"phase={structure.get('trend_phase')} entry={precision['entry_quality']}")

        if tier == "SKIP": continue

        extra = f"📡 <i>Regime: {regime['regime']} | BTC: {regime['btc_trend']}</i>"
        if is_bull and ema50>ema200: extra += "\n✨ <i>Golden Cross EMA 50/200</i>"
        if not is_bull and ema50<ema200: extra += "\n💀 <i>Death Cross EMA 50/200</i>"
        if divergence == ("BULLISH" if is_bull else "BEARISH"):
            extra += f"\n🔀 <i>{'Bullish' if is_bull else 'Bearish'} Divergence!</i>"
        if fg < 20 and is_bull: extra += f"\n😱 <i>Extreme Fear {fg}</i>"
        if poc: extra += f"\n📊 <i>POC: ${poc:.6f}</i>"
        if is_trending: extra += "\n🔥 <i>Trending CoinGecko!</i>"
        if entry_note != "Market entry":
            extra += f"\n🎯 <i>Entry Type: {entry_note}</i>"

        queue_signal(
            pair=pair, signal_type="SWING", side=side,
            entry=entry_price, tp1=tp1, tp2=tp2, sl=sl,
            strength=stars_val, timeframe="4h", valid_minutes=720,
            tier=tier, score=score,
            sources="Gate.io · Structure · Liq Engine · EMA50/200 · Ichimoku · OI",
            extra=extra, structure=structure, liq_map=liq_map,
            precision=precision, ob=ob,
        )


# ═══════════════════════════════════════════════
#  STRATEGY: MOONSHOT
# ═══════════════════════════════════════════════
def check_moonshot(client, pair, price, change_24h, trending, liq, regime):
    if regime.get("regime") == "RISK_OFF": return

    closes, highs, lows, volumes = get_candles(client, pair, "1h", 48)
    if closes is None: return

    vol_avg   = np.mean(volumes[:-6])
    vol_now   = np.mean(volumes[-6:])
    vol_ratio = vol_now/vol_avg if vol_avg > 0 else 0
    rsi       = calc_rsi(closes)
    atr       = calc_atr(closes, highs, lows)
    vwap      = calc_vwap(closes, highs, lows, volumes)
    support, resistance = calc_support_resistance(highs, lows, closes)

    structure  = detect_structure(closes, highs, lows, lookback=30)
    liq_map    = detect_liquidity_map(closes, highs, lows, volumes, lookback=20)

    symbol      = pair.replace("_USDT","")
    is_trending = symbol in trending

    if vol_ratio > 4.0 and 25 < rsi < 70 and change_24h > 3.0:
        if regime.get("block_buy"): return

        conditions = [
            (vol_ratio > 7.0,                                "vol_spike"),
            (is_trending,                                    "trending"),
            (change_24h > 8.0,                              "vol_spike"),   # FIX v6: was mislabeled rsi_extreme
            (rsi < 55,                                       "stoch_rsi"),
            (price > resistance,                             "support_res"),
            (price > vwap,                                   "vwap"),
            (liq.get("liq_bias") == "BUY",                  "liq_cluster"),
            (structure.get("bos") == "BULLISH",             "bos"),
            (liq_map.get("sweep_bull"),                      "liq_sweep"),
            (liq_map.get("equal_lows") is not None,          "equal_hl"),
        ]
        score     = wscore(conditions)
        stars_val = min(5, max(1, score//2))
        tier      = assign_tier(score, structure, {"entry_quality":"READY"}, liq_map)

        print(f"  📊 {pair} [MOONSHOT] score={score} tier={tier} vol={vol_ratio:.1f}x")
        if tier == "SKIP": return

        tp1 = price*1.20; tp2 = price*1.50
        sl  = max(price*0.88, support*0.98)

        extra = (
            f"📡 <i>Regime: {regime['regime']}</i>\n━━━━━━━━━━━━━━━━━━\n"
            f"🔥 Vol: <b>{vol_ratio:.1f}x</b> | 24h: <b>+{change_24h:.1f}%</b>\n"
        )
        if price > resistance: extra += "🚀 <i>Breakout Resistance!</i>\n"
        if is_trending:        extra += "🌟 <i>Trending CoinGecko!</i>\n"
        extra += "⚠️ <i>High Risk — Max 5% modal. SL wajib.</i>"

        # [v10 FIX] Jalankan entry precision yang nyata — sebelumnya hardcode READY
        # sehingga bisa masuk di puncak pump. Sekarang cek RSI + candle + rejection.
        # Jika RSI >= 70 (overbought), skip moonshot — beli di top sangat berisiko.
        if rsi >= 70:
            print(f"  ↩️ {pair} [MOONSHOT] skip — RSI overbought ({rsi:.1f}), risiko beli di top")
            return

        ob_moon   = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=20)
        prec_moon = check_entry_precision(
            closes, highs, lows, volumes, "BUY", structure, liq_map, ob_moon)

        # Moonshot boleh lanjut jika READY atau WAIT (bukan SKIP)
        if prec_moon["entry_quality"] == "SKIP":
            print(f"  ↩️ {pair} [MOONSHOT] skip — entry precision SKIP")
            return

        entry_info  = resolve_entry(price, "BUY", atr, ob_moon, liq_map, structure, prec_moon)
        final_entry = entry_info["entry_price"]
        tp1 = final_entry * 1.20
        tp2 = final_entry * 1.50
        sl  = max(final_entry * 0.88, support * 0.98)
        queue_signal(
            pair=pair, signal_type="MOONSHOT", side="BUY",
            entry=final_entry, tp1=tp1, tp2=tp2, sl=sl,
            strength=stars_val, timeframe="1h", valid_minutes=360,
            tier=tier, score=score,
            sources="Gate.io · Structure · Liq Engine · CoinGecko",
            extra=extra, structure=structure, liq_map=liq_map,
            precision=prec_moon, ob=ob_moon, entry_info=entry_info,
        )


# ═══════════════════════════════════════════════
#  MARKET SUMMARY
# ═══════════════════════════════════════════════
def should_send_summary():
    now = datetime.now(WIB)
    return now.hour == SUMMARY_HOUR_WIB and now.minute < 35


def send_market_summary(fg, fg_label, market_data, trending, regime, perf):
    if not market_data: return
    now        = datetime.now(WIB)
    btc_dom    = market_data.get("btc_dominance", 0)
    mcap_chg   = market_data.get("market_cap_change_24h", 0)
    mcap_emoji = "📈" if mcap_chg > 0 else "📉"
    trend_str  = ", ".join(trending[:5]) if trending else "N/A"
    alt_note   = "🟢 Alt Season" if btc_dom < 48 else "🔴 BTC Season"

    saran_map = [
        (20,  "😱 <b>Extreme Fear</b> — Akumulasi selektif"),
        (40,  "😨 <b>Fear</b> — Peluang beli, hati-hati"),
        (60,  "😐 <b>Neutral</b> — Tunggu konfirmasi"),
        (80,  "😊 <b>Greed</b> — Kurangi posisi"),
        (101, "🤑 <b>Extreme Greed</b> — Potensi koreksi"),
    ]
    saran = next(s for t, s in saran_map if fg < t)

    # Summary portfolio state
    port_str = (f"Risk on: <b>{_portfolio_state['total_risk_pct']:.1f}%</b> / "
                f"{MAX_PORTFOLIO_RISK_PCT}%")

    # Kill switch status untuk briefing
    ks_halted, ks_reason = check_kill_switch()
    ks_str = f"🛑 HALT — {ks_reason[:60]}" if ks_halted else "✅ Aktif"

    tg(
        f"📊 <b>MORNING BRIEFING — {now.strftime('%d %b %Y %H:%M WIB')}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Fear & Greed:  <b>{fg} — {fg_label}</b>\n"
        f"BTC Dominance: <b>{btc_dom:.1f}%</b> {alt_note}\n"
        f"Market Cap:    {mcap_emoji} <b>{mcap_chg:+.2f}%</b> (24h)\n"
        f"Regime:        <b>{regime['regime']}</b> [{regime.get('aggressiveness')}]\n"
        f"Portfolio:     {port_str}\n"
        f"Kill Switch:   {ks_str}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔥 Trending: <b>{trend_str}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💡 {saran}"
    )
    if perf:
        send_performance_report(perf)


# ═══════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════
def run():
    global _signal_queue, _portfolio_state

    # FIX 3: Load portfolio state dari Supabase — posisi aktif dari cycle sebelumnya
    # tetap dihitung, bukan di-reset ke kosong setiap cycle.
    _signal_queue    = []
    _portfolio_state = load_portfolio_state()

    client = get_client()
    print(f"=== SIGNAL SCAN | {SIGNAL_MODE} | v10 Decision Engine ===")

    # Engine 9: Kill Switch — cek sebelum scan dimulai
    if KILL_SWITCH_ENABLED:
        halted, ks_reason = check_kill_switch()
        if halted:
            tg(f"🛑 <b>KILL SWITCH AKTIF</b>\n{ks_reason}\n"
               f"<i>Scan dibatalkan. Lakukan evaluasi manual sebelum restart.</i>")
            print(f"🛑 Kill Switch: {ks_reason}")
            return

    # Auto-update expired signals
    print("📝 Updating expired signals...")
    update_expired_signals(client)

    # Engine 7: Load strategy performance untuk adaptive sizing
    print("🧠 Loading strategy performance...")
    perf = load_strategy_performance()
    if perf:
        for k, v in perf.items():
            print(f"   {k}: WR={v['win_rate']}% PF={v['profit_factor']} [{v['status']}]")
    else:
        print("   No performance data yet")

    # External data
    fg, fg_label = get_fear_greed()
    market_data  = get_coingecko_market()
    trending     = get_coingecko_trending()
    regime       = get_market_regime(client)
    vol_state    = get_btc_volatility_state(client)

    btc_dom_str = f"{market_data.get('btc_dominance','N/A'):.1f}" if market_data else "N/A"
    print(f"BTC Dom: {btc_dom_str}% | F&G: {fg} | Vol: {vol_state} | "
          f"Regime: {regime['regime']} [{regime.get('aggressiveness')}]")

    # Volatility spike guard
    if vol_state == "SPIKE":
        tg(f"⚠️ <b>Volatility Spike</b>\nScan ditunda. Regime: <b>{regime['regime']}</b>")
        print("❌ Volatility spike — scan dihentikan")
        return

    # Morning summary + performance report
    if should_send_summary():
        send_market_summary(fg, fg_label, market_data, trending, regime, perf)

    # BTC crash guard
    try:
        btc    = client.list_tickers(currency_pair="BTC_USDT")[0]
        change = float(btc.change_percentage or 0)
        print(f"BTC 24h: {change:.2f}%")
        if change < -12:
            tg("⚠️ <b>Market Crash Alert</b>\nBTC turun &gt;12%. Scan ditunda.")
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

            symbol   = pair.replace("_USDT","")
            funding  = get_funding_rate(pair)
            ob_ratio = get_order_book_pressure(client, pair)

            if pair in FUTURES_PAIRS:
                liq     = get_liquidation_data(symbol)
                oi_data = get_open_interest(symbol)
                # [v9] Fallback: jika Coinglass gagal (oi_usd=0), pakai Gate.io
                if oi_data["oi_usd"] == 0:
                    oi_data = get_open_interest_gate(pair)
                    if oi_data["oi_usd"] > 0:
                        print(f"  🔄 [{pair}] OI fallback Gate.io: ${oi_data['oi_usd']}M")
                oi_signal = interpret_oi(oi_data, change_24h)
            else:
                liq       = {"liq_short_above":0,"liq_long_below":0,"liq_bias":"NEUTRAL"}
                oi_signal = "NEUTRAL"

            check_scalping(client, pair, price, fg, ob_ratio, funding,
                           liq, oi_signal, regime)
            check_intraday(client, pair, price, fg, ob_ratio, funding,
                           trending, market_data, liq, oi_signal, regime)
            check_swing(client, pair, price, fg, ob_ratio, funding,
                        trending, market_data, liq, oi_signal, regime)
            check_moonshot(client, pair, price, change_24h, trending, liq, regime)

            total += 1
            time.sleep(0.15)  # v9: 0.05→0.15 cegah rate limit Gate.io

        except Exception as e:
            print(f"⚠️ [{pair}]: {e}")
            continue

    # Engine 4+5+6+7: Flush queue dengan semua guards aktif
    print(f"\n📨 Queue: {len(_signal_queue)} candidates")
    flush_signal_queue(perf, regime)   # v6: pass live regime

    print(f"=== DONE | {total} pairs scanned ===")

    tg(
        f"🔍 <b>Scan Selesai — v10 Decision Engine</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pairs scanned  : <b>{total}</b>\n"
        f"Regime         : <b>{regime['regime']}</b> [{regime.get('aggressiveness')}]\n"
        f"BTC 1h         : <b>{regime['btc_1h_chg']:+.1f}%</b>\n"
        f"F&G            : <b>{fg}</b>\n"
        f"Volatility     : <b>{vol_state}</b>\n"
        f"Block BUY      : <b>{'Ya' if regime['block_buy'] else 'Tidak'}</b>\n"
        f"Block SELL     : <b>{'Ya' if regime['block_sell'] else 'Tidak'}</b>\n"
        f"Portfolio Risk : <b>{_portfolio_state['total_risk_pct']:.1f}%</b> / "
        f"{MAX_PORTFOLIO_RISK_PCT}%\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<i>Top {MAX_SIGNALS_PER_CYCLE} sinyal, ranked by Tier → Score.</i>"
    )


if __name__ == "__main__":
    run()
