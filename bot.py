"""
╔══════════════════════════════════════════════════════════════════╗
║           SIGNAL BOT — CLEAN v2.1                               ║
║                                                                  ║
║  Arsitektur bersih dari nol:                                    ║
║  - INTRADAY (1h) + SWING (4h) + PUMP SCANNER (15m)            ║
║  - Tidak ada performance feedback loop (data palsu dihapus)     ║
║  - Scoring murni teknikal: BOS/CHoCH + MACD + RSI + OB + Vol   ║
║  - RR minimum realistis: INTRADAY 1.5 | SWING 2.0              ║
║  - Dedup sederhana: tidak kirim ulang pair yang sama < 4 jam    ║
║  - Output Telegram bersih dan actionable                        ║
║                                                                  ║
║  SCAN_MODE via ENV:                                             ║
║    SCAN_MODE=full  → INTRADAY + SWING (setiap 4 jam)           ║
║    SCAN_MODE=pump  → Pump Scanner saja (setiap 1 jam)          ║
║    default         → full                                       ║
║                                                                  ║
║  Target: 5–10 signal/hari, akurasi > volume                    ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os, json, time
import urllib.request
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from supabase import create_client
import gate_api

# ════════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════════

API_KEY      = os.environ.get("GATE_API_KEY")
SECRET_KEY   = os.environ.get("GATE_SECRET_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TG_TOKEN     = os.environ.get("TELEGRAM_TOKEN")
TG_CHAT_ID   = os.environ.get("CHAT_ID")
WIB          = timezone(timedelta(hours=7))

# Validasi environment
_missing = [k for k, v in {
    "SUPABASE_URL": SUPABASE_URL, "SUPABASE_KEY": SUPABASE_KEY,
    "TELEGRAM_TOKEN": TG_TOKEN,   "CHAT_ID": TG_CHAT_ID,
}.items() if not v]
if _missing:
    raise EnvironmentError(f"ENV belum diset: {', '.join(_missing)}")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Volume & Pair Filter ──────────────────────────────
MIN_VOLUME_USDT    = 500_000     # hanya pair dengan likuiditas cukup
MAX_SIGNALS_CYCLE  = 8           # maksimal signal per run (4 jam)
DEDUP_HOURS        = 4           # tidak kirim ulang pair yang sama dalam 4 jam

# ── Scoring Thresholds ───────────────────────────────
# Tier ditentukan oleh score murni — tidak ada override lain
TIER_MIN_SCORE = {
    "S":  14,   # sangat terkonfirmasi
    "A+": 10,
    "A":   7,   # minimum untuk signal bot
}
SIGNAL_MIN_TIER = "A"  # tier B tidak dikirim

# ── RR Minimum ───────────────────────────────────────
# Realistis berdasarkan ATR dan structure, bukan terlalu agresif
MIN_RR = {
    "INTRADAY": 1.5,
    "SWING":    2.0,
}

# ── Regime Guard ─────────────────────────────────────
BTC_DROP_BLOCK  = -3.0   # BTC turun > 3% dalam 1h → blok semua BUY
BTC_CRASH_BLOCK = -10.0  # BTC crash > 10% dalam 4h → halt semua signal

# ── SL / TP Parameters ───────────────────────────────
INTRADAY_SL_ATR = 1.5    # SL = entry ± ATR × 1.5
INTRADAY_TP1_R  = 1.5    # TP1 = SL distance × 1.5
INTRADAY_TP2_R  = 2.5    # TP2 = SL distance × 2.5
SWING_SL_ATR    = 2.0    # SL lebih longgar untuk 4h
SWING_TP1_R     = 2.0
SWING_TP2_R     = 3.5

# ── Pump Scanner Config ──────────────────────────────
PUMP_VOL_SPIKE      = 3.0    # volume candle terakhir harus > 3× rata-rata 10 candle
PUMP_PRICE_CHANGE   = 4.0    # harga naik > 4% dalam 3 candle 15m terakhir
PUMP_RSI_MAX        = 72     # RSI belum overbought ekstrem
PUMP_MIN_VOLUME     = 200_000  # volume 24h minimum lebih rendah dari main bot
PUMP_DEDUP_HOURS    = 1      # dedup pump signal lebih pendek (1 jam)
MAX_PUMP_SIGNALS    = 5      # maksimal pump signal per run

# ── Scan Mode ────────────────────────────────────────
# Set via ENV: SCAN_MODE=full (default) atau SCAN_MODE=pump
SCAN_MODE = os.environ.get("SCAN_MODE", "full").lower()

# ── Blacklist ─────────────────────────────────────────
BLACKLIST_TOKENS = {
    "TUSD","USDC","BUSD","DAI","FDUSD","USD1","USDP","USDD","USDJ",
    "ZUSD","GUSD","CUSD","SUSD","FRAX","LUSD","USDN","3S","3L","5S","5L",
}
ETF_KEYWORDS = {
    "NVDA","TSLA","AAPL","AMZN","MSFT","META","GOOG","NFLX",
    "COIN","MSTR","MCD","BABA","AMD","INTC","PYPL","UBER",
}

# ── Weighted Score Components ──────────────────────────
# Setiap komponen punya bobot — score adalah jumlah bobot yang terpenuhi
W = {
    "bos":         6,   # Break of Structure — paling penting
    "choch":       5,   # Change of Character — reversal confirmed
    "liq_sweep":   4,   # Liquidity sweep — smart money move
    "order_block": 4,   # Order block valid — institutional zone
    "macd_cross":  3,   # MACD crossover searah
    "rsi_zone":    3,   # RSI di zona optimal (tidak overbought/oversold ekstrem)
    "vol_confirm": 3,   # Volume konfirmasi breakout
    "ema_align":   2,   # EMA alignment searah trend
    "vwap_side":   2,   # Harga di sisi yang benar dari VWAP
    "pullback":    2,   # Entry dari pullback, bukan kejar harga
    "candle_body": 2,   # Candle konfirmasi bullish/bearish
    "equal_lows":  1,   # Equal lows/highs sebagai target likuiditas
    "ob_ratio":    1,   # Order book ratio mendukung arah
}

# ════════════════════════════════════════════════════════
#  UTILITIES
# ════════════════════════════════════════════════════════

def tg(msg: str):
    """Kirim pesan ke Telegram."""
    try:
        if not TG_TOKEN or not TG_CHAT_ID: return
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
        print(f"⚠️ Telegram: {e}")


def http_get(url: str, timeout: int = 8):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"⚠️ HTTP {url[:60]}: {e}")
        return None


def get_client():
    cfg = gate_api.Configuration(
        host="https://api.gateio.ws/api/v4",
        key=API_KEY, secret=SECRET_KEY
    )
    return gate_api.SpotApi(gate_api.ApiClient(cfg))


def is_valid_pair(pair: str) -> bool:
    if not pair.endswith("_USDT"): return False
    base = pair.replace("_USDT", "")
    if any(b in base for b in BLACKLIST_TOKENS): return False
    if any(base.startswith(kw) for kw in ETF_KEYWORDS): return False
    # Filter leveraged token
    if any(base.endswith(sfx) for sfx in ["UP","DOWN","BULL","BEAR","3L","3S"]): return False
    return True


_candle_cache: dict = {}

def get_candles(client, pair: str, interval: str, limit: int):
    """Fetch candles dengan cache per cycle."""
    key = (pair, interval)
    if key in _candle_cache:
        return _candle_cache[key]
    try:
        raw = client.list_candlesticks(currency_pair=pair, interval=interval, limit=limit)
        if not raw or len(raw) < 30:
            _candle_cache[key] = None; return None
        closes  = np.array([float(c[2]) for c in raw])
        highs   = np.array([float(c[3]) for c in raw])
        lows    = np.array([float(c[4]) for c in raw])
        volumes = np.array([float(c[1]) for c in raw])
        result  = (closes, highs, lows, volumes)
        _candle_cache[key] = result
        return result
    except Exception as e:
        print(f"⚠️ candles [{pair}|{interval}]: {e}")
        _candle_cache[key] = None
        return None


# ════════════════════════════════════════════════════════
#  INDICATORS — Kalkulasi murni
# ════════════════════════════════════════════════════════

def calc_rsi(closes, period=14) -> float:
    s = pd.Series(closes); d = s.diff()
    gain = d.clip(lower=0).rolling(period).mean()
    loss = (-d.clip(upper=0)).rolling(period).mean()
    return float((100 - 100/(1+gain/(loss+1e-9))).iloc[-1])


def calc_ema(closes, period) -> float:
    return float(pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1])


def calc_macd(closes):
    s = pd.Series(closes)
    macd   = s.ewm(span=12, adjust=False).mean() - s.ewm(span=26, adjust=False).mean()
    signal = macd.ewm(span=9, adjust=False).mean()
    return float(macd.iloc[-1]), float(signal.iloc[-1])


def calc_atr(closes, highs, lows, period=14) -> float:
    tr = [max(highs[i]-lows[i],
              abs(highs[i]-closes[i-1]),
              abs(lows[i]-closes[i-1]))
          for i in range(1, len(closes))]
    return float(pd.Series(tr).rolling(period).mean().iloc[-1])


def calc_vwap(closes, highs, lows, volumes) -> float:
    tp = (highs + lows + closes) / 3
    return float((np.cumsum(tp * volumes) / (np.cumsum(volumes) + 1e-9))[-1])


def calc_bb(closes, period=20):
    s = pd.Series(closes)
    mid = s.rolling(period).mean().iloc[-1]
    std = s.rolling(period).std().iloc[-1]
    return float(mid - 2*std), float(mid + 2*std)


# ════════════════════════════════════════════════════════
#  STRUCTURE ENGINE
# ════════════════════════════════════════════════════════

def detect_swing_points(highs, lows, strength=3, lookback=80):
    """Deteksi swing high/low dengan strength filter."""
    points = []
    n = min(len(highs), lookback)
    start = len(highs) - n
    for i in range(start + strength, len(highs) - strength):
        if (all(highs[i] > highs[i-j] for j in range(1, strength+1)) and
                all(highs[i] > highs[i+j] for j in range(1, strength+1))):
            points.append((i, highs[i], "SH"))
        if (all(lows[i] < lows[i-j] for j in range(1, strength+1)) and
                all(lows[i] < lows[i+j] for j in range(1, strength+1))):
            points.append((i, lows[i], "SL"))
    return sorted(points, key=lambda x: x[0])


def detect_structure(closes, highs, lows, strength=3, lookback=80) -> dict:
    """
    Deteksi BOS dan CHoCH.
    BOS   = Break of Structure searah trend (konfirmasi lanjutan)
    CHoCH = Change of Character berlawanan trend (konfirmasi reversal)
    """
    result = {
        "bos": None, "choch": None,
        "last_sh": None, "last_sl": None,
        "prev_sh": None, "prev_sl": None,
        "bias": "NEUTRAL", "valid": False,
    }
    if len(closes) < lookback: return result

    pts = detect_swing_points(highs, lows, strength=strength, lookback=lookback)
    shs = [(i, p) for i, p, t in pts if t == "SH"]
    sls = [(i, p) for i, p, t in pts if t == "SL"]
    if len(shs) < 2 or len(sls) < 2: return result

    last_sh, prev_sh = shs[-1][1], shs[-2][1]
    last_sl, prev_sl = sls[-1][1], sls[-2][1]
    price = float(closes[-1])
    prev  = float(closes[-2]) if len(closes) >= 2 else price

    result.update({
        "last_sh": last_sh, "prev_sh": prev_sh,
        "last_sl": last_sl, "prev_sl": prev_sl,
        "valid": True,
    })

    # Tentukan bias trend
    hh = last_sh > prev_sh; hl = last_sl > prev_sl
    lh = last_sh < prev_sh; ll = last_sl < prev_sl
    if hh and hl:   result["bias"] = "BULLISH"
    elif lh and ll: result["bias"] = "BEARISH"
    else:           result["bias"] = "NEUTRAL"

    # BOS: deteksi dalam 3 candle terakhir saja — BOS lama tidak valid
    # Lebih dari 3 candle = harga sudah terlalu jauh, entry terlambat
    recent_closes = closes[-3:]
    recent_highs  = highs[-3:]
    recent_lows   = lows[-3:]

    bull_break = any(recent_closes[i] > last_sh and
                     (i == 0 or recent_closes[i-1] <= last_sh * 1.008)
                     for i in range(len(recent_closes)))

    bear_break = any(recent_closes[i] < last_sl and
                     (i == 0 or recent_closes[i-1] >= last_sl * 0.992)
                     for i in range(len(recent_closes)))

    if bull_break:
        if result["bias"] == "BULLISH": result["bos"]   = "BULLISH"
        else:                           result["choch"]  = "BULLISH"
    elif bear_break:
        if result["bias"] == "BEARISH": result["bos"]   = "BEARISH"
        else:                           result["choch"]  = "BEARISH"

    return result


def detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=30) -> dict:
    """
    Order block = candle bearish (untuk BUY) sebelum impulse bullish besar,
    atau candle bullish (untuk SELL) sebelum impulse bearish besar.
    Ini zona dimana institusi menempatkan order.
    """
    result = {"valid": False, "ob_high": None, "ob_low": None}
    if len(closes) < lookback: return result
    c = closes[-lookback:]; h = highs[-lookback:]
    l = lows[-lookback:];   v = volumes[-lookback:]
    avg_body = float(np.mean([abs(c[i] - c[i-1]) for i in range(1, len(c))]))

    for i in range(len(c) - 4, 1, -1):
        impulse = abs(c[i+1] - c[i])
        if impulse < avg_body * 1.5: continue
        if side == "BUY" and c[i] < c[i-1] and c[i+1] > c[i]:
            return {"valid": True, "ob_high": float(h[i]), "ob_low": float(l[i])}
        if side == "SELL" and c[i] > c[i-1] and c[i+1] < c[i]:
            return {"valid": True, "ob_high": float(h[i]), "ob_low": float(l[i])}
    return result


def detect_liquidity(closes, highs, lows, lookback=50) -> dict:
    """Deteksi equal highs/lows dan liquidity sweep."""
    result = {
        "equal_lows": None, "equal_highs": None,
        "sweep_bull": False, "sweep_bear": False,
    }
    if len(closes) < lookback: return result
    h = highs[-lookback:]; l = lows[-lookback:]
    tol = 0.003  # 0.3% toleransi

    # Equal highs
    for i in range(len(h) - 1, 0, -1):
        for j in range(i - 1, max(i - 10, 0), -1):
            if abs(h[i] - h[j]) / (h[j] + 1e-9) < tol:
                result["equal_highs"] = float((h[i] + h[j]) / 2)
                break
        if result["equal_highs"]: break

    # Equal lows
    for i in range(len(l) - 1, 0, -1):
        for j in range(i - 1, max(i - 10, 0), -1):
            if abs(l[i] - l[j]) / (l[j] + 1e-9) < tol:
                result["equal_lows"] = float((l[i] + l[j]) / 2)
                break
        if result["equal_lows"]: break

    # Liquidity sweep: wick tembus level tapi close kembali
    ref_low  = float(np.min(l[:-5]))
    ref_high = float(np.max(h[:-5]))
    for i in range(-5, 0):
        if lows[i] < ref_low and closes[i] > ref_low:
            result["sweep_bull"] = True
        if highs[i] > ref_high and closes[i] < ref_high:
            result["sweep_bear"] = True

    return result


# ════════════════════════════════════════════════════════
#  SCORING ENGINE
# ════════════════════════════════════════════════════════

def score_signal(side: str, price: float, closes, highs, lows, volumes,
                 structure: dict, liq: dict, ob: dict,
                 rsi: float, macd: float, msig: float,
                 ema_fast: float, ema_slow: float,
                 vwap: float, ob_ratio: float) -> int:
    """
    Hitung score murni teknikal.
    Setiap kondisi bernilai sesuai bobot W.
    Tidak ada gate eksternal — murni dari data pasar.
    """
    is_bull = (side == "BUY")
    score   = 0

    if is_bull:
        if structure.get("bos")   == "BULLISH": score += W["bos"]
        if structure.get("choch") == "BULLISH": score += W["choch"]
        if liq.get("sweep_bull"):               score += W["liq_sweep"]
        if ob.get("valid"):                     score += W["order_block"]
        if macd > msig:                         score += W["macd_cross"]
        if 30 < rsi < 60:                       score += W["rsi_zone"]   # tidak overbought
        vol_avg = float(np.mean(volumes[-10:-1]))
        if float(volumes[-1]) > vol_avg * 1.3:  score += W["vol_confirm"]
        if ema_fast > ema_slow:                 score += W["ema_align"]
        if price > vwap:                        score += W["vwap_side"]
        last_sl = structure.get("last_sl")
        if last_sl and price <= last_sl * 1.03: score += W["pullback"]
        prev = float(closes[-2])
        body  = price - prev
        rng   = float(highs[-1]) - float(lows[-1]) + 1e-9
        if body > 0 and body / rng > 0.5:       score += W["candle_body"]
        if liq.get("equal_lows"):               score += W["equal_lows"]
        if ob_ratio > 1.1:                      score += W["ob_ratio"]
    else:
        if structure.get("bos")   == "BEARISH": score += W["bos"]
        if structure.get("choch") == "BEARISH": score += W["choch"]
        if liq.get("sweep_bear"):               score += W["liq_sweep"]
        if ob.get("valid"):                     score += W["order_block"]
        if macd < msig:                         score += W["macd_cross"]
        if 40 < rsi < 70:                       score += W["rsi_zone"]
        vol_avg = float(np.mean(volumes[-10:-1]))
        if float(volumes[-1]) > vol_avg * 1.3:  score += W["vol_confirm"]
        if ema_fast < ema_slow:                 score += W["ema_align"]
        if price < vwap:                        score += W["vwap_side"]
        last_sh = structure.get("last_sh")
        if last_sh and price >= last_sh * 0.97: score += W["pullback"]
        prev = float(closes[-2])
        body  = prev - price
        rng   = float(highs[-1]) - float(lows[-1]) + 1e-9
        if body > 0 and body / rng > 0.5:       score += W["candle_body"]
        if liq.get("equal_highs"):              score += W["equal_lows"]
        if ob_ratio < 0.9:                      score += W["ob_ratio"]

    return score


def assign_tier(score: int) -> str:
    if score >= TIER_MIN_SCORE["S"]:  return "S"
    if score >= TIER_MIN_SCORE["A+"]: return "A+"
    if score >= TIER_MIN_SCORE["A"]:  return "A"
    return "SKIP"


# ════════════════════════════════════════════════════════
#  TP / SL CALCULATOR
# ════════════════════════════════════════════════════════

def calc_sl_tp(entry: float, side: str, atr: float,
               structure: dict, strategy: str) -> tuple:
    """
    SL berbasis ATR + dikonfirmasi struktur.
    TP berbasis RR multiplier dari SL distance.
    """
    if strategy == "INTRADAY":
        sl_mult, tp1_r, tp2_r = INTRADAY_SL_ATR, INTRADAY_TP1_R, INTRADAY_TP2_R
    else:
        sl_mult, tp1_r, tp2_r = SWING_SL_ATR, SWING_TP1_R, SWING_TP2_R

    sl_dist = atr * sl_mult

    if side == "BUY":
        # SL di bawah last_sl jika tersedia, fallback ke ATR
        last_sl = structure.get("last_sl")
        if last_sl and last_sl < entry:
            sl = min(entry - sl_dist, last_sl * 0.998)
        else:
            sl = entry - sl_dist
        tp1 = entry + sl_dist * tp1_r
        tp2 = entry + sl_dist * tp2_r
    else:
        last_sh = structure.get("last_sh")
        if last_sh and last_sh > entry:
            sl = max(entry + sl_dist, last_sh * 1.002)
        else:
            sl = entry + sl_dist
        tp1 = entry - sl_dist * tp1_r
        tp2 = entry - sl_dist * tp2_r

    return round(sl, 8), round(tp1, 8), round(tp2, 8)


# ════════════════════════════════════════════════════════
#  MARKET CONTEXT
# ════════════════════════════════════════════════════════

def get_btc_regime(client) -> dict:
    """
    Cek kondisi BTC untuk guard:
    - Crash guard: BTC drop > 10% dalam 4h → halt semua
    - Drop guard: BTC drop > 3% dalam 1h → blok BUY baru
    """
    default = {"halt": False, "block_buy": False, "btc_1h": 0.0, "btc_4h": 0.0}
    try:
        c1h = get_candles(client, "BTC_USDT", "1h", 10)
        c4h = get_candles(client, "BTC_USDT", "4h", 10)
        if c1h is None or c4h is None: return default

        chg_1h = (c1h[0][-1] - c1h[0][-2]) / c1h[0][-2] * 100
        chg_4h = (c4h[0][-1] - c4h[0][-5]) / c4h[0][-5] * 100

        halt      = chg_4h < BTC_CRASH_BLOCK
        block_buy = chg_1h < BTC_DROP_BLOCK

        print(f"📡 BTC 1h:{chg_1h:+.1f}% 4h:{chg_4h:+.1f}% | "
              f"{'🛑 HALT' if halt else '⛔ BUY BLOCKED' if block_buy else '✅ OK'}")
        return {"halt": halt, "block_buy": block_buy,
                "btc_1h": round(chg_1h, 2), "btc_4h": round(chg_4h, 2)}
    except Exception as e:
        print(f"⚠️ btc_regime: {e}")
        return default


def get_fear_greed() -> int:
    try:
        d = http_get("https://api.alternative.me/fng/?limit=1")
        if d: return int(d["data"][0]["value"])
    except Exception: pass
    return 50


def get_order_book_ratio(client, pair: str) -> float:
    try:
        ob      = client.list_order_book(currency_pair=pair, limit=10)
        bid_vol = sum(float(b[1]) for b in ob.bids)
        ask_vol = sum(float(a[1]) for a in ob.asks)
        return bid_vol / ask_vol if ask_vol > 0 else 1.0
    except Exception:
        return 1.0


# ════════════════════════════════════════════════════════
#  DEDUPLICATION via Supabase
# ════════════════════════════════════════════════════════

def already_sent(pair: str, strategy: str) -> bool:
    """Cek apakah signal pair+strategy sudah dikirim dalam DEDUP_HOURS jam terakhir."""
    try:
        since = (datetime.now(WIB) - timedelta(hours=DEDUP_HOURS)).isoformat()
        res = supabase.table("signals_v2") \
            .select("id") \
            .eq("pair", pair) \
            .eq("strategy", strategy) \
            .gt("sent_at", since) \
            .execute()
        return len(res.data) > 0
    except Exception as e:
        print(f"⚠️ dedup check: {e}")
        return False


def save_signal(pair: str, strategy: str, side: str, entry: float,
                tp1: float, tp2: float, sl: float, tier: str, score: int,
                timeframe: str):
    """Simpan signal ke Supabase untuk tracking dan deduplication."""
    try:
        now = datetime.now(WIB)
        supabase.table("signals_v2").insert({
            "pair":      pair,
            "strategy":  strategy,
            "side":      side,
            "entry":     entry,
            "tp1":       tp1,
            "tp2":       tp2,
            "sl":        sl,
            "tier":      tier,
            "score":     score,
            "timeframe": timeframe,
            "sent_at":   now.isoformat(),
            "result":    None,   # diisi manual atau via update script terpisah
        }).execute()
    except Exception as e:
        print(f"⚠️ save_signal [{pair}]: {e}")


# ════════════════════════════════════════════════════════
#  SIGNAL STRATEGIES
# ════════════════════════════════════════════════════════

def check_intraday(client, pair: str, price: float, ob_ratio: float,
                   btc: dict) -> dict | None:
    """
    INTRADAY signal — timeframe 1h.
    Gate wajib: BOS atau CHoCH harus ada + MACD searah + RSI tidak ekstrem
    + EMA7/EMA20 alignment sebagai momentum filter.
    """
    if btc["block_buy"]: return None  # BTC drop — skip BUY

    data = get_candles(client, pair, "1h", 100)
    if data is None: return None
    closes, highs, lows, volumes = data

    atr  = calc_atr(closes, highs, lows)
    if atr / price * 100 < 0.2: return None  # volatilitas terlalu rendah
    if atr / price * 100 > 8.0: return None  # terlalu volatil

    rsi        = calc_rsi(closes)
    macd, msig = calc_macd(closes)
    ema7       = calc_ema(closes, 7)
    ema20      = calc_ema(closes, 20)
    ema50      = calc_ema(closes, 50)
    vwap       = calc_vwap(closes, highs, lows, volumes)
    structure  = detect_structure(closes, highs, lows, strength=3, lookback=60)
    liq        = detect_liquidity(closes, highs, lows, lookback=40)

    if not structure["valid"]: return None

    # Hanya BUY (BUY_ONLY sesuai kebutuhan — SELL bisa diaktifkan nanti)
    side = "BUY"

    # Gate 1: harus ada BOS atau CHoCH bullish
    has_struct = (structure.get("bos") == "BULLISH" or
                  structure.get("choch") == "BULLISH" or
                  liq.get("sweep_bull"))
    if not has_struct:
        return None

    # Gate 2: MACD harus bullish cross
    if macd <= msig:
        return None

    # Gate 3: RSI tidak overbought
    if rsi > 70:
        return None

    # Gate 4: Momentum filter — harga harus di atas EMA7
    # Jika masih di bawah EMA7, momentum belum balik → entry terlalu dini
    if price < ema7:
        return None

    # Gate 5: EMA7 harus di atas EMA20 — konfirmasi short-term trend bullish
    # Mencegah entry di tengah downtrend seperti kasus KITE (EMA7 < EMA25 < EMA99)
    if ema7 < ema20:
        return None

    ob    = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=25)
    score = score_signal(side, price, closes, highs, lows, volumes,
                         structure, liq, ob, rsi, macd, msig,
                         ema20, ema50, vwap, ob_ratio)
    tier  = assign_tier(score)
    if tier == "SKIP": return None

    sl, tp1, tp2 = calc_sl_tp(price, side, atr, structure, "INTRADAY")

    # Sanity check
    if tp1 <= price or sl >= price: return None

    # Late signal filter — batalkan jika harga sudah terlalu jauh dari SL
    # Artinya entry sudah terlambat, tidak boleh kejar harga
    sl_dist = price - sl
    if sl_dist <= 0: return None
    if sl_dist / price > 0.05:   # SL lebih dari 5% dari entry = setup terlalu lebar
        return None

    # Cek apakah harga masih dekat dengan titik BOS (last_sh)
    # Jika harga sudah > 3% di atas last_sh, entry sudah terlambat
    last_sh = structure.get("last_sh")
    if last_sh and price > last_sh * 1.03:
        return None
    rr = (tp1 - price) / sl_dist
    if rr < MIN_RR["INTRADAY"]: return None

    return {
        "pair": pair, "strategy": "INTRADAY", "side": side,
        "timeframe": "1h", "entry": price,
        "tp1": tp1, "tp2": tp2, "sl": sl,
        "tier": tier, "score": score, "rr": round(rr, 1),
        "rsi": round(rsi, 1), "structure": structure,
    }


def check_swing(client, pair: str, price: float, ob_ratio: float,
                btc: dict) -> dict | None:
    """
    SWING signal — timeframe 4h.
    Gate lebih ketat dari intraday karena timeframe lebih panjang.
    Wajib: BOS/CHoCH + MACD + EMA momentum alignment + price > EMA50.
    """
    if btc["block_buy"]: return None

    data = get_candles(client, pair, "4h", 200)
    if data is None: return None
    closes, highs, lows, volumes = data

    atr  = calc_atr(closes, highs, lows)
    if atr / price * 100 < 0.5: return None
    if atr / price * 100 > 12.0: return None

    rsi        = calc_rsi(closes)
    macd, msig = calc_macd(closes)
    ema9       = calc_ema(closes, 9)
    ema21      = calc_ema(closes, 21)
    ema50      = calc_ema(closes, 50)
    ema200     = calc_ema(closes, 200)
    vwap       = calc_vwap(closes, highs, lows, volumes)
    structure  = detect_structure(closes, highs, lows, strength=4, lookback=100)
    liq        = detect_liquidity(closes, highs, lows, lookback=60)

    if not structure["valid"]: return None

    side = "BUY"

    # Gate 1: struktur bullish
    has_struct = (structure.get("bos") == "BULLISH" or
                  structure.get("choch") == "BULLISH" or
                  liq.get("sweep_bull"))
    if not has_struct: return None

    # Gate 2: MACD bullish
    if macd <= msig: return None

    # Gate 3: price harus di atas EMA50 — trend jangka menengah bullish
    if price <= ema50: return None

    # Gate 4: RSI tidak overbought
    if rsi > 65: return None

    # Gate 5: Momentum filter — harga harus di atas EMA9 (4h)
    # Sama seperti EMA7 di intraday — momentum belum balik jika masih di bawah
    if price < ema9: return None

    # Gate 6: EMA9 harus di atas EMA21 — konfirmasi trend 4h bullish
    # Filter downtrend kuat seperti KITE yang semua EMA bearish
    if ema9 < ema21: return None

    ob    = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=40)
    score = score_signal(side, price, closes, highs, lows, volumes,
                         structure, liq, ob, rsi, macd, msig,
                         ema50, ema200, vwap, ob_ratio)
    tier  = assign_tier(score)
    if tier == "SKIP": return None

    sl, tp1, tp2 = calc_sl_tp(price, side, atr, structure, "SWING")

    if tp1 <= price or sl >= price: return None

    # Late signal filter SWING — lebih ketat karena 4h candle = pergerakan lebih besar
    # Jika harga sudah > 5% di atas last_sh (titik BOS), entry sudah terlambat
    last_sh = structure.get("last_sh")
    if last_sh and price > last_sh * 1.05:
        return None

    sl_dist = price - sl
    if sl_dist <= 0: return None
    if sl_dist / price > 0.10:   # SL lebih dari 10% = setup terlalu lebar untuk SWING
        return None
    rr = (tp1 - price) / sl_dist
    if rr < MIN_RR["SWING"]: return None

    return {
        "pair": pair, "strategy": "SWING", "side": side,
        "timeframe": "4h", "entry": price,
        "tp1": tp1, "tp2": tp2, "sl": sl,
        "tier": tier, "score": score, "rr": round(rr, 1),
        "rsi": round(rsi, 1), "structure": structure,
    }


# ════════════════════════════════════════════════════════
#  TELEGRAM OUTPUT
# ════════════════════════════════════════════════════════

def send_signal(sig: dict):
    pair     = sig["pair"].replace("_USDT", "/USDT")
    strategy = sig["strategy"]
    side     = sig["side"]
    tier     = sig["tier"]
    score    = sig["score"]
    rr       = sig["rr"]
    entry    = sig["entry"]
    tp1      = sig["tp1"]
    tp2      = sig["tp2"]
    sl       = sig["sl"]
    tf       = sig["timeframe"]
    rsi      = sig["rsi"]
    bos      = sig["structure"].get("bos") or sig["structure"].get("choch") or "—"

    pct_tp1 = abs((tp1 - entry) / entry * 100)
    pct_tp2 = abs((tp2 - entry) / entry * 100)
    pct_sl  = abs((sl  - entry) / entry * 100)

    tier_emoji = {"S": "💎", "A+": "🏆", "A": "🥇"}.get(tier, "🎯")
    strat_emoji = {"INTRADAY": "📈", "SWING": "🌊"}.get(strategy, "🎯")
    side_emoji  = "🟢 BUY" if side == "BUY" else "🔴 SELL"

    # Valid window
    hours = 4 if strategy == "INTRADAY" else 16
    now   = datetime.now(WIB)
    valid_until = (now + timedelta(hours=hours)).strftime("%H:%M WIB")

    msg = (
        f"{strat_emoji} <b>{tier_emoji} [{tier}] SIGNAL {side_emoji} — {strategy}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair:    <b>{pair}</b> [{tf}]\n"
        f"⏰ Valid: {now.strftime('%H:%M')} → {valid_until}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Entry:   <b>${entry:.6f}</b>\n"
        f"TP1:     <b>${tp1:.6f}</b> <i>(+{pct_tp1:.1f}%)</i>\n"
        f"TP2:     <b>${tp2:.6f}</b> <i>(+{pct_tp2:.1f}%)</i>\n"
        f"SL:      <b>${sl:.6f}</b> <i>(-{pct_sl:.1f}%)</i>\n"
        f"R/R:     <b>1:{rr}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Score:   {score} | RSI: {rsi}\n"
        f"Struct:  {bos}\n"
        f"<i>⚠️ Pasang SL wajib. Ini signal, bukan rekomendasi finansial.</i>"
    )
    tg(msg)
    print(f"  ✅ SIGNAL {tier} {strategy} {side} {pair} RR:1:{rr} Score:{score}")


# ════════════════════════════════════════════════════════
#  PUMP SCANNER
# ════════════════════════════════════════════════════════

def already_sent_pump(pair: str) -> bool:
    """Cek apakah pump alert pair ini sudah dikirim dalam PUMP_DEDUP_HOURS jam terakhir."""
    try:
        since = (datetime.now(WIB) - timedelta(hours=PUMP_DEDUP_HOURS)).isoformat()
        res = supabase.table("signals_v2") \
            .select("id") \
            .eq("pair", pair) \
            .eq("strategy", "PUMP") \
            .gt("sent_at", since) \
            .execute()
        return len(res.data) > 0
    except Exception as e:
        print(f"⚠️ dedup pump check: {e}")
        return False


def check_pump(client, pair: str, price: float, vol_24h: float) -> dict | None:
    """
    PUMP SCANNER — timeframe 15m.
    Deteksi early pump berdasarkan:
      1. Volume spike: candle terakhir > PUMP_VOL_SPIKE × rata-rata 10 candle
      2. Price change: harga naik > PUMP_PRICE_CHANGE% dalam 3 candle 15m terakhir
      3. RSI belum overbought: RSI < PUMP_RSI_MAX
      4. MACD bullish cross searah
    Tidak menggunakan scoring tier — ini alert cepat, bukan sinyal entry penuh.
    """
    data = get_candles(client, pair, "15m", 50)
    if data is None:
        return None
    closes, highs, lows, volumes = data

    # Gate 1: volume spike
    vol_avg = float(np.mean(volumes[-11:-1]))  # rata-rata 10 candle sebelum terakhir
    if vol_avg <= 0:
        return None
    vol_ratio = float(volumes[-1]) / vol_avg
    if vol_ratio < PUMP_VOL_SPIKE:
        return None

    # Gate 2: price change dalam 3 candle 15m terakhir (~45 menit)
    price_3c_ago = float(closes[-4])
    if price_3c_ago <= 0:
        return None
    pct_change = (price - price_3c_ago) / price_3c_ago * 100
    if pct_change < PUMP_PRICE_CHANGE:
        return None

    # Gate 3: RSI belum overbought ekstrem
    rsi = calc_rsi(closes)
    if rsi > PUMP_RSI_MAX:
        return None

    # Gate 4: MACD bullish
    macd, msig = calc_macd(closes)
    if macd <= msig:
        return None

    # ATR untuk estimasi TP/SL kasar
    atr = calc_atr(closes, highs, lows)
    sl  = round(price - atr * 1.2, 8)
    tp1 = round(price + atr * 2.0, 8)

    pct_sl  = abs((sl  - price) / price * 100)
    pct_tp1 = abs((tp1 - price) / price * 100)

    return {
        "pair":       pair,
        "strategy":   "PUMP",
        "side":       "BUY",
        "timeframe":  "15m",
        "entry":      price,
        "tp1":        tp1,
        "sl":         sl,
        "rsi":        round(rsi, 1),
        "vol_ratio":  round(vol_ratio, 1),
        "pct_change": round(pct_change, 2),
        "pct_tp1":    round(pct_tp1, 2),
        "pct_sl":     round(pct_sl, 2),
    }


def send_pump_signal(sig: dict):
    """Kirim pump alert ke Telegram — format ringkas dan cepat."""
    pair       = sig["pair"].replace("_USDT", "/USDT")
    entry      = sig["entry"]
    tp1        = sig["tp1"]
    sl         = sig["sl"]
    rsi        = sig["rsi"]
    vol_ratio  = sig["vol_ratio"]
    pct_change = sig["pct_change"]
    pct_tp1    = sig["pct_tp1"]
    pct_sl     = sig["pct_sl"]

    now         = datetime.now(WIB)
    valid_until = (now + timedelta(hours=1)).strftime("%H:%M WIB")

    msg = (
        f"🚀 <b>PUMP ALERT — EARLY SIGNAL</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair  : <b>{pair}</b> [15m]\n"
        f"⏰ Valid: {now.strftime('%H:%M')} → {valid_until}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Entry : <b>${entry:.6f}</b>\n"
        f"TP1   : <b>${tp1:.6f}</b> <i>(+{pct_tp1:.1f}%)</i>\n"
        f"SL    : <b>${sl:.6f}</b> <i>(-{pct_sl:.1f}%)</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 Vol Spike : <b>{vol_ratio:.1f}×</b> rata-rata\n"
        f"📈 Naik 45m  : <b>+{pct_change:.2f}%</b>\n"
        f"RSI          : {rsi}\n"
        f"<i>⚡ Early pump alert. Entry cepat, SL wajib ketat.</i>\n"
        f"<i>⚠️ High risk — bukan rekomendasi finansial.</i>"
    )
    tg(msg)
    print(f"  🚀 PUMP ALERT {pair} | Vol:{vol_ratio:.1f}× | +{pct_change:.2f}% | RSI:{rsi}")


def run_pump_scan(client):
    """Jalankan pump scanner saja — dipanggil saat SCAN_MODE=pump."""
    print(f"\n{'='*60}")
    print(f"🚀 PUMP SCANNER — {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')}")
    print(f"{'='*60}")

    btc = get_btc_regime(client)
    print(f"BTC 1h: {btc['btc_1h']:+.1f}% | BTC 4h: {btc['btc_4h']:+.1f}%")

    # Halt jika BTC crash
    if btc["halt"]:
        print("🛑 BTC crash — pump scan skip"); return

    # Blok BUY jika BTC drop > 3% dalam 1h
    if btc["block_buy"]:
        print("⛔ BTC drop — pump scan skip"); return

    tickers = client.list_tickers()
    pumps   = []
    scanned = 0

    for t in tickers:
        pair = t.currency_pair
        if not is_valid_pair(pair):
            continue
        try:
            price   = float(t.last or 0)
            vol_24h = float(t.quote_volume or 0)
            if price <= 0 or vol_24h < PUMP_MIN_VOLUME:
                continue

            if already_sent_pump(pair):
                continue

            scanned += 1
            sig = check_pump(client, pair, price, vol_24h)
            if sig:
                pumps.append(sig)

            time.sleep(0.08)

        except Exception as e:
            print(f"⚠️ [{pair}]: {e}"); continue

    print(f"\n📊 Pump scan: {scanned} pairs | {len(pumps)} kandidat")

    if not pumps:
        print("📭 Tidak ada pump terdeteksi"); return

    # Sort by vol_ratio terbesar
    pumps.sort(key=lambda x: -x["vol_ratio"])

    sent = 0
    for sig in pumps:
        if sent >= MAX_PUMP_SIGNALS:
            break
        send_pump_signal(sig)
        # Simpan ke Supabase dengan strategy=PUMP
        save_signal(
            sig["pair"], "PUMP", sig["side"],
            sig["entry"], sig["tp1"], sig["tp1"],  # tp2 = tp1 untuk pump
            sig["sl"], "PUMP", 0, sig["timeframe"]
        )
        sent += 1
        time.sleep(0.5)

    print(f"\n✅ Pump scan done — {sent} alert terkirim")


# ════════════════════════════════════════════════════════
#  MAIN RUN
# ════════════════════════════════════════════════════════

def run():
    global _candle_cache
    _candle_cache = {}   # flush cache setiap cycle

    client = get_client()

    # ── Route berdasarkan SCAN_MODE ──────────────────────
    if SCAN_MODE == "pump":
        run_pump_scan(client)
        return

    # ── FULL MODE: INTRADAY + SWING ──────────────────────
    print(f"\n{'='*60}")
    print(f"🚀 SIGNAL BOT v2.1 — {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')} [FULL SCAN]")
    print(f"{'='*60}")

    fg     = get_fear_greed()
    btc    = get_btc_regime(client)
    print(f"F&G: {fg} | BTC 1h: {btc['btc_1h']:+.1f}% | BTC 4h: {btc['btc_4h']:+.1f}%")

    # Halt total jika BTC crash
    if btc["halt"]:
        tg(f"🛑 <b>SIGNAL BOT HALT</b>\n"
           f"BTC crash {btc['btc_4h']:+.1f}% dalam 4h.\n"
           f"Tidak ada signal sampai kondisi stabil.")
        print("🛑 BTC crash — bot halt"); return

    # Ambil semua pair
    tickers = client.list_tickers()
    signals  = []
    scanned  = 0
    skip_vol = 0

    for t in tickers:
        pair = t.currency_pair
        if not is_valid_pair(pair): continue

        try:
            price   = float(t.last or 0)
            vol_24h = float(t.quote_volume or 0)
            if price <= 0 or vol_24h < MIN_VOLUME_USDT:
                skip_vol += 1; continue

            ob_ratio = get_order_book_ratio(client, pair)
            scanned += 1

            # INTRADAY check
            if not already_sent(pair, "INTRADAY"):
                sig = check_intraday(client, pair, price, ob_ratio, btc)
                if sig: signals.append(sig)

            # SWING check
            if not already_sent(pair, "SWING"):
                sig = check_swing(client, pair, price, ob_ratio, btc)
                if sig: signals.append(sig)

            time.sleep(0.1)

        except Exception as e:
            print(f"⚠️ [{pair}]: {e}"); continue

    print(f"\n📊 Scanned: {scanned} | Vol filter: {skip_vol} | Candidates: {len(signals)}")

    if not signals:
        tg(f"🔍 <b>Scan Selesai — v2.1</b>\n"
           f"Pairs: {scanned} | F&G: {fg}\n"
           f"Tidak ada signal memenuhi kriteria saat ini.\n"
           f"<i>Bot akan scan lagi dalam 4 jam.</i>")
        print("📭 Tidak ada signal"); return

    # Sort by tier → score
    tier_order = {"S": 0, "A+": 1, "A": 2}
    signals.sort(key=lambda x: (tier_order.get(x["tier"], 9), -x["score"]))

    # Kirim maksimal MAX_SIGNALS_CYCLE signal terbaik
    sent = 0
    for sig in signals:
        if sent >= MAX_SIGNALS_CYCLE: break
        send_signal(sig)
        save_signal(
            sig["pair"], sig["strategy"], sig["side"],
            sig["entry"], sig["tp1"], sig["tp2"], sig["sl"],
            sig["tier"], sig["score"], sig["timeframe"]
        )
        sent += 1
        time.sleep(0.5)

    # Summary
    intraday_count = sum(1 for s in signals[:sent] if s["strategy"] == "INTRADAY")
    swing_count    = sum(1 for s in signals[:sent] if s["strategy"] == "SWING")

    tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v2.1</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Pairs scanned : <b>{scanned}</b>\n"
       f"F&G           : <b>{fg}</b>\n"
       f"BTC 1h/4h     : <b>{btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}%</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Signal terkirim: <b>{sent}</b>\n"
       f"  📈 INTRADAY : {intraday_count}\n"
       f"  🌊 SWING    : {swing_count}\n"
       f"<i>Scan berikutnya dalam 4 jam.</i>")

    print(f"\n✅ Done — {sent} signal terkirim dari {len(signals)} kandidat")


if __name__ == "__main__":
    run()
