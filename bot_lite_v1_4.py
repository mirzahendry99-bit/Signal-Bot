# -*- coding: utf-8 -*-
# Signal Bot Lite v1.4 — HIGH VOLUME MODE
# Diturunkan dari v1.3 — strategi identik, scoring dilonggarkan + kapasitas 13 slot
#
# CHANGELOG v1.3 → v1.4
#   [TUNE-1]   MIN_SCORE 3.5 → 2.5: threshold diturunkan agar lebih banyak sinyal lolos.
#              Tier label disesuaikan: A+ ≥ 3.5, A ≥ 3.0, B (sisanya ≥ MIN_SCORE).
#              Conviction label juga diperbarui sesuai skala baru.
#   [TUNE-2]   MAX_OPEN_TRADES 5 → 13: kapasitas portofolio diperluas penuh.
#   [TUNE-3]   MAX_SAME_SIDE 3 → 10: mengakomodasi 13 slot BUY di pasar bullish.
#   [TUNE-4]   MAX_SIGNALS_CYCLE 5 → 13: scan bisa mengirim sampai 13 sinyal per siklus.
#   [TUNE-5]   MAX_RISK_TOTAL 8% → 15%: budget risiko disesuaikan (13 × 1% = 13%).
#   [TUNE-6]   MIN_RR 1.5 → 1.2: RR minimum diturunkan agar lebih banyak setup lolos.
#   [TUNE-7]   PAIR_COOLDOWN_HOURS 24 → 12: cooldown lebih singkat, pair bisa re-entry lebih cepat.
#   [TUNE-8]   DEDUP_HOURS 6 → 4: window dedup diperpendek.
#   [NOTE]     Strategi inti, indikator, lifecycle state machine, SL/TP calc —
#              TIDAK BERUBAH. Hanya parameter threshold dan kapasitas yang diubah.
#
# CHANGELOG v1.2 → v1.3
#   [BUG-1]    Gate.io candle indexing fix — c[6] (SALAH) → c[1] (volume quote/USDT).
#              c[6] adalah 'sum' field atau IndexError pada 6-field format.
#              Tambah: diagnostic log field order saat pertama kali dipanggil,
#              dan guard len(c) < 6 sebelum indexing.
#   [BUG-2]    ACTIVE_HOURS clarification — eksplisit UTC (bukan WIB).
#              Log sekarang tampilkan UTC dan WIB bersamaan.
#              Override via env: ACTIVE_HOURS_UTC="1,16" (format start,end UTC).
#   [INFO-3]   DB Migration dikonsolidasikan — semua kolom baru dalam 1 blok SQL.
#
# ╔══════════════════════════════════════════════════════════╗
# ║  DB MIGRATION — WAJIB sebelum deploy                    ║
# ║  Jalankan di Supabase SQL editor (satu kali):           ║
# ║                                                          ║
# ║  ALTER TABLE signals_v2                                  ║
# ║    ADD COLUMN IF NOT EXISTS state TEXT DEFAULT 'OPEN',   ║
# ║    ADD COLUMN IF NOT EXISTS sl_breakeven DOUBLE PRECISION,║
# ║    ADD COLUMN IF NOT EXISTS remaining_size DOUBLE PRECISION;║
# ║                                                          ║
# ║  Bot AKAN ERROR runtime saat TP1 jika kolom belum ada.  ║
# ╚══════════════════════════════════════════════════════════╝

import os, time, json, math, logging
import numpy as np
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timedelta, timezone
from supabase import create_client
import gate_api

# ════════════════════════════════════════════════════════
#  VERSI & LOGGING
# ════════════════════════════════════════════════════════

BOT_VERSION = "1.4.0-lite"
WIB = timezone(timedelta(hours=7))

class _WIBFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=WIB)
        return dt.strftime("%Y-%m-%d %H:%M:%S WIB")

_handler = logging.StreamHandler()
_handler.setFormatter(_WIBFormatter("%(asctime)s [%(levelname)s] %(message)s"))
_logger = logging.getLogger("bot_lite")
_logger.setLevel(logging.INFO)
_logger.addHandler(_handler)
_logger.propagate = False

def log(msg: str, level: str = "info"):
    {"warn": _logger.warning, "error": _logger.error}.get(level, _logger.info)(msg)

# ════════════════════════════════════════════════════════
#  ENV & CLIENT
# ════════════════════════════════════════════════════════

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_KEY"]
TG_TOKEN      = os.environ["TELEGRAM_TOKEN"]
TG_CHAT_ID    = os.environ["CHAT_ID"]
GATE_API_KEY  = os.environ["GATE_API_KEY"]
GATE_SECRET   = os.environ["GATE_SECRET_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_gate_client():
    cfg = gate_api.Configuration(key=GATE_API_KEY, secret=GATE_SECRET)
    return gate_api.SpotApi(gate_api.ApiClient(cfg))

# ════════════════════════════════════════════════════════
#  KONFIGURASI
# ════════════════════════════════════════════════════════

MIN_VOLUME_USDT     = 150_000
MAX_SIGNALS_CYCLE   = 13        # [TUNE-4] 5 → 13
DEDUP_HOURS         = 4         # [TUNE-8] 6 → 4
PAIR_COOLDOWN_HOURS = 12        # [TUNE-7] 24 → 12

MIN_SCORE           = 2.5       # [TUNE-1] 3.5 → 2.5  ← threshold dilonggarkan
MIN_RR              = 1.2       # [TUNE-6] 1.5 → 1.2  ← RR minimum dilonggarkan
MAX_ENTRY_DEV       = 0.02

ADX_TREND           = 25
ADX_CHOP            = 20
ADX_PERIOD          = 14

BTC_DROP_BLOCK         = -3.0
BTC_CRASH_BLOCK        = -10.0
BTC_TREND_LOOKBACK     = 4
BTC_TREND_MIN_BEARISH  = 3

TP1_R             = 1.5
TP2_R             = 2.5
SL_ATR_MULT       = 2.0
ATR_SL_BUFFER     = 0.5
MIN_SL_PCT        = 0.005
MAX_SL_PCT        = 0.050

INITIAL_EQUITY    = float(os.getenv("INITIAL_EQUITY_USDT", "").strip() or "350.0")
RISK_PER_TRADE    = 0.01
BASE_POSITION     = 25.0
MAX_POSITION      = 40.0
MIN_POSITION      = 12.0

MAX_OPEN_TRADES   = 13          # [TUNE-2] 5 → 13
MAX_SAME_SIDE     = 13          # [TUNE-3] sama dengan MAX_OPEN_TRADES — SELL off jadi BUY bisa full 13
MAX_RISK_TOTAL    = 0.15        # [TUNE-5] 8% → 15% (13 × 1% = 13%, buffer 2%)

DD_WARN_PCT       = 0.07
DD_HALT_PCT       = 0.12
STREAK_WARN       = 3
STREAK_HALT       = 7

SIGNAL_EXPIRE_HOURS = 36

# ── ACTIVE_HOURS — 24 JAM MODE ───────────────────────────────────────────────
# [v1.4] Bot berjalan 24 jam penuh — tidak ada jam aktif / jam tidur.
# Crypto market tidak pernah tutup, jadi tidak perlu time-gate.
# Gate waktu di run() sudah dihapus — bot selalu scan kapanpun dipanggil.

# ── SELL system toggle ────────────────────────────────────────────────────────
# Crypto adalah structurally bullish market. Short breakdown jauh lebih sering
# false. Disable SELL sampai ada cukup sample untuk verifikasi edge-nya.
# Set ke True hanya jika SELL WR sudah terverifikasi dari data aktual.
SELL_ENABLED        = False

# ── Telegram notification level ──────────────────────────────────────────────
# "full"    : semua notifikasi (TP1, TP2, warn, scan, equity, lifecycle)
# "signal"  : hanya trade events (OPEN, TP1_HIT, CLOSE) — tanpa scan summary
# "minimal" : hanya trade close + equity report
TG_NOTIFY_LEVEL = os.getenv("TG_NOTIFY_LEVEL", "").strip() or "full"  # default: full
API_FAILURE_THRESHOLD = 5   # halt scan jika consecutive failure mencapai ini
API_DECAY_ON_SUCCESS  = 1   # setiap success: kurangi counter 1 (tidak langsung 0)
JSONL_PATH            = "signals.jsonl"

# ── SCAN_MODE ─────────────────────────────────────────────────────────────────
# "full"    : evaluate open trades + scan pair baru (default, jalankan tiap jam)
# "monitor" : hanya evaluate open trades — cepat, untuk cron 15/30 menit
SCAN_MODE = os.getenv("SCAN_MODE", "full").strip().lower()

# ════════════════════════════════════════════════════════
#  [CRITICAL-1] TRADE DATACLASS
#  Semua lifecycle functions menerima Trade, bukan dict.
#  Eliminates typo risk, field inconsistency, and silent bugs.
# ════════════════════════════════════════════════════════

@dataclass
class Trade:
    id:              str
    pair:            str
    side:            str           # "BUY" | "SELL"
    entry:           float
    sl:              float
    tp1:             float
    tp2:             float
    score:           float
    state:           str           # "OPEN" | "TP1_HIT" | "CLOSED"
    size:            float         # position size USDT
    sent_at:         str
    partial_result:  Optional[str]   = None
    sl_breakeven:    Optional[float] = None  # entry price, set saat TP1 hit
    remaining_size:  Optional[float] = None  # 50% dari size, set saat TP1 hit

    @classmethod
    def from_db_row(cls, row: dict) -> "Trade":
        """Build Trade dari Supabase row — safe dengan fallback."""
        return cls(
            id             = str(row["id"]),
            pair           = str(row["pair"]),
            side           = str(row.get("side") or "BUY"),
            entry          = float(row.get("entry") or 0),
            sl             = float(row.get("sl") or 0),
            tp1            = float(row.get("tp1") or 0),
            tp2            = float(row.get("tp2") or 0),
            score          = float(row.get("score") or 0),
            state          = str(row.get("state") or "OPEN"),
            size           = float(row.get("position_size") or BASE_POSITION),
            sent_at        = str(row.get("sent_at") or ""),
            partial_result  = row.get("partial_result"),
            sl_breakeven    = float(row["sl_breakeven"]) if row.get("sl_breakeven") else None,
            remaining_size  = float(row["remaining_size"]) if row.get("remaining_size") else None,
        )

# ════════════════════════════════════════════════════════
#  [MEDIUM-9] API FAILURE TRACKER
# ════════════════════════════════════════════════════════

_api_failures = 0

def _track_api(success: bool) -> None:
    global _api_failures
    if success:
        # Decay: kurangi 1 per success supaya tidak terlalu sticky
        # Endpoint buruk di 1 fungsi tidak langsung nol-kan counter
        _api_failures = max(0, _api_failures - API_DECAY_ON_SUCCESS)
    else:
        _api_failures += 1

def api_is_degraded() -> bool:
    if _api_failures >= API_FAILURE_THRESHOLD:
        log(f"⚠️ API degraded: {_api_failures} consecutive failures — scan dihentikan", "warn")
        return True
    return False

# ════════════════════════════════════════════════════════
#  [MEDIUM-8] JSONL ANALYTICS
#  Setiap trade event append ke signals.jsonl
#  Massively membantu debugging solo dev tanpa query DB.
# ════════════════════════════════════════════════════════

def append_jsonl(record: dict) -> None:
    try:
        record["_ts"] = datetime.now(timezone.utc).isoformat()
        with open(JSONL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
    except Exception as e:
        log(f"JSONL write error: {e}", "warn")

# ════════════════════════════════════════════════════════
#  TELEGRAM
# ════════════════════════════════════════════════════════

import urllib.request, urllib.parse

def tg(text: str) -> None:
    import re as _re
    # Guard: Telegram max 4096 chars
    MAX_TG = 4000
    if len(text) > MAX_TG:
        text = text[:MAX_TG] + "\n... (pesan dipotong)"

    def _send(msg: str, parse_mode: str = "HTML") -> bool:
        try:
            payload = urllib.parse.urlencode({
                "chat_id":    TG_CHAT_ID,
                "text":       msg,
                "parse_mode": parse_mode,
            }).encode()
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data=payload, method="POST"
            )
            urllib.request.urlopen(req, timeout=10)
            return True
        except Exception as e:
            log(f"TG error ({parse_mode}): {e}", "warn")
            return False

    # Coba HTML dulu — jika 400, retry sebagai plain text
    if not _send(text, "HTML"):
        plain = _re.sub(r"<[^>]+>", "", text)
        _send(plain, "")

def tg_operator(text: str) -> None:
    """
    Notifikasi untuk operator: scan summary, warn, API degraded, equity report.
    Dikirim hanya pada level 'full'. Level 'signal' atau 'minimal' → skip.
    """
    if TG_NOTIFY_LEVEL == "full":
        tg(text)

def tg_signal(text: str) -> None:
    """
    Notifikasi untuk trade event: OPEN, TP1_HIT, CLOSE.
    Dikirim pada level 'full' dan 'signal'. Level 'minimal' → skip.
    """
    if TG_NOTIFY_LEVEL in ("full", "signal"):
        tg(text)

def tg_close(text: str) -> None:
    """
    Notifikasi untuk final trade close dan equity report.
    Selalu dikirim di semua level.
    """
    tg(text)

# ════════════════════════════════════════════════════════
#  INDIKATOR TEKNIKAL
# ════════════════════════════════════════════════════════

def calc_ema(closes: list, period: int) -> float:
    if len(closes) < period:
        return closes[-1]
    k = 2.0 / (period + 1)
    ema = closes[0]
    for p in closes[1:]:
        ema = p * k + ema * (1.0 - k)
    return ema

def calc_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas    = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains     = [d if d > 0 else 0.0 for d in deltas[-period:]]
    losses    = [-d if d < 0 else 0.0 for d in deltas[-period:]]
    avg_gain  = sum(gains) / period
    avg_loss  = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)

# [CRITICAL-3] MACD fixed — signal = EMA9 dari MACD series, bukan EMA9 dari closes
def calc_macd(closes: list) -> tuple[float, float]:
    """
    MACD line  = EMA12 - EMA26
    Signal line = EMA9 dari MACD line series (bukan EMA9 dari closes!)
    Perlu minimal 34 candles (26 warmup + 9 - 1).
    """
    if len(closes) < 34:
        return 0.0, 0.0

    k12 = 2.0 / 13    # 2/(12+1)
    k26 = 2.0 / 27    # 2/(26+1)
    k9  = 2.0 / 10    # 2/(9+1)

    # Bangun MACD line series
    ema12 = closes[0]
    ema26 = closes[0]
    macd_series = []
    for price in closes:
        ema12 = price * k12 + ema12 * (1.0 - k12)
        ema26 = price * k26 + ema26 * (1.0 - k26)
        macd_series.append(ema12 - ema26)

    # Signal line = EMA9 dari MACD series
    signal = macd_series[0]
    for v in macd_series[1:]:
        signal = v * k9 + signal * (1.0 - k9)

    return round(macd_series[-1], 8), round(signal, 8)

def calc_atr(closes: list, highs: list, lows: list, period: int = 14) -> float:
    if len(closes) < 2:
        return highs[-1] - lows[-1]
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i]  - lows[i],
            abs(highs[i]  - closes[i-1]),
            abs(lows[i]   - closes[i-1])
        )
        trs.append(tr)
    return sum(trs[-period:]) / min(period, len(trs))

def calc_adx(closes: list, highs: list, lows: list, period: int = 14) -> float:
    if len(closes) < period * 2:
        return 20.0
    plus_dm_list, minus_dm_list, tr_list = [], [], []
    for i in range(1, len(closes)):
        h_diff = highs[i]  - highs[i-1]
        l_diff = lows[i-1] - lows[i]
        plus_dm_list.append(h_diff if h_diff > l_diff and h_diff > 0 else 0)
        minus_dm_list.append(l_diff if l_diff > h_diff and l_diff > 0 else 0)
        tr_list.append(max(
            highs[i] - lows[i],
            abs(highs[i]  - closes[i-1]),
            abs(lows[i]   - closes[i-1])
        ))
    def smooth(lst):
        s = sum(lst[:period])
        result = [s]
        for v in lst[period:]:
            s = s - s / period + v
            result.append(s)
        return result
    sm_tr    = smooth(tr_list)
    sm_plus  = smooth(plus_dm_list)
    sm_minus = smooth(minus_dm_list)
    dx_list  = []
    for i in range(len(sm_tr)):
        if sm_tr[i] == 0:
            continue
        pdi  = 100 * sm_plus[i]  / sm_tr[i]
        mdi  = 100 * sm_minus[i] / sm_tr[i]
        dsum = pdi + mdi
        dx_list.append(100 * abs(pdi - mdi) / dsum if dsum > 0 else 0)
    if not dx_list:
        return 20.0
    return sum(dx_list[-period:]) / min(period, len(dx_list))

def detect_regime(closes: list, highs: list, lows: list) -> dict:
    adx = calc_adx(closes, highs, lows)
    if adx >= ADX_TREND:
        regime = "TRENDING"
    elif adx >= ADX_CHOP:
        regime = "RANGING"
    else:
        regime = "CHOPPY"
    return {"regime": regime, "adx": round(adx, 1)}

def detect_structure(closes: list, highs: list, lows: list,
                     lookback: int = 60) -> dict:
    c = closes[-lookback:]
    h = highs[-lookback:]
    l = lows[-lookback:]
    n = len(c)
    last_sh, last_sl = None, None
    for i in range(n-2, 1, -1):
        if h[i] > h[i-1] and h[i] > h[i+1] and last_sh is None:
            last_sh = h[i]
        if l[i] < l[i-1] and l[i] < l[i+1] and last_sl is None:
            last_sl = l[i]
        if last_sh and last_sl:
            break
    return {
        "valid":   last_sh is not None and last_sl is not None,
        "last_sh": last_sh,
        "last_sl": last_sl,
    }

# ════════════════════════════════════════════════════════
#  [HIGH-7] SCORING — bonus hard-capped +0.5 total
#  Core factors max 3.0 — bonus range [-0.5, +0.5]
# ════════════════════════════════════════════════════════

def score_signal(side: str, price: float, closes: list,
                 highs: list, lows: list, volumes: list,
                 structure: dict, rsi: float, macd: float, msig: float,
                 ema20: float, ema50: float, regime: str,
                 btc_4h: float = 0.0, fg: int = 50) -> float:
    """
    Core factors (max 3.0):
      1. Trend    — EMA alignment
      2. Momentum — MACD crossover (pakai signal line yang kini mathematically valid)
      3. Volume   — volume spike vs rata-rata

    Bonus (dikumpulkan, lalu di-clamp ke [-0.5, +0.5]):
      +0.3 RSI zona ideal (40-60 untuk BUY, 40-60 untuk SELL)
      +0.3 BTC 4h searah entry
      +0.2 Structure quality
      -0.5 F&G ekstrem (<20 atau >80) — penalty tidak di-clamp

    Regime: RANGING multiplier 0.85
    """
    score = 0.0

    # ── Factor 1: Trend ──────────────────────────────────
    if side == "BUY":
        if ema20 > ema50 and price > ema20:
            score += 1.0
        elif ema20 > ema50:
            score += 0.5
    else:
        if ema20 < ema50 and price < ema20:
            score += 1.0
        elif ema20 < ema50:
            score += 0.5

    # ── Factor 2: Momentum (MACD — kini mathematically valid) ──
    if side == "BUY":
        if macd > msig and macd > 0:
            score += 1.0
        elif macd > msig:
            score += 0.5
    else:
        if macd < msig and macd < 0:
            score += 1.0
        elif macd < msig:
            score += 0.5

    # ── Factor 3: Volume spike ───────────────────────────
    avg_vol = sum(volumes[-11:-1]) / 10 if len(volumes) >= 11 else 0
    if avg_vol > 0:
        if volumes[-1] > avg_vol * 1.5:
            score += 1.0
        elif volumes[-1] > avg_vol * 1.2:
            score += 0.5

    # ── Bonus accumulation (di-clamp setelah terkumpul) ──
    raw_bonus = 0.0

    # RSI zona ideal
    if side == "BUY" and 40 <= rsi <= 65:
        raw_bonus += 0.3
    elif side == "SELL" and 35 <= rsi <= 60:
        raw_bonus += 0.3

    # BTC alignment
    if side == "BUY" and btc_4h > 0:
        raw_bonus += 0.3
    elif side == "SELL" and btc_4h < 0:
        raw_bonus += 0.3

    # Structure quality
    sh = structure.get("last_sh")
    sl_lvl = structure.get("last_sl")
    if sh and sl_lvl and (sh - sl_lvl) / sl_lvl > 0.02:
        raw_bonus += 0.2

    # Hard cap bonus — TIDAK BOLEH melebihi +0.5
    bonus = min(raw_bonus, 0.5)

    # F&G extreme penalty (applied setelah cap — tidak di-clamp)
    penalty = -0.5 if (fg < 20 or fg > 80) else 0.0

    score += bonus + penalty

    # Regime multiplier — RANGING sedikit dipenalti
    if regime == "RANGING":
        score *= 0.85

    return round(score, 2)

# ════════════════════════════════════════════════════════
#  SL / TP CALCULATOR
# ════════════════════════════════════════════════════════

def calc_sl_tp(entry: float, side: str, atr: float,
               structure: dict) -> tuple[float, float, float]:
    if side == "BUY":
        last_sl = structure.get("last_sl")
        if last_sl and last_sl < entry:
            sl = last_sl - atr * ATR_SL_BUFFER
        else:
            sl = entry - atr * SL_ATR_MULT
        sl = max(sl, entry * (1 - MAX_SL_PCT))
        sl = min(sl, entry * (1 - MIN_SL_PCT))
        sl_dist = entry - sl
        tp1 = entry + sl_dist * TP1_R
        tp2 = entry + sl_dist * TP2_R
    else:  # SELL — SL di atas swing high (sudah benar)
        last_sh = structure.get("last_sh")
        if last_sh and last_sh > entry:
            sl = last_sh + atr * ATR_SL_BUFFER
        else:
            sl = entry + atr * SL_ATR_MULT
        sl = min(sl, entry * (1 + MAX_SL_PCT))
        sl = max(sl, entry * (1 + MIN_SL_PCT))
        sl_dist = sl - entry
        tp1 = entry - sl_dist * TP1_R
        tp2 = entry - sl_dist * TP2_R

    return round(sl, 8), round(tp1, 8), round(tp2, 8)

# ════════════════════════════════════════════════════════
#  POSITION SIZING
# ════════════════════════════════════════════════════════

def calc_position_size(entry: float, sl: float, equity: float,
                       drawdown_mode: str = "normal") -> float:
    sl_pct = abs(entry - sl) / entry
    if sl_pct <= 0:
        return MIN_POSITION
    size = equity * RISK_PER_TRADE / sl_pct
    size = max(size, MIN_POSITION)
    size = min(size, MAX_POSITION)
    size = min(size, equity * 0.12)
    mult = {"normal": 1.0, "warn": 0.7, "halt": 0.4}.get(drawdown_mode, 1.0)
    return round(size * mult, 2)

# ════════════════════════════════════════════════════════
#  GATE.IO — CANDLES & TICKER
# ════════════════════════════════════════════════════════

# Gate.io API v4 candlestick field order (confirmed from official docs):
#   [0] t   — Unix timestamp (str)
#   [1] v   — Trading volume in quote currency (USDT)   ← VOLUME
#   [2] c   — Close price
#   [3] h   — Highest price
#   [4] l   — Lowest price
#   [5] o   — Opening price
#   [6] sum — Quote volume variant (8-field only, NOT base volume)
#
# Previous bug: c[6] dipakai sebagai volume — SALAH.
# c[6] adalah 'sum' (ada di 8-field format) atau IndexError (di 6-field format).
# Fix: pakai c[1] yang selalu ada dan merupakan trading volume sebenarnya.

_CANDLE_FORMAT_LOGGED = False   # log field order sekali saja saat startup

def get_candles(client, pair: str, interval: str = "1h",
                limit: int = 100) -> tuple | None:
    """
    Ambil candle data dari Gate.io.
    Return (closes, highs, lows, volumes) atau None.

    Field mapping (Gate.io API v4):
      c[0]=timestamp, c[1]=volume_usdt, c[2]=close,
      c[3]=high, c[4]=low, c[5]=open, c[6]=sum (optional)
    """
    global _CANDLE_FORMAT_LOGGED
    try:
        candles = client.list_candlesticks(pair, interval=interval, limit=limit)
        if not candles or len(candles) < 10:
            _track_api(True)
            return None

        # ── Diagnostic: log raw field order sekali saat pertama kali dipanggil ──
        if not _CANDLE_FORMAT_LOGGED:
            sample = candles[0]
            log(f"[CANDLE FORMAT] {pair} raw[0]: {list(sample)} "
                f"(len={len(sample)})", "info")
            log(f"[CANDLE FORMAT] idx→ [0]={sample[0]} ts | "
                f"[1]={sample[1]} vol | [2]={sample[2]} close | "
                f"[3]={sample[3]} high | [4]={sample[4]} low | "
                f"[5]={sample[5]} open", "info")
            _CANDLE_FORMAT_LOGGED = True

        # ── Guard: minimal 6 field per candle ──────────────────────────────────
        if len(candles[0]) < 6:
            log(f"   Unexpected candle format {pair}: only {len(candles[0])} fields", "warn")
            _track_api(False)
            return None

        closes  = [float(c[2]) for c in candles]
        highs   = [float(c[3]) for c in candles]
        lows    = [float(c[4]) for c in candles]
        # c[1] = quote volume (USDT) — selalu ada di 6-field dan 8-field format
        volumes = [float(c[1]) for c in candles]

        _track_api(True)
        return closes, highs, lows, volumes
    except Exception as e:
        log(f"   Candle error {pair}: {e}", "warn")
        _track_api(False)
        return None

# [HIGH-5] get_all_pairs fixed — list_tickers() tanpa arg, filter _USDT manual
def get_all_pairs(client) -> list[str]:
    """Ambil semua pair USDT tradable dari Gate.io."""
    try:
        tickers = client.list_tickers()   # ← tanpa currency_pair argument
        pairs = []
        for t in tickers:
            try:
                if not str(t.currency_pair).endswith("_USDT"):
                    continue
                vol = float(t.quote_volume or 0)
                if vol >= MIN_VOLUME_USDT:
                    pairs.append(t.currency_pair)
            except Exception:
                continue
        _track_api(True)
        return pairs
    except Exception as e:
        log(f"get_all_pairs error: {e}", "error")
        _track_api(False)
        return []

def get_ticker_price(client, pair: str) -> float | None:
    try:
        tickers = client.list_tickers(currency_pair=pair)
        if tickers:
            _track_api(True)
            return float(tickers[0].last)
    except Exception:
        _track_api(False)
    return None

# ════════════════════════════════════════════════════════
#  BTC REGIME
# ════════════════════════════════════════════════════════

def get_btc_regime(client) -> dict:
    data_1h = get_candles(client, "BTC_USDT", "1h", 10)
    data_4h = get_candles(client, "BTC_USDT", "4h", BTC_TREND_LOOKBACK + 2)

    btc_1h = btc_4h = 0.0
    halt = block_buy = btc_bearish_trend = False
    btc_bearish_cycles = 0

    if data_1h:
        closes = data_1h[0]
        if len(closes) >= 2:
            btc_1h = (closes[-1] - closes[-2]) / closes[-2] * 100
        if btc_1h <= BTC_CRASH_BLOCK:
            halt = True
        elif btc_1h <= BTC_DROP_BLOCK:
            block_buy = True

    if data_4h:
        closes = data_4h[0]
        if len(closes) >= 2:
            btc_4h = (closes[-1] - closes[-2]) / closes[-2] * 100
        recent = closes[-BTC_TREND_LOOKBACK:]
        bearish_count = sum(
            1 for i in range(1, len(recent))
            if recent[i] < recent[i-1]
        )
        btc_bearish_cycles = bearish_count
        if bearish_count >= BTC_TREND_MIN_BEARISH:
            btc_bearish_trend = True

    return {
        "btc_1h":             round(btc_1h, 2),
        "btc_4h":             round(btc_4h, 2),
        "halt":               halt,
        "block_buy":          block_buy,
        "btc_bearish_trend":  btc_bearish_trend,
        "btc_bearish_cycles": btc_bearish_cycles,
    }

# ════════════════════════════════════════════════════════
#  FEAR & GREED
# ════════════════════════════════════════════════════════

def get_fear_greed() -> int:
    try:
        req = urllib.request.Request(
            "https://api.alternative.me/fng/?limit=1",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read())
            return int(data["data"][0]["value"])
    except Exception:
        return 50

# ════════════════════════════════════════════════════════
#  SUPABASE — PORTFOLIO & DRAWDOWN
# ════════════════════════════════════════════════════════

def get_portfolio_state() -> dict:
    try:
        rows = (
            supabase.table("signals_v2")
            .select("strategy, side, pair, score, pnl_usdt, sl, entry, position_size")
            .is_("result", "null")
            .eq("strategy", "INTRADAY")  # filter: hanya trade bot_lite
            .execute()
            .data
        ) or []
        total      = len(rows)
        buy_count  = sum(1 for r in rows if r.get("side") == "BUY")
        sell_count = total - buy_count
        total_risk = sum(
            abs(float(r.get("entry", 0) or 0) - float(r.get("sl", 0) or 0))
            / float(r.get("entry", 1) or 1)
            * float(r.get("position_size", 0) or 0)
            for r in rows
        )
        open_pairs = [r.get("pair") for r in rows]
        return {
            "total": total, "buy": buy_count, "sell": sell_count,
            "total_risk_usdt": round(total_risk, 2),
            "open_pairs": open_pairs,
            "rows": rows,
        }
    except Exception as e:
        log(f"Portfolio state error: {e}", "warn")
        return {"total": 0, "buy": 0, "sell": 0,
                "total_risk_usdt": 0, "open_pairs": [], "rows": []}

def get_drawdown_state() -> dict:
    WIN_VALUES  = {"WIN", "TP1", "TP2", "PARTIAL_WIN"}
    LOSS_VALUES = {"LOSS", "SL", "EXPIRED_LOSS", "SL_AFTER_TP1"}
    try:
        rows = (
            supabase.table("signals_v2")
            .select("result, pnl_usdt, sent_at, strategy")
            .not_.is_("result", "null")
            .neq("result", "EXPIRED")
            .order("sent_at", desc=False)
            .limit(200)
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"Drawdown query error: {e}", "warn")
        return {"streak": 0, "mode": "normal", "dd_pct": 0.0}

    if not rows:
        persisted = _read_config("bot_streak", 0)
        return {"streak": persisted, "mode": "normal", "dd_pct": 0.0}

    streak = 0
    for row in reversed(rows):
        result   = (row.get("result") or "").upper()
        strategy = (row.get("strategy") or "").upper()
        if result in LOSS_VALUES and strategy != "SYSTEM":
            streak += 1
        elif result in WIN_VALUES:
            break

    equity  = INITIAL_EQUITY
    cum_pnl = 0.0
    peak_eq = equity
    for row in rows:
        pnl = row.get("pnl_usdt")
        if pnl is not None:
            try:
                cum_pnl += float(pnl)
            except (ValueError, TypeError):
                pass
        current_eq = equity + cum_pnl
        if current_eq > peak_eq:
            peak_eq = current_eq

    current_equity = equity + cum_pnl
    dd_pct = max(0.0, (peak_eq - current_equity) / peak_eq) if peak_eq > 0 else 0.0

    streak_mode = (
        "halt" if streak >= STREAK_HALT else
        "warn" if streak >= STREAK_WARN else "normal"
    )
    equity_mode = (
        "halt" if dd_pct >= DD_HALT_PCT else
        "warn" if dd_pct >= DD_WARN_PCT else "normal"
    )
    SEVERITY = {"normal": 0, "warn": 1, "halt": 2}
    if equity_mode == "halt":
        mode = "halt"
    elif streak_mode == "halt" and equity_mode == "normal":
        mode = "warn"
    else:
        mode = max(streak_mode, equity_mode, key=lambda m: SEVERITY[m])

    return {"streak": streak, "mode": mode, "dd_pct": round(dd_pct, 4)}

def _read_config(key: str, default=None):
    try:
        row = (
            supabase.table("bot_config")
            .select("value")
            .eq("key", key)
            .single()
            .execute()
            .data
        )
        val = row.get("value") if row else None
        if val is None:
            return default
        try:
            return type(default)(val) if default is not None else val
        except Exception:
            return val
    except Exception:
        return default

def _write_config(key: str, value) -> None:
    try:
        supabase.table("bot_config").upsert(
            {"key": key, "value": str(value),
             "updated_at": datetime.now(timezone.utc).isoformat()}
        ).execute()
    except Exception as e:
        log(f"Config write error ({key}): {e}", "warn")

def check_bot_halt() -> tuple[bool, str, int]:
    halted = _read_config("bot_halt", "false").lower() == "true"
    reason = _read_config("bot_halt_reason", "")
    streak = int(_read_config("bot_streak", 0) or 0)
    return halted, reason, streak

def set_bot_halt(halted: bool, reason: str = "", streak: int = 0) -> None:
    _write_config("bot_halt", "true" if halted else "false")
    _write_config("bot_halt_reason", reason)
    _write_config("bot_streak", streak)

# ════════════════════════════════════════════════════════
#  DEDUP
# ════════════════════════════════════════════════════════

def is_recently_signaled(pair: str) -> bool:
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=DEDUP_HOURS)).isoformat()
        rows = (
            supabase.table("signals_v2")
            .select("id")
            .eq("pair", pair)
            .gte("sent_at", cutoff)
            .limit(1)
            .execute()
            .data
        )
        return bool(rows)
    except Exception:
        return False

def is_in_cooldown(pair: str) -> bool:
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=PAIR_COOLDOWN_HOURS)).isoformat()
        rows = (
            supabase.table("signals_v2")
            .select("id")
            .eq("pair", pair)
            .not_.is_("result", "null")
            .gte("closed_at", cutoff)
            .limit(1)
            .execute()
            .data
        )
        return bool(rows)
    except Exception:
        return False

# ════════════════════════════════════════════════════════
#  SIGNAL CHECKER — INTRADAY
# ════════════════════════════════════════════════════════

def check_intraday(client, pair: str, price: float,
                   btc: dict, fg: int = 50,
                   side: str = "BUY") -> dict | None:
    if btc.get("halt"):
        return None
    if side == "BUY" and btc.get("block_buy"):
        return None
    if side == "BUY" and btc.get("btc_bearish_trend"):
        return None

    data = get_candles(client, pair, "1h", 100)
    if data is None:
        return None
    closes, highs, lows, volumes = data

    atr = calc_atr(closes, highs, lows)
    atr_pct = atr / price * 100
    if atr_pct < 0.2 or atr_pct > 8.0:
        return None

    mkt = detect_regime(closes, highs, lows)
    if mkt["regime"] == "CHOPPY":
        return None

    rsi        = calc_rsi(closes)
    macd, msig = calc_macd(closes)
    ema20      = calc_ema(closes, 20)
    ema50      = calc_ema(closes, 50)
    structure  = detect_structure(closes, highs, lows)

    if not structure["valid"]:
        return None

    score = score_signal(
        side, price, closes, highs, lows, volumes,
        structure, rsi, macd, msig, ema20, ema50,
        mkt["regime"], btc.get("btc_4h", 0.0), fg
    )

    if score < MIN_SCORE:
        return None

    # Adaptive threshold saat BTC bearish
    bearish_cycles = btc.get("btc_bearish_cycles", 0)
    adaptive_min   = MIN_SCORE + (0.5 if bearish_cycles >= 2 else 0.0)
    if score < adaptive_min:
        return None

    last_sh = structure.get("last_sh")
    last_sl = structure.get("last_sl")

    # [HIGH-4] Entry logic yang benar
    if side == "BUY":
        # BUY: entry di atas breakout swing high
        entry = round(last_sh * 1.002, 8) if (last_sh and price > last_sh) else price
    else:
        # SELL: entry di bawah breakdown swing low (bukan swing high)
        if last_sl and price <= last_sl * 1.01:
            entry = round(last_sl * 0.999, 8)   # sedikit di bawah support yang breakdown
        else:
            entry = price  # sudah breakdown, enter di current price

    dev = abs(price - entry) / entry
    if dev > MAX_ENTRY_DEV:
        return None

    sl, tp1, tp2 = calc_sl_tp(entry, side, atr, structure)

    if side == "BUY":
        if tp1 <= entry or sl >= entry:
            return None
        sl_dist = entry - sl
    else:
        if tp1 >= entry or sl <= entry:
            return None
        sl_dist = sl - entry

    if sl_dist <= 0:
        return None

    rr = abs(tp1 - entry) / sl_dist
    if rr < MIN_RR:
        return None

    tier = "A+" if score >= 3.5 else "A" if score >= 3.0 else "B"  # [TUNE-1] skala disesuaikan

    return {
        "pair":          pair,
        "strategy":      "INTRADAY",
        "side":          side,
        "timeframe":     "1h",
        "entry":         entry,
        "current_price": price,
        "tp1":           tp1,
        "tp2":           tp2,
        "sl":            sl,
        "tier":          tier,
        "score":         score,
        "rr":            round(rr, 1),
        "rsi":           round(rsi, 1),
        "regime":        mkt["regime"],
        "adx":           mkt["adx"],
    }

# ════════════════════════════════════════════════════════
#  PORTFOLIO GATE
# ════════════════════════════════════════════════════════

def portfolio_allows(sig: dict, state: dict, drawdown: dict) -> bool:
    if state["total"] >= MAX_OPEN_TRADES:
        log(f"   ⛔ {sig['pair']} — portfolio penuh ({state['total']}/{MAX_OPEN_TRADES})")
        return False
    if sig["side"] == "BUY" and state["buy"] >= MAX_SAME_SIDE:
        log(f"   ⛔ {sig['pair']} — max BUY ({state['buy']}/{MAX_SAME_SIDE})")
        return False
    if sig["side"] == "SELL" and state["sell"] >= MAX_SAME_SIDE:
        log(f"   ⛔ {sig['pair']} — max SELL ({state['sell']}/{MAX_SAME_SIDE})")
        return False
    if sig["pair"] in state.get("open_pairs", []):
        log(f"   ⛔ {sig['pair']} — pair sudah open")
        return False
    equity        = INITIAL_EQUITY
    new_risk      = equity * RISK_PER_TRADE
    total_risk_pct = (state["total_risk_usdt"] + new_risk) / equity
    if total_risk_pct > MAX_RISK_TOTAL:
        log(f"   ⛔ {sig['pair']} — risk budget penuh ({total_risk_pct*100:.1f}%)")
        return False
    return True

# ════════════════════════════════════════════════════════
#  [CRITICAL-2 + HIGH-6] TRADE LIFECYCLE — TRUE STATE MACHINE
#
#  States: OPEN → TP1_HIT → CLOSED
#
#  OPEN state:
#    - Cek 1m candle highs/lows (bukan ticker saja — wick terdeteksi)
#    - SL hit           → close "SL"
#    - TP2 hit          → close "TP2"
#    - TP1 hit          → transisi ke TP1_HIT (NOT closed), SL moved to entry (BE)
#
#  TP1_HIT state:
#    - Cek price vs sl_breakeven (= entry)
#    - sl_breakeven hit → close "SL_AFTER_TP1" (pnl = TP1 partial gain)
#    - TP2 hit          → close "TP2" (pnl = full TP2)
# ════════════════════════════════════════════════════════

def _resolve_trade_from_candles(trade: Trade,
                                 candle_highs: list,
                                 candle_lows: list) -> tuple[str | None, float | None]:
    """
    Periksa candle highs/lows untuk transisi state.
    Check SL dulu (worst case) sebelum TP — konservatif.
    Return (result_str, pnl) atau (None, None) jika tidak ada event.
    """
    if trade.state == "OPEN":
        if trade.side == "BUY":
            for lo, hi in zip(candle_lows, candle_highs):
                if lo <= trade.sl:
                    pnl = (trade.sl - trade.entry) / trade.entry * trade.size
                    return "SL", round(pnl, 4)
                if hi >= trade.tp2:
                    pnl = (trade.tp2 - trade.entry) / trade.entry * trade.size
                    return "TP2", round(pnl, 4)
                if hi >= trade.tp1:
                    return "TP1_HIT", None   # transisi state, bukan close
        else:  # SELL
            for lo, hi in zip(candle_lows, candle_highs):
                if hi >= trade.sl:
                    pnl = (trade.entry - trade.sl) / trade.entry * trade.size
                    return "SL", round(pnl, 4)
                if lo <= trade.tp2:
                    pnl = (trade.entry - trade.tp2) / trade.entry * trade.size
                    return "TP2", round(pnl, 4)
                if lo <= trade.tp1:
                    return "TP1_HIT", None

    elif trade.state == "TP1_HIT":
        be_sl = trade.sl_breakeven if trade.sl_breakeven is not None else trade.entry
        # remaining_size adalah 50% dari original size, di-set saat TP1_HIT
        rem = trade.remaining_size if trade.remaining_size is not None else trade.size * 0.5
        if trade.side == "BUY":
            for lo, hi in zip(candle_lows, candle_highs):
                if lo <= be_sl:
                    # Keluar di breakeven — lock TP1 partial gain (50%)
                    pnl = (trade.tp1 - trade.entry) / trade.entry * (trade.size * 0.5)
                    return "SL_AFTER_TP1", round(pnl, 4)
                if hi >= trade.tp2:
                    # TP2 hanya pada remaining_size (50%) — bukan full size
                    pnl = (trade.tp2 - trade.entry) / trade.entry * rem
                    return "TP2", round(pnl, 4)
        else:
            for lo, hi in zip(candle_lows, candle_highs):
                if hi >= be_sl:
                    pnl = (trade.entry - trade.tp1) / trade.entry * (trade.size * 0.5)
                    return "SL_AFTER_TP1", round(pnl, 4)
                if lo <= trade.tp2:
                    # TP2 hanya pada remaining_size (50%) — bukan full size
                    pnl = (trade.entry - trade.tp2) / trade.entry * rem
                    return "TP2", round(pnl, 4)

    return None, None


def evaluate_open_trades(client) -> dict:
    """
    Evaluasi semua open trades menggunakan 1m candle highs/lows.
    State machine: OPEN → TP1_HIT → CLOSED
    """
    try:
        open_rows = (
            supabase.table("signals_v2")
            .select("*")
            .is_("result", "null")
            .eq("strategy", "INTRADAY")  # filter: hanya trade bot_lite
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"Open trades query error: {e}", "warn")
        return {"evaluated": 0, "closed": 0}

    log(f"📋 Lifecycle: mengevaluasi {len(open_rows)} open trade(s)...")
    closed = 0

    for row in open_rows:
        try:
            trade = Trade.from_db_row(row)
        except Exception as e:
            log(f"   Trade parse error ({row.get('id')}): {e}", "warn")
            continue

        if not trade.pair:
            continue

        # [HIGH-6] Ambil 1m candles — deteksi intra-candle wick
        candle_data = get_candles(client, trade.pair, "1m", 10)
        if candle_data is None:
            # Fallback ke ticker last price jika candle gagal
            price = get_ticker_price(client, trade.pair)
            if price is None:
                continue
            # Gunakan ticker sebagai single-candle approximation
            candle_highs = [price]
            candle_lows  = [price]
        else:
            _, candle_highs, candle_lows, _ = candle_data

        # Cek expire sebelum level evaluation
        sent_at = trade.sent_at
        if sent_at:
            try:
                sent_dt = datetime.fromisoformat(sent_at.replace("Z", "+00:00"))
                age_h   = (datetime.now(timezone.utc) - sent_dt).total_seconds() / 3600
            except Exception:
                age_h = 0
        else:
            age_h = 0

        if age_h > SIGNAL_EXPIRE_HOURS:
            # Expire — tutup dengan last known price
            last_price = candle_data[0][-1] if candle_data else get_ticker_price(client, trade.pair)
            if last_price is None:
                continue
            if trade.side == "BUY":
                expire_result = "EXPIRED_LOSS" if last_price < trade.entry else "EXPIRED"
                pnl = (last_price - trade.entry) / trade.entry * trade.size
            else:
                expire_result = "EXPIRED_LOSS" if last_price > trade.entry else "EXPIRED"
                pnl = (trade.entry - last_price) / trade.entry * trade.size

            pnl_rounded = round(pnl, 4)

            # Hitung posisi % dari entry saat expired
            if trade.entry > 0:
                pos_pct = (last_price - trade.entry) / trade.entry * 100
                if trade.side == "SELL":
                    pos_pct = -pos_pct
            else:
                pos_pct = 0.0

            usia_j = int(age_h)

            # Kirim notifikasi expired SEBELUM _close_trade agar bisa custom format
            if expire_result == "EXPIRED_LOSS":
                tg_signal(
                    f"⏰❌ <b>Signal Expired — Posisi Rugi — {trade.pair}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"Strategy : {trade.side} {getattr(trade, 'strategy', 'INTRADAY') if hasattr(trade, 'strategy') else 'INTRADAY'}\n"
                    f"Usia     : {usia_j}j / {SIGNAL_EXPIRE_HOURS}j\n"
                    f"Posisi   : {pos_pct:+.2f}% dari entry\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"Posisi minus saat expired.\n"
                    f"Jika sudah entry, kelola posisi secara manual."
                )
            else:
                tg_signal(
                    f"⏰✅ <b>Signal Expired — Posisi Aman — {trade.pair}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"Strategy : {trade.side} INTRADAY\n"
                    f"Usia     : {usia_j}j / {SIGNAL_EXPIRE_HOURS}j\n"
                    f"Posisi   : {pos_pct:+.2f}% dari entry\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"Signal expired dalam kondisi positif."
                )

            _close_trade(trade, expire_result, pnl_rounded)
            closed += 1
            continue

        # Resolve state transition dari candle data
        event, pnl = _resolve_trade_from_candles(trade, candle_highs, candle_lows)

        if event is None:
            continue

        if event == "TP1_HIT":
            # ── Transisi ke TP1_HIT — BUKAN close ──────────────────
            remaining = round(trade.size * 0.5, 4)
            try:
                supabase.table("signals_v2").update({
                    "state":          "TP1_HIT",
                    "partial_result": "TP1",
                    "sl_breakeven":   trade.entry,
                    "remaining_size": remaining,
                }).eq("id", trade.id).execute()

                tp1_pct = (trade.tp1 - trade.entry) / trade.entry * 100 if trade.side == "BUY" \
                          else (trade.entry - trade.tp1) / trade.entry * 100
                realized_pnl = (abs(trade.tp1 - trade.entry) / trade.entry) * (trade.size * 0.5)

                tg_signal(
                    f"🎯 <b>Partial Profit Taken — {trade.pair}</b>\n"
                    f"TP1 tercapai {tp1_pct:+.1f}% ✅\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"• 50% posisi ditutup (adaptive RR={trade.score:.1f})\n"
                    f"• Realized: <b>+{realized_pnl:.2f} USDT</b>\n"
                    f"• SL digeser ke entry (breakeven)\n"
                    f"• Menunggu TP2 untuk sisa posisi..."
                )
                append_jsonl({
                    "event": "TP1_HIT", "pair": trade.pair, "side": trade.side,
                    "entry": trade.entry, "tp1": trade.tp1, "sl_be": trade.entry,
                    "remaining_size": remaining, "score": trade.score,
                })
                log(f"   ⚡ {trade.pair} TP1 HIT → state=TP1_HIT, SL moved to BE")
            except Exception as e:
                log(f"   TP1_HIT update error ({trade.pair}): {e}", "warn")

        else:
            # ── Final close (SL / TP2 / SL_AFTER_TP1) ─────────────
            _close_trade(trade, event, pnl)
            closed += 1

    log(f"📋 Lifecycle: {closed} trade(s) closed")
    return {"evaluated": len(open_rows), "closed": closed}


def _close_trade(trade: Trade, result: str, pnl: float | None) -> None:
    """Update DB dan kirim notifikasi untuk trade yang final-close."""
    try:
        supabase.table("signals_v2").update({
            "result":    result,
            "state":     "CLOSED",
            "pnl_usdt":  round(pnl, 4) if pnl is not None else None,
            "closed_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", trade.id).execute()

        pnl_str  = f"{pnl:+.2f} USDT" if pnl is not None else "-"
        pnl_pct  = (pnl / trade.size * 100) if (pnl is not None and trade.size > 0) else 0.0

        # Ambil kurs IDR untuk notifikasi close
        _idr = _get_idr_rate()

        def _idr_str(usdt_val: float) -> str:
            if _idr <= 0 or usdt_val == 0:
                return ""
            idr = abs(usdt_val) * _idr
            if idr >= 1_000_000:
                return f" (~Rp{idr/1_000_000:.2f}jt)"
            return f" (~Rp{idr:,.0f})"

        strat_label = f"INTRADAY {trade.side}"

        if result == "TP2":
            tp2_pct = (abs(trade.tp2 - trade.entry) / trade.entry * 100) if trade.entry > 0 else 0
            msg = (
                f"✅✅ <b>Full Target Reached — {trade.pair}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Strategy : {strat_label}\n"
                f"PnL      : <b>+{abs(pnl):.2f} USDT{_idr_str(pnl)}</b>\n"
                f"TP2 tercapai {tp2_pct:+.1f}% — target penuh tercapai 🎯"
            )
        elif result == "SL_AFTER_TP1":
            msg = (
                f"🔄 <b>Closed at Breakeven — {trade.pair}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Strategy : {strat_label}\n"
                f"PnL      : <b>{pnl_str}{_idr_str(pnl)}</b>\n"
                f"SL breakeven tercapai — TP1 profit tetap terkunci"
            )
        elif result == "SL":
            msg = (
                f"❌ <b>Stop Loss — {trade.pair}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Strategy : {strat_label}\n"
                f"PnL      : <b>{pnl_str}{_idr_str(pnl)}</b>\n"
                f"SL tersentuh — loss terkontrol"
            )
        else:
            msg = (
                f"⏰ <b>{trade.pair} — {result}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Strategy : {strat_label}\n"
                f"PnL      : <b>{pnl_str}{_idr_str(pnl)}</b>"
            )
        tg_close(msg)
        # [MEDIUM-8] JSONL analytics
        append_jsonl({
            "event": "CLOSE", "pair": trade.pair, "side": trade.side,
            "result": result, "pnl": pnl, "entry": trade.entry,
            "tp1": trade.tp1, "tp2": trade.tp2, "sl": trade.sl,
            "score": trade.score,
        })
        log(f"   {'✅' if result in ('TP2','SL_AFTER_TP1') else '❌'} "
            f"{trade.pair} — {result} | PnL: {pnl_str}")
    except Exception as e:
        log(f"   Close trade error ({trade.pair}): {e}", "warn")

# ════════════════════════════════════════════════════════
#  SEND SIGNAL
# ════════════════════════════════════════════════════════

def get_pair_winrate(pair: str) -> dict:
    """
    Ambil historical win rate untuk pair tertentu dari Supabase.
    Return: { wr_pct, wins, total, label, icon }
    """
    try:
        rows = (
            supabase.table("signals_v2")
            .select("result")
            .eq("pair", pair)
            .not_.is_("result", "null")
            .neq("result", "EXPIRED")
            .execute()
            .data
        ) or []

        WIN_RESULTS  = {"TP2", "TP1", "WIN", "PARTIAL_WIN", "SL_AFTER_TP1"}
        LOSS_RESULTS = {"SL", "LOSS", "EXPIRED_LOSS"}

        wins   = sum(1 for r in rows if r.get("result") in WIN_RESULTS)
        losses = sum(1 for r in rows if r.get("result") in LOSS_RESULTS)
        total  = wins + losses

        if total == 0:
            return {"wr_pct": None, "wins": 0, "total": 0,
                    "label": "Belum ada data", "icon": "⚪"}

        wr = wins / total * 100

        if total < 5:
            label = f"{wr:.0f}% (n={total}, data terbatas)"
            icon  = "🟡"
        elif wr >= 60:
            label = f"{wr:.0f}%★ (n={total})"
            icon  = "🟢"
        elif wr >= 45:
            label = f"{wr:.0f}% (n={total})"
            icon  = "🟡"
        else:
            label = f"{wr:.0f}%★ (n={total})"
            icon  = "🔴"

        return {"wr_pct": round(wr, 1), "wins": wins, "total": total,
                "label": label, "icon": icon}
    except Exception:
        return {"wr_pct": None, "wins": 0, "total": 0,
                "label": "Error", "icon": "⚪"}


def _fmt_price_signal(p: float) -> str:
    """Format harga untuk signal — presisi otomatis."""
    if p == 0:
        return "-"
    if p < 0.0001:
        return f"${p:.8f}"
    if p < 0.01:
        return f"${p:.6f}"
    if p < 1:
        return f"${p:.4f}"
    return f"${p:,.4f}"


def _fmt_idr_signal(usd_val: float, rate: float) -> str:
    """Format IDR dari nilai USD."""
    if rate <= 0 or usd_val <= 0:
        return ""
    idr = usd_val * rate
    if idr >= 1_000_000:
        return f" ≈ Rp{idr/1_000_000:.2f}jt"
    return f" ≈ Rp{idr:,.0f}"


def _get_idr_rate() -> float:
    """Ambil kurs USD/IDR dari exchangerate-api."""
    try:
        req = urllib.request.Request(
            "https://api.exchangerate-api.com/v4/latest/USD",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            return float(data["rates"].get("IDR", 0))
    except Exception:
        return 0.0


def send_signal(sig: dict, drawdown_mode: str = "normal") -> bool:
    equity  = INITIAL_EQUITY
    size    = calc_position_size(sig["entry"], sig["sl"], equity, drawdown_mode)
    sent_at = datetime.now(timezone.utc).isoformat()

    row = {
        "pair":          sig["pair"],
        "strategy":      sig["strategy"],
        "side":          sig["side"],
        "entry":         sig["entry"],
        "tp1":           sig["tp1"],
        "tp2":           sig["tp2"],
        "sl":            sig["sl"],
        "score":         sig["score"],
        "tier":          sig["tier"],
        "timeframe":     sig["timeframe"],
        "sent_at":       sent_at,
        "position_size": size,
        "status":        "OPEN",
        "state":         "OPEN",
    }

    try:
        supabase.table("signals_v2").insert(row).execute()
    except Exception as e:
        log(f"Insert signal error ({sig['pair']}): {e}", "error")
        return False

    # [MEDIUM-8] JSONL
    append_jsonl({
        "event": "OPEN", "pair": sig["pair"], "side": sig["side"],
        "entry": sig["entry"], "tp1": sig["tp1"], "tp2": sig["tp2"],
        "sl": sig["sl"], "score": sig["score"], "size": size,
        "rsi": sig["rsi"], "regime": sig["regime"],
    })

    # ── Data tambahan untuk format signal ───────────────────
    idr      = _get_idr_rate()
    hist_wr  = get_pair_winrate(sig["pair"])
    entry    = sig["entry"]
    tp1      = sig["tp1"]
    tp2      = sig["tp2"]
    sl       = sig["sl"]

    # Hitung persentase dari entry
    tp1_pct  = (tp1 - entry) / entry * 100 if entry > 0 else 0
    tp2_pct  = (tp2 - entry) / entry * 100 if entry > 0 else 0
    sl_pct   = (sl  - entry) / entry * 100 if entry > 0 else 0

    # Jam valid (4 jam dari sekarang, WIB)
    now_wib    = datetime.now(WIB)
    valid_until = (now_wib + timedelta(hours=4)).strftime("%H:%M")
    valid_from  = now_wib.strftime("%H:%M")

    # Regime icon
    regime_icon = "🔥" if sig["regime"] == "TRENDING" else "〰️"

    # Conviction dari score — [TUNE-1] skala disesuaikan dengan MIN_SCORE=2.5
    score = sig["score"]
    if score >= 3.5:
        conviction = "STRONG ✅✅"
    elif score >= 3.0:
        conviction = "GOOD ✅"
    elif score >= 2.5:
        conviction = "MODERATE ⚠️"
    else:
        conviction = "WEAK 🔻"

    # Why string
    why_parts = []
    if sig.get("regime") == "TRENDING":
        why_parts.append("EMA✅")
    why_parts.append("MACD✅")
    why_parts.append(sig["regime"])
    why_str = " | ".join(why_parts)

    # Struct label
    struct_label = "✅ Valid" if sig.get("regime") != "CHOPPY" else "—"

    side_icon  = "🟢" if sig["side"] == "BUY"  else "🔴"
    tier_medal = "🥇" if sig["tier"] == "A+" else "🥈" if sig["tier"] == "A" else "🥉"

    tg_signal(
        f"📈 {tier_medal} [{sig['tier']}] SIGNAL {side_icon} {sig['side']} — {sig['strategy']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair:    {sig['pair']} [{sig['timeframe']}]\n"
        f"⏰ Valid: {valid_from} → {valid_until} WIB\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Entry Zone : {_fmt_price_signal(entry)}{_fmt_idr_signal(entry, idr)} (limit / retest BOS)\n"
        f"TP1  : {_fmt_price_signal(tp1)}{_fmt_idr_signal(tp1, idr)} ({tp1_pct:+.1f}%)\n"
        f"TP2  : {_fmt_price_signal(tp2)}{_fmt_idr_signal(tp2, idr)} ({tp2_pct:+.1f}%)\n"
        f"SL   : {_fmt_price_signal(sl)}{_fmt_idr_signal(sl, idr)} ({sl_pct:+.1f}%)\n"
        f"R/R  : 1:{sig['rr']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Score:      {score:.1f}/4 | RSI: {sig['rsi']}\n"
        f"Struct:     {struct_label}\n"
        f"Regime:     {regime_icon} {sig['regime']} (ADX: {sig['adx']})\n"
        f"Hist WR:    {hist_wr['icon']} {hist_wr['label']}\n"
        f"Conviction: {conviction}\n"
        f"Why:        {why_str}\n"
        f"💰 Pos.Size : ${size:.2f} USDT (tier-adjusted)\n"
        f"⚠️ Pasang SL wajib. Ini signal, bukan rekomendasi finansial."
    )
    return True

# ════════════════════════════════════════════════════════
#  EQUITY REPORT
# ════════════════════════════════════════════════════════

def save_equity_snapshot(open_trades: int = 0) -> None:
    try:
        rows = (
            supabase.table("signals_v2")
            .select("pnl_usdt, result")
            .not_.is_("result", "null")
            .execute()
            .data
        ) or []

        cum_pnl = sum(
            float(r["pnl_usdt"]) for r in rows
            if r.get("pnl_usdt") is not None
        )
        equity = INITIAL_EQUITY + cum_pnl
        dd     = get_drawdown_state()

        total_closed = len(rows)
        wins   = sum(1 for r in rows if r.get("result") in ("WIN", "TP1", "TP2", "PARTIAL_WIN"))
        losses = sum(1 for r in rows if r.get("result") in ("LOSS", "SL", "EXPIRED_LOSS", "SL_AFTER_TP1"))
        wr     = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0

        supabase.table("equity_snapshots").insert({
            "equity_usdt": round(equity, 2),
            "pnl_usdt":    round(cum_pnl, 2),
            "open_trades": open_trades,
            "snapshot_at": datetime.now(timezone.utc).isoformat(),
        }).execute()

        tg_close(
            f"📊 <b>Equity Report</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Equity   : <b>${equity:.2f} USDT</b>\n"
            f"PnL      : {cum_pnl:+.2f} USDT\n"
            f"DD       : {dd['dd_pct']*100:.1f}% dari peak\n"
            f"WR       : {wr:.1f}% ({wins}W / {losses}L)\n"
            f"Open     : {open_trades} trades\n"
            f"<i>Snapshot: {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')}</i>"
        )
    except Exception as e:
        log(f"Equity snapshot error: {e}", "warn")

# ════════════════════════════════════════════════════════
#  MAIN RUN
# ════════════════════════════════════════════════════════

def run():
    log(f"\n{'='*55}")
    log(f"🚀 SIGNAL BOT LITE v{BOT_VERSION} — "
        f"{datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')}")
    log(f"⏰ Mode: 24 JAM ({SCAN_MODE.upper()}) | MIN_SCORE={MIN_SCORE} | MAX_OPEN={MAX_OPEN_TRADES}")
    log(f"{'='*55}")

    is_halted, halt_reason, persisted_streak = check_bot_halt()
    if is_halted:
        log(f"🛑 BOT HALT — {halt_reason}", "error")
        tg_operator(
            f"🛑 <b>Bot HALT — Scan dibatalkan</b>\n"
            f"Reason: {halt_reason}\n"
            f"<i>Bot akan auto-reset jika DD &lt; {DD_WARN_PCT*100:.0f}% "
            f"dan streak &lt; {STREAK_HALT}.</i>"
        )
        save_equity_snapshot(open_trades=0)
        return

    client = get_gate_client()

    # Evaluate open trades dulu — pakai state machine + 1m candles
    lifecycle = evaluate_open_trades(client)

    # [MEDIUM-9] Cek API health setelah evaluate (candle calls bisa trigger failure counter)
    if api_is_degraded():
        tg_operator(f"⚠️ <b>API Degraded</b>\n"
           f"{_api_failures} consecutive failures. Scan dibatalkan. Cek koneksi Gate.io.")
        return

    # ── MONITOR MODE: hanya evaluate open trades, skip scan pair baru ──────────
    if SCAN_MODE == "monitor":
        log(f"📡 MONITOR mode — {lifecycle['evaluated']} trade dievaluasi, "
            f"{lifecycle['closed']} ditutup. Skip scan.")
        portfolio = get_portfolio_state()
        tg_operator(
            f"📡 <b>Monitor — SIGNAL BOT LITE v{BOT_VERSION}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Open trades : {portfolio['total']}/{MAX_OPEN_TRADES}\n"
            f"Closed      : {lifecycle['closed']} trade(s)\n"
            f"<i>{datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')}</i>"
        )
        save_equity_snapshot(open_trades=portfolio["total"])
        return

    fg  = get_fear_greed()
    btc = get_btc_regime(client)
    log(f"🌡️ BTC 1h:{btc['btc_1h']:+.1f}% | 4h:{btc['btc_4h']:+.1f}% | "
        f"F&G:{fg} | Trend:{'⚠️ BEARISH' if btc['btc_bearish_trend'] else '✅ OK'}")

    if btc["halt"]:
        log("🚨 BTC CRASH — semua signal diblok", "error")
        tg_operator("🚨 <b>BTC Crash Alert</b>\nSemua signal diblok sementara.")
        save_equity_snapshot(open_trades=lifecycle.get("evaluated", 0))
        return

    drawdown = get_drawdown_state()
    dd_mode  = drawdown["mode"]
    log(f"📉 Drawdown: streak={drawdown['streak']} | "
        f"dd={drawdown['dd_pct']*100:.1f}% | mode={dd_mode.upper()}")

    if dd_mode == "halt":
        reason = f"streak={drawdown['streak']}, equity_dd={drawdown['dd_pct']*100:.1f}%"
        set_bot_halt(True, reason, drawdown["streak"])
        tg_operator(
            f"⚠️ <b>Drawdown Alert — HALT</b>\n"
            f"Streak : {drawdown['streak']} loss berturutan\n"
            f"DD     : {drawdown['dd_pct']*100:.1f}% dari peak\n"
            f"<i>Bot HALT. Auto-reset jika kondisi membaik.</i>"
        )
        save_equity_snapshot()
        return

    if dd_mode == "warn":
        tg_operator(
            f"⚠️ <b>Drawdown Warning</b>\n"
            f"Streak : {drawdown['streak']} loss berturutan\n"
            f"DD     : {drawdown['dd_pct']*100:.1f}% dari peak\n"
            f"<i>Position size dikurangi 30%.</i>"
        )

    portfolio = get_portfolio_state()
    log(f"🧠 Portfolio: {portfolio['total']}/{MAX_OPEN_TRADES} open "
        f"(BUY:{portfolio['buy']} SELL:{portfolio['sell']}) | "
        f"Risk: ${portfolio['total_risk_usdt']:.2f}")

    log("🔍 Mengambil daftar pair...")
    all_pairs = get_all_pairs(client)
    log(f"   {len(all_pairs)} pair memenuhi volume minimum")

    # [MEDIUM-9] Check API setelah get_all_pairs
    if api_is_degraded():
        tg_operator(f"⚠️ <b>API Degraded saat pair fetch</b>\nScan dibatalkan.")
        return

    signals_sent = 0
    scanned      = 0

    for pair in all_pairs:
        if signals_sent >= MAX_SIGNALS_CYCLE:
            break
        if portfolio["total"] + signals_sent >= MAX_OPEN_TRADES:
            break
        if pair in portfolio.get("open_pairs", []):
            continue
        if is_recently_signaled(pair):
            continue
        if is_in_cooldown(pair):
            continue

        # [MEDIUM-9] Per-pair API check
        if api_is_degraded():
            log("⚠️ API degraded mid-scan — membatalkan loop", "warn")
            break

        price = get_ticker_price(client, pair)
        if price is None:
            continue

        scanned += 1

        sig = check_intraday(client, pair, price, btc, fg, side="BUY")
        if sig is None and SELL_ENABLED and not btc.get("btc_bearish_trend"):
            sig = check_intraday(client, pair, price, btc, fg, side="SELL")

        if sig is None:
            continue

        if not portfolio_allows(sig, portfolio, drawdown):
            continue

        log(f"   ✅ SIGNAL: {pair} {sig['side']} score={sig['score']:.2f} "
            f"tier={sig['tier']} rr={sig['rr']}")

        if send_signal(sig, dd_mode):
            signals_sent += 1
            portfolio["total"] += 1
            portfolio["open_pairs"].append(pair)
            if sig["side"] == "BUY":
                portfolio["buy"] += 1
            else:
                portfolio["sell"] += 1

        time.sleep(0.3)

    log(f"\n{'='*55}")
    log(f"✅ Scan selesai — {scanned} pair diperiksa | {signals_sent} signal terkirim")

    # ── Ambil kurs IDR ──────────────────────────────────────
    idr_rate = 0
    try:
        idr_req = urllib.request.Request(
            "https://api.exchangerate-api.com/v4/latest/USD",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(idr_req, timeout=5) as idr_resp:
            idr_data = json.loads(idr_resp.read())
            idr_rate = float(idr_data["rates"].get("IDR", 0))
    except Exception:
        idr_rate = 0

    def fmt_idr(usd_val: float) -> str:
        """Format nilai USD ke IDR string, misal: ≈ Rp15,234"""
        if idr_rate <= 0 or usd_val <= 0:
            return ""
        idr = usd_val * idr_rate
        if idr >= 1_000_000:
            return f" ≈ Rp{idr/1_000_000:.2f}jt"
        return f" ≈ Rp{idr:,.0f}"

    # ── Build open trades detail ─────────────────────────────
    def build_open_trades_msg(client_ref) -> str:
        """Buat pesan detail open trades dengan harga terkini, IDR, usia, PnL%."""
        try:
            open_rows = (
                supabase.table("signals_v2")
                .select("pair, side, entry, sl, tp1, tp2, state, sent_at, score, strategy")
                .in_("state", ["OPEN", "TP1_HIT"])
                .eq("strategy", "INTRADAY")  # filter: hanya trade bot_lite
                .execute()
                .data
            ) or []
        except Exception as e:
            log(f"Open trades fetch error: {e}", "warn")
            return "📂 <b>Open Trades:</b> Gagal diambil"

        if not open_rows:
            return "📂 <b>Open Trades (0):</b> Tidak ada posisi terbuka"

        # Format harga — didefinisikan di luar loop agar tidak recreate tiap iterasi
        def fmt_price(p: float) -> str:
            if p == 0:
                return "-"
            if p < 0.0001:
                return f"${p:.8f}"
            if p < 0.01:
                return f"${p:.6f}"
            if p < 1:
                return f"${p:.4f}"
            return f"${p:,.4f}"

        now_wib = datetime.now(WIB)
        lines   = []

        for idx, r in enumerate(open_rows, 1):
            pair    = r.get("pair", "-")
            side    = r.get("side", "BUY")
            entry   = float(r.get("entry") or 0)
            sl      = float(r.get("sl") or 0)
            tp1     = float(r.get("tp1") or 0)
            tp2     = float(r.get("tp2") or 0)
            state   = r.get("state", "OPEN")
            score   = float(r.get("score") or 0)
            strat   = r.get("strategy", "INTRADAY")

            # Usia trade
            usia_str = "-"
            try:
                sent_raw = r.get("sent_at", "")
                if sent_raw:
                    sent_dt  = datetime.fromisoformat(sent_raw.replace("Z", "+00:00"))
                    delta    = now_wib - sent_dt.astimezone(WIB)
                    total_h  = int(delta.total_seconds() // 3600)
                    usia_str = f"{total_h}j"
            except Exception:
                pass

            # Harga terkini dari Gate.io
            now_price = get_ticker_price(client_ref, pair)

            # PnL % dari entry
            pnl_str = ""
            trend_icon = ""
            if now_price and entry > 0:
                pct = (now_price - entry) / entry * 100
                if side == "SELL":
                    pct = -pct
                trend_icon = "📈" if pct >= 0 else "📉"
                pnl_str = f"{pct:+.2f}%"

            # State badge
            if state == "TP1_HIT":
                state_icon = "⚡"
                state_badge = "TP1✅ nunggu TP2"
            else:
                state_icon = "🟢"
                state_badge = "OPEN"

            now_price_str = fmt_price(now_price) if now_price else "-"
            entry_str     = fmt_price(entry)
            sl_str        = fmt_price(sl)
            tp1_str       = fmt_price(tp1)
            tp2_str       = fmt_price(tp2)

            # Hitung TP% dari entry
            tp1_pct = f"({(tp1-entry)/entry*100:+.1f}%)" if entry > 0 and tp1 > 0 else ""
            tp2_pct = f"({(tp2-entry)/entry*100:+.1f}%)" if entry > 0 and tp2 > 0 else ""

            line = (
                f"{idx}. {state_icon} <b>{side} {pair}</b> [{strat}] {state_badge}\n"
                f"   Entry : {entry_str}{fmt_idr(entry)}\n"
                f"   TP1   : {tp1_str}{fmt_idr(tp1)} {tp1_pct}\n"
                f"   TP2   : {tp2_str}{fmt_idr(tp2)} {tp2_pct}\n"
                f"   SL    : {sl_str}{fmt_idr(sl)}\n"
                f"   Usia  : {usia_str} | Score: {score:.2f}\n"
                f"   Now   : {now_price_str}{fmt_idr(now_price) if now_price else ''} "
                f"{trend_icon} {pnl_str}"
            )
            lines.append(line)

        header = f"📂 <b>Open Trades ({len(open_rows)}/{MAX_OPEN_TRADES})</b>\n{'━'*22}\n"
        return header + "\n\n".join(lines)

    # ── Kirim pesan open trades terpisah ────────────────────
    open_trades_msg = build_open_trades_msg(client)
    tg(open_trades_msg)   # selalu kirim di semua level

    # ── Scan summary (dengan ringkasan open trades) ──────────
    open_summary = (
        f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"📂 Open: {portfolio['total']}/{MAX_OPEN_TRADES} "
        f"(BUY:{portfolio['buy']} SELL:{portfolio['sell']})"
    )

    tg_operator(
        f"🔍 <b>Scan Selesai — SIGNAL BOT LITE v{BOT_VERSION}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Pairs scanned  : {len(all_pairs)}\n"
        f"F&G            : {fg}\n"
        f"BTC 1h/4h      : {btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}%\n"
        f"API failures   : {_api_failures}\n"
        f"Sell mode      : {'ON' if SELL_ENABLED else 'OFF (disabled)'}\n"
        f"Equity aktif   : ${INITIAL_EQUITY:.2f} USDT\n"
        f"Portfolio open : {portfolio['total']} "
        f"(BUY:{portfolio['buy']} SELL:{portfolio['sell']})\n"
        f"Risk           : ${portfolio['total_risk_usdt']:.2f} / {MAX_RISK_TOTAL*100:.0f}% equity\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Signal terkirim : <b>{signals_sent}</b>\n"
        + (f"<i>Tidak ada signal memenuhi kriteria saat ini.</i>"
           if signals_sent == 0 else "")
        + open_summary
    )

    save_equity_snapshot(open_trades=portfolio["total"])


# ════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════

if __name__ == "__main__":
    run()
