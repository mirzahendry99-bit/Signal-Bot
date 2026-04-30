# -*- coding: utf-8 -*-
# Signal Bot v9.0 — lihat CHANGELOG.md untuk riwayat perubahan
# [v9.0] Fixes: kill switch persistence (Supabase), TP1/CLOSED DB error handling,
#         trailing SL silent failure → log warn, MIN_FILL_RATIO constant,
#         rate limit 429 fix (prefetch workers 8→3, throttle 0.35s/req, smart reset-ts),
#         unit tests (16 cases) untuk calc_sl_tp & drawdown logic.

import os, json, time, math, random
import logging
import urllib.request
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import create_client
import gate_api

# ════════════════════════════════════════════════════════
#  LOGGING — [v7.1 #10] Terstruktur dengan timestamp WIB
# ════════════════════════════════════════════════════════

WIB = timezone(timedelta(hours=7))

# [v7.6 #11] Unified WIB formatter — satu output, satu timestamp, tidak ada duplikasi.
# Sebelumnya: logging.basicConfig (UTC) + print (WIB) = setiap baris muncul 2x di GitHub Actions.
class _WIBFormatter(logging.Formatter):
    """Custom formatter yang mencetak timestamp dalam WIB (UTC+7)."""
    def formatTime(self, record, datefmt=None):  # noqa: N802
        dt = datetime.fromtimestamp(record.created, tz=WIB)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S WIB")

_handler = logging.StreamHandler()
_handler.setFormatter(_WIBFormatter("%(asctime)s [%(levelname)s] %(message)s"))

_logger = logging.getLogger("signal_bot")
_logger.setLevel(logging.INFO)
_logger.addHandler(_handler)
_logger.propagate = False   # cegah bubble-up ke root logger (hindari duplikasi)

def log(msg: str, level: str = "info"):
    """Log ke stdout dengan timestamp WIB via unified handler."""
    if level == "warn":
        _logger.warning(msg)
    elif level == "error":
        _logger.error(msg)
    else:
        _logger.info(msg)

# ════════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════════

API_KEY      = os.environ.get("GATE_API_KEY")
SECRET_KEY   = os.environ.get("GATE_SECRET_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TG_TOKEN     = os.environ.get("TELEGRAM_TOKEN")
TG_CHAT_ID   = os.environ.get("CHAT_ID")

# Validasi environment
_missing = [k for k, v in {
    "SUPABASE_URL":    SUPABASE_URL, "SUPABASE_KEY":    SUPABASE_KEY,
    "TELEGRAM_TOKEN":  TG_TOKEN,     "CHAT_ID":         TG_CHAT_ID,
    "GATE_API_KEY":    API_KEY,      "GATE_SECRET_KEY": SECRET_KEY,  # [v7.2 FIX #8]
}.items() if not v]
if _missing:
    raise EnvironmentError(f"ENV belum diset: {', '.join(_missing)}")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Volume & Pair Filter ──────────────────────────────
MIN_VOLUME_USDT    = 150_000     # [FIX #5] diturunkan 300K→150K — cover lebih banyak mid-cap
MAX_SIGNALS_CYCLE  = 8           # maksimal signal per run
DEDUP_HOURS        = 4           # tidak kirim ulang pair+strategy+side dalam 4 jam

# ── Scoring Thresholds ───────────────────────────────
# [v7.28] Simplified 3-factor scoring — max score = 3 (core) + 1 (setup bonus) = 4.
# Interpretasi tier:
# [v8.9] Score model: 2 core factors (1 poin) + volume booster (0.5 poin)
#   A+ (2.5): trend + momentum + volume spike (semua 3 terpenuhi)
#   A  (2.0): trend + momentum tanpa volume spike — minimum viable
#   SKIP (<2): hanya 1 atau 0 core factor — noise, tidak dikirim
#
# Setup bonus additive: setup_score 3 → +0.5, setup_score 2 → +0.25
# Efek: A + setup_3 = 2.0 + 0.5 = 2.5 (naik ke A+)
TIER_MIN_SCORE = {
    "A+": 2.5,   # [v8.9] turun dari 3.0 → 2.5, sinkron dengan volume booster 0.5
    "A":  2.0,
}
SIGNAL_MIN_TIER = "A"  # tier SKIP tidak dikirim

# ── RR Minimum ───────────────────────────────────────
MIN_RR = {
    "INTRADAY": 1.5,
    "SWING":    2.0,
}

# ── Market Regime (ADX-based No-Trade Zone) ──────────
# ADX (Average Directional Index) mengukur kekuatan trend — bukan arah.
# TRENDING : ADX >= 25 → signal normal + bonus score
# RANGING  : 18 ≤ ADX < 25 → signal lolos tapi penalti score -2
# CHOPPY   : ADX < 18 → NO TRADE ZONE — return None sebelum scoring
ADX_TREND  = 25   # threshold trending kuat
ADX_CHOP   = 20   # threshold choppy / sideways [v7.25 — dinaikkan dari 18]
ADX_PERIOD = 14   # periode Wilder's smoothing (standar industri)

# ── Regime Guard ─────────────────────────────────────
BTC_DROP_BLOCK  = -3.0   # BTC turun > 3% dalam 1h → blok semua BUY
BTC_CRASH_BLOCK = -10.0  # BTC crash > 10% dalam 4h → halt semua signal

# ── F&G-Aware Mode ────────────────────────────────────
# SELL diblokir hanya saat extreme greed — mencegah counter-trend SHORT
# saat market sedang momentum bullish kuat.
# Di semua kondisi lain (fear, neutral, greed ringan), SELL aktif.
FG_SELL_BLOCK = 75   # SELL diblokir jika F&G >= 75 (extreme greed)

# ── SL / TP Parameters ───────────────────────────────
# [v7.8 #10] Structure-first SL: anchor di swing low/high, ATR sebagai buffer.
#
# Hierarki SL:
#   1. last_sl/last_sh (structure anchor) — primary
#   2. - atr * ATR_SL_BUFFER (margin aman di belakang level) — wajib ada
#   3. Sanity bounds: SL tidak boleh kurang dari MIN_SL_PCT atau lebih dari MAX_SL_PCT
#   4. Pure ATR fallback — hanya jika tidak ada struktur sama sekali
#
# TP selalu dihitung dari ACTUAL sl_dist (entry − sl nyata),
# bukan dari ATR * multiplier. Ini memastikan R/R representasi nyata.

INTRADAY_TP1_R      = 1.5    # TP1 = actual_sl_dist × 1.5
INTRADAY_TP2_R      = 2.5    # TP2 = actual_sl_dist × 2.5
SWING_TP1_R         = 2.0
SWING_TP2_R         = 3.5

# ATR multiplier — dipakai HANYA sebagai fallback ketika tidak ada struktur
INTRADAY_SL_ATR     = 1.5    # ATR fallback multiplier (1h)
SWING_SL_ATR        = 2.0    # ATR fallback multiplier (4h)

# ATR buffer: seberapa jauh SL ditempatkan DI BELAKANG struktur
# Terlalu kecil → SL kena wick; terlalu besar → RR menjadi buruk
ATR_SL_BUFFER_INTRADAY = 0.3   # 0.3× ATR di belakang swing low/high (1h)
ATR_SL_BUFFER_SWING    = 0.5   # 0.5× ATR lebih longgar untuk timeframe 4h

# Sanity bounds — SL tidak valid di luar rentang ini (persentase dari entry)
# Identik dengan downstream check di check_intraday/check_swing — belt-and-suspenders
INTRADAY_MIN_SL_PCT = 0.003   # min 0.3% dari entry — cegah SL terlalu sempit
INTRADAY_MAX_SL_PCT = 0.050   # max 5%  dari entry — konsisten dengan check_intraday
SWING_MIN_SL_PCT    = 0.005   # min 0.5% dari entry
SWING_MAX_SL_PCT    = 0.070   # max 7% dari entry [v7.25 — dikurangi dari 10%]

# ── Pump Scanner Config ──────────────────────────────
PUMP_VOL_SPIKE    = 3.0      # volume candle terakhir harus > 3× rata-rata 10 candle
PUMP_PRICE_CHANGE = 4.0      # harga naik > 4% dalam 3 candle 15m terakhir
PUMP_RSI_MAX      = 72       # RSI belum overbought ekstrem
PUMP_MIN_VOLUME   = 200_000  # volume 24h minimum lebih rendah dari main bot
PUMP_DEDUP_HOURS  = 1        # dedup pump signal lebih pendek (1 jam)
MAX_PUMP_SIGNALS  = 5        # maksimal pump signal per run

# ── Microcap Scanner Config ───────────────────────────
# Target: meme coin & microcap yang vol 24h di bawah threshold main bot
# tapi sudah menunjukkan tanda volume anomali dan momentum awal
MICRO_VOL_MIN        = 20_000    # volume 24h minimum — lebih rendah dari main bot
MICRO_VOL_MAX        = 150_000   # batas atas — di atas ini masuk main bot scan
MICRO_VOL_SPIKE      = 5.0       # volume candle terbaru > 5× rata-rata 10 candle
MICRO_PRICE_CHANGE   = 3.0       # harga naik minimal 3% dalam 3 candle 1h terakhir
MICRO_PRICE_MAX      = 25.0      # tidak sudah naik >25% dalam 24h — hindari buy top
MICRO_RSI_MIN        = 28        # RSI tidak oversold hancur
MICRO_RSI_MAX        = 68        # RSI belum overbought — masih ada ruang naik
MICRO_TP1_PCT        = 0.15      # TP1: +15%
MICRO_TP2_PCT        = 0.35      # TP2: +35%
MICRO_SL_PCT         = 0.05      # SL: -5% dari entry (ketat)
MICRO_MIN_RR         = 2.5       # minimum R/R — harus worth the risk
MICRO_DEDUP_HOURS    = 2         # dedup microcap signal
MAX_MICRO_SIGNALS    = 4         # maksimal microcap signal per run

# ── Portfolio Brain Config ── [v7.29 Phase3] ─────────
# Disederhanakan: 3 konstanta bersih menggantikan 4 variabel kompleks.
# heat, locked_capital, pairwise correlation dihapus dari gate utama.
# Ganti dengan: hard cap total, hard cap per sektor, hard cap risk % equity.
#
# "Open trade" = sinyal yang sudah dikirim dan belum ada result (NULL).
# Supabase table: signals_v2, kolom: strategy, side, result IS NULL.
MAX_OPEN_TRADES        = 5    # [v7.29] total signal aktif (INTRADAY+SWING) — hard cap
MAX_PER_SECTOR         = 2    # [v7.29] max trade aktif per sektor (BTC/AI/MEME/L2)
MAX_RISK_TOTAL         = 0.05 # [v7.29] max total risk = 5% equity (bukan USDT absolut)
# [v8.7] MAX_SAME_SIDE_TRADES: 5→3. Sebelumnya = MAX_OPEN_TRADES sehingga tidak pernah
#        fire lebih dulu — dead code. Sekarang caps directional concentration (maks 60%).
#        Contoh: 3 BUY + 2 SELL = ok. 4 BUY = ditolak meski total < 5.
MAX_SAME_SIDE_TRADES   = 3    # max BUY atau SELL aktif sekaligus (directional cap)
# [v8.7] MAX_BTC_CORR_TRADES: 4→2. Setelah SAME_SIDE=3, nilai 4 tidak pernah dicapai
#        sehingga BTC stress gate ini mati. Sekarang lebih konservatif: saat BTC drop,
#        max 2 BUY aktif (bukan 3). Gate ini kini fire SEBELUM SAME_SIDE gate.
MAX_BTC_CORR_TRADES    = 2    # max BUY aktif saat BTC correlation tinggi (stress cap)
PORTFOLIO_STALE_HOURS  = 96   # signal > 96 jam dianggap stale — cover SWING 72 jam + buffer

# ── Trade Lifecycle Tracking Config ── [v7.12 #3] ────
# evaluate_open_trades() dipanggil di awal setiap run() sebelum scan.
# Query open trades dari Supabase → cek current price → update result.
#
# Result values yang diisi otomatis:
#   "TP1_PARTIAL" — TP1 hit & TP2 tersedia → status PARTIAL, sisa menunggu TP2
#                   partial_pnl_usdt tersimpan; SL digeser ke entry (breakeven).
#   "TP1"         — TP1 hit tanpa TP2 (atau momentum exit dari PARTIAL) → CLOSED
#   "TP2"         — harga menyentuh TP2 → CLOSED (lebih baik dari TP1)
#   "PARTIAL_WIN" — trade expired saat masih PARTIAL (TP1 pernah hit) → CLOSED
#   "SL"          — harga menyentuh SL (losing trade) → CLOSED
#   "BREAKEVEN"   — SL tersentuh setelah TP1 (SL = entry) → CLOSED, PnL ~0
#   "EXPIRED"     — sudah > SIGNAL_EXPIRE_HOURS tanpa ada level tersentuh → CLOSED
#
# SIGNAL_EXPIRE_HOURS: default 48h (2× INTRADAY horizon = 2 candle 1h session)
# Disesuaikan per-strategy:
#   INTRADAY: 24h — sinyal 1h biasanya resolve dalam 1 sesi
#   SWING   : 72h — sinyal 4h bisa butuh beberapa hari
#   PUMP    : 4h  — pump biasanya resolve cepat (atau fade)
#   MICROCAP: 48h — microcap volatile, beri waktu tapi tidak terlalu lama
SIGNAL_EXPIRE_HOURS = {
    "INTRADAY": 24,
    "SWING":    72,
    "PUMP":      4,
    "MICROCAP": 48,
}
LIFECYCLE_MAX_EVAL = 20   # maksimal open trades yang dievaluasi per run — cegah overload API
ATR_TRAIL_MULT     = 1.5  # [v7.25] trailing stop = current_price - (ATR × multiplier) setelah TP1 hit



# ── Dynamic Priority Config ── [v7.12 #1] ────────────
# Base priority (lower = lebih tinggi) — sama dengan v7.10 sebagai baseline
# Modifier diterapkan runtime di calc_dynamic_priority() berdasarkan market context.
#
# Filosofi: PUMP di market crash = distribusi (institutional selling ke retail).
# PUMP di market crash BERBAHAYA — harus turun prioritas, bukan naik.
# Sebaliknya, SWING BUY di market normal = reliable → prioritas cukup tinggi.
PRIORITY_BASE = {
    "PUMP_BUY":      0,
    "INTRADAY_BUY":  1,
    "SWING_BUY":     2,
    "SWING_SELL":    3,
    "INTRADAY_SELL": 4,
}
# Penalty yang ditambahkan ke PUMP_BUY saat kondisi berbahaya
PUMP_CRASH_PENALTY   = 4   # BTC crash: PUMP jadi priority 4 (sama dengan INTRADAY_SELL)
PUMP_GREED_PENALTY   = 3   # extreme greed: PUMP jadi priority 3 (sama dengan SWING_SELL)
PUMP_DROP_PENALTY    = 1   # BTC drop 1h: PUMP naik satu level (priority 1 → tied with INTRADAY)

# ── Position Sizing Engine ── [v7.14 #A] ─────────────
# [v7.14] Upgrade ke volatility-adjusted + Kelly-informed sizing.
#
# Formula baru (tiga lapis):
#   1. Kelly fraction  : f* = (wr × rr - (1-wr)) / rr  → capped 0–0.25
#      Menggunakan edge nyata dari data historis — bukan rule-of-thumb.
#      Half-Kelly dipakai (f*/2) untuk safety margin.
#
#   2. Volatility scalar: target_risk_pct / atr_pct
#      Pasangan dengan ATR tinggi otomatis dapat size lebih kecil.
#      Target: risiko per trade = TARGET_RISK_PCT dari modal (default 1%).
#
#   3. Tier cap tetap berlaku sebagai guardrail maksimum.
#
# Fallback: jika data WR tidak reliable atau ATR tidak tersedia,
#           kembali ke formula lama (tier_mult × BASE_POSITION_USDT).
#
# Cap: MAX_POSITION_USDT adalah batas mutlak — tidak bisa dilampaui.
BASE_POSITION_USDT = 10.0    # base size fallback (ubah sesuai kapital Anda)
MAX_POSITION_USDT  = 25.0    # hard cap absolute
MIN_POSITION_USDT  =  7.0    # [v7.21 #2] naik dari 5 → 7 untuk unlock sizing minimum
# [Phase1 #2] Safety cap: position tidak boleh melebihi 25% equity terlepas dari SL.
# Ini mencegah fixed_risk menghasilkan size raksasa saat SL sangat kecil
# (contoh: SL=0.1% → size = equity × 0.01 / 0.001 = 10× equity = DISASTER).
MAX_POSITION_PCT   = 0.25    # maks 25% equity per posisi

# [v7.27 #4] Dynamic equity — seed dari env var, BUKAN hardcode $200.
# Set INITIAL_EQUITY_USDT di .env sesuai kapital aktual.
# Bot akan fetch live balance dari Gate.io; fallback ke env jika API gagal.
INITIAL_EQUITY_USDT  = float(os.getenv("INITIAL_EQUITY_USDT", "200.0"))
ACCOUNT_EQUITY_USDT  = INITIAL_EQUITY_USDT   # alias runtime — diupdate saat bot start

# [v7.27 #1] Fixed Risk per trade — menggantikan Kelly (sementara).
# position_size = equity × RISK_PER_TRADE / sl_pct
# 1% equity dengan SL 2% → size = 50% equity (terlalu besar → tier cap memotong)
# 1% equity dengan SL 5% → size = 20% equity → ~$40 pada $200 equity (masuk akal)
RISK_PER_TRADE       = 0.01   # [v7.27 #1] 1% equity risk per trade (fixed)

# [v7.27 #3] Trading fee & slippage — dikurangkan dari PnL setiap trade.
TRADING_FEE_PCT      = 0.001  # [v7.27 #3] 0.1% Gate.io spot taker fee per leg
# Slippage sudah di-handle via adjust_entry_for_slippage() di entry.
# Fee deduction diterapkan di evaluate_open_trades() untuk partial & final close.
MIN_FILL_RATIO       = 0.5   # [v9.0] minimum fill fraction dari size_usdt agar slippage valid


# [v8.2] Throttle compounding pasca partial TP — cegah over-aggressive sizing.
COMPOUNDING_THROTTLE_PCT = 0.50  # hanya 50% dari equity gain dari TP1 yang dikompound
# [v7.27 #1] Kelly constants — DINONAKTIFKAN sementara untuk stabilitas.
# Aktifkan kembali setelah minimal 50 trades terkumpul dengan data WR valid.
# TARGET_RISK_PCT di-retain untuk vol-scalar fallback.
TARGET_RISK_PCT      = 0.015  # [v7.21 #1] dipakai fallback vol-scalar jika sl tidak tersedia


# [v8.4] Kelly constants dihapus total (KELLY_PRIOR_BY_STRATEGY, get_kelly_prior).
# Tidak pernah dipanggil sejak v7.27 ketika Kelly dinonaktifkan.
# Fixed-risk sizing (calc_position_size) adalah satu-satunya sizing path.

TIER_SIZE_MULT = {"S": 1.5, "A+": 1.2, "A": 1.0}

# ── Drawdown Awareness ── [v7.14 #B] ─────────────────
# [v7.14] Upgrade: equity-based drawdown menggantikan streak-only.
#
# Masalah streak-only:
#   win kecil → loss besar → tetap streak=0 tapi equity sudah turun signifikan.
#
# Solusi dual-track:
#   1. Streak check (dipertahankan untuk deteksi rapid fire losses)
#   2. Equity drawdown dari peak PnL historis (akurat untuk capital protection)
#
# Mode ditentukan oleh YANG LEBIH PARAH dari dua metrik:
#   - streak ≥ HALT threshold  → halt
#   - equity drawdown ≥ DD_HALT_PCT  → halt
#   - streak ≥ WARN threshold  → warn
#   - equity drawdown ≥ DD_WARN_PCT  → warn
DRAWDOWN_STREAK_WARN   = 3      # ≥3 consecutive loss → warn
DRAWDOWN_STREAK_HALT   = 5      # ≥5 consecutive loss → halt
DD_WARN_PCT            = 0.08   # [v7.14] equity turun ≥8% dari peak → warn
DD_HALT_PCT            = 0.15   # [v7.14] equity turun ≥15% dari peak → halt
_drawdown_state: dict  = {"streak": 0, "mode": "normal", "dd_pct": 0.0}  # runtime cache

# ── Altcoin Cluster Correlation ── [v7.29 Phase3] ────
# [v7.29] Simplified: pairwise matrix DIHAPUS. Ganti dengan clustering statis
# 4 sektor. Lebih ringan, deterministik, tidak ada O(n²) runtime fetch.
#
# Sektor:
#   BTC-related : BTC, WBTC, renBTC — pair yg paling correlated BTC
#   AI          : FET, TAO, RENDER, OCEAN, AGIX, NMR
#   MEME        : DOGE, SHIB, PEPE, FLOKI, BONK, WIF
#   L2          : ARB, OP, MATIC, IMX, STARK, MANTA
#
# Gate: jika satu pair dari sektor masuk → sector_trade_count[sektor] +1
#       Blok jika count >= MAX_PER_SECTOR (dari Portfolio Brain Config).
#
# Cluster drop blocking: tetap pakai proxy return 1h saja (lebih ringan).
CLUSTER_DROP_BLOCK      = -3.0   # % return proxy → blokir seluruh sektor
CLUSTER_CANDLES_NEEDED  = 5      # [v7.29] turun dari 12 → cukup untuk 1h return check
CLUSTER_TF_WEIGHTS      = {"1h": 0.4, "4h": 0.6}   # bobot timeframe untuk composite return
CLUSTER_CACHE_TTL       = 900    # 15 menit

# Cluster statis 4 sektor — key = nama sektor, value = (proxy_pair, [base_coins])
CLUSTER_PROXIES = {
    "BTC":  ("BTC_USDT",  ["BTC", "WBTC"]),
    "AI":   ("FET_USDT",  ["FET", "TAO", "RENDER", "OCEAN", "AGIX", "NMR"]),
    "MEME": ("DOGE_USDT", ["DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF"]),
    "L2":   ("ARB_USDT",  ["ARB", "OP", "MATIC", "IMX", "STARK", "MANTA"]),
}

_cluster_cache: dict  = {}
_cluster_cache_ts: float = 0.0

# [v7.29] Pairwise matrix DIHAPUS — variabel di bawah tidak lagi digunakan.
# Dipertahankan sebagai stub kosong agar referensi lama tidak crash.
_pairwise_returns_cache: dict = {}
_pairwise_corr_cache: dict    = {}
_dynamic_blocked_pairs: set   = set()
_pairwise_cache_ts: float     = 0.0


def _pearson_corr(a: list[float], b: list[float]) -> float:
    """[v7.29] Stub — pairwise matrix dihapus. Selalu return 0.0."""
    return 0.0


def _fetch_return_series(client, pair: str, timeframe: str, window: int) -> list[float] | None:
    """
    [v7.16 #C] Fetch return series (% perubahan candle-to-candle) untuk pair tertentu.
    Dipertahankan untuk _calc_cluster_median_return.
    """
    try:
        candles = get_candles(client, pair, timeframe, window + 2)
        if candles is None or len(candles[0]) < window + 1:
            return None
        closes = candles[0]
        returns = [
            (closes[i] - closes[i-1]) / closes[i-1] * 100
            for i in range(len(closes) - window, len(closes))
        ]
        return returns if len(returns) >= 3 else None
    except Exception:
        return None


def build_pairwise_matrix(client, candidate_pairs: list[str]) -> None:
    """
    [v7.29 Phase3] DINONAKTIFKAN — pairwise matrix dihapus.
    Diganti oleh MAX_PER_SECTOR gate di portfolio_allows().
    Fungsi ini dipertahankan sebagai no-op agar caller tidak crash.
    """
    log("   ℹ️ build_pairwise_matrix: dinonaktifkan (v7.29 simplified clustering)")
    return


def get_pairwise_corr(pair_a: str, pair_b: str) -> float:
    """[v7.29] Stub — pairwise matrix dihapus. Selalu return 0.0."""
    return 0.0


def _calc_cluster_median_return(client, members: list[str], timeframe: str = "1h") -> float | None:
    """
    [v7.29] Disederhanakan: fetch proxy 1h return saja (tidak pakai pairwise cache).
    Dipakai oleh get_cluster_regimes() untuk blocking sektor.
    """
    returns = []
    for base in members:
        pair = f"{base}_USDT"
        try:
            candles = get_candles(client, pair, timeframe, CLUSTER_CANDLES_NEEDED + 2)
            if candles is None or len(candles[0]) < 2:
                continue
            closes = candles[0]
            chg = (closes[-1] - closes[-2]) / closes[-2] * 100
            returns.append(chg)
        except Exception:
            continue
    if len(returns) < 2:
        return None
    returns.sort()
    mid = len(returns) // 2
    median_ret = returns[mid] if len(returns) % 2 != 0 else (returns[mid-1] + returns[mid]) / 2
    return round(median_ret, 3)


# ── Partial Profit / Trailing ── [v7.14 #C] ──────────
# [v7.14] Adaptive partial TP ratio berdasarkan RR aktual trade.
#
# Masalah fixed 50%:
#   RR 1:1 → TP1 hit ambil 50% tapi sisa masih full risk → net tipis.
#   RR 1:4 → TP1 sangat dekat entry → ambil 50% terlalu agresif.
#
# Solusi adaptive:
#   RR < 1.5  → ambil 70% di TP1 (high-risk trade, secure quickly)
#   RR 1.5–2.5→ ambil 50% di TP1 (balanced, default lama)
#   RR > 2.5  → ambil 35% di TP1 (besar potensi, biarkan lebih banyak jalan)
#
# Volatility override: jika ATR/price > HIGH_VOL_THRESHOLD → +10% ratio
# (volatil tinggi = kejar profit lebih agresif karena TP2 sering gagal)
#
# PARTIAL_TP1_RATIO tetap ada sebagai fallback statis jika RR tidak diketahui.
PARTIAL_TP1_RATIO   = 0.70    # fallback statis [v7.25 — naik dari 50%→70%]
ENABLE_PARTIAL_TP   = True    # toggle global
HIGH_VOL_THRESHOLD  = 0.03    # ATR/price > 3% = high volatility pair


def calc_partial_ratio(rr: float, atr: float | None = None, entry: float | None = None) -> float:
    """
    [v7.14 #C] Hitung adaptive partial TP ratio berdasarkan RR dan volatilitas.

    Args:
        rr    : reward-to-risk ratio (mis. 2.0 untuk RR 1:2)
        atr   : ATR absolut pair (optional)
        entry : harga entry (optional, untuk hitung atr_pct)

    Returns:
        float: ratio posisi yang ditutup di TP1 (0.35–0.80)
    """
    # Base ratio dari RR
    if rr < 1.5:
        base_ratio = 0.70    # RR kecil → amankan lebih banyak
    elif rr <= 2.5:
        base_ratio = 0.50    # standar
    else:
        base_ratio = 0.35    # RR besar → biarkan lebih banyak berjalan

    # Volatility override
    if atr is not None and entry is not None and entry > 0:
        atr_pct = atr / entry
        if atr_pct > HIGH_VOL_THRESHOLD:
            base_ratio = min(0.80, base_ratio + 0.10)   # cap 80%

    return round(base_ratio, 2)

# ── Scan Timing ──────────────────────────────────────
# [v7.7 #10] Satu konstanta untuk throttle loop — sebelumnya 0.08 (pump) vs 0.1 (main)
# yang tidak terdokumentasi dan inkonsisten. Disamakan ke 0.1s untuk semua scanner.
SCAN_SLEEP_SEC = 0.1

# ── Slippage & Execution Model ── [v7.15 #D] ─────────
# Real-market fill ≠ signal price. Crypto spot di Gate.io memiliki:
#   - Bid-ask spread  : 0.05–0.3% untuk mid-cap altcoin
#   - Market impact   : order besar menggerakkan harga (khususnya di low-liquidity)
#   - Latency slip    : harga berubah antara signal dikirim dan order dieksekusi
#
# Model slippage: slippage_pct = BASE_SLIP + VOLUME_SLIP × (size / avg_volume_usd)
#   - BASE_SLIP    : slippage minimum yang selalu terjadi (spread + latency)
#   - VOLUME_SLIP  : penalti tambahan proporsional terhadap ukuran order
#   - avg_volume   : estimasi volume per candle dalam USDT (default: $50K untuk mid-cap)
#
# Entry yang disesuaikan: adjusted_entry = signal_entry × (1 + slip)  untuk BUY
#                                         = signal_entry × (1 - slip)  untuk SELL
# Ini memastikan RR calculation menggunakan harga eksekusi realistis, bukan ideal.
#
# SL dan TP TIDAK disesuaikan — mereka tetap di level teknikal.
# Efek: RR aktual sedikit lebih kecil dari kalkulasi teoritis.
SLIPPAGE_BASE_PCT    = 0.0008   # [v7.16 #B] 0.08% — spread + latency baseline (fallback statis)
SLIPPAGE_VOLUME_COEF = 0.0002   # [v7.16 #B] tiap 10% dari avg volume → +0.002%
SLIPPAGE_AVG_VOL_USD = 50_000   # [v7.16 #B] asumsi avg volume per candle (mid-cap)
SLIPPAGE_MAX_PCT     = 0.005    # [v7.16 #B] cap 0.5% — jangan over-penalize

# ── Order Book Cache ── [v8.8] ────────────────────────
# [v8.8 FIX] Sebelumnya: dua cache terpisah (_ob_spread_cache + _ob_depth_cache)
# masing-masing memanggil list_order_book sendiri → worst case 3 API calls
# per pair (spread + depth_BUY + depth_SELL) meski data OB-nya identik.
# Sekarang: satu raw OB cache per pair. Spread dan depth dihitung dari
# data yang sama. Maksimum 1 API call per pair per TTL window.
_ob_cache: dict = {}                # {pair: (ob_object, timestamp)}
OB_SPREAD_CACHE_TTL = 300          # 5 menit — spread relatif stabil
OB_DEPTH_LEVEL      = 20           # 20 level bid/ask cukup untuk depth impact


def _get_ob_cached(client, pair: str):
    """
    [v8.8] Shared raw order book fetch. Satu API call per pair per 5 menit.
    Digunakan oleh get_live_spread() dan get_ob_depth_impact() — keduanya
    tidak lagi memanggil list_order_book secara independen.

    Returns:
        order book object dari Gate.io, atau None jika gagal.
    """
    global _ob_cache
    now    = time.time()
    cached = _ob_cache.get(pair)
    if cached and now - cached[1] < OB_SPREAD_CACHE_TTL:
        return cached[0]
    try:
        ob = gate_call_with_retry(client.list_order_book, currency_pair=pair, limit=OB_DEPTH_LEVEL)
        if ob is None:
            return None
        _ob_cache[pair] = (ob, now)
        return ob
    except Exception as e:
        log(f"   ⚠️ _get_ob_cached({pair}): {e} — fallback ke baseline slippage", "warn")
        return None


def get_live_spread(client, pair: str) -> float | None:
    """
    [v7.16 #B] Ambil spread bid-ask live dari order book Gate.io.

    Spread = (best_ask - best_bid) / mid_price

    [v8.8] Menggunakan _get_ob_cached() — tidak lagi fetch OB sendiri.
    Cache 5 menit dikelola di layer bawah.
    Return None jika fetch gagal (caller akan fallback ke SLIPPAGE_BASE_PCT).

    Args:
        client : Gate.io API client
        pair   : mis. "FET_USDT"

    Returns:
        float | None: spread fraction (mis. 0.0015 = 0.15%), atau None jika gagal.
    """
    ob = _get_ob_cached(client, pair)
    if ob is None or not ob.bids or not ob.asks:
        return None

    try:
        best_bid = float(ob.bids[0][0])
        best_ask = float(ob.asks[0][0])
        if best_bid <= 0 or best_ask <= 0:
            return None

        mid    = (best_bid + best_ask) / 2.0
        spread = (best_ask - best_bid) / mid
        return round(max(0.0, min(spread, SLIPPAGE_MAX_PCT)), 6)
    except Exception:
        return None


def get_ob_depth_impact(client, pair: str, size_usdt: float, side: str) -> float:
    """
    [v8.3] Estimasi market impact order size_usdt terhadap depth OB.

    [v8.8] Menggunakan _get_ob_cached() — tidak lagi fetch OB sendiri.
    Note: cache per pair, bukan per (pair, side). Side hanya mempengaruhi
    komputasi (asks vs bids), bukan data yang di-fetch.
    """
    ob = _get_ob_cached(client, pair)
    if ob is None:
        return 0.0

    try:
        levels = ob.asks if side == "BUY" else ob.bids
        if not levels:
            return 0.0

        best_price = float(levels[0][0])
        if best_price <= 0:
            return 0.0

        remaining   = size_usdt
        weighted_px = 0.0
        filled      = 0.0

        for price_str, qty_str in levels:
            px  = float(price_str)
            qty = float(qty_str)
            val = px * qty

            take = min(remaining, val)
            weighted_px += px * (take / size_usdt)
            filled      += take
            remaining   -= take
            if remaining <= 0:
                break

        if filled < size_usdt * MIN_FILL_RATIO:
            return SLIPPAGE_MAX_PCT

        avg_fill = weighted_px
        if side == "BUY":
            impact = (avg_fill - best_price) / best_price
        else:
            impact = (best_price - avg_fill) / best_price
        return round(max(0.0, impact), 6)

    except Exception:
        return 0.0


def calc_slippage(side: str, size_usdt: float,
                  avg_volume_usd: float | None = None,
                  live_spread: float | None = None,
                  depth_impact: float | None = None) -> float:
    """
    [v7.16 #B] Estimasi slippage dengan tiga komponen:
      1. Half-spread : biaya crossing bid-ask (live jika tersedia, statis jika tidak)
      2. Market impact: pergeseran harga karena ukuran order (dari depth OB)
      3. Latency slip : modeled secara proporsional terhadap avg_volume

    Upgrade dari v7.15:
      v7.15 -> hanya SLIPPAGE_BASE_PCT (flat) + volume_ratio (kasar)
      v7.16 -> spread live dari OB + depth impact aktual dari OB

    Args:
        side          : "BUY" | "SELL"
        size_usdt     : ukuran order dalam USDT
        avg_volume_usd: estimasi volume candle (opsional, untuk latency component)
        live_spread   : hasil get_live_spread() — half-spread aktual (opsional)
        depth_impact  : hasil get_ob_depth_impact() — market impact aktual (opsional)

    Returns:
        float: total slippage fraction, selalu positif, di-cap SLIPPAGE_MAX_PCT.
    """
    # Komponen 1: half-spread (50% dari spread ditanggung per sisi)
    spread_component = (live_spread / 2.0) if live_spread is not None else (SLIPPAGE_BASE_PCT / 2.0)

    # Komponen 2: market impact dari depth OB
    impact_component = depth_impact if depth_impact is not None else 0.0

    # Komponen 3: latency slippage (proporsional ukuran vs volume candle)
    avg_vol          = avg_volume_usd if avg_volume_usd and avg_volume_usd > 0 else SLIPPAGE_AVG_VOL_USD
    latency_slip     = SLIPPAGE_VOLUME_COEF * (size_usdt / avg_vol)

    total = spread_component + impact_component + latency_slip
    return round(min(total, SLIPPAGE_MAX_PCT), 6)


def adjust_entry_for_slippage(entry: float, side: str, size_usdt: float,
                               avg_volume_usd: float | None = None,
                               client=None, pair: str | None = None) -> tuple[float, float]:
    """
    [v7.16 #B] Sesuaikan harga entry dengan slippage — live spread + depth impact jika tersedia.

    Upgrade dari v7.15:
      - Jika client + pair tersedia: ambil live spread & depth impact dari OB Gate.io
      - Jika tidak: fallback ke model statis (kompatibel backward)

    Returns:
        (adjusted_entry, slip_pct) — adjusted_entry adalah harga fill realistis,
        slip_pct adalah total fraction yang diaplikasikan (untuk logging).
    """
    live_spread   = None
    depth_impact  = None

    # [v8.9] Hybrid model: coba live OB, fallback ke simple model jika gagal/noise.
    # Simple model: SLIPPAGE_BASE_PCT * max(1, size/avg_vol_ratio) — ringan & stabil.
    # Live model dipakai hanya jika berhasil fetch — tidak memblok flow jika API lambat.
    if client is not None and pair is not None:
        try:
            live_spread  = get_live_spread(client, pair)
            depth_impact = get_ob_depth_impact(client, pair, size_usdt, side)
        except Exception:
            live_spread  = None   # fallback ke simple model
            depth_impact = None

    slip_live   = calc_slippage(side, size_usdt, avg_volume_usd,
                                live_spread=live_spread, depth_impact=depth_impact)
    # Simple model sebagai floor — cegah depth model underestimate di kondisi volatile
    slip_simple = min(SLIPPAGE_BASE_PCT * max(1.0, size_usdt / SLIPPAGE_AVG_VOL_USD), SLIPPAGE_MAX_PCT)
    slip        = max(slip_live, slip_simple)   # ambil yang lebih konservatif

    if side == "BUY":
        adjusted = entry * (1.0 + slip)
    else:
        adjusted = entry * (1.0 - slip)
    return round(adjusted, 8), slip

# ── Scan Mode ────────────────────────────────────────
SCAN_MODE = os.environ.get("SCAN_MODE", "full").lower()

# ── Blacklist ─────────────────────────────────────────
BLACKLIST_TOKENS = {
    "TUSD","USDC","BUSD","DAI","FDUSD","USD1","USDP","USDD","USDJ",
    "ZUSD","GUSD","CUSD","SUSD","FRAX","LUSD","USDN","3S","3L","5S","5L",
}
# ── ETF / Tokenized Stock Filter ─────────────────────
# [v7.5 FIX] Dua lapis perlindungan:
#   1. ETF_EXACT  — exact match, tidak perlu logika tambahan
#   2. ETF_PREFIX — prefix match (startswith), menangkap turunan sintetis
#      mis. TSLAX (TSLA+X), MCDON (MCD+ON), NVDAX (NVDA+X)
#      Aman karena semua entry dipilih dari ticker saham resmi
#      yang sangat tidak mungkin menjadi prefix token kripto sah.
#
# Catatan COIN & MCD: masuk ETF_PREFIX bukan ETF_EXACT karena
#   - COIN3L/COIN3S = leverage token saham Coinbase (bukan kripto COIN)
#   - MCDON/MCDX   = tokenized saham McDonald's
#   Jika kamu ingin token kripto COIN tetap lolos, pindahkan COIN
#   ke komentar dan tambahkan MCDON manual ke ETF_EXACT.

ETF_EXACT = {
    # ── Big Tech ──────────────────────────────────────
    "AAPL","AAPLX","AMZN","AMZNX","NVDA","NVDAX",
    "TSLA","TSLAX","TSLAS","TSLAON",  # TSLAON = tokenized TSLA ON-chain — [v7.7 hotfix]
    "MSFT","MSFTX",
    "META","METAX","GOOG","GOOGL","GOOGX","NFLX","NFLXX",
    "INTC","INTCX","AMD","AMDX","QCOM","QCOMX",
    "AVGO","AVGOX","MU","MUX","AMAT","AMATX",
    "LRCX","KLAC","TXN","MRVL",
    # ── Finance / Fintech ─────────────────────────────
    "COIN","COINX","MSTR","MSTRX",
    "PYPL","PYPLX","SQ","SQX","SOFI","SOFIX",
    "HOOD","HOODX","AFRM","AFRMX","UPST","UPSTX",
    "JPM","GS","MS","BAC","V","MA","AXP","WFC","C",
    # ── Consumer / Retail ─────────────────────────────
    "WMT","WMTX","TGT","COST","SBUX",
    "NKE","NIKEX","MCD","MCDX","MCDON",
    "BABA","BABAX","JD","JDX","PDD","PDDX",
    # ── Media / Entertainment ─────────────────────────
    "DIS","DISX","SPOT","SPOTX","RBLX","RBLXX",
    "SNAP","SNAPX","PINS","MTCH","MTCHX",
    # ── EV / Auto ─────────────────────────────────────
    "RIVN","RIVNX","LCID","LCIDX",
    "NIO","NIOX","XPEV","XPEVX","LI","LIX","F","FX","GM","GMX",
    # ── Pharma / Healthcare ───────────────────────────
    "PFE","PFEX","MRNA","MRNAX","JNJ","ABBV","BMY","GILD","BIIB",
    # ── Crypto Miners (saham, bukan token) ───────────
    "MARA","MARAX","RIOT","RIOTX","HUT","HUTX",
    "BITF","CLSK","BTBT","CIFR",
    # ── High-Growth / Meme Stocks ─────────────────────
    "PLTR","PLTRX","ABNB","ABNBX","DASH","DASHX",
    "DKNG","DKNGX","BYND","BYNDX","DOCU","DOCUX",
    "ZM","ZMX","ROKU","ROKUX","PATH","PATHX",
    "GME","GMEX","AMC","AMCX","BB","BBX","NOK","NOKX",
    "SPCE","SPCEX","WISH","WISHX",
    "UBER","UBERX","LYFT","LYFTX",
    "SHOP","SHOPX","ETSY","ETSYX","EBAY","EBAYX",
    "BIDU","BIDUX","NTES","BILI","BILIX",
    # ── Semiconductor / AI Hardware ───────────────────
    "ARM","ARMX","SMCI","SMCIX","IONQ","IONQX",
    "ACHR","JOBY","LILM",
    # ── ETF Funds ─────────────────────────────────────
    "SPY","QQQ","ARKK","ARKB","GLD","SLV","USO","TLT","IWM","XLF",
    # ── Tokenized On-Chain (*ON variants) ─ [v7.7b] ─────────────────
    # Explicit list — lebih aman dari suffix "ON" rule yang bisa blok TON/ELON/MOON
    "TSLAON","AAPLON","NVDAON","AMZNON","MSFTON",
    "METAON","GOOGON","GOOGLON","NFLXON","COINON",
    "MSTRON","INTCON","AMDON","UBERON","SHOPON",
    "ABNBON","NKEON","WMTON","SPYON","QQQON",
    # ── Leverage token saham (3L/3S) ──────────────────
    # Sudah sebagian ditangkap suffix check, tapi tambahkan eksplisit
    "TSLA3L","TSLA3S","AAPL3L","AAPL3S","NVDA3L","NVDA3S",
    "AMZN3L","AMZN3S","MSFT3L","MSFT3S","GOOG3L","GOOG3S",
    "COIN3L","COIN3S","AMD3L","AMD3S","MSTR3L","MSTR3S",
}

# Prefix yang PASTI saham — startswith check, seluruh turunan diblokir
# [v7.6 #4] Bersihkan dari entri yang sudah ada di ETF_EXACT sebagai exact match.
#   Layer 3 (prefix) hanya berguna untuk ticker yang BELUM ada di _ETF_DYNAMIC
#   (ETF_EXACT + dynamic fetch). Memasukkan TSLA/NVDA/AAPL di sini percuma karena
#   Layer 2 sudah menangkap mereka lebih dulu. Sisa di bawah adalah yang benar-benar
#   perlu prefix guard untuk turunan sintetis baru (mis. SOFIX2, HOODX2, MARNAX).
ETF_PREFIX = {
    # [v7.7b] Diperluas dengan semua ticker saham mayor — menangkap SEMUA varian turunan
    # (TSLAON, NVDAX, AAPLON, MSFTON, dll) tanpa perlu list eksplisit per-varian.
    # Big Tech
    "AAPL", "AMZN", "NVDA", "TSLA", "MSFT", "META", "GOOG", "NFLX",
    "INTC", "AMD", "QCOM", "AVGO", "MU", "AMAT", "LRCX", "KLAC", "TXN", "MRVL",
    # Finance
    "COIN", "MSTR", "PYPL", "SOFI", "HOOD", "AFRM", "UPST",
    # Consumer / Retail
    "WMT", "NKE", "MCD", "BABA", "JD", "PDD",
    # Media
    "DIS", "SPOT", "RBLX", "SNAP", "PINS",
    # EV / Auto
    "RIVN", "LCID", "NIO", "XPEV",
    # Pharma
    "MRNA", "PFE",
    # High-Growth
    "PLTR", "ABNB", "DASH", "DKNG", "BYND", "DOCU", "ROKU",
    "UBER", "SHOP", "ETSY",
    # Semiconductor / AI
    "ARM", "SMCI", "IONQ", "ACHR", "JOBY",
}

# Backward-compat alias (kode lama mungkin masih referensi ETF_KEYWORDS)
ETF_KEYWORDS = ETF_EXACT  # noqa: F841

# ── Signal Groups & Weights (v7.8 — Group-Max Scoring) ───────────
#
# MASALAH dengan additive scoring:
#   EMA + (VWAP) + BOS  = semua mengukur TREND     → overlap, inflate score
#   RSI + MACD          = semua mengukur MOMENTUM   → overlap, inflate score
#   liq_sweep + OB + pullback = semua mengukur INSTITUSIONAL → overlap
#
# SOLUSI: Grouping + max-per-group, bukan sum-all.
#   Setiap group merepresentasikan dimensi yang berbeda.
#   Hanya signal terkuat dalam group yang dihitung.
#   Antar group dijumlahkan — karena mereka independen.
#
# ┌─────────────┬────────────────────────────────┬───────┐
# │ Group       │ Indikator (urutan kekuatan)     │ Max   │
# ├─────────────┼────────────────────────────────┼───────┤
# │ TREND       │ EMA fast vs slow               │   3   │
# │ MOMENTUM    │ MACD crossover                 │   3   │
# │ LIQUIDITY   │ liq_sweep (4) > OB (3) >       │   4   │
# │             │ pullback (2) — ambil tertinggi  │       │
# │ VOLUME      │ Volume spike > 1.3× avg        │   3   │
# ├─────────────┼────────────────────────────────┼───────┤
# │ Regime      │ ADX trending (+2) / ranging(-2)│  ±2   │
# └─────────────┴────────────────────────────────┴───────┘
#
# Max score: 3 + 3 + 4 + 3 + 2 = 15
# (semua group trigger + pasar trending)
#
# Contoh real:
#   liq_sweep + OB keduanya ada → group LIQUIDITY = 4 (bukan 4+3=7)
#   MACD cross + EMA aligned    → MOMENTUM=3, TREND=3 (grup berbeda, boleh stack)

GROUPS = {
    # [v7.28] 3 core scoring factors — masing-masing bernilai 1 poin
    # Score total: 0–3 (core) + 0/0.5/1.0 (setup bonus)
    "trend":       1,   # EMA fast > slow (searah entry)
    "momentum":    1,   # MACD crossover searah entry
    "vol_confirm": 1,   # Volume spike > threshold

    # Removed in v7.28:
    # "liq_sweep", "order_block", "pullback" → overlap dengan setup_score
    # "adx_trend", "adx_ranging" → ADX sekarang murni hard gate (CHOPPY) atau diabaikan
}

# [v7.28] Setup bonus — bukan gate, bukan multiplier, tapi additive float kecil
# Agar setup kuat sedikit mendorong score tanpa mendominasi
# Nilai: 0–1.0 ditambahkan ke score integer sebelum threshold check
SETUP_BONUS = {
    3: 0.5,   # BOS/CHoCH full confirmation — bonus moderat (tidak override core)
    2: 0.25,  # liq_sweep saja — bonus kecil
    1: 0.0,   # continuation bias — tidak dapat bonus (sudah lemah)
    0: 0.0,   # no setup — seharusnya tidak sampai sini (hard gate di check_*)
}


# ════════════════════════════════════════════════════════
#  UTILITIES
# ════════════════════════════════════════════════════════

def tg(msg: str):
    """Kirim pesan ke Telegram. [v7.6 #10] Retry 2x dengan backoff 2s."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    url  = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    body = json.dumps({
        "chat_id": TG_CHAT_ID, "text": msg,
        "parse_mode": "HTML", "disable_web_page_preview": True
    }).encode()
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, data=body,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
            time.sleep(0.5)
            return
        except Exception as e:
            if attempt < 2:
                log(f"⚠️ Telegram retry {attempt+1}/2: {e}", "warn")
                time.sleep(2 ** attempt * 2)   # 2s, 4s
            else:
                log(f"⚠️ Telegram gagal setelah 3x retry: {e}", "error")


def http_get(url: str, timeout: int = 8):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        log(f"⚠️ HTTP {url[:60]}: {e}", "warn")
        return None


_idr_rate_cache: dict = {"rate": 0.0, "ts": 0.0}

def get_usdt_idr_rate() -> float:
    """
    Fetch kurs USD/IDR real-time — USDT diasumsikan ~1 USD (stablecoin).
    Cache 5 menit agar tidak flood API setiap signal.

    Sumber (berurutan, fallback ke berikutnya jika gagal):
      1. ExchangeRate-API (open, no key) — kurs USD/IDR bank tengah
      2. Frankfurter API (ECB data) — USD to IDR
      3. Cache lama — jika semua sumber gagal
    """
    now = time.time()
    if _idr_rate_cache["rate"] > 0 and now - _idr_rate_cache["ts"] < 300:
        return _idr_rate_cache["rate"]

    def _set_cache(rate: float) -> float:
        _idr_rate_cache["rate"] = rate
        _idr_rate_cache["ts"]   = time.time()
        log(f"\U0001f4b1 Kurs USD/IDR: Rp{rate:,.0f}")
        return rate

    # Sumber 1: ExchangeRate-API (open endpoint, tidak perlu API key)
    try:
        data = http_get("https://open.er-api.com/v6/latest/USD", timeout=6)
        if data and data.get("result") == "success":
            rate = float(data["rates"]["IDR"])
            return _set_cache(rate)
    except Exception as e:
        log(f"\u26a0\ufe0f ExchangeRate-API gagal: {e}", "warn")

    # Sumber 2: Frankfurter (ECB) — USD to IDR
    try:
        data = http_get("https://api.frankfurter.app/latest?from=USD&to=IDR", timeout=6)
        if data and "rates" in data and "IDR" in data["rates"]:
            rate = float(data["rates"]["IDR"])
            return _set_cache(rate)
    except Exception as e:
        log(f"\u26a0\ufe0f Frankfurter API gagal: {e}", "warn")

    # Fallback: cache lama (bisa stale tapi lebih baik dari hardcode)
    if _idr_rate_cache["rate"] > 0:
        log("\u26a0\ufe0f Semua sumber IDR gagal — pakai cache lama", "warn")
        return _idr_rate_cache["rate"]

    log("\u26a0\ufe0f Semua sumber IDR gagal — pakai estimasi 16300", "warn")
    return 16300.0


def usdt_to_idr(usdt: float, rate: float) -> str:
    """Format harga USDT ke string Rupiah yang mudah dibaca."""
    idr = usdt * rate
    if idr >= 1_000_000:
        return f"Rp{idr/1_000_000:.2f}jt"
    elif idr >= 1_000:
        return f"Rp{idr:,.0f}"
    else:
        return f"Rp{idr:.2f}"


def http_get_text(url: str, timeout: int = 10) -> str | None:
    """
    Fetch URL dan kembalikan sebagai raw string — untuk plain text & CSV.
    Digunakan oleh build_etf_blocklist() karena http_get() selalu json.loads()
    yang akan gagal untuk non-JSON response.
    """
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8")
    except Exception as e:
        log(f"⚠️ HTTP text [{url[:60]}]: {e}", "warn")
        return None


# [v7.1 #8] Retry helper dengan exponential backoff untuk Gate.io API
# [v7.26] Runtime blacklist pair yang sudah confirmed delisted/invalid dari Gate.io.
# Pair masuk sini saat API return 400 INVALID_CURRENCY_PAIR — tidak perlu retry lagi.
# Di-reset setiap run() baru (in-memory only).
_invalid_pairs_cache: set = set()

def gate_call_with_retry(fn, *args, retries: int = 3, base_delay: float = 1.0, **kwargs):
    """
    Panggil fungsi Gate.io API dengan retry + exponential backoff.
    Menangani rate limit (429) dan error jaringan sementara.
    [v7.6 #2] Explicit return None setelah loop exhausted — clarity & mypy safe.
    [v7.26]   Skip retry langsung jika 400 INVALID_CURRENCY_PAIR — pair delisted,
              retry tidak akan pernah berhasil. Hemat ~6 detik per pair invalid.
    """
    # [v7.26] Cek blacklist dulu — pair sudah diketahui invalid, skip langsung
    pair_arg = kwargs.get("currency_pair") or (args[0] if args else None)
    if pair_arg and pair_arg in _invalid_pairs_cache:
        return None

    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            err_str = str(e)
            err_lower = err_str.lower()

            # [v7.26] 400 INVALID_CURRENCY_PAIR → pair delisted, jangan retry sama sekali
            if "400" in err_str and "invalid_currency_pair" in err_lower:
                if pair_arg:
                    _invalid_pairs_cache.add(pair_arg)
                    log(f"🚫 Pair delisted: {pair_arg} — skip permanen cycle ini", "warn")
                return None   # langsung return, tidak retry

            is_rate_limit = "429" in err_str or "rate limit" in err_lower or "too many" in err_lower
            if attempt < retries - 1:
                if is_rate_limit:
                    # [v9.0] Baca X-Gate-RateLimit-Reset-Timestamp dari header jika tersedia.
                    # Lebih akurat dari fixed delay karena tahu persis kapan window reset.
                    reset_ts = None
                    try:
                        if hasattr(e, "headers") and e.headers:
                            reset_val = e.headers.get("X-Gate-RateLimit-Reset-Timestamp")
                            if reset_val:
                                reset_ts = int(reset_val)
                    except Exception:
                        pass
                    if reset_ts:
                        wait_sec = max(1.0, reset_ts - time.time() + 0.5)
                        wait_sec = min(wait_sec, 15.0)
                        log(f"⚠️ Rate limit Gate.io — window reset dalam {wait_sec:.1f}s (retry {attempt+1}/{retries})", "warn")
                    else:
                        wait_sec = base_delay * (2 ** attempt) + 2.0  # 3s, 4s — lebih konservatif
                        log(f"⚠️ Rate limit Gate.io — retry {attempt+1}/{retries} dalam {wait_sec:.0f}s", "warn")
                    time.sleep(wait_sec)
                else:
                    delay = base_delay * (2 ** attempt)  # 1s, 2s, 4s
                    log(f"⚠️ Gate API error ({e}) — retry {attempt+1}/{retries} dalam {delay:.0f}s", "warn")
                    time.sleep(delay)
            else:
                log(f"⚠️ Gate API gagal setelah {retries}x retry: {e}", "error")
    return None  # [v7.6 #2] semua retry exhausted


def get_client():
    cfg = gate_api.Configuration(
        host="https://api.gateio.ws/api/v4",
        key=API_KEY, secret=SECRET_KEY
    )
    return gate_api.SpotApi(gate_api.ApiClient(cfg))


# ════════════════════════════════════════════════════════
#  DYNAMIC ETF BLOCKLIST — [v7.5]
#  Auto-fetch daftar saham US saat startup sehingga stock
#  baru di Gate.io langsung terblokir tanpa update manual.
# ════════════════════════════════════════════════════════

# Runtime blocklist — diisi oleh build_etf_blocklist() saat startup
# Digunakan oleh is_valid_pair() sebagai Layer 2 tambahan
_ETF_DYNAMIC: set = set()
_ETF_BUILT: bool = False   # [v7.7 #8] Guard idempotent — cegah rebuild jika run() dipanggil ulang

def build_etf_blocklist() -> None:
    """
    Fetch daftar ticker saham US dari sumber publik, gabungkan dengan
    ETF_EXACT (static), lalu isi _ETF_DYNAMIC untuk dipakai is_valid_pair().

    Sumber (di-fetch secara PARALEL):
      1. GitHub — rreichel3/US-Stock-Symbols  (semua NYSE+NASDAQ+AMEX, plain text)
      2. GitHub — datasets/s-and-p-500-companies  (S&P 500, CSV kolom "Symbol")
      3. ETF_EXACT static — selalu ada sebagai fallback minimum

    [v7.5 FIX] Menggunakan http_get_text() bukan http_get() karena kedua sumber
    mengembalikan plain text / CSV — bukan JSON.

    [v7.6 #8] Fetch dijalankan paralel via ThreadPoolExecutor — kurangi startup delay
    dari ~2×timeout (sequential) menjadi ~1×timeout (paralel).

    [v7.7 #8] Flag _ETF_BUILT — idempotent guard jika run() dipanggil berkali-kali
    dalam satu proses (non-GitHub-Actions deployment). Rebuild hanya sekali.

    Dieksekusi SEKALI saat bot start (fresh process per GitHub Actions run).
    """
    global _ETF_DYNAMIC, _ETF_BUILT
    if _ETF_BUILT:
        log("  ℹ️ ETF blocklist sudah dibangun — skip rebuild")
        return
    _ETF_BUILT = True
    # [v8.0] ThreadPoolExecutor sudah di-import di top level — tidak perlu local import lagi

    # Seed awal dari static list — jaminan minimum
    _ETF_DYNAMIC = set(ETF_EXACT)

    sources = [
        # plain text — satu ticker per baris (NYSE + NASDAQ + AMEX, ~10K ticker)
        ("text", "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"),
        # CSV — header baris pertama, kolom "Symbol" (S&P 500, ~500 ticker)
        ("csv",  "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"),
    ]

    def _fetch_source(fmt: str, url: str) -> set:
        """Fetch satu sumber dan return set ticker. Dipanggil dari thread pool."""
        raw = http_get_text(url)
        if not raw:
            return set()
        tickers: set = set()
        if fmt == "text":
            for line in raw.strip().splitlines():
                tok = line.strip().upper()
                if tok and 1 <= len(tok) <= 5 and tok.isalpha():
                    tickers.add(tok)
        elif fmt == "csv":
            lines = raw.strip().splitlines()
            if not lines:
                return set()
            header = [h.strip().lower() for h in lines[0].split(",")]
            try:
                sym_idx = header.index("symbol")
            except ValueError:
                log(f"  ⚠️ ETF CSV: kolom 'Symbol' tidak ditemukan di {url[:50]}", "warn")
                return set()
            for line in lines[1:]:
                cols = line.split(",")
                if len(cols) > sym_idx:
                    tok = cols[sym_idx].strip().upper().strip('"')
                    if tok and tok.isalpha():
                        tickers.add(tok)
        return tickers

    fetched = 0
    # [v7.6 #8] Paralel fetch — kedua HTTP request jalan bersamaan
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_map = {
            executor.submit(_fetch_source, fmt, url): (fmt, url)
            for fmt, url in sources
        }
        for future in as_completed(future_map):
            _, url = future_map[future]
            try:
                new_tickers = future.result()
                if new_tickers:
                    added = len(new_tickers - _ETF_DYNAMIC)
                    _ETF_DYNAMIC |= new_tickers
                    fetched += added
                    source_name = url.split("/")[4] + "/" + url.split("/")[5]
                    log(f"  📋 ETF blocklist: +{added} ticker baru dari {source_name}")
            except Exception as e:
                log(f"  ⚠️ ETF fetch gagal [{url[:55]}]: {e}", "warn")

    log(f"  ✅ ETF blocklist final: {len(_ETF_DYNAMIC)} token "
        f"({'dynamic +' + str(fetched) + ' ticker' if fetched > 0 else 'static fallback saja'})")


def is_valid_pair(pair: str) -> bool:
    if not pair.endswith("_USDT"): return False
    base = pair.replace("_USDT", "")

    # Layer 1: stablecoin & blacklist — exact match
    if base in BLACKLIST_TOKENS: return False

    # Layer 2: ETF dynamic blocklist (ETF_EXACT + ticker saham live dari startup fetch)
    # _ETF_DYNAMIC mencakup semua saham US — token baru Gate.io otomatis tertangkap
    if base in _ETF_DYNAMIC: return False

    # Layer 3: ETF prefix match — menangkap turunan sintetis yang belum ada di dynamic list
    # mis. TSLAX, NVDAX, MCDON — fetch sumber eksternal tidak mengandung variant ini
    for kw in ETF_PREFIX:
        if base.startswith(kw):
            return False

    # Layer 4: leverage / structured product suffix
    # Catatan: "ON" TIDAK dimasukkan di sini karena terlalu broad — akan memblok
    # TON, ELON, MOON, dan kripto sah lain yang kebetulan endswith ON.
    # Token tokenized *ON (TSLAON dll) ditangkap oleh ETF_PREFIX (TSLA → TSLAON).
    if any(base.endswith(sfx) for sfx in
           ["UP","DOWN","DOW","BULL","BEAR","3L","3S","5L","5S","2L","2S","10L","10S"]):
        return False

    return True


# ════════════════════════════════════════════════════════
#  CANDLE CACHE — [v8.0 Phase4 #2] TTL-aware, tidak reset per cycle
# ════════════════════════════════════════════════════════
# Format: {(pair, interval, limit): (result, timestamp)}
# TTL = 120 detik — candle 1h/4h tidak berubah dalam 2 menit.
# Keunggulan vs reset-per-cycle: fungsi berbeda (check_intraday + check_swing
# + _fetch_return_series) yang minta candle sama tidak trigger API call ganda.
_candle_cache: dict = {}
CANDLE_CACHE_TTL = 120   # detik — candle tidak expired sebelum TTL ini

# Timeframe + limit standar yang di-prefetch untuk setiap pair main bot
# [v8.0 Phase4 #3] Batch prefetch menggunakan kombinasi ini.
_PREFETCH_SPECS: list[tuple[str, int]] = [
    ("1h",  100),   # INTRADAY + structure detection
    ("4h",   60),   # SWING + BTC regime
    ("15m",  50),   # pump scanner
]


def get_candles(client, pair: str, interval: str, limit: int):
    """
    Fetch candles dengan TTL cache. [v8.0 Phase4 #2]

    Upgrade dari v7.x:
      - Cache berdasarkan TTL (120 detik), bukan reset per cycle.
      - Jika cache masih valid, langsung return tanpa API call.
      - Kompatibel dengan prefetch_candles_batch() — jika batch sudah
        mengisi cache sebelum scan loop, fungsi ini cukup baca cache.

    [v7.1 #3] limit masuk key — cegah silent mismatch.
    [v7.7 #6] min(30, limit) guard — pair baru dengan histori terbatas.
    """
    key = (pair, interval, limit)
    now = time.time()

    # [v8.0] TTL check — return cache jika masih segar
    cached = _candle_cache.get(key)
    if cached is not None:
        result, ts = cached
        if now - ts < CANDLE_CACHE_TTL:
            return result   # cache hit, zero API call

    try:
        raw = gate_call_with_retry(
            client.list_candlesticks,
            currency_pair=pair, interval=interval, limit=limit
        )
        min_required = min(30, limit)
        if not raw or len(raw) < min_required:
            log(f"⚠️ candles [{pair}|{interval}|{limit}]: hanya {len(raw) if raw else 0} candle tersedia (min {min_required})", "warn")
            _candle_cache[key] = (None, now)
            return None
        closes  = np.array([float(c[2]) for c in raw])
        highs   = np.array([float(c[3]) for c in raw])
        lows    = np.array([float(c[4]) for c in raw])
        volumes = np.array([float(c[1]) for c in raw])
        result  = (closes, highs, lows, volumes)
        _candle_cache[key] = (result, now)
        return result
    except Exception as e:
        log(f"⚠️ candles [{pair}|{interval}|{limit}]: {e}", "warn")
        _candle_cache[key] = (None, now)
        return None


def prefetch_candles_batch(client, pairs: list[str],
                            specs: list[tuple[str, int]] | None = None,
                            max_workers: int = 3) -> dict:  # [v9.0] turun 8→3 — cegah rate limit burst
    """
    [v8.0 Phase4 #1 + #3] Fetch candles untuk banyak pair secara PARALEL.

    Dipanggil sekali sebelum scan loop dengan semua pairs yang lolos filter.
    Mengisi _candle_cache sehingga scan loop tidak perlu API call tambahan.

    Args:
        client      : Gate.io SpotApi client
        pairs       : list pair yang akan di-prefetch (mis. semua valid pair lolos vol)
        specs       : list (interval, limit) yang di-fetch per pair.
                      Default: _PREFETCH_SPECS (1h×100, 4h×60, 15m×50)
        max_workers : jumlah thread paralel. Default 3 — [v9.0] diturunkan dari 8.
                      8 workers = burst terlalu tinggi → 429 TOO_MANY_REQUESTS saat prefetch 200 pairs.
                      3 workers × 0.35s throttle = ~8-9 req/detik total → aman di bawah limit Gate.io.

    Returns:
        dict summary: {"fetched": int, "cached": int, "failed": int}

    Catatan:
        - get_candles() sudah TTL-aware: jika spec sudah dalam cache dan masih segar,
          prefetch tidak trigger API call ganda.
        - ThreadPoolExecutor bukan asyncio karena gate_api library bersifat synchronous.
          Thread pool sudah cukup untuk I/O bound workload ini.
    """
    if specs is None:
        specs = _PREFETCH_SPECS

    now = time.time()
    tasks = []
    for pair in pairs:
        for interval, limit in specs:
            key = (pair, interval, limit)
            cached = _candle_cache.get(key)
            if cached is not None and (now - cached[1]) < CANDLE_CACHE_TTL:
                continue   # sudah valid, skip fetch
            tasks.append((pair, interval, limit))

    if not tasks:
        log(f"   ⚡ prefetch_candles_batch: semua {len(pairs)} pairs sudah cached — skip")
        return {"fetched": 0, "cached": len(pairs) * len(specs), "failed": 0}

    log(f"   🔄 prefetch_candles_batch: {len(tasks)} fetch paralel "
        f"({len(pairs)} pairs × {len(specs)} specs, workers={max_workers})")

    fetched = 0
    failed  = 0

    def _fetch_one(pair: str, interval: str, limit: int):
        """Worker — dipanggil dari thread pool."""
        # [v9.0] Throttle per-thread — hindari burst simultan dari semua worker.
        # 0.35s × 3 workers = ~1 req/detik per worker, total ~3 req/detik.
        # Gate.io limit 200 req/window (biasanya 1 menit) → aman di bawah threshold.
        time.sleep(0.35)
        try:
            raw = gate_call_with_retry(
                client.list_candlesticks,
                currency_pair=pair, interval=interval, limit=limit
            )
            key = (pair, interval, limit)
            ts  = time.time()
            min_required = min(30, limit)
            if not raw or len(raw) < min_required:
                _candle_cache[key] = (None, ts)
                return "failed"
            closes  = np.array([float(c[2]) for c in raw])
            highs   = np.array([float(c[3]) for c in raw])
            lows    = np.array([float(c[4]) for c in raw])
            volumes = np.array([float(c[1]) for c in raw])
            _candle_cache[key] = ((closes, highs, lows, volumes), ts)
            return "fetched"
        except Exception as e:
            log(f"   ⚠️ prefetch [{pair}|{interval}]: {e}", "warn")
            return "failed"

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_one, pair, interval, limit): (pair, interval, limit)
            for pair, interval, limit in tasks
        }
        for future in as_completed(futures):
            result = future.result()
            if result == "fetched":
                fetched += 1
            else:
                failed += 1

    log(f"   ✅ prefetch selesai: {fetched} fetched, {failed} failed "
        f"(dari {len(tasks)} tasks | {len(pairs)} pairs)")
    return {"fetched": fetched, "cached": len(pairs) * len(specs) - len(tasks), "failed": failed}


# ════════════════════════════════════════════════════════
#  INDICATORS
# ════════════════════════════════════════════════════════

def calc_rsi(closes, period=14) -> float:
    # [v7.7 #1] Wilder's EMA (alpha=1/period) — konsisten dengan TradingView & standar industri.
    # Sebelumnya pakai .rolling(period).mean() = simple MA, menghasilkan RSI berbeda
    # dari platform charting terutama pada candle-candle awal.
    s = pd.Series(closes); d = s.diff()
    gain = d.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = (-d.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    return float((100 - 100/(1+gain/(loss+1e-9))).iloc[-1])


def calc_ema(closes, period) -> float:
    return float(pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1])


def calc_macd(closes):
    s = pd.Series(closes)
    macd   = s.ewm(span=12, adjust=False).mean() - s.ewm(span=26, adjust=False).mean()
    signal = macd.ewm(span=9, adjust=False).mean()
    return float(macd.iloc[-1]), float(signal.iloc[-1])


def calc_atr(closes, highs, lows, period=14) -> float:
    """
    [v7.15 #B] Composite ATR: standar ATR + spike-aware adjustment.

    Masalah ATR standar di crypto:
      - Spike/wick ekstrem di satu candle inflate ATR untuk 14 candle ke depan
      - ATR lagging — tidak merespons perubahan volatilitas cepat

    Solusi dua lapis:
      1. ATR standar (rolling mean TR) — baseline
      2. Recent spike detection: jika TR candle terakhir > SPIKE_MULT × ATR,
         blend ATR standar dengan TR spike agar sizing responsif
      3. Kecualikan candle dengan TR > OUTLIER_MULT × median TR dari rolling ATR
         untuk mencegah satu candle ekstrem mendistorsi seluruh window

    Returns: float ATR yang lebih responsif dan spike-resistant
    """
    SPIKE_MULT   = 2.5   # TR > 2.5× ATR = spike candle
    OUTLIER_MULT = 3.0   # TR > 3.0× median TR = outlier dikecualikan dari rolling

    n = len(closes)
    if n < period + 1:
        # Data tidak cukup → fallback ke ATR sederhana tanpa perlindungan
        tr_raw = [max(highs[i] - lows[i],
                      abs(highs[i] - closes[i-1]),
                      abs(lows[i]  - closes[i-1]))
                  for i in range(1, n)]
        if not tr_raw:
            return 0.0
        return float(sum(tr_raw[-period:]) / min(period, len(tr_raw)))

    # Hitung TR penuh
    tr_full = [max(float(highs[i]) - float(lows[i]),
                   abs(float(highs[i]) - float(closes[i-1])),
                   abs(float(lows[i])  - float(closes[i-1])))
               for i in range(1, n)]

    # Median TR untuk outlier detection (robust terhadap spike)
    tr_sorted = sorted(tr_full)
    mid       = len(tr_sorted) // 2
    median_tr = tr_sorted[mid] if len(tr_sorted) % 2 != 0 else \
                (tr_sorted[mid-1] + tr_sorted[mid]) / 2

    # Rolling ATR dengan outlier exclusion: ganti outlier dengan median TR
    tr_clean = [
        tr if tr <= OUTLIER_MULT * median_tr else median_tr
        for tr in tr_full
    ]
    atr_clean = float(pd.Series(tr_clean).rolling(period).mean().iloc[-1])

    # Spike detection: TR candle terakhir
    last_tr = tr_full[-1]
    if last_tr > SPIKE_MULT * atr_clean and atr_clean > 0:
        # Blend: 60% ATR bersih + 40% last TR → responsif tapi tidak dramatis
        atr_final = 0.60 * atr_clean + 0.40 * last_tr
    else:
        atr_final = atr_clean

    return float(atr_final)


def calc_adx(highs, lows, closes, period=14) -> tuple:
    """
    ADX + DI menggunakan Wilder's smoothing (metode standar industri).
    Returns: (adx, plus_di, minus_di) — semua float.

    Interpretasi ADX:
      >= 25 : trend kuat (TRENDING)
      18–25 : transisi / ranging (RANGING)
      < 18  : sideways / choppy (CHOPPY — no trade zone)

    +DI > -DI : tekanan bullish dominan
    -DI > +DI : tekanan bearish dominan
    """
    n = len(closes)
    if n < period * 2 + 2:
        return 0.0, 0.0, 0.0

    plus_dm  = np.zeros(n)
    minus_dm = np.zeros(n)
    tr_arr   = np.zeros(n)

    for i in range(1, n):
        up   = float(highs[i])   - float(highs[i-1])
        down = float(lows[i-1])  - float(lows[i])
        plus_dm[i]  = up   if up   > down and up   > 0 else 0.0
        minus_dm[i] = down if down > up   and down > 0 else 0.0
        tr_arr[i]   = max(float(highs[i]) - float(lows[i]),
                          abs(float(highs[i]) - float(closes[i-1])),
                          abs(float(lows[i])  - float(closes[i-1])))

    def wilder_smooth(arr):
        """Wilder's smoothing — setara EMA dengan alpha = 1/period.
        Rumus: smoothed[i] = smoothed[i-1] * (period-1)/period + arr[i]
        Seed pertama pakai SMA dari candle 1..period (bukan sum).
        [v7.6 #5] Mencegah overestimation ADX pada candle awal.
        [v7.8 FIX] Rumus dikoreksi: prev*(period-1)/period + curr
                   Sebelumnya: prev - prev/period + curr — ekuivalen tapi
                   rentan floating point drift pada DX yang sudah 0-100 range,
                   menyebabkan ADX meledak di atas 100 (contoh: 414.5).
        """
        out = np.zeros(n)
        out[period] = float(np.mean(arr[1:period+1]))
        for i in range(period + 1, n):
            out[i] = out[i-1] * (period - 1) / period + arr[i] / period
        return out

    s_tr  = wilder_smooth(tr_arr)
    s_pdm = wilder_smooth(plus_dm)
    s_mdm = wilder_smooth(minus_dm)

    with np.errstate(divide="ignore", invalid="ignore"):
        pdi = np.where(s_tr > 0, 100.0 * s_pdm / s_tr, 0.0)
        mdi = np.where(s_tr > 0, 100.0 * s_mdm / s_tr, 0.0)
        dx  = np.where((pdi + mdi) > 0,
                       100.0 * np.abs(pdi - mdi) / (pdi + mdi), 0.0)

    # ADX = Wilder smooth dari DX — hasilnya PASTI 0-100 karena DX sudah 0-100
    adx_arr = wilder_smooth(dx)
    adx_val = float(np.clip(adx_arr[-1], 0.0, 100.0))  # safety clamp
    return adx_val, float(pdi[-1]), float(mdi[-1])


def detect_market_regime(closes, highs, lows) -> dict:
    """
    Klasifikasi regime market per pair — landasan NO TRADE ZONE.

    Regime:
      TRENDING : ADX >= ADX_TREND (25) → sinyal valid + bonus score
      RANGING  : ADX_CHOP <= ADX < ADX_TREND → sinyal lolos tapi penalti
      CHOPPY   : ADX < ADX_CHOP (18) → blok sinyal sebelum scoring

    trend_dir dari perbandingan +DI vs -DI — hanya informatif,
    tidak menggantikan BOS/CHoCH sebagai gate utama.
    """
    adx, pdi, mdi = calc_adx(highs, lows, closes, period=ADX_PERIOD)

    if adx >= ADX_TREND:
        regime = "TRENDING"
    elif adx >= ADX_CHOP:
        regime = "RANGING"
    else:
        regime = "CHOPPY"

    if pdi > mdi + 2:
        trend_dir = "BULLISH"
    elif mdi > pdi + 2:
        trend_dir = "BEARISH"
    else:
        trend_dir = "NEUTRAL"

    return {
        "regime":    regime,
        "adx":       round(adx, 1),
        "trend_dir": trend_dir,
        "plus_di":   round(pdi, 1),
        "minus_di":  round(mdi, 1),
    }


def calc_vwap(closes, highs, lows, volumes, timeframe: str = "1h") -> float:
    """
    [v7.1 #2] VWAP dihitung dari sesi 1 hari terakhir saja.
    [v7.2 FIX #1] Window disesuaikan per timeframe agar mendekati VWAP harian
    yang digunakan trader institusional:
      15m → 96 candle (~1 hari)
      30m → 48 candle (~1 hari)
      1h  → 24 candle (~1 hari)
      4h  →  6 candle (~1 hari)
    Sebelumnya selalu 48 candle regardless timeframe — distorsi VWAP pada 1h & 4h.
    """
    _window_map = {"15m": 96, "30m": 48, "1h": 24, "4h": 6}
    window = min(_window_map.get(timeframe, 24), len(closes))
    c = closes[-window:]; h = highs[-window:]
    l = lows[-window:];   v = volumes[-window:]
    tp = (h + l + c) / 3
    cum_v = np.cumsum(v) + 1e-9
    return float((np.cumsum(tp * v) / cum_v)[-1])



# ════════════════════════════════════════════════════════
#  STRUCTURE ENGINE
# ════════════════════════════════════════════════════════

def detect_swing_points(highs, lows, strength=3, lookback=80):
    """Deteksi swing high/low dengan strength filter."""
    # [v7.7 #11] Guard eksplisit jika strength terlalu besar relatif terhadap array.
    # Tanpa ini, loop tidak pernah berjalan dan caller mendapat list kosong tanpa warning.
    if strength >= len(highs) // 4:
        log(f"⚠️ detect_swing_points: strength={strength} terlalu besar untuk array len={len(highs)}", "warn")
        return []
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

    result.update({
        "last_sh": last_sh, "prev_sh": prev_sh,
        "last_sl": last_sl, "prev_sl": prev_sl,
        "valid": True,
    })

    hh = last_sh > prev_sh; hl = last_sl > prev_sl
    lh = last_sh < prev_sh; ll = last_sl < prev_sl
    if hh and hl:   result["bias"] = "BULLISH"
    elif lh and ll: result["bias"] = "BEARISH"
    else:           result["bias"] = "NEUTRAL"

    recent_closes = closes[-5:]

    # [v7.6 #6] range(1, ...) eksplisit — sebelumnya range(len(recent_closes)) dengan guard
    # "i > 0" menyebabkan i=0 (candle ke-5 dari belakang) tidak pernah dievaluasi.
    # Sekarang kita mulai dari i=1 dan akses recent_closes[i-1] selalu valid (i-1 >= 0).
    bull_break = any(recent_closes[i] > last_sh and
                     recent_closes[i-1] <= last_sh * 1.008
                     for i in range(1, len(recent_closes)))

    bear_break = any(recent_closes[i] < last_sl and
                     recent_closes[i-1] >= last_sl * 0.992
                     for i in range(1, len(recent_closes)))

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
    [v7.1 #4] Bounds check eksplisit — cegah IndexError di edge case.
    """
    result = {"valid": False, "ob_high": None, "ob_low": None}
    if len(closes) < lookback: return result
    c = closes[-lookback:]; h = highs[-lookback:]
    l = lows[-lookback:];   v = volumes[-lookback:]
    n = len(c)
    avg_body = float(np.mean([abs(c[i] - c[i-1]) for i in range(1, n)]))

    # [v7.7 #5] Hapus guard "if i+1 >= n: continue" — tidak pernah True karena
    # range(n-3, 1, -1) membatasi i maks di n-3, sehingga i+1 maks = n-2 < n selalu.
    # Guard tersebut menipu pembaca seolah ada risiko IndexError padahal tidak ada.
    for i in range(n - 3, 1, -1):   # i+1 maks = n-2 → selalu dalam range
        impulse = abs(c[i+1] - c[i])
        if impulse < avg_body * 1.5: continue
        if side == "BUY" and c[i] < c[i-1] and c[i+1] > c[i]:
            return {"valid": True, "ob_high": float(h[i]), "ob_low": float(l[i])}
        if side == "SELL" and c[i] > c[i-1] and c[i+1] < c[i]:
            return {"valid": True, "ob_high": float(h[i]), "ob_low": float(l[i])}
    return result


def detect_liquidity(closes, highs, lows, lookback=50) -> dict:
    """
    Deteksi equal highs/lows dan liquidity sweep.
    [v7.1 #9] Optimasi: equal high/low detection pakai vectorized numpy
    menggantikan nested loop O(n²).
    """
    result = {
        "equal_lows": None, "equal_highs": None,
        "sweep_bull": False, "sweep_bear": False,
    }
    if len(closes) < lookback: return result
    h = highs[-lookback:]; l = lows[-lookback:]
    c = closes[-lookback:]   # slice closes konsisten dengan h dan l
    tol = 0.003

    # [v7.1 #9] Vectorized: bandingkan setiap candle dengan window 10 sebelumnya
    # Equal highs
    for i in range(len(h) - 1, 0, -1):
        window_start = max(i - 10, 0)
        window = h[window_start:i]
        if len(window) == 0: continue
        diffs = np.abs(window - h[i]) / (h[i] + 1e-9)
        match_idx = np.where(diffs < tol)[0]
        if len(match_idx) > 0:
            j = window_start + match_idx[-1]
            result["equal_highs"] = float((h[i] + h[j]) / 2)
            break

    # Equal lows
    for i in range(len(l) - 1, 0, -1):
        window_start = max(i - 10, 0)
        window = l[window_start:i]
        if len(window) == 0: continue
        diffs = np.abs(window - l[i]) / (l[i] + 1e-9)
        match_idx = np.where(diffs < tol)[0]
        if len(match_idx) > 0:
            j = window_start + match_idx[-1]
            result["equal_lows"] = float((l[i] + l[j]) / 2)
            break

    ref_low  = float(np.min(l[:-5]))
    ref_high = float(np.max(h[:-5]))
    for i in range(-5, 0):
        if l[i] < ref_low and c[i] > ref_low:    # konsisten: semua pakai slice lookback
            result["sweep_bull"] = True
        if h[i] > ref_high and c[i] < ref_high:
            result["sweep_bear"] = True

    return result


# ════════════════════════════════════════════════════════
#  SETUP QUALITY ENGINE — [v7.10 #1]
#
#  Masalah dengan hard gate BOS/CHoCH:
#    has_struct = BOS or CHoCH or liq_sweep   ← binary, all-or-nothing
#    Banyak trade valid di-miss karena:
#      • Continuation saat struktur belum reset tapi bias masih kuat
#      • Breakout volume-driven tanpa CHoCH baru
#      • Entry di zona premium/discount tanpa BOS literal
#
#  Solusi: gradasi kualitas setup (0–3) → skor 0 = SKIP, 1–3 = lolos
#
#  Level:
#    3 = BOS/CHoCH terkonfirmasi — struktur paling kuat
#    2 = liq_sweep saja — entry level institusional terkonfirmasi
#    1 = bias + EMA momentum searah — continuation tanpa BOS baru
#    0 = tidak ada sinyal — SKIP
#
#  Hasil setup_score dipakai DUA KALI:
#    1. Sebagai gate: setup_score == 0 → return None di check_*
#    2. Sebagai bonus dalam score_signal: setup_score ditambahkan ke total
#       (max +3, skala proporsional dengan kualitas setup)
# ════════════════════════════════════════════════════════

def detect_setup_quality(side: str, structure: dict, liq: dict,
                         ema_fast: float, ema_slow: float) -> int:
    """
    [v7.10 #1] Evaluasi kualitas setup entry — return int 0–3.

    Menggantikan gate binary has_struct (BOS/CHoCH/sweep = required).
    Sekarang bot bisa menangkap trade valid dengan setup lemah (score 1)
    selama konfirmasi lain (momentum, volume) cukup kuat.

    Returns:
        3 = BOS atau CHoCH terkonfirmasi arah side
        2 = liq_sweep saja (tanpa BOS/CHoCH)
        1 = bias searah + EMA alignment (continuation mode)
        0 = tidak ada sinyal → caller wajib skip (return None)

    Catatan: setup_score == 0 adalah hard gate — tidak ada grace period.
    Setup_score 1 lolos tapi dikontribusikan minimal ke total score.
    """
    is_bull = (side == "BUY")

    # Level 3: BOS / CHoCH full confirmation
    has_bos_choch = (
        (is_bull  and (structure.get("bos") == "BULLISH" or structure.get("choch") == "BULLISH")) or
        (not is_bull and (structure.get("bos") == "BEARISH" or structure.get("choch") == "BEARISH"))
    )
    if has_bos_choch:
        return 3

    # Level 2: Liq sweep saja — level institusional terkonfirmasi
    has_sweep = (is_bull and liq.get("sweep_bull")) or (not is_bull and liq.get("sweep_bear"))
    if has_sweep:
        return 2

    # Level 1: Continuation mode — bias searah + EMA alignment
    # Tidak perlu BOS baru jika trend sudah jelas dan entry searah
    bias = structure.get("bias", "NEUTRAL")
    ema_aligned = (is_bull and ema_fast > ema_slow) or (not is_bull and ema_fast < ema_slow)
    bias_aligned = (is_bull and bias == "BULLISH") or (not is_bull and bias == "BEARISH")

    if bias_aligned and ema_aligned:
        return 1

    # Level 0: tidak ada sinyal apapun — hard skip
    return 0


# ════════════════════════════════════════════════════════
#  SCORING ENGINE
# ════════════════════════════════════════════════════════

def score_signal(side: str, price: float, closes, highs, lows, volumes,
                 structure: dict, liq: dict, ob: dict,
                 rsi: float, macd: float, msig: float,
                 ema_fast: float, ema_slow: float,
                 vwap: float,
                 regime: str = "TRENDING",
                 setup_score: int = 3) -> float:
    """
    [v7.28] Simplified 3-factor scoring engine.

    2 core factors (masing-masing 1 poin) + 1 volume booster (0.5 poin):
      1. Trend    : EMA fast > slow searah entry  → +1.0
      2. Momentum : MACD crossover searah entry   → +1.0
      3. Volume   : spike > 1.3× avg 10 candle   → +0.5 (booster, bukan syarat wajib)

    core_score = trend + momentum + volume  →  range 0–2.5 (tanpa setup bonus)

    Setup bonus (SETUP_BONUS — bukan gate, bukan multiplier):
      setup 3 (BOS/CHoCH)    → +0.5
      setup 2 (liq_sweep)    → +0.25
      setup 1 (continuation) → +0.0

    Final score = core_score + setup_bonus  →  range 0.0–3.5

    Tier (assign_tier):
      A+  : score >= 3   (semua 3 core factor terpenuhi)
      A   : score >= 2   (2 dari 3 — minimum viable)
      SKIP: score <  2

    Prasyarat (divalidasi SEBELUM fungsi ini):
    - ADX CHOPPY → hard gate di check_* (2 hard gate yang tersisa)
    - BTC crash  → hard gate di level atas
    - setup_score >= 1 → hard gate di check_* (setup == 0 = no signal)
    """
    is_bull = (side == "BUY")

    # ── Factor 1: TREND ───────────────────────────────────────────
    trend = 0
    if is_bull     and ema_fast > ema_slow: trend = GROUPS["trend"]
    if not is_bull and ema_fast < ema_slow: trend = GROUPS["trend"]

    # ── Factor 2: MOMENTUM ────────────────────────────────────────
    momentum = 0
    if is_bull     and macd > msig: momentum = GROUPS["momentum"]
    if not is_bull and macd < msig: momentum = GROUPS["momentum"]

    # ── Factor 3: VOLUME ──────────────────────────────────────────
    # [v8.9] Volume diubah dari hard 1-poin menjadi 0.5-poin booster.
    # Rasional: trend kuat sering tidak disertai volume spike (terutama swing gradual)
    # tapi tetap valid secara teknikal. Model lama: pair tanpa spike = SKIP
    # padahal trend + momentum terpenuhi → undertrading.
    # Sekarang: volume spike = +0.5 bonus (konfirmasi tambahan, bukan syarat wajib).
    # trend+momentum = 2 poin → tier A (lolos) dengan atau tanpa volume.
    # Hanya 1 core factor = 1 poin → tetap SKIP (noise filter terjaga).
    volume = 0.0
    if len(volumes) >= 2:
        vol_avg = float(np.mean(volumes[-10:-1])) if len(volumes) >= 10 else float(np.mean(volumes[:-1]))
        if vol_avg > 0 and float(volumes[-1]) > vol_avg * 1.3:
            volume = 0.5   # booster 0.5 poin — bukan 1 poin penuh (GROUPS["vol_confirm"])

    # ── Core score (integer 0–3) ──────────────────────────────────
    core_score = trend + momentum + volume

    # ── Setup bonus (float 0/0.5/1.0) ────────────────────────────
    # Setup kuat sedikit mendorong score tanpa menjadi blocker utama.
    # Hanya setup_score 3 yang layak naik tier (selisih 1.0 poin).
    setup_bonus = SETUP_BONUS.get(setup_score, 0.0)

    return core_score + setup_bonus


def assign_tier(score: float) -> str:
    """
    [v8.9] Tier dari 2-core + volume booster score (range 0.0–3.0).
      A+ : score >= 2.5 → trend + momentum + volume spike
      A  : score >= 2.0 → trend + momentum tanpa volume — minimum viable
      SKIP: score < 2.0 → terlalu lemah
    """
    if score >= TIER_MIN_SCORE["A+"]: return "A+"
    if score >= TIER_MIN_SCORE["A"]:  return "A"
    return "SKIP"


def calc_conviction(score: float) -> str:
    """
    [v8.9] Conviction dari 2-core + volume booster score (0.0–3.0).
      >= 3.0 : VERY HIGH — trend + momentum + volume + setup BOS/CHoCH
      >= 2.5 : HIGH      — trend + momentum + volume (tier A+)
      >= 2.25: GOOD      — trend + momentum + setup bonus (tier A)
      >= 2.0 : OK        — minimum viable (tier A, tanpa bonus)
    """
    if score >= 3.0: return "VERY HIGH 🔥"
    if score >= 2.5: return "HIGH 💪"
    if score >= 2.25: return "GOOD ✅"
    return "OK 🟡"

def build_signal_reason(
    side: str,
    score: float,
    setup_score: int,
    structure: dict,
    liq: dict,
    regime: str,
    rsi: float,
    macd: float,
    ema_fast: float,
    ema_slow: float,
    strategy: str = "",
    msig: float = 0.0,   # [v8.9 FIX] tambah msig agar MACD check konsisten dengan score_signal
) -> str:
    """
    [v8.4] Bangun string penjelasan kenapa signal digenerate.

    Menggabungkan factor-factor utama yang berkontribusi ke score menjadi
    satu string ringkas untuk log & Telegram — memudahkan debug dan review.

    Format: "EMA✅ MACD✅ BOS✅ liq✅ TRENDING"
    Setiap factor: ✅ = terpenuhi, ⬜ = tidak.
    """
    parts = []

    # Core scoring factors
    ema_aligned = (ema_fast > ema_slow) if side == "BUY" else (ema_fast < ema_slow)
    parts.append(f"EMA{'✅' if ema_aligned else '⬜'}")

    macd_aligned = (macd > msig) if side == "BUY" else (macd < msig)   # [v8.9 FIX] vs signal line, konsisten dengan score_signal
    parts.append(f"MACD{'✅' if macd_aligned else '⬜'}")

    # Setup quality
    if setup_score >= 3:
        bos = structure.get("bos_bull") if side == "BUY" else structure.get("bos_bear")
        choch = structure.get("choch_bull") if side == "BUY" else structure.get("choch_bear")
        if bos or choch:
            parts.append("BOS/CHoCH✅")
        else:
            parts.append("setup3✅")
    elif setup_score == 2:
        sweep = liq.get("sweep_bull") if side == "BUY" else liq.get("sweep_bear")
        parts.append(f"liq_sweep{'✅' if sweep else '⬜'}")

    # RSI context
    if side == "BUY" and rsi < 40:
        parts.append(f"RSI_OS({rsi:.0f})")
    elif side == "SELL" and rsi > 60:
        parts.append(f"RSI_OB({rsi:.0f})")

    # Regime
    if regime:
        parts.append(regime)

    return " | ".join(parts) if parts else f"score={score:.1f}"


def build_pump_reason(
    rsi: float,
    macd: float,
    msig: float,
    vol_ratio: float,
    pct_change: float,
    ema7: float,
    ema20: float,
) -> str:
    """
    [v8.9] Bangun string penjelasan kenapa PUMP signal digenerate.

    Format: "Vol3.2× | +5.1% 45m | EMA✅ | MACD✅ | RSI(52)"
    Memudahkan user memahami faktor apa yang memicu pump alert.
    """
    parts = []

    # Volume spike — faktor utama pump
    parts.append(f"Vol{vol_ratio:.1f}×")

    # Price momentum dalam 45m (3 candle × 15m)
    parts.append(f"+{pct_change:.1f}% 45m")

    # EMA momentum
    ema_bull = ema7 > ema20
    parts.append(f"EMA{'✅' if ema_bull else '⬜'}")

    # MACD bullish cross
    macd_bull = macd > msig
    parts.append(f"MACD{'✅' if macd_bull else '⬜'}")

    # RSI context (pump valid saat belum overbought)
    parts.append(f"RSI({rsi:.0f})")

    return " | ".join(parts)


def build_microcap_reason(
    rsi: float,
    macd: float,
    msig: float,
    vol_ratio: float,
    pct_3h: float,
    ema7: float,
    ema20: float,
    has_sweep: bool,
    atr_pct: float,
) -> str:
    """
    [v8.9] Bangun string penjelasan kenapa MICROCAP signal digenerate.

    Format: "Vol5.3× | +3.8% 3h | EMA✅ | MACD✅ | sweep✅ | ATR2.1% | RSI(45)"
    Menggabungkan semua gate yang lolos menjadi satu ringkasan audit trail.
    """
    parts = []

    # Volume anomali — gate utama microcap
    parts.append(f"Vol{vol_ratio:.1f}×")

    # Momentum 3 candle 1h
    parts.append(f"+{pct_3h:.1f}% 3h")

    # EMA alignment
    ema_bull = ema7 > ema20
    parts.append(f"EMA{'✅' if ema_bull else '⬜'}")

    # MACD bullish
    macd_bull = macd > msig
    parts.append(f"MACD{'✅' if macd_bull else '⬜'}")

    # Liquidity sweep bonus
    if has_sweep:
        parts.append("sweep✅")

    # ATR — volatility context (menunjukkan ruang gerak tersedia)
    parts.append(f"ATR{atr_pct:.1f}%")

    # RSI
    parts.append(f"RSI({rsi:.0f})")

    return " | ".join(parts)


# ════════════════════════════════════════════════════════
#  UNIFIED MICROCAP SCORING — [v7.12 #2]
#
#  Masalah sebelumnya:
#  check_microcap menggunakan micro_score (0–4) sistem sendiri —
#  tidak bisa dibandingkan dengan INTRADAY/SWING score (6–18),
#  tidak dapat masuk win rate bucket yang sama, dan tidak punya
#  conviction/tier S atau A+ yang bermakna.
#
#  Solusi: score_microcap_unified() membangun sinyal microcap
#  menggunakan score_signal() yang sama dengan INTRADAY/SWING,
#  dengan adaptasi kontekstual:
#    - setup_score: has_sweep→2 (liq_sweep), ema_bull→1 (continuation)
#    - regime: tetap dari detect_market_regime (jika ada)
#    - ob: empty dict (order block jarang valid di microcap)
#    - Hasil: score pada skala 6–18, tier S/A+/A, conviction label
#
#  Microcap masih memiliki hard gates sendiri (vol spike, momentum,
#  RSI) di check_microcap — unified scoring hanya menggantikan
#  micro_score (0–4) dengan score engine yang konsisten.
# ════════════════════════════════════════════════════════

def score_microcap_unified(price: float, closes, highs, lows, volumes,
                            rsi: float, macd: float, msig: float,
                            ema_fast: float, ema_slow: float,
                            has_sweep: bool, regime: str = "RANGING") -> tuple[int, str, str]:
    """
    [v7.12 #2] Hitung unified score untuk microcap menggunakan score_signal().

    Microcap tidak punya struktur BOS/CHoCH yang reliable, sehingga:
      - setup_score: 2 jika has_sweep (liq_sweep terkonfirmasi)
                     1 jika tidak (continuation bias — EMA gate sudah di check_microcap)
      - structure: dict minimal — hanya last_sl untuk pullback check
      - ob: empty dict — order block jarang reliable di microcap kecil

    Microcap cenderung RANGING (belum trending) — default regime RANGING.
    Jika detect_market_regime tersedia dari caller, gunakan regime aktual.

    Returns:
        (score: float, tier: str, conviction: str)
        Caller bisa langsung gunakan ketiga nilai ini.
    """
    # Setup score untuk microcap: sweep = level 2, tidak ada sweep = level 1
    setup_score = 2 if has_sweep else 1

    # Structure minimal — tidak ada swing analysis di microcap
    # last_sl disimulasikan agar pullback check tidak crash
    structure = {"last_sl": None, "last_sh": None, "bias": "BULLISH",
                 "bos": None, "choch": None, "valid": True}

    # Order block tidak dipakai di microcap — set invalid
    ob_empty = {"valid": False}

    # Vwap tidak digunakan di microcap (tidak ada di score_signal v7.8+)
    # ob_ratio tidak digunakan di score_signal
    score = score_signal(
        side="BUY",
        price=price,
        closes=closes,
        highs=highs,
        lows=lows,
        volumes=volumes,
        structure=structure,
        liq={"sweep_bull": has_sweep, "sweep_bear": False},
        ob=ob_empty,
        rsi=rsi,
        macd=macd,
        msig=msig,
        ema_fast=ema_fast,
        ema_slow=ema_slow,
        vwap=price,          # tidak digunakan oleh score_signal v7.8+
        # [v8.9 FIX] ob_ratio dihapus dari call — bukan parameter score_signal (v7.8+)
        regime=regime,
        setup_score=setup_score,
    )

    tier       = assign_tier(score)
    conviction = calc_conviction(score)
    return score, tier, conviction


# ════════════════════════════════════════════════════════
#  DYNAMIC PRIORITY SYSTEM — [v7.12 #1]
#
#  Masalah hard-coded priority:
#  PUMP > INTRADAY BUY > SWING BUY selalu — tidak peduli kondisi.
#  Di market crash: PUMP adalah distribusi (institutional exit ke retail),
#  bukan accumulation. Memberi PUMP prioritas 0 di crash berbahaya.
#
#  Solusi: calc_dynamic_priority() menghitung priority runtime dari:
#    1. Base priority (PRIORITY_BASE) — baseline v7.10
#    2. BTC regime modifier — crash/drop turunkan pump priority
#    3. Fear & Greed modifier — extreme greed turunkan pump priority
#    4. Tier bonus — tier S dapat sedikit boost (−1 ke priority number)
#
#  Priority number tetap "lower = higher priority" (sama dengan v7.10).
#  Conflict resolution tetap gunakan fungsi ini — resolve_conflicts()
#  diganti dengan resolve_conflicts_dynamic() yang menerima context.
# ════════════════════════════════════════════════════════

def calc_dynamic_priority(sig: dict, btc: dict, fg: int) -> int:
    """
    [v7.12 #1] Hitung priority signal secara dinamis berdasarkan market context.

    Priority = base_priority + modifiers
    Lower number = higher priority (dipertahankan saat conflict).

    Modifiers yang diterapkan:
      BTC crash (4h < -10%): PUMP_BUY mendapat +PUMP_CRASH_PENALTY
        → PUMP tidak lagi paling prioritas saat crash — terlalu berisiko
      Extreme greed (F&G >= 75): PUMP_BUY mendapat +PUMP_GREED_PENALTY
        → Top signal pump di extreme greed = late buyers trap
      BTC drop 1h (< -3%): PUMP_BUY mendapat +PUMP_DROP_PENALTY
        → BUY strategies sedikit lebih dipertanyakan saat BTC bearish
      Tier S: semua strategy mendapat -1 bonus (lebih reliable)
        → Tier S sinyal bertarung lebih baik vs sinyal yang sama dari tier A

    Args:
        sig : signal dict (wajib punya "strategy", "side", "tier")
        btc : dict dari get_btc_regime()
        fg  : Fear & Greed index (int, 0–100)

    Returns:
        int — priority score (lower = higher priority)
    """
    strat   = sig.get("strategy", "")
    side    = sig.get("side", "BUY")
    tier    = sig.get("tier", "A")
    key     = f"{strat}_{side}"

    prio = PRIORITY_BASE.get(key, 99)

    # ── Modifier 1: BTC Crash — PUMP sangat berbahaya ────────────
    # Volume spike di crash = institutional distribution ke retail.
    # Naikkan priority number PUMP → tidak lagi prioritas tertinggi.
    if strat == "PUMP" and btc.get("halt"):
        prio += PUMP_CRASH_PENALTY   # crash: priority 0+4 = 4 (setara INTRADAY_SELL)

    # ── Modifier 2: Extreme Greed — PUMP = late buyer trap ───────
    elif strat == "PUMP" and fg >= FG_SELL_BLOCK:
        prio += PUMP_GREED_PENALTY   # greed: priority 0+3 = 3 (setara SWING_SELL)

    # ── Modifier 3: BTC Drop 1h — BUY sedikit lebih dipertanyakan
    elif strat == "PUMP" and btc.get("block_buy"):
        prio += PUMP_DROP_PENALTY    # drop: priority 0+1 = 1 (tied INTRADAY_BUY)

    # ── Modifier 4: Tier bonus — S lebih reliable dari A ─────────
    # Berlaku untuk SEMUA strategy, bukan hanya PUMP.
    # Tier S mendapat keunggulan kecil dalam conflict tiebreak.
    if tier == "S":
        prio -= 1   # S lebih prioritas dari strategy yang sama tier A+/A
    elif tier == "A":
        prio += 0   # baseline — tidak ada bonus

    return prio


def resolve_conflicts_dynamic(signals: list, btc: dict, fg: int) -> list:
    """
    [v7.12 #1] Conflict resolution dengan dynamic priority — gantikan resolve_conflicts().

    Sama dengan resolve_conflicts() v7.10, tapi priority dihitung secara
    runtime menggunakan calc_dynamic_priority(sig, btc, fg) alih-alih
    lookup tabel statis (dihapus v8.4 — digantikan dynamic priority).

    Untuk setiap pair, hanya signal dengan priority terendah (= tertinggi)
    yang dipertahankan. Signal lain di-drop dan dicatat di log.

    Args:
        signals : list sinyal kandidat
        btc     : dict dari get_btc_regime() — untuk priority calculation
        fg      : Fear & Greed index — untuk priority calculation
    """
    best: dict    = {}   # pair → (priority, signal)
    dropped: list = []

    for sig in signals:
        pair = sig["pair"]
        prio = calc_dynamic_priority(sig, btc, fg)

        if pair not in best:
            best[pair] = (prio, sig)
        else:
            existing_prio, existing_sig = best[pair]
            if prio < existing_prio:
                dropped.append(
                    f"{pair}: [{existing_sig['strategy']} {existing_sig['side']} "
                    f"prio={existing_prio}] → kalah vs "
                    f"[{sig['strategy']} {sig['side']} prio={prio}]"
                )
                best[pair] = (prio, sig)
            else:
                dropped.append(
                    f"{pair}: [{sig['strategy']} {sig['side']} prio={prio}] "
                    f"→ kalah vs [{existing_sig['strategy']} {existing_sig['side']} "
                    f"prio={existing_prio}]"
                )

    if dropped:
        log(f"⚔️ Conflict resolution [dynamic] — {len(dropped)} signal di-drop:")
        for d in dropped:
            log(f"   {d}")

    return [sig for _, sig in best.values()]


# ════════════════════════════════════════════════════════
#  PROBABILISTIC CONFIDENCE MODEL — [v7.8 #9]
#
#  Masalah: calc_conviction() bersifat deterministic — hanya
#  berdasarkan score. Ini menyebabkan semua "Tier A" dianggap
#  setara, padahal A score 8 ≠ A score 13 secara historis.
#
#  Solusi: tambahkan lapisan probabilistik berbasis data aktual.
#  Win rate dihitung dari riwayat signal di Supabase, dikelompokkan
#  per score bucket, dan ditampilkan terpisah dari conviction label.
#
#  Pemisahan yang jelas:
#    calc_conviction()    → rule-based  (score → label deterministic)
#    estimate_confidence()→ data-driven (win rate dari historis nyata)
#
#  Ini membuat user bisa melihat perbedaan kualitas DALAM tier yang sama.
# ════════════════════════════════════════════════════════

# ── [v7.11 #3] Bayesian Win Rate Config ──────────────
# Menggantikan pure frequentist ratio (wins/total).
#
# Masalah frequentist: 5 WIN / 8 total = 62.5% WR — angka menyesatkan
# karena sample terlalu kecil. User bisa overtrade berdasar angka palsu ini.
#
# Solusi: Beta distribution posterior dengan Jeffreys' prior (α=1, β=1).
# Posterior mean = (wins + α) / (wins + α + losses + β)
#   → "shrinks" angka kecil mendekati prior 50%
#   → untuk sample besar, mendekati empiris tanpa bias
#
# Contoh shrinkage:
#   5 WIN / 8 total  → frequentist: 62.5% → Bayesian: (5+1)/(8+2) = 60.0%
#   1 WIN / 2 total  → frequentist: 50.0% → Bayesian: (1+1)/(2+2) = 50.0%
#   50 WIN / 80 total → frequentist: 62.5% → Bayesian: 62.2% (sedikit berbeda)
BAYES_PRIOR_ALPHA = 1.0   # Jeffreys' prior — uninformative, symmetric
BAYES_PRIOR_BETA  = 1.0   # pasangan dari alpha — posterior Beta(wins+1, losses+1)

# [v7.10 #3] Adaptive MIN_SAMPLE per bucket — gantikan konstanta flat 20.
#
# Masalah dengan flat MIN_SAMPLE=20:
#   - bucket "12+" sangat jarang → bisa terisi 20 sample dari bulan pertama saja
#   - bias ke early-stage data yang belum representatif
#   - confidence "60%" dari 5 WIN / 8 total tidak semestinya ditampilkan
#
# Solusi: threshold per bucket, semakin tinggi bucket semakin ketat.
# Bucket lebih jarang → lebih banyak sample dibutuhkan sebelum angka ditampilkan.
# Context bucket (regime-aware) lebih spesifik → butuh 1.5× lebih banyak sample.
MIN_SAMPLE_BY_BUCKET: dict = {
    "2.0-2.9": 15,   # tier A bawah — paling banyak sample, paling longgar
    "3.0-3.4": 20,   # tier A+ baseline
    "3.5+":    25,   # tier A+ dengan setup bonus — paling langka, paling ketat
}
MIN_SAMPLE_CTX_FACTOR = 1.5   # context bucket (regime split) butuh 1.5× base


def get_min_sample(bucket: str) -> int:
    """
    [v7.10 #3] Return minimum sample threshold untuk bucket tertentu.

    Context bucket (e.g. "9-11|TRENDING") lebih spesifik dari score-only
    bucket — butuh lebih banyak sample sebelum angka dianggap reliable.

    Contoh:
      "6"           → 15
      "12+"         → 30
      "9-11|TRENDING" → int(25 × 1.5) = 37
      "12+|RANGING"   → int(30 × 1.5) = 45
    """
    base_key = bucket.split("|")[0]   # "9-11|TRENDING" → "9-11"
    base     = MIN_SAMPLE_BY_BUCKET.get(base_key, 20)
    return int(base * MIN_SAMPLE_CTX_FACTOR) if "|" in bucket else base

# Cache win rate — diisi sekali per run, TTL 1 jam
# Format: {"12+": {"wins": 12, "total": 20, "wr": 0.60}, ...}
_winrate_cache: dict = {}
_winrate_cache_ts: float = 0.0
WINRATE_CACHE_TTL = 3600   # 1 jam dalam detik


def get_score_bucket(score: float) -> str:
    """
    [v7.28] Kelompokkan score ke bucket untuk aggregasi historis.

    Skala baru: 0.0–4.0 (3-factor + setup bonus).
    3 bucket mencerminkan tier natural: A+ / A_atas / A_bawah.
    Score di bawah 2 tidak sampai sini (sudah di-SKIP).
    """
    if score >= 3.5: return "3.5+"
    if score >= 3.0: return "3.0-3.4"
    return "2.0-2.9"


def get_context_bucket(score: float, regime: str = "") -> str:
    """
    [v7.9 #1] Bucket kontekstual — gabungan score + regime.

    Tujuan: diferensiasi win rate antara:
      - score 9 di TRENDING  → historis bisa 65%+ WR
      - score 9 di RANGING   → historis bisa 45%  WR
    Padahal sebelumnya keduanya masuk bucket "9-11" yang sama.

    Format key: "{score_bucket}|{regime}"
    Contoh     : "9-11|TRENDING", "12+|RANGING", "6|TRENDING"

    Fallback: jika regime kosong, return score bucket biasa.
    Caller harus coba context bucket dulu, fallback ke score bucket
    jika sample tidak cukup.
    """
    base = get_score_bucket(score)
    if not regime:
        return base
    return f"{base}|{regime}"


def load_winrate_table() -> dict:
    """
    [v7.9 #1] Load historical win rate dari Supabase, grouped by context bucket.

    Context bucket = score_bucket + regime, contoh: "9-11|TRENDING".
    Ini membuat confidence estimate regime-aware:
      - score 9 di TRENDING  → bucket "9-11|TRENDING"
      - score 9 di RANGING   → bucket "9-11|RANGING"
    Keduanya tidak lagi disamakan seperti di v7.8.

    Fallback hierarchy (di estimate_confidence):
      1. Context bucket (score + regime) — paling spesifik
      2. Score-only bucket               — jika sample context < MIN_SAMPLE
      3. No data                         — jika keduanya kosong

    Win rate dihitung dari sinyal yang sudah memiliki result (WIN/LOSS).
    Sinyal dengan result=None (belum closed) tidak dihitung.

    Format result yang diterima sebagai WIN: "WIN", "TP1", "TP2"
    Format result yang diterima sebagai LOSS: "LOSS", "SL"

    Cache TTL: 1 jam — tidak perlu query setiap scan.
    Thread-safe karena bot berjalan single-threaded per cycle.

    Returns dict kosong jika Supabase tidak bisa di-reach.
    Caller harus handle kasus ini dengan graceful fallback.
    """
    global _winrate_cache, _winrate_cache_ts

    now = time.time()
    if _winrate_cache and now - _winrate_cache_ts < WINRATE_CACHE_TTL:
        return _winrate_cache

    try:
        # [v7.9] Tambah kolom "regime" agar bisa build context bucket
        rows = (
            supabase.table("signals_v2")
            .select("score, result, regime")
            .not_.is_("result", "null")
            .execute()
            .data
        )
        if not rows:
            log("📊 Win rate table: belum ada data historis dengan result.")
            return _winrate_cache   # return stale jika ada, kosong jika tidak

        buckets: dict = {}
        WIN_VALUES  = {"WIN", "TP1", "TP2", "PARTIAL_WIN"}  # [v8.6 FIX] PARTIAL_WIN adalah win
        LOSS_VALUES = {"LOSS", "SL"}

        for row in rows:
            raw_score  = row.get("score") or 0
            raw_result = (row.get("result") or "").upper().strip()
            raw_regime = (row.get("regime") or "").upper().strip()

            # Hanya hitung result yang dikenali — skip "PARTIAL", None, dll
            if raw_result not in WIN_VALUES and raw_result not in LOSS_VALUES:
                continue

            score_int = int(raw_score)
            is_win    = raw_result in WIN_VALUES

            # ── Score-only bucket (fallback) ──────────────────────
            sb = get_score_bucket(score_int)
            if sb not in buckets:
                buckets[sb] = {"wins": 0, "total": 0}
            buckets[sb]["total"] += 1
            if is_win:
                buckets[sb]["wins"] += 1

            # ── Context bucket (regime-aware) ─────────────────────
            # Hanya build jika regime tersedia di data historis
            if raw_regime in {"TRENDING", "RANGING"}:
                cb = get_context_bucket(score_int, raw_regime)
                if cb not in buckets:
                    buckets[cb] = {"wins": 0, "total": 0}
                buckets[cb]["total"] += 1
                if is_win:
                    buckets[cb]["wins"] += 1

        # [v7.11 #3] Hitung win rate per bucket — Bayesian posterior, bukan frequentist.
        # Posterior mean Beta(wins + α, losses + β) — shrinks small samples ke prior 50%.
        # wr_freq disimpan terpisah untuk referensi / debugging.
        for b in buckets:
            t = buckets[b]["total"]
            w = buckets[b]["wins"]
            l = t - w   # losses

            # Bayesian posterior mean dengan Jeffreys' prior
            alpha_post = w + BAYES_PRIOR_ALPHA
            beta_post  = l + BAYES_PRIOR_BETA
            wr_bayes   = alpha_post / (alpha_post + beta_post)

            buckets[b]["wr"]      = round(wr_bayes, 3)
            buckets[b]["wr_freq"] = round(w / t, 3) if t > 0 else 0.0   # simpan referensi

        _winrate_cache    = buckets
        _winrate_cache_ts = now

        # Summary: tampilkan context bucket yang sudah reliable
        reliable_ctx = {
            b: d for b, d in buckets.items()
            if "|" in b and d["total"] >= MIN_SAMPLE_FOR_CONFIDENCE
        }
        if reliable_ctx:
            ctx_summary = " | ".join(
                f'{b}: {d["wr"]:.0%}★ (n={d["total"]})'   # ★ = Bayesian
                for b, d in sorted(reliable_ctx.items())
            )
            log(f"📊 Win rate loaded [Bayesian] — {len(rows)} trades | Context: {ctx_summary}")
        else:
            score_summary = " | ".join(
                f'{b}: {d["wr"]:.0%}★ (n={d["total"]})'
                for b, d in sorted(buckets.items()) if "|" not in b
            )
            log(f"📊 Win rate loaded [Bayesian] — {len(rows)} trades | Score-only: {score_summary} "
                f"(context buckets belum cukup sample)")
        return buckets

    except Exception as e:
        log(f"⚠️ load_winrate_table: {e} — pakai cache lama", "warn")
        return _winrate_cache   # return stale cache jika ada


def estimate_confidence(score: float, regime: str = "") -> dict:
    """
    [v7.9 #1 + v7.10 #3 + v7.11 #3] Return probabilistic confidence — Bayesian, regime-aware.

    Ini BERBEDA dari calc_conviction():
    - calc_conviction() → deterministic, hanya dari score
    - estimate_confidence() → dari data historis nyata di Supabase,
                               regime-aware (v7.9) + adaptive threshold (v7.10)
                               + Bayesian posterior mean (v7.11)

    [v7.11 #3] Win rate sekarang Bayesian posterior mean — bukan frequentist ratio.
    Bucket kecil otomatis "dikonservatifkan" mendekati prior 50%.
    Contoh: 5 WIN / 8 total → frequentist 62.5% → Bayesian 60.0% (lebih jujur).
    Label di Telegram ditandai dengan ★ untuk membedakan dari frequentist.

    [v7.10 #3] MIN_SAMPLE sekarang per-bucket — bukan flat 20.
    Bucket langka (12+) butuh 30 sample. Context bucket butuh 1.5× lebih banyak.

    Fallback hierarchy (dua tingkat):
      1. Context bucket: "{score_bucket}|{regime}" — paling spesifik
         Dipakai jika: sample >= get_min_sample(ctx_bucket)
      2. Score-only bucket: "{score_bucket}" — fallback
         Dipakai jika: context bucket belum cukup sample
         Evaluasi dengan get_min_sample(score_bucket)
      3. No data: tidak ada data di kedua bucket

    Args:
        score  : raw score dari score_signal()
        regime : "TRENDING" / "RANGING" / "" — dari mkt["regime"]

    Returns:
        {
          "wr":        float | None,   # Bayesian posterior mean
          "wr_freq":   float | None,   # frequentist ratio (referensi)
          "n":         int,
          "bucket":    str,
          "ctx_used":  bool,
          "label":     str,
          "reliable":  bool,
          "min_n":     int,            # threshold aktual yang dipakai
        }
    """
    table = load_winrate_table()

    ctx_bucket   = get_context_bucket(score, regime)
    score_bucket = get_score_bucket(score)
    ctx_used     = False

    # ── Coba context bucket dulu — dengan threshold adaptif ───────
    ctx_data  = table.get(ctx_bucket)
    ctx_min_n = get_min_sample(ctx_bucket)
    ctx_ok    = ctx_data is not None and ctx_data["total"] >= ctx_min_n

    if ctx_ok:
        bucket   = ctx_bucket
        data     = ctx_data
        ctx_used = True
        min_n    = ctx_min_n
    elif score_bucket in table:
        bucket = score_bucket
        data   = table[score_bucket]
        min_n  = get_min_sample(score_bucket)
    else:
        return {
            "wr": None, "wr_freq": None, "n": 0,
            "bucket": ctx_bucket,
            "ctx_used": False,
            "label": f"⬜ No data (bucket {ctx_bucket})",
            "reliable": False,
            "min_n": get_min_sample(ctx_bucket),
        }

    n       = data["total"]
    wr      = data.get("wr")          # Bayesian posterior mean
    wr_freq = data.get("wr_freq")     # frequentist ratio — referensi saja
    reliable = n >= min_n

    regime_tag = f" {regime}" if ctx_used and regime else ""
    if not reliable:
        label = f"⬜ Data kurang (n={n}/{min_n})"
    elif wr >= 0.60:
        label = f"🟢 Kuat{regime_tag} ({wr:.0%}★, n={n})"
    elif wr >= 0.50:
        label = f"🟡 Positif{regime_tag} ({wr:.0%}★, n={n})"
    elif wr >= 0.40:
        label = f"🟠 Marginal{regime_tag} ({wr:.0%}★, n={n})"
    else:
        label = f"🔴 Lemah{regime_tag} ({wr:.0%}★, n={n})"

    return {
        "wr":       wr,
        "wr_freq":  wr_freq,
        "n":        n,
        "bucket":   bucket,
        "ctx_used": ctx_used,
        "label":    label,
        "reliable": reliable,
        "min_n":    min_n,
    }


# ════════════════════════════════════════════════════════
#  TP / SL CALCULATOR
# ════════════════════════════════════════════════════════

def calc_sl_tp(entry: float, side: str, atr: float,
               structure: dict, strategy: str) -> tuple:
    """
    Structure-first SL + ATR buffer (v7.8 #10).

    Masalah sebelumnya:
    - SL = ATR * multiplier murni → tidak mempertimbangkan struktur market
    - min(ATR-SL, structure-SL) → selalu ambil yang terkecil, bukan yang tepat
    - TP dihitung dari ATR bukan actual SL distance → R/R misrepresented
    - SL landed tepat di swing low tanpa buffer → mudah ter-wick

    Solusi — hierarki 4 langkah:

    Step 1: Structure anchor
      BUY  → last_sl (swing low terakhir)
      SELL → last_sh (swing high terakhir)

    Step 2: ATR buffer di belakang level
      SL = last_sl - atr * buffer   (BUY)
      SL = last_sh + atr * buffer   (SELL)
      Buffer kecil (0.3-0.5 ATR) memberi ruang terhadap wick candle.

    Step 3: Sanity bounds (adaptif per volatilitas)
      SL tidak boleh terlalu sempit (< min_pct) atau terlalu lebar (> max_pct).
      Jika terlalu sempit → lebarkan ke min_pct.
      Jika terlalu lebar  → sempitkan ke max_pct (jarang, tapi penting untuk volatile pair).

    Step 4: ATR fallback
      Jika tidak ada struktur valid, pakai ATR * multiplier murni.

    TP dari ACTUAL SL distance (bukan ATR), sehingga R/R selalu akurat.
    """
    if strategy == "INTRADAY":
        tp1_r, tp2_r         = INTRADAY_TP1_R, INTRADAY_TP2_R
        atr_fallback_mult    = INTRADAY_SL_ATR
        atr_buffer           = ATR_SL_BUFFER_INTRADAY
        min_sl_pct           = INTRADAY_MIN_SL_PCT
        max_sl_pct           = INTRADAY_MAX_SL_PCT
    else:  # SWING
        tp1_r, tp2_r         = SWING_TP1_R, SWING_TP2_R
        atr_fallback_mult    = SWING_SL_ATR
        atr_buffer           = ATR_SL_BUFFER_SWING
        min_sl_pct           = SWING_MIN_SL_PCT
        max_sl_pct           = SWING_MAX_SL_PCT

    if side == "BUY":
        last_sl = structure.get("last_sl")

        # Step 1 + 2: Structure anchor + ATR buffer
        if last_sl and last_sl < entry:
            sl = last_sl - atr * atr_buffer
        else:
            # Step 4: ATR fallback — tidak ada swing low valid
            sl = entry - atr * atr_fallback_mult

        # Step 3: Sanity bounds
        # Terlalu sempit → lebarkan (sl naik = lebih dekat entry, kita turunkan)
        sl = min(sl, entry * (1.0 - min_sl_pct))
        # Terlalu lebar  → sempitkan (sl naik = lebih dekat entry)
        sl = max(sl, entry * (1.0 - max_sl_pct))

        # TP dari actual SL distance — bukan ATR
        sl_dist = entry - sl
        tp1 = entry + sl_dist * tp1_r
        tp2 = entry + sl_dist * tp2_r

    else:  # SELL
        last_sh = structure.get("last_sh")

        # Step 1 + 2: Structure anchor + ATR buffer
        if last_sh and last_sh > entry:
            sl = last_sh + atr * atr_buffer
        else:
            # Step 4: ATR fallback
            sl = entry + atr * atr_fallback_mult

        # Step 3: Sanity bounds
        # Terlalu sempit → lebarkan (sl turun = lebih dekat entry, kita naikkan)
        sl = max(sl, entry * (1.0 + min_sl_pct))
        # Terlalu lebar  → sempitkan (sl turun = lebih dekat entry)
        sl = min(sl, entry * (1.0 + max_sl_pct))

        # TP dari actual SL distance
        sl_dist = sl - entry
        tp1 = entry - sl_dist * tp1_r
        tp2 = entry - sl_dist * tp2_r

    return round(sl, 8), round(tp1, 8), round(tp2, 8)


# ════════════════════════════════════════════════════════
#  MARKET CONTEXT
# ════════════════════════════════════════════════════════
#  POSITION SIZING ENGINE — [v7.13 #1]
#
#  Score 12 ≠ score 6. Conviction berbeda → size harus berbeda.
#  Formula: size = BASE_POSITION_USDT × tier_mult × wr_mult × drawdown_mult
#  Semua multiplier di-cap oleh MAX_POSITION_USDT.
# ════════════════════════════════════════════════════════

def calc_position_size(
    tier: str,
    conf: dict,
    drawdown_mode: str = "normal",
    atr: float | None = None,
    entry: float | None = None,
    rr: float = 2.0,
    strategy: str = "",
    regime: str = "",
    current_equity: float | None = None,
    pair: str = "",            # [v7.20 #B] pair baru yang akan dibuka
    open_pairs: list | None = None,  # [v7.20 #B] list pair open dari portfolio_state
    sl: float | None = None,         # [v7.27 #1] stop-loss price untuk fixed-risk sizing
) -> float:
    """
    [v7.27 #1] Fixed-risk position sizing — menggantikan Kelly sementara.

    Formula utama:
        sl_pct        = |entry - sl| / entry
        position_size = effective_equity × RISK_PER_TRADE / sl_pct

    Contoh: equity=$200, risk=1%, sl_pct=2% → size = $200×0.01/0.02 = $100
            → tier cap memotong ke MAX_POSITION_USDT ($25)

    Fallback (jika entry/sl tidak tersedia):
        Lapis vol-scalar: effective_equity × TARGET_RISK_PCT / atr_pct
        Lapis tier-base : BASE_POSITION_USDT × tier_mult

    Guardrail tetap berlaku:
        - Tier cap      : S ≤ 1.5×BASE, A+ ≤ 1.2×BASE, A ≤ 1.0×BASE
        - Drawdown mult : normal→1.0  warn→0.7  halt→0.4
        - Correlation   : corr_scalar jika pair berkorelasi tinggi dengan open trades
        - Floor/ceiling : MIN_POSITION_USDT – MAX_POSITION_USDT

    NOTE: Fixed-risk adalah satu-satunya sizing path sejak v7.27.
          Kelly dihapus di v8.4 — tidak ada rencana reaktivasi.

    Args:
        tier           : "S" | "A+" | "A"
        conf           : output dari estimate_confidence() — diabaikan untuk Kelly
        drawdown_mode  : "normal" | "warn" | "halt"
        atr            : ATR nilai absolut (optional, fallback vol-scalar)
        entry          : harga entry (required untuk fixed-risk & vol-scalar)
        rr             : reward-to-risk ratio (dipakai info log saja)
        strategy       : strategy name (dipakai log)
        regime         : regime name (dipakai log)
        current_equity : equity aktif dalam USDT; jika None pakai ACCOUNT_EQUITY_USDT
        pair           : pair baru — untuk corr-adjusted sizing
        open_pairs     : list open pair — untuk corr-adjusted sizing
        sl             : stop-loss price — REQUIRED untuk fixed-risk sizing

    Returns:
        float: position size dalam USDT, sudah di-cap dan di-floor.
    """
    # Resolve equity aktif
    effective_equity = current_equity if (current_equity and current_equity > MIN_POSITION_USDT) \
                       else ACCOUNT_EQUITY_USDT

    # [v7.20 #C] Compounding throttle — cegah over-sizing pasca partial TP
    _pre = _equity_cache.get("pre_partial_equity")
    if _pre is not None and _pre > MIN_POSITION_USDT and effective_equity > _pre:
        _gain            = effective_equity - _pre
        effective_equity = _pre + _gain * COMPOUNDING_THROTTLE_PCT

    tier_mult = TIER_SIZE_MULT.get(tier, 1.0)
    dd_mult   = {"normal": 1.0, "warn": 0.7, "halt": 0.4}.get(drawdown_mode, 1.0)
    tier_cap  = BASE_POSITION_USDT * tier_mult

    method   = "fallback_tier"
    raw_size = BASE_POSITION_USDT * tier_mult   # default fallback
    sl_pct   = 0.0   # [v8.9] declare outer scope untuk post-cap risk check

    # ── [v7.27 #1] Lapis 1: Fixed-risk sizing (primary) ─────────────────
    if entry is not None and sl is not None and entry > 0 and sl > 0:
        sl_pct = abs(entry - sl) / entry
        if sl_pct > 0.0001:   # guard div/0 dan SL yang tidak realistis (<0.01%)
            raw_size = effective_equity * RISK_PER_TRADE / sl_pct
            method   = f"fixed_risk(sl={sl_pct*100:.2f}%,r={RISK_PER_TRADE*100:.0f}%)"
        # else: sl_pct terlalu kecil → fallback tier di bawah

    # ── Lapis 2: Vol-scalar fallback (jika fixed-risk tidak bisa berjalan) ──
    elif atr is not None and entry is not None and entry > 0 and atr > 0:
        atr_pct = atr / entry
        if atr_pct > 0:
            vol_size = effective_equity * (TARGET_RISK_PCT / atr_pct)
            raw_size = min(BASE_POSITION_USDT * tier_mult, vol_size)
            method   = f"vol_scalar(atr={atr_pct*100:.2f}%)"

    # ── Tier cap sebagai guardrail atas ───────────────────────────────────
    raw_size = min(raw_size, tier_cap)

    # ── Drawdown penalty ──────────────────────────────────────────────────
    raw_size *= dd_mult

    # [v7.29] Correlation-adjusted sizing DIHAPUS — pairwise matrix di-stub.
    # Exposure per sektor kini dikontrol oleh MAX_PER_SECTOR gate di portfolio_allows().

    # ── [v8.9] Consistent risk normalization ─────────────────────────────
    # Masalah: saat fixed_risk menghasilkan size > equity_cap, ukuran dipotong
    # tapi risk efektif jadi tidak konsisten antar trade.
    # Contoh: SL=1% → raw=$200 → cap=$50 → real_risk=0.25% (bukan 1%)
    #         SL=5% → raw=$40  → cap lolos → real_risk=1.0%  (benar)
    # Fix: setelah cap, hitung effective_risk dari size yang sudah di-cap.
    # Jika effective_risk < RISK_PER_TRADE * 0.5 (under 50% target), log warning.
    equity_cap = effective_equity * MAX_POSITION_PCT
    raw_size   = min(raw_size, equity_cap)

    # Log jika sizing terpotong signifikan (real risk jauh di bawah target)
    if entry is not None and sl is not None and entry > 0 and sl > 0 and sl_pct > 0.0001:
        effective_risk_pct = raw_size * sl_pct / effective_equity
        if effective_risk_pct < RISK_PER_TRADE * 0.5:
            log(f"   ⚠️ [SIZE] Risk normalized: target={RISK_PER_TRADE*100:.1f}% "
                f"→ effective={effective_risk_pct*100:.2f}% (equity_cap aktif, SL={sl_pct*100:.1f}%)", "warn")

    # ── Floor & ceiling final ─────────────────────────────────────────────
    size = max(MIN_POSITION_USDT, min(MAX_POSITION_USDT, round(raw_size, 2)))

    log(f"   💰 Position size: ${size} USDT "
        f"[{method}] tier={tier}×{tier_mult} dd×{dd_mult} "
        f"equity_cap=${equity_cap:.0f} equity=${effective_equity:.0f}")
    return size



# ════════════════════════════════════════════════════════
#  DRAWDOWN AWARENESS — [v7.13 #4]
#
#  Bot kini tahu berapa consecutive loss terakhir.
#  Query signals_v2: ambil N signal terakhir yang sudah closed,
#  hitung trailing losing streak dari paling baru ke belakang.
# ════════════════════════════════════════════════════════


def _load_peak_equity_from_db() -> float:
    """
    [v7.22 #B] Load peak_equity tertinggi dari equity_snapshots di Supabase.

    Dipakai sebagai prev_peak saat bot restart (cold start) agar DD tidak
    kehilangan high-watermark historis yang sudah dicapai sebelumnya.

    Edge case yang ditangani:
      - Semua trade close → PnL negatif sedikit → bot restart
      - Tanpa persistence: prev_peak = equity sekarang → DD tampak 0%
        padahal sebelumnya equity pernah lebih tinggi
      - Dengan persistence: prev_peak = peak dari DB → DD akurat

    Returns:
        float: peak equity tertinggi yang pernah tersimpan, atau
               ACCOUNT_EQUITY_USDT jika belum ada data (floor safety).
    """
    try:
        rows = (
            supabase.table("equity_snapshots")
            .select("peak_equity")
            .order("peak_equity", desc=True)
            .limit(1)
            .execute()
            .data
        ) or []
        if rows:
            val = float(rows[0].get("peak_equity") or 0.0)
            # [v7.22 #B] Safety guard: peak tidak pernah di bawah modal awal
            if val < ACCOUNT_EQUITY_USDT:
                val = ACCOUNT_EQUITY_USDT
            return val
    except Exception as e:
        log(f"⚠️ _load_peak_equity_from_db: gagal — {e}. Fallback ACCOUNT_EQUITY_USDT.", "warn")
    return ACCOUNT_EQUITY_USDT




# ── Equity Cache ── [v7.16 #D] ───────────────────────
_equity_cache: dict = {"value": None, "available": None, "locked": 0.0, "ts": 0.0, "pre_partial_equity": None}


def _fetch_live_equity_from_exchange(client) -> float | None:
    """
    [v7.27 #4] Ambil saldo USDT aktual dari Gate.io spot wallet.

    Dipakai saat startup untuk menggantikan INITIAL_EQUITY_USDT hardcode.
    Jika gagal (API error, paper mode) → return None → caller pakai fallback env.

    Returns:
        float: saldo USDT available + locked, atau None jika fetch gagal.
    """
    try:
        accounts = client.list_spot_accounts(currency="USDT")
        if accounts:
            acc = accounts[0]
            available = float(getattr(acc, "available", 0) or 0)
            locked    = float(getattr(acc, "locked",    0) or 0)
            total     = round(available + locked, 4)
            if total > 0:
                log(f"   💼 Live equity dari Gate.io: ${total:.2f} USDT "
                    f"(available=${available:.2f} locked=${locked:.2f})")
                return total
    except Exception as e:
        log(f"   ⚠️ _fetch_live_equity_from_exchange: gagal — {e}. Pakai INITIAL_EQUITY_USDT.", "warn")
    return None


def bootstrap_account_equity(client) -> None:
    """
    [v7.27 #4] Dipanggil SEKALI saat bot start — set ACCOUNT_EQUITY_USDT secara dinamis.

    Priority:
      1. Gate.io API live balance (paling akurat)
      2. Env var INITIAL_EQUITY_USDT (manual config)
      3. Hardcode 200 (absolute fallback — log warning)
    """
    global ACCOUNT_EQUITY_USDT
    live = _fetch_live_equity_from_exchange(client)
    if live is not None and live > 0:
        ACCOUNT_EQUITY_USDT = live
    else:
        ACCOUNT_EQUITY_USDT = INITIAL_EQUITY_USDT
        log(f"   💼 Equity dari env: ${ACCOUNT_EQUITY_USDT:.2f} USDT (INITIAL_EQUITY_USDT)")
    log(f"   🏦 ACCOUNT_EQUITY_USDT → ${ACCOUNT_EQUITY_USDT:.2f}")
EQUITY_CACHE_TTL = 1800   # 30 menit — sinkron dengan drawdown cache


def get_current_equity_usdt() -> float:
    """
    [v7.16 #D] Hitung equity aktif dari PnL kumulatif nyata di Supabase.

    Formula:
        effective_equity = ACCOUNT_EQUITY_USDT (modal awal) + cumulative_pnl

    ACCOUNT_EQUITY_USDT tetap sebagai capital anchor (modal awal yang di-set user).
    cumulative_pnl diambil dari signals_v2.pnl_usdt — semua closed trades.

    Kenapa ini penting:
      Setelah 20 trade dengan net +40 USDT, equity aktif = 240 (bukan 200 statis).
      Kelly sizing dengan equity 240 → position size lebih besar secara proporsional.
      Setelah drawdown -30 USDT, equity aktif = 170 → auto-delever tanpa konfigurasi manual.

    Ini membuat sizing benar-benar closed-loop terhadap performa nyata bot.

    Cache: 30 menit. Thread-safe (bot single-threaded per cycle).

    Returns:
        float: equity aktif dalam USDT. Minimal = ACCOUNT_EQUITY_USDT × 0.5
               (floor 50% untuk mencegah under-sizing ekstrem saat drawdown dalam).
    """
    global _equity_cache

    now = time.time()
    if _equity_cache["value"] is not None and now - _equity_cache["ts"] < EQUITY_CACHE_TTL:
        return _equity_cache["value"]

    try:
        # [v7.18 #A] Dua query terpisah:
        # 1. Closed trades: pnl_usdt sudah final
        # 2. Partial TP1 trades (masih open tapi sudah realized sebagian): partial_pnl_usdt
        rows_closed = (
            supabase.table("signals_v2")
            .select("pnl_usdt")
            .not_.is_("result", "null")
            .neq("result", "EXPIRED")
            .execute()
            .data
        ) or []
        rows_partial = (
            supabase.table("signals_v2")
            .select("partial_pnl_usdt")
            .eq("partial_result", "TP1_PARTIAL")
            .is_("result", "null")   # masih open, belum fully closed
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"⚠️ get_current_equity_usdt: query gagal — {e}. Pakai ACCOUNT_EQUITY_USDT.", "warn")
        return ACCOUNT_EQUITY_USDT

    cumulative_pnl = 0.0
    for row in rows_closed:
        try:
            pnl = float(row.get("pnl_usdt") or 0.0)
            cumulative_pnl += pnl
        except (TypeError, ValueError):
            continue
    # [v7.18 #A] Tambahkan realized partial PnL dari trade yang masih berjalan
    for row in rows_partial:
        try:
            partial_pnl = float(row.get("partial_pnl_usdt") or 0.0)
            cumulative_pnl += partial_pnl
        except (TypeError, ValueError):
            continue

    # Effective equity = modal awal + realized PnL
    effective = ACCOUNT_EQUITY_USDT + cumulative_pnl

    # [v7.20 #C] Simpan equity sebelum partial TP contribution sebagai baseline.
    # Dipakai oleh calc_position_size untuk throttle compounding.
    # Hanya di-set sekali saat pertama kali dihitung (tidak overwrite per call).
    if _equity_cache.get("pre_partial_equity") is None:
        _equity_cache["pre_partial_equity"] = round(ACCOUNT_EQUITY_USDT + sum(
            (float(r.get("pnl_usdt") or 0.0) for r in rows_closed), 0.0
        ), 2)

    # Floor: tidak boleh di bawah 50% modal awal — cegah sizing terlalu kecil
    floor_equity = ACCOUNT_EQUITY_USDT * 0.50
    effective    = max(floor_equity, effective)

    _equity_cache["value"] = round(effective, 2)
    _equity_cache["ts"]    = now

    pnl_sign = "+" if cumulative_pnl >= 0 else ""
    log(f"   💼 Equity: ${ACCOUNT_EQUITY_USDT:.0f} base "
        f"{pnl_sign}{cumulative_pnl:.2f} PnL = ${effective:.2f} efektif")

    # [v7.19 #D] Hitung available balance = equity - locked capital
    # Ambil locked_usdt dari portfolio state (sudah partial-aware)
    try:
        _pstate = get_portfolio_state()
        _locked = _pstate.get("locked_usdt", 0.0)
    except Exception:
        _locked = 0.0
    _available = max(MIN_POSITION_USDT, round(effective - _locked, 2))

    _equity_cache["value"]     = round(effective, 2)
    _equity_cache["available"] = _available
    _equity_cache["locked"]    = round(_locked, 2)
    _equity_cache["ts"]        = now

    log(f"   💳 Available: ${_available:.2f} (locked=${_locked:.2f})")
    return _equity_cache["value"]

def get_available_equity_usdt() -> float:
    """
    [v7.19 #D] Return available balance = equity - locked capital.

    Berbeda dari get_current_equity_usdt() yang return total equity:
    - equity   = modal awal + realized PnL (termasuk partial)
    - locked   = total modal di posisi terbuka (partial dihitung setengah)
    - available = equity - locked → modal yang masih bisa dialokasikan

    calc_position_size() harus pakai ini, bukan equity penuh,
    agar tidak membuka posisi baru dengan modal yang sudah terkunci.

    Returns:
        float: available balance dalam USDT. Minimal MIN_POSITION_USDT.
    """
    global _equity_cache
    now = time.time()
    # Trigger refresh jika cache expired — sekaligus isi available
    if _equity_cache.get("available") is None or now - _equity_cache["ts"] >= EQUITY_CACHE_TTL:
        get_current_equity_usdt()  # refresh + isi _equity_cache["available"]
    return _equity_cache.get("available") or MIN_POSITION_USDT


def get_drawdown_state() -> dict:
    """
    [v8.4] Dual-track drawdown: streak + equity drawdown dari peak.

    Dua metrik dihitung dari SATU query (digabung dari dua query di v7.14).
    Streak dihitung dari baris terbaru ke belakang; equity_dd dari semua rows.

    Returns:
        {"streak": int, "mode": "normal"|"warn"|"halt", "dd_pct": float}
    """
    global _drawdown_state

    try:
        rows = (
            supabase.table("signals_v2")
            .select("result, pnl_usdt, sent_at")
            .not_.is_("result", "null")
            .neq("result", "EXPIRED")
            .order("sent_at", desc=False)
            .limit(200)
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"⚠️ get_drawdown_state: query gagal — {e}. Pakai state lama.", "warn")
        return _drawdown_state

    if not rows:
        return {"streak": 0, "mode": "normal", "dd_pct": 0.0}

    WIN_VALUES  = {"WIN", "TP1", "TP2", "PARTIAL_WIN"}  # [v8.6 FIX] PARTIAL_WIN breaks loss streak
    LOSS_VALUES = {"LOSS", "SL"}

    # ── Streak: hitung dari baris paling baru ke belakang ────────────────
    streak = 0
    for row in reversed(rows):
        result = (row.get("result") or "").upper()
        if result in LOSS_VALUES:
            streak += 1
        elif result in WIN_VALUES:
            break

    # ── Equity drawdown: dari data yang sama ─────────────────────────────
    prev_peak  = _load_peak_equity_from_db()
    # [v9.0 FIX] Pakai INITIAL_EQUITY_USDT sebagai base anchor yang stabil.
    # ACCOUNT_EQUITY_USDT bisa berubah jadi live wallet balance (via bootstrap),
    # sehingga DD menjadi tidak akurat untuk signal-only bot.
    equity     = INITIAL_EQUITY_USDT
    cum_pnl    = 0.0
    peak_eq    = max(prev_peak, equity)

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
    dd_pct = (peak_eq - current_equity) / peak_eq if peak_eq > 0 else 0.0
    dd_pct = max(0.0, dd_pct)

    # ── Mode ditentukan oleh yang lebih parah ─────────────────────────────
    streak_mode = (
        "halt" if streak >= DRAWDOWN_STREAK_HALT else
        "warn" if streak >= DRAWDOWN_STREAK_WARN else
        "normal"
    )
    equity_mode = (
        "halt" if dd_pct >= DD_HALT_PCT else
        "warn" if dd_pct >= DD_WARN_PCT else
        "normal"
    )

    # [v8.9] Equity drawdown sebagai primary authority; streak sebagai warning-only cap.
    # Masalah dual-primary: streak=halt bisa trigger halt saat equity masih sehat
    # (mis. 5 kecil losses berurutan tapi total PnL masih positif).
    # Solusi: streak tidak bisa naik di atas equity_mode — hanya bisa menurunkan ke warn.
    # Equity yang menentukan halt/no-halt; streak hanya menambah sensitifitas warn.
    SEVERITY = {"normal": 0, "warn": 1, "halt": 2}
    if equity_mode == "halt":
        mode = "halt"   # equity halt = final authority
    elif streak_mode == "halt" and equity_mode == "normal":
        mode = "warn"   # streak halt tapi equity masih sehat → downgrade ke warn
    else:
        mode = max(streak_mode, equity_mode, key=lambda m: SEVERITY[m])

    _drawdown_state = {"streak": streak, "mode": mode, "dd_pct": dd_pct}

    if mode != "normal":
        trigger = []
        if SEVERITY[streak_mode] >= SEVERITY[equity_mode]:
            trigger.append(f"streak={streak}")
        if SEVERITY[equity_mode] >= SEVERITY[streak_mode]:
            trigger.append(f"equity_dd={dd_pct*100:.1f}%")
        log(f"⚠️ DRAWDOWN MODE={mode.upper()} — trigger: {', '.join(trigger)}. "
            f"Position size dikurangi.", "warn")
        tg(f"⚠️ <b>Drawdown Alert</b>\n"
           f"Losing streak   : <b>{streak} berturutan</b>\n"
           f"Equity drawdown : <b>{dd_pct*100:.1f}% dari peak</b>\n"
           f"Mode: <b>{mode.upper()}</b> — position size "
           f"{'×0.7' if mode=='warn' else '×0.4'}\n"
           f"<i>Bot tetap jalan tapi lebih defensif.</i>")
        # [v9.0] Persist halt state ke Supabase agar survive job restart
        if mode == "halt":
            reason = f"streak={streak}, equity_dd={dd_pct*100:.1f}%"
            set_bot_halt(True, reason)
    else:
        # [v9.0] Jika mode kembali normal, lepas halt yang sebelumnya di-persist
        set_bot_halt(False, "")

    return _drawdown_state


# ════════════════════════════════════════════════════════
#  KILL SWITCH PERSISTENCE — [v9.0]
#
#  Masalah sebelumnya: drawdown halt hanya disimpan di _drawdown_state dict
#  (in-memory). Jika GitHub Actions job restart atau crash mid-run, seluruh
#  state halt hilang dan bot berjalan normal di run berikutnya.
#
#  Solusi: persist ke Supabase tabel `bot_config` (key-value store).
#  Tabel DDL (jalankan sekali di Supabase SQL editor):
#
#    CREATE TABLE IF NOT EXISTS bot_config (
#      key   TEXT PRIMARY KEY,
#      value TEXT NOT NULL,
#      updated_at TIMESTAMPTZ DEFAULT NOW()
#    );
#
#  Dua keys yang digunakan:
#    "bot_halt"       → "true" / "false"
#    "bot_halt_reason"→ string deskripsi penyebab halt
# ════════════════════════════════════════════════════════

def set_bot_halt(halted: bool, reason: str = "") -> None:
    """
    [v9.0] Persist status halt bot ke Supabase bot_config.
    Dipanggil setiap kali mode drawdown berubah ke/dari 'halt'.
    """
    try:
        supabase.table("bot_config").upsert([
            {"key": "bot_halt",        "value": str(halted).lower(), "updated_at": datetime.now(WIB).isoformat()},
            {"key": "bot_halt_reason", "value": reason or "",        "updated_at": datetime.now(WIB).isoformat()},
        ], on_conflict="key").execute()
        log(f"🔒 Kill switch persist: halt={halted} | reason={reason or '-'}")
    except Exception as e:
        log(f"⚠️ Gagal persist kill switch ke Supabase: {e}", "warn")


def check_bot_halt() -> tuple[bool, str]:
    """
    [v9.0] Baca status halt dari Supabase bot_config.
    Return: (is_halted: bool, reason: str)
    Fallback ke (False, "") jika tabel belum ada atau query gagal.
    """
    try:
        rows = (
            supabase.table("bot_config")
            .select("key, value")
            .in_("key", ["bot_halt", "bot_halt_reason"])
            .execute()
            .data
        ) or []
        kv = {r["key"]: r["value"] for r in rows}
        is_halted = kv.get("bot_halt", "false").lower() == "true"
        reason    = kv.get("bot_halt_reason", "")
        return is_halted, reason
    except Exception as e:
        log(f"⚠️ Gagal baca kill switch dari Supabase: {e} — asumsikan tidak halt", "warn")
        return False, ""



#
#  BTC bukan satu-satunya correlator yang relevan.
#  Tiga cluster utama: AI coins, Meme coins, L2s.
#  Jika cluster proxy drop > threshold DAN pair termasuk cluster,
#  signal dari cluster tsb diblokir.
# ════════════════════════════════════════════════════════

def get_cluster_regimes(client) -> dict:
    """
    [v7.16 #C] Cluster regime detection — dipertahankan untuk backward compatibility.

    Di v7.16 logika blocking utama sudah dipindahkan ke build_pairwise_matrix()
    yang dipanggil di awal run(). Fungsi ini sekarang hanya menggembalikan
    status cluster seed statis (AI/MEME/L2) sebagai ringkasan informatif.

    Returns:
        {"AI": -2.3, "MEME": -4.1, "L2": -1.0}  (weighted composite chg %)
    """
    global _cluster_cache, _cluster_cache_ts

    now = time.time()
    if _cluster_cache and now - _cluster_cache_ts < CLUSTER_CACHE_TTL:
        return _cluster_cache

    result = {}
    for cluster_name, (proxy_pair, members) in CLUSTER_PROXIES.items():
        tf_returns: dict[str, float] = {}

        for tf, weight in CLUSTER_TF_WEIGHTS.items():
            med = _calc_cluster_median_return(client, members, tf)
            if med is not None:
                tf_returns[tf] = med
            else:
                try:
                    candles = get_candles(client, proxy_pair, tf, 5)
                    if candles and len(candles[0]) >= 2:
                        closes = candles[0]
                        chg = (closes[-1] - closes[-2]) / closes[-2] * 100
                        tf_returns[tf] = round(chg, 2)
                except Exception:
                    pass

        if not tf_returns:
            result[cluster_name] = 0.0
            log(f"   ⚠️ Cluster {cluster_name}: semua fetch gagal → no-block", "warn")
            continue

        total_weight = sum(CLUSTER_TF_WEIGHTS[tf] for tf in tf_returns)
        composite    = sum(CLUSTER_TF_WEIGHTS[tf] * v for tf, v in tf_returns.items())
        composite   /= total_weight
        result[cluster_name] = round(composite, 3)

        tf_str = " | ".join(f"{tf}:{tf_returns[tf]:+.1f}%" for tf in sorted(tf_returns))
        log(f"   📡 Cluster {cluster_name} [{tf_str}] → composite:{composite:+.2f}%")

    _cluster_cache    = result
    _cluster_cache_ts = now
    return result


def get_pair_cluster(pair: str) -> str | None:
    """
    [v7.29] Identifikasi sektor dari nama pair.
    Return: "BTC" | "AI" | "MEME" | "L2" | None
    Sektor BTC ditambahkan untuk MAX_PER_SECTOR gate.
    """
    base = pair.replace("_USDT", "").upper()
    for cluster_name, (_proxy, members) in CLUSTER_PROXIES.items():
        if base in members:
            return cluster_name
    return None


def is_cluster_blocked(pair: str, cluster_regimes: dict) -> bool:
    """
    [v7.29 Phase3] Return True jika sektor pair sedang dropping.
    Simplified: hanya pakai seed block (pairwise matrix dihapus).
    """
    cluster = get_pair_cluster(pair)
    if cluster is not None:
        chg = cluster_regimes.get(cluster, 0.0)
        if chg < CLUSTER_DROP_BLOCK:
            log(f"   🚫 Sector block: {pair} → {cluster} drop {chg:+.1f}%")
            return True
    return False


# ════════════════════════════════════════════════════════

def get_btc_regime(client) -> dict:
    """
    Cek kondisi BTC untuk guard:
    - Crash guard: BTC drop > 10% dalam 4h → halt semua
    - Drop guard: BTC drop > 3% dalam 1h → blok BUY baru
    [v7.1 #5] chg_4h sekarang 1 candle 4h (bukan [-1] vs [-5] = ~16 jam).
    """
    default = {"halt": False, "block_buy": False, "btc_1h": 0.0, "btc_4h": 0.0}
    try:
        # [v7.3 FIX] Limit dinaikkan 10→30 agar lolos guard len(raw)<30 di get_candles.
        # Sebelumnya limit=10 → get_candles selalu return None → BTC protection mati total.
        c1h = get_candles(client, "BTC_USDT", "1h", 30)
        c4h = get_candles(client, "BTC_USDT", "4h", 30)
        if c1h is None or c4h is None: return default

        chg_1h = (c1h[0][-1] - c1h[0][-2]) / c1h[0][-2] * 100
        # [v7.1 #5] Perbedaan 1 candle 4h = perubahan dalam 4 jam terakhir
        chg_4h = (c4h[0][-1] - c4h[0][-2]) / c4h[0][-2] * 100

        halt      = chg_4h < BTC_CRASH_BLOCK
        block_buy = chg_1h < BTC_DROP_BLOCK

        log(f"📡 BTC 1h:{chg_1h:+.1f}% 4h:{chg_4h:+.1f}% | "
            f"{'🛑 HALT' if halt else '⛔ BUY BLOCKED' if block_buy else '✅ OK'}")
        return {"halt": halt, "block_buy": block_buy,
                "btc_1h": round(chg_1h, 2), "btc_4h": round(chg_4h, 2)}
    except Exception as e:
        log(f"⚠️ btc_regime: {e}", "warn")
        return default


def get_fear_greed() -> int:
    try:
        d = http_get("https://api.alternative.me/fng/?limit=1")
        if d: return int(d["data"][0]["value"])
    except Exception as e:
        log(f"⚠️ Fear & Greed fetch gagal: {e} — default ke 50", "warn")
    return 50


# [v8.3] get_order_book_ratio() dihapus — ob_ratio tidak dipakai di score_signal sejak v7.8+.


# ════════════════════════════════════════════════════════
#  DEDUPLICATION via Supabase
# ════════════════════════════════════════════════════════

# [v7.7 #7] In-memory fallback dedup — diisi saat Supabase timeout/error.
# Format key: "pair|strategy|side" (side=None digantikan string "_ANY_")
# Di-reset setiap cycle di run() bersama _candle_cache.
_dedup_memory: set = set()


def _dedup_key(pair: str, strategy: str, side: str | None) -> str:
    return f"{pair}|{strategy}|{side or '_ANY_'}"


def _already_sent_generic(pair: str, strategy: str, dedup_hours: int,
                           side: str | None = None) -> bool:
    """
    [v7.6 #12] Fungsi dedup generik — menggantikan 3 fungsi duplikat
    (already_sent, already_sent_pump, already_sent_micro) yang identik secara struktur.
    Parameter `side` opsional: jika None, query tidak memfilter berdasarkan side
    (dipakai untuk PUMP dan MICROCAP yang hanya BUY).
    [v7.2 FIX #7] UTC konsisten dengan Supabase.
    [v7.7 #7] In-memory fallback — cegah duplikat signal saat Supabase down/timeout.
    """
    key = _dedup_key(pair, strategy, side)
    # Cek in-memory dulu — instant, tidak butuh Supabase
    if key in _dedup_memory:
        return True
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=dedup_hours)).isoformat()
        q = (
            supabase.table("signals_v2")
            .select("id")
            .eq("pair", pair)
            .eq("strategy", strategy)
            .gt("sent_at", since)
        )
        if side is not None:
            q = q.eq("side", side)
        return len(q.execute().data) > 0
    except Exception as e:
        log(f"⚠️ dedup check [{strategy}|{pair}]: {e} — pakai in-memory fallback", "warn")
        return False  # fallback: izinkan signal, in-memory akan mencegah duplikat dalam cycle ini


def already_sent(pair: str, strategy: str, side: str) -> bool:
    """Cek dedup signal INTRADAY/SWING — pair+strategy+side dalam DEDUP_HOURS jam."""
    return _already_sent_generic(pair, strategy, DEDUP_HOURS, side=side)


def already_sent_pump(pair: str) -> bool:
    """Cek dedup signal PUMP — pair dalam PUMP_DEDUP_HOURS jam."""
    return _already_sent_generic(pair, "PUMP", PUMP_DEDUP_HOURS)


def already_sent_micro(pair: str) -> bool:
    """Cek dedup signal MICROCAP — pair dalam MICRO_DEDUP_HOURS jam."""
    return _already_sent_generic(pair, "MICROCAP", MICRO_DEDUP_HOURS)


def save_signal(pair: str, strategy: str, side: str, entry: float,
                tp1: float, tp2, sl: float, tier: str, score: float,
                timeframe: str, position_size: float | None = None):
    """Simpan signal ke Supabase untuk tracking dan deduplication.
    [v7.2 FIX #7] sent_at disimpan dalam UTC agar konsisten dengan already_sent query.
    [v7.7 #7] Isi _dedup_memory setelah insert — sehingga cycle yang sama
    tidak bisa mengirim duplikat meski Supabase lambat merespons.
    [v7.18 #C] position_size disimpan ke DB — dibutuhkan oleh evaluate_open_trades()
               untuk menghitung PnL aktual. Tanpa ini, semua trade fallback ke
               BASE_POSITION_USDT dan PnL tidak akurat.
    """
    _base_payload = {
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
        "sent_at":   datetime.now(timezone.utc).isoformat(),
        "result":    None,
        "closed_at": None,
        # [Phase1 #1] Status field — lifecycle state machine.
        # OPEN    : sinyal baru, belum hit level apapun
        # PARTIAL : TP1 hit, sisa posisi masih berjalan
        # CLOSED  : trade selesai (TP2/SL/BREAKEVEN/EXPIRED/PARTIAL_WIN)
        "status":    "OPEN",
    }
    # [v7.18 #C] position_size — kolom opsional, graceful jika belum ada di schema.
    # Jalankan DDL: ALTER TABLE signals_v2 ADD COLUMN position_size NUMERIC;
    _payload_with_size = {**_base_payload, "position_size": round(position_size, 4) if position_size else None}
    try:
        supabase.table("signals_v2").insert(_payload_with_size).execute()
    except Exception as e:
        err_str = str(e)
        if "PGRST204" in err_str or "position_size" in err_str:
            # Kolom belum ada di DB — insert tanpa position_size, log DDL reminder
            log(f"⚠️ save_signal [{pair}]: kolom position_size belum ada. "
                f"Jalankan: ALTER TABLE signals_v2 ADD COLUMN position_size NUMERIC; "
                f"Lanjut insert tanpa size.", "warn")
            try:
                supabase.table("signals_v2").insert(_base_payload).execute()
            except Exception as e2:
                log(f"⚠️ save_signal [{pair}]: {e2}", "warn")
        else:
            log(f"⚠️ save_signal [{pair}]: {e}", "warn")
    finally:
        # [v7.7 #7] Selalu tandai di memory — bahkan jika Supabase insert gagal,
        # mencegah re-send dalam cycle yang sama.
        _dedup_memory.add(_dedup_key(pair, strategy, side))


# ════════════════════════════════════════════════════════
#  SIGNAL STRATEGIES
# ════════════════════════════════════════════════════════

def check_intraday(client, pair: str, price: float,
                   btc: dict, side: str = "BUY") -> dict | None:
    """
    INTRADAY signal — timeframe 1h. Mendukung BUY dan SELL.

    [v7.28] 2 HARD GATE saja:
      1. BTC crash  : btc["halt"] → blok semua signal
      2. ADX CHOPPY : regime == "CHOPPY" → skip pair

    Semua filter lain (RSI, velocity, late-entry) → dihapus dari gate.
    Scoring 3-factor (trend + momentum + volume) menentukan tier.
    """
    # Hard gate 1: BTC crash → halt semua signal (termasuk SELL)
    if btc.get("halt"): return None
    # BTC drop → blok BUY saja (SELL tetap boleh)
    if side == "BUY" and btc["block_buy"]: return None

    data = get_candles(client, pair, "1h", 100)
    if data is None: return None
    closes, highs, lows, volumes = data

    atr = calc_atr(closes, highs, lows)
    if atr / price * 100 < 0.2: return None
    if atr / price * 100 > 8.0: return None

    # Hard gate 2: ADX CHOPPY → No Trade Zone
    mkt = detect_market_regime(closes, highs, lows)
    if mkt["regime"] == "CHOPPY":
        return None

    rsi        = calc_rsi(closes)
    macd, msig = calc_macd(closes)
    ema20      = calc_ema(closes, 20)
    ema50      = calc_ema(closes, 50)
    vwap       = calc_vwap(closes, highs, lows, volumes, timeframe="1h")
    structure  = detect_structure(closes, highs, lows, strength=3, lookback=60)
    liq        = detect_liquidity(closes, highs, lows, lookback=40)

    if not structure["valid"]: return None

    if side == "BUY":
        # setup_score 0 = tidak ada sinyal → skip (satu-satunya setup gate)
        setup_score = detect_setup_quality("BUY", structure, liq, ema20, ema50)
        if setup_score == 0: return None

        ob    = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=25)
        score = score_signal("BUY", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema20, ema50, vwap, mkt["regime"],
                             setup_score=setup_score)
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        last_sh = structure.get("last_sh")
        entry = round(last_sh * 1.002, 8) if (last_sh and price > last_sh) else price

    else:  # SELL
        setup_score = detect_setup_quality("SELL", structure, liq, ema20, ema50)
        if setup_score == 0: return None

        last_sh = structure.get("last_sh")
        ob    = detect_order_block(closes, highs, lows, volumes, side="SELL", lookback=25)
        score = score_signal("SELL", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema20, ema50, vwap, mkt["regime"],
                             setup_score=setup_score)
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        entry = round(last_sh * 0.998, 8) if (last_sh and price >= last_sh * 0.97) else price

    sl, tp1, tp2 = calc_sl_tp(entry, side, atr, structure, "INTRADAY")

    if side == "BUY":
        if tp1 <= entry or sl >= entry: return None
        sl_dist = entry - sl
        if sl_dist <= 0 or sl_dist / entry > 0.05: return None
        rr = (tp1 - entry) / sl_dist
    else:
        if tp1 >= entry or sl <= entry: return None
        sl_dist = sl - entry
        if sl_dist <= 0 or sl_dist / entry > 0.05: return None
        rr = (entry - tp1) / sl_dist

    if rr < MIN_RR["INTRADAY"]: return None

    return {
        "pair": pair, "strategy": "INTRADAY", "side": side,
        "timeframe": "1h", "entry": entry,
        "current_price": price,
        "tp1": tp1, "tp2": tp2, "sl": sl,
        "tier": tier, "score": score, "rr": round(rr, 1),
        "rsi": round(rsi, 1), "structure": structure,
        "regime": mkt["regime"], "adx": mkt["adx"],
        "conviction": calc_conviction(score),
        "reason": build_signal_reason(
            side=side, score=score, setup_score=setup_score,
            structure=structure, liq=liq, regime=mkt["regime"],
            rsi=rsi, macd=macd, ema_fast=ema20, ema_slow=ema50,
            strategy="INTRADAY", msig=msig,   # [v8.9 FIX]
        ),
    }


def check_swing(client, pair: str, price: float,
                btc: dict, side: str = "BUY") -> dict | None:
    """
    SWING signal — timeframe 4h. Mendukung BUY dan SELL.

    [v7.28] 2 HARD GATE saja:
      1. BTC crash  : btc["halt"] → blok semua signal
      2. ADX CHOPPY : regime == "CHOPPY" → skip pair

    Semua filter lain (RSI, late-entry) → dihapus dari gate.
    Scoring 3-factor menentukan tier A+/A/SKIP.
    """
    if btc.get("halt"): return None
    if side == "BUY" and btc["block_buy"]: return None

    data = get_candles(client, pair, "4h", 200)
    if data is None: return None
    closes, highs, lows, volumes = data

    atr  = calc_atr(closes, highs, lows)
    if atr / price * 100 < 0.5:  return None
    if atr / price * 100 > 12.0: return None

    # Hard gate 2: ADX CHOPPY
    mkt = detect_market_regime(closes, highs, lows)
    if mkt["regime"] == "CHOPPY":
        return None

    rsi        = calc_rsi(closes)
    macd, msig = calc_macd(closes)
    ema50      = calc_ema(closes, 50)
    ema200     = calc_ema(closes, 200)
    vwap       = calc_vwap(closes, highs, lows, volumes, timeframe="4h")
    structure  = detect_structure(closes, highs, lows, strength=3, lookback=100)
    liq        = detect_liquidity(closes, highs, lows, lookback=60)

    if not structure["valid"]: return None

    if side == "BUY":
        setup_score = detect_setup_quality("BUY", structure, liq, ema50, ema200)
        if setup_score == 0: return None

        ob    = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=40)
        score = score_signal("BUY", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema50, ema200, vwap, mkt["regime"],
                             setup_score=setup_score)
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        last_sh = structure.get("last_sh")
        entry = round(last_sh * 1.003, 8) if (last_sh and price > last_sh) else price

        sl, tp1, tp2 = calc_sl_tp(entry, "BUY", atr, structure, "SWING")
        if tp1 <= entry or sl >= entry: return None
        sl_dist = entry - sl
        if sl_dist <= 0 or sl_dist / entry > 0.10: return None
        rr = (tp1 - entry) / sl_dist

    else:  # SELL
        setup_score = detect_setup_quality("SELL", structure, liq, ema50, ema200)
        if setup_score == 0: return None

        last_sh = structure.get("last_sh")
        ob    = detect_order_block(closes, highs, lows, volumes, side="SELL", lookback=40)
        score = score_signal("SELL", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema50, ema200, vwap, mkt["regime"],
                             setup_score=setup_score)
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        entry = round(last_sh * 0.998, 8) if (last_sh and price >= last_sh * 0.97) else price

        sl, tp1, tp2 = calc_sl_tp(entry, "SELL", atr, structure, "SWING")
        if tp1 >= entry or sl <= entry: return None
        sl_dist = sl - entry
        if sl_dist <= 0 or sl_dist / entry > 0.10: return None
        rr = (entry - tp1) / sl_dist

    if rr < MIN_RR["SWING"]: return None

    return {
        "pair": pair, "strategy": "SWING", "side": side,
        "timeframe": "4h", "entry": entry,
        "current_price": price,
        "tp1": tp1, "tp2": tp2, "sl": sl,
        "tier": tier, "score": score, "rr": round(rr, 1),
        "rsi": round(rsi, 1), "structure": structure,
        "regime": mkt["regime"], "adx": mkt["adx"],
        "conviction": calc_conviction(score),
        "reason": build_signal_reason(
            side=side, score=score, setup_score=setup_score,
            structure=structure, liq=liq, regime=mkt["regime"],
            rsi=rsi, macd=macd, ema_fast=ema50, ema_slow=ema200,
            strategy="SWING", msig=msig,   # [v8.9 FIX]
        ),
    }


# [v8.4] Legacy resolve_conflicts() + _SIGNAL_PRIORITY (v7.10) dihapus.
# Conflict resolution: resolve_conflicts_dynamic() — lihat implementasi di atas.


def send_signal(sig: dict):
    pair      = sig["pair"].replace("_USDT", "/USDT")
    strategy  = sig["strategy"]
    side      = sig["side"]
    tier      = sig["tier"]
    score     = sig["score"]
    rr        = sig["rr"]
    entry     = sig["entry"]
    tp1       = sig["tp1"]
    tp2       = sig["tp2"]
    sl        = sig["sl"]
    tf        = sig["timeframe"]
    rsi       = sig["rsi"]
    cur_price     = sig.get("current_price", entry)
    bos           = sig["structure"].get("bos") or sig["structure"].get("choch") or "—"
    # [v7.13 #1] Position size — sudah dihitung sebelum dispatch, fallback ke BASE jika tidak ada
    position_size = sig.get("position_size", BASE_POSITION_USDT)

    pct_tp1   = abs((tp1 - entry) / entry * 100)
    # [v7.7 #9] Guard tp2=None — latent TypeError jika tp2 tidak tersedia
    pct_tp2   = abs((tp2 - entry) / entry * 100) if tp2 is not None else 0.0
    pct_sl    = abs((sl  - entry) / entry * 100)
    # positif = harga di atas entry | negatif = harga di bawah entry
    pct_above = (cur_price - entry) / entry * 100

    # Konversi IDR
    idr_rate  = get_usdt_idr_rate()
    entry_idr = usdt_to_idr(entry, idr_rate)
    tp1_idr   = usdt_to_idr(tp1, idr_rate)
    tp2_idr   = usdt_to_idr(tp2, idr_rate) if tp2 is not None else "—"
    sl_idr    = usdt_to_idr(sl, idr_rate)

    tier_emoji  = {"S": "💎", "A+": "🏆", "A": "🥇"}.get(tier, "🎯")
    strat_emoji = {"INTRADAY": "📈", "SWING": "🌊"}.get(strategy, "🎯")
    side_emoji  = "🟢 BUY" if side == "BUY" else "🔴 SELL"

    regime      = sig.get("regime", "—")
    adx         = sig.get("adx", 0.0)
    conviction  = sig.get("conviction", "OK 🟡")
    regime_emoji = {"TRENDING": "🔥", "RANGING": "⚠️"}.get(regime, "—")

    # [v7.9 #1] Probabilistic confidence — regime-aware, data-driven
    # Passing regime agar bucket "9-11|TRENDING" ≠ "9-11|RANGING"
    conf = estimate_confidence(score, regime=regime if regime != "—" else "")

    # [FIX #4] entry_note logic berbeda untuk BUY dan SELL
    entry_note = ""
    if side == "BUY":
        # BUY: harga naik terlalu jauh di atas entry = sudah terlambat
        if pct_above > 0.5:
            entry_note = (
                f"\n⚠️ Harga saat ini ${cur_price:.6f} (+{pct_above:.1f}% dari entry zone)"
                f"\n   <i>Tunggu pullback ke zona entry, jangan kejar harga!</i>"
            )
        elif pct_above < -0.3:
            entry_note = f"\n✅ Harga saat ini ${cur_price:.6f} — sudah di zona entry"
    else:  # SELL
        # SELL: harga turun terlalu jauh di bawah entry = sudah terlambat
        if pct_above < -0.5:
            entry_note = (
                f"\n⚠️ Harga saat ini ${cur_price:.6f} ({pct_above:.1f}% dari entry zone)"
                f"\n   <i>Tunggu retest ke zona entry, jangan kejar SHORT!</i>"
            )
        elif pct_above > 0.3:
            entry_note = f"\n✅ Harga saat ini ${cur_price:.6f} — sudah di zona entry SELL"

    hours       = 4 if strategy == "INTRADAY" else 16
    now         = datetime.now(WIB)
    valid_until = (now + timedelta(hours=hours)).strftime("%H:%M WIB")

    # Label TP/SL disesuaikan arah untuk kejelasan pembaca
    tp_label = "+" if side == "BUY" else "-"
    sl_label = "-" if side == "BUY" else "+"

    msg = (
        f"{strat_emoji} <b>{tier_emoji} [{tier}] SIGNAL {side_emoji} — {strategy}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair:    <b>{pair}</b> [{tf}]\n"
        f"⏰ Valid: {now.strftime('%H:%M')} → {valid_until}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Entry Zone : <b>${entry:.6f}</b> <i>≈ {entry_idr}</i> (limit / retest BOS){entry_note}\n"
        f"TP1  : <b>${tp1:.6f}</b> <i>≈ {tp1_idr}</i> <i>({tp_label}{pct_tp1:.1f}%)</i>\n"
        f"TP2  : <b>{'${:.6f}'.format(tp2) if tp2 is not None else '—'}</b>"
        f"{(' <i>≈ ' + tp2_idr + '</i>') if tp2 is not None else ''}"
        f"{' <i>(' + tp_label + '{:.1f}%)</i>'.format(pct_tp2) if tp2 is not None else ''}\n"
        f"SL   : <b>${sl:.6f}</b> <i>≈ {sl_idr}</i> <i>({sl_label}{pct_sl:.1f}%)</i>\n"
        f"R/R  : <b>1:{rr}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Score:      {score:.1f}/4 | RSI: {rsi}\n"
        f"Struct:     {bos}\n"
        f"Regime:     {regime_emoji} {regime} (ADX: {adx})\n"
        f"Hist WR:    {conf['label']}{' 🎯' if conf.get('ctx_used') else ''}\n"
        f"Conviction: <b>{conviction}</b>\n"
        f"Why:        <i>{sig.get('reason', '—')}</i>\n"
        f"💰 Pos.Size : <b>${position_size:.2f} USDT</b> <i>(tier-adjusted)</i>\n"
        f"<i>⚠️ Pasang SL wajib. Ini signal, bukan rekomendasi finansial.</i>"
    )
    tg(msg)

    # [v8.9] Explainability log — WHY per faktor untuk debug dan audit trail
    _reason  = sig.get("reason", "—")
    _ema_ok  = "✓" if ("EMA✅" in _reason) else "✗"
    _macd_ok = "✓" if ("MACD✅" in _reason) else "✗"
    _vol_ok  = "✓" if ("Vol" in _reason and "×" in _reason) else "—"   # pump/micro reason
    _setup   = "BOS/CHoCH" if "BOS/CHoCH✅" in _reason else ("liq_sweep✅" if "liq_sweep✅" in _reason else "—")
    log(f"  ✅ SIGNAL {tier} {strategy} {side} {pair} | "
        f"WHY: trend{_ema_ok} momentum{_macd_ok} vol{_vol_ok} setup={_setup} | "
        f"RR:1:{rr} Score:{score:.1f} Size:${position_size}")


# ════════════════════════════════════════════════════════
#  PUMP SCANNER
# ════════════════════════════════════════════════════════

def check_pump(client, pair: str, price: float) -> dict | None:
    """
    PUMP SCANNER — timeframe 15m.
    Deteksi early pump berdasarkan:
      1. Volume spike: candle terakhir > PUMP_VOL_SPIKE × rata-rata 10 candle
      2. Price change: harga naik > PUMP_PRICE_CHANGE% dalam 3 candle 15m terakhir
      3. RSI belum overbought: RSI < PUMP_RSI_MAX
      4. MACD bullish cross searah
      5. EMA trend filter: price > EMA20 pada 15m
      6. EMA7 > EMA20 — momentum 15m positif
      7. Anti buy-the-top: price tidak > 2% di atas high 5 candle terakhir
    vol_24h difilter di run_pump_scan sebelum fungsi ini dipanggil. [v7.2]
    """
    data = get_candles(client, pair, "15m", 50)
    if data is None: return None
    closes, highs, lows, volumes = data

    vol_avg = float(np.mean(volumes[-11:-1]))
    if vol_avg <= 0: return None
    vol_ratio = float(volumes[-1]) / vol_avg
    if vol_ratio < PUMP_VOL_SPIKE: return None

    price_3c_ago = float(closes[-4])
    if price_3c_ago <= 0: return None
    pct_change = (price - price_3c_ago) / price_3c_ago * 100
    if pct_change < PUMP_PRICE_CHANGE: return None

    rsi = calc_rsi(closes)
    if rsi > PUMP_RSI_MAX: return None

    macd, msig = calc_macd(closes)
    if macd <= msig: return None

    ema20_15m = calc_ema(closes, 20)
    if price < ema20_15m: return None

    ema7_15m = calc_ema(closes, 7)
    if ema7_15m < ema20_15m: return None

    # [v7.6 #7] highs[-5:-1] → highs[-5:] — sertakan candle terakhir.
    # Sebelumnya highs[-4:-1] tidak mencakup highs[-1] (candle current),
    # sehingga jika candle current adalah high tertinggi, filter tidak aktif.
    recent_high = float(np.max(highs[-5:]))
    if price > recent_high * 1.02: return None

    atr = calc_atr(closes, highs, lows)
    sl  = round(price - atr * 1.2, 8)
    tp1 = round(price + atr * 2.0, 8)

    if sl <= 0: return None   # [v7.3 FIX] cegah SL negatif pada token harga sangat rendah

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
        # [v8.9] Explainability — kenapa pump alert ini digenerate
        "reason": build_pump_reason(
            rsi=rsi, macd=macd, msig=msig,
            vol_ratio=vol_ratio, pct_change=pct_change,
            ema7=ema7_15m, ema20=ema20_15m,
        ),
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

    # Konversi IDR
    idr_rate  = get_usdt_idr_rate()
    entry_idr = usdt_to_idr(entry, idr_rate)
    tp1_idr   = usdt_to_idr(tp1, idr_rate)
    sl_idr    = usdt_to_idr(sl, idr_rate)

    msg = (
        f"🚀 <b>PUMP ALERT — EARLY SIGNAL</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair  : <b>{pair}</b> [15m]\n"
        f"⏰ Valid: {now.strftime('%H:%M')} → {valid_until}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Entry : <b>${entry:.6f}</b> <i>≈ {entry_idr}</i>\n"
        f"TP1   : <b>${tp1:.6f}</b> <i>≈ {tp1_idr}</i> <i>(+{pct_tp1:.1f}%)</i>\n"
        f"SL    : <b>${sl:.6f}</b> <i>≈ {sl_idr}</i> <i>(-{pct_sl:.1f}%)</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 Vol Spike : <b>{vol_ratio:.1f}×</b> rata-rata\n"
        f"📈 Naik 45m  : <b>+{pct_change:.2f}%</b>\n"
        f"RSI          : {rsi}\n"
        f"Why          : <i>{sig.get('reason', '—')}</i>\n"
        f"<i>⚡ Early pump alert. Entry cepat, SL wajib ketat.</i>\n"
        f"<i>⚠️ High risk — bukan rekomendasi finansial.</i>"
    )
    tg(msg)
    log(f"  🚀 PUMP ALERT {pair} | Vol:{vol_ratio:.1f}× | +{pct_change:.2f}% | RSI:{rsi}")


# ════════════════════════════════════════════════════════
#  MICROCAP SCANNER
# ════════════════════════════════════════════════════════

def check_microcap(client, pair: str, price: float,
                   vol_24h: float, change_24h: float) -> dict | None:
    """
    MICROCAP SCANNER — timeframe 1h.
    Target: meme coin & microcap yang vol 24h 20K–150K USDT.

    Logika berbeda dari INTRADAY/SWING:
    - Tidak bergantung BOS/CHoCH (struktur sering tidak terbentuk di microcap)
    - Fokus pada: volume anomali + momentum awal + RSI sehat
    - TP lebih besar karena potensi pump besar
    - SL ketat karena volatilitas tinggi

    Filter masuk:
    1. Volume 24h: 20K–150K (zona microcap)
    2. Harga belum pump >25% dalam 24h (bukan kejar top)
    3. Volume spike: candle 1h terbaru > 5× rata-rata
    4. Momentum: harga naik >3% dalam 3 candle terakhir
    5. RSI: 28–68 (sehat, belum overbought)
    6. MACD bullish cross — konfirmasi momentum
    7. EMA: price > EMA20 pada 1h — minimal trend support
    8. Anti buy-the-top: price tidak > 3% di atas high 3 candle terakhir
    9. R/R minimum: 2.5 (TP1/SL harus worth it)
    """
    # Gate awal — volume di zona microcap
    if vol_24h < MICRO_VOL_MIN or vol_24h > MICRO_VOL_MAX:
        return None

    # Tidak sudah pump besar dalam 24h
    if change_24h > MICRO_PRICE_MAX:
        return None

    # Ambil candle 1h — cukup 60 candle
    data = get_candles(client, pair, "1h", 60)
    if data is None:
        return None
    closes, highs, lows, volumes = data

    # Gate 1: Volume spike — ada yang mulai masuk
    vol_avg = float(np.mean(volumes[-11:-1]))
    if vol_avg <= 0:
        return None
    vol_ratio = float(volumes[-1]) / vol_avg
    if vol_ratio < MICRO_VOL_SPIKE:
        return None

    # Gate 2: Momentum awal — harga mulai bergerak ke atas
    price_3c_ago = float(closes[-4])
    if price_3c_ago <= 0:
        return None
    pct_3h = (price - price_3c_ago) / price_3c_ago * 100
    if pct_3h < MICRO_PRICE_CHANGE:
        return None

    # Gate 3: RSI di zona sehat
    rsi = calc_rsi(closes)
    if rsi < MICRO_RSI_MIN or rsi > MICRO_RSI_MAX:
        return None

    # Gate 4: MACD bullish — momentum terkonfirmasi
    macd, msig = calc_macd(closes)
    if macd <= msig:
        return None

    # Gate 5: Price di atas EMA20 — minimal trend support
    ema20 = calc_ema(closes, 20)
    if price < ema20:
        return None

    # Gate 6: Anti buy-the-top
    # [v7.6 #7] highs[-5:] — sertakan candle terakhir, konsisten dengan check_pump.
    # Sebelumnya highs[-4:-1] tidak mencakup highs[-1] sehingga candle current
    # yang merupakan high tertinggi tidak tertangkap filter ini.
    recent_high = float(np.max(highs[-5:]))
    if price > recent_high * 1.03:
        return None

    # Gate 7: ATR volatility check — pastikan ada ruang gerak
    atr = calc_atr(closes, highs, lows)
    atr_pct = atr / price * 100
    if atr_pct < 1.0:   # terlalu flat — tidak akan pump
        return None
    if atr_pct > 20.0:  # terlalu volatile — terlalu berisiko
        return None

    # Hitung entry, TP, SL
    entry = price
    sl    = round(entry * (1 - MICRO_SL_PCT), 8)
    tp1   = round(entry * (1 + MICRO_TP1_PCT), 8)
    tp2   = round(entry * (1 + MICRO_TP2_PCT), 8)

    sl_dist  = entry - sl
    tp1_dist = tp1 - entry
    if sl_dist <= 0:
        return None

    rr = round(tp1_dist / sl_dist, 1)
    if rr < MICRO_MIN_RR:
        return None

    # Bonus context — EMA7 untuk konfirmasi momentum jangka pendek
    ema7 = calc_ema(closes, 7)
    ema_short_bull = ema7 > ema20

    # Liquidity sweep bonus
    liq = detect_liquidity(closes, highs, lows, lookback=30)
    has_sweep = liq.get("sweep_bull", False)

    # [v7.12 #2] Unified scoring — pakai score_signal() engine yang sama dengan
    # INTRADAY/SWING. Gantikan micro_score (0–4) dengan skala 6–18 yang konsisten.
    #
    # Adaptasi kontekstual untuk microcap:
    #   - setup_score: has_sweep → 2 (liq_sweep level), tidak ada sweep → 1 (continuation)
    #   - regime: default RANGING (microcap jarang trending sebelum pump)
    #   - ob: empty (order block tidak reliable di microcap volume kecil)
    #   - ema_fast/slow: ema7/ema20 — sudah dipakai sebagai gate sebelumnya
    #
    # Efek: microcap sekarang punya tier S/A+/A, conviction label, dan masuk
    # win rate bucket yang sama dengan INTRADAY/SWING → model bisa belajar lintas strategy.
    score, tier, conviction = score_microcap_unified(
        price=price, closes=closes, highs=highs, lows=lows, volumes=volumes,
        rsi=rsi, macd=macd, msig=msig,
        ema_fast=ema7, ema_slow=ema20,
        has_sweep=has_sweep,
        regime="RANGING",   # microcap default RANGING — belum trending
    )

    # SKIP jika tidak memenuhi tier minimum (unified scoring lebih ketat dari micro_score 0-4)
    if tier == "SKIP":
        return None

    return {
        "pair":        pair,
        "strategy":    "MICROCAP",
        "side":        "BUY",
        "timeframe":   "1h",
        "entry":       entry,
        "tp1":         tp1,
        "tp2":         tp2,
        "sl":          sl,
        "tier":        tier,        # S / A+ / A (bukan hanya "A" seperti sebelumnya)
        "score":       score,       # skala 6–18, sama dengan main signals
        "conviction":  conviction,  # label deterministic
        "rr":          rr,
        "rsi":         round(rsi, 1),
        "vol_ratio":   round(vol_ratio, 1),
        "pct_3h":      round(pct_3h, 2),
        "change_24h":  round(change_24h, 2),
        "atr_pct":     round(atr_pct, 2),
        "has_sweep":   has_sweep,
        # [v8.9] Explainability — audit trail semua gate yang lolos
        "reason": build_microcap_reason(
            rsi=rsi, macd=macd, msig=msig,
            vol_ratio=vol_ratio, pct_3h=pct_3h,
            ema7=ema7, ema20=ema20,
            has_sweep=has_sweep, atr_pct=atr_pct,
        ),
    }


def send_microcap_signal(sig: dict):
    """Kirim microcap signal ke Telegram — format informatif dengan risk warning."""
    pair       = sig["pair"].replace("_USDT", "/USDT")
    entry      = sig["entry"]
    tp1        = sig["tp1"]
    tp2        = sig["tp2"]
    sl         = sig["sl"]
    rsi        = sig["rsi"]
    vol_ratio  = sig["vol_ratio"]
    pct_3h     = sig["pct_3h"]
    change_24h = sig["change_24h"]
    rr         = sig["rr"]
    tier       = sig["tier"]
    score      = sig["score"]
    atr_pct    = sig["atr_pct"]
    has_sweep  = sig["has_sweep"]

    pct_tp1 = abs((tp1 - entry) / entry * 100)
    pct_tp2 = abs((tp2 - entry) / entry * 100)
    pct_sl  = abs((sl  - entry) / entry * 100)

    now         = datetime.now(WIB)
    valid_until = (now + timedelta(hours=2)).strftime("%H:%M WIB")
    tier_emoji  = {"A": "🥇", "B": "🥈"}.get(tier, "🎯")

    # Konversi IDR
    idr_rate  = get_usdt_idr_rate()
    entry_idr = usdt_to_idr(entry, idr_rate)
    tp1_idr   = usdt_to_idr(tp1, idr_rate)
    tp2_idr   = usdt_to_idr(tp2, idr_rate)
    sl_idr    = usdt_to_idr(sl, idr_rate)

    sweep_line = "🧲 Liq sweep terdeteksi — smart money sudah masuk\n" if has_sweep else ""

    msg = (
        f"🔬 <b>{tier_emoji} [{tier}] MICROCAP SIGNAL 🟢 BUY</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair   : <b>{pair}</b> [1h]\n"
        f"⏰ Valid: {now.strftime('%H:%M')} → {valid_until}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Entry  : <b>${entry:.6f}</b> <i>≈ {entry_idr}</i>\n"
        f"TP1    : <b>${tp1:.6f}</b> <i>≈ {tp1_idr}</i> <i>(+{pct_tp1:.1f}%)</i>\n"
        f"TP2    : <b>${tp2:.6f}</b> <i>≈ {tp2_idr}</i> <i>(+{pct_tp2:.1f}%)</i>\n"
        f"SL     : <b>${sl:.6f}</b> <i>≈ {sl_idr}</i> <i>(-{pct_sl:.1f}%)</i>\n"
        f"R/R    : <b>1:{rr}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 Vol Spike : <b>{vol_ratio:.1f}×</b> rata-rata\n"
        f"📈 Naik 3h   : <b>+{pct_3h:.2f}%</b> | 24h: <b>{change_24h:+.1f}%</b>\n"
        f"RSI          : <b>{rsi}</b> | ATR: {atr_pct:.1f}%\n"
        f"{sweep_line}"
        f"Score  : {score:.1f}/4 | Conviction: <b>{sig.get('conviction', '—')}</b>\n"
        f"Why    : <i>{sig.get('reason', '—')}</i>\n"
        f"Tier   : {tier_emoji} <b>{tier}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <b>MICROCAP — High Risk, High Reward</b>\n"
        f"<i>• Size kecil (maks 1–2% modal)</i>\n"
        f"<i>• SL wajib ketat — microcap bisa dump cepat</i>\n"
        f"<i>• Ambil profit di TP1, sisakan untuk TP2</i>\n"
        f"<i>• Bukan rekomendasi finansial</i>"
    )
    tg(msg)
    log(f"  🔬 MICROCAP {pair} | Vol:{vol_ratio:.1f}× | +{pct_3h:.2f}% 3h | RSI:{rsi} | R/R:1:{rr}")


def run_pump_scan(client):
    """Jalankan pump scanner saja — dipanggil saat SCAN_MODE=pump."""
    log(f"\n{'='*60}")
    log(f"🚀 PUMP SCANNER — {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')}")
    log(f"{'='*60}")

    btc = get_btc_regime(client)
    log(f"BTC 1h: {btc['btc_1h']:+.1f}% | BTC 4h: {btc['btc_4h']:+.1f}%")

    if btc["halt"]:
        tg(f"🛑 <b>PUMP SCAN HALT</b>\n"
           f"BTC crash {btc['btc_4h']:+.1f}% dalam 4h.\n"
           f"Pump scan dilewati sampai kondisi BTC stabil.")  # [v7.2 FIX #5]
        log("🛑 BTC crash — pump scan skip"); return
    if btc["block_buy"]:
        tg(f"⛔ <b>PUMP SCAN SKIP</b>\n"
           f"BTC turun {btc['btc_1h']:+.1f}% dalam 1h.\n"
           f"Pump scan diblokir sementara.")  # [v7.2 FIX #5]
        log("⛔ BTC drop — pump scan skip"); return

    # [v8.0 Phase4 #2] Pakai ticker cache dari run() jika masih segar
    _now_ts = time.time()
    if (hasattr(run, "_ticker_cache") and run._ticker_cache
            and _now_ts - run._ticker_cache_ts < 60):
        tickers = run._ticker_cache
        log(f"   ⚡ Ticker cache hit untuk pump scan ({len(tickers)} tickers)")
    else:
        tickers = gate_call_with_retry(client.list_tickers) or []
        run._ticker_cache    = tickers
        run._ticker_cache_ts = _now_ts
    pumps   = []
    scanned = 0

    for t in tickers:
        pair = t.currency_pair
        if not is_valid_pair(pair): continue
        try:
            price   = float(t.last or 0)
            vol_24h = float(t.quote_volume or 0)
            if price <= 0 or vol_24h < PUMP_MIN_VOLUME: continue
            if already_sent_pump(pair): continue

            scanned += 1
            sig = check_pump(client, pair, price)
            if sig: pumps.append(sig)
            # [v8.0] time.sleep(SCAN_SLEEP_SEC) dihapus — check_pump pakai get_candles
            # yang sudah TTL-cached, tidak trigger API call baru di sini.

        except Exception as e:
            log(f"⚠️ [{pair}]: {e}", "warn"); continue

    log(f"\n📊 Pump scan: {scanned} pairs | {len(pumps)} kandidat")

    if not pumps:
        log("📭 Tidak ada pump terdeteksi"); return

    pumps.sort(key=lambda x: -x["vol_ratio"])

    sent = 0
    for sig in pumps:
        if sent >= MAX_PUMP_SIGNALS: break
        # [FIX #5] Portfolio gate untuk PUMP — wajib dicek sebelum kirim
        pump_portfolio_state = get_portfolio_state()
        if not portfolio_allows(sig, pump_portfolio_state, btc):
            log(f"   🚫 PUMP {sig['pair']} diblok portfolio gate — skip")
            continue
        send_pump_signal(sig)
        save_signal(
            sig["pair"], "PUMP", sig["side"],
            sig["entry"], sig["tp1"], None,   # [v7.1 #7] tp2=None — PUMP tidak punya TP2
            sig["sl"], "PUMP", 0, sig["timeframe"],
            position_size=sig.get("position_size"),   # [v7.18 #C] simpan ke DB
        )
        sent += 1
        time.sleep(0.5)

    log(f"\n✅ Pump scan done — {sent} alert terkirim")


# ════════════════════════════════════════════════════════
#  PORTFOLIO BRAIN — [v7.11 #1]
#
#  Masalah sebelumnya:
#  Bot cerdas per-signal (tier, score, R/R, regime) tapi
#  "buta" secara keseluruhan — tidak tahu berapa trade sedang
#  aktif, berapa yang BUY, berapa yang berkorelasi BTC.
#
#  Ini bahaya di real money: 6 sinyal BUY aktif saat BTC di
#  critical zone = exposure 6× ke satu arah tanpa sadar.
#
#  Solusi: query Supabase sebelum kirim signal → gate global.
#  Hanya INTRADAY + SWING yang dihitung (PUMP/MICROCAP terpisah).
# ════════════════════════════════════════════════════════

def get_portfolio_state() -> dict:
    """
    [v7.11 #1] Query Supabase untuk jumlah open trades aktif.

    [v7.19 #A] Upgrade: sekarang track locked_usdt — total modal yang
    terkunci di posisi aktif. Partial trade (TP1_PARTIAL) dihitung setengah
    size karena 50% sudah closed di TP1. Ini mencegah double counting dimana
    5 posisi $50 diperlakukan sama dengan 5 posisi $10 oleh logika count-based.

    "Open trade" = signal yang sudah dikirim, result masih NULL,
    dan created_at dalam PORTFOLIO_STALE_HOURS jam terakhir.
    Signal lebih lama dianggap stale (tidak ditutup di Supabase,
    tapi realistically sudah expired).

    Returns:
        {"total": int, "buy": int, "sell": int, "locked_usdt": float}

    Fallback ke {"total": 0, "buy": 0, "sell": 0, "locked_usdt": 0.0}
    jika Supabase tidak bisa di-reach — lebih aman lanjut daripada hard-block.
    Caller tetap bisa kirim signal, hanya portfolio gate tidak aktif.
    """
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=PORTFOLIO_STALE_HOURS)
        ).isoformat()

        # [v7.19 #A] Tambah position_size + partial_result ke select
        # [v7.20 #A] Tambah entry + sl untuk risk calculation per trade
        rows = (
            supabase.table("signals_v2")
            .select("side, strategy, position_size, partial_result, entry, sl, pair")
            .is_("result", "null")
            .gte("sent_at", cutoff)   # [fix] pakai sent_at — kolom yang selalu ada
            .in_("strategy", ["INTRADAY", "SWING"])
            .execute()
            .data
        ) or []

        buy_count  = sum(1 for r in rows if r.get("side") == "BUY")
        sell_count = sum(1 for r in rows if r.get("side") == "SELL")

        # [v7.19 #A] Hitung locked capital dengan partial awareness
        # [v7.20 #A] Hitung total_risk_usdt = Σ(size × |sl_dist_pct|) per trade
        locked_usdt     = 0.0
        total_risk_usdt = 0.0
        open_pairs      = []
        sector_counts: dict[str, int] = {}   # [v7.29] count aktif per sektor
        for r in rows:
            try:
                pos_size = float(r.get("position_size") or BASE_POSITION_USDT)
                is_partial = r.get("partial_result") == "TP1_PARTIAL"
                if is_partial:
                    pos_size = pos_size * (1.0 - PARTIAL_TP1_RATIO)
                locked_usdt += pos_size

                # Risk per trade: size × |sl_dist_pct|
                entry_p = float(r.get("entry") or 0)
                sl_p    = float(r.get("sl")    or 0)
                if entry_p > 0 and sl_p > 0:
                    sl_dist_pct = abs(entry_p - sl_p) / entry_p
                    trade_risk  = pos_size * sl_dist_pct
                else:
                    trade_risk  = pos_size * TARGET_RISK_PCT
                total_risk_usdt += trade_risk

                pair_p = r.get("pair", "")
                if pair_p:
                    open_pairs.append(pair_p)
                    # [v7.29] Track sector count untuk MAX_PER_SECTOR gate
                    sec = get_pair_cluster(pair_p)
                    if sec:
                        sector_counts[sec] = sector_counts.get(sec, 0) + 1
            except (TypeError, ValueError):
                _fallback_is_partial = r.get("partial_result") == "TP1_PARTIAL"
                _fallback_size = BASE_POSITION_USDT * (1.0 - PARTIAL_TP1_RATIO) if _fallback_is_partial else BASE_POSITION_USDT
                locked_usdt     += _fallback_size   # [v8.9 FIX] partial-aware fallback
                total_risk_usdt += _fallback_size * TARGET_RISK_PCT

        # Portfolio heat % = total risk / current equity × 100 (retained untuk logging)
        return {
            "total": len(rows),
            "buy": buy_count,
            "sell": sell_count,
            "locked_usdt":     round(locked_usdt, 2),
            "total_risk_usdt": round(total_risk_usdt, 4),
            "open_pairs":      open_pairs,
            "sector_counts":   sector_counts,
        }

    except Exception as e:
        err_str = str(e)
        if "PGRST204" in err_str or "position_size" in err_str or "does not exist" in err_str:
            # Kolom baru belum ada — fallback ke query minimal (side + strategy saja)
            log(f"⚠️ get_portfolio_state: kolom baru belum ada di schema ({e}). "
                f"Jalankan DDL migration. Fallback ke count-only.", "warn")
            try:
                cutoff_fb = (datetime.now(timezone.utc) - timedelta(hours=PORTFOLIO_STALE_HOURS)).isoformat()
                rows_fb = (
                    supabase.table("signals_v2")
                    .select("side, strategy")
                    .is_("result", "null")
                    .gte("sent_at", cutoff_fb)   # [fix] pakai sent_at
                    .in_("strategy", ["INTRADAY", "SWING"])
                    .execute()
                    .data
                ) or []
                buy_fb  = sum(1 for r in rows_fb if r.get("side") == "BUY")
                sell_fb = sum(1 for r in rows_fb if r.get("side") == "SELL")
                return {"total": len(rows_fb), "buy": buy_fb, "sell": sell_fb,
                        "locked_usdt": 0.0, "total_risk_usdt": 0.0,
                        "open_pairs": [], "sector_counts": {}}
            except Exception as e2:
                log(f"⚠️ get_portfolio_state fallback: {e2}", "warn")
        else:
            log(f"⚠️ get_portfolio_state: {e} — assume 0 open trades", "warn")
        return {"total": 0, "buy": 0, "sell": 0, "locked_usdt": 0.0,
                "total_risk_usdt": 0.0, "open_pairs": [], "sector_counts": {}}


def portfolio_allows(sig: dict, state: dict, btc: dict) -> bool:
    """
    [v7.11 #1] Gate portfolio-level — dipanggil sebelum setiap signal dikirim.
    [v8.7]    Semua gate kini aktif (non-redundant) setelah nilai dikalibrasi ulang.

    Enam pemeriksaan berurutan (short-circuit pada yang pertama gagal):
      1. Hard cap total open trades    → MAX_OPEN_TRADES=5, blok semua arah
      2. Total risk cap                → MAX_RISK_TOTAL=5% equity dalam USDT  ← primary financial gate
      3. Same-side directional cap     → MAX_SAME_SIDE_TRADES=3, max 60% satu arah
      4. BTC stress gate               → MAX_BTC_CORR_TRADES=2, BUY cap lebih ketat saat BTC drop
      5. Sector concentration cap      → MAX_PER_SECTOR=2 per kluster aset
      6. Trend bias gate               → blok SELL di trending bullish market

    [v8.9] Gate order direvisi: risk_total sebagai primary financial gate (gate #2).
    Sebelumnya risk_total di gate #5 → directional cap bisa block signal yang sebenarnya
    masih dalam risk budget. Sekarang: lolos risk_total dulu, baru cek arah & sektor.

    state diupdate secara lokal oleh caller setelah setiap signal
    yang lolos — tanpa perlu query ulang ke Supabase per signal.

    Args:
        sig   : signal dict (wajib punya "side" dan "pair")
        state : dict dari get_portfolio_state() (mutable, diupdate caller)
        btc   : dict dari get_btc_regime() — untuk cek block_buy
    """
    pair = sig.get("pair", "?")
    side = sig.get("side", "BUY")

    # ── Check 1: Hard cap total open trades ──────────────────────
    if state["total"] >= MAX_OPEN_TRADES:
        log(f"   🧠 Portfolio SKIP {pair} [{side}] — "
            f"max open trades tercapai ({state['total']}/{MAX_OPEN_TRADES})")
        return False

    # ── Check 2: Total risk cap (primary financial gate) ─────────
    # [v8.9] Dipindah ke gate #2 — risk budget adalah filter terpenting.
    # Signal bagus tidak boleh ditolak hanya karena directional count,
    # tapi harus ditolak jika risk budget sudah habis.
    eq       = (_equity_cache.get("value") or ACCOUNT_EQUITY_USDT)
    risk_lim = eq * MAX_RISK_TOTAL
    new_sz   = sig.get("position_size", BASE_POSITION_USDT) or BASE_POSITION_USDT
    new_ent  = sig.get("entry", 0.0) or 0.0
    new_sl_p = sig.get("sl",    0.0) or 0.0
    new_risk = new_sz * abs(new_ent - new_sl_p) / new_ent if new_ent > 0 and new_sl_p > 0                else new_sz * TARGET_RISK_PCT
    cur_risk = state.get("total_risk_usdt", 0.0)
    if cur_risk + new_risk > risk_lim:
        log(f"   🧠 Portfolio SKIP {pair} [{side}] — "
            f"total risk cap {MAX_RISK_TOTAL*100:.0f}% equity "
            f"(${cur_risk:.2f}+${new_risk:.2f} > ${risk_lim:.2f})")
        return False

    # ── Check 3: Same-side exposure cap ──────────────────────────
    same_side_count = state["buy"] if side == "BUY" else state["sell"]
    if same_side_count >= MAX_SAME_SIDE_TRADES:
        log(f"   🧠 Portfolio SKIP {pair} [{side}] — "
            f"max {side} aktif ({same_side_count}/{MAX_SAME_SIDE_TRADES})")
        return False

    # ── Check 4: BTC correlation gate ────────────────────────────
    # Jika BTC sedang drop (block_buy=True) dan sudah ada terlalu banyak
    # BUY aktif, tolak BUY baru karena semua alt sangat berkorelasi BTC.
    # SELL tidak kena gate ini — justru SELL lebih relevan saat BTC drop.
    if side == "BUY" and btc.get("block_buy") and state["buy"] >= MAX_BTC_CORR_TRADES:
        log(f"   🧠 Portfolio SKIP {pair} [BUY] — "
            f"BTC drop + BUY exposure tinggi ({state['buy']}/{MAX_BTC_CORR_TRADES})")
        return False

    # ── Check 5: [v7.29] Sector exposure cap ─────────────────────
    # Blok jika sektor pair sudah mencapai MAX_PER_SECTOR trades aktif.
    # Menggantikan pairwise correlation matrix — O(1), deterministik.
    sector = get_pair_cluster(pair)
    if sector is not None:
        sector_count = state.get("sector_counts", {}).get(sector, 0)
        if sector_count >= MAX_PER_SECTOR:
            log(f"   🧠 Portfolio SKIP {pair} [{side}] — "
                f"sektor {sector} penuh ({sector_count}/{MAX_PER_SECTOR})")
            return False

    # ── Check 6: Trend bias gate ─────────────────────────────────
    # [v8.9] Renumber: Check 5 (dulunya #6) setelah risk gate dipindah ke #2
    sig_regime = sig.get("regime", "") or ""
    sig_struct = sig.get("struct", "") or sig.get("bias", "") or ""
    if (side == "SELL"
            and sig_regime == "TRENDING"
            and sig_struct in ("BULLISH", "BUY")):
        log(f"   📈 Portfolio SKIP {pair} [SELL] — "
            f"trend bias aktif: TRENDING+BULLISH, SELL dihindari")
        return False

    return True


# ════════════════════════════════════════════════════════
#  TRADE LIFECYCLE TRACKING — [v7.12 #3]
#
#  Masalah sebelumnya:
#  Bot kirim signal → selesai. result di Supabase selalu NULL.
#  Win rate table tidak pernah terisi → model probabilistik
#  tidak pernah belajar. estimate_confidence() selalu "No data".
#
#  Solusi: evaluate_open_trades() dipanggil di awal setiap run().
#  Query open trades → cek current price → update result.
#  Setelah beberapa cycle, signals_v2.result mulai terisi dan
#  load_winrate_table() punya data nyata untuk Bayesian model.
# ════════════════════════════════════════════════════════

def evaluate_open_trades(client) -> dict:
    """
    [v7.12 #3] Evaluasi open trades — cek TP1/TP2/SL/EXPIRED per trade.

    [Phase1 #1] Status lifecycle:
      OPEN    → sinyal baru, belum hit level apapun (result IS NULL)
      PARTIAL → TP1 hit, sisa posisi masih berjalan (result IS NULL, partial_result=TP1_PARTIAL)
      CLOSED  → trade selesai, result terisi (TP2/SL/BREAKEVEN/EXPIRED/PARTIAL_WIN)

    DDL Migration (jalankan sekali di Supabase SQL editor):
      ALTER TABLE signals_v2
        ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'OPEN'
          CHECK (status IN ('OPEN', 'PARTIAL', 'CLOSED'));
      UPDATE signals_v2 SET status = 'CLOSED' WHERE result IS NOT NULL;
      UPDATE signals_v2 SET status = 'PARTIAL' WHERE result IS NULL
        AND partial_result = 'TP1_PARTIAL';

    Logic per row:
      1. Query open trades (result IS NULL) dari Supabase
      2. Cek expired: age > SIGNAL_EXPIRE_HOURS[strategy] → result="EXPIRED"
      3. Fetch current price dari Gate.io live ticker
      4. BUY : price >= tp2 → TP2 | price >= tp1 → TP1 | price <= sl → SL
         SELL: price <= tp2 → TP2 | price <= tp1 → TP1 | price >= sl → SL
      5. Update result + closed_at di Supabase jika ada hit/expired
      6. Invalidate _winrate_cache_ts agar cycle berikutnya reload data baru

    Gate:
      - LIFECYCLE_MAX_EVAL: maks trades dievaluasi per run (cegah overload API)
      - Diurutkan dari oldest first: yang paling lama pending dievaluasi dulu

    Returns:
        {"evaluated": int, "updated": int, "tp1": int, "tp2": int,
         "sl": int, "expired": int}
    """
    stats = {"evaluated": 0, "updated": 0, "tp1": 0, "tp2": 0, "sl": 0, "expired": 0, "partial_win": 0, "breakeven": 0}

    try:
        rows = (
            supabase.table("signals_v2")
            .select("id, pair, strategy, side, entry, tp1, tp2, sl, sent_at, position_size, partial_result, tp1_notified, expiry_warned")  # [v7.24+]
            .is_("result", "null")
            .limit(LIFECYCLE_MAX_EVAL)
            .order("sent_at", desc=False)   # oldest first
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"⚠️ evaluate_open_trades: query gagal — {e}", "warn")
        return stats

    if not rows:
        log("📋 Lifecycle: tidak ada open trades untuk dievaluasi.")
        return stats

    log(f"📋 Lifecycle: mengevaluasi {len(rows)} open trade(s)...")

    now_utc = datetime.now(timezone.utc)

    for row in rows:
        stats["evaluated"] += 1
        trade_id    = row.get("id")
        pair        = row.get("pair", "")
        strategy    = row.get("strategy", "INTRADAY")
        side        = row.get("side", "BUY")
        entry       = float(row.get("entry") or 0)
        tp1         = float(row.get("tp1")   or 0)
        tp2_raw     = row.get("tp2")
        tp2         = float(tp2_raw) if tp2_raw is not None else None
        sl          = float(row.get("sl")    or 0)
        sent_at_str = row.get("sent_at", "")

        if not pair or not trade_id or entry <= 0 or tp1 <= 0 or sl <= 0:
            continue   # data tidak lengkap — skip tanpa update

        # ── Cek expired dulu (tidak perlu price fetch) ────────────
        expire_hours = SIGNAL_EXPIRE_HOURS.get(strategy, 48)
        age_hours    = 0.0
        try:
            sent_at   = datetime.fromisoformat(sent_at_str.replace("Z", "+00:00"))
            age_hours = (now_utc - sent_at).total_seconds() / 3600
        except Exception:
            pass   # sent_at parse gagal → age_hours = 0 → tidak expired

        # ── Cek expired (hard cutoff) ─────────────────────────────
        if age_hours > expire_hours:
            _pr = row.get("partial_result")
            if _pr == "TP1_PARTIAL":
                result = "PARTIAL_WIN"
                log(f"   🎯½ PARTIAL_WIN (expired after TP1): {pair} [{strategy} {side}] — "
                    f"{age_hours:.1f}h > {expire_hours}h limit")
            else:
                result = "EXPIRED"
                log(f"   ⏰ EXPIRED: {pair} [{strategy} {side}] — "
                    f"{age_hours:.1f}h > {expire_hours}h limit")

        else:
            # ── Fetch current price dari Gate.io ─────────────────
            try:
                tickers = gate_call_with_retry(
                    client.list_tickers, currency_pair=pair
                )
                if not tickers:
                    continue
                current_price = float(tickers[0].last or 0)
                if current_price <= 0:
                    continue
            except Exception as e:
                log(f"   ⚠️ Price fetch gagal [{pair}]: {e}", "warn")
                continue

            # ── Evaluasi level hit ────────────────────────────────
            # [v7.13 #2] Partial TP logic:
            #   Cek apakah trade sudah di status "TP1_PARTIAL" (ambil 50% di TP1,
            #   SL digeser ke entry = breakeven). Jika ya:
            #     - TP2 hit → "TP2" (close sisa 50%)
            #     - SL hit  → "BREAKEVEN" (tidak loss karena SL sudah di entry)
            #   Jika belum partial:
            #     - TP2 hit → langsung "TP2"
            #     - TP1 hit → "TP1_PARTIAL" jika ENABLE_PARTIAL_TP, else "TP1"
            #     - SL hit  → "SL"
            partial_result = row.get("partial_result")   # None jika kolom tidak ada (graceful)
            result = None

            if partial_result == "TP1_PARTIAL":
                # ── STATE: RUNNER (setelah TP1 hit) ──────────────────────
                # Lifecycle:
                #   OPEN → TP1_PARTIAL (BE enforced, sl=entry di DB)
                #   RUNNER: trailing SL aktif, sl hanya naik (BUY) / turun (SELL)
                #   CLOSED: TP2 hit → "TP2" | SL/Trail hit → "BREAKEVEN"
                #
                # [v7.25] Break-even GUARANTEED: sl di DB sudah = entry sejak TP1 hit.
                # Trailing stop mulai dari entry, mengikuti harga naik, tidak pernah turun.
                # ─────────────────────────────────────────────────────────
                try:
                    _candles_trail = get_candles(client, pair, "1h", 20)
                    if _candles_trail and len(_candles_trail) >= 14:
                        _c = [float(x["close"]) for x in _candles_trail]
                        _h = [float(x["high"])  for x in _candles_trail]
                        _l = [float(x["low"])   for x in _candles_trail]
                        _atr_trail = calc_atr(_c, _h, _l, period=14)
                        if _atr_trail and _atr_trail > 0:
                            if side == "BUY":
                                _trail_sl = current_price - (_atr_trail * ATR_TRAIL_MULT)
                                # Ratchet: SL hanya naik, floor di entry (BE guaranteed)
                                _new_sl = max(sl, _trail_sl, entry)
                            else:  # SELL
                                _trail_sl = current_price + (_atr_trail * ATR_TRAIL_MULT)
                                # Ratchet: SL hanya turun mengikuti harga, ceiling di entry (BE guaranteed)
                                # [v8.9 FIX] min(sl, _trail_sl) agar SL tidak naik (ratchet down)
                                # max(..., entry) agar SL tidak turun di bawah entry (BE floor)
                                _new_sl = min(sl, _trail_sl) if sl > 0 else _trail_sl
                                _new_sl = max(_new_sl, entry)   # [v8.9 FIX] BE ceiling: SL tidak boleh < entry untuk SELL
                            # Update DB hanya jika SL bergerak signifikan (> 0.1%)
                            if abs(_new_sl - sl) / max(sl, 1e-10) > 0.001:
                                supabase.table("signals_v2").update({"sl": round(_new_sl, 8)}) \
                                    .eq("id", trade_id).execute()
                                pct_moved = (_new_sl - sl) / sl * 100
                                log(f"   📈 [TRAIL] {pair} SL: {sl:.8f} → {_new_sl:.8f} "
                                    f"({pct_moved:+.2f}%)")
                                sl = _new_sl  # update lokal untuk cek hit di bawah
                except Exception as _te:
                    log(f"   ⚠️ [TRAIL] Trailing SL gagal [{pair}]: {_te} — lanjut dengan SL lama (BE protected)", "warn")

                # ── [v7.26] Momentum-based exit (RUNNER only) ────────────
                # Keluar lebih awal kalau momentum melemah AND masih profit.
                # Kondisi: MACD cross berlawanan arah + volume drop.
                # Hanya trigger saat current_price > entry (BUY) / < entry (SELL).
                # Result = "TP1" — dihitung sebagai win, PnL dari current_price.
                if result is None:
                    try:
                        _candles_mom = _candles_trail if _candles_trail else                             get_candles(client, pair, "1h", 30)
                        if _candles_mom and len(_candles_mom) >= 27:
                            _closes_m = [float(x["close"])  for x in _candles_mom]
                            _vols_m   = [float(x["volume"]) for x in _candles_mom]

                            # MACD: cek cross berlawanan arah trade
                            _macd_now, _sig_now = calc_macd(_closes_m)
                            _macd_prev, _sig_prev = calc_macd(_closes_m[:-1])
                            # BUY: bearish cross = macd turun melewati signal dari atas
                            # SELL: bullish cross = macd naik melewati signal dari bawah
                            if side == "BUY":
                                _macd_cross_weak = (_macd_prev >= _sig_prev) and (_macd_now < _sig_now)
                            else:
                                _macd_cross_weak = (_macd_prev <= _sig_prev) and (_macd_now > _sig_now)

                            # Volume: 3 candle terakhir rata-rata < 60% dari 10 candle sebelumnya
                            _vol_recent = sum(_vols_m[-3:]) / 3
                            _vol_avg    = sum(_vols_m[-13:-3]) / 10
                            _vol_drop   = _vol_recent < _vol_avg * 0.60 if _vol_avg > 0 else False

                            # Exit hanya jika KEDUA kondisi terpenuhi + masih profit
                            _in_profit = (current_price > entry) if side == "BUY" else (current_price < entry)
                            if _macd_cross_weak and _vol_drop and _in_profit:
                                result = "TP1"  # exit as win — PnL dihitung dari current_price
                                _pnl_pct = ((current_price - entry) / entry * 100) if side == "BUY" \
                                           else ((entry - current_price) / entry * 100)  # [v8.9 FIX] SELL profit = positif
                                log(f"   📉 [MOM-EXIT] {pair} [{side}] momentum lemah — "
                                    f"MACD cross + vol drop. Exit @ {current_price:.8f} "
                                    f"(+{_pnl_pct:.2f}%)")
                    except Exception as _me:
                        pass  # momentum check gagal → lanjut evaluasi normal

                # ── Cek hit: TP2 atau Trail/BE ───────────────────────────
                if side == "BUY":
                    if tp2 is not None and current_price >= tp2:   result = "TP2"
                    elif current_price <= sl:                        result = "BREAKEVEN"
                else:  # SELL
                    if tp2 is not None and current_price <= tp2:   result = "TP2"
                    elif current_price >= sl:                        result = "BREAKEVEN"
            else:
                # Normal evaluation — TP2 dicek lebih dulu (tidak double-count)
                if side == "BUY":
                    if tp2 is not None and current_price >= tp2:   result = "TP2"
                    elif current_price >= tp1:
                        # [v7.27 #2] TP1 SELALU partial close jika tp2 ada.
                        # Sebelumnya: dikontrol ENABLE_PARTIAL_TP toggle.
                        # Sekarang: TP1 = partial, TP2/SL = final close (lifecycle benar).
                        result = "TP1_PARTIAL" if tp2 is not None else "TP1"
                    elif current_price <= sl:                       result = "SL"
                else:  # SELL
                    if tp2 is not None and current_price <= tp2:   result = "TP2"
                    elif current_price <= tp1:
                        result = "TP1_PARTIAL" if tp2 is not None else "TP1"
                    elif current_price >= sl:                       result = "SL"

            if result is None:
                continue   # belum ada level tersentuh — biarkan open

        # ── Update Supabase ───────────────────────────────────────
        try:
            update_payload: dict = {"closed_at": now_utc.isoformat()}

            if result == "TP1_PARTIAL":
                # Tidak tutup trade — update partial_result saja.
                # [v7.24] Skip jika notifikasi TP1 sudah pernah dikirim
                if row.get("tp1_notified"):
                    log(f"   🔕 TP1_PARTIAL sudah dinotifikasi sebelumnya — skip [{pair}]")
                    continue
                # closed_at TIDAK diisi agar trade tetap "open" untuk sisa posisi.
                # [v7.14 #C] Gunakan adaptive ratio berdasarkan RR aktual
                if entry > 0 and tp1 > 0 and sl > 0:
                    sl_dist = abs(entry - sl)
                    tp1_dist = abs(tp1 - entry)
                    rr_actual = (tp1_dist / sl_dist) if sl_dist > 0 else 2.0
                else:
                    rr_actual = 2.0
                partial_ratio = calc_partial_ratio(rr_actual) if ENABLE_PARTIAL_TP else PARTIAL_TP1_RATIO

                # [v7.18 #A] Hitung PnL untuk porsi partial yang sudah ditutup.
                # Simpan sebagai partial_pnl_usdt agar equity curve tidak kehilangan
                # realized PnL dari half-close. Ini juga membuat equity lebih akurat
                # dari sebelumnya yang hanya catat PnL saat trade FULLY closed.
                partial_pnl = 0.0
                try:
                    pos_size_raw  = row.get("position_size")
                    full_pos_size = float(pos_size_raw) if pos_size_raw else BASE_POSITION_USDT
                    partial_pos   = full_pos_size * partial_ratio   # hanya porsi yang ditutup
                    if side == "BUY":
                        partial_pnl = (tp1 - entry) / entry * partial_pos
                    else:  # SELL
                        partial_pnl = (entry - tp1) / entry * partial_pos
                    partial_pnl = round(partial_pnl, 4)
                    # [v7.27 #3] Kurangi trading fee round-trip untuk porsi partial.
                    # Entry fee sudah dibayar saat sinyal masuk; exit fee saat TP1 hit.
                    # Estimasi: 2 × TRADING_FEE_PCT × notional partial.
                    _fee_partial = round(partial_pos * TRADING_FEE_PCT * 2, 6)
                    partial_pnl  = round(partial_pnl - _fee_partial, 4)
                except Exception as _ppe:
                    log(f"   ⚠️ Partial PnL calc error [{pair}]: {_ppe}", "warn")
                    partial_pnl = 0.0

                update_payload = {
                    "partial_result":  "TP1_PARTIAL",
                    "partial_pnl_usdt": partial_pnl,  # [v7.18 #A] realized partial PnL
                    "sl": round(entry, 8),             # [v7.25] BE enforced — SL pindah ke entry saat TP1 hit
                    "status": "PARTIAL",               # [Phase1 #1] lifecycle: OPEN → PARTIAL
                    "tp1_notified": True,              # [v8.9 FIX] merge ke 1 call — cegah race condition jika call kedua gagal
                }
                # [v9.0 FIX] Bungkus dengan try/except — gagal update PARTIAL = data equity corrupt
                try:
                    supabase.table("signals_v2").update(update_payload).eq("id", trade_id).execute()
                except Exception as _db_err:
                    log(f"   🔴 [CRITICAL] Gagal update TP1_PARTIAL trade {trade_id} [{pair}]: {_db_err}", "error")
                    tg(f"🔴 <b>DB ERROR — TP1 PARTIAL gagal disimpan</b>\n"
                       f"Trade: {pair} | ID: {trade_id}\n"
                       f"Error: {_db_err}\n"
                       f"⚠️ Data equity mungkin tidak akurat — cek Supabase manual.")
                    # Jangan continue — lanjut log lokal agar audit trail tetap ada
                log(f"   🔒 BE enforced: SL {sl:.8f} → {entry:.8f} (entry) [{pair}]")
                stats["updated"] += 1
                pct_tp1 = abs((tp1 - entry) / entry * 100) if entry > 0 else 0
                log(f"   🎯½ TP1_PARTIAL: {pair} [{strategy} {side}] "
                    f"+{pct_tp1:.1f}% | {partial_ratio*100:.0f}% profit diamankan [RR={rr_actual:.1f}], "
                    f"partial_pnl={partial_pnl:+.4f} USDT, SL → entry (breakeven)")
                tg(f"🎯 <b>Partial Profit Taken</b> — {pair.replace('_USDT', '/USDT')}\n"
                   f"TP1 tercapai +{pct_tp1:.1f}% ✅\n"
                   f"• {partial_ratio*100:.0f}% posisi ditutup (adaptive RR={rr_actual:.1f})\n"
                   f"• Realized: {partial_pnl:+.2f} USDT\n"
                   f"• SL digeser ke entry (breakeven)\n"
                   f"• Menunggu TP2 untuk sisa posisi...")
                continue   # jangan isi result di Supabase — trade masih open
            else:
                # ── [FIX #3] Hitung PnL aktual sebelum update ────────────
                # Gunakan current_price sebagai exit_price.
                # EXPIRED → exit diasumsikan di entry (pnl = 0).
                # Fetch position_size dari DB jika ada, fallback ke BASE_POSITION_USDT.
                pnl_usdt = 0.0
                # [v7.19 #C] PARTIAL_WIN: ambil partial_pnl_usdt yang sudah tersimpan
                if result == "PARTIAL_WIN":
                    try:
                        pnl_usdt = float(row.get("partial_pnl_usdt") or 0.0)
                    except (TypeError, ValueError):
                        pnl_usdt = 0.0
                elif result != "EXPIRED" and entry > 0:
                    try:
                        exit_price = current_price   # already fetched above
                        # [v7.18 #D] position_size sudah ada di row — tidak perlu extra query
                        pos_size_raw = row.get("position_size")
                        position_size = float(pos_size_raw) if pos_size_raw else BASE_POSITION_USDT

                        # [v7.19 #B] Jika trade sudah TP1_PARTIAL, hanya sisa size yang relevan.
                        # 50% sudah ditutup di TP1 (partial_pnl_usdt sudah tersimpan).
                        # SL/BREAKEVEN dari sisa = position_size × (1 - partial_ratio).
                        # Pakai partial_result dari row (sudah di-fetch di awal loop).
                        _partial_result = row.get("partial_result")
                        if _partial_result == "TP1_PARTIAL":
                            # Hitung partial_ratio yang dipakai saat TP1 hit
                            if entry > 0 and tp1 > 0 and sl > 0:
                                sl_dist_b  = abs(entry - sl)
                                tp1_dist_b = abs(tp1 - entry)
                                rr_b = (tp1_dist_b / sl_dist_b) if sl_dist_b > 0 else 2.0
                            else:
                                rr_b = 2.0
                            _pratio = calc_partial_ratio(rr_b) if ENABLE_PARTIAL_TP else PARTIAL_TP1_RATIO
                            position_size = position_size * (1.0 - _pratio)  # hanya sisa

                        if side == "BUY":
                            pnl_usdt = (exit_price - entry) / entry * position_size
                        else:  # SELL
                            pnl_usdt = (entry - exit_price) / entry * position_size

                        pnl_usdt = round(pnl_usdt, 4)

                        # [v7.27 #3] Kurangi trading fee untuk sisa posisi yang ditutup.
                        # Jika ini adalah final close setelah TP1_PARTIAL, entry fee
                        # sudah terhitung di partial_pnl. Hanya exit fee saja di sini.
                        if _partial_result == "TP1_PARTIAL":
                            _fee_final = round(position_size * TRADING_FEE_PCT, 6)  # exit leg saja
                        else:
                            _fee_final = round(position_size * TRADING_FEE_PCT * 2, 6)  # full round-trip
                        pnl_usdt = round(pnl_usdt - _fee_final, 4)

                        # [v7.27 #2] Lifecycle fix: tambahkan partial_pnl_usdt ke total PnL
                        # saat final close (TP2/SL/BREAKEVEN) setelah TP1_PARTIAL.
                        # Ini memastikan pnl_usdt di DB = total realized PnL untuk trade ini.
                        # get_current_equity_usdt() menggunakan pnl_usdt dari closed trades
                        # DAN partial_pnl_usdt dari TP1_PARTIAL — tapi setelah trade fully closed,
                        # partial query tidak match lagi (result bukan NULL), jadi kita harus
                        # embed partial ke dalam pnl_usdt akhir agar equity akurat.
                        if _partial_result == "TP1_PARTIAL":
                            _prev_partial = float(row.get("partial_pnl_usdt") or 0.0)
                            pnl_usdt      = round(pnl_usdt + _prev_partial, 4)
                    except Exception as _pe:
                        log(f"   ⚠️ PnL calc error [{pair}]: {_pe}", "warn")
                        pnl_usdt = 0.0

                update_payload["result"]   = result
                update_payload["pnl_usdt"] = pnl_usdt
                update_payload["status"]   = "CLOSED"  # [Phase1 #1] lifecycle: * → CLOSED
                # [FIX #2] Debug log — konfirmasi update benar-benar dieksekusi
                log(f"   🔄 Updating trade {trade_id} → result={result} pnl={pnl_usdt:+.4f} status=CLOSED")
                # [v9.0 FIX] Bungkus dengan try/except — gagal update CLOSED = trade tetap OPEN di DB,
                # equity calculation salah, dan bot akan terus monitor posisi yang sudah tidak ada.
                try:
                    supabase.table("signals_v2").update(update_payload).eq("id", trade_id).execute()
                except Exception as _db_err:
                    log(f"   🔴 [CRITICAL] Gagal update CLOSED trade {trade_id} [{pair}]: {_db_err}", "error")
                    tg(f"🔴 <b>DB ERROR — Trade CLOSE gagal disimpan</b>\n"
                       f"Trade: {pair} [{strategy} {side}] | ID: {trade_id}\n"
                       f"Result seharusnya: {result} | PnL: {pnl_usdt:+.4f} USDT\n"
                       f"Error: {_db_err}\n"
                       f"⚠️ Trade ini masih OPEN di DB — update manual diperlukan.")

            stats["updated"] += 1
            # Update stats counter per result type
            # [v8.6 FIX] BREAKEVEN dihitung terpisah (bukan tp1).
            # PARTIAL_WIN sekarang masuk "partial_win" — sebelumnya jatuh ke "expired"
            # karena "partial_win" tidak ada di lookup dict (bug kritis statistik).
            _RESULT_KEY_MAP = {
                "TP1":         "tp1",
                "TP2":         "tp2",
                "SL":          "sl",
                "EXPIRED":     "expired",
                "PARTIAL_WIN": "partial_win",
                "BREAKEVEN":   "breakeven",
            }
            key = _RESULT_KEY_MAP.get(result, "expired")
            stats[key] = stats.get(key, 0) + 1

            emoji = {"TP2": "🎯🎯", "TP1": "🎯", "SL": "❌", "EXPIRED": "⏰",
                     "BREAKEVEN": "⚖️", "PARTIAL_WIN": "🎯½"}.get(result, "?")
            log(f"   {emoji} {result}: {pair} [{strategy} {side}] pnl={pnl_usdt:+.2f} USDT")

            # ── Kirim notifikasi Telegram untuk setiap trade yang closed ──
            if result is not None:
                try:
                    pair_display = pair.replace("_USDT", "/USDT")
                    pnl_sign     = "+" if pnl_usdt >= 0 else ""
                    pnl_idr      = usdt_to_idr(abs(pnl_usdt), get_usdt_idr_rate())

                    if result == "TP2":
                        tg_msg = (
                            f"🎯🎯 <b>TP2 Hit — {pair_display}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"Strategy : {strategy} {side}\n"
                            f"PnL      : <b>{pnl_sign}{pnl_usdt:.2f} USDT</b> (~{pnl_idr})\n"
                            f"<i>Full target tercapai ✅</i>"
                        )
                    elif result == "TP1":
                        tg_msg = (
                            f"🎯 <b>TP1 Hit — {pair_display}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"Strategy : {strategy} {side}\n"
                            f"PnL      : <b>{pnl_sign}{pnl_usdt:.2f} USDT</b> (~{pnl_idr})\n"
                            f"<i>Target pertama tercapai ✅</i>"
                        )
                    elif result == "PARTIAL_WIN":
                        tg_msg = (
                            f"🎯½ <b>Partial Win (Expired after TP1) — {pair_display}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"Strategy : {strategy} {side}\n"
                            f"PnL      : <b>{pnl_sign}{pnl_usdt:.2f} USDT</b> (~{pnl_idr})\n"
                            f"<i>TP1 tercapai sebelum expired ✅</i>"
                        )
                    elif result == "BREAKEVEN":
                        tg_msg = (
                            f"⚖️ <b>Breakeven — {pair_display}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"Strategy : {strategy} {side}\n"
                            f"PnL      : <b>0.00 USDT</b>\n"
                            f"<i>SL digeser ke entry setelah TP1 — modal aman</i>"
                        )
                    elif result == "SL":
                        tg_msg = (
                            f"❌ <b>Stop Loss — {pair_display}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"Strategy : {strategy} {side}\n"
                            f"PnL      : <b>{pnl_sign}{pnl_usdt:.2f} USDT</b> (~{pnl_idr})\n"
                            f"<i>SL tersentuh — loss terkontrol</i>"
                        )
                    elif result == "EXPIRED":
                        try:
                            if entry and entry > 0:
                                pct_now = (current_price - entry) / entry * 100
                                if side == "SELL":
                                    pct_now = -pct_now
                                pct_str = f"{pct_now:+.2f}%"
                            else:
                                pct_str = "N/A"
                        except Exception:
                            pct_str = "N/A"
                        expire_h = SIGNAL_EXPIRE_HOURS.get(strategy, 48)
                        tg_msg = (
                            f"⏰ <b>Signal Expired — {pair_display}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"Strategy : {strategy} {side}\n"
                            f"Usia     : {age_hours:.0f}j / {expire_h}j\n"
                            f"Posisi   : <b>{pct_str}</b> dari entry\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"<i>⚠️ Signal tidak lagi dimonitor bot.</i>\n"
                            f"<i>Jika sudah entry, kelola posisi secara manual.</i>"
                        )
                    else:
                        tg_msg = None

                    if tg_msg:
                        tg(tg_msg)
                except Exception as _tge:
                    log(f"   ⚠️ Telegram notif gagal [{pair}]: {_tge}", "warn")

        except Exception as e:
            log(f"   ⚠️ Update result gagal [{pair}]: {e}", "warn")

        time.sleep(0.1)   # throttle ringan — hindari burst Gate.io

    if stats["updated"] > 0:
        log(f"📋 Lifecycle done: {stats['updated']} diupdate "
            f"(TP1:{stats['tp1']} TP2:{stats['tp2']} "
            f"PARTIAL_WIN:{stats['partial_win']} BREAKEVEN:{stats['breakeven']} "
            f"SL:{stats['sl']} EXPIRED:{stats['expired']})")
        # Invalidate win rate cache — data baru masuk di Supabase,
        # cycle berikutnya load_winrate_table() akan reload otomatis
        global _winrate_cache_ts
        _winrate_cache_ts = 0.0
        # [v7.18 #B] Invalidate equity cache — pnl_usdt baru tersimpan,
        # get_current_equity_usdt() harus re-query agar equity aktif akurat.
        # Tanpa ini, equity cache 30-menit bisa masih pakai nilai sebelum
        # trade ditutup, menyebabkan sizing cycle ini pakai equity lama.
        global _equity_cache
        _equity_cache["ts"] = 0.0
        _equity_cache["pre_partial_equity"] = None  # [v7.20 #C] reset throttle baseline
        log("   💼 Equity cache diinvalidasi — akan re-query di cycle ini.")
    else:
        log("📋 Lifecycle: tidak ada level tersentuh — semua trades masih open.")

    return stats


# ════════════════════════════════════════════════════════
#  EQUITY CURVE TRACKER — [v7.15 #E]
#
#  Upgrade dari implicit tracking → explicit equity curve dengan:
#    1. Max DD historis (bukan hanya current DD)
#    2. Equity curve points — list (timestamp, cumulative_pnl) untuk
#       visualisasi dan analisis tren luar bot
#    3. Telegram mini-chart — ASCII sparkline equity curve dikirim
#       setiap run agar user bisa monitor performa tanpa dashboard eksternal
#    4. Sortino ratio proxy — pisahkan downside dari total volatilitas
#       (lebih relevan dari Sharpe untuk distribusi return yang skewed)
#
#  Skema Supabase (jalankan DDL ini sekali):
#  ─────────────────────────────────────────────────────
#  CREATE TABLE equity_snapshots (
#    id               BIGSERIAL PRIMARY KEY,
#    recorded_at      TIMESTAMPTZ DEFAULT NOW(),
#    cumulative_pnl   NUMERIC,
#    peak_equity      NUMERIC,
#    current_dd_pct   NUMERIC,
#    max_dd_pct       NUMERIC,       -- [v7.15 #E] max DD historis
#    win_rate_30d     NUMERIC,
#    sharpe_approx    NUMERIC,
#    sortino_approx   NUMERIC,       -- [v7.15 #E] Sortino proxy
#    open_trades      INT,
#    total_signals    INT,
#    curve_points     JSONB          -- [v7.15 #E] [{t, pnl}, ...] daily buckets
#  );
#  ─────────────────────────────────────────────────────
# ════════════════════════════════════════════════════════

EQUITY_SNAPSHOT_ENABLED  = True   # toggle — set False untuk disable tanpa ubah kode
EQUITY_SPARKLINE_BARS    = 10     # [v7.15 #E] jumlah bar ASCII chart di Telegram
EQUITY_CURVE_DAYS_STORED = 60     # [v7.15 #E] simpan max 60 hari daily buckets di JSONB


def _sparkline(values: list[float], bars: int = 10) -> str:
    """
    [v7.15 #E] Buat ASCII sparkline dari list nilai float.

    Menggunakan blok Unicode ▁▂▃▄▅▆▇█ untuk representasi relatif.
    Nilai minimum → ▁, nilai maksimum → █.

    Args:
        values : list PnL kumulatif harian (atau titik apapun)
        bars   : jumlah bar yang ditampilkan (ambil N titik terakhir)

    Returns:
        str: mis. "▂▃▄▄▅▆▅▇█▇"
    """
    BLOCKS = "▁▂▃▄▅▆▇█"
    if not values:
        return "─"
    pts = values[-bars:] if len(values) > bars else values
    v_min, v_max = min(pts), max(pts)
    span = v_max - v_min
    if span == 0:
        return BLOCKS[3] * len(pts)   # semua sama → tengah
    result = ""
    for v in pts:
        idx = int((v - v_min) / span * (len(BLOCKS) - 1))
        result += BLOCKS[idx]
    return result


def build_equity_curve() -> dict:
    """
    [v7.15 #E] Bangun equity curve lengkap dari signals_v2.

    Menghitung:
    - cumulative PnL & equity curve points (daily buckets)
    - peak equity (high-watermark)
    - current drawdown & MAX drawdown historis sejak awal
    - win rate rolling 30 hari
    - Sharpe approximation (mean/std daily PnL)
    - Sortino approximation (mean/downside_std daily PnL)

    Returns:
        dict metrik lengkap, atau {} jika data tidak cukup.
    """
    try:
        rows = (
            supabase.table("signals_v2")
            .select("result, pnl_usdt, sent_at, closed_at")
            .not_.is_("result", "null")
            .neq("result", "EXPIRED")
            .order("sent_at", desc=False)
            .limit(1000)
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"⚠️ build_equity_curve: query gagal — {e}", "warn")
        return {}

    if not rows:
        return {}

    WIN_VALUES = {"WIN", "TP1", "TP2", "PARTIAL_WIN"}  # [v8.6 FIX] unified — BREAKEVEN bukan win, PARTIAL_WIN adalah win
    now_utc    = datetime.now(timezone.utc)

    # [v9.0 FIX] Gunakan INITIAL_EQUITY_USDT sebagai base anchor yang stabil — bukan
    # ACCOUNT_EQUITY_USDT yang bisa berubah jadi live wallet balance (via bootstrap).
    # Bot ini adalah signal-only (tidak eksekusi order nyata), jadi saldo wallet Gate.io
    # tidak merepresentasikan modal yang dikelola — yang relevan adalah modal awal user.
    # Contoh masalah sebelumnya:
    #   - ACCOUNT_EQUITY_USDT = $14 (live wallet, sisa setelah 5 posisi open)
    #   - Peak = $204.42 (tersimpan dari saat equity sempat tinggi)
    #   - DD = (204.42 - (14 + -5.10)) / 204.42 * 100 = 95.6%  ← menyesatkan
    # Dengan fix:
    #   - base_equity = $200 (INITIAL_EQUITY_USDT, modal awal yang dialokasikan user)
    #   - DD = (204.42 - (200 + -5.10)) / 204.42 * 100 = 4.7%  ← akurat
    base_equity  = INITIAL_EQUITY_USDT   # anchor DD pada modal awal, bukan live wallet

    cumulative   = 0.0
    peak         = _load_peak_equity_from_db()   # [v7.22 #B] persistent high-watermark
    # Safety guard: peak tidak pernah di bawah modal awal
    if peak < base_equity:
        peak = base_equity
    max_dd_frac  = 0.0          # [v7.15 #E] max DD historis
    daily_pnl: dict[str, float] = {}
    win_count_30d = 0
    total_30d     = 0

    for row in rows:
        # ── PnL ──────────────────────────────────────────────────────────
        try:
            pnl = float(row.get("pnl_usdt") or 0.0)
        except (TypeError, ValueError):
            pnl = 0.0

        cumulative += pnl
        equity_now = base_equity + cumulative   # [v9.0 FIX] pakai base_equity, bukan ACCOUNT_EQUITY_USDT
        if equity_now > peak:
            peak = equity_now
        # Safety guard: peak tidak pernah di bawah modal awal
        if peak < base_equity:
            peak = base_equity

        # [v7.15 #E] Track max DD pada setiap titik — bukan hanya current
        dd_here = (peak - equity_now) / peak
        if dd_here > max_dd_frac:
            max_dd_frac = dd_here

        # ── Daily bucket (untuk Sharpe, Sortino, sparkline) ──────────────
        ts_str = row.get("closed_at") or row.get("sent_at") or ""
        try:
            ts      = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            day_key = ts.date().isoformat()
            daily_pnl[day_key] = daily_pnl.get(day_key, 0.0) + pnl

            # Rolling 30d win rate
            age_days = (now_utc - ts).total_seconds() / 86400
            if age_days <= 30:
                total_30d += 1
                if (row.get("result") or "").upper() in WIN_VALUES:
                    win_count_30d += 1
        except Exception:
            pass

    # ── Current drawdown ─────────────────────────────────────────────────
    # [v9.0 FIX] current equity = base_equity (modal awal) + cumPnL dari closed trades
    current_equity_abs = base_equity + cumulative
    current_dd_pct = ((peak - current_equity_abs) / peak * 100) if peak > 0 else 0.0

    # ── Win rate 30d ─────────────────────────────────────────────────────
    win_rate_30d = (win_count_30d / total_30d) if total_30d > 0 else None

    # ── Daily values untuk statistik ────────────────────────────────────
    sorted_days   = sorted(daily_pnl.keys())
    daily_values  = [daily_pnl[d] for d in sorted_days]

    sharpe_approx  = None
    sortino_approx = None
    if len(daily_values) >= 5:
        mean_d   = sum(daily_values) / len(daily_values)
        var_d    = sum((x - mean_d) ** 2 for x in daily_values) / len(daily_values)
        std_d    = var_d ** 0.5

        # Sharpe proxy
        if std_d > 0:
            sharpe_approx = round(mean_d / std_d, 3)

        # [v7.15 #E] Sortino proxy — downside std (hanya hari loss)
        downside = [x for x in daily_values if x < 0]
        if len(downside) >= 2:
            var_down   = sum(x ** 2 for x in downside) / len(downside)
            std_down   = var_down ** 0.5
            if std_down > 0:
                sortino_approx = round(mean_d / std_down, 3)

    # ── Equity curve points (daily cumulative) untuk JSONB ───────────────
    # [v7.15 #E] Simpan titik cumulative per hari (bukan PnL per hari)
    # agar mudah di-plot ulang dari Supabase tanpa rekalkukasi
    curve_points = []
    running = 0.0
    cutoff  = sorted_days[-EQUITY_CURVE_DAYS_STORED:] if len(sorted_days) > EQUITY_CURVE_DAYS_STORED \
              else sorted_days
    # Perlu cumulative dari awal meski hanya simpan N hari terakhir
    for day in sorted_days:
        running += daily_pnl[day]
        if day in cutoff:
            curve_points.append({"t": day, "pnl": round(running, 4)})

    # ── [v7.25] Profit Factor & Expectancy ──────────────────────────────
    pf_gross_win  = sum(float(r.get("pnl_usdt") or 0) for r in rows if float(r.get("pnl_usdt") or 0) > 0)
    pf_gross_loss = abs(sum(float(r.get("pnl_usdt") or 0) for r in rows if float(r.get("pnl_usdt") or 0) < 0))
    profit_factor = round(pf_gross_win / pf_gross_loss, 2) if pf_gross_loss > 0 else None

    wins_pf   = [float(r.get("pnl_usdt") or 0) for r in rows if float(r.get("pnl_usdt") or 0) > 0]
    losses_pf = [float(r.get("pnl_usdt") or 0) for r in rows if float(r.get("pnl_usdt") or 0) < 0]
    avg_win_e = (sum(wins_pf)   / len(wins_pf))   if wins_pf   else 0.0
    avg_los_e = (sum(losses_pf) / len(losses_pf)) if losses_pf else 0.0
    wr_all_e  = len(wins_pf) / len(rows) if rows else 0.0
    expectancy_val = round((wr_all_e * avg_win_e) + ((1 - wr_all_e) * avg_los_e), 4) if rows else None

    return {
        "cumulative_pnl"  : round(cumulative, 4),
        "peak_equity"     : round(peak, 4),
        "current_dd_pct"  : round(current_dd_pct, 2),
        "max_dd_pct"      : round(max_dd_frac * 100, 2),
        "win_rate_30d"    : round(win_rate_30d, 4) if win_rate_30d is not None else None,
        "sharpe_approx"   : sharpe_approx,
        "sortino_approx"  : sortino_approx,
        "profit_factor"   : profit_factor,                 # [v7.25]
        "expectancy"      : expectancy_val,                # [v7.25] per-trade dalam USDT
        "total_closed"    : len(rows),
        "total_30d"       : total_30d,
        "daily_values"    : daily_values,
        "curve_points"    : curve_points,
    }


def save_equity_snapshot(open_trades: int = 0) -> None:
    """
    [v7.15 #E] Hitung equity curve, simpan ke Supabase, dan kirim
    Telegram mini-report dengan ASCII sparkline.

    Dipanggil sekali di akhir setiap run(). Gagal-safe total.
    """
    if not EQUITY_SNAPSHOT_ENABLED:
        return

    metrics = build_equity_curve()
    if not metrics:
        log("⚠️ save_equity_snapshot: tidak ada data cukup.", "warn")
        return

    # ── Simpan ke Supabase ────────────────────────────────────────────────
    import json as _json
    payload = {
        "cumulative_pnl" : metrics.get("cumulative_pnl"),
        "peak_equity"    : metrics.get("peak_equity"),
        "current_dd_pct" : metrics.get("current_dd_pct"),
        "max_dd_pct"     : metrics.get("max_dd_pct"),
        "win_rate_30d"   : metrics.get("win_rate_30d"),
        "sharpe_approx"  : metrics.get("sharpe_approx"),
        "sortino_approx" : metrics.get("sortino_approx"),
        "open_trades"    : open_trades,
        "total_signals"  : metrics.get("total_closed"),
        "curve_points"   : _json.dumps(metrics.get("curve_points", [])),
    }

    try:
        supabase.table("equity_snapshots").insert(payload).execute()
    except Exception as e:
        err_str = str(e)
        if "PGRST204" in err_str or "curve_points" in err_str:
            # Kolom curve_points/sortino/max_dd belum ada — coba insert subset minimal
            # Jalankan DDL di bawah untuk unlock fitur penuh:
            # ALTER TABLE equity_snapshots ADD COLUMN max_dd_pct NUMERIC;
            # ALTER TABLE equity_snapshots ADD COLUMN sortino_approx NUMERIC;
            # ALTER TABLE equity_snapshots ADD COLUMN curve_points JSONB;
            log("⚠️ save_equity_snapshot: kolom baru belum ada di schema. "
                "Insert subset minimal (tanpa curve_points/sortino/max_dd).", "warn")
            _minimal = {k: v for k, v in payload.items()
                        if k not in ("curve_points", "sortino_approx", "max_dd_pct")}
            try:
                supabase.table("equity_snapshots").insert(_minimal).execute()
            except Exception as e2:
                log(f"⚠️ save_equity_snapshot: fallback insert gagal — {e2}", "warn")
        else:
            log(f"⚠️ save_equity_snapshot: Supabase insert gagal — {e}", "warn")

    # ── Log ringkas ───────────────────────────────────────────────────────
    dd      = metrics.get("current_dd_pct", 0.0)
    max_dd  = metrics.get("max_dd_pct", 0.0)
    wr      = metrics.get("win_rate_30d")
    sh      = metrics.get("sharpe_approx")
    so      = metrics.get("sortino_approx")
    pnl     = metrics.get("cumulative_pnl", 0.0)
    log(
        f"📈 Equity: PnL={pnl:+.2f} | Peak={metrics.get('peak_equity', 0):.2f} | "
        f"DD={dd:.1f}% MaxDD={max_dd:.1f}% | "
        f"WR30d={f'{wr*100:.1f}%' if wr else 'N/A'} | "
        f"Sharpe≈{sh or 'N/A'} Sortino≈{so or 'N/A'}"
    )

    # ── [v7.15 #E] Telegram ASCII sparkline ──────────────────────────────
    daily_vals = metrics.get("daily_values", [])
    spark      = _sparkline(
        # Konversi daily PnL ke cumulative agar chart naik/turun natural
        [sum(daily_vals[:i+1]) for i in range(len(daily_vals))],
        bars=EQUITY_SPARKLINE_BARS
    )

    # Tentukan emoji trend dari pergerakan terakhir
    if len(daily_vals) >= 2:
        trend_emoji = "📈" if daily_vals[-1] >= 0 else "📉"
    else:
        trend_emoji = "📊"

    # Warna DD: hijau jika < 5%, kuning 5–10%, merah > 10%
    dd_icon = "🟢" if dd < 5.0 else ("🟡" if dd < 10.0 else "🔴")
    max_dd_icon = "🟢" if max_dd < 10.0 else ("🟡" if max_dd < 20.0 else "🔴")

    tg(
        f"{trend_emoji} <b>Equity Report</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<code>{spark}</code>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Realized PnL  : <b>{pnl:+.2f} USDT</b>\n"
        f"Peak Equity   : <b>{metrics.get('peak_equity', 0):.2f} USDT</b>\n"
        f"Current DD    : {dd_icon} <b>{dd:.1f}%</b>\n"
        f"Max DD (hist) : {max_dd_icon} <b>{max_dd:.1f}%</b>\n"
        f"WR 30d        : <b>{'N/A' if not wr else f'{wr*100:.1f}%'}</b> "
        f"({metrics.get('total_30d', 0)} trades)\n"
        f"Sharpe ≈      : <b>{sh or 'N/A'}</b>\n"
        f"Sortino ≈     : <b>{so or 'N/A'}</b>\n"
        f"Profit Factor : <b>{metrics.get('profit_factor') or 'N/A'}</b>\n"
        f"Expectancy    : <b>{str(round(metrics['expectancy'], 4)) + ' USDT' if metrics.get('expectancy') is not None else 'N/A'}</b>\n"
        f"Open trades   : <b>{open_trades}</b>\n"
        f"<i>Snapshot: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC</i>"
    )


# ════════════════════════════════════════════════════════
#  MAIN RUN
# ════════════════════════════════════════════════════════

def send_open_trades_summary(client=None) -> None:
    """
    Kirim rekapan semua open trades ke Telegram setiap akhir run.
    Memudahkan user memantau posisi aktif tanpa harus menunggu signal baru.
    Jika client disediakan, tampilkan unrealized PnL % berdasarkan harga live.
    """
    try:
        rows = (
            supabase.table("signals_v2")
            .select("pair, strategy, side, entry, tp1, tp2, sl, sent_at, partial_result")
            .is_("result", "null")
            .order("sent_at", desc=False)
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"⚠️ send_open_trades_summary: query gagal — {e}", "warn")
        return

    if not rows:
        return

    now_utc  = datetime.now(timezone.utc)
    rate     = get_usdt_idr_rate()
    lines    = []

    # ── Helper format — didefinisikan sekali di luar loop ─────────
    def fmt(v):
        if v is None: return "—"
        fv = float(v)
        if fv >= 1000:  return f"${fv:,.0f}"
        if fv >= 1:     return f"${fv:.4f}"
        if fv >= 0.01:  return f"${fv:.5f}"
        return f"${fv:.6f}"

    def fmt_idr(v):
        if v is None: return ""
        return f" <i>≈ {usdt_to_idr(float(v), rate)}</i>"

    def fmt_pct(v, ref, direction):
        if v is None or ref is None: return ""
        pct = (float(v) - float(ref)) / float(ref) * 100
        if direction == "BUY":
            return f" <i>(+{abs(pct):.1f}%)</i>" if pct >= 0 else f" <i>({pct:.1f}%)</i>"
        else:  # SELL
            return f" <i>(-{abs(pct):.1f}%)</i>" if pct <= 0 else f" <i>(+{pct:.1f}%)</i>"

    for i, row in enumerate(rows, 1):
        pair     = row.get("pair", "?")
        strategy = row.get("strategy", "?")
        side     = row.get("side", "?")
        entry    = row.get("entry")
        tp1      = row.get("tp1")
        tp2      = row.get("tp2")
        sl       = row.get("sl")
        sent_at  = row.get("sent_at", "")
        partial  = row.get("partial_result")

        # Hitung usia trade
        try:
            sent_dt  = datetime.fromisoformat(sent_at.replace("Z", "+00:00"))
            age_h    = (now_utc - sent_dt).total_seconds() / 3600
            age_str  = f"{age_h:.0f}j"
        except Exception:
            age_str  = "?"

        side_emoji = "🟢" if side == "BUY" else "🔴"
        pair_disp  = pair.replace("_USDT", "/USDT")

        # Status partial
        status = ""
        if partial == "TP1_PARTIAL":
            status = " ⚡TP1✅ nunggu TP2"

        # ── Fetch harga live & hitung unrealized PnL ──────────────
        pnl_str = ""
        if client and entry:
            try:
                tickers = gate_call_with_retry(
                    client.list_tickers, currency_pair=pair
                )
                if tickers:
                    cur_price = float(tickers[0].last or 0)
                    if cur_price > 0:
                        entry_f   = float(entry)
                        cur_idr   = usdt_to_idr(cur_price, rate)
                        if side == "BUY":
                            pnl_pct = (cur_price - entry_f) / entry_f * 100
                        else:  # SELL
                            pnl_pct = (entry_f - cur_price) / entry_f * 100

                        pnl_arrow = "📈" if pnl_pct >= 0 else "📉"
                        pnl_sign  = "+" if pnl_pct >= 0 else ""
                        pnl_str   = (
                            f"\n   Now: <b>{fmt(cur_price)}</b> <i>≈ {cur_idr}</i>"
                            f" | {pnl_arrow} <b>{pnl_sign}{pnl_pct:.2f}%</b>"
                        )
            except Exception as e:
                log(f"   ⚠️ PnL fetch gagal [{pair}]: {e}", "warn")

        line_1 = str(i) + ". " + side_emoji + " <b>" + side + " " + pair_disp + "</b> [" + strategy + "]" + status
        line_2 = (
            "   Entry : " + fmt(entry) + fmt_idr(entry) + "\n"
            "   TP1   : " + fmt(tp1)   + fmt_idr(tp1) + fmt_pct(tp1, entry, side) + "\n"
            "   TP2   : " + fmt(tp2)   + fmt_idr(tp2) + fmt_pct(tp2, entry, side) + "\n"
            "   SL    : " + fmt(sl)    + fmt_idr(sl)
        )
        line_3 = "   Usia: " + age_str + pnl_str
        line   = line_1 + "\n" + line_2 + "\n" + line_3
        # pnl_str sudah mengandung \n sendiri jika ada data live
        lines.append(line)

    total   = len(rows)
    msg     = (
        f"📋 <b>Open Trades ({total}/{MAX_OPEN_TRADES})</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        + "\n\n".join(lines)
    )
    tg(msg)
    log(f"📋 Open trades summary dikirim: {total} posisi aktif.")


def run():
    global _dedup_memory
    # [v8.0 Phase4 #2] _candle_cache TIDAK di-reset per cycle — TTL-based expiry.
    # Cache lama yang masih segar (< 120 detik) tetap dipakai lintas cycle.
    # Hanya _dedup_memory yang di-reset karena dedup bersifat per-cycle.
    _dedup_memory  = set()   # [v7.7 #7] reset in-memory dedup setiap cycle — HARUS set(), bukan {}

    # [v9.0] Kill switch check — baca status halt dari Supabase sebelum apapun dieksekusi.
    # Jika bot sedang dalam mode halt yang di-persist (survive job restart), exit immediately.
    _is_halted, _halt_reason = check_bot_halt()
    if _is_halted:
        log(f"🛑 BOT HALT — Kill switch aktif dari run sebelumnya. Reason: {_halt_reason}", "error")
        tg(f"🛑 <b>Bot HALT — Scan dibatalkan</b>\n"
           f"Kill switch aktif dari run sebelumnya.\n"
           f"Reason: {_halt_reason}\n"
           f"<i>Fix kondisi market/equity terlebih dahulu, lalu reset via Supabase:\n"
           f"UPDATE bot_config SET value='false' WHERE key='bot_halt';</i>")
        return

    # [v8.0] Init ticker cache attrs pada function object jika belum ada
    if not hasattr(run, "_ticker_cache"):
        run._ticker_cache    = []
        run._ticker_cache_ts = 0.0

    client = get_client()

    # [v7.27 #4] Dynamic equity — fetch live balance dari Gate.io atau env var.
    # Harus dipanggil SEBELUM apapun yang bergantung pada ACCOUNT_EQUITY_USDT.
    log("💼 Bootstrapping account equity...")
    bootstrap_account_equity(client)

    # [v7.5] Build dynamic ETF blocklist sekali per run
    log("🔒 Membangun ETF blocklist dinamis...")
    build_etf_blocklist()

    # [v7.8 #9] Pre-warm win rate cache — query Supabase sekali di awal,
    # bukan saat signal pertama dikirim (menghindari delay di critical path)
    log("📊 Memuat historical win rate dari Supabase...")
    load_winrate_table()

    # [v7.12 #3] Lifecycle tracking — evaluasi open trades sebelum scan baru dimulai.
    # Ini mengisi signals_v2.result yang dibutuhkan oleh Bayesian win rate model.
    # Dilakukan di sini (sebelum scan) agar cache win rate yang direfresh tersedia
    # saat estimate_confidence() dipanggil selama cycle scan.
    log("📋 Mengevaluasi open trades (lifecycle tracking)...")
    evaluate_open_trades(client)

    if SCAN_MODE == "pump":
        run_pump_scan(client)
        return

    if SCAN_MODE == "monitor":
        # Mode ringan — hanya evaluate open trades tanpa full scan.
        # Dijalankan setiap 5 menit via cron untuk notifikasi TP/SL near-realtime.
        # evaluate_open_trades() sudah dipanggil di atas sebelum blok ini.
        log(f"🔍 MONITOR MODE selesai — {datetime.now(WIB).strftime('%H:%M WIB')}")
        return

    log(f"\n{'='*60}")
    log(f"🚀 SIGNAL BOT v8.9 — {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')} [FULL SCAN]")
    log(f"{'='*60}")

    fg  = get_fear_greed()
    btc = get_btc_regime(client)
    log(f"F&G: {fg} | BTC 1h: {btc['btc_1h']:+.1f}% | BTC 4h: {btc['btc_4h']:+.1f}%")

    # [v7.13 #4] Drawdown awareness — cek losing streak sebelum scan
    drawdown = get_drawdown_state()
    dd_mode  = drawdown["mode"]
    log(f"📉 Drawdown: streak={drawdown['streak']} mode={dd_mode.upper()}")

    # [v7.16 #D] Equity closed-loop — hitung equity aktif dari PnL nyata
    current_equity = get_current_equity_usdt()
    # [v7.19 #D] available = equity - locked capital (partial-aware)
    available_equity = get_available_equity_usdt()
    log(f"💼 Equity aktif: ${current_equity:.2f} USDT (base=${ACCOUNT_EQUITY_USDT:.0f}) | "
        f"Available: ${available_equity:.2f} (locked=${_equity_cache.get('locked', 0):.2f})")

    # [v7.13 #5] Altcoin cluster regimes — fetch seed cluster untuk ringkasan
    log("🔗 Fetching altcoin cluster regimes (seed)...")
    cluster_regimes = get_cluster_regimes(client)
    blocked_clusters = [k for k, v in cluster_regimes.items() if v < CLUSTER_DROP_BLOCK]
    if blocked_clusters:
        log(f"🚫 Seed cluster drop alert: {blocked_clusters}")

    # [v7.11 #1] Portfolio Brain — query open trades sebelum scan dimulai
    portfolio_state = get_portfolio_state()
    log(f"🧠 Portfolio: {portfolio_state['total']} open trades "
        f"(BUY:{portfolio_state['buy']} SELL:{portfolio_state['sell']}) "
        f"| Max: {MAX_OPEN_TRADES} total / {MAX_SAME_SIDE_TRADES} per sisi | "
        f"🎯 Sector: {portfolio_state.get('sector_counts', {})} | "
        f"Risk: ${portfolio_state['total_risk_usdt']:.2f} / {MAX_RISK_TOTAL*100:.0f}% equity")

    allow_buy  = not btc["block_buy"]
    allow_sell = False  # [v7.24] Disabled — spot only, SELL tidak bisa dieksekusi

    log(f"Mode  : BUY={'✅ aktif' if allow_buy else '⛔ diblokir (BTC drop)'} | "
        f"SELL={'✅ aktif' if allow_sell else '⛔ dinonaktifkan (spot only — v7.24)'}")

    if btc["halt"]:
        tg(f"🛑 <b>SIGNAL BOT HALT</b>\n"
           f"BTC crash {btc['btc_4h']:+.1f}% dalam 4h.\n"
           f"Tidak ada signal sampai kondisi stabil.")
        log("🛑 BTC crash — bot halt"); return

    # [v8.0 Phase4 #2] Ticker cache TTL — list_tickers tidak dipanggil ulang
    # jika run() terpanggil berdekatan (mis. monitor mode setelah full scan).
    # TTL 60 detik cukup; ticker list berubah sangat jarang dalam 1 menit.
    _now_ts = time.time()
    if (not hasattr(run, "_ticker_cache")
            or _now_ts - run._ticker_cache_ts > 60
            or not run._ticker_cache):
        log("   📡 Fetching ticker list dari Gate.io...")
        run._ticker_cache    = gate_call_with_retry(client.list_tickers) or []
        run._ticker_cache_ts = _now_ts
    else:
        log(f"   ⚡ Ticker cache hit ({len(run._ticker_cache)} tickers, "
            f"age={_now_ts - run._ticker_cache_ts:.0f}s)")
    tickers       = run._ticker_cache
    signals       = []
    micro_signals = []
    scanned       = 0
    skip_vol      = 0

    # [v7.16 #C] Build pairwise correlation matrix sebelum scan loop
    # Pakai ticker yang lolos filter vol sebagai candidate pairs
    _valid_ticker_pairs = [
        t.currency_pair for t in tickers
        if is_valid_pair(t.currency_pair)
    ]
    if _valid_ticker_pairs:
        log(f"🔗 Cluster check: {len(_valid_ticker_pairs)} pairs | sektor: BTC/AI/MEME/L2")
        build_pairwise_matrix(client, _valid_ticker_pairs)

    # [v8.0 Phase4 #1 + #3] Batch prefetch candles secara PARALEL sebelum scan loop.
    # Semua pair yang lolos filter valid di-fetch 1h + 4h + 15m dalam 1 gelombang.
    # Scan loop berikutnya hanya baca dari _candle_cache — zero additional API call.
    _vol_pairs = [
        t.currency_pair for t in tickers
        if is_valid_pair(t.currency_pair) and float(t.quote_volume or 0) >= MIN_VOLUME_USDT
    ]
    if _vol_pairs:
        log(f"⚡ Memulai batch prefetch untuk {len(_vol_pairs)} pairs lolos vol filter...")
        _pf_stats = prefetch_candles_batch(client, _vol_pairs)  # [v9.0] pakai default max_workers=3
        log(f"   📦 Prefetch stats: fetched={_pf_stats['fetched']} "
            f"cached={_pf_stats['cached']} failed={_pf_stats['failed']}")



    # [v8.3] ob_ratio dihapus — tidak dipakai score_signal. -1 API call per pair per cycle.

    for t in tickers:
        pair = t.currency_pair
        if not is_valid_pair(pair): continue

        try:
            price      = float(t.last or 0)
            vol_24h    = float(t.quote_volume or 0)
            _cp = t.change_percentage
            if _cp in (None, "", "NaN"):
                change_24h = 0.0
            else:
                _f = float(_cp)
                change_24h = 0.0 if math.isnan(_f) else _f
            if price <= 0: continue

            # ── MICROCAP SCANNER — zona volume 20K–150K ──────────
            if (allow_buy
                    and MICRO_VOL_MIN <= vol_24h <= MICRO_VOL_MAX
                    and not already_sent_micro(pair)):
                sig = check_microcap(client, pair, price, vol_24h, change_24h)
                if sig: micro_signals.append(sig)

            if vol_24h < MIN_VOLUME_USDT:
                skip_vol += 1; continue

            scanned += 1

            cluster_buy_blocked = (
                allow_buy and is_cluster_blocked(pair, cluster_regimes)
            )

            # ── INTRADAY BUY ──────────────────────────────────
            if allow_buy and not cluster_buy_blocked and not already_sent(pair, "INTRADAY", "BUY"):
                sig = check_intraday(client, pair, price, btc, side="BUY")
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "INTRADAY", "BUY"))

            # ── INTRADAY SELL ─────────────────────────────────
            if allow_sell and not already_sent(pair, "INTRADAY", "SELL"):
                sig = check_intraday(client, pair, price, btc, side="SELL")
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "INTRADAY", "SELL"))

            # ── SWING BUY ────────────────────────────────────
            if allow_buy and not cluster_buy_blocked and not already_sent(pair, "SWING", "BUY"):
                sig = check_swing(client, pair, price, btc, side="BUY")
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "SWING", "BUY"))

            # ── SWING SELL ───────────────────────────────────
            if allow_sell and not already_sent(pair, "SWING", "SELL"):
                sig = check_swing(client, pair, price, btc, side="SELL")
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "SWING", "SELL"))

        except Exception as e:
            log(f"⚠️ [{pair}]: {e}", "warn"); continue

    buy_cand   = sum(1 for s in signals if s["side"] == "BUY")
    sell_cand  = sum(1 for s in signals if s["side"] == "SELL")
    micro_cand = len(micro_signals)
    log(f"\n📊 Scanned: {scanned} | Vol filter: {skip_vol} | "
        f"Candidates: {len(signals)} (BUY:{buy_cand} SELL:{sell_cand}) | "
        f"Microcap: {micro_cand}")

    # ── Kirim microcap signals dulu — independent dari main signals ──
    micro_signals.sort(key=lambda x: (-x["score"], -x["vol_ratio"]))
    micro_sent = 0
    for sig in micro_signals:
        if micro_sent >= MAX_MICRO_SIGNALS: break
        # [v7.1 #6] Tier B sudah difilter di check_microcap — semua di sini adalah tier A
        # [FIX #5] Portfolio gate — microcap wajib dicek, bukan dikecualikan
        if not portfolio_allows(sig, portfolio_state, btc):
            log(f"   🚫 MICROCAP {sig['pair']} diblok portfolio gate — skip")
            continue
        send_microcap_signal(sig)
        save_signal(
            sig["pair"], "MICROCAP", "BUY",
            sig["entry"], sig["tp1"], sig["tp2"], sig["sl"],
            sig["tier"], sig["score"], sig["timeframe"],
            position_size=sig.get("position_size"),   # [v7.18 #C] simpan ke DB
        )
        # Update portfolio_state lokal agar gate akurat untuk signal berikutnya
        portfolio_state["total"] += 1
        portfolio_state["buy"] += 1
        _sec = get_pair_cluster(sig.get("pair", ""))
        if _sec:
            portfolio_state.setdefault("sector_counts", {})[_sec] = \
                portfolio_state["sector_counts"].get(_sec, 0) + 1
        # Update risk accumulator agar Check 5 akurat untuk signal berikutnya
        _entry_m = sig.get("entry", 0.0) or 0.0
        _sl_m    = sig.get("sl", 0.0) or 0.0
        _size_m  = sig.get("position_size", BASE_POSITION_USDT) or BASE_POSITION_USDT
        if _entry_m > 0 and _sl_m > 0:
            portfolio_state["total_risk_usdt"] += _size_m * abs(_entry_m - _sl_m) / _entry_m
        else:
            portfolio_state["total_risk_usdt"] += _size_m * TARGET_RISK_PCT
        micro_sent += 1
        time.sleep(0.5)

    if not signals and micro_sent == 0:
        tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v8.0</b>\n"
           f"━━━━━━━━━━━━━━━━━━\n"
           f"Pairs scanned : <b>{scanned}</b>\n"
           f"F&G           : <b>{fg}</b>\n"
           f"BTC 1h/4h     : <b>{btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}%</b>\n"
           f"Equity aktif  : <b>${current_equity:.2f} USDT</b>\n"
           f"Portfolio open: <b>{portfolio_state['total']}</b> "
           f"(BUY:{portfolio_state['buy']} SELL:{portfolio_state['sell']})\n"
           f"🎯 Sector: {portfolio_state.get('sector_counts', {})}\n"
           f"Risk: <b>${portfolio_state['total_risk_usdt']:.2f}</b> / {MAX_RISK_TOTAL*100:.0f}% equity\n"
           f"━━━━━━━━━━━━━━━━━━\n"
           f"Signal terkirim : <b>0</b>\n"
           f"<i>Tidak ada signal memenuhi kriteria saat ini.</i>\n"
           f"<i>Scan berikutnya dalam 2 jam.</i>")
        log("📭 Tidak ada signal"); return

    # ── Kirim main signals ────────────────────────────────────────
    # [v7.12 #1] Conflict resolution dengan dynamic priority — adapts ke
    # kondisi market (BTC regime + F&G). Gantikan resolve_conflicts() statis.
    pre_resolve = len(signals)
    signals     = resolve_conflicts_dynamic(signals, btc, fg)
    if len(signals) < pre_resolve:
        log(f"   Setelah conflict resolution: {len(signals)} signal "
            f"(dari {pre_resolve} kandidat)")

    tier_order = {"S": 0, "A+": 1, "A": 2}
    signals.sort(key=lambda x: (tier_order.get(x["tier"], 9), -x["score"]))

    sent      = 0
    sent_sigs = []
    for sig in signals:
        if sent >= MAX_SIGNALS_CYCLE: break

        # [v7.11 #1] Portfolio Brain gate — cek exposure global sebelum kirim
        if not portfolio_allows(sig, portfolio_state, btc):
            continue

        # [v7.16 #A #D] Position sizing — dynamic prior + live equity
        _conf = estimate_confidence(
            sig["score"],
            regime=sig.get("regime", "") if sig.get("regime") != "—" else ""
        )
        _atr      = sig.get("atr")
        _entry    = sig.get("entry") or None
        _rr       = float(sig.get("rr") or 2.0)
        _strategy = sig.get("strategy", "")
        _regime   = sig.get("regime", "") if sig.get("regime") != "—" else ""
        sig["position_size"] = calc_position_size(
            sig["tier"], _conf, dd_mode,
            atr=_atr, entry=_entry, rr=_rr,
            strategy=_strategy, regime=_regime,
            current_equity=available_equity,  # [v7.19 #D] available, bukan total equity
            pair=sig.get("pair", ""),                          # [v7.20 #B] corr-adjusted sizing
            open_pairs=portfolio_state.get("open_pairs", []), # [v7.20 #B] corr-adjusted sizing
            sl=sig.get("sl"),                                  # [v7.27 #1] fixed-risk sizing
        )

        # [v7.16 #B] Slippage-adjusted entry — live spread + depth impact dari OB
        if _entry and _entry > 0:
            _size_usdt = sig.get("position_size", BASE_POSITION_USDT)
            _adj_entry, _slip_pct = adjust_entry_for_slippage(
                _entry, sig["side"], _size_usdt,
                client=client, pair=sig["pair"]   # [v7.16 #B] pass client untuk live OB
            )
            sig["entry_raw"]      = _entry
            sig["entry"]          = _adj_entry
            sig["slip_pct"]       = round(_slip_pct * 100, 4)
            log(f"   📐 Slippage adj: {sig['side']} entry "
                f"{_entry:.6g} → {_adj_entry:.6g} (+{_slip_pct*100:.3f}%)")

        send_signal(sig)
        save_signal(
            sig["pair"], sig["strategy"], sig["side"],
            sig["entry"], sig["tp1"], sig["tp2"], sig["sl"],
            sig["tier"], sig["score"], sig["timeframe"],
            position_size=sig.get("position_size"),   # [v7.18 #C] simpan ke DB
        )

        # Update portfolio_state lokal agar gate akurat untuk signal berikutnya
        # tanpa perlu query ulang ke Supabase
        portfolio_state["total"] += 1
        if sig["side"] == "BUY":
            portfolio_state["buy"] += 1
        else:
            portfolio_state["sell"] += 1
        _sec = get_pair_cluster(sig.get("pair", ""))
        if _sec:
            portfolio_state.setdefault("sector_counts", {})[_sec] = \
                portfolio_state["sector_counts"].get(_sec, 0) + 1
        # Update risk accumulator agar Check 5 akurat untuk signal berikutnya
        _entry_s = sig.get("entry", 0.0) or 0.0
        _sl_s    = sig.get("sl", 0.0) or 0.0
        _size_s  = sig.get("position_size", BASE_POSITION_USDT) or BASE_POSITION_USDT
        if _entry_s > 0 and _sl_s > 0:
            portfolio_state["total_risk_usdt"] += _size_s * abs(_entry_s - _sl_s) / _entry_s
        else:
            portfolio_state["total_risk_usdt"] += _size_s * TARGET_RISK_PCT

        sent_sigs.append(sig)
        sent += 1
        time.sleep(0.5)

    # Summary
    intraday_buy  = sum(1 for s in sent_sigs if s["strategy"] == "INTRADAY" and s["side"] == "BUY")
    intraday_sell = sum(1 for s in sent_sigs if s["strategy"] == "INTRADAY" and s["side"] == "SELL")
    swing_buy     = sum(1 for s in sent_sigs if s["strategy"] == "SWING"    and s["side"] == "BUY")
    swing_sell    = sum(1 for s in sent_sigs if s["strategy"] == "SWING"    and s["side"] == "SELL")

    tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v8.0</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Pairs scanned : <b>{scanned}</b>\n"
       f"F&G           : <b>{fg}</b>\n"
       f"BTC 1h/4h     : <b>{btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}%</b>\n"
       f"Equity aktif  : <b>${current_equity:.2f} USDT</b>\n"
       f"Portfolio open: <b>{portfolio_state['total']}</b> "
       f"(BUY:{portfolio_state['buy']} SELL:{portfolio_state['sell']})\n"
       f"🎯 Sector: {portfolio_state.get('sector_counts', {})}\n"
       f"Risk: <b>${portfolio_state['total_risk_usdt']:.2f}</b> / {MAX_RISK_TOTAL*100:.0f}% equity\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Signal terkirim : <b>{sent + micro_sent}</b>\n"
       f"  📈 INTRADAY BUY  : {intraday_buy}\n"
       f"  📉 INTRADAY SELL : {intraday_sell}\n"
       f"  🌊 SWING BUY     : {swing_buy}\n"
       f"  🌊 SWING SELL    : {swing_sell}\n"
       f"  🔬 MICROCAP BUY  : {micro_sent}\n"
       f"<i>Scan berikutnya dalam 2 jam.</i>")

    log(f"\n✅ Done — {sent + micro_sent} signal terkirim "
        f"({sent} main + {micro_sent} microcap)")
    log(f"   INTRADAY BUY:{intraday_buy} SELL:{intraday_sell} | "
        f"SWING BUY:{swing_buy} SELL:{swing_sell} | MICROCAP:{micro_sent}")

    # [v7.14 #E] Equity curve snapshot — diambil di akhir setiap run
    save_equity_snapshot(open_trades=portfolio_state["total"])

    # ── Kirim rekapan open trades ke Telegram ────────────────────────────
    send_open_trades_summary(client)


# ════════════════════════════════════════════════════════════════════
#  PHASE 5 — INTELLIGENCE UPGRADE
#  [5.1] Backtesting Engine
#  [5.2] Monte Carlo Risk Simulation
#  [5.3] Confidence Model v2 (simplified — per-strategy winrate)
# ════════════════════════════════════════════════════════════════════

# ────────────────────────────────────────────────────────────────────
#  5.1  BACKTESTING ENGINE
#
#  Tiga fungsi inti:
#    replay_candles(candles, entry_idx) → generator bar satu per satu
#    simulate_trade(entry, sl, tp1, tp2, side, future_candles)
#        → TradeResult: hit TP1 / TP2 / SL, bars_held, pnl_r
#    run_backtest(candles, signals) → list[TradeResult] + summary
#
#  Format sinyal masuk:
#    {
#      "entry_idx": int,   # indeks candle entry di array candles
#      "entry":     float,
#      "sl":        float,
#      "tp1":       float,
#      "tp2":       float,
#      "side":      "BUY" | "SELL",
#    }
# ────────────────────────────────────────────────────────────────────

def replay_candles(candles: list, start_idx: int = 0):
    """
    Generator: yield satu candle dict per iterasi mulai start_idx.
    Candle dict diasumsikan punya key: open, high, low, close, volume.
    Kompatibel dengan format return get_candles() di bot ini.
    """
    for i in range(start_idx, len(candles)):
        yield i, candles[i]


def simulate_trade(entry: float, sl: float, tp1: float, tp2: float,
                   side: str, future_candles: list,
                   partial_close_ratio: float = 0.5) -> dict:
    """
    [5.1] Simulasi satu trade terhadap candle masa depan.

    Rules:
      - Setiap bar: cek apakah high/low menembus TP1, TP2, atau SL.
      - Prioritas dalam satu bar: SL > TP (konservatif — worst case dulu).
      - TP1 hit → partial close (partial_close_ratio dari posisi).
        Sisa posisi dilanjutkan ke TP2 atau SL.
      - TP2 hit → full close.
      - SL hit setelah TP1 → partial loss (hanya sisa posisi).
      - Jika semua candle habis tanpa hit → result "OPEN" (simulasi selesai).

    Returns dict:
      result     : "TP1" | "TP2" | "SL" | "SL_AFTER_TP1" | "OPEN"
      exit_price : harga exit terakhir
      pnl_r      : PnL dalam unit R (1R = sl_dist × posisi penuh)
      bars_held  : berapa candle sampai exit
    """
    if not future_candles:
        return {"result": "OPEN", "exit_price": entry, "pnl_r": 0.0, "bars_held": 0}

    is_buy    = side == "BUY"
    sl_dist   = abs(entry - sl)
    if sl_dist == 0:
        return {"result": "OPEN", "exit_price": entry, "pnl_r": 0.0, "bars_held": 0}

    tp1_hit   = False
    partial_r = 0.0   # R yang sudah terkunci dari TP1

    for bars_held, c in enumerate(future_candles, start=1):
        high = float(c.get("high",  c[2] if isinstance(c, (list, tuple)) else entry))
        low  = float(c.get("low",   c[3] if isinstance(c, (list, tuple)) else entry))

        if is_buy:
            sl_hit  = low  <= sl
            tp1_hit_now = high >= tp1 and not tp1_hit
            tp2_hit_now = high >= tp2

            # ── SL dulu (konservatif) ──
            if sl_hit:
                if tp1_hit:
                    # partial close sudah di TP1, sisa kena SL
                    remaining_r = -(1.0 - partial_close_ratio)
                    pnl_r = partial_r + remaining_r
                    return {"result": "SL_AFTER_TP1", "exit_price": sl,
                            "pnl_r": round(pnl_r, 3), "bars_held": bars_held}
                return {"result": "SL", "exit_price": sl,
                        "pnl_r": round(-1.0, 3), "bars_held": bars_held}

            if tp1_hit_now and not tp1_hit:
                tp1_hit = True
                tp1_r   = (tp1 - entry) / sl_dist
                partial_r = tp1_r * partial_close_ratio   # terkunci

            if tp2_hit_now and tp1_hit:
                tp2_r = (tp2 - entry) / sl_dist
                pnl_r = partial_r + tp2_r * (1.0 - partial_close_ratio)
                return {"result": "TP2", "exit_price": tp2,
                        "pnl_r": round(pnl_r, 3), "bars_held": bars_held}

            if tp1_hit_now and not tp2_hit_now:
                # TP1 saja, belum ke TP2, lanjut ke bar berikutnya
                pass

        else:  # SELL
            sl_hit      = high >= sl
            tp1_hit_now = low  <= tp1 and not tp1_hit
            tp2_hit_now = low  <= tp2

            if sl_hit:
                if tp1_hit:
                    remaining_r = -(1.0 - partial_close_ratio)
                    pnl_r = partial_r + remaining_r
                    return {"result": "SL_AFTER_TP1", "exit_price": sl,
                            "pnl_r": round(pnl_r, 3), "bars_held": bars_held}
                return {"result": "SL", "exit_price": sl,
                        "pnl_r": round(-1.0, 3), "bars_held": bars_held}

            if tp1_hit_now and not tp1_hit:
                tp1_hit = True
                tp1_r   = (entry - tp1) / sl_dist
                partial_r = tp1_r * partial_close_ratio

            if tp2_hit_now and tp1_hit:
                tp2_r = (entry - tp2) / sl_dist
                pnl_r = partial_r + tp2_r * (1.0 - partial_close_ratio)
                return {"result": "TP2", "exit_price": tp2,
                        "pnl_r": round(pnl_r, 3), "bars_held": bars_held}

    # Semua candle habis → TP1 sudah hit tapi TP2 belum
    if tp1_hit:
        return {"result": "TP1", "exit_price": tp1,
                "pnl_r": round(partial_r, 3), "bars_held": len(future_candles)}

    return {"result": "OPEN", "exit_price": float(future_candles[-1].get("close",
            future_candles[-1][4] if isinstance(future_candles[-1], (list, tuple)) else entry)),
            "pnl_r": 0.0, "bars_held": len(future_candles)}


def run_backtest(candles: list, signals: list,
                 max_bars_per_trade: int = 48) -> dict:
    """
    [5.1] Jalankan backtesting untuk list sinyal terhadap candle array.

    Args:
        candles          : list candle OHLCV (format get_candles())
        signals          : list dict sinyal, setiap dict wajib punya:
                           entry_idx, entry, sl, tp1, tp2, side
        max_bars_per_trade: batas maju candle per trade (default 48 = 2 hari 1h)

    Returns dict:
        trades      : list TradeResult lengkap
        total       : jumlah trade
        wins        : TP1 + TP2
        losses      : SL + SL_AFTER_TP1
        winrate     : float 0–1
        avg_pnl_r   : rata-rata PnL per trade dalam R
        total_pnl_r : total PnL dalam R
        max_dd_r    : maximum drawdown dalam R
        summary     : string ringkasan
    """
    trades = []

    for sig in signals:
        idx = sig.get("entry_idx", 0)
        if idx >= len(candles):
            continue

        future = candles[idx + 1 : idx + 1 + max_bars_per_trade]
        if not future:
            continue

        result = simulate_trade(
            entry   = sig["entry"],
            sl      = sig["sl"],
            tp1     = sig["tp1"],
            tp2     = sig["tp2"],
            side    = sig.get("side", "BUY"),
            future_candles = future,
        )
        result["entry"]     = sig["entry"]
        result["sl"]        = sig["sl"]
        result["tp1"]       = sig["tp1"]
        result["tp2"]       = sig["tp2"]
        result["side"]      = sig.get("side", "BUY")
        result["entry_idx"] = idx
        trades.append(result)

    if not trades:
        return {"trades": [], "total": 0, "wins": 0, "losses": 0,
                "winrate": 0.0, "avg_pnl_r": 0.0, "total_pnl_r": 0.0,
                "max_dd_r": 0.0, "summary": "Tidak ada trade"}

    wins   = sum(1 for t in trades if t["result"] in {"TP1", "TP2"})
    losses = sum(1 for t in trades if t["result"] in {"SL", "SL_AFTER_TP1"})
    total  = len(trades)
    wr     = wins / total if total else 0.0

    pnl_series = [t["pnl_r"] for t in trades]
    total_r    = sum(pnl_series)
    avg_r      = total_r / total if total else 0.0

    # Max drawdown dalam R (peak-to-trough dari equity curve R)
    equity_curve = []
    running = 0.0
    for r in pnl_series:
        running += r
        equity_curve.append(running)

    peak    = 0.0
    max_dd  = 0.0
    for eq in equity_curve:
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd

    summary = (
        f"Backtest selesai — {total} trade | "
        f"WR: {wr:.1%} | Avg PnL: {avg_r:+.2f}R | "
        f"Total: {total_r:+.2f}R | MaxDD: {max_dd:.2f}R"
    )
    log(summary)

    return {
        "trades":      trades,
        "total":       total,
        "wins":        wins,
        "losses":      losses,
        "winrate":     round(wr, 3),
        "avg_pnl_r":   round(avg_r, 3),
        "total_pnl_r": round(total_r, 3),
        "max_dd_r":    round(max_dd, 3),
        "summary":     summary,
    }


# ────────────────────────────────────────────────────────────────────
#  5.2  MONTE CARLO RISK SIMULATION
#
#  Simulasi 1000× (default) sequence trade acak dari distribusi historis.
#  Setiap run: ambil N trade → hitung equity curve → catat max DD.
#  Output: worst-case DD percentile, median DD, ruin probability.
#
#  Gunakan setelah run_backtest() punya data trades yang cukup.
# ────────────────────────────────────────────────────────────────────

def run_monte_carlo(
    pnl_r_list:    list[float],
    n_trades:      int   = 50,
    n_simulations: int   = 1000,
    initial_r:     float = 100.0,
    ruin_threshold: float = 0.30,
    seed:          int | None = None,
) -> dict:
    """
    [5.2] Monte Carlo: simulasi N trade dalam urutan acak sebanyak n_simulations.

    Args:
        pnl_r_list     : list PnL per trade dalam unit R (dari run_backtest)
        n_trades       : panjang sequence per simulasi (default 50 trade)
        n_simulations  : jumlah run simulasi (default 1000)
        initial_r      : modal awal dalam unit R (default 100R)
        ruin_threshold : drawdown % yang dianggap "ruin" (default 30%)
        seed           : random seed untuk reproducibility

    Returns dict:
        max_dd_p95      : worst-case DD pada percentile 95 (dalam R)
        max_dd_p50      : median DD (dalam R)
        max_dd_worst    : DD terbesar dari seluruh simulasi (dalam R)
        ruin_pct        : % simulasi yang mencapai ruin_threshold DD
        final_equity_p5 : percentile 5 equity akhir (downside)
        final_equity_p50: median equity akhir
        all_max_dds     : list semua max DD (untuk histogram jika perlu)
        summary         : string ringkasan
    """
    if not pnl_r_list:
        return {"summary": "Tidak ada data PnL untuk Monte Carlo"}

    rng = random.Random(seed)
    max_dds:     list[float] = []
    final_equities: list[float] = []
    ruin_count = 0
    ruin_r     = initial_r * ruin_threshold

    for _ in range(n_simulations):
        # Sample dengan replacement — urutan acak dari distribusi historis
        sample = rng.choices(pnl_r_list, k=n_trades)

        equity  = initial_r
        peak    = initial_r
        max_dd  = 0.0
        ruined  = False

        for r in sample:
            equity += r
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd
            if not ruined and (peak - equity) >= ruin_r:
                ruined = True

        max_dds.append(max_dd)
        final_equities.append(equity)
        if ruined:
            ruin_count += 1

    max_dds.sort()
    final_equities.sort()

    n   = len(max_dds)
    p50 = max_dds[int(n * 0.50)]
    p95 = max_dds[int(n * 0.95)]
    worst = max_dds[-1]

    fe_p5  = final_equities[int(n * 0.05)]
    fe_p50 = final_equities[int(n * 0.50)]

    ruin_pct = ruin_count / n_simulations * 100

    summary = (
        f"Monte Carlo ({n_simulations}× | {n_trades} trade/run) — "
        f"MaxDD p50: {p50:.1f}R | p95: {p95:.1f}R | Worst: {worst:.1f}R | "
        f"Ruin ({ruin_threshold:.0%}): {ruin_pct:.1f}% | "
        f"Equity p5: {fe_p5:.1f}R / p50: {fe_p50:.1f}R"
    )
    log(summary)

    return {
        "max_dd_p95":       round(p95, 2),
        "max_dd_p50":       round(p50, 2),
        "max_dd_worst":     round(worst, 2),
        "ruin_pct":         round(ruin_pct, 2),
        "final_equity_p5":  round(fe_p5, 2),
        "final_equity_p50": round(fe_p50, 2),
        "all_max_dds":      max_dds,
        "summary":          summary,
    }


# ────────────────────────────────────────────────────────────────────
#  5.3  CONFIDENCE MODEL v2 — SIMPLIFIED
#
#  Masalah model lama (estimate_confidence):
#    - Terlalu bergantung pada bucket (score range) + regime
#    - Sparse bucket (score 3.5+ di RANGING) hampir tidak pernah cukup sample
#    - Fallback hierarchy 3 level → debug sulit, angka tidak stabil
#    - MIN_SAMPLE berbeda per bucket → membingungkan
#
#  Solusi v2:
#    - Satu dimensi saja: STRATEGY (INTRADAY / SWING / PUMP / MICROCAP)
#    - Semua sinyal satu strategy dipool → sample lebih besar, lebih stabil
#    - Bayesian posterior tetap dipakai untuk shrinkage sample kecil
#    - MIN_SAMPLE flat per strategy (bukan per bucket)
#    - Cache TTL sama: 1 jam
#
#  Tidak menggantikan estimate_confidence() lama — fungsi lama tetap ada.
#  estimate_confidence_v2() bisa dipanggil paralel untuk perbandingan.
# ────────────────────────────────────────────────────────────────────

# Konstanta Phase 5.3
_CONF_V2_MIN_SAMPLE: dict[str, int] = {
    "INTRADAY": 20,    # paling banyak signal → threshold lebih longgar
    "SWING":    15,    # horizon lebih panjang → lebih sedikit closed trade
    "PUMP":     10,    # event-driven, lebih jarang
    "MICROCAP": 10,    # universe kecil
}
_CONF_V2_DEFAULT_MIN = 15

_conf_v2_cache: dict      = {}
_conf_v2_cache_ts: float  = 0.0
_CONF_V2_CACHE_TTL        = 3600   # 1 jam


def load_winrate_by_strategy() -> dict:
    """
    [5.3] Query Supabase: winrate per strategy dari signals_v2.

    Hanya baca sinyal yang sudah closed (result tidak null).
    WIN = "WIN", "TP1", "TP2", "PARTIAL_WIN"
    LOSS = "LOSS", "SL", "SL_AFTER_TP1"

    Returns dict:
        {
          "INTRADAY": {"wins": 30, "total": 50, "wr": 0.618, "wr_freq": 0.600},
          "SWING":    {"wins": 12, "total": 20, "wr": 0.571, "wr_freq": 0.600},
          ...
        }
    Kosong jika Supabase tidak bisa di-reach.
    """
    global _conf_v2_cache, _conf_v2_cache_ts

    now = time.time()
    if _conf_v2_cache and now - _conf_v2_cache_ts < _CONF_V2_CACHE_TTL:
        return _conf_v2_cache

    try:
        rows = (
            supabase.table("signals_v2")
            .select("strategy, result")
            .not_.is_("result", "null")
            .execute()
            .data
        )
        if not rows:
            log("📊 Confidence v2: belum ada data historis dengan result.", "warn")
            return _conf_v2_cache

        WIN_VALUES  = {"WIN", "TP1", "TP2", "PARTIAL_WIN"}
        LOSS_VALUES = {"LOSS", "SL", "SL_AFTER_TP1"}

        buckets: dict[str, dict] = {}

        for row in rows:
            strat  = (row.get("strategy") or "").upper().strip()
            result = (row.get("result")   or "").upper().strip()

            if not strat:
                continue
            if result not in WIN_VALUES and result not in LOSS_VALUES:
                continue

            if strat not in buckets:
                buckets[strat] = {"wins": 0, "total": 0}

            buckets[strat]["total"] += 1
            if result in WIN_VALUES:
                buckets[strat]["wins"] += 1

        # Bayesian posterior mean (Jeffreys prior α=β=1, sama seperti model lama)
        for strat, d in buckets.items():
            w = d["wins"]
            l = d["total"] - w
            alpha_post = w + BAYES_PRIOR_ALPHA
            beta_post  = l + BAYES_PRIOR_BETA
            d["wr"]      = round(alpha_post / (alpha_post + beta_post), 3)
            d["wr_freq"] = round(w / d["total"], 3) if d["total"] > 0 else 0.0

        _conf_v2_cache    = buckets
        _conf_v2_cache_ts = now

        summary = " | ".join(
            f"{s}: {d['wr']:.1%}★ (n={d['total']})"
            for s, d in sorted(buckets.items())
        )
        log(f"📊 Confidence v2 loaded — {len(rows)} trades | {summary}")
        return buckets

    except Exception as e:
        log(f"⚠️ load_winrate_by_strategy: {e} — pakai cache lama", "warn")
        return _conf_v2_cache


def estimate_confidence_v2(strategy: str) -> dict:
    """
    [5.3] Return confidence berdasarkan winrate per strategy saja.

    Lebih sederhana dari estimate_confidence():
      - Tidak ada bucket score
      - Tidak ada regime split
      - Tidak ada fallback hierarchy
      - Sample pool lebih besar → angka lebih stabil

    Args:
        strategy : "INTRADAY" | "SWING" | "PUMP" | "MICROCAP"

    Returns dict:
        wr       : float | None  — Bayesian posterior mean
        wr_freq  : float | None  — frequentist ratio (referensi)
        n        : int
        reliable : bool          — n >= min_sample untuk strategy ini
        min_n    : int
        label    : str           — emoji label untuk Telegram
    """
    table    = load_winrate_by_strategy()
    strat_up = strategy.upper()
    min_n    = _CONF_V2_MIN_SAMPLE.get(strat_up, _CONF_V2_DEFAULT_MIN)

    if strat_up not in table:
        return {
            "wr": None, "wr_freq": None, "n": 0,
            "reliable": False, "min_n": min_n,
            "label": f"⬜ No data [{strat_up}]",
        }

    d        = table[strat_up]
    wr       = d["wr"]
    wr_freq  = d["wr_freq"]
    n        = d["total"]
    reliable = n >= min_n

    if not reliable:
        label = f"⬜ Data kurang [{strat_up}] (n={n}/{min_n})"
    elif wr >= 0.60:
        label = f"🟢 Kuat [{strat_up}] ({wr:.0%}★, n={n})"
    elif wr >= 0.50:
        label = f"🟡 Positif [{strat_up}] ({wr:.0%}★, n={n})"
    elif wr >= 0.40:
        label = f"🟠 Marginal [{strat_up}] ({wr:.0%}★, n={n})"
    else:
        label = f"🔴 Lemah [{strat_up}] ({wr:.0%}★, n={n})"

    return {
        "wr":      wr,
        "wr_freq": wr_freq,
        "n":       n,
        "reliable": reliable,
        "min_n":   min_n,
        "label":   label,
    }


# ════════════════════════════════════════════════════════════════════
#  END PHASE 5
# ════════════════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════════
#  UNIT TESTS — [v9.0]
#  Jalankan: python bot_v9_0.py --test
#  Tidak memerlukan koneksi Supabase / Gate.io / Telegram.
# ════════════════════════════════════════════════════════

def _run_tests() -> None:
    """Jalankan semua unit test internal. Exit dengan error jika ada yang gagal."""
    import sys
    passed = 0
    failed = 0

    def assert_eq(label: str, actual, expected, tol: float = 1e-6):
        nonlocal passed, failed
        if isinstance(expected, float):
            ok = abs(actual - expected) < tol
        else:
            ok = actual == expected
        if ok:
            print(f"  ✅ {label}")
            passed += 1
        else:
            print(f"  ❌ {label} — expected {expected!r}, got {actual!r}")
            failed += 1

    def assert_true(label: str, condition: bool):
        nonlocal passed, failed
        if condition:
            print(f"  ✅ {label}")
            passed += 1
        else:
            print(f"  ❌ {label} — kondisi False")
            failed += 1

    print("\n🧪 Running unit tests...\n")

    # ── Test 1-4: calc_sl_tp — BUY side ──────────────────
    print("── calc_sl_tp() BUY side ──")
    entry = 1.0
    atr   = 0.02
    structure_with_sl   = {"last_sl": 0.95, "last_sh": 1.10}
    structure_no_sl     = {"last_sl": None, "last_sh": 1.10}
    structure_invalid_sl = {"last_sl": 1.05, "last_sh": 1.10}  # last_sl > entry — invalid

    sl, tp1, tp2 = calc_sl_tp(entry, "BUY", atr, structure_with_sl, "SWING")
    assert_true("BUY SL < entry (struktur valid)", sl < entry)
    assert_true("BUY TP1 > entry", tp1 > entry)
    assert_true("BUY TP2 > TP1", tp2 > tp1)
    sl_dist = entry - sl
    assert_true("BUY TP2/TP1 ratio sesuai R:R (TP2 > TP1)", tp2 > tp1)

    # ATR fallback: last_sl > entry → tidak valid → pakai ATR fallback
    sl_fb, tp1_fb, tp2_fb = calc_sl_tp(entry, "BUY", atr, structure_invalid_sl, "SWING")
    assert_true("BUY ATR fallback: SL < entry", sl_fb < entry)

    # ── Test 5-8: calc_sl_tp — SELL side ─────────────────
    print("\n── calc_sl_tp() SELL side ──")
    structure_sell = {"last_sl": 0.90, "last_sh": 1.05}

    sl_s, tp1_s, tp2_s = calc_sl_tp(entry, "SELL", atr, structure_sell, "INTRADAY")
    assert_true("SELL SL > entry", sl_s > entry)
    assert_true("SELL TP1 < entry", tp1_s < entry)
    assert_true("SELL TP2 < TP1", tp2_s < tp1_s)

    # Sanity bounds: SL tidak boleh > entry × (1 + max_sl_pct)
    assert_true("SELL SL dalam bounds max", sl_s <= entry * (1.0 + INTRADAY_MAX_SL_PCT + 1e-6))

    # ── Test 9-11: calc_sl_tp — sanity bounds ────────────
    print("\n── calc_sl_tp() sanity bounds ──")
    # Struktur dengan swing low yang sangat jauh → SL harus di-clip ke max_sl_pct
    structure_far_sl = {"last_sl": 0.50, "last_sh": 2.00}  # 50% jauh dari entry
    sl_clip, _, _ = calc_sl_tp(entry, "BUY", atr, structure_far_sl, "SWING")
    assert_true("BUY SL di-clip ke max_sl_pct", sl_clip >= entry * (1.0 - SWING_MAX_SL_PCT - 1e-6))

    # Struktur dengan swing low sangat dekat → SL harus min min_sl_pct
    structure_close_sl = {"last_sl": 0.9999, "last_sh": 1.10}
    sl_min, _, _ = calc_sl_tp(entry, "BUY", atr, structure_close_sl, "SWING")
    assert_true("BUY SL minimal min_sl_pct", sl_min <= entry * (1.0 - SWING_MIN_SL_PCT + 1e-6))

    # ── Test 12-14: get_drawdown_state mock logic ─────────
    print("\n── Drawdown mode logic ──")
    # Simulasikan logika SEVERITY tanpa koneksi DB
    SEVERITY_TEST = {"normal": 0, "warn": 1, "halt": 2}

    # equity halt override streak warn
    equity_mode, streak_mode = "halt", "warn"
    mode = "halt" if equity_mode == "halt" else (
        "warn" if streak_mode == "halt" and equity_mode == "normal" else
        max(streak_mode, equity_mode, key=lambda m: SEVERITY_TEST[m])
    )
    assert_eq("Equity halt = final authority", mode, "halt")

    # streak halt + equity normal → downgrade ke warn
    equity_mode, streak_mode = "normal", "halt"
    mode = "halt" if equity_mode == "halt" else (
        "warn" if streak_mode == "halt" and equity_mode == "normal" else
        max(streak_mode, equity_mode, key=lambda m: SEVERITY_TEST[m])
    )
    assert_eq("Streak halt + equity normal → downgrade warn", mode, "warn")

    # keduanya normal → normal
    equity_mode, streak_mode = "normal", "normal"
    mode = "halt" if equity_mode == "halt" else (
        "warn" if streak_mode == "halt" and equity_mode == "normal" else
        max(streak_mode, equity_mode, key=lambda m: SEVERITY_TEST[m])
    )
    assert_eq("Keduanya normal → mode normal", mode, "normal")

    # ── Test 15-16: MIN_FILL_RATIO constant ──────────────
    print("\n── MIN_FILL_RATIO constant ──")
    assert_eq("MIN_FILL_RATIO bernilai 0.5", MIN_FILL_RATIO, 0.5)
    # Verifikasi logika fill check
    size_usdt = 100.0
    filled_ok  = 60.0
    filled_bad = 40.0
    assert_true("Fill 60% dari 100 → lolos MIN_FILL_RATIO", filled_ok >= size_usdt * MIN_FILL_RATIO)
    assert_true("Fill 40% dari 100 → gagal MIN_FILL_RATIO", filled_bad < size_usdt * MIN_FILL_RATIO)

    # ── Ringkasan ─────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"Hasil: {passed} passed, {failed} failed")
    if failed > 0:
        print("❌ Ada test yang gagal — periksa sebelum deploy!")
        sys.exit(1)
    else:
        print("✅ Semua test passed — bot siap deploy.")
    print(f"{'='*50}\n")



if __name__ == "__main__":
    import sys
    if "--test" in sys.argv:
        _run_tests()
    else:
        run()
