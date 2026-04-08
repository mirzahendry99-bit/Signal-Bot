"""
╔══════════════════════════════════════════════════════════════════╗
║        SIGNAL BOT — STABLE RELEASE v1.5                        ║
║                                                                  ║
║  Upgrade v1.5 — Signal-Only Mode (Pure Signal Bot):            ║
║    NEW 5. SIGNAL_ONLY_MODE = True                                ║
║           Bypass semua portfolio/risk logic                      ║
║           Fokus: kualitas signal + winrate tertinggi             ║
║           Target: 8-15 signal per cycle dari ratusan pairs       ║
║                                                                  ║
║  Upgrade v1.4 — Intelligent Risk & Feedback Systems:           ║
║    NEW 1. Performance Feedback Loop                              ║
║    NEW 2. Adaptive Thresholds                                    ║
║    NEW 3. Capital Allocation per Strategy                        ║
║    NEW 4. Equity Protection System                               ║
║                                                                  ║
║  Upgrade v1.3 — False Positive Reduction & Risk Control:        ║
║    FIX 1. Confirmation Layer (check_confirmation_layer)          ║
║           Gate 2/3 kondisi wajib lolos sebelum sinyal speculative║
║    FIX 2. Priority Queue System (SIGNAL_PRIORITY + slot alloc)  ║
║    FIX 3. Signal slot allocation per bucket (confirmed/spec)     ║
║    FIX 4. Speculative Risk Cap & sizing override                 ║
║           MAX_SPECULATIVE_RISK_PCT + SPECULATIVE_TIER_RISK       ║
║                                                                  ║
║  Dibangun berdasarkan SIGNAL_SPEC v1.0                          ║
║  ARSITEKTUR (layer terpisah):                                    ║
║  L1. CONFIG          — semua parameter di satu tempat           ║
║  L2. DATA LAYER      — fetch + cache candle, ticker, external   ║
║  L3. INDICATOR LAYER — kalkulasi teknikal murni                 ║
║  L4. ENGINE LAYER    — structure, liquidity, entry precision     ║
║  L5. STRATEGY LAYER  — scalping / intraday / swing / moonshot   ║
║  L6. PORTFOLIO LAYER — sizing, exposure, kill switch            ║
║  L7. OUTPUT LAYER    — queue, flush, telegram, supabase         ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os, json, time
import urllib.request
import gate_api
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from supabase import create_client

print("🚀 SIGNAL BOT — STABLE v1.5 | Signal-Only Mode | High Winrate Focus")

# ════════════════════════════════════════════════════════
#  L1. CONFIG — SEMUA PARAMETER DI SINI
#  Tidak ada angka magic di dalam fungsi manapun
# ════════════════════════════════════════════════════════

# ── Environment ──────────────────────────────────────
API_KEY      = os.environ.get("GATE_API_KEY")
SECRET_KEY   = os.environ.get("GATE_SECRET_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TG_TOKEN     = os.environ.get("TELEGRAM_TOKEN")
TG_CHAT_ID   = os.environ.get("CHAT_ID")
WIB          = timezone(timedelta(hours=7))

_missing = [k for k, v in {
    "SUPABASE_URL": SUPABASE_URL, "SUPABASE_KEY": SUPABASE_KEY,
    "TELEGRAM_TOKEN": TG_TOKEN,   "CHAT_ID": TG_CHAT_ID,
}.items() if not v]
if _missing:
    raise EnvironmentError(f"ENV belum diset: {', '.join(_missing)}")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Account ───────────────────────────────────────────
ACCOUNT_BALANCE         = float(os.environ.get("ACCOUNT_BALANCE", "1000"))
MAX_PORTFOLIO_RISK_PCT  = 10.0
MAX_SIGNALS_PER_CYCLE   = 15   # dinaikkan — signal bot bukan trading bot
SUMMARY_HOUR_WIB        = 8

# ── [NEW 5] Signal-Only Mode ──────────────────────────
# True  = pure signal bot, bypass semua portfolio/risk/capital logic
# False = mode hybrid (simulasi portfolio, cocok untuk live trading bot)
SIGNAL_ONLY_MODE        = True

# ── [NEW 5] High Winrate Filter (aktif saat SIGNAL_ONLY_MODE) ────
# Minimum tier yang boleh masuk signal — buang semua tier B
SIGNAL_MIN_TIER         = "A"        # "A", "A+", atau "S"
TIER_ORDER              = {"S": 0, "A+": 1, "A": 2, "B": 3}

# Minimum RR lebih ketat untuk signal bot — hanya signal dengan RR bagus
MIN_RR_SIGNAL = {
    "SCALPING":         {"S": 2.0, "A+": 1.8, "A": 1.8, "B": 1.8},
    "INTRADAY":         {"S": 2.5, "A+": 2.2, "A": 2.2, "B": 2.2},
    "SWING":            {"S": 3.0, "A+": 2.8, "A": 2.8, "B": 2.8},
    "MOONSHOT":         {"S": 2.5, "A+": 2.2, "A": 2.2, "B": 2.2},
    "MOONSHOT_PREPUMP": {"S": 2.5, "A+": 2.2, "A": 2.2, "B": 2.2},
    "TRENDING_EARLY":   {"S": 2.5, "A+": 2.0, "A": 2.0, "B": 2.0},
}

# Performance gate untuk signal bot:
# KILL  = tidak kirim signal sama sekali (strategy tidak profitable)
# CAUTION = tidak kirim (terlalu berisiko untuk subscriber)
# NORMAL + SCALE = kirim
SIGNAL_PERF_ALLOWED     = {"NORMAL", "SCALE"}  # CAUTION dan KILL diblock

# Slot signal per cycle — lebih banyak karena pure signal
MAX_SLOTS_CONFIRMED     = 8    # SWING + INTRADAY + SCALPING
MAX_SLOTS_SPECULATIVE   = 4    # MOONSHOT + PREPUMP + TRENDING

# ── Volume Filter ────────────────────────────────────
MIN_VOLUME_USDT         = 300_000

# ── Structure Detection ──────────────────────────────
SCALPING_BOS_LOOKBACK   = 25
INTRADAY_BOS_LOOKBACK   = 60
SWING_BOS_LOOKBACK      = 100
MOONSHOT_BOS_LOOKBACK   = 30
SCALPING_SWING_STRENGTH = 3
INTRADAY_SWING_STRENGTH = 3
SWING_SWING_STRENGTH    = 4
MOONSHOT_SWING_STRENGTH = 3

# ── Entry Thresholds ────────────────────────────────
BREAKOUT_THRESHOLD_BUY  = 0.995   # price < last_sh * ini → BREAKOUT
BREAKOUT_THRESHOLD_SELL = 1.005
PULLBACK_OB_BUY         = 1.002   # price > ob_high * ini → PULLBACK
PULLBACK_OB_SELL        = 0.998
PULLBACK_CLUSTER_BUY    = 1.010
PULLBACK_CLUSTER_SELL   = 0.990
PULLBACK_STRUCT_BUY     = 1.020
PULLBACK_STRUCT_SELL    = 0.980

# ── Late Signal Filter ──────────────────────────────
LATE_SIGNAL_LIMIT_SCALP = 0.010   # 1% — scalping batal jika entry sudah lewat
LATE_SIGNAL_LIMIT_INTRA = 0.020   # 2% — intraday

# ── SL Parameters ───────────────────────────────────
SCALPING_SL_ATR_MULT    = 1.5
INTRADAY_SL_ATR_MULT    = 2.0
SWING_SL_ATR_MULT       = 2.5

# ── TP Parameters ───────────────────────────────────
SCALPING_TP1_ATR_MULT   = 2.0
SCALPING_TP2_ATR_MULT   = 3.5
INTRADAY_TP1_PCT        = 0.06
INTRADAY_TP2_PCT        = 0.12
SWING_TP1_PCT           = 0.10
SWING_TP2_PCT           = 0.20
MOONSHOT_TP1_PCT        = 0.20
MOONSHOT_TP2_PCT        = 0.40

# ── Microcap Hunter ──────────────────────────────────
MIN_VOLUME_MICROCAP      = 50_000     # threshold vol lebih rendah dari main scan
MICROCAP_CHANGE_MIN      = 1.5       # cukup 1.5% mulai bergerak (bukan 3%)
MICROCAP_VOL_RATIO_MIN   = 2.0       # cukup 2x average (bukan 4x)
MICROCAP_RSI_MAX         = 55        # belum overbought — masih ada ruang naik
MICROCAP_RSI_MIN         = 20        # filter dump yang sudah terlalu dalam
MICROCAP_MAX_SIGNALS     = 3         # maks sinyal microcap per cycle
MICROCAP_TP1_PCT         = 0.30      # TP1: +30%
MICROCAP_TP2_PCT         = 0.80      # TP2: +80%
MICROCAP_SL_ATR_MULT     = 2.0       # SL lebih ketat karena volatil
MICROCAP_MIN_RR          = 2.5       # minimum R/R agar worth the risk

# ── Dump Early Warning ───────────────────────────────
DUMP_VOL_SPIKE_RATIO     = 3.0       # volume 3x rata-rata = distribusi besar
DUMP_BEARISH_CANDLE_PCT  = 0.65      # body candle >65% dari range = engulf bearish
DUMP_RSI_DROP_THRESHOLD  = 65        # RSI turun dari overbought area
DUMP_PRICE_DROP_PCT      = 0.03      # harga turun >3% dari puncak terakhir

# ── Volatility Filter ────────────────────────────────
ATR_MIN_PCT = {"SCALPING": 0.2, "INTRADAY": 0.3, "SWING": 0.5}
ATR_MAX_PCT = {"SCALPING": 6.0, "INTRADAY": 10.0, "SWING": 15.0}

# ── Tier Thresholds ──────────────────────────────────
TIER_THRESHOLDS = {"S": 16, "A+": 11, "A": 7, "B": 4}
TIER_RISK_PCT   = {"S": 2.0, "A+": 1.5, "A": 1.0, "B": 0.5}

# ── Min R/R per Tier ────────────────────────────────
MIN_RR = {
    "SCALPING":         {"S": 1.8, "A+": 1.5, "A": 1.5, "B": 1.5},
    "INTRADAY":         {"S": 2.5, "A+": 2.0, "A": 2.0, "B": 2.0},
    "SWING":            {"S": 3.0, "A+": 2.5, "A": 2.5, "B": 2.5},
    "MOONSHOT":         {"S": 2.5, "A+": 2.0, "A": 2.0, "B": 2.0},
    "MOONSHOT_PREPUMP": {"S": 2.5, "A+": 2.0, "A": 2.0, "B": 2.0},  # [P2]
    "TRENDING_EARLY":   {"S": 2.5, "A+": 2.0, "A": 2.0, "B": 1.8},  # [P3] sedikit lebih longgar
}

# ── BTC Regime Filter ────────────────────────────────
BTC_1H_DROP_BLOCK       = -3.0
BTC_1H_PUMP_BLOCK       =  3.0

# ── WATCHING Alert ──────────────────────────────────
WATCHING_RADIUS_PCT     = 1.5
MAX_WATCHING_PER_CYCLE  = 5

# ── Kill Switch ──────────────────────────────────────
KILL_SWITCH_LOSS_STREAK = 3
KILL_SWITCH_WINDOW_HRS  = 6
KILL_SWITCH_ENABLED     = True

# ── [NEW 3] Capital Allocation per Strategy ───────────────────────
# Setiap strategy punya budget risk maksimum dari total portfolio risk
# Total: 100% (dari MAX_PORTFOLIO_RISK_PCT = 10%)
# SWING    : 40% → max 4.0% risk dari akun
# INTRADAY : 30% → max 3.0% risk dari akun
# SCALPING : 10% → max 1.0% risk dari akun
# SPECULATIVE (MOONSHOT/PREPUMP/TRENDING): 20% → max 2.0% → dijaga oleh MAX_SPECULATIVE_RISK_PCT
CAPITAL_ALLOCATION_PCT = {
    "SWING":          40.0,   # Prioritas utama — 4h confirmed
    "INTRADAY":       30.0,   # Prioritas kedua — 1h confirmed
    "SCALPING":       10.0,   # Kecil — 5m cepat, capital rotasi cepat
    "MOONSHOT":       10.0,   # Reaktif moonshot
    "MOONSHOT_PREPUMP": 5.0,  # Prediktif — paling kecil, belum confirmed
    "TRENDING_EARLY": 5.0,    # Trending early — speculative
}
# Konversi ke risk absolute (% dari akun)
def _alloc_to_risk_pct(strategy: str) -> float:
    alloc = CAPITAL_ALLOCATION_PCT.get(strategy, 10.0) / 100.0
    return round(MAX_PORTFOLIO_RISK_PCT * alloc, 2)

STRATEGY_MAX_RISK_PCT = {k: _alloc_to_risk_pct(k) for k in CAPITAL_ALLOCATION_PCT}

# ── [NEW 4] Equity Protection System ─────────────────────────────
# Daily drawdown tracker — halt semua trading jika drawdown harian > threshold
EQUITY_PROTECTION_ENABLED     = True
DAILY_MAX_DRAWDOWN_PCT         = 3.0   # stop trading kalau rugi >3% dari akun hari ini
DAILY_LOSS_HALT_MSG            = "🛡️ Equity Protection: Daily loss limit tercapai. Bot halt."
# Adaptive: kalau market sangat volatil (BTC ATR > 3%), threshold diperketat
EQUITY_DRAWDOWN_STRICT_PCT     = 2.0   # threshold lebih ketat saat volatile
EQUITY_VOLATILE_BTC_ATR_TRIGGER = 3.0  # BTC ATR% yang dianggap volatile

# ── [NEW 2] Adaptive Threshold Parameters ────────────────────────
# Threshold ini akan di-override saat runtime berdasarkan volatility + regime
# Base values (fallback jika adaptive tidak bisa dihitung)
PREPUMP_RSI_EXIT_BASE   = 35     # RSI base threshold untuk pre-pump exit
PREPUMP_RSI_EXIT_MIN    = 28     # Minimum threshold (pasar sangat volatile)
PREPUMP_RSI_EXIT_MAX    = 42     # Maximum threshold (pasar tenang)
# ATR bands adaptive — base dari config lama, akan di-scale
ATR_ADAPTIVE_ENABLED    = True
ATR_SCALE_VOLATILE      = 1.25   # Perlebar ATR band saat vol tinggi
ATR_SCALE_CALM          = 0.85   # Persempit ATR band saat vol rendah
BTC_ATR_VOLATILE_THRESH = 2.5    # BTC ATR% dianggap pasar volatile

# ── Session Filter ───────────────────────────────────
# Dead hours WIB: scalping dinonaktifkan (00:00–06:00)
DEAD_HOURS_START        = 0
DEAD_HOURS_END          = 6

# ── [UPGRADE P1] Volume Dry-Up Detector ──────────────────
# Deteksi akumulasi senyap: volume mengecil drastis setelah koreksi
# → tanda distribusi selesai, smart money mulai akumulasi diam-diam
DRYUP_WINDOW            = 5      # jumlah candle recent yang dievaluasi
DRYUP_RATIO_THRESHOLD   = 0.45   # volume recent < 45% baseline = dry-up terkonfirmasi
DRYUP_BASELINE_WINDOW   = 30     # candle baseline (sebelum window recent)
DRYUP_PRICE_STABLE_PCT  = 0.03   # harga tidak turun lebih dari 3% selama dry-up

# ── [UPGRADE P2] Pre-Pump Moonshot (Predictive) ──────────
# Moonshot kini aktif SEBELUM pump terjadi, bukan setelah
# Path A (lama): vol spike tinggi + change tinggi = MOMENTUM (reaktif)
# Path B (baru): dry-up selesai + RSI keluar oversold + struktur baru = PRE_PUMP (prediktif)
PREPUMP_MAX_CHANGE      = 5.0    # harga belum naik lebih dari 5% — belum pump
PREPUMP_VOL_REVIVAL_MIN = 1.5    # volume mulai bangkit: minimal 1.5x dari dry-up level
PREPUMP_RSI_EXIT        = 35     # RSI mulai naik melewati 35 = keluar oversold
PREPUMP_RSI_MAX         = 58     # RSI belum overbought — masih ada ruang naik
PREPUMP_MIN_SCORE       = 7      # skor minimum untuk pre-pump signal

# ── [UPGRADE P3] Trending Pre-Signal (Standalone) ────────
# Pair baru masuk CoinGecko trending tapi harga belum bergerak = early entry window
TRENDING_MAX_CHANGE     = 5.0    # harga belum naik >5% — masih early, belum telat
TRENDING_RSI_MAX        = 62     # belum terlalu panas
TRENDING_RSI_MIN        = 22     # filter pair yang sedang crash/dump besar
TRENDING_MIN_SCORE      = 5      # threshold skor (lebih longgar dari strategi lain)
TRENDING_VOL_RATIO_MIN  = 1.3    # ada sedikit kenaikan volume = interest mulai masuk
MAX_TRENDING_PER_CYCLE  = 3      # maksimal 3 sinyal trending per cycle

# ── [FIX 1] Confirmation Layer ───────────────────────────
# Semua sinyal speculative (PREPUMP, TRENDING_EARLY) wajib lolos
# minimal 2 dari 3 konfirmasi sebelum masuk queue
CONF_MIN_PASSED         = 2      # minimum konfirmasi yang harus terpenuhi dari 3 kondisi
# Konfirmasi 1: breakout kecil — harga sudah di atas level kunci
CONF_BREAKOUT_MARGIN    = 0.005  # harga > last_sh * (1 + margin) = micro breakout
# Konfirmasi 2: volume kedua — candle terbaru lebih tinggi dari 2 candle sebelumnya
CONF_VOL2_RATIO         = 1.2    # volume candle[-1] > mean(volume[-3:-1]) * ratio
# Konfirmasi 3: momentum candle — close bullish dengan body yang cukup
CONF_CANDLE_BODY_MIN    = 0.5    # body / full_range >= nilai ini = candle tegas

# ── [FIX 2] Signal Priority System ───────────────────────
# Ranking priority untuk flush — angka kecil = lebih diprioritaskan
SIGNAL_PRIORITY = {
    "SWING":            1,   # paling terkonfirmasi — 4h structure
    "INTRADAY":         2,   # terkonfirmasi — 1h multi-indicator
    "SCALPING":         3,   # konfirmasi cepat — 5m
    "MOONSHOT":         4,   # reaktif — harga sudah bergerak
    "MOONSHOT_PREPUMP": 5,   # prediktif — belum terkonfirmasi penuh
    "TRENDING_EARLY":   6,   # paling speculative — hanya berbasis trending
}
# Slot dikontrol oleh config di atas (MAX_SLOTS_CONFIRMED / MAX_SLOTS_SPECULATIVE)
# nilai lama sudah diganti di section [NEW 5] di atas

# ── [FIX 3] Microcap & Speculative Risk Cap ──────────────
# Exposure total untuk semua sinyal speculative (microcap + prepump + trending)
MAX_SPECULATIVE_RISK_PCT = 4.0   # maks 4% total akun untuk semua posisi speculative
# Override risk per tier khusus sinyal speculative (lebih kecil dari main)
SPECULATIVE_TIER_RISK = {
    "S":  1.0,   # half dari main S (2.0%)
    "A+": 0.75,
    "A":  0.5,
    "B":  0.25,  # sangat kecil — high risk position
}
# Strategy yang diklasifikasikan sebagai speculative
SPECULATIVE_TYPES = {"MOONSHOT_PREPUMP", "TRENDING_EARLY", "MOONSHOT"}

# ── Weighted Scoring ────────────────────────────────
W = {
    "bos": 5, "choch": 5, "liq_sweep": 4, "order_block": 4,
    "equal_hl": 3, "stop_cluster": 3, "fake_bo": 3,
    "divergence": 3, "liq_cluster": 2, "oi_signal": 2,
    "ichimoku": 2, "vwap": 2, "bb_extreme": 2, "funding": 2, "poc": 2,
    "rsi_extreme": 1, "stoch_rsi": 1, "ema_cross": 1, "ob_ratio": 1,
    "trending": 1, "fg_extreme": 1, "vol_spike": 1, "support_res": 1,
    "momentum": 1, "pullback": 2, "rejection": 2, "candle_conf": 2,
}

# ── Pairs ────────────────────────────────────────────
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
SAFE_PAIRS_RISK_OFF = {"BTC_USDT", "ETH_USDT"}
BLACKLIST = {
    "3S","3L","5S","5L","TUSD","USDC","BUSD","DAI","FDUSD","USD1",
    "USDP","USDD","USDJ","ZUSD","GUSD","CUSD","SUSD","STBL","FRAX",
    "LUSD","USDN","STABLE","BARD",
}
ETF_STOCK_SUFFIXES = {
    # Original suffixes
    "HON","NVDAX","TSLAX","AAPLX","AMZNX","MSFX","METAX","GOOGX",
    "COINX","MSTRX","AMZX","GOOQX","MSLX","NFLXX","ARKX",
    "BTCB","ETHB","SOLB","PAXG","XAUT",
    # Gate.io leveraged stock ETF tokens — tambahan
    "NVDAON","NVDASHT","NVDAUP","NVDADOWN",
    "TSLAON","TSLASHT","TSLAL","TSLAUP","TSLADOWN",
    "AAPLON","AAPLSHT","AAPLUS","AAPLDOWN",
    "AMZNON","AMZNSHT","AMZNUP","AMZNDOWN",
    "MSFTON","MSFTSHT","MSFTUP","MSFTDOWN",
    "METAON","METASHT","METAUP","METADOWN",
    "GOOGON","GOOGSHT","GOOGUP","GOOGDOWN",
    "NFLXON","NFLXSHT","NFLXUP","NFLXDOWN",
    "COINON","COINSHT","COINUP","COINDOWN",
    "MSTRON","MSTRSHT","MSTRUP","MSTRDOWN",
    "MCDON","MCDONSHT","MCDUP","MCDDOWN",
    "BABAON","BABASHT","BABAUP","BABADOWN",
    "SON","SHT","3UP","3DOWN","5UP","5DOWN",
}

# ── ETF Keyword Prefix Filter ─────────────────────────
# Pair yang BASE-nya diawali nama saham ini pasti ETF token
ETF_KEYWORDS = {
    "NVDA","TSLA","AAPL","AMZN","MSFT","META","GOOG","NFLX",
    "COIN","MSTR","MCD","BABA","BIDU","AMD","INTC","PYPL",
    "UBER","SHOP","SNAP","TWTR","RBLX","PLTR","HOOD",
}

# ── Signal Direction Filter ───────────────────────────
BUY_ONLY = True   # True = hanya sinyal BUY | False = BUY + SELL

# ── Sector Map ───────────────────────────────────────
SECTOR_MAP = {
    "BTC":"store_of_value","LTC":"store_of_value",
    "ETH":"smart_contract","SOL":"smart_contract","ADA":"smart_contract",
    "AVAX":"smart_contract","NEAR":"smart_contract","APT":"smart_contract",
    "SUI":"smart_contract","TRX":"smart_contract","TON":"smart_contract",
    "ARB":"l2","OP":"l2","MATIC":"l2","STRK":"l2","IMX":"l2",
    "BNB":"exchange","OKB":"exchange",
    "XRP":"payment","XLM":"payment",
    "UNI":"defi","AAVE":"defi","CRV":"defi","INJ":"defi","DYDX":"defi",
    "LINK":"oracle","BAND":"oracle",
    "DOT":"interop","ATOM":"interop","RUNE":"interop",
    "DOGE":"meme","SHIB":"meme","PEPE":"meme","WIF":"meme","BONK":"meme",
    "RNDR":"ai","FET":"ai","TAO":"ai","WLD":"ai","GRT":"ai",
    "AXS":"gaming","SAND":"gaming","MANA":"gaming",
    "FIL":"storage","AR":"storage",
}
MAX_SECTOR_EXPOSURE = 4   # FIX: was 2 — caused most altcoins to be blocked

# ════════════════════════════════════════════════════════
#  GLOBAL STATE (reset tiap cycle di run())
# ════════════════════════════════════════════════════════
_signal_queue:   list = []
_watching_sent:  set  = set()
_watching_count: int  = 0
_candle_cache:   dict = {}   # (pair, interval) → (closes, highs, lows, volumes)
_portfolio_state: dict = {
    "total_risk_pct": 0.0,
    "speculative_risk_pct": 0.0,
    "strategy_risk_pct": {},        # [NEW 3] risk per strategy type
    "open_positions": {},
    "sector_exposure": defaultdict(float),
}
# [FIX 2] Slot counters — diupdate oleh flush, dibaca oleh scan summary
_flush_slots: dict = {"confirmed": 0, "speculative": 0}

# [NEW 2] Adaptive thresholds — dihitung tiap cycle berdasarkan market condition
_adaptive: dict = {
    "prepump_rsi_exit": PREPUMP_RSI_EXIT,   # akan di-update
    "atr_min_pct": dict(ATR_MIN_PCT),       # copy, akan di-scale
    "atr_max_pct": dict(ATR_MAX_PCT),
    "btc_atr_pct": 0.0,
    "is_volatile": False,
}

# [NEW 4] Equity protection state — daily P&L tracker
_equity_state: dict = {
    "daily_realized_pct": 0.0,   # cumulative P&L hari ini
    "halted": False,
    "halt_reason": "",
}

# ════════════════════════════════════════════════════════
#  L2. DATA LAYER
# ════════════════════════════════════════════════════════

def tg(msg: str):
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
        time.sleep(0.4)
    except Exception as e:
        print(f"⚠️ Telegram: {e}")


def http_get(url: str, timeout: int = 10):
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


def get_candles(client, pair: str, interval: str, limit: int):
    """
    Fetch candle dengan CACHE per (pair, interval).
    Setiap pasangan hanya di-fetch SEKALI per cycle — tidak ada double call.
    """
    global _candle_cache
    cache_key = (pair, interval)
    if cache_key in _candle_cache:
        return _candle_cache[cache_key]

    try:
        candles = client.list_candlesticks(
            currency_pair=pair, interval=interval, limit=limit)
        if not candles or len(candles) < 20:
            _candle_cache[cache_key] = (None, None, None, None)
            return None, None, None, None
        closes  = np.array([float(c[2]) for c in candles])
        highs   = np.array([float(c[3]) for c in candles])
        lows    = np.array([float(c[4]) for c in candles])
        volumes = np.array([float(c[1]) for c in candles])
        result  = (closes, highs, lows, volumes)
        _candle_cache[cache_key] = result
        return result
    except Exception as e:
        print(f"⚠️ candles [{pair}|{interval}]: {e}")
        _candle_cache[cache_key] = (None, None, None, None)
        return None, None, None, None


def get_fear_greed():
    try:
        data = http_get("https://api.alternative.me/fng/?limit=1")
        if data:
            val = int(data["data"][0]["value"])
            lbl = data["data"][0]["value_classification"]
            return val, lbl
    except Exception:
        pass
    return 50, "Neutral"


def get_coingecko_market():
    data = http_get("https://api.coingecko.com/api/v3/global")
    if not data: return None
    try:
        d = data["data"]
        return {
            "btc_dominance":         float(d.get("market_cap_percentage", {}).get("btc", 50)),
            "market_cap_change_24h": float(d.get("market_cap_change_percentage_24h_usd", 0)),
        }
    except Exception:
        return None


def get_coingecko_trending():
    data = http_get("https://api.coingecko.com/api/v3/search/trending")
    if not data: return []
    try:
        return [item.get("item", {}).get("symbol", "").upper()
                for item in data.get("coins", [])]
    except Exception:
        return []


def get_funding_rate(pair: str):
    """Hanya dipanggil untuk FUTURES_PAIRS."""
    if pair not in FUTURES_PAIRS: return None
    try:
        data = http_get(
            f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{pair}", timeout=8)
        if data and "funding_rate" in data:
            return float(data["funding_rate"])
    except Exception:
        pass
    return None


def get_open_interest(pair: str, symbol: str, change_24h: float) -> str:
    """OI signal — hanya untuk FUTURES_PAIRS. Return interpretasi string."""
    if pair not in FUTURES_PAIRS: return "NEUTRAL"
    try:
        # Coba Coinglass dulu
        url  = f"https://open-api.coinglass.com/public/v2/open_interest?symbol={symbol}"
        data = http_get(url, timeout=8)
        if data and data.get("code") == "0":
            items  = data.get("data", [])
            oi_chg = float(items[0].get("openInterestChangePercent24h", 0)) if items else 0
            if oi_chg != 0:
                if oi_chg > 5  and change_24h > 1:  return "STRONG_BUY"
                if oi_chg > 5  and change_24h < -1: return "SQUEEZE"
                if oi_chg < -5 and change_24h > 1:  return "WEAK_RALLY"
                if oi_chg < -5 and change_24h < -1: return "STRONG_SELL"
                return "NEUTRAL"
        # Fallback Gate.io
        data = http_get(
            f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{pair}", timeout=8)
        if data:
            pos   = float(data.get("position_size", 0) or 0)
            mark  = float(data.get("mark_price",    0) or 0)
            mult  = float(data.get("quanto_multiplier", 1) or 1)
            oi_usd = pos * mark * mult
            if oi_usd > 0:
                if change_24h > 3:  return "STRONG_BUY"
                if change_24h < -3: return "STRONG_SELL"
                if change_24h > 1:  return "WEAK_RALLY"
    except Exception:
        pass
    return "NEUTRAL"


_coinglass_fail_count = 0
_coinglass_skip_until = 0.0

def get_liquidation_bias(symbol: str) -> str:
    """
    FIX: Circuit breaker added. After 3 consecutive failures,
    Coinglass is skipped for 10 minutes to prevent log flooding.
    """
    global _coinglass_fail_count, _coinglass_skip_until
    if time.time() < _coinglass_skip_until:
        return "NEUTRAL"
    try:
        url  = f"https://open-api.coinglass.com/public/v2/liquidation_map?symbol={symbol}&interval=12h"
        data = http_get(url, timeout=8)
        if not data or data.get("code") != "0":
            _coinglass_fail_count += 1
            if _coinglass_fail_count >= 3:
                _coinglass_skip_until = time.time() + 600
                print("⚠️ Coinglass circuit open — skip 10 min (HTTP errors)")
                _coinglass_fail_count = 0
            return "NEUTRAL"
        _coinglass_fail_count = 0
        items     = data.get("data", {})
        liq_short = sum(float(s.get("liquidationAmount", 0))
                        for s in items.get("shorts", [])[:10])
        liq_long  = sum(float(l.get("liquidationAmount", 0))
                        for l in items.get("longs",  [])[:10])
        if liq_short > liq_long * 1.5: return "BUY"
        if liq_long  > liq_short * 1.5: return "SELL"
    except Exception:
        _coinglass_fail_count += 1
        if _coinglass_fail_count >= 3:
            _coinglass_skip_until = time.time() + 600
            print("⚠️ Coinglass circuit open — skip 10 min (exceptions)")
            _coinglass_fail_count = 0
    return "NEUTRAL"


def get_order_book_ratio(client, pair: str) -> float:
    if pair not in TOP_PAIRS_OB: return 1.0
    try:
        ob      = client.list_order_book(currency_pair=pair, limit=20)
        bid_vol = sum(float(b[1]) for b in ob.bids)
        ask_vol = sum(float(a[1]) for a in ob.asks)
        return bid_vol / ask_vol if ask_vol > 0 else 1.0
    except Exception:
        return 1.0


# ════════════════════════════════════════════════════════
#  L3. INDICATOR LAYER — kalkulasi murni, no side effects
# ════════════════════════════════════════════════════════

def calc_rsi(closes, period=14) -> float:
    s = pd.Series(closes); delta = s.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    return float((100 - 100/(1+gain/(loss+1e-9))).iloc[-1])


def calc_ema(closes, period) -> float:
    return float(pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1])


def calc_macd(closes):
    s = pd.Series(closes)
    macd   = s.ewm(span=12, adjust=False).mean() - s.ewm(span=26, adjust=False).mean()
    signal = macd.ewm(span=9, adjust=False).mean()
    return float(macd.iloc[-1]), float(signal.iloc[-1])


def calc_bb(closes, period=20):
    s = pd.Series(closes)
    mid = s.rolling(period).mean().iloc[-1]
    std = s.rolling(period).std().iloc[-1]
    return float(mid-2*std), float(mid), float(mid+2*std)


def calc_atr(closes, highs, lows, period=14) -> float:
    tr  = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(1, len(closes))]
    return float(pd.Series(tr).rolling(period).mean().iloc[-1])


def calc_stoch_rsi(closes, period=14) -> float:
    s = pd.Series(closes); delta = s.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rsi  = 100 - (100/(1+gain/(loss+1e-9)))
    rmin = rsi.rolling(period).min(); rmax = rsi.rolling(period).max()
    return float(((rsi-rmin)/(rmax-rmin+1e-9)).iloc[-1])


def calc_vwap(closes, highs, lows, volumes) -> float:
    tp = (highs+lows+closes)/3
    return float((np.cumsum(tp*volumes)/(np.cumsum(volumes)+1e-9))[-1])


def calc_ichimoku(closes, highs, lows) -> dict:
    def hl(h, l, p):
        if len(h)<p: return None, None
        return float(np.max(h[-p:])), float(np.min(l[-p:]))
    th,tl = hl(highs,lows,9); kh,kl = hl(highs,lows,26)
    if None in (th,tl,kh,kl):
        return {"valid":False,"above_cloud":False,"below_cloud":False,"tk_bull":False,"tk_bear":False}
    tenkan = (th+tl)/2; kijun = (kh+kl)/2
    senkou_a = (tenkan+kijun)/2
    sh,sl2 = hl(highs,lows,52)
    senkou_b = (sh+sl2)/2 if sh else senkou_a
    price = float(closes[-1])
    ct,cb = max(senkou_a,senkou_b), min(senkou_a,senkou_b)
    return {"valid":True,"above_cloud":price>ct,"below_cloud":price<cb,
            "tk_bull":tenkan>kijun,"tk_bear":tenkan<kijun,"cloud_top":ct,"cloud_bot":cb}


def calc_support_resistance(highs, lows, closes, lookback=20):
    return (float(np.percentile(lows[-lookback:],  15)),
            float(np.percentile(highs[-lookback:], 85)))


def calc_rsi_divergence(closes, highs, lows, period=14, lookback=20):
    if len(closes)<lookback+period: return None
    s = pd.Series(closes); delta = s.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rsi  = (100-(100/(1+gain/(loss+1e-9)))).values
    wl,wh,wr = lows[-lookback:],highs[-lookback:],rsi[-lookback:]
    sl_idx,sh_idx = [],[]
    for i in range(2,len(wl)-2):
        if wl[i]<wl[i-1] and wl[i]<wl[i-2] and wl[i]<wl[i+1] and wl[i]<wl[i+2]: sl_idx.append(i)
        if wh[i]>wh[i-1] and wh[i]>wh[i-2] and wh[i]>wh[i+1] and wh[i]>wh[i+2]: sh_idx.append(i)
    if len(sl_idx)>=2:
        i1,i2 = sl_idx[-2],sl_idx[-1]
        if wl[i2]<wl[i1] and wr[i2]>wr[i1] and wr[i2]<45: return "BULLISH"
    if len(sh_idx)>=2:
        i1,i2 = sh_idx[-2],sh_idx[-1]
        if wh[i2]>wh[i1] and wr[i2]<wr[i1] and wr[i2]>55: return "BEARISH"
    return None


def calc_volume_profile(closes, volumes, bins=20):
    if len(closes)<10: return None
    pmin,pmax = float(min(closes)),float(max(closes))
    if pmax==pmin: return None
    bsize = (pmax-pmin)/bins
    vbins = [0.0]*bins
    for i,p in enumerate(closes):
        vbins[min(int((p-pmin)/bsize),bins-1)] += float(volumes[i])
    return float(pmin+(vbins.index(max(vbins))+0.5)*bsize)


# ── [UPGRADE P1] Volume Dry-Up Detector ──────────────────────────────────────
def calc_volume_dryup(closes, volumes) -> dict:
    """
    Deteksi akumulasi senyap melalui pola volume dry-up.

    Pola yang dicari:
    1. Volume turun drastis selama DRYUP_WINDOW candle terakhir
       (< DRYUP_RATIO_THRESHOLD dari baseline)
    2. Harga relatif stabil selama periode tersebut (tidak dumping lanjut)
    3. Opsional: ada tanda kebangkitan volume di candle paling akhir

    Kondisi ini mengindikasikan distribusi selesai dan smart money
    sedang diam-diam mengakumulasi sebelum markup dimulai.

    Returns dict:
      - is_dryup       : bool — apakah kondisi dry-up terkonfirmasi
      - dryup_ratio    : float — rasio volume recent vs baseline
      - price_stable   : bool — harga tidak turun drastis selama dry-up
      - vol_revival    : bool — candle terakhir mulai ada kebangkitan volume
      - revival_ratio  : float — rasio volume candle terakhir vs dry-up rata-rata
      - dryup_score    : int — skor kekuatan sinyal (0–3)
    """
    result = {
        "is_dryup": False,
        "dryup_ratio": 1.0,
        "price_stable": False,
        "vol_revival": False,
        "revival_ratio": 1.0,
        "dryup_score": 0,
    }

    total_needed = DRYUP_BASELINE_WINDOW + DRYUP_WINDOW + 2
    if len(volumes) < total_needed:
        return result

    # Pisahkan baseline vs window recent
    baseline_vols = volumes[-(DRYUP_BASELINE_WINDOW + DRYUP_WINDOW):-(DRYUP_WINDOW)]
    recent_vols   = volumes[-DRYUP_WINDOW:]
    baseline_avg  = float(np.mean(baseline_vols)) + 1e-9
    recent_avg    = float(np.mean(recent_vols))
    dryup_ratio   = recent_avg / baseline_avg

    result["dryup_ratio"] = round(dryup_ratio, 3)

    # Kondisi 1: volume dry-up terkonfirmasi
    if dryup_ratio >= DRYUP_RATIO_THRESHOLD:
        return result  # volume masih normal, bukan dry-up

    result["is_dryup"] = True

    # Kondisi 2: harga stabil selama periode dry-up (tidak crash lanjut)
    dryup_closes      = closes[-DRYUP_WINDOW:]
    price_high_dryup  = float(np.max(dryup_closes))
    price_low_dryup   = float(np.min(dryup_closes))
    price_range_pct   = (price_high_dryup - price_low_dryup) / (price_low_dryup + 1e-9)
    result["price_stable"] = price_range_pct <= DRYUP_PRICE_STABLE_PCT

    # Kondisi 3: apakah candle terakhir mulai ada kebangkitan volume?
    # (volume 1 candle terakhir lebih tinggi dari rata-rata dry-up window)
    last_vol    = float(volumes[-1])
    revival_ratio = last_vol / (recent_avg + 1e-9)
    result["revival_ratio"] = round(revival_ratio, 3)
    result["vol_revival"]   = revival_ratio >= PREPUMP_VOL_REVIVAL_MIN

    # Hitung skor kekuatan sinyal dry-up (0–3)
    score = 0
    if result["is_dryup"]:                            score += 1
    if result["price_stable"]:                        score += 1
    if result["vol_revival"]:                         score += 1
    result["dryup_score"] = score

    return result


# ════════════════════════════════════════════════════════
#  L4. ENGINE LAYER
# ════════════════════════════════════════════════════════

# ── [FIX 1] Confirmation Layer ───────────────────────────────────────────────
def check_confirmation_layer(closes, highs, lows, volumes,
                              structure: dict, side: str = "BUY") -> dict:
    """
    [FIX 1] Gate terakhir untuk sinyal speculative (PREPUMP, TRENDING_EARLY).

    Tiga kondisi independen dievaluasi. Sinyal hanya lolos jika minimal
    CONF_MIN_PASSED (default: 2) dari 3 kondisi terpenuhi.

    Kondisi 1 — Micro Breakout:
      Harga sudah sedikit menembus level struktur kunci (last_sh untuk BUY).
      Bukan hanya "dekat" dengan level, tapi sudah ada candle close di atasnya.
      Filter: batal jika harga masih jauh di bawah struktur.

    Kondisi 2 — Volume Konfirmasi Kedua:
      Volume candle terbaru lebih tinggi dari rata-rata 2 candle sebelumnya.
      Ini memastikan interest bukan hanya satu spike isolasi, tapi ada
      kontinuitas buying pressure.

    Kondisi 3 — Momentum Candle:
      Candle terbaru adalah candle bullish tegas — body setidaknya 50% dari
      full range (bukan doji atau candle dengan shadow panjang).
      Upper wick tidak boleh dominan (tanda penolakan di atas).

    Returns dict:
      - passed   : int  — berapa kondisi yang terpenuhi
      - go        : bool — True jika passed >= CONF_MIN_PASSED
      - details   : list[str] — keterangan tiap kondisi
      - c1_micro_bo: bool
      - c2_vol2   : bool
      - c3_candle : bool
    """
    result = {
        "passed": 0, "go": False, "details": [],
        "c1_micro_bo": False, "c2_vol2": False, "c3_candle": False,
    }
    if len(closes) < 5 or len(volumes) < 5:
        return result

    price      = float(closes[-1])
    prev_close = float(closes[-2])
    hi         = float(highs[-1])
    lo         = float(lows[-1])
    body       = abs(price - prev_close)
    full_range = (hi - lo) + 1e-9
    upper_wick = hi - max(price, prev_close)

    # ── Kondisi 1: Micro Breakout ─────────────────────────
    last_sh = structure.get("last_sh")
    last_sl = structure.get("last_sl")
    if side == "BUY" and last_sh:
        c1 = price > last_sh * (1 + CONF_BREAKOUT_MARGIN)
        if c1:
            result["c1_micro_bo"] = True
            result["details"].append(f"✅ Micro BO: ${price:.5f} > ${last_sh*(1+CONF_BREAKOUT_MARGIN):.5f}")
        else:
            result["details"].append(f"❌ Micro BO: harga belum break ${last_sh:.5f}")
    elif side == "SELL" and last_sl:
        c1 = price < last_sl * (1 - CONF_BREAKOUT_MARGIN)
        if c1:
            result["c1_micro_bo"] = True
            result["details"].append(f"✅ Micro BO: ${price:.5f} < ${last_sl*(1-CONF_BREAKOUT_MARGIN):.5f}")
        else:
            result["details"].append(f"❌ Micro BO: harga belum break ${last_sl:.5f}")
    else:
        # Tidak ada level struktur — kondisi ini tidak bisa dievaluasi, dianggap pass
        result["c1_micro_bo"] = True
        result["details"].append("⚪ Micro BO: no structure level — skip")

    # ── Kondisi 2: Volume Konfirmasi Kedua ────────────────
    vol_prev2_avg = float(np.mean(volumes[-3:-1])) + 1e-9
    vol_now       = float(volumes[-1])
    c2 = vol_now > vol_prev2_avg * CONF_VOL2_RATIO
    result["c2_vol2"] = c2
    if c2:
        result["details"].append(f"✅ Vol2: {vol_now/vol_prev2_avg:.2f}x (>{CONF_VOL2_RATIO}x)")
    else:
        result["details"].append(f"❌ Vol2: {vol_now/vol_prev2_avg:.2f}x (<{CONF_VOL2_RATIO}x)")

    # ── Kondisi 3: Momentum Candle ────────────────────────
    body_ratio    = body / full_range
    upper_dominant = upper_wick > body * 1.5  # upper wick dominan = bearish rejection

    if side == "BUY":
        c3 = (price > prev_close and
              body_ratio >= CONF_CANDLE_BODY_MIN and
              not upper_dominant)
    else:
        lower_wick    = min(price, prev_close) - lo
        lower_dominant = lower_wick > body * 1.5
        c3 = (price < prev_close and
              body_ratio >= CONF_CANDLE_BODY_MIN and
              not lower_dominant)

    result["c3_candle"] = c3
    if c3:
        result["details"].append(f"✅ Candle: body {body_ratio:.0%} tegas")
    else:
        result["details"].append(f"❌ Candle: body {body_ratio:.0%} lemah / rejection")

    # ── Final verdict ─────────────────────────────────────
    passed = sum([result["c1_micro_bo"], result["c2_vol2"], result["c3_candle"]])
    result["passed"] = passed
    result["go"]     = passed >= CONF_MIN_PASSED
    return result

def detect_swing_points(highs, lows, lookback=60, strength=2):
    points = []
    n = min(len(highs), lookback); start = len(highs)-n
    for i in range(start+strength, len(highs)-strength):
        if all(highs[i]>highs[i-j] for j in range(1,strength+1)) and \
           all(highs[i]>highs[i+j] for j in range(1,strength+1)):
            points.append((i, highs[i], "SH"))
        if all(lows[i]<lows[i-j] for j in range(1,strength+1)) and \
           all(lows[i]<lows[i+j] for j in range(1,strength+1)):
            points.append((i, lows[i], "SL"))
    return sorted(points, key=lambda x: x[0])


def detect_structure(closes, highs, lows, lookback: int, strength: int) -> dict:
    """Deteksi BOS, CHoCH, trend phase. lookback dan strength wajib eksplisit dari caller."""
    result = {
        "bos": None, "choch": None, "trend_phase": "UNKNOWN",
        "structure_bias": "NEUTRAL", "last_sh": None, "last_sl": None,
        "prev_sh": None, "prev_sl": None, "valid": False,
    }
    if len(closes) < lookback: return result

    points      = detect_swing_points(highs, lows, lookback=lookback, strength=strength)
    swing_highs = [(i, p) for i,p,t in points if t=="SH"]
    swing_lows  = [(i, p) for i,p,t in points if t=="SL"]
    if len(swing_highs)<2 or len(swing_lows)<2: return result

    last_sh = swing_highs[-1][1]; last_sl = swing_lows[-1][1]
    prev_sh = swing_highs[-2][1]; prev_sl = swing_lows[-2][1]
    current = float(closes[-1])
    result.update({"last_sh":last_sh,"last_sl":last_sl,
                   "prev_sh":prev_sh,"prev_sl":prev_sl,"valid":True})

    hh,hl = last_sh>prev_sh, last_sl>prev_sl
    lh,ll = last_sh<prev_sh, last_sl<prev_sl
    if hh and hl:   phase,bias = "MARKUP","BULLISH"
    elif lh and ll: phase,bias = "MARKDOWN","BEARISH"
    elif hh and ll: phase,bias = "DISTRIBUTION","NEUTRAL"
    elif lh and hl: phase,bias = "ACCUMULATION","NEUTRAL"
    else:           phase,bias = "RANGING","NEUTRAL"
    result["trend_phase"]    = phase
    result["structure_bias"] = bias

    prev_close = float(closes[-2]) if len(closes)>=2 else current
    candles_above = sum(1 for c in closes[-4:-1] if c>last_sh) if len(closes)>=4 else 0
    candles_below = sum(1 for c in closes[-4:-1] if c<last_sl) if len(closes)>=4 else 0

    bull_break = ((current>last_sh and prev_close<=last_sh*1.005) or
                  (current>last_sh and candles_above>=3))
    bear_break = ((current<last_sl and prev_close>=last_sl*0.995) or
                  (current<last_sl and candles_below>=3))

    if bull_break:
        result["bos" if bias!="BEARISH" else "choch"] = "BULLISH"
    elif bear_break:
        result["bos" if bias!="BULLISH" else "choch"] = "BEARISH"

    return result


def detect_liquidity_map(closes, highs, lows, volumes, lookback=50) -> dict:
    result = {
        "equal_highs":None,"equal_lows":None,
        "stop_cluster_above":None,"stop_cluster_below":None,
        "fake_bo_bull":False,"fake_bo_bear":False,
        "sweep_bull":False,"sweep_bear":False,"sweep_level":None,
        "liq_above":None,"liq_below":None,
    }
    if len(closes)<lookback: return result
    h_s = highs[-lookback:]; l_s = lows[-lookback:]
    c_s = closes[-lookback:]; v_s = volumes[-lookback:]
    tol = 0.003

    h_arr = h_s.reshape(-1,1); h_mat = np.abs(h_arr-h_s)/(h_arr+1e-9)
    np.fill_diagonal(h_mat,1.0); eq_h = h_mat<tol
    if eq_h.any():
        rows,cols = np.where(eq_h & (np.arange(len(h_s))[:,None]<np.arange(len(h_s))))
        if len(rows)>0:
            result["equal_highs"]        = float(np.median((h_s[rows]+h_s[cols])/2))
            result["stop_cluster_above"] = result["equal_highs"]*1.002

    l_arr = l_s.reshape(-1,1); l_mat = np.abs(l_arr-l_s)/(l_arr+1e-9)
    np.fill_diagonal(l_mat,1.0); eq_l = l_mat<tol
    if eq_l.any():
        rows,cols = np.where(eq_l & (np.arange(len(l_s))[:,None]<np.arange(len(l_s))))
        if len(rows)>0:
            result["equal_lows"]         = float(np.median((l_s[rows]+l_s[cols])/2))
            result["stop_cluster_below"] = result["equal_lows"]*0.998

    result["liq_above"] = float(np.max(h_s)*1.001)
    result["liq_below"] = float(np.min(l_s)*0.999)

    ref_high = float(np.max(h_s[:-5])); ref_low = float(np.min(l_s[:-5]))
    for i in range(-5,0):
        if lows[i]<ref_low and closes[i]>ref_low:
            result["sweep_bull"]=True; result["sweep_level"]=ref_low
        if highs[i]>ref_high and closes[i]<ref_high:
            result["sweep_bear"]=True; result["sweep_level"]=ref_high

    recent3 = closes[-4:-1]; cur = closes[-1]
    if any(c>ref_high for c in recent3) and cur<ref_high: result["fake_bo_bull"]=True
    if any(c<ref_low  for c in recent3) and cur>ref_low:  result["fake_bo_bear"]=True
    return result


def detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=30) -> dict:
    result = {"valid":False,"ob_high":None,"ob_low":None,"ob_mid":None}
    if len(closes)<lookback: return result
    c=closes[-lookback:]; h=highs[-lookback:]; l=lows[-lookback:]
    avg_body = np.mean([abs(c[i]-c[i-1]) for i in range(1,len(c))])
    for i in range(len(c)-4,0,-1):
        body_next = abs(c[i+1]-c[i])
        if side=="BUY" and c[i]<c[i-1] and c[i+1]>c[i] and body_next>avg_body*1.5:
            return {"valid":True,"ob_high":float(h[i]),"ob_low":float(l[i]),"ob_mid":float((h[i]+l[i])/2)}
        if side=="SELL" and c[i]>c[i-1] and c[i+1]<c[i] and body_next>avg_body*1.5:
            return {"valid":True,"ob_high":float(h[i]),"ob_low":float(l[i]),"ob_mid":float((h[i]+l[i])/2)}
    return result


def check_entry_precision(closes, highs, lows, volumes,
                          side: str, structure: dict,
                          liq_map: dict, ob: dict) -> dict:
    result = {"pullback":False,"rejection":False,"candle_conf":False,
              "precision_score":0,"entry_quality":"WAIT","detail":[]}
    if len(closes)<5: return result

    price      = float(closes[-1]); prev_close = float(closes[-2])
    hi=float(highs[-1]); lo=float(lows[-1])
    body       = abs(price-prev_close); full_range = hi-lo+1e-9
    upper_wick = hi-max(price,prev_close); lower_wick = min(price,prev_close)-lo
    avg_vol    = float(np.mean(volumes[-10:-1])); curr_vol = float(volumes[-1])
    last_sh    = structure.get("last_sh"); last_sl = structure.get("last_sl")
    ob_high    = ob.get("ob_high") if ob.get("valid") else None
    ob_low     = ob.get("ob_low")  if ob.get("valid") else None
    bos_bull   = structure.get("bos")=="BULLISH" or structure.get("choch")=="BULLISH"
    bos_bear   = structure.get("bos")=="BEARISH" or structure.get("choch")=="BEARISH"

    # Pullback
    if side=="BUY":
        if last_sl and price<=last_sl*1.03:
            result["pullback"]=True; result["detail"].append("Pullback ke structure support")
        elif ob_high and price<=ob_high*1.01:
            result["pullback"]=True; result["detail"].append("Pullback ke Order Block")
        elif liq_map.get("stop_cluster_below") and price<=liq_map["stop_cluster_below"]*1.015:
            result["pullback"]=True; result["detail"].append("Pullback ke stop cluster")
    else:
        if last_sh and price>=last_sh*0.97:
            result["pullback"]=True; result["detail"].append("Pullback ke structure resistance")
        elif ob_low and price>=ob_low*0.99:
            result["pullback"]=True; result["detail"].append("Pullback ke Order Block")
        elif liq_map.get("stop_cluster_above") and price>=liq_map["stop_cluster_above"]*0.985:
            result["pullback"]=True; result["detail"].append("Pullback ke stop cluster")

    # Rejection candle
    if side=="BUY":
        if lower_wick>body*2 and upper_wick<body*0.5:
            result["rejection"]=True; result["detail"].append("Hammer / Bullish Pin Bar")
        elif price>prev_close and body/full_range>0.7:
            result["rejection"]=True; result["detail"].append("Bullish Engulfing")
    else:
        if upper_wick>body*2 and lower_wick<body*0.5:
            result["rejection"]=True; result["detail"].append("Shooting Star")
        elif price<prev_close and body/full_range>0.7:
            result["rejection"]=True; result["detail"].append("Bearish Engulfing")

    # Candle confirmation — primary
    if side=="BUY":
        if ob_high and price>ob_high and prev_close<=ob_high:
            result["candle_conf"]=True; result["detail"].append(f"Close konfirmasi di atas OB ${ob_high:.6f}")
        elif bos_bull and last_sh and price>last_sh and prev_close<=last_sh:
            result["candle_conf"]=True; result["detail"].append(f"Close BOS konfirmasi ${last_sh:.6f}")
        elif liq_map.get("sweep_bull") and price>prev_close and body/full_range>0.6 and curr_vol>avg_vol*1.3:
            result["candle_conf"]=True; result["detail"].append("Bullish close + vol post-sweep")
    else:
        if ob_low and price<ob_low and prev_close>=ob_low:
            result["candle_conf"]=True; result["detail"].append(f"Close konfirmasi di bawah OB ${ob_low:.6f}")
        elif bos_bear and last_sl and price<last_sl and prev_close>=last_sl:
            result["candle_conf"]=True; result["detail"].append(f"Close BOS konfirmasi ${last_sl:.6f}")
        elif liq_map.get("sweep_bear") and price<prev_close and body/full_range>0.6 and curr_vol>avg_vol*1.3:
            result["candle_conf"]=True; result["detail"].append("Bearish close + vol post-sweep")

    # Candle confirmation — fallback (dengan syarat volume)
    if not result["candle_conf"]:
        if side=="BUY" and last_sl and price>last_sl and price>prev_close and curr_vol>avg_vol*1.1:
            result["candle_conf"]=True; result["detail"].append("Candle conf minimal + vol")
        elif side=="SELL" and last_sh and price<last_sh and price<prev_close and curr_vol>avg_vol*1.1:
            result["candle_conf"]=True; result["detail"].append("Candle conf minimal + vol")

    score = (2 if result["pullback"] else 0) + \
            (2 if result["rejection"] else 0) + \
            (2 if result["candle_conf"] else 0)
    result["precision_score"] = score
    result["entry_quality"]   = "READY" if score>=4 else "WAIT" if score>=2 else "SKIP"
    return result


def assign_tier(score: int, structure: dict, precision: dict, liq_map: dict) -> str:
    has_structure  = structure.get("bos") or structure.get("choch")
    has_liq_strong = (liq_map.get("sweep_bull") or liq_map.get("sweep_bear") or
                      (liq_map.get("stop_cluster_above") and liq_map.get("stop_cluster_below")))
    has_liq_any    = has_liq_strong or liq_map.get("equal_highs") or liq_map.get("equal_lows")
    entry_ready    = precision.get("entry_quality")=="READY"

    if score>=TIER_THRESHOLDS["S"] and has_structure and has_liq_strong and entry_ready: return "S"
    if score>=TIER_THRESHOLDS["A+"] and has_structure and entry_ready:                   return "A+"
    if score>=TIER_THRESHOLDS["A"]  and (has_structure or has_liq_any):                  return "A"
    if score>=TIER_THRESHOLDS["B"]:                                                       return "B"
    return "SKIP"


def resolve_entry(price: float, side: str, atr: float,
                  ob: dict, liq_map: dict, structure: dict,
                  precision: dict, late_limit: float) -> dict:
    """
    Tentukan entry method. Jika MARKET tapi harga sudah > late_limit dari
    level trigger → return None (sinyal batal, late signal).
    """
    is_bull   = (side=="BUY")
    ob_valid  = ob.get("valid", False)
    last_sh   = structure.get("last_sh")
    last_sl   = structure.get("last_sl")

    # BREAKOUT — harga belum break level
    if is_bull and last_sh and price<last_sh*BREAKOUT_THRESHOLD_BUY:
        return {"method":"BREAKOUT","entry_price":round(last_sh*1.001,8),
                "entry_zone":(round(last_sh*1.000,8),round(last_sh*1.003,8)),
                "trigger":f"Tunggu candle CLOSE di atas ${last_sh:.6f}",
                "invalidate":f"Batal jika turun di bawah ${last_sl:.6f}" if last_sl else "Pantau SL",
                "emoji":"🚀"}
    if not is_bull and last_sl and price>last_sl*BREAKOUT_THRESHOLD_SELL:
        return {"method":"BREAKOUT","entry_price":round(last_sl*0.999,8),
                "entry_zone":(round(last_sl*0.997,8),round(last_sl*1.000,8)),
                "trigger":f"Tunggu candle CLOSE di bawah ${last_sl:.6f}",
                "invalidate":f"Batal jika naik di atas ${last_sh:.6f}" if last_sh else "Pantau SL",
                "emoji":"🚀"}

    # PULLBACK — entry di OB/cluster/structure
    if is_bull:
        if ob_valid and price>ob["ob_high"]*PULLBACK_OB_BUY:
            return {"method":"PULLBACK","entry_price":round(ob["ob_mid"],8),
                    "entry_zone":(round(ob["ob_low"],8),round(ob["ob_high"],8)),
                    "trigger":f"Tunggu pullback ke OB ${ob['ob_low']:.6f}–${ob['ob_high']:.6f}",
                    "invalidate":f"Batal jika close di bawah ${ob['ob_low']*0.998:.6f}","emoji":"⏬"}
        if liq_map.get("stop_cluster_below") and price>liq_map["stop_cluster_below"]*PULLBACK_CLUSTER_BUY:
            lvl = liq_map["stop_cluster_below"]
            return {"method":"PULLBACK","entry_price":round(lvl*1.003,8),
                    "entry_zone":(round(lvl*0.999,8),round(lvl*1.006,8)),
                    "trigger":f"Tunggu pullback ke cluster ${lvl:.6f}",
                    "invalidate":f"Batal jika break di bawah ${lvl*0.995:.6f}","emoji":"⏬"}
        if last_sl and price>last_sl*PULLBACK_STRUCT_BUY:
            return {"method":"PULLBACK","entry_price":round(last_sl*1.005,8),
                    "entry_zone":(round(last_sl,8),round(last_sl*1.015,8)),
                    "trigger":f"Tunggu pullback ke support ${last_sl:.6f}",
                    "invalidate":f"Batal jika close di bawah ${last_sl*0.997:.6f}","emoji":"⏬"}
    else:
        if ob_valid and price<ob["ob_low"]*PULLBACK_OB_SELL:
            return {"method":"PULLBACK","entry_price":round(ob["ob_mid"],8),
                    "entry_zone":(round(ob["ob_low"],8),round(ob["ob_high"],8)),
                    "trigger":f"Tunggu rally ke OB ${ob['ob_low']:.6f}–${ob['ob_high']:.6f}",
                    "invalidate":f"Batal jika close di atas ${ob['ob_high']*1.002:.6f}","emoji":"⏫"}
        if liq_map.get("stop_cluster_above") and price<liq_map["stop_cluster_above"]*PULLBACK_CLUSTER_SELL:
            lvl = liq_map["stop_cluster_above"]
            return {"method":"PULLBACK","entry_price":round(lvl*0.997,8),
                    "entry_zone":(round(lvl*0.994,8),round(lvl*1.001,8)),
                    "trigger":f"Tunggu rally ke cluster ${lvl:.6f}",
                    "invalidate":f"Batal jika break di atas ${lvl*1.005:.6f}","emoji":"⏫"}
        if last_sh and price<last_sh*PULLBACK_STRUCT_SELL:
            return {"method":"PULLBACK","entry_price":round(last_sh*0.995,8),
                    "entry_zone":(round(last_sh*0.985,8),round(last_sh,8)),
                    "trigger":f"Tunggu rally ke resistance ${last_sh:.6f}",
                    "invalidate":f"Batal jika close di atas ${last_sh*1.003:.6f}","emoji":"⏫"}

    # MARKET — cek late signal filter
    trigger_level = last_sh if is_bull else last_sl
    if trigger_level:
        drift = abs(price - trigger_level) / trigger_level
        if drift > late_limit:
            return None   # LATE SIGNAL — sinyal dibatalkan

    inval = (f"Batal jika turun di bawah ${last_sl:.6f}" if is_bull and last_sl
             else f"Batal jika naik di atas ${last_sh:.6f}" if not is_bull and last_sh
             else "Pantau SL")
    return {"method":"MARKET","entry_price":price,"entry_zone":None,
            "trigger":"Entry sekarang — semua konfirmasi terpenuhi",
            "invalidate":inval,"emoji":"✅"}


def calc_sl_tp(entry: float, side: str, atr: float,
               structure: dict, liq_map: dict,
               strategy: str) -> tuple:
    """
    SL berbasis structure + ATR buffer. TP berbasis spec per strategi.
    Return: (sl, tp1, tp2)
    """
    last_sh = structure.get("last_sh"); last_sl = structure.get("last_sl")
    atr_mult = {"SCALPING":SCALPING_SL_ATR_MULT,"INTRADAY":INTRADAY_SL_ATR_MULT,
                "SWING":SWING_SL_ATR_MULT,"MOONSHOT":SWING_SL_ATR_MULT}.get(strategy, 2.0)

    if side=="BUY":
        # SL: last swing low - ATR buffer
        base_sl = (last_sl - atr*atr_mult) if last_sl else (entry - atr*atr_mult)
        if liq_map.get("sweep_level"):
            base_sl = min(base_sl, liq_map["sweep_level"]*0.998)
        sl = base_sl
        if strategy=="SCALPING":
            tp1 = entry + atr*SCALPING_TP1_ATR_MULT
            tp2 = entry + atr*SCALPING_TP2_ATR_MULT
        elif strategy=="INTRADAY":
            tp1 = entry*(1+INTRADAY_TP1_PCT); tp2 = entry*(1+INTRADAY_TP2_PCT)
        elif strategy=="SWING":
            tp1 = entry*(1+SWING_TP1_PCT);    tp2 = entry*(1+SWING_TP2_PCT)
        else:  # MOONSHOT
            tp1 = entry*(1+MOONSHOT_TP1_PCT); tp2 = entry*(1+MOONSHOT_TP2_PCT)
    else:
        base_sl = (last_sh + atr*atr_mult) if last_sh else (entry + atr*atr_mult)
        if liq_map.get("sweep_level"):
            base_sl = max(base_sl, liq_map["sweep_level"]*1.002)
        sl = base_sl
        if strategy=="SCALPING":
            tp1 = entry - atr*SCALPING_TP1_ATR_MULT
            tp2 = entry - atr*SCALPING_TP2_ATR_MULT
        elif strategy=="INTRADAY":
            tp1 = entry*(1-INTRADAY_TP1_PCT); tp2 = entry*(1-INTRADAY_TP2_PCT)
        elif strategy=="SWING":
            tp1 = entry*(1-SWING_TP1_PCT);    tp2 = entry*(1-SWING_TP2_PCT)
        else:
            tp1 = entry*(1-MOONSHOT_TP1_PCT); tp2 = entry*(1-MOONSHOT_TP2_PCT)

    return round(sl,8), round(tp1,8), round(tp2,8)


def wscore(conditions: list) -> int:
    return sum(W.get(key,0) for cond,key in conditions if cond)


def volatility_ok(atr: float, price: float, mode: str) -> tuple:
    pct = (atr/price)*100 if price>0 else 0
    # [NEW 2] Gunakan adaptive ATR bands jika tersedia
    mn = _adaptive["atr_min_pct"].get(mode, ATR_MIN_PCT.get(mode, 0.3))
    mx = _adaptive["atr_max_pct"].get(mode, ATR_MAX_PCT.get(mode, 8.0))
    if pct<mn: return False, f"Vol rendah ({pct:.2f}%)"
    if pct>mx: return False, f"Vol tinggi ({pct:.2f}%)"
    return True, ""


def get_htf_bias(client, pair: str) -> str:
    closes,_,_,_ = get_candles(client, pair, "1h", 55)
    if closes is None: return "NEUTRAL"
    ema20 = calc_ema(closes,20); ema50 = calc_ema(closes,50); price = float(closes[-1])
    if price>ema20 and ema20>ema50: return "BULLISH"
    if price<ema20 and ema20<ema50: return "BEARISH"
    return "NEUTRAL"


def is_dead_session() -> bool:
    """True jika jam WIB saat ini masuk dead hours (scalping off)."""
    hour = datetime.now(WIB).hour
    return DEAD_HOURS_START <= hour < DEAD_HOURS_END


# ════════════════════════════════════════════════════════
#  L5. STRATEGY LAYER
# ════════════════════════════════════════════════════════

def check_scalping(client, pair, price, fg, ob_ratio, funding,
                   liq_bias, oi_signal, regime):
    if is_dead_session():
        return  # scalping off saat dead hours

    closes,highs,lows,volumes = get_candles(client, pair, "5m", 80)
    if closes is None: return

    rsi       = calc_rsi(closes);       stoch_rsi = calc_stoch_rsi(closes)
    ema9      = calc_ema(closes,9);     ema21     = calc_ema(closes,21)
    bb_low,_,bb_high = calc_bb(closes)
    atr       = calc_atr(closes,highs,lows)
    vwap      = calc_vwap(closes,highs,lows,volumes)
    support,resistance = calc_support_resistance(highs,lows,closes)
    divergence = calc_rsi_divergence(closes,highs,lows)
    vol_avg   = np.mean(volumes[-20:])
    vol_ratio = volumes[-1]/vol_avg if vol_avg>0 else 0

    structure = detect_structure(closes,highs,lows,
                                 lookback=SCALPING_BOS_LOOKBACK,
                                 strength=SCALPING_SWING_STRENGTH)
    liq_map   = detect_liquidity_map(closes,highs,lows,volumes,lookback=20)

    vol_ok,_ = volatility_ok(atr,price,"SCALPING")
    if not vol_ok: return

    htf_bias  = get_htf_bias(client, pair)

    # WATCHING alert
    _emit_watching(pair, structure, liq_map, price, "5m")

    for side in (["BUY"] if BUY_ONLY else ["BUY", "SELL"]):
        is_bull = (side=="BUY")

        htf_ct = (is_bull and htf_bias=="BEARISH") or (not is_bull and htf_bias=="BULLISH")

        has_structure_signal = (
            (is_bull and (structure.get("bos")=="BULLISH" or structure.get("choch")=="BULLISH")) or
            (not is_bull and (structure.get("bos")=="BEARISH" or structure.get("choch")=="BEARISH"))
        )
        has_liq_signal = (
            (is_bull  and (liq_map.get("sweep_bull") or liq_map.get("equal_lows") is not None)) or
            (not is_bull and (liq_map.get("sweep_bear") or liq_map.get("equal_highs") is not None))
        )
        if not has_structure_signal and not has_liq_signal: continue
        if is_bull  and liq_map.get("fake_bo_bull"): continue
        if not is_bull and liq_map.get("fake_bo_bear"): continue

        ob        = detect_order_block(closes,highs,lows,volumes,side=side,lookback=20)
        precision = check_entry_precision(closes,highs,lows,volumes,side,structure,liq_map,ob)
        if precision["entry_quality"]=="SKIP": continue

        if is_bull:
            conditions = [
                (structure.get("bos")=="BULLISH",        "bos"),
                (structure.get("choch")=="BULLISH",      "choch"),
                (liq_map.get("sweep_bull"),              "liq_sweep"),
                (liq_map.get("equal_lows") is not None, "equal_hl"),
                (ob.get("valid"),                        "order_block"),
                (precision.get("pullback"),              "pullback"),
                (precision.get("rejection"),             "rejection"),
                (precision.get("candle_conf"),           "candle_conf"),
                (rsi<35,                                 "rsi_extreme"),
                (stoch_rsi<0.2,                         "stoch_rsi"),
                (ema9>ema21,                             "ema_cross"),
                (price<=bb_low,                         "bb_extreme"),
                (ob_ratio>1.2,                           "ob_ratio"),
                (fg<25,                                  "fg_extreme"),
                (vol_ratio>2.5,                         "vol_spike"),
                (divergence=="BULLISH",                 "divergence"),
                (price<=support*1.02,                   "support_res"),
                (funding and funding<-0.001,            "funding"),
                (price>vwap,                            "vwap"),
                (liq_bias=="BUY",                       "liq_cluster"),
                (oi_signal in("STRONG_BUY","SQUEEZE"),  "oi_signal"),
            ]
        else:
            conditions = [
                (structure.get("bos")=="BEARISH",          "bos"),
                (structure.get("choch")=="BEARISH",        "choch"),
                (liq_map.get("sweep_bear"),                "liq_sweep"),
                (liq_map.get("equal_highs") is not None,  "equal_hl"),
                (ob.get("valid"),                          "order_block"),
                (precision.get("pullback"),                "pullback"),
                (precision.get("rejection"),               "rejection"),
                (precision.get("candle_conf"),             "candle_conf"),
                (rsi>68,                                   "rsi_extreme"),
                (stoch_rsi>0.8,                           "stoch_rsi"),
                (ema9<ema21,                               "ema_cross"),
                (price>=bb_high,                          "bb_extreme"),
                (ob_ratio<0.8,                             "ob_ratio"),
                (fg>65,                                    "fg_extreme"),
                (vol_ratio>2.5,                           "vol_spike"),
                (divergence=="BEARISH",                   "divergence"),
                (price>=resistance*0.98,                  "support_res"),
                (funding and funding>0.001,               "funding"),
                (price<vwap,                              "vwap"),
                (liq_bias=="SELL",                        "liq_cluster"),
                (oi_signal=="STRONG_SELL",                "oi_signal"),
            ]

        score = wscore(conditions)
        if htf_ct: score = max(0, score-3)
        tier  = assign_tier(score,structure,precision,liq_map)
        if tier=="SKIP": continue

        entry_info = resolve_entry(price,side,atr,ob,liq_map,structure,precision,
                                   LATE_SIGNAL_LIMIT_SCALP)
        if entry_info is None:
            print(f"  ↩️ {pair} [SCALP {side}] LATE SIGNAL — skip")
            continue

        final_entry = entry_info["entry_price"]
        sl,tp1,tp2  = calc_sl_tp(final_entry,side,atr,structure,liq_map,"SCALPING")
        stars_val   = min(5,max(1,score//2))

        extra = f"📡 <i>Regime: {regime['regime']} | BTC 1h: {regime['btc_1h_chg']:+.1f}%</i>"
        if htf_ct: extra += f"\n⚠️ <i>Counter-trend HTF {htf_bias} — size kecil</i>"

        queue_signal(pair=pair,signal_type="SCALPING",side=side,
                     entry=final_entry,tp1=tp1,tp2=tp2,sl=sl,
                     strength=stars_val,timeframe="5m",valid_minutes=30,
                     tier=tier,score=score,
                     sources="Gate.io · Structure · Liq · EMA · BB",
                     extra=extra,structure=structure,liq_map=liq_map,
                     precision=precision,ob=ob,entry_info=entry_info)


def check_intraday(client, pair, price, fg, ob_ratio, funding,
                   trending, liq_bias, oi_signal, regime):
    closes,highs,lows,volumes = get_candles(client, pair, "1h", 100)
    if closes is None: return

    rsi               = calc_rsi(closes);       stoch_rsi = calc_stoch_rsi(closes)
    macd,msig         = calc_macd(closes)
    bb_low,_,bb_high  = calc_bb(closes)
    atr               = calc_atr(closes,highs,lows)
    ema20             = calc_ema(closes,20);     ema50 = calc_ema(closes,50)
    vwap              = calc_vwap(closes,highs,lows,volumes)
    ichi              = calc_ichimoku(closes,highs,lows)
    support,resistance= calc_support_resistance(highs,lows,closes)
    divergence        = calc_rsi_divergence(closes,highs,lows)
    poc               = calc_volume_profile(closes,volumes)
    is_trending       = pair.replace("_USDT","") in trending

    structure = detect_structure(closes,highs,lows,
                                 lookback=INTRADAY_BOS_LOOKBACK,
                                 strength=INTRADAY_SWING_STRENGTH)
    liq_map   = detect_liquidity_map(closes,highs,lows,volumes,lookback=30)

    vol_ok,_ = volatility_ok(atr,price,"INTRADAY")
    if not vol_ok: return

    _emit_watching(pair, structure, liq_map, price, "1h")

    for side in (["BUY"] if BUY_ONLY else ["BUY", "SELL"]):
        is_bull = (side=="BUY")

        has_struct = (
            (is_bull and (structure.get("bos")=="BULLISH" or structure.get("choch")=="BULLISH"
                          or liq_map.get("sweep_bull"))) or
            (not is_bull and (structure.get("bos")=="BEARISH" or structure.get("choch")=="BEARISH"
                              or liq_map.get("sweep_bear")))
        )
        if not has_struct: continue
        if regime.get("regime")=="RISK_OFF" and pair not in SAFE_PAIRS_RISK_OFF: continue
        if is_bull and regime.get("block_buy"): continue
        if not is_bull and regime.get("block_sell"): continue
        if is_bull  and liq_map.get("fake_bo_bull"): continue
        if not is_bull and liq_map.get("fake_bo_bear"): continue
        if is_bull and macd<=msig: continue
        if not is_bull and macd>=msig: continue

        ob        = detect_order_block(closes,highs,lows,volumes,side=side,lookback=30)
        precision = check_entry_precision(closes,highs,lows,volumes,side,structure,liq_map,ob)
        if precision["entry_quality"]=="SKIP": continue

        if is_bull:
            conditions = [
                (structure.get("bos")=="BULLISH",          "bos"),
                (structure.get("choch")=="BULLISH",        "choch"),
                (liq_map.get("sweep_bull"),                "liq_sweep"),
                (liq_map.get("equal_lows") is not None,   "equal_hl"),
                (liq_map.get("stop_cluster_below") is not None,"stop_cluster"),
                (ob.get("valid"),                          "order_block"),
                (precision.get("pullback"),                "pullback"),
                (precision.get("rejection"),               "rejection"),
                (precision.get("candle_conf"),             "candle_conf"),
                (rsi<35,                                   "rsi_extreme"),
                (stoch_rsi<0.25,                          "stoch_rsi"),
                (price<=bb_low,                           "bb_extreme"),
                (fg<25,                                    "fg_extreme"),
                (ob_ratio>1.1,                             "ob_ratio"),
                (is_trending,                              "trending"),
                (ema20>ema50,                              "ema_cross"),
                (divergence=="BULLISH",                   "divergence"),
                (poc and price<=poc,                      "poc"),
                (price<=support*1.03,                     "support_res"),
                (funding and funding<-0.001,              "funding"),
                (price>vwap,                              "vwap"),
                (ichi.get("above_cloud"),                 "ichimoku"),
                (liq_bias=="BUY",                         "liq_cluster"),
                (oi_signal in("STRONG_BUY","SQUEEZE"),    "oi_signal"),
            ]
        else:
            conditions = [
                (structure.get("bos")=="BEARISH",          "bos"),
                (structure.get("choch")=="BEARISH",        "choch"),
                (liq_map.get("sweep_bear"),                "liq_sweep"),
                (liq_map.get("equal_highs") is not None,  "equal_hl"),
                (liq_map.get("stop_cluster_above") is not None,"stop_cluster"),
                (ob.get("valid"),                          "order_block"),
                (precision.get("pullback"),                "pullback"),
                (precision.get("rejection"),               "rejection"),
                (precision.get("candle_conf"),             "candle_conf"),
                (rsi>65,                                   "rsi_extreme"),
                (stoch_rsi>0.75,                          "stoch_rsi"),
                (price>=bb_high,                          "bb_extreme"),
                (fg>60 or fg<20,                           "fg_extreme"),
                (ob_ratio<0.9,                             "ob_ratio"),
                (divergence=="BEARISH",                   "divergence"),
                (poc and price>=poc,                      "poc"),
                (price>=resistance*0.97,                  "support_res"),
                (funding and funding>0.001,               "funding"),
                (price<vwap,                              "vwap"),
                (ichi.get("below_cloud"),                 "ichimoku"),
                (liq_bias=="SELL",                        "liq_cluster"),
                (oi_signal=="STRONG_SELL",                "oi_signal"),
            ]

        score = wscore(conditions)
        tier  = assign_tier(score,structure,precision,liq_map)
        if tier=="SKIP": continue

        entry_info = resolve_entry(price,side,atr,ob,liq_map,structure,precision,
                                   LATE_SIGNAL_LIMIT_INTRA)
        if entry_info is None:
            print(f"  ↩️ {pair} [INTRA {side}] LATE SIGNAL — skip")
            continue

        final_entry = entry_info["entry_price"]
        sl,tp1,tp2  = calc_sl_tp(final_entry,side,atr,structure,liq_map,"INTRADAY")
        stars_val   = min(5,max(1,score//3))

        extra = f"📡 <i>Regime: {regime['regime']} | BTC 1h: {regime['btc_1h_chg']:+.1f}%</i>"
        if ichi.get("above_cloud" if is_bull else "below_cloud"):
            extra += f"\n☁️ <i>Ichimoku: {'di atas' if is_bull else 'di bawah'} cloud</i>"

        queue_signal(pair=pair,signal_type="INTRADAY",side=side,
                     entry=final_entry,tp1=tp1,tp2=tp2,sl=sl,
                     strength=stars_val,timeframe="1h",valid_minutes=120,
                     tier=tier,score=score,
                     sources="Gate.io · Structure · Liq · MACD · Ichimoku",
                     extra=extra,structure=structure,liq_map=liq_map,
                     precision=precision,ob=ob,entry_info=entry_info)


def check_swing(client, pair, price, fg, ob_ratio, funding,
                trending, liq_bias, oi_signal, regime):
    closes,highs,lows,volumes = get_candles(client, pair, "4h", 210)
    if closes is None: return

    rsi        = calc_rsi(closes);       stoch_rsi  = calc_stoch_rsi(closes)
    ema50      = calc_ema(closes,50);    ema200     = calc_ema(closes,200)
    atr        = calc_atr(closes,highs,lows)
    macd,msig  = calc_macd(closes)
    vwap       = calc_vwap(closes,highs,lows,volumes)
    ichi       = calc_ichimoku(closes,highs,lows)
    support,resistance = calc_support_resistance(highs,lows,closes,lookback=50)
    divergence = calc_rsi_divergence(closes,highs,lows,lookback=40)
    poc        = calc_volume_profile(closes,volumes,bins=20)
    vol_trend  = np.mean(volumes[-5:])>np.mean(volumes[-20:])*1.2
    is_trending= pair.replace("_USDT","") in trending

    structure = detect_structure(closes,highs,lows,
                                 lookback=SWING_BOS_LOOKBACK,
                                 strength=SWING_SWING_STRENGTH)
    liq_map   = detect_liquidity_map(closes,highs,lows,volumes,lookback=50)

    vol_ok,_ = volatility_ok(atr,price,"SWING")
    if not vol_ok: return

    for side in (["BUY"] if BUY_ONLY else ["BUY", "SELL"]):
        is_bull = (side=="BUY")

        has_struct = (
            (is_bull and (structure.get("bos")=="BULLISH" or structure.get("choch")=="BULLISH"
                          or liq_map.get("sweep_bull"))) or
            (not is_bull and (structure.get("bos")=="BEARISH" or structure.get("choch")=="BEARISH"
                              or liq_map.get("sweep_bear")))
        )
        if not has_struct: continue
        if regime.get("regime")=="RISK_OFF" and pair not in SAFE_PAIRS_RISK_OFF: continue
        if is_bull  and regime.get("block_buy"):   continue
        if not is_bull and regime.get("block_sell"): continue
        if is_bull  and liq_map.get("fake_bo_bull"): continue
        if not is_bull and liq_map.get("fake_bo_bear"): continue
        if is_bull  and macd<=msig: continue
        if not is_bull and macd>=msig: continue

        ob        = detect_order_block(closes,highs,lows,volumes,side=side,lookback=40)
        precision = check_entry_precision(closes,highs,lows,volumes,side,structure,liq_map,ob)
        if precision["entry_quality"]=="SKIP": continue

        if is_bull:
            conditions = [
                (structure.get("bos")=="BULLISH",          "bos"),
                (structure.get("choch")=="BULLISH",        "choch"),
                (liq_map.get("sweep_bull"),                "liq_sweep"),
                (liq_map.get("equal_lows") is not None,   "equal_hl"),
                (liq_map.get("stop_cluster_below") is not None,"stop_cluster"),
                (ob.get("valid"),                          "order_block"),
                (precision.get("pullback"),                "pullback"),
                (precision.get("rejection"),               "rejection"),
                (precision.get("candle_conf"),             "candle_conf"),
                (ema50>ema200,                             "ema_cross"),
                (rsi<35,                                   "rsi_extreme"),
                (stoch_rsi<0.3,                           "stoch_rsi"),
                (fg<25,                                    "fg_extreme"),
                (vol_trend,                                "vol_spike"),
                (is_trending,                              "trending"),
                (ob_ratio>1.1,                             "ob_ratio"),
                (divergence=="BULLISH",                   "divergence"),
                (poc and price<=poc,                      "poc"),
                (price<=support*1.05,                     "support_res"),
                (funding and funding<-0.002,              "funding"),
                (price>vwap,                              "vwap"),
                (ichi.get("above_cloud"),                 "ichimoku"),
                (liq_bias=="BUY",                         "liq_cluster"),
                (oi_signal in("STRONG_BUY","SQUEEZE"),    "oi_signal"),
            ]
        else:
            conditions = [
                (structure.get("bos")=="BEARISH",          "bos"),
                (structure.get("choch")=="BEARISH",        "choch"),
                (liq_map.get("sweep_bear"),                "liq_sweep"),
                (liq_map.get("equal_highs") is not None,  "equal_hl"),
                (liq_map.get("stop_cluster_above") is not None,"stop_cluster"),
                (ob.get("valid"),                          "order_block"),
                (precision.get("pullback"),                "pullback"),
                (precision.get("rejection"),               "rejection"),
                (precision.get("candle_conf"),             "candle_conf"),
                (ema50<ema200,                             "ema_cross"),
                (rsi>65,                                   "rsi_extreme"),
                (stoch_rsi>0.7,                           "stoch_rsi"),
                (fg>60,                                    "fg_extreme"),
                (vol_trend,                                "vol_spike"),
                (divergence=="BEARISH",                   "divergence"),
                (funding and funding>0.002,               "funding"),
                (price<vwap,                              "vwap"),
                (ichi.get("below_cloud"),                 "ichimoku"),
                (liq_bias=="SELL",                        "liq_cluster"),
                (oi_signal=="STRONG_SELL",                "oi_signal"),
            ]

        score = wscore(conditions)
        tier  = assign_tier(score,structure,precision,liq_map)
        if tier=="SKIP": continue

        entry_info  = resolve_entry(price,side,atr,ob,liq_map,structure,precision,0.03)
        if entry_info is None: continue
        final_entry = entry_info["entry_price"]
        sl,tp1,tp2  = calc_sl_tp(final_entry,side,atr,structure,liq_map,"SWING")
        stars_val   = min(5,max(1,score//4))

        extra = f"📡 <i>Regime: {regime['regime']} | BTC: {regime['btc_trend']}</i>"
        if is_bull  and ema50>ema200: extra += "\n✨ <i>Golden Cross EMA 50/200</i>"
        if not is_bull and ema50<ema200: extra += "\n💀 <i>Death Cross EMA 50/200</i>"

        queue_signal(pair=pair,signal_type="SWING",side=side,
                     entry=final_entry,tp1=tp1,tp2=tp2,sl=sl,
                     strength=stars_val,timeframe="4h",valid_minutes=720,
                     tier=tier,score=score,
                     sources="Gate.io · Structure · Liq · EMA50/200 · Ichimoku",
                     extra=extra,structure=structure,liq_map=liq_map,
                     precision=precision,ob=ob,entry_info=entry_info)


def check_moonshot(client, pair, price, change_24h, trending, liq_bias, regime):
    """
    [UPGRADE P2] Moonshot kini memiliki dua path deteksi:

    PATH A — MOMENTUM (reaktif, seperti sebelumnya):
      vol_ratio tinggi + change_24h tinggi → harga sudah mulai pump
      Sinyal type: MOONSHOT

    PATH B — PRE_PUMP (baru, prediktif):
      Volume dry-up selesai + RSI keluar oversold + struktur baru terbentuk
      → Sinyal sebelum pump terjadi, entry lebih awal, R/R lebih baik
      Sinyal type: MOONSHOT_PREPUMP
    """
    if regime.get("regime")=="RISK_OFF": return
    if regime.get("block_buy"): return

    closes,highs,lows,volumes = get_candles(client, pair, "1h", 72)
    if closes is None: return

    vol_avg   = float(np.mean(volumes[:-6]))
    vol_now   = float(np.mean(volumes[-6:]))
    vol_ratio = vol_now / (vol_avg + 1e-9)
    rsi       = calc_rsi(closes)
    if rsi >= 70: return   # overbought — skip kedua path

    atr        = calc_atr(closes,highs,lows)
    vwap       = calc_vwap(closes,highs,lows,volumes)
    support,resistance = calc_support_resistance(highs,lows,closes)
    is_trending = pair.replace("_USDT","") in trending

    structure = detect_structure(closes,highs,lows,
                                 lookback=MOONSHOT_BOS_LOOKBACK,
                                 strength=MOONSHOT_SWING_STRENGTH)
    liq_map   = detect_liquidity_map(closes,highs,lows,volumes,lookback=20)

    # HTF filter 4h — sama untuk kedua path
    try:
        c4h,_,_,_ = get_candles(client, pair, "4h", 55)
        if c4h is not None:
            e20 = calc_ema(c4h,20); e50 = calc_ema(c4h,50)
            if float(c4h[-1])<e20 and e20<e50:
                return  # 4h bearish — skip
    except Exception:
        pass

    # ── PATH B: PRE_PUMP (prediktif) ─────────────────────
    # Dijalankan lebih dulu — jika kondisi terpenuhi, Path A tidak perlu
    dryup = calc_volume_dryup(closes, volumes)

    if (dryup["is_dryup"] and
        change_24h <= PREPUMP_MAX_CHANGE and
        rsi >= _adaptive.get("prepump_rsi_exit", PREPUMP_RSI_EXIT) and   # [NEW 2] adaptive
        rsi <= PREPUMP_RSI_MAX):

        # Butuh minimal ada tanda struktur baru terbentuk
        has_new_structure = (
            structure.get("bos") == "BULLISH" or
            structure.get("choch") == "BULLISH" or
            liq_map.get("sweep_bull")
        )
        if not has_new_structure:
            pass  # lanjut ke path A jika tidak ada struktur
        else:
            ob   = detect_order_block(closes,highs,lows,volumes,side="BUY",lookback=20)
            prec = check_entry_precision(closes,highs,lows,volumes,"BUY",structure,liq_map,ob)
            if prec["entry_quality"] != "SKIP":

                # [FIX 1] Confirmation Layer — wajib lolos sebelum masuk queue
                conf = check_confirmation_layer(closes,highs,lows,volumes,structure,"BUY")
                if not conf["go"]:
                    print(f"  🚫 PRE_PUMP {pair} conf FAIL "
                          f"({conf['passed']}/{CONF_MIN_PASSED})")
                    # Tidak return — biarkan lanjut ke Path A
                else:
                    conditions = [
                        (structure.get("bos")=="BULLISH",          "bos"),
                        (structure.get("choch")=="BULLISH",        "choch"),
                        (liq_map.get("sweep_bull"),                "liq_sweep"),
                        (liq_map.get("equal_lows") is not None,   "equal_hl"),
                        (ob.get("valid"),                          "order_block"),
                        (prec.get("pullback"),                     "pullback"),
                        (prec.get("rejection"),                    "rejection"),
                        (prec.get("candle_conf"),                  "candle_conf"),
                        (dryup["price_stable"],                    "support_res"),
                        (dryup["vol_revival"],                     "vol_spike"),
                        (is_trending,                              "trending"),
                        (rsi < 50,                                 "rsi_extreme"),
                        (price > vwap,                             "vwap"),
                        (liq_bias == "BUY",                        "liq_cluster"),
                    ]
                    score = wscore(conditions)

                    if score >= PREPUMP_MIN_SCORE:
                        tier = assign_tier(score,structure,prec,liq_map)
                        if tier != "SKIP":
                            entry_info  = resolve_entry(price,"BUY",atr,ob,liq_map,structure,prec,0.03)
                            if entry_info is not None:
                                final_entry = entry_info["entry_price"]
                                sl,tp1,tp2  = calc_sl_tp(final_entry,"BUY",atr,structure,liq_map,"MOONSHOT")
                                stars_val   = min(5,max(1,score//2))

                                conf_str = f"{conf['passed']}/3 conf ✅"
                                extra = (
                                    f"📡 <i>Regime: {regime['regime']}</i>\n"
                                    f"💤 <b>Vol Dry-Up: {dryup['dryup_ratio']:.2f}x</b> → "
                                    f"Revival: <b>{dryup['revival_ratio']:.1f}x</b>\n"
                                    f"📊 RSI: <b>{rsi:.0f}</b> (keluar oversold)\n"
                                    f"🔒 Conf: <b>{conf_str}</b>\n"
                                    f"⚡ <i>PRE-PUMP — Entry sebelum pump terjadi</i>\n"
                                    f"⚠️ <i>High Risk — SL wajib ketat</i>"
                                )
                                queue_signal(pair=pair,signal_type="MOONSHOT_PREPUMP",side="BUY",
                                             entry=final_entry,tp1=tp1,tp2=tp2,sl=sl,
                                             strength=stars_val,timeframe="1h",valid_minutes=480,
                                             tier=tier,score=score,
                                             sources="Gate.io · Vol Dry-Up · Structure · RSI Revival",
                                             extra=extra,structure=structure,liq_map=liq_map,
                                             precision=prec,ob=ob,entry_info=entry_info)
                                print(f"  🔮 PRE_PUMP {pair} dryup:{dryup['dryup_ratio']:.2f}x "
                                      f"revival:{dryup['revival_ratio']:.1f}x rsi:{rsi:.0f} "
                                      f"score:{score} tier:{tier} conf:{conf['passed']}/3")
                                return  # tidak lanjut ke path A

    # ── PATH A: MOMENTUM (reaktif, kondisi asli) ─────────
    if not (vol_ratio > 4.0 and 25 < rsi < 70 and change_24h > 3.0): return

    ob   = detect_order_block(closes,highs,lows,volumes,side="BUY",lookback=20)
    prec = check_entry_precision(closes,highs,lows,volumes,"BUY",structure,liq_map,ob)
    if prec["entry_quality"]=="SKIP": return

    conditions = [
        (vol_ratio>7.0,                           "vol_spike"),
        (is_trending,                             "trending"),
        (change_24h>8.0,                          "momentum"),
        (rsi<55,                                  "stoch_rsi"),
        (price>resistance,                        "support_res"),
        (price>vwap,                              "vwap"),
        (liq_bias=="BUY",                         "liq_cluster"),
        (structure.get("bos")=="BULLISH",         "bos"),
        (liq_map.get("sweep_bull"),               "liq_sweep"),
        (liq_map.get("equal_lows") is not None,  "equal_hl"),
    ]
    score = wscore(conditions)
    tier  = assign_tier(score,structure,prec,liq_map)
    if tier=="SKIP": return

    entry_info  = resolve_entry(price,"BUY",atr,ob,liq_map,structure,prec,0.03)
    if entry_info is None: return
    final_entry = entry_info["entry_price"]
    sl,tp1,tp2  = calc_sl_tp(final_entry,"BUY",atr,structure,liq_map,"MOONSHOT")
    stars_val   = min(5,max(1,score//2))

    extra = (f"📡 <i>Regime: {regime['regime']}</i>\n"
             f"🔥 Vol: <b>{vol_ratio:.1f}x</b> | 24h: <b>+{change_24h:.1f}%</b>\n"
             f"⚠️ <i>High Risk — size kecil, SL wajib ketat</i>")

    queue_signal(pair=pair,signal_type="MOONSHOT",side="BUY",
                 entry=final_entry,tp1=tp1,tp2=tp2,sl=sl,
                 strength=stars_val,timeframe="1h",valid_minutes=360,
                 tier=tier,score=score,
                 sources="Gate.io · Structure · Liq · CoinGecko",
                 extra=extra,structure=structure,liq_map=liq_map,
                 precision=prec,ob=ob,entry_info=entry_info)


def check_microcap_accumulation(client, pair, price, vol_24h, change_24h,
                                 trending, regime, microcap_count: list):
    """
    Deteksi koin microcap yang sedang dalam fase akumulasi / awal pre-pump.
    Berbeda dari MOONSHOT:
    - Tidak butuh change_24h > 3% — cukup mulai bergerak (>1.5%)
    - Volume threshold jauh lebih rendah (50K bukan 300K)
    - RSI harus masih di zona sehat (<55) — belum overbought
    - Filter 4H tidak membuang jika sideways — hanya buang jika FULL bearish

    Sinyal ini adalah MOONSHOT_ACCUM — type terpisah dari MOONSHOT reguler.
    """
    # Jangan jalankan di kondisi panik pasar
    if regime.get("regime") == "RISK_OFF": return
    if regime.get("block_buy"): return

    # Batas sinyal microcap per cycle
    if microcap_count[0] >= MICROCAP_MAX_SIGNALS: return

    # Volume minimum microcap (lebih longgar)
    if vol_24h < MIN_VOLUME_MICROCAP: return

    # Harga harus mulai bergerak — minimal 1.5%, bukan sudah 3%
    if change_24h < MICROCAP_CHANGE_MIN: return

    # Ambil data 1h (48 candle = 2 hari)
    closes, highs, lows, volumes = get_candles(client, pair, "1h", 48)
    if closes is None: return

    # === RSI CHECK ===
    rsi = calc_rsi(closes)
    if rsi >= MICROCAP_RSI_MAX: return   # sudah terlalu panas, telat masuk
    if rsi < MICROCAP_RSI_MIN: return    # terlalu hancur, bukan akumulasi

    # === VOLUME ANOMALY ===
    # Bandingkan rata-rata volume 6 candle terakhir vs 42 candle sebelumnya
    vol_baseline = float(np.mean(volumes[:42]))
    vol_recent   = float(np.mean(volumes[-6:]))
    vol_ratio    = vol_recent / (vol_baseline + 1e-9)

    # Butuh minimal 2x kenaikan volume — tanda ada yang mulai akumulasi
    if vol_ratio < MICROCAP_VOL_RATIO_MIN: return

    # === FILTER 4H — lebih longgar dari moonshot ===
    # Buang HANYA kalau EMA20 < EMA50 DAN harga jauh di bawah EMA50 (full bearish)
    try:
        c4h, _, _, _ = get_candles(client, pair, "4h", 55)
        if c4h is not None:
            e20 = calc_ema(c4h, 20)
            e50 = calc_ema(c4h, 50)
            price_4h = float(c4h[-1])
            # Hanya buang kalau harga di bawah EMA50 lebih dari 10% — full downtrend
            if e20 < e50 and price_4h < e50 * 0.90:
                return
    except Exception:
        pass

    # === STRUCTURE DETECTION ===
    structure = detect_structure(closes, highs, lows,
                                 lookback=MOONSHOT_BOS_LOOKBACK,
                                 strength=MOONSHOT_SWING_STRENGTH)
    liq_map = detect_liquidity_map(closes, highs, lows, volumes, lookback=20)

    # === ORDER BLOCK & PRECISION ===
    ob   = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=20)
    prec = check_entry_precision(closes, highs, lows, volumes, "BUY", structure, liq_map, ob)

    # Untuk microcap accumulation, SKIP jika tidak ada konfirmasi sama sekali
    if prec["entry_quality"] == "SKIP": return

    # === VWAP CHECK ===
    vwap = calc_vwap(closes, highs, lows, volumes)
    atr  = calc_atr(closes, highs, lows)

    # === [UPGRADE P1] VOLUME DRY-UP CHECK ===
    # Integrasikan dry-up detection ke dalam microcap scoring
    dryup = calc_volume_dryup(closes, volumes)

    # === SCORING — lebih sederhana, fokus pada tanda akumulasi ===
    is_trending = pair.replace("_USDT", "") in trending

    # Tanda-tanda akumulasi yang valid
    phase = structure.get("trend_phase", "")
    is_accumulation_phase = phase in ("ACCUMULATION", "MARKUP", "RANGING")

    conditions = [
        # Core: volume + momentum mulai bangkit
        (vol_ratio > 3.0,                            "vol_spike"),       # +1
        (vol_ratio > 5.0,                            "vol_spike"),       # bonus +1 jika sangat spike
        (change_24h > 3.0,                           "momentum"),        # +1 jika sudah 3%
        (is_trending,                                "trending"),        # +1 trending di CoinGecko

        # [P1] Volume dry-up signals — akumulasi senyap
        (dryup["is_dryup"] and dryup["price_stable"], "support_res"),   # +1 dry-up + harga stabil
        (dryup["vol_revival"],                        "vol_spike"),      # +1 volume mulai bangkit

        # Structure
        (structure.get("bos") == "BULLISH",          "bos"),             # +5 terkuat
        (structure.get("choch") == "BULLISH",        "choch"),           # +5
        (is_accumulation_phase,                      "support_res"),     # +1 fase akumulasi

        # Liquidity
        (liq_map.get("sweep_bull"),                  "liq_sweep"),       # +4 liq sweep bawah
        (liq_map.get("equal_lows") is not None,      "equal_hl"),        # +3 equal lows = stop hunt target

        # Entry quality
        (prec.get("pullback"),                       "pullback"),        # +2
        (prec.get("rejection"),                      "rejection"),       # +2
        (prec.get("candle_conf"),                    "candle_conf"),     # +2

        # Price vs VWAP
        (price > vwap,                               "vwap"),            # +2 di atas VWAP
        (rsi < 45,                                   "rsi_extreme"),     # +1 RSI masih rendah = belum terlambat
    ]

    score = wscore(conditions)

    # Microcap butuh minimal skor 6 — tidak seketat strategi lain tapi punya filter sendiri
    if score < 6: return

    # Assign tier — microcap pakai tier yang sama tapi lebih mudah dapat B
    has_structure = structure.get("bos") or structure.get("choch")
    has_liq       = liq_map.get("sweep_bull") or liq_map.get("equal_lows")

    if score >= 14 and has_structure and has_liq and prec["entry_quality"] == "READY":
        tier = "S"
    elif score >= 9 and has_structure and prec["entry_quality"] == "READY":
        tier = "A+"
    elif score >= 6 and (has_structure or has_liq):
        tier = "A"
    else:
        tier = "B"

    # === ENTRY ===
    entry_info = resolve_entry(price, "BUY", atr, ob, liq_map, structure, prec, 0.05)
    if entry_info is None: return

    final_entry = entry_info["entry_price"]

    # SL berbasis structure + ATR buffer
    last_sl = structure.get("last_sl")
    sl = (last_sl - atr * MICROCAP_SL_ATR_MULT) if last_sl else (final_entry - atr * MICROCAP_SL_ATR_MULT * 1.5)
    sl = round(sl, 8)

    # TP: lebih besar dari MOONSHOT biasa — ini potensi 100%+
    tp1 = round(final_entry * (1 + MICROCAP_TP1_PCT), 8)  # +30%
    tp2 = round(final_entry * (1 + MICROCAP_TP2_PCT), 8)  # +80%

    # R/R check
    pct_tp1 = abs((tp1 - final_entry) / final_entry * 100)
    pct_sl  = abs((sl - final_entry) / final_entry * 100)
    rr      = round(pct_tp1 / pct_sl, 1) if pct_sl > 0 else 0
    if rr < MICROCAP_MIN_RR: return

    stars_val = min(5, max(1, score // 3))

    extra = (
        f"📡 <i>Regime: {regime['regime']}</i>\n"
        f"🔬 Vol: <b>{vol_ratio:.1f}x</b> avg | RSI: <b>{rsi:.0f}</b>\n"
        f"📊 24h: <b>+{change_24h:.1f}%</b> | Phase: <b>{phase}</b>\n"
        f"⚠️ <i>MICROCAP — potensi tinggi, risk tinggi. Size kecil, SL wajib ketat.</i>"
    )

    # Track jumlah sinyal microcap terkirim
    microcap_count[0] += 1

    queue_signal(
        pair=pair, signal_type="MOONSHOT", side="BUY",
        entry=final_entry, tp1=tp1, tp2=tp2, sl=sl,
        strength=stars_val, timeframe="1h", valid_minutes=480,
        tier=tier, score=score,
        sources="Gate.io · Microcap Accumulation · Structure · Vol Anomaly",
        extra=extra, structure=structure, liq_map=liq_map,
        precision=prec, ob=ob, entry_info=entry_info
    )

    print(f"  🔬 MICROCAP_ACCUM {pair} vol:{vol_ratio:.1f}x rsi:{rsi:.0f} score:{score} tier:{tier}")


# ── [UPGRADE P3] Trending Pre-Signal ─────────────────────────────────────────
def check_trending_presignal(client, pair, price, change_24h, trending,
                              regime, trending_count: list):
    """
    [UPGRADE P3] Deteksi early entry ketika pair baru masuk CoinGecko trending
    tapi harga belum bergerak signifikan.

    Logika:
    - CoinGecko trending = ada interest eksternal yang mulai masuk
    - Jika harga belum naik > TRENDING_MAX_CHANGE%, window entry masih terbuka
    - Diperkuat oleh: RSI sehat, ada sedikit kenaikan volume, tidak bearish struktur

    Sinyal type: TRENDING_EARLY — terpisah dari MOONSHOT dan MICROCAP
    TP/SL menggunakan parameter MOONSHOT (high risk, high reward)
    """
    if regime.get("regime") == "RISK_OFF": return
    if regime.get("block_buy"): return
    if trending_count[0] >= MAX_TRENDING_PER_CYCLE: return

    symbol = pair.replace("_USDT", "")
    if symbol not in trending: return

    # Harga belum naik terlalu jauh — masih early
    if change_24h > TRENDING_MAX_CHANGE: return
    if change_24h < -10.0: return  # filter crash besar

    closes, highs, lows, volumes = get_candles(client, pair, "1h", 48)
    if closes is None: return

    rsi = calc_rsi(closes)
    if rsi > TRENDING_RSI_MAX: return
    if rsi < TRENDING_RSI_MIN: return

    # Volume check — ada sedikit kenaikan interest
    vol_baseline = float(np.mean(volumes[:-6])) + 1e-9
    vol_recent   = float(np.mean(volumes[-6:]))
    vol_ratio    = vol_recent / vol_baseline
    if vol_ratio < TRENDING_VOL_RATIO_MIN: return

    # HTF filter 4h — skip jika full bearish
    try:
        c4h, _, _, _ = get_candles(client, pair, "4h", 55)
        if c4h is not None:
            e20_4h = calc_ema(c4h, 20)
            e50_4h = calc_ema(c4h, 50)
            if float(c4h[-1]) < e50_4h * 0.92 and e20_4h < e50_4h:
                return
    except Exception:
        pass

    # [P1] Volume dry-up bonus
    dryup = calc_volume_dryup(closes, volumes)

    structure = detect_structure(closes, highs, lows,
                                 lookback=MOONSHOT_BOS_LOOKBACK,
                                 strength=MOONSHOT_SWING_STRENGTH)
    liq_map   = detect_liquidity_map(closes, highs, lows, volumes, lookback=20)

    # Jangan masuk kalau struktur aktif bearish
    if structure.get("bos") == "BEARISH" or structure.get("choch") == "BEARISH":
        return

    atr  = calc_atr(closes, highs, lows)
    vwap = calc_vwap(closes, highs, lows, volumes)

    ob   = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=20)
    prec = check_entry_precision(closes, highs, lows, volumes, "BUY", structure, liq_map, ob)
    if prec["entry_quality"] == "SKIP": return

    # [FIX 1] Confirmation Layer — wajib lolos sebelum masuk queue
    conf = check_confirmation_layer(closes, highs, lows, volumes, structure, "BUY")
    if not conf["go"]:
        print(f"  🚫 TRENDING_EARLY {pair} conf FAIL "
              f"({conf['passed']}/{CONF_MIN_PASSED})")
        return

    conditions = [
        (True,                                         "trending"),
        (vol_ratio > 2.0,                              "vol_spike"),
        (dryup["vol_revival"],                         "vol_spike"),
        (dryup["is_dryup"] and dryup["price_stable"], "support_res"),
        (structure.get("bos") == "BULLISH",            "bos"),
        (structure.get("choch") == "BULLISH",          "choch"),
        (liq_map.get("sweep_bull"),                    "liq_sweep"),
        (ob.get("valid"),                              "order_block"),
        (prec.get("pullback"),                         "pullback"),
        (prec.get("rejection"),                        "rejection"),
        (prec.get("candle_conf"),                      "candle_conf"),
        (rsi < 50,                                     "rsi_extreme"),
        (price > vwap,                                 "vwap"),
        (liq_map.get("equal_lows") is not None,       "equal_hl"),
    ]
    score = wscore(conditions)
    if score < TRENDING_MIN_SCORE: return

    tier = assign_tier(score, structure, prec, liq_map)
    if tier == "SKIP":
        if score >= TRENDING_MIN_SCORE:
            tier = "B"
        else:
            return

    entry_info = resolve_entry(price, "BUY", atr, ob, liq_map, structure, prec, 0.04)
    if entry_info is None: return

    final_entry = entry_info["entry_price"]
    sl, tp1, tp2 = calc_sl_tp(final_entry, "BUY", atr, structure, liq_map, "MOONSHOT")
    stars_val    = min(5, max(1, score // 2))

    try:
        trending_rank = trending.index(symbol) + 1
    except ValueError:
        trending_rank = 0

    extra = (
        f"🔥 <b>CoinGecko Trending #{trending_rank}</b>\n"
        f"📊 RSI: <b>{rsi:.0f}</b> | Vol: <b>{vol_ratio:.1f}x</b>\n"
        f"24h: <b>{change_24h:+.1f}%</b> (belum pump — window masih terbuka)\n"
        f"🔒 Conf: <b>{conf['passed']}/3</b> ({' | '.join(conf['details'][:2])})\n"
    )
    if dryup["is_dryup"]:
        extra += (f"💤 <i>Keluar dari Vol Dry-Up "
                  f"({dryup['dryup_ratio']:.2f}x → revival {dryup['revival_ratio']:.1f}x)</i>\n")
    extra += f"📡 <i>Regime: {regime['regime']}</i>\n⚠️ <i>Trending signal — SL wajib ketat</i>"

    queue_signal(pair=pair, signal_type="TRENDING_EARLY", side="BUY",
                 entry=final_entry, tp1=tp1, tp2=tp2, sl=sl,
                 strength=stars_val, timeframe="1h", valid_minutes=360,
                 tier=tier, score=score,
                 sources="CoinGecko Trending · Gate.io · Structure · Vol",
                 extra=extra, structure=structure, liq_map=liq_map,
                 precision=prec, ob=ob, entry_info=entry_info)

    trending_count[0] += 1
    print(f"  🌟 TRENDING_EARLY {pair} #{trending_rank} "
          f"rsi:{rsi:.0f} vol:{vol_ratio:.1f}x change:{change_24h:+.1f}% "
          f"score:{score} tier:{tier}")


def detect_dump_warning(client, pair, price, change_24h, regime):
    """
    Deteksi tanda-tanda awal distribusi / dump yang akan datang.
    Digunakan sebagai WATCHING alert tipe SELL — bukan sinyal entry sell,
    tapi peringatan untuk yang sudah hold atau ingin SHORT.

    Kondisi yang dicari:
    - Volume meledak tapi harga tidak naik (distribusi)
    - RSI turun dari overbought setelah pump
    - CHoCH bearish terbentuk di timeframe 15m
    - Candle besar bearish setelah rally
    """
    # Hanya relevan di kondisi non-bear (kalau sudah bear bukan berita)
    if regime.get("regime") == "RISK_OFF": return
    if change_24h < 5.0: return  # hanya pantau yang sudah naik signifikan

    closes, highs, lows, volumes = get_candles(client, pair, "15m", 96)
    if closes is None: return

    rsi = calc_rsi(closes)

    # RSI harus turun dari zona overbought — tanda momentum melemah
    # Cek apakah RSI sebelumnya (5 candle lalu) lebih tinggi dari sekarang
    try:
        rsi_series = []
        for i in range(5):
            r = calc_rsi(closes[:-(i+1)] if i > 0 else closes)
            rsi_series.append(r)
        rsi_prev_peak = max(rsi_series[1:])  # peak RSI 5 candle terakhir
    except Exception:
        rsi_prev_peak = rsi

    rsi_dropping = (rsi_prev_peak > DUMP_RSI_DROP_THRESHOLD) and (rsi < rsi_prev_peak - 5)
    if not rsi_dropping: return

    # Volume spike — tapi bersamaan dengan harga turun (distribusi)
    vol_avg = float(np.mean(volumes[-20:-4]))
    vol_now = float(np.mean(volumes[-4:]))
    vol_ratio = vol_now / (vol_avg + 1e-9)

    if vol_ratio < DUMP_VOL_SPIKE_RATIO: return

    # Candle bearish engulfing atau close bearish besar
    price_now   = float(closes[-1])
    price_prev  = float(closes[-2])
    hi_now      = float(highs[-1])
    lo_now      = float(lows[-1])
    body        = abs(price_now - price_prev)
    full_range  = hi_now - lo_now + 1e-9
    is_bearish_candle = (price_now < price_prev) and (body / full_range > DUMP_BEARISH_CANDLE_PCT)

    if not is_bearish_candle: return

    # Structure — cek CHoCH bearish
    structure = detect_structure(closes, highs, lows,
                                 lookback=30, strength=3)
    has_bearish_choch = structure.get("choch") == "BEARISH"
    has_bearish_bos   = structure.get("bos") == "BEARISH"

    # Butuh minimal satu tanda struktur bearish
    if not has_bearish_choch and not has_bearish_bos: return

    # Cek puncak terakhir vs harga sekarang
    last_sh = structure.get("last_sh")
    drop_from_peak = ((last_sh - price_now) / last_sh) if last_sh and last_sh > 0 else 0

    # Kirim alert sebagai pesan Telegram khusus (bukan masuk queue sinyal)
    struct_label = "CHoCH Bearish" if has_bearish_choch else "BOS Bearish"
    drop_str = f"-{drop_from_peak*100:.1f}% dari puncak" if drop_from_peak > 0 else ""

    tg(
        f"⚠️🔴 <b>DUMP WARNING — {pair.replace('_USDT', '/USDT')}</b> [15m]\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Harga:    <b>${price:.6f}</b>\n"
        f"24h:      <b>+{change_24h:.1f}%</b> (sudah pump)\n"
        f"RSI:      <b>{rsi:.0f}</b> ↓ (dari {rsi_prev_peak:.0f})\n"
        f"Volume:   <b>{vol_ratio:.1f}x</b> average\n"
        f"Struct:   <b>{struct_label}</b> {drop_str}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🧲 <i>Volume spike + RSI drop + struktur bearish terbentuk</i>\n"
        f"⚡ <i>Jika hold: pertimbangkan ambil profit atau pasang trailing SL</i>"
    )
    print(f"  ⚠️ DUMP_WARNING {pair} rsi:{rsi:.0f} vol:{vol_ratio:.1f}x {struct_label}")


# ════════════════════════════════════════════════════════
#  L6. PORTFOLIO & RISK LAYER
# ════════════════════════════════════════════════════════

SECTOR_MAP_FULL = SECTOR_MAP

# ════════════════════════════════════════════════════════
#  [NEW 2] ADAPTIVE THRESHOLD ENGINE
# ════════════════════════════════════════════════════════

def compute_adaptive_thresholds(client, regime: dict) -> dict:
    """
    [NEW 2] Hitung threshold adaptif berdasarkan:
    - BTC ATR% (ukuran volatilitas pasar saat ini)
    - Regime (RISK_ON / NEUTRAL / RISK_OFF)

    Output:
    - prepump_rsi_exit : threshold RSI pre-pump yang disesuaikan
    - atr_min_pct      : ATR minimum per strategy (scaled)
    - atr_max_pct      : ATR maximum per strategy (scaled)
    - is_volatile      : apakah pasar sedang volatile
    - btc_atr_pct      : nilai ATR BTC dalam persen

    Logika:
    - Pasar volatile → RSI exit lebih tinggi (tunggu konfirmasi lebih kuat)
                     → ATR bands diperlebar (pasar bergerak lebih jauh)
    - Pasar tenang   → RSI exit lebih rendah (entry lebih awal)
                     → ATR bands dipersempit (entry hanya jika cukup bergerak)
    - RISK_OFF       → semua threshold diperketat (konservatif)
    """
    global _adaptive
    btc_atr_pct = 0.0
    is_volatile  = False

    try:
        c1h, h1h, l1h, _ = get_candles(client, "BTC_USDT", "1h", 24)
        if c1h is not None:
            atr_btc = calc_atr(c1h, h1h, l1h)
            btc_atr_pct = round((atr_btc / float(c1h[-1])) * 100, 2)
            is_volatile = btc_atr_pct > BTC_ATR_VOLATILE_THRESH
    except Exception as e:
        print(f"⚠️ adaptive_threshold BTC ATR: {e}")

    # Scale factor berdasarkan volatilitas
    scale = ATR_SCALE_VOLATILE if is_volatile else ATR_SCALE_CALM

    # ATR bands di-scale — kalau volatile, batas min naik, batas max naik
    new_atr_min = {k: round(v * scale, 2) for k, v in ATR_MIN_PCT.items()}
    new_atr_max = {k: round(v * scale, 2) for k, v in ATR_MAX_PCT.items()}

    # RSI exit adaptif untuk pre-pump
    if is_volatile:
        rsi_exit = min(PREPUMP_RSI_EXIT_MAX, PREPUMP_RSI_EXIT_BASE + 5)
    elif regime.get("regime") == "RISK_OFF":
        rsi_exit = max(PREPUMP_RSI_EXIT_MIN, PREPUMP_RSI_EXIT_BASE + 3)
    else:
        rsi_exit = PREPUMP_RSI_EXIT_BASE

    _adaptive.update({
        "prepump_rsi_exit": rsi_exit,
        "atr_min_pct":      new_atr_min,
        "atr_max_pct":      new_atr_max,
        "btc_atr_pct":      btc_atr_pct,
        "is_volatile":      is_volatile,
    })

    print(f"🎚️ Adaptive | BTC ATR:{btc_atr_pct:.2f}% {'[VOLATILE]' if is_volatile else '[CALM]'} | "
          f"RSI_exit:{rsi_exit} | ATR scale:{scale:.2f}x")
    return _adaptive


# ════════════════════════════════════════════════════════
#  [NEW 4] EQUITY PROTECTION SYSTEM
# ════════════════════════════════════════════════════════

def check_equity_protection(client=None) -> tuple:
    """
    [NEW 4] Equity Protection System — cek daily drawdown.

    Cara kerja:
    1. Ambil semua sinyal yang result-nya TP atau SL HARI INI dari Supabase
    2. Hitung total P&L hari ini dalam % akun
    3. Jika total loss > DAILY_MAX_DRAWDOWN_PCT → halt

    Adaptive threshold:
    - Jika pasar volatile (BTC ATR > trigger) → gunakan EQUITY_DRAWDOWN_STRICT_PCT
    - Jika normal → gunakan DAILY_MAX_DRAWDOWN_PCT

    Return: (halted: bool, reason: str, daily_pnl_pct: float)
    """
    if not EQUITY_PROTECTION_ENABLED:
        return False, "", 0.0

    try:
        now    = datetime.now(WIB)
        today  = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

        res = supabase.table("signals").select(
            "result,risk_pct,tp1,sl,entry_price,side"
        ).not_.is_("result", "null").gt("valid_from", today).execute()

        if not res.data:
            return False, "", 0.0

        daily_pnl_pct = 0.0
        for sig in res.data:
            result   = sig.get("result", "EXPIRED")
            risk_pct = float(sig.get("risk_pct") or 0)
            entry    = float(sig.get("entry_price") or 0)
            tp1      = float(sig.get("tp1") or 0)
            sl       = float(sig.get("sl") or 0)
            side     = sig.get("side", "BUY")

            if result == "EXPIRED" or risk_pct == 0:
                continue

            if entry > 0 and tp1 > 0 and sl > 0:
                rr = abs(tp1 - entry) / (abs(sl - entry) + 1e-9)
            else:
                rr = 1.0

            if result == "TP":
                daily_pnl_pct += risk_pct * rr
            elif result == "SL":
                daily_pnl_pct -= risk_pct

        # Adaptive threshold — pasar volatile → lebih ketat
        is_volatile = _adaptive.get("is_volatile", False)
        threshold   = EQUITY_DRAWDOWN_STRICT_PCT if is_volatile else DAILY_MAX_DRAWDOWN_PCT

        _equity_state["daily_realized_pct"] = round(daily_pnl_pct, 2)

        if daily_pnl_pct <= -threshold:
            reason = (
                f"Daily drawdown {daily_pnl_pct:.2f}% "
                f"melewati batas -{threshold:.1f}%"
                f"{' [VOLATILE MARKET — strict mode]' if is_volatile else ''}. "
                f"Bot halt — evaluasi manual."
            )
            _equity_state["halted"]      = True
            _equity_state["halt_reason"] = reason
            return True, reason, daily_pnl_pct

        return False, "", daily_pnl_pct

    except Exception as e:
        print(f"⚠️ check_equity_protection: {e}")
        return False, "", 0.0


# ════════════════════════════════════════════════════════
#  [NEW 3] STRATEGY CAPITAL ALLOCATION TRACKER
# ════════════════════════════════════════════════════════

def strategy_budget_allows(signal_type: str, risk_pct: float) -> tuple:
    """
    [NEW 3] Cek apakah strategy masih punya budget capital yang tersisa.

    Setiap strategy punya batas risk independen dari total portfolio risk.
    Contoh: SCALPING hanya 10% dari MAX_PORTFOLIO_RISK_PCT = 1.0%
    Jika sudah ada 2x SCALPING signal @ 0.5% → budget habis.

    Return: (allowed: bool, reason: str)
    """
    max_for_strategy = STRATEGY_MAX_RISK_PCT.get(signal_type, MAX_PORTFOLIO_RISK_PCT)
    current_used     = _portfolio_state.get("strategy_risk_pct", {}).get(signal_type, 0.0)

    if current_used + risk_pct > max_for_strategy:
        return False, (
            f"Capital limit [{signal_type}]: "
            f"{current_used:.2f}%+{risk_pct:.2f}% "
            f"> {max_for_strategy:.2f}% budget"
        )
    return True, ""


def register_strategy_capital(signal_type: str, risk_pct: float):
    """[NEW 3] Catat penggunaan capital per strategy."""
    if "strategy_risk_pct" not in _portfolio_state:
        _portfolio_state["strategy_risk_pct"] = {}
    prev = _portfolio_state["strategy_risk_pct"].get(signal_type, 0.0)
    _portfolio_state["strategy_risk_pct"][signal_type] = round(prev + risk_pct, 3)


def get_pair_sector(pair: str) -> str:
    return SECTOR_MAP_FULL.get(pair.replace("_USDT",""), "altcoin")


def load_portfolio_state() -> dict:
    state = {
        "total_risk_pct": 0.0,
        "speculative_risk_pct": 0.0,
        "strategy_risk_pct": {},        # [NEW 3] risk per strategy
        "open_positions": {},
        "sector_exposure": defaultdict(float),
    }
    try:
        now = datetime.now(WIB)
        res = supabase.table("signals").select("pair,side,risk_pct,result,type") \
            .gt("valid_until",now.isoformat()).is_("result","null").execute()
        for sig in (res.data or []):
            pair=sig.get("pair",""); side=sig.get("side","")
            risk=float(sig.get("risk_pct") or 0)
            sig_type=sig.get("type","")
            if not pair or not side: continue
            state["total_risk_pct"] += risk
            if sig_type in SPECULATIVE_TYPES:
                state["speculative_risk_pct"] += risk
            # [NEW 3] Akumulasi per strategy
            if sig_type not in state["strategy_risk_pct"]:
                state["strategy_risk_pct"][sig_type] = 0.0
            state["strategy_risk_pct"][sig_type] += risk
            state["open_positions"][pair] = side
            state["sector_exposure"][get_pair_sector(pair)] += 1
        print(f"📂 Portfolio: {len(state['open_positions'])} posisi | "
              f"risk {state['total_risk_pct']:.1f}% | "
              f"speculative {state['speculative_risk_pct']:.1f}%/{MAX_SPECULATIVE_RISK_PCT}%")
        # [NEW 3] Print strategy breakdown
        for k, v in state["strategy_risk_pct"].items():
            budget = STRATEGY_MAX_RISK_PCT.get(k, 0)
            print(f"   [{k}] {v:.2f}% / {budget:.2f}% budget")
    except Exception as e:
        print(f"⚠️ load_portfolio_state: {e}")
    return state


def calc_position_size(entry: float, sl: float, tier: str,
                       signal_type: str = "") -> dict:
    """
    [FIX 4] Jika signal_type masuk SPECULATIVE_TYPES, gunakan
    SPECULATIVE_TIER_RISK yang lebih kecil daripada TIER_RISK_PCT utama.
    """
    if entry<=0 or sl<=0 or abs(entry-sl)<1e-9:
        return {"size_usdt":0,"risk_usdt":0,"risk_pct":0,"units":0}
    # Pilih risk table berdasarkan jenis sinyal
    if signal_type in SPECULATIVE_TYPES:
        risk_pct = SPECULATIVE_TIER_RISK.get(tier, 0.25)
    else:
        risk_pct = TIER_RISK_PCT.get(tier, 0.5)
    risk_usdt = ACCOUNT_BALANCE*(risk_pct/100)
    sl_dist   = abs(entry-sl)
    units     = risk_usdt/sl_dist
    return {"size_usdt":round(units*entry,2),"risk_usdt":round(risk_usdt,2),
            "risk_pct":round(risk_pct,2),"units":round(units,6)}


def portfolio_allows(pair: str, side: str, tier: str, risk_pct: float,
                     regime: dict, signal_type: str = "") -> tuple:
    global _portfolio_state
    if regime.get("regime")=="RISK_OFF" and pair not in SAFE_PAIRS_RISK_OFF:
        return False,"RISK_OFF: hanya BTC/ETH"
    if side=="BUY"  and regime.get("block_buy"):
        return False,f"BUY diblokir BTC {regime['btc_1h_chg']:+.1f}%"
    if side=="SELL" and regime.get("block_sell"):
        return False,f"SELL diblokir BTC {regime['btc_1h_chg']:+.1f}%"
    if _portfolio_state["total_risk_pct"]+risk_pct>MAX_PORTFOLIO_RISK_PCT:
        return False,f"Risk limit {_portfolio_state['total_risk_pct']:.1f}%+{risk_pct}%>{MAX_PORTFOLIO_RISK_PCT}%"
    # [FIX 4] Speculative exposure cap
    if signal_type in SPECULATIVE_TYPES:
        spec_risk = _portfolio_state.get("speculative_risk_pct", 0.0)
        if spec_risk + risk_pct > MAX_SPECULATIVE_RISK_PCT:
            return False,(f"Speculative cap: {spec_risk:.1f}%+{risk_pct}%"
                          f">{MAX_SPECULATIVE_RISK_PCT}% (PREPUMP/TRENDING/MOONSHOT)")
    # [NEW 3] Per-strategy capital allocation check
    budget_ok, budget_reason = strategy_budget_allows(signal_type, risk_pct)
    if not budget_ok:
        return False, budget_reason
    if _portfolio_state["open_positions"].get(pair)==side:
        return False,f"Sudah ada posisi {side} di {pair}"
    sector = get_pair_sector(pair)
    if _portfolio_state["sector_exposure"][sector]>=MAX_SECTOR_EXPOSURE:
        return False,f"Sector {sector} limit"
    return True,""


def register_to_portfolio(pair: str, side: str, risk_pct: float,
                          signal_type: str = ""):
    global _portfolio_state
    _portfolio_state["total_risk_pct"] += risk_pct
    if signal_type in SPECULATIVE_TYPES:
        _portfolio_state["speculative_risk_pct"] = \
            _portfolio_state.get("speculative_risk_pct", 0.0) + risk_pct
    _portfolio_state["open_positions"][pair] = side
    _portfolio_state["sector_exposure"][get_pair_sector(pair)] += 1
    register_strategy_capital(signal_type, risk_pct)  # [NEW 3]


def load_strategy_performance() -> dict:
    perf = {}
    try:
        res = supabase.table("signals").select("*").not_.is_("result","null").execute()
        if not res.data or len(res.data)<15: return {}
        grouped = defaultdict(list)
        for sig in res.data:
            key    = f"{sig.get('type','?')}_{sig.get('side','?')}"
            result = sig.get("result","EXPIRED")
            entry  = float(sig.get("entry_price") or 0)
            tp1    = float(sig.get("tp1") or 0)
            sl     = float(sig.get("sl") or 0)
            if entry>0 and tp1>0 and sl>0:
                rr  = abs(tp1-entry)/abs(sl-entry) if abs(sl-entry)>0 else 1.0
                pnl = rr if result=="TP" else (-1.0 if result=="SL" else 0.0)
                grouped[key].append(pnl)
        for key,pnl_list in grouped.items():
            if len(pnl_list)<5: continue
            wins  = sum(1 for p in pnl_list if p>0)
            total = len(pnl_list)
            wr    = wins/total
            gw    = sum(p for p in pnl_list if p>0)
            gl    = abs(sum(p for p in pnl_list if p<0))+1e-9
            pf    = gw/gl
            eq    = np.cumsum(pnl_list); pk = np.maximum.accumulate(eq)
            dd    = float(np.min(eq-pk))
            avg_w = float(np.mean([p for p in pnl_list if p>0])) if wins>0 else 0.0
            avg_l = float(np.mean([p for p in pnl_list if p<0])) if total-wins>0 else 0.0
            exp   = round((wr*avg_w)+((1-wr)*avg_l),3)
            status = ("SCALE" if wr>=0.55 and pf>=1.5 and dd>-3 and exp>0.3
                      else "NORMAL" if wr>=0.45 and pf>=1.0 and exp>0
                      else "CAUTION" if wr>=0.35 or pf>=0.8
                      else "KILL")
            perf[key] = {"win_rate":round(wr*100,1),"profit_factor":round(pf,2),
                         "max_drawdown":round(dd,2),"expectancy":exp,
                         "total":total,"status":status}
    except Exception as e:
        print(f"⚠️ load_strategy_performance: {e}")
    return perf


def get_risk_multiplier(strategy_key: str, perf: dict) -> float:
    if not perf or strategy_key not in perf: return 1.0
    return {"SCALE":1.5,"NORMAL":1.0,"CAUTION":0.5,"KILL":0.0}.get(
        perf[strategy_key]["status"],1.0)


def check_kill_switch() -> tuple:
    if not KILL_SWITCH_ENABLED: return False,""
    try:
        now   = datetime.now(WIB)
        since = (now-timedelta(hours=KILL_SWITCH_WINDOW_HRS)).isoformat()
        res   = supabase.table("signals").select("result") \
                    .not_.is_("result","null").gt("valid_until",since) \
                    .order("valid_until",desc=True).limit(20).execute()
        if not res.data: return False,""
        recent = [r.get("result") for r in res.data if r.get("result") in ("TP","SL")]
        streak = 0
        for r in recent:
            if r=="SL":
                streak+=1
                if streak>=KILL_SWITCH_LOSS_STREAK:
                    return True,(f"Loss streak {streak}x dalam {KILL_SWITCH_WINDOW_HRS}h. "
                                 f"Bot halt — evaluasi manual.")
            else: break
    except Exception as e:
        print(f"⚠️ check_kill_switch: {e}")
    return False,""


def get_market_regime(client) -> dict:
    default = {"regime":"NEUTRAL","btc_trend":"SIDEWAYS","btc_1h_chg":0.0,
               "btc_4h_chg":0.0,"block_buy":False,"block_sell":False,
               "reason":"Default","aggressiveness":"NORMAL"}
    try:
        c4h,_,_,_ = get_candles(client,"BTC_USDT","4h",210)
        c1h,_,_,_ = get_candles(client,"BTC_USDT","1h",10)
        if c4h is None or c1h is None: return default

        ema200  = calc_ema(c4h,200); ema50 = calc_ema(c4h,50)
        price   = float(c4h[-1])
        chg_1h  = (c1h[-1]-c1h[-2])/c1h[-2]*100
        chg_4h  = (c4h[-1]-c4h[-5])/c4h[-5]*100
        trend   = ("BULL" if price>ema200 and ema50>ema200
                   else "BEAR" if price<ema200 and ema50<ema200 else "SIDEWAYS")

        block_buy=False; block_sell=False; reason=""
        if chg_1h<BTC_1H_DROP_BLOCK: block_buy=True;  reason=f"BTC drop {chg_1h:.1f}%"
        elif chg_1h>BTC_1H_PUMP_BLOCK: block_sell=True; reason=f"BTC pump {chg_1h:.1f}%"

        if trend=="BULL" and chg_4h>1.0 and not block_buy:
            regime="RISK_ON"; aggressiveness="HIGH"; reason=reason or "BTC bull"
        elif trend=="BEAR" or chg_4h<-4.0:
            regime="RISK_OFF"; aggressiveness="LOW"; block_buy=True; reason=reason or "BTC bear"
        else:
            regime="NEUTRAL"; aggressiveness="NORMAL"; reason=reason or "BTC sideways"

        print(f"📡 Regime:{regime} BTC:{trend} 1h:{chg_1h:+.1f}% 4h:{chg_4h:+.1f}%")
        return {"regime":regime,"btc_trend":trend,"btc_1h_chg":round(chg_1h,2),
                "btc_4h_chg":round(chg_4h,2),"block_buy":block_buy,"block_sell":block_sell,
                "reason":reason,"aggressiveness":aggressiveness}
    except Exception as e:
        print(f"⚠️ Regime: {e}"); return default


# ════════════════════════════════════════════════════════
#  L7. OUTPUT LAYER
# ════════════════════════════════════════════════════════

def _emit_watching(pair: str, structure: dict, liq_map: dict,
                   price: float, timeframe: str):
    """Kirim WATCHING alert jika harga dalam radius dari level kunci."""
    global _watching_count
    if _watching_count >= MAX_WATCHING_PER_CYCLE: return

    last_sh = structure.get("last_sh")
    last_sl = structure.get("last_sl")

    checks = []
    if last_sh and price < last_sh:
        checks.append(("SELL", "Last SH", last_sh))
    if last_sl and price > last_sl:
        checks.append(("BUY", "Last SL", last_sl))

    for side, level_name, level_price in checks:
        if _watching_count >= MAX_WATCHING_PER_CYCLE: break
        key = (pair, side, timeframe)
        if key in _watching_sent: continue
        prox = abs(price-level_price)/level_price*100
        if prox > WATCHING_RADIUS_PCT: continue

        _watching_sent.add(key)
        _watching_count += 1
        phase    = structure.get("trend_phase","?")
        bos_str  = structure.get("bos") or structure.get("choch") or "—"
        emoji    = "👀🟢" if side=="BUY" else "👀🔴"
        sweep_note = ""
        if liq_map.get("sweep_bull") and side=="BUY":
            sweep_note = "\n🧲 <i>Liq sweep bawah sudah terjadi</i>"
        elif liq_map.get("sweep_bear") and side=="SELL":
            sweep_note = "\n🧲 <i>Liq sweep atas sudah terjadi</i>"

        tg(f"{emoji} <b>WATCHING — {pair.replace('_USDT','/USDT')}</b> [{timeframe}]\n"
           f"━━━━━━━━━━━━━━━━━━\n"
           f"Harga:  <b>${price:.6f}</b>\n"
           f"Level:  <b>{level_name} @ ${level_price:.6f}</b>\n"
           f"Jarak:  <b>{prox:.2f}%</b> | Phase: {phase} | Struct: {bos_str}\n"
           f"━━━━━━━━━━━━━━━━━━\n"
           f"⏳ <i>Pasang limit order di zona — tunggu candle close konfirmasi</i>"
           f"{sweep_note}")
        print(f"  👀 WATCHING {pair} {side} [{timeframe}] {level_name} ${level_price:.6f}")


def is_valid(pair: str) -> bool:
    if not pair.endswith("_USDT"): return False
    base = pair.replace("_USDT", "")
    # Blacklist stablecoin & junk
    if any(b in base for b in BLACKLIST): return False
    # Exact match atau suffix ETF token
    for sfx in ETF_STOCK_SUFFIXES:
        if base == sfx or (len(base) > len(sfx) and base.endswith(sfx)):
            return False
    # Keyword prefix: nama saham terkenal → pasti ETF token Gate.io
    for kw in ETF_KEYWORDS:
        if base.startswith(kw):
            return False
    return True


def already_sent(pair: str, signal_type: str, timeframe: str) -> bool:
    try:
        now = datetime.now(WIB)
        res = supabase.table("signals").select("id") \
            .eq("pair",pair).eq("type",signal_type).eq("timeframe",timeframe) \
            .gt("valid_until",now.isoformat()).execute()
        return len(res.data)>0
    except Exception as e:
        print(f"⚠️ already_sent: {e}"); return False


def save_signal(pair, signal_type, side, entry, tp1, tp2, sl,
                strength, timeframe, valid_until, tier, size_usdt, risk_pct,
                structure, liq_map, precision):
    """
    Save signal ke Supabase.
    Kolom JSONB: bos (TEXT), structure_json, liq_json, precision_json.
    Data analysis disimpan compact dalam 3 kolom JSONB.
    """
    try:
        now = datetime.now(WIB)

        # ── Ringkasan structure untuk kolom bos (TEXT) ──────────────
        bos_value = structure.get("bos") or structure.get("choch") or None

        # ── structure_json — semua data deteksi struktur ────────────
        structure_json = {
            "bos":         structure.get("bos"),
            "choch":       structure.get("choch"),
            "trend_phase": structure.get("trend_phase"),
            "bias":        structure.get("structure_bias"),
            "last_sh":     structure.get("last_sh"),
            "last_sl":     structure.get("last_sl"),
        }

        # ── liq_json — semua data likuidasi / liquidity map ─────────
        liq_json = {
            "sweep_bull":      liq_map.get("sweep_bull", False),
            "sweep_bear":      liq_map.get("sweep_bear", False),
            "sweep_level":     liq_map.get("sweep_level"),
            "equal_highs":     liq_map.get("equal_highs"),
            "equal_lows":      liq_map.get("equal_lows"),
            "stop_cluster_above": liq_map.get("stop_cluster_above"),
            "stop_cluster_below": liq_map.get("stop_cluster_below"),
            "fake_bo_bull":    liq_map.get("fake_bo_bull", False),
            "fake_bo_bear":    liq_map.get("fake_bo_bear", False),
        }

        # ── precision_json — entry quality detail ───────────────────
        precision_json = {
            "entry_quality":   precision.get("entry_quality", "WAIT"),
            "precision_score": precision.get("precision_score", 0),
            "pullback":        precision.get("pullback", False),
            "rejection":       precision.get("rejection", False),
            "candle_conf":     precision.get("candle_conf", False),
            "detail":          precision.get("detail", [])[:3],
        }

        supabase.table("signals").insert({
            "pair":           pair,
            "type":           signal_type,
            "side":           side,
            "entry_price":    entry,
            "tp1":            tp1,
            "tp2":            tp2,
            "sl":             sl,
            "strength":       strength,
            "timeframe":      timeframe,
            "valid_from":     now.isoformat(),
            "valid_until":    valid_until.isoformat(),
            "tier":           tier,
            "size_usdt":      size_usdt,
            "risk_pct":       risk_pct,
            "bos":            bos_value,
            "structure_json": json.dumps(structure_json),
            "liq_json":       json.dumps(liq_json),
            "precision_json": json.dumps(precision_json),
        }).execute()

    except Exception as e:
        print(f"⚠️ save_signal [{pair}]: {e}")


def get_win_rate(signal_type: str):
    try:
        res = supabase.table("signals").select("result") \
            .eq("type",signal_type).not_.is_("result","null").execute()
        if not res.data or len(res.data)<5: return None
        return round(sum(1 for r in res.data if r.get("result")=="TP")/len(res.data)*100,1)
    except Exception:
        return None


def update_expired_signals(client):
    try:
        now = datetime.now(WIB)
        res = supabase.table("signals").select("*") \
            .lt("valid_until",now.isoformat()).is_("result","null").limit(20).execute()
        if not res.data: return
        tickers = {t.currency_pair:t for t in client.list_tickers()}
        updated = 0
        for sig in res.data:
            pair  = sig.get("pair"); side = sig.get("side")
            entry = float(sig.get("entry_price") or 0)
            tp1   = float(sig.get("tp1") or 0)
            sl    = float(sig.get("sl")  or 0)
            if not pair or entry<=0: continue
            ticker  = tickers.get(pair)
            current = float(ticker.last or 0) if ticker else 0
            if current<=0: continue

            # Cek candle 1m untuk SL yang sudah recover
            result = "EXPIRED"
            try:
                _,h1m,l1m,_ = get_candles(client,pair,"1m",60)
                if h1m is not None:
                    ph = float(np.max(h1m)); pl = float(np.min(l1m))
                    if side=="BUY":
                        result = "TP" if ph>=tp1 else "SL" if pl<=sl else "EXPIRED"
                    else:
                        result = "TP" if pl<=tp1 else "SL" if ph>=sl else "EXPIRED"
            except Exception:
                result = ("TP" if (side=="BUY" and current>=tp1) or (side=="SELL" and current<=tp1)
                          else "SL" if (side=="BUY" and current<=sl) or (side=="SELL" and current>=sl)
                          else "EXPIRED")

            supabase.table("signals").update({"result":result}).eq("id",sig["id"]).execute()
            print(f"📝 [{pair}] {side} → {result}")
            updated += 1
        if updated: print(f"📝 Updated {updated} expired signals")
    except Exception as e:
        print(f"⚠️ update_expired_signals: {e}")


def queue_signal(**kwargs):
    """
    Tambah sinyal ke queue dengan deduplication per pair+side.
    Jika sudah ada sinyal dari pair+side yang sama, ambil tier terbaik.
    MOONSHOT dikecualikan dari dedup.
    """
    new_pair = kwargs.get("pair"); new_side = kwargs.get("side")
    new_type = kwargs.get("signal_type")
    tier_ord = {"S":0,"A+":1,"A":2,"B":3}

    if new_type!="MOONSHOT":
        for ex in _signal_queue:
            if ex.get("pair")==new_pair and ex.get("side")==new_side:
                if tier_ord.get(kwargs.get("tier","B"),3) < tier_ord.get(ex.get("tier","B"),3):
                    _signal_queue.remove(ex)
                    print(f"  🔄 Queue upgrade: {new_pair} {new_side} [{ex.get('signal_type')}→{new_type}]")
                    break
                else:
                    print(f"  ⏭️ Queue dedup: {new_pair} {new_side} [{new_type}] skip")
                    return
    _signal_queue.append(kwargs)


def flush_signal_queue(perf: dict, regime: dict):
    global _flush_slots
    if not _signal_queue:
        print("  📭 Queue kosong"); return

    tier_ord = {"S":0,"A+":1,"A":2,"B":3}

    sorted_q = sorted(
        _signal_queue,
        key=lambda x: (
            SIGNAL_PRIORITY.get(x.get("signal_type",""), 9),
            tier_ord.get(x.get("tier","B"), 9),
            -x.get("score", 0),
        )
    )

    print(f"\n📨 Queue: {len(sorted_q)} candidates → top {MAX_SIGNALS_PER_CYCLE} "
          f"({'SIGNAL_ONLY' if SIGNAL_ONLY_MODE else f'confirmed≤{MAX_SLOTS_CONFIRMED} | speculative≤{MAX_SLOTS_SPECULATIVE}'})")

    sent              = 0
    slots_confirmed   = 0
    slots_speculative = 0
    CONFIRMED_TYPES   = {"SWING","INTRADAY","SCALPING"}

    for sig in sorted_q:
        if sent >= MAX_SIGNALS_PER_CYCLE: break

        signal_type = sig["signal_type"]
        is_spec     = signal_type in SPECULATIVE_TYPES
        is_conf     = signal_type in CONFIRMED_TYPES

        # Slot allocation — tetap berlaku di signal only mode
        if is_conf and slots_confirmed >= MAX_SLOTS_CONFIRMED:
            print(f"  ⏭️ {sig['pair']} [{signal_type}] confirmed slot full"); continue
        if is_spec and slots_speculative >= MAX_SLOTS_SPECULATIVE:
            print(f"  ⏭️ {sig['pair']} [{signal_type}] speculative slot full"); continue

        pair       = sig["pair"];    side       = sig["side"]
        entry      = sig["entry"];   tp1        = sig["tp1"]
        tp2        = sig["tp2"];     sl         = sig["sl"]
        strength   = sig["strength"]; tier      = sig["tier"]
        score      = sig["score"];   sources    = sig.get("sources","")
        extra      = sig.get("extra","")
        timeframe  = sig["timeframe"]
        structure  = sig.get("structure",{}); liq_map = sig.get("liq_map",{})
        precision  = sig.get("precision",{}); ob      = sig.get("ob",{})
        entry_info = sig.get("entry_info",{})
        valid_mins = sig["valid_minutes"]

        # [NEW 5] Tier minimum filter — buang tier B di signal only mode
        if SIGNAL_ONLY_MODE:
            if tier_ord.get(tier, 9) > tier_ord.get(SIGNAL_MIN_TIER, 2):
                print(f"  ⏭️ {pair} [{signal_type}] tier {tier} < minimum {SIGNAL_MIN_TIER}"); continue

        if already_sent(pair, signal_type, timeframe):
            print(f"  ⏭️ already_sent: {pair} [{signal_type}]"); continue

        # Performance gate
        strat_key = f"{signal_type}_{side}"
        if perf and strat_key in perf:
            status = perf[strat_key]["status"]
            if SIGNAL_ONLY_MODE:
                # Signal bot: CAUTION dan KILL tidak boleh kirim signal
                if status not in SIGNAL_PERF_ALLOWED:
                    print(f"  🚫 {pair} [{signal_type}] perf {status} — tidak kirim signal"); continue
            else:
                risk_mult = get_risk_multiplier(strat_key, perf)
                if risk_mult == 0.0:
                    print(f"  🚫 {pair} [{signal_type}] KILL strategy"); continue

        risk_mult = get_risk_multiplier(strat_key, perf) if perf else 1.0

        # Sizing — untuk signal bot ini hanya informatif, bukan eksekusi
        sizing   = calc_position_size(entry, sl, tier, signal_type)
        adj_risk = round(sizing["risk_pct"] * risk_mult, 2)
        adj_size = round(sizing["size_usdt"] * risk_mult, 2)

        # [NEW 5] Portfolio check — bypass di signal only mode
        if not SIGNAL_ONLY_MODE:
            ok, reason = portfolio_allows(pair, side, tier, adj_risk, regime, signal_type)
            if not ok:
                print(f"  ⏭️ {pair} portfolio: {reason}"); continue

        # Sanity check arah TP/SL
        if side=="BUY"  and (tp1<=entry or sl>=entry): continue
        if side=="SELL" and (tp1>=entry or sl<=entry): continue

        # R/R check — gunakan MIN_RR_SIGNAL di signal only mode
        pct_tp1 = abs((tp1-entry)/entry*100)
        pct_tp2 = abs((tp2-entry)/entry*100)
        pct_sl  = abs((sl-entry)/entry*100)
        rr      = round(pct_tp1/pct_sl, 1) if pct_sl>0 else 0

        rr_table = MIN_RR_SIGNAL if SIGNAL_ONLY_MODE else MIN_RR
        if rr < rr_table.get(signal_type, {}).get(tier, 1.5):
            print(f"  ⏭️ R/R {pair} [{signal_type}|{tier}] rr={rr}"); continue

        now         = datetime.now(WIB)
        e_method    = entry_info.get("method","MARKET")
        eff_minutes = valid_mins*2 if e_method in ("PULLBACK","BREAKOUT") else valid_mins
        valid_until = now+timedelta(minutes=eff_minutes)
        win_rate    = get_win_rate(signal_type)
        wr_str      = f"{win_rate}%" if win_rate else "Akumulasi..."
        tier_emoji  = {"S":"💎","A+":"🏆","A":"🥇","B":"🥈"}.get(tier,"🎯")
        emoji_side  = "🟢 BUY" if side=="BUY" else "🔴 SELL"
        emoji_type  = {
            "SCALPING":"⚡","INTRADAY":"📈","SWING":"🌊","MOONSHOT":"🚀",
            "MOONSHOT_PREPUMP":"🔮","TRENDING_EARLY":"🌟",
        }.get(signal_type,"🎯")

        method_label = {"MARKET":"✅ MARKET — Entry Sekarang",
                        "PULLBACK":"⏬ LIMIT PULLBACK — Tunggu Retrace",
                        "BREAKOUT":"🚀 LIMIT BREAKOUT — Tunggu Konfirmasi"}.get(e_method,e_method)
        zone_str = ""
        ez = entry_info.get("entry_zone")
        if ez: zone_str = f"\nZona:   <b>${ez[0]:.6f} – ${ez[1]:.6f}</b>"

        size_note = ""
        if perf and strat_key in perf:
            st = perf[strat_key].get("status","")
            if risk_mult != 1.0:
                size_note = f" [x{risk_mult} {st}]"

        sl_lines = []
        if structure.get("bos"):   sl_lines.append(f"🏗️ BOS {structure['bos']} | {structure.get('trend_phase')}")
        if structure.get("choch"): sl_lines.append(f"🔄 CHoCH {structure['choch']} — Early Reversal")
        if liq_map.get("sweep_bull") or liq_map.get("sweep_bear"):
            sl_lines.append(f"🧲 Liq Sweep @ ${liq_map.get('sweep_level',0):.6f}")
        if liq_map.get("equal_highs"): sl_lines.append(f"📌 Equal Highs: ${liq_map['equal_highs']:.6f}")
        if liq_map.get("equal_lows"):  sl_lines.append(f"📌 Equal Lows: ${liq_map['equal_lows']:.6f}")
        if ob.get("valid"): sl_lines.append(f"📦 OB: ${ob['ob_low']:.6f}–${ob['ob_high']:.6f}")

        msg = (
            f"{emoji_type} <b>{tier_emoji} [{tier}] SIGNAL {emoji_side} — {signal_type}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Pair:   <b>{pair.replace('_USDT','/USDT')}</b>\n"
            f"⏰ Valid: {now.strftime('%H:%M')} → {valid_until.strftime('%H:%M')} WIB\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📌 <b>CARA ENTRY</b>\n"
            f"Metode: <b>{method_label}</b>\n"
            f"Entry:  <b>${entry:.6f}</b>{zone_str}\n"
            f"Trigger: <i>{entry_info.get('trigger','')}</i>\n"
            f"Batal:  <i>{entry_info.get('invalidate','')}</i>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"TP1:    ${tp1:.6f} <i>(+{pct_tp1:.1f}%)</i>\n"
            f"TP2:    ${tp2:.6f} <i>(+{pct_tp2:.1f}%)</i>\n"
            f"SL:     ${sl:.6f} <i>(-{pct_sl:.1f}%)</i>\n"
            f"R/R:    <b>1:{rr}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Tier:   <b>{tier}</b> | Score: {score} | WR: {wr_str}\n"
            f"TF:     {timeframe} | {'⭐'*strength}{'☆'*(5-strength)}\n"
            f"💰 Suggested Risk: {adj_risk}% | Size ref: ${adj_size}{size_note}\n"
            f"Data:   {sources}"
        )
        if sl_lines: msg += "\n━━━━━━━━━━━━━━━━━━\n🏗️ <b>Structure</b>\n" + "\n".join(sl_lines)
        if precision.get("detail"): msg += "\n🎯 <b>Entry Precision</b>\n" + "\n".join(precision["detail"][:3])
        if extra: msg += f"\n{extra}"

        tg(msg)
        save_signal(pair,signal_type,side,entry,tp1,tp2,sl,
                    strength,timeframe,valid_until,tier,adj_size,adj_risk,
                    structure,liq_map,precision)

        # Portfolio tracking — tetap jalan tapi hanya untuk data, tidak memblokir
        if not SIGNAL_ONLY_MODE:
            register_to_portfolio(pair, side, adj_risk, signal_type)

        print(f"  ✅ [{sent+1}] [{tier}] {signal_type} {side} {pair} R/R:1:{rr} Score:{score}")
        sent += 1
        if signal_type in CONFIRMED_TYPES:
            slots_confirmed += 1
        elif signal_type in SPECULATIVE_TYPES:
            slots_speculative += 1

    _flush_slots["confirmed"]   = slots_confirmed
    _flush_slots["speculative"] = slots_speculative
    _signal_queue.clear()


def should_send_summary() -> bool:
    now = datetime.now(WIB)
    return now.hour==SUMMARY_HOUR_WIB and now.minute<35


def send_market_summary(fg, fg_label, market_data, trending, regime, perf):
    if not market_data: return
    now      = datetime.now(WIB)
    btc_dom  = market_data.get("btc_dominance",0)
    mcap_chg = market_data.get("market_cap_change_24h",0)
    trends   = ", ".join(trending[:5]) if trending else "N/A"
    saran_map= [(20,"😱 Extreme Fear — Akumulasi selektif"),(40,"😨 Fear — Peluang beli"),
                (60,"😐 Neutral — Tunggu konfirmasi"),(80,"😊 Greed — Kurangi posisi"),
                (101,"🤑 Extreme Greed — Potensi koreksi")]
    saran    = next(s for t,s in saran_map if fg<t)
    ks_halt,_ = check_kill_switch()

    tg(f"📊 <b>MORNING BRIEFING — {now.strftime('%d %b %Y %H:%M WIB')}</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Fear & Greed:  <b>{fg} — {fg_label}</b>\n"
       f"BTC Dom:       <b>{btc_dom:.1f}%</b>\n"
       f"Market Cap:    <b>{'📈' if mcap_chg>0 else '📉'} {mcap_chg:+.2f}%</b>\n"
       f"Regime:        <b>{regime['regime']}</b> [{regime.get('aggressiveness')}]\n"
       f"Kill Switch:   {'🛑 HALT' if ks_halt else '✅ Aktif'}\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"🔥 Trending: <b>{trends}</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"💡 {saran}")

    # Performance report hanya jika data cukup
    if perf:
        lines = ["📊 <b>PERFORMANCE REPORT</b>","━━━━━━━━━━━━━━━━━━"]
        for key,s in sorted(perf.items()):
            em = {"SCALE":"🚀","NORMAL":"✅","CAUTION":"⚠️","KILL":"❌"}.get(s["status"],"❓")
            lines.append(f"{em} <b>{key}</b> ({s['total']} trades)\n"
                         f"   WR:{s['win_rate']}% PF:{s['profit_factor']} "
                         f"E(x):{s['expectancy']:+.3f}R [{s['status']}]")
        tg("\n".join(lines))


# ════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════

def run():
    global _signal_queue, _watching_sent, _watching_count, _candle_cache, _portfolio_state, _flush_slots
    global _adaptive, _equity_state

    # Reset semua state per cycle
    _signal_queue    = []
    _watching_sent   = set()
    _watching_count  = 0
    _candle_cache    = {}   # flush cache — data baru setiap cycle
    _portfolio_state = load_portfolio_state()
    _microcap_count  = [0]  # list agar bisa dimodifikasi di dalam fungsi
    _trending_count  = [0]  # [UPGRADE P3] counter sinyal trending per cycle
    _flush_slots     = {"confirmed": 0, "speculative": 0}  # [FIX 2] reset slot counters

    # [NEW 4] Reset equity state — akan di-isi oleh check_equity_protection
    _equity_state = {"daily_realized_pct": 0.0, "halted": False, "halt_reason": ""}

    # FIX: Hanya reset Coinglass counter jika skip_until sudah lewat.
    global _coinglass_fail_count, _coinglass_skip_until
    if time.time() > _coinglass_skip_until:
        _coinglass_fail_count = 0

    client = get_client()
    print(f"=== SIGNAL SCAN | STABLE v1.4 (Feedback Loop + Adaptive + Equity Protection) ===")

    # Kill switch
    if KILL_SWITCH_ENABLED:
        halted, reason = check_kill_switch()
        if halted:
            tg(f"🛑 <b>KILL SWITCH AKTIF</b>\n{reason}\n<i>Scan dibatalkan.</i>")
            print(f"🛑 {reason}"); return

    fg, fg_label = get_fear_greed()
    market_data  = get_coingecko_market()
    trending     = get_coingecko_trending()
    regime       = get_market_regime(client)

    # [NEW 2] Compute adaptive thresholds — harus setelah regime diketahui
    compute_adaptive_thresholds(client, regime)

    # [NEW 4] Equity protection check — halt sebelum scan dimulai
    eq_halted, eq_reason, daily_pnl = check_equity_protection(client)
    if eq_halted:
        tg(f"🛡️ <b>EQUITY PROTECTION AKTIF</b>\n{eq_reason}\n"
           f"Daily P&L: <b>{daily_pnl:+.2f}%</b>\n<i>Scan dibatalkan.</i>")
        print(f"🛡️ {eq_reason}"); return

    print(f"F&G:{fg} | Regime:{regime['regime']} | Session:{'DEAD' if is_dead_session() else 'ACTIVE'} | "
          f"Daily P&L:{daily_pnl:+.2f}%")

    print("📝 Updating expired signals...")
    update_expired_signals(client)

    print("🧠 Loading strategy performance...")
    perf = load_strategy_performance()
    for k,v in perf.items():
        print(f"   {k}: WR={v['win_rate']}% [{v['status']}]")

    # BTC volatility guard
    try:
        c1h,h1h,l1h,_ = get_candles(client,"BTC_USDT","1h",24)
        if c1h is not None:
            atr_btc = calc_atr(c1h,h1h,l1h)
            if (atr_btc/c1h[-1])*100 > 4.0:
                tg("⚠️ <b>BTC Volatility Spike</b> — Scan ditunda.")
                print("❌ BTC vol spike"); return
    except Exception: pass

    # BTC crash guard
    try:
        btc    = client.list_tickers(currency_pair="BTC_USDT")[0]
        change = float(btc.change_percentage or 0)
        if change < -12:
            tg("⚠️ <b>Market Crash</b> — BTC >12% drop. Scan ditunda.")
            return
    except Exception: pass

    if should_send_summary():
        send_market_summary(fg,fg_label,market_data,trending,regime,perf)

    tickers = client.list_tickers()
    total   = 0

    for t in tickers:
        pair = t.currency_pair
        if not is_valid(pair): continue

        try:
            price      = float(t.last or 0)
            vol_24h    = float(t.quote_volume or 0)
            change_24h = float(t.change_percentage or 0)
            if price<=0 or vol_24h<MIN_VOLUME_USDT: continue

            symbol   = pair.replace("_USDT","")
            ob_ratio = get_order_book_ratio(client,pair)

            if pair in FUTURES_PAIRS:
                funding   = get_funding_rate(pair)
                liq_bias  = get_liquidation_bias(symbol)
                oi_signal = get_open_interest(pair,symbol,change_24h)
            else:
                funding   = None
                liq_bias  = "NEUTRAL"
                oi_signal = "NEUTRAL"

            # Hierarki: SWING → INTRADAY → SCALPING (satu pair satu strategi)
            q_before = len(_signal_queue)
            check_swing(client,pair,price,fg,ob_ratio,funding,
                        trending,liq_bias,oi_signal,regime)
            swing_ok = len(_signal_queue)>q_before

            check_intraday(client,pair,price,fg,ob_ratio,funding,
                           trending,liq_bias,oi_signal,regime)
            intra_ok = len(_signal_queue)>(q_before+(1 if swing_ok else 0))

            if not swing_ok and not intra_ok:
                check_scalping(client,pair,price,fg,ob_ratio,funding,
                               liq_bias,oi_signal,regime)

            # MOONSHOT independen — slot terpisah
            # [UPGRADE P2] Kini memiliki dua path: MOMENTUM (reaktif) + PRE_PUMP (prediktif)
            check_moonshot(client,pair,price,change_24h,trending,liq_bias,regime)

            # [UPGRADE P3] Trending pre-signal — pair trending tapi belum pump
            # Dijalankan untuk semua pair yang lolos volume minimum
            check_trending_presignal(client,pair,price,change_24h,
                                     trending,regime,_trending_count)

            # Microcap accumulation hunter — scan pair yang vol < main threshold
            # tapi sudah mulai bergerak. Gunakan threshold vol lebih rendah.
            if vol_24h >= MIN_VOLUME_MICROCAP and vol_24h < MIN_VOLUME_USDT:
                check_microcap_accumulation(
                    client, pair, price, vol_24h, change_24h,
                    trending, regime, _microcap_count
                )

            # Dump warning — untuk pair yang sudah naik >5% dan ada tanda distribusi
            if change_24h > 5.0:
                detect_dump_warning(client, pair, price, change_24h, regime)

            total += 1
            time.sleep(0.15)

        except Exception as e:
            print(f"⚠️ [{pair}]: {e}"); continue

    print(f"\n📨 Queue: {len(_signal_queue)} candidates")
    flush_signal_queue(perf, regime)
    print(f"=== DONE | {total} pairs scanned ===")

    tg(f"🔍 <b>Scan Selesai — STABLE v1.5</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Pairs scanned : <b>{total}</b>\n"
       f"Regime        : <b>{regime['regime']}</b>\n"
       f"BTC 1h        : <b>{regime['btc_1h_chg']:+.1f}%</b>\n"
       f"F&G           : <b>{fg}</b>\n"
       f"Session       : <b>{'DEAD (Scalping OFF)' if is_dead_session() else 'ACTIVE'}</b>\n"
       f"Mode          : <b>{'🎯 SIGNAL ONLY' if SIGNAL_ONLY_MODE else '💼 PORTFOLIO'}</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Adaptive | BTC ATR:{_adaptive['btc_atr_pct']:.2f}% | "
       f"RSI_exit:{_adaptive['prepump_rsi_exit']} | "
       f"{'⚡ VOLATILE' if _adaptive['is_volatile'] else '😴 CALM'}\n"
       f"Daily P&L: <b>{daily_pnl:+.2f}%</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Signal sent   : <b>{_flush_slots['confirmed'] + _flush_slots['speculative']}</b> "
       f"(conf:{_flush_slots['confirmed']} spec:{_flush_slots['speculative']})\n"
       f"WATCHING sent : <b>{_watching_count}</b>\n"
       f"Trending Early: <b>{_trending_count[0]}</b> 🌟\n"
       f"Microcap      : <b>{_microcap_count[0]}</b> 🔬\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Min Tier      : <b>{SIGNAL_MIN_TIER}</b> | Max/cycle: <b>{MAX_SIGNALS_PER_CYCLE}</b>\n"
       f"Slot used     : Conf <b>{_flush_slots['confirmed']}/{MAX_SLOTS_CONFIRMED}</b> | "
       f"Spec <b>{_flush_slots['speculative']}/{MAX_SLOTS_SPECULATIVE}</b>\n"
       f"<i>Priority: SWING→INTRA→SCALP→MOON→PREPUMP→TRENDING</i>")



if __name__ == "__main__":
    run()
