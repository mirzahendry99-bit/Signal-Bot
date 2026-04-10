"""
╔══════════════════════════════════════════════════════════════════╗
║           SIGNAL BOT — CLEAN v7.7                               ║
║                                                                  ║
║  Upgrade dari v6.0:                                             ║
║                                                                  ║
║  [NEW] MICROCAP SCANNER — strategy baru terpisah                ║
║        Target: meme coin & microcap volume 20K–150K USDT        ║
║        Timeframe: 1h                                            ║
║        Trigger: volume spike 5x + momentum awal + RSI sehat     ║
║        TP besar (+15–40%), SL ketat (-5% max)                   ║
║        Tidak bergantung BOS/CHoCH — fokus volume anomali        ║
║                                                                  ║
║  Perbaikan dari v5.0 (Signal Volume Fix):                       ║
║  [FIX #5] MIN_VOLUME_USDT: 300K → 150K                         ║
║  [FIX #6] INTRADAY EMA gate → soft (penalty score)             ║
║  [FIX #7] SWING strength: 4 → 3 + tier A score naik 7→8        ║
║  [FIX #8] MACD gate → soft gate (penalty score)                ║
║  [FIX #9] RSI extreme bonus ke scoring                          ║
║                                                                  ║
║  Perbaikan v7.1 (Bug Fix & Hardening):                          ║
║  [v7.1 #1] Versi string konsisten di seluruh file               ║
║  [v7.1 #2] VWAP dihitung dari sesi 1 hari (48 candle 30m)      ║
║            bukan cumulative seluruh histori                      ║
║  [v7.1 #3] Cache key candle menyertakan limit — cegah           ║
║            silent data mismatch antar fungsi                     ║
║  [v7.1 #4] detect_order_block — bounds check eksplisit          ║
║            pada loop impulse agar tidak IndexError              ║
║  [v7.1 #5] BTC 4h change dihitung dari 1 candle 4h             ║
║            (bukan 5 candle = ~16 jam)                           ║
║  [v7.1 #6] Tier B microcap difilter di check_microcap           ║
║            (tidak return) — hemat komputasi                     ║
║  [v7.1 #7] PUMP save_signal: tp2 diisi None, bukan tp1          ║
║  [v7.1 #8] Rate limit Gate.io: retry + exponential backoff      ║
║  [v7.1 #9] detect_liquidity O(n²) → vectorized numpy           ║
║  [v7.1 #10] Logging struktural: semua print + timestamp WIB     ║
║                                                                  ║
║  Perbaikan v7.2 (Audit Fix — semua issue dari audit):           ║
║  [v7.2 #1] calc_vwap window disesuaikan per timeframe           ║
║            15m=96, 30m=48, 1h=24, 4h=6 — sebelumnya selalu 48  ║
║  [v7.2 #2] W dict: tambah key "equal_highs" terpisah           ║
║            dari "equal_lows" — fix semantic bug SELL branch     ║
║  [v7.2 #3] score_signal SELL pakai W["equal_highs"]            ║
║            bukan W["equal_lows"] — benar secara semantik        ║
║  [v7.2 #4] Entry SELL di check_intraday & check_swing          ║
║            mengacu last_sh (supply zone), bukan last_sl         ║
║  [v7.2 #5] run_pump_scan: BTC halt/drop kirim Telegram alert   ║
║            (sebelumnya hanya log, tanpa notifikasi ke user)     ║
║  [v7.2 #6] change_24h guard: handle None/""/NaN dari API       ║
║            Gate.io pada pair baru / ticker tidak lengkap        ║
║  [v7.2 #7] Dedup (already_sent*) & save_signal pakai UTC       ║
║            konsisten dengan storage default Supabase            ║
║  [v7.2 #8] API_KEY & SECRET_KEY divalidasi di startup          ║
║            bersama env vars lainnya — fail fast jika kosong     ║
║  [v7.2 #9] ob_ratio fetch lazy per pair — hanya dipanggil      ║
║            saat pair lolos dedup, bukan unconditional per pair  ║
║            → drastis kurangi API calls & potensi rate limit     ║
║                                                                  ║
║  Perbaikan v7.3 (Audit Fix Lanjutan):                           ║
║  [v7.3 #1] score_signal: candle_body pakai closes[-1]          ║
║            bukan price (live) — evaluasi candle confirmed        ║
║            berlaku BUY dan SELL branch                          ║
║  [v7.3 #2] detect_structure: bull_break/bear_break             ║
║            filter previous-candle diterapkan ke semua index     ║
║            — cegah stale BOS dari candle terlalu lama           ║
║  [v7.3 #3] gate_call_with_retry: return None eksplisit         ║
║            setelah loop exhausted — clarity & mypy compliance   ║
║  [v7.3 #4] is_valid_pair: ETF_KEYWORDS pakai exact match        ║
║            bukan startswith — cegah false-positive pada         ║
║            token meme/microcap (misal GOOGOL, COINDOG)          ║
║  [v7.3 #5] Komentar fix number env validation dikoreksi         ║
║            [v7.2 FIX #9] → [v7.2 FIX #8]                      ║
║  [v7.3 #6] get_btc_regime: limit 10→30 agar lolos guard        ║
║            len(raw)<30 di get_candles — sebelumnya BTC          ║
║            crash/drop protection SELALU return default          ║
║            (halt=False, block_buy=False) = proteksi mati total  ║
║  [v7.3 #7] check_pump: tambah guard sl>0 — cegah SL negatif   ║
║            pada token harga sangat rendah dengan ATR besar      ║
║  [v7.3 #8] score_signal BUY pullback: tambah lower bound       ║
║            price >= last_sl — cegah breakdown dianggap          ║
║            pullback dan mendapat +2 score secara salah          ║
║                                                                  ║
║  Fitur Baru v7.4 (Goal-Based Upgrade):                          ║
║  [v7.4 #1] ADX No-Trade Zone — detect_market_regime()          ║
║            CHOPPY (ADX<18): hard block INTRADAY & SWING         ║
║            RANGING (18≤ADX<25): lolos tapi penalti score -2     ║
║            TRENDING (ADX≥25): bonus score +2                    ║
║            Bot sekarang TAHU kapan harus diam                   ║
║  [v7.4 #2] calc_adx() — Wilder's smoothing standar industri    ║
║            returns (adx, +DI, -DI) untuk regime & trend dir     ║
║  [v7.4 #3] calc_conviction() — diferensiasi kualitas           ║
║            dalam tier: OK / GOOD / HIGH / VERY HIGH / EXTREME   ║
║            Tier A bukan lagi semuanya setara                     ║
║  [v7.4 #4] send_signal: tampilkan Regime + ADX + Conviction    ║
║            di Telegram — user bisa prioritaskan sinyal terbaik  ║
║                                                                  ║
║  Perbaikan v7.5 (Audit Fix):                                    ║
║  [v7.5 #1] ETF blocklist: dua lapis — ETF_EXACT (exact match)  ║
║            + ETF_PREFIX (prefix match turunan sintetis)         ║
║  [v7.5 #2] build_etf_blocklist pakai http_get_text() —         ║
║            fix silent fail karena http_get() json.loads()       ║
║            pada plain text / CSV response                        ║
║                                                                  ║
║  Perbaikan v7.6 (Full Audit Fix):                               ║
║  [v7.6 #1] run(): ob_ratio_cache refactor ke dict per-pair     ║
║            — fix closure bug _ob_cache list trick dalam loop    ║
║  [v7.6 #2] gate_call_with_retry: tambah explicit return None   ║
║            setelah loop exhausted — clarity & mypy safe         ║
║  [v7.6 #3] Version string summary Telegram: v7.4 → v7.6        ║
║            konsisten di seluruh file                            ║
║  [v7.6 #4] ETF_PREFIX: hapus entry yang sudah ada di ETF_EXACT ║
║            — eliminasi redundansi Layer 3 yang tidak berguna    ║
║  [v7.6 #5] calc_adx: Wilder seed pakai np.mean (SMA) bukan     ║
║            np.sum — fix overestimation ADX pada candle awal     ║
║  [v7.6 #6] detect_structure: range(1, ...) eksplisit — fix     ║
║            i=0 guard yang menyebabkan candle pertama terlewat   ║
║  [v7.6 #7] check_pump/check_microcap: highs window diperluas   ║
║            sertakan candle terakhir — fix anti buy-the-top      ║
║            yang tidak cek candle current jika ia high tertinggi ║
║  [v7.6 #8] build_etf_blocklist: parallel fetch via             ║
║            ThreadPoolExecutor — kurangi startup delay           ║
║  [v7.6 #9] calc_bb: hapus fungsi unused — bersihkan codebase   ║
║  [v7.6 #10] tg(): tambah retry 2x dengan backoff 2s            ║
║             — cegah kehilangan signal saat Telegram timeout     ║
║  [v7.6 #11] log(): unified ke WIB formatter di logging handler  ║
║             — hapus double output (logging + print)             ║
║  [v7.6 #12] already_sent*: 3 fungsi duplikat → 1 fungsi        ║
║             generik already_sent_generic() — DRY principle      ║
║                                                                  ║
║  Perbaikan v7.7 (Full Audit Fix — 12 temuan):                   ║
║  [v7.7 #1] calc_rsi: rolling mean → Wilder's EMA (ewm alpha     ║
║            1/period) — RSI sekarang konsisten dengan            ║
║            TradingView & platform charting standar              ║
║  [v7.7 #2] check_intraday SELL: late-entry filter dipindahkan   ║
║            sebelum scoring — hemat komputasi OB/ADX/MACD        ║
║            untuk pair yang akan di-skip anyway                  ║
║  [v7.7 #3] check_swing SELL: idem — late-entry filter           ║
║            dipindahkan sebelum scoring (konsistensi BUY branch) ║
║  [v7.7 #4] change_24h: tambahkan komentar unit eksplisit        ║
║            (Gate.io mengembalikan persen, bukan rasio)          ║
║            + guard NaN via math.isnan setelah float cast        ║
║  [v7.7 #5] detect_order_block: hapus safety guard               ║
║            "if i+1 >= n: continue" yang tidak pernah True       ║
║            karena loop sudah bounded di range(n-3, ...)         ║
║  [v7.7 #6] get_candles: guard len(raw) < 30 → < min(30, limit) ║
║            + log warning eksplisit — cegah silent skip          ║
║            pada pair baru dengan histori candle terbatas        ║
║  [v7.7 #7] _already_sent_generic: tambahkan in-memory fallback  ║
║            dedup set per cycle — cegah signal duplikat          ║
║            saat Supabase timeout/down                           ║
║  [v7.7 #8] build_etf_blocklist: tambahkan flag _ETF_BUILT       ║
║            — guard idempotent jika run() dipanggil berkali-kali ║
║            dalam satu proses (non-Actions deployment)           ║
║  [v7.7 #9] send_signal: guard tp2=None — cegah latent           ║
║            TypeError jika tp2 tidak tersedia                    ║
║  [v7.7 #10] SCAN_SLEEP_SEC konstanta — ekstrak 0.08/0.1 ke     ║
║             satu konstanta SCAN_SLEEP_SEC=0.1 untuk konsistensi ║
║  [v7.7 #11] detect_swing_points: tambahkan guard eksplisit      ║
║             jika strength terlalu besar relatif array           ║
║             — cegah silent no-signal tanpa warning              ║
║  [v7.7 #12] ETF_EXACT: hapus duplikat "SBUX" — code cleanliness ║
║  [v7.7 #13] ob_ratio scoring: tambahkan komentar simetri        ║
║             threshold 1.1/0.9 — clarity untuk reviewer          ║
║                                                                  ║
║  Hotfix v7.7b (post-deploy bug):                                 ║
║  [v7.7b #1] _dedup_memory reset ke set() bukan {} — fix         ║
║             AttributeError 'dict' has no attribute 'add'        ║
║             yang crash bot setelah signal pertama terkirim       ║
║  [v7.7b #2] TSLAON ditambahkan ke ETF_EXACT                     ║
║  [v7.7b #3] TSLA ditambahkan ke ETF_PREFIX — semua TSLA*        ║
║             tertangkap otomatis (TSLAON, TSLAB, dll)            ║
║  [v7.7b #4] Layer 4 suffix: tambahkan "ON" — blok semua         ║
║             tokenized on-chain stock (*ON pattern)               ║
║                                                                  ║
║  Arsitektur:                                                     ║
║  - INTRADAY (1h)     : BUY + SELL                               ║
║  - SWING    (4h)     : BUY + SELL                               ║
║  - PUMP SCANNER (15m): BUY only — big cap pump                  ║
║  - MICROCAP (1h)     : BUY only — meme/microcap early entry     ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os, json, time, math
import logging
import urllib.request
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
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
# [FIX #7] Tier A dinaikkan 7→8 sebagai kompensasi swing strength dilonggarkan
# Lebih banyak kandidat masuk scoring, tapi bar kualitas tetap terjaga
TIER_MIN_SCORE = {
    "S":  14,   # sangat terkonfirmasi — tidak berubah
    "A+": 10,   # tidak berubah
    "A":   8,   # [FIX #7] naik dari 7 → 8 (kompensasi gate yang dilonggarkan)
}
SIGNAL_MIN_TIER = "A"  # tier B tidak dikirim — digunakan sebagai referensi dokumentasi
                       # enforcement dilakukan via assign_tier() → return "SKIP" jika < A

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
ADX_CHOP   = 18   # threshold choppy / sideways
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
INTRADAY_SL_ATR = 1.5    # SL = entry ± ATR × 1.5
INTRADAY_TP1_R  = 1.5    # TP1 = SL distance × 1.5
INTRADAY_TP2_R  = 2.5    # TP2 = SL distance × 2.5
SWING_SL_ATR    = 2.0    # SL lebih longgar untuk 4h
SWING_TP1_R     = 2.0
SWING_TP2_R     = 3.5

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

# ── Scan Timing ──────────────────────────────────────
# [v7.7 #10] Satu konstanta untuk throttle loop — sebelumnya 0.08 (pump) vs 0.1 (main)
# yang tidak terdokumentasi dan inkonsisten. Disamakan ke 0.1s untuk semua scanner.
SCAN_SLEEP_SEC = 0.1

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

# ── Weighted Score Components ──────────────────────────
W = {
    "bos":          6,   # Break of Structure — paling penting
    "choch":        5,   # Change of Character — reversal confirmed
    "liq_sweep":    4,   # Liquidity sweep — smart money move
    "order_block":  4,   # Order block valid — institutional zone
    "macd_cross":   3,   # MACD crossover searah
    "rsi_zone":     3,   # RSI di zona optimal
    "vol_confirm":  3,   # Volume konfirmasi breakout
    "ema_align":    2,   # EMA alignment searah trend
    "vwap_side":    2,   # Harga di sisi yang benar dari VWAP
    "pullback":     2,   # Entry dari pullback, bukan kejar harga
    "candle_body":  2,   # Candle konfirmasi bullish/bearish
    "equal_lows":   1,   # Equal lows sebagai target likuiditas (BUY)
    "equal_highs":  1,   # Equal highs sebagai target likuiditas (SELL) — [v7.2 FIX #3]
    "ob_ratio":     1,   # Order book ratio mendukung arah
                         # BUY: ob_ratio > 1.1 (bid 10% dominan)
                         # SELL: ob_ratio < 0.9 (ask 10% dominan — simetris matematis)
    # [FIX #9] Bonus RSI ekstrem — area oversold/overbought lebih kuat
    "rsi_extreme":  2,   # RSI < 30 untuk BUY atau RSI > 70 untuk SELL
    # [FIX #8] Penalti MACD berlawanan — soft gate bukan hard reject
    "macd_soft":   -2,   # MACD counter-arah = kurangi score (tidak langsung reject)
    # Market Regime (ADX-based) — [v7.4]
    "adx_trend":    2,   # ADX >= 25: pasar sedang trend kuat → bonus
    "adx_ranging": -2,   # ADX 18-25: pasar ranging → penalti (CHOPPY di-block sebelum scoring)
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
def gate_call_with_retry(fn, *args, retries: int = 3, base_delay: float = 1.0, **kwargs):
    """
    Panggil fungsi Gate.io API dengan retry + exponential backoff.
    Menangani rate limit (429) dan error jaringan sementara.
    [v7.6 #2] Explicit return None setelah loop exhausted — clarity & mypy safe.
    """
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            err_str = str(e).lower()
            is_rate_limit = "429" in err_str or "rate limit" in err_str or "too many" in err_str
            if attempt < retries - 1:
                delay = base_delay * (2 ** attempt)  # 1s, 2s, 4s
                if is_rate_limit:
                    log(f"⚠️ Rate limit Gate.io — retry {attempt+1}/{retries} dalam {delay:.0f}s", "warn")
                else:
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
    from concurrent.futures import ThreadPoolExecutor, as_completed

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


_candle_cache: dict = {}

def get_candles(client, pair: str, interval: str, limit: int):
    """Fetch candles dengan cache per cycle. [v7.1 #3] Key menyertakan limit."""
    key = (pair, interval, limit)   # [v7.1 #3] limit masuk key — cegah silent mismatch
    if key in _candle_cache:
        return _candle_cache[key]
    try:
        raw = gate_call_with_retry(
            client.list_candlesticks,
            currency_pair=pair, interval=interval, limit=limit
        )
        # [v7.7 #6] min(30, limit) — cegah silent skip pada pair baru dengan histori terbatas.
        # Sebelumnya len(raw) < 30 selalu reject pair yang limit-nya < 30 atau histori < 30 candle.
        min_required = min(30, limit)
        if not raw or len(raw) < min_required:
            log(f"⚠️ candles [{pair}|{interval}|{limit}]: hanya {len(raw) if raw else 0} candle tersedia (min {min_required})", "warn")
            _candle_cache[key] = None; return None
        closes  = np.array([float(c[2]) for c in raw])
        highs   = np.array([float(c[3]) for c in raw])
        lows    = np.array([float(c[4]) for c in raw])
        volumes = np.array([float(c[1]) for c in raw])
        result  = (closes, highs, lows, volumes)
        _candle_cache[key] = result
        return result
    except Exception as e:
        log(f"⚠️ candles [{pair}|{interval}|{limit}]: {e}", "warn")
        _candle_cache[key] = None
        return None


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
    tr = [max(highs[i]-lows[i],
              abs(highs[i]-closes[i-1]),
              abs(lows[i]-closes[i-1]))
          for i in range(1, len(closes))]
    return float(pd.Series(tr).rolling(period).mean().iloc[-1])


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

    def wilder(arr):
        """Wilder's smoothing — setara EMA dengan alpha = 1/period.
        [v7.6 #5] Seed pertama pakai SMA (np.mean) bukan sum — sesuai definisi Wilder
        dan mencegah overestimation ADX pada candle-candle awal kalkulasi.
        """
        out = np.zeros(n)
        out[period] = float(np.mean(arr[1:period+1]))   # [v7.6 #5] SMA seed, bukan sum
        for i in range(period + 1, n):
            out[i] = out[i-1] - out[i-1] / period + arr[i]
        return out

    s_tr  = wilder(tr_arr)
    s_pdm = wilder(plus_dm)
    s_mdm = wilder(minus_dm)

    with np.errstate(divide="ignore", invalid="ignore"):
        pdi = np.where(s_tr > 0, 100.0 * s_pdm / s_tr, 0.0)
        mdi = np.where(s_tr > 0, 100.0 * s_mdm / s_tr, 0.0)
        dx  = np.where((pdi + mdi) > 0,
                       100.0 * np.abs(pdi - mdi) / (pdi + mdi), 0.0)

    adx_arr = wilder(dx)
    return float(adx_arr[-1]), float(pdi[-1]), float(mdi[-1])


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
#  SCORING ENGINE
# ════════════════════════════════════════════════════════

def score_signal(side: str, price: float, closes, highs, lows, volumes,
                 structure: dict, liq: dict, ob: dict,
                 rsi: float, macd: float, msig: float,
                 ema_fast: float, ema_slow: float,
                 vwap: float, ob_ratio: float,
                 regime: str = "TRENDING") -> int:
    is_bull = (side == "BUY")
    score   = 0

    if is_bull:
        if structure.get("bos")   == "BULLISH": score += W["bos"]
        if structure.get("choch") == "BULLISH": score += W["choch"]
        if liq.get("sweep_bull"):               score += W["liq_sweep"]
        if ob.get("valid"):                     score += W["order_block"]
        if macd > msig:                         score += W["macd_cross"]
        elif macd < msig:                       score += W["macd_soft"]   # [FIX #8] penalti, bukan reject
        if 30 < rsi < 60:                       score += W["rsi_zone"]
        if rsi <= 30:                           score += W["rsi_extreme"] # [FIX #9] bonus oversold
        vol_avg = float(np.mean(volumes[-10:-1]))
        if float(volumes[-1]) > vol_avg * 1.3:  score += W["vol_confirm"]
        if ema_fast > ema_slow:                 score += W["ema_align"]
        if price > vwap:                        score += W["vwap_side"]
        last_sl = structure.get("last_sl")
        # [v7.3 FIX] Tambah lower bound: price >= last_sl agar kondisi ini hanya aktif
        # saat price benar-benar di zona support, bukan saat breakdown di bawah swing low.
        if last_sl and last_sl <= price <= last_sl * 1.015: score += W["pullback"]
        last_close = float(closes[-1])
        prev       = float(closes[-2])
        body  = last_close - prev   # confirmed candle body (bullish = positive)
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
        elif macd > msig:                       score += W["macd_soft"]   # [FIX #8] penalti, bukan reject
        if 40 < rsi < 70:                       score += W["rsi_zone"]
        if rsi >= 70:                           score += W["rsi_extreme"] # [FIX #9] bonus overbought
        vol_avg = float(np.mean(volumes[-10:-1]))
        if float(volumes[-1]) > vol_avg * 1.3:  score += W["vol_confirm"]
        if ema_fast < ema_slow:                 score += W["ema_align"]
        if price < vwap:                        score += W["vwap_side"]
        last_sh = structure.get("last_sh")
        # Upper bound: price <= last_sh * 1.01 — harga harus DI DEKAT atau di bawah resistance,
        # bukan jauh di atas. Tanpa batas ini, price 3% di atas last_sh pun dapat +2 score.
        if last_sh and last_sh * 0.97 <= price <= last_sh * 1.01: score += W["pullback"]
        last_close = float(closes[-1])
        prev       = float(closes[-2])
        body  = prev - last_close   # confirmed candle body (bearish = positive)
        rng   = float(highs[-1]) - float(lows[-1]) + 1e-9
        if body > 0 and body / rng > 0.5:       score += W["candle_body"]
        if liq.get("equal_highs"):              score += W["equal_highs"]  # [v7.2 FIX #3]
        if ob_ratio < 0.9:                      score += W["ob_ratio"]

    # Market Regime adjustment — berlaku untuk BUY dan SELL
    # CHOPPY sudah diblokir di check_intraday/check_swing sebelum fungsi ini dipanggil
    if regime == "TRENDING":  score += W["adx_trend"]    # pasar trending → sinyal lebih valid
    elif regime == "RANGING": score += W["adx_ranging"]  # pasar ranging → sinyal lebih berisiko

    return score


def assign_tier(score: int) -> str:
    if score >= TIER_MIN_SCORE["S"]:  return "S"
    if score >= TIER_MIN_SCORE["A+"]: return "A+"
    if score >= TIER_MIN_SCORE["A"]:  return "A"
    return "SKIP"


def calc_conviction(score: int) -> str:
    """
    Diferensiasi kualitas sinyal di dalam tier — bukan pengganti tier,
    tapi label tambahan agar user bisa prioritaskan sinyal terbaik.

    Ini menjawab: "Tier A yang ini lebih layak dari Tier A yang lain?"
    Score bisa naik karena ADX bonus jadi scale ke atas lebih natural.
    """
    if score >= 18: return "EXTREME ⚡"
    if score >= 14: return "VERY HIGH 🔥"
    if score >= 12: return "HIGH 💪"
    if score >= 10: return "GOOD ✅"
    return "OK 🟡"


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


def get_order_book_ratio(client, pair: str) -> float:
    try:
        ob      = gate_call_with_retry(client.list_order_book, currency_pair=pair, limit=10)
        if ob is None: return 1.0
        bid_vol = sum(float(b[1]) for b in ob.bids)
        ask_vol = sum(float(a[1]) for a in ob.asks)
        return bid_vol / ask_vol if ask_vol > 0 else 1.0
    except Exception:
        return 1.0


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
                tp1: float, tp2, sl: float, tier: str, score: int,
                timeframe: str):
    """Simpan signal ke Supabase untuk tracking dan deduplication.
    [v7.2 FIX #7] sent_at disimpan dalam UTC agar konsisten dengan already_sent query.
    [v7.7 #7] Isi _dedup_memory setelah insert — sehingga cycle yang sama
    tidak bisa mengirim duplikat meski Supabase lambat merespons.
    """
    try:
        supabase.table("signals_v2").insert({
            "pair":      pair,
            "strategy":  strategy,
            "side":      side,
            "entry":     entry,
            "tp1":       tp1,
            "tp2":       tp2,   # bisa None untuk PUMP
            "sl":        sl,
            "tier":      tier,
            "score":     score,
            "timeframe": timeframe,
            "sent_at":   datetime.now(timezone.utc).isoformat(),  # [v7.2 FIX #7] UTC
            "result":    None,
        }).execute()
    except Exception as e:
        log(f"⚠️ save_signal [{pair}]: {e}", "warn")
    finally:
        # [v7.7 #7] Selalu tandai di memory — bahkan jika Supabase insert gagal,
        # mencegah re-send dalam cycle yang sama.
        _dedup_memory.add(_dedup_key(pair, strategy, side))


# ════════════════════════════════════════════════════════
#  SIGNAL STRATEGIES
# ════════════════════════════════════════════════════════

def check_intraday(client, pair: str, price: float, ob_ratio: float,
                   btc: dict, side: str = "BUY") -> dict | None:
    """
    INTRADAY signal — timeframe 1h.
    Mendukung BUY dan SELL.

    Gate BUY:
    - BOS/CHoCH bullish | MACD bullish | EMA7 > EMA20 | price > EMA7 | RSI < 70

    Gate SELL:
    - BOS/CHoCH bearish | MACD bearish | EMA7 < EMA20 | price < EMA7 | RSI > 25
    """
    if side == "BUY" and btc["block_buy"]: return None

    data = get_candles(client, pair, "1h", 100)
    if data is None: return None
    closes, highs, lows, volumes = data

    atr = calc_atr(closes, highs, lows)
    if atr / price * 100 < 0.2: return None
    if atr / price * 100 > 8.0: return None

    # ── Market Regime Gate — No Trade Zone ───────────────
    mkt = detect_market_regime(closes, highs, lows)
    if mkt["regime"] == "CHOPPY":
        return None   # ADX < 18: sideways/chop — jangan trade

    rsi        = calc_rsi(closes)
    macd, msig = calc_macd(closes)
    ema20      = calc_ema(closes, 20)
    ema50      = calc_ema(closes, 50)
    vwap       = calc_vwap(closes, highs, lows, volumes, timeframe="1h")  # [v7.2 FIX #1]
    structure  = detect_structure(closes, highs, lows, strength=3, lookback=60)
    liq        = detect_liquidity(closes, highs, lows, lookback=40)

    if not structure["valid"]: return None

    if side == "BUY":
        has_struct = (structure.get("bos")   == "BULLISH" or
                      structure.get("choch") == "BULLISH" or
                      liq.get("sweep_bull"))
        if not has_struct: return None          # Gate 1: WAJIB — struktur bullish
        if rsi > 72:       return None          # Gate 2: WAJIB — tidak overbought ekstrem
        # [FIX #6] EMA7>EMA20 sekarang soft — tidak lolos = score penalty di scoring
        # [FIX #8] MACD sekarang soft — tidak lolos = score penalty di scoring
        # Keduanya tetap dievaluasi di score_signal, bukan hard reject di sini

        ob    = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=25)
        score = score_signal("BUY", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema20, ema50, vwap, ob_ratio, mkt["regime"])
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        last_sh = structure.get("last_sh")
        if last_sh and price > last_sh * 1.02: return None  # late entry filter
        entry = round(last_sh * 1.002, 8) if (last_sh and price > last_sh) else price

    else:  # SELL
        has_struct = (structure.get("bos")   == "BEARISH" or
                      structure.get("choch") == "BEARISH" or
                      liq.get("sweep_bear"))
        if not has_struct: return None          # Gate 1: WAJIB — struktur bearish
        if rsi < 22:       return None          # Gate 2: WAJIB — tidak oversold ekstrem
        # [FIX #6] EMA alignment sekarang soft
        # [FIX #8] MACD sekarang soft

        # [v7.7 #2] Late-entry filter DIPINDAHKAN ke sini — sebelum scoring.
        # Sebelumnya filter ini ada setelah score_signal() + assign_tier() selesai,
        # membuang komputasi OB/ADX/MACD untuk pair yang akan di-skip.
        # Konsisten dengan BUY branch yang sudah melakukan filter sebelum scoring.
        last_sh = structure.get("last_sh")
        if last_sh and price < last_sh * 0.97: return None   # sudah terlalu jauh turun

        ob    = detect_order_block(closes, highs, lows, volumes, side="SELL", lookback=25)
        score = score_signal("SELL", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema20, ema50, vwap, ob_ratio, mkt["regime"])
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        # [v7.2 FIX #4] Entry SELL di dekat last_sh (resistance/supply zone),
        # bukan last_sl. last_sh sudah di-compute di atas untuk late-entry filter.
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
    }


def check_swing(client, pair: str, price: float, ob_ratio: float,
                btc: dict, side: str = "BUY") -> dict | None:
    """
    SWING signal — timeframe 4h. Mendukung BUY dan SELL.  ← [FIX #1]
    Gate lebih ketat dari intraday karena timeframe lebih panjang.

    Gate BUY:
    - BOS/CHoCH bullish | MACD bullish | price > EMA50 | RSI < 65
    - price > EMA9 | EMA9 > EMA21

    Gate SELL:
    - BOS/CHoCH bearish | MACD bearish | price < EMA50 | RSI > 30
    - price < EMA9 | EMA9 < EMA21
    """
    # [FIX #1] Blok BUY jika BTC drop — SELL tetap boleh jalan
    if side == "BUY" and btc["block_buy"]: return None

    data = get_candles(client, pair, "4h", 200)
    if data is None: return None
    closes, highs, lows, volumes = data

    atr  = calc_atr(closes, highs, lows)
    if atr / price * 100 < 0.5:  return None
    if atr / price * 100 > 12.0: return None

    # ── Market Regime Gate — No Trade Zone ───────────────
    mkt = detect_market_regime(closes, highs, lows)
    if mkt["regime"] == "CHOPPY":
        return None   # ADX < 18: sideways/chop — jangan trade SWING

    rsi        = calc_rsi(closes)
    macd, msig = calc_macd(closes)
    ema50      = calc_ema(closes, 50)
    ema200     = calc_ema(closes, 200)
    vwap       = calc_vwap(closes, highs, lows, volumes, timeframe="4h")  # [v7.2 FIX #1]
    structure  = detect_structure(closes, highs, lows, strength=3, lookback=100)  # [FIX #7] 4→3
    liq        = detect_liquidity(closes, highs, lows, lookback=60)

    if not structure["valid"]: return None

    if side == "BUY":
        # Gate 1: struktur bullish — WAJIB
        has_struct = (structure.get("bos")   == "BULLISH" or
                      structure.get("choch") == "BULLISH" or
                      liq.get("sweep_bull"))
        if not has_struct: return None

        # Gate 2: RSI tidak overbought — WAJIB (proteksi buy the top)
        if rsi > 68: return None

        ob    = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=40)
        score = score_signal("BUY", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema50, ema200, vwap, ob_ratio, mkt["regime"])
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        last_sh = structure.get("last_sh")
        if last_sh and price > last_sh * 1.02: return None
        entry = round(last_sh * 1.003, 8) if (last_sh and price > last_sh) else price

        sl, tp1, tp2 = calc_sl_tp(entry, "BUY", atr, structure, "SWING")
        if tp1 <= entry or sl >= entry: return None
        sl_dist = entry - sl
        if sl_dist <= 0 or sl_dist / entry > 0.10: return None
        rr = (tp1 - entry) / sl_dist

    else:  # SELL
        # Gate 1: struktur bearish — WAJIB
        has_struct = (structure.get("bos")   == "BEARISH" or
                      structure.get("choch") == "BEARISH" or
                      liq.get("sweep_bear"))
        if not has_struct: return None

        # Gate 2: RSI tidak oversold ekstrem — WAJIB
        if rsi < 28: return None

        # [v7.7 #3] Late-entry filter DIPINDAHKAN ke sini — sebelum scoring.
        # Konsisten dengan BUY branch dan check_intraday SELL (v7.7 #2).
        last_sh = structure.get("last_sh")
        if last_sh and price < last_sh * 0.97: return None

        ob    = detect_order_block(closes, highs, lows, volumes, side="SELL", lookback=40)
        score = score_signal("SELL", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema50, ema200, vwap, ob_ratio, mkt["regime"])
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        # [v7.2 FIX #4] Entry SELL di dekat last_sh (supply zone), konsisten dengan intraday.
        # last_sh sudah di-compute di atas untuk late-entry filter.
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
    }


# ════════════════════════════════════════════════════════
#  TELEGRAM OUTPUT
# ════════════════════════════════════════════════════════

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
    cur_price = sig.get("current_price", entry)
    bos       = sig["structure"].get("bos") or sig["structure"].get("choch") or "—"

    pct_tp1   = abs((tp1 - entry) / entry * 100)
    # [v7.7 #9] Guard tp2=None — latent TypeError jika tp2 tidak tersedia
    pct_tp2   = abs((tp2 - entry) / entry * 100) if tp2 is not None else 0.0
    pct_sl    = abs((sl  - entry) / entry * 100)
    # positif = harga di atas entry | negatif = harga di bawah entry
    pct_above = (cur_price - entry) / entry * 100

    tier_emoji  = {"S": "💎", "A+": "🏆", "A": "🥇"}.get(tier, "🎯")
    strat_emoji = {"INTRADAY": "📈", "SWING": "🌊"}.get(strategy, "🎯")
    side_emoji  = "🟢 BUY" if side == "BUY" else "🔴 SELL"

    regime      = sig.get("regime", "—")
    adx         = sig.get("adx", 0.0)
    conviction  = sig.get("conviction", "OK 🟡")
    regime_emoji = {"TRENDING": "🔥", "RANGING": "⚠️"}.get(regime, "—")

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
        f"Entry Zone : <b>${entry:.6f}</b> (limit / retest BOS){entry_note}\n"
        f"TP1  : <b>${tp1:.6f}</b> <i>({tp_label}{pct_tp1:.1f}%)</i>\n"
        f"TP2  : <b>{'${:.6f}'.format(tp2) if tp2 is not None else '—'}</b>"
        f"{' <i>(' + tp_label + '{:.1f}%)</i>'.format(pct_tp2) if tp2 is not None else ''}\n"
        f"SL   : <b>${sl:.6f}</b> <i>({sl_label}{pct_sl:.1f}%)</i>\n"
        f"R/R  : <b>1:{rr}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Score:      {score} | RSI: {rsi}\n"
        f"Struct:     {bos}\n"
        f"Regime:     {regime_emoji} {regime} (ADX: {adx})\n"
        f"Conviction: <b>{conviction}</b>\n"
        f"<i>⚠️ Pasang SL wajib. Ini signal, bukan rekomendasi finansial.</i>"
    )
    tg(msg)
    log(f"  ✅ SIGNAL {tier} {strategy} {side} {pair} RR:1:{rr} Score:{score}")


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

    # Hitung simple score untuk info di Telegram
    micro_score = 0
    if vol_ratio >= 8.0:      micro_score += 3   # volume spike kuat
    elif vol_ratio >= 5.0:    micro_score += 2
    if pct_3h >= 6.0:         micro_score += 2   # momentum kuat
    elif pct_3h >= 3.0:       micro_score += 1
    if rsi < 50:              micro_score += 1   # RSI masih rendah = lebih banyak ruang
    if ema_short_bull:        micro_score += 1   # EMA7 > EMA20
    if has_sweep:             micro_score += 2   # liq sweep = smart money move
    if change_24h < 5.0:      micro_score += 1   # belum pump banyak = early entry

    # [v7.1 #6] Filter tier B di sini — hemat komputasi di caller
    tier = "A" if micro_score >= 6 else "B"
    if tier == "B":
        return None   # tidak dikirim — jangan buang waktu di caller

    return {
        "pair":        pair,
        "strategy":    "MICROCAP",
        "side":        "BUY",
        "timeframe":   "1h",
        "entry":       entry,
        "tp1":         tp1,
        "tp2":         tp2,
        "sl":          sl,
        "tier":        "A",   # tier B sudah di-return None di atas
        "score":       micro_score,
        "rr":          rr,
        "rsi":         round(rsi, 1),
        "vol_ratio":   round(vol_ratio, 1),
        "pct_3h":      round(pct_3h, 2),
        "change_24h":  round(change_24h, 2),
        "atr_pct":     round(atr_pct, 2),
        "has_sweep":   has_sweep,
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

    sweep_line = "🧲 Liq sweep terdeteksi — smart money sudah masuk\n" if has_sweep else ""

    msg = (
        f"🔬 <b>{tier_emoji} [{tier}] MICROCAP SIGNAL 🟢 BUY</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair   : <b>{pair}</b> [1h]\n"
        f"⏰ Valid: {now.strftime('%H:%M')} → {valid_until}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Entry  : <b>${entry:.6f}</b>\n"
        f"TP1    : <b>${tp1:.6f}</b> <i>(+{pct_tp1:.1f}%)</i>\n"
        f"TP2    : <b>${tp2:.6f}</b> <i>(+{pct_tp2:.1f}%)</i>\n"
        f"SL     : <b>${sl:.6f}</b> <i>(-{pct_sl:.1f}%)</i>\n"
        f"R/R    : <b>1:{rr}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 Vol Spike : <b>{vol_ratio:.1f}×</b> rata-rata\n"
        f"📈 Naik 3h   : <b>+{pct_3h:.2f}%</b> | 24h: <b>{change_24h:+.1f}%</b>\n"
        f"RSI          : <b>{rsi}</b> | ATR: {atr_pct:.1f}%\n"
        f"{sweep_line}"
        f"Score  : {score}/10\n"
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

    tickers = gate_call_with_retry(client.list_tickers) or []
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
            time.sleep(SCAN_SLEEP_SEC)

        except Exception as e:
            log(f"⚠️ [{pair}]: {e}", "warn"); continue

    log(f"\n📊 Pump scan: {scanned} pairs | {len(pumps)} kandidat")

    if not pumps:
        log("📭 Tidak ada pump terdeteksi"); return

    pumps.sort(key=lambda x: -x["vol_ratio"])

    sent = 0
    for sig in pumps:
        if sent >= MAX_PUMP_SIGNALS: break
        send_pump_signal(sig)
        save_signal(
            sig["pair"], "PUMP", sig["side"],
            sig["entry"], sig["tp1"], None,   # [v7.1 #7] tp2=None — PUMP tidak punya TP2
            sig["sl"], "PUMP", 0, sig["timeframe"]
        )
        sent += 1
        time.sleep(0.5)

    log(f"\n✅ Pump scan done — {sent} alert terkirim")


# ════════════════════════════════════════════════════════
#  MAIN RUN
# ════════════════════════════════════════════════════════

def run():
    global _candle_cache, _dedup_memory
    _candle_cache  = {}   # flush cache setiap cycle
    _dedup_memory  = set()   # [v7.7 #7] reset in-memory dedup setiap cycle — HARUS set(), bukan {}

    client = get_client()

    # [v7.5] Build dynamic ETF blocklist sekali per run
    log("🔒 Membangun ETF blocklist dinamis...")
    build_etf_blocklist()

    if SCAN_MODE == "pump":
        run_pump_scan(client)
        return

    log(f"\n{'='*60}")
    log(f"🚀 SIGNAL BOT v7.7 — {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')} [FULL SCAN]")
    log(f"{'='*60}")

    fg  = get_fear_greed()
    btc = get_btc_regime(client)
    log(f"F&G: {fg} | BTC 1h: {btc['btc_1h']:+.1f}% | BTC 4h: {btc['btc_4h']:+.1f}%")

    allow_buy  = not btc["block_buy"]
    allow_sell = fg < FG_SELL_BLOCK

    log(f"Mode  : BUY={'✅ aktif' if allow_buy else '⛔ diblokir (BTC drop)'} | "
        f"SELL={'✅ aktif' if allow_sell else f'⛔ diblokir (F&G={fg} ≥ {FG_SELL_BLOCK})'}")

    if btc["halt"]:
        tg(f"🛑 <b>SIGNAL BOT HALT</b>\n"
           f"BTC crash {btc['btc_4h']:+.1f}% dalam 4h.\n"
           f"Tidak ada signal sampai kondisi stabil.")
        log("🛑 BTC crash — bot halt"); return

    tickers       = gate_call_with_retry(client.list_tickers) or []
    signals       = []
    micro_signals = []
    scanned       = 0
    skip_vol      = 0

    # [v7.6 #1] ob_ratio_cache dict per-pair di luar loop — menggantikan _ob_cache list trick
    # yang rawan closure bug ketika fungsi nested di-definisikan ulang setiap iterasi loop.
    # Dengan dict ini, fetch hanya terjadi sekali per pair dalam satu cycle, thread-safe,
    # dan tidak ada ambiguitas scope antara iterasi.
    _ob_ratio_cache: dict = {}

    def get_ob_ratio_lazy(p: str) -> float:
        """Fetch ob_ratio sekali per pair per cycle, cache hasilnya di dict."""
        if p not in _ob_ratio_cache:
            _ob_ratio_cache[p] = get_order_book_ratio(client, p)
        return _ob_ratio_cache[p]

    for t in tickers:
        pair = t.currency_pair
        if not is_valid_pair(pair): continue

        try:
            price      = float(t.last or 0)
            vol_24h    = float(t.quote_volume or 0)
            # [v7.2 FIX #6] Guard None/""/NaN dari Gate.io pada pair baru / ticker tidak lengkap
            # [v7.7 #4] Unit eksplisit: Gate.io change_percentage mengembalikan PERSEN (mis. 5.3 = 5.3%),
            # BUKAN rasio desimal (0.053). Semua threshold (MICRO_PRICE_MAX=25, dll) sudah dalam persen.
            _cp = t.change_percentage
            if _cp in (None, "", "NaN"):
                change_24h = 0.0
            else:
                _f = float(_cp)
                change_24h = 0.0 if math.isnan(_f) else _f
            if price <= 0: continue

            # ── MICROCAP SCANNER — zona volume 20K–150K ──────────
            # Dijalankan SEBELUM vol filter main bot
            # Pair yang dibuang main bot bisa ditangkap microcap scanner
            if (allow_buy
                    and MICRO_VOL_MIN <= vol_24h <= MICRO_VOL_MAX
                    and not already_sent_micro(pair)):
                sig = check_microcap(client, pair, price, vol_24h, change_24h)
                if sig: micro_signals.append(sig)  # [v7.1 #6] tier B sudah difilter di check_microcap

            # Vol filter untuk main bot (INTRADAY + SWING)
            if vol_24h < MIN_VOLUME_USDT:
                skip_vol += 1; continue

            scanned += 1

            # ── INTRADAY BUY ──────────────────────────────────
            if allow_buy and not already_sent(pair, "INTRADAY", "BUY"):
                sig = check_intraday(client, pair, price, get_ob_ratio_lazy(pair), btc, side="BUY")
                if sig: signals.append(sig)

            # ── INTRADAY SELL ─────────────────────────────────
            if allow_sell and not already_sent(pair, "INTRADAY", "SELL"):
                sig = check_intraday(client, pair, price, get_ob_ratio_lazy(pair), btc, side="SELL")
                if sig: signals.append(sig)

            # ── SWING BUY ────────────────────────────────────
            if allow_buy and not already_sent(pair, "SWING", "BUY"):
                sig = check_swing(client, pair, price, get_ob_ratio_lazy(pair), btc, side="BUY")
                if sig: signals.append(sig)

            # ── SWING SELL ───────────────────────────────────
            if allow_sell and not already_sent(pair, "SWING", "SELL"):
                sig = check_swing(client, pair, price, get_ob_ratio_lazy(pair), btc, side="SELL")
                if sig: signals.append(sig)

            time.sleep(SCAN_SLEEP_SEC)

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
        send_microcap_signal(sig)
        save_signal(
            sig["pair"], "MICROCAP", "BUY",
            sig["entry"], sig["tp1"], sig["tp2"], sig["sl"],
            sig["tier"], sig["score"], sig["timeframe"]
        )
        micro_sent += 1
        time.sleep(0.5)

    if not signals and micro_sent == 0:
        tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v7.7</b>\n"
           f"Pairs: {scanned} | F&G: {fg}\n"
           f"BUY : {'aktif' if allow_buy else 'diblokir (BTC drop)'}\n"
           f"SELL: {'aktif' if allow_sell else 'diblokir (extreme greed)'}\n"
           f"Tidak ada signal memenuhi kriteria saat ini.\n"
           f"<i>Bot akan scan lagi dalam 4 jam.</i>")
        log("📭 Tidak ada signal"); return

    # ── Kirim main signals ────────────────────────────────────────
    tier_order = {"S": 0, "A+": 1, "A": 2}
    signals.sort(key=lambda x: (tier_order.get(x["tier"], 9), -x["score"]))

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
    sent_sigs     = signals[:sent]
    intraday_buy  = sum(1 for s in sent_sigs if s["strategy"] == "INTRADAY" and s["side"] == "BUY")
    intraday_sell = sum(1 for s in sent_sigs if s["strategy"] == "INTRADAY" and s["side"] == "SELL")
    swing_buy     = sum(1 for s in sent_sigs if s["strategy"] == "SWING"    and s["side"] == "BUY")
    swing_sell    = sum(1 for s in sent_sigs if s["strategy"] == "SWING"    and s["side"] == "SELL")

    tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v7.7</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Pairs scanned : <b>{scanned}</b>\n"
       f"F&G           : <b>{fg}</b>\n"
       f"BTC 1h/4h     : <b>{btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}%</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Signal terkirim : <b>{sent + micro_sent}</b>\n"
       f"  📈 INTRADAY BUY  : {intraday_buy}\n"
       f"  📉 INTRADAY SELL : {intraday_sell}\n"
       f"  🌊 SWING BUY     : {swing_buy}\n"
       f"  🌊 SWING SELL    : {swing_sell}\n"
       f"  🔬 MICROCAP BUY  : {micro_sent}\n"
       f"<i>Scan berikutnya dalam 4 jam.</i>")

    log(f"\n✅ Done — {sent + micro_sent} signal terkirim "
        f"({sent} main + {micro_sent} microcap)")
    log(f"   INTRADAY BUY:{intraday_buy} SELL:{intraday_sell} | "
        f"SWING BUY:{swing_buy} SELL:{swing_sell} | MICROCAP:{micro_sent}")


if __name__ == "__main__":
    run()
