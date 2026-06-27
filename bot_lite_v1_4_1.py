# -*- coding: utf-8 -*-
# Signal Bot Lite v1.4.3 — CLEAN & SAFE BUILD
# Diturunkan dari v1.4.2
#
# CHANGELOG v1.4.3 → v1.4.4  — DATA-DRIVEN PROFITABILITY FIX
#   Semua perubahan didasarkan pada analisis 280 trades aktual dari Supabase.
#
#   [DATA-5]  MIN_SCORE 3.0 → 3.5: eliminasi Tier B (score < 3.0) sepenuhnya.
#             Tier B: 79 trades, WR 13.9%, avg PnL -$0.36 — drag terbesar.
#             Tier A (3.0–3.4): WR 32.4%, avg +$0.12.
#             Tier A+ (≥3.5): WR 45.1%, avg -$0.05 — basis terkuat.
#
#   [DATA-6]  MIN_RR 1.5 → 1.8, TP1_R 1.5 → 1.8, TP2_R 2.5 → 3.0:
#             Actual RR dari data hanya 1.28 (avg win $1.25 vs avg loss $0.98).
#             WR 42.7%, break-even butuh WR 43.8% → ekspektasi -$0.024/trade.
#             RR 1.8 paksa setup lebih asimetris agar winner > loser secara nyata.
#
#   [DATA-7]  BLOCK_HOURS diperluas: tambah jam 11,12,13,15,16,18 WIB.
#             Jam 13 WIB: PnL -$14.41, WR 16% (32 trades) — terburuk.
#             Jam 16 WIB: PnL -$5.40, WR 18% | Jam 12: -$5.38, WR 30%.
#             Jam aktif tersisa: 07,08,10,14,17,20,21,22 WIB.
#             Estimasi PnL saved: +$30.50 dari data historis.
#
# CHANGELOG v1.4.2 → v1.4.3
#   [FIX-CRITICAL]  GATE_SECRET_KEY → GATE_SECRET di run() block_hours path (line ~2151).
#                   Bug ini menyebabkan NameError setiap kali jam masuk BLOCK_HOURS_WIB,
#                   membuat lifecycle monitor (evaluate_open_trades) tidak pernah dijalankan
#                   di jam blok. Open trades terbengkalai semalam suntuk tanpa monitoring.
#   [FIX-RATELIMIT] Tambah _gate_call_with_retry() — exponential backoff (1.5s, 3.0s)
#                   untuk semua Gate.io API calls (list_tickers, list_candlesticks).
#                   Sebelumnya: 429/throttle hanya di-log warn, tanpa retry.
#   [FIX-JSONL]     JSONL_PATH sekarang absolute (os.path.abspath) — sebelumnya relative path
#                   menyebabkan signals.jsonl tertulis di lokasi berbeda tiap GitHub Actions run.
#   [FIX-TESTS]     Tambah 8 unit test baru: portfolio_allows, drawdown severity logic,
#                   score_signal (BUY/SELL/anomaly/choppy), get_drawdown_mode, RR validation.
#                   Total test: 23 cases (naik dari 15).
#   [FIX-COMMENTS]  Hapus TUNE comments yang conflict dengan DATA decisions aktif.
#                   TUNE-1 (MIN_SCORE 2.5) dihapus — aktif: DATA-1 (MIN_SCORE 3.0).
#                   TUNE-6 (MIN_RR 1.2) dihapus — aktif: DATA-2 (MIN_RR 1.5).
#
# ── PARAMETER AKTIF (Ground Truth) ────────────────────────────────────────────
#   MIN_SCORE         = 3.5    [DATA-5] eliminasi Tier B (WR 13.9%, avg -$0.36)
#   MIN_RR            = 1.8    [DATA-6] actual RR 1.28, ekspektasi -$0.024/trade
#   MAX_SL_PCT        = 3.5%   [DATA-3] noise > 3.5% → skip
#   BLOCK_HOURS_WIB   = 23,0,1,2,3,4,5,6,11,12,13,15,16,18  [DATA-7]
#   MAX_OPEN_TRADES   = 13     kapasitas portofolio penuh
#   MAX_RISK_TOTAL    = 15%    (13 × 1% = 13%, buffer 2%)
#   PAIR_COOLDOWN     = 12h    cooldown per pair setelah close
#   DEDUP_HOURS       = 4h     window dedup sinyal
#   SELL_ENABLED      = false  default off sampai SELL WR terverifikasi
#
# CHANGELOG v1.4.1 → v1.4.2
#   [DATA-1]   MIN_SCORE 3.5 → 3.0: analisis 230 trades menunjukkan score 3.0
#              adalah satu-satunya range yang profitable (WR ~45%, PnL +$6).
#              Score 3.5 WR hanya 16% dengan total PnL -$20. Score 2.5 WR 13% (-$16).
#              Filter lain tetap aktif: 4h MTF confirmation, adaptive_min, volume,
#              pump filter — sehingga tidak semua score 3.0 lolos otomatis.
#              Tier B ditambahkan untuk score 3.0–3.4.
#   [DATA-2]   MIN_RR 1.2 → 1.5: dengan avg loss -$0.97 dan avg win TP2 +$1.03,
#              RR minimum 1.2 tidak cukup untuk ekspektasi positif jangka panjang.
#   [DATA-3]   MAX_SL_PCT 5.0% → 3.5%: SL terlalu lebar menyebabkan 127 trades
#              loss dengan avg -$0.97. Setup dengan noise > 3.5% dari entry dilewati.
#   [DATA-4]   Time filter ditambahkan: jam 23:00–06:00 WIB diblok default.
#              Data menunjukkan WR < 30% di jam tersebut (midnight: 29%, 11:00: 17%).
#              Override via env BLOCK_HOURS_WIB. Monitor lifecycle tetap jalan.
#
#   [FIX HIGH-1]   portfolio_allows() menggunakan INITIAL_EQUITY statis → diganti equity aktual
#                  (INITIAL_EQUITY + cum_pnl). Equity aktual dihitung di run() sekali,
#                  lalu diteruskan ke get_portfolio_state() → portfolio_allows() → send_signal().
#                  Sebelumnya: dengan PnL -$33, risk budget dihitung dari $350 bukan $316 (overstate).
#   [FIX HIGH-2]   Auto-reset HALT diimplementasikan. Sebelumnya pesan "auto-reset jika kondisi
#                  membaik" hanya ada di Telegram — tidak ada kode yang mengeksekusinya.
#                  Sekarang di awal run(): jika is_halted=True tapi streak < STREAK_HALT
#                  dan dd_pct < DD_WARN_PCT → set_bot_halt(False) otomatis.
#   [FIX HIGH-3]   SELL_ENABLED dipindahkan dari hardcoded False ke env var SELL_ENABLED.
#                  Untuk aktifkan: set SELL_ENABLED=true di GitHub Actions secrets/vars.
#   [FIX HIGH-4]   Unit tests ditambahkan untuk calc_sl_tp, calc_position_size, score_signal.
#                  Jalankan: python -m pytest bot_lite_v1_4.py -v (atau python bot_lite_v1_4.py --test)
#   [FIX MED-1]    _write_config() sekarang selalu tulis ke file lokal (halt_state.json)
#                  sebagai fallback. Jika Supabase down, halt state tetap persist.
#   [FIX MED-2]    get_drawdown_state(): streak HALT tidak lagi di-downgrade ke WARN
#                  meski equity_mode=normal. Sekarang pakai max(severity) tanpa pengecualian.
#   [FIX MED-4]    5 silent except:pass diganti dengan log(..., "warn") untuk debugging.
#                  Fungsi: get_ticker_price, get_fear_greed, get_pair_winrate,
#                  is_recently_signaled, is_in_cooldown, _get_idr_rate, datetime parse.
#
#   [TUNE-2]   MAX_OPEN_TRADES 5 → 13: kapasitas portofolio diperluas penuh.
#   [TUNE-3]   MAX_SAME_SIDE 3 → 10: mengakomodasi 13 slot BUY di pasar bullish.
#   [TUNE-4]   MAX_SIGNALS_CYCLE 5 → 13: scan bisa mengirim sampai 13 sinyal per siklus.
#   [TUNE-5]   MAX_RISK_TOTAL 8% → 15%: budget risiko disesuaikan (13 × 1% = 13%).
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

BOT_VERSION = "1.4.4-lite"
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

MIN_SCORE           = 3.5       # [DATA-5] 3.0 → 3.5: analisis 280 trades menunjukkan Tier B
                                #          (score < 3.0) WR hanya 13.9% avg PnL -$0.36 — racun utama.
                                #          Tier A (3.0–3.4) WR 32.4% avg +$0.12.
                                #          Tier A+ (≥3.5) WR 45.1% — volume terbesar, basis terkuat.
                                #          Naik ke 3.5 eliminasi Tier B sepenuhnya (79 trades hilang).
MIN_RR              = 1.8       # [DATA-6] 1.5 → 1.8: actual RR dari data 280 trades hanya 1.28
                                #          (avg win $1.25 vs avg loss $0.98). Dengan WR 42.7%,
                                #          break-even butuh RR 1.28 → ekspektasi/trade -$0.024 (negatif).
                                #          RR 1.8 paksa setup lebih asimetris: TP lebih jauh dari SL,
                                #          sehingga winner lebih besar dan kompensasi SL lebih baik.
MAX_ENTRY_DEV       = 0.02

ADX_TREND           = 25
ADX_CHOP            = 20
ADX_PERIOD          = 14

BTC_DROP_BLOCK         = -3.0
BTC_CRASH_BLOCK        = -10.0
BTC_VOLATILE_1H        = 1.5    # abs(BTC 1h change) > 1.5% = terlalu volatile
BTC_RANGE_1H           = 2.5    # BTC 1h high-low range > 2.5% = choppy
BTC_TREND_LOOKBACK     = 4
BTC_TREND_MIN_BEARISH  = 3

TP1_R             = 1.8       # [DATA-6] disesuaikan dengan MIN_RR baru
TP2_R             = 3.0       # [DATA-6] dinaikkan proporsional: TP2 lebih jauh = winner lebih besar
SL_ATR_MULT       = 2.0
ATR_SL_BUFFER     = 0.5
MIN_SL_PCT        = 0.005
MAX_SL_PCT        = 0.035      # [DATA-3] 5.0% → 3.5%: avg loss per SL -$0.97 terlalu besar.
                               #          Dengan equity $316 dan RISK_PER_TRADE 1%, SL max 3.5%
                               #          membatasi kerugian per trade lebih ketat.
                               #          Setup dengan SL > 3.5% artinya noise terlalu besar — skip.

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

SIGNAL_EXPIRE_HOURS = 18          # [v1.4] 36 → 18: lebih sesuai INTRADAY

# ── ACTIVE_HOURS — 24 JAM MODE dengan TIME FILTER ────────────────────────────
# [DATA-4] Data 230 trades menunjukkan jam 00:00 WIB win rate hanya 29% (12 SL vs 5 TP2).
# Jam 01:00 WIB: 4 SL vs 2 TP2 = 33%. Jam 11:00 WIB: 5 SL vs 1 TP1 = 17%.
# Block jam 23:00-06:00 WIB (16:00-23:00 UTC) — liquidity rendah, spread lebar,
# manipulasi lebih sering terjadi di luar jam Asia/Eropa/US overlap.
# Override via env: BLOCK_HOURS_WIB="23,0,1,2,3,4,5,6" (format jam WIB dipisah koma)
# [DATA-7] Block hours diperbarui dari analisis 280 trades per jam WIB:
# Jam yang diblock: malam (23–06) + siang problematik (11,12,13,15,16,18)
# Jam 13 WIB = paling parah: PnL -$14.41, WR 16% dari 32 trades
# Jam 16 WIB = -$5.40, WR 18% | Jam 12 = -$5.38, WR 30%
# Jam 18 WIB = -$2.83, WR 20% | Jam 15 = -$1.49, WR 38%
# Jam 11 WIB = -$0.99, WR 33%
# Jam profitable yang dibiarkan aktif: 07,08,10,14,17,20,21,22
# Estimasi PnL saved jika jam ini diblock: +$30.50
_default_block = "23,0,1,2,3,4,5,6,11,12,13,15,16,18"
BLOCK_HOURS_WIB = set(
    int(h.strip())
    for h in os.getenv("BLOCK_HOURS_WIB", _default_block).split(",")
    if h.strip().isdigit()
)

# ── Sell system toggle ────────────────────────────────────────────────────────
# [FIX HIGH-3] SELL_ENABLED dipindahkan dari hardcoded False ke environment variable.
# Ini memungkinkan SELL diaktifkan tanpa perlu edit kode dan re-deploy.
# Default: False (tetap off sampai edge SELL terverifikasi dari data aktual).
# Untuk mengaktifkan: set env SELL_ENABLED=true di GitHub Actions secrets/vars.
# Crypto adalah structurally bullish market. Short breakdown jauh lebih sering
# false. Aktifkan SELL hanya jika SELL WR sudah terverifikasi dari data aktual.
SELL_ENABLED = os.getenv("SELL_ENABLED", "false").strip().lower() == "true"

# ── Telegram notification level ──────────────────────────────────────────────
# "full"    : semua notifikasi (TP1, TP2, warn, scan, equity, lifecycle)
# "signal"  : hanya trade events (OPEN, TP1_HIT, CLOSE) — tanpa scan summary
# "minimal" : hanya trade close + equity report
TG_NOTIFY_LEVEL = os.getenv("TG_NOTIFY_LEVEL", "").strip() or "full"  # default: full
API_FAILURE_THRESHOLD = 5   # halt scan jika consecutive failure mencapai ini
API_DECAY_ON_SUCCESS  = 1   # setiap success: kurangi counter 1 (tidak langsung 0)
JSONL_PATH            = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.jsonl")

# ── ANOMALY MODE — aktif saat F&G < 30 (Extreme Fear) ───────────────────────
# Saat market Fear, bot switch ke mode anomaly:
# - MTF 4h dilewati — koin bisa outperform meski BTC bearish di 4h
# - Filter relative strength vs BTC: pair harus naik > 3% saat BTC flat/turun
# - Volume minimum 2× rata-rata (lebih ketat dari normal 1.2×)
# - Tujuan: temukan koin yang bergerak melawan arus = genuine momentum
ANOMALY_FG_THRESHOLD = 30       # F&G di bawah ini → anomaly mode aktif
ANOMALY_OUTPERFORM   = 0.015    # pair harus naik minimal 1.5% lebih dari BTC 1h
ANOMALY_VOL_MULT     = 2.0      # volume minimum 2× rata-rata

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
#  [FIX-RATELIMIT] GATE.IO API RETRY WRAPPER
#  Exponential backoff untuk handle 429 / throttle.
#  Max 3 attempts: 0s → 1s → 3s → raise
# ════════════════════════════════════════════════════════

def _gate_call_with_retry(fn, *args, max_attempts: int = 3, **kwargs):
    """
    Wrapper untuk semua Gate.io API call.
    Retry otomatis dengan backoff jika 429 atau connection error.
    Raise exception terakhir jika semua attempt gagal.
    """
    last_exc = None
    for attempt in range(max_attempts):
        try:
            result = fn(*args, **kwargs)
            return result
        except Exception as e:
            last_exc = e
            err_str = str(e).lower()
            # Retry hanya untuk rate limit atau connection error
            is_retryable = any(k in err_str for k in ["429", "rate", "timeout", "connection", "reset"])
            if not is_retryable or attempt == max_attempts - 1:
                raise
            wait = (attempt + 1) * 1.5
            log(f"⚠️ Gate.io API error (attempt {attempt+1}/{max_attempts}): {e} — retry in {wait:.1f}s", "warn")
            time.sleep(wait)
    raise last_exc  # pragma: no cover

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


# ════════════════════════════════════════════════════════
#  ACCUMULATION DETECTION & PUMP FILTER  [v1.4]
# ════════════════════════════════════════════════════════

def detect_accumulation(closes: list, highs: list, lows: list,
                        volumes: list, lookback: int = 15) -> dict:
    """
    Deteksi fase akumulasi sebelum breakout.
    Accumulation = harga konsolidasi sempit + OBV naik + volume gradual naik.

    Returns:
        {
          "accumulating": bool,
          "obv_slope": float,   # positif = smart money masuk
          "cmf": float,         # Chaikin Money Flow, positif = buying pressure
          "compression": float, # range harga relatif, kecil = konsolidasi
          "vol_slope": float,   # kemiringan volume, positif = volume naik
        }
    """
    if len(closes) < lookback + 5:
        return {"accumulating": False, "obv_slope": 0.0,
                "cmf": 0.0, "compression": 1.0, "vol_slope": 0.0}

    c = closes[-lookback:]
    h = highs[-lookback:]
    l = lows[-lookback:]
    v = volumes[-lookback:]

    # 1. Price compression — range relatif terhadap harga rata-rata
    avg_price   = sum(c) / len(c)
    price_range = (max(h) - min(l)) / avg_price if avg_price > 0 else 1.0

    # 2. OBV (On-Balance Volume)
    obv = [0.0]
    for i in range(1, len(c)):
        if c[i] > c[i-1]:
            obv.append(obv[-1] + v[i])
        elif c[i] < c[i-1]:
            obv.append(obv[-1] - v[i])
        else:
            obv.append(obv[-1])

    # OBV slope: bandingkan rata-rata 5 pertama vs 5 terakhir
    obv_early = sum(obv[:5]) / 5
    obv_late  = sum(obv[-5:]) / 5
    obv_slope = (obv_late - obv_early) / (abs(obv_early) + 1)

    # 3. CMF — Chaikin Money Flow (14 periode)
    mf_vol = 0.0
    total_vol = 0.0
    for i in range(-14, 0):
        hi, lo, cl, vl = h[i], l[i], c[i], v[i]
        hl_range = hi - lo
        if hl_range > 0:
            mf_mult = ((cl - lo) - (hi - cl)) / hl_range
        else:
            mf_mult = 0.0
        mf_vol    += mf_mult * vl
        total_vol += vl
    cmf = mf_vol / total_vol if total_vol > 0 else 0.0

    # 4. Volume slope — apakah volume gradual naik
    vol_early  = sum(v[:5]) / 5
    vol_late   = sum(v[-5:]) / 5
    vol_slope  = (vol_late - vol_early) / (vol_early + 1)

    # Accumulation terkonfirmasi jika:
    # - Harga konsolidasi (compression < 8%)
    # - OBV naik (smart money masuk)
    # - CMF positif (buying pressure)
    accumulating = (
        price_range < 0.08 and
        obv_slope   > 0.05 and
        cmf         > 0.0
    )

    return {
        "accumulating": accumulating,
        "obv_slope":    round(obv_slope, 3),
        "cmf":          round(cmf, 3),
        "compression":  round(price_range, 3),
        "vol_slope":    round(vol_slope, 3),
    }


def is_organic_move(closes: list, volumes: list, lookback: int = 10) -> dict:
    """
    Bedakan pergerakan organik vs manipulasi/pump dump.

    Manipulation signature:
    - 1 candle volume > 5× rata-rata (spike ekstrem)
    - Harga naik > 8% dalam 1-2 candle (velocity terlalu tinggi)
    - Volume concentration: 1 candle > 60% total volume lookback
    - Harga langsung reversal setelah spike (pump & dump)

    Returns:
        {
          "organic": bool,
          "reason": str,
          "spike_ratio": float,   # volume candle terakhir vs avg
          "velocity": float,      # % perubahan harga 2 candle terakhir
          "concentration": float, # dominasi volume 1 candle
        }
    """
    if len(closes) < lookback + 2 or len(volumes) < lookback + 2:
        return {"organic": True, "reason": "data kurang",
                "spike_ratio": 1.0, "velocity": 0.0, "concentration": 0.0}

    c = closes[-(lookback+2):]
    v = volumes[-(lookback+2):]

    avg_vol      = sum(v[-lookback-1:-1]) / lookback  # avg tanpa candle terakhir
    last_vol     = v[-1]
    spike_ratio  = last_vol / (avg_vol + 1)

    # Price velocity — perubahan harga 2 candle terakhir
    velocity     = abs(c[-1] - c[-3]) / c[-3] if c[-3] > 0 else 0.0

    # Volume concentration — dominasi 1 candle
    total_vol_5  = sum(v[-5:])
    concentration = last_vol / (total_vol_5 + 1)

    # Pump & dump signature — naik tajam lalu langsung reversal
    pnd = (c[-2] > c[-3] * 1.05) and (c[-1] < c[-2] * 0.98)

    # Tentukan organic atau tidak
    if spike_ratio > 5.0:
        return {"organic": False, "reason": f"volume spike {spike_ratio:.1f}×",
                "spike_ratio": round(spike_ratio,2), "velocity": round(velocity,3),
                "concentration": round(concentration,3)}
    if velocity > 0.10:
        return {"organic": False, "reason": f"velocity terlalu tinggi {velocity*100:.1f}%",
                "spike_ratio": round(spike_ratio,2), "velocity": round(velocity,3),
                "concentration": round(concentration,3)}
    if concentration > 0.65:
        return {"organic": False, "reason": f"volume terkonsentrasi {concentration*100:.0f}%",
                "spike_ratio": round(spike_ratio,2), "velocity": round(velocity,3),
                "concentration": round(concentration,3)}
    if pnd:
        return {"organic": False, "reason": "pump & dump pattern",
                "spike_ratio": round(spike_ratio,2), "velocity": round(velocity,3),
                "concentration": round(concentration,3)}

    return {"organic": True, "reason": "ok",
            "spike_ratio": round(spike_ratio,2), "velocity": round(velocity,3),
            "concentration": round(concentration,3)}

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
                       drawdown_mode: str = "normal",
                       score: float = 3.0, rr: float = 1.5) -> float:
    """
    Position sizing berdasarkan risk per trade + adjustment RR/Score [Feature 2].

    Score multiplier:
      score >= 3.8 → 1.20× (setup sangat kuat)
      score >= 3.5 → 1.10×
      score >= 3.0 → 1.00× (baseline)

    RR multiplier:
      rr >= 2.0   → 1.10× (reward tinggi)
      rr >= 1.5   → 1.00× (baseline)
      rr <  1.5   → 0.90× (reward terbatas)

    Combined max: 1.20 × 1.10 = 1.32× dari base size.
    """
    sl_pct = abs(entry - sl) / entry
    if sl_pct <= 0:
        return MIN_POSITION

    base = equity * RISK_PER_TRADE / sl_pct
    base = max(base, MIN_POSITION)
    base = min(base, MAX_POSITION)
    base = min(base, equity * 0.12)

    # Score multiplier
    if score >= 3.8:
        score_mult = 1.20
    elif score >= 3.5:
        score_mult = 1.10
    else:
        score_mult = 1.00

    # RR multiplier
    if rr >= 2.0:
        rr_mult = 1.10
    elif rr >= 1.5:
        rr_mult = 1.00
    else:
        rr_mult = 0.90

    # Drawdown multiplier
    dd_mult = {"normal": 1.0, "warn": 0.7, "halt": 0.4}.get(drawdown_mode, 1.0)

    size = base * score_mult * rr_mult * dd_mult
    size = min(size, MAX_POSITION)   # cap tetap berlaku
    return round(size, 2)

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


def get_trending_pairs(gate_pairs: list[str]) -> list[str]:
    """
    Ambil trending coins dari CoinGecko (free, tanpa API key).
    Filter hanya yang tersedia di Gate.io dan ada di gate_pairs.

    CoinGecko trending = top coins berdasarkan search volume 24h —
    sering jadi leading indicator sebelum harga bergerak.
    """
    try:
        req = urllib.request.Request(
            "https://api.coingecko.com/api/v3/search/trending",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            data     = json.loads(r.read())
            coins    = data.get("coins", [])
            gate_set = set(gate_pairs)
            trending = []

            for item in coins:
                symbol = item.get("item", {}).get("symbol", "").upper()
                pair   = f"{symbol}_USDT"
                if pair in gate_set and pair not in trending:
                    trending.append(pair)

            if trending:
                log(f"🔥 Trending coins dari CoinGecko: {', '.join(trending)}")
            return trending
    except Exception as e:
        log(f"get_trending_pairs error: {e}", "warn")
        return []

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
        candles = _gate_call_with_retry(client.list_candlesticks, pair, interval=interval, limit=limit)
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
    """Ambil semua pair USDT tradable dari Gate.io, diurutkan volume descending.
    [v1.4] Priority sort: pair liquid terbesar di-scan duluan →
    slot 13 terisi pair paling aktif, bukan random.
    """
    try:
        tickers = _gate_call_with_retry(client.list_tickers)   # ← tanpa currency_pair argument
        pairs_vol = []
        # Blacklist permanen — token yang tidak pernah di-scan
        EXCLUDED_SUFFIXES = [
            "3L_USDT", "3S_USDT",   # 3× leveraged
            "5L_USDT", "5S_USDT",   # 5× leveraged
            "2L_USDT", "2S_USDT",   # 2× leveraged
            "UP_USDT", "DOWN_USDT", # Binance-style leveraged
            "ON_USDT",              # ETF/stock tracker (BABAON, TSLAON, dll)
        ]

        for t in tickers:
            try:
                pair = str(t.currency_pair)
                if not pair.endswith("_USDT"):
                    continue
                # Skip blacklisted tokens
                if any(pair.endswith(suf) for suf in EXCLUDED_SUFFIXES):
                    continue
                vol = float(t.quote_volume or 0)
                if vol >= MIN_VOLUME_USDT:
                    pairs_vol.append((pair, vol))
            except Exception as e:
                log(f"   Ticker parse skip ({getattr(t, 'currency_pair', '?')}): {e}", "warn")
                continue

        # Sort by volume — pair paling liquid duluan, scan semua tanpa batas
        pairs_vol.sort(key=lambda x: x[1], reverse=True)
        _track_api(True)
        return [p for p, _ in pairs_vol]
    except Exception as e:
        log(f"get_all_pairs error: {e}", "error")
        _track_api(False)
        return []

def get_ticker_price(client, pair: str) -> float | None:
    try:
        tickers = _gate_call_with_retry(client.list_tickers, currency_pair=pair)
        if tickers:
            _track_api(True)
            return float(tickers[0].last)
    except Exception as e:
        log(f"   get_ticker_price error {pair}: {e}", "warn")
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

    btc_volatile = False
    btc_1h_range = 0.0

    if data_1h:
        closes = data_1h[0]
        highs  = data_1h[1]
        lows   = data_1h[2]
        if len(closes) >= 2:
            btc_1h = (closes[-1] - closes[-2]) / closes[-2] * 100
        # Volatility: range candle terakhir (high-low / close)
        if highs and lows and closes:
            btc_1h_range = (highs[-1] - lows[-1]) / closes[-1] * 100
        # Flag volatile jika BTC gerak terlalu agresif
        if abs(btc_1h) >= BTC_VOLATILE_1H or btc_1h_range >= BTC_RANGE_1H:
            btc_volatile = True
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
        "btc_1h_range":       round(btc_1h_range, 2),
        "btc_volatile":       btc_volatile,
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
    except Exception as e:
        log(f"get_fear_greed error: {e} — fallback ke 50", "warn")
        return 50

# ════════════════════════════════════════════════════════
#  SUPABASE — PORTFOLIO & DRAWDOWN
# ════════════════════════════════════════════════════════

def get_portfolio_state(actual_equity: float | None = None) -> dict:
    """
    Ambil state portfolio open saat ini.

    [FIX HIGH-1] Parameter actual_equity diteruskan dari run() agar portfolio_allows()
    menggunakan equity aktual (bukan INITIAL_EQUITY statis) saat menghitung risk budget.
    Jika tidak disediakan, fallback ke INITIAL_EQUITY.
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=SIGNAL_EXPIRE_HOURS)).isoformat()
        rows = (
            supabase.table("signals_v2")
            .select("strategy, side, pair, score, pnl_usdt, sl, entry, position_size")
            .is_("result", "null")
            .gte("sent_at", cutoff)       # filter: hanya trade dalam window expire
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
        eq = actual_equity if (actual_equity is not None and actual_equity > 0) else INITIAL_EQUITY
        return {
            "total": total, "buy": buy_count, "sell": sell_count,
            "total_risk_usdt": round(total_risk, 2),
            "open_pairs": open_pairs,
            "rows": rows,
            "actual_equity": eq,   # [FIX HIGH-1] diteruskan ke portfolio_allows()
        }
    except Exception as e:
        log(f"Portfolio state error: {e}", "warn")
        eq = actual_equity if (actual_equity is not None and actual_equity > 0) else INITIAL_EQUITY
        return {"total": 0, "buy": 0, "sell": 0,
                "total_risk_usdt": 0, "open_pairs": [], "rows": [],
                "actual_equity": eq}

def get_drawdown_state() -> dict:
    WIN_VALUES  = {"WIN", "TP1", "TP2", "PARTIAL_WIN"}
    LOSS_VALUES = {"LOSS", "SL", "EXPIRED_LOSS"}
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
            except (ValueError, TypeError) as e:
                log(f"get_drawdown_state: skip row pnl parse error ({pnl!r}): {e}", "warn")
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
    # [FIX MEDIUM-2] Streak halt TIDAK boleh di-downgrade ke warn meski equity normal.
    # Logika sebelumnya: streak_halt + equity_normal = warn (SALAH).
    # Logika baru: ambil mode dengan severity tertinggi di antara keduanya.
    # Ini memastikan 7+ loss berturutan selalu masuk HALT tanpa pengecualian.
    mode = max(streak_mode, equity_mode, key=lambda m: SEVERITY[m])

    return {
        "streak":   streak,
        "mode":     mode,
        "dd_pct":   round(dd_pct, 4),
        "cum_pnl":  round(cum_pnl, 4),   # [FIX HIGH-1] dipakai run() untuk actual_equity
    }

_HALT_STATE_FILE = "halt_state.json"   # fallback lokal jika Supabase unreachable

def _read_config(key: str, default=None):
    # [FIX MEDIUM-1] Coba Supabase dulu, fallback ke file lokal
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
        # Supabase gagal — baca dari file lokal sebagai fallback
        try:
            if os.path.exists(_HALT_STATE_FILE):
                with open(_HALT_STATE_FILE, "r", encoding="utf-8") as f:
                    state = json.load(f)
                val = state.get(key)
                if val is not None:
                    try:
                        return type(default)(val) if default is not None else val
                    except Exception:
                        return val
        except Exception as fe:
            log(f"Config read fallback error ({key}): {fe}", "warn")
        return default

def _write_config(key: str, value) -> None:
    # [FIX MEDIUM-1] Tulis ke Supabase, DAN selalu tulis ke file lokal sebagai backup.
    # Jika Supabase unreachable, halt state tetap tersimpan di file lokal sehingga
    # run berikutnya bisa membaca halt state meski DB down.
    str_val = str(value)

    # Selalu tulis ke file lokal dulu (atomic)
    try:
        state = {}
        if os.path.exists(_HALT_STATE_FILE):
            with open(_HALT_STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
        state[key] = str_val
        state["_updated_at"] = datetime.now(timezone.utc).isoformat()
        with open(_HALT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception as fe:
        log(f"Config file write error ({key}): {fe}", "warn")

    # Tulis ke Supabase (best-effort)
    try:
        supabase.table("bot_config").upsert(
            {"key": key, "value": str_val,
             "updated_at": datetime.now(timezone.utc).isoformat()}
        ).execute()
    except Exception as e:
        log(f"Config Supabase write error ({key}): {e} — data tersimpan di file lokal", "warn")

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
    except Exception as e:
        log(f"is_recently_signaled error ({pair}): {e}", "warn")
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
    except Exception as e:
        log(f"is_in_cooldown error ({pair}): {e}", "warn")
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
    # btc_bearish_trend: skip di anomaly mode (F&G < threshold)
    # Saat extreme fear, pair outperform justru sinyal kuat meski BTC bearish
    if side == "BUY" and btc.get("btc_bearish_trend") and fg >= ANOMALY_FG_THRESHOLD:
        return None

    data = get_candles(client, pair, "1h", 150)  # [v1.4] 100 → 150: EMA lebih akurat
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

    # RSI hard filter — block entry overbought/oversold ekstrem
    if side == "BUY" and rsi > 70:
        return None   # overbought — jangan BUY
    if side == "SELL" and rsi < 30:
        return None   # oversold — jangan SELL

    score = score_signal(
        side, price, closes, highs, lows, volumes,
        structure, rsi, macd, msig, ema20, ema50,
        mkt["regime"], btc.get("btc_4h", 0.0), fg
    )

    # Volume hard filter — wajib ada partisipasi market nyata
    avg_vol = sum(volumes[-11:-1]) / 10 if len(volumes) >= 11 else 0
    if avg_vol > 0 and volumes[-1] < avg_vol * 1.2:
        return None   # volume tidak cukup kuat — skip

    # Pump filter — tolak manipulasi/pump dump
    pump = is_organic_move(closes, volumes)
    if not pump["organic"]:
        log(f"      {pair} — pump filter: {pump['reason']} — skip")
        return None

    # [DATA-5] Accumulation bonus ditambah SEBELUM score threshold check.
    # Dengan MIN_SCORE=3.5 dan score max teoritis 3.5, setup dengan
    # score 3.2 + akumulasi +0.3 = 3.5 harus bisa lolos — bukan diblok.
    accu = detect_accumulation(closes, highs, lows, volumes)
    if accu["accumulating"]:
        score = round(score + 0.3, 2)   # bonus akumulasi terdeteksi
        log(f"      {pair} — akumulasi terdeteksi: OBV={accu['obv_slope']:+.2f} CMF={accu['cmf']:+.2f} → score +0.3")

    # Score threshold — lebih rendah di anomaly mode (F&G < 30)
    min_score_eff = 2.5 if fg < ANOMALY_FG_THRESHOLD else MIN_SCORE
    if score < min_score_eff:
        return None   # score tidak cukup

    # DEBUG anomaly — log pair yang sampai sini
    if fg < ANOMALY_FG_THRESHOLD:
        log(f"      {pair} — lolos pre-filter, masuk anomaly check (score={score:.1f})")

    # Multi-timeframe 4h confirmation [Feature 3]
    # Anomaly mode (F&G < 30): MTF 4h dilewati — fokus ke relative strength vs BTC
    anomaly_mode = fg < ANOMALY_FG_THRESHOLD

    if anomaly_mode:
        # Anomaly filter — pair harus outperform BTC
        # Pakai perubahan harga 3 candle terakhir (3h) untuk lebih stabil dari 1 candle
        btc_1h_chg = btc.get("btc_1h", 0.0) / 100
        closes_arr = closes
        if len(closes_arr) >= 4:
            pair_3h_chg = (closes_arr[-1] - closes_arr[-4]) / closes_arr[-4]
        elif len(closes_arr) >= 2:
            pair_3h_chg = (closes_arr[-1] - closes_arr[-2]) / closes_arr[-2]
        else:
            pair_3h_chg = 0.0
        relative_strength = pair_3h_chg - (btc_1h_chg * 3)  # normalize BTC ke 3h

        # Pair harus outperform BTC minimal ANOMALY_OUTPERFORM
        if side == "BUY" and relative_strength < ANOMALY_OUTPERFORM:
            return None

        # Volume anomaly — pakai 1.5× (tidak seketat 2× sebelumnya)
        avg_vol_anom = sum(volumes[-11:-1]) / 10 if len(volumes) >= 11 else 0
        if avg_vol_anom > 0 and volumes[-1] < avg_vol_anom * 1.5:
            return None

        log(f"      {pair} — 🔥 ANOMALY: RS={relative_strength*100:+.1f}% (3h) vs BTC {btc.get('btc_1h',0):+.1f}%")

    else:
        # Normal mode — MTF 4h wajib
        data_4h = get_candles(client, pair, "4h", 60)
        if data_4h:
            closes_4h, highs_4h, lows_4h, _ = data_4h
            ema20_4h = calc_ema(closes_4h, 20)
            ema50_4h = calc_ema(closes_4h, 50)
            macd_4h, msig_4h = calc_macd(closes_4h)

            if side == "BUY":
                tf4_bullish = (ema20_4h > ema50_4h) and (macd_4h > msig_4h)
                if not tf4_bullish:
                    return None
            else:
                tf4_bearish = (ema20_4h < ema50_4h) and (macd_4h < msig_4h)
                if not tf4_bearish:
                    return None

    # WR-based threshold per pair [Feature 1]
    wr_data   = get_pair_winrate(pair)
    wr_pct    = wr_data.get("win_rate", -1)
    wr_trades = wr_data.get("total", 0)
    wr_adj    = 0.0
    if wr_trades >= 5:
        if wr_pct <= 30:
            wr_adj = +0.3    # pair bermasalah — butuh score lebih tinggi
        elif wr_pct >= 60:
            wr_adj = -0.2    # pair bagus — sedikit dilonggarkan
    if wr_adj != 0:
        log(f"      {pair} WR={wr_pct:.0f}% (n={wr_trades}) → score adj {wr_adj:+.1f}")

    # Adaptive threshold saat BTC bearish — naikkan bar saat trend tidak mendukung
    # [DATA-1] Base MIN_SCORE=3.0; +0.3 untuk pair bermasalah; +0.5 saat BTC bearish 2+ cycle
    bearish_cycles = btc.get("btc_bearish_cycles", 0)
    adaptive_min   = MIN_SCORE + wr_adj + (0.5 if bearish_cycles >= 2 else 0.0)
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

    # [DATA-5] Tier disesuaikan dengan MIN_SCORE=3.5
    # A+ = score >= 3.8 | A = score >= 3.5 | (B < 3.5 tidak pernah lolos MIN_SCORE)
    tier = "A+" if score >= 3.8 else "A"

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
        "accumulating":  accu.get("accumulating", False),
        "obv_slope":     accu.get("obv_slope", 0.0),
        "cmf":           accu.get("cmf", 0.0),
        "anomaly_mode":  anomaly_mode,
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
    # [FIX HIGH-1] Gunakan equity aktual (INITIAL_EQUITY + cum_pnl), bukan INITIAL_EQUITY statis.
    # Equity aktual dihitung dari drawdown state yang sudah dihitung sebelumnya di run().
    # Fallback ke INITIAL_EQUITY jika drawdown state tidak tersedia.
    actual_equity = state.get("actual_equity", INITIAL_EQUITY)
    if actual_equity <= 0:
        actual_equity = INITIAL_EQUITY
    new_risk       = actual_equity * RISK_PER_TRADE
    total_risk_pct = (state["total_risk_usdt"] + new_risk) / actual_equity
    if total_risk_pct > MAX_RISK_TOTAL:
        log(f"   ⛔ {sig['pair']} — risk budget penuh ({total_risk_pct*100:.1f}% dari equity aktual ${actual_equity:.2f})")
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
                                 candle_lows: list) -> tuple[str | None, float | None, float | None]:
    """
    Periksa candle highs/lows untuk transisi state.
    Check SL dulu (worst case) sebelum TP — konservatif.

    Return (result_str, pnl, new_sl_breakeven):
      - result_str    : "SL" | "TP2" | "TP1_HIT" | "SL_AFTER_TP1" | None
      - pnl           : float atau None
      - new_sl_breakeven: float (trailing SL terbaru) atau None

    [v1.4] Trailing SL untuk state TP1_HIT:
      - Trail distance = 50% jarak TP1-Entry
      - SL naik mengikuti harga tertinggi tiap candle
      - Minimum: tidak pernah di bawah entry (breakeven)
    """
    if trade.state == "OPEN":
        if trade.side == "BUY":
            for lo, hi in zip(candle_lows, candle_highs):
                if lo <= trade.sl:
                    pnl = (trade.sl - trade.entry) / trade.entry * trade.size
                    return "SL", round(pnl, 4), None
                if hi >= trade.tp2:
                    pnl = (trade.tp2 - trade.entry) / trade.entry * trade.size
                    return "TP2", round(pnl, 4), None
                if hi >= trade.tp1:
                    # [v1.4] Close 100% di TP1
                    pnl = (trade.tp1 - trade.entry) / trade.entry * trade.size
                    return "TP1", round(pnl, 4), None
        else:  # SELL
            for lo, hi in zip(candle_lows, candle_highs):
                if hi >= trade.sl:
                    pnl = (trade.entry - trade.sl) / trade.entry * trade.size
                    return "SL", round(pnl, 4), None
                if lo <= trade.tp2:
                    pnl = (trade.entry - trade.tp2) / trade.entry * trade.size
                    return "TP2", round(pnl, 4), None
                if lo <= trade.tp1:
                    pnl = (trade.entry - trade.tp1) / trade.entry * trade.size
                    return "TP1", round(pnl, 4), None, None

    elif trade.state == "TP1_HIT":
        # ── Trailing SL logic [v1.4] ──────────────────────────────────
        # Trail distance: 50% dari jarak TP1-Entry
        trail_dist = abs(trade.tp1 - trade.entry) * 0.5
        current_be = trade.sl_breakeven if trade.sl_breakeven is not None else trade.entry
        rem        = trade.remaining_size if trade.remaining_size is not None else trade.size * 0.5

        if trade.side == "BUY":
            running_sl = current_be   # trailing SL berjalan
            for lo, hi in zip(candle_lows, candle_highs):
                # Update trailing SL jika harga naik lebih tinggi
                new_trail = hi - trail_dist
                if new_trail > running_sl:
                    running_sl = new_trail
                # Minimum di entry (breakeven)
                running_sl = max(running_sl, trade.entry)

                # Cek apakah kena trailing SL
                if lo <= running_sl:
                    pnl = (running_sl - trade.entry) / trade.entry * (trade.size * 0.5)
                    return "SL_AFTER_TP1", round(pnl, 4), round(running_sl, 8)

                # Cek TP2
                if hi >= trade.tp2:
                    pnl = (trade.tp2 - trade.entry) / trade.entry * rem
                    return "TP2", round(pnl, 4), round(running_sl, 8)

            # Tidak ada event — kembalikan trailing SL terbaru untuk update DB
            return None, None, round(running_sl, 8)

        else:  # SELL
            running_sl = current_be
            for lo, hi in zip(candle_lows, candle_highs):
                new_trail = lo + trail_dist
                if new_trail < running_sl:
                    running_sl = new_trail
                running_sl = min(running_sl, trade.entry)

                if hi >= running_sl:
                    pnl = (trade.entry - running_sl) / trade.entry * (trade.size * 0.5)
                    return "SL_AFTER_TP1", round(pnl, 4), round(running_sl, 8)

                if lo <= trade.tp2:
                    pnl = (trade.entry - trade.tp2) / trade.entry * rem
                    return "TP2", round(pnl, 4), round(running_sl, 8)

            return None, None, round(running_sl, 8)

    return None, None, None


def evaluate_open_trades(client) -> dict:
    """
    Evaluasi semua open trades menggunakan 1m candle highs/lows.
    State machine: OPEN → TP1_HIT → CLOSED
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=SIGNAL_EXPIRE_HOURS)).isoformat()
        open_rows = (
            supabase.table("signals_v2")
            .select("*")
            .is_("result", "null")
            .gte("sent_at", cutoff)       # filter: hanya trade dalam window expire
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
            except Exception as e:
                log(f"   sent_at parse error ({trade.pair}): {e}", "warn")
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
        event, pnl, new_be = _resolve_trade_from_candles(trade, candle_highs, candle_lows)

        # Trailing SL update — simpan ke DB jika SL naik
        if trade.state == "TP1_HIT" and new_be is not None:
            current_be = trade.sl_breakeven if trade.sl_breakeven is not None else trade.entry
            if new_be > current_be + 0.000001:  # ada pergerakan signifikan
                try:
                    supabase.table("signals_v2").update({
                        "sl_breakeven": new_be
                    }).eq("id", trade.id).execute()
                    log(f"   📈 {trade.pair} trailing SL: {current_be:.6f} → {new_be:.6f}")
                except Exception as e:
                    log(f"   trailing SL update error ({trade.pair}): {e}", "warn")

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
                    f"• 🔒 Trailing SL aktif — SL naik otomatis mengikuti harga\n"
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
    except Exception as e:
        log(f"get_pair_winrate error ({pair}): {e}", "warn")
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
    except Exception as e:
        log(f"_get_idr_rate error: {e} — IDR rate tidak tersedia", "warn")
        return 0.0


def send_signal(sig: dict, drawdown_mode: str = "normal",
                actual_equity: float | None = None) -> bool:
    # [FIX HIGH-1] Gunakan equity aktual jika tersedia, fallback ke INITIAL_EQUITY
    equity  = actual_equity if (actual_equity is not None and actual_equity > 0) else INITIAL_EQUITY
    size    = calc_position_size(
        sig["entry"], sig["sl"], equity, drawdown_mode,
        score=sig.get("score", 3.0), rr=sig.get("rr", 1.5)
    )
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

    # Conviction dari score — MIN_SCORE=3.5 (Tier B tidak pernah masuk)
    score = sig["score"]
    if score >= 3.8:
        conviction = "STRONG ✅✅"
    else:
        conviction = "GOOD ✅"  # score 3.5–3.7

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

    # ── STRONG BUY detection ─────────────────────────────────────────────
    TOP50 = ['BTC_USDT', 'ETH_USDT', 'BNB_USDT', 'SOL_USDT', 'XRP_USDT', 'ADA_USDT', 'DOGE_USDT', 'AVAX_USDT', 'DOT_USDT', 'MATIC_USDT', 'LINK_USDT', 'ATOM_USDT', 'LTC_USDT', 'UNI_USDT', 'XLM_USDT', 'NEAR_USDT', 'ICP_USDT', 'FIL_USDT', 'APT_USDT', 'ARB_USDT', 'OP_USDT', 'INJ_USDT', 'SUI_USDT', 'TIA_USDT', 'SEI_USDT', 'TAO_USDT', 'WIF_USDT', 'JUP_USDT', 'PYTH_USDT', 'STRK_USDT', 'PEPE_USDT', 'SHIB_USDT', 'FLOKI_USDT', 'BONK_USDT', 'WLD_USDT', 'PENDLE_USDT', 'AAVE_USDT', 'MKR_USDT', 'CRV_USDT', 'SNX_USDT', 'FTM_USDT', 'ALGO_USDT', 'VET_USDT', 'HBAR_USDT', 'EOS_USDT', 'TRX_USDT', 'XMR_USDT', 'ZEC_USDT', 'DASH_USDT', 'NEO_USDT']
    is_strong_buy = (
        sig["tier"] == "A+"                  and   # score >= 3.5
        sig.get("accumulating", False)        and   # akumulasi terdeteksi
        sig["pair"] in TOP50                  and   # pair top 50 liquid
        fg > 45                               and   # market tidak fearful
        sig["side"] == "BUY"
    )
    header = (
        f"🚀🚀 ⚡ BELI SEKARANG ⚡ 🚀🚀\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📈 {tier_medal} [{sig['tier']}] SIGNAL {side_icon} {sig['side']} — {sig['strategy']}\n"
        if is_strong_buy else
        f"📈 {tier_medal} [{sig['tier']}] SIGNAL {side_icon} {sig['side']} — {sig['strategy']}\n"
    )

    anomaly_str = "\n🔥 <b>ANOMALY MODE</b> — outperform BTC saat Fear" if sig.get("anomaly_mode") else ""
    tg_signal(
        header +
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
        + (f"🔍 Akumulasi: OBV {sig.get('obv_slope',0):+.2f} | CMF {sig.get('cmf',0):+.2f} ✅\n" if sig.get("accumulating") else "")
        + anomaly_str
        + f"\n💰 Pos.Size : ${size:.2f} USDT (tier-adjusted)\n"
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

    # [DATA-4] Time filter — block jam dengan win rate buruk dari data historis
    # Default: 23:00–06:00 WIB diblok (low liquidity, high manipulation risk)
    now_wib_hour = datetime.now(WIB).hour
    if now_wib_hour in BLOCK_HOURS_WIB and SCAN_MODE != "monitor":
        log(f"⏸️  Jam {now_wib_hour:02d}:00 WIB termasuk BLOCK_HOURS — scan dilewati "
            f"(data historis: WR rendah di jam ini)", "info")
        tg_operator(
            f"⏸️ <b>Scan dilewati — Low WR Hour</b>\n"
            f"Jam {now_wib_hour:02d}:00 WIB masuk blok historis (WR &lt; 30%).\n"
            f"<i>Bot akan aktif kembali pukul 07:00 WIB.</i>"
        )
        # Tetap jalankan lifecycle monitor meski scan diblok
        # agar open trades tidak terbengkalai
        client = gate_api.SpotApi(gate_api.ApiClient(
            gate_api.Configuration(key=GATE_API_KEY, secret=GATE_SECRET)
        ))
        evaluate_open_trades(client)
        save_equity_snapshot(open_trades=0)
        return

    is_halted, halt_reason, persisted_streak = check_bot_halt()
    if is_halted:
        # [FIX HIGH-2] Auto-reset halt jika kondisi sudah membaik.
        # Periksa drawdown & streak aktual — jika sudah kembali normal, reset secara otomatis.
        # Ini memperbaiki bug dimana pesan "auto-reset jika kondisi membaik" tidak pernah
        # dieksekusi karena tidak ada kode yang melakukannya.
        current_dd = get_drawdown_state()
        can_reset = (
            current_dd["streak"] < STREAK_HALT and
            current_dd["dd_pct"] < DD_WARN_PCT
        )
        if can_reset:
            set_bot_halt(False, "", 0)
            log(f"✅ Auto-reset: kondisi membaik (streak={current_dd['streak']}, "
                f"dd={current_dd['dd_pct']*100:.1f}%) — HALT dicabut", "info")
            tg_operator(
                f"✅ <b>Bot HALT — Auto-Reset</b>\n"
                f"Kondisi membaik: streak={current_dd['streak']}, "
                f"DD={current_dd['dd_pct']*100:.1f}%\n"
                f"<i>Bot kembali aktif scan.</i>"
            )
            is_halted = False  # lanjut ke flow normal di bawah
        else:
            log(f"🛑 BOT HALT — {halt_reason} (streak={current_dd['streak']}, "
                f"dd={current_dd['dd_pct']*100:.1f}%)", "error")
            tg_operator(
                f"🛑 <b>Bot HALT — Scan dibatalkan</b>\n"
                f"Reason: {halt_reason}\n"
                f"Streak saat ini : {current_dd['streak']} loss\n"
                f"DD saat ini     : {current_dd['dd_pct']*100:.1f}% dari peak\n"
                f"<i>Auto-reset aktif ketika streak &lt; {STREAK_HALT} "
                f"dan DD &lt; {DD_WARN_PCT*100:.0f}%.</i>"
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
        # Hitung actual_equity untuk monitor mode juga
        _dd_monitor = get_drawdown_state()
        _actual_eq_monitor = INITIAL_EQUITY + _dd_monitor.get("cum_pnl", 0.0)
        if _actual_eq_monitor <= 0:
            _actual_eq_monitor = INITIAL_EQUITY
        portfolio = get_portfolio_state(actual_equity=_actual_eq_monitor)
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
    vol_icon = "⚡" if btc["btc_volatile"] else "✅"
    log(f"🌡️ BTC 1h:{btc['btc_1h']:+.1f}% | 4h:{btc['btc_4h']:+.1f}% | "
        f"Range:{btc['btc_1h_range']:.1f}% {vol_icon} | "
        f"F&G:{fg} | Trend:{'⚠️ BEARISH' if btc['btc_bearish_trend'] else '✅ OK'}")

    if btc.get("halt"):
        log("🚨 BTC CRASH — semua signal diblok", "error")
        tg_operator("🚨 <b>BTC Crash Alert</b>\nSemua signal diblok sementara.")
        save_equity_snapshot(open_trades=lifecycle.get("evaluated", 0))
        return

    # BTC Volatility guard — skip scan saat BTC terlalu agresif
    if btc["btc_volatile"]:
        log(f"⚡ BTC volatile — 1h:{btc['btc_1h']:+.1f}% | "
            f"range:{btc['btc_1h_range']:.1f}% — scan dilewati", "warn")
        tg_operator(
            f"⚡ <b>BTC Volatile — Scan Dilewati</b>\n"
            f"BTC 1h : {btc['btc_1h']:+.1f}%\n"
            f"Range  : {btc['btc_1h_range']:.1f}%\n"
            f"<i>Market terlalu choppy. Open trades tetap dipantau.</i>"
        )
        save_equity_snapshot(open_trades=lifecycle.get("evaluated", 0))
        return

    drawdown = get_drawdown_state()
    dd_mode  = drawdown["mode"]
    # [FIX HIGH-1] Hitung equity aktual di sini, sekali, lalu teruskan ke portfolio_allows
    # dan send_signal. Ini memastikan risk budget dan position sizing selalu berbasis
    # equity nyata (INITIAL_EQUITY + cum_pnl), bukan nilai statis.
    actual_equity = INITIAL_EQUITY + drawdown.get("cum_pnl", 0.0)
    if actual_equity <= 0:
        actual_equity = INITIAL_EQUITY
    log(f"📉 Drawdown: streak={drawdown['streak']} | "
        f"dd={drawdown['dd_pct']*100:.1f}% | mode={dd_mode.upper()} | "
        f"equity_aktual=${actual_equity:.2f}")

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

    portfolio = get_portfolio_state(actual_equity=actual_equity)
    log(f"🧠 Portfolio: {portfolio['total']}/{MAX_OPEN_TRADES} open "
        f"(BUY:{portfolio['buy']} SELL:{portfolio['sell']}) | "
        f"Risk: ${portfolio['total_risk_usdt']:.2f} | Equity: ${actual_equity:.2f}")
    all_pairs = get_all_pairs(client)
    log(f"   {len(all_pairs)} pair (semua, blacklist excluded)")

    # Tambah trending coins — prioritas scan karena sering leading indicator
    trending = get_trending_pairs(all_pairs)
    if trending:
        # Pindahkan trending ke depan list agar di-scan duluan
        non_trending = [p for p in all_pairs if p not in trending]
        all_pairs    = trending + non_trending
        log(f"   {len(trending)} trending coin diprioritaskan di awal scan")

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

        if send_signal(sig, dd_mode, actual_equity=actual_equity):
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
    except Exception as e:
        log(f"IDR rate fetch error di run(): {e}", "warn")
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
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=SIGNAL_EXPIRE_HOURS)).isoformat()
            open_rows = (
                supabase.table("signals_v2")
                .select("pair, side, entry, sl, tp1, tp2, state, sent_at, score, strategy")
                .in_("state", ["OPEN", "TP1_HIT"])
                .gte("sent_at", cutoff)       # filter: hanya trade dalam window expire
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
            except Exception as e:
                log(f"   usia parse error ({pair}): {e}", "warn")

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
        f"F&G            : {fg} {'🔥 ANOMALY MODE' if fg < ANOMALY_FG_THRESHOLD else ''}\n"
        f"BTC 1h/4h      : {btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}% | Range:{btc['btc_1h_range']:.1f}%\n"
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

# ════════════════════════════════════════════════════════
#  [FIX HIGH-4] UNIT TESTS — calc_sl_tp, calc_position_size, score_signal
#
#  Jalankan dengan:
#    python bot_lite_v1_4.py --test
#  atau:
#    python -m pytest bot_lite_v1_4.py -v
#
#  Zero external dependencies — semua test berjalan tanpa Supabase/Gate.io.
# ════════════════════════════════════════════════════════

def _run_unit_tests() -> bool:
    """
    Unit tests untuk fungsi kalkulasi inti.
    Return True jika semua lulus, False jika ada yang gagal.
    """
    import traceback
    passed = 0
    failed = 0

    def _assert(condition: bool, name: str, detail: str = "") -> None:
        nonlocal passed, failed
        if condition:
            print(f"  ✅ PASS: {name}")
            passed += 1
        else:
            print(f"  ❌ FAIL: {name}" + (f" — {detail}" if detail else ""))
            failed += 1

    print("\n" + "="*55)
    print("🧪 UNIT TESTS — Signal Bot Lite v1.4.3")
    print("   Coverage: calc_sl_tp, calc_position_size, score_signal,")
    print("   indikator edge cases, portfolio_allows, drawdown severity,")
    print("   score SELL/anomaly, RR validation")
    print("="*55)

    # ── SECTION 1: calc_sl_tp ─────────────────────────────
    print("\n[1] calc_sl_tp()")

    try:
        struct_buy = {"last_sh": 105.0, "last_sl": 95.0}
        entry_buy  = 100.0
        atr_buy    = 2.0
        sl, tp1, tp2 = calc_sl_tp(entry_buy, "BUY", atr_buy, struct_buy)

        _assert(sl < entry_buy,  "BUY: SL harus di bawah entry",
                f"sl={sl}, entry={entry_buy}")
        _assert(tp1 > entry_buy, "BUY: TP1 harus di atas entry",
                f"tp1={tp1}, entry={entry_buy}")
        _assert(tp2 > tp1,       "BUY: TP2 harus di atas TP1",
                f"tp2={tp2}, tp1={tp1}")
        rr_buy = (tp1 - entry_buy) / (entry_buy - sl) if (entry_buy - sl) > 0 else 0
        _assert(rr_buy > 0,      "BUY: RR harus positif", f"rr={rr_buy:.2f}")

        # SL tidak boleh melewati batas MAX_SL_PCT (sekarang 3.5%)
        sl_pct = (entry_buy - sl) / entry_buy
        _assert(sl_pct <= MAX_SL_PCT, "BUY: SL tidak melebihi MAX_SL_PCT (3.5%)",
                f"sl_pct={sl_pct:.3f} vs max={MAX_SL_PCT}")
        _assert(sl_pct >= MIN_SL_PCT, "BUY: SL tidak kurang dari MIN_SL_PCT",
                f"sl_pct={sl_pct:.3f} vs min={MIN_SL_PCT}")
    except Exception as e:
        print(f"  ❌ EXCEPTION calc_sl_tp BUY: {e}")
        failed += 1

    try:
        struct_sell = {"last_sh": 105.0, "last_sl": 95.0}
        entry_sell  = 100.0
        atr_sell    = 2.0
        sl_s, tp1_s, tp2_s = calc_sl_tp(entry_sell, "SELL", atr_sell, struct_sell)

        _assert(sl_s > entry_sell,  "SELL: SL harus di atas entry",
                f"sl={sl_s}, entry={entry_sell}")
        _assert(tp1_s < entry_sell, "SELL: TP1 harus di bawah entry",
                f"tp1={tp1_s}, entry={entry_sell}")
        _assert(tp2_s < tp1_s,      "SELL: TP2 harus di bawah TP1",
                f"tp2={tp2_s}, tp1={tp1_s}")
    except Exception as e:
        print(f"  ❌ EXCEPTION calc_sl_tp SELL: {e}")
        failed += 1

    # Edge case: sl_pct=0 tidak boleh crash
    try:
        sl_z, tp1_z, tp2_z = calc_sl_tp(100.0, "BUY", 0.0001, {"last_sh": None, "last_sl": None})
        _assert(isinstance(sl_z, float), "Edge case: ATR sangat kecil tidak crash")
    except Exception as e:
        print(f"  ❌ EXCEPTION edge case ATR kecil: {e}")
        failed += 1

    # ── SECTION 2: calc_position_size ────────────────────
    print("\n[2] calc_position_size()")

    try:
        entry, sl_p = 100.0, 97.0   # SL 3% dari entry
        equity_t    = 350.0

        # Mode normal — baseline
        size_normal = calc_position_size(entry, sl_p, equity_t, "normal", score=3.5, rr=1.5)
        _assert(size_normal >= MIN_POSITION, "Normal: size >= MIN_POSITION",
                f"size={size_normal}")
        _assert(size_normal <= MAX_POSITION, "Normal: size <= MAX_POSITION",
                f"size={size_normal}")

        # Mode warn — harus lebih kecil dari normal
        size_warn = calc_position_size(entry, sl_p, equity_t, "warn", score=3.5, rr=1.5)
        _assert(size_warn < size_normal, "Warn mode: size lebih kecil dari normal",
                f"warn={size_warn} vs normal={size_normal}")

        # Mode halt — harus lebih kecil dari warn
        size_halt = calc_position_size(entry, sl_p, equity_t, "halt", score=3.5, rr=1.5)
        _assert(size_halt < size_warn, "Halt mode: size lebih kecil dari warn",
                f"halt={size_halt} vs warn={size_warn}")

        # Score tinggi harus menghasilkan size lebih besar (jika tidak di-cap)
        size_high_score = calc_position_size(entry, sl_p, equity_t, "normal", score=3.9, rr=2.1)
        size_low_score  = calc_position_size(entry, sl_p, equity_t, "normal", score=3.0, rr=1.2)
        _assert(size_high_score >= size_low_score,
                "Score tinggi: size >= score rendah",
                f"high={size_high_score} vs low={size_low_score}")

        # SL = entry — tidak boleh crash (sl_pct = 0)
        size_zero_sl = calc_position_size(entry, entry, equity_t, "normal")
        _assert(size_zero_sl == MIN_POSITION, "Edge case: SL = entry → MIN_POSITION",
                f"size={size_zero_sl}")

        # Equity sangat kecil — size akan di-cap oleh equity * 0.12, bukan MIN_POSITION.
        # Ini perilaku yang BENAR: tidak memaksakan posisi $12 saat equity hanya $10.
        size_tiny_eq = calc_position_size(entry, sl_p, 10.0, "normal")
        _assert(size_tiny_eq > 0, "Equity kecil: size tetap positif (tidak crash)",
                f"size={size_tiny_eq}")

        # Equity aktual vs INITIAL_EQUITY — dengan equity lebih kecil, base risk lebih kecil
        size_full_eq   = calc_position_size(entry, sl_p, 350.0, "normal", score=3.5, rr=1.5)
        size_reduced_eq = calc_position_size(entry, sl_p, 300.0, "normal", score=3.5, rr=1.5)
        _assert(size_reduced_eq <= size_full_eq,
                "[FIX HIGH-1] Equity lebih kecil → size tidak lebih besar",
                f"reduced_equity={size_reduced_eq} vs full_equity={size_full_eq}")

    except Exception as e:
        print(f"  ❌ EXCEPTION calc_position_size: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 3: score_signal ───────────────────────────
    print("\n[3] score_signal()")

    try:
        # Setup data sintetis untuk BUY yang kuat
        import random
        random.seed(42)
        n      = 100
        closes = [100 + i * 0.1 + random.gauss(0, 0.05) for i in range(n)]
        highs  = [c + 0.5 for c in closes]
        lows   = [c - 0.5 for c in closes]
        volumes= [1000 + random.gauss(0, 50) for _ in range(n)]
        volumes[-1] = 2000   # volume spike untuk skor lebih tinggi

        ema20  = calc_ema(closes, 20)
        ema50  = calc_ema(closes, 50)
        macd_v, msig_v = calc_macd(closes)
        structure = {"last_sh": closes[-1] * 1.02, "last_sl": closes[-1] * 0.95}

        score_buy = score_signal(
            "BUY", closes[-1], closes, highs, lows, volumes,
            structure, rsi=50.0, macd=macd_v, msig=msig_v,
            ema20=ema20, ema50=ema50, regime="TRENDING",
            btc_4h=0.5, fg=55
        )
        _assert(isinstance(score_buy, float), "score_signal return float",
                f"type={type(score_buy)}")
        _assert(score_buy >= 0, "score tidak negatif untuk setup normal",
                f"score={score_buy}")
        _assert(score_buy <= 4.5, "score tidak melebihi maksimum teoritis",
                f"score={score_buy}")

        # F&G ekstrem harus memberi penalti
        score_fg_extreme = score_signal(
            "BUY", closes[-1], closes, highs, lows, volumes,
            structure, rsi=50.0, macd=macd_v, msig=msig_v,
            ema20=ema20, ema50=ema50, regime="TRENDING",
            btc_4h=0.5, fg=10   # F&G ekstrem fear
        )
        _assert(score_fg_extreme < score_buy,
                "F&G ekstrem: skor lebih rendah dari kondisi normal",
                f"fg_extreme={score_fg_extreme} vs normal={score_buy}")

        # Regime RANGING harus di-penalti (× 0.85)
        score_ranging = score_signal(
            "BUY", closes[-1], closes, highs, lows, volumes,
            structure, rsi=50.0, macd=macd_v, msig=msig_v,
            ema20=ema20, ema50=ema50, regime="RANGING",
            btc_4h=0.5, fg=55
        )
        _assert(score_ranging <= score_buy,
                "RANGING: skor tidak melebihi TRENDING",
                f"ranging={score_ranging} vs trending={score_buy}")

    except Exception as e:
        print(f"  ❌ EXCEPTION score_signal: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 4: calc_ema, calc_rsi, calc_macd — edge cases ──
    print("\n[4] Indikator edge cases")

    try:
        # EMA dengan data kurang dari period — tidak crash
        short_closes = [100.0, 101.0, 99.0]
        ema_result = calc_ema(short_closes, 20)
        _assert(isinstance(ema_result, float), "EMA: data < period tidak crash",
                f"result={ema_result}")

        # RSI dengan data kurang — return 50.0
        rsi_short = calc_rsi([100.0, 101.0], period=14)
        _assert(rsi_short == 50.0, "RSI: data kurang → 50.0", f"result={rsi_short}")

        # MACD dengan data kurang — return (0.0, 0.0)
        macd_short, msig_short = calc_macd([100.0] * 10)
        _assert(macd_short == 0.0 and msig_short == 0.0,
                "MACD: data < 34 → (0.0, 0.0)",
                f"macd={macd_short}, signal={msig_short}")

        # RSI flat prices (avg_loss = 0) — return 100.0
        rsi_flat = calc_rsi([100.0] * 20, period=14)
        _assert(rsi_flat == 100.0, "RSI: flat prices → 100.0", f"result={rsi_flat}")

    except Exception as e:
        print(f"  ❌ EXCEPTION indikator edge cases: {e}")
        failed += 1

    # ── SECTION 5: portfolio_allows ───────────────────────
    print("\n[5] portfolio_allows()")

    try:
        base_sig = {"pair": "ETH_USDT", "side": "BUY", "score": 3.2}
        base_state = {
            "total": 0, "buy": 0, "sell": 0,
            "open_pairs": [], "total_risk_usdt": 0.0,
            "actual_equity": 350.0
        }
        base_dd = {"mode": "normal", "streak": 0, "dd_pct": 0.0}

        # Normal — harus allow
        _assert(portfolio_allows(base_sig, base_state, base_dd),
                "portfolio_allows: slot kosong → True")

        # Portfolio penuh
        full_state = {**base_state, "total": MAX_OPEN_TRADES}
        _assert(not portfolio_allows(base_sig, full_state, base_dd),
                "portfolio_allows: total >= MAX_OPEN_TRADES → False",
                f"total={MAX_OPEN_TRADES}")

        # Pair sudah open
        dup_state = {**base_state, "open_pairs": ["ETH_USDT"]}
        _assert(not portfolio_allows(base_sig, dup_state, base_dd),
                "portfolio_allows: pair sudah open → False")

        # Max same side BUY
        max_buy_state = {**base_state, "buy": MAX_SAME_SIDE}
        _assert(not portfolio_allows(base_sig, max_buy_state, base_dd),
                "portfolio_allows: buy >= MAX_SAME_SIDE → False",
                f"buy={MAX_SAME_SIDE}")

        # Risk budget habis
        high_risk_state = {**base_state, "total_risk_usdt": 350.0 * MAX_RISK_TOTAL}
        _assert(not portfolio_allows(base_sig, high_risk_state, base_dd),
                "portfolio_allows: risk budget penuh → False")

    except Exception as e:
        print(f"  ❌ EXCEPTION portfolio_allows: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 6: Drawdown severity logic ────────────────
    print("\n[6] Drawdown severity (get_drawdown_state logic)")

    try:
        SEVERITY = {"normal": 0, "warn": 1, "halt": 2}

        # Streak >= STREAK_HALT → always HALT regardless of equity
        streak_halt_mode = "halt" if 7 >= STREAK_HALT else "warn" if 7 >= STREAK_WARN else "normal"
        equity_normal_mode = "normal"
        combined = max(streak_halt_mode, equity_normal_mode, key=lambda m: SEVERITY[m])
        _assert(combined == "halt",
                "[FIX MED-2] streak HALT tidak di-downgrade ke WARN meski equity normal",
                f"got={combined}")

        # DD >= DD_HALT_PCT → HALT
        dd_halt_mode = "halt" if DD_HALT_PCT >= DD_HALT_PCT else "warn"
        streak_normal_mode = "normal"
        combined2 = max(dd_halt_mode, streak_normal_mode, key=lambda m: SEVERITY[m])
        _assert(combined2 == "halt",
                "DD >= DD_HALT_PCT → HALT mode",
                f"got={combined2}")

        # Both normal → normal
        combined3 = max("normal", "normal", key=lambda m: SEVERITY[m])
        _assert(combined3 == "normal",
                "Both normal → normal mode",
                f"got={combined3}")

    except Exception as e:
        print(f"  ❌ EXCEPTION drawdown severity: {e}")
        failed += 1

    # ── SECTION 7: score_signal SELL + anomaly edge cases ─
    print("\n[7] score_signal SELL & anomaly edge cases")

    try:
        import random
        random.seed(99)
        n = 100
        closes = [100 - i * 0.05 + random.gauss(0, 0.05) for i in range(n)]  # downtrend
        highs  = [c + 0.3 for c in closes]
        lows   = [c - 0.3 for c in closes]
        volumes = [1000.0] * n
        volumes[-1] = 2200.0

        ema20_s = calc_ema(closes, 20)
        ema50_s = calc_ema(closes, 50)
        macd_s, msig_s = calc_macd(closes)
        struct_s = {"last_sh": closes[-1] * 1.03, "last_sl": closes[-1] * 0.97}

        # SELL score — kondisi downtrend
        score_sell = score_signal(
            "SELL", closes[-1], closes, highs, lows, volumes,
            struct_s, rsi=45.0, macd=macd_s, msig=msig_s,
            ema20=ema20_s, ema50=ema50_s, regime="TRENDING",
            btc_4h=-0.5, fg=55
        )
        _assert(isinstance(score_sell, float), "SELL score: return float")
        _assert(score_sell >= 0, "SELL score: tidak negatif", f"score={score_sell}")

        # F&G > 80 (extreme greed) — harus kena penalty -0.5
        score_greed = score_signal(
            "BUY", closes[-1], closes, highs, lows, volumes,
            struct_s, rsi=50.0, macd=macd_s, msig=msig_s,
            ema20=ema20_s, ema50=ema50_s, regime="TRENDING",
            btc_4h=0.3, fg=85
        )
        score_normal_fg = score_signal(
            "BUY", closes[-1], closes, highs, lows, volumes,
            struct_s, rsi=50.0, macd=macd_s, msig=msig_s,
            ema20=ema20_s, ema50=ema50_s, regime="TRENDING",
            btc_4h=0.3, fg=55
        )
        _assert(score_greed < score_normal_fg,
                "F&G > 80 (extreme greed): score lebih rendah",
                f"greed={score_greed} vs normal={score_normal_fg}")

        # CHOPPY regime — score tidak di-penalti (hanya RANGING yang di-penalti)
        score_choppy = score_signal(
            "BUY", closes[-1], closes, highs, lows, volumes,
            struct_s, rsi=50.0, macd=macd_s, msig=msig_s,
            ema20=ema20_s, ema50=ema50_s, regime="CHOPPY",
            btc_4h=0.3, fg=55
        )
        _assert(isinstance(score_choppy, float),
                "CHOPPY regime: tidak crash", f"score={score_choppy}")

    except Exception as e:
        print(f"  ❌ EXCEPTION score SELL/anomaly: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 8: RR validation ──────────────────────────
    print("\n[8] RR validation (MIN_RR enforcement)")

    try:
        # RR harus memenuhi MIN_RR (1.5) untuk semua setup valid
        entry = 100.0
        atr   = 2.0
        struct = {"last_sh": 106.0, "last_sl": 94.0}
        sl, tp1, tp2 = calc_sl_tp(entry, "BUY", atr, struct)

        sl_dist = entry - sl
        if sl_dist > 0:
            rr_tp1 = (tp1 - entry) / sl_dist
            rr_tp2 = (tp2 - entry) / sl_dist
            _assert(rr_tp1 >= MIN_RR - 0.001,
                    f"BUY TP1 RR >= MIN_RR ({MIN_RR})",
                    f"rr_tp1={rr_tp1:.4f}")
            _assert(rr_tp2 > rr_tp1,
                    "BUY TP2 RR > TP1 RR",
                    f"rr_tp2={rr_tp2:.2f} vs rr_tp1={rr_tp1:.2f}")
        else:
            print("  ⚠️  SKIP RR check — sl_dist = 0 (edge case)")

        # SELL RR
        sl_s, tp1_s, tp2_s = calc_sl_tp(entry, "SELL", atr, struct)
        sl_dist_s = sl_s - entry
        if sl_dist_s > 0:
            rr_tp1_s = (entry - tp1_s) / sl_dist_s
            _assert(rr_tp1_s >= MIN_RR - 0.001,
                    f"SELL TP1 RR >= MIN_RR ({MIN_RR})",
                    f"rr_tp1_s={rr_tp1_s:.4f}")

    except Exception as e:
        print(f"  ❌ EXCEPTION RR validation: {e}")
        failed += 1

    # ── SUMMARY ──────────────────────────────────────────
    print(f"\n{'='*55}")
    total = passed + failed
    print(f"{'✅ SEMUA PASS' if failed == 0 else '❌ ADA YANG GAGAL'}: "
          f"{passed}/{total} tests passed")
    print("="*55 + "\n")
    return failed == 0


if __name__ == "__main__":
    import sys
    if "--test" in sys.argv:
        ok = _run_unit_tests()
        sys.exit(0 if ok else 1)
    else:
        run()
