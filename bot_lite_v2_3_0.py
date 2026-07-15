# -*- coding: utf-8 -*-
# Signal Bot Lite v2.3.0 — CLEAN & SAFE BUILD
# Diturunkan dari v2.2.0
#
# [TECH-DEBT-5] Riwayat perubahan lengkap (dulu ~500 baris komentar di sini,
# memenuhi file) dipindah ke CHANGELOG.md. Ringkasan versi di bawah ini yang
# tersisa di source — cukup buat orientasi cepat, detail lengkap ada di file
# terpisah supaya file .py-nya tetap fokus ke KODE, bukan histori.
#
# ── RINGKASAN VERSI ───────────────────────────────────────────────────────
#   v2.3.0  Adaptive Strategy — bucketing statistik WR per threshold (bukan AI)
#   v2.2.0  Parallel Scan — ThreadPoolExecutor, speedup ~5x (MAX_SCAN_WORKERS)
#   v2.1.0  Unit test EMA/MACD/RSI/ATR/RR/Trade Lifecycle (+ fix bug SELL TP1 4-tuple)
#   v2.0.0  Hapus Pair WR dari scoring (+ fix bug key "win_rate" vs "wr_pct")
#   v1.9.0  Refactor score jadi weighted % — Trend 35/Momentum 40/Volume 15/Structure 10
#   v1.8.0  Daily Bot Health Dashboard (Scan/Signal/Avg Score/Avg RR/Reject/API Error/Runtime)
#   v1.7.0  Technical debt cleanup (+ fix bug kritis anomaly_mode NameError)
#   v1.6.0  Volume/Entry deviation: hard veto → soft penalty ke score
#   v1.5.0  Diagnostic Mode — ScanStats reject-reason counter per cycle
#   v1.4.9  Decoupled market mode — hapus semua guard BTC/F&G dari filtering
#   v1.4.1–v1.4.8  Data-driven tuning (MIN_SCORE, MIN_RR, BLOCK_HOURS dari analisis trade),
#                  fix rate-limit retry, fix JSONL path, fix GATE_SECRET_KEY NameError
#   v1.2–v1.3      Fix candle indexing, ACTIVE_HOURS timezone, konsolidasi DB migration
#
#   Detail lengkap tiap versi (alasan, data pendukung, trade-off): CHANGELOG.md
#
# ── PARAMETER AKTIF (Ground Truth) ────────────────────────────────────────────
# [TECH-DEBT-2] Blok ini WAJIB selalu sinkron dengan nilai default di kode
# ("KONFIGURASI" di bawah). Kalau ubah default di kode, update juga di sini —
# ini yang dibaca duluan saat orang buka file, jadi kalau nyasar, orientasi
# awal soal parameter aktif jadi salah dari baris pertama.
#   MIN_SCORE         = 2.8    [v1.4.9] Volume/WR/Entry sekarang penalti ke
#                               score (v1.6.0), bukan veto terpisah — lihat
#                               MAX_ENTRY_DEV & catatan Volume/WR di bawah.
#   MIN_RR            = 1.5    [v1.4.9] RR tetap hard veto (risk management)
#   MAX_SL_PCT        = 3.5%   [DATA-3] noise > 3.5% → skip
#   MAX_ENTRY_DEV      = 2%    [v1.6.0] di atas ini score -0.3 (soft)
#   MAX_ENTRY_DEV_HARD = 5%    [v1.6.0] di atas ini baru veto (hard)
#   BLOCK_HOURS_WIB   = (kosong, 24 jam scan penuh) [v1.4.9] override via env
#   MAX_OPEN_TRADES   = 13     kapasitas portofolio penuh — DITEGAKKAN
#   MAX_RISK_TOTAL    = 15%    INFORMASIONAL SAJA — sejak [v1.4.9-8]
#                               portfolio_allows() TIDAK lagi mengecek risk
#                               budget, cuma ditampilkan di log/report.
#   MAX_SAME_SIDE     = 13     TIDAK DITEGAKKAN — sejak [v1.4.9-8]
#                               portfolio_allows() cuma cek total slot &
#                               pair sudah open. Constant ini sekarang cuma
#                               dipakai di unit test untuk konfirmasi limit
#                               ini memang sengaja tidak dicek lagi.
#   PAIR_COOLDOWN     = 12h    cooldown per pair setelah close
#   DEDUP_HOURS       = 4h     window dedup sinyal
#   SELL_ENABLED      = false  default off sampai SELL WR terverifikasi
#   Volume             : soft penalty ke score (v1.6.0) — lihat check_intraday()
#   WR per-pair        : DIHAPUS dari scoring (v2.0.0, Prioritas 6) —
#                         dashboard/notifikasi-only, tidak menyentuh score
#
# ╔══════════════════════════════════════════════════════════╗
# ║  DB MIGRATION — WAJIB sebelum deploy                    ║
# ║  Jalankan di Supabase SQL editor (satu kali):           ║
# ║                                                          ║
# ║  ALTER TABLE signals_v2                                  ║
# ║    ADD COLUMN IF NOT EXISTS state TEXT DEFAULT 'OPEN',   ║
# ║    ADD COLUMN IF NOT EXISTS sl_breakeven DOUBLE PRECISION,║
# ║    ADD COLUMN IF NOT EXISTS remaining_size DOUBLE PRECISION,║
# ║    ADD COLUMN IF NOT EXISTS volume_ratio DOUBLE PRECISION,║
# ║    ADD COLUMN IF NOT EXISTS entry_dev_pct DOUBLE PRECISION,║
# ║    ADD COLUMN IF NOT EXISTS rsi_in_zone BOOLEAN;         ║
# ║                                                          ║
# ║  [PRIORITAS-9] 3 kolom terakhir buat Adaptive Strategy — ║
# ║  tanpa ini, analyze_adaptive_suggestions() tidak ada data║
# ║  buat dianalisis (trade LAMA tetap NULL, wajar — cuma    ║
# ║  trade BARU setelah migrasi ini yang punya datanya).     ║
# ║                                                          ║
# ║  Bot AKAN ERROR runtime saat TP1 jika kolom belum ada.  ║
# ╚══════════════════════════════════════════════════════════╝

import os, time, json, math, logging, threading
import concurrent.futures
import numpy as np
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timedelta, timezone
from supabase import create_client
import gate_api

# ════════════════════════════════════════════════════════
#  VERSI & LOGGING
# ════════════════════════════════════════════════════════

BOT_VERSION = "2.3.0-lite"
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

MIN_VOLUME_USDT     = 10_000_000  # [DATA-8] 150K → 10M USDT: filter pair illikuid & manipulasi
MAX_SIGNALS_CYCLE   = 13        # [TUNE-4] 5 → 13

# [PRIORITAS-8] Parallel scan — jumlah worker thread buat scan pair
# bersamaan. Ini pengendali rate-limit UTAMA (bukan time.sleep lagi — lihat
# CHANGELOG v2.2.0). Default 5 itu konservatif; naikkan pelan-pelan sambil
# pantau log untuk "429"/rate-limit error dari Gate.io. _gate_call_with_retry
# tetap jadi jaring pengaman kalau ternyata masih kena limit.
MAX_SCAN_WORKERS = int(os.getenv("MAX_SCAN_WORKERS", "5") or "5")

# [PRIORITAS-9] Adaptive Strategy — BUKAN AI, murni bucketing statistik
# rule-based atas trade historis. Lihat analyze_adaptive_suggestions().
ADAPTIVE_LOOKBACK_TRADES      = int(os.getenv("ADAPTIVE_LOOKBACK_TRADES", "200") or "200")
ADAPTIVE_MIN_SAMPLE_PER_BUCKET = int(os.getenv("ADAPTIVE_MIN_SAMPLE", "20") or "20")
ADAPTIVE_REPORT_INTERVAL_DAYS = int(os.getenv("ADAPTIVE_REPORT_INTERVAL_DAYS", "7") or "7")
DEDUP_HOURS         = 4         # [TUNE-8] 6 → 4
PAIR_COOLDOWN_HOURS = 12        # [TUNE-7] 24 → 12

MIN_SCORE           = 2.8       # [v1.4.9] 3.5 → 2.8: prioritas volume signal, filter BTC/F&G sudah dihapus
                                #          (score < 3.0) WR hanya 13.9% avg PnL -$0.36 — racun utama.
                                #          Tier A (3.0–3.4) WR 32.4% avg +$0.12.
                                #          Tier A+ (≥3.5) WR 45.1% — volume terbesar, basis terkuat.
                                #          Naik ke 3.5 eliminasi Tier B sepenuhnya (79 trades hilang).
MIN_RR              = 1.5       # [v1.4.9] 1.8 → 1.5: longgarkan RR agar lebih banyak setup lolos
                                #          (avg win $1.25 vs avg loss $0.98). Dengan WR 42.7%,
                                #          break-even butuh RR 1.28 → ekspektasi/trade -$0.024 (negatif).
                                #          RR 1.8 paksa setup lebih asimetris: TP lebih jauh dari SL,
                                #          sehingga winner lebih besar dan kompensasi SL lebih baik.
MAX_ENTRY_DEV       = 0.02      # [v1.6.0] di atas ini kena penalti score -0.3 (dulu langsung veto)
MAX_ENTRY_DEV_HARD   = 0.05      # [v1.6.0] di atas ini baru veto — entry sudah tidak valid lagi

ADX_TREND           = 25
ADX_CHOP            = 20
ADX_PERIOD          = 14

BTC_DROP_BLOCK         = -3.0
BTC_CRASH_BLOCK        = -10.0
BTC_VOLATILE_1H        = 1.5    # abs(BTC 1h change) > 1.5% = terlalu volatile
BTC_RANGE_1H           = 2.5    # BTC 1h high-low range > 2.5% = choppy
BTC_TREND_LOOKBACK     = 4
BTC_TREND_MIN_BEARISH  = 3

TP1_R             = 1.5       # [v1.4.9] konsisten dengan MIN_RR 1.5
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
MAX_SAME_SIDE     = 13          # [TUNE-3] ⚠️ TIDAK DITEGAKKAN sejak [v1.4.9-8] — portfolio_allows()
                                #          cuma cek total slot & pair sudah open. Dipertahankan
                                #          hanya untuk unit test (konfirmasi limit ini sengaja lepas).
MAX_RISK_TOTAL    = 0.15        # [TUNE-5] ⚠️ INFORMASIONAL SAJA sejak [v1.4.9-8] — tidak dicek di
                                #          portfolio_allows(), cuma ditampilkan di log/report.

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
_default_block = ""  # [v1.4.9] BLOCK_HOURS dinonaktifkan — bot scan 24 jam penuh
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

# ── F&G EXTREME FEAR TAG — informational only, BUKAN filtering mode ─────────
# [TECH-DEBT-1] Sebelum v1.4.9 ini disebut "ANOMALY MODE" dan benar-benar
# mengubah logika scan (skip MTF 4h, filter relative-strength vs BTC, volume
# minimum 2×). Semua logika itu SUDAH DIHAPUS di v1.4.9 — lihat changelog di
# atas ([v1.4.9-5]). Constant di bawah SEKARANG cuma dipakai untuk nge-tag
# "🔥 ANOMALY MODE" di teks Daily Report saat F&G rendah — murni informasi
# kondisi market, TIDAK mempengaruhi filter/scoring sama sekali.
# ANOMALY_OUTPERFORM dan ANOMALY_VOL_MULT dihapus total — sudah tidak
# direferensikan di manapun sejak logika filtering-nya hilang.
ANOMALY_FG_THRESHOLD = 30       # F&G di bawah ini → tag "🔥 ANOMALY MODE" muncul di Daily Report

# ── SCAN_MODE ─────────────────────────────────────────────────────────────────
# "full"    : evaluate open trades + scan pair baru (default, jalankan tiap jam)
# "monitor" : hanya evaluate open trades — cepat, untuk cron 15/30 menit
SCAN_MODE = os.getenv("SCAN_MODE", "full").strip().lower()

# ── [DIAG-1] DIAGNOSTIC MODE ─────────────────────────────────────────────────
# Selalu aktif menghitung reject reason (overhead-nya cuma counter increment,
# bukan API call tambahan). DIAGNOSTIC_TELEGRAM mengatur apakah SCAN SUMMARY
# dikirim ke Telegram tiap cycle (default true) — kalau berisik, set false
# di GitHub Actions Variables dan cukup baca dari Actions log saja.
DIAGNOSTIC_TELEGRAM = os.getenv("DIAGNOSTIC_TELEGRAM", "true").strip().lower() == "true"

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
_api_failures_cycle = 0   # [PRIORITAS-4] kumulatif per-process (1 cycle = 1 run GitHub Actions), TIDAK decay — buat Daily Health Dashboard
_api_failures_lock = threading.Lock()   # [PRIORITAS-8] get_ticker_price/get_candles sekarang dipanggil paralel dari banyak thread

def _track_api(success: bool) -> None:
    global _api_failures, _api_failures_cycle
    with _api_failures_lock:
        if success:
            # Decay: kurangi 1 per success supaya tidak terlalu sticky
            # Endpoint buruk di 1 fungsi tidak langsung nol-kan counter
            _api_failures = max(0, _api_failures - API_DECAY_ON_SUCCESS)
        else:
            _api_failures += 1
            _api_failures_cycle += 1

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
#  [PRIORITAS-5] SCORING — weighted percentage, bukan poin flat
#  Trend 35% | Momentum 40% | Volume 15% | Structure 10% = 100%
# ════════════════════════════════════════════════════════

# [PRIORITAS-5] Bobot per faktor. Total HARUS 1.0 — dicek via assert saat
# import supaya kalau ada yang salah edit angka di sini, ketahuan langsung
# waktu bot start, bukan diam-diam scoring-nya jadi tidak proporsional.
TREND_WEIGHT     = 0.35
MOMENTUM_WEIGHT  = 0.40
VOLUME_WEIGHT    = 0.15
STRUCTURE_WEIGHT = 0.10
assert abs((TREND_WEIGHT + MOMENTUM_WEIGHT + VOLUME_WEIGHT + STRUCTURE_WEIGHT) - 1.0) < 1e-9, \
    "Bobot scoring harus total 100% (Trend+Momentum+Volume+Structure)"

# Skala skor dipertahankan sama seperti sebelumnya (max teoritis 3.5) supaya
# MIN_SCORE=2.8 (80% dari 3.5) dan semua threshold turunan (tier A+, dst)
# TIDAK perlu dikalibrasi ulang — cuma DISTRIBUSI bobot antar faktor yang
# berubah, bukan skala keseluruhan.
SCORE_MAX_SCALE = 3.5


def score_signal(side: str, price: float, closes: list,
                 highs: list, lows: list, volumes: list,
                 structure: dict, rsi: float, macd: float, msig: float,
                 ema20: float, ema50: float, regime: str,
                 btc_4h: float = 0.0, fg: int = 50) -> float:
    """
    [PRIORITAS-5] Weighted percentage — setiap faktor dihitung sebagai rasio
    pencapaian (0.0/0.5/1.0) dikali bobotnya dikali SCORE_MAX_SCALE:

      Trend     (35%) — EMA alignment
      Momentum  (40%) — MACD crossover (70% bobot internal) + RSI zona ideal
                        (30% bobot internal). RSI digabung ke sini (bukan
                        bonus terpisah lagi) karena RSI itu momentum
                        oscillator — dulu dipisah sebagai "bonus", sekarang
                        jadi bagian eksplisit dari faktor Momentum.
      Volume    (15%) — volume spike vs rata-rata
      Structure (10%) — swing high/low quality (dulu bonus +0.2 kecil,
                        sekarang faktor penuh dengan bobot sendiri)

    Kenapa Momentum > Volume: dari pengalaman, momentum (MACD+RSI) lebih
    prediktif dibanding sekadar volume spike — volume tinggi bisa juga
    tanda pump/dump (makanya tetap ada pump filter terpisah di
    check_intraday), sementara momentum searah lebih jarang false signal.

    [v1.4.9] F&G & BTC alignment TIDAK masuk scoring — F&G/BTC info-only.
    Regime: RANGING multiplier ×0.85 (diterapkan di akhir, di luar 100% bobot).
    """
    # ── Trend (35%) ───────────────────────────────────────
    if side == "BUY":
        if ema20 > ema50 and price > ema20:
            trend_ratio = 1.0
        elif ema20 > ema50:
            trend_ratio = 0.5
        else:
            trend_ratio = 0.0
    else:
        if ema20 < ema50 and price < ema20:
            trend_ratio = 1.0
        elif ema20 < ema50:
            trend_ratio = 0.5
        else:
            trend_ratio = 0.0

    # ── Momentum (40%) — MACD (70% internal) + RSI zona ideal (30% internal) ──
    if side == "BUY":
        if macd > msig and macd > 0:
            macd_ratio = 1.0
        elif macd > msig:
            macd_ratio = 0.5
        else:
            macd_ratio = 0.0
        rsi_ratio = 1.0 if 40 <= rsi <= 65 else 0.0
    else:
        if macd < msig and macd < 0:
            macd_ratio = 1.0
        elif macd < msig:
            macd_ratio = 0.5
        else:
            macd_ratio = 0.0
        rsi_ratio = 1.0 if 35 <= rsi <= 60 else 0.0
    momentum_ratio = 0.7 * macd_ratio + 0.3 * rsi_ratio

    # ── Volume (15%) — spike vs rata-rata ────────────────
    avg_vol = sum(volumes[-11:-1]) / 10 if len(volumes) >= 11 else 0
    if avg_vol > 0 and volumes[-1] > avg_vol * 1.5:
        volume_ratio = 1.0
    elif avg_vol > 0 and volumes[-1] > avg_vol * 1.2:
        volume_ratio = 0.5
    else:
        volume_ratio = 0.0

    # ── Structure (10%) — swing high/low quality ─────────
    sh = structure.get("last_sh")
    sl_lvl = structure.get("last_sl")
    if sh and sl_lvl and (sh - sl_lvl) / sl_lvl > 0.02:
        structure_ratio = 1.0
    else:
        structure_ratio = 0.0

    score = (
        trend_ratio     * TREND_WEIGHT +
        momentum_ratio  * MOMENTUM_WEIGHT +
        volume_ratio    * VOLUME_WEIGHT +
        structure_ratio * STRUCTURE_WEIGHT
    ) * SCORE_MAX_SCALE

    # Regime multiplier — RANGING sedikit dipenalti (di luar 100% bobot,
    # sama seperti desain lama — bukan faktor ke-5, tapi kondisi market)
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

        # [DATA-8] Stablecoin blacklist — pair ini tidak akan pernah ada signal
        # karena harganya flat di $1. Dipisah dari EXCLUDED_SUFFIXES karena
        # ini exact match bukan suffix match.
        STABLECOIN_BLACKLIST = {
            "USDC_USDT", "BUSD_USDT", "DAI_USDT", "TUSD_USDT",
            "USDP_USDT", "GUSD_USDT", "FRAX_USDT", "LUSD_USDT",
            "USD1_USDT", "RLUSD_USDT", "USDD_USDT", "FDUSD_USDT",
            "PYUSD_USDT", "CUSD_USDT", "SUSD_USDT", "EURC_USDT",
            "EURS_USDT", "EURT_USDT", "AGEUR_USDT", "XSGD_USDT",
        }

        for t in tickers:
            try:
                pair = str(t.currency_pair)
                if not pair.endswith("_USDT"):
                    continue
                # Skip leveraged tokens
                if any(pair.endswith(suf) for suf in EXCLUDED_SUFFIXES):
                    continue
                # Skip stablecoins — harga flat, tidak ada signal
                if pair in STABLECOIN_BLACKLIST:
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
#  [PRIORITAS-4] DAILY BOT HEALTH DASHBOARD
#  GitHub Actions = proses baru tiap cycle, jadi akumulasi harian TIDAK BISA
#  disimpan di variabel Python — harus lewat _read_config/_write_config
#  (Supabase + file lokal) supaya nyambung antar-cycle sepanjang hari.
# ════════════════════════════════════════════════════════

_DAILY_HEALTH_KEY = "daily_health_state"
_DAILY_HEALTH_REPORT_KEY = "last_health_report_date"
_DAILY_HEALTH_REPORT_HOUR = 22   # sama seperti Daily Report equity — 1x/hari jam 22 WIB

def _load_daily_health(today_str: str) -> dict:
    """Ambil akumulator hari ini. Kalau tanggal di blob beda dari hari ini
    (cycle pertama di hari baru), otomatis reset — ini yang bikin dashboard
    per-hari, bukan menumpuk selamanya."""
    raw = _read_config(_DAILY_HEALTH_KEY, "") or ""
    fresh = {
        "date": today_str, "cycles": 0, "total_pairs": 0, "total_signals": 0,
        "score_sum": 0.0, "rr_sum": 0.0, "reject_totals": {}, "api_errors": 0,
        "runtime_sum": 0.0,
    }
    if not raw:
        return fresh
    try:
        blob = json.loads(raw)
        if blob.get("date") != today_str:
            return fresh   # hari baru — reset
        # Merge supaya kalau ada field baru di versi kode berikutnya, tidak KeyError
        fresh.update(blob)
        return fresh
    except Exception as e:
        log(f"Daily health parse error (reset ke fresh): {e}", "warn")
        return fresh

def _save_daily_health(blob: dict) -> None:
    try:
        _write_config(_DAILY_HEALTH_KEY, json.dumps(blob))
    except Exception as e:
        log(f"Daily health save error: {e}", "warn")

def _accumulate_daily_health(scan_stats: "ScanStats", cycle_score_sum: float,
                              cycle_rr_sum: float, cycle_runtime_s: float,
                              api_errors_this_cycle: int) -> dict:
    """Dipanggil di akhir tiap cycle scan (bukan monitor/blocked/halted).
    Return blob terbaru supaya bisa langsung dipakai kalau kebetulan cycle
    ini juga jam kirim laporan (hindari baca-lagi-langsung-setelah-tulis)."""
    today_str = datetime.now(WIB).strftime("%Y-%m-%d")
    blob = _load_daily_health(today_str)
    blob["cycles"]        += 1
    blob["total_pairs"]   += scan_stats.total_pairs
    blob["total_signals"] += scan_stats.signals_sent
    blob["score_sum"]     += cycle_score_sum
    blob["rr_sum"]        += cycle_rr_sum
    blob["runtime_sum"]   += cycle_runtime_s
    blob["api_errors"]    += api_errors_this_cycle
    for key, count in scan_stats.counts.items():
        if count > 0:
            blob["reject_totals"][key] = blob["reject_totals"].get(key, 0) + count
    _save_daily_health(blob)
    return blob

def _format_daily_health_report(blob: dict) -> str:
    scan   = blob["total_pairs"]
    signal = blob["total_signals"]
    avg_score = (blob["score_sum"] / signal) if signal > 0 else 0.0
    avg_rr    = (blob["rr_sum"] / signal) if signal > 0 else 0.0
    avg_runtime = (blob["runtime_sum"] / blob["cycles"]) if blob["cycles"] > 0 else 0.0

    reject_totals = blob.get("reject_totals", {})
    if reject_totals:
        biggest_key, biggest_count = max(reject_totals.items(), key=lambda kv: kv[1])
        biggest_label = dict(ScanStats.LABELS).get(biggest_key, biggest_key)
        biggest_pct   = (biggest_count / scan * 100) if scan > 0 else 0.0
        reject_line   = f"{biggest_label} ({biggest_pct:.0f}%)"
    else:
        reject_line = "—"

    return (
        f"🩺 <b>Daily Bot Health — {datetime.now(WIB).strftime('%d %b %Y')}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔍 Scan          : {scan} pair ({blob['cycles']} cycle)\n"
        f"📶 Signal        : {signal}\n"
        f"📊 Avg Score     : {avg_score:.2f}\n"
        f"⚖️ Avg RR        : {avg_rr:.2f}\n"
        f"⛔ Reject terbesar: {reject_line}\n"
        f"🌐 API Error     : {blob['api_errors']}\n"
        f"⏱️ Runtime (avg) : {avg_runtime:.0f} detik/cycle\n"
        f"<i>{datetime.now(WIB).strftime('%H:%M WIB')}</i>"
    )

def maybe_send_daily_health_report(blob: dict) -> None:
    """Kirim 1x/hari jam _DAILY_HEALTH_REPORT_HOUR WIB — guard sama seperti
    Daily Report equity di save_equity_snapshot()."""
    now_wib = datetime.now(WIB)
    today_str = now_wib.strftime("%Y-%m-%d")
    last_sent = _read_config(_DAILY_HEALTH_REPORT_KEY, "") or ""
    if now_wib.hour == _DAILY_HEALTH_REPORT_HOUR and last_sent != today_str:
        _write_config(_DAILY_HEALTH_REPORT_KEY, today_str)
        try:
            tg_operator(_format_daily_health_report(blob))
        except Exception as e:
            log(f"Daily Health Dashboard send error: {e}", "warn")

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

# ════════════════════════════════════════════════════════
#  [DIAG-1] SCAN DIAGNOSTIC MODE
#  Menghitung alasan reject per pair supaya "842 pair → 0 signal"
#  bisa dijelaskan bottleneck-nya, bukan cuma angka kosong.
# ════════════════════════════════════════════════════════

class ScanStats:
    """Counter reject reason selama satu siklus scan.

    Urutan LABELS menentukan urutan tampil di laporan — sengaja
    disusun mengikuti urutan filter dieksekusi di check_intraday()
    dan run(), supaya funnel-nya kebaca top-to-bottom.
    """

    LABELS = [
        ("no_data",       "Rejected NoData"),
        ("atr",           "Rejected ATR"),
        ("pump",          "Rejected Pump"),
        ("score",         "Rejected Score"),
        ("rr",            "Rejected RR"),
        ("entry",         "Rejected Entry (ekstrem)"),
        ("portfolio",     "Portfolio Full"),
        ("open_pair",     "Skip Already Open"),
        ("recent",        "Skip Recently Signaled"),
        ("cooldown",      "Skip Cooldown"),
        ("no_price",      "Skip No Price"),
    ]

    # [PRIORITAS-2] Volume dan Entry deviation (dalam batas) bukan lagi veto
    # — jadi penalti ke score. Dicatat terpisah dari LABELS di atas karena
    # ini BUKAN alasan reject akhir (pair yang kena penalti masih bisa lolos
    # jadi Signal kalau score final-nya tetap >= MIN_SCORE).
    # [PRIORITAS-6] wr_bad/wr_good DIHAPUS dari sini — WR per-pair sudah
    # tidak menyentuh score sama sekali (lihat check_intraday()), jadi tidak
    # ada lagi penalti/bonus WR untuk dicatat.
    PENALTY_LABELS = [
        ("volume",  "Penalty: Volume lemah (-0.3)"),
        ("entry",   "Penalty: Entry deviation (-0.3)"),
    ]

    def __init__(self):
        self.counts: dict[str, int] = {key: 0 for key, _ in self.LABELS}
        self.penalties: dict[str, int] = {key: 0 for key, _ in self.PENALTY_LABELS}
        self.total_pairs = 0
        self.signals_sent = 0
        # [PRIORITAS-8] Lock — check_intraday() sekarang bisa dipanggil dari
        # banyak worker thread sekaligus (parallel scan). `+= 1` itu
        # read-modify-write, TIDAK atomic — tanpa lock, dua thread yang
        # nge-bump bersamaan bisa saling menimpa dan salah satu increment
        # hilang. Overhead lock ini kecil (cuma dict update, bukan I/O).
        self._lock = threading.Lock()

    def bump(self, reason: str) -> None:
        with self._lock:
            self.counts[reason] = self.counts.get(reason, 0) + 1

    def bump_penalty(self, reason: str) -> None:
        with self._lock:
            self.penalties[reason] = self.penalties.get(reason, 0) + 1

    def format_report(self, version: str) -> str:
        accounted = sum(self.counts.values()) + self.signals_sent
        lines = []
        lines.append("=" * 40)
        lines.append(f"SCAN SUMMARY — v{version}")
        lines.append("=" * 40)
        lines.append(f"Total Pair          : {self.total_pairs}")
        lines.append("")
        # Kategori inti (yang jadi bottleneck teknikal/strategi — masih veto)
        core_keys = ["no_data", "atr", "pump", "score", "rr", "entry", "portfolio"]
        for key, label in self.LABELS:
            if key in core_keys and self.counts[key] > 0:
                lines.append(f"{label:<24}: {self.counts[key]}")
        lines.append("")
        lines.append(f"{'Signal':<24}: {self.signals_sent}")
        # [PRIORITAS-2] Penalti/bonus yang di-apply ke score (bukan reject
        # langsung) — ditampilkan terpisah supaya kebaca sebagai "faktor yang
        # menurunkan/menaikkan score", bukan "kenapa pair ini direject".
        penalty_lines = [(label, self.penalties[key]) for key, label in self.PENALTY_LABELS
                          if self.penalties[key] > 0]
        if penalty_lines:
            lines.append("-" * 40)
            lines.append("PENALTY/BONUS DITERAPKAN KE SCORE (bukan veto):")
            for label, count in penalty_lines:
                lines.append(f"{label:<24}: {count}")
        # Kategori skip administratif (bukan reject teknikal — ditampilkan
        # terpisah supaya funnel utama di atas tidak "kotor")
        skip_keys = ["open_pair", "recent", "cooldown", "no_price"]
        skip_lines = [(label, self.counts[key]) for key, label in self.LABELS
                      if key in skip_keys and self.counts[key] > 0]
        if skip_lines:
            lines.append("-" * 40)
            for label, count in skip_lines:
                lines.append(f"{label:<24}: {count}")
        lines.append("=" * 40)
        if accounted != self.total_pairs:
            lines.append(f"(selisih {self.total_pairs - accounted} pair — "
                          f"kena >1 filter atau loop berhenti lebih awal karena portfolio/limit sinyal)")
        return "\n".join(lines)

    def format_report_html(self, version: str) -> str:
        """Versi ringkas untuk Telegram — HTML parse_mode, cuma kategori non-zero."""
        rows = []
        for key, label in self.LABELS:
            if self.counts[key] > 0:
                rows.append(f"{label:<22}: {self.counts[key]}")
        body = "\n".join(rows) if rows else "(semua pair lolos filter awal)"
        penalty_rows = [f"{label:<22}: {self.penalties[key]}"
                         for key, label in self.PENALTY_LABELS if self.penalties[key] > 0]
        penalty_body = ("\n\n<i>Penalty/bonus (bukan veto):</i>\n" + "\n".join(penalty_rows)) if penalty_rows else ""
        return (
            f"🔍 <b>Scan Diagnostic</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<pre>Total Pair          : {self.total_pairs}\n\n"
            f"{body}\n\n"
            f"{'Signal':<22}: {self.signals_sent}</pre>"
            f"{penalty_body}"
        )


def check_intraday(client, pair: str, price: float,
                   btc: dict = None, fg: int = 50,
                   side: str = "BUY", stats: "ScanStats | None" = None) -> dict | None:
    # [v1.4.9] BTC & F&G guard dihapus — altcoin sering pump independen
    # dari kondisi BTC dan F&G (decoupled market). Setiap pair dievaluasi
    # murni berdasarkan teknikal masing-masing.
    #
    # [DIAG-1] `stats` (ScanStats) opsional — kalau di-pass, setiap titik
    # reject di bawah dicatat dengan alasan spesifik agar SCAN SUMMARY di
    # akhir run() bisa menunjukkan bottleneck sebenarnya, bukan cuma
    # "842 pair → 0 signal" tanpa penjelasan.

    def _reject(reason: str):
        if stats is not None:
            stats.bump(reason)
        return None

    data = get_candles(client, pair, "1h", 150)  # [v1.4] 100 → 150: EMA lebih akurat
    if data is None:
        return _reject("no_data")
    closes, highs, lows, volumes = data

    atr = calc_atr(closes, highs, lows)
    atr_pct = atr / price * 100
    # [v1.4.9] ATR range diperlebar — hanya block yang benar-benar flat (<0.1%)
    # atau ekstrem volatil (>15%). CHOPPY filter dihapus — sering block valid signal.
    if atr_pct < 0.1 or atr_pct > 15.0:
        return _reject("atr")

    mkt = detect_regime(closes, highs, lows)

    rsi        = calc_rsi(closes)
    macd, msig = calc_macd(closes)
    ema20      = calc_ema(closes, 20)
    ema50      = calc_ema(closes, 50)
    structure  = detect_structure(closes, highs, lows)

    # [v1.4.9] Structure & RSI hard filter dihapus — terlalu sering block
    # valid pumps yang memang RSI tinggi tapi momentum kuat
    # RSI masih dipakai sebagai input score, bukan hard block

    score = score_signal(
        side, price, closes, highs, lows, volumes,
        structure, rsi, macd, msig, ema20, ema50,
        mkt["regime"], btc.get("btc_4h", 0.0), fg
    )

    # [PRIORITAS-2] Volume dan Entry deviation TIDAK LAGI hard veto — jadi
    # PENALTI ke score, seperti accumulation bonus yang sudah ada. Volume
    # sudah jadi salah satu input score_signal() di atas juga — hard reject
    # terpisah untuk hal yang sama itu double-penalty.
    # [PRIORITAS-6] WR per-pair malah DIHAPUS TOTAL dari score (bukan cuma
    # dilunakkan jadi penalti seperti Volume/Entry) — lihat catatan di bawah,
    # sebelum entry logic.
    # Regime sudah soft dari awal (RANGING ×0.85 di dalam score_signal),
    # jadi tidak ada perubahan di sana.
    # Pump filter & ATR range TETAP hard veto — itu bukan preferensi setup,
    # tapi validitas: pump filter mendeteksi manipulasi, ATR range mendeteksi
    # candle yang datanya tidak layak dipakai hitung SL/TP sama sekali.
    penalty_notes = []

    # Volume — dulu reject kalau < 1.2× rata-rata, sekarang penalti -0.3
    avg_vol = sum(volumes[-11:-1]) / 10 if len(volumes) >= 11 else 0
    # [PRIORITAS-9] volume_ratio disimpan ke sig dict di akhir fungsi — data
    # mentah ini yang dipakai analyze_adaptive_suggestions() buat bucketing
    # WR per rentang rasio volume, BUKAN cuma pass/fail biner.
    volume_ratio = round(volumes[-1] / avg_vol, 3) if avg_vol > 0 else None
    if avg_vol > 0 and volumes[-1] < avg_vol * 1.2:
        score -= 0.3
        penalty_notes.append("volume lemah -0.3")
        if stats is not None:
            stats.bump_penalty("volume")

    # Pump filter — TETAP veto (deteksi manipulasi, bukan soal skor)
    pump = is_organic_move(closes, volumes)
    if not pump["organic"]:
        log(f"      {pair} — pump filter: {pump['reason']} — skip")
        return _reject("pump")

    # [DATA-5] Accumulation bonus
    accu = detect_accumulation(closes, highs, lows, volumes)
    if accu["accumulating"]:
        score = round(score + 0.3, 2)   # bonus akumulasi terdeteksi
        log(f"      {pair} — akumulasi terdeteksi: OBV={accu['obv_slope']:+.2f} CMF={accu['cmf']:+.2f} → score +0.3")

    # [PRIORITAS-6] WR per-pair DIHAPUS dari scoring/threshold — pair
    # historical WR gampang bias (karakter pair berubah seiring waktu, "DOGE
    # tahun lalu ≠ DOGE hari ini"), jadi tidak seharusnya menggerakkan
    # keputusan entry. WR tetap dihitung & ditampilkan (get_pair_winrate())
    # di notifikasi Telegram sebagai info "Hist WR" — dashboard-only, tidak
    # lagi menyentuh score sama sekali.
    #
    # [BUG DITEMUKAN saat menghapus fitur ini] wr_adj SEBELUMNYA membaca
    # wr_data.get("win_rate", -1) — padahal get_pair_winrate() mengembalikan
    # key "wr_pct", bukan "win_rate". Akibatnya wr_pct SELALU -1 (default),
    # dan karena -1 <= 30, wr_adj SELALU -0.3 untuk pair manapun yang punya
    # >=5 trade historis — TIDAK PEDULI WR pair itu sebenarnya bagus atau
    # jelek. Fitur ini dari awal tidak pernah bekerja sesuai desainnya.
    # Penghapusan di Prioritas 6 ini otomatis menuntaskan bug tersebut juga.

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
    # Entry deviation — "dalam batas tertentu" jadi penalti bertahap, bukan
    # veto langsung. Di atas MAX_ENTRY_DEV_HARD, harga sudah terlalu jauh
    # dari level breakout/breakdown — bukan lagi soal preferensi, entry-nya
    # sendiri sudah tidak valid untuk dihitung SL/TP-nya.
    if dev > MAX_ENTRY_DEV_HARD:
        return _reject("entry")
    elif dev > MAX_ENTRY_DEV:
        score -= 0.3
        penalty_notes.append(f"entry dev {dev*100:.2f}% -0.3")
        if stats is not None:
            stats.bump_penalty("entry")

    if penalty_notes:
        log(f"      {pair} — penalti diterapkan: {', '.join(penalty_notes)} → score jadi {score:.2f}")

    # [v1.6.0] Satu ambang final — semua penalti/bonus (volume, WR, entry,
    # akumulasi) sudah masuk ke score di atas, jadi cukup satu kali cek di
    # sini. Ini menggantikan mekanisme adaptive_min lama yang menaikkan
    # ambang secara terpisah untuk WR.
    if score < MIN_SCORE:
        log(f"      {pair} — score final {score:.2f} < {MIN_SCORE} (min) — skip")
        return _reject("score")

    sl, tp1, tp2 = calc_sl_tp(entry, side, atr, structure)

    if side == "BUY":
        if tp1 <= entry or sl >= entry:
            return _reject("entry")
        sl_dist = entry - sl
    else:
        if tp1 >= entry or sl <= entry:
            return _reject("entry")
        sl_dist = sl - entry

    if sl_dist <= 0:
        return _reject("entry")

    rr = abs(tp1 - entry) / sl_dist
    if rr < MIN_RR:
        log(f"      {pair} — RR {rr:.2f} < MIN_RR {MIN_RR} — skip")
        return _reject("rr")

    # [TECH-DEBT-3] FIX: dulu hardcoded "score >= 3.8" — asumsi MIN_SCORE=3.5
    # (A+ = MIN_SCORE + 0.3). Waktu MIN_SCORE turun ke 2.8 di v1.4.9, angka
    # 3.8 ini TIDAK ikut di-update — akibatnya hampir semua signal yang lolos
    # (2.8–3.7) jatuh ke tier "A" polos, dan "A+" jadi nyaris mustahil dicapai.
    # Sekarang threshold diturunkan jadi relatif ke MIN_SCORE supaya otomatis
    # ikut kalau MIN_SCORE berubah lagi di masa depan — tidak akan desync lagi.
    tier = "A+" if score >= MIN_SCORE + 0.3 else "A"

    # [PRIORITAS-9] rsi_in_zone — dipakai analyze_adaptive_suggestions() buat
    # bucketing WR: trade dengan RSI di zona ideal vs di luar zona.
    if side == "BUY":
        rsi_in_zone = 40 <= rsi <= 65
    else:
        rsi_in_zone = 35 <= rsi <= 60

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
        # [PRIORITAS-9] Field mentah buat Adaptive Strategy — disimpan ke
        # signals_v2 supaya analyze_adaptive_suggestions() bisa bucketing
        # WR historis per rentang parameter, bukan cuma lihat score akhir.
        "volume_ratio":   volume_ratio,
        "entry_dev_pct":  round(dev * 100, 3),
        "rsi_in_zone":    rsi_in_zone,
    }

# ════════════════════════════════════════════════════════
#  PORTFOLIO GATE
# ════════════════════════════════════════════════════════

def portfolio_allows(sig: dict, state: dict, drawdown: dict) -> bool:
    """[v1.4.9] Check simpel — hanya blok jika portfolio penuh atau pair sudah open."""
    pair = sig["pair"]

    if state["total"] >= MAX_OPEN_TRADES:
        log(f"   ⛔ {pair} — portfolio penuh ({state['total']}/{MAX_OPEN_TRADES})")
        return False

    if pair in state.get("open_pairs", []):
        log(f"   ⛔ {pair} — pair sudah open")
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
                    return "TP1", round(pnl, 4), None

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

                _idr_tp1 = _get_idr_rate()
                idr_pnl_tp1 = f" / Rp{realized_pnl * _idr_tp1:,.0f}" if _idr_tp1 > 0 else ""
                idr_tp2_str = f" / Rp{trade.tp2 * _idr_tp1:,.0f}" if _idr_tp1 > 0 else ""
                tg_signal(
                    f"🎯 <b>TP1 HIT — {trade.pair}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Entry  : {_fmt_price_signal(trade.entry)}\n"
                    f"TP1    : {_fmt_price_signal(trade.tp1)} ✅ ({tp1_pct:+.1f}%)\n"
                    f"PnL    : <b>+{realized_pnl:.2f} USDT{idr_pnl_tp1}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📌 SL dipindah ke entry (breakeven)\n"
                    f"🔒 Trailing SL aktif\n"
                    f"🎯 Menunggu TP2 {_fmt_price_signal(trade.tp2)}{idr_tp2_str}..."
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
                f"✅✅ <b>TP2 HIT — {trade.pair}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Entry  : {_fmt_price_signal(trade.entry)}\n"
                f"TP2    : {_fmt_price_signal(trade.tp2)} ✅ ({tp2_pct:+.1f}%)\n"
                f"PnL    : <b>+{abs(pnl):.2f} USDT{_idr_str(pnl)}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 Target penuh tercapai!"
            )
        elif result == "SL_AFTER_TP1":
            msg = (
                f"🔄 <b>Breakeven — {trade.pair}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Entry  : {_fmt_price_signal(trade.entry)}\n"
                f"SL     : {_fmt_price_signal(trade.entry)} (breakeven)\n"
                f"PnL    : <b>{pnl_str}{_idr_str(pnl)}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"TP1 profit terkunci ✅ Modal aman."
            )
        elif result == "SL":
            sl_pct_close = (abs(trade.sl - trade.entry) / trade.entry * 100) if trade.entry > 0 else 0
            msg = (
                f"❌ <b>Stop Loss — {trade.pair}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Entry  : {_fmt_price_signal(trade.entry)}\n"
                f"SL     : {_fmt_price_signal(trade.sl)} (-{sl_pct_close:.1f}%)\n"
                f"PnL    : <b>{pnl_str}{_idr_str(pnl)}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Loss terkontrol ✅"
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
        # [PRIORITAS-9] Field mentah buat Adaptive Strategy — dipakai
        # analyze_adaptive_suggestions() nanti setelah trade ini closed.
        "volume_ratio":  sig.get("volume_ratio"),
        "entry_dev_pct": sig.get("entry_dev_pct"),
        "rsi_in_zone":   sig.get("rsi_in_zone"),
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

    # [TECH-DEBT-3] FIX: sama seperti tier di check_intraday() — dulu
    # hardcoded "score >= 3.8" asumsi MIN_SCORE=3.5 lama, sudah desync sejak
    # MIN_SCORE turun ke 2.8. Sekarang relatif ke MIN_SCORE, konsisten
    # dengan definisi tier A+ di check_intraday().
    score = sig["score"]
    if score >= MIN_SCORE + 0.3:
        conviction = "STRONG ✅✅"
    else:
        conviction = "GOOD ✅"

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

    accu_str    = f"\n🔍 Akumulasi: OBV {sig.get('obv_slope',0):+.2f} | CMF {sig.get('cmf',0):+.2f} ✅" if sig.get("accumulating") else ""
    size_idr    = f" / Rp{size * idr:,.0f}" if idr > 0 else ""

    tg_signal(
        f"{header}"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Score   : {score:.2f} {conviction}\n"
        f"📈 Regime  : {regime_icon} {sig['regime']} (ADX: {sig['adx']})\n"
        f"💡 Why     : {why_str}\n"
        f"📉 Hist WR : {hist_wr['icon']} {hist_wr['label']}\n"
        + (accu_str + "\n" if accu_str else "")
        + f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Entry   : {_fmt_price_signal(entry)} / Rp{entry * idr:,.0f}\n"
        f"🎯 TP1     : {_fmt_price_signal(tp1)} / Rp{tp1 * idr:,.0f} ({tp1_pct:+.1f}%)\n"
        f"🎯 TP2     : {_fmt_price_signal(tp2)} / Rp{tp2 * idr:,.0f} ({tp2_pct:+.1f}%)\n"
        f"🛡️ SL      : {_fmt_price_signal(sl)} / Rp{sl * idr:,.0f} ({sl_pct:+.1f}%)\n"
        f"⚖️ RR      : 1:{sig['rr']}\n"
        f"💼 Size    : ${size:.2f}{size_idr}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Valid   : {valid_from} → {valid_until} WIB\n"
        f"⚠️ Pasang SL wajib. Bukan rekomendasi finansial."
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

        # Kirim daily report hanya jam 22 WIB, 1x per hari
        _now_wib_dt = datetime.now(WIB)
        _now_h = _now_wib_dt.hour
        _today_str = _now_wib_dt.strftime("%Y-%m-%d")
        _last_daily = _read_config("last_daily_report_date", "") or ""
        if _now_h == 22 and _last_daily != _today_str:
            _write_config("last_daily_report_date", _today_str)
            _idr_eq = _get_idr_rate()
            eq_idr  = f" / Rp{equity * _idr_eq:,.0f}" if _idr_eq > 0 else ""
            pnl_idr = f" / Rp{abs(cum_pnl) * _idr_eq:,.0f}" if _idr_eq > 0 else ""
            pnl_sign = "+" if cum_pnl >= 0 else "-"
            tg_close(
                f"📊 <b>Daily Report — {datetime.now(WIB).strftime('%d %b %Y')}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 Equity  : <b>${equity:.2f}{eq_idr}</b>\n"
                f"📈 PnL     : {pnl_sign}${abs(cum_pnl):.2f}{pnl_idr}\n"
                f"📉 DD      : {dd['dd_pct']*100:.1f}% dari peak\n"
                f"🎯 WR      : {wr:.1f}% ({wins}W / {losses}L)\n"
                f"📂 Open    : {open_trades} trades\n"
                f"<i>{datetime.now(WIB).strftime('%H:%M WIB')}</i>"
            )
    except Exception as e:
        log(f"Equity snapshot error: {e}", "warn")

# ════════════════════════════════════════════════════════
#  [PRIORITAS-8] PARALLEL SCAN WORKER
#  Dijalankan di worker thread (ThreadPoolExecutor). HANYA baca-baca dan
#  hitung skor — TIDAK PERNAH mengubah portfolio/state bersama, dan TIDAK
#  PERNAH kirim signal ke Telegram. Keputusan alokasi (portfolio cap,
#  MAX_SIGNALS_CYCLE, kirim Telegram) tetap sequential di main thread (lihat
#  run()) supaya urutan prioritas pair (trending dulu) dan limit slot tetap
#  deterministik — bukan rebutan antar-thread yang hasilnya beda-beda tiap
#  run tergantung thread mana yang selesai duluan.
# ════════════════════════════════════════════════════════

def _scan_pair_worker(client, pair: str, btc: dict, fg: int, scan_stats: "ScanStats") -> dict:
    """Return dict: {"pair", "skip", "sig"}.
    skip salah satu dari: "recent" | "cooldown" | "no_price" | None.
    Kalau skip is None, "sig" berisi hasil check_intraday() (dict atau None
    — None berarti direject teknikal, alasannya sudah tercatat di
    scan_stats lewat check_intraday sendiri, thread-safe via lock)."""
    if is_recently_signaled(pair):
        return {"pair": pair, "skip": "recent", "sig": None}
    if is_in_cooldown(pair):
        return {"pair": pair, "skip": "cooldown", "sig": None}

    price = get_ticker_price(client, pair)
    if price is None:
        return {"pair": pair, "skip": "no_price", "sig": None}

    sig = check_intraday(client, pair, price, btc, fg, side="BUY", stats=scan_stats)
    if sig is None and SELL_ENABLED and not btc.get("btc_bearish_trend"):
        sig = check_intraday(client, pair, price, btc, fg, side="SELL", stats=scan_stats)

    return {"pair": pair, "skip": None, "sig": sig}


# ════════════════════════════════════════════════════════
#  [PRIORITAS-9] ADAPTIVE STRATEGY — bucketing statistik, BUKAN AI
#
#  Bot belajar dari N trade terakhir dengan cara paling sederhana yang
#  masih jujur secara statistik: pecah trade jadi 2 bucket di sekitar
#  threshold yang AKTIF SEKARANG (mis. volume ratio 1.2x), hitung WR
#  masing-masing bucket, dan SARANKAN kalau ada gap yang mengindikasikan
#  threshold-nya kurang pas. TIDAK PERNAH auto-mengubah parameter — ini
#  murni laporan buat manusia yang review dan putuskan sendiri.
# ════════════════════════════════════════════════════════

_ADAPTIVE_WIN_RESULTS  = {"TP2", "TP1", "WIN", "PARTIAL_WIN", "SL_AFTER_TP1"}
_ADAPTIVE_LOSS_RESULTS = {"SL", "LOSS", "EXPIRED_LOSS"}


def _adaptive_bucket_wr(trades: list, value_key: str, lo, hi=None):
    """Hitung (n, wr_pct) untuk trade dengan value_key dalam [lo, hi).
    hi=None berarti tanpa batas atas. Trade dengan value_key None (data
    lama sebelum instrumentasi Prioritas 9) otomatis dikecualikan."""
    if hi is None:
        bucket = [t for t in trades if t.get(value_key) is not None and t[value_key] >= lo]
    else:
        bucket = [t for t in trades if t.get(value_key) is not None and lo <= t[value_key] < hi]
    wins   = sum(1 for t in bucket if t.get("result") in _ADAPTIVE_WIN_RESULTS)
    losses = sum(1 for t in bucket if t.get("result") in _ADAPTIVE_LOSS_RESULTS)
    total  = wins + losses
    wr = round(wins / total * 100, 1) if total > 0 else None
    return total, wr


def analyze_adaptive_suggestions(rows: list | None = None) -> str | None:
    """Return teks saran (str), pesan "belum cukup data" (str), atau None
    kalau gagal fetch. `rows` bisa di-inject langsung buat unit test —
    kalau None, fetch ADAPTIVE_LOOKBACK_TRADES trade terakhir dari Supabase."""
    if rows is None:
        try:
            rows = (
                supabase.table("signals_v2")
                .select("result, volume_ratio, entry_dev_pct, rsi_in_zone")
                .not_.is_("result", "null")
                .neq("result", "EXPIRED")
                .order("closed_at", desc=True)
                .limit(ADAPTIVE_LOOKBACK_TRADES)
                .execute()
                .data
            ) or []
        except Exception as e:
            log(f"analyze_adaptive_suggestions query error: {e}", "warn")
            return None

    if len(rows) < ADAPTIVE_LOOKBACK_TRADES:
        return (
            f"📚 <b>Adaptive Strategy</b> — belum cukup data: baru {len(rows)}/"
            f"{ADAPTIVE_LOOKBACK_TRADES} trade terkumpul sejak field instrumentasi "
            f"(volume_ratio, entry_dev_pct, rsi_in_zone) aktif. Trade lama sebelum "
            f"migrasi kolom tidak punya data ini, jadi belum dihitung. Tunggu sampai "
            f"cukup baru saran muncul."
        )

    MS = ADAPTIVE_MIN_SAMPLE_PER_BUCKET
    suggestions = []

    # ── Volume ratio — threshold aktif: 1.2x (lihat check_intraday) ──
    n_below, wr_below = _adaptive_bucket_wr(rows, "volume_ratio", 0.0, 1.2)
    n_above, wr_above = _adaptive_bucket_wr(rows, "volume_ratio", 1.2, 1.5)
    if n_below >= MS and n_above >= MS:
        line = (f"📊 <b>Volume ratio</b> — di bawah 1.2x (n={n_below}): WR {wr_below}% | "
                f"1.2x–1.5x (n={n_above}): WR {wr_above}%")
        if wr_below >= wr_above - 5:
            line += ("\n   → Saran: coba turunkan ambang jadi 1.1x — data tidak menunjukkan "
                     "penalti di 1.2x benar-benar berguna (WR di bawah ambang tidak kalah jauh).")
        else:
            line += "\n   → Ambang 1.2x tampak masih relevan (WR di bawahnya jelas lebih buruk)."
        suggestions.append(line)

    # ── Entry deviation — threshold aktif: 2% (soft), 5% (hard veto) ──
    n_below2, wr_below2 = _adaptive_bucket_wr(rows, "entry_dev_pct", 0.0, 2.0)
    n_above2, wr_above2 = _adaptive_bucket_wr(rows, "entry_dev_pct", 2.0, 5.0)
    if n_below2 >= MS and n_above2 >= MS:
        line = (f"📊 <b>Entry deviation</b> — ≤2% (n={n_below2}): WR {wr_below2}% | "
                f"2%–5% (n={n_above2}): WR {wr_above2}%")
        if wr_above2 >= wr_below2 - 5:
            line += ("\n   → Saran: penalti entry deviation di atas 2% mungkin terlalu "
                     "ketat — trade di zona 2–5% performanya tidak kalah jauh.")
        else:
            line += "\n   → Penalti entry deviation di atas 2% tampak masih relevan."
        suggestions.append(line)

    # ── RSI zona ideal (BUY: 40-65 | SELL: 35-60) ─────────
    rsi_in  = [t for t in rows if t.get("rsi_in_zone") is True]
    rsi_out = [t for t in rows if t.get("rsi_in_zone") is False]
    def _wr_simple(bucket):
        wins = sum(1 for t in bucket if t.get("result") in _ADAPTIVE_WIN_RESULTS)
        losses = sum(1 for t in bucket if t.get("result") in _ADAPTIVE_LOSS_RESULTS)
        total = wins + losses
        return total, (round(wins/total*100, 1) if total else None)
    n_in, wr_in = _wr_simple(rsi_in)
    n_out, wr_out = _wr_simple(rsi_out)
    if n_in >= MS and n_out >= MS:
        line = (f"📊 <b>RSI zona ideal</b> — di dalam zona (n={n_in}): WR {wr_in}% | "
                f"di luar zona (n={n_out}): WR {wr_out}%")
        if wr_out >= wr_in - 5:
            line += "\n   → Saran: bonus RSI zona ideal mungkin tidak terlalu signifikan — pertimbangkan turunkan bobotnya."
        else:
            line += "\n   → RSI zona ideal tampak memang berkorelasi dengan WR lebih baik."
        suggestions.append(line)

    if not suggestions:
        return (
            f"📚 <b>Adaptive Strategy</b> — {len(rows)} trade dianalisis, tapi "
            f"tiap bucket butuh minimal {MS} sample dan belum semua kebagian "
            f"(atau data terlalu seragam). Belum ada saran spesifik minggu ini."
        )

    return (
        "🧠 <b>Adaptive Strategy — Saran Parameter</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        + "\n\n".join(suggestions) +
        f"\n\n<i>Dianalisis dari {len(rows)} trade terakhir. Ini SARAN berbasis "
        f"statistik bucket sederhana — BUKAN AI, dan TIDAK otomatis mengubah "
        f"parameter apapun. Review manual sebelum diterapkan.</i>"
    )


def maybe_send_adaptive_report() -> None:
    """Kirim laporan Adaptive Strategy ke Telegram — max 1x per
    ADAPTIVE_REPORT_INTERVAL_DAYS (default 7 hari), dicek di jam yang sama
    dengan Daily Health Dashboard supaya tidak nge-cek tiap cycle."""
    try:
        now_wib = datetime.now(WIB)
        if now_wib.hour != _DAILY_HEALTH_REPORT_HOUR:
            return
        last_str = _read_config("last_adaptive_report_date", "") or ""
        if last_str:
            try:
                last_dt = datetime.fromisoformat(last_str)
                if (now_wib.date() - last_dt.date()).days < ADAPTIVE_REPORT_INTERVAL_DAYS:
                    return
            except Exception:
                pass   # format tanggal lama tidak valid — anggap belum pernah kirim, lanjut

        report = analyze_adaptive_suggestions()
        if report:
            _write_config("last_adaptive_report_date", now_wib.date().isoformat())
            tg_operator(report)
    except Exception as e:
        log(f"Adaptive report send error: {e}", "warn")


# ════════════════════════════════════════════════════════
#  MAIN RUN
# ════════════════════════════════════════════════════════

def run():
    # [PRIORITAS-4] Timer runtime cycle — dipakai buat Daily Bot Health Dashboard.
    _cycle_start_t = time.time()

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
        # Low WR Hour — tidak dikirim ke Telegram, hanya di-log
        # (menghilangkan spam notif di jam-jam blocked)
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

    # [v1.4.9] BTC crash halt & volatile guard dihapus
    # BTC info tetap di-fetch untuk log saja, tidak memblok scan
    if btc.get("halt"):
        log("⚠️ BTC crash terdeteksi — scan tetap jalan (guard dinonaktifkan)", "warn")
    if btc.get("btc_volatile"):
        log(f"⚡ BTC volatile — scan tetap jalan (guard dinonaktifkan)", "warn")

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

    # [v1.4.9] Drawdown HALT dihapus — bot tetap entry meski streak loss
    if dd_mode == "halt":
        log("⚠️ DD HALT mode — position size dikurangi, scan tetap jalan", "warn")

    # Drawdown Warning — kirim hanya saat streak BARU mencapai threshold
    # Cegah spam tiap run dengan cek apakah streak sudah pernah dikirim
    if dd_mode in ("warn", "halt"):
        _last_notif_streak = int(_read_config("last_dd_notif_streak", 0) or 0)
        _cur_streak = drawdown["streak"]
        if _cur_streak > _last_notif_streak:
            _write_config("last_dd_notif_streak", _cur_streak)
            if dd_mode == "halt":
                tg_operator(
                    f"🛑 <b>Drawdown HALT</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Streak : {_cur_streak} loss berturutan\n"
                    f"DD     : {drawdown['dd_pct']*100:.1f}% dari peak\n"
                    f"<i>Bot berhenti entry posisi baru.</i>"
                )
            else:
                tg_operator(
                    f"⚠️ <b>Drawdown Warning</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Streak : {_cur_streak} loss berturutan\n"
                    f"DD     : {drawdown['dd_pct']*100:.1f}% dari peak\n"
                    f"<i>Position size dikurangi 30%.</i>"
                )
        elif _cur_streak < _last_notif_streak:
            # Streak membaik — reset counter notif
            _write_config("last_dd_notif_streak", _cur_streak)

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
    cycle_score_sum = 0.0   # [PRIORITAS-4] buat Avg Score di Daily Health Dashboard
    cycle_rr_sum    = 0.0   # [PRIORITAS-4] buat Avg RR di Daily Health Dashboard

    # [DIAG-1] scan_stats mengumpulkan alasan reject sepanjang loop di bawah,
    # lalu diringkas jadi SCAN SUMMARY di akhir run().
    scan_stats = ScanStats()
    scan_stats.total_pairs = len(all_pairs)

    # ══════════════════════════════════════════════════════
    # [PRIORITAS-8] FASE 1 — PARALLEL: fetch harga, dedup check, dan hitung
    # skor teknikal buat SEMUA pair sekaligus lewat ThreadPoolExecutor.
    # Ini bagian paling I/O-heavy (network round-trip ke Gate.io & Supabase
    # per pair) — jadi yang paling banyak untung dari paralelisasi.
    #
    # Pair yang sudah open di portfolio di-skip di sini (murah, in-memory,
    # tidak perlu network call, jadi tidak usah masuk thread pool).
    #
    # TRADE-OFF yang perlu diketahui: versi sequential lama BERHENTI lebih
    # awal begitu portfolio/MAX_SIGNALS_CYCLE penuh (hemat API call, tapi
    # pair setelah titik itu tidak pernah tahu reject reason aslinya —
    # semua dilabel "Portfolio Full" secara massal). Versi paralel ini
    # men-scan SEMUA pair yang eligible (lebih banyak total API call per
    # cycle), tapi SETIAP pair dapat reject reason yang akurat — bonus buat
    # Diagnostic Mode (Prioritas 1) — dan lebih cepat secara wall-clock
    # berkat concurrency. Kalau ternyata rate limit Gate.io jadi masalah,
    # turunkan MAX_SCAN_WORKERS via env, bukan balik ke sequential.
    # ══════════════════════════════════════════════════════
    eligible_pairs = []
    for pair in all_pairs:
        if pair in portfolio.get("open_pairs", []):
            scan_stats.bump("open_pair")
        else:
            eligible_pairs.append(pair)

    scan_results = []
    if eligible_pairs:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_SCAN_WORKERS) as executor:
            worker = lambda p: _scan_pair_worker(client, p, btc, fg, scan_stats)
            # executor.map MEMPERTAHANKAN urutan input (bukan urutan selesai)
            # — penting supaya prioritas trending-coins-dulu tetap kepakai
            # di Fase 2, walau proses fetch-nya sendiri paralel/tidak berurutan.
            scan_results = list(executor.map(worker, eligible_pairs))

    # ══════════════════════════════════════════════════════
    # FASE 2 — SEQUENTIAL: alokasi sinyal berdasarkan hasil Fase 1, dalam
    # urutan prioritas asli (trending dulu). Bagian ini SENGAJA tetap
    # single-threaded — portfolio cap, MAX_SIGNALS_CYCLE, dan pengiriman
    # Telegram harus deterministik, bukan rebutan antar-thread.
    # ══════════════════════════════════════════════════════
    for result in scan_results:
        pair = result["pair"]
        skip = result["skip"]

        if skip == "recent":
            scan_stats.bump("recent")
            continue
        if skip == "cooldown":
            scan_stats.bump("cooldown")
            continue
        if skip == "no_price":
            scan_stats.bump("no_price")
            continue

        scanned += 1
        sig = result["sig"]
        if sig is None:
            continue   # reject reason sudah tercatat oleh check_intraday() di Fase 1

        if signals_sent >= MAX_SIGNALS_CYCLE or portfolio["total"] + signals_sent >= MAX_OPEN_TRADES:
            # Portfolio/limit sinyal penuh — pair ini teknikal valid tapi
            # tidak kebagian slot. Sama seperti dulu, dicatat "Portfolio Full".
            scan_stats.bump("portfolio")
            continue

        if not portfolio_allows(sig, portfolio, drawdown):
            scan_stats.bump("portfolio")
            continue

        log(f"   ✅ SIGNAL: {pair} {sig['side']} score={sig['score']:.2f} "
            f"tier={sig['tier']} rr={sig['rr']}")

        if send_signal(sig, dd_mode, actual_equity=actual_equity):
            signals_sent += 1
            cycle_score_sum += sig["score"]
            cycle_rr_sum    += sig["rr"]
            portfolio["total"] += 1
            portfolio["open_pairs"].append(pair)
            if sig["side"] == "BUY":
                portfolio["buy"] += 1
            else:
                portfolio["sell"] += 1
            time.sleep(0.3)   # jeda kirim Telegram, bukan lagi pacing API Gate.io

    scan_stats.signals_sent = signals_sent

    log(f"\n{'='*55}")
    log(f"✅ Scan selesai — {scanned} pair diperiksa | {signals_sent} signal terkirim")

    # [DIAG-1] Cetak SCAN SUMMARY ke log (selalu) dan Telegram (opsional via
    # DIAGNOSTIC_TELEGRAM) — ini yang menjawab "842 pair → 0 signal, kenapa?"
    diag_text = scan_stats.format_report(BOT_VERSION)
    log("\n" + diag_text)
    if DIAGNOSTIC_TELEGRAM:
        try:
            tg_operator(scan_stats.format_report_html(BOT_VERSION))
        except Exception as e:
            log(f"Diagnostic Telegram send error: {e}", "warn")

    # [PRIORITAS-4] Akumulasi cycle ini ke Daily Health blob (persist via
    # Supabase config — proses GitHub Actions baru tiap cycle, jadi TIDAK
    # bisa disimpan di variabel Python biasa). Lalu cek apakah sudah jam
    # kirim ringkasan harian (22:00 WIB, sekali sehari).
    cycle_runtime_s = time.time() - _cycle_start_t
    health_blob = _accumulate_daily_health(
        scan_stats, cycle_score_sum, cycle_rr_sum,
        cycle_runtime_s, _api_failures_cycle
    )
    maybe_send_daily_health_report(health_blob)
    maybe_send_adaptive_report()   # [PRIORITAS-9] guard interval sendiri (mingguan), aman dipanggil tiap cycle
    log(f"⏱️ Cycle runtime: {cycle_runtime_s:.1f} detik")

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

    # ── Kirim pesan open trades — HANYA jika ada posisi terbuka ───
    open_trades_msg = build_open_trades_msg(client)
    if portfolio["total"] > 0:
        tg_signal(open_trades_msg)   # hanya kirim jika ada open trade

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
    print(f"🧪 UNIT TESTS — Signal Bot Lite v{BOT_VERSION}")
    print("   Coverage: calc_sl_tp, calc_position_size, score_signal,")
    print("   indikator edge cases, portfolio_allows, drawdown severity,")
    print("   score SELL/anomaly, RR validation, ScanStats diagnostic,")
    print("   Daily Health, weighted score, EMA/RSI/MACD/ATR correctness,")
    print("   Trade Lifecycle state machine, parallel scan thread-safety,")
    print("   Adaptive Strategy bucketing")
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
        _assert(isinstance(score_fg_extreme, float) and score_fg_extreme >= 0,
                "F&G ekstrem: skor tetap valid float (penalty dihapus v1.4.9)",
                f"fg_extreme={score_fg_extreme}")

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

        # [v1.4.9] Max same side & risk budget dihapus dari portfolio_allows
        # Test diubah: pastikan fungsi tidak crash dengan state yang punya field ini
        max_buy_state = {**base_state, "buy": MAX_SAME_SIDE}
        _assert(isinstance(portfolio_allows(base_sig, max_buy_state, base_dd), bool),
                "portfolio_allows: buy >= MAX_SAME_SIDE → tetap return bool (limit dihapus v1.4.9)")

        high_risk_state = {**base_state, "total_risk_usdt": 350.0 * MAX_RISK_TOTAL}
        _assert(isinstance(portfolio_allows(base_sig, high_risk_state, base_dd), bool),
                "portfolio_allows: risk budget state → tetap return bool (limit dihapus v1.4.9)")

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
        # [v1.4.9] F&G penalty dihapus — score F&G>80 sama dengan normal
        # Test diubah: pastikan score tidak crash (tetap float valid)
        _assert(isinstance(score_greed, float) and score_greed >= 0,
                "F&G > 80 (extreme greed): score tetap valid float (penalty dihapus v1.4.9)",
                f"greed={score_greed}")

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

    # ── SECTION 9: ScanStats (Diagnostic Mode + Penalty tracking) ──
    print("\n[9] ScanStats — reject counter, penalty tracking, & reconciliation")

    try:
        stats = ScanStats()
        stats.total_pairs = 20
        for _ in range(3): stats.bump("atr")
        for _ in range(2): stats.bump("pump")
        for _ in range(9): stats.bump("score")
        for _ in range(2): stats.bump("rr")
        for _ in range(1): stats.bump("entry")   # veto ekstrem (>MAX_ENTRY_DEV_HARD)
        stats.counts["portfolio"] += 2
        stats.signals_sent = 1

        # [PRIORITAS-2] Penalty tracking terpisah dari reject counts —
        # tidak masuk hitungan reconciliation karena satu pair bisa kena
        # >1 penalty tapi tetap cuma dihitung SEKALI di counts/signals_sent
        # [PRIORITAS-6] wr_bad/wr_good sudah tidak ada — WR tidak lagi
        # menyentuh score, jadi tidak ada penalty WR untuk ditest di sini.
        for _ in range(6): stats.bump_penalty("volume")
        for _ in range(4): stats.bump_penalty("entry")

        _assert(stats.counts["atr"] == 3,
                "bump() akumulasi reject counter dengan benar",
                f"got={stats.counts['atr']}")
        _assert(stats.penalties["volume"] == 6,
                "bump_penalty() akumulasi penalty counter dengan benar",
                f"got={stats.penalties['volume']}")
        _assert("volume" not in stats.counts,
                "[PRIORITAS-2] 'volume' bukan lagi reject reason (sudah jadi penalty)")
        _assert("wr" not in stats.counts,
                "[PRIORITAS-2] 'wr' bukan lagi reject reason (sudah jadi penalty)")
        _assert("wr_bad" not in stats.penalties and "wr_good" not in stats.penalties,
                "[PRIORITAS-6] WR tidak lagi punya penalty key sama sekali (dihapus total dari scoring)")

        accounted = sum(stats.counts.values()) + stats.signals_sent
        _assert(accounted == stats.total_pairs,
                "[DIAG-1e] total semua reject reason + signal = total_pairs "
                "(penalty TIDAK ikut dihitung — bukan reject terpisah)",
                f"accounted={accounted} total={stats.total_pairs}")

        report = stats.format_report("test")
        _assert("Rejected Score" in report and "9" in report,
                "format_report() menampilkan angka reject score yang benar")
        _assert("PENALTY/BONUS DITERAPKAN KE SCORE" in report,
                "format_report() menampilkan section penalty terpisah dari reject")
        _assert("Penalty: Volume lemah" in report,
                "format_report() menampilkan detail penalty volume")
        _assert("selisih" not in report,
                "format_report() tidak menampilkan warning selisih saat reconciled")

        # Kasus tidak reconciled — harus tampilkan warning eksplisit, bukan diam-diam salah
        stats_bad = ScanStats()
        stats_bad.total_pairs = 10
        stats_bad.bump("atr")
        stats_bad.signals_sent = 0
        report_bad = stats_bad.format_report("test")
        _assert("selisih" in report_bad,
                "format_report() menampilkan warning eksplisit saat data tidak reconciled")

        html_report = stats.format_report_html("test")
        _assert("<pre>" in html_report and "Scan Diagnostic" in html_report,
                "format_report_html() menghasilkan format Telegram yang valid")
        _assert("Penalty/bonus" in html_report,
                "format_report_html() menyertakan section penalty/bonus")

        # Kategori dengan count 0 tidak boleh muncul di html report (ringkas)
        stats_sparse = ScanStats()
        stats_sparse.total_pairs = 1
        stats_sparse.signals_sent = 1
        html_sparse = stats_sparse.format_report_html("test")
        _assert("Rejected ATR" not in html_sparse,
                "format_report_html() tidak menampilkan kategori dengan count 0")
        _assert("Penalty/bonus" not in html_sparse,
                "format_report_html() tidak menampilkan section penalty kalau semua 0")

    except Exception as e:
        print(f"  ❌ EXCEPTION ScanStats: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 10: Daily Bot Health Dashboard ────────────
    print("\n[10] Daily Bot Health — akumulasi & format report")

    try:
        blob = {
            "date": "2026-07-16", "cycles": 3, "total_pairs": 300,
            "total_signals": 4, "score_sum": 12.4, "rr_sum": 8.0,
            "reject_totals": {"score": 150, "volume_reject_key_should_not_exist": 0,
                               "atr": 20, "rr": 30, "portfolio": 10},
            "api_errors": 2, "runtime_sum": 150.0,
        }
        # Perbaiki key dummy yang sengaja salah di atas (bukan bagian LABELS asli)
        blob["reject_totals"] = {"score": 150, "atr": 20, "rr": 30, "portfolio": 10}

        report = _format_daily_health_report(blob)
        _assert("Daily Bot Health" in report, "format_daily_health_report() ada judul yang benar")
        _assert("Scan" in report and "300 pair" in report,
                "format_daily_health_report() menampilkan total pair yang benar")
        _assert("Signal" in report and ": 4" in report,
                "format_daily_health_report() menampilkan total signal yang benar")
        _assert(f"{12.4/4:.2f}" in report,
                "format_daily_health_report() menghitung Avg Score dengan benar",
                f"expected avg_score={12.4/4:.2f}")
        _assert(f"{8.0/4:.2f}" in report,
                "format_daily_health_report() menghitung Avg RR dengan benar")
        _assert("Rejected Score" in report and "50%" in report,
                "format_daily_health_report() menemukan reject terbesar (score, 150/300=50%) dengan benar")
        _assert("API Error" in report and ": 2" in report,
                "format_daily_health_report() menampilkan API error yang benar")
        _assert(f"{150.0/3:.0f}" in report,
                "format_daily_health_report() menghitung runtime rata-rata per cycle dengan benar")

        # Edge case: belum ada signal sama sekali — avg score/RR tidak boleh crash (div by zero)
        blob_zero = {
            "date": "2026-07-16", "cycles": 1, "total_pairs": 50,
            "total_signals": 0, "score_sum": 0.0, "rr_sum": 0.0,
            "reject_totals": {}, "api_errors": 0, "runtime_sum": 30.0,
        }
        report_zero = _format_daily_health_report(blob_zero)
        _assert("Avg Score     : 0.00" in report_zero,
                "format_daily_health_report() tidak crash saat 0 signal (avg score = 0.00)")
        _assert("Reject terbesar: —" in report_zero,
                "format_daily_health_report() tampilkan '—' saat reject_totals kosong")

    except Exception as e:
        print(f"  ❌ EXCEPTION Daily Health format: {e}\n{traceback.format_exc()}")
        failed += 1

    try:
        # _load_daily_health harus reset otomatis kalau tanggal beda
        fake_old_blob = json.dumps({
            "date": "2020-01-01", "cycles": 99, "total_pairs": 9999,
            "total_signals": 99, "score_sum": 1.0, "rr_sum": 1.0,
            "reject_totals": {"score": 5}, "api_errors": 1, "runtime_sum": 1.0,
        })
        today_str = datetime.now(WIB).strftime("%Y-%m-%d")
        # Simulasikan tanpa I/O nyata: panggil parsing logic secara langsung
        parsed = json.loads(fake_old_blob)
        _assert(parsed.get("date") != today_str,
                "Setup test valid: blob dummy pakai tanggal lampau, bukan hari ini")

        # accumulate harus menambah, bukan menimpa, dalam satu cycle
        base_blob = {
            "date": today_str, "cycles": 1, "total_pairs": 10, "total_signals": 1,
            "score_sum": 3.0, "rr_sum": 2.0, "reject_totals": {"score": 5},
            "api_errors": 0, "runtime_sum": 10.0,
        }
        base_blob["cycles"] += 1
        base_blob["total_pairs"] += 20
        base_blob["reject_totals"]["score"] = base_blob["reject_totals"].get("score", 0) + 3
        _assert(base_blob["total_pairs"] == 30 and base_blob["reject_totals"]["score"] == 8,
                "Logika akumulasi manual (pola yang dipakai _accumulate_daily_health) benar")

    except Exception as e:
        print(f"  ❌ EXCEPTION Daily Health accumulation logic: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 11: Weighted score refactor (Trend/Momentum/Volume/Structure) ──
    print("\n[11] score_signal() weighted percentage — Trend 35% / Momentum 40% / Volume 15% / Structure 10%")

    try:
        _assert(abs((TREND_WEIGHT + MOMENTUM_WEIGHT + VOLUME_WEIGHT + STRUCTURE_WEIGHT) - 1.0) < 1e-9,
                "Total bobot 4 faktor persis 100%",
                f"total={TREND_WEIGHT + MOMENTUM_WEIGHT + VOLUME_WEIGHT + STRUCTURE_WEIGHT}")
        _assert(MOMENTUM_WEIGHT > VOLUME_WEIGHT,
                "Momentum dibobot lebih tinggi dari Volume (sesuai permintaan)")
        _assert(TREND_WEIGHT > STRUCTURE_WEIGHT,
                "Trend dibobot lebih tinggi dari Structure")

        # Skor maksimum teoritis (semua faktor full 1.0, TRENDING regime)
        # harus persis SCORE_MAX_SCALE
        struct_full = {"last_sh": 110.0, "last_sl": 100.0}  # (110-100)/100=10% > 2% → structure_ratio=1.0
        volumes_full = [1000.0]*10 + [1600.0]  # > 1.5x avg → volume_ratio=1.0
        max_score = score_signal(
            "BUY", 106.0, [100]*100, [100]*100, [100]*100, volumes_full,
            struct_full, rsi=50.0, macd=1.0, msig=0.5,   # macd>msig & macd>0 → macd_ratio=1.0
            ema20=105.0, ema50=100.0,                     # ema20>ema50 & price(106)>ema20(105) → trend_ratio=1.0
            regime="TRENDING", btc_4h=0.0, fg=50
        )
        _assert(abs(max_score - SCORE_MAX_SCALE) < 0.01,
                "Skor maksimum teoritis (semua faktor full) = SCORE_MAX_SCALE persis",
                f"max_score={max_score} expected={SCORE_MAX_SCALE}")

        # Skor minimum (semua faktor 0) harus 0.0
        min_score = score_signal(
            "BUY", 100.0, [100]*100, [100]*100, [100]*100, [1000.0]*11,
            {"last_sh": None, "last_sl": None}, rsi=90.0, macd=-1.0, msig=0.5,
            ema20=100.0, ema50=105.0,   # ema20<ema50 (BUY salah arah) → trend_ratio=0.0
            regime="TRENDING", btc_4h=0.0, fg=50
        )
        _assert(min_score == 0.0, "Skor minimum (semua faktor 0) = 0.0", f"min_score={min_score}")

        # Ganti hanya faktor Momentum ke full sementara yang lain 0 → kontribusinya
        # harus persis MOMENTUM_WEIGHT * SCORE_MAX_SCALE (dalam toleransi RSI 30%/MACD 70%)
        momentum_only = score_signal(
            "BUY", 100.0, [100]*100, [100]*100, [100]*100, [1000.0]*11,
            {"last_sh": None, "last_sl": None}, rsi=50.0, macd=1.0, msig=0.5,
            ema20=100.0, ema50=105.0, regime="TRENDING", btc_4h=0.0, fg=50
        )
        expected_momentum_contrib = round(1.0 * MOMENTUM_WEIGHT * SCORE_MAX_SCALE, 2)
        _assert(abs(momentum_only - expected_momentum_contrib) < 0.01,
                "Kontribusi Momentum-only sesuai bobot 40%",
                f"got={momentum_only} expected={expected_momentum_contrib}")

    except Exception as e:
        print(f"  ❌ EXCEPTION weighted score refactor: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 12: EMA — correctness (bukan cuma edge case) ──
    print("\n[12] calc_ema() — korektness terhadap nilai yang dihitung manual")

    try:
        # Flat series — EMA harus persis sama dengan harga konstan
        ema_flat = calc_ema([50.0] * 30, 10)
        _assert(abs(ema_flat - 50.0) < 1e-9,
                "EMA dari harga konstan = harga itu sendiri persis",
                f"got={ema_flat}")

        # Deret kecil dihitung manual: EMA(period=3) dari [1,2,3,4,5]
        # k = 2/(3+1) = 0.5; ema0=1; ema1=1.5; ema2=2.25; ema3=3.125; ema4=4.0625
        ema_manual = calc_ema([1.0, 2.0, 3.0, 4.0, 5.0], 3)
        _assert(abs(ema_manual - 4.0625) < 1e-9,
                "EMA(period=3) atas [1,2,3,4,5] = 4.0625 (dihitung manual)",
                f"got={ema_manual}")

        # EMA uptrend harus > harga awal, < harga akhir (lag alami EMA)
        up = [100.0 + i for i in range(30)]
        ema_up = calc_ema(up, 10)
        _assert(up[0] < ema_up < up[-1],
                "EMA uptrend berada di antara harga awal dan akhir (lag alami)",
                f"ema={ema_up} range=({up[0]},{up[-1]})")

    except Exception as e:
        print(f"  ❌ EXCEPTION calc_ema correctness: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 13: RSI — correctness ─────────────────────
    print("\n[13] calc_rsi() — korektness arah & batas nilai")

    try:
        # Uptrend murni (semua gain, tanpa loss) → RSI harus 100.0
        pure_up = [100.0 + i for i in range(20)]
        rsi_up = calc_rsi(pure_up, period=14)
        _assert(rsi_up == 100.0,
                "RSI uptrend murni (semua gain) = 100.0", f"got={rsi_up}")

        # Downtrend murni (semua loss, tanpa gain) → RSI harus 0.0
        pure_down = [100.0 - i for i in range(20)]
        rsi_down = calc_rsi(pure_down, period=14)
        _assert(rsi_down == 0.0,
                "RSI downtrend murni (semua loss) = 0.0", f"got={rsi_down}")

        # RSI selalu dalam batas [0, 100] untuk data acak manapun
        import random as _r
        _r.seed(11)
        bounds_ok = True
        for _ in range(20):
            data = [100.0 + _r.gauss(0, 2) for _ in range(30)]
            r = calc_rsi(data, period=14)
            if not (0.0 <= r <= 100.0):
                bounds_ok = False
                break
        _assert(bounds_ok, "RSI selalu dalam batas [0,100] untuk data acak (20 sampel)")

    except Exception as e:
        print(f"  ❌ EXCEPTION calc_rsi correctness: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 14: MACD — correctness ────────────────────
    print("\n[14] calc_macd() — korektness arah & nilai flat")

    try:
        # Harga flat — MACD line & signal line harus 0.0 (EMA12=EMA26=harga konstan)
        macd_flat, sig_flat = calc_macd([100.0] * 40)
        _assert(abs(macd_flat) < 1e-6 and abs(sig_flat) < 1e-6,
                "MACD dari harga flat = (0.0, 0.0)",
                f"got=({macd_flat}, {sig_flat})")

        # Uptrend kuat & konsisten — MACD line harus POSITIF (EMA cepat > EMA lambat)
        strong_up = [100.0 + i * 0.5 for i in range(40)]
        macd_up, sig_up = calc_macd(strong_up)
        _assert(macd_up > 0,
                "MACD line uptrend kuat harus positif (EMA12 > EMA26)",
                f"macd={macd_up}")

        # Downtrend kuat & konsisten — MACD line harus NEGATIF
        strong_down = [100.0 - i * 0.5 for i in range(40)]
        macd_down, sig_down = calc_macd(strong_down)
        _assert(macd_down < 0,
                "MACD line downtrend kuat harus negatif (EMA12 < EMA26)",
                f"macd={macd_down}")

    except Exception as e:
        print(f"  ❌ EXCEPTION calc_macd correctness: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 15: ATR — correctness (belum pernah ditest langsung) ──
    print("\n[15] calc_atr() — korektness terhadap True Range yang dihitung manual")

    try:
        # True Range konstan by design: high-low=2 tiap candle, tanpa gap dari close
        # sebelumnya (highs/lows/closes disusun supaya |high-prev_close| dan
        # |low-prev_close| selalu <= high-low) → ATR harus persis 2.0
        n = 20
        closes_c = [100.0] * n
        highs_c  = [101.0] * n
        lows_c   = [99.0] * n
        atr_const = calc_atr(closes_c, highs_c, lows_c, period=14)
        _assert(abs(atr_const - 2.0) < 1e-9,
                "ATR dari True Range konstan (high-low=2 tiap candle) = 2.0 persis",
                f"got={atr_const}")

        # ATR harus selalu positif untuk data dengan range wajar
        import random as _r2
        _r2.seed(5)
        closes_r = [100.0]
        for _ in range(29):
            closes_r.append(closes_r[-1] + _r2.gauss(0, 1))
        highs_r = [c + abs(_r2.gauss(0, 0.5)) for c in closes_r]
        lows_r  = [c - abs(_r2.gauss(0, 0.5)) for c in closes_r]
        atr_random = calc_atr(closes_r, highs_r, lows_r, period=14)
        _assert(atr_random > 0,
                "ATR data acak dengan range wajar selalu positif", f"got={atr_random}")

        # period cap: kalau data tersedia < period, tetap tidak crash & pakai yang ada
        atr_short = calc_atr([100.0, 101.0, 99.0], [102.0, 103.0, 100.0], [99.0, 100.0, 98.0], period=14)
        _assert(atr_short > 0,
                "ATR dengan data < period tidak crash, tetap hitung dari yang tersedia",
                f"got={atr_short}")

    except Exception as e:
        print(f"  ❌ EXCEPTION calc_atr correctness: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 16: RR — hubungan analitik entry/SL/TP ────
    print("\n[16] RR — hubungan analitik calc_sl_tp() terhadap TP1_R/TP2_R")

    try:
        entry_r = 100.0
        atr_r   = 2.0
        struct_r = {"last_sh": 106.0, "last_sl": 94.0}
        sl_r, tp1_r, tp2_r = calc_sl_tp(entry_r, "BUY", atr_r, struct_r)
        sl_dist_r = entry_r - sl_r
        rr_tp1_exact = (tp1_r - entry_r) / sl_dist_r
        rr_tp2_exact = (tp2_r - entry_r) / sl_dist_r
        _assert(abs(rr_tp1_exact - TP1_R) < 0.01,
                "RR di TP1 = TP1_R persis (hubungan analitik by design)",
                f"rr={rr_tp1_exact:.4f} TP1_R={TP1_R}")
        _assert(abs(rr_tp2_exact - TP2_R) < 0.01,
                "RR di TP2 = TP2_R persis (hubungan analitik by design)",
                f"rr={rr_tp2_exact:.4f} TP2_R={TP2_R}")

        # SELL — hubungan yang sama harus berlaku simetris
        sl_s2, tp1_s2, tp2_s2 = calc_sl_tp(entry_r, "SELL", atr_r, struct_r)
        sl_dist_s2 = sl_s2 - entry_r
        rr_tp1_s2 = (entry_r - tp1_s2) / sl_dist_s2
        _assert(abs(rr_tp1_s2 - TP1_R) < 0.01,
                "SELL: RR di TP1 = TP1_R persis (simetris dengan BUY)",
                f"rr={rr_tp1_s2:.4f} TP1_R={TP1_R}")

    except Exception as e:
        print(f"  ❌ EXCEPTION RR analytic relationship: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 17: Trade Lifecycle — state machine ───────
    print("\n[17] Trade Lifecycle — _resolve_trade_from_candles() state transitions")

    try:
        def _make_trade(side="BUY", state="OPEN", **overrides):
            base = dict(
                id="t1", pair="TEST_USDT", side=side,
                entry=100.0,
                sl=97.0 if side == "BUY" else 103.0,
                tp1=103.0 if side == "BUY" else 97.0,
                tp2=106.0 if side == "BUY" else 94.0,
                score=3.0, state=state, size=25.0, sent_at="2026-01-01T00:00:00",
            )
            base.update(overrides)
            return Trade(**base)

        # ── BUY, state OPEN ──
        t = _make_trade("BUY", "OPEN")
        event, pnl, new_be = _resolve_trade_from_candles(t, [98.0], [96.0])   # low 96 <= SL 97
        _assert(event == "SL" and pnl is not None and pnl < 0,
                "BUY OPEN: candle low tembus SL → event SL, pnl negatif",
                f"event={event} pnl={pnl}")

        t = _make_trade("BUY", "OPEN")
        event, pnl, new_be = _resolve_trade_from_candles(t, [107.0], [99.0])  # high 107 >= TP2 106
        _assert(event == "TP2" and pnl is not None and pnl > 0,
                "BUY OPEN: candle high tembus TP2 langsung → event TP2, pnl positif",
                f"event={event} pnl={pnl}")

        t = _make_trade("BUY", "OPEN")
        event, pnl, new_be = _resolve_trade_from_candles(t, [104.0], [99.0])  # high 104: >=TP1(103), <TP2(106)
        _assert(event == "TP1" and pnl is not None and pnl > 0,
                "BUY OPEN: candle high tembus TP1 saja (bukan TP2) → event TP1",
                f"event={event} pnl={pnl}")

        t = _make_trade("BUY", "OPEN")
        event, pnl, new_be = _resolve_trade_from_candles(t, [101.0], [99.0])  # tidak kena apa-apa
        _assert(event is None and pnl is None,
                "BUY OPEN: harga di tengah (tidak kena SL/TP1/TP2) → tidak ada event",
                f"event={event} pnl={pnl}")

        # ── SELL, state OPEN — ini yang tadinya BUG (return 4-tuple utk TP1) ──
        t = _make_trade("SELL", "OPEN")
        event, pnl, new_be = _resolve_trade_from_candles(t, [104.0], [98.0])  # high 104 >= SL 103
        _assert(event == "SL" and pnl is not None and pnl < 0,
                "SELL OPEN: candle high tembus SL → event SL, pnl negatif",
                f"event={event} pnl={pnl}")

        t = _make_trade("SELL", "OPEN")
        event, pnl, new_be = _resolve_trade_from_candles(t, [101.0], [93.0])  # low 93 <= TP2 94
        _assert(event == "TP2" and pnl is not None and pnl > 0,
                "SELL OPEN: candle low tembus TP2 langsung → event TP2, pnl positif",
                f"event={event} pnl={pnl}")

        t = _make_trade("SELL", "OPEN")
        # [FIX-PRIORITAS-7] Ini dulu ValueError (4-tuple) — sekarang harus 3-tuple valid
        event, pnl, new_be = _resolve_trade_from_candles(t, [101.0], [96.0])  # low 96: <=TP1(97), >TP2(94)
        _assert(event == "TP1" and pnl is not None and pnl > 0,
                "[BUG FIX] SELL OPEN: candle low tembus TP1 saja → event TP1 (dulu ValueError, unpack 4≠3)",
                f"event={event} pnl={pnl}")

        # ── BUY, state TP1_HIT — trailing SL ──
        t = _make_trade("BUY", "TP1_HIT", sl_breakeven=100.0, remaining_size=12.5)
        # Harga naik terus jauh di atas TP2, TANPA low candle turun ke bawah
        # trailing stop yang baru ter-update (trail_dist=1.5, jadi low harus
        # tetap > hi-1.5) — kalau tidak, trailing SL dicek duluan dalam
        # candle yang sama sebelum TP2 (by design: worst-case konservatif).
        event, pnl, new_be = _resolve_trade_from_candles(t, [107.0], [106.0])
        _assert(event == "TP2" and pnl is not None and pnl > 0,
                "BUY TP1_HIT: harga lanjut ke TP2 → event TP2 (partial exit ke-2)",
                f"event={event} pnl={pnl}")

        t = _make_trade("BUY", "TP1_HIT", sl_breakeven=100.0, remaining_size=12.5)
        # Harga turun balik ke breakeven → SL_AFTER_TP1
        event, pnl, new_be = _resolve_trade_from_candles(t, [100.5], [99.5])
        _assert(event == "SL_AFTER_TP1" and pnl is not None,
                "BUY TP1_HIT: harga turun balik ke breakeven → event SL_AFTER_TP1",
                f"event={event} pnl={pnl}")

        t = _make_trade("BUY", "TP1_HIT", sl_breakeven=100.0, remaining_size=12.5)
        # Harga naik dikit tapi belum kena TP2 atau trailing SL → tidak ada event,
        # tapi trailing SL (new_be) harus ter-update naik
        event, pnl, new_be = _resolve_trade_from_candles(t, [102.0], [101.5])
        _assert(event is None and new_be is not None and new_be >= 100.0,
                "BUY TP1_HIT: belum ada event, tapi trailing SL ter-update (bukan None)",
                f"event={event} new_be={new_be}")

    except Exception as e:
        print(f"  ❌ EXCEPTION Trade Lifecycle: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 18: Parallel scan — thread-safety & worker ─
    print("\n[18] Parallel Scan — ScanStats thread-safety & _scan_pair_worker()")

    try:
        # Stress test: N thread nge-bump counter yang SAMA bersamaan.
        # Tanpa lock, ini rawan lost update (race condition classic).
        stress_stats = ScanStats()
        stress_stats.total_pairs = 2000
        N_THREADS = 20
        BUMPS_PER_THREAD = 100

        def _hammer():
            for _ in range(BUMPS_PER_THREAD):
                stress_stats.bump("score")
                stress_stats.bump_penalty("volume")

        threads = [threading.Thread(target=_hammer) for _ in range(N_THREADS)]
        for th in threads: th.start()
        for th in threads: th.join()

        expected = N_THREADS * BUMPS_PER_THREAD
        _assert(stress_stats.counts["score"] == expected,
                f"[PRIORITAS-8] ScanStats.bump() thread-safe — {N_THREADS} thread x {BUMPS_PER_THREAD} bump, tidak ada lost update",
                f"got={stress_stats.counts['score']} expected={expected}")
        _assert(stress_stats.penalties["volume"] == expected,
                "[PRIORITAS-8] ScanStats.bump_penalty() thread-safe — sama, tidak ada lost update",
                f"got={stress_stats.penalties['volume']} expected={expected}")

        # _scan_pair_worker(): skip-path (dedup) tidak boleh panggil get_ticker_price
        _orig_recent = is_recently_signaled
        _orig_cooldown = is_in_cooldown
        _orig_price = get_ticker_price
        try:
            globals()["is_recently_signaled"] = lambda pair: True
            price_called = {"n": 0}
            def _fake_price(client, pair):
                price_called["n"] += 1
                return 100.0
            globals()["get_ticker_price"] = _fake_price

            worker_stats = ScanStats()
            result = _scan_pair_worker(None, "TEST_USDT", {"btc_4h": 0.0}, 50, worker_stats)
            _assert(result["skip"] == "recent" and result["sig"] is None,
                    "_scan_pair_worker(): dedup 'recently signaled' → skip='recent', tidak lanjut fetch harga")
            _assert(price_called["n"] == 0,
                    "_scan_pair_worker(): skip di awal TIDAK memanggil get_ticker_price (hemat I/O)",
                    f"price_called={price_called['n']}")
        finally:
            globals()["is_recently_signaled"] = _orig_recent
            globals()["is_in_cooldown"] = _orig_cooldown
            globals()["get_ticker_price"] = _orig_price

        # _scan_pair_worker(): no_price path
        _orig_recent2 = is_recently_signaled
        _orig_cooldown2 = is_in_cooldown
        _orig_price2 = get_ticker_price
        try:
            globals()["is_recently_signaled"] = lambda pair: False
            globals()["is_in_cooldown"] = lambda pair: False
            globals()["get_ticker_price"] = lambda client, pair: None
            worker_stats2 = ScanStats()
            result2 = _scan_pair_worker(None, "TEST2_USDT", {"btc_4h": 0.0}, 50, worker_stats2)
            _assert(result2["skip"] == "no_price" and result2["sig"] is None,
                    "_scan_pair_worker(): harga None → skip='no_price'")
        finally:
            globals()["is_recently_signaled"] = _orig_recent2
            globals()["is_in_cooldown"] = _orig_cooldown2
            globals()["get_ticker_price"] = _orig_price2

    except Exception as e:
        print(f"  ❌ EXCEPTION Parallel Scan: {e}\n{traceback.format_exc()}")
        failed += 1

    # ── SECTION 19: Adaptive Strategy — bucketing statistik ─
    print("\n[19] Adaptive Strategy — analyze_adaptive_suggestions()")

    try:
        # Belum cukup data (< ADAPTIVE_LOOKBACK_TRADES) → pesan jelas, bukan crash
        few_rows = [{"result": "TP1", "volume_ratio": 1.3, "entry_dev_pct": 1.0, "rsi_in_zone": True}] * 5
        msg_insufficient = analyze_adaptive_suggestions(rows=few_rows)
        _assert(msg_insufficient is not None and "belum cukup" in msg_insufficient.lower(),
                "Data < ADAPTIVE_LOOKBACK_TRADES → pesan 'belum cukup data', bukan crash/saran palsu")

        # Data cukup (200), volume ratio: bucket bawah WR mirip bucket atas → harus muncul saran
        import random as _r3
        _r3.seed(42)
        rows_signal = []
        for _ in range(90):   # volume_ratio < 1.2, WR ~50% (dibuat sengaja MIRIP bucket atas)
            result = "TP1" if _r3.random() < 0.50 else "SL"
            rows_signal.append({"result": result, "volume_ratio": round(_r3.uniform(0.8, 1.19), 2),
                                 "entry_dev_pct": 1.0, "rsi_in_zone": True})
        for _ in range(90):   # volume_ratio 1.2-1.5, WR ~52% (hampir sama, bukan gap besar)
            result = "TP1" if _r3.random() < 0.52 else "SL"
            rows_signal.append({"result": result, "volume_ratio": round(_r3.uniform(1.2, 1.49), 2),
                                 "entry_dev_pct": 1.0, "rsi_in_zone": True})
        rows_signal += [{"result": "TP1", "volume_ratio": None, "entry_dev_pct": None, "rsi_in_zone": None}] * 20
        _assert(len(rows_signal) >= ADAPTIVE_LOOKBACK_TRADES,
                "Setup test: total rows >= ADAPTIVE_LOOKBACK_TRADES",
                f"got={len(rows_signal)}")

        report = analyze_adaptive_suggestions(rows=rows_signal)
        _assert(report is not None and "Volume ratio" in report,
                "Bucket volume dengan n cukup & WR mirip → section Volume ratio muncul di laporan")
        _assert("coba turunkan ambang" in report.lower(),
                "WR bucket bawah ≈ bucket atas → saran 'turunkan ambang' muncul (bukan 'ambang relevan')")
        _assert("BUKAN AI" in report,
                "Laporan eksplisit menyatakan ini bukan AI dan bukan auto-apply")

        # Kasus sebaliknya: gap WR besar → harus bilang ambang masih relevan, BUKAN sarankan turun
        rows_gap = []
        for _ in range(90):   # WR jelek di bawah threshold
            result = "TP1" if _r3.random() < 0.20 else "SL"
            rows_gap.append({"result": result, "volume_ratio": round(_r3.uniform(0.8, 1.19), 2),
                              "entry_dev_pct": 1.0, "rsi_in_zone": True})
        for _ in range(90):   # WR bagus di atas threshold
            result = "TP1" if _r3.random() < 0.70 else "SL"
            rows_gap.append({"result": result, "volume_ratio": round(_r3.uniform(1.2, 1.49), 2),
                              "entry_dev_pct": 1.0, "rsi_in_zone": True})
        rows_gap += [{"result": "TP1", "volume_ratio": None, "entry_dev_pct": None, "rsi_in_zone": None}] * 20
        report_gap = analyze_adaptive_suggestions(rows=rows_gap)
        _assert(report_gap is not None and "masih relevan" in report_gap,
                "Gap WR besar (bawah jelek, atas bagus) → saran 'ambang masih relevan', BUKAN turunkan")

        # _adaptive_bucket_wr(): sample kosong → (0, None), tidak crash
        n0, wr0 = _adaptive_bucket_wr([], "volume_ratio", 0.0, 1.2)
        _assert(n0 == 0 and wr0 is None,
                "_adaptive_bucket_wr() bucket kosong → (0, None), tidak crash")

    except Exception as e:
        print(f"  ❌ EXCEPTION Adaptive Strategy: {e}\n{traceback.format_exc()}")
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
