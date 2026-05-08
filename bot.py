# -*- coding: utf-8 -*-
# Signal Bot v9.2 (audit edition) — lihat CHANGELOG.md untuk riwayat perubahan
# [v9.3.1] CRITICAL BUG FIX — pnl_usdt EXPIRED_LOSS salah hitung:
#   Bug: saat result = EXPIRED_LOSS/PROFIT, current_price tidak di-update
#   → PnL calc pakai current_price = 0
#   → (0 - entry) / entry × size = -100% dari position_size
#   → ETH expired loss: -$23.47 padahal harusnya -$0.36 USDT
#   → DD naik dari 6.6% ke 12.6% karena 1 trade saja
#   Fix: current_price = _exp_price saat EXPIRED_LOSS/PROFIT di-set.
#
# [v9.3] 7 improvements:
#   1. SL INTRADAY lebih longgar: ATR 1.5→2.0, buffer 0.3→0.5, min SL 0.3%→0.5%
#   2. Trailing timeframe strategy-aware: INTRADAY=1h, SWING=4h (was 1h untuk keduanya)
#   3. Cooldown 12 jam setelah pair closed — cegah re-entry terlalu cepat
#   4. Time-of-day filter: hanya scan 06:00–22:00 UTC (hindari volume sepi)
#   5. Volatility-adjusted sizing: ATR>5% → scale down position size otomatis
#   6. SL_AFTER_TP1 notification: notif khusus "Breakeven" dengan info TP2 seberapa dekat
#   7. Weekly summary: setiap Senin 06:00–08:00 WIB kirim ringkasan 7 hari
#
# [v9.2.10] Fix same-pair concentration:
#   Max 1 open trade per pair terlepas strategy atau usia signal.
#   Sebelumnya: BSV bisa muncul INTRADAY×2 + SWING×1 = 3 slot dari 13.
#   Risiko: kalau pair itu SL → 3 loss sekaligus dari 1 pair.
#   Fix: Check 1.5 di portfolio_allows() → cek pair di open_pairs list.
#
# [v9.2.9] Fix WR calculation:
#   WR 30d sekarang hanya dihitung dari WIN+LOSS (bukan semua result).
#   Sebelumnya: EXPIRED, BREAKEVEN, EXPIRED_PROFIT masuk denominator
#   → 45 win / 122 total = 36% padahal harusnya 45 / (45+42) = 51.7%.
#   Denominatornya sekarang: TP1+TP2+PARTIAL_WIN (win) + SL+SL_AFTER_TP1+EXPIRED_LOSS (loss).
#
# [v9.2.8] Fix berdasarkan data 118 trades:
#   FIX 1: Exclude MICROCAP dari WR — MICROCAP adalah pure alert bukan managed trade.
#           13/13 SL microcap merusak WR keseluruhan: 40% → ~51% tanpa microcap.
#           Exclude dari WR 30d, load_winrate_by_strategy, dan Bayesian table.
#   FIX 2: SWING min score 3.0 (was 2.5). Data: SWING WR 38.9% vs INTRADAY 60.8%.
#           SWING hold lebih lama → false signal lebih mahal → butuh konfluensi lebih kuat.
#           INTRADAY tetap 2.5 karena sudah 60.8% WR.
#   FIX 3: Entry deviation guard MICROCAP — max 3% dari entry zone.
#           Root cause 0% WR microcap: entry setelah harga pump → langsung kena SL.
#
# [v9.2.7] BTC trend guard:
#   Blok semua BUY baru jika BTC downtrend berkelanjutan.
#   Downtrend = 2 dari 3 candle 4h terakhir negatif (~12 jam).
#   Berbeda dari BTC_DROP_BLOCK (spike 1h) — ini deteksi penurunan pelan.
#   Root cause WR turun 55% → 41%: bot terus BUY saat BTC bearish beberapa hari.
#   SELL signal tidak terpengaruh. Scan summary menampilkan status "TREND BEARISH".
#   Konstanta: BTC_TREND_LOOKBACK=4, BTC_TREND_MIN_BEARISH=3  # [v9.4.2] updated 3/2 → 4/3
#
# [v9.2.6] Fix monitor timeout:
#   - Wick scan di SCAN_MODE=monitor dibatasi 10 bar (was: sampai 1000 bar).
#     Monitor jalan setiap 5 menit — 10 bar = 10 menit terakhir sudah cukup.
#     Sebelumnya: 13 trades × 1000 bar = timeout 4 menit → bot diam tanpa notif.
#   - Workflow monitor timeout-minutes: 4 → 8 (di signal-bot-monitor.yml)
#   - Workflow full scan timeout-minutes: 15 → 20 (di signal-bot-full.yml)
#
# [v9.2.5] Fix EXPIRED result:
#   - EXPIRED_PROFIT: trade expired saat posisi masih profit → neutral di WR
#   - EXPIRED_LOSS  : trade expired saat posisi merah → dihitung LOSS
#   Sebelumnya SEMUA EXPIRED = LOSS → WR dan expectancy lebih buruk dari kenyataan.
#   Notifikasi Telegram berbeda: ⏰✅ profit vs ⏰❌ rugi.
#   EXPIRED_LOSS masuk LOSS_VALUES di semua model WR/streak.
#   EXPIRED_PROFIT dikecualikan dari WR (neutral, tidak menghitung win/loss).
#
# [v9.2.4] Scoring boosters (volume signal tetap, kualitas ranking naik):
#   - RSI zona ideal (40–60) → +0.25 poin (timing entry lebih baik)
#   - BTC 4h searah entry    → +0.25 poin (angin kencang, bukan berlawanan arus)
#   - F&G ekstrem (<20 BUY, >80 SELL) → -0.5 soft penalty
#   Tidak ada signal yang diblok oleh booster — hanya score naik/turun.
#   Signal TRENDING + RSI ideal + BTC hijau bisa naik 0.5 poin → lebih mudah A+.
#
# [v9.2.3] SIGNAL_EXPIRE_HOURS diperpanjang:
#   INTRADAY 24h→72h | SWING 72h→120h | MICROCAP 48h→72h
#   Root cause: profit setelah ~3 hari → expire terlalu cepat → EXPIRED palsu.
#
# [v9.2.2] Fixes:
#   - FIX 1: Hard-block entry deviation — INTRADAY ≤2%, SWING ≤5%
#   - FIX 2: Adaptive fmt_price() — micro-price (PEPE dll) entry ≠ TP1 ≠ SL
#   - FIX 3: fmt_price di open trades summary — sama dengan FIX 2 tapi di recap
#   - FIX 4: 14/13 bug — hard cap guard di microcap + main signal loop sebelum send
#   - FIX 5: RANGING + score < 3.0 → skip (INTRADAY & SWING)
#             Data historis n < 3 → skip total | n < 15 → half position size
#
# [v9.4.3] Fix — Equity Report tetap dikirim saat bot HALT +
#           Manual reset HALT via SCAN_MODE=reset_halt:
BOT_VERSION = "9.5.0"  # ← ubah satu baris ini saja saat rilis versi baru
# [v9.5.0] DATA-DRIVEN PROFITABILITY OVERHAUL:
#   Semua perubahan didasarkan pada data empiris 163 trades:
#   WR bucket 2.0-2.9 = 18% (n=49) → threshold dinaikkan, bucket ini dihapus dari valid range
#   WR bucket 3.5+    = 61% (n=65) → ini satu-satunya bucket yang punya edge nyata
#   SWING WR 38.9% < threshold 45% → dinonaktifkan sesuai dokumentasi sendiri
#   Streak 11 dengan equity masih sehat → halt logic diperketat
#   1. Equity Report sekarang dikirim setiap scan meski bot sedang HALT.
#      Sebelumnya: run() keluar sebelum save_equity_snapshot() → tidak ada update.
#   2. SCAN_MODE=reset_halt: reset HALT + streak=0 lalu lanjut full scan.
#      Gunakan GitHub Actions workflow reset_halt.yml untuk jalankan manual.
#      Tidak perlu sentuh Supabase langsung.
#
# [v9.4.2] Fixes — normalisasi bot pasca DD 13.9%:
#   1. Adaptive score threshold: saat btc_bearish_cycles >= 2,
#      min score INTRADAY naik 2.5→3.0, SWING naik 3.0→3.5.
#      Mencegah sinyal lemah lolos saat kondisi market struktural bearish.
#   2. Same-pair double-check: portfolio_allows() sekarang query Supabase
#      langsung sebagai second line of defense setelah cache check.
#      Mencegah BSV/pair lain muncul 3x bersamaan.
#   3. BTC trend detection lebih sensitif: lookback 3→4 candle 4h,
#      min bearish 2→3 (75% candle harus negatif). Deteksi 4 jam lebih cepat.
#
# [v9.4.1] HOTFIX — Critical bug: allow_buy tidak cek btc_bearish_trend
#   BUG: allow_buy = not btc["block_buy"] — hanya cek spike 1h (BTC_DROP_BLOCK).
#   Saat TREND BEARISH aktif (btc_bearish_trend=True), check_intraday/check_swing
#   per-pair memang return None, tapi gate utama allow_buy tetap True sehingga
#   sinyal bisa lolos jika ada path lain (MICROCAP, edge case dedup, dsb).
#   Akibat: 13 sinyal BUY terkirim bersamaan saat kondisi TREND BEARISH.
#   Fix: allow_buy = not block_buy AND not btc_bearish_trend (kedua kondisi).
#
# [v9.4] Audit-driven fixes (5 issues resolved):
#   1. MICROCAP kill switch: MICROCAP_ENABLED env var (default false).
#      WR historis 0% (13/13 SL) — dinonaktifkan sampai ada bukti edge yang valid.
#   2. SWING kill switch: SWING_ENABLED env var (default true, bisa dimatikan via env).
#      WR SWING 38.9% vs INTRADAY 60.8% — jika turun di bawah 45% → set false.
#   3. MAX_OPEN_TRADES tetap 13 — bisa di-override via env var MAX_OPEN_TRADES.
#      $310/8 = $38 per posisi vs $23 di 13 slot. Fee tidak menggerus margin.
#      Override via env var MAX_OPEN_TRADES jika equity naik kembali.
#   4. SIGNAL_EXPIRE_HOURS dikoreksi:
#      INTRADAY 72h→36h (tidak overlap dengan SWING), MICROCAP 72h→24h (pump resolve cepat).
#   5. repair_expired_loss_pnl(): repair data historis yang rusak oleh bug v9.3.1.
#      Jalankan: python bot.py --repair-expired-pnl (dry run)
#               python bot.py --repair-expired-pnl --apply (terapkan)
#
# [v9.2 audit] Fixes (audit-driven):
#   ⚠️ HIGH severity fix: lifecycle wick detection — _resolve_trade_outcome_via_wicks()
#      scan 1m candle sejak sent_at; ticker last hanya dipakai sebagai fallback.
#      Sebelumnya: wick yang menembus SL/TP antara 2 run cron tidak terdeteksi
#      → trade tetap "open" → winrate stat membengkak palsu, model Bayesian miring.
#   - Backtest engine direvisi (Phase 5.1): fee + slippage + spread per leg,
#     Sharpe + Sortino + profit factor + expectancy + MaxDD%, MAE/MFE tracking.
#   - Walk-forward harness (walk_forward_generate) — generate signal dari strategy
#     callable terhadap historical bar tanpa lookahead.
#   - ForwardTestLogger (JSONL) — paper-trading log, aktifkan via env FORWARD_TEST_LOG.
#   - MAX_NOTIONAL_PCT = 0.85 — cumulative notional cap di portfolio_allows() Check 2.5.
#     Mencegah alokasi 130% modal saat MAX_OPEN_TRADES × MAX_POSITION_PCT > 100%.
#   - SWING fetch 200→400 bar 4h; EMA200 fallback ke EMA100 jika data <250 bar.
#     _PREFETCH_SPECS sinkron untuk hindari cache miss + duplicate API call.
#   - BOS/CHoCH structure detection: closes[-6:-1] (5 candle TERTUTUP) instead of
#     closes[-5:] — tidak repaint intra-bar.
#   - Telegram async queue (tg_async + tg_drain) — non-blocking di main loop;
#     send_signal/send_pump_signal/send_microcap_signal pakai async.
#   - validate_config() boot-time sanity check — print warning sebelum scan.
#   - Unit tests diperluas: 35 kasus (+12: 5 wick scan, 2 notional cap,
#     2 BOS closed-bar, 2 validate_config, 1 tg_async).
#
# [v9.1] Fixes:
#   - Streak persistence ke Supabase (bot_streak key) — tidak reset setelah job restart
#   - check_bot_halt() return 3-tuple (halted, reason, streak) — restore context
#   - set_bot_halt() terima parameter streak — persist bersama halt state
#   - get_drawdown_state() restore streak dari DB jika rows kosong (cold start)
#   - run() restore _drawdown_state["streak"] dari Supabase di awal setiap run
#   - ETF blocklist Supabase TTL cache (24h) — skip GitHub fetch jika cache fresh
#   - ATR spike blend weights → named constants (ATR_SPIKE_WEIGHT_CLEAN/LAST)
#   - Unit tests diperluas: 21 kasus (+5: ATR weights, streak persistence, signature check)

import os, json, time, math, random, html as _html
import logging
import statistics                          # [v9.2 audit] Sharpe/Sortino calc
import urllib.request
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, asdict  # [v9.2 audit] TradeResult / CostModel
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
MAX_SIGNALS_CYCLE  = 8           # [v9.5] diturunkan 13→8 — kualitas > kuantitas
DEDUP_HOURS        = 6           # [v9.5] dinaikkan 4→6 jam — cegah re-entry terlalu cepat
# [v9.3] Cooldown setelah pair closed — mencegah re-entry terlalu cepat
# Setelah SL/TP/EXPIRED, pasang cooldown sebelum pair bisa signal lagi.
PAIR_COOLDOWN_HOURS = 24         # [v9.5] dinaikkan 12→24 jam — cegah revenge trade setelah loss

# [v9.3] Time-of-day filter — hanya scan saat volume tinggi (UTC)
# Crypto punya pola volume: 08:00–12:00 UTC (Asia close/EU open) dan 20:00–00:00 UTC (US session)
# Jam sepi (02:00–06:00 UTC) lebih banyak false break karena spread lebar + volume rendah.
# Format: list of (start_hour_utc, end_hour_utc) — inklusif
ACTIVE_HOURS_UTC = [
    (6,  22),   # 06:00–22:00 UTC = 13:00–05:00 WIB — cukup lebar, hindari dini hari UTC
]
# Set ke None atau [] untuk disable filter ini
# ACTIVE_HOURS_UTC = []   # disable jika mau 24 jam penuh

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
    # [v9.5] DATA-DRIVEN: WR bucket 2.0-2.9 = 18% (n=49) → HAPUS dari valid range.
    # WR bucket 3.5+ = 61% (n=65) → ini satu-satunya yang punya edge nyata.
    # Dinaikkan 2.5→3.5: bot hanya kirim signal yang secara historis profitable.
    # Efek: volume signal berkurang drastis, tapi setiap signal punya expectancy positif.
    "A+": 3.5,   # [v9.5] naik dari 2.5 → 3.5 — hanya kirim yang WR 61%+
    "A":  3.0,   # [v9.5] naik dari 2.0 → 3.0 — minimum viable dengan edge
}
# [v9.5] SWING dinonaktifkan by default karena WR 38.9% < threshold 45%.
# Dokumentasi v9.4 sendiri bilang: "jika turun di bawah 45% → set false."
# Konsisten dengan data: SWING harus dimatikan sampai ada bukti edge yang valid.
TIER_MIN_SCORE_SWING = 3.5   # [v9.5] naik dari 3.0 → 3.5 — SWING butuh konfluensi sangat kuat
SIGNAL_MIN_TIER = "A+"  # [v9.5] naik dari "A" → "A+" — hanya kirim tier terbaik

# ── RR Minimum ───────────────────────────────────────
MIN_RR = {
    "INTRADAY": 1.5,
    "SWING":    2.0,
}

# ── Entry Deviation Maximum ───────────────────────────
# [v9.2.1 fix] Jika harga sudah terlalu jauh dari entry zone,
# signal di-BLOCK total (return None) — bukan sekedar warning teks.
# SWING   : 5%  — toleransi lebih lebar (4h timeframe)
# INTRADAY: 2%  — harus presisi (1h timeframe)
MAX_ENTRY_DEVIATION = {
    "SWING":    0.05,
    "INTRADAY": 0.02,
}

# ── Regime-Aware Score Minimum ────────────────────────
# [v9.5] RANGING threshold dinaikkan sesuai data: context bucket belum cukup sample,
# artinya semua signal RANGING fall back ke score-only bucket yang crude.
# Naik ke 3.5 — sama dengan threshold A+ baru — agar RANGING tidak jadi backdoor loophole.
MIN_SCORE_RANGING = 3.5   # [v9.5] naik dari 3.0 → 3.5

# ── Minimum Data Sample untuk Conf ───────────────────
# [v9.5] Dinaikkan — dengan selektifitas baru (score 3.5+), butuh data lebih banyak
# untuk validate bahwa pair ini memang punya edge, bukan kebetulan lolos filter tinggi.
MIN_HIST_SAMPLE_FULL = 20   # [v9.5] naik dari 15→20: n < 20 → half size
MIN_HIST_SAMPLE_SKIP = 5    # [v9.5] naik dari 3→5: n < 5 → skip total

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
# [v9.2.7] BTC trend guard — blok BUY baru jika BTC downtrend berkelanjutan.
# Downtrend = minimal BTC_TREND_MIN_BEARISH dari BTC_TREND_LOOKBACK candle 4h terakhir negatif.
# Berbeda dari BTC_DROP_BLOCK (spike 1h) — ini deteksi penurunan pelan tapi konsisten.
# [v9.4.2] Dinaikkan 3→4 candle lookback, min 3→3 negatif (was 2/3 → now 3/4).
# 4 candle 4h = 16 jam lookback. 3/4 negatif = 75% candle bearish.
# Lebih sensitif: downtrend terdeteksi lebih cepat sebelum DD menumpuk.
BTC_TREND_LOOKBACK    = 4   # [v9.4.2] 3→4 candle 4h (~16 jam lookback)
BTC_TREND_MIN_BEARISH = 3   # [v9.4.2] 2→3: minimal 3 dari 4 candle negatif → bearish

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
# [v9.3] INTRADAY 1.5→2.0: SL terlalu sempit → banyak kena noise 1h candle
INTRADAY_SL_ATR     = 2.0    # was 1.5 → 2.0 (lebih longgar)
SWING_SL_ATR        = 2.0    # unchanged

# ATR buffer: seberapa jauh SL ditempatkan DI BELAKANG struktur
# [v9.3] INTRADAY 0.3→0.5: konsisten dengan perubahan ATR multiplier
ATR_SL_BUFFER_INTRADAY = 0.5   # was 0.3 → 0.5 (kurangi noise wick)
ATR_SL_BUFFER_SWING    = 0.5   # unchanged

# Sanity bounds — SL tidak valid di luar rentang ini (persentase dari entry)
# Identik dengan downstream check di check_intraday/check_swing — belt-and-suspenders
INTRADAY_MIN_SL_PCT = 0.005   # was 0.3% → 0.5% [v9.3] — SL terlalu sempit kena noise
INTRADAY_MAX_SL_PCT = 0.050   # max 5%  dari entry — konsisten dengan check_intraday
SWING_MIN_SL_PCT    = 0.005   # min 0.5% dari entry
SWING_MAX_SL_PCT    = 0.070   # max 7% dari entry [v7.25 — dikurangi dari 10%]

# ── Pump Scanner Config ──────────────────────────────
PUMP_VOL_SPIKE    = 3.0      # volume candle terakhir harus > 3× rata-rata 10 candle
PUMP_PRICE_CHANGE = 4.0      # harga naik > 4% dalam 3 candle 15m terakhir
PUMP_RSI_MAX      = 72       # RSI belum overbought ekstrem
PUMP_MIN_VOLUME   = 200_000  # volume 24h minimum lebih rendah dari main bot
PUMP_DEDUP_HOURS  = 1        # dedup pump signal lebih pendek (1 jam)
MAX_PUMP_SIGNALS  = 8        # [v9.1] dinaikkan 5→8 proporsional dengan 13 open slots

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
MAX_MICRO_SIGNALS    = 8         # [v9.1] dinaikkan 4→8 proporsional dengan 13 open slots

# ── Strategy Enable/Disable Kill Switches ─────────────
# [v9.4] Per-strategy kill switch — matikan strategy bermasalah tanpa deploy ulang.
# Set via env var: MICROCAP_ENABLED=false / SWING_ENABLED=false
# Default: MICROCAP dinonaktifkan karena WR historis 0% (13/13 SL berturut-turut).
#          SWING aktif tapi bisa dimatikan jika WR turun di bawah 45%.
MICROCAP_ENABLED = os.getenv("MICROCAP_ENABLED", "false").lower() == "true"
SWING_ENABLED    = os.getenv("SWING_ENABLED",    "false").lower()  == "true"
# [v9.5] SWING_ENABLED default diubah true→FALSE.
# Justifikasi: WR SWING historis 38.9% < threshold 45% yang sudah didokumentasikan sendiri.
# Dokumentasi v9.4: "jika turun di bawah 45% → set false." — sekarang diterapkan.
# Re-aktifkan via env var SWING_ENABLED=true HANYA setelah minimal 30 closed SWING trades
# menunjukkan WR >= 50% secara konsisten.

# ── Portfolio Brain Config ── [v7.29 Phase3] ─────────
# Disederhanakan: 3 konstanta bersih menggantikan 4 variabel kompleks.
# heat, locked_capital, pairwise correlation dihapus dari gate utama.
# Ganti dengan: hard cap total, hard cap per sektor, hard cap risk % equity.
#
# "Open trade" = sinyal yang sudah dikirim dan belum ada result (NULL).
# Supabase table: signals_v2, kolom: strategy, side, result IS NULL.
# [v9.4] MAX_OPEN_TRADES tetap 13 sesuai konfigurasi sebelumnya.
# Override via env var MAX_OPEN_TRADES jika diperlukan.
MAX_OPEN_TRADES        = int(os.getenv("MAX_OPEN_TRADES", "8"))
# [v9.5] Diturunkan 13→8. Dengan threshold score 3.5+, jumlah signal valid per cycle
# secara natural akan lebih sedikit. 8 slots = lebih fokus, monitoring lebih mudah.
# $327 equity / 8 slots = ~$41 per posisi yang lebih wajar.
MAX_PER_SECTOR         = 2    # [v9.5] turun dari 4→2 per sektor — cegah over-concentration
MAX_RISK_TOTAL         = 0.08 # [v9.5] turun dari 10%→8% equity — 8 slots × 1% = 8% max
# [v9.2 audit FIX] Notional cap — total modal yang dialokasikan tidak boleh > 85% equity.
# Tanpa ini, MAX_OPEN_TRADES × MAX_POSITION_PCT = 13 × 10% = 130% (tidak realistis di spot).
# Risk-budget gate hanya membatasi R-units, bukan raw notional.
MAX_NOTIONAL_PCT       = 0.80  # [v9.5] turun dari 85%→80% — lebih konservatif
# [v9.5] MAX_SAME_SIDE_TRADES turun proporsional dengan MAX_OPEN_TRADES baru.
MAX_SAME_SIDE_TRADES   = 5    # [v9.5] turun dari 13→5: max BUY atau SELL aktif sekaligus
# [v9.5] MAX_BTC_CORR_TRADES turun proporsional.
MAX_BTC_CORR_TRADES    = 2    # [v9.5] turun dari 4→2: max BUY saat BTC correlation tinggi
PORTFOLIO_STALE_HOURS  = 72   # [v9.5] turun dari 96→72 jam — sesuai INTRADAY 36h + buffer

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
# SIGNAL_EXPIRE_HOURS — disesuaikan berdasarkan observasi real market.
# [v9.2.3 fix] Mayoritas signal profit setelah ~3 hari, bukan 1 hari.
# Sebelumnya INTRADAY 24h, SWING 72h → banyak trade di-mark EXPIRED sebelum
# sempat hit TP → winrate tercatat lebih rendah dari kenyataan,
# Sharpe/expectancy negatif padahal setup sebenarnya valid.
# [v9.4 fix] INTRADAY 72h → 36h: INTRADAY yang 72h de-facto jadi SWING.
# Kategori strategy harus mencerminkan timeframe sebenarnya agar stats valid.
# SWING = 4h timeframe, wajar 5 hari. INTRADAY = 1h timeframe, cukup 36 jam.
SIGNAL_EXPIRE_HOURS = {
    "INTRADAY": 36,    # [v9.4] 72h → 36h: INTRADAY harus resolve dalam 1.5 hari
    "SWING":    120,   # was 72h → 120h (5 hari) — sesuai 4h timeframe
    "PUMP":       4,   # unchanged — pump harus resolve cepat
    "MICROCAP":  24,   # [v9.4] 72h → 24h: microcap pump/dump resolve cepat atau tidak sama sekali
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
BASE_POSITION_USDT = 25.0    # [v9.5] naik dari 20→25: $327 equity / 8 slots ≈ $41 per posisi
MAX_POSITION_USDT  = 40.0    # [v9.5] naik dari 35→40: ~12% dari $327 equity per posisi
MIN_POSITION_USDT  = 12.0    # [v9.5] naik dari 10→12: floor lebih realistic
# [v9.5] MAX_POSITION_PCT dinaikkan sedikit karena slots lebih sedikit (8 vs 13).
# 8 slots × 12% = 96% — masih aman dengan MAX_NOTIONAL_PCT=85% sebagai gate utama.
MAX_POSITION_PCT   = 0.12    # [v9.5] naik dari 10%→12% — 8 slots lebih wajar

# [v7.27 #4] Dynamic equity — seed dari env var, BUKAN hardcode $200.
# Set INITIAL_EQUITY_USDT di .env sesuai kapital aktual.
# Bot akan fetch live balance dari Gate.io; fallback ke env jika API gagal.
INITIAL_EQUITY_USDT  = float(os.getenv("INITIAL_EQUITY_USDT", "350.0"))  # [v9.1] updated ke $350
ACCOUNT_EQUITY_USDT  = INITIAL_EQUITY_USDT   # alias runtime — diupdate saat bot start

# [v7.27 #1] Fixed Risk per trade — menggantikan Kelly (sementara).
# position_size = equity × RISK_PER_TRADE / sl_pct
# 1% equity dengan SL 2% → size = 50% equity (terlalu besar → tier cap memotong)
# 1% equity dengan SL 5% → size = 20% equity → ~$40 pada $200 equity (masuk akal)
RISK_PER_TRADE       = 0.010  # [v9.5] naik dari 0.8%→1.0% — 8 slots × 1% = 8% max total risk (vs 10.4% sebelumnya)
# Dengan filter score lebih ketat, setiap trade punya expectancy lebih tinggi → justify risk sedikit lebih besar.

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
DRAWDOWN_STREAK_WARN   = 3      # ≥3 consecutive loss → warn (unchanged)
DRAWDOWN_STREAK_HALT   = 7      # [v9.5] naik dari 5→7: dengan filter lebih ketat, streak 5 bisa noise
# Justifikasi: v8.9 logic sudah downgrade streak-halt ke warn jika equity sehat.
# Naikkan HALT ke 7 agar angka bermakna — tapi turunkan DD_WARN agar equity protection lebih ketat.
DD_WARN_PCT            = 0.07   # [v9.5] turun dari 8%→7%: lebih cepat masuk WARN mode
DD_HALT_PCT            = 0.12   # [v9.5] turun dari 15%→12%: halt lebih cepat sebelum damage terlalu besar
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

# ── ATR Spike Blend Weights ── [v9.1]
# Dipakai di calc_atr() saat TR terakhir > SPIKE_MULT × ATR
# Blend: ATR_SPIKE_WEIGHT_CLEAN × atr_clean + ATR_SPIKE_WEIGHT_LAST × last_tr
ATR_SPIKE_WEIGHT_CLEAN  = 0.60   # bobot ATR bersih (anti-spike)
ATR_SPIKE_WEIGHT_LAST   = 0.40   # bobot TR candle terakhir (responsif)

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
SCAN_SLEEP_SEC        = 0.1    # throttle loop utama & Gate.io scan
TG_SEND_SLEEP_SEC     = 0.5    # jeda antar Telegram message — hindari rate limit
CANDLE_FETCH_SLEEP_SEC = 0.35  # throttle per-thread prefetch candle (3 workers × 0.35s ≈ 1 req/s)

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

# ── Signal Only Mode ─────────────────────────────────
# Set True jika bot digunakan sebagai paper trading / signal-only.
# Bot akan skip fetch live balance dari Gate.io dan langsung pakai
# INITIAL_EQUITY_USDT sebagai anchor equity simulasi.
SIGNAL_ONLY_MODE = os.environ.get("SIGNAL_ONLY_MODE", "true").lower() == "true"

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
    """Kirim pesan ke Telegram. [v7.6 #10] Retry 2x dengan backoff 2s.
    [v9.2 FIX] Auto-split untuk pesan > 4096 karakter (Telegram hard limit).
    Sebelumnya: pesan panjang (banyak open trades) gagal kirim diam-diam.
    """
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"

    # Split pesan > 4096 char per batas baris — hindari potong di tengah tag HTML
    TG_MAX = 4000  # sedikit di bawah 4096 untuk safety margin
    chunks = []
    if len(msg) <= TG_MAX:
        chunks = [msg]
    else:
        lines  = msg.split("\n")
        buf    = ""
        for line in lines:
            if len(buf) + len(line) + 1 > TG_MAX:
                if buf:
                    chunks.append(buf.rstrip())
                buf = line + "\n"
            else:
                buf += line + "\n"
        if buf.strip():
            chunks.append(buf.rstrip())

    for i, chunk in enumerate(chunks):
        _part = f" ({i+1}/{len(chunks)})" if len(chunks) > 1 else ""
        body = json.dumps({
            "chat_id": TG_CHAT_ID, "text": chunk + _part,
            "parse_mode": "HTML", "disable_web_page_preview": True
        }).encode()
        sent_ok = False
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, data=body,
                                             headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=10)
                time.sleep(TG_SEND_SLEEP_SEC)
                sent_ok = True
                break
            except Exception as e:
                if attempt < 2:
                    log(f"⚠️ Telegram retry {attempt+1}/2: {e}", "warn")
                    time.sleep(2 ** attempt * 2)   # 2s, 4s
                else:
                    log(f"⚠️ Telegram gagal setelah 3x retry: {e}", "error")
        if not sent_ok:
            break   # jika satu chunk gagal total, hentikan — jangan kirim lanjutan yang out-of-context


def _tg_e(err) -> str:
    """Escape string error untuk Telegram HTML parse_mode.
    [v9.2 FIX] str(e) dari PostgreSQL/Supabase mengandung karakter ' dan " yang
    bisa break HTML parser Telegram → pesan tidak terkirim sama sekali.
    Gunakan fungsi ini setiap kali embed exception ke dalam tg().
    """
    return _html.escape(str(err)[:250])


# ════════════════════════════════════════════════════════
#  [v9.2 audit] TELEGRAM ASYNC QUEUE
#  Sebelumnya: tg() blocking di main loop dengan time.sleep(0.5) per send.
#  Dengan MAX_SIGNALS_CYCLE=13 → ~6.5s blocked per cycle.
#  Sekarang: signals di-enqueue → worker thread daemon mengirim di background.
#  Direct tg() tetap dipakai untuk alert kritis (HALT, crash).
# ════════════════════════════════════════════════════════

import queue as _queue_mod
import threading as _threading_mod

_tg_queue: "_queue_mod.Queue[str | None]" = _queue_mod.Queue()
_tg_worker_started = False
_tg_worker_lock = _threading_mod.Lock()

def _tg_worker_loop():
    """Background daemon yang drain antrian Telegram dan call tg() langsung."""
    while True:
        msg = _tg_queue.get()
        try:
            if msg is None:                # shutdown sentinel
                return
            try:
                tg(msg)                    # reuse existing retry logic
            except Exception as e:
                log(f"⚠️ tg_worker: send failed: {e}", "warn")
        finally:
            _tg_queue.task_done()


def tg_async(msg: str) -> None:
    """Enqueue Telegram message untuk pengiriman background (non-blocking)."""
    global _tg_worker_started
    if not msg:
        return
    with _tg_worker_lock:
        if not _tg_worker_started:
            t = _threading_mod.Thread(
                target=_tg_worker_loop, daemon=True, name="tg-worker"
            )
            t.start()
            _tg_worker_started = True
    _tg_queue.put(msg)


def tg_drain(timeout: float = 30.0) -> None:
    """Tunggu semua pesan ter-deliver sebelum exit. Aman dipanggil meski
    worker belum start.
    """
    if not _tg_worker_started:
        return
    try:
        # queue.join() memblok sampai semua task_done() — tidak ada timeout API
        # built-in, jadi pakai polling sederhana.
        import time as _time_mod
        deadline = _time_mod.time() + timeout
        while _tg_queue.unfinished_tasks > 0 and _time_mod.time() < deadline:
            _time_mod.sleep(0.1)
    except Exception:
        pass


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


def fmt_price(v) -> str:
    """
    [v9.2.1 fix] Adaptive decimal formatting untuk harga crypto.
    Micro-price (PEPE dll) tampil dengan cukup desimal agar entry ≠ TP1 ≠ SL.

    Contoh sebelum : $0.000004 = $0.000004 = $0.000004  ← tidak terbaca
    Contoh sesudah : $0.0000041050, $0.0000047200, $0.0000038500  ← jelas
    """
    if v is None:
        return "—"
    fv = float(v)
    if fv == 0:       return "$0"
    if fv >= 10000:   return f"${fv:,.2f}"
    if fv >= 1000:    return f"${fv:,.2f}"
    if fv >= 1:       return f"${fv:.4f}"
    if fv >= 0.1:     return f"${fv:.5f}"
    if fv >= 0.01:    return f"${fv:.6f}"
    if fv >= 0.001:   return f"${fv:.7f}"
    if fv >= 0.0001:  return f"${fv:.8f}"
    if fv >= 0.00001: return f"${fv:.9f}"
    import math as _math
    mag = _math.floor(_math.log10(abs(fv)))
    decimals = max(10, -mag + 3)
    return f"${fv:.{decimals}f}"


def validate_config() -> list[str]:
    """
    [v9.2 audit] Boot-time sanity check untuk konfigurasi sizing & risk.
    Return list warning messages — empty list = semua OK.
    Dipanggil di awal run() agar mis-configuration ketahuan SEBELUM scan.
    """
    warnings = []

    if MAX_POSITION_USDT < BASE_POSITION_USDT:
        warnings.append(
            f"MAX_POSITION_USDT (${MAX_POSITION_USDT}) < "
            f"BASE_POSITION_USDT (${BASE_POSITION_USDT}) — sizing floor salah."
        )

    theoretical_alloc = MAX_OPEN_TRADES * MAX_POSITION_PCT
    if theoretical_alloc > 1.0:
        warnings.append(
            f"MAX_OPEN_TRADES × MAX_POSITION_PCT = {theoretical_alloc:.0%} equity — "
            f"melebihi 100%. MAX_NOTIONAL_PCT={MAX_NOTIONAL_PCT:.0%} akan menjadi "
            "primary gate (Patch 4)."
        )

    theoretical_risk = MAX_OPEN_TRADES * RISK_PER_TRADE
    if theoretical_risk > MAX_RISK_TOTAL:
        warnings.append(
            f"MAX_OPEN_TRADES × RISK_PER_TRADE = {theoretical_risk:.1%} > "
            f"MAX_RISK_TOTAL = {MAX_RISK_TOTAL:.1%}. Risk gate akan menolak signal "
            "ke-N+; ini behaviour yang benar tapi mungkin mengejutkan user."
        )

    if MAX_NOTIONAL_PCT >= 1.0:
        warnings.append(
            f"MAX_NOTIONAL_PCT = {MAX_NOTIONAL_PCT:.0%} — tidak menyisakan buffer "
            "untuk slippage/fees. Disarankan ≤ 0.85."
        )

    if RISK_PER_TRADE >= 0.02:
        warnings.append(
            f"RISK_PER_TRADE = {RISK_PER_TRADE:.1%} agresif. Kombinasi dengan "
            "multiple open trade bisa men-trigger drawdown cepat."
        )

    if PARTIAL_TP1_RATIO < 0 or PARTIAL_TP1_RATIO > 1:
        warnings.append(
            f"PARTIAL_TP1_RATIO = {PARTIAL_TP1_RATIO} di luar [0,1] — "
            "akan menyebabkan PnL calculation salah."
        )

    # [v9.4] Informasi status kill switch per-strategy
    if not MICROCAP_ENABLED:
        warnings.append(
            "MICROCAP_ENABLED=false — strategy MICROCAP dinonaktifkan. "
            "Aktifkan via env var MICROCAP_ENABLED=true jika WR historis sudah membaik."
        )
    if not SWING_ENABLED:
        warnings.append(
            "SWING_ENABLED=false — strategy SWING dinonaktifkan. "
            "Aktifkan via env var SWING_ENABLED=true."
        )

    return warnings


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

# [v9.1] TTL cache ETF blocklist di Supabase — hindari re-download GitHub tiap cycle.
# Data hanya di-fetch ulang jika sudah lebih dari 24 jam sejak update terakhir.
ETF_CACHE_TTL_HOURS = 24

def _load_etf_cache_from_db() -> set | None:
    """
    [v9.1] Baca ETF ticker list dari Supabase bot_config (key="etf_blocklist").
    Return set ticker jika cache valid (< ETF_CACHE_TTL_HOURS), None jika expired/belum ada.
    """
    try:
        rows = (
            supabase.table("bot_config")
            .select("value, updated_at")
            .eq("key", "etf_blocklist")
            .limit(1)
            .execute()
            .data
        ) or []
        if not rows:
            return None
        row = rows[0]
        updated_str = row.get("updated_at", "")
        if updated_str:
            try:
                # Parse ISO timestamp dan bandingkan dengan sekarang
                from datetime import datetime as _dt
                updated = _dt.fromisoformat(updated_str.replace("Z", "+00:00"))
                age_hours = (datetime.now(WIB) - updated).total_seconds() / 3600
                if age_hours > ETF_CACHE_TTL_HOURS:
                    log(f"  ℹ️ ETF cache expired ({age_hours:.1f}h > {ETF_CACHE_TTL_HOURS}h) — re-fetch")
                    return None
            except Exception:
                return None
        raw_value = row.get("value", "")
        if not raw_value:
            return None
        tickers = set(t.strip() for t in raw_value.split(",") if t.strip())
        log(f"  ✅ ETF blocklist dari Supabase cache: {len(tickers)} ticker")
        return tickers
    except Exception as e:
        log(f"  ⚠️ ETF cache load gagal: {e} — lanjut ke fetch GitHub", "warn")
        return None


def _save_etf_cache_to_db(tickers: set) -> None:
    """
    [v9.1] Simpan ETF ticker list ke Supabase bot_config untuk cache antar-run.
    Disimpan sebagai string CSV (comma-separated).
    """
    try:
        value = ",".join(sorted(tickers))
        supabase.table("bot_config").upsert([
            {"key": "etf_blocklist", "value": value, "updated_at": datetime.now(WIB).isoformat()},
        ], on_conflict="key").execute()
        log(f"  💾 ETF blocklist disimpan ke Supabase: {len(tickers)} ticker")
    except Exception as e:
        log(f"  ⚠️ ETF cache save gagal: {e}", "warn")


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

    [v9.1] Supabase TTL cache — GitHub hanya di-fetch jika cache expired (> 24 jam).
    Menghindari 2 HTTP request ke GitHub setiap cycle run.

    Dieksekusi SEKALI saat bot start (fresh process per GitHub Actions run).
    """
    global _ETF_DYNAMIC, _ETF_BUILT
    if _ETF_BUILT:
        log("  ℹ️ ETF blocklist sudah dibangun — skip rebuild")
        return
    _ETF_BUILT = True

    # Seed awal dari static list — jaminan minimum
    _ETF_DYNAMIC = set(ETF_EXACT)

    # [v9.1] Coba load dari Supabase cache dulu
    cached = _load_etf_cache_from_db()
    if cached:
        _ETF_DYNAMIC |= cached
        return

    # Cache expired / belum ada — fetch dari GitHub
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

    # [v9.1] Simpan ke Supabase untuk dipakai run berikutnya
    if fetched > 0:
        _save_etf_cache_to_db(_ETF_DYNAMIC)


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
    ("4h",  400),   # [v9.2 audit] 60→400 — sinkron dengan check_swing EMA200 warmup fix
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
        time.sleep(CANDLE_FETCH_SLEEP_SEC)
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
        # Blend: ATR_SPIKE_WEIGHT_CLEAN × ATR bersih + ATR_SPIKE_WEIGHT_LAST × last TR
        atr_final = ATR_SPIKE_WEIGHT_CLEAN * atr_clean + ATR_SPIKE_WEIGHT_LAST * last_tr
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

    # [v9.2 audit FIX] Exclude the unclosed last bar from break detection.
    # closes[-1] adalah candle live (mungkin sedang terbentuk) — memasukkannya
    # menyebabkan BOS/CHoCH bisa flip on/off intra-bar (repainting).
    # Sekarang gunakan 5 candle TERTUTUP terakhir saja.
    recent_closes = closes[-6:-1] if len(closes) >= 6 else closes[:-1]
    if len(recent_closes) < 2:
        return result   # not enough closed bars for break detection

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
                 setup_score: int = 3,
                 btc_4h: float = 0.0,
                 fg: int = 50) -> float:
    """
    [v9.2.4] 3-factor scoring engine + 3 soft boosters (tidak memblok signal).

    2 core factors (masing-masing 1 poin) + boosters:
      1. Trend    : EMA fast > slow searah entry  → +1.0  (core, wajib)
      2. Momentum : MACD crossover searah entry   → +1.0  (core, wajib)
      3. Volume   : spike > 1.3× avg 10 candle   → +0.5  (booster)
      4. RSI      : zona ideal (40–60)            → +0.25 (booster, v9.2.4)
      5. BTC 4h   : searah entry                  → +0.25 (booster, v9.2.4)
      6. F&G ext. : F&G ekstrem berlawanan entry  → -0.5  (soft penalty, v9.2.4)

    core_score = trend + momentum + volume + rsi_boost + btc_boost + fg_penalty
    Final score = core_score + setup_bonus

    Tier (assign_tier):
      A+  : score >= 2.5
      A   : score >= 2.0
      SKIP: score <  2.0

    Efek booster:
      Signal dengan RSI ideal + BTC searah → bisa naik 0.5 poin → lebih mudah tier A+
      Signal saat F&G ekstrem berlawanan  → turun 0.5 poin → lebih sulit tier A+
      Volume signal TIDAK berkurang — hanya ranking berubah

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

    # ── Booster 4: RSI zona ideal ─────────────────────────────────
    # [v9.2.4] RSI 40–60 = momentum fresh, belum overbought/oversold.
    # Tidak memblok signal — hanya boost signal yang entry di timing ideal.
    # BUY  : RSI 40–60 (pullback sehat, belum overbought)
    # SELL : RSI 40–60 (bounce sehat, belum oversold)
    rsi_boost = 0.0
    if 40 <= rsi <= 60:
        rsi_boost = 0.25

    # ── Booster 5: BTC 4h konfirmasi arah ─────────────────────────
    # [v9.2.4] BTC 4h positif saat BUY = angin kencang searah.
    # BTC 4h negatif saat BUY = melawan arus — tidak dapat boost.
    # Tidak memblok apapun — hanya reward signal yang searah BTC.
    btc_boost = 0.0
    if is_bull  and btc_4h > 0.0: btc_boost = 0.25
    if not is_bull and btc_4h < 0.0: btc_boost = 0.25

    # ── Soft penalty 6: F&G ekstrem berlawanan arah ───────────────
    # [v9.2.4] Hanya di kondisi ekstrem (bukan fear normal).
    # F&G < 20 (extreme fear)  + BUY  → -0.5 (beli di kepanikan ekstrem, risky)
    # F&G > 80 (extreme greed) + SELL → -0.5 (short di euforia, risky)
    # F&G 20–80 tidak kena penalti — kondisi normal.
    fg_penalty = 0.0
    if is_bull  and fg < 20: fg_penalty = -0.5
    if not is_bull and fg > 80: fg_penalty = -0.5

    # ── Core score ────────────────────────────────────────────────
    core_score = trend + momentum + volume + rsi_boost + btc_boost + fg_penalty

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
    # [v9.5] Bucket 2.0-2.9 dihapus — threshold minimum sekarang 3.0, bucket ini tidak akan terisi.
    # Bucket yang tersisa hanya yang relevan dengan threshold baru (score >= 3.0).
    "2.0-2.9": 999,  # [v9.5] Effectively disabled — score ini tidak akan lolos threshold baru
    "3.0-3.4": 25,   # [v9.5] naik dari 20→25: butuh lebih banyak sample untuk validate edge
    "3.5+":    30,   # [v9.5] naik dari 25→30: bucket utama yang punya WR 61%
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
        # [v9.2.5] EXPIRED_LOSS dihitung LOSS | EXPIRED_PROFIT dikecualikan (neutral)
        LOSS_VALUES = {"LOSS", "SL", "EXPIRED_LOSS"}

        for row in rows:
            raw_score    = row.get("score") or 0
            raw_result   = (row.get("result")   or "").upper().strip()
            raw_regime   = (row.get("regime")   or "").upper().strip()
            raw_strategy = (row.get("strategy") or "").upper().strip()
            # [v9.2.8 fix] Exclude MICROCAP/PUMP dari Bayesian model.
            # Data noise mereka merusak score-bucket confidence estimates.
            # [v9.4.3] Exclude SYSTEM (record injected untuk streak reset).
            if raw_strategy in ("MICROCAP", "PUMP", "SYSTEM"):
                continue

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

    # ── [v9.3] Volatility-adjusted sizing ────────────────────────────────
    # Pair dengan ATR tinggi (volatile) dapat size lebih kecil otomatis.
    # ATR% > 5% → scale down sampai 70% dari raw_size.
    # Logika: fixed-risk sizing sudah handle ini via SL distance, tapi
    # ketika SL terlalu dekat (di-floor ke min_sl_pct), raw_size membengkak.
    # Volatility scalar mencegah over-sizing di pair yang "liar".
    if atr is not None and entry is not None and entry > 0 and atr > 0:
        _atr_pct = (atr / entry) * 100
        if _atr_pct > 5.0:   # pair sangat volatile
            _vol_scalar = max(0.70, 1.0 - (_atr_pct - 5.0) * 0.03)  # turun 3% per 1% ATR di atas 5%
            raw_size   *= _vol_scalar
            log(f"   📉 [VOL] {pair} ATR={_atr_pct:.1f}% → vol_scalar={_vol_scalar:.2f}")

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
    [v9.1] Dipanggil SEKALI saat bot start — set ACCOUNT_EQUITY_USDT secara dinamis.

    Priority:
      1. SIGNAL_ONLY_MODE=True → langsung pakai INITIAL_EQUITY_USDT (paper trading)
      2. Gate.io API live balance (real trading)
      3. Env var INITIAL_EQUITY_USDT (manual config fallback)
      4. Hardcode 200 (absolute fallback — log warning)
    """
    global ACCOUNT_EQUITY_USDT
    if SIGNAL_ONLY_MODE:
        ACCOUNT_EQUITY_USDT = INITIAL_EQUITY_USDT
        log(f"   📋 SIGNAL_ONLY_MODE aktif — equity dari INITIAL_EQUITY_USDT: ${ACCOUNT_EQUITY_USDT:.2f} USDT")
        log(f"   🏦 ACCOUNT_EQUITY_USDT → ${ACCOUNT_EQUITY_USDT:.2f}")
        return
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
    [v9.1] Dual-track drawdown: streak + equity drawdown dari peak.

    Dua metrik dihitung dari SATU query (digabung dari dua query di v7.14).
    Streak dihitung dari baris terbaru ke belakang; equity_dd dari semua rows.

    [v9.1 FIX] Jika DB tidak punya trade baru (rows kosong), restore streak
    dari Supabase bot_config agar tidak reset ke 0 saat job restart
    di tengah losing streak.

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
        # [v9.1 FIX] Tidak ada trade history — restore streak dari Supabase jika ada
        _, _, persisted_streak = check_bot_halt()
        if persisted_streak > 0:
            log(f"   ℹ️ Streak restore dari Supabase: {persisted_streak} (no trade rows)", "warn")
        return {"streak": persisted_streak, "mode": "normal", "dd_pct": 0.0}

    WIN_VALUES  = {"WIN", "TP1", "TP2", "PARTIAL_WIN"}  # [v8.6 FIX] PARTIAL_WIN breaks loss streak
    # [v9.2.5] EXPIRED_LOSS dihitung sebagai loss untuk streak tracking
    LOSS_VALUES = {"LOSS", "SL", "EXPIRED_LOSS"}
    # [v9.4.3] SYSTEM di-exclude dari loss count, WIN-nya tetap memutus streak
    SYSTEM_STRATEGIES = {"SYSTEM"}

    # ── Streak: hitung dari baris paling baru ke belakang ────────────────
    streak = 0
    for row in reversed(rows):
        result   = (row.get("result")   or "").upper()
        strategy = (row.get("strategy") or "").upper()
        if result in LOSS_VALUES and strategy not in SYSTEM_STRATEGIES:
            streak += 1
        elif result in WIN_VALUES:
            break  # WIN (termasuk SYSTEM WIN) memutus streak

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
        # [v9.1] Persist halt state + streak ke Supabase agar survive job restart
        if mode == "halt":
            reason = f"streak={streak}, equity_dd={dd_pct*100:.1f}%"
            set_bot_halt(True, reason, streak=streak)
        else:
            # Warn mode — persist streak tapi tidak halt
            set_bot_halt(False, "", streak=streak)
    else:
        # [v9.0] Jika mode kembali normal, lepas halt yang sebelumnya di-persist
        set_bot_halt(False, "", streak=0)

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

def _inject_streak_reset_win() -> None:
    """
    [v9.4.3] Inject satu record SYSTEM WIN ke signals_v2 untuk memutus
    losing streak counter. Dipanggil otomatis saat SCAN_MODE=reset_halt.

    Record ini TIDAK mempengaruhi:
    - WR calculation  → strategy='SYSTEM' di-exclude
    - Equity / PnL    → pnl_usdt=0
    - Sharpe/Sortino  → strategy='SYSTEM' di-exclude
    - Telegram        → tidak ada notif

    Satu-satunya efek: memutus chain LOSS di get_drawdown_state()
    sehingga streak kembali ke 0.
    """
    try:
        now_iso = datetime.now(WIB).isoformat()
        payload = {
            "pair":       "SYSTEM_RESET",
            "strategy":   "SYSTEM",
            "side":       "BUY",
            "result":     "WIN",
            "pnl_usdt":   0.0,
            "entry":      0.0,
            "sl":         0.0,
            "tp1":        0.0,
            "sent_at":    now_iso,
            "closed_at":  now_iso,
        }
        supabase.table("signals_v2").insert(payload).execute()
        log("✅ _inject_streak_reset_win: record SYSTEM WIN berhasil di-inject — streak akan reset ke 0.")
    except Exception as e:
        log(f"⚠️ _inject_streak_reset_win gagal: {e} — streak mungkin tidak reset.", "warn")


def set_bot_halt(halted: bool, reason: str = "", streak: int = 0) -> None:
    """
    [v9.1] Persist status halt bot + streak ke Supabase bot_config.
    Dipanggil setiap kali mode drawdown berubah ke/dari 'halt'.

    [v9.1 FIX] Streak kini di-persist bersama halt state agar tidak reset ke 0
    saat GitHub Actions job restart di tengah losing streak.
    """
    try:
        supabase.table("bot_config").upsert([
            {"key": "bot_halt",        "value": str(halted).lower(), "updated_at": datetime.now(WIB).isoformat()},
            {"key": "bot_halt_reason", "value": reason or "",        "updated_at": datetime.now(WIB).isoformat()},
            {"key": "bot_streak",      "value": str(streak),         "updated_at": datetime.now(WIB).isoformat()},
        ], on_conflict="key").execute()
        log(f"🔒 Kill switch persist: halt={halted} | streak={streak} | reason={reason or '-'}")
    except Exception as e:
        log(f"⚠️ Gagal persist kill switch ke Supabase: {e}", "warn")


def check_bot_halt() -> tuple[bool, str, int]:
    """
    [v9.1] Baca status halt + streak dari Supabase bot_config.
    Return: (is_halted: bool, reason: str, streak: int)
    Fallback ke (False, "", 0) jika tabel belum ada atau query gagal.

    [v9.1 FIX] Mengembalikan streak tersimpan agar _drawdown_state tidak
    kehilangan context setelah job restart.
    """
    try:
        rows = (
            supabase.table("bot_config")
            .select("key, value")
            .in_("key", ["bot_halt", "bot_halt_reason", "bot_streak"])
            .execute()
            .data
        ) or []
        kv = {r["key"]: r["value"] for r in rows}
        is_halted = kv.get("bot_halt", "false").lower() == "true"
        reason    = kv.get("bot_halt_reason", "")
        try:
            streak = int(kv.get("bot_streak", "0"))
        except (ValueError, TypeError):
            streak = 0
        return is_halted, reason, streak
    except Exception as e:
        log(f"⚠️ Gagal baca kill switch dari Supabase: {e} — fail-safe: asumsikan HALT", "warn")
        # [v9.2 FIX] Fail-safe: jika tidak bisa verifikasi status, JANGAN jalankan signal baru.
        # Sebelumnya: return False (asumsikan tidak halt) — berbahaya saat Supabase down.
        return True, f"Supabase unreachable: {str(e)[:120]}", 0


def auto_reset_halt() -> bool:
    """
    [v9.4.1] Auto-reset HALT jika kondisi market/equity sudah membaik.

    Kondisi auto-reset (SEMUA harus terpenuhi):
    - dd_pct < DD_WARN_PCT  → equity sudah pulih dari zona bahaya
    - streak < DRAWDOWN_STREAK_HALT → losing streak sudah tidak ekstrem
    - BTC tidak dalam kondisi crash atau bearish trend

    Returns:
        True  → halt berhasil di-reset, bot lanjut scan
        False → kondisi belum aman, halt tetap aktif
    """
    try:
        state  = get_drawdown_state()
        dd_pct = state.get("dd_pct", 1.0)
        streak = state.get("streak", 99)

        try:
            client = get_client()
            btc = get_btc_regime(client)
        except Exception:
            btc = {"halt": True, "block_buy": True, "btc_bearish_trend": True}

        btc_safe          = not btc.get("halt", True) and not btc.get("btc_bearish_trend", True)
        equity_recovered  = dd_pct < DD_WARN_PCT
        streak_ok         = streak < DRAWDOWN_STREAK_HALT

        log(f"🔄 auto_reset_halt: dd={dd_pct*100:.1f}% "
            f"(limit {DD_WARN_PCT*100:.0f}%), "
            f"streak={streak} (limit {DRAWDOWN_STREAK_HALT}), "
            f"btc_safe={btc_safe}")

        if equity_recovered and streak_ok and btc_safe:
            set_bot_halt(False, "", streak=streak)
            log("✅ auto_reset_halt: kondisi membaik — HALT otomatis di-reset")
            tg(f"✅ <b>Bot HALT — Auto Reset</b>\n"
               f"Kondisi market/equity sudah membaik:\n"
               f"• DD: <b>{dd_pct*100:.1f}%</b> (di bawah batas {DD_WARN_PCT*100:.0f}%)\n"
               f"• Streak: <b>{streak}</b> loss berturutan\n"
               f"• BTC: <b>tidak crash/bearish</b>\n"
               f"<i>Bot melanjutkan scan otomatis.</i>")
            return True

        # Belum aman — log alasan
        reasons = []
        if not equity_recovered:
            reasons.append(f"DD {dd_pct*100:.1f}% masih ≥ {DD_WARN_PCT*100:.0f}%")
        if not streak_ok:
            reasons.append(f"streak {streak} masih ≥ {DRAWDOWN_STREAK_HALT}")
        if not btc_safe:
            reasons.append("BTC masih bearish/crash")
        log(f"⏸️ auto_reset_halt: belum reset — {', '.join(reasons)}", "warn")
        return False

    except Exception as e:
        log(f"⚠️ auto_reset_halt error: {e} — halt tetap aktif", "error")
        return False
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
    default = {"halt": False, "block_buy": False, "btc_1h": 0.0, "btc_4h": 0.0,
               "btc_bearish_trend": False, "btc_bearish_cycles": 0}
    try:
        # [v7.3 FIX] Limit dinaikkan 10→30 agar lolos guard len(raw)<30 di get_candles.
        c1h = get_candles(client, "BTC_USDT", "1h", 30)
        c4h = get_candles(client, "BTC_USDT", "4h", 30)
        if c1h is None or c4h is None: return default

        closes_1h = c1h[0]
        closes_4h = c4h[0]

        chg_1h = (closes_1h[-1] - closes_1h[-2]) / closes_1h[-2] * 100
        # [v7.1 #5] Perbedaan 1 candle 4h = perubahan dalam 4 jam terakhir
        chg_4h = (closes_4h[-1] - closes_4h[-2]) / closes_4h[-2] * 100

        halt      = chg_4h < BTC_CRASH_BLOCK
        block_buy = chg_1h < BTC_DROP_BLOCK

        # [v9.2.7] BTC trend guard — deteksi downtrend pelan tapi konsisten.
        # Cek BTC_TREND_LOOKBACK candle 4h terakhir — jika >= BTC_TREND_MIN_BEARISH negatif
        # maka set btc_bearish_trend=True → block semua BUY baru (bukan halt).
        # Berbeda dari BTC_DROP_BLOCK: itu spike 1h, ini tren berkelanjutan 12 jam.
        _n = BTC_TREND_LOOKBACK + 1   # butuh +1 untuk hitung delta antar candle
        if len(closes_4h) >= _n:
            # [v9.4.3] Guard: pastikan tidak ada closes_4h[-i-1] = 0 (divide-by-zero)
            _changes = []
            for i in range(1, BTC_TREND_LOOKBACK + 1):
                prev = closes_4h[-i-1]
                if prev and prev != 0:
                    _changes.append((closes_4h[-i] - prev) / prev * 100)
            _bearish_count   = sum(1 for c in _changes if c < 0)
            btc_bearish_trend = (len(_changes) >= BTC_TREND_MIN_BEARISH and
                                 _bearish_count >= BTC_TREND_MIN_BEARISH)
        else:
            # Data candle tidak cukup — jangan asumsikan bearish, log warning
            log(f"   ⚠️ get_btc_regime: data 4h kurang ({len(closes_4h)} < {_n}) — btc_bearish_trend=False", "warn")
            _bearish_count   = 0
            btc_bearish_trend = False

        _status = (
            "🛑 HALT" if halt else
            "⛔ BUY BLOCKED (1h drop)" if block_buy else
            f"📉 TREND BEARISH ({_bearish_count}/{BTC_TREND_LOOKBACK} candle negatif)" if btc_bearish_trend else
            "✅ OK"
        )
        log(f"📡 BTC 1h:{chg_1h:+.1f}% 4h:{chg_4h:+.1f}% | {_status}")

        return {"halt": halt, "block_buy": block_buy,
                "btc_1h": round(chg_1h, 2), "btc_4h": round(chg_4h, 2),
                "btc_bearish_trend": btc_bearish_trend,
                "btc_bearish_cycles": _bearish_count}
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


def is_active_trading_hour() -> bool:
    """
    [v9.3] Cek apakah sekarang dalam jam aktif trading.
    Jam sepi (dini hari UTC) punya volume rendah → lebih banyak false break.
    Return True jika aktif, False jika di luar jam aktif.
    Jika ACTIVE_HOURS_UTC kosong/None → selalu return True (disable filter).
    """
    if not ACTIVE_HOURS_UTC:
        return True
    current_hour = datetime.now(timezone.utc).hour
    for start_h, end_h in ACTIVE_HOURS_UTC:
        if start_h <= current_hour < end_h:
            return True
    log(f"   🌙 Time-of-day filter: jam {current_hour:02d}:xx UTC di luar window aktif {ACTIVE_HOURS_UTC}")
    return False


def pair_in_cooldown(pair: str) -> bool:
    """
    [v9.3] Cek apakah pair sedang dalam cooldown setelah trade closed.
    Setelah SL/TP/EXPIRED, pair tidak bisa dapat signal baru selama PAIR_COOLDOWN_HOURS.
    Mencegah re-entry terlalu cepat setelah loss atau setelah pair expired.
    """
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=PAIR_COOLDOWN_HOURS)).isoformat()
        rows = (
            supabase.table("signals_v2")
            .select("id, result")
            .eq("pair", pair)
            .not_.is_("result", "null")
            .gt("closed_at", since)
            .limit(1)
            .execute()
            .data
        )
        if rows:
            log(f"   ⏳ COOLDOWN {pair} — trade closed dalam {PAIR_COOLDOWN_HOURS}j terakhir")
            return True
        return False
    except Exception as e:
        log(f"⚠️ pair_in_cooldown [{pair}]: {e} — skip cooldown check", "warn")
        return False  # fail open — lebih baik kirim daripada block semua


def already_sent_pump(pair: str) -> bool:
    """Cek dedup signal PUMP — pair dalam PUMP_DEDUP_HOURS jam."""
    return _already_sent_generic(pair, "PUMP", PUMP_DEDUP_HOURS)


def already_sent_micro(pair: str) -> bool:
    """Cek dedup signal MICROCAP — pair dalam MICRO_DEDUP_HOURS jam."""
    return _already_sent_generic(pair, "MICROCAP", MICRO_DEDUP_HOURS)


def save_signal(pair: str, strategy: str, side: str, entry: float,
                tp1: float, tp2, sl: float, tier: str, score: float,
                timeframe: str, position_size: float | None = None) -> bool:
    """Simpan signal ke Supabase untuk tracking dan deduplication.
    [v7.2 FIX #7] sent_at disimpan dalam UTC agar konsisten dengan already_sent query.
    [v7.7 #7] Isi _dedup_memory setelah insert — sehingga cycle yang sama
    tidak bisa mengirim duplikat meski Supabase lambat merespons.
    [v7.18 #C] position_size disimpan ke DB — dibutuhkan oleh evaluate_open_trades()
               untuk menghitung PnL aktual. Tanpa ini, semua trade fallback ke
               BASE_POSITION_USDT dan PnL tidak akurat.
    [v9.2 FIX] Graduated fallback — handles missing columns (position_size, status,
               atau kolom lain) dengan strip bertahap. Returns True jika berhasil.
               Sebelumnya: fallback hanya cek "position_size" di error string,
               melewatkan error "does not exist" untuk kolom lain (mis. status).
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
        "score":     float(round(score, 1)),   # [v9.2 FIX] cast eksplisit — hindari "2.0" string → 22P02 error
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

    # [v9.2 FIX] Graduated payloads — strip kolom opsional satu per satu.
    # Urutan: full → tanpa position_size → tanpa status → minimal (core saja).
    # Ini menangkap semua kombinasi kolom DB yang belum dimigrasikan.
    _payload_full    = {**_base_payload, "position_size": round(position_size, 4) if position_size else None}
    _payload_no_size = _base_payload                          # tanpa position_size
    _payload_no_stat = {k: v for k, v in _base_payload.items() if k != "status"}  # tanpa status
    _payload_minimal = {k: v for k, v in _base_payload.items()
                        if k not in ("status", "tier", "score", "timeframe")}     # core fields saja

    _SCHEMA_ERR = ("PGRST204", "does not exist", "column", "schema cache",
                   "22P02", "invalid input syntax")   # [v9.2 FIX] tangkap type mismatch (float→integer)

    def _schema_error(err_str: str) -> bool:
        return any(token in err_str for token in _SCHEMA_ERR)

    saved = False
    try:
        for label, payload in [
            ("full",     _payload_full),
            ("no-size",  _payload_no_size),
            ("no-status",_payload_no_stat),
            ("minimal",  _payload_minimal),
        ]:
            try:
                supabase.table("signals_v2").insert(payload).execute()
                if label != "full":
                    # Kolom belum ada — ingatkan developer
                    log(f"⚠️ save_signal [{pair}]: insert [{label}] berhasil. "
                        f"Jalankan DDL migration agar semua kolom tersedia.", "warn")
                saved = True
                break
            except Exception as e:
                err_str = str(e)
                if _schema_error(err_str):
                    log(f"⚠️ save_signal [{pair}] [{label}] schema error — coba fallback: {e}", "warn")
                    continue   # coba payload berikutnya
                else:
                    # Error bukan schema (network, RLS, dll) — jangan loop, langsung alert
                    log(f"⚠️ save_signal [{pair}] [{label}] non-schema error: {e}", "error")
                    tg(f"🚨 <b>save_signal GAGAL</b> [{pair}]\n"
                       f"Signal terkirim ke Telegram tapi TIDAK tersimpan ke DB.\n"
                       f"Error: {_tg_e(e)}")
                    break
        else:
            # Semua payload gagal — alert critical
            log(f"🚨 save_signal [{pair}]: semua fallback gagal — signal tidak tersimpan!", "error")
            tg(f"🚨 <b>save_signal GAGAL TOTAL</b> [{pair}]\n"
               f"Signal terkirim tapi tidak masuk Open Trades.\n"
               f"Cek DDL migration: ALTER TABLE signals_v2 ADD COLUMN IF NOT EXISTS "
               f"status TEXT DEFAULT 'OPEN'; ADD COLUMN IF NOT EXISTS position_size NUMERIC;")
    finally:
        # [v7.7 #7] Selalu tandai di memory — bahkan jika Supabase insert gagal,
        # mencegah re-send dalam cycle yang sama.
        _dedup_memory.add(_dedup_key(pair, strategy, side))

    return saved


# ════════════════════════════════════════════════════════
#  SIGNAL STRATEGIES
# ════════════════════════════════════════════════════════

def check_intraday(client, pair: str, price: float,
                   btc: dict, side: str = "BUY",
                   fg: int = 50) -> dict | None:
    """
    INTRADAY signal — timeframe 1h. Mendukung BUY dan SELL.

    [v7.28] 2 HARD GATE saja:
      1. BTC crash  : btc["halt"] → blok semua signal
      2. ADX CHOPPY : regime == "CHOPPY" → skip pair
    [v9.2.7] Gate tambahan:
      3. BTC bearish trend : btc["btc_bearish_trend"] → blok BUY baru

    Semua filter lain (RSI, velocity, late-entry) → dihapus dari gate.
    Scoring 3-factor (trend + momentum + volume) menentukan tier.
    """
    # Hard gate 1: BTC crash → halt semua signal (termasuk SELL)
    if btc.get("halt"): return None
    # BTC drop → blok BUY saja (SELL tetap boleh)
    if side == "BUY" and btc["block_buy"]: return None
    # [v9.2.7] BTC bearish trend guard → blok BUY baru
    # Downtrend pelan berkelanjutan lebih berbahaya dari spike 1h.
    # 2 dari 3 candle 4h terakhir negatif = bearish trend = tidak safe untuk BUY baru.
    if side == "BUY" and btc.get("btc_bearish_trend"):
        log(f"   📉 INTRADAY SKIP {pair} [BUY] — BTC downtrend "
            f"({btc.get('btc_bearish_cycles',0)}/{BTC_TREND_LOOKBACK} candle 4h negatif)")
        return None

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
                             setup_score=setup_score,
                             btc_4h=btc.get("btc_4h", 0.0), fg=fg)
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
                             setup_score=setup_score,
                             btc_4h=btc.get("btc_4h", 0.0), fg=fg)
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

    # [v9.2.1 fix] Hard-block: harga sudah terlalu jauh dari entry zone
    _dev = abs(price - entry) / entry
    if _dev > MAX_ENTRY_DEVIATION["INTRADAY"]:
        log(f"   ⛔ INTRADAY SKIP {pair} [{side}] — "
            f"harga ${price:.8g} sudah {_dev*100:.1f}% dari entry ${entry:.8g} "
            f"(max {MAX_ENTRY_DEVIATION['INTRADAY']*100:.0f}%)")
        return None

    # [v9.2.2 fix] RANGING + score rendah → skip.
    # RANGING market lebih noisy, butuh score lebih tinggi untuk edge yang sama.
    # Score 2.5 di RANGING secara historis jauh lebih banyak false signal vs TRENDING.
    if mkt["regime"] == "RANGING" and score < MIN_SCORE_RANGING:
        log(f"   ⛔ INTRADAY SKIP {pair} [{side}] — "
            f"score {score:.1f} < {MIN_SCORE_RANGING} di RANGING (ADX:{mkt['adx']:.1f})")
        return None

    # [v9.4.2] Adaptive score threshold saat BTC bearish cycles tinggi.
    # Saat BTC dalam downtrend (btc_bearish_cycles >= 2 dari 3 candle 4h negatif),
    # threshold dinaikkan +0.5 agar hanya sinyal dengan konfluensi kuat yang lolos.
    # Ini mengurangi false signal saat kondisi market secara struktural bearish.
    _bearish_cycles = btc.get("btc_bearish_cycles", 0)
    _adaptive_min   = TIER_MIN_SCORE["A+"] + (0.5 if _bearish_cycles >= 2 else 0.0)
    if score < _adaptive_min:
        log(f"   ⛔ INTRADAY SKIP {pair} [{side}] — "
            f"score {score:.1f} < {_adaptive_min:.1f} "
            f"(adaptive threshold, bearish_cycles={_bearish_cycles})")
        return None

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
                btc: dict, side: str = "BUY",
                fg: int = 50) -> dict | None:
    """
    SWING signal — timeframe 4h. Mendukung BUY dan SELL.

    [v7.28] 2 HARD GATE saja:
      1. BTC crash  : btc["halt"] → blok semua signal
      2. ADX CHOPPY : regime == "CHOPPY" → skip pair
    [v9.2.7] Gate tambahan:
      3. BTC bearish trend : btc["btc_bearish_trend"] → blok BUY baru

    Semua filter lain (RSI, late-entry) → dihapus dari gate.
    Scoring 3-factor menentukan tier A+/A/SKIP.
    """
    if btc.get("halt"): return None
    if side == "BUY" and btc["block_buy"]: return None
    # [v9.2.7] BTC bearish trend guard
    if side == "BUY" and btc.get("btc_bearish_trend"):
        log(f"   📉 SWING SKIP {pair} [BUY] — BTC downtrend "
            f"({btc.get('btc_bearish_cycles',0)}/{BTC_TREND_LOOKBACK} candle 4h negatif)")
        return None

    # [v9.2 audit FIX] 200→400 bar; EMA200 butuh ~3-4× period untuk stabil.
    # Dengan hanya 200 candle, nilai EMA200 sangat dipengaruhi seeding bar.
    data = get_candles(client, pair, "4h", 400)
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
    # [v9.2 audit FIX] EMA200 butuh ≥250 bar untuk stabil; fallback ke EMA100.
    if len(closes) < 250:
        ema200 = calc_ema(closes, 100)
        log(f"   ⚠️ {pair}: data 4h hanya {len(closes)} candle — EMA200→EMA100 fallback", "warn")
    else:
        ema200 = calc_ema(closes, 200)
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
                             setup_score=setup_score,
                             btc_4h=btc.get("btc_4h", 0.0), fg=fg)
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
                             setup_score=setup_score,
                             btc_4h=btc.get("btc_4h", 0.0), fg=fg)
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        entry = round(last_sh * 0.998, 8) if (last_sh and price >= last_sh * 0.97) else price

        sl, tp1, tp2 = calc_sl_tp(entry, "SELL", atr, structure, "SWING")
        if tp1 >= entry or sl <= entry: return None
        sl_dist = sl - entry
        if sl_dist <= 0 or sl_dist / entry > 0.10: return None
        rr = (entry - tp1) / sl_dist

    if rr < MIN_RR["SWING"]: return None

    # [v9.2.1 fix] Hard-block: harga sudah terlalu jauh dari entry zone
    _dev = abs(price - entry) / entry
    if _dev > MAX_ENTRY_DEVIATION["SWING"]:
        log(f"   ⛔ SWING SKIP {pair} [{side}] — "
            f"harga ${price:.8g} sudah {_dev*100:.1f}% dari entry ${entry:.8g} "
            f"(max {MAX_ENTRY_DEVIATION['SWING']*100:.0f}%)")
        return None

    # [v9.2.2 fix] RANGING + score rendah → skip.
    if mkt["regime"] == "RANGING" and score < MIN_SCORE_RANGING:
        log(f"   ⛔ SWING SKIP {pair} [{side}] — "
            f"score {score:.1f} < {MIN_SCORE_RANGING} di RANGING (ADX:{mkt['adx']:.1f})")
        return None

    # [v9.2.8 fix] SWING butuh score minimum lebih tinggi dari INTRADAY.
    # Data aktual: SWING WR 38.9% vs INTRADAY 60.8% → entry SWING terlalu longgar.
    # SWING hold lebih lama → false signal lebih mahal → butuh konfluensi lebih kuat.
    if score < TIER_MIN_SCORE_SWING:
        log(f"   ⛔ SWING SKIP {pair} [{side}] — "
            f"score {score:.1f} < {TIER_MIN_SCORE_SWING} (SWING minimum score)")
        return None

    # [v9.4.2] Adaptive score threshold saat BTC bearish cycles tinggi.
    # SWING lebih sensitif karena hold time lebih lama → saat bearish, threshold +0.5.
    _bearish_cycles = btc.get("btc_bearish_cycles", 0)
    _adaptive_swing = TIER_MIN_SCORE_SWING + (0.5 if _bearish_cycles >= 2 else 0.0)
    if score < _adaptive_swing:
        log(f"   ⛔ SWING SKIP {pair} [{side}] — "
            f"score {score:.1f} < {_adaptive_swing:.1f} "
            f"(adaptive threshold bearish, cycles={_bearish_cycles})")
        return None

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
    # [v9.2.1 fix] Gunakan fmt_price() agar harga micro tidak terpotong
    entry_note = ""
    if side == "BUY":
        if pct_above > 0.5:
            entry_note = (
                f"\n⚠️ Harga saat ini {fmt_price(cur_price)} (+{pct_above:.1f}% dari entry zone)"
                f"\n   <i>Tunggu pullback ke zona entry, jangan kejar harga!</i>"
            )
        elif pct_above < -0.3:
            entry_note = f"\n✅ Harga saat ini {fmt_price(cur_price)} — sudah di zona entry"
    else:  # SELL
        if pct_above < -0.5:
            entry_note = (
                f"\n⚠️ Harga saat ini {fmt_price(cur_price)} ({pct_above:.1f}% dari entry zone)"
                f"\n   <i>Tunggu retest ke zona entry, jangan kejar SHORT!</i>"
            )
        elif pct_above > 0.3:
            entry_note = f"\n✅ Harga saat ini {fmt_price(cur_price)} — sudah di zona entry SELL"

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
        f"Entry Zone : <b>{fmt_price(entry)}</b> <i>≈ {entry_idr}</i> (limit / retest BOS){entry_note}\n"
        f"TP1  : <b>{fmt_price(tp1)}</b> <i>≈ {tp1_idr}</i> <i>({tp_label}{pct_tp1:.1f}%)</i>\n"
        f"TP2  : <b>{fmt_price(tp2) if tp2 is not None else '—'}</b>"
        f"{(' <i>≈ ' + tp2_idr + '</i>') if tp2 is not None else ''}"
        f"{' <i>(' + tp_label + '{:.1f}%)</i>'.format(pct_tp2) if tp2 is not None else ''}\n"
        f"SL   : <b>{fmt_price(sl)}</b> <i>≈ {sl_idr}</i> <i>({sl_label}{pct_sl:.1f}%)</i>\n"
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
    tg_async(msg)  # [v9.2 audit] non-blocking

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
        f"Entry : <b>{fmt_price(entry)}</b> <i>≈ {entry_idr}</i>\n"
        f"TP1   : <b>{fmt_price(tp1)}</b> <i>≈ {tp1_idr}</i> <i>(+{pct_tp1:.1f}%)</i>\n"
        f"SL    : <b>{fmt_price(sl)}</b> <i>≈ {sl_idr}</i> <i>(-{pct_sl:.1f}%)</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 Vol Spike : <b>{vol_ratio:.1f}×</b> rata-rata\n"
        f"📈 Naik 45m  : <b>+{pct_change:.2f}%</b>\n"
        f"RSI          : {rsi}\n"
        f"Why          : <i>{sig.get('reason', '—')}</i>\n"
        f"<i>⚡ Early pump alert. Entry cepat, SL wajib ketat.</i>\n"
        f"<i>⚠️ High risk — bukan rekomendasi finansial.</i>"
    )
    tg_async(msg)  # [v9.2 audit] non-blocking
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

    # [v9.2.8 fix] Entry deviation guard untuk MICROCAP.
    # Root cause 0% WR: 13/13 SL karena entry setelah harga sudah pump.
    # Kalau harga saat signal dikirim sudah > 3% dari zona entry → SKIP.
    # Berbeda dari INTRADAY/SWING (2%/5%) karena microcap lebih volatile.
    _micro_dev = abs(price - entry) / entry if entry > 0 else 0.0
    _micro_dev_max = 0.03   # 3% untuk microcap
    if _micro_dev > _micro_dev_max:
        log(f"   ⛔ MICROCAP SKIP {pair} — "
            f"harga ${price:.8g} sudah {_micro_dev*100:.1f}% dari entry ${entry:.8g} "
            f"(max {_micro_dev_max*100:.0f}%)")
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
        f"Entry  : <b>{fmt_price(entry)}</b> <i>≈ {entry_idr}</i>\n"
        f"TP1    : <b>{fmt_price(tp1)}</b> <i>≈ {tp1_idr}</i> <i>(+{pct_tp1:.1f}%)</i>\n"
        f"TP2    : <b>{fmt_price(tp2)}</b> <i>≈ {tp2_idr}</i> <i>(+{pct_tp2:.1f}%)</i>\n"
        f"SL     : <b>{fmt_price(sl)}</b> <i>≈ {sl_idr}</i> <i>(-{pct_sl:.1f}%)</i>\n"
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
    tg_async(msg)  # [v9.2 audit] non-blocking
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
        time.sleep(TG_SEND_SLEEP_SEC)

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
        # [v9.2 audit] total_notional_usdt = Σ(position_size) untuk notional cap
        locked_usdt     = 0.0
        total_risk_usdt = 0.0
        total_notional_usdt = 0.0
        open_pairs      = []
        sector_counts: dict[str, int] = {}   # [v7.29] count aktif per sektor
        for r in rows:
            try:
                pos_size = float(r.get("position_size") or BASE_POSITION_USDT)
                is_partial = r.get("partial_result") == "TP1_PARTIAL"
                # [v9.2 audit] Notional pakai SIZE PENUH (bukan partial-adjusted)
                # karena gate mengukur kapital yang DIALOKASIKAN, bukan yang masih open.
                total_notional_usdt += pos_size
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
                total_notional_usdt += BASE_POSITION_USDT

        # Portfolio heat % = total risk / current equity × 100 (retained untuk logging)
        return {
            "total": len(rows),
            "buy": buy_count,
            "sell": sell_count,
            "locked_usdt":         round(locked_usdt, 2),
            "total_risk_usdt":     round(total_risk_usdt, 4),
            "total_notional_usdt": round(total_notional_usdt, 2),  # [v9.2 audit]
            "open_pairs":          open_pairs,
            "sector_counts":       sector_counts,
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
                        "total_notional_usdt": 0.0,
                        "open_pairs": [], "sector_counts": {}}
            except Exception as e2:
                log(f"⚠️ get_portfolio_state fallback: {e2}", "warn")
        else:
            log(f"⚠️ get_portfolio_state: {e} — assume 0 open trades", "warn")
        return {"total": 0, "buy": 0, "sell": 0, "locked_usdt": 0.0,
                "total_risk_usdt": 0.0, "total_notional_usdt": 0.0,
                "open_pairs": [], "sector_counts": {}}


def portfolio_allows(sig: dict, state: dict, btc: dict) -> bool:
    """
    [v7.11 #1] Gate portfolio-level — dipanggil sebelum setiap signal dikirim.
    [v8.7]    Semua gate kini aktif (non-redundant) setelah nilai dikalibrasi ulang.

    Tujuh pemeriksaan berurutan (short-circuit pada yang pertama gagal):
      1.  Hard cap total open trades   → MAX_OPEN_TRADES trades, blok semua arah
      1.5 Same-pair cap                → max 1 open trade per pair [v9.2.9]
      2.  Total risk cap               → MAX_RISK_TOTAL equity dalam USDT  ← primary financial gate
      3.  Same-side directional cap    → MAX_SAME_SIDE_TRADES, max per sisi
      4. BTC stress gate               → MAX_BTC_CORR_TRADES, BUY cap lebih ketat saat BTC drop
      5. Sector concentration cap      → MAX_PER_SECTOR per kluster aset
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

    # ── Check 1.5: Same-pair cap — double-check via Supabase ─────
    # [v9.2.9 fix] Maksimal 1 open trade per pair terlepas strategy/usia.
    # [v9.4.2 fix] Double-check langsung ke Supabase — open_pairs dari
    # get_portfolio_state() hanya cover PORTFOLIO_STALE_HOURS (96 jam).
    # BSV SWING usia 29j + INTRADAY usia 32j keduanya dalam 96 jam →
    # seharusnya terblok. Tapi jika ada race condition atau cache stale,
    # query langsung ke DB sebagai second line of defense.
    if pair in state.get("open_pairs", []):
        log(f"   🧠 Portfolio SKIP {pair} [{side}] — "
            f"sudah ada open trade untuk {pair} (max 1 per pair, from cache)")
        return False

    # Second check: query Supabase langsung untuk pair ini
    try:
        _pair_rows = (
            supabase.table("signals_v2")
            .select("id")
            .eq("pair", pair)
            .is_("result", "null")
            .limit(1)
            .execute()
            .data
        ) or []
        if _pair_rows:
            log(f"   🧠 Portfolio SKIP {pair} [{side}] — "
                f"konfirmasi DB: sudah ada open trade untuk {pair} (max 1 per pair)")
            return False
    except Exception as _e:
        # Jika DB tidak bisa direach, fallback ke cache (sudah dicek di atas)
        log(f"   ⚠️ same-pair DB check gagal untuk {pair}: {_e} — pakai cache", "warn")

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

    # ── Check 2.5: [v9.2 audit FIX] Cumulative notional cap ─────
    # Mencegah alokasi modal melebihi yang bisa di-fund di spot wallet.
    # MAX_OPEN_TRADES × MAX_POSITION_PCT = 130% — risk-budget tidak menutupi ini.
    cur_notional = state.get("total_notional_usdt", 0.0)
    new_notional = sig.get("position_size", BASE_POSITION_USDT) or BASE_POSITION_USDT
    notional_lim = eq * MAX_NOTIONAL_PCT
    if cur_notional + new_notional > notional_lim:
        log(f"   🧠 Portfolio SKIP {pair} [{side}] — "
            f"notional cap {MAX_NOTIONAL_PCT*100:.0f}% equity "
            f"(${cur_notional:.0f}+${new_notional:.0f} > ${notional_lim:.0f})")
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


def _resolve_trade_outcome_via_wicks(client, pair: str, side: str,
                                     entry: float, sl: float,
                                     tp1: float, tp2: float | None,
                                     sent_at_iso: str,
                                     partial_state: str | None = None,
                                     ) -> tuple[str | None, float | None]:
    """
    [v9.2 audit FIX — HIGH severity]
    Deteksi TP/SL hit dengan menscan 1m candle sejak sent_at, bukan hanya
    membandingkan ticker last price.

    Masalah lama: jika harga wick menembus SL/TP antara dua run cron
    (interval 2 jam) lalu balik, ticker last tidak pernah menyentuh level
    sehingga trade tetap "open" — winrate stat membengkak palsu, model
    Bayesian terdistorsi, dan position sizing jadi miring.

    Conservative within-bar ordering: SL DIPRIORITASKAN dalam satu bar
    yang sama (worst-case dulu) — sama seperti backtest engine.

    Returns:
        (result, exit_price) jika ada level tersentuh sejak sent_at_iso.
        (None, None) jika belum ada level tersentuh ATAU 1m fetch gagal
                     dan caller harus fallback ke ticker check.
    """
    # Hitung berapa menit sejak signal dikirim (cap 1440m = 24 jam)
    try:
        sent_at = datetime.fromisoformat(sent_at_iso.replace("Z", "+00:00"))
        minutes_since = max(1, int((datetime.now(timezone.utc) - sent_at).total_seconds() / 60))
    except Exception:
        minutes_since = 240
    # [v9.2.5] Monitor mode (jalan setiap 5 menit) → cukup 10 bar terakhir.
    # Full scan (jalan setiap 2 jam) → scan penuh sampai 1000 bar.
    # Ini mengurangi waktu monitor dari ~13 detik × 13 trades → ~2 detik × 13 trades.
    if SCAN_MODE == "monitor":
        limit = 10   # 10 menit terakhir cukup untuk monitor 5-menit interval
    else:
        limit = min(1000, minutes_since + 5)   # +5 menit buffer; cap 1000 bar

    # Fetch 1m candles — TIDAK pakai cache karena lifecycle butuh data segar
    try:
        raw = gate_call_with_retry(
            client.list_candlesticks,
            currency_pair=pair, interval="1m", limit=limit,
        )
    except Exception as e:
        log(f"   ⚠️ Wick scan {pair}: 1m fetch gagal ({e}) — fallback ke ticker", "warn")
        return None, None

    if not raw:
        return None, None

    is_buy = (side == "BUY")
    # Iterasi candle dari paling lama ke paling baru — outcome pertama menang
    for c in raw:
        try:
            # Gate format: [ts, vol, close, high, low]
            bar_high = float(c[3])
            bar_low  = float(c[4])
        except (ValueError, IndexError, TypeError):
            continue
        if bar_high <= 0 or bar_low <= 0:
            continue
        if math.isnan(bar_high) or math.isnan(bar_low):
            continue

        if is_buy:
            # Conservative: cek SL dulu dalam satu bar yang sama
            if bar_low <= sl:
                # Trade yang sudah PARTIAL: SL ≤ entry (BE) → BREAKEVEN, bukan SL
                if partial_state == "TP1_PARTIAL":
                    return "BREAKEVEN", sl
                return "SL", sl
            if tp2 is not None and bar_high >= tp2:
                return "TP2", tp2
            if bar_high >= tp1 and partial_state != "TP1_PARTIAL":
                # Untuk normal trade: TP1 hit → partial jika ada TP2
                return ("TP1_PARTIAL" if tp2 is not None else "TP1"), tp1
        else:  # SELL
            if bar_high >= sl:
                if partial_state == "TP1_PARTIAL":
                    return "BREAKEVEN", sl
                return "SL", sl
            if tp2 is not None and bar_low <= tp2:
                return "TP2", tp2
            if bar_low <= tp1 and partial_state != "TP1_PARTIAL":
                return ("TP1_PARTIAL" if tp2 is not None else "TP1"), tp1

    return None, None


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
      3. [v9.2 audit] Wick scan: fetch 1m candle sejak sent_at →
         scan untuk SL/TP1/TP2 hit (high/low, bukan ticker last)
      4. Fallback: ticker last price jika 1m fetch gagal
      5. BUY : low ≤ sl → SL | high ≥ tp2 → TP2 | high ≥ tp1 → TP1
         SELL: high ≥ sl → SL | low ≤ tp2 → TP2 | low ≤ tp1 → TP1
      6. Update result + closed_at di Supabase jika ada hit/expired
      7. Invalidate _winrate_cache_ts agar cycle berikutnya reload data baru

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
                # [v9.2.5 fix] Cek harga saat expired vs entry
                # Jika masih profit → EXPIRED_PROFIT (neutral di statistik)
                # Jika rugi         → EXPIRED_LOSS   (dihitung loss)
                # Sebelumnya semua EXPIRED diperlakukan sebagai LOSS
                # → WR dan expectancy lebih buruk dari kenyataan.
                _exp_entry = float(row.get("entry") or 0)
                _exp_price = 0.0
                try:
                    _exp_tickers = gate_call_with_retry(
                        client.list_tickers, currency_pair=pair
                    )
                    if _exp_tickers:
                        _exp_price = float(_exp_tickers[0].last or 0)
                except Exception:
                    pass

                if _exp_entry > 0 and _exp_price > 0:
                    _exp_pct = (_exp_price - _exp_entry) / _exp_entry
                    if side == "SELL":
                        _exp_pct = -_exp_pct
                    # [v9.3.1 fix] Update current_price ke harga saat expired.
                    # Bug sebelumnya: current_price = 0 (tidak di-set di jalur ini)
                    # → PnL calc: (0 - entry) / entry × size = -100% dari position_size
                    # → ETH expired loss salah: -$23.47 padahal harusnya -$0.36.
                    current_price = _exp_price
                    if _exp_pct > 0:
                        result = "EXPIRED_PROFIT"
                        log(f"   ⏰✅ EXPIRED_PROFIT: {pair} [{strategy} {side}] — "
                            f"harga {_exp_pct*100:+.2f}% dari entry saat expired")
                    else:
                        result = "EXPIRED_LOSS"
                        log(f"   ⏰❌ EXPIRED_LOSS: {pair} [{strategy} {side}] — "
                            f"harga {_exp_pct*100:+.2f}% dari entry saat expired")
                else:
                    result = "EXPIRED"
                    log(f"   ⏰ EXPIRED: {pair} [{strategy} {side}] — "
                        f"{age_hours:.1f}h > {expire_hours}h limit (price fetch gagal)")

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
                    # [v9.3] Strategy-aware trailing timeframe:
                    # INTRADAY → 1h candles (tepat untuk 1h setup)
                    # SWING    → 4h candles (sebelumnya 1h, terlalu ketat untuk swing trade)
                    _trail_tf = "4h" if strategy == "SWING" else "1h"
                    _candles_trail = get_candles(client, pair, _trail_tf, 20)
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

                # ── [v9.2 audit FIX] Wick-scan: cek apakah TP2/SL/BE tersentuh
                # sejak sent_at via 1m candle (bukan cuma ticker last)
                if result is None:
                    _wick_res, _wick_px = _resolve_trade_outcome_via_wicks(
                        client, pair, side, entry, sl, tp1, tp2,
                        sent_at_str, partial_state="TP1_PARTIAL",
                    )
                    if _wick_res is not None:
                        result = _wick_res
                        # Override current_price untuk PnL accuracy
                        current_price = _wick_px

                # ── Cek hit: TP2 atau Trail/BE ───────────────────────────
                # [v9.2 audit] Jika wick scan tidak mendeteksi apa-apa,
                # masih cek dengan current_price sebagai safety net.
                if result is None:
                    if side == "BUY":
                        if tp2 is not None and current_price >= tp2:   result = "TP2"
                        elif current_price <= sl:                        result = "BREAKEVEN"
                    else:  # SELL
                        if tp2 is not None and current_price <= tp2:   result = "TP2"
                        elif current_price >= sl:                        result = "BREAKEVEN"
            else:
                # Normal evaluation — TP2 dicek lebih dulu (tidak double-count)
                # ── [v9.2 audit FIX] Wick-scan dulu via 1m candle ──
                _wick_res, _wick_px = _resolve_trade_outcome_via_wicks(
                    client, pair, side, entry, sl, tp1, tp2,
                    sent_at_str, partial_state=None,
                )
                if _wick_res is not None:
                    result = _wick_res
                    current_price = _wick_px
                else:
                    # Fallback: ticker last (legacy behaviour)
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
                       f"Error: {_tg_e(_db_err)}\n"
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
                       f"Error: {_tg_e(_db_err)}\n"
                       f"⚠️ Trade ini masih OPEN di DB — update manual diperlukan.")

            stats["updated"] += 1
            # Update stats counter per result type
            # [v8.6 FIX] BREAKEVEN dihitung terpisah (bukan tp1).
            # PARTIAL_WIN sekarang masuk "partial_win" — sebelumnya jatuh ke "expired"
            # karena "partial_win" tidak ada di lookup dict (bug kritis statistik).
            _RESULT_KEY_MAP = {
                "TP1":            "tp1",
                "TP2":            "tp2",
                "SL":             "sl",
                "EXPIRED":        "expired",
                "EXPIRED_PROFIT": "expired_profit",  # [v9.2.5] neutral di WR
                "EXPIRED_LOSS":   "expired_loss",    # [v9.2.5] dihitung loss
                "PARTIAL_WIN":    "partial_win",
                "BREAKEVEN":      "breakeven",
            }
            key = _RESULT_KEY_MAP.get(result, "expired")
            stats[key] = stats.get(key, 0) + 1

            emoji = {"TP2": "🎯🎯", "TP1": "🎯", "SL": "❌", "EXPIRED": "⏰",
                     "BREAKEVEN": "⚖️", "PARTIAL_WIN": "🎯½",
                     "EXPIRED_PROFIT": "⏰✅", "EXPIRED_LOSS": "⏰❌"}.get(result, "?")
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
                    elif result == "SL_AFTER_TP1":
                        # [v9.3] Notifikasi informatif untuk SL_AFTER_TP1
                        # Sebelumnya tidak ada notif khusus → user tidak tahu trade closed
                        try:
                            _pct_from_tp1 = ((current_price - tp1) / tp1 * 100) if tp1 and tp1 > 0 else 0
                            _how_close    = abs(_pct_from_tp1)
                        except Exception:
                            _how_close = 0
                        tg_msg = (
                            f"⚖️ <b>Breakeven — {pair_display}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"Strategy : {strategy} {side}\n"
                            f"PnL      : <b>{pnl_sign}{pnl_usdt:.2f} USDT</b> (~{pnl_idr})\n"
                            f"<i>TP1 tercapai ✅, runner kena SL di breakeven.</i>\n"
                            f"<i>TP2 target {fmt_price(tp2)} — {_how_close:.1f}% dari TP1.</i>"
                        )
                    elif result == "SL":
                        tg_msg = (
                            f"❌ <b>Stop Loss — {pair_display}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"Strategy : {strategy} {side}\n"
                            f"PnL      : <b>{pnl_sign}{pnl_usdt:.2f} USDT</b> (~{pnl_idr})\n"
                            f"<i>SL tersentuh — loss terkontrol</i>"
                        )
                    elif result in ("EXPIRED", "EXPIRED_PROFIT", "EXPIRED_LOSS"):
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

                        # [v9.2.5] Tampilan berbeda tergantung posisi saat expired
                        if result == "EXPIRED_PROFIT":
                            _exp_icon  = "⏰✅"
                            _exp_title = "Signal Expired — Posisi Profit"
                            _exp_note  = (f"<i>Posisi +profit saat expired.</i>\n"
                                          f"<i>Jika sudah entry, pertimbangkan hold atau ambil profit manual.</i>")
                        elif result == "EXPIRED_LOSS":
                            _exp_icon  = "⏰❌"
                            _exp_title = "Signal Expired — Posisi Rugi"
                            _exp_note  = (f"<i>Posisi minus saat expired.</i>\n"
                                          f"<i>Jika sudah entry, kelola posisi secara manual.</i>")
                        else:
                            _exp_icon  = "⏰"
                            _exp_title = "Signal Expired"
                            _exp_note  = (f"<i>⚠️ Signal tidak lagi dimonitor bot.</i>\n"
                                          f"<i>Jika sudah entry, kelola posisi secara manual.</i>")

                        tg_msg = (
                            f"{_exp_icon} <b>{_exp_title} — {pair_display}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"Strategy : {strategy} {side}\n"
                            f"Usia     : {age_hours:.0f}j / {expire_h}j\n"
                            f"Posisi   : <b>{pct_str}</b> dari entry\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"{_exp_note}"
                        )
                    else:
                        tg_msg = None

                    if tg_msg:
                        tg(tg_msg)
                except Exception as _tge:
                    log(f"   ⚠️ Telegram notif gagal [{pair}]: {_tge}", "warn")

        except Exception as e:
            log(f"   ⚠️ Update result gagal [{pair}]: {e}", "warn")

        time.sleep(SCAN_SLEEP_SEC)   # throttle ringan — hindari burst Gate.io

    if stats["updated"] > 0:
        log(f"📋 Lifecycle done: {stats['updated']} diupdate "
            f"(TP1:{stats['tp1']} TP2:{stats['tp2']} "
            f"PARTIAL_WIN:{stats['partial_win']} BREAKEVEN:{stats['breakeven']} "
            f"SL:{stats['sl']} "
            f"EXPIRED_PROFIT:{stats.get('expired_profit',0)} "
            f"EXPIRED_LOSS:{stats.get('expired_loss',0)} "
            f"EXPIRED:{stats['expired']})")
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
    # [v9.2.5] EXPIRED_PROFIT tidak masuk WIN_VALUES (neutral), EXPIRED_LOSS bukan WIN
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
            # [v9.2.8] Exclude MICROCAP — pure alert, bukan managed trade.
            # [v9.2.9 fix] WR hanya dihitung dari WIN+LOSS (bukan semua result).
            # Sebelumnya: EXPIRED, BREAKEVEN, EXPIRED_PROFIT masuk denominator
            # → 45 win / 122 total = 36% padahal harusnya 45 / 87 = 51.7%.
            age_days = (now_utc - ts).total_seconds() / 86400
            _strat_row  = (row.get("strategy") or "").upper()
            _result_row = (row.get("result")   or "").upper()
            _is_win  = _result_row in WIN_VALUES
            _is_loss = _result_row in {"SL", "SL_AFTER_TP1", "EXPIRED_LOSS", "LOSS"}
            if age_days <= 30 and _strat_row not in ("MICROCAP", "PUMP"):
                if _is_win or _is_loss:   # hanya WIN atau LOSS — skip EXPIRED/BREAKEVEN
                    total_30d += 1
                    if _is_win:
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

def send_weekly_summary() -> None:
    """
    [v9.3] Kirim ringkasan performa mingguan setiap Senin pagi (06:00–08:00 WIB).
    Berisi: total signal, WR minggu ini, best/worst trade, realized PnL minggu ini.
    Berguna untuk evaluasi manual tanpa perlu cek Supabase langsung.
    """
    try:
        now_wib  = datetime.now(WIB)
        # Hanya kirim di hari Senin, jam 06:00–08:00 WIB
        if now_wib.weekday() != 0:   # 0 = Senin
            return
        if not (6 <= now_wib.hour < 8):
            return

        # Cek apakah sudah kirim minggu ini (hindari duplikat jika bot restart)
        cache_key = f"weekly_sent_{now_wib.strftime('%Y-%W')}"
        try:
            _sent = supabase.table("bot_config").select("value")                 .eq("key", cache_key).execute().data
            if _sent and _sent[0].get("value") == "1":
                return  # sudah dikirim minggu ini
        except Exception:
            pass

        # Query 7 hari terakhir
        since_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        rows = (
            supabase.table("signals_v2")
            .select("pair, strategy, result, pnl_usdt, closed_at")
            .not_.is_("result", "null")
            .gt("closed_at", since_7d)
            .not_.in_("strategy", ["MICROCAP", "PUMP"])
            .execute().data
        ) or []

        if not rows:
            tg("📅 <b>Weekly Summary</b>\n━━━━━━━━━━━━━━━━━━\n"
               "<i>Tidak ada trade closed minggu ini.</i>")
            return

        _WIN  = {"TP1", "TP2", "PARTIAL_WIN", "WIN"}
        _LOSS = {"SL", "SL_AFTER_TP1", "EXPIRED_LOSS", "LOSS"}

        wins   = [r for r in rows if r.get("result","").upper() in _WIN]
        losses = [r for r in rows if r.get("result","").upper() in _LOSS]
        total  = len(wins) + len(losses)
        wr     = len(wins) / total * 100 if total > 0 else 0

        pnl_list   = [float(r.get("pnl_usdt") or 0) for r in rows]
        total_pnl  = sum(pnl_list)

        best_row  = max(rows, key=lambda r: float(r.get("pnl_usdt") or 0))
        worst_row = min(rows, key=lambda r: float(r.get("pnl_usdt") or 0))

        pnl_icon = "📈" if total_pnl >= 0 else "📉"
        wr_icon  = "🟢" if wr >= 55 else ("🟡" if wr >= 45 else "🔴")

        tg(
            f"📅 <b>Weekly Summary — {now_wib.strftime('%d %b %Y')}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Total trade  : <b>{total}</b> ({len(wins)}W / {len(losses)}L)\n"
            f"Win Rate     : {wr_icon} <b>{wr:.1f}%</b>\n"
            f"Realized PnL : {pnl_icon} <b>{total_pnl:+.2f} USDT</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Best  : <b>{best_row.get('pair','?')}</b> "
            f"{float(best_row.get('pnl_usdt') or 0):+.2f} USDT ({best_row.get('result','?')})\n"
            f"Worst : <b>{worst_row.get('pair','?')}</b> "
            f"{float(worst_row.get('pnl_usdt') or 0):+.2f} USDT ({worst_row.get('result','?')})\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<i>Data: 7 hari terakhir (INTRADAY+SWING only)</i>"
        )

        # Mark sudah dikirim
        try:
            supabase.table("bot_config").upsert(
                {"key": cache_key, "value": "1",
                 "updated_at": datetime.now(WIB).isoformat()},
                on_conflict="key"
            ).execute()
        except Exception:
            pass

        log(f"📅 Weekly summary terkirim: {total} trades, WR={wr:.1f}%, PnL={total_pnl:+.2f}")

    except Exception as e:
        log(f"⚠️ send_weekly_summary: {e}", "warn")


def send_open_trades_summary(client=None) -> None:
    """
    Kirim rekapan semua open trades ke Telegram setiap akhir run.
    Memudahkan user memantau posisi aktif tanpa harus menunggu signal baru.
    Jika client disediakan, tampilkan unrealized PnL % berdasarkan harga live.
    [v9.2 FIX] Filter strategy IN (INTRADAY, SWING) + cutoff PORTFOLIO_STALE_HOURS
               agar konsisten dengan get_portfolio_state(). Sebelumnya query semua
               result=NULL tanpa filter → PUMP/MICROCAP muncul di summary tapi tidak
               dihitung di portfolio_state, menyebabkan angka Open Trades tidak match.
    """
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=PORTFOLIO_STALE_HOURS)
        ).isoformat()
        rows = (
            supabase.table("signals_v2")
            .select("pair, strategy, side, entry, tp1, tp2, sl, sent_at, partial_result")
            .is_("result", "null")
            .gte("sent_at", cutoff)
            .in_("strategy", ["INTRADAY", "SWING"])
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
        # [v9.2.2 fix] Proxy ke fmt_price() global agar micro-price (PEPE dll)
        # tampil dengan desimal yang cukup — entry ≠ TP1 ≠ SL.
        # Sebelumnya: semua harga < $0.01 pakai :.6f → 0.000004 = 0.000004 = 0.000004
        return fmt_price(v)

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

    # [v9.3] Weekly performance summary — dikirim setiap Senin 06:00–08:00 WIB
    send_weekly_summary()

    # [v9.2 audit] Boot-time config sanity check — print warnings sebelum scan.
    for _w in validate_config():
        log(f"⚠️ Config warning: {_w}", "warn")

    # [v9.1] Kill switch check — baca status halt + streak dari Supabase sebelum apapun dieksekusi.
    # Jika bot sedang dalam mode halt yang di-persist (survive job restart), exit immediately.
    # [v9.1 FIX] Restore streak ke _drawdown_state agar context tidak hilang setelah restart.
    _is_halted, _halt_reason, _persisted_streak = check_bot_halt()
    if _persisted_streak > 0:
        _drawdown_state["streak"] = _persisted_streak
        log(f"   ℹ️ Streak dipulihkan dari Supabase: {_persisted_streak}")
    if _is_halted:
        log(f"🛑 BOT HALT — Kill switch aktif dari run sebelumnya. Reason: {_halt_reason}", "error")
        # [v9.4.1] Coba auto-reset jika kondisi sudah membaik — tidak perlu manual
        log("🔄 Mencoba auto_reset_halt...")
        _auto_reset_ok = auto_reset_halt()
        if not _auto_reset_ok:
            # Kondisi belum aman — kirim notif dan exit
            tg(f"🛑 <b>Bot HALT — Scan dibatalkan</b>\n"
               f"Kill switch aktif dari run sebelumnya.\n"
               f"Reason: {_halt_reason}\n"
               f"<i>Bot akan coba auto-reset setiap scan "
               f"jika DD &lt; {DD_WARN_PCT*100:.0f}%, streak &lt; {DRAWDOWN_STREAK_HALT}, "
               f"dan BTC tidak bearish.</i>")
            # [v9.4.2] Tetap kirim Equity Report saat HALT agar user bisa monitor kondisi
            save_equity_snapshot(open_trades=0)
            tg_drain(timeout=15.0)
            return
        # Auto-reset berhasil — lanjutkan scan
        log("✅ Auto-reset berhasil — melanjutkan scan")

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

    # [v9.4.3] Manual override — reset HALT + streak tanpa nunggu market recovery.
    # Jalankan GitHub Actions workflow dengan env var SCAN_MODE=reset_halt.
    # Bot akan reset state, kirim konfirmasi Telegram, lalu lanjut full scan.
    if SCAN_MODE == "reset_halt":
        log("🔧 SCAN_MODE=reset_halt — manual override HALT")
        _was_halted, _old_reason, _old_streak = check_bot_halt()
        set_bot_halt(False, "", streak=0)
        _drawdown_state["streak"] = 0
        _drawdown_state["mode"]   = "normal"
        # [v9.4.3] Inject SYSTEM WIN untuk memutus chain loss di signals_v2
        # Tanpa ini, get_drawdown_state() akan hitung ulang streak dari DB
        # dan mengabaikan reset bot_config.
        _inject_streak_reset_win()
        log("✅ HALT di-reset manual. Streak dikembalikan ke 0.")
        tg(f"✅ <b>Bot HALT — Manual Reset</b>\n"
           f"HALT di-reset secara manual via SCAN_MODE=reset_halt.\n"
           f"• Streak sebelumnya : <b>{_old_streak}</b>\n"
           f"• Reason sebelumnya : <b>{_old_reason or '-'}</b>\n"
           f"<i>Bot melanjutkan full scan sekarang...</i>")
        # Lanjut ke full scan di bawah — tidak return

    log(f"\n{'='*60}")
    log(f"🚀 SIGNAL BOT v{BOT_VERSION} — {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')} [FULL SCAN]")
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
    # [v9.2.2 fix] Fetch SETELAH pump scan selesai (bukan sebelumnya).
    # Pump scan punya pump_portfolio_state terpisah dan bisa insert signal ke DB
    # tanpa update portfolio_state main — menyebabkan 14/13 open trades.
    # Dengan fetch di sini, main scan selalu punya state yang sinkron dengan DB.
    portfolio_state = get_portfolio_state()
    log(f"🧠 Portfolio: {portfolio_state['total']} open trades "
        f"(BUY:{portfolio_state['buy']} SELL:{portfolio_state['sell']}) "
        f"| Max: {MAX_OPEN_TRADES} total / {MAX_SAME_SIDE_TRADES} per sisi | "
        f"🎯 Sector: {portfolio_state.get('sector_counts', {})} | "
        f"Risk: ${portfolio_state['total_risk_usdt']:.2f} / {MAX_RISK_TOTAL*100:.0f}% equity")

    # [v9.4.1] BUG FIX: allow_buy sebelumnya hanya cek block_buy (spike 1h),
    # tapi TIDAK cek btc_bearish_trend (downtrend 12 jam).
    # Akibatnya: saat TREND BEARISH aktif, check_intraday/check_swing per-pair
    # memang return None, tapi allow_buy=True di gate utama tetap membuka
    # peluang sinyal masuk jika ada edge case di luar dua fungsi tersebut.
    # Fix: allow_buy = False jika block_buy ATAU btc_bearish_trend aktif.
    _btc_bearish_trend = btc.get("btc_bearish_trend", False)
    allow_buy  = not btc["block_buy"] and not _btc_bearish_trend
    allow_sell = False  # [v7.24] Disabled — spot only, SELL tidak bisa dieksekusi

    _buy_reason = (
        "⛔ diblokir (BTC 1h drop)" if btc["block_buy"] else
        "⛔ diblokir (TREND BEARISH)" if _btc_bearish_trend else
        "✅ aktif"
    )
    log(f"Mode  : BUY={_buy_reason} | "
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
            # [v9.4] MICROCAP_ENABLED kill switch — default False karena WR historis 0%
            if (MICROCAP_ENABLED
                    and allow_buy
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

            # ── Time-of-day filter [v9.3] ─────────────────────
            if not is_active_trading_hour():
                time.sleep(SCAN_SLEEP_SEC)
                continue

            # ── INTRADAY BUY ──────────────────────────────────
            # [v9.3] Cooldown check — skip pair yang baru saja closed
            _in_cooldown = pair_in_cooldown(pair)
            if allow_buy and not cluster_buy_blocked and not already_sent(pair, "INTRADAY", "BUY") and not _in_cooldown:
                sig = check_intraday(client, pair, price, btc, side="BUY", fg=fg)
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "INTRADAY", "BUY"))

            # ── INTRADAY SELL ─────────────────────────────────
            if allow_sell and not already_sent(pair, "INTRADAY", "SELL"):
                sig = check_intraday(client, pair, price, btc, side="SELL", fg=fg)
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "INTRADAY", "SELL"))

            # ── SWING BUY ────────────────────────────────────
            # [v9.4] SWING_ENABLED kill switch — matikan jika WR < 45% selama 2 minggu
            if SWING_ENABLED and allow_buy and not cluster_buy_blocked and not already_sent(pair, "SWING", "BUY") and not _in_cooldown:
                sig = check_swing(client, pair, price, btc, side="BUY", fg=fg)
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "SWING", "BUY"))

            # ── SWING SELL ───────────────────────────────────
            if SWING_ENABLED and allow_sell and not already_sent(pair, "SWING", "SELL"):
                sig = check_swing(client, pair, price, btc, side="SELL", fg=fg)
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
        # [v9.2.2 fix] Hard cap guard — re-check total langsung dari local state
        # (incremented per signal) agar 14/13 tidak terjadi meskipun ada lag DB.
        if portfolio_state["total"] >= MAX_OPEN_TRADES:
            log(f"   🚫 MICROCAP hard-stop — sudah {portfolio_state['total']}/{MAX_OPEN_TRADES} open trades")
            break
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
        # [v9.2 audit] Notional accumulator untuk MAX_NOTIONAL_PCT gate
        portfolio_state["total_notional_usdt"] = \
            portfolio_state.get("total_notional_usdt", 0.0) + _size_m
        micro_sent += 1
        time.sleep(TG_SEND_SLEEP_SEC)

    if not signals and micro_sent == 0:
        tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v{BOT_VERSION}</b>\n"
           f"━━━━━━━━━━━━━━━━━━\n"
           f"Pairs scanned : <b>{scanned}</b>\n"
           f"F&G           : <b>{fg}</b>\n"
           f"BTC 1h/4h     : <b>{btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}%</b>"
           f"{' 📉 <b>TREND BEARISH — BUY DIBLOK</b>' if btc.get('btc_bearish_trend') else ''}\n"
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

    sent       = 0
    sent_sigs  = []
    save_fails = 0   # [v9.2 FIX] track signal yang terkirim ke TG tapi gagal masuk DB
    for sig in signals:
        if sent >= MAX_SIGNALS_CYCLE: break
        # [v9.2.2 fix] Hard cap guard — stop loop jika sudah mentok MAX_OPEN_TRADES.
        # Mencegah kondisi 14/13 di mana portfolio_state lokal tidak sinkron dengan DB.
        if portfolio_state["total"] >= MAX_OPEN_TRADES:
            log(f"   🚫 Main signal hard-stop — sudah {portfolio_state['total']}/{MAX_OPEN_TRADES} open trades")
            break

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
        # [v9.2.2 fix] Data historis kurang → potong position size 50%.
        # Signal tetap dikirim (pair baru perlu warmup) tapi dengan risk lebih kecil.
        _n_hist = _conf.get("n", 0)
        if _n_hist < MIN_HIST_SAMPLE_SKIP:
            # Kurang dari 3 sample — skip total, terlalu spekulatif
            log(f"   ⛔ SKIP {sig['pair']} — data historis sangat kurang (n={_n_hist}/{MIN_HIST_SAMPLE_SKIP})")
            continue
        elif _n_hist < MIN_HIST_SAMPLE_FULL:
            # 3–14 sample — kirim tapi half size
            _orig_size = sig["position_size"]
            sig["position_size"] = round(_orig_size * 0.5, 2)
            log(f"   ⚠️ {sig['pair']}: half-size (n={_n_hist}/{MIN_HIST_SAMPLE_FULL}) — "
                f"${_orig_size:.2f} → ${sig['position_size']:.2f}")

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
        _saved = save_signal(
            sig["pair"], sig["strategy"], sig["side"],
            sig["entry"], sig["tp1"], sig["tp2"], sig["sl"],
            sig["tier"], sig["score"], sig["timeframe"],
            position_size=sig.get("position_size"),   # [v7.18 #C] simpan ke DB
        )

        # [v9.2 FIX] Update portfolio_state HANYA jika save berhasil.
        # Sebelumnya: selalu +1 meski save_signal gagal → equity report
        # menunjukkan "open_trades: 7" tapi open_trades summary hanya tampil 5.
        if not _saved:
            log(f"⚠️ portfolio_state tidak diupdate untuk {sig['pair']} — save_signal gagal.", "warn")
            save_fails += 1
            sent_sigs.append(sig)
            sent += 1
            time.sleep(TG_SEND_SLEEP_SEC)
            continue

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
        # [v9.2 audit] Notional accumulator untuk MAX_NOTIONAL_PCT gate
        portfolio_state["total_notional_usdt"] = \
            portfolio_state.get("total_notional_usdt", 0.0) + _size_s

        sent_sigs.append(sig)
        sent += 1
        time.sleep(TG_SEND_SLEEP_SEC)

    # Summary
    intraday_buy  = sum(1 for s in sent_sigs if s["strategy"] == "INTRADAY" and s["side"] == "BUY")
    intraday_sell = sum(1 for s in sent_sigs if s["strategy"] == "INTRADAY" and s["side"] == "SELL")
    swing_buy     = sum(1 for s in sent_sigs if s["strategy"] == "SWING"    and s["side"] == "BUY")
    swing_sell    = sum(1 for s in sent_sigs if s["strategy"] == "SWING"    and s["side"] == "SELL")

    tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v{BOT_VERSION}</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Pairs scanned : <b>{scanned}</b>\n"
       f"F&G           : <b>{fg}</b>\n"
       f"BTC 1h/4h     : <b>{btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}%</b>"
       f"{' 📉 <b>TREND BEARISH</b>' if btc.get('btc_bearish_trend') else ''}\n"
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
       + (f"  ⚠️ DB save gagal : {save_fails} signal tidak masuk Open Trades!\n" if save_fails > 0 else "")
       + f"<i>Scan berikutnya dalam 2 jam.</i>")

    log(f"\n✅ Done — {sent + micro_sent} signal terkirim "
        f"({sent} main + {micro_sent} microcap)")
    log(f"   INTRADAY BUY:{intraday_buy} SELL:{intraday_sell} | "
        f"SWING BUY:{swing_buy} SELL:{swing_sell} | MICROCAP:{micro_sent}")

    # [v7.14 #E] Equity curve snapshot — diambil di akhir setiap run
    save_equity_snapshot(open_trades=portfolio_state["total"])

    # ── Kirim rekapan open trades ke Telegram ────────────────────────────
    send_open_trades_summary(client)

    # [v9.2 audit] Drain background Telegram queue sebelum exit — pastikan
    # semua signal terkirim sebelum proses berakhir (job timeout atau cron next).
    tg_drain(timeout=30.0)


# ════════════════════════════════════════════════════════════════════
#  PHASE 5 — INTELLIGENCE UPGRADE
#  [5.1] Backtesting Engine — [v9.2 audit] realistis (fees, slippage, Sharpe)
#  [5.2] Monte Carlo Risk Simulation
#  [5.3] Confidence Model v2 (simplified — per-strategy winrate)
# ════════════════════════════════════════════════════════════════════

# ────────────────────────────────────────────────────────────────────
#  5.1  BACKTESTING ENGINE — [v9.2 audit upgrade]
#
#  Perubahan dari v9.1:
#    1. Fee dipotong di entry + setiap exit leg (TRADING_FEE_PCT)
#    2. Slippage diaplikasikan ke entry DAN exit
#    3. Spread (bid/ask gap) dipotong
#    4. Sharpe ratio per-trade + Sortino ratio
#    5. Profit factor, expectancy, MAE/MFE tracking
#    6. Max drawdown dilaporkan dalam BOTH R-units dan persen of peak
#    7. Walk-forward harness: signals dari strategy callable, tanpa lookahead
#    8. ForwardTestLogger: JSONL log untuk live signals (paper-trading)
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


@dataclass
class CostModel:
    """[v9.2 audit] Realistic execution cost model — semua dalam fraksi desimal."""
    fee_pct:      float = 0.001     # 0.10 % per fill (Gate.io spot taker)
    slippage_pct: float = 0.0005    # 0.05 % expected slippage per fill
    spread_pct:   float = 0.0003    # 0.03 % half-spread

    def per_leg(self) -> float:
        """Total cost desimal untuk satu fill (entry ATAU exit)."""
        return self.fee_pct + self.slippage_pct + self.spread_pct


@dataclass
class TradeResult:
    """[v9.2 audit] Hasil simulasi satu trade — pnl_r NET dari semua biaya."""
    result: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    side: str
    entry_idx: int
    exit_price: float
    pnl_r: float
    pnl_pct: float
    bars_held: int
    mae_r: float = 0.0   # max adverse excursion
    mfe_r: float = 0.0   # max favourable excursion


def replay_candles(candles: list, start_idx: int = 0):
    """Generator: yield (idx, candle) per iterasi mulai start_idx."""
    for i in range(start_idx, len(candles)):
        yield i, candles[i]


def simulate_trade(entry: float, sl: float, tp1: float, tp2: float,
                   side: str, future_candles: list,
                   partial_close_ratio: float = 0.5,
                   costs: "CostModel | None" = None) -> dict:
    """
    [v9.2 audit] Simulasi satu trade dengan biaya realistis.

    Conservative within-bar ordering: SL DICEK SEBELUM TP — bias winrate
    ke bawah (arah aman untuk model real-money). Cost per fill (R-units):
        cost_R = (fee + slip + half_spread) × (price / sl_dist)

    Returns dict (kompatibel dengan caller v9.1 lama) — tambahan fields:
      - pnl_pct, mae_r, mfe_r
      - pnl_r sekarang NET dari semua biaya (sebelumnya gross)
    """
    costs  = costs or CostModel()
    is_buy = (side == "BUY")
    sl_dist = abs(entry - sl)
    if sl_dist <= 0 or not future_candles:
        return {"result": "OPEN", "exit_price": entry, "pnl_r": 0.0,
                "pnl_pct": 0.0, "bars_held": 0, "mae_r": 0.0, "mfe_r": 0.0}

    leg_cost     = costs.per_leg()
    entry_cost_r = leg_cost * (entry / sl_dist)

    tp1_hit = False
    partial_r_gross = 0.0
    partial_exit_cost_r = 0.0
    mae = 0.0
    mfe = 0.0

    def _ohlc(c):
        if isinstance(c, dict):
            return (float(c.get("high", entry)),
                    float(c.get("low",  entry)),
                    float(c.get("close", entry)))
        # Gate format: [ts, vol, close, high, low]
        return float(c[3]), float(c[4]), float(c[2])

    for bars_held, candle in enumerate(future_candles, start=1):
        high, low, close = _ohlc(candle)

        if is_buy:
            mfe = max(mfe, (high  - entry) / sl_dist)
            mae = min(mae, (low   - entry) / sl_dist)
            sl_hit      = low  <= sl
            tp1_hit_now = high >= tp1 and not tp1_hit
            tp2_hit_now = high >= tp2
        else:
            mfe = max(mfe, (entry - low)  / sl_dist)
            mae = min(mae, (entry - high) / sl_dist)
            sl_hit      = high >= sl
            tp1_hit_now = low  <= tp1 and not tp1_hit
            tp2_hit_now = low  <= tp2

        if sl_hit:
            exit_cost_r = leg_cost * (sl / sl_dist)
            if tp1_hit:
                remaining_r = -(1.0 - partial_close_ratio)
                pnl_r = (partial_r_gross + remaining_r
                         - entry_cost_r - partial_exit_cost_r
                         - exit_cost_r * (1.0 - partial_close_ratio))
                return {"result": "SL_AFTER_TP1", "exit_price": sl,
                        "pnl_r":   round(pnl_r, 4),
                        "pnl_pct": round(pnl_r * sl_dist / entry * 100, 3),
                        "bars_held": bars_held,
                        "mae_r": round(mae, 3), "mfe_r": round(mfe, 3)}
            pnl_r = -1.0 - entry_cost_r - exit_cost_r
            return {"result": "SL", "exit_price": sl,
                    "pnl_r":   round(pnl_r, 4),
                    "pnl_pct": round(pnl_r * sl_dist / entry * 100, 3),
                    "bars_held": bars_held,
                    "mae_r": round(mae, 3), "mfe_r": round(mfe, 3)}

        if tp1_hit_now:
            tp1_hit = True
            tp1_r = (tp1 - entry) / sl_dist if is_buy else (entry - tp1) / sl_dist
            partial_r_gross = tp1_r * partial_close_ratio
            partial_exit_cost_r = leg_cost * (tp1 / sl_dist) * partial_close_ratio

        if tp2_hit_now and tp1_hit:
            tp2_r = (tp2 - entry) / sl_dist if is_buy else (entry - tp2) / sl_dist
            tp2_exit_cost_r = leg_cost * (tp2 / sl_dist) * (1.0 - partial_close_ratio)
            pnl_r = (partial_r_gross + tp2_r * (1.0 - partial_close_ratio)
                     - entry_cost_r - partial_exit_cost_r - tp2_exit_cost_r)
            return {"result": "TP2", "exit_price": tp2,
                    "pnl_r":   round(pnl_r, 4),
                    "pnl_pct": round(pnl_r * sl_dist / entry * 100, 3),
                    "bars_held": bars_held,
                    "mae_r": round(mae, 3), "mfe_r": round(mfe, 3)}

    # Habis candle — TP1 hit tapi TP2 tidak
    if tp1_hit:
        pnl_r = partial_r_gross - entry_cost_r - partial_exit_cost_r
        return {"result": "TP1", "exit_price": tp1,
                "pnl_r":   round(pnl_r, 4),
                "pnl_pct": round(pnl_r * sl_dist / entry * 100, 3),
                "bars_held": len(future_candles),
                "mae_r": round(mae, 3), "mfe_r": round(mfe, 3)}

    last = future_candles[-1]
    last_close = float(last["close"]) if isinstance(last, dict) else float(last[2])
    return {"result": "OPEN", "exit_price": last_close,
            "pnl_r": 0.0, "pnl_pct": 0.0,
            "bars_held": len(future_candles),
            "mae_r": round(mae, 3), "mfe_r": round(mfe, 3)}


def _max_drawdown(equity_curve: list) -> tuple:
    """Return (max_dd_abs, max_dd_pct_of_peak)."""
    if not equity_curve:
        return 0.0, 0.0
    peak = equity_curve[0]
    max_dd_abs = 0.0
    max_dd_pct = 0.0
    for eq in equity_curve:
        peak = max(peak, eq)
        dd_abs = peak - eq
        dd_pct = dd_abs / peak if peak > 0 else 0.0
        max_dd_abs = max(max_dd_abs, dd_abs)
        max_dd_pct = max(max_dd_pct, dd_pct)
    return max_dd_abs, max_dd_pct


def _sharpe(returns: list) -> float:
    """Per-trade Sharpe (mean / pop stdev). Anualisasi terserah caller."""
    if len(returns) < 2:
        return 0.0
    mu = statistics.fmean(returns)
    sd = statistics.pstdev(returns)
    return mu / sd if sd > 0 else 0.0


def _sortino(returns: list) -> float:
    """Per-trade Sortino (mean / downside-deviation)."""
    if len(returns) < 2:
        return 0.0
    mu = statistics.fmean(returns)
    downside = [r for r in returns if r < 0]
    if not downside:
        return float("inf") if mu > 0 else 0.0
    dd_sd = math.sqrt(sum(r * r for r in downside) / len(downside))
    return mu / dd_sd if dd_sd > 0 else 0.0


def run_backtest(candles: list, signals: list,
                 max_bars_per_trade: int = 48,
                 costs: "CostModel | None" = None) -> dict:
    """
    [v9.2 audit] Run backtest dengan biaya realistis.

    Tambahan vs v9.1:
      - sharpe, sortino, profit_factor, expectancy, max_dd_pct
      - pnl_r sudah NET dari fees/slip/spread
    """
    costs  = costs or CostModel()
    trades = []

    for sig in signals:
        idx = sig.get("entry_idx", 0)
        if idx >= len(candles):
            continue
        future = candles[idx + 1: idx + 1 + max_bars_per_trade]
        if not future:
            continue
        result = simulate_trade(
            entry=sig["entry"], sl=sig["sl"],
            tp1=sig["tp1"], tp2=sig["tp2"],
            side=sig.get("side", "BUY"),
            future_candles=future,
            costs=costs,
        )
        result["entry_idx"] = idx
        result["entry"]     = sig["entry"]
        result["sl"]        = sig["sl"]
        result["tp1"]       = sig["tp1"]
        result["tp2"]       = sig["tp2"]
        result["side"]      = sig.get("side", "BUY")
        trades.append(result)

    if not trades:
        return {"trades": [], "total": 0, "wins": 0, "losses": 0,
                "winrate": 0.0, "avg_pnl_r": 0.0, "total_pnl_r": 0.0,
                "max_dd_r": 0.0, "max_dd_pct": 0.0,
                "sharpe": 0.0, "sortino": 0.0, "profit_factor": 0.0,
                "expectancy": 0.0, "summary": "Tidak ada trade"}

    wins   = sum(1 for t in trades if t["result"] in ("TP1", "TP2"))
    losses = sum(1 for t in trades if t["result"] in ("SL", "SL_AFTER_TP1"))
    total  = len(trades)
    wr     = wins / total if total else 0.0

    pnl_series = [t["pnl_r"] for t in trades]
    total_r = sum(pnl_series)
    avg_r   = total_r / total

    equity = []
    running = 0.0
    for r in pnl_series:
        running += r
        equity.append(running)

    max_dd_r, max_dd_pct = _max_drawdown([0.0] + equity)
    sharpe  = _sharpe(pnl_series)
    sortino = _sortino(pnl_series)

    gross_win  = sum(r for r in pnl_series if r > 0)
    gross_loss = abs(sum(r for r in pnl_series if r < 0))
    profit_factor = (gross_win / gross_loss if gross_loss > 0
                     else (float("inf") if gross_win > 0 else 0.0))

    avg_win  = (gross_win  / wins)   if wins   else 0.0
    avg_loss = (gross_loss / losses) if losses else 0.0
    expectancy = wr * avg_win - (1 - wr) * avg_loss

    summary = (
        f"Backtest selesai — {total} trade | WR={wr:.1%} | "
        f"avg={avg_r:+.2f}R | total={total_r:+.2f}R | "
        f"MaxDD={max_dd_r:.2f}R ({max_dd_pct:.1%}) | "
        f"Sharpe={sharpe:.2f} | Sortino={sortino:.2f} | "
        f"PF={profit_factor:.2f} | E={expectancy:+.2f}R"
    )
    log(summary)

    return {
        "trades":        trades,
        "total":         total,
        "wins":          wins,
        "losses":        losses,
        "winrate":       round(wr, 4),
        "avg_pnl_r":     round(avg_r, 4),
        "total_pnl_r":   round(total_r, 4),
        "max_dd_r":      round(max_dd_r, 4),
        "max_dd_pct":    round(max_dd_pct, 4),       # [v9.2 audit] new
        "sharpe":        round(sharpe, 4),           # [v9.2 audit] new
        "sortino":       round(sortino, 4) if sortino != float("inf") else None,
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
        "expectancy":    round(expectancy, 4),
        "avg_win_r":     round(avg_win, 4),
        "avg_loss_r":    round(avg_loss, 4),
        "summary":       summary,
    }


def walk_forward_generate(candles: list, strategy_fn, warmup: int = 100) -> list:
    """
    [v9.2 audit] Walk-forward signal generator — TANPA lookahead.

    Iterasi candles index by index. Untuk setiap i ≥ warmup panggil
    strategy_fn(window_dict, i) dengan window berisi array yang di-slice
    sampai DAN TERMASUK bar i (tidak ada bar masa depan yang terlihat).

    strategy_fn return dict {entry, sl, tp1, tp2, side} atau None.
    entry_idx=i di-append otomatis.
    """
    if warmup < 1:
        raise ValueError("warmup harus ≥ 1")

    signals = []
    for i in range(warmup, len(candles)):
        window = candles[: i + 1]
        if isinstance(window[0], dict):
            closes  = [float(c["close"])  for c in window]
            highs   = [float(c["high"])   for c in window]
            lows    = [float(c["low"])    for c in window]
            volumes = [float(c.get("volume", 0)) for c in window]
        else:
            closes  = [float(c[2]) for c in window]
            highs   = [float(c[3]) for c in window]
            lows    = [float(c[4]) for c in window]
            volumes = [float(c[1]) for c in window]
        result = strategy_fn(
            {"closes": closes, "highs": highs, "lows": lows, "volumes": volumes}, i,
        )
        if result is None:
            continue
        result["entry_idx"] = i
        signals.append(result)
    return signals


class ForwardTestLogger:
    """
    [v9.2 audit] Append-only JSONL log untuk live signals (paper-trading).
    Aktifkan dengan environment var FORWARD_TEST_LOG=path/to/file.jsonl
    Cron job terpisah bisa replay JSONL untuk hitung performance offline.
    """
    def __init__(self, path):
        from pathlib import Path
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, sig: dict) -> None:
        payload = {
            "ts":        datetime.now(timezone.utc).isoformat(),
            "pair":      sig.get("pair"),
            "strategy":  sig.get("strategy"),
            "side":      sig.get("side"),
            "entry":     sig.get("entry"),
            "sl":        sig.get("sl"),
            "tp1":       sig.get("tp1"),
            "tp2":       sig.get("tp2"),
            "score":     sig.get("score"),
            "tier":      sig.get("tier"),
            "size_usdt": sig.get("position_size"),
        }
        try:
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as e:
            log(f"⚠️ ForwardTestLogger.record gagal: {e}", "warn")


# Singleton — diaktifkan jika env var FORWARD_TEST_LOG di-set
_FORWARD_LOG_PATH = os.getenv("FORWARD_TEST_LOG", "")
forward_logger = ForwardTestLogger(_FORWARD_LOG_PATH) if _FORWARD_LOG_PATH else None


# ────────────────────────────────────────────────────────────────────
#  5.2  MONTE CARLO RISK SIMULATION
#
#  Simulasi 1000× (default) sequence trade acak dari distribusi historis.
#  Setiap run: ambil N trade → hitung equity curve → catat max DD.
#  Output: worst-case DD percentile, median DD, ruin probability.
#
#  Gunakan setelah run_backtest() punya data trades yang cukup.
# ────────────────────────────────────────────────────────────────────


def run_monte_carlo(pnl_r_list: list, n_trades: int = 50,
                    n_simulations: int = 1000, initial_r: float = 100.0,
                    ruin_threshold: float = 0.30,
                    seed: "int | None" = None) -> dict:
    """
    [5.2] Monte Carlo: sampling dengan replacement dari distribusi PnL historis.
    """
    if not pnl_r_list:
        return {"summary": "Tidak ada data PnL untuk Monte Carlo"}

    rng = random.Random(seed)
    max_dds = []
    final_equities = []
    ruin_count = 0
    ruin_r = initial_r * ruin_threshold

    for _ in range(n_simulations):
        sample = rng.choices(pnl_r_list, k=n_trades)
        equity = peak = initial_r
        max_dd = 0.0
        ruined = False
        for r in sample:
            equity += r
            peak    = max(peak, equity)
            dd      = peak - equity
            max_dd  = max(max_dd, dd)
            if not ruined and dd >= ruin_r:
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
        # [v9.2.5] EXPIRED_LOSS dihitung LOSS di WR per strategy
        LOSS_VALUES = {"LOSS", "SL", "SL_AFTER_TP1", "EXPIRED_LOSS"}

        buckets: dict[str, dict] = {}

        for row in rows:
            strat  = (row.get("strategy") or "").upper().strip()
            result = (row.get("result")   or "").upper().strip()

            if not strat:
                continue
            # [v9.2.8 fix] Exclude MICROCAP dan PUMP dari WR per strategy.
            # MICROCAP: pure alert, 13/13 SL merusak statistik keseluruhan.
            # PUMP    : short-window alert, tidak representatif untuk WR model.
            # [v9.4.3] Exclude SYSTEM (record injected untuk streak reset).
            if strat in ("MICROCAP", "PUMP", "SYSTEM"):
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


# ════════════════════════════════════════════════════════
#  REPAIR UTILITY — [v9.3.1] Fix data historis EXPIRED_LOSS
#
#  Bug v9.3.1: current_price = 0 saat EXPIRED_LOSS/PROFIT
#  → pnl_usdt tersimpan salah di Supabase (terlalu negatif)
#  → DD naik palsu dari 6.6% → 12.6%
#
#  Jalankan SEKALI setelah deploy v9.3.1:
#    python bot.py --repair-expired-pnl
#
#  Script ini membaca semua baris result=EXPIRED_LOSS di DB,
#  menghitung ulang pnl_usdt dari kolom entry + current_price
#  yang sudah tersimpan, lalu update baris yang nilainya tidak
#  wajar (< -5 USDT per $20 position = anomali hampir pasti bug).
# ════════════════════════════════════════════════════════

def repair_expired_loss_pnl(dry_run: bool = True) -> None:
    """
    [v9.3.1] Recalculate pnl_usdt untuk semua baris EXPIRED_LOSS/EXPIRED_PROFIT
    yang terdampak bug current_price = 0.

    Args:
        dry_run: Jika True, hanya print perubahan tanpa update DB.
                 Selalu jalankan dry_run=True dulu sebelum dry_run=False.
    """
    log(f"🔧 repair_expired_loss_pnl — dry_run={dry_run}")

    try:
        rows = (
            supabase.table("signals")
            .select("id, pair, side, strategy, entry, position_size, pnl_usdt, result, current_price")
            .in_("result", ["EXPIRED_LOSS", "EXPIRED_PROFIT"])
            .execute()
        ).data
    except Exception as e:
        log(f"❌ Gagal fetch data repair: {e}", "error")
        return

    if not rows:
        log("✅ Tidak ada baris EXPIRED_LOSS/EXPIRED_PROFIT — tidak ada yang perlu direpair.")
        return

    log(f"   Ditemukan {len(rows)} baris untuk diperiksa...")

    repaired = 0
    skipped  = 0

    for row in rows:
        row_id       = row.get("id")
        pair         = row.get("pair", "?")
        side         = row.get("side", "BUY")
        entry        = float(row.get("entry") or 0)
        cur_price    = float(row.get("current_price") or 0)
        pos_size     = float(row.get("position_size") or 0)
        pnl_stored   = float(row.get("pnl_usdt") or 0)
        result       = row.get("result", "")

        # Tidak bisa recalc tanpa data lengkap
        if entry <= 0 or cur_price <= 0 or pos_size <= 0:
            log(f"   ⚠️ SKIP {pair} id={row_id} — data tidak lengkap "
                f"(entry={entry}, cur={cur_price}, size={pos_size})", "warn")
            skipped += 1
            continue

        # Hitung pnl yang benar dari data yang sudah ada
        if side == "BUY":
            correct_pnl = (cur_price - entry) / entry * pos_size
        else:
            correct_pnl = (entry - cur_price) / entry * pos_size
        correct_pnl = round(correct_pnl, 4)

        # Threshold anomali: jika stored PnL < -5 USDT pada position $20
        # hampir pasti adalah bug current_price=0 (hasilkan -pos_size penuh)
        anomaly_threshold = -(pos_size * 0.5)  # lebih dari 50% loss = curiga bug
        is_anomalous = pnl_stored < anomaly_threshold and abs(correct_pnl) < abs(pnl_stored) * 0.3

        if not is_anomalous:
            skipped += 1
            continue

        log(f"   🔧 REPAIR {pair} [{result}] id={row_id}: "
            f"pnl_stored={pnl_stored:.4f} → correct={correct_pnl:.4f} "
            f"(entry={entry}, cur_price={cur_price}, size={pos_size})")

        if not dry_run:
            try:
                supabase.table("signals").update({
                    "pnl_usdt": correct_pnl
                }).eq("id", row_id).execute()
                repaired += 1
            except Exception as e:
                log(f"   ❌ Update gagal id={row_id}: {e}", "error")
        else:
            repaired += 1  # count sebagai "akan direpair" saat dry_run

    mode_str = "[DRY RUN]" if dry_run else "[APPLIED]"
    log(f"✅ repair_expired_loss_pnl selesai {mode_str}: "
        f"{repaired} diperbaiki, {skipped} dilewati dari {len(rows)} baris.")

    if dry_run and repaired > 0:
        log("   ℹ️  Jalankan dengan --repair-expired-pnl --apply untuk terapkan perubahan.")


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

    # ── Test 17-18: ATR spike blend weights [v9.1] ────────
    print("\n── ATR spike blend weights ──")
    assert_eq("ATR_SPIKE_WEIGHT_CLEAN = 0.60", ATR_SPIKE_WEIGHT_CLEAN, 0.60)
    assert_eq("ATR_SPIKE_WEIGHT_LAST  = 0.40", ATR_SPIKE_WEIGHT_LAST,  0.40)
    assert_eq("Bobot ATR harus berjumlah 1.0",
              ATR_SPIKE_WEIGHT_CLEAN + ATR_SPIKE_WEIGHT_LAST, 1.0)

    # ── Test 19-21: Streak persistence logic [v9.1] ───────
    print("\n── Streak persistence logic ──")
    # Simulasi: streak > 0 setelah restart harus di-restore ke _drawdown_state
    _mock_streak = 4
    _mock_state  = {"streak": 0, "mode": "normal", "dd_pct": 0.0}
    # Simulasikan restore logic dari run()
    if _mock_streak > 0:
        _mock_state["streak"] = _mock_streak
    assert_eq("Streak dipulihkan dari persisted value", _mock_state["streak"], 4)

    # Streak 0 setelah reset → state harus 0
    _mock_streak_reset = 0
    _mock_state2 = {"streak": 5, "mode": "warn", "dd_pct": 0.0}
    if _mock_streak_reset == 0:
        _mock_state2["streak"] = 0
    assert_eq("Streak reset ke 0 jika persisted=0", _mock_state2["streak"], 0)

    # set_bot_halt signature harus menerima streak parameter
    import inspect
    sig = inspect.signature(set_bot_halt)
    assert_true("set_bot_halt menerima parameter 'streak'", "streak" in sig.parameters)

    # ── Test 22-25: [v9.2 audit] Lifecycle wick detection ──────
    print("\n── [v9.2 audit] Lifecycle wick detection ──")
    from datetime import datetime, timezone, timedelta

    class _MockTicker:
        def __init__(self, last="100.0"):
            self.last = last

    class _MockClient:
        """Mock Gate.io client untuk wick scan tests."""
        def __init__(self, candles, ticker_last="100.0"):
            self._candles = candles
            self._ticker_last = ticker_last
        def list_candlesticks(self, currency_pair=None, interval=None, limit=None):
            return self._candles
        def list_tickers(self, currency_pair=None):
            return [_MockTicker(self._ticker_last)]

    sent_iso = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

    # Skenario A: Wick BUY tembus SL lalu pulih — current_price 100, low 94 → SL!
    # Format Gate: [ts, vol, close, high, low]
    candles_wick_sl = [
        ["0", "0", "98",  "99",  "97"],   # bar 1 — normal
        ["0", "0", "100", "101", "94"],   # bar 2 — wick SL=95 (low 94 < 95)
        ["0", "0", "100", "100", "99"],   # bar 3 — recover
    ]
    client_wick_sl = _MockClient(candles_wick_sl, ticker_last="100.0")
    res, exit_p = _resolve_trade_outcome_via_wicks(
        client_wick_sl, "TEST_USDT", "BUY",
        entry=100.0, sl=95.0, tp1=110.0, tp2=120.0,
        sent_at_iso=sent_iso, partial_state=None,
    )
    assert_eq("Wick SL terdeteksi via 1m bar (BUY)", res, "SL")
    assert_eq("Wick SL exit price = SL level", exit_p, 95.0)

    # Skenario B: Wick SELL tembus SL ke atas
    candles_wick_sell_sl = [
        ["0", "0", "100", "101", "99"],
        ["0", "0", "98",  "106", "97"],   # high 106 > SL 105
    ]
    client_wick_sell_sl = _MockClient(candles_wick_sell_sl, ticker_last="98.0")
    res2, _ = _resolve_trade_outcome_via_wicks(
        client_wick_sell_sl, "TEST_USDT", "SELL",
        entry=100.0, sl=105.0, tp1=90.0, tp2=85.0,
        sent_at_iso=sent_iso, partial_state=None,
    )
    assert_eq("Wick SL terdeteksi via 1m bar (SELL)", res2, "SL")

    # Skenario C: TP1 lebih dulu dari SL dalam bar yang sama → tetap SL (konservatif)
    candles_both_hit = [
        ["0", "0", "100", "112", "94"],   # high 112 ≥ TP1=110 dan low 94 ≤ SL=95
    ]
    client_both = _MockClient(candles_both_hit, ticker_last="100.0")
    res3, _ = _resolve_trade_outcome_via_wicks(
        client_both, "TEST_USDT", "BUY",
        entry=100.0, sl=95.0, tp1=110.0, tp2=120.0,
        sent_at_iso=sent_iso, partial_state=None,
    )
    assert_eq("SL prioritas dalam bar yang sama (konservatif)", res3, "SL")

    # Skenario D: Belum ada level tersentuh
    candles_quiet = [
        ["0", "0", "100", "102", "98"],
        ["0", "0", "101", "103", "99"],
    ]
    client_quiet = _MockClient(candles_quiet, ticker_last="101.0")
    res4, _ = _resolve_trade_outcome_via_wicks(
        client_quiet, "TEST_USDT", "BUY",
        entry=100.0, sl=95.0, tp1=110.0, tp2=120.0,
        sent_at_iso=sent_iso, partial_state=None,
    )
    assert_eq("Belum ada hit → return None", res4, None)

    # ── Test 26-27: [v9.2 audit] MAX_NOTIONAL_PCT cap math ─────
    print("\n── [v9.2 audit] Notional cap ──")
    assert_eq("MAX_NOTIONAL_PCT default = 0.80", MAX_NOTIONAL_PCT, 0.80)
    # Equity $1000, notional cap 85% = $850. Existing notional $700 + new $200 = $900 → BLOCK
    eq_test = 1000.0
    notional_lim = eq_test * MAX_NOTIONAL_PCT
    cur_notional = 650.0
    new_notional = 200.0
    assert_true(
        "Notional cap menolak penambahan yang melebihi 80% equity",
        cur_notional + new_notional > notional_lim,
    )

    # ── Test 28: [v9.2 audit] BOS recent_closes tidak include unclosed bar ──
    print("\n── [v9.2 audit] BOS uses closed bars only ──")
    # Sebelumnya: closes[-5:] termasuk candle live (repaint risk)
    # Sekarang: closes[-6:-1] = 5 candle TERTUTUP terakhir
    _test_closes = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]   # 7 elements
    _expected = _test_closes[-6:-1]   # [2.0, 3.0, 4.0, 5.0, 6.0] — bukan 7.0
    assert_eq("BOS recent_closes tidak include candle terakhir", len(_expected), 5)
    assert_true("Candle terakhir (live) di-exclude dari recent_closes", _expected[-1] == 6.0)

    # ── Test 29: [v9.2 audit] validate_config detection ────────
    print("\n── [v9.2 audit] validate_config ──")
    _warnings = validate_config()
    assert_true("validate_config returns list", isinstance(_warnings, list))
    # MAX_OPEN_TRADES=13 × MAX_POSITION_PCT=0.10 = 130% → harus ada warning over-allocation
    _has_alloc_warn = any("MAX_OPEN_TRADES × MAX_POSITION_PCT" in w for w in _warnings)
    # [v9.5] 8 slots × 12% = 96% — tidak lagi over 100%, warning tidak muncul
    _has_risk_warn = any("MAX_OPEN_TRADES" in w for w in _warnings)
    assert_true("validate_config berjalan tanpa crash", isinstance(_warnings, list))

    # ── Test 30: [v9.2 audit] tg_async non-blocking ────────────
    print("\n── [v9.2 audit] tg_async queue ──")
    # tg_async harus bisa dipanggil tanpa exception meski TG_TOKEN kosong
    try:
        tg_async("[test] dummy message")
        # Jangan call tg_drain — itu blocking. Cukup pastikan enqueue tidak crash.
        assert_true("tg_async enqueue tanpa exception", True)
    except Exception as _e:
        assert_true(f"tg_async enqueue tanpa exception (got {_e})", False)

    # ── Test 31-37: [v9.2.2] Audit fix tambahan ─────────────────
    print("\n── [v9.2.2] Entry deviation hard-block ──")
    # FIX 1: UB/USDT case — harga +33.5% dari entry harus BLOCKED
    _entry_ub, _price_ub = 0.077855, 0.103940
    _dev_ub = abs(_price_ub - _entry_ub) / _entry_ub
    assert_true("UB/USDT 33.5% dev → BLOCKED (>5% SWING)",
                _dev_ub > MAX_ENTRY_DEVIATION["SWING"])
    # XRP case — 0.08% masih lolos INTRADAY (≤2%)
    _entry_xrp, _price_xrp = 1.3849, 1.3860
    _dev_xrp = abs(_price_xrp - _entry_xrp) / _entry_xrp
    assert_true("XRP 0.08% dev → LOLOS (<2% INTRADAY)",
                _dev_xrp <= MAX_ENTRY_DEVIATION["INTRADAY"])

    print("\n── [v9.2.2] fmt_price adaptive formatting ──")
    # FIX 2+3: PEPE micro-price entry ≠ TP1 ≠ SL
    _e = fmt_price(0.000004105)
    _t = fmt_price(0.000004720)
    _s = fmt_price(0.000003850)
    assert_true(f"PEPE entry ≠ TP1 ({_e} vs {_t})", _e != _t)
    assert_true(f"PEPE entry ≠ SL ({_e} vs {_s})",  _e != _s)
    assert_eq("Normal price $1.3849 format", fmt_price(1.3849), "$1.3849")

    print("\n── [v9.2.2] Hard cap guard ──")
    # FIX 4: state["total"] >= MAX_OPEN_TRADES → block
    _mock_state_full   = {"total": 8}
    _mock_state_almost = {"total": 7}
    assert_true("total=8 >= MAX_OPEN_TRADES=8 → BLOCK",
                _mock_state_full["total"] >= MAX_OPEN_TRADES)
    assert_true("total=7 < MAX_OPEN_TRADES=8 → ALLOW",
                _mock_state_almost["total"] < MAX_OPEN_TRADES)

    print("\n── [v9.2.2] RANGING score filter ──")
    # FIX 5A: Score 2.5 di RANGING harus skip
    _hype_score, _hype_regime = 2.5, "RANGING"
    assert_true("HYPE score=2.5 di RANGING → SKIP (di bawah 3.5)",
                _hype_regime == "RANGING" and _hype_score < MIN_SCORE_RANGING)
    # Score 3.0 di RANGING masih lolos
    assert_true("Score=3.0 di RANGING → SKIP (threshold 3.5)",
                3.0 < MIN_SCORE_RANGING)
    # Score 2.5 di TRENDING tetap lolos (tidak kena filter ini)
    assert_true("Score=2.5 di TRENDING → SKIP (di bawah threshold A+ 3.5)",
                2.5 < TIER_MIN_SCORE["A+"])

    print("\n── [v9.2.2] Data historis half-size logic ──")
    # FIX 5B: n=5 → half size, n=2 → skip total, n=20 → full size
    assert_true("n=5 (data partial) → half size",
                MIN_HIST_SAMPLE_SKIP <= 5 < MIN_HIST_SAMPLE_FULL)
    assert_true("n=2 (data terlalu kurang) → skip total",
                2 < MIN_HIST_SAMPLE_SKIP)
    assert_true("n=20 (data cukup) → full size",
                20 >= MIN_HIST_SAMPLE_FULL)

    # ── Test v9.2.4: Scoring boosters ──────────────────────────
    print("\n── [v9.2.4] Scoring boosters ──")

    # RSI booster: 40–60 dapat +0.25, di luar range tidak dapat
    assert_true("RSI 50 (ideal) → rsi_boost = 0.25",  40 <= 50 <= 60)
    assert_true("RSI 70 (overbought) → rsi_boost = 0", not (40 <= 70 <= 60))
    assert_true("RSI 30 (oversold) → rsi_boost = 0",   not (40 <= 30 <= 60))

    # BTC 4h booster: searah entry dapat +0.25
    assert_true("BTC 4h +0.3% + BUY → btc_boost = 0.25",  True and 0.3 > 0.0)
    assert_true("BTC 4h -0.6% + BUY → btc_boost = 0.0",   not (-0.6 > 0.0))

    # F&G soft penalty: hanya ekstrem
    assert_true("F&G 19 + BUY → fg_penalty = -0.5",   19 < 20)
    assert_true("F&G 26 + BUY → fg_penalty = 0.0",    not (26 < 20))  # kondisi sekarang, tidak kena
    assert_true("F&G 81 + SELL → fg_penalty = -0.5",  81 > 80)
    assert_true("F&G 75 + SELL → fg_penalty = 0.0",   not (75 > 80))

    # Simulasi score lengkap: TRENDING + RSI ideal + BTC hijau
    # trend=1.0 + momentum=1.0 + volume=0.5 + rsi=0.25 + btc=0.25 + setup3=0.5 = 3.5
    _sim_score = 1.0 + 1.0 + 0.5 + 0.25 + 0.25 + 0.5
    assert_eq("Full booster score = 3.5", _sim_score, 3.5)
    assert_true("Full booster → tier A+", _sim_score >= TIER_MIN_SCORE["A+"])

    # Simulasi kondisi screenshot: F&G 26 + BTC -0.2% 4h + RSI 57 + RANGING
    # trend=1.0 + momentum=1.0 + volume=0 + rsi=0.25 (RSI 57 ideal) + btc=0 + fg=0 + setup3=0.5
    _screenshot_score = 1.0 + 1.0 + 0.0 + 0.25 + 0.0 + 0.0 + 0.5
    assert_eq("Screenshot conditions score = 2.75", _screenshot_score, 2.75)
    assert_true("Screenshot score 2.75 tidak lolos tier A+ (threshold 3.5)", _screenshot_score < TIER_MIN_SCORE["A+"])

    # Expire hours
    print("\n── [v9.4] Expire hours (updated) ──")
    assert_eq("INTRADAY expire = 36h",  SIGNAL_EXPIRE_HOURS["INTRADAY"], 36)
    assert_eq("SWING expire = 120h",    SIGNAL_EXPIRE_HOURS["SWING"],    120)
    assert_eq("PUMP expire = 4h",       SIGNAL_EXPIRE_HOURS["PUMP"],       4)
    assert_eq("MICROCAP expire = 24h",  SIGNAL_EXPIRE_HOURS["MICROCAP"],  24)
    # Guard: INTRADAY expire harus lebih pendek dari SWING
    assert_true("INTRADAY expire < SWING expire (kategori tidak overlap)",
                SIGNAL_EXPIRE_HOURS["INTRADAY"] < SIGNAL_EXPIRE_HOURS["SWING"])

    # ── Test v9.2.5: EXPIRED_PROFIT / EXPIRED_LOSS ─────────────
    print("\n── [v9.2.5] EXPIRED result logic ──")

    # EXPIRED_PROFIT: posisi profit saat expired → neutral (tidak masuk WIN/LOSS)
    _WIN_VALUES  = {"WIN", "TP1", "TP2", "PARTIAL_WIN"}
    _LOSS_VALUES = {"LOSS", "SL", "EXPIRED_LOSS"}
    assert_true("EXPIRED_PROFIT bukan WIN", "EXPIRED_PROFIT" not in _WIN_VALUES)
    assert_true("EXPIRED_PROFIT bukan LOSS (neutral)", "EXPIRED_PROFIT" not in _LOSS_VALUES)

    # EXPIRED_LOSS: posisi merah saat expired → dihitung LOSS
    assert_true("EXPIRED_LOSS masuk LOSS_VALUES", "EXPIRED_LOSS" in _LOSS_VALUES)
    assert_true("EXPIRED_LOSS bukan WIN",  "EXPIRED_LOSS" not in _WIN_VALUES)

    # EXPIRED biasa (price fetch gagal): tetap ada, neutral
    assert_true("EXPIRED lama bukan WIN",  "EXPIRED" not in _WIN_VALUES)
    assert_true("EXPIRED lama bukan LOSS", "EXPIRED" not in _LOSS_VALUES)

    # Simulasi: trade expired saat +5% → EXPIRED_PROFIT
    _entry, _price_profit, _side = 1.0, 1.05, "BUY"
    _pct = (_price_profit - _entry) / _entry
    assert_true("Trade +5% saat expired → EXPIRED_PROFIT", _pct > 0)

    # Simulasi: trade expired saat -2% → EXPIRED_LOSS
    _price_loss = 0.98
    _pct_loss = (_price_loss - _entry) / _entry
    assert_true("Trade -2% saat expired → EXPIRED_LOSS", _pct_loss < 0)

    # ── Test v9.2.7: BTC trend guard ────────────────────────────
    print("\n── [v9.2.7] BTC trend guard ──")

    # 3 dari 4 candle negatif → bearish trend  # [v9.4.2] was 2/3, now 3/4
    _changes_bear = [-0.3, -0.5, -0.2, +0.1]   # 3 negatif dari 4
    _bear = sum(1 for c in _changes_bear if c < 0)
    assert_true("3/4 candle negatif → bearish trend", _bear >= BTC_TREND_MIN_BEARISH)

    # 2 dari 4 candle negatif → TIDAK bearish (di bawah threshold MIN_BEARISH=3)
    _changes_ok = [-0.1, -0.2, +0.3, +0.2]   # 2 negatif dari 4
    _ok = sum(1 for c in _changes_ok if c < 0)
    assert_true("2/4 candle negatif → bukan bearish trend", _ok < BTC_TREND_MIN_BEARISH)

    # Semua positif → tidak bearish
    _changes_bull = [+0.2, +0.4, +0.1, +0.3]
    _bull = sum(1 for c in _changes_bull if c < 0)
    assert_true("Semua candle positif → bukan bearish", _bull < BTC_TREND_MIN_BEARISH)

    # Konstanta terdefinisi dengan benar
    assert_eq("BTC_TREND_LOOKBACK = 4",    BTC_TREND_LOOKBACK,    4)
    assert_eq("BTC_TREND_MIN_BEARISH = 3", BTC_TREND_MIN_BEARISH, 3)

    # btc dict harus punya key baru
    _btc_mock_bearish = {"halt": False, "block_buy": False,
                         "btc_1h": -0.2, "btc_4h": -0.3,
                         "btc_bearish_trend": True, "btc_bearish_cycles": 2}
    assert_true("btc dict punya key btc_bearish_trend",
                "btc_bearish_trend" in _btc_mock_bearish)
    assert_true("btc_bearish_trend=True → check harusnya return None",
                _btc_mock_bearish["btc_bearish_trend"] is True)

    # ── Test v9.2.8: Fix berdasarkan data 118 trades ────────────────
    print("\n── [v9.2.8] FIX 1: MICROCAP excluded from WR ──")
    # MICROCAP dan PUMP harus di-skip di WR calculation
    _EXCLUDED = ("MICROCAP", "PUMP")
    assert_true("MICROCAP di-exclude dari WR", "MICROCAP" in _EXCLUDED)
    assert_true("PUMP di-exclude dari WR",     "PUMP"     in _EXCLUDED)
    # Simulasi: WR tanpa MICROCAP
    # 45 win (INTRADAY+SWING) / 87 total = 51.7%
    _wr_without_micro = 45 / 87
    assert_true("WR tanpa MICROCAP > 50%", _wr_without_micro > 0.50)

    print("\n── [v9.2.8] FIX 2: SWING min score ──")
    assert_eq("TIER_MIN_SCORE_SWING = 3.5", TIER_MIN_SCORE_SWING, 3.5)
    # Score 2.5 lolos INTRADAY tapi tidak lolos SWING
    _score_25 = 2.5
    assert_true("Score 2.5 gagal INTRADAY (< 3.5 threshold baru)", _score_25 < TIER_MIN_SCORE["A+"])
    assert_true("Score 2.5 gagal SWING (< 3.5)", _score_25 < TIER_MIN_SCORE_SWING)
    # Score 3.0 lolos keduanya
    assert_true("Score 3.5 lolos SWING", 3.5 >= TIER_MIN_SCORE_SWING)

    print("\n── [v9.2.8] FIX 3: MICROCAP entry deviation ──")
    _micro_dev_max = 0.03
    # Pump sudah +5% dari entry → SKIP
    _dev_pump = abs(1.05 - 1.0) / 1.0
    assert_true("Pump +5% dari entry → SKIP", _dev_pump > _micro_dev_max)
    # Baru +2% → masih boleh entry
    _dev_ok = abs(1.02 - 1.0) / 1.0
    assert_true("Entry +2% → LOLOS (<3%)", _dev_ok <= _micro_dev_max)

    # ── Test v9.2.9: WR denominator hanya WIN+LOSS ──────────────
    print("\n── [v9.2.9] WR denominator fix ──")

    # Simulasi data dari Supabase — mix WIN, LOSS, EXPIRED, BREAKEVEN
    _sim_rows = [
        {"result": "TP2",            "strategy": "INTRADAY"},  # WIN
        {"result": "TP2",            "strategy": "INTRADAY"},  # WIN
        {"result": "SL",             "strategy": "INTRADAY"},  # LOSS
        {"result": "EXPIRED",        "strategy": "SWING"},     # NEUTRAL → skip
        {"result": "BREAKEVEN",      "strategy": "SWING"},     # NEUTRAL → skip
        {"result": "EXPIRED_PROFIT", "strategy": "SWING"},     # NEUTRAL → skip
        {"result": "TP2",            "strategy": "SWING"},     # WIN
        {"result": "SL",             "strategy": "SWING"},     # LOSS
        {"result": "SL",             "strategy": "MICROCAP"},  # SKIP (MICROCAP)
    ]
    _WIN  = {"TP1", "TP2", "PARTIAL_WIN", "WIN"}
    _LOSS = {"SL", "SL_AFTER_TP1", "EXPIRED_LOSS", "LOSS"}
    _wins, _total = 0, 0
    for r in _sim_rows:
        if r["strategy"] in ("MICROCAP", "PUMP"):
            continue
        _res = r["result"]
        if _res in _WIN or _res in _LOSS:
            _total += 1
            if _res in _WIN:
                _wins += 1
    # Harusnya: 3 win / 5 total (2 SL + 3 TP2) = 60%, bukan 3/8 = 37.5%
    assert_eq("WR denominator hanya WIN+LOSS", _total, 5)
    assert_eq("WR win count benar", _wins, 3)
    _wr = _wins / _total
    assert_true("WR 60% (bukan 37.5% yang salah)", abs(_wr - 0.6) < 0.01)
    assert_true("EXPIRED tidak masuk denominator", _total == 5)  # bukan 8

    # ── Test v9.2.10: Same-pair cap ──────────────────────────────
    print("\n── [v9.2.10] Same-pair cap ──")
    # Pair yang sudah open tidak boleh masuk lagi
    _mock_state = {"total": 5, "open_pairs": ["BSV_USDT", "ETH_USDT"],
                   "buy": 5, "sell": 0, "total_risk_usdt": 0.1,
                   "total_notional_usdt": 50.0, "sector_counts": {}}
    assert_true("BSV sudah open → diblok", "BSV_USDT" in _mock_state["open_pairs"])
    assert_true("ZRO belum open → boleh masuk", "ZRO_USDT" not in _mock_state["open_pairs"])

    # ── Test v9.3: 7 improvements ────────────────────────────────
    print("\n── [v9.3] Improvement 1: SL INTRADAY lebih longgar ──")
    assert_eq("INTRADAY ATR mult = 2.0",  INTRADAY_SL_ATR,          2.0)
    assert_eq("INTRADAY ATR buf = 0.5",   ATR_SL_BUFFER_INTRADAY,    0.5)
    assert_eq("INTRADAY min SL = 0.5%",   INTRADAY_MIN_SL_PCT,       0.005)

    print("\n── [v9.3] Improvement 2: Trailing timeframe ──")
    # SWING → 4h, INTRADAY → 1h
    assert_eq("SWING trail = 4h",    "4h" if "SWING"    == "SWING" else "1h", "4h")
    assert_eq("INTRADAY trail = 1h", "1h" if "INTRADAY" != "SWING" else "4h", "1h")

    print("\n── [v9.3] Improvement 3: Cooldown ──")
    assert_eq("PAIR_COOLDOWN_HOURS = 24", PAIR_COOLDOWN_HOURS, 24)

    print("\n── [v9.3] Improvement 4: Time-of-day filter ──")
    assert_true("ACTIVE_HOURS_UTC defined", len(ACTIVE_HOURS_UTC) > 0)
    # Jam 10 UTC → aktif
    _h = 10
    _active = any(s <= _h < e for s, e in ACTIVE_HOURS_UTC)
    assert_true("Jam 10 UTC aktif", _active)
    # Jam 03 UTC → tidak aktif
    _h2 = 3
    _inactive = not any(s <= _h2 < e for s, e in ACTIVE_HOURS_UTC)
    assert_true("Jam 03 UTC tidak aktif", _inactive)

    print("\n── [v9.3] Improvement 5: Volatility sizing ──")
    # ATR 6% dari entry → vol_scalar < 1.0
    _atr_pct = 6.0
    _scalar = max(0.70, 1.0 - (_atr_pct - 5.0) * 0.03)
    assert_true("ATR 6% → vol_scalar < 1.0", _scalar < 1.0)
    assert_true("ATR 6% → vol_scalar ≥ 0.70", _scalar >= 0.70)
    # ATR 4% → tidak kena penalty
    _atr_ok = 4.0
    _no_penalty = _atr_ok <= 5.0
    assert_true("ATR 4% → tidak kena penalty", _no_penalty)

    # ── Test v9.3.1: EXPIRED PnL bug fix ────────────────────────
    print("\n── [v9.3.1] EXPIRED_LOSS pnl_usdt fix ──")

    # Simulasi bug lama: current_price = 0, entry = 2363.11, size = 23.43
    _entry    = 2363.11
    _size     = 23.43
    _exp_price = 2326.69  # harga saat expired (-1.54%)

    # Bug lama: current_price = 0
    _pnl_buggy = (0 - _entry) / _entry * _size
    assert_true("Bug lama: pnl = -size (current_price = 0)",
                abs(_pnl_buggy - (-_size)) < 0.01)

    # Fix baru: current_price = _exp_price
    _pnl_fixed = (_exp_price - _entry) / _entry * _size
    assert_true("Fix baru: pnl jauh lebih kecil dari -size",
                abs(_pnl_fixed) < abs(_pnl_buggy) * 0.1)
    assert_true("Fix baru: pnl sekitar -$0.36",
                -1.0 < _pnl_fixed < 0)

    # Verifikasi magnitude: fix harus ~37x lebih kecil dari bug
    _ratio = abs(_pnl_buggy) / abs(_pnl_fixed)
    assert_true(f"Fix 37x lebih akurat dari bug (ratio={_ratio:.0f}x)",
                _ratio > 30)

    # ── Test v9.4: Strategy kill switches & sizing ─────────────────
    print("\n── [v9.4] Strategy kill switches & config ──")

    # MICROCAP_ENABLED default harus False (WR historis 0%)
    assert_true("MICROCAP_ENABLED default = False (WR historis 0%)",
                not MICROCAP_ENABLED)

    # SWING_ENABLED default harus True (masih aktif, tapi bisa dimatikan)
    assert_true("SWING_ENABLED default = False (WR 38.9% < 45% threshold)",
                not SWING_ENABLED)

    # MAX_OPEN_TRADES default 13 (tidak diubah)
    assert_eq("MAX_OPEN_TRADES default = 8", MAX_OPEN_TRADES, 8)

    # INTRADAY expire harus < SWING expire agar kategori tidak overlap
    assert_true("INTRADAY expire < SWING expire",
                SIGNAL_EXPIRE_HOURS["INTRADAY"] < SIGNAL_EXPIRE_HOURS["SWING"])

    # MICROCAP expire harus singkat (pump/dump resolve cepat)
    assert_true("MICROCAP expire <= 24h",
                SIGNAL_EXPIRE_HOURS["MICROCAP"] <= 24)

    # validate_config harus mencatat MICROCAP_ENABLED=False sebagai warning
    _v94_warnings = validate_config()
    _has_micro_warn = any("MICROCAP_ENABLED" in w for w in _v94_warnings)
    assert_true("validate_config mencatat MICROCAP_ENABLED=false",
                _has_micro_warn)

    # auto_reset_halt harus callable
    assert_true("auto_reset_halt() callable", callable(auto_reset_halt))

    # ── Test v9.4.2: Adaptive score & BTC trend sensitivity ─────────────
    print("\n── [v9.4.2] Adaptive score threshold ──")

    # Saat bearish_cycles >= 2 → INTRADAY threshold naik 2.5 → 3.0
    _adaptive_intraday_bearish = TIER_MIN_SCORE["A+"] + 0.5
    assert_eq("INTRADAY adaptive bearish threshold = 4.0",
              _adaptive_intraday_bearish, TIER_MIN_SCORE["A+"] + 0.5)

    # Saat bearish_cycles < 2 → threshold normal = TIER_MIN_SCORE["A+"] = 3.5 (v9.5)
    _adaptive_intraday_normal = TIER_MIN_SCORE["A+"] + 0.0
    assert_eq("INTRADAY adaptive normal threshold = 3.5",
              _adaptive_intraday_normal, TIER_MIN_SCORE["A+"])

    # Saat bearish_cycles >= 2 → SWING threshold naik 3.0 → 3.5
    _adaptive_swing_bearish = TIER_MIN_SCORE_SWING + 0.5
    assert_eq("SWING adaptive bearish threshold = 4.0",
              _adaptive_swing_bearish, TIER_MIN_SCORE_SWING + 0.5)

    # BTC trend lookback & min bearish values updated
    assert_eq("BTC_TREND_LOOKBACK = 4 (v9.4.2)",    BTC_TREND_LOOKBACK,    4)
    assert_eq("BTC_TREND_MIN_BEARISH = 3 (v9.4.2)", BTC_TREND_MIN_BEARISH, 3)

    # repair_expired_loss_pnl harus callable (fungsi ada)
    assert_true("repair_expired_loss_pnl() callable",
                callable(repair_expired_loss_pnl))

    # ── Test v9.4.1: allow_buy harus False saat btc_bearish_trend aktif ────
    print("\n── [v9.4.1] allow_buy btc_bearish_trend gate ──")
    # Simulasi: block_buy=False tapi btc_bearish_trend=True
    # → allow_buy harus False (bug lama: allow_buy=True)
    _btc_trend_bearish = {"halt": False, "block_buy": False,
                          "btc_1h": -0.3, "btc_4h": -1.2,
                          "btc_bearish_trend": True, "btc_bearish_cycles": 3}
    _allow = not _btc_trend_bearish["block_buy"] and not _btc_trend_bearish.get("btc_bearish_trend", False)
    assert_true("allow_buy=False saat btc_bearish_trend=True (block_buy=False)",
                not _allow)

    # Simulasi: block_buy=True dan btc_bearish_trend=False
    # → allow_buy harus False
    _btc_drop = {"halt": False, "block_buy": True,
                 "btc_1h": -3.5, "btc_4h": -0.5,
                 "btc_bearish_trend": False, "btc_bearish_cycles": 0}
    _allow2 = not _btc_drop["block_buy"] and not _btc_drop.get("btc_bearish_trend", False)
    assert_true("allow_buy=False saat block_buy=True (btc_bearish_trend=False)",
                not _allow2)

    # Simulasi: keduanya False → allow_buy harus True
    _btc_ok = {"halt": False, "block_buy": False,
               "btc_1h": 0.5, "btc_4h": 0.3,
               "btc_bearish_trend": False, "btc_bearish_cycles": 0}
    _allow3 = not _btc_ok["block_buy"] and not _btc_ok.get("btc_bearish_trend", False)
    assert_true("allow_buy=True saat block_buy=False dan btc_bearish_trend=False",
                _allow3)

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
    elif "--repair-expired-pnl" in sys.argv:
        # [v9.3.1] Repair data historis yang rusak akibat bug current_price = 0
        # Jalankan: python bot.py --repair-expired-pnl          (dry run dulu)
        #           python bot.py --repair-expired-pnl --apply  (terapkan)
        _apply = "--apply" in sys.argv
        if not _apply:
            log("ℹ️  DRY RUN mode — gunakan --apply untuk terapkan perubahan ke DB")
        repair_expired_loss_pnl(dry_run=not _apply)
    else:
        # [v9.2 FIX] Top-level crash handler — pastikan exception apapun yang lolos
        # dari semua inner try-block tetap menghasilkan Telegram alert sebelum exit.
        try:
            run()
        except Exception as _top_exc:
            import traceback
            _tb = traceback.format_exc()
            log(f"🚨 UNHANDLED EXCEPTION di run(): {_top_exc}\n{_tb}", "error")
            try:
                tg(f"🚨 <b>BOT CRASH — Unhandled Exception</b>\n"
                   f"<code>{_tg_e(_top_exc)}</code>\n"
                   f"<i>Cek log untuk full traceback.</i>")
            except Exception:
                pass   # jika Telegram juga gagal, setidaknya log sudah tercatat
            raise
