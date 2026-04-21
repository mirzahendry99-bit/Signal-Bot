"""
Simple Profit Bot v1.0 — Gate.io × Telegram × Supabase
========================================================
DROP-IN REPLACEMENT untuk Signal Bot v8.9.12.

Koneksi identik dengan bot lama:
  - ENV vars  : GATE_API_KEY, GATE_SECRET_KEY, SUPABASE_URL, SUPABASE_KEY,
                TELEGRAM_TOKEN, CHAT_ID
  - Tabel     : signals_v2 (kolom yang sama persis)
  - Logger    : signal_bot

Fitur yang disederhanakan:
  - Scan semua pair _USDT aktif di Gate.io
  - Filter: volume 24h ≥ 500K USDT + EMA trend
  - Entry: breakout recent high + volume spike + RSI sehat
  - SL berbasis ATR, TP = RR 2.0
  - Maksimal 3 signal per cycle
  - Dedup 4 jam via signals_v2 (strategy=SIMPLE, side=BUY)
  - Telegram alert + CSV fallback jika Supabase down
"""

import os, json, time, csv
import urllib.request
import logging
import threading
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from supabase import create_client
import gate_api


# ════════════════════════════════════════════════════════
#  LOGGING — WIB timestamp, logger name sama dengan bot lama
# ════════════════════════════════════════════════════════

WIB = timezone(timedelta(hours=7))

class _WIBFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=WIB)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S WIB")

_handler = logging.StreamHandler()
_handler.setFormatter(_WIBFormatter("%(asctime)s [%(levelname)s] %(message)s"))
_logger = logging.getLogger("signal_bot")   # sama dengan bot lama
_logger.setLevel(logging.INFO)
_logger.addHandler(_handler)
_logger.propagate = False

def log(msg: str, level: str = "info"):
    if level == "warn":
        _logger.warning(msg)
    elif level == "error":
        _logger.error(msg)
    else:
        _logger.info(msg)


# ════════════════════════════════════════════════════════
#  CONFIG — ENV vars identik dengan bot lama
# ════════════════════════════════════════════════════════

API_KEY      = os.environ.get("GATE_API_KEY")
SECRET_KEY   = os.environ.get("GATE_SECRET_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TG_TOKEN     = os.environ.get("TELEGRAM_TOKEN")
TG_CHAT_ID   = os.environ.get("CHAT_ID")

# ── Trading Parameters ────────────────────────────────
MIN_VOLUME_USDT  = float(os.environ.get("MIN_VOLUME_USDT", "500000"))  # volume 24h minimum
MAX_SIGNALS      = int(os.environ.get("MAX_SIGNALS", "3"))             # max signal per cycle
MAX_DAILY_SIGNALS = int(os.environ.get("MAX_DAILY_SIGNALS", "10"))     # max signal per hari (risk control)
MIN_RR           = float(os.environ.get("MIN_RR", "2.0"))              # RR minimum
SL_ATR_MULT      = float(os.environ.get("SL_ATR_MULT", "1.5"))         # SL = ATR × ini
DEDUP_HOURS      = int(os.environ.get("DEDUP_HOURS", "4"))             # window dedup jam
GATE_RATE_LIMIT_RPS = float(os.environ.get("GATE_RATE_LIMIT_RPS", "8.0"))

# ── Candle Config ──────────────────────────────────────
CANDLE_INTERVAL = "1h"
CANDLE_LIMIT    = 100

# ── Fallback CSV jika Supabase down ───────────────────
SIGNAL_FALLBACK_FILE = os.environ.get("SIGNAL_FALLBACK_FILE", "/tmp/signals_fallback.csv")

# ── Blacklist: stablecoin & leverage token ─────────────
BLACKLIST_TOKENS = {
    "USDC","BUSD","DAI","TUSD","FDUSD","USDP","USDD","USDJ",
    "ZUSD","GUSD","CUSD","SUSD","FRAX","LUSD","USDN",
}
BLACKLIST_SUFFIX = {"3L","3S","5L","5S","2L","2S","UP","DOWN","BULL","BEAR"}

# ── Validasi ENV — sama dengan bot lama ──────────────
_missing = [k for k, v in {
    "GATE_API_KEY":    API_KEY,
    "GATE_SECRET_KEY": SECRET_KEY,
    "SUPABASE_URL":    SUPABASE_URL,
    "SUPABASE_KEY":    SUPABASE_KEY,
    "TELEGRAM_TOKEN":  TG_TOKEN,
    "CHAT_ID":         TG_CHAT_ID,
}.items() if not v]
if _missing:
    raise EnvironmentError(f"ENV belum diset: {', '.join(_missing)}")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ════════════════════════════════════════════════════════
#  TELEGRAM — retry logic sama dengan bot lama
# ════════════════════════════════════════════════════════

def send_telegram(msg: str) -> None:
    """Kirim pesan ke Telegram dengan retry 3x + exponential backoff."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    url  = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    body = json.dumps({
        "chat_id": TG_CHAT_ID, "text": msg,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }).encode()
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, data=body, headers={"Content-Type": "application/json"}
            )
            urllib.request.urlopen(req, timeout=10)
            return
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt * 2)   # 2s, 4s
            else:
                log(f"⚠️ Telegram gagal setelah 3x retry: {e}", "error")


# ════════════════════════════════════════════════════════
#  GATE.IO — rate limiter + retry (diambil dari bot lama)
# ════════════════════════════════════════════════════════

class _TokenBucket:
    """Thread-safe token bucket rate limiter — diambil dari bot v8.9.12."""
    def __init__(self, rate: float):
        self._rate        = rate
        self._tokens      = rate
        self._last_refill = time.monotonic()
        self._lock        = threading.Lock()

    def consume(self) -> None:
        with self._lock:
            now           = time.monotonic()
            elapsed       = now - self._last_refill
            self._tokens  = min(self._rate, self._tokens + elapsed * self._rate)
            self._last_refill = now
            wait          = (1.0 - self._tokens) / self._rate if self._tokens < 1.0 else 0.0
            self._tokens -= 1.0
        if wait > 0:
            time.sleep(wait)

_gate_rate_limiter = _TokenBucket(GATE_RATE_LIMIT_RPS)


def gate_call_with_retry(fn, *args, retries: int = 3, base_delay: float = 1.0, **kwargs):
    """Panggil Gate.io API dengan retry + exponential backoff — diambil dari bot v8.9.12."""
    for attempt in range(retries):
        try:
            _gate_rate_limiter.consume()
            return fn(*args, **kwargs)
        except Exception as e:
            err_str = str(e).lower()
            is_rate = "429" in err_str or "rate limit" in err_str or "too many" in err_str
            if attempt < retries - 1:
                delay = base_delay * (2 ** attempt)   # 1s, 2s, 4s
                label = "Rate limit" if is_rate else "API error"
                log(f"⚠️ {label} — retry {attempt+1}/{retries} dalam {delay:.0f}s: {e}", "warn")
                time.sleep(delay)
            else:
                log(f"⚠️ Gate API gagal setelah {retries}x retry: {e}", "error")
    return None


def get_client():
    """Buat Gate.io Spot API client — identik dengan bot lama."""
    cfg = gate_api.Configuration(
        host="https://api.gateio.ws/api/v4",
        key=API_KEY, secret=SECRET_KEY
    )
    return gate_api.SpotApi(gate_api.ApiClient(cfg))


# ════════════════════════════════════════════════════════
#  DATA FETCH — candle cache + sanity check (dari bot lama)
# ════════════════════════════════════════════════════════

_candle_cache: dict = {}

def get_candles(client, pair: str, interval: str = CANDLE_INTERVAL, limit: int = CANDLE_LIMIT):
    """
    Fetch OHLCV dari Gate.io dengan cache per cycle + sanity check candle ekstrem.
    Return: tuple (closes, highs, lows, volumes) sebagai numpy arrays, atau None.
    Diambil dari bot v8.9.12 — logika identik.
    """
    key = (pair, interval, limit)
    if key in _candle_cache:
        return _candle_cache[key]

    try:
        raw = gate_call_with_retry(
            client.list_candlesticks,
            currency_pair=pair, interval=interval, limit=limit
        )
        min_req = min(50, limit)
        if not raw or len(raw) < min_req:
            _candle_cache[key] = None
            return None

        closes  = np.array([float(c[2]) for c in raw])
        highs   = np.array([float(c[3]) for c in raw])
        lows    = np.array([float(c[4]) for c in raw])
        volumes = np.array([float(c[1]) for c in raw])

        # Sanity check: skip pair dengan candle flash crash / pump ekstrem
        if len(closes) >= 21:
            avg_range  = float(np.mean(highs[-21:-1] - lows[-21:-1]))
            last_range = float(highs[-1] - lows[-1])
            if avg_range > 0 and last_range > avg_range * 5:
                log(f"⚠️ [{pair}] candle ekstrem — skip", "warn")
                _candle_cache[key] = None
                return None

        result = (closes, highs, lows, volumes)
        _candle_cache[key] = result
        return result

    except Exception as e:
        log(f"⚠️ get_candles [{pair}]: {e}", "warn")
        _candle_cache[key] = None
        return None


def get_all_pairs(client) -> list[str]:
    """
    Ambil semua pair _USDT aktif dari Gate.io.
    Filter: volume 24h ≥ MIN_VOLUME_USDT, bukan stablecoin/leverage token.
    """
    try:
        tickers = gate_call_with_retry(client.list_tickers)
        if not tickers:
            return []

        pairs = []
        for t in tickers:
            pair = t.currency_pair
            if not pair.endswith("_USDT"):
                continue
            base = pair.replace("_USDT", "")

            # Filter blacklist exact
            if base in BLACKLIST_TOKENS:
                continue
            # Filter suffix leverage
            if any(base.endswith(sfx) for sfx in BLACKLIST_SUFFIX):
                continue

            # Filter volume minimum
            try:
                vol_24h = float(t.quote_volume or 0)
            except Exception:
                continue
            if vol_24h < MIN_VOLUME_USDT:
                continue

            pairs.append(pair)

        log(f"📋 Pair lolos filter volume: {len(pairs)}")
        return pairs

    except Exception as e:
        log(f"⚠️ get_all_pairs: {e}", "error")
        return []


def get_btc_change(client) -> float:
    """
    Ambil perubahan harga BTC dalam 1 candle terakhir (close-to-close %).
    Digunakan sebagai market filter — jika BTC turun lebih dari 2.5%, skip semua trade.
    Return: persentase change, atau 0.0 jika gagal fetch.
    """
    try:
        data = get_candles(client, "BTC_USDT")
        if data is None:
            return 0.0
        closes = data[0]
        if len(closes) < 2:
            return 0.0
        change = (closes[-1] - closes[-2]) / closes[-2] * 100
        return float(change)
    except Exception as e:
        log(f"⚠️ get_btc_change: {e}", "warn")
        return 0.0


# ════════════════════════════════════════════════════════
#  INDIKATOR — Wilder's EMA, identik dengan bot lama
# ════════════════════════════════════════════════════════

def calc_ema(closes, period: int) -> float:
    try:
        return float(pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1])
    except Exception:
        return 0.0


def calc_rsi(closes, period: int = 14) -> float:
    """RSI dengan Wilder's EMA — sama persis dengan bot lama."""
    try:
        s    = pd.Series(closes)
        d    = s.diff()
        gain = d.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
        loss = (-d.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
        return float((100 - 100 / (1 + gain / (loss + 1e-9))).iloc[-1])
    except Exception:
        return 50.0


def calc_atr(closes, highs, lows, period: int = 14) -> float:
    try:
        tr = [
            max(highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i]  - closes[i-1]))
            for i in range(1, len(closes))
        ]
        return float(pd.Series(tr).rolling(period).mean().iloc[-1])
    except Exception:
        return 0.0


# ════════════════════════════════════════════════════════
#  DEDUPLICATION — pakai tabel signals_v2 yang sudah ada
# ════════════════════════════════════════════════════════

_dedup_memory: set = set()   # in-memory fallback, sama dengan bot lama
LAST_SIGNAL_TIME: dict = {}  # cooldown tracker per pair
_daily_signal_count: int = 0          # counter signal hari ini
_daily_reset_date: str = ""           # tanggal terakhir reset counter


def is_on_cooldown(pair: str, cooldown: int = 3600) -> bool:
    """
    Cek apakah pair masih dalam cooldown period (default 1 jam).
    Mencegah entry berulang pada pair yang sama saat sideways.
    """
    now = time.time()
    last = LAST_SIGNAL_TIME.get(pair, 0)
    if now - last < cooldown:
        return True
    LAST_SIGNAL_TIME[pair] = now
    return False


def check_daily_limit() -> bool:
    """
    Cek apakah daily signal limit sudah tercapai.
    Counter auto-reset setiap hari baru (UTC).
    Return True jika masih boleh kirim signal, False jika sudah limit.
    """
    global _daily_signal_count, _daily_reset_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _daily_reset_date != today:
        _daily_signal_count = 0
        _daily_reset_date = today
    return _daily_signal_count < MAX_DAILY_SIGNALS


def increment_daily_count() -> None:
    """Tambah counter signal harian setelah signal berhasil dikirim."""
    global _daily_signal_count
    _daily_signal_count += 1


def already_sent(pair: str) -> bool:
    """
    Cek apakah pair ini sudah punya signal SIMPLE/BUY dalam DEDUP_HOURS jam.
    Query ke signals_v2 — tabel yang sama dengan bot lama.
    Fallback ke in-memory jika Supabase error.
    """
    key = f"{pair}|SIMPLE|BUY"
    if key in _dedup_memory:
        return True
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=DEDUP_HOURS)).isoformat()
        rows  = (
            supabase.table("signals_v2")
            .select("id")
            .eq("pair", pair)
            .eq("strategy", "SIMPLE")
            .eq("side", "BUY")
            .gt("sent_at", since)
            .execute()
        )
        return len(rows.data) > 0
    except Exception as e:
        log(f"⚠️ dedup check [{pair}]: {e} — pakai in-memory", "warn")
        return False


def save_signal(pair: str, entry: float, sl: float,
                tp1: float, rr: float, score: int) -> None:
    """
    Simpan signal ke signals_v2 dengan kolom yang kompatibel dengan bot lama.
    Kolom opsional (tier, tp2, result, closed_at, pnl_pct) diisi dengan default
    agar tidak ada schema error.
    CSV fallback jika Supabase down — sama dengan bot lama.
    """
    record = {
        "pair":      pair,
        "strategy":  "SIMPLE",
        "side":      "BUY",
        "entry":     entry,
        "tp1":       tp1,
        "tp2":       None,        # bot sederhana tidak pakai TP2
        "sl":        sl,
        "tier":      "A",         # default tier — tidak ada scoring tier di bot ini
        "score":     score,
        "timeframe": CANDLE_INTERVAL,
        "sent_at":   datetime.now(timezone.utc).isoformat(),
        "result":    None,        # diisi oleh evaluate_signals jika masih jalan
        "closed_at": None,
        "pnl_pct":   None,
    }

    # Tandai di memory DULU — cegah duplikat dalam cycle yang sama
    _dedup_memory.add(f"{pair}|SIMPLE|BUY")

    supabase_ok = False
    try:
        supabase.table("signals_v2").insert(record).execute()
        supabase_ok = True
    except Exception as e:
        log(f"⚠️ save_signal Supabase [{pair}]: {e} — tulis ke CSV fallback", "warn")

    if not supabase_ok:
        # CSV fallback — sama dengan bot lama
        try:
            file_exists = os.path.isfile(SIGNAL_FALLBACK_FILE)
            with open(SIGNAL_FALLBACK_FILE, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(record.keys()))
                if not file_exists:
                    writer.writeheader()
                writer.writerow(record)
            log(f"  ℹ️ Signal [{pair}] tercatat di CSV fallback saja", "warn")
        except Exception as csv_err:
            log(f"⚠️ CSV fallback gagal [{pair}]: {csv_err}", "error")


# ════════════════════════════════════════════════════════
#  SIGNAL LOGIC
# ════════════════════════════════════════════════════════

def analyze_pair(client, pair: str, btc_change: float) -> dict | None:
    """
    Analisis satu pair. Return dict signal jika semua kondisi terpenuhi.

    Kondisi entry (SEMUA harus terpenuhi):
      0. BTC change > -2.5%     — market filter (dicek di run() sebelum loop)
      0. Cooldown 1 jam per pair — cegah entry berulang saat sideways
      0. Session filter UTC 06–22 — hindari jam sepi Asia dini hari
      1. EMA20 > EMA50          — struktur uptrend
      2. Harga > recent high 20 candle — breakout konfirmasi
      3. Tidak lebih dari 5% di atas recent high — anti buy-the-top
      4. Volume candle terakhir > rata-rata 10 candle × 1.5 — ada partisipasi
      5. RSI 50–68              — momentum sehat, tidak overbought
      6. RR >= 2.0 (atau 2.5 saat vol spike kuat) — worth the risk
      7. SL distance ≤ 6% dari entry — risk filter ketat
    """
    # ── Cooldown per pair ─────────────────────────────────
    if is_on_cooldown(pair):
        return None

    # ── Session Filter: UTC 06:00–22:00 ──────────────────
    hour = datetime.now(timezone.utc).hour
    if hour < 6 or hour > 22:
        return None

    data = get_candles(client, pair)
    if data is None:
        return None

    closes, highs, lows, volumes = data
    price = float(closes[-1])

    # ── Kondisi 1: EMA trend ─────────────────────────────
    ema20 = calc_ema(closes, 20)
    ema50 = calc_ema(closes, 50)
    if ema20 <= ema50:
        return None

    # ── Kondisi 2 & 3: Breakout + anti buy-the-top ───────
    recent_high = float(np.max(highs[-21:-1]))
    if price <= recent_high:
        return None
    if price > recent_high * 1.05:
        return None   # sudah terlambat — harga sudah 5% di atas breakout

    # ── Kondisi 4: Volume spike ───────────────────────────
    vol_avg = float(np.mean(volumes[-11:-1]))
    if vol_avg <= 0:
        return None
    vol_ratio = float(volumes[-1]) / vol_avg
    if vol_ratio < 1.5:
        return None

    # ── Kondisi 5: RSI ────────────────────────────────────
    rsi = calc_rsi(closes)
    if not (50.0 <= rsi <= 68.0):
        return None

    # ── Entry, SL, TP ─────────────────────────────────────
    atr = calc_atr(closes, highs, lows)
    if atr <= 0:
        return None

    entry   = round(price, 8)
    sl      = round(entry - atr * SL_ATR_MULT, 8)
    sl_dist = entry - sl

    if sl_dist <= 0:
        return None
    if sl_dist / entry > 0.06:   # SL tidak boleh lebih dari 6% — lebih stabil untuk account kecil
        return None

    rr_target = 2.5 if vol_ratio >= 3.0 else 2.0   # strong move → target lebih agresif
    tp1 = round(entry + sl_dist * rr_target, 8)
    rr  = (tp1 - entry) / sl_dist

    if rr < MIN_RR:
        return None

    # Skor sederhana: hitung berapa kondisi bonus terpenuhi (untuk logging)
    score = 3   # base: EMA + breakout + volume (semua sudah terkonfirmasi)
    if rsi < 55:    score += 1   # RSI masih ada ruang naik
    if vol_ratio >= 3.0: score += 1   # volume spike sangat kuat

    return {
        "pair":      pair,
        "entry":     entry,
        "sl":        sl,
        "tp1":       tp1,
        "rr":        round(rr, 2),
        "rsi":       round(rsi, 1),
        "vol_ratio": round(vol_ratio, 1),
        "atr":       round(atr, 8),
        "score":     score,
    }


# ════════════════════════════════════════════════════════
#  TELEGRAM ALERT
# ════════════════════════════════════════════════════════

def send_signal_alert(sig: dict) -> None:
    pair   = sig["pair"].replace("_USDT", "/USDT")
    entry  = sig["entry"]
    sl     = sig["sl"]
    tp1    = sig["tp1"]
    rr     = sig["rr"]
    rsi    = sig["rsi"]
    vol    = sig["vol_ratio"]

    sl_pct  = round((entry - sl) / entry * 100, 2)
    tp1_pct = round((tp1 - entry) / entry * 100, 2)

    # Fix 5: Tampilkan exit strategy — TP1 partial exit + trailing untuk sisa posisi
    exit_note = "⚡ <i>Strategi: TP1 = exit 50% | sisanya trailing SL ke entry</i>"

    msg = (
        f"📈 <b>BUY SIGNAL</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair   : <b>{pair}</b>\n\n"
        f"Entry  : <b>{entry}</b>\n"
        f"SL     : <b>{sl}</b>  <i>(-{sl_pct}%)</i>\n"
        f"TP1    : <b>{tp1}</b>  <i>(+{tp1_pct}%)</i>\n"
        f"RR     : <b>{rr}R</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"RSI    : {rsi}\n"
        f"Volume : {vol}× spike\n"
        f"{exit_note}\n"
        f"<i>Simple Profit Bot v1.0</i>"
    )
    send_telegram(msg)
    log(f"  📤 Signal: {pair} | Entry:{entry} SL:{sl} TP1:{tp1} RR:{rr}")


# ════════════════════════════════════════════════════════
#  MAIN RUN
# ════════════════════════════════════════════════════════

def run():
    global _candle_cache, _dedup_memory

    log("=" * 50)
    log("🚀 Simple Profit Bot v1.0 — scan dimulai")
    log("=" * 50)

    # Reset cache setiap cycle — sama dengan bot lama
    # Catatan: LAST_SIGNAL_TIME TIDAK di-reset agar cooldown per pair tetap aktif antar cycle
    _candle_cache = {}
    _dedup_memory = set()

    client = get_client()
    pairs  = get_all_pairs(client)

    if not pairs:
        msg = "❌ <b>Simple Bot</b>\nTidak ada pair yang lolos filter volume. Bot berhenti."
        log(msg.replace("<b>", "").replace("</b>", ""), "error")
        send_telegram(msg)
        return

    # ── Fix 2: BTC market filter — cek SEKALI sebelum loop ───
    btc_change = get_btc_change(client)
    log(f"📡 BTC change (last candle): {btc_change:.2f}%")
    if btc_change < -2.5:
        msg = (
            f"⛔ <b>Market Dump Detected</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"BTC turun {btc_change:.2f}% — semua signal di-skip.\n"
            f"<i>Simple Profit Bot v1.0</i>"
        )
        log("⛔ Market dump — skip ALL signals")
        send_telegram(msg)
        return

    # ── Fix 3: Daily limit check ──────────────────────────────
    if not check_daily_limit():
        msg = (
            f"🚫 <b>Daily Limit Tercapai</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Signal hari ini sudah {_daily_signal_count}/{MAX_DAILY_SIGNALS}. Scan dihentikan.\n"
            f"<i>Simple Profit Bot v1.0</i>"
        )
        log(f"🚫 Daily limit tercapai ({_daily_signal_count}/{MAX_DAILY_SIGNALS}) — skip cycle")
        send_telegram(msg)
        return

    signals = []
    scanned = 0

    for pair in pairs:
        if len(signals) >= MAX_SIGNALS:
            break
        if not check_daily_limit():
            log(f"🚫 Daily limit tercapai mid-cycle — berhenti scan")
            break

        if already_sent(pair):
            continue

        scanned += 1
        time.sleep(0.1)   # throttle ringan antar pair

        try:
            sig = analyze_pair(client, pair, btc_change)
            if sig:
                signals.append(sig)
                log(f"  ✅ Signal: {pair} | RR:{sig['rr']} RSI:{sig['rsi']} Vol:{sig['vol_ratio']}×")
        except Exception as e:
            log(f"⚠️ analyze_pair [{pair}]: {e}", "warn")

    log(f"📊 Scan selesai — {scanned} pair dianalisis, {len(signals)} signal")

    if not signals:
        send_telegram(
            f"🔍 <b>Scan Selesai</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Pairs dianalisis : {scanned}\n"
            f"Signal ditemukan : 0\n"
            f"Tidak ada yang memenuhi kriteria saat ini.\n"
            f"<i>Simple Profit Bot v1.0</i>"
        )
        return

    # Kirim signal — urutkan RR tertinggi dulu
    signals.sort(key=lambda x: -x["rr"])
    for sig in signals:
        send_signal_alert(sig)
        save_signal(
            sig["pair"], sig["entry"], sig["sl"],
            sig["tp1"], sig["rr"], sig["score"]
        )
        increment_daily_count()
        time.sleep(0.5)   # jeda antar Telegram message

    # Summary
    send_telegram(
        f"✅ <b>Scan Selesai</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pairs dianalisis : {scanned}\n"
        f"Signal terkirim  : {len(signals)}\n"
        f"Signal hari ini  : {_daily_signal_count}/{MAX_DAILY_SIGNALS}\n"
        f"<i>Simple Profit Bot v1.0</i>"
    )

    log(f"✅ Done — {len(signals)} signal terkirim dari {scanned} pair dianalisis")
    log(f"📅 Daily count: {_daily_signal_count}/{MAX_DAILY_SIGNALS}")
    log("=" * 50)


if __name__ == "__main__":
    run()
