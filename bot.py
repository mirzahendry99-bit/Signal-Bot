"""
=============================================================
  CRYPTO SIGNAL BOT — FINAL VERSION
  Exchange   : Gate.io (gate-api)
  Sinyal     : Teknikal + Fear & Greed + CoinGecko Trending
               + TradingView Webhook support
  Storage    : Supabase
  Notifikasi : Telegram
  Runner     : GitHub Actions (cron)
=============================================================

REQUIREMENTS:
    pip install gate-api supabase requests pandas numpy flask

ENV VARIABLES (set di GitHub Secrets):
    SUPABASE_URL
    SUPABASE_KEY
    TELEGRAM_TOKEN
    CHAT_ID
    COINGECKO_API_KEY   (opsional, pakai free tier juga bisa)
    WEBHOOK_SECRET      (untuk TradingView webhook, bebas isi)
=============================================================
"""

import os
import logging
import time
from datetime import datetime, timezone

import gate_api
import requests
import pandas as pd
import numpy as np

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("signal_bot.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  ENV VARIABLES
# ─────────────────────────────────────────────
SUPABASE_URL      = os.environ.get("SUPABASE_URL")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY")
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID           = os.environ.get("CHAT_ID")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")  # opsional
WEBHOOK_SECRET    = os.environ.get("WEBHOOK_SECRET", "")

_missing = [k for k, v in {
    "SUPABASE_URL":   SUPABASE_URL,
    "SUPABASE_KEY":   SUPABASE_KEY,
    "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
    "CHAT_ID":        CHAT_ID,
}.items() if not v]

if _missing:
    raise EnvironmentError(f"ENV belum diset: {', '.join(_missing)}")

from supabase import create_client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ─────────────────────────────────────────────
#  KONFIGURASI
# ─────────────────────────────────────────────
CONFIG = {
    # Mode: (timeframe, cooldown_menit)
    # GitHub Actions menjalankan tiap 15 menit via cron
    # Mode aktif ditentukan dari jam saat script dijalankan
    "MODES": {
        "scalping": ("5m",  15),
        "intraday": ("1h",  120),
        "swing":    ("4h",  480),
    },

    # Whitelist pair — edit sesuai kebutuhan
    "WHITELIST": [
        "BTC_USDT", "ETH_USDT", "SOL_USDT", "BNB_USDT", "XRP_USDT",
        "DOGE_USDT", "ARB_USDT", "OP_USDT", "MATIC_USDT", "AVAX_USDT",
        "LINK_USDT", "DOT_USDT", "UNI_USDT", "LTC_USDT", "ADA_USDT",
        "TRX_USDT", "ATOM_USDT", "NEAR_USDT", "APT_USDT", "SUI_USDT",
    ],

    # Minimum skor teknikal (maks 8)
    "MIN_SCORE": {
        "scalping": 4,
        "intraday": 5,
        "swing":    5,
    },

    # Fear & Greed threshold
    "FNG_EXTREME_FEAR":  25,   # < 25 → boost BUY confidence
    "FNG_EXTREME_GREED": 75,   # > 75 → suppress BUY / boost SELL

    # Indikator
    "RSI_PERIOD":     14,
    "RSI_OVERSOLD":   32,
    "RSI_OVERBOUGHT": 68,
    "EMA_FAST":       9,
    "EMA_SLOW":       21,
    "MACD_FAST":      12,
    "MACD_SLOW":      26,
    "MACD_SIGNAL":    9,
    "BB_PERIOD":      20,
    "BB_STD":         2.0,
    "ATR_PERIOD":     14,

    # TP/SL multiplier dari ATR
    "TP_ATR_MULT": {"scalping": 1.5, "intraday": 2.5, "swing": 3.5},
    "SL_ATR_MULT": {"scalping": 1.0, "intraday": 1.5, "swing": 2.0},

    "OHLCV_LIMIT": 150,
    "PAIR_DELAY":  0.3,
}


# ═══════════════════════════════════════════════
#  LAYER 2 — FEAR & GREED INDEX
# ═══════════════════════════════════════════════
def get_fear_greed() -> dict:
    """
    Ambil Fear & Greed Index dari alternative.me
    Return: {"value": int, "label": str, "sentiment": "FEAR"|"GREED"|"NEUTRAL"}
    """
    try:
        resp = requests.get(
            "https://api.alternative.me/fng/",
            timeout=10,
            params={"limit": 1}
        )
        data  = resp.json()["data"][0]
        value = int(data["value"])
        label = data["value_classification"]

        if value <= CONFIG["FNG_EXTREME_FEAR"]:
            sentiment = "EXTREME_FEAR"
        elif value <= 45:
            sentiment = "FEAR"
        elif value >= CONFIG["FNG_EXTREME_GREED"]:
            sentiment = "EXTREME_GREED"
        elif value >= 55:
            sentiment = "GREED"
        else:
            sentiment = "NEUTRAL"

        log.info(f"📊 Fear & Greed: {value} ({label}) → {sentiment}")
        return {"value": value, "label": label, "sentiment": sentiment}

    except Exception as e:
        log.warning(f"Fear & Greed fetch gagal: {e}")
        return {"value": 50, "label": "Neutral", "sentiment": "NEUTRAL"}


# ═══════════════════════════════════════════════
#  LAYER 3 — COINGECKO TRENDING
# ═══════════════════════════════════════════════
def get_coingecko_trending() -> set:
    """
    Ambil top trending coins dari CoinGecko (24 jam).
    Return: set of ticker symbols, e.g. {"BTC", "SOL", "ARB"}
    """
    try:
        headers = {}
        if COINGECKO_API_KEY:
            headers["x-cg-pro-api-key"] = COINGECKO_API_KEY

        resp = requests.get(
            "https://api.coingecko.com/api/v3/search/trending",
            headers=headers,
            timeout=10
        )
        data  = resp.json()
        coins = data.get("coins", [])
        symbols = {c["item"]["symbol"].upper() for c in coins}
        log.info(f"🔥 CoinGecko trending: {symbols}")
        return symbols

    except Exception as e:
        log.warning(f"CoinGecko fetch gagal: {e}")
        return set()


def get_coingecko_market_data() -> dict:
    """
    Ambil BTC dominance & total market cap change.
    Return: {"btc_dominance": float, "market_cap_change_24h": float}
    """
    try:
        headers = {}
        if COINGECKO_API_KEY:
            headers["x-cg-pro-api-key"] = COINGECKO_API_KEY

        resp = requests.get(
            "https://api.coingecko.com/api/v3/global",
            headers=headers,
            timeout=10
        )
        data = resp.json()["data"]
        btc_dom    = float(data["market_cap_percentage"].get("btc", 50))
        mkt_change = float(data.get("market_cap_change_percentage_24h_usd", 0))

        log.info(f"🌍 BTC Dominance: {btc_dom:.1f}% | Market 24h: {mkt_change:+.2f}%")
        return {"btc_dominance": btc_dom, "market_cap_change_24h": mkt_change}

    except Exception as e:
        log.warning(f"CoinGecko global fetch gagal: {e}")
        return {"btc_dominance": 50.0, "market_cap_change_24h": 0.0}


# ═══════════════════════════════════════════════
#  LAYER 4 — TRADINGVIEW WEBHOOK (CEK SUPABASE)
# ═══════════════════════════════════════════════
def get_tradingview_signals() -> dict:
    """
    Baca sinyal TradingView yang sudah masuk via webhook endpoint
    dan tersimpan di Supabase tabel 'tv_signals'.
    Return: dict {pair: signal_type}, e.g. {"BTC_USDT": "BUY"}

    TradingView webhook harus POST ke endpoint kamu dengan body:
    {
        "secret": "WEBHOOK_SECRET_KAMU",
        "pair": "BTC_USDT",
        "signal": "BUY",
        "timeframe": "1h"
    }
    """
    try:
        # Ambil sinyal yang masuk dalam 30 menit terakhir
        res = supabase.table("tv_signals") \
            .select("pair, signal") \
            .gte("created_at", _minutes_ago_iso(30)) \
            .execute()

        signals = {}
        for row in (res.data or []):
            signals[row["pair"]] = row["signal"]

        if signals:
            log.info(f"📺 TradingView signals: {signals}")
        return signals

    except Exception as e:
        log.warning(f"TradingView signal fetch gagal: {e}")
        return {}


def _minutes_ago_iso(minutes: int) -> str:
    from datetime import timedelta
    dt = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return dt.isoformat()


# ═══════════════════════════════════════════════
#  GATE.IO — DATA & INDIKATOR
# ═══════════════════════════════════════════════
def get_client() -> gate_api.SpotApi:
    config = gate_api.Configuration(host="https://api.gateio.ws/api/v4")
    return gate_api.SpotApi(gate_api.ApiClient(config))


def fetch_candles(client: gate_api.SpotApi, pair: str, tf: str) -> pd.DataFrame:
    try:
        raw = client.list_candlesticks(
            currency_pair=pair,
            interval=tf,
            limit=CONFIG["OHLCV_LIMIT"]
        )
        if not raw or len(raw) < 50:
            return pd.DataFrame()

        df = pd.DataFrame(raw, columns=[
            "timestamp", "volume", "close", "high", "low", "open",
            *[f"_x{i}" for i in range(max(0, len(raw[0]) - 6))]
        ])
        return df[["open", "high", "low", "close", "volume"]].astype(float).reset_index(drop=True)

    except gate_api.ApiException as e:
        log.error(f"Gate.io API error [{pair}|{tf}]: {e.status} {e.reason}")
        return pd.DataFrame()
    except Exception as e:
        log.exception(f"fetch_candles error [{pair}|{tf}]: {e}")
        return pd.DataFrame()


def compute_indicators(df: pd.DataFrame) -> dict:
    close = df["close"]
    high  = df["high"]
    low   = df["low"]

    # RSI
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(CONFIG["RSI_PERIOD"]).mean()
    loss  = (-delta.clip(upper=0)).rolling(CONFIG["RSI_PERIOD"]).mean()
    rsi_s = 100 - (100 / (1 + gain / (loss + 1e-9)))

    # EMA
    ema_f = close.ewm(span=CONFIG["EMA_FAST"], adjust=False).mean()
    ema_s = close.ewm(span=CONFIG["EMA_SLOW"], adjust=False).mean()

    # MACD
    mf    = close.ewm(span=CONFIG["MACD_FAST"],   adjust=False).mean()
    ms    = close.ewm(span=CONFIG["MACD_SLOW"],   adjust=False).mean()
    macd  = mf - ms
    sig   = macd.ewm(span=CONFIG["MACD_SIGNAL"], adjust=False).mean()
    hist  = macd - sig

    # Bollinger Bands
    bb_mid   = close.rolling(CONFIG["BB_PERIOD"]).mean()
    bb_std   = close.rolling(CONFIG["BB_PERIOD"]).std()
    bb_upper = bb_mid + CONFIG["BB_STD"] * bb_std
    bb_lower = bb_mid - CONFIG["BB_STD"] * bb_std

    # ATR
    prev_c = close.shift(1)
    tr     = pd.concat([(high - low), (high - prev_c).abs(), (low - prev_c).abs()], axis=1).max(axis=1)
    atr    = tr.rolling(CONFIG["ATR_PERIOD"]).mean()

    def last(s):  return float(s.iloc[-1])
    def prev(s):  return float(s.iloc[-2])

    return {
        "price":     last(close),
        "rsi":       last(rsi_s),   "rsi_prev":  prev(rsi_s),
        "ef_now":    last(ema_f),   "ef_prev":   prev(ema_f),
        "es_now":    last(ema_s),   "es_prev":   prev(ema_s),
        "macd_now":  last(macd),    "macd_prev": prev(macd),
        "sig_now":   last(sig),     "sig_prev":  prev(sig),
        "hist_now":  last(hist),    "hist_prev": prev(hist),
        "bb_upper":  last(bb_upper),"bb_lower":  last(bb_lower),
        "bb_mid":    last(bb_mid),
        "atr":       last(atr),
        "vol_now":   float(df["volume"].iloc[-1]),
        "vol_avg":   float(df["volume"].iloc[-20:].mean()),
    }


def score_signal(ind: dict) -> dict:
    buy_score = sell_score = 0
    reasons = []

    # RSI
    if ind["rsi"] <= CONFIG["RSI_OVERSOLD"]:
        buy_score += 2;  reasons.append(f"RSI oversold ({ind['rsi']:.1f})")
    elif ind["rsi"] >= CONFIG["RSI_OVERBOUGHT"]:
        sell_score += 2; reasons.append(f"RSI overbought ({ind['rsi']:.1f})")
    elif ind["rsi"] < 45:
        buy_score += 1;  reasons.append(f"RSI zona lemah ({ind['rsi']:.1f})")
    elif ind["rsi"] > 55:
        sell_score += 1; reasons.append(f"RSI zona kuat ({ind['rsi']:.1f})")

    # EMA Cross
    if ind["ef_now"] > ind["es_now"] and ind["ef_prev"] <= ind["es_prev"]:
        buy_score += 2;  reasons.append(f"Golden Cross EMA{CONFIG['EMA_FAST']}/{CONFIG['EMA_SLOW']}")
    elif ind["ef_now"] < ind["es_now"] and ind["ef_prev"] >= ind["es_prev"]:
        sell_score += 2; reasons.append(f"Death Cross EMA{CONFIG['EMA_FAST']}/{CONFIG['EMA_SLOW']}")
    elif ind["ef_now"] > ind["es_now"]:
        buy_score += 1;  reasons.append("EMA bullish trend")
    elif ind["ef_now"] < ind["es_now"]:
        sell_score += 1; reasons.append("EMA bearish trend")

    # MACD
    if ind["macd_now"] > ind["sig_now"] and ind["macd_prev"] <= ind["sig_prev"]:
        buy_score += 2;  reasons.append("MACD bullish crossover")
    elif ind["macd_now"] < ind["sig_now"] and ind["macd_prev"] >= ind["sig_prev"]:
        sell_score += 2; reasons.append("MACD bearish crossover")
    elif ind["hist_now"] > 0 and ind["hist_prev"] <= 0:
        buy_score += 1;  reasons.append("MACD histogram positif baru")
    elif ind["hist_now"] < 0 and ind["hist_prev"] >= 0:
        sell_score += 1; reasons.append("MACD histogram negatif baru")
    elif ind["macd_now"] > 0:
        buy_score += 1
    elif ind["macd_now"] < 0:
        sell_score += 1

    # Bollinger Bands
    p = ind["price"]
    if p <= ind["bb_lower"]:
        buy_score += 1;  reasons.append("Sentuh BB lower — potensi bounce")
    elif p >= ind["bb_upper"]:
        sell_score += 1; reasons.append("Sentuh BB upper — potensi pullback")

    # Volume Surge
    if ind["vol_avg"] > 0 and ind["vol_now"] > ind["vol_avg"] * 1.5:
        ratio = ind["vol_now"] / ind["vol_avg"]
        if buy_score >= sell_score:
            buy_score += 1
        else:
            sell_score += 1
        reasons.append(f"Volume surge {ratio:.1f}x rata-rata")

    return {"buy_score": buy_score, "sell_score": sell_score, "reasons": reasons}


# ═══════════════════════════════════════════════
#  SENTIMENT FILTER & CONFIDENCE ADJUSTMENT
# ═══════════════════════════════════════════════
def apply_sentiment_filter(
    signal: str,
    base_score: int,
    fng: dict,
    is_trending: bool,
    tv_signal: str,
    market: dict,
) -> tuple:
    """
    Sesuaikan confidence score berdasarkan semua layer sinyal eksternal.
    Return: (adjusted_score, sentiment_notes, allow_signal)
    """
    score = base_score
    notes = []
    allow = True

    fng_val = fng["value"]
    fng_sent = fng["sentiment"]

    # ── Fear & Greed ─────────────────────────
    if signal == "BUY":
        if fng_sent == "EXTREME_FEAR":
            score += 2
            notes.append(f"✅ F&G Extreme Fear ({fng_val}) — confirm BUY")
        elif fng_sent == "FEAR":
            score += 1
            notes.append(f"✅ F&G Fear ({fng_val}) — support BUY")
        elif fng_sent == "EXTREME_GREED":
            score -= 2
            allow = score >= CONFIG["MIN_SCORE"].get("scalping", 4)
            notes.append(f"⚠️ F&G Extreme Greed ({fng_val}) — BUY berisiko tinggi")
        elif fng_sent == "GREED":
            score -= 1
            notes.append(f"⚠️ F&G Greed ({fng_val}) — BUY dengan hati-hati")

    elif signal == "SELL":
        if fng_sent == "EXTREME_GREED":
            score += 2
            notes.append(f"✅ F&G Extreme Greed ({fng_val}) — confirm SELL")
        elif fng_sent == "GREED":
            score += 1
            notes.append(f"✅ F&G Greed ({fng_val}) — support SELL")
        elif fng_sent == "EXTREME_FEAR":
            score -= 1
            notes.append(f"⚠️ F&G Extreme Fear ({fng_val}) — SELL mungkin oversold")

    # ── CoinGecko Trending ────────────────────
    if is_trending:
        if signal == "BUY":
            score += 1
            notes.append("🔥 Pair trending di CoinGecko — momentum positif")
        elif signal == "SELL":
            notes.append("⚠️ Pair trending — SELL bisa lebih volatile")

    # ── CoinGecko Market Condition ────────────
    mkt_change = market.get("market_cap_change_24h", 0)
    btc_dom    = market.get("btc_dominance", 50)

    if signal == "BUY" and mkt_change > 2:
        score += 1
        notes.append(f"🌍 Market bullish 24h (+{mkt_change:.1f}%)")
    elif signal == "SELL" and mkt_change < -2:
        score += 1
        notes.append(f"🌍 Market bearish 24h ({mkt_change:.1f}%)")

    # BTC dominance tinggi → altcoin lebih berisiko
    if btc_dom > 55 and signal == "BUY":
        pair_is_btc = False  # akan dicek di caller
        notes.append(f"⚠️ BTC dominance tinggi ({btc_dom:.1f}%) — altcoin lemah")

    # ── TradingView Webhook ───────────────────
    if tv_signal:
        if tv_signal == signal:
            score += 2
            notes.append(f"📺 TradingView konfirmasi {signal}")
        elif tv_signal != signal:
            score -= 1
            notes.append(f"📺 TradingView berbeda ({tv_signal}) — konflik sinyal")

    return max(score, 0), notes, allow


# ═══════════════════════════════════════════════
#  TP / SL DINAMIS
# ═══════════════════════════════════════════════
def calc_tp_sl(price: float, atr: float, signal: str, mode: str) -> tuple:
    tp_m = CONFIG["TP_ATR_MULT"][mode]
    sl_m = CONFIG["SL_ATR_MULT"][mode]
    if signal == "BUY":
        tp, sl = price + atr * tp_m, price - atr * sl_m
    else:
        tp, sl = price - atr * tp_m, price + atr * sl_m
    return round(tp, 6), round(sl, 6), round(tp_m / sl_m, 2)


# ═══════════════════════════════════════════════
#  COOLDOWN
# ═══════════════════════════════════════════════
def is_on_cooldown(pair: str, mode: str, cooldown_min: int) -> bool:
    try:
        res = supabase.table("signal_cooldowns") \
            .select("last_signal") \
            .eq("pair", pair).eq("mode", mode).execute()
        if not res.data:
            return False
        last_dt = datetime.fromisoformat(res.data[0]["last_signal"].replace("Z", "+00:00"))
        elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60
        return elapsed < cooldown_min
    except Exception as e:
        log.warning(f"Cooldown check error [{pair}|{mode}]: {e}")
        return False


def update_cooldown(pair: str, mode: str):
    try:
        supabase.table("signal_cooldowns").upsert({
            "pair": pair, "mode": mode,
            "last_signal": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        log.warning(f"Cooldown update error: {e}")


# ═══════════════════════════════════════════════
#  SUPABASE — SIMPAN SINYAL
# ═══════════════════════════════════════════════
def save_signal(data: dict):
    try:
        supabase.table("signals").insert(data).execute()
        log.info(f"✅ Sinyal tersimpan: {data['pair']} {data['type']}")
    except Exception as e:
        log.error(f"Supabase insert error: {e}")


# ═══════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════
def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }, timeout=10)
        if resp.status_code != 200:
            log.warning(f"Telegram error: {resp.status_code} {resp.text}")
    except Exception as e:
        log.error(f"Telegram send error: {e}")


def format_message(
    pair, mode, tf, signal, price, tp, sl, rr,
    tech_score, final_score, max_score,
    rsi, atr, tech_reasons, sentiment_notes, fng
) -> str:
    emoji_sig  = "🟢" if signal == "BUY"  else "🔴"
    emoji_mode = {"scalping": "🔁", "intraday": "⚡", "swing": "📈"}.get(mode, "📊")
    conf_bar   = "▓" * min(final_score, max_score) + "░" * max(0, max_score - final_score)
    pair_fmt   = pair.replace("_", "/")

    tech_txt = "\n".join([f"  ✦ {r}" for r in tech_reasons]) or "  —"
    sent_txt = "\n".join([f"  {n}" for n in sentiment_notes]) or "  — Tidak ada filter aktif"

    fng_emoji = "😱" if fng["value"] < 25 else "😨" if fng["value"] < 45 else \
                "😏" if fng["value"] > 75 else "😌" if fng["value"] > 55 else "😐"

    return (
        f"{emoji_sig} *{signal} SIGNAL* — `{pair_fmt}`\n"
        f"{emoji_mode} Mode: *{mode.upper()}* · TF: `{tf}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Entry
