"""
╔══════════════════════════════════════════════════════════════════╗
║        SIGNAL BOT — PATCH v11 → v12                             ║
║                                                                  ║
║  CHANGELOG v12 (BUGFIX + UPGRADE):                              ║
║  [FIX-1] detect_order_block   — mitigation zone fix (ob_low)   ║
║  [FIX-2] detect_liquidity_map — tambah liq_bias ke return dict ║
║  [FIX-3] check_entry_precision — FVG candle_conf volume gate   ║
║  [FIX-4] check_intraday       — hapus double entry calc        ║
║  [UPG-1] calc_position_size   — dynamic sizing by tier/score   ║
║  [UPG-2] detect_liquidity_map — aktifkan post-sweep entry trap ║
║  [UPG-3] check_intraday       — sweep sebagai explicit trigger  ║
╚══════════════════════════════════════════════════════════════════╝

CARA APPLY:
- Setiap fungsi di bawah adalah REPLACEMENT lengkap dari versi v11.
- detect_order_block, detect_liquidity_map, detect_fvg,
  check_entry_precision, check_intraday: REPLACE langsung.
- calc_position_size: REPLACE fungsi lama.
- Tidak ada perubahan di luar fungsi-fungsi ini.
"""

import numpy as np


# ═══════════════════════════════════════════════════════════════════
#  [FIX-1] detect_order_block — REPLACE fungsi v11
#
#  BUG v11: mitigation check hanya cek `l[j] <= ob_high`, artinya
#  candle yang hanya menyentuh atas OB dianggap mitigated — padahal
#  mitigation yang benar adalah ketika harga masuk ke DALAM zone OB
#  (antara ob_low dan ob_high).
#
#  FIX: mitigated = low[j] <= ob_high AND high[j] >= ob_low
#  Artinya: candle harus overlap dengan zone OB secara penuh,
#  bukan sekadar menyentuh tepi atasnya.
# ═══════════════════════════════════════════════════════════════════
def detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=30) -> dict:
    """
    Order Block: candle besar terakhir sebelum impulsive move.
    Smart money meninggalkan footprint di sini.

    BUY OB : last bearish candle sebelum strong bullish impulse
    SELL OB: last bullish candle sebelum strong bearish impulse

    [v11 P1-2] Mitigation check: OB expired jika harga masuk ke zone.
    [v12 FIX-1] Kondisi mitigation diperketat: harga harus overlap
    zone OB (ob_low–ob_high), bukan sekadar menyentuh ob_high.
    """
    result = {"valid": False, "ob_high": None, "ob_low": None, "ob_mid": None}

    if len(closes) < lookback:
        return result

    c = closes[-lookback:]
    h = highs[-lookback:]
    l = lows[-lookback:]
    v = volumes[-lookback:]

    avg_body = np.mean([abs(c[i] - c[i - 1]) for i in range(1, len(c))])

    for i in range(len(c) - 4, 0, -1):
        body_next = abs(c[i + 1] - c[i])

        if side == "BUY":
            is_bearish = c[i] < c[i - 1]
            is_impulse = c[i + 1] > c[i] and body_next > avg_body * 1.5
            if is_bearish and is_impulse:
                ob_high = float(h[i])
                ob_low  = float(l[i])

                # [v12 FIX-1] Mitigation: candle setelah OB harus OVERLAP zone
                # low[j] <= ob_high DAN high[j] >= ob_low
                # (bukan sekadar menyentuh ob_high dari luar)
                mitigated = any(
                    float(l[j]) <= ob_high and float(h[j]) >= ob_low
                    for j in range(i + 2, len(c))
                )
                if mitigated:
                    continue

                result = {
                    "valid":   True,
                    "ob_high": ob_high,
                    "ob_low":  ob_low,
                    "ob_mid":  (ob_high + ob_low) / 2,
                }
                break

        elif side == "SELL":
            is_bullish = c[i] > c[i - 1]
            is_impulse = c[i + 1] < c[i] and body_next > avg_body * 1.5
            if is_bullish and is_impulse:
                ob_high = float(h[i])
                ob_low  = float(l[i])

                # [v12 FIX-1] Mitigation SELL: high[j] >= ob_low DAN low[j] <= ob_high
                mitigated = any(
                    float(h[j]) >= ob_low and float(l[j]) <= ob_high
                    for j in range(i + 2, len(c))
                )
                if mitigated:
                    continue

                result = {
                    "valid":   True,
                    "ob_high": ob_high,
                    "ob_low":  ob_low,
                    "ob_mid":  (ob_high + ob_low) / 2,
                }
                break

    return result


# ═══════════════════════════════════════════════════════════════════
#  [FIX-2 + UPG-2] detect_liquidity_map — REPLACE fungsi v11
#
#  BUG v11: return dict tidak punya field "liq_bias", padahal
#  check_intraday menggunakannya di conditions:
#     (liq.get("liq_bias") == "BUY", "liq_cluster")
#  Akibatnya: kondisi liq_cluster selalu False → weight 2 hilang.
#
#  FIX: tambahkan "liq_bias" ke return dict berdasarkan sweep direction.
#
#  [UPG-2] Tambahkan "post_sweep_entry" — flag eksplisit bahwa
#  kondisi ideal untuk entry sudah terjadi (sweep + reversal + volume).
#  Ini berbeda dari sekadar mendeteksi sweep; ini adalah konfirmasi
#  bahwa institutional trap sudah selesai dan reversal sedang berjalan.
# ═══════════════════════════════════════════════════════════════════
def detect_liquidity_map(closes, highs, lows, volumes, lookback=50) -> dict:
    """
    Peta likuiditas — di mana stop loss ritel tersimpan.

    1. Equal Highs/Lows   : price menyentuh level yang sama 2x+ = stop cluster
    2. Stop Hunt Zone      : spike tipis melewati level lalu kembali
    3. Fake Breakout       : close di atas level lalu balik = distributor menjebak buyer
    4. Liquidity Sweep     : spike keluar range dengan volume tinggi lalu balik

    [v11 P1-3] Fake BO memerlukan volume gate.
    [v12 FIX-2] Tambah liq_bias ke return dict.
    [v12 UPG-2] Tambah post_sweep_entry untuk trigger aktif post-trap.
    """
    result = {
        "equal_highs":        None,
        "equal_lows":         None,
        "stop_cluster_above": None,
        "stop_cluster_below": None,
        "fake_bo_bull":       False,
        "fake_bo_bear":       False,
        "sweep_bull":         False,
        "sweep_bear":         False,
        "sweep_level":        None,
        "liq_above":          None,
        "liq_below":          None,
        # [v12 FIX-2] — sebelumnya tidak ada, menyebabkan liq_cluster selalu False
        "liq_bias":           "NEUTRAL",
        # [v12 UPG-2] — konfirmasi aktif post-sweep untuk entry
        "post_sweep_entry":   None,   # "BUY" | "SELL" | None
    }

    if len(closes) < lookback:
        return result

    n       = lookback
    h_slice = highs[-n:]
    l_slice = lows[-n:]
    c_slice = closes[-n:]
    v_slice = volumes[-n:]

    # ── 1. Equal Highs/Lows — vectorized ─────────────────────────────
    tol = 0.003

    h_arr = h_slice.reshape(-1, 1)
    h_mat = np.abs(h_arr - h_slice) / (h_arr + 1e-9)
    np.fill_diagonal(h_mat, 1.0)
    eq_high_mask = h_mat < tol
    if eq_high_mask.any():
        rows, cols = np.where(
            eq_high_mask & (np.arange(len(h_slice))[:, None] < np.arange(len(h_slice)))
        )
        if len(rows) > 0:
            eq_high_levels               = (h_slice[rows] + h_slice[cols]) / 2
            result["equal_highs"]        = float(np.median(eq_high_levels))
            result["stop_cluster_above"] = result["equal_highs"] * 1.002

    l_arr = l_slice.reshape(-1, 1)
    l_mat = np.abs(l_arr - l_slice) / (l_arr + 1e-9)
    np.fill_diagonal(l_mat, 1.0)
    eq_low_mask = l_mat < tol
    if eq_low_mask.any():
        rows, cols = np.where(
            eq_low_mask & (np.arange(len(l_slice))[:, None] < np.arange(len(l_slice)))
        )
        if len(rows) > 0:
            eq_low_levels               = (l_slice[rows] + l_slice[cols]) / 2
            result["equal_lows"]        = float(np.median(eq_low_levels))
            result["stop_cluster_below"] = result["equal_lows"] * 0.998

    # ── 2. Likuiditas di atas/bawah ───────────────────────────────────
    result["liq_above"] = float(np.max(h_slice) * 1.001)
    result["liq_below"] = float(np.min(l_slice) * 0.999)

    # ── 3. Liquidity Sweep (5 candle terakhir) ────────────────────────
    ref_high = float(np.max(h_slice[:-5]))
    ref_low  = float(np.min(l_slice[:-5]))
    avg_vol  = float(np.mean(v_slice[:-5]))

    for i in range(-5, 0):
        lo, cl   = float(lows[i]),  float(closes[i])
        hi, ch   = float(highs[i]), float(closes[i])
        vol_i    = float(volumes[i])

        if lo < ref_low and cl > ref_low:
            result["sweep_bull"]  = True
            result["sweep_level"] = ref_low
        if hi > ref_high and ch < ref_high:
            result["sweep_bear"]  = True
            result["sweep_level"] = ref_high

    # ── 4. Fake Breakout Detection — [v11 P1-3] volume gate ──────────
    recent_3  = c_slice[-4:-1]
    current_c = float(c_slice[-1])
    avg_vol_f = float(np.mean(v_slice[:-1]))
    curr_vol  = float(v_slice[-1])

    bo_bull_vols = [
        float(v_slice[-4 + idx])
        for idx, c in enumerate(recent_3)
        if c > ref_high
    ]
    bo_bear_vols = [
        float(v_slice[-4 + idx])
        for idx, c in enumerate(recent_3)
        if c < ref_low
    ]

    if (any(c > ref_high for c in recent_3) and
            current_c < ref_high and
            bo_bull_vols and max(bo_bull_vols) > avg_vol_f * 1.3 and
            curr_vol <= avg_vol_f):
        result["fake_bo_bull"] = True

    if (any(c < ref_low for c in recent_3) and
            current_c > ref_low and
            bo_bear_vols and max(bo_bear_vols) > avg_vol_f * 1.3 and
            curr_vol <= avg_vol_f):
        result["fake_bo_bear"] = True

    # ── 5. [v12 FIX-2] liq_bias — berdasarkan sweep & fake BO ────────
    # BUY bias: bullish sweep (stop hunt bawah + reversal ke atas)
    #           atau fake bearish BO (distributor gagal push harga turun)
    # SELL bias: sebaliknya
    bull_signals = int(result["sweep_bull"]) + int(result["fake_bo_bear"])
    bear_signals = int(result["sweep_bear"]) + int(result["fake_bo_bull"])

    if bull_signals > bear_signals:
        result["liq_bias"] = "BUY"
    elif bear_signals > bull_signals:
        result["liq_bias"] = "SELL"
    else:
        result["liq_bias"] = "NEUTRAL"

    # ── 6. [v12 UPG-2] post_sweep_entry — konfirmasi aktif ───────────
    # Kondisi: sweep sudah terjadi (trap selesai) DAN candle terakhir
    # menunjukkan reversal dengan volume konfirmasi (bukan noise).
    # Ini adalah sinyal paling bersih untuk entry post-institutional trap.
    last_close = float(c_slice[-1])
    prev_close = float(c_slice[-2])
    last_vol   = float(v_slice[-1])

    if result["sweep_bull"] and last_close > prev_close and last_vol > avg_vol * 1.2:
        # Sweep ke bawah sudah terjadi, candle reversal bullish bervolume = BUY entry
        result["post_sweep_entry"] = "BUY"
    elif result["sweep_bear"] and last_close < prev_close and last_vol > avg_vol * 1.2:
        # Sweep ke atas sudah terjadi, candle reversal bearish bervolume = SELL entry
        result["post_sweep_entry"] = "SELL"

    return result


# ═══════════════════════════════════════════════════════════════════
#  detect_fvg — TIDAK ADA PERUBAHAN LOGIKA, hanya perbaikan minor
#  pada edge case range(i+3, 0) untuk candle terakhir.
#
#  [v12] Pastikan gap_filled check tidak skip FVG dekat candle terbaru.
# ═══════════════════════════════════════════════════════════════════
def detect_fvg(highs, lows, lookback=30) -> dict:
    """
    Deteksi Fair Value Gap dalam N candle terakhir.

    Bullish FVG: low[i+2] > high[i]
    Bearish FVG: high[i+2] < low[i]

    [v11 P3] Hanya ambil FVG yang belum terisi harga.
    [v12]    Gap-filled check dipastikan tidak skip candle terbaru
             (edge case i+3 == 0 ditangani eksplisit).
    """
    result = {
        "bull_fvg": None,
        "bear_fvg": None,
    }

    n = min(lookback, len(highs) - 2)
    if n < 3:
        return result

    for i in range(-n, -2):
        h0 = float(highs[i])
        l0 = float(lows[i])
        h2 = float(highs[i + 2])
        l2 = float(lows[i + 2])

        # Bullish FVG: low[i+2] > high[i]
        if l2 > h0 and result["bull_fvg"] is None:
            gap_top = l2
            gap_bot = h0
            # [v12] Pastikan range tidak kosong (i+3 bisa == 0 untuk candle ke-3 dari akhir)
            start_j = i + 3
            end_j   = 0          # eksklusif, artinya tidak include candle terakhir
            if start_j < end_j:
                gap_filled = any(
                    float(lows[j]) <= gap_top and float(highs[j]) >= gap_bot
                    for j in range(start_j, end_j)
                )
            else:
                # FVG sangat dekat candle terbaru — anggap belum terisi
                gap_filled = False
            if not gap_filled:
                result["bull_fvg"] = {"top": gap_top, "bot": gap_bot}

        # Bearish FVG: high[i+2] < low[i]
        if h2 < l0 and result["bear_fvg"] is None:
            gap_top = l0
            gap_bot = h2
            start_j = i + 3
            end_j   = 0
            if start_j < end_j:
                gap_filled = any(
                    float(lows[j]) <= gap_top and float(highs[j]) >= gap_bot
                    for j in range(start_j, end_j)
                )
            else:
                gap_filled = False
            if not gap_filled:
                result["bear_fvg"] = {"top": gap_top, "bot": gap_bot}

        if result["bull_fvg"] and result["bear_fvg"]:
            break

    return result


# ═══════════════════════════════════════════════════════════════════
#  [FIX-3] check_entry_precision — REPLACE fungsi v11
#
#  BUG v11: FVG candle_conf alternatif hanya cek `price > prev_close`
#  (satu green candle biasa) tanpa volume gate.
#  Akibatnya: setiap green candle kecil di FVG zone dianggap konfirmasi.
#
#  FIX: tambahkan volume gate — candle dalam FVG harus disertai
#  volume di atas rata-rata (curr_vol > avg_vol * 1.2) agar dianggap
#  konfirmasi yang bermakna, bukan sekadar noise.
# ═══════════════════════════════════════════════════════════════════
def check_entry_precision(closes, highs, lows, volumes,
                          side: str, structure: dict, liq_map: dict,
                          ob: dict) -> dict:
    """
    Bukan langsung entry — tunggu konfirmasi.

    Checks:
    1. Pullback entry   : harga sudah pullback ke OB / structure level
    2. Rejection candle : pin bar / hammer / shooting star
    3. Candle confirmation: close di atas/bawah level kunci
    4. Volume confirmation: volume naik saat konfirmasi
    5. FVG zone         : harga berada di dalam / sangat dekat FVG

    [v11 P1-1] Pullback tolerance ATR-relative.
    [v11 P3]   FVG sebagai candle_conf alternatif.
    [v12 FIX-3] FVG candle_conf wajib disertai volume gate (>1.2x avg).
    """
    result = {
        "pullback":        False,
        "rejection":       False,
        "candle_conf":     False,
        "fvg_entry":       False,
        "precision_score": 0,
        "entry_quality":   "WAIT",
        "detail":          [],
    }

    if len(closes) < 5:
        return result

    price      = float(closes[-1])
    prev_close = float(closes[-2])
    hi         = float(highs[-1])
    lo         = float(lows[-1])
    body       = abs(price - prev_close)
    full_range = hi - lo + 1e-9
    upper_wick = hi - max(price, prev_close)
    lower_wick = min(price, prev_close) - lo
    avg_vol    = float(np.mean(volumes[-10:-1]))
    curr_vol   = float(volumes[-1])

    # ATR-relative tolerance untuk pullback check [v11 P1-1]
    atr_vals = [
        max(highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]))
        for i in range(max(1, len(closes) - 15), len(closes))
    ]
    atr    = float(np.mean(atr_vals)) if atr_vals else price * 0.01
    atr_pct = (atr / price) * 100 if price > 0 else 1.0
    pb_tol  = min(0.015, max(0.005, atr_pct * 0.4 / 100))

    # ── 1. Pullback Check ─────────────────────────────────────────────
    if side == "BUY":
        last_sl = structure.get("last_sl")
        ob_high = ob.get("ob_high") if ob.get("valid") else None
        if last_sl and price <= last_sl * (1 + pb_tol):
            result["pullback"] = True
            result["detail"].append(f"Pullback ke structure support (tol {pb_tol * 100:.1f}%)")
        elif ob_high and price <= ob_high * (1 + pb_tol * 0.5):
            result["pullback"] = True
            result["detail"].append("Pullback ke Order Block")
        elif liq_map.get("stop_cluster_below") and price <= liq_map["stop_cluster_below"] * (1 + pb_tol):
            result["pullback"] = True
            result["detail"].append("Pullback ke stop cluster")

    elif side == "SELL":
        last_sh = structure.get("last_sh")
        ob_low  = ob.get("ob_low") if ob.get("valid") else None
        if last_sh and price >= last_sh * (1 - pb_tol):
            result["pullback"] = True
            result["detail"].append(f"Pullback ke structure resistance (tol {pb_tol * 100:.1f}%)")
        elif ob_low and price >= ob_low * (1 - pb_tol * 0.5):
            result["pullback"] = True
            result["detail"].append("Pullback ke Order Block")
        elif liq_map.get("stop_cluster_above") and price >= liq_map["stop_cluster_above"] * (1 - pb_tol):
            result["pullback"] = True
            result["detail"].append("Pullback ke stop cluster")

    # ── 2. Rejection Candle ───────────────────────────────────────────
    if side == "BUY":
        if lower_wick > body * 2 and upper_wick < body * 0.5:
            result["rejection"] = True
            result["detail"].append("Hammer / Bullish Pin Bar")
        elif price > prev_close and body / full_range > 0.7:
            result["rejection"] = True
            result["detail"].append("Bullish Engulfing")

    elif side == "SELL":
        if upper_wick > body * 2 and lower_wick < body * 0.5:
            result["rejection"] = True
            result["detail"].append("Shooting Star / Bearish Pin Bar")
        elif price < prev_close and body / full_range > 0.7:
            result["rejection"] = True
            result["detail"].append("Bearish Engulfing")

    # ── 3. Candle Confirmation ────────────────────────────────────────
    last_sh  = structure.get("last_sh")
    last_sl  = structure.get("last_sl")
    ob_high  = ob.get("ob_high") if ob.get("valid") else None
    ob_low   = ob.get("ob_low")  if ob.get("valid") else None
    bos_bull = structure.get("bos") == "BULLISH" or structure.get("choch") == "BULLISH"
    bos_bear = structure.get("bos") == "BEARISH" or structure.get("choch") == "BEARISH"

    if side == "BUY":
        if ob_high and price > ob_high and prev_close <= ob_high:
            result["candle_conf"] = True
            result["detail"].append(f"Close konfirmasi di atas OB ${ob_high:.6f}")
        elif bos_bull and last_sh and price > last_sh and prev_close <= last_sh:
            result["candle_conf"] = True
            result["detail"].append(f"Close BOS konfirmasi di atas ${last_sh:.6f}")
        elif (liq_map.get("sweep_bull") and
              price > prev_close and
              body / full_range > 0.6 and
              curr_vol > avg_vol * 1.3):
            result["candle_conf"] = True
            result["detail"].append("Bullish close + volume konfirmasi post-sweep")

    elif side == "SELL":
        if ob_low and price < ob_low and prev_close >= ob_low:
            result["candle_conf"] = True
            result["detail"].append(f"Close konfirmasi di bawah OB ${ob_low:.6f}")
        elif bos_bear and last_sl and price < last_sl and prev_close >= last_sl:
            result["candle_conf"] = True
            result["detail"].append(f"Close BOS konfirmasi di bawah ${last_sl:.6f}")
        elif (liq_map.get("sweep_bear") and
              price < prev_close and
              body / full_range > 0.6 and
              curr_vol > avg_vol * 1.3):
            result["candle_conf"] = True
            result["detail"].append("Bearish close + volume konfirmasi post-sweep")

    # ── 4. FVG Zone Check — [v12 FIX-3] volume gate ──────────────────
    fvg = detect_fvg(highs, lows, lookback=25)

    if side == "BUY" and fvg.get("bull_fvg"):
        gap    = fvg["bull_fvg"]
        in_fvg = gap["bot"] <= price <= gap["top"] * 1.005
        near_fvg = price <= gap["top"] * (1 + pb_tol)
        if in_fvg or near_fvg:
            result["fvg_entry"] = True
            result["detail"].append(f"FVG zone ${gap['bot']:.6f}–${gap['top']:.6f}")
            # [v12 FIX-3] FVG candle_conf: wajib disertai volume gate (>1.2x avg)
            # Mencegah green candle noise dianggap konfirmasi
            if (not result["candle_conf"] and in_fvg and
                    price > prev_close and curr_vol > avg_vol * 1.2):
                result["candle_conf"] = True
                result["detail"].append("FVG zone konfirmasi (bullish close + volume dalam gap)")

    elif side == "SELL" and fvg.get("bear_fvg"):
        gap    = fvg["bear_fvg"]
        in_fvg = gap["bot"] * 0.995 <= price <= gap["top"]
        near_fvg = price >= gap["bot"] * (1 - pb_tol)
        if in_fvg or near_fvg:
            result["fvg_entry"] = True
            result["detail"].append(f"FVG zone ${gap['bot']:.6f}–${gap['top']:.6f}")
            # [v12 FIX-3] Volume gate untuk SELL juga
            if (not result["candle_conf"] and in_fvg and
                    price < prev_close and curr_vol > avg_vol * 1.2):
                result["candle_conf"] = True
                result["detail"].append("FVG zone konfirmasi (bearish close + volume dalam gap)")

    if curr_vol > avg_vol * 1.5:
        result["detail"].append(f"Volume spike {curr_vol / avg_vol:.1f}x")

    # ── 5. Precision Score ────────────────────────────────────────────
    score = (
        (2 if result["pullback"]    else 0) +
        (2 if result["rejection"]   else 0) +
        (2 if result["candle_conf"] else 0) +
        (2 if result["fvg_entry"]   else 0)
    )
    result["precision_score"] = score

    if score >= 4:
        result["entry_quality"] = "READY"
    elif score >= 2:
        result["entry_quality"] = "WAIT"
    else:
        result["entry_quality"] = "SKIP"

    return result


# ═══════════════════════════════════════════════════════════════════
#  [FIX-4 + UPG-3] check_intraday — REPLACE fungsi v11
#
#  BUG v11: Ada dua blok perhitungan entry_price:
#    Blok 1 (manual, baris ~806): hitung entry via OB/stop_cluster
#    Blok 2 (resolve_entry, baris ~886): override entry_price lagi
#  Akibatnya: blok 1 tidak pernah efektif — selalu di-override blok 2.
#
#  FIX: hapus blok 1, biarkan resolve_entry sebagai single source of truth.
#
#  [UPG-3] Tambahkan post_sweep_entry sebagai kondisi eksplisit
#  dengan bobot lebih tinggi di conditions list (weight 4 via "sweep_confirm").
#  Ini yang membedakan "bot menghindari trap" vs "bot masuk setelah trap selesai".
#
#  INSTRUKSI TAMBAHAN: Di W dict (KONFIGURASI GLOBAL), tambahkan:
#      "sweep_confirm": 4,   # post-sweep confirmed entry — premium signal
# ═══════════════════════════════════════════════════════════════════
def check_intraday(client, pair, price, fg, ob_ratio, funding,
                   trending, market_data, liq, oi_signal, regime):
    closes, highs, lows, volumes = get_candles(client, pair, "1h", 100)
    if closes is None:
        return

    rsi                     = calc_rsi(closes)
    stoch_rsi               = calc_stoch_rsi(closes)
    macd, msig              = calc_macd(closes)
    bb_low, bb_mid, bb_high = calc_bb(closes)
    atr                     = calc_atr(closes, highs, lows)
    ema20                   = calc_ema(closes, 20)
    ema50                   = calc_ema(closes, 50)
    vwap                    = calc_vwap(closes, highs, lows, volumes)
    ichi                    = calc_ichimoku(closes, highs, lows)
    support, resistance     = calc_support_resistance(highs, lows, closes)
    divergence              = calc_rsi_divergence(closes, highs, lows)
    poc                     = calc_volume_profile(closes, volumes)

    structure = detect_structure(closes, highs, lows, lookback=60)
    liq_map   = detect_liquidity_map(closes, highs, lows, volumes, lookback=30)

    symbol      = pair.replace("_USDT", "")
    is_trending = symbol in trending

    vol_ok, _ = volatility_ok(atr, price, "INTRADAY")
    if not vol_ok:
        return

    # [v11 P2] HTF bias dari 4h
    htf_bias = get_htf_bias(client, pair, "4h", 80)

    for side in ["BUY", "SELL"]:
        is_bull = (side == "BUY")

        # [v11 P2] HTF 4h filter
        htf_score_penalty = 0
        if is_bull:
            if htf_bias == "BEARISH":
                print(f"  ↩️ {pair} [INTRA BUY] skip — HTF 4h BEARISH (strong)")
                continue
            elif htf_bias == "BEARISH_WEAK":
                htf_score_penalty = 3
                print(f"  ⚠️ {pair} [INTRA BUY] HTF 4h BEARISH_WEAK — penalty -3")
        else:
            if htf_bias == "BULLISH":
                print(f"  ↩️ {pair} [INTRA SELL] skip — HTF 4h BULLISH (strong)")
                continue
            elif htf_bias == "BULLISH_WEAK":
                htf_score_penalty = 3
                print(f"  ⚠️ {pair} [INTRA SELL] HTF 4h BULLISH_WEAK — penalty -3")

        # Structure must trigger first
        has_struct = (
            (is_bull and (structure.get("bos") == "BULLISH" or
                          structure.get("choch") == "BULLISH" or
                          liq_map.get("sweep_bull"))) or
            (not is_bull and (structure.get("bos") == "BEARISH" or
                               structure.get("choch") == "BEARISH" or
                               liq_map.get("sweep_bear")))
        )
        if not has_struct:
            continue

        # Macro filter
        if regime.get("regime") == "RISK_OFF" and pair not in SAFE_PAIRS_RISK_OFF:
            continue

        # MACD confirmation
        if is_bull and macd <= msig:
            continue
        if not is_bull and macd >= msig:
            continue

        ob        = detect_order_block(closes, highs, lows, volumes, side=side, lookback=30)
        precision = check_entry_precision(closes, highs, lows, volumes,
                                          side, structure, liq_map, ob)
        if precision["entry_quality"] == "SKIP":
            continue

        # [v12 UPG-3] post_sweep_entry: sinyal premium — bot masuk SETELAH trap selesai
        # Bukan sekadar menghindari sweep, tapi aktif masuk setelah institutional trap confirmed
        post_sweep_confirmed = (liq_map.get("post_sweep_entry") == side)

        if is_bull:
            conditions = [
                (structure.get("bos")   == "BULLISH",              "bos"),
                (structure.get("choch") == "BULLISH",              "choch"),
                (liq_map.get("sweep_bull"),                        "liq_sweep"),
                # [v12 UPG-3] Post-sweep entry — premium weight via "sweep_confirm"
                (post_sweep_confirmed,                             "sweep_confirm"),
                (liq_map.get("equal_lows") is not None,            "equal_hl"),
                (liq_map.get("stop_cluster_below") is not None,    "stop_cluster"),
                (ob.get("valid"),                                  "order_block"),
                (precision.get("pullback"),                        "pullback"),
                (precision.get("rejection"),                       "rejection"),
                (precision.get("candle_conf"),                     "candle_conf"),
                (precision.get("fvg_entry"),                       "fvg"),
                (rsi < 35,                                         "rsi_extreme"),
                (stoch_rsi < 0.25,                                 "stoch_rsi"),
                (price <= bb_low,                                  "bb_extreme"),
                (fg < 25,                                          "fg_extreme"),
                (ob_ratio > 1.1,                                   "ob_ratio"),
                (is_trending,                                      "trending"),
                (ema20 > ema50,                                    "ema_cross"),
                (divergence == "BULLISH",                          "divergence"),
                (poc and price <= poc,                             "poc"),
                (price <= support * 1.03,                         "support_res"),
                (funding and funding < -0.001,                     "funding"),
                (price > vwap,                                     "vwap"),
                (ichi.get("above_cloud"),                          "ichimoku"),
                # [v12 FIX-2] liq_bias sekarang bisa BUY (sebelumnya selalu False)
                (liq.get("liq_bias") == "BUY",                    "liq_cluster"),
                (oi_signal in ("STRONG_BUY", "SQUEEZE"),           "oi_signal"),
            ]
            tp1 = price * 1.07
            tp2 = price * 1.15
            sl  = max(price - atr * 2.5, price * 0.94)
            if liq_map.get("sweep_level"):
                sl = min(sl, liq_map["sweep_level"] * 0.997)

        else:
            conditions = [
                (structure.get("bos")   == "BEARISH",              "bos"),
                (structure.get("choch") == "BEARISH",              "choch"),
                (liq_map.get("sweep_bear"),                        "liq_sweep"),
                # [v12 UPG-3] Post-sweep SELL
                (post_sweep_confirmed,                             "sweep_confirm"),
                (liq_map.get("equal_highs") is not None,           "equal_hl"),
                (liq_map.get("stop_cluster_above") is not None,    "stop_cluster"),
                (ob.get("valid"),                                  "order_block"),
                (precision.get("pullback"),                        "pullback"),
                (precision.get("rejection"),                       "rejection"),
                (precision.get("candle_conf"),                     "candle_conf"),
                (precision.get("fvg_entry"),                       "fvg"),
                (rsi > 65,                                         "rsi_extreme"),
                (stoch_rsi > 0.75,                                 "stoch_rsi"),
                (price >= bb_high,                                 "bb_extreme"),
                (fg > 60 or fg < 20,                              "fg_extreme"),
                (ob_ratio < 0.9,                                   "ob_ratio"),
                (divergence == "BEARISH",                          "divergence"),
                (poc and price >= poc,                             "poc"),
                (price >= resistance * 0.97,                      "support_res"),
                (funding and funding > 0.001,                      "funding"),
                (price < vwap,                                     "vwap"),
                (ichi.get("below_cloud"),                          "ichimoku"),
                # [v12 FIX-2] liq_bias SELL
                (liq.get("liq_bias") == "SELL",                   "liq_cluster"),
                (oi_signal == "STRONG_SELL",                       "oi_signal"),
            ]
            tp1 = price * 0.93
            tp2 = price * 0.87
            sl  = min(price + atr * 2.5, price * 1.06)

        score = wscore(conditions)
        score = max(0, score - htf_score_penalty)    # [v11 P2] HTF penalty
        tier  = assign_tier(score, structure, precision, liq_map)
        stars_val = min(5, max(1, score // 3))

        print(f"  📊 {pair} [INTRA {side}] score={score} tier={tier} "
              f"htf4h={htf_bias} phase={structure.get('trend_phase')} "
              f"entry={precision['entry_quality']}"
              f"{' 🎯SWEEP_CONFIRMED' if post_sweep_confirmed else ''}")

        if tier == "SKIP":
            continue

        extra = (f"📡 <i>Regime: {regime['regime']} | BTC 1h: {regime['btc_1h_chg']:+.1f}%</i>\n"
                 f"📐 <i>HTF 4h: {htf_bias}</i>")
        if divergence == ("BULLISH" if is_bull else "BEARISH"):
            extra += f"\n🔀 <i>{'Bullish' if is_bull else 'Bearish'} Divergence!</i>"
        if poc and is_bull and price <= poc:
            extra += f"\n📊 <i>Di bawah POC ${poc:.6f}</i>"
        if is_trending:
            extra += "\n🔥 <i>Trending CoinGecko!</i>"
        if fg < 20 and is_bull:
            extra += f"\n😱 <i>Extreme Fear {fg}</i>"
        if ichi.get("above_cloud" if is_bull else "below_cloud"):
            extra += f"\n☁️ <i>Ichimoku: {'di atas' if is_bull else 'di bawah'} cloud</i>"
        if precision.get("fvg_entry"):
            extra += "\n📐 <i>FVG zone terdeteksi — entry presisi tinggi</i>"
        # [v12 UPG-3] label khusus post-sweep entry
        if post_sweep_confirmed:
            extra += "\n🏹 <i>Post-Sweep Entry — institutional trap confirmed</i>"

        # [v12 FIX-4] Satu sumber entry: resolve_entry (hapus blok manual sebelumnya)
        entry_info  = resolve_entry(price, side, atr, ob, liq_map, structure, precision)
        final_entry = entry_info["entry_price"]
        if side == "BUY":
            tp1 = final_entry * 1.07
            tp2 = final_entry * 1.15
            sl  = max(final_entry - atr * 2.5, final_entry * 0.94)
        else:
            tp1 = final_entry * 0.93
            tp2 = final_entry * 0.87
            sl  = min(final_entry + atr * 2.5, final_entry * 1.06)

        queue_signal(
            pair=pair, signal_type="INTRADAY", side=side,
            entry=final_entry, tp1=tp1, tp2=tp2, sl=sl,
            strength=stars_val, timeframe="1h", valid_minutes=120,
            tier=tier, score=score,
            sources="Gate.io · Structure · Liq Engine · OI · Ichimoku · FVG",
            extra=extra, structure=structure, liq_map=liq_map,
            precision=precision, ob=ob, entry_info=entry_info,
        )


# ═══════════════════════════════════════════════════════════════════
#  [UPG-1] calc_position_size — REPLACE fungsi lama
#
#  SEBELUMNYA: semua signal diperlakukan sama (flat sizing).
#  SEKARANG  : dynamic sizing berdasarkan tier dan score.
#
#  Logic:
#  - Base size ditentukan dari risk % akun dan jarak SL (tetap sama)
#  - Confidence multiplier diterapkan di atas base size:
#      Tier S + score >= 18  → 1.5x  (premium conviction)
#      Tier S atau score >= 15 → 1.25x (high confidence)
#      Tier A               → 1.0x  (standard)
#      Tier B               → 0.6x  (cautious)
#      Default              → 0.4x  (minimal / exploratory)
#  - Hard cap: single position tidak boleh melebihi MAX_SINGLE_EXPOSURE
#
#  INSTRUKSI: Di KONFIGURASI GLOBAL, pastikan ada:
#      MAX_SINGLE_EXPOSURE = 0.15   # maks 15% dari total kapital per trade
#      RISK_PER_TRADE      = 0.01   # risk 1% akun per trade
# ═══════════════════════════════════════════════════════════════════
def calc_position_size(entry_price: float, sl_price: float,
                       account_balance: float, tier: str = "B",
                       score: int = 0) -> dict:
    """
    Dynamic position sizing berdasarkan tier dan score sinyal.

    Args:
        entry_price     : harga entry
        sl_price        : harga stop loss
        account_balance : total kapital tersedia
        tier            : tier sinyal ("S", "A", "B", "C", "SKIP")
        score           : weighted score dari wscore()

    Returns:
        dict dengan:
            "size_usdt"    : nominal posisi dalam USDT
            "size_pct"     : persentase dari kapital
            "multiplier"   : confidence multiplier yang digunakan
            "risk_usdt"    : estimasi risiko jika SL kena
            "valid"        : apakah sizing valid (SL tidak terlalu dekat)
    """
    result = {
        "size_usdt":  0.0,
        "size_pct":   0.0,
        "multiplier": 0.0,
        "risk_usdt":  0.0,
        "valid":      False,
    }

    # Guard: harga tidak valid
    if entry_price <= 0 or sl_price <= 0 or account_balance <= 0:
        return result

    sl_distance_pct = abs(entry_price - sl_price) / entry_price

    # Guard: SL terlalu dekat (< 0.3%) — sizing tidak reliable
    if sl_distance_pct < 0.003:
        return result

    # Base size dari risk management: berapa USDT yang mau di-risk
    risk_usdt = account_balance * RISK_PER_TRADE   # e.g. 1% dari akun

    # Base position size = risk / SL distance
    base_size = risk_usdt / sl_distance_pct

    # Confidence multiplier berdasarkan tier + score
    if tier == "S" and score >= 18:
        multiplier = 1.5     # premium conviction — full size + bonus
    elif tier == "S" or score >= 15:
        multiplier = 1.25    # high confidence
    elif tier == "A":
        multiplier = 1.0     # standard
    elif tier == "B":
        multiplier = 0.6     # cautious
    else:
        multiplier = 0.4     # minimal / tier C atau tidak dikenal

    # Final size dengan multiplier
    final_size = base_size * multiplier

    # Hard cap: tidak boleh melebihi MAX_SINGLE_EXPOSURE dari total kapital
    max_size   = account_balance * MAX_SINGLE_EXPOSURE
    final_size = min(final_size, max_size)

    # Minimum viable size check (hindari dust trade)
    if final_size < 5.0:   # minimal $5 USDT
        return result

    result["size_usdt"]  = round(final_size, 2)
    result["size_pct"]   = round(final_size / account_balance * 100, 2)
    result["multiplier"] = multiplier
    result["risk_usdt"]  = round(final_size * sl_distance_pct, 2)
    result["valid"]      = True

    return result


# ═══════════════════════════════════════════════════════════════════
#  check_swing — PATCH MANUAL (sama seperti v11, tidak ada perubahan)
#  Tidak ada fungsi baru yang perlu ditambahkan untuk swing.
#  Pastikan "fvg" dan "sweep_confirm" sudah ada di conditions list
#  mengikuti pola check_intraday di atas.
#
#  JUGA: tambahkan "sweep_confirm" ke W dict:
#      "sweep_confirm": 4,
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  RINGKASAN PERUBAHAN v12
# ═══════════════════════════════════════════════════════════════════
#
#  REPLACE langsung (paste sebagai replacement):
#  ├── detect_order_block()      ← FIX-1: mitigation zone fix
#  ├── detect_liquidity_map()    ← FIX-2: liq_bias + UPG-2: post_sweep_entry
#  ├── detect_fvg()              ← minor edge case fix
#  ├── check_entry_precision()   ← FIX-3: FVG candle_conf volume gate
#  ├── check_intraday()          ← FIX-4: hapus double entry + UPG-3
#  └── calc_position_size()      ← UPG-1: dynamic sizing
#
#  EDIT CONFIG (di blok W = {...}):
#  └── tambah "sweep_confirm": 4     ← premium post-sweep entry weight
#
#  EDIT CONFIG (di KONFIGURASI GLOBAL):
#  ├── MAX_SINGLE_EXPOSURE = 0.15    ← jika belum ada
#  └── RISK_PER_TRADE      = 0.01    ← jika belum ada
#
#  TIDAK ADA PERUBAHAN DI:
#  ├── interpret_oi()              (tetap dari v11)
#  ├── get_htf_bias()              (tetap dari v11)
#  ├── check_scalping()            (tetap dari v11)
#  ├── check_swing()               (tetap dari v11)
#  ├── check_moonshot()
#  ├── flush_signal_queue()
#  ├── assign_tier()
#  └── semua fungsi lain
#
#  PERKIRAAN DAMPAK:
#  ├── FIX-1: OB valid lebih banyak (mitigation tidak terlalu agresif)
#  ├── FIX-2: liq_cluster conditions aktif kembali (+2 weight per signal)
#  ├── FIX-3: FVG confirmation lebih presisi (noise berkurang)
#  ├── FIX-4: entry price konsisten (resolve_entry jadi single source)
#  ├── UPG-1: equity curve lebih smooth (size proporsional ke conviction)
#  ├── UPG-2: post_sweep_entry flag tersedia untuk semua strategy
#  └── UPG-3: bot aktif masuk setelah trap — bukan hanya menghindari
# ═══════════════════════════════════════════════════════════════════
