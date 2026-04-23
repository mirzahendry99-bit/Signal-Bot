"""
╔══════════════════════════════════════════════════════════════════╗
║           SIGNAL BOT — CLEAN v7.22                              ║
║                                                                  ║
║  Perbaikan v7.22 (Critical Drawdown Fix):                       ║
║                                                                  ║
║  [v7.22 #A] Peak equity dihitung dari equity ABSOLUT            ║
║             Bug v7.21: peak mulai dari 0 (cumulative PnL saja)  ║
║             → Peak = $1.21 (PnL tertinggi, bukan equity)        ║
║             → DD = (1.21 - (-0.08)) / 1.21 = 106.8% ← salah   ║
║             → Bot terjebak HALT selamanya meski equity $199.93  ║
║                                                                  ║
║             Fix: peak di-anchor ke ACCOUNT_EQUITY_USDT ($200)   ║
║             equity_now = ACCOUNT_EQUITY_USDT + cumulative_pnl   ║
║             peak = max(prev_peak, equity_now)                   ║
║             DD = (peak - equity_now) / peak → angka realistis   ║
║                                                                  ║
║             Dampak langsung setelah fix:                         ║
║             Peak → $200.xx (benar, dalam USDT absolut)          ║
║             Current DD → ~0.04% (bukan 106.8%)                  ║
║             HALT mode → kembali ke NORMAL                        ║
║             Position sizing → kembali ke tier normal (×1.0)     ║
║                                                                  ║
║             Fungsi yang diperbaiki:                              ║
║             - _calc_equity_drawdown() [dipakai get_drawdown_state]║
║             - build_equity_curve() [dipakai Equity Report Tg]   ║
║                                                                  ║
║             Tambah MAX_RISK_USDT = ACCOUNT_EQUITY_USDT × 0.06   ║
║             get_portfolio_state() hitung total_risk_usdt dari    ║
║             sl_dist per trade (risk = size × |sl-entry|/entry).  ║
║             portfolio_allows() gate by total_risk_usdt — bukan   ║
║             locked saja. SL dekat = risk kecil = boleh masuk.   ║
║             SEBELUMNYA: MAX_LOCKED_USDT tidak membedakan SL      ║
║             dekat vs jauh — sama-sama blok padahal risk beda.   ║
║                                                                  ║
║  [v7.20 #B] Correlation-adjusted position sizing                 ║
║             calc_position_size() terima param pair + open_pairs. ║
║             Hitung max corr antara pair baru vs semua open trade. ║
║             corr_scalar = 1 - (max_corr × CORR_SIZE_PENALTY)    ║
║             Contoh: 3 pair r=0.85 → size × (1 - 0.85×0.5)=×0.575║
║             SEBELUMNYA: 3 pair highly correlated = full size ×3  ║
║             → real exposure jauh lebih besar dari estimasi       ║
║                                                                  ║
║  [v7.20 #C] Equity compounding throttle post-TP1                ║
║             Partial TP → equity naik → cache langsung dipakai   ║
║             sizing berikutnya = over-aggressive compounding.     ║
║             Fix: tambah COMPOUNDING_THROTTLE_PCT (default 50%). ║
║             Size dari equity_gain × 0.5 saja yang dikompound.   ║
║             get_current_equity_usdt() simpan pre_tp1_equity.    ║
║             calc_position_size pakai blended equity jika ada    ║
║             recent partial TP dalam siklus yang sama.            ║
║                                                                  ║
║  [v7.20 #D] Portfolio heat — total risk % lintas semua trade    ║
║             get_portfolio_state() hitung portfolio_heat_pct:     ║
║             sum(risk_usdt per trade) / equity × 100             ║
║             portfolio_allows() blok jika heat ≥ MAX_HEAT_PCT.   ║
║             Log & Telegram summary tampilkan heat % per cycle.   ║
║             SEBELUMNYA: tidak ada konsep "berapa % equity        ║
║             yang sedang dalam risiko" secara aggregate           ║
║                                                                  ║
║  Perbaikan v7.19 (Edge Case Realism Fix):                       ║
║                                                                  ║
║  [v7.19 #A] Double counting fix — exposure pakai locked_usdt   ║
║             get_portfolio_state() sekarang query position_size  ║
║             dan akumulasi sebagai locked_usdt. Partial trade     ║
║             dihitung setengah size (sudah close 50% di TP1).    ║
║             portfolio_allows() cek MAX_LOCKED_USDT sebelum izin ║
║             signal baru — mencegah overestimate exposure karena  ║
║             count-based portfolio tidak tahu ukuran posisi.      ║
║             SEBELUMNYA: 5 trade $10 = 5 trade $50 di mata bot   ║
║                                                                  ║
║  [v7.19 #B] SL after partial TP pakai remaining size saja       ║
║             evaluate_open_trades(): jika trade di TP1_PARTIAL   ║
║             dan SL kena (→BREAKEVEN), pnl_usdt dihitung dari    ║
║             sisa size (1-partial_ratio), bukan full size.        ║
║             SEBELUMNYA: SL hit setelah TP1 = pnl negatif dari   ║
║             full size → equity distorted, WR table salah         ║
║                                                                  ║
║  [v7.19 #C] EXPIRED setelah partial TP → result "PARTIAL_WIN"  ║
║             Jika trade sudah TP1_PARTIAL lalu expired sebelum   ║
║             TP2 kena, result diset "PARTIAL_WIN" bukan "EXPIRED" ║
║             pnl_usdt = partial_pnl_usdt yang sudah tersimpan.   ║
║             SEBELUMNYA: PARTIAL_WIN dianggap EXPIRED → winrate  ║
║             bias turun, expectancy salah                         ║
║                                                                  ║
║  [v7.19 #D] available_balance — pisahkan equity vs cash         ║
║             get_current_equity_usdt() sekarang return dict:      ║
║             {"equity": float, "available": float,               ║
║              "locked": float}                                    ║
║             available = equity - locked_capital_in_open_trades   ║
║             calc_position_size() pakai "available" bukan equity  ║
║             penuh — cegah oversizing saat modal sudah terkunci   ║
║                                                                  ║
║  Perbaikan v7.18 (Accuracy & Integrity Fix):                    ║
║                                                                  ║
║  [v7.18 #A] Partial TP PnL — realized PnL tidak lagi hilang    ║
║             TP1_PARTIAL sekarang menyimpan partial_pnl_usdt     ║
║             ke DB: (tp1-entry)/entry * (pos_size * ratio)       ║
║             get_current_equity_usdt() menjumlahkan:             ║
║             closed pnl_usdt + open partial_pnl_usdt             ║
║             SEBELUMNYA: half-close profit tidak terekam          ║
║             → equity underestimate saat banyak trade di TP1     ║
║                                                                  ║
║  [v7.18 #B] Equity cache diinvalidasi setelah lifecycle update  ║
║             evaluate_open_trades() set _equity_cache["ts"]=0    ║
║             saat ada trade yang diupdate — memaksa re-query      ║
║             SEBELUMNYA: cache 30 menit bisa pakai equity lama   ║
║             meski ada trade baru yang baru ditutup              ║
║                                                                  ║
║  [v7.18 #C] position_size tersimpan ke DB via save_signal()     ║
║             Semua caller (main/microcap/pump) pass position_size ║
║             evaluate_open_trades() pakai size dari DB, bukan    ║
║             fallback BASE_POSITION_USDT yang salah              ║
║             SEBELUMNYA: pnl_usdt selalu pakai BASE=10 USDT      ║
║             terlepas dari berapa size aktual yang digunakan      ║
║                                                                  ║
║  [v7.18 #D] Extra DB query per trade dieliminasi                ║
║             position_size ikut di-select di query awal           ║
║             evaluate_open_trades — tidak lagi query terpisah     ║
║             per trade untuk fetch size. N trade = N-1 DB calls  ║
║             lebih sedikit per cycle.                             ║
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
║  Perbaikan v7.8 (Anti-Overfitting — Risk Logic Fix):            ║
║  [v7.8 #1] score_signal: faktor 15+ → 6 core scoring factor    ║
║            Pisahkan arsitektur menjadi 3 layer eksplisit:       ║
║            • Setup Detection: BOS/CHoCH/liq_sweep (hard gate)  ║
║            • Confirmation   : OB + MACD + Volume (scoring)     ║
║            • Execution      : EMA alignment + pullback (bonus)  ║
║  [v7.8 #2] Hapus dari scoring: rsi_zone, rsi_extreme,          ║
║            vwap_side, candle_body, equal_lows, equal_highs,     ║
║            ob_ratio, macd_soft — semua terlalu "idealized"      ║
║            atau bobot terlalu kecil untuk jadi penentu keputusan║
║  [v7.8 #3] BOS + CHoCH: pindah dari scoring → pure hard gate   ║
║            (sudah ada di has_struct check, tidak perlu          ║
║            double-dip sebagai score contributor)                ║
║  [v7.8 #4] MACD: soft penalty (-2) dihapus — MACD berlawanan   ║
║            arah = tidak dapat skor, bukan hukuman aktif.        ║
║            Model tidak boleh "menghukum" ketidakhadiran signal  ║
║  [v7.8 #5] TIER_MIN_SCORE disesuaikan ke skala baru:           ║
║            S:14 → S:11 | A+:10 → A+:8 | A:8 → A:6             ║
║            Max score baru: 4+3+3+3+2+2+2=19 (vs 37 sebelumnya)║
║  [v7.8 #6] calc_conviction: threshold disesuaikan ke skala baru║
║  [v7.8 #7] check_microcap micro_score: anti-overfitting fix     ║
║            Hapus double-dip: vol_ratio & pct_3h sudah jadi gate ║
║            Hapus idealized: rsi<50, change_24h<5.0              ║
║            Pertahankan: has_sweep (+2), ema_short_bull (+1)     ║
║            Tambah: vol_ratio>=10x bonus (+1, jauh dari gate)    ║
║            Max score: 4 | Tier A threshold: 2                   ║
║            Score display Telegram: X/10 → X/4                   ║
║  [v7.8 #8] score_signal: additive → group-max scoring           ║
║            Masalah: semua indikator dianggap independen →        ║
║            EMA+BOS=trend overlap, RSI+MACD=momentum overlap,    ║
║            liq_sweep+OB+pullback=institusional overlap           ║
║            Solusi: 4 grup scoring, max per grup, sum antar grup  ║
║            • TREND    : EMA alignment          (max 3)          ║
║            • MOMENTUM : MACD crossover         (max 3)          ║
║            • LIQUIDITY: liq_sweep>OB>pullback  (max 4, 1 terbaik)║
║            • VOLUME   : vol spike              (max 3)          ║
║            • Regime   : ADX trending/ranging   (±2, additive)   ║
║            Max score: 15 | Tier S:12 A+:9 A:6                   ║
║  [v7.8 #9] Probabilistic confidence model (baru)                ║
║            Masalah: semua Tier A dianggap sama — A score 8 =    ║
║            A score 13 di mata bot, padahal tidak di live        ║
║            Solusi: estimate_confidence(score) → win rate dari   ║
║            riwayat signal nyata di Supabase, per score bucket   ║
║            • Bucket: 6 | 7-8 | 9-11 | 12+                     ║
║            • Minimum 20 sample untuk angka reliable             ║
║            • Di bawah threshold: "Data kurang (n=X)"           ║
║            • Tampil di Telegram sebagai baris "Hist WR" terpisah║
║            • Beda dari Conviction (rule-based) — ini data-driven║
║            • Cache 1 jam, pre-warm di startup run()             ║
║  [v7.8 #10] calc_sl_tp: structure-first SL + ATR buffer         ║
║            Masalah: SL = ATR*mult murni → tidak adapt ke        ║
║            struktur; min(ATR,struct) → selalu ambil terkecil;   ║
║            TP dari ATR bukan actual SL dist → R/R salah;        ║
║            SL landed tepat di swing low → mudah ter-wick        ║
║            Solusi 4 langkah:                                     ║
║            1. Structure anchor: last_sl/last_sh (primary)       ║
║            2. ATR buffer di belakang level (0.3/0.5× ATR)       ║
║            3. Sanity bounds: min/max pct dari entry              ║
║            4. ATR fallback jika tidak ada struktur valid         ║
║            TP dari ACTUAL sl_dist (entry−sl nyata), bukan ATR   ║
║            Baru: ATR_SL_BUFFER_INTRADAY=0.3, SWING=0.5          ║
║                  INTRADAY_MIN/MAX_SL_PCT, SWING_MIN/MAX_SL_PCT  ║
║                                                                  ║
║  Perbaikan v7.10 (Setup Soft Gate, Conflict Resolution, WR Fix):║
║  [v7.10 #1] Setup gate: hard binary → soft scoring            ║
║            Masalah: BOS/CHoCH = wajib → bot miss:             ║
║            • breakout valid tanpa CHoCH baru                  ║
║            • continuation saat struktur belum reset           ║
║            Solusi: detect_setup_quality() → skor 0–3          ║
║            • 0 = tidak ada sinyal setup apapun → SKIP         ║
║            • 1 = BIAS + momentum saja (continuation mode)     ║
║            • 2 = liq_sweep saja (entry level kuat)            ║
║            • 3 = BOS/CHoCH terkonfirmasi (full structure)     ║
║            Hard gate diganti: require setup_score >= 1         ║
║            Setup score dimasukkan ke score_signal via param    ║
║  [v7.10 #2] Strategy conflict resolution — priority system     ║
║            Masalah: INTRADAY SELL + PUMP BUY pair sama =       ║
║            sinyal kontradiktif terkirim tanpa filter           ║
║            Solusi: resolve_conflicts(signals) sebelum kirim    ║
║            Priority: PUMP > INTRADAY BUY > SWING BUY >        ║
║                      SWING SELL > INTRADAY SELL                ║
║            Rule: satu pair, satu arah — arah prioritas tinggi  ║
║            veto arah lain untuk pair yang sama dalam 1 cycle   ║
║            Override log: pair conflict tercatat di run summary  ║
║  [v7.10 #3] Probabilistic sample — per-bucket adaptive MIN     ║
║            Masalah: MIN_SAMPLE=20 flat untuk semua bucket      ║
║            → bucket "12+" lebih jarang muncul → cepat bias     ║
║            → confidence angka "60%" dari 5 trades menyesatkan  ║
║            Solusi: MIN_SAMPLE per bucket (sparse bucket = ketat)║
║            • bucket "6"     : MIN 15 (banyak sampel tersedia)  ║
║            • bucket "7-8"   : MIN 20 (baseline)                ║
║            • bucket "9-11"  : MIN 25 (lebih hati-hati)         ║
║            • bucket "12+"   : MIN 30 (rarest — paling ketat)   ║
║            Context bucket (regime-aware): ambil ×1.5 dari base ║
║            Efek: confidence angka "12+" tidak muncul sampai     ║
║            ada 30 trade nyata — bukan 20 seperti sebelumnya    ║
║                                                                  ║
║  Perbaikan v7.9 (Context-Aware Confidence & Flexible Grouping): ║
║  [v7.9 #1] estimate_confidence(): regime-aware & context-aware  ║
║            Masalah: confidence hanya dari score bucket           ║
║            → score 9 di TRENDING ≠ score 9 di RANGING           ║
║            → bot treat keduanya identik (tidak adaptive)        ║
║            Solusi:                                               ║
║            • load_winrate_table() query tambah kolom "regime"   ║
║            • Bucket baru: "12+|TRENDING", "9-11|RANGING", dll   ║
║            • Fallback ke bucket score-only jika sample <20      ║
║            • estimate_confidence() terima arg regime + strategy  ║
║            • Label tampilkan konteks: "🟢 Kuat (TRENDING 67%)"  ║
║  [v7.9 #2] score_signal: group_weight_multiplier per regime      ║
║            Masalah: grouping rigid — semua market treatall group ║
║            → breakout tanpa pullback: liquidity group lemah      ║
║              tapi trade tetap valid                              ║
║            → di RANGING: volume lebih predictive dari EMA       ║
║            Solusi: per-regime weight multiplier pada setiap grup ║
║            • TRENDING : trend×1.0, momentum×1.0, liq×1.0,      ║
║                         vol×1.0 (baseline — semua equal)        ║
║            • RANGING  : trend×0.7, momentum×0.8, liq×1.3,      ║
║                         vol×1.2 (liquidity & vol lebih reliable)║
║            • Multiplier tidak ubah max group — tetap floor 0    ║
║            • Score masih integer (round after multiply)         ║
║            • Efek: breakout strong (liq=0) dapat full score     ║
║              di TRENDING, tapi sedikit penalti di RANGING       ║
║                                                                  ║
║  Perbaikan v7.12 (Adaptive Priority + Unified Scoring + Lifecycle):║
║  [v7.12 #1] Dynamic priority system — adapt ke kondisi market   ║
║             Sebelumnya: PUMP > INTRADAY BUY > SWING BUY (fixed) ║
║             Masalah: market crash → PUMP tetap prioritas 0,       ║
║             padahal volume spike di crash = distribusi, bukan     ║
║             accumulation — pump signal lebih berbahaya            ║
║             Solusi: calc_dynamic_priority(sig, btc, fg)           ║
║             Priority dihitung runtime dari: BTC regime,          ║
║             Fear & Greed index, tier signal, dan strategy         ║
║             • BTC crash: PUMP turun prioritas (lebih rendah dari  ║
║               SWING BUY) — extreme greed juga demikian            ║
║             • BTC drop (1h): PUMP prioritas normal, BUY          ║
║               strategies sedikit dinaikkan threshold              ║
║             • Market normal: base priority (seperti v7.10)        ║
║  [v7.12 #2] Unified scoring engine untuk Microcap                ║
║             Sebelumnya: check_microcap pakai micro_score (0–4)   ║
║             sistem scoring beda — tidak bisa dibandingkan lintas  ║
║             strategy dan tidak dapat masuk win rate bucket yang   ║
║             sama                                                  ║
║             Solusi: score_microcap_unified() — panggil            ║
║             score_signal() dengan konteks microcap, lalu          ║
║             normalisasi ke skala yang sama                        ║
║             Microcap sekarang punya: tier S/A+/A, regime,        ║
║             conviction, dan masuk win rate tracking yang sama     ║
║             Setup score microcap: sweep=2, ema_bull=1 (konsisten)║
║  [v7.12 #3] Trade lifecycle tracking                              ║
║             Sebelumnya: bot kirim signal → selesai, tidak ada     ║
║             follow-up, result di Supabase selalu NULL             ║
║             Masalah: win rate table tidak pernah terisi →         ║
║             model probabilistik tidak pernah belajar              ║
║             Solusi: evaluate_open_trades() — fungsi baru          ║
║             dipanggil di awal setiap run() sebelum scan           ║
║             Logic: query open trades → cek current price di       ║
║             Gate.io → bandingkan vs TP1/TP2/SL → update result   ║
║             di Supabase jika level sudah tersentuh                ║
║             Result values: "TP1", "TP2", "SL", "EXPIRED"          ║
║             Expired: signal > SIGNAL_EXPIRE_HOURS tanpa hit       ║
║             Jika TP1 hit: result="TP1" (trailing bisa dikembangkan║
║             nanti — untuk sekarang treat sebagai closed)          ║
║                                                                  ║
║  Perbaikan v7.11 (Portfolio Brain + Setup Weight + Bayesian WR):║
║  [v7.11 #1] PORTFOLIO BRAIN — kontrol exposure global           ║
║             MAX_OPEN_TRADES: hard cap jumlah signal aktif        ║
║             MAX_SAME_SIDE_TRADES: max BUY atau SELL sekaligus   ║
║             MAX_BTC_CORR_TRADES: batas pair berkorelasi BTC     ║
║             get_portfolio_state(): query Supabase open signals  ║
║             portfolio_allows(): gate sebelum kirim signal        ║
║             Bot tidak lagi "buta" terhadap exposure total        ║
║  [v7.11 #2] Setup score sebagai core weight (bukan hanya bonus) ║
║             Sebelumnya: setup +1/+2/+3 additive kecil           ║
║             Sekarang: SETUP_WEIGHT_MULTIPLIER per level          ║
║             setup 3 (BOS/CHoCH): ×1.00 (tidak ada penalti)      ║
║             setup 2 (liq_sweep): ×0.85 (diskonto 15%)           ║
║             setup 1 (continuation): ×0.70 (diskonto 30%)        ║
║             Efek: Setup 1 vs Setup 3 beda SIGNIFIKAN di tier     ║
║             TIER_MIN_SCORE disesuaikan ke skala baru (max ~18)   ║
║  [v7.11 #3] Bayesian win rate — gantikan pure lookup bucket     ║
║             Sebelumnya: ratio wins/total per bucket (frequentist)║
║             Masalah: bucket kecil sangat noisy (5/8 = 62.5% WR) ║
║             Sekarang: Beta posterior dengan prior 50% (Jeffrey's)║
║             alpha = wins + prior_alpha                           ║
║             beta  = losses + prior_beta                          ║
║             wr    = alpha / (alpha + beta) → shrinkage otomatis  ║
║             Bucket baru (n=5): (5+1)/(8+2) = 60% (lebih konservatif)║
║             Bucket mature (n=100): mendekati empiris tanpa biasa ║
║             CI 90% ditampilkan agar user tahu ketidakpastian     ║
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
# [v7.8] Group-max scoring — max score = 15.
# Interpretasi tier:
#   S  (12+): ≥3 grup firing kuat + trending, atau semua 4 grup terpenuhi
#   A+ ( 9+): 2 grup kuat + trending, atau 3 grup solid
#   A  ( 6+): minimum viable — 2 grup berbeda firing (misal momentum + volume)
TIER_MIN_SCORE = {
    "S":  12,
    "A+":  9,
    "A":   6,
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
SWING_MAX_SL_PCT    = 0.100   # max 10% dari entry — konsisten dengan check_swing

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

# ── Portfolio Brain Config ── [v7.11 #1] ─────────────
# Kontrol exposure global — bot tidak lagi "buta" terhadap jumlah signal aktif.
# Hanya berlaku untuk sinyal INTRADAY dan SWING (bukan PUMP/MICROCAP).
#
# Filosofi:
#   Signal bagus secara individual tidak berarti bagus secara portofolio.
#   Jika 8 signal BUY terkirim saat BTC sedang di critical zone,
#   exposure menjadi terlalu terkonsentrasi di satu arah.
#
# "Open trade" = sinyal yang sudah dikirim dan belum ada result (NULL).
# Supabase table: signals_v2, kolom: strategy, side, result IS NULL.
MAX_OPEN_TRADES        = 6    # total signal aktif (INTRADAY+SWING) — hard cap
MAX_SAME_SIDE_TRADES   = 4    # max BUY atau SELL aktif sekaligus
MAX_BTC_CORR_TRADES    = 3    # max BUY aktif saat BTC correlation tinggi
                               # (proxy: >4 BUY aktif = terlalu correlated)
PORTFOLIO_STALE_HOURS  = 96   # [fix] signal > 96 jam dianggap stale — cover SWING 72 jam + buffer

# ── Trade Lifecycle Tracking Config ── [v7.12 #3] ────
# evaluate_open_trades() dipanggil di awal setiap run() sebelum scan.
# Query open trades dari Supabase → cek current price → update result.
#
# Result values yang diisi otomatis:
#   "TP1"     — harga menyentuh TP1 (treated as closed, winning trade)
#   "TP2"     — harga menyentuh TP2 (lebih baik dari TP1)
#   "SL"      — harga menyentuh SL (losing trade)
#   "EXPIRED" — sudah > SIGNAL_EXPIRE_HOURS tanpa ada level tersentuh
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
ACCOUNT_EQUITY_USDT  = 200.0  # [v7.14] total modal aktif — update berkala
MAX_LOCKED_USDT      = ACCOUNT_EQUITY_USDT * 0.80  # [v7.19 #A] max modal terkunci di posisi aktif (80% equity)
# [v7.20 #A] Risk-based exposure cap — lebih presisi dari locked-capital cap.
# total_risk = Σ(size × |sl_pct|) per open trade. Limit 6% equity = ~$12 dari $200.
MAX_RISK_USDT        = ACCOUNT_EQUITY_USDT * 0.06
# [v7.20 #D] Portfolio heat cap — max total risk % dari equity lintas semua trade.
# 8% = jika semua trade hit SL bersamaan, max drawdown satu siklus = 8% equity.
MAX_HEAT_PCT         = 8.0
# [v7.20 #B] Correlation penalty scalar saat pair baru berkorelasi tinggi dengan open trades.
CORR_SIZE_PENALTY    = 0.30   # [v7.21 #3] turun dari 0.50 → 0.30 — kurangi penalty korelasi
# [v7.20 #C] Throttle compounding pasca partial TP — cegah over-aggressive sizing.
COMPOUNDING_THROTTLE_PCT = 0.50  # hanya 50% dari equity gain dari TP1 yang dikompound
TARGET_RISK_PCT      = 0.015  # [v7.21 #1] naik dari 0.01 → 0.015 — risk 1.5% per trade
MAX_KELLY_FRACTION   = 0.25   # [v7.14] cap Kelly agar tidak over-bet
# [v7.15 #A] Kelly credibility: fraksi Kelly di-shrink berdasarkan ukuran sampel.
# Rumus: credibility = n / (n + KELLY_CREDIBILITY_K)
# Contoh n=5  → cred=0.20 → kelly_eff = 0.20×kelly + 0.80×prior  (sangat konservatif)
# Contoh n=50 → cred=0.71 → kelly_eff = 0.71×kelly + 0.29×prior  (lebih percaya data)
KELLY_CREDIBILITY_K  = 20    # [v7.16 #A] 20 trades = 50% weight data vs prior
KELLY_MIN_SAMPLES    = 5     # [v7.16 #A] di bawah ini, tolak Kelly → pure tier-fallback

# [v7.16 #A] Dynamic Kelly prior per strategy + regime.
#
# Masalah prior statis (2% flat):
#   MICROCAP trade dengan vol_spike 10x seharusnya punya prior lebih agresif dari
#   SWING trade yang masuk di ranging market — tapi 2% flat meratakan keduanya.
#
# Solusi: lookup table prior per (strategy, regime).
#   prior = f* awal sebelum ada data — mencerminkan ekspektasi edge per konteks.
#
# Filosofi per entry:
#   PUMP + TRENDING    : momen kuat, target 6% dari equity (agresif)
#   INTRADAY + TRENDING: setup normal, 3%
#   INTRADAY + RANGING : choppy, 1.5% (defensif)
#   SWING + TRENDING   : hold lama, lebih besar 4%
#   SWING + RANGING    : swing di ranging = risiko tinggi, 1.5%
#   MICROCAP           : high risk/reward, prior 2.5% — jangan terlalu kecil
#   default (fallback) : 2% — sama seperti v7.15
KELLY_PRIOR_BY_STRATEGY: dict = {
    # (strategy, regime) → fraction dari equity
    ("PUMP",      "TRENDING"): 0.060,
    ("PUMP",      "RANGING"):  0.030,
    ("PUMP",      ""):         0.040,   # tidak ada regime info
    ("INTRADAY",  "TRENDING"): 0.030,
    ("INTRADAY",  "RANGING"):  0.015,
    ("INTRADAY",  ""):         0.020,
    ("SWING",     "TRENDING"): 0.040,
    ("SWING",     "RANGING"):  0.015,
    ("SWING",     ""):         0.025,
    ("MICROCAP",  ""):         0.025,   # microcap tidak punya regime dari ADX
}
KELLY_PRIOR_DEFAULT = 0.020   # fallback jika key tidak ditemukan


def get_kelly_prior(strategy: str, regime: str) -> float:
    """
    [v7.16 #A] Return Kelly prior fraction dinamis berdasarkan strategy + regime.

    Prior mencerminkan ekspektasi edge sebelum ada data historis cukup.
    Lebih agresif untuk setup kuat (PUMP TRENDING), lebih defensif untuk
    setup lemah (SWING RANGING).

    Args:
        strategy : "INTRADAY" | "SWING" | "PUMP" | "MICROCAP"
        regime   : "TRENDING" | "RANGING" | "" (tidak tersedia)

    Returns:
        float: prior fraction (mis. 0.02 = 2% dari equity)
    """
    key = (strategy.upper(), regime.upper() if regime else "")
    # Exact match dulu
    if key in KELLY_PRIOR_BY_STRATEGY:
        return KELLY_PRIOR_BY_STRATEGY[key]
    # Fallback: strategy saja (regime kosong)
    fallback_key = (strategy.upper(), "")
    return KELLY_PRIOR_BY_STRATEGY.get(fallback_key, KELLY_PRIOR_DEFAULT)

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

# ── Altcoin Cluster Correlation ── [v7.16 #C] ────────
# [v7.16] Upgrade: pairwise full matrix + dynamic clustering menggantikan
# cluster statis hardcoded.
#
# Masalah cluster statis (v7.15):
#   1. Hanya 3 cluster (AI, MEME, L2) — pair di luar cluster tidak terdeteksi
#   2. Clustering manual → tidak adaptif saat market structure berubah
#   3. Median return cluster ≠ korelasi nyata antar pair individual
#
# Solusi v7.16:
#   A. Fetch return series (1h + 4h) untuk semua pair kandidat dalam scan cycle
#   B. Bangun pairwise pearson correlation matrix NxN secara runtime
#   C. Dynamic clustering: pair dengan r > CORR_CLUSTER_THRESHOLD saling
#      dimasukkan ke cluster yang sama (union-find / transitive closure sederhana)
#   D. Jika satu pair dalam cluster "dropping" (weighted return < CLUSTER_DROP_BLOCK)
#      → seluruh cluster di-blok, termasuk pair yang belum dicek sebelumnya
#   E. Cluster statis (AI/MEME/L2) tetap ada sebagai seed awal — dijamin terdeteksi
#      bahkan jika pasangan baru belum punya cukup return history
#
# Complexity: O(n²) pair × O(k) candle fetch → dilimit oleh PAIRWISE_MAX_PAIRS
# Cache: matrix di-cache 15 menit agar tidak re-fetch setiap signal
CLUSTER_DROP_BLOCK      = -3.0   # % weighted return → blokir cluster
CORR_BLOCK_THRESHOLD    = 0.75   # pearson r → masuk cluster jika >= ini
CORR_CLUSTER_THRESHOLD  = 0.70   # pearson r → dianggap "satu cluster" (lebih longgar)
CLUSTER_CANDLES_NEEDED  = 12     # [v7.16 #C] naik dari 5 → lebih stabil untuk pearson
CLUSTER_TF_WEIGHTS      = {"1h": 0.4, "4h": 0.6}
PAIRWISE_MAX_PAIRS      = 40     # [v7.16 #C] limit pairs yang masuk matrix — cegah timeout
PAIRWISE_RETURN_WINDOW  = 10     # [v7.16 #C] jumlah candle return yang dibandingkan

# Cluster seed statis — tetap dipertahankan sebagai inisialisasi awal
CLUSTER_PROXIES = {
    "AI":   ("FET_USDT",  ["FET", "TAO", "RENDER", "OCEAN", "AGIX", "NMR"]),
    "MEME": ("DOGE_USDT", ["DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF"]),
    "L2":   ("ARB_USDT",  ["ARB", "OP", "MATIC", "IMX", "STARK", "MANTA"]),
}

_cluster_cache: dict  = {}
_cluster_cache_ts: float = 0.0
CLUSTER_CACHE_TTL = 900       # 15 menit

# [v7.16 #C] Pairwise matrix cache — diisi sekali per cycle oleh build_pairwise_matrix()
# Format: {"PAIR_A": [r1,r2,...], "PAIR_B": [...], ...} (return series per pair)
_pairwise_returns_cache: dict = {}
# Format: {frozenset(pair_a, pair_b): pearson_r}
_pairwise_corr_cache: dict = {}
# Format: set of frozenset — pair yang saling correlated ke cluster blocker
_dynamic_blocked_pairs: set = set()
_pairwise_cache_ts: float = 0.0


def _pearson_corr(a: list[float], b: list[float]) -> float:
    """Hitung pearson correlation antara dua list return. Return 0.0 jika gagal."""
    n = min(len(a), len(b))
    if n < 3:
        return 0.0
    a_t, b_t = a[-n:], b[-n:]
    mean_a = sum(a_t) / n
    mean_b = sum(b_t) / n
    cov = sum((a_t[i] - mean_a) * (b_t[i] - mean_b) for i in range(n))
    std_a = (sum((x - mean_a) ** 2 for x in a_t) / n) ** 0.5
    std_b = (sum((x - mean_b) ** 2 for x in b_t) / n) ** 0.5
    if std_a == 0 or std_b == 0:
        return 0.0
    return round(cov / (n * std_a * std_b), 4)


def _fetch_return_series(client, pair: str, timeframe: str, window: int) -> list[float] | None:
    """
    [v7.16 #C] Fetch return series (% perubahan candle-to-candle) untuk pair tertentu.

    Args:
        client    : Gate.io API client
        pair      : mis. "FET_USDT"
        timeframe : "1h" | "4h"
        window    : jumlah return yang diambil (butuh window+1 candle)

    Returns:
        list[float]: return series length=window, atau None jika data tidak cukup.
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
    [v7.16 #C] Bangun pairwise pearson correlation matrix dari semua candidate pairs.

    Dipanggil sekali di awal run() sebelum scan loop dimulai.
    Hasilnya disimpan di _pairwise_returns_cache dan _pairwise_corr_cache.

    Proses:
      1. Batasi ke PAIRWISE_MAX_PAIRS pair (prioritas: yang termasuk cluster seed dulu)
      2. Fetch return series 1h + 4h per pair
      3. Bangun weighted composite return series (0.4×1h + 0.6×4h)
      4. Hitung pearson r untuk semua pasangan unik
      5. Tandai pair sebagai blocked jika berkorelasi tinggi dengan pair dropping

    Side effects: mengupdate _pairwise_returns_cache, _pairwise_corr_cache,
                  _dynamic_blocked_pairs, _pairwise_cache_ts.
    """
    global _pairwise_returns_cache, _pairwise_corr_cache
    global _dynamic_blocked_pairs, _pairwise_cache_ts

    now = time.time()
    if _pairwise_returns_cache and now - _pairwise_cache_ts < CLUSTER_CACHE_TTL:
        return   # cache masih valid

    # ── Step 1: Pilih pairs ────────────────────────────────────────────────
    # Prioritaskan pair yang termasuk cluster seed agar selalu terwakili
    seed_bases: set[str] = set()
    for _, members in CLUSTER_PROXIES.values():
        seed_bases.update(members)

    seed_pairs   = [p for p in candidate_pairs if p.replace("_USDT", "") in seed_bases]
    other_pairs  = [p for p in candidate_pairs if p not in seed_pairs]
    selected     = (seed_pairs + other_pairs)[:PAIRWISE_MAX_PAIRS]

    log(f"   🔗 Pairwise matrix: {len(selected)} pairs dipilih dari {len(candidate_pairs)} kandidat")

    # ── Step 2 + 3: Fetch return series per pair ───────────────────────────
    returns_map: dict[str, list[float]] = {}
    for pair in selected:
        r1h = _fetch_return_series(client, pair, "1h", PAIRWISE_RETURN_WINDOW)
        r4h = _fetch_return_series(client, pair, "4h", PAIRWISE_RETURN_WINDOW)

        if r1h is None and r4h is None:
            continue

        # Weighted composite: jika salah satu tidak tersedia, pakai yang ada saja
        if r1h is not None and r4h is not None:
            n   = min(len(r1h), len(r4h))
            w1  = CLUSTER_TF_WEIGHTS["1h"]
            w4  = CLUSTER_TF_WEIGHTS["4h"]
            composite = [w1 * r1h[i] + w4 * r4h[i] for i in range(n)]
        elif r1h is not None:
            composite = r1h
        else:
            composite = r4h   # type: ignore[assignment]

        if len(composite) >= 3:
            returns_map[pair] = composite

    _pairwise_returns_cache = returns_map

    # ── Step 4: Hitung pairwise pearson r ─────────────────────────────────
    pairs_list = list(returns_map.keys())
    corr_map: dict = {}
    for i in range(len(pairs_list)):
        for j in range(i + 1, len(pairs_list)):
            pa, pb = pairs_list[i], pairs_list[j]
            r = _pearson_corr(returns_map[pa], returns_map[pb])
            corr_map[frozenset([pa, pb])] = r

    _pairwise_corr_cache = corr_map

    # ── Step 5: Dynamic cluster blocking ─────────────────────────────────
    # Identifikasi pair "dropping" → propagasi block ke pair berkorelasi tinggi
    blocked: set[str] = set()

    # Seed blocking dari cluster statis (tetap menggunakan composite return 1-candle)
    for cluster_name, (proxy_pair, members) in CLUSTER_PROXIES.items():
        proxy_rets_1h = _fetch_return_series(client, proxy_pair, "1h", 2)
        proxy_rets_4h = _fetch_return_series(client, proxy_pair, "4h", 2)
        chg_1h = proxy_rets_1h[-1] if proxy_rets_1h else 0.0
        chg_4h = proxy_rets_4h[-1] if proxy_rets_4h else 0.0
        composite_chg = CLUSTER_TF_WEIGHTS["1h"] * chg_1h + CLUSTER_TF_WEIGHTS["4h"] * chg_4h
        if composite_chg < CLUSTER_DROP_BLOCK:
            for base in members:
                blocked.add(f"{base}_USDT")
            log(f"   🚫 Cluster seed {cluster_name} dropping ({composite_chg:+.2f}%) → blok seed members")

    # Propagasi: pair yang sangat correlated dengan pair dropping → ikut diblok
    changed = True
    while changed:
        changed = False
        for key, r in corr_map.items():
            if r < CORR_CLUSTER_THRESHOLD:
                continue
            pair_set = list(key)
            if len(pair_set) != 2:
                continue
            pa, pb = pair_set[0], pair_set[1]
            if pa in blocked and pb not in blocked:
                blocked.add(pb)
                changed = True
                log(f"   🔗 Dynamic block: {pb} (r={r:.2f} vs {pa})")
            elif pb in blocked and pa not in blocked:
                blocked.add(pa)
                changed = True
                log(f"   🔗 Dynamic block: {pa} (r={r:.2f} vs {pb})")

    _dynamic_blocked_pairs = blocked
    _pairwise_cache_ts     = now
    log(f"   ✅ Pairwise matrix selesai: {len(corr_map)} pasang | {len(blocked)} pair diblokir dinamis")


def get_pairwise_corr(pair_a: str, pair_b: str) -> float:
    """
    [v7.16 #C] Ambil pearson r antara dua pair dari cache matrix.
    Return 0.0 jika pasangan tidak ada di cache.
    """
    return _pairwise_corr_cache.get(frozenset([pair_a, pair_b]), 0.0)


def _calc_cluster_median_return(client, members: list[str], timeframe: str = "1h") -> float | None:
    """
    Retained untuk kompatibilitas fungsi lama yang masih mungkin dipanggil.
    v7.16: diprioritaskan pakai return series dari _pairwise_returns_cache jika tersedia.
    """
    returns = []
    for base in members:
        pair = f"{base}_USDT"
        # Gunakan data cache pairwise jika ada (lebih efisien, tidak double-fetch)
        cached_rets = _pairwise_returns_cache.get(pair)
        if cached_rets is not None:
            returns.append(cached_rets[-1])   # return terakhir sebagai proxy 1-candle
            continue
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
PARTIAL_TP1_RATIO   = 0.50    # fallback statis jika calc_partial_ratio() tidak bisa dipanggil
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

# ── Order Book Spread Cache ── [v7.16 #B] ─────────────
# Cache spread live per pair agar tidak re-fetch setiap pemanggilan slippage.
# Format: {"PAIR_USDT": (spread_fraction, timestamp)}
_ob_spread_cache: dict = {}
OB_SPREAD_CACHE_TTL = 300   # 5 menit — spread relatif stabil di luar event besar

# Level depth order book untuk estimasi spread & market impact
OB_DEPTH_LEVEL  = 20   # ambil 20 level bid/ask — mencukupi untuk estimasi impact


def get_live_spread(client, pair: str) -> float | None:
    """
    [v7.16 #B] Ambil spread bid-ask live dari order book Gate.io.

    Spread = (best_ask - best_bid) / mid_price

    Cache 5 menit — spread tidak berubah cepat kecuali saat flash event.
    Return None jika fetch gagal (caller akan fallback ke SLIPPAGE_BASE_PCT).

    Args:
        client : Gate.io API client
        pair   : mis. "FET_USDT"

    Returns:
        float | None: spread fraction (mis. 0.0015 = 0.15%), atau None jika gagal.
    """
    global _ob_spread_cache
    now = time.time()
    cached = _ob_spread_cache.get(pair)
    if cached and now - cached[1] < OB_SPREAD_CACHE_TTL:
        return cached[0]

    try:
        ob = gate_call_with_retry(client.list_order_book, currency_pair=pair, limit=OB_DEPTH_LEVEL)
        if ob is None or not ob.bids or not ob.asks:
            return None

        best_bid = float(ob.bids[0][0])
        best_ask = float(ob.asks[0][0])
        if best_bid <= 0 or best_ask <= 0:
            return None

        mid   = (best_bid + best_ask) / 2.0
        spread = (best_ask - best_bid) / mid
        spread = round(max(0.0, min(spread, SLIPPAGE_MAX_PCT)), 6)

        _ob_spread_cache[pair] = (spread, now)
        return spread
    except Exception as e:
        log(f"   ⚠️ get_live_spread({pair}): {e} — fallback ke baseline", "warn")
        return None


def get_ob_depth_impact(client, pair: str, size_usdt: float, side: str) -> float:
    """
    [v7.16 #B] Estimasi market impact dari order ukuran size_usdt berdasarkan
    depth order book aktual.

    Simulasi: telusuri level bid/ask sampai size_usdt terpenuhi,
    hitung rata-rata fill price vs best price.

    Args:
        client    : Gate.io API client
        pair      : mis. "ETH_USDT"
        size_usdt : ukuran order dalam USDT
        side      : "BUY" (telan ask) | "SELL" (telan bid)

    Returns:
        float: market impact fraction (tambahan di atas spread).
               0.0 jika order kecil atau data tidak tersedia.
    """
    try:
        ob = gate_call_with_retry(client.list_order_book, currency_pair=pair, limit=OB_DEPTH_LEVEL)
        if ob is None:
            return 0.0

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
            val = px * qty   # nilai level ini dalam base currency × price ≈ USDT kasar

            take = min(remaining, val)
            weighted_px += px * (take / size_usdt)
            filled      += take
            remaining   -= take
            if remaining <= 0:
                break

        if filled < size_usdt * 0.5:
            # Tidak cukup depth — order akan sangat slippy, pakai batas max
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
      v7.15 → hanya SLIPPAGE_BASE_PCT (flat) + volume_ratio (kasar)
      v7.16 → spread live dari OB + depth impact aktual dari OB

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

    if client is not None and pair is not None:
        live_spread  = get_live_spread(client, pair)
        depth_impact = get_ob_depth_impact(client, pair, size_usdt, side)

    slip = calc_slippage(side, size_usdt, avg_volume_usd,
                         live_spread=live_spread, depth_impact=depth_impact)

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
    # Group TREND
    "trend":        3,

    # Group MOMENTUM
    "momentum":     3,

    # Group LIQUIDITY — nilai adalah ceiling per sinyal, ambil max
    "liq_sweep":    4,   # paling kuat: smart money sudah bergerak
    "order_block":  3,   # zona institusional confirmed
    "pullback":     2,   # entry di level kunci, sinyal paling lemah di group ini

    # Group VOLUME
    "vol_confirm":  3,

    # Regime modifier (additive — bukan group, murni konteks pasar)
    "adx_trend":    2,   # ADX >= 25: pasar trending kuat
    "adx_ranging": -2,   # ADX 18-25: pasar ranging (CHOPPY sudah diblok sebelumnya)
}

# ── [v7.11 #2] Setup Weight Multiplier ───────────────
#
# Masalah sebelumnya: setup_score hanya additive kecil (+1/+2/+3).
# Ini berarti Setup 1 (continuation) vs Setup 3 (BOS/CHoCH) hanya beda 2 poin —
# tidak cukup signifikan untuk memengaruhi tier di skala score 6–18.
#
# Solusi: multiplier terhadap BASE score (sebelum ditambah setup contribution).
# Setup kualitas rendah mengecilkan seluruh skor kelompok indikator,
# bukan hanya mengurangi bonus beberapa poin.
#
# Hierarki:
#   setup_score 3 (BOS/CHoCH terkonfirmasi): ×1.00 — tidak ada diskonto
#   setup_score 2 (liq_sweep saja)          : ×0.85 — diskonto 15%
#   setup_score 1 (continuation bias only)  : ×0.70 — diskonto 30%
#
# Contoh nyata (base score = 11, trending):
#   setup 3 → 11 × 1.00 + 3 = 14 → Tier S
#   setup 2 → 11 × 0.85 + 2 = 11 → Tier A+
#   setup 1 → 11 × 0.70 + 1 =  9 → Tier A+/batas bawah
#
# Efek: tier S hampir tidak mungkin dicapai dari continuation bias saja.
SETUP_WEIGHT_MULTIPLIER = {
    3: 1.00,   # BOS/CHoCH — full confirmation, tidak ada diskonto
    2: 0.85,   # liq_sweep — institutional entry, minor discount
    1: 0.70,   # continuation bias — lowest quality, significant discount
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
                 vwap: float, ob_ratio: float,
                 regime: str = "TRENDING",
                 setup_score: int = 3) -> int:
    """
    Group-max scoring engine (v7.10 — soft setup gate + regime multiplier).

    [v7.10 #1] setup_score (1–3) dimasukkan sebagai contrib langsung ke total.
    Menggantikan hard gate binary BOS/CHoCH — sekarang kualitas setup
    gradual, bukan all-or-nothing:
      setup_score 3 (BOS/CHoCH)   → +3 ke total  (full confirmation)
      setup_score 2 (liq_sweep)   → +2 ke total  (entry level kuat)
      setup_score 1 (continuation) → +1 ke total  (soft, continuation)
      Caller wajib pastikan setup_score >= 1 sebelum memanggil fungsi ini.

    Max score baru: 3 (trend) + 3 (momentum) + 5 (liq×1.3) + 4 (vol×1.2)
                    + 3 (setup) + 2 (ADX) ≈ 20 (teoritis RANGING penuh)
    Practical max TRENDING: 3+3+4+3+3+2 = 18

    Prasyarat (divalidasi SEBELUM fungsi ini):
    - setup_score >= 1 (via detect_setup_quality di check_*)
    - RSI batas ekstrem (di check_* sebelum scoring)
    - ADX CHOPPY → return None di check_* sebelum scoring
    - Late-entry → return None di check_* sebelum scoring
    """
    is_bull = (side == "BUY")

    # ── [v7.9 #2] Per-regime group weight multiplier ──────────────
    # Masalah: grouping rigid — semua market treat all group identik.
    # Contoh: breakout tanpa pullback → liquidity group = 0,
    #         tapi trade tetap valid di TRENDING yang kuat.
    #         Di RANGING: volume & liquidity lebih predictive dari EMA.
    #
    # Solusi: multiplier per group, per regime.
    # TRENDING (baseline): semua group equal weight.
    # RANGING: trend group dikurangi (EMA lag di sideways),
    #          liquidity & volume dinaikkan (lebih reliable di ranging).
    #
    # Multiplier tidak ubah max group score secara langsung —
    # hanya menggeser kontribusi aktual group ke total score.
    # Score tetap floor 0 per group (tidak bisa negatif).
    #
    # Referensi multiplier:
    #   TRENDING : trend=1.0, momentum=1.0, liq=1.0, vol=1.0
    #   RANGING  : trend=0.7, momentum=0.8, liq=1.3, vol=1.2
    if regime == "TRENDING":
        _mw = {"trend": 1.0, "momentum": 1.0, "liq": 1.0, "vol": 1.0}
    elif regime == "RANGING":
        _mw = {"trend": 0.7, "momentum": 0.8, "liq": 1.3, "vol": 1.2}
    else:
        # CHOPPY harusnya sudah diblok sebelum fungsi ini — fallback safe
        _mw = {"trend": 1.0, "momentum": 1.0, "liq": 1.0, "vol": 1.0}

    # ── Group TREND ───────────────────────────────────────────────
    # Pertanyaan: apakah entry searah trend struktur price?
    # Indikator: EMA fast vs slow.
    # (VWAP dihapus dari scoring — overlap dengan EMA, noise lebih banyak)
    g_trend = 0
    if is_bull and ema_fast > ema_slow:      g_trend = GROUPS["trend"]
    if not is_bull and ema_fast < ema_slow:  g_trend = GROUPS["trend"]
    g_trend = round(g_trend * _mw["trend"])

    # ── Group MOMENTUM ────────────────────────────────────────────
    # Pertanyaan: apakah momentum dikonfirmasi?
    # Indikator: MACD crossover.
    # (RSI tidak di sini — RSI sudah jadi hard gate di check_*, bukan scoring)
    g_momentum = 0
    if is_bull and macd > msig:      g_momentum = GROUPS["momentum"]
    if not is_bull and macd < msig:  g_momentum = GROUPS["momentum"]
    g_momentum = round(g_momentum * _mw["momentum"])

    # ── Group LIQUIDITY ───────────────────────────────────────────
    # Pertanyaan: apakah ada institutional presence di level ini?
    # Indikator (berurutan kekuatan): liq_sweep > order_block > pullback.
    # Ambil MAX — jika sweep ada, OB dan pullback tidak ditambahkan.
    # Ini mencegah triple-dip dari 3 sinyal yang mengukur hal yang sama.
    #
    # [v7.9] Di RANGING: liquidity zone lebih reliable (mean-reversion).
    # Di TRENDING: breakout tanpa pullback (liq=0) tetap valid —
    #              group lain (trend+momentum) yang carry weight.
    liq_candidates: list[int] = []

    if is_bull and liq.get("sweep_bull"):      liq_candidates.append(GROUPS["liq_sweep"])
    if not is_bull and liq.get("sweep_bear"):  liq_candidates.append(GROUPS["liq_sweep"])

    if ob.get("valid"):                         liq_candidates.append(GROUPS["order_block"])

    if is_bull:
        last_sl = structure.get("last_sl")
        # [v7.3] lower bound: cegah breakdown di bawah swing low dianggap pullback
        if last_sl and last_sl <= price <= last_sl * 1.015:
            liq_candidates.append(GROUPS["pullback"])
    else:
        last_sh = structure.get("last_sh")
        # upper bound: price harus dekat resistance, bukan jauh di atasnya
        if last_sh and last_sh * 0.97 <= price <= last_sh * 1.01:
            liq_candidates.append(GROUPS["pullback"])

    g_liquidity = round((max(liq_candidates) if liq_candidates else 0) * _mw["liq"])

    # ── Group VOLUME ──────────────────────────────────────────────
    # Pertanyaan: apakah pergerakan dibackup kekuatan pasar nyata?
    # Spike > 1.3× rata-rata 10 candle sebelumnya.
    # [v7.9] Di RANGING: volume spike lebih predictive (menandai breakout keluar range).
    g_volume = 0
    vol_avg = float(np.mean(volumes[-10:-1]))
    if float(volumes[-1]) > vol_avg * 1.3:
        g_volume = GROUPS["vol_confirm"]
    g_volume = round(g_volume * _mw["vol"])

    # ── Sum independent groups ────────────────────────────────────
    base_score = g_trend + g_momentum + g_liquidity + g_volume

    # ── [v7.11 #2] Setup weight multiplier — setup sebagai core weight ──
    # Sebelumnya setup_score hanya additive (+1/+2/+3) — terlalu kecil dampaknya.
    # Sekarang: multiplier terhadap base score (sebelum regime modifier).
    # Setup 1 (continuation) mendapat diskonto 30% dari seluruh base score,
    # membuat perbedaan antara Setup 1 vs Setup 3 menjadi substantif secara tier.
    setup_mult = SETUP_WEIGHT_MULTIPLIER.get(setup_score, 1.0)
    score = round(base_score * setup_mult)

    # ── Regime modifier (additive — bukan grup, murni konteks) ────
    # CHOPPY sudah diblokir di check_intraday/check_swing sebelum fungsi ini
    if regime == "TRENDING":   score += GROUPS["adx_trend"]    # trend kuat → lebih valid
    elif regime == "RANGING":  score += GROUPS["adx_ranging"]  # ranging → lebih berisiko

    # ── [v7.10 #1] Setup quality contribution ────────────────────
    # Bonus additive kecil — bukan pengganti multiplier di atas.
    # setup_score 3 dapat +3, setup_score 1 dapat +1.
    # Dikombinasikan dengan multiplier: Setup 3 mendapat (full base × 1.0) + 3,
    # Setup 1 mendapat (discounted base × 0.7) + 1 — gap yang jauh lebih besar.
    score += setup_score   # +1 / +2 / +3

    return score


def assign_tier(score: int) -> str:
    if score >= TIER_MIN_SCORE["S"]:  return "S"
    if score >= TIER_MIN_SCORE["A+"]: return "A+"
    if score >= TIER_MIN_SCORE["A"]:  return "A"
    return "SKIP"


def calc_conviction(score: int) -> str:
    """
    Diferensiasi kualitas sinyal di dalam tier.
    [v7.10] Skala disesuaikan — max score praktis ~18.
    Mapping terhadap kombinasi grup:
      >= 16 : EXTREME  — setup penuh + semua grup + trending
      >= 13 : VERY HIGH — BOS/CHoCH + 3 grup kuat + trending
      >= 10 : HIGH     — setup solid + 2 grup kuat + trending
      >= 7  : GOOD     — 2 grup firing + setup ok
      < 7   : OK       — minimum tier A (continuation + momentum)
    """
    if score >= 16: return "EXTREME ⚡"
    if score >= 13: return "VERY HIGH 🔥"
    if score >= 10: return "HIGH 💪"
    if score >= 7:  return "GOOD ✅"
    return "OK 🟡"


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
        (score: int, tier: str, conviction: str)
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
        ob_ratio=1.0,        # tidak digunakan oleh score_signal v7.8+
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
    lookup tabel statis _SIGNAL_PRIORITY.

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
    "6":    15,   # banyak sample — paling longgar
    "7-8":  20,   # baseline sebelumnya
    "9-11": 25,   # perlu lebih hati-hati
    "12+":  30,   # bucket paling langka — paling ketat
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


def get_score_bucket(score: int) -> str:
    """
    Kelompokkan score ke bucket untuk aggregasi historis.

    Bucket tidak terlalu granular agar setiap bucket punya cukup sample.
    4 bucket mencerminkan tier natural: S / A+ / A_atas / A_bawah.
    """
    if score >= 12: return "12+"
    if score >= 9:  return "9-11"
    if score >= 7:  return "7-8"
    return "6"


def get_context_bucket(score: int, regime: str = "") -> str:
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
        WIN_VALUES  = {"WIN", "TP1", "TP2"}
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


def estimate_confidence(score: int, regime: str = "") -> dict:
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
) -> float:
    """
    [v7.16 #A #D] Volatility-adjusted + Kelly-informed position sizing.

    Upgrade dari v7.15:
      #A — Prior dinamis per (strategy, regime) via get_kelly_prior()
      #D — Equity aktif dari closed-loop PnL tracking, bukan konstanta statis

    Tiga lapis kalkulasi (dengan fallback aman ke setiap lapis sebelumnya):

    Lapis 1 — Kelly sizing (jika data WR reliable):
        f* = (wr × rr - (1 - wr)) / rr   ← Half-Kelly dipakai: f*/2
        Blend dengan prior dinamis per strategy+regime (bukan 2% flat)
        kelly_size = effective_equity × f_blended

    Lapis 2 — Volatility scalar (jika ATR & entry tersedia):
        atr_pct    = atr / entry
        vol_scalar = TARGET_RISK_PCT / atr_pct
        vol_size   = effective_equity × vol_scalar
        Size akhir = min(kelly_size, vol_size)

    Lapis 3 — Tier cap & drawdown multiplier (selalu berlaku):
        tier cap : S → ≤1.5×BASE, A+ → ≤1.2×BASE, A → ≤1.0×BASE
        dd_mult  : normal→1.0  warn→0.7  halt→0.4

    Args:
        tier           : "S" | "A+" | "A"
        conf           : output dari estimate_confidence()
        drawdown_mode  : "normal" | "warn" | "halt"
        atr            : ATR nilai absolut pair (optional, untuk vol-adjust)
        entry          : harga entry (optional, untuk vol-adjust)
        rr             : reward-to-risk ratio trade ini (default 2.0)
        strategy       : [v7.16 #A] "INTRADAY"|"SWING"|"PUMP"|"MICROCAP" untuk prior dinamis
        regime         : [v7.16 #A] "TRENDING"|"RANGING"|"" untuk prior dinamis
        current_equity : [v7.16 #D] equity aktif dalam USDT; jika None pakai ACCOUNT_EQUITY_USDT

    Returns:
        float: position size dalam USDT, sudah di-cap dan di-floor.
    """
    # [v7.16 #D] Gunakan equity aktif jika tersedia, fallback ke konstanta
    effective_equity = current_equity if (current_equity and current_equity > MIN_POSITION_USDT) \
                       else ACCOUNT_EQUITY_USDT

    # [v7.20 #C] Compounding throttle — jika ada partial TP gain belum settled,
    # blend effective_equity dengan pre-partial baseline agar sizing tidak loncat agresif.
    # Hanya aktif jika: pre_partial_equity tersimpan DAN lebih kecil dari effective_equity.
    _pre = _equity_cache.get("pre_partial_equity")
    if _pre is not None and _pre > MIN_POSITION_USDT and effective_equity > _pre:
        _gain         = effective_equity - _pre
        _throttled_eq = _pre + _gain * COMPOUNDING_THROTTLE_PCT
        effective_equity = _throttled_eq  # pakai blended, bukan full equity baru

    tier_mult = TIER_SIZE_MULT.get(tier, 1.0)
    dd_mult   = {"normal": 1.0, "warn": 0.7, "halt": 0.4}.get(drawdown_mode, 1.0)
    tier_cap  = BASE_POSITION_USDT * tier_mult

    method   = "fallback"
    raw_size = BASE_POSITION_USDT * tier_mult   # default fallback

    # ── Lapis 1: Kelly fraction dengan credibility shrinkage ─────────────
    # [v7.16 #A] Prior sekarang dinamis per (strategy, regime)
    kelly_size = None
    if conf.get("reliable") and conf.get("wr") is not None:
        wr        = conf["wr"]
        n_samples = conf.get("n", 0)
        if rr > 0 and wr > 0 and n_samples >= KELLY_MIN_SAMPLES:
            f_star = (wr * rr - (1.0 - wr)) / rr
            f_star = max(0.0, min(MAX_KELLY_FRACTION, f_star))
            half_kelly = f_star / 2.0

            # [v7.16 #A] Dynamic prior per strategy+regime — bukan 2% flat
            dynamic_prior = get_kelly_prior(strategy, regime)

            cred       = n_samples / (n_samples + KELLY_CREDIBILITY_K)
            f_blended  = cred * half_kelly + (1.0 - cred) * dynamic_prior
            # [v7.16 #D] Pakai effective_equity (live), bukan ACCOUNT_EQUITY_USDT statis
            kelly_size = effective_equity * f_blended
            method     = f"kelly(n={n_samples},cred={cred:.2f},prior={dynamic_prior:.3f})"
            raw_size   = kelly_size

    # ── Lapis 2: Volatility scalar ────────────────────────────────────────
    if atr is not None and entry is not None and entry > 0 and atr > 0:
        atr_pct  = atr / entry
        if atr_pct > 0:
            vol_scalar = TARGET_RISK_PCT / atr_pct
            # [v7.16 #D] Pakai effective_equity di sini juga
            vol_size   = effective_equity * vol_scalar
            if kelly_size is not None:
                raw_size = min(kelly_size, vol_size)
                method = "kelly+vol"
            else:
                raw_size = min(BASE_POSITION_USDT * tier_mult, vol_size)
                method = "vol-adj"

    # ── Tier cap sebagai guardrail atas ───────────────────────────────────
    raw_size = min(raw_size, tier_cap)

    # ── Drawdown penalty ──────────────────────────────────────────────────
    raw_size = raw_size * dd_mult

    # ── [v7.20 #B] Correlation-adjusted sizing ───────────────────────────
    # Jika pair baru berkorelasi tinggi dengan open trades, kurangi size
    # agar real portfolio exposure tidak meledak karena co-movement.
    # corr_scalar = 1 - (max_corr × CORR_SIZE_PENALTY)
    # corr=0.85, penalty=0.5 → scalar=0.575 → size dikurangi 42.5%
    corr_scalar = 1.0
    if pair and open_pairs:
        try:
            max_corr = max(
                (abs(get_pairwise_corr(pair, op)) for op in open_pairs if op != pair),
                default=0.0,
            )
            if max_corr > 0.5:   # threshold — korelasi rendah tidak dipenalti
                corr_scalar = max(0.4, 1.0 - max_corr * CORR_SIZE_PENALTY)
                raw_size   *= corr_scalar
                method     += f"+corr({max_corr:.2f}×{corr_scalar:.2f})"
        except Exception:
            pass   # jika corr lookup gagal → no penalty, aman

    # ── Floor & ceiling final ─────────────────────────────────────────────
    size = max(MIN_POSITION_USDT, min(MAX_POSITION_USDT, round(raw_size, 2)))

    log(f"   💰 Position size: ${size} USDT "
        f"[{method}] tier={tier}×{tier_mult} dd×{dd_mult} equity=${effective_equity:.0f}"
        + (f" atr_pct={atr/entry*100:.2f}%" if atr and entry else "")
        + (f" corr_scalar={corr_scalar:.2f}" if corr_scalar < 1.0 else ""))
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


def _calc_equity_drawdown() -> float:
    """
    [v7.14 #B] Hitung equity drawdown dari peak PnL kumulatif.

    Logic:
      1. Ambil semua closed signals dengan kolom pnl_usdt (bisa NULL)
      2. Hitung cumulative PnL → running peak
      3. current_dd = (peak - current_equity) / peak  jika peak > 0

    Returns:
        float: drawdown fraction (0.0 = no drawdown, 0.15 = 15% dari peak)
        Jika tidak ada data PnL, return 0.0 (fail-safe — jangan halt tanpa data).
    """
    try:
        rows = (
            supabase.table("signals_v2")
            .select("pnl_usdt, sent_at")
            .not_.is_("result", "null")
            .neq("result", "EXPIRED")
            .order("sent_at", desc=False)
            .limit(200)
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"⚠️ _calc_equity_drawdown: query gagal — {e}. Asumsikan dd=0.", "warn")
        return 0.0

    if not rows:
        return 0.0

    # [v7.22 #A] Fix: peak dihitung dari equity ABSOLUT (base + cumPnL),
    # bukan dari cumulative PnL saja.
    # Bug sebelumnya: peak mulai dari 0 → Peak=1.21 USDT (hanya PnL tertinggi)
    # → DD = (1.21 - (-0.08)) / 1.21 = 106.8% ← nonsense, bot stuck HALT selamanya.
    # Fix: peak mulai dari ACCOUNT_EQUITY_USDT → Peak = $200 + cumPnL_tertinggi.
    # DD = (peak_equity - current_equity) / peak_equity → angka realistis.
    #
    # [v7.22 #B] Persistent peak: load high-watermark dari DB sebagai starting point.
    # Ini mencegah edge case cold start dimana semua trade sudah close, PnL tipis negatif,
    # bot restart → tanpa persistence peak = equity sekarang ($199) → DD = 0% (salah).
    # Dengan persistence: peak = max yang pernah tersimpan (misal $201.21) → DD akurat.
    cumulative = 0.0
    peak       = _load_peak_equity_from_db()   # [v7.22 #B] persistent high-watermark
    # Safety guard: peak tidak pernah di bawah modal awal (double-check)
    if peak < ACCOUNT_EQUITY_USDT:
        peak = ACCOUNT_EQUITY_USDT
    for row in rows:
        pnl = row.get("pnl_usdt") or 0.0
        try:
            pnl = float(pnl)
        except (TypeError, ValueError):
            pnl = 0.0
        cumulative += pnl
        equity_now = ACCOUNT_EQUITY_USDT + cumulative
        # Safety guard: peak hanya boleh naik, tidak pernah turun
        if equity_now > peak:
            peak = equity_now

    current_equity = ACCOUNT_EQUITY_USDT + cumulative

    # Safety guard final: pastikan peak >= ACCOUNT_EQUITY_USDT dalam kondisi apapun
    if peak < ACCOUNT_EQUITY_USDT:
        peak = ACCOUNT_EQUITY_USDT

    dd_frac = (peak - current_equity) / peak
    return max(0.0, round(dd_frac, 4))


# ── Equity Cache ── [v7.16 #D] ───────────────────────
_equity_cache: dict = {"value": None, "available": None, "locked": 0.0, "ts": 0.0, "pre_partial_equity": None}
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
    [v7.14 #B] Dual-track drawdown: streak + equity drawdown dari peak.

    Dua metrik dihitung independen, mode ditentukan oleh yang lebih parah:
      - streak: consecutive SL/LOSS terbaru (cepat, tapi bisa misleading)
      - equity dd: % turun dari peak PnL kumulatif (akurat untuk capital protection)

    Cache: 30 menit.

    Returns:
        {"streak": int, "mode": "normal"|"warn"|"halt", "dd_pct": float, "cached": bool}
    """
    global _drawdown_state

    # ── Streak kalkulasi ──────────────────────────────────────────────────
    try:
        rows = (
            supabase.table("signals_v2")
            .select("result")
            .not_.is_("result", "null")
            .neq("result", "EXPIRED")
            .order("sent_at", desc=True)
            .limit(20)
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"⚠️ get_drawdown_state: query gagal — {e}. Pakai state lama.", "warn")
        return _drawdown_state

    WIN_VALUES  = {"WIN", "TP1", "TP2"}
    LOSS_VALUES = {"LOSS", "SL"}

    streak = 0
    for row in rows:
        result = (row.get("result") or "").upper()
        if result in LOSS_VALUES:
            streak += 1
        elif result in WIN_VALUES:
            break

    # ── Equity drawdown kalkulasi ─────────────────────────────────────────
    dd_pct = _calc_equity_drawdown()

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

    SEVERITY = {"normal": 0, "warn": 1, "halt": 2}
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

    return _drawdown_state


# ════════════════════════════════════════════════════════
#  ALTCOIN CLUSTER CORRELATION — [v7.13 #5]
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
    """Identifikasi cluster seed dari nama pair. Return: "AI" | "MEME" | "L2" | None"""
    base = pair.replace("_USDT", "").upper()
    for cluster_name, (_proxy, members) in CLUSTER_PROXIES.items():
        if base in members:
            return cluster_name
    return None


def is_cluster_blocked(pair: str, cluster_regimes: dict) -> bool:
    """
    [v7.16 #C] Return True jika pair diblokir oleh sistem correlation.

    Dua jalur blocking (OR logic — salah satu cukup untuk blokir):
      1. Dynamic block: pair ada di _dynamic_blocked_pairs (pairwise matrix)
         → lebih komprehensif, mencakup pair di luar cluster seed
      2. Seed block: pair dari cluster seed yang composite return < threshold
         → fallback jika matrix belum dibangun (mis. awal cycle)
    """
    # Jalur 1: Dynamic pairwise block (diprioritaskan)
    if pair in _dynamic_blocked_pairs:
        log(f"   🚫 Dynamic pairwise block: {pair}")
        return True

    # Jalur 2: Cluster seed block (backward compatible)
    cluster = get_pair_cluster(pair)
    if cluster is not None:
        chg = cluster_regimes.get(cluster, 0.0)
        if chg < CLUSTER_DROP_BLOCK:
            log(f"   🚫 Seed cluster block: {pair} → {cluster} drop {chg:+.1f}%")
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
        # [v7.10 #1] Soft gate — gantikan binary has_struct
        # setup_score 0 = hard skip, 1-3 = lolos dengan kualitas berbeda
        setup_score = detect_setup_quality("BUY", structure, liq, ema20, ema50)
        if setup_score == 0: return None    # tidak ada sinyal setup apapun — skip
        if rsi > 72:         return None    # Gate: tidak overbought ekstrem

        ob    = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=25)
        score = score_signal("BUY", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema20, ema50, vwap, ob_ratio, mkt["regime"],
                             setup_score=setup_score)
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        last_sh = structure.get("last_sh")
        if last_sh and price > last_sh * 1.02: return None  # late entry filter
        entry = round(last_sh * 1.002, 8) if (last_sh and price > last_sh) else price

    else:  # SELL
        # [v7.10 #1] Soft gate SELL
        setup_score = detect_setup_quality("SELL", structure, liq, ema20, ema50)
        if setup_score == 0: return None    # tidak ada sinyal setup apapun — skip
        if rsi < 22:         return None    # Gate: tidak oversold ekstrem

        last_sh = structure.get("last_sh")
        if last_sh and price < last_sh * 0.97: return None   # sudah terlalu jauh turun

        ob    = detect_order_block(closes, highs, lows, volumes, side="SELL", lookback=25)
        score = score_signal("SELL", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema20, ema50, vwap, ob_ratio, mkt["regime"],
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
        # [v7.10 #1] Soft gate BUY — gantikan binary has_struct
        setup_score = detect_setup_quality("BUY", structure, liq, ema50, ema200)
        if setup_score == 0: return None
        if rsi > 68:         return None   # proteksi buy the top

        ob    = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=40)
        score = score_signal("BUY", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema50, ema200, vwap, ob_ratio, mkt["regime"],
                             setup_score=setup_score)
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
        # [v7.10 #1] Soft gate SELL
        setup_score = detect_setup_quality("SELL", structure, liq, ema50, ema200)
        if setup_score == 0: return None
        if rsi < 28:         return None   # proteksi oversold ekstrem

        last_sh = structure.get("last_sh")
        if last_sh and price < last_sh * 0.97: return None

        ob    = detect_order_block(closes, highs, lows, volumes, side="SELL", lookback=40)
        score = score_signal("SELL", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema50, ema200, vwap, ob_ratio, mkt["regime"],
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
    }


# ════════════════════════════════════════════════════════
#  CONFLICT RESOLUTION — [v7.10 #2]
#
#  Masalah: dalam satu cycle, pair yang sama bisa menghasilkan:
#    - INTRADAY SELL (1h bearish structure)
#    - PUMP BUY (15m spike) — dari scanner terpisah
#  Keduanya terkirim tanpa filter → sinyal kontradiktif di Telegram.
#
#  Solusi: priority system — satu pair, satu arah per cycle.
#  Signal prioritas rendah di-drop sebelum pengiriman.
#
#  Priority order (0 = tertinggi):
#    0  PUMP        — volume anomali mendominasi semua sinyal arah
#    1  INTRADAY BUY  — timeframe pendek, momentum bullish
#    2  SWING BUY     — timeframe panjang, tren bullish
#    3  SWING SELL    — timeframe panjang, tren bearish
#    4  INTRADAY SELL — paling mudah false positive, prioritas terendah
#
#  Override logic:
#    Jika pair X sudah punya PUMP BUY → INTRADAY SELL pair X di-drop
#    Jika pair X punya INTRADAY BUY + SWING SELL → SWING SELL di-drop
#    dst.
#
#  Semua drop dicatat di log untuk audit trail.
# ════════════════════════════════════════════════════════

# Priority map: key = "STRATEGY_SIDE" → lower = higher priority
_SIGNAL_PRIORITY: dict = {
    "PUMP_BUY":      0,
    "INTRADAY_BUY":  1,
    "SWING_BUY":     2,
    "SWING_SELL":    3,
    "INTRADAY_SELL": 4,
}


def resolve_conflicts(signals: list) -> list:
    """
    [v7.10 #2] Filter sinyal kontradiktif — satu pair, satu arah per cycle.

    Untuk setiap pair, hanya signal dengan prioritas tertinggi yang dipertahankan.
    Signal lain dalam pair yang sama di-drop dan dicatat di log.

    Args:
        signals: list sinyal kandidat (belum disort, belum difilter)

    Returns:
        list sinyal bersih — maksimum satu signal per pair
    """
    best: dict   = {}   # pair → (priority, signal)
    dropped: list = []

    for sig in signals:
        pair  = sig["pair"]
        strat = sig["strategy"]
        side  = sig["side"]
        key   = f"{strat}_{side}"
        prio  = _SIGNAL_PRIORITY.get(key, 99)

        if pair not in best:
            best[pair] = (prio, sig)
        else:
            existing_prio, existing_sig = best[pair]
            if prio < existing_prio:
                # Signal baru menang — existing didrop
                dropped.append(
                    f"{pair}: [{existing_sig['strategy']} {existing_sig['side']}] "
                    f"→ kalah vs [{strat} {side}]"
                )
                best[pair] = (prio, sig)
            else:
                # Existing menang — signal baru didrop
                dropped.append(
                    f"{pair}: [{strat} {side}] "
                    f"→ kalah vs [{existing_sig['strategy']} {existing_sig['side']}]"
                )

    if dropped:
        log(f"⚔️ Conflict resolution — {len(dropped)} signal di-drop:")
        for d in dropped:
            log(f"   {d}")

    return [sig for _, sig in best.values()]



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
        f"Score:      {score}/15 | RSI: {rsi}\n"
        f"Struct:     {bos}\n"
        f"Regime:     {regime_emoji} {regime} (ADX: {adx})\n"
        f"Hist WR:    {conf['label']}{' 🎯' if conf.get('ctx_used') else ''}\n"
        f"Conviction: <b>{conviction}</b>\n"
        f"💰 Pos.Size : <b>${position_size:.2f} USDT</b> <i>(tier-adjusted)</i>\n"
        f"<i>⚠️ Pasang SL wajib. Ini signal, bukan rekomendasi finansial.</i>"
    )
    tg(msg)
    log(f"  ✅ SIGNAL {tier} {strategy} {side} {pair} RR:1:{rr} Score:{score} Size:${position_size}")


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
        f"Score  : {score}/18 | Conviction: <b>{sig.get('conviction', '—')}</b>\n"
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
        # [v7.20 #D] Portfolio heat = total_risk / equity × 100
        locked_usdt     = 0.0
        total_risk_usdt = 0.0
        open_pairs      = []   # [v7.20 #B] list pair aktif untuk corr lookup
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
                    # Partial trade: risk hanya dari sisa size
                    trade_risk  = pos_size * sl_dist_pct
                else:
                    trade_risk  = pos_size * TARGET_RISK_PCT  # fallback 1%
                total_risk_usdt += trade_risk

                pair_p = r.get("pair", "")
                if pair_p:
                    open_pairs.append(pair_p)
            except (TypeError, ValueError):
                locked_usdt     += BASE_POSITION_USDT
                total_risk_usdt += BASE_POSITION_USDT * TARGET_RISK_PCT

        # Portfolio heat % = total risk / current equity × 100
        try:
            _eq = _equity_cache.get("value") or ACCOUNT_EQUITY_USDT
            portfolio_heat_pct = round(total_risk_usdt / _eq * 100, 2) if _eq > 0 else 0.0
        except Exception:
            portfolio_heat_pct = 0.0

        return {
            "total": len(rows),
            "buy": buy_count,
            "sell": sell_count,
            "locked_usdt":       round(locked_usdt, 2),
            "total_risk_usdt":   round(total_risk_usdt, 4),
            "portfolio_heat_pct": portfolio_heat_pct,
            "open_pairs":        open_pairs,           # [v7.20 #B]
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
                        "portfolio_heat_pct": 0.0, "open_pairs": []}
            except Exception as e2:
                log(f"⚠️ get_portfolio_state fallback: {e2}", "warn")
        else:
            log(f"⚠️ get_portfolio_state: {e} — assume 0 open trades", "warn")
        return {"total": 0, "buy": 0, "sell": 0, "locked_usdt": 0.0,
                "total_risk_usdt": 0.0, "portfolio_heat_pct": 0.0, "open_pairs": []}


def portfolio_allows(sig: dict, state: dict, btc: dict) -> bool:
    """
    [v7.11 #1] Gate portfolio-level — dipanggil sebelum setiap signal dikirim.

    Tiga pemeriksaan berurutan (short-circuit pada yang pertama gagal):
      1. Hard cap total open trades → jika penuh, blok semua arah
      2. Same-side exposure cap     → jika satu arah terlalu banyak, blok
      3. BTC correlation gate       → jika BTC drop + terlalu banyak BUY,
                                       blok BUY baru (SELL tetap boleh)

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

    # ── Check 2: Same-side exposure cap ──────────────────────────
    same_side_count = state["buy"] if side == "BUY" else state["sell"]
    if same_side_count >= MAX_SAME_SIDE_TRADES:
        log(f"   🧠 Portfolio SKIP {pair} [{side}] — "
            f"max {side} aktif ({same_side_count}/{MAX_SAME_SIDE_TRADES})")
        return False

    # ── Check 3: BTC correlation gate ────────────────────────────
    # Jika BTC sedang drop (block_buy=True) dan sudah ada terlalu banyak
    # BUY aktif, tolak BUY baru karena semua alt sangat berkorelasi BTC.
    # SELL tidak kena gate ini — justru SELL lebih relevan saat BTC drop.
    if side == "BUY" and btc.get("block_buy") and state["buy"] >= MAX_BTC_CORR_TRADES:
        log(f"   🧠 Portfolio SKIP {pair} [BUY] — "
            f"BTC drop + BUY exposure tinggi ({state['buy']}/{MAX_BTC_CORR_TRADES})")
        return False

    # ── Check 4: [v7.19 #A] Locked capital cap ───────────────────
    # Cegah overexposure saat ada campuran posisi besar dan kecil.
    # new_size = estimasi size sinyal baru (fallback ke BASE jika tidak ada).
    # Partial trade sudah dihitung setengah size di get_portfolio_state().
    new_size = sig.get("position_size", BASE_POSITION_USDT) or BASE_POSITION_USDT
    if state["locked_usdt"] + new_size > MAX_LOCKED_USDT:
        log(f"   🧠 Portfolio SKIP {pair} [{side}] — "
            f"locked capital limit tercapai "
            f"(${state['locked_usdt']:.1f}+${new_size:.1f} > ${MAX_LOCKED_USDT:.1f})")
        return False

    # ── Check 5: [v7.20 #A] Risk-based exposure cap ──────────────
    # Risk trade baru = size × sl_dist_pct. Lebih presisi dari locked-cap
    # karena SL dekat = risk kecil, SL jauh = risk besar.
    new_entry = sig.get("entry", 0.0) or 0.0
    new_sl    = sig.get("sl",    0.0) or 0.0
    if new_entry > 0 and new_sl > 0:
        new_risk = new_size * abs(new_entry - new_sl) / new_entry
    else:
        new_risk = new_size * TARGET_RISK_PCT
    if state["total_risk_usdt"] + new_risk > MAX_RISK_USDT:
        log(f"   🧠 Portfolio SKIP {pair} [{side}] — "
            f"risk cap tercapai "
            f"(${state['total_risk_usdt']:.2f}+${new_risk:.2f} > ${MAX_RISK_USDT:.2f})")
        return False

    # ── Check 6: [v7.20 #D] Portfolio heat cap ───────────────────
    # Heat = total_risk / equity × 100. Blok jika sudah terlalu panas.
    heat = state.get("portfolio_heat_pct", 0.0)
    if heat >= MAX_HEAT_PCT:
        log(f"   🔥 Portfolio SKIP {pair} [{side}] — "
            f"portfolio heat {heat:.1f}% ≥ {MAX_HEAT_PCT:.0f}% limit")
        return False

    # ── Check 7: [v7.21 #4] Trend bias gate ──────────────────────
    # Saat market TRENDING + struktur BULLISH, SELL signal memiliki
    # probabilitas keberhasilan yang jauh lebih rendah (melawan trend).
    # Gate ini memblok SELL baru saat kondisi tersebut terdeteksi.
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
    stats = {"evaluated": 0, "updated": 0, "tp1": 0, "tp2": 0, "sl": 0, "expired": 0, "partial_win": 0}

    try:
        rows = (
            supabase.table("signals_v2")
            .select("id, pair, strategy, side, entry, tp1, tp2, sl, sent_at, position_size")  # [v7.18 #D] position_size ikut di-fetch
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

        if age_hours > expire_hours:
            # [v7.19 #C] Jika trade sudah TP1_PARTIAL sebelum expired,
            # result = "PARTIAL_WIN" bukan "EXPIRED".
            # pnl_usdt akan diisi dari partial_pnl_usdt yang sudah tersimpan di DB.
            # Ini mencegah bias winrate turun dan expectancy yang salah.
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
                # Trade sudah ambil setengah profit di TP1
                # SL efektif sekarang = entry (breakeven)
                if side == "BUY":
                    if tp2 is not None and current_price >= tp2:   result = "TP2"
                    elif current_price <= entry:                     result = "BREAKEVEN"
                else:  # SELL
                    if tp2 is not None and current_price <= tp2:   result = "TP2"
                    elif current_price >= entry:                     result = "BREAKEVEN"
            else:
                # Normal evaluation — TP2 dicek lebih dulu (tidak double-count)
                if side == "BUY":
                    if tp2 is not None and current_price >= tp2:   result = "TP2"
                    elif current_price >= tp1:
                        # [v7.13 #2] TP1 hit: partial profit mode
                        result = "TP1_PARTIAL" if (ENABLE_PARTIAL_TP and tp2 is not None) else "TP1"
                    elif current_price <= sl:                       result = "SL"
                else:  # SELL
                    if tp2 is not None and current_price <= tp2:   result = "TP2"
                    elif current_price <= tp1:
                        result = "TP1_PARTIAL" if (ENABLE_PARTIAL_TP and tp2 is not None) else "TP1"
                    elif current_price >= sl:                       result = "SL"

            if result is None:
                continue   # belum ada level tersentuh — biarkan open

        # ── Update Supabase ───────────────────────────────────────
        try:
            update_payload: dict = {"closed_at": now_utc.isoformat()}

            if result == "TP1_PARTIAL":
                # Tidak tutup trade — update partial_result saja.
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
                except Exception as _ppe:
                    log(f"   ⚠️ Partial PnL calc error [{pair}]: {_ppe}", "warn")
                    partial_pnl = 0.0

                update_payload = {
                    "partial_result":  "TP1_PARTIAL",
                    "partial_pnl_usdt": partial_pnl,  # [v7.18 #A] realized partial PnL
                }
                supabase.table("signals_v2").update(update_payload).eq("id", trade_id).execute()
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
                    except Exception as _pe:
                        log(f"   ⚠️ PnL calc error [{pair}]: {_pe}", "warn")
                        pnl_usdt = 0.0

                update_payload["result"]   = result
                update_payload["pnl_usdt"] = pnl_usdt
                # [FIX #2] Debug log — konfirmasi update benar-benar dieksekusi
                log(f"   🔄 Updating trade {trade_id} → result={result} pnl={pnl_usdt:+.4f}")
                supabase.table("signals_v2").update(update_payload).eq("id", trade_id).execute()

            stats["updated"] += 1
            # Update stats counter per result type
            # BREAKEVEN dianggap "neutral" — tidak masuk WIN/LOSS untuk WR calc
            key = result.lower() if result in ("TP1", "TP2", "SL", "EXPIRED", "PARTIAL_WIN") else (
                "tp1" if result == "BREAKEVEN" else "expired"
            )
            key = {"tp1": "tp1", "tp2": "tp2", "sl": "sl", "expired": "expired"}.get(key, "expired")
            stats[key] = stats.get(key, 0) + 1

            emoji = {"TP2": "🎯🎯", "TP1": "🎯", "SL": "❌", "EXPIRED": "⏰",
                     "BREAKEVEN": "⚖️", "PARTIAL_WIN": "🎯½"}.get(result, "?")
            log(f"   {emoji} {result}: {pair} [{strategy} {side}] pnl={pnl_usdt:+.2f} USDT")

            # ── Kirim notifikasi Telegram untuk setiap trade yang closed ──
            # EXPIRED tidak dikirim — tidak informatif untuk user
            if result != "EXPIRED":
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
            f"SL:{stats['sl']} EXPIRED:{stats['expired']} PARTIAL_WIN:{stats['partial_win']})")
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

    WIN_VALUES = {"WIN", "TP1", "TP2", "BREAKEVEN"}
    now_utc    = datetime.now(timezone.utc)

    cumulative   = 0.0
    peak         = _load_peak_equity_from_db()   # [v7.22 #B] persistent high-watermark
    # Safety guard: peak tidak pernah di bawah modal awal
    if peak < ACCOUNT_EQUITY_USDT:
        peak = ACCOUNT_EQUITY_USDT
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
        equity_now = ACCOUNT_EQUITY_USDT + cumulative   # [v7.22 #A] equity absolut
        if equity_now > peak:
            peak = equity_now
        # Safety guard: peak tidak pernah di bawah modal awal
        if peak < ACCOUNT_EQUITY_USDT:
            peak = ACCOUNT_EQUITY_USDT

        # [v7.15 #E] Track max DD pada setiap titik — bukan hanya current
        # [v7.22 #A] DD dihitung dari equity absolut, peak selalu >= ACCOUNT_EQUITY_USDT
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
    # [v7.22 #A] current equity = base + cumPnL, bukan hanya cumPnL
    current_equity_abs = ACCOUNT_EQUITY_USDT + cumulative
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

    return {
        "cumulative_pnl"  : round(cumulative, 4),
        "peak_equity"     : round(peak, 4),         # [v7.22 #A] sekarang dalam USDT absolut (misal $200.85)
        "current_dd_pct"  : round(current_dd_pct, 2),
        "max_dd_pct"      : round(max_dd_frac * 100, 2),   # [v7.15 #E]
        "win_rate_30d"    : round(win_rate_30d, 4) if win_rate_30d is not None else None,
        "sharpe_approx"   : sharpe_approx,
        "sortino_approx"  : sortino_approx,                # [v7.15 #E]
        "total_closed"    : len(rows),
        "total_30d"       : total_30d,
        "daily_values"    : daily_values,                  # [v7.15 #E] untuk sparkline
        "curve_points"    : curve_points,                  # [v7.15 #E] untuk JSONB
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
        f"Open trades   : <b>{open_trades}</b>\n"
        f"<i>Snapshot: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC</i>"
    )


# ════════════════════════════════════════════════════════
#  MAIN RUN
# ════════════════════════════════════════════════════════

def send_open_trades_summary() -> None:
    """
    Kirim rekapan semua open trades ke Telegram setiap akhir run.
    Memudahkan user memantau posisi aktif tanpa harus menunggu signal baru.
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

        # Format entry/tp2/sl ringkas
        def fmt(v):
            if v is None: return "—"
            f = float(v)
            if f >= 1000:   return f"${f:,.0f}"
            if f >= 1:      return f"${f:.4f}"
            if f >= 0.01:   return f"${f:.5f}"
            return f"${f:.6f}"

        line_1 = str(i) + ". " + side_emoji + " <b>" + side + " " + pair_disp + "</b> [" + strategy + "]" + status
        line_2 = "   Entry: " + fmt(entry) + " | TP2: " + fmt(tp2) + " | SL: " + fmt(sl)
        line_3 = "   Usia: " + age_str
        line   = line_1 + "\n" + line_2 + "\n" + line_3
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
    global _candle_cache, _dedup_memory
    _candle_cache  = {}   # flush cache setiap cycle
    _dedup_memory  = set()   # [v7.7 #7] reset in-memory dedup setiap cycle — HARUS set(), bukan {}

    client = get_client()

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

    log(f"\n{'='*60}")
    log(f"🚀 SIGNAL BOT v7.20 — {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')} [FULL SCAN]")
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
        f"🔥 Heat: {portfolio_state['portfolio_heat_pct']:.1f}% / {MAX_HEAT_PCT:.0f}% "
        f"| Risk: ${portfolio_state['total_risk_usdt']:.2f} / ${MAX_RISK_USDT:.2f}")

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

    # [v7.16 #C] Build pairwise correlation matrix sebelum scan loop
    # Pakai ticker yang lolos filter vol sebagai candidate pairs
    _valid_ticker_pairs = [
        t.currency_pair for t in tickers
        if is_valid_pair(t.currency_pair)
    ]
    if _valid_ticker_pairs:
        log(f"🔗 Building pairwise correlation matrix ({len(_valid_ticker_pairs)} valid pairs)...")
        build_pairwise_matrix(client, _valid_ticker_pairs)
        log(f"   Dynamic blocked: {len(_dynamic_blocked_pairs)} pairs")

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

            # [v7.13 #5] Cluster correlation gate — blokir BUY jika cluster sedang drop
            cluster_buy_blocked = (
                allow_buy and is_cluster_blocked(pair, cluster_regimes)
            )

            # ── INTRADAY BUY ──────────────────────────────────
            if allow_buy and not cluster_buy_blocked and not already_sent(pair, "INTRADAY", "BUY"):
                sig = check_intraday(client, pair, price, get_ob_ratio_lazy(pair), btc, side="BUY")
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "INTRADAY", "BUY"))

            # ── INTRADAY SELL ─────────────────────────────────
            if allow_sell and not already_sent(pair, "INTRADAY", "SELL"):
                sig = check_intraday(client, pair, price, get_ob_ratio_lazy(pair), btc, side="SELL")
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "INTRADAY", "SELL"))

            # ── SWING BUY ────────────────────────────────────
            if allow_buy and not cluster_buy_blocked and not already_sent(pair, "SWING", "BUY"):
                sig = check_swing(client, pair, price, get_ob_ratio_lazy(pair), btc, side="BUY")
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "SWING", "BUY"))

            # ── SWING SELL ───────────────────────────────────
            if allow_sell and not already_sent(pair, "SWING", "SELL"):
                sig = check_swing(client, pair, price, get_ob_ratio_lazy(pair), btc, side="SELL")
                if sig:
                    signals.append(sig)
                    _dedup_memory.add(_dedup_key(pair, "SWING", "SELL"))

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
        micro_sent += 1
        time.sleep(0.5)

    if not signals and micro_sent == 0:
        tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v7.20</b>\n"
           f"━━━━━━━━━━━━━━━━━━\n"
           f"Pairs scanned : <b>{scanned}</b>\n"
           f"F&G           : <b>{fg}</b>\n"
           f"BTC 1h/4h     : <b>{btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}%</b>\n"
           f"Equity aktif  : <b>${current_equity:.2f} USDT</b>\n"
           f"Portfolio open: <b>{portfolio_state['total']}</b> "
           f"(BUY:{portfolio_state['buy']} SELL:{portfolio_state['sell']})\n"
           f"🔥 Heat : <b>{portfolio_state['portfolio_heat_pct']:.1f}%</b> / {MAX_HEAT_PCT:.0f}% "
           f"| Risk: <b>${portfolio_state['total_risk_usdt']:.2f}</b> / ${MAX_RISK_USDT:.2f}\n"
           f"━━━━━━━━━━━━━━━━━━\n"
           f"Signal terkirim : <b>0</b>\n"
           f"<i>Tidak ada signal memenuhi kriteria saat ini.</i>\n"
           f"<i>Scan berikutnya dalam 4 jam.</i>")
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

        sent_sigs.append(sig)
        sent += 1
        time.sleep(0.5)

    # Summary
    intraday_buy  = sum(1 for s in sent_sigs if s["strategy"] == "INTRADAY" and s["side"] == "BUY")
    intraday_sell = sum(1 for s in sent_sigs if s["strategy"] == "INTRADAY" and s["side"] == "SELL")
    swing_buy     = sum(1 for s in sent_sigs if s["strategy"] == "SWING"    and s["side"] == "BUY")
    swing_sell    = sum(1 for s in sent_sigs if s["strategy"] == "SWING"    and s["side"] == "SELL")

    tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v7.20</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Pairs scanned : <b>{scanned}</b>\n"
       f"F&G           : <b>{fg}</b>\n"
       f"BTC 1h/4h     : <b>{btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}%</b>\n"
       f"Equity aktif  : <b>${current_equity:.2f} USDT</b>\n"
       f"Portfolio open: <b>{portfolio_state['total']}</b> "
       f"(BUY:{portfolio_state['buy']} SELL:{portfolio_state['sell']})\n"
       f"🔥 Heat : <b>{portfolio_state['portfolio_heat_pct']:.1f}%</b> / {MAX_HEAT_PCT:.0f}% "
       f"| Risk: <b>${portfolio_state['total_risk_usdt']:.2f}</b> / ${MAX_RISK_USDT:.2f}\n"
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

    # [v7.14 #E] Equity curve snapshot — diambil di akhir setiap run
    save_equity_snapshot(open_trades=portfolio_state["total"])

    # ── Kirim rekapan open trades ke Telegram ────────────────────────────
    send_open_trades_summary()


if __name__ == "__main__":
    run()
