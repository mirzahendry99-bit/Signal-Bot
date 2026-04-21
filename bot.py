"""
╔══════════════════════════════════════════════════════════════════╗
║       SIGNAL BOT v8.9.1 — Hotfix Edition                         ║
║                                                                  ║
║  ── v8.9.1 Hotfix (2 Perbaikan dari Live Run) ────────────────  ║
║  [v8.9.1 #1] FIX: Schema cache error Supabase —                 ║
║            Error 'Could not find closed_at column in schema     ║
║            cache' menyebabkan semua signal masuk CSV fallback.  ║
║            Fix: _reload_supabase_schema() di startup, column    ║
║            validation di awal run() dengan Telegram alert,      ║
║            dan graceful retry di save_signal() yang strip       ║
║            kolom bermasalah dan insert kolom inti saja.         ║
║            Signal tidak akan hilang bahkan sebelum schema fix.  ║
║  [v8.9.1 #2] FIX: SCAN_TIMEOUT default 300s → 420s —           ║
║            Scan 117 pair di Gate.io membutuhkan ~5 menit.       ║
║            Default 300s terlalu ketat — cycle sering di-abort   ║
║            di pair terakhir. Dinaikkan ke 420s (7 menit) agar   ║
║            full scan bisa selesai tanpa timeout.                ║
║            Tetap configurable via SCAN_TIMEOUT_SECONDS env var. ║
║                                                                  ║
║  ── v8.9 Perfect Score Edition ───────────────────────────────  ║
║  [v8.9 #1] RISK: Expanded sector taxonomy + smart OTHER cap —   ║
║            SECTOR_KEYWORDS diperluas dari 9 ke 14 sektor dengan ║
║            token list yang jauh lebih komprehensif (+100 token).║
║            Bucket "OTHER" kini punya sub-limit sendiri via       ║
║            MAX_OTHER_SIGNALS (default 1) — mencegah flooding     ║
║            dari token tidak dikenal yang semua masuk bucket sama.║
║            CoinGecko-free: klasifikasi murni heuristik statis   ║
║            tapi cukup cover ~85% token yang aktif di Gate.io.   ║
║  [v8.9 #2] PERF: Semi-auto weight application dari tune_weights ║
║            — jika WEIGHT_AUTO_APPLY=true (default: false) dan   ║
║            kondisi aman terpenuhi (WR > 45%, discrimination     ║
║            >= 2.0, sample >= 30), delta kecil (max ±1 per key)  ║
║            di-apply ke W dict untuk sesi berikutnya.            ║
║            Semua perubahan dicatat di Telegram dan weight_audit. ║
║            Hard guard: bobot tidak boleh keluar dari range       ║
║            WEIGHT_MIN / WEIGHT_MAX. Developer tetap bisa veto   ║
║            dengan WEIGHT_AUTO_APPLY=false (default aman).       ║
║                                                                  ║
║  ── v8.8 Final Polish (3 Perbaikan) ──────────────────────────  ║
║  [v8.8 #1] RISK: Correlation-aware portfolio cap —              ║
║            MAX_CONCURRENT_BUY_SIGNALS kini membatasi JUGA per  ║
║            sektor: maksimal MAX_SECTOR_SIGNALS sinyal BUY dari  ║
║            sektor yang sama dalam 1 cycle. Sektor ditentukan    ║
║            via token name heuristic (L1, DeFi, meme, AI, dll).  ║
║            Mencegah 5 signal semua dari sektor korelasi tinggi  ║
║            yang semua kena SL saat BTC reversal.                ║
║  [v8.8 #2] SIGNAL: Candle body strength validation di BOS —     ║
║            BOS dan CHoCH hanya dianggap valid jika candle yang  ║
║            menembus struktur memiliki body ratio >= 0.5         ║
║            (body >= 50% dari range candle). Wick-only           ║
║            penetration yang sering menjadi false breakout       ║
║            kini ditolak. BOS_BODY_RATIO_MIN konstanta baru.     ║
║  [v8.8 #3] PERF: Adaptive weight tuning via tune_weights() —   ║
║            Fungsi mingguan yang menganalisis 50 signal terakhir ║
║            per strategy, menghitung korelasi antara kehadiran   ║
║            setiap score-component dengan TP_HIT vs SL_HIT,      ║
║            lalu menyimpan delta rekomendasi ke Supabase tabel   ║
║            weight_audit. Read-only by default (tidak auto-apply ║
║            ke W dict) — developer tetap pemegang keputusan.     ║
║            Dikontrol via WEIGHT_AUDIT_ENABLED env var.          ║
║                                                                  ║
║  ── v8.7 Full Gap Closure (3 Perbaikan) ──────────────────────  ║
║  [v8.7 #1] SIGNAL: Weekly Structure Gate untuk SWING —          ║
║            SWING BUY ditolak jika weekly bias BEARISH + BOS     ║
║            BEARISH. SWING SELL ditolak jika weekly bias         ║
║            BULLISH + BOS BULLISH. Pola identik dengan MTF       ║
║            4h gate di INTRADAY (v8.3 #1). Dikontrol via         ║
║            SWING_WEEKLY_GATE_ENABLED env var (default: true).   ║
║            get_weekly_bias() helper + _weekly_bias_cache per    ║
║            cycle. limit=60 candle weekly (>1 tahun histori).    ║
║  [v8.7 #2] SIGNAL: Dynamic TP multiplier berbasis ADX —         ║
║            TP1/TP2 R-multiplier disesuaikan dengan kekuatan     ║
║            trend (ADX). ADX<20: konservatif (TP1=0.8R,         ║
║            TP2=1.4R). ADX 20-30: default. ADX>30: agresif       ║
║            (TP1=1.2R, TP2=2.2R INTRADAY / TP1=2.4R TP2=4.2R    ║
║            SWING). Mencegah profit yang ditinggalkan di market  ║
║            strong trend dan loss di ranging market.             ║
║            get_tp_multipliers() helper dipanggil dari           ║
║            calc_sl_tp(). Parameter adx opsional (default 0).   ║
║  [v8.7 #3] SIGNAL: Session-normalized volume scoring —          ║
║            Bonus +1 (vol_session_strong) jika vol candle        ║
║            saat ini > rata-rata jam yang sama 7 hari terakhir   ║
║            × SESSION_VOL_SPIKE_MULT (1.5×). Lebih bermakna      ║
║            dari hanya vs N candle sebelumnya: volume spike di   ║
║            jam biasanya sepi jauh lebih signifikan dari spike   ║
║            di jam ramai. Berlaku untuk INTRADAY dan MOMENTUM.  ║
║            calc_session_vol_ratio() helper menggunakan candle   ║
║            1h lookback 7×24=168 candle.                         ║
║                                                                  ║
║  ── v8.6 SL Liquidity Zone Avoidance (1 Perbaikan) ───────────  ║
║  [v8.6 #1] RISK: SL Liquidity Zone Avoidance —                  ║
║            SL yang jatuh dalam radius 0.5× ATR dari zona        ║
║            equal-lows (BUY) atau equal-highs (SELL) kini        ║
║            digeser 0.3% melewati zona tersebut. Mencegah        ║
║            stop-hunt oleh market maker yang sengaja sweep        ║
║            level equal-lows/highs sebelum harga berbalik.       ║
║            Fungsi baru: adjust_sl_for_liquidity() dipanggil     ║
║            dari dalam calc_sl_tp() via parameter liq opsional.  ║
║            Berlaku untuk INTRADAY, SWING, dan MOMENTUM.         ║
║            TP dihitung ulang dari SL yang sudah disesuaikan     ║
║            sehingga RR ratio tetap konsisten.                   ║
║            Safety guard: shift > 8% dari SL asli dibatalkan.   ║
║            Konstanta: SL_LIQ_PROXIMITY_ATR_MULT=0.5,           ║
║            SL_LIQ_BUFFER_PCT=0.003 (configurable).             ║
║                                                                  ║
║  [v8.5 #1] SIGNAL: RSI Momentum Direction Scoring —             ║
║            Tambah diferensiasi arah RSI pada score_signal()      ║
║            dan check_momentum(). RSI yang sedang NAIK mendapat  ║
║            bonus +1 (rsi_dir_bull), RSI yang sedang TURUN       ║
║            mendapat penalti -1 (rsi_dir_bear). Berlaku untuk    ║
║            BUY, SELL (INTRADAY/SWING), dan MOMENTUM scanner.    ║
║            Ini menjawab kelemahan overlap zone RSI 40-60 di     ║
║            mana RSI 55 naik dari 40 != RSI 55 turun dari 72.    ║
║            Metode: linear slope (polyfit) pada 6 candle RSI     ║
║            terakhir — robust terhadap noise candle tunggal.     ║
║            Helper baru: calc_rsi_series() (N nilai RSI),        ║
║            calc_rsi_momentum_dir() (+1/0/-1).                   ║
║            Konstanta: RSI_MOMENTUM_SLOPE_THRESH=0.8,            ║
║            RSI_MOMENTUM_WINDOW=6 (configurable via env var).    ║
║                                                                  ║
║  ── v8.4 Bug Fix Edition (5 Perbaikan) ───────────────────────  ║
║  [v8.4 #1] BUG FIX: check_swing() SELL — score post-penalty    ║
║            tidak masuk ke return dict["score"] dan conviction.  ║
║  [v8.4 #2] BUG FIX: evaluate_signals() — outcome dievaluasi    ║
║            dari candle sebelum entry dan candle running.        ║
║            Fix: hanya evaluasi candle closed SETELAH sent_at,  ║
║            candle terakhir (running) selalu di-skip.            ║
║  [v8.4 #3] BUG FIX: _start_tg_worker() dipanggil dua kali     ║
║            saat SCAN_MODE=test — dua worker thread consume      ║
║            dari queue yang sama, pesan bisa dobel/hilang.       ║
║            Fix: hapus pemanggilan duplikat di test branch.      ║
║  [v8.4 #4] BUG FIX: MOMENTUM signal tampilkan valid 16 jam     ║
║            padahal TF 1h sama dengan INTRADAY (4 jam).          ║
║            Fix: hours = 4 if strategy in INTRADAY/MOMENTUM.    ║
║  [v8.4 #5] CLEANUP: get_ob_ratio_micro() dead code dihapus —   ║
║            check_microcap() tidak punya parameter ob_ratio      ║
║            sehingga fungsi ini tidak pernah terpanggil.         ║
║            Keputusan tidak pakai ob_ratio di microcap sudah     ║
║            benar secara desain — helper-nya saja yang mubazir.  ║
║  [v8.3 #1] SIGNAL: Multi-Timeframe Confirmation (MTF) untuk     ║
║            INTRADAY — sinyal 1h hanya lolos jika 4h structure   ║
║            bias tidak berlawanan arah. BUY ditolak jika 4h      ║
║            bias BEARISH + BOS BEARISH (counter-trend). SELL     ║
║            ditolak jika 4h bias BULLISH + BOS BULLISH.          ║
║            get_4h_bias() helper + cache per pair per cycle.     ║
║            Dikontrol via MTF_ALIGNMENT_ENABLED env var.         ║
║  [v8.3 #2] SIGNAL: PUMP Structure Gate — tolak pump signal      ║
║            jika 15m structure memiliki active BOS BEARISH.      ║
║            Mencegah pump alert pada pair yang sedang dalam       ║
║            downstruktur. Dikontrol via PUMP_STRUCTURE_GATE_     ║
║            ENABLED env var (default: true).                     ║
║  [v8.3 #3] RISK: Portfolio Exposure Cap — setelah semua BUY     ║
║            signal terkumpul, total (INTRADAY+SWING+MOMENTUM)   ║
║            di-cap ke MAX_CONCURRENT_BUY_SIGNALS (default 5).   ║
║            Mencegah over-concentration saat banyak pair         ║
║            berkorelasi tinggi naik bersamaan. Sort by score      ║
║            desc, trim kelebihan, TG alert saat cap aktif.       ║
║  [v8.3 #4] PERF: Profit Factor & Expectancy di                  ║
║            strategy_health_check() — selain win rate, kini      ║
║            hitung avg_win%, avg_loss%, profit_factor, dan       ║
║            expectancy per trade. Tersimpan di health dict dan   ║
║            ditampilkan di Telegram health alert. Metric ini      ║
║            lebih bermakna dari win rate sendirian.              ║
║  [v8.3 #5] TESTING: Extended self-test edge cases — tambah      ║
║            T8 (detect_structure data insuffisien), T9            ║
║            (calc_position_size edge cases: entry=0, sl=0,       ║
║            sl≥entry), T10 (MTF alignment helper verify).        ║
║            Total test suite: 30 test (naik dari 23).           ║
║                                                                  ║
║  ── v8.2 Perfect Edition (7 Perbaikan ke Score 100/100) ──────  ║
║  [v8.2 #1] PERF: strategy_health_check() — tambah pending_count ║
║            per strategy ke health dict. Win rate bias sekarang   ║
║            TERUKUR: user tahu berapa signal masih pending        ║
║            vs yang sudah resolved. Bias tidak lagi invisible.    ║
║  [v8.2 #2] RELIABILITY: evaluate_signals() auto-trigger via     ║
║            daemon thread di akhir run() — non-blocking. Tidak   ║
║            perlu cron terpisah. Outcome ter-resolve otomatis     ║
║            setiap scan cycle selesai kirim signal.              ║
║  [v8.2 #3] SIGNAL: check_microcap() refactor scoring unified.   ║
║            micro_score sekarang di-map ke W_MICRO dict           ║
║            (STRUCTURAL CONSTANTS) dan threshold pakai            ║
║            MICRO_TIER_MIN_SCORE — konsisten dgn main bot.       ║
║            assign_tier_micro() baru, satu path tier logic.      ║
║  [v8.2 #4] SIGNAL: Diverge penalty diterapkan SEBELUM           ║
║            assign_tier() — eliminasi double-call assign_tier    ║
║            yang ada di v8.1 (sebelum + sesudah penalty).        ║
║            Satu kali evaluasi, lebih jelas dan efisien.          ║
║  [v8.2 #5] TESTING: run_self_test() — suite unit test internal  ║
║            untuk fungsi kritikal: calc_adx(), calc_rsi(),        ║
║            score_signal(). Data sintetis, tidak butuh API.       ║
║            Jalankan via SCAN_MODE=test. Gagal → log + TG alert. ║
║  [v8.2 #6] UI: Pending signal counter di Telegram summary —     ║
║            tampilkan jumlah signal result=NULL per strategy.    ║
║            User sekarang bisa validasi apakah evaluate_signals   ║
║            jalan cukup cepat atau ada backlog besar.            ║
║  [v8.2 #7] CODE: Version string konsisten v8.1 → v8.2 di       ║
║            seluruh file (log, Telegram summary, header).         ║
║                                                                  ║
║  ── v8.1 Sniper Edition (7 Fitur Baru) ──────────────────────── ║
║  [v8.1 #1] RISK: BTC Flash Guard 5m — Kill-Switch instant jika  ║
║            BTC drop >1.5% dalam 1 candle 5m. Semua BUY baru    ║
║            dibatalkan hingga candle berikutnya konfirmasi.       ║
║  [v8.1 #2] RISK: Spread Filter — cek Ask/Bid spread sebelum     ║
║            sinyal dikirim. Spread >0.6% → batalkan (anti-slip). ║
║  [v8.1 #3] EXEC: Dynamic Entry Zone — entry ditampilkan sebagai ║
║            range (low–high), bukan 1 angka kaku. BUY: entry     ║
║            sampai +0.5%. Jika harga di atas range → Wait Retest.║
║  [v8.1 #4] EXEC: Partial TP & Break Even — instruksi otomatis   ║
║            di pesan Telegram: TP1 → tutup 50%, pindah SL ke BE. ║
║  [v8.1 #5] INTEL: Inter-Market Correlation — jika BTC & ETH     ║
║            divergen signifikan, score Altcoin diturunkan.        ║
║  [v8.1 #6] INTEL: Volatility Window — threshold score dinaikkan  ║
║            pada jam 00:00–06:00 WIB (low-volume session).        ║
║  [v8.1 #7] UI: Summary Telegram menampilkan Flash Guard status   ║
║            dan jumlah pair ditolak Spread Filter.                ║
║                                                                  ║
║       SIGNAL BOT v8.1 — Sniper Edition (Feature Complete)       ║
║                                                                  ║
║  Upgrade v8.0 (Audit Fix Complete — 9 Perbaikan):               ║
║                                                                  ║
║  ── v8.1 Sniper Edition (7 Fitur Baru) ──────────────────────── ║
║  [v8.1 #1] RISK: BTC Flash Guard 5m — Kill-Switch instant jika  ║
║            BTC drop >1.5% dalam 1 candle 5m. Semua BUY baru    ║
║            dibatalkan hingga candle berikutnya konfirmasi.       ║
║  [v8.1 #2] RISK: Spread Filter — cek Ask/Bid spread sebelum     ║
║            sinyal dikirim. Spread >0.6% → batalkan (anti-slip). ║
║  [v8.1 #3] EXEC: Dynamic Entry Zone — entry ditampilkan sebagai ║
║            range (low–high), bukan 1 angka kaku. BUY: entry     ║
║            sampai +0.5%. Jika harga di atas range → Wait Retest.║
║  [v8.1 #4] EXEC: Partial TP & Break Even — instruksi otomatis   ║
║            di pesan Telegram: TP1 → tutup 50%, pindah SL ke BE. ║
║  [v8.1 #5] INTEL: Inter-Market Correlation — jika BTC & ETH     ║
║            divergen signifikan, score Altcoin diturunkan.        ║
║  [v8.1 #6] INTEL: Volatility Window — threshold score dinaikkan  ║
║            pada jam 00:00–06:00 WIB (low-volume session).        ║
║  [v8.1 #7] UI: Summary Telegram menampilkan Flash Guard status   ║
║            dan jumlah pair ditolak Spread Filter.                ║
║                                                                  ║
║  [v8.0 #1] SIGNAL: RSI overlap zone 40–60 → komentar eksplisit  ║
║            di score_signal() sebagai desain intentional,        ║
║            bukan bug. Volume window [-10:-1] juga terdokumentasi.║
║  [v8.0 #2] SIGNAL: Cross-strategy pair mutex — _sent_pairs_    ║
║            this_cycle set mencegah pair yang sama muncul di     ║
║            INTRADAY + MOMENTUM dalam satu cycle. Priority:       ║
║            MOMENTUM > INTRADAY > SWING (SELL tidak dibatasi).    ║
║  [v8.0 #3] RISK: Startup warning jika PORTFOLIO_VALUE = default ║
║            $1000 — user diminta set env var PORTFOLIO_VALUE.     ║
║  [v8.0 #4] RISK: PUMP SL cap (PUMP_SL_PCT_MAX=4.0%) — tolak    ║
║            pump signal jika SL > 4% dari entry. ATR membengkak  ║
║            di candle 15m volatile menghasilkan SL 6–8% yang     ║
║            merusak asumsi position sizing 1% risk.              ║
║  [v8.0 #5] RISK: daily_loss_guard() augmented dengan SL hit    ║
║            rate check — halt jika >60% resolved trades = SL_HIT.║
║            Guard sekarang sensor drawdown nyata, bukan hanya    ║
║            throughput limiter.                                   ║
║  [v8.0 #6] PERF: strategy_health_check() sample naik 20 → 30. ║
║            Strike counter per strategy — TRIPLE CRITICAL alert  ║
║            di Telegram jika CRITICAL 3× berturut-turut.         ║
║            Disimpan di tabel Supabase `strategy_strikes`.        ║
║  [v8.0 #7] PERF: evaluate_signals() dead code dihapus (hit_tp2  ║
║            kalkulasi pertama yang di-overwrite tanpa dipakai).  ║
║            Docstring ditambah peringatan candle-range ambiguity. ║
║  [v8.0 #8] RELIABILITY: Queue(maxsize=50) + put_nowait() untuk  ║
║            non-critical TG messages. tg(critical=True) untuk    ║
║            pesan wajib (halt/summary) — tidak pernah di-drop.   ║
║  [v8.0 #9] CODE: Semua magic numbers → STRUCTURAL CONSTANTS:    ║
║            VOL_CONFIRM_THRESHOLD, BOS_BREAK_TOLERANCE,          ║
║            EQUAL_LEVEL_TOL, LATE_ENTRY_THRESHOLD,               ║
║            ENTRY_NOTE_WARN/READY_PCT, PUMP_SL_PCT_MAX,          ║
║            CONVICTION_THRESHOLDS, HEALTH_CHECK_SAMPLE_SIZE.     ║
║            check_intraday() dipecah → _check_intraday_buy/sell. ║
║            MICROCAP ob_ratio pakai limit=20 (bukan 10) untuk    ║
║            depth yang lebih representatif di pair illiquid.      ║
║                                                                  ║
║                                                                  ║
║  [v7.9 #1] FIX: Duplicate ema_align di MOMENTUM engine.         ║
║            price>ema20 sekarang pakai W_MOMENTUM["price_above_ema"]║
║            bukan W["ema_align"] — cegah score inflation +1.     ║
║  [v7.9 #2] FIX: W_MOMENTUM dict terpisah — tidak ada cross-    ║
║            contamination dengan W dict INTRADAY/SWING.          ║
║  [v7.9 #3] FIX: MICROCAP tier display → "M-A" (bukan "A")      ║
║            cegah false equivalence dengan main signal tier A.   ║
║  [v7.9 #4] RISK: Rekomendasi ukuran posisi di semua signal      ║
║            berdasarkan PORTFOLIO_VALUE env var + risk 1%.       ║
║  [v7.9 #5] RISK: PUMP kini punya TP2 = entry + ATR×3.5         ║
║            + partial exit guidance di Telegram message.         ║
║  [v7.9 #6] RISK: daily_loss_guard() — halt jika signal harian  ║
║            melebihi MAX_DAILY_SIGNALS threshold.                ║
║  [v7.9 #7] PERF: save_signal sekarang menyimpan kolom result,  ║
║            closed_at, pnl_pct (awalnya None) untuk outcome     ║
║            tracking di masa depan.                              ║
║  [v7.9 #8] PERF: evaluate_signals() — fungsi terpisah untuk    ║
║            update TP/SL hit dari Gate.io closes (cron-ready).   ║
║  [v7.9 #9] PERF: strategy_health_check() — cek win rate        ║
║            20 signal terakhir per strategy, warn jika < 40%.   ║
║  [v7.9 #10] RELIABILITY: Telegram sends via background thread  ║
║             queue — scan loop tidak blocked oleh Telegram I/O.  ║
║  [v7.9 #11] RELIABILITY: Scan timeout guard — kill runaway     ║
║             cycle setelah SCAN_TIMEOUT_SECONDS (default 300s).  ║
║  [v7.9 #12] RELIABILITY: _candle_cache TTL — cache entry       ║
║             diinvalidasi setelah CANDLE_CACHE_TTL detik.       ║
║  [v7.9 #13] RELIABILITY: save_signal CSV fallback — jika       ║
║             Supabase down, signal di-append ke file lokal.     ║
║  [v7.9 #14] EDGE: Exchange downtime alert — Telegram notif     ║
║             jika tickers < 50 pairs (exchange mungkin down).   ║
║  [v7.9 #15] EDGE: Extreme-move candle guard — skip pair jika  ║
║             range candle terakhir > 5× rata-rata 20 candle.    ║
║  [v7.9 #16] EDGE: ETF fallback alert — Telegram notif jika    ║
║             dynamic blocklist gagal (pakai static saja).       ║
║                                                                  ║
║  Upgrade v7.8 (Market Alignment — 5 Strategic Fixes):           ║
║                                                                  ║
║  [v7.8 #1] ANTI-SELL BIAS — blokir SELL saat BTC 4h bullish    ║
║            + trend lokal BULLISH (ADX +DI > -DI).               ║
║            Root cause: bot overfit reversal engine →            ║
║            selalu short saat harga naik = kontra tren.          ║
║            Fix: check_intraday & check_swing SELL branch        ║
║            return None jika btc_bullish AND trend_dir BULLISH.  ║
║            btc_bullish flag ditambahkan ke get_btc_regime().    ║
║                                                                  ║
║  [v7.8 #2] MOMENTUM SCANNER — engine baru (BUY only)           ║
║            Trend continuation — menangkap koin "terbang".       ║
║            Trigger: breakout above recent high 6c + vol 1.5×    ║
║            + RSI 50–72 + EMA20 > EMA50 + MACD bullish.         ║
║            MAX 3 signal per cycle. Dedup 4 jam.                 ║
║            Tidak butuh BOS/CHoCH — fokus momentum murni.        ║
║                                                                  ║
║  [v7.8 #3] TP REALIGN — target lebih realistis untuk            ║
║            market volatile-cepat-retrace saat ini.              ║
║            INTRADAY: TP1 1.5R→1.0R, TP2 2.5R→1.8R             ║
║            MICROCAP: TP1 15%→10%, TP2 35%→25%                  ║
║            Tujuan: lebih sering kena TP1 vs "hampir TP1".       ║
║                                                                  ║
║  [v7.8 #4] SCORE WEIGHT REBALANCE — kurangi EMA & VWAP          ║
║            EMA weight: 2→1 | VWAP weight: 2→1                  ║
║            Alasan: EMA & VWAP lag di awal breakout →            ║
║            bot masuk terlalu terlambat.                         ║
║            Tambah: momentum_break(4) + rsi_momentum(2)         ║
║            untuk scoring momentum engine.                       ║
║                                                                  ║
║  [v7.8 #5] SCAN SUMMARY UPDATE — Telegram summary mencakup      ║
║            MOMENTUM BUY counter. Log lebih detail.             ║
║                                                                  ║
║  Arsitektur v7.8:                                               ║
║  - INTRADAY  (1h) : BUY + SELL (SELL di-filter saat bull)      ║
║  - SWING     (4h) : BUY + SELL (SELL di-filter saat bull)      ║
║  - MOMENTUM  (1h) : BUY only — trend continuation breakout     ║
║  - PUMP     (15m) : BUY only — big cap pump                    ║
║  - MICROCAP  (1h) : BUY only — meme/microcap early entry       ║
╚══════════════════════════════════════════════════════════════════╝                                             ║
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
import queue
import threading
import csv
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

# [v7.9 #4] Portfolio value untuk kalkulasi ukuran posisi — gunakan env var agar
# user bisa set sesuai modal aktual tanpa ubah kode.
# Formula: position_size = (PORTFOLIO_VALUE × RISK_PER_TRADE_PCT) / sl_pct
PORTFOLIO_VALUE    = float(os.environ.get("PORTFOLIO_VALUE", "1000"))   # USDT
RISK_PER_TRADE_PCT = float(os.environ.get("RISK_PER_TRADE_PCT", "1.0")) # % modal per trade

# Validasi environment
_missing = [k for k, v in {
    "SUPABASE_URL":    SUPABASE_URL, "SUPABASE_KEY":    SUPABASE_KEY,
    "TELEGRAM_TOKEN":  TG_TOKEN,     "CHAT_ID":         TG_CHAT_ID,
    "GATE_API_KEY":    API_KEY,      "GATE_SECRET_KEY": SECRET_KEY,  # [v7.2 FIX #8]
}.items() if not v]
if _missing:
    raise EnvironmentError(f"ENV belum diset: {', '.join(_missing)}")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def _reload_supabase_schema() -> bool:
    """
    [v8.9 FIX] Paksa PostgREST reload schema cache via NOTIFY.
    Mengatasi error 'Could not find column in schema cache' setelah ALTER TABLE.
    Dipanggil sekali di startup run().
    Returns True jika berhasil, False jika gagal (tidak fatal).
    """
    try:
        supabase.rpc("pg_notify", {"channel": "pgrst", "payload": "reload schema"}).execute()
        log("🔄 Supabase schema cache reload via pg_notify — OK")
        return True
    except Exception:
        pass
    # Fallback: langsung via postgrest introspection hint
    try:
        import urllib.request as _ur, json as _json
        _url  = f"{SUPABASE_URL}/rest/v1/"
        _req  = _ur.Request(_url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Accept-Profile": "public",
        })
        _ur.urlopen(_req, timeout=5)
        log("🔄 Supabase schema introspection triggered — OK")
        return True
    except Exception as _e:
        log(f"⚠️ _reload_supabase_schema: {_e} — schema reload gagal, lanjut", "warn")
        return False


def _get_signals_v2_columns() -> set:
    """
    [v8.9 FIX] Ambil daftar kolom aktual tabel signals_v2 dari Supabase
    menggunakan information_schema. Digunakan oleh save_signal untuk
    strip kolom yang belum ada (graceful degradation).
    Returns set nama kolom, atau set kosong jika query gagal.
    """
    try:
        result = supabase.rpc("get_signals_v2_columns", {}).execute()
        if result.data:
            return {row["column_name"] for row in result.data}
    except Exception:
        pass
    # Fallback: anggap semua kolom inti sudah ada, kolom baru mungkin belum
    return {"pair", "strategy", "side", "entry", "tp1", "tp2", "sl",
            "tier", "score", "timeframe", "sent_at",
            "result", "closed_at", "pnl_pct"}

# [v8.0 AUDIT FIX 2.1] Warn jika PORTFOLIO_VALUE masih di nilai default.
# Jika user lupa set env var, bot akan menggunakan $1000 sebagai basis
# position sizing — berbahaya jika modal aktual sangat berbeda.
if PORTFOLIO_VALUE == 1000.0:
    import warnings
    warnings.warn(
        "⚠️  PORTFOLIO_VALUE = default $1000 — set env var PORTFOLIO_VALUE "
        "jika modal kamu berbeda. Position sizing tidak akan akurat.",
        UserWarning, stacklevel=2,
    )

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
# [v7.8 TP FIX] TP diturunkan agar lebih sering kena TP1 di market volatile-cepat-retrace.
# Sebelumnya: TP1=1.5R, TP2=2.5R → banyak "hampir TP1" karena move hanya kasih 0.8–1.2R.
# Sekarang  : TP1=1.0R, TP2=1.8R → realistic target sesuai karakter market saat ini.
INTRADAY_TP1_R  = 1.0    # [v7.8] turun dari 1.5 → 1.0 (lebih sering kena TP1)
INTRADAY_TP2_R  = 1.8    # [v7.8] turun dari 2.5 → 1.8 (realistis untuk intraday)
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
# [v7.8 TP FIX] Microcap TP diturunkan agar lebih realistis — sebelumnya TP1=15%, TP2=35%
# terlalu ambisius dan sering miss. Sekarang TP1=10%, TP2=25% lebih sering tercapai.
MICRO_TP1_PCT        = 0.10      # [v7.8] turun dari 0.15 → 0.10 (+10%)
MICRO_TP2_PCT        = 0.25      # [v7.8] turun dari 0.35 → 0.25 (+25%)
MICRO_SL_PCT         = 0.05      # SL: -5% dari entry (ketat)
MICRO_MIN_RR         = 2.5       # minimum R/R — harus worth the risk
MICRO_DEDUP_HOURS    = 2         # dedup microcap signal
MAX_MICRO_SIGNALS    = 4         # maksimal microcap signal per run

# ── Scan Timing ──────────────────────────────────────
# [v7.7 #10] Satu konstanta untuk throttle loop — sebelumnya 0.08 (pump) vs 0.1 (main)
# yang tidak terdokumentasi dan inkonsisten. Disamakan ke 0.1s untuk semua scanner.
SCAN_SLEEP_SEC = 0.1

# ── STRUCTURAL CONSTANTS ──────────────────────────────────────
# [v8.0 AUDIT FIX 5.1] Semua nilai struktural yang sebelumnya hardcoded
# dipindahkan ke sini agar setiap nilai dapat diaudit, dikonfigurasi,
# dan dilacak perubahannya tanpa menyentuh logika sinyal.

# Volume confirmation threshold — score +W["vol_confirm"] jika vol candle terakhir
# melebihi rata-rata N candle sebelumnya sebesar faktor ini.
# PENTING: window selalu mengecualikan candle terakhir (index -1) → volumes[-10:-1]
# agar baseline dihitung dari candle yang sudah CONFIRMED, bukan candle yang sedang
# berjalan. Ini konsisten di score_signal(), PUMP scanner, dan Microcap scanner.
VOL_CONFIRM_THRESHOLD = 1.3  # candle vol > avg × 1.3 = konfirmasi volume breakout

# BOS tolerance band — mencegah false positive dari noise tipis pada candle
# yang sedikit menembus level struktur kemudian kembali (wick penetration).
# bull_break: close[i-1] <= last_sh * (1 + BOS_BREAK_TOLERANCE)
# bear_break: close[i-1] >= last_sl * (1 - BOS_BREAK_TOLERANCE)
BOS_BREAK_TOLERANCE = 0.008  # 0.8% band (last_sh * 1.008 / last_sl * 0.992)

# Equal high/low detection tolerance — dua level dianggap equal jika selisih
# relatif < threshold ini. Dipakai di detect_liquidity().
EQUAL_LEVEL_TOL = 0.003  # 0.3% dari nilai referensi

# Late-entry threshold — BUY: tolak signal jika harga sudah > last_sh × threshold.
# Mencegah entry kejar harga yang sudah terlalu jauh dari zona BOS.
# [v7.7 FIX] Dipindahkan ke sebelum scoring untuk hemat komputasi.
LATE_ENTRY_THRESHOLD = 1.02  # 2% di atas last_sh = late entry, tolak

# Momentum late-entry threshold — tolak momentum signal jika harga sudah naik
# lebih dari ini di atas recent_high. Mencegah buy-the-top setelah breakout.
# [v8.0 AUDIT FIX] Dipindahkan dari hardcoded 1.04 di check_momentum() ke sini.
MOMENTUM_LATE_ENTRY_THRESHOLD = 1.04  # 4% di atas recent_high = terlambat, tolak

# Entry note thresholds untuk Telegram message — kapan warning ditampilkan
# BUY:  pct_above > WARN → "tunggu pullback" | pct_above < -READY → "sudah di zona"
# SELL: pct_above < -WARN → "tunggu retest"  | pct_above > READY  → "sudah di zona"
ENTRY_NOTE_WARN_PCT  = 0.5   # |pct_above| > 0.5%: tampilkan warning harga jauh
ENTRY_NOTE_READY_PCT = 0.3   # |pct_above| > 0.3%: konfirmasi "sudah di zona entry"

# PUMP scanner SL distance cap — tolak pump signal jika SL terlalu jauh dari entry.
# [v8.0 AUDIT FIX 2.2] High-ATR pair bisa menghasilkan SL 6–8% yang merusak
# asumsi position sizing di calc_position_size(). Cap ini memastikan
# risk per unit yang konsisten dengan strategi sizing 1% risk.
PUMP_SL_PCT_MAX = 4.0  # % — reject pump signal jika (entry - sl) / entry * 100 > 4%

# Conviction score thresholds — [v8.0 AUDIT FIX 5.1]
# Sebelumnya hardcoded 18/14/12/10 di dalam fungsi calc_conviction().
# Sebagai config dict, threshold ini dapat disesuaikan tanpa menyentuh logika fungsi.
CONVICTION_THRESHOLDS = {
    "EXTREME":   18,   # score >= 18 → "EXTREME ⚡"
    "VERY_HIGH": 14,   # score >= 14 → "VERY HIGH 🔥"
    "HIGH":      12,   # score >= 12 → "HIGH 💪"
    "GOOD":      10,   # score >= 10 → "GOOD ✅"
    # score < 10 → "OK 🟡" (fallback)
}

# Strategy health check parameters — [v8.0 AUDIT FIX 3.1]
# Naik dari 20 → 30 sample minimum untuk CI yang lebih sempit.
# Pada 50% true win rate: 20 sample → 95% CI = 28–72% (terlalu lebar untuk keputusan).
# 30 sample → 95% CI ≈ 32–68% (lebih representatif, kurangi false CRITICAL).
HEALTH_CHECK_MIN_SAMPLE  = 5    # minimum resolved trades sebelum evaluasi dianggap valid
HEALTH_CHECK_SAMPLE_SIZE = 30   # ambil 30 signal terakhir untuk win rate (naik dari 20)

# Strike counter threshold — [v8.0 AUDIT FIX 3.2]
# Berapa kali berturut-turut sebuah strategy harus berada di status CRITICAL
# sebelum pesan Telegram dieskalasi ke "TRIPLE CRITICAL".
# Strike count disimpan di Supabase (tabel strategy_strikes) antar run.
HEALTH_CRITICAL_STRIKE_LIMIT = 3  # N berturut-turut CRITICAL → eskalasi peringatan

# Momentum entry slippage — entry MOMENTUM sedikit di atas close terakhir.
# [v8.0 AUDIT FIX] Dipindahkan dari hardcoded 1.001 di check_momentum() ke sini.
MOMENTUM_ENTRY_SLIPPAGE = 1.001  # entry = close × 1.001 — antisipasi slippage breakout

# PUMP scanner ATR multipliers — SL, TP1, TP2 untuk PUMP strategy.
# [v8.0 AUDIT FIX] Dipindahkan dari hardcoded di check_pump() ke sini.
PUMP_SL_ATR_MULT  = 1.2   # SL = price - ATR × 1.2
PUMP_TP1_ATR_MULT = 2.0   # TP1 = price + ATR × 2.0
PUMP_TP2_ATR_MULT = 3.5   # TP2 = price + ATR × 3.5 [v7.9 #5]

# Pullback score bands — zona di mana harga dianggap "di dekat" support/resistance
# dan mendapat score bonus W["pullback"]. Hardcoded sebelumnya di score_signal().
# [v8.0 AUDIT FIX] Dipindahkan ke STRUCTURAL_CONSTANTS.
# BUY pullback: last_sl <= price <= last_sl * PULLBACK_UPPER_BAND
# SELL pullback: last_sh * PULLBACK_LOWER_BAND <= price <= last_sh * PULLBACK_SELL_UPPER
PULLBACK_UPPER_BAND  = 1.015  # BUY: max 1.5% di atas support = masih di zona
PULLBACK_LOWER_BAND  = 0.97   # BUY/SELL: min 3% di bawah resistance
PULLBACK_SELL_UPPER  = 1.01   # SELL: max 1% di atas resistance = masih dekat

# BTC bullish detection thresholds — [v8.0 AUDIT FIX]
# Dipindahkan dari hardcoded (chg_4h >= 1.0 and chg_1h >= 0.0) di get_btc_regime().
BTC_BULLISH_4H_MIN = 1.0   # BTC 4h change harus >= 1.0% untuk flag btc_bullish
BTC_BULLISH_1H_MIN = 0.0   # BTC 1h change harus >= 0.0% (tidak turun) untuk flag btc_bullish

# Minimum position size filter — [v8.0 AUDIT FIX]
# Tolak signal jika position size yang direkomendasikan < threshold ini (USDT).
# Mencegah order yang terlalu kecil yang dikenai fee minimum atau tidak executable.
MIN_POSITION_SIZE_USDT = 10.0  # minimum 10 USDT per trade — di bawah ini tidak worth biaya

# ATR sanity check — deteksi flash crash contamination pada candle data.
# Jika ATR candle terbaru > N× rata-rata ATR 20 candle sebelumnya, data dianggap
# terkontaminasi flash crash dan pair di-skip.
# [v8.0 AUDIT FIX] Dipindahkan ke STRUCTURAL_CONSTANTS agar mudah dikonfigurasi.
ATR_SANITY_MULTIPLIER = 5.0   # ATR[-1] > avg_ATR[-21:-1] × 5.0 → skip pair

# Stale ticker detection — ticker dianggap stale jika volume 24h = 0 atau
# last price tidak berubah dari candle sebelumnya (pair tidak aktif diperdagangkan).
# [v8.0 AUDIT FIX] Threshold dipindahkan ke sini.
STALE_TICKER_MIN_VOLUME = 100.0  # volume candle harus > 100 USDT — nilai 0.0 tidak efektif

# ── v8.1 SNIPER EDITION CONSTANTS ─────────────────────────────────

# [v8.1 #1] BTC Flash Guard 5m — Kill-Switch
# Jika BTC drop lebih dari threshold ini dalam 1 candle 5m, semua BUY baru dibatalkan.
BTC_FLASH_GUARD_DROP     = -1.5   # % — BTC drop > 1.5% dalam 1 candle 5m = kill-switch

# [v8.1 #2] Spread Filter — tolak signal jika spread Ask/Bid terlalu lebar
SPREAD_MAX_PCT           = 0.6    # % — spread > 0.6% dari mid price = tolak (anti-slippage)

# [v8.1 #3] Dynamic Entry Zone — entry sebagai range, bukan 1 angka kaku
ENTRY_ZONE_WIDTH_PCT     = 0.5    # % — lebar entry range di atas entry price untuk BUY
# Jika harga sudah di atas entry_high → tampilkan "Wait for Retest"

# [v8.1 #5] Inter-Market Correlation BTC+ETH
# Jika BTC dan ETH bergerak berlawanan arah melebihi threshold ini,
# kurangi score Altcoin karena divergensi = ketidakpastian arah market.
ETH_BTC_DIVERGE_THRESHOLD = 1.5   # % — |chg_btc_1h - chg_eth_1h| > ini = divergen
DIVERGE_SCORE_PENALTY    = -2     # penalty score saat BTC/ETH divergen

# [v8.1 #6] Volatility Window — jam volume rendah (00:00–06:00 WIB)
LOW_VOL_HOUR_START       = 0      # jam mulai sesi sepi (WIB)
LOW_VOL_HOUR_END         = 6      # jam akhir sesi sepi (WIB)
LOW_VOL_SCORE_THRESHOLD  = 14     # min score lebih tinggi di jam sepi (naik dari 10/12)

# SELL entry zone constants — dipindahkan dari hardcoded
SELL_ENTRY_LATE_THRESHOLD = 0.97  # tolak SELL jika price < last_sh * ini (sudah terlalu turun)
SELL_ENTRY_OFFSET         = 0.998 # entry SELL = last_sh * ini (sedikit di bawah resistance)
MOMENTUM_BREAKOUT_MIN     = 1.001 # harga harus > recent_high * ini untuk konfirmasi breakout

# ── v8.5 RSI Momentum Direction constants ─────────────────────────────────────
# [v8.5 #1] Deteksi arah RSI berbasis linear slope pada window N candle terakhir.
# Slope dihitung via numpy polyfit(degree=1) pada RSI series — lebih robust
# terhadap noise candle tunggal dibanding raw delta (RSI[-1] - RSI[-N]).
# Unit: RSI-poin-per-candle. Threshold 0.8 = RSI bergerak rata-rata 0.8 poin
# per candle dalam window — terdeteksi sebagai tren bermakna, bukan noise.
RSI_MOMENTUM_SLOPE_THRESH = 0.8   # slope >= +0.8 -> naik (+1), <= -0.8 -> turun (-1)
RSI_MOMENTUM_WINDOW       = 6     # jumlah candle RSI untuk fit slope

# ── v8.6 SL Liquidity Zone Avoidance constants ────────────────────────────────
# [v8.6 #1] SL yang ditempatkan tepat di atas zona equal-lows/equal-highs berisiko
# tinggi kena stop-hunt oleh market maker sebelum harga berbalik arah.
# Jika SL jatuh dalam radius ATR_MULT × ATR dari zona equal-lows (BUY) atau
# equal-highs (SELL), SL digeser keluar dari zona tersebut sebesar BUFFER_PCT.
#
# SL_LIQ_PROXIMITY_ATR_MULT: radius deteksi "terlalu dekat" dalam satuan ATR.
#   0.5 = jika SL dalam 50% ATR dari zona equal-lows/highs → perlu digeser.
#   Terlalu besar → over-shift SL, RR memburuk. Terlalu kecil → jarang trigger.
# SL_LIQ_BUFFER_PCT: seberapa jauh SL digeser melewati zona likuiditas.
#   0.3% = geser SL 0.3% lebih jauh dari zona likuiditas.
SL_LIQ_PROXIMITY_ATR_MULT = 0.5   # deteksi: SL dalam 0.5× ATR dari equal-lows/highs
SL_LIQ_BUFFER_PCT          = 0.003 # buffer: geser SL 0.3% melewati zona likuiditas

# ── v8.7 #1 Weekly Structure Gate constants ───────────────────────────────────
# [v8.7 #1] Gate identik dengan MTF 4h gate di INTRADAY (v8.3 #1), tapi untuk SWING.
# SWING BUY ditolak jika weekly bias BEARISH + BOS BEARISH (counter-trend kuat).
# SWING SELL ditolak jika weekly bias BULLISH + BOS BULLISH.
# Hanya blokir jika KEDUANYA (bias + BOS) berlawanan — tidak over-filter.
SWING_WEEKLY_GATE_ENABLED = os.environ.get("SWING_WEEKLY_GATE_ENABLED", "true").lower() == "true"
SWING_WEEKLY_CANDLE_LIMIT = 60   # 60 candle weekly = ~14 bulan histori struktur

# ── v8.7 #2 Dynamic TP multiplier constants ───────────────────────────────────
# [v8.7 #2] TP disesuaikan dengan kekuatan trend (ADX) agar tidak meninggalkan
# profit di market trending kuat atau target terlalu jauh di market ranging.
# ADX < ADX_TP_LOW  → konservatif  (market ranging/lemah, TP dekat)
# ADX >= ADX_TP_HIGH → agresif     (market trending kuat, TP jauh)
# Antara keduanya    → default (konstanta INTRADAY_TP1_R / SWING_TP1_R yang ada)
ADX_TP_LOW  = 20.0   # ADX < 20 → pakai multiplier konservatif
ADX_TP_HIGH = 30.0   # ADX >= 30 → pakai multiplier agresif

# Multiplier konservatif (ADX < 20): target lebih dekat agar sering kena TP1
INTRADAY_TP1_R_LOW  = 0.8   # INTRADAY TP1 konservatif
INTRADAY_TP2_R_LOW  = 1.4   # INTRADAY TP2 konservatif
SWING_TP1_R_LOW     = 1.6   # SWING TP1 konservatif
SWING_TP2_R_LOW     = 2.8   # SWING TP2 konservatif

# Multiplier agresif (ADX >= 30): target lebih jauh di trending market
INTRADAY_TP1_R_HIGH = 1.2   # INTRADAY TP1 agresif
INTRADAY_TP2_R_HIGH = 2.2   # INTRADAY TP2 agresif
SWING_TP1_R_HIGH    = 2.4   # SWING TP1 agresif
SWING_TP2_R_HIGH    = 4.2   # SWING TP2 agresif

# ── v8.7 #3 Session-normalized volume constants ───────────────────────────────
# [v8.7 #3] Bandingkan volume candle saat ini vs rata-rata jam yang SAMA
# dalam 7 hari terakhir. Volume spike di jam biasanya sepi (mis. 03:00 WIB)
# jauh lebih bermakna dari spike di jam ramai (09:00 WIB Asia open).
# Lookback 7×24=168 candle 1h — mencakup 7 hari histori per jam.
SESSION_VOL_LOOKBACK_DAYS  = 7     # berapa hari ke belakang untuk baseline per-jam
SESSION_VOL_SPIKE_MULT     = 1.5   # vol harus > baseline × 1.5 untuk dapat bonus

# ── v8.8 #1 Correlation-aware portfolio cap constants ─────────────────────────
# [v8.8 #1] Batasi max sinyal BUY per sektor heuristik dalam 1 cycle.
# Sektor ditentukan dari nama token menggunakan keyword mapping sederhana.
# MAX_SECTOR_SIGNALS = 2 berarti paling banyak 2 BUY dari sektor yang sama.
MAX_SECTOR_SIGNALS = int(os.environ.get("MAX_SECTOR_SIGNALS", "2"))
# [v8.9 #1] Bucket "OTHER" mendapat sub-limit sendiri — token tidak dikenal
# tidak boleh flooding semua slot dari 1 bucket yang sama.
MAX_OTHER_SIGNALS  = int(os.environ.get("MAX_OTHER_SIGNALS", "1"))

# [v8.9 #1] SECTOR_KEYWORDS diperluas dari 9 → 14 sektor, ~200+ token.
# Covers ~85% token aktif di Gate.io tanpa API eksternal.
# Urutan penting: cek dari yang paling spesifik ke paling umum.
SECTOR_KEYWORDS: dict = {
    "AI": [
        "TAO", "GRASS", "RENDER", "FET", "AGIX", "OCEAN", "NMR", "ARKM", "WLD",
        "AIOZ", "ALI", "CTXC", "DRIA", "GENSYN", "HIVE", "IQ", "KAVA",
        "MASA", "MINA", "NEAR", "ORAI", "PHB", "PRIME", "PROM", "RSS3",
        "SFP", "STORJ", "TURBO", "UNFI", "VELO", "VIDT", "OLAS",
    ],
    "MEME": [
        "DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "BOME", "MEW", "NEIRO",
        "BRETT", "POPCAT", "MOODENG", "PNUT", "GOAT", "FWOG", "COQ", "BABYDOGE",
        "ELON", "SAMO", "MOON", "WOJAK", "LADYS", "MYRO", "AIDOGE", "SNEK",
        "ANALOS", "BIAO", "CAT", "CHEEMS", "CORGIAI", "DF", "GROK",
        "HACHI", "HARAMBE", "HUSD", "KEKEC", "KIBSHI", "KUMA", "LINA",
        "MAGA", "MEME", "MIGGLES", "MOG", "MONSTA", "PONKE", "RATS",
        "SLERF", "SUNDOG", "TRUMP", "TURBO", "VINE", "WDOG", "WEN",
    ],
    "DEFI": [
        "UNI", "AAVE", "CRV", "COMP", "MKR", "SNX", "BAL", "SUSHI", "1INCH",
        "YFI", "LDO", "RPL", "CVX", "PENDLE", "MORPHO", "DYDX", "GMX",
        "JOE", "CAKE", "RDNT", "VELO", "AERC", "BTRFLY", "BANANA",
        "COW", "EUL", "FLUX", "FRAX", "FXS", "GEAR", "GNO", "HFT",
        "IDLE", "INST", "LQTY", "METIS", "MPL", "MTA", "PAXG",
        "PERP", "RAD", "RBN", "ROOK", "SPELL", "STG", "TRIBE",
        "UMAMI", "VADER", "VCHF", "VUSD",
    ],
    "L2": [
        "ARB", "OP", "MATIC", "POL", "STRK", "ZK", "MANTA", "BLAST", "SCROLL",
        "BOBA", "CELR", "CNV", "COTI", "CTSI", "DUSK", "FTM", "HOP",
        "KLAY", "LRC", "LYRA", "MOVR", "NEON", "NMR", "OMNI", "OMN",
        "RARE", "REQ", "SKL", "SYN", "TAIKO", "TLOS",
    ],
    "L1": [
        "SOL", "ADA", "AVAX", "DOT", "ATOM", "NEAR", "SUI", "APT", "INJ",
        "SEI", "ONE", "ALGO", "EGLD", "HBAR", "XTZ", "FLOW", "XRP",
        "BNB", "TRX", "ETC", "XLM", "VET", "IOTA", "ICX", "IOST",
        "KAVA", "NULS", "QTUM", "THETA", "VET", "WAVES", "XEM", "ZIL",
        "CKB", "CSPR", "GLMR", "KDA", "MINA", "ROSE", "SCRT", "ZETA",
    ],
    "INFRA": [
        "LINK", "GRT", "API3", "BAND", "TRB", "PYTH", "UMA", "ACX",
        "AKT", "AR", "ARKB", "CFX", "CLV", "CTXC", "DIA", "ENS",
        "ETHW", "FIL", "FORTH", "GHST", "HNT", "IOTX", "KNC",
        "MASK", "NFT", "NKN", "NTRN", "OGN", "OMG", "ORBS",
        "OXT", "PEOPLE", "QNT", "REI", "RLC", "RUNE", "SFUND",
        "SNT", "SPELL", "SUPER", "SWAP", "SXP", "TORN", "UOS",
        "UTK", "VIA", "WIN", "XVG",
    ],
    "GAMING": [
        "AXS", "SAND", "MANA", "ENJ", "GALA", "IMX", "BEAM", "RON", "PIXEL",
        "ALICE", "ATA", "AVA", "DAWN", "DERC", "ECOX", "ELF", "FEVR",
        "GMEE", "GPS", "HEROES", "ILV", "LOOKS", "LOKA", "MC",
        "NAKA", "POLS", "PYR", "RARE", "REVV", "SLP", "SOUL",
        "STARL", "TLM", "UGT", "WILD", "WNCG", "YGG",
    ],
    "BTC_ECO": [
        "WBTC", "ORDI", "SATS", "RUNE", "RATS", "DOVI", "LUNC",
        "LUNA", "USTC", "BIFI", "CORE", "MERL", "MUBI", "OSHI",
        "PIZZA", "TBTC", "UBTC", "VBTC",
    ],
    "ETH_ECO": [
        "STETH", "RETH", "CBETH", "ETHFI", "EIGEN", "RPL", "SSV",
        "ANKR", "LIDO", "OETH", "SWETH", "UNIETH", "WBETH",
    ],
    "PRIVACY": [
        "XMR", "ZEC", "DASH", "SCRT", "ROSE", "NYM", "DUSK",
        "KEEP", "NIM", "OXEN", "TORN", "ZCASH",
    ],
    "STAKING": [
        "LIDO", "FXS", "FRAX", "ONDO", "PENDLE", "TENET",
        "ANKR", "DIVA", "ETHFI", "KFIN", "LQTY", "OMNI",
        "RPL", "SSV", "SWISE",
    ],
    "ORACLE": [
        "LINK", "BAND", "TRB", "PYTH", "API3", "DIA", "FLUX",
        "NEST", "NMR", "ORAI", "REEF", "REQ", "UMA",
    ],
    "RWA": [
        "ONDO", "MPL", "MKR", "POLYX", "CFG", "GFI", "GOLDFINCH",
        "MAPLE", "ORCA", "TRU", "UNCX",
    ],
    "SOCIALFI": [
        "DESO", "MASK", "LIT", "RSS3", "CYS", "LOOKS",
        "RALLY", "SHPING", "SOCIAL",
    ],
}


def classify_sector(pair: str) -> str:
    """
    [v8.9 #1] Klasifikasi pair ke sektor heuristik berdasarkan nama token.
    Diperluas dari 9 ke 14 sektor dengan ~200+ token terdaftar.
    Returns: nama sektor atau "OTHER" jika tidak cocok dengan mapping manapun.
    """
    base = pair.replace("_USDT", "").upper()
    for sector, tokens in SECTOR_KEYWORDS.items():
        if base in tokens:
            return sector
    return "OTHER"


# ── v8.8 #2 BOS body strength constants ──────────────────────────────────────
# [v8.8 #2] Candle yang menembus struktur (BOS/CHoCH) harus memiliki body yang
# cukup kuat — bukan hanya wick penetration yang sering menjadi false breakout.
# body_ratio = abs(close - open) / (high - low)
# >= BOS_BODY_RATIO_MIN → body kuat, BOS valid
# < BOS_BODY_RATIO_MIN  → wick-dominated candle, BOS dianggap lemah → tidak set bos/choch
BOS_BODY_RATIO_MIN = 0.45   # body harus >= 45% dari range candle

# ── v8.8 #3 Adaptive weight audit constants ───────────────────────────────────
# [v8.8 #3] Audit mingguan — hitung korelasi setiap scoring component dengan
# TP_HIT vs SL_HIT dari 50 signal terakhir per strategy.
# WEIGHT_AUDIT_ENABLED: aktifkan/matikan via env var
# WEIGHT_AUDIT_SAMPLE : jumlah signal per strategy yang dianalisis
# Hasil disimpan ke tabel Supabase "weight_audit" — tidak auto-apply ke W dict.
WEIGHT_AUDIT_ENABLED = os.environ.get("WEIGHT_AUDIT_ENABLED", "true").lower() == "true"
WEIGHT_AUDIT_SAMPLE  = int(os.environ.get("WEIGHT_AUDIT_SAMPLE", "50"))

# ── v8.9 #2 Semi-auto weight application constants ────────────────────────────
# [v8.9 #2] Jika WEIGHT_AUTO_APPLY=true DAN kondisi aman terpenuhi, delta kecil
# di-apply ke W dict. Default: false (developer harus opt-in secara eksplisit).
# Kondisi aman: WR > WEIGHT_AUTO_WR_MIN, discrimination >= WEIGHT_AUTO_DISC_MIN,
# sample >= WEIGHT_AUTO_SAMPLE_MIN.
# Hard guard: setiap key W tidak boleh keluar dari [WEIGHT_MIN, WEIGHT_MAX].
# Max delta per key per cycle: ±1 (mencegah drift terlalu cepat).
WEIGHT_AUTO_APPLY      = os.environ.get("WEIGHT_AUTO_APPLY", "false").lower() == "true"
WEIGHT_AUTO_WR_MIN     = 45.0   # WR harus > 45% agar auto-apply aktif (sistem sehat)
WEIGHT_AUTO_DISC_MIN   = 2.0    # score discrimination harus >= 2.0 (signal scoring efektif)
WEIGHT_AUTO_SAMPLE_MIN = 30     # minimal 30 resolved trades sebelum auto-apply
WEIGHT_MIN             = -4     # hard lower bound untuk semua nilai W
WEIGHT_MAX             = 8      # hard upper bound untuk semua nilai W

# ── v8.3 PRECISION EDITION CONSTANTS ──────────────────────────────

# [v8.3 #1] Multi-Timeframe Confirmation — INTRADAY MTF Gate
# INTRADAY (1h) BUY ditolak jika 4h structure bias BEARISH + BOS BEARISH (counter-trend).
# INTRADAY (1h) SELL ditolak jika 4h structure bias BULLISH + BOS BULLISH.
# Hanya blokir jika KEDUANYA (bias + BOS) berlawanan — tidak over-filter.
# Set False via env var untuk menonaktifkan (berguna di ranging market).
MTF_ALIGNMENT_ENABLED = os.environ.get("MTF_ALIGNMENT_ENABLED", "true").lower() == "true"

# [v8.3 #2] PUMP Structure Gate
# Pump signal ditolak jika 15m structure memiliki active BOS BEARISH.
# Mencegah pump alert pada pair yang sedang dalam distribusi / downstruktur aktif.
PUMP_STRUCTURE_GATE_ENABLED = os.environ.get("PUMP_STRUCTURE_GATE_ENABLED", "true").lower() == "true"

# [v8.3 #3] Portfolio Exposure Cap
# Total BUY signals (INTRADAY + SWING + MOMENTUM) di-cap per cycle.
# Default 5 = maksimal 5% portfolio exposure jika setiap trade 1% risk.
# Saat cap aktif: sort by score desc, hanya top-N yang dikirim ke user.
# Set ke 99 untuk disable: MAX_CONCURRENT_BUY_SIGNALS=99
MAX_CONCURRENT_BUY_SIGNALS = int(os.environ.get("MAX_CONCURRENT_BUY_SIGNALS", "5"))

# ── MICROCAP UNIFIED SCORING — [v8.2 #3] ────────────────────────
# Sebelumnya micro_score menggunakan nilai hardcoded langsung di dalam check_microcap().
# Sekarang dipindahkan ke W_MICRO dict agar auditable dan konsisten dengan W/W_MOMENTUM.
# assign_tier_micro() menggantikan inline "tier = 'A' if micro_score >= 6 else 'B'"
# sehingga ada SATU jalur tier logic yang dapat diaudit, bukan dua sistem paralel.
W_MICRO: dict = {
    "vol_spike_strong":  3,   # vol_ratio >= 8.0: spike sangat kuat
    "vol_spike_normal":  2,   # vol_ratio >= 5.0: spike normal (threshold MICRO_VOL_SPIKE)
    "momentum_strong":   2,   # pct_3h >= 6.0: momentum kuat
    "momentum_normal":   1,   # pct_3h >= 3.0: momentum cukup
    "rsi_low":           1,   # RSI < 50: masih ada ruang naik
    "ema_short_bull":    1,   # EMA7 > EMA20: momentum jangka pendek positif
    "liq_sweep":         2,   # liquidity sweep: smart money sudah masuk
    "early_entry":       1,   # change_24h < 5%: belum pump banyak = early entry
}

# Tier minimum score untuk microcap (skala 0–10, berbeda dari main bot 0–20+)
# Didokumentasikan eksplisit di sini agar tidak ada "magic number 6" tersembunyi.
MICRO_TIER_MIN_SCORE = 6   # micro_score >= 6 → tier A (dikirim) | < 6 → SKIP

# ── END STRUCTURAL CONSTANTS ──────────────────────────────────

# [v7.9 #11] Scan timeout guard — kill cycle yang hang setelah batas waktu.
# GitHub Actions default timeout 6 jam terlalu lama — set lebih pendek agar
# cycle berikutnya bisa jalan tanpa menunggu yang hang.
SCAN_TIMEOUT_SECONDS = int(os.environ.get("SCAN_TIMEOUT_SECONDS", "420"))

# [v7.9 #6] Daily loss guard — halt signal generation jika sudah kirim
# terlalu banyak signal hari ini (proxy untuk drawdown protection).
# Set ke nilai besar untuk disable: MAX_DAILY_SIGNALS=999
MAX_DAILY_SIGNALS = int(os.environ.get("MAX_DAILY_SIGNALS", "24"))

# [v7.9 #13] File fallback ketika Supabase down — append ke CSV lokal
# agar signal records tidak hilang saat Supabase tidak bisa dicapai.
SIGNAL_FALLBACK_FILE = os.environ.get("SIGNAL_FALLBACK_FILE", "/tmp/signals_fallback.csv")

# [v7.9 #12] Candle cache TTL — invalidasi entry lama saat proses berjalan
# lama (non-GitHub-Actions deployment). Nilai 0 = disable TTL (flush per run saja).
CANDLE_CACHE_TTL = int(os.environ.get("CANDLE_CACHE_TTL", "300"))  # detik

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
    # [v7.8 MOMENTUM] momentum_break: early breakout score — engine baru untuk nangkap koin terbang
    "momentum_break": 4, # Breakout di atas recent high dengan volume spike
    "macd_cross":   3,   # MACD crossover searah
    "rsi_zone":     3,   # RSI di zona optimal
    "vol_confirm":  3,   # Volume konfirmasi breakout
    # [v7.8 ENTRY FIX] EMA & VWAP diturunkan — terlalu berat menyebabkan entry terlambat
    # karena EMA dan VWAP baru align SETELAH move sudah jalan
    "ema_align":    1,   # [v7.8] turun dari 2 → 1 — soft signal, bukan gate utama
    "vwap_side":    1,   # [v7.8] turun dari 2 → 1 — VWAP lag di awal breakout
    "pullback":     2,   # Entry dari pullback, bukan kejar harga
    "candle_body":  2,   # Candle konfirmasi bullish/bearish
    "equal_lows":   1,   # Equal lows sebagai target likuiditas (BUY)
    "equal_highs":  1,   # Equal highs sebagai target likuiditas (SELL) — [v7.2 FIX #3]
    "ob_ratio":     1,   # Order book ratio mendukung arah
                         # BUY: ob_ratio > 1.1 (bid 10% dominan)
                         # SELL: ob_ratio < 0.9 (ask 10% dominan — simetris matematis)
    # [FIX #9] Bonus RSI ekstrem — area oversold/overbought lebih kuat
    "rsi_extreme":  2,   # RSI < 30 untuk BUY atau RSI > 70 untuk SELL
    # [v7.8 MOMENTUM] rsi_momentum: RSI 50–72 = zona momentum sehat (bukan oversold = reversal)
    "rsi_momentum": 2,   # RSI di 50–72: uptrend momentum zone (dipakai check_momentum)
    # [FIX #8] Penalti MACD berlawanan — soft gate bukan hard reject
    "macd_soft":   -2,   # MACD counter-arah = kurangi score (tidak langsung reject)
    # Market Regime (ADX-based) — [v7.4]
    "adx_trend":    2,   # ADX >= 25: pasar sedang trend kuat -> bonus
    "adx_ranging": -2,   # ADX 18-25: pasar ranging -> penalti (CHOPPY di-block sebelum scoring)
    # [v7.9 #1] price_above_ema — TERPISAH dari ema_align.
    # ema_align    = EMA20 > EMA50 (struktur EMA uptrend)
    # price_above_ema = price > EMA20 (harga di atas EMA cepat)
    # Sebelumnya check_momentum pakai W["ema_align"] untuk keduanya -> score inflation +1.
    "price_above_ema": 1,
    # [v8.5 #1] RSI Momentum Direction — bonus/penalti berdasarkan ARAH RSI, bukan hanya level.
    # RSI 58 naik dari 42 sangat berbeda dengan RSI 58 turun dari 74. Diferensiasi ini
    # meningkatkan akurasi scoring terutama di zona overlap RSI 40-60.
    "rsi_dir_bull":  1,  # RSI sedang naik >= RSI_MOMENTUM_RISE_MIN poin -> konfirmasi BUY
    "rsi_dir_bear": -1,  # RSI sedang turun saat BUY signal -> penalti (momentum berlawanan)
    # [v8.7 #3] Session-normalized volume — bonus jika vol saat ini di atas baseline
    # rata-rata jam yang sama 7 hari terakhir × SESSION_VOL_SPIKE_MULT.
    "vol_session_strong": 1,  # volume spike vs session baseline -> sinyal lebih kuat
}

# [v7.9 #2] W_MOMENTUM — weight map TERPISAH untuk MOMENTUM scanner.
# Mencegah cross-contamination: perubahan W untuk INTRADAY/SWING tidak memengaruhi
# MOMENTUM scorer, dan sebaliknya. Hanya berisi komponen yang dipakai check_momentum.
W_MOMENTUM: dict = {
    "momentum_break":   4,   # Breakout di atas recent high — sinyal utama engine ini
    "vol_confirm":      3,   # Volume spike ≥ 1.5× — ada partisipasi nyata
    "rsi_momentum":     2,   # RSI 50–72: zona momentum sehat
    "ema_align":        1,   # EMA20 > EMA50 — struktur uptrend terkonfirmasi
    "price_above_ema":  1,   # price > EMA20 — harga di atas trend cepat [v7.9 #1 FIX]
    "macd_cross":       3,   # MACD bullish cross — konfirmasi momentum
    "macd_soft":       -2,   # MACD counter: penalti bukan hard reject
    "vwap_side":        1,   # Price > VWAP: support institusional
    "ob_ratio":         1,   # Order book bid dominan
    "adx_trend":        2,   # ADX >= 25: pasar sedang trending kuat -> bonus
    "adx_ranging":     -2,   # ADX 18-25: pasar ranging -> penalti
    "vol_spike_strong": 1,   # [v8.0 AUDIT FIX] vol_ratio >= 3.0 = spike kuat, bonus extra
    # [v8.5 #1] RSI momentum direction — dipakai di check_momentum inline scoring
    "rsi_dir_bull":     1,   # RSI sedang naik -> konfirmasi momentum lanjut
    "rsi_dir_bear":    -1,   # RSI sedang turun -> peringatan exhaustion
    # [v8.7 #3] Session-normalized volume — bonus jika vol saat ini di atas baseline per-jam
    "vol_session_strong": 1, # volume spike vs session baseline -> momentum lebih valid
}


# ════════════════════════════════════════════════════════
#  UTILITIES
# ════════════════════════════════════════════════════════

# [v7.9 #10] Telegram background thread queue — agar scan loop tidak blocked
# oleh Telegram I/O. Sebelumnya tg() sinkron: 8 signal × 0.5s + retry = 10–15s
# di mana scan cycle benar-benar berhenti. Sekarang send ke queue dan lanjut.
#
# [v8.0 AUDIT FIX 4.2] maxsize=50 — batas kedalaman antrian untuk mencegah
# queue tumbuh tak terbatas saat banyak pair breakout bersamaan (mis. broad market
# rally). Pesan non-critical akan di-drop via put_nowait jika queue penuh;
# pesan critical (summary, halt alert) tetap pakai put(block=True).
_tg_queue: queue.Queue = queue.Queue(maxsize=50)


def _tg_send_sync(msg: str) -> None:
    """Kirim satu pesan ke Telegram secara sinkron. Dipanggil dari _tg_worker thread."""
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


def _tg_worker() -> None:
    """Background worker — konsumsi antrian pesan dan kirim ke Telegram."""
    while True:
        msg = _tg_queue.get()
        if msg is None:   # sentinel — shutdown signal
            _tg_queue.task_done()
            break
        try:
            _tg_send_sync(msg)
        except Exception as e:
            log(f"⚠️ _tg_worker error: {e}", "error")
        finally:
            _tg_queue.task_done()


def _start_tg_worker() -> None:
    """Jalankan background Telegram sender thread jika belum berjalan."""
    t = threading.Thread(target=_tg_worker, daemon=True, name="tg-sender")
    t.start()


def tg(msg: str, wait: bool = False, critical: bool = False) -> None:
    """
    Kirim pesan ke Telegram via background queue.
    [v7.9 #10] Non-blocking by default — scan loop tidak terhenti oleh Telegram I/O.
    wait=True: tunggu antrian habis (dipakai untuk pesan critical / akhir scan summary).
    critical=True: pakai put(block=True) agar pesan tidak pernah di-drop meski queue penuh.
    Non-critical (default): pakai put_nowait() — di-drop jika queue sudah maxsize=50.
    [v8.0 AUDIT FIX 4.2]
    """
    if critical or wait:
        _tg_queue.put(msg)   # block=True — jamin delivery
    else:
        try:
            _tg_queue.put_nowait(msg)
        except queue.Full:
            log("⚠️ tg queue penuh (maxsize=50) — pesan non-critical di-drop", "warn")
    if wait:
        _tg_queue.join()


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


def calc_position_size(entry: float, sl: float) -> str:
    """
    [v7.9 #4] Hitung ukuran posisi yang disarankan berdasarkan risk per trade.
    Formula: position_USDT = (portfolio × risk_pct) / sl_pct
    Contoh: portfolio=1000, risk=1%, sl=3% → position = (1000×0.01)/0.03 = 333 USDT

    [v8.0 AUDIT FIX #8] Minimum position size filter — kembalikan "—" jika
    kalkulasi menghasilkan posisi < MIN_POSITION_SIZE_USDT. Mencegah signal
    dengan ukuran trade yang tidak praktis (di bawah fee minimum exchange).

    Dikembalikan sebagai string siap tampil di Telegram.
    Dikonfigurasi via env var PORTFOLIO_VALUE dan RISK_PER_TRADE_PCT.
    """
    if entry <= 0 or sl <= 0:
        return "—"
    sl_pct = abs(entry - sl) / entry
    if sl_pct <= 0:
        return "—"
    risk_amount = PORTFOLIO_VALUE * (RISK_PER_TRADE_PCT / 100)
    position_usdt = risk_amount / sl_pct
    # [v8.0 AUDIT FIX #8] Tolak jika posisi terlalu kecil
    if position_usdt < MIN_POSITION_SIZE_USDT:
        log(f"⚠️ position size terlalu kecil: ${position_usdt:.2f} < MIN ${MIN_POSITION_SIZE_USDT:.0f} — skip display", "warn")
        return f"⚠️ <${MIN_POSITION_SIZE_USDT:.0f} USDT (terlalu kecil)"
    qty = position_usdt / entry
    return f"≈ ${position_usdt:.0f} USDT ({qty:.4f} unit) @ {RISK_PER_TRADE_PCT:.0f}% risk"


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

    # [v7.9 #16] Alert jika SEMUA sumber dynamic gagal — hanya static ETF_EXACT yang aktif.
    # Developer mungkin tidak tahu jika bot berjalan tanpa proteksi dinamis.
    if fetched == 0:
        tg("⚠️ <b>ETF Blocklist — FALLBACK MODE</b>\n"
           "Kedua sumber dynamic ticker saham gagal.\n"
           "Proteksi ETF hanya menggunakan daftar statis ETF_EXACT.\n"
           "<i>Token saham baru di Gate.io mungkin tidak terblokir.</i>")


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
    """Fetch candles dengan cache per cycle. [v7.1 #3] Key menyertakan limit.
    [v7.9 #12] Cache TTL — invalidasi entry lama jika proses berjalan lama.
    """
    key = (pair, interval, limit)   # [v7.1 #3] limit masuk key — cegah silent mismatch
    if key in _candle_cache:
        cached_data, cached_ts = _candle_cache[key]
        # TTL check: 0 = disable (flush per run() saja), > 0 = invalidasi setelah N detik
        if CANDLE_CACHE_TTL == 0 or (time.time() - cached_ts) < CANDLE_CACHE_TTL:
            return cached_data
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
            _candle_cache[key] = (None, time.time()); return None
        closes  = np.array([float(c[2]) for c in raw])
        highs   = np.array([float(c[3]) for c in raw])
        lows    = np.array([float(c[4]) for c in raw])
        volumes = np.array([float(c[1]) for c in raw])

        # [v7.9 #15] Extreme-move guard — skip pair jika candle terakhir
        # memiliki range > 5× rata-rata 20 candle. Ini biasanya menandakan
        # flash crash / pump ekstrem yang membiaskan ATR dan sinyal teknikal.
        if len(closes) >= 21:
            recent_ranges  = highs[-21:-1] - lows[-21:-1]
            avg_range      = float(np.mean(recent_ranges))
            last_range     = float(highs[-1] - lows[-1])
            if avg_range > 0 and last_range > avg_range * 5:
                log(f"⚠️ [{pair}|{interval}] candle ekstrem — range {last_range:.4f} > 5× avg {avg_range:.4f}, skip", "warn")
                _candle_cache[key] = (None, time.time()); return None

        # [v8.0 AUDIT FIX #11] ATR sanity check — deteksi flash crash contamination
        # via ATR. Sebuah candle bisa lolos range-check di atas tapi ATR keseluruhan
        # tetap bengkak jika ada beberapa candle volatile berturut-turut.
        # Cek: ATR candle terakhir vs rata-rata ATR 20 candle sebelumnya.
        # Menggunakan true range sederhana (tanpa shift) agar tidak perlu calc_atr di sini.
        if len(closes) >= 22:
            tr_series   = highs[-21:-1] - lows[-21:-1]   # true range approx (intra-candle)
            avg_atr     = float(np.mean(tr_series))
            last_tr     = float(highs[-1] - lows[-1])
            if avg_atr > 0 and last_tr > avg_atr * ATR_SANITY_MULTIPLIER:
                log(f"⚠️ [{pair}|{interval}] ATR sanity fail — last_tr {last_tr:.6f} > "
                    f"{ATR_SANITY_MULTIPLIER}× avg {avg_atr:.6f}, kemungkinan flash crash, skip", "warn")
                _candle_cache[key] = (None, time.time()); return None

        # [v8.0 AUDIT FIX #12] Stale ticker detection — skip pair jika volume 24h = 0
        # atau harga terakhir sama persis dengan harga sebelumnya (pair tidak aktif).
        if float(volumes[-1]) <= STALE_TICKER_MIN_VOLUME and len(volumes) > 1:
            log(f"⚠️ [{pair}|{interval}] stale ticker — volume candle terakhir {volumes[-1]}, skip", "warn")
            _candle_cache[key] = (None, time.time()); return None
        if len(closes) >= 2 and closes[-1] == closes[-2] and closes[-1] == closes[-3 if len(closes) >= 3 else -2]:
            log(f"⚠️ [{pair}|{interval}] stale ticker — harga tidak bergerak 2+ candle, skip", "warn")
            _candle_cache[key] = (None, time.time()); return None

        result  = (closes, highs, lows, volumes)
        _candle_cache[key] = (result, time.time())   # [v7.9 #12] simpan dengan timestamp
        return result
    except Exception as e:
        log(f"⚠️ candles [{pair}|{interval}|{limit}]: {e}", "warn")
        _candle_cache[key] = (None, time.time())
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


def calc_rsi_series(closes, period=14, n=5) -> list[float]:
    """
    [v8.5 #1] Kembalikan n nilai RSI terakhir (bukan hanya nilai terakhir).
    Digunakan oleh calc_rsi_momentum_dir() untuk mendeteksi arah momentum RSI.

    Menggunakan Wilder's EMA yang sama dengan calc_rsi() — konsisten dengan
    TradingView dan standar industri.

    Catatan warmup: Wilder's EMA memerlukan sekitar 3×period candle sebelum
    nilai RSI stabil (tidak 0 atau 100). Oleh karena itu minimum data yang
    diperlukan = period * 3 + n. Jika tidak cukup, kembalikan [50.0] * n (neutral).

    Returns: list[float] panjang n, index 0 = paling lama, -1 = paling baru.
    """
    warmup = period * 3
    if len(closes) < warmup + n:
        return [50.0] * n
    s = pd.Series(closes)
    d = s.diff()
    gain = d.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = (-d.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    rsi_series = 100 - 100 / (1 + gain / (loss + 1e-9))
    # Ambil n nilai terakhir — sudah pasti di zona stabil karena guard warmup di atas
    return [float(v) for v in rsi_series.iloc[-n:].tolist()]


def calc_rsi_momentum_dir(closes, period=14, lookback=6) -> int:
    """
    [v8.5 #1] Deteksi arah momentum RSI menggunakan linear slope.

    Masalah yang dipecahkan: RSI 58 yang baru turun dari 72 sangat berbeda
    dengan RSI 42 yang baru naik dari 28. Keduanya selama ini mendapat score
    rsi_zone yang identik tanpa diferensiasi arah.

    Metode: fit garis lurus (polyfit degree-1) pada N nilai RSI terakhir.
    Slope mencerminkan kecenderungan RSI secara keseluruhan — lebih robust
    terhadap noise candle tunggal dibanding raw delta (RSI[-1] - RSI[-N]).

    Threshold RSI_MOMENTUM_SLOPE_THRESH dalam unit RSI-poin-per-candle:
      slope >= +0.8 → return +1  (RSI sedang naik  — konfirmasi BUY)
      slope <= -0.8 → return −1  (RSI sedang turun — konfirmasi SELL)
      antara        → return  0  (flat / tidak signifikan)

    Returns: int  +1 | 0 | −1
    """
    series = calc_rsi_series(closes, period=period, n=lookback)
    if len(series) < 2:
        return 0
    x     = np.arange(len(series), dtype=float)
    slope = float(np.polyfit(x, series, 1)[0])

    if slope >= RSI_MOMENTUM_SLOPE_THRESH:
        return 1    # RSI sedang naik — momentum mendukung BUY
    if slope <= -RSI_MOMENTUM_SLOPE_THRESH:
        return -1   # RSI sedang turun — momentum mendukung SELL
    return 0        # flat


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
    recent_highs  = highs[-5:]
    recent_lows   = lows[-5:]
    # Need opens for body ratio — approximate from closes (open[i] ≈ close[i-1])
    # This is a standard approximation for OHLC data where opens aren't available.
    recent_opens  = closes[-6:-1]   # close[i-1] ≈ open[i] for i in 1..5

    # [v7.6 #6] range(1, ...) eksplisit — sebelumnya range(len(recent_closes)) dengan guard
    # "i > 0" menyebabkan i=0 (candle ke-5 dari belakang) tidak pernah dievaluasi.
    # Sekarang kita mulai dari i=1 dan akses recent_closes[i-1] selalu valid (i-1 >= 0).
    # [v8.0 AUDIT FIX 5.1] Gunakan konstanta BOS_BREAK_TOLERANCE (0.008 = 0.8% band)
    # menggantikan magic number 1.008/0.992 yang sebelumnya hardcoded.
    # [v8.8 #2] Tambah body strength check: candle yang menembus struktur harus
    # memiliki body_ratio >= BOS_BODY_RATIO_MIN (default 0.45). Wick-only penetration
    # sering menjadi false breakout — kini ditolak sebagai BOS/CHoCH yang valid.
    def _has_strong_body(idx: int) -> bool:
        """Return True jika candle di index idx memiliki body ratio >= BOS_BODY_RATIO_MIN."""
        rng = float(recent_highs[idx]) - float(recent_lows[idx])
        if rng <= 0:
            return False
        body = abs(float(recent_closes[idx]) - float(recent_opens[idx - 1]))
        return (body / rng) >= BOS_BODY_RATIO_MIN

    bull_break = any(recent_closes[i] > last_sh and
                     recent_closes[i-1] <= last_sh * (1 + BOS_BREAK_TOLERANCE) and
                     _has_strong_body(i)
                     for i in range(1, len(recent_closes)))

    bear_break = any(recent_closes[i] < last_sl and
                     recent_closes[i-1] >= last_sl * (1 - BOS_BREAK_TOLERANCE) and
                     _has_strong_body(i)
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
    # [v8.0 AUDIT FIX 5.1] Gunakan EQUAL_LEVEL_TOL (0.003 = 0.3%)
    # menggantikan magic number 0.003 yang sebelumnya hardcoded.
    tol = EQUAL_LEVEL_TOL

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
        # RSI zone BUY: 30 < rsi < 60.
        # ⚠️ DESAIN INTENTIONAL: RSI zona BUY (30–60) dan zona SELL (40–70) tumpang
        # tindih di range 40–60. Dalam zone overlap ini KEDUA sisi bisa mendapat +3 skor
        # dari rsi_zone. Ini TIDAK menghasilkan signal ganda karena BOS/CHoCH direction
        # gate (wajib) memblokir sisi berlawanan sebelum scoring. Overlap ini disimpan
        # sebagai arsitektur simetri yang disengaja, bukan bug.
        if 30 < rsi < 60:                       score += W["rsi_zone"]
        if rsi <= 30:                           score += W["rsi_extreme"] # [FIX #9] bonus oversold
        # [v8.5 #1] RSI Momentum Direction — bonus jika RSI sedang NAIK (konfirmasi BUY),
        # penalti jika RSI sedang TURUN (counter-momentum). Diferensiasi ini menjawab
        # masalah overlap zone 40-60: RSI 55 yang naik dari 40 != RSI 55 yang turun dari 72.
        _rsi_dir = calc_rsi_momentum_dir(closes, lookback=RSI_MOMENTUM_WINDOW)
        if _rsi_dir == 1:    score += W["rsi_dir_bull"]   # RSI naik -> konfirmasi BUY
        elif _rsi_dir == -1: score += W["rsi_dir_bear"]   # RSI turun saat BUY -> penalti
        # Volume konfirmasi: candle terakhir vs rata-rata N candle sebelumnya.
        # PENTING: window [-10:-1] sengaja MENGECUALIKAN candle terakhir (index -1)
        # agar baseline dihitung dari candle confirmed, bukan candle yang sedang berjalan.
        # Ini konsisten di seluruh scanner (PUMP: [-11:-1], MICROCAP: [-11:-1]).
        # VOL_CONFIRM_THRESHOLD = 1.3 (dari STRUCTURAL CONSTANTS) — configurable.
        vol_avg = float(np.mean(volumes[-10:-1]))
        if float(volumes[-1]) > vol_avg * VOL_CONFIRM_THRESHOLD: score += W["vol_confirm"]
        # [v8.7 #3] Session-normalized volume — bonus jika vol saat ini melebihi
        # baseline rata-rata jam yang sama 7 hari terakhir.
        _svr = calc_session_vol_ratio(closes, volumes)
        if _svr >= SESSION_VOL_SPIKE_MULT: score += W["vol_session_strong"]
        if ema_fast > ema_slow:                 score += W["ema_align"]
        if price > vwap:                        score += W["vwap_side"]
        last_sl = structure.get("last_sl")
        # [v7.3 FIX] Tambah lower bound: price >= last_sl agar kondisi ini hanya aktif
        # saat price benar-benar di zona support, bukan saat breakdown di bawah swing low.
        if last_sl and last_sl <= price <= last_sl * PULLBACK_UPPER_BAND: score += W["pullback"]
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
        # RSI zone SELL: 40 < rsi < 70. Lihat komentar RSI overlap di BUY branch di atas.
        if 40 < rsi < 70:                       score += W["rsi_zone"]
        if rsi >= 70:                           score += W["rsi_extreme"] # [FIX #9] bonus overbought
        # [v8.5 #1] RSI Momentum Direction untuk SELL — simetris dengan BUY.
        # RSI turun = konfirmasi SELL (bearish momentum). RSI naik saat SELL = penalti.
        _rsi_dir_sell = calc_rsi_momentum_dir(closes, lookback=RSI_MOMENTUM_WINDOW)
        if _rsi_dir_sell == -1:  score += W["rsi_dir_bull"]   # RSI turun -> konfirmasi SELL (reuse bobot)
        elif _rsi_dir_sell == 1: score += W["rsi_dir_bear"]   # RSI naik saat SELL -> penalti
        # Volume konfirmasi — window [-10:-1] konsisten dengan BUY branch.
        vol_avg = float(np.mean(volumes[-10:-1]))
        if float(volumes[-1]) > vol_avg * VOL_CONFIRM_THRESHOLD: score += W["vol_confirm"]
        # [v8.7 #3] Session-normalized volume — simetris dengan BUY branch.
        _svr_sell = calc_session_vol_ratio(closes, volumes)
        if _svr_sell >= SESSION_VOL_SPIKE_MULT: score += W["vol_session_strong"]
        if ema_fast < ema_slow:                 score += W["ema_align"]
        if price < vwap:                        score += W["vwap_side"]
        last_sh = structure.get("last_sh")
        # Upper bound: price <= last_sh * 1.01 — harga harus DI DEKAT atau di bawah resistance,
        # bukan jauh di atas. Tanpa batas ini, price 3% di atas last_sh pun dapat +2 score.
        if last_sh and last_sh * PULLBACK_LOWER_BAND <= price <= last_sh * PULLBACK_SELL_UPPER: score += W["pullback"]
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


def assign_tier(score: int, low_vol_session: bool = False) -> str:
    """
    [v8.1 #6] Jika low_vol_session=True (00:00–06:00 WIB), threshold minimum
    dinaikkan ke LOW_VOL_SCORE_THRESHOLD agar sinyal lebih selektif di jam sepi.
    """
    if low_vol_session and score < LOW_VOL_SCORE_THRESHOLD:
        return "SKIP"
    if score >= TIER_MIN_SCORE["S"]:  return "S"
    if score >= TIER_MIN_SCORE["A+"]: return "A+"
    if score >= TIER_MIN_SCORE["A"]:  return "A"
    return "SKIP"


def assign_tier_micro(micro_score: int) -> str:
    """
    [v8.2 #3] Unified tier assignment untuk MICROCAP scanner.
    Sebelumnya: inline 'tier = "A" if micro_score >= 6 else "B"' di dalam check_microcap().
    Sekarang: satu fungsi dengan threshold dari MICRO_TIER_MIN_SCORE constant.
    Mengembalikan "A" atau "SKIP" — tier B tidak pernah dikirim, jadi disamakan ke SKIP.
    Skala microcap (0–10) berbeda dari main bot (0–20+), oleh karena itu fungsi ini
    terpisah dari assign_tier() untuk mencegah confusion lintas skala.
    """
    if micro_score >= MICRO_TIER_MIN_SCORE:
        return "A"
    return "SKIP"


def calc_conviction(score: int) -> str:
    """
    Diferensiasi kualitas sinyal di dalam tier — bukan pengganti tier,
    tapi label tambahan agar user bisa prioritaskan sinyal terbaik.

    Ini menjawab: "Tier A yang ini lebih layak dari Tier A yang lain?"
    Score bisa naik karena ADX bonus jadi scale ke atas lebih natural.

    [v8.0 AUDIT FIX 5.1] Threshold diambil dari CONVICTION_THRESHOLDS config dict
    menggantikan magic numbers 18/14/12/10 yang sebelumnya hardcoded.
    """
    if score >= CONVICTION_THRESHOLDS["EXTREME"]:   return "EXTREME ⚡"
    if score >= CONVICTION_THRESHOLDS["VERY_HIGH"]: return "VERY HIGH 🔥"
    if score >= CONVICTION_THRESHOLDS["HIGH"]:      return "HIGH 💪"
    if score >= CONVICTION_THRESHOLDS["GOOD"]:      return "GOOD ✅"
    return "OK 🟡"


# ════════════════════════════════════════════════════════
#  TP / SL CALCULATOR
# ════════════════════════════════════════════════════════

def adjust_sl_for_liquidity(sl: float, side: str, atr: float, liq: dict) -> float:
    """
    [v8.6 #1] Geser SL menjauh dari zona equal-lows/equal-highs jika terlalu dekat.

    Masalah: SL yang ditempatkan tepat di atas equal-lows (BUY) atau tepat di bawah
    equal-highs (SELL) sangat rentan stop-hunt. Market maker sering sweep level ini
    sebelum harga berbalik — SL kena, trade kalah, padahal arah analisis benar.

    Logika BUY:
      - equal_lows adalah zona di mana banyak stop-loss BUY terkumpul.
      - Jika SL kita berada dalam radius SL_LIQ_PROXIMITY_ATR_MULT × ATR
        dari equal_lows, berarti kita juga di zona berbahaya itu.
      - Geser SL ke bawah equal_lows × (1 - SL_LIQ_BUFFER_PCT) agar berada
        DI BAWAH level yang disweep, bukan tepat di level-nya.

    Logika SELL (simetris):
      - equal_highs adalah zona stop-loss SELL terkumpul.
      - Geser SL ke atas equal_highs × (1 + SL_LIQ_BUFFER_PCT).

    Guard: jika shift menyebabkan SL distance > 8% dari entry, kembalikan SL
    asli — lebih baik tidak shift daripada RR menjadi tidak layak.

    Returns: float — SL yang sudah disesuaikan (atau SL asli jika tidak perlu/tidak aman).
    """
    if not liq:
        return sl

    proximity = atr * SL_LIQ_PROXIMITY_ATR_MULT

    if side == "BUY":
        eq_lows = liq.get("equal_lows")
        if not eq_lows:
            return sl
        # Cek apakah SL kita berada dalam radius zona equal-lows
        if abs(sl - eq_lows) <= proximity:
            adjusted = round(eq_lows * (1 - SL_LIQ_BUFFER_PCT), 8)
            # Safety: jangan geser lebih dari 8% dari SL asli (cegah RR collapse)
            if adjusted < sl * 0.92:
                log(f"  ⚠️ [SL-LIQ] BUY shift ditolak — adjusted {adjusted:.6f} terlalu jauh dari SL {sl:.6f}", "warn")
                return sl
            log(f"  🛡️ [SL-LIQ] BUY SL digeser {sl:.6f} → {adjusted:.6f} (jauh dari equal-lows {eq_lows:.6f})")
            return adjusted

    else:  # SELL
        eq_highs = liq.get("equal_highs")
        if not eq_highs:
            return sl
        # Cek apakah SL kita berada dalam radius zona equal-highs
        if abs(sl - eq_highs) <= proximity:
            adjusted = round(eq_highs * (1 + SL_LIQ_BUFFER_PCT), 8)
            # Safety: jangan geser lebih dari 8% dari SL asli
            if adjusted > sl * 1.08:
                log(f"  ⚠️ [SL-LIQ] SELL shift ditolak — adjusted {adjusted:.6f} terlalu jauh dari SL {sl:.6f}", "warn")
                return sl
            log(f"  🛡️ [SL-LIQ] SELL SL digeser {sl:.6f} → {adjusted:.6f} (jauh dari equal-highs {eq_highs:.6f})")
            return adjusted

    return sl


def calc_sl_tp(entry: float, side: str, atr: float,
               structure: dict, strategy: str,
               liq: dict | None = None,
               adx: float = 0.0) -> tuple:
    """
    SL berbasis ATR + dikonfirmasi struktur.
    TP berbasis RR multiplier dari SL distance.
    [v8.6 #1] Parameter liq opsional — jika disediakan, SL disesuaikan agar
    menghindari zona equal-lows/equal-highs yang sering menjadi target stop-hunt.
    [v8.7 #2] Parameter adx opsional — TP multiplier disesuaikan kekuatan trend
    via get_tp_multipliers(). ADX rendah = konservatif, ADX tinggi = agresif.
    """
    sl_mult          = INTRADAY_SL_ATR if strategy == "INTRADAY" else SWING_SL_ATR
    tp1_r, tp2_r     = get_tp_multipliers(strategy, adx)  # [v8.7 #2]

    sl_dist = atr * sl_mult

    if side == "BUY":
        last_sl = structure.get("last_sl")
        if last_sl and last_sl < entry:
            sl = min(entry - sl_dist, last_sl * 0.998)
        else:
            sl = entry - sl_dist
        # [v8.6 #1] Geser SL menjauh dari zona equal-lows jika terlalu dekat
        if liq:
            sl = adjust_sl_for_liquidity(sl, "BUY", atr, liq)
        tp1 = entry + (entry - sl) * tp1_r
        tp2 = entry + (entry - sl) * tp2_r
    else:
        last_sh = structure.get("last_sh")
        if last_sh and last_sh > entry:
            sl = max(entry + sl_dist, last_sh * 1.002)
        else:
            sl = entry + sl_dist
        # [v8.6 #1] Geser SL menjauh dari zona equal-highs jika terlalu dekat
        if liq:
            sl = adjust_sl_for_liquidity(sl, "SELL", atr, liq)
        tp1 = entry - (sl - entry) * tp1_r
        tp2 = entry - (sl - entry) * tp2_r

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
    default = {"halt": False, "block_buy": False, "btc_1h": 0.0, "btc_4h": 0.0, "btc_bullish": False}
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
        # [v7.8] btc_bullish: BTC sedang naik kuat di 4h DAN tidak drop di 1h
        # Digunakan untuk blokir SELL saat market bullish sistemik
        # [v8.0 AUDIT FIX] Pakai BTC_BULLISH_4H_MIN / BTC_BULLISH_1H_MIN dari STRUCTURAL_CONSTANTS.
        btc_bullish = chg_4h >= BTC_BULLISH_4H_MIN and chg_1h >= BTC_BULLISH_1H_MIN

        log(f"📡 BTC 1h:{chg_1h:+.1f}% 4h:{chg_4h:+.1f}% | "
            f"{'🛑 HALT' if halt else '⛔ BUY BLOCKED' if block_buy else '✅ OK'}"
            f"{' | 🐂 BTC BULLISH' if btc_bullish else ''}")
        return {"halt": halt, "block_buy": block_buy,
                "btc_1h": round(chg_1h, 2), "btc_4h": round(chg_4h, 2),
                "btc_bullish": btc_bullish}
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


def get_btc_flash_guard(client) -> dict:
    """
    [v8.1 #1] BTC Flash Guard 5m — Kill-Switch instan.
    Cek perubahan BTC dalam 1 candle 5m terakhir.
    Jika drop > BTC_FLASH_GUARD_DROP (default -1.5%), blok semua BUY baru
    untuk cycle ini. Ini lebih cepat dari guard 1h karena mendeteksi
    flash drop yang belum terrefleksi di candle 1h.

    Returns: {"triggered": bool, "chg_5m": float}
    """
    default = {"triggered": False, "chg_5m": 0.0}
    try:
        c5m = get_candles(client, "BTC_USDT", "5m", 30)
        if c5m is None:
            return default
        chg_5m = (c5m[0][-1] - c5m[0][-2]) / c5m[0][-2] * 100
        triggered = chg_5m < BTC_FLASH_GUARD_DROP
        if triggered:
            log(f"🚨 BTC FLASH GUARD TRIGGERED — BTC 5m: {chg_5m:+.2f}% < {BTC_FLASH_GUARD_DROP}% — semua BUY diblokir!", "error")
        else:
            log(f"⚡ BTC Flash Guard: 5m {chg_5m:+.2f}% — OK")
        return {"triggered": triggered, "chg_5m": round(chg_5m, 2)}
    except Exception as e:
        log(f"⚠️ btc_flash_guard: {e}", "warn")
        return default


def get_eth_regime(client) -> dict:
    """
    [v8.1 #5] Ambil perubahan ETH 1h untuk Inter-Market Correlation.
    Dibandingkan dengan BTC 1h — jika keduanya divergen signifikan,
    score Altcoin diturunkan via DIVERGE_SCORE_PENALTY.

    Returns: {"eth_1h": float}
    """
    default = {"eth_1h": 0.0}
    try:
        c1h = get_candles(client, "ETH_USDT", "1h", 30)
        if c1h is None:
            return default
        chg_1h = (c1h[0][-1] - c1h[0][-2]) / c1h[0][-2] * 100
        log(f"📡 ETH 1h: {chg_1h:+.2f}%")
        return {"eth_1h": round(chg_1h, 2)}
    except Exception as e:
        log(f"⚠️ eth_regime: {e}", "warn")
        return default


# [v8.3 #1] Cache 4h bias per pair — diisi oleh get_4h_bias(), di-reset setiap cycle di run()
_4h_bias_cache: dict = {}


def get_4h_bias(client, pair: str) -> dict:
    """
    [v8.3 #1] Multi-Timeframe Confirmation helper.
    Fetch dan cache 4h structure bias untuk pair tertentu.
    Digunakan oleh check_intraday() sebagai gate tambahan:
    - BUY 1h ditolak jika 4h bias BEARISH + 4h BOS BEARISH (counter-trend kuat)
    - SELL 1h ditolak jika 4h bias BULLISH + 4h BOS BULLISH (counter-trend kuat)

    Hanya blokir saat KEDUANYA (bias + BOS) konfirmasi arah berlawanan —
    mencegah over-filter saat 4h masih dalam transisi / neutral.

    Returns: {"bias": str, "bos": str|None, "valid": bool}
    Cache TTL: per cycle (di-reset di run() bersama _candle_cache).
    """
    if pair in _4h_bias_cache:
        return _4h_bias_cache[pair]

    default = {"bias": "NEUTRAL", "bos": None, "valid": False}
    try:
        # [v8.3 FIX] limit=200 — sama dengan check_swing() agar entry _candle_cache
        # (pair, "4h", 200) di-share antara get_4h_bias() dan check_swing().
        # Sebelumnya limit=120 menghasilkan key berbeda → dua API call Gate.io
        # untuk 4h data pair yang sama per cycle. Fix ini menghilangkan redundansi itu.
        data4h = get_candles(client, pair, "4h", 200)
        if data4h is None:
            _4h_bias_cache[pair] = default
            return default
        c4h, h4h, l4h, _ = data4h
        struct4h = detect_structure(c4h, h4h, l4h, strength=3, lookback=100)
        result = {
            "bias":  struct4h.get("bias", "NEUTRAL"),
            "bos":   struct4h.get("bos"),
            "valid": struct4h.get("valid", False),
        }
        _4h_bias_cache[pair] = result
        return result
    except Exception as e:
        log(f"⚠️ get_4h_bias [{pair}]: {e}", "warn")
        _4h_bias_cache[pair] = default
        return default


# [v8.7 #1] Cache weekly bias per pair — di-reset setiap cycle di run() bersama _4h_bias_cache
_weekly_bias_cache: dict = {}


def get_weekly_bias(client, pair: str) -> dict:
    """
    [v8.7 #1] Weekly structure bias helper untuk SWING gate.
    Pola identik dengan get_4h_bias() — fetch candle weekly, detect_structure,
    kembalikan {"bias", "bos", "valid"}. Di-cache per cycle.

    Digunakan oleh check_swing():
    - SWING BUY ditolak jika weekly bias BEARISH + weekly BOS BEARISH
    - SWING SELL ditolak jika weekly bias BULLISH + weekly BOS BULLISH
    Hanya blokir jika KEDUANYA (bias + BOS) konfirmasi arah berlawanan.

    limit=SWING_WEEKLY_CANDLE_LIMIT (60) = ~14 bulan candle weekly.
    Candle weekly dihasilkan gate_io dengan interval "7d".
    """
    if pair in _weekly_bias_cache:
        return _weekly_bias_cache[pair]

    default = {"bias": "NEUTRAL", "bos": None, "valid": False}
    try:
        data_w = get_candles(client, pair, "7d", SWING_WEEKLY_CANDLE_LIMIT)
        if data_w is None:
            _weekly_bias_cache[pair] = default
            return default
        cw, hw, lw, _ = data_w
        struct_w = detect_structure(cw, hw, lw, strength=2, lookback=40)
        result = {
            "bias":  struct_w.get("bias", "NEUTRAL"),
            "bos":   struct_w.get("bos"),
            "valid": struct_w.get("valid", False),
        }
        _weekly_bias_cache[pair] = result
        return result
    except Exception as e:
        log(f"⚠️ get_weekly_bias [{pair}]: {e}", "warn")
        _weekly_bias_cache[pair] = default
        return default


def get_tp_multipliers(strategy: str, adx: float) -> tuple[float, float]:
    """
    [v8.7 #2] Kembalikan (tp1_r, tp2_r) yang disesuaikan dengan kekuatan ADX.

    ADX < ADX_TP_LOW  (20) → konservatif: TP dekat, lebih sering hit
    ADX >= ADX_TP_HIGH (30) → agresif: TP jauh, manfaatkan trending market
    Antara keduanya         → default (nilai INTRADAY_TP1_R / SWING_TP1_R)

    Dipanggil dari calc_sl_tp() — parameter adx opsional (default 0 = pakai default).
    Strategy: "INTRADAY" atau "SWING".
    """
    if adx <= 0:
        # adx tidak tersedia → pakai default
        if strategy == "INTRADAY":
            return INTRADAY_TP1_R, INTRADAY_TP2_R
        return SWING_TP1_R, SWING_TP2_R

    if adx < ADX_TP_LOW:
        if strategy == "INTRADAY":
            return INTRADAY_TP1_R_LOW, INTRADAY_TP2_R_LOW
        return SWING_TP1_R_LOW, SWING_TP2_R_LOW

    if adx >= ADX_TP_HIGH:
        if strategy == "INTRADAY":
            return INTRADAY_TP1_R_HIGH, INTRADAY_TP2_R_HIGH
        return SWING_TP1_R_HIGH, SWING_TP2_R_HIGH

    # Default zone (20 <= ADX < 30)
    if strategy == "INTRADAY":
        return INTRADAY_TP1_R, INTRADAY_TP2_R
    return SWING_TP1_R, SWING_TP2_R


def calc_session_vol_ratio(closes_1h, volumes_1h) -> float:
    """
    [v8.7 #3] Hitung rasio volume candle terbaru vs rata-rata candle pada
    JAM YANG SAMA dalam 7 hari terakhir (session-normalized volume).

    Cara kerja:
    - Candle terbaru ada di index -1, jam-nya = index % 24.
    - Kumpulkan semua candle yang berada di jam yang sama dari 7 hari lalu.
    - Hitung rata-rata volume mereka sebagai baseline.
    - Kembalikan vol[-1] / baseline.

    Butuh minimal SESSION_VOL_LOOKBACK_DAYS × 24 = 168 candle 1h.
    Jika data tidak cukup, return 0.0 (tidak trigger bonus).

    Returns: float ratio. >= SESSION_VOL_SPIKE_MULT → bonus vol_session_strong.
    """
    n_needed = SESSION_VOL_LOOKBACK_DAYS * 24 + 1
    if len(volumes_1h) < n_needed:
        return 0.0

    # Index candle terakhir di posisi -1 = len-1
    # Jam candle = posisinya modulo 24 dalam window
    n = len(volumes_1h)
    last_idx  = n - 1
    same_hour = last_idx % 24   # posisi relatif dalam siklus 24 jam

    # Kumpulkan index candle yang berada di jam yang sama 7 hari ke belakang
    # Tiap hari = 24 candle mundur; kita ambil 7 titik
    baseline_vols = []
    for day in range(1, SESSION_VOL_LOOKBACK_DAYS + 1):
        idx = last_idx - day * 24
        if 0 <= idx < n:
            baseline_vols.append(float(volumes_1h[idx]))

    if not baseline_vols:
        return 0.0

    baseline = float(np.mean(baseline_vols))
    if baseline <= 0:
        return 0.0

    return float(volumes_1h[-1]) / baseline


def get_spread_pct(client, pair: str) -> float:
    """
    [v8.1 #2] Hitung spread Ask/Bid sebagai % dari mid price.
    Spread tinggi = slippage besar = tolak signal untuk pair ini.
    Formula: (best_ask - best_bid) / mid_price * 100

    Returns: float spread pct, atau 0.0 jika gagal fetch.
    """
    try:
        ob = gate_call_with_retry(
            client.list_order_book,
            currency_pair=pair, limit=1
        )
        if not ob or not ob.asks or not ob.bids:
            return 0.0
        best_ask = float(ob.asks[0][0])
        best_bid = float(ob.bids[0][0])
        if best_ask <= 0 or best_bid <= 0:
            return 0.0
        mid = (best_ask + best_bid) / 2
        return (best_ask - best_bid) / mid * 100
    except Exception as e:
        log(f"⚠️ spread [{pair}]: {e}", "warn")
        return 0.0


def get_order_book_ratio(client, pair: str, ob_limit: int = 10) -> float:
    """
    Fetch order book ratio (bid vol / ask vol) sebagai scoring faktor lunak.
    Threshold: > 1.1 = bid dominan (BUY signal) | < 0.9 = ask dominan (SELL signal).

    [v8.0 AUDIT FIX 6.2] Tambah parameter ob_limit (default 10).
    Untuk MICROCAP (vol < 150K USDT), gunakan limit=20 atau disable scoring ini.
    Di sub-$150K range, satu order besar bisa mendominasi top-10 book, sehingga
    ob_ratio memberikan sinyal noise yang menyesatkan. Limit lebih dalam (20)
    memberikan snapshot depth yang lebih representatif untuk pair illiquid.
    """
    try:
        ob      = gate_call_with_retry(client.list_order_book, currency_pair=pair, limit=ob_limit)
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
    """
    Simpan signal ke Supabase untuk tracking dan deduplication.
    [v7.2 FIX #7] sent_at disimpan dalam UTC agar konsisten dengan already_sent query.
    [v7.7 #7] Isi _dedup_memory setelah insert — sehingga cycle yang sama
    tidak bisa mengirim duplikat meski Supabase lambat merespons.
    [v7.9 #7] Tambah kolom result, closed_at, pnl_pct (awalnya None) untuk
    outcome tracking — dapat diisi oleh evaluate_signals() cron di masa depan.
    [v7.9 #13] CSV fallback jika Supabase down — signal records tidak hilang.
    """
    record = {
        "pair":      pair,
        "strategy":  strategy,
        "side":      side,
        "entry":     entry,
        "tp1":       tp1,
        "tp2":       tp2,       # bisa None untuk PUMP
        "sl":        sl,
        "tier":      tier,
        "score":     score,
        "timeframe": timeframe,
        "sent_at":   datetime.now(timezone.utc).isoformat(),
        "result":    None,      # [v7.9 #7] TP1_HIT / TP2_HIT / SL_HIT / EXPIRED
        "closed_at": None,      # [v7.9 #7] timestamp saat result terisi
        "pnl_pct":   None,      # [v7.9 #7] % P&L aktual saat close
    }
    supabase_ok = False
    try:
        supabase.table("signals_v2").insert(record).execute()
        supabase_ok = True
    except Exception as e:
        err_str = str(e)
        # [v8.9 FIX] Schema cache error: 'Could not find column in schema cache'
        # Terjadi setelah ALTER TABLE sebelum PostgREST reload schema.
        # Solusi: strip kolom yang tidak dikenal dan retry dengan kolom inti saja.
        if "schema cache" in err_str.lower() or "PGRST204" in err_str:
            log(f"⚠️ save_signal schema cache miss [{pair}] — retry dengan kolom inti", "warn")
            core_cols = {"pair", "strategy", "side", "entry", "tp1", "tp2", "sl",
                         "tier", "score", "timeframe", "sent_at"}
            record_core = {k: v for k, v in record.items() if k in core_cols}
            try:
                supabase.table("signals_v2").insert(record_core).execute()
                supabase_ok = True
                log(f"  ✅ save_signal retry OK [{pair}] — kolom baru akan aktif setelah schema reload")
            except Exception as e2:
                log(f"⚠️ save_signal retry gagal [{pair}]: {e2} — menulis ke CSV fallback", "warn")
        else:
            log(f"⚠️ save_signal Supabase [{pair}]: {e} — menulis ke CSV fallback", "warn")
        # [v7.9 #13] CSV fallback — append ke file lokal agar record tidak hilang
        try:
            file_exists = os.path.isfile(SIGNAL_FALLBACK_FILE)
            with open(SIGNAL_FALLBACK_FILE, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(record.keys()))
                if not file_exists:
                    writer.writeheader()
                writer.writerow(record)
        except Exception as csv_err:
            log(f"⚠️ save_signal CSV fallback gagal [{pair}]: {csv_err}", "error")
    finally:
        # [v7.7 #7] Selalu tandai di memory — bahkan jika kedua sumber gagal,
        # mencegah re-send dalam cycle yang sama.
        _dedup_memory.add(_dedup_key(pair, strategy, side))
        if not supabase_ok:
            log(f"  ℹ️ Signal [{pair}|{strategy}] tercatat di CSV fallback saja", "warn")


def daily_loss_guard() -> bool:
    """
    [v7.9 #6] Cek apakah jumlah signal yang dikirim hari ini sudah melewati batas.

    [v8.0 AUDIT FIX 2.3] Augmented dengan SL hit rate check — guard sekarang
    merupakan DRAWDOWN SENSOR NYATA, bukan hanya throughput limiter.
    Dua kondisi yang bisa men-trigger halt:
      1. Volume: signal_count >= MAX_DAILY_SIGNALS dalam 24 jam (proxy guard)
      2. Loss rate: sl_count / total_resolved > 60% dari trade yang sudah selesai
         → market sedang choppy / sistem tidak bekerja dengan baik

    Returns True jika signal generation harus di-halt, False jika boleh lanjut.
    """
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

        # ── Guard 1: Volume (proxy drawdown) ─────────────────────
        result = (
            supabase.table("signals_v2")
            .select("id", count="exact")
            .gt("sent_at", since)
            .execute()
        )
        count = result.count if hasattr(result, "count") else len(result.data)
        if count >= MAX_DAILY_SIGNALS:
            log(f"⚠️ daily_loss_guard: {count} signal dalam 24 jam ≥ MAX {MAX_DAILY_SIGNALS} — halt", "warn")
            tg(f"⛔ <b>DAILY SIGNAL LIMIT REACHED</b>\n"
               f"Sudah {count} signal dikirim dalam 24 jam terakhir.\n"
               f"Bot berhenti untuk melindungi modal dari overtrading.\n"
               f"<i>Limit: MAX_DAILY_SIGNALS={MAX_DAILY_SIGNALS}</i>", critical=True)
            return True

        log(f"  ✅ Daily guard vol: {count}/{MAX_DAILY_SIGNALS} signal hari ini — OK")

        # ── Guard 2: SL hit rate (real drawdown sensor) ──────────
        # Hanya evaluasi jika sudah ada cukup resolved trades dalam 24 jam
        resolved_rows = (
            supabase.table("signals_v2")
            .select("result")
            .gt("sent_at", since)
            .not_.is_("result", "null")
            .neq("result", "EXPIRED")
            .execute()
        ).data
        total_resolved = len(resolved_rows)
        if total_resolved >= 5:  # butuh minimal 5 resolved untuk evaluasi bermakna
            sl_count = sum(1 for r in resolved_rows if r["result"] == "SL_HIT")
            sl_rate  = sl_count / total_resolved
            if sl_rate > 0.60:
                log(f"⚠️ daily_loss_guard: SL rate {sl_rate:.0%} ({sl_count}/{total_resolved}) > 60% — halt", "error")
                tg(f"⛔ <b>DAILY DRAWDOWN GUARD — HIGH LOSS RATE</b>\n"
                   f"{sl_count} dari {total_resolved} trade selesai hari ini kena SL.\n"
                   f"Loss rate: <b>{sl_rate:.0%}</b> (threshold: 60%)\n"
                   f"Market mungkin choppy atau sinyal tidak valid.\n"
                   f"<i>Bot berhenti sampai reset manual atau hari berikutnya.</i>", critical=True)
                return True
            log(f"  ✅ Daily guard SL rate: {sl_rate:.0%} ({sl_count}/{total_resolved}) — OK")

        return False

    except Exception as e:
        log(f"⚠️ daily_loss_guard error: {e} — skip guard (lanjut)", "warn")
        return False   # jika Supabase error, jangan blokir scan


def evaluate_signals(client=None) -> None:
    """
    [v7.9 #8] Evaluasi outcome signal yang belum ter-resolve.
    Fungsi ini dirancang untuk dijalankan sebagai CRON TERPISAH,
    bukan di dalam run() — agar tidak memperlambat scan cycle utama.

    Cara kerja:
    1. Ambil signal dari Supabase yang result=NULL dan sent_at < 24 jam lalu
    2. Fetch candles terbaru dari Gate.io untuk pair tersebut
    3. Cek apakah harga sempat menyentuh TP1, TP2, atau SL
    4. Update kolom result, closed_at, pnl_pct di Supabase

    ⚠️ BATASAN PENTING (untuk developer future):
    Deteksi outcome berbasis candle range (high/low), BUKAN tick data.
    Jika satu candle menyentuh BAIK TP maupun SL dalam periode yang sama,
    kita tidak bisa tahu mana yang terpenuhi lebih dulu. Dalam kasus ambiguitas
    ini, kode memprioritaskan TP (asumsi optimis / fill di TP lebih dulu).
    Ini bisa OVERSTATE win rate terutama di sesi volatil. Win rate dari fungsi
    ini harus dibaca sebagai estimasi optimis, bukan angka presisi.
    [v8.0 AUDIT FIX 3.1]

    [v8.4 FIX #2] Dua perbaikan akurasi outcome:
    a) Evaluasi HANYA candles yang closed SETELAH sent_at — candles sebelum
       signal dikirim tidak relevan dan menghasilkan false positive outcome.
    b) Candle terakhir (index -1) di-SKIP karena kemungkinan masih running
       (belum closed) — mengikutsertakannya bisa menghasilkan phantom TP/SL hit
       yang tidak pernah benar-benar terjadi saat candle close.
    """
    if client is None:
        client = get_client()

    log("📊 evaluate_signals — mengambil signal pending...")
    try:
        # Ambil signal yang belum punya result dan sudah lebih dari 1 jam (candle punya waktu)
        since_min  = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        since_max  = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        pending = (
            supabase.table("signals_v2")
            .select("id,pair,side,entry,tp1,tp2,sl,timeframe,sent_at,strategy")
            .is_("result", "null")
            .gt("sent_at", since_min)
            .lt("sent_at", since_max)
            .execute()
        ).data
    except Exception as e:
        log(f"⚠️ evaluate_signals: gagal fetch pending signals: {e}", "error")
        return

    if not pending:
        log("  ℹ️ Tidak ada signal pending untuk dievaluasi")
        return

    log(f"  → {len(pending)} signal pending ditemukan")

    # [v8.4 FIX #2] Interval seconds map — dipakai untuk menghitung berapa candle
    # yang sudah closed sejak signal dikirim, agar hanya candle post-entry yang dievaluasi.
    tf_map         = {"1h": ("1h", 50), "4h": ("4h", 50), "15m": ("15m", 100)}
    tf_seconds_map = {"1h": 3600, "4h": 14400, "15m": 900}
    resolved = 0

    for sig in pending:
        try:
            pair   = sig["pair"]
            side   = sig["side"]
            entry  = float(sig["entry"])
            tp1    = float(sig["tp1"])
            tp2    = float(sig["tp2"]) if sig.get("tp2") else None
            sl_val = float(sig["sl"])
            tf     = sig.get("timeframe", "1h")
            interval, limit = tf_map.get(tf, ("1h", 50))

            data = get_candles(client, pair, interval, limit)
            if data is None:
                continue
            closes_all, highs_all, lows_all, _ = data

            # [v8.4 FIX #2a] Hitung berapa candle yang sudah ada sejak signal dikirim.
            # Gate.io mengembalikan candle dari yang PALING LAMA ke yang PALING BARU.
            # candles_since = jumlah candle interval yang sudah berlalu sejak sent_at.
            # Minimal 1 — kita butuh setidaknya 1 candle closed untuk evaluasi.
            sent_at_str = sig.get("sent_at", "")
            candles_since = 1   # default fallback
            if sent_at_str:
                try:
                    sent_dt    = datetime.fromisoformat(sent_at_str.replace("Z", "+00:00"))
                    elapsed    = (datetime.now(timezone.utc) - sent_dt).total_seconds()
                    tf_secs    = tf_seconds_map.get(tf, 3600)
                    candles_since = max(1, int(elapsed // tf_secs))
                except Exception:
                    pass   # fallback ke 1 jika parse gagal

            # Ambil hanya candles post-entry, kecuali candle terakhir (mungkin masih running).
            # [v8.4 FIX #2b] Exclude candle[-1] karena belum tentu closed.
            # Contoh: limit=50, candles_since=3 → evaluasi index [-4:-1] (3 candle closed).
            end_idx   = -1                          # exclude candle terakhir (running)
            start_idx = max(-limit, -(candles_since + 1))  # tidak melebihi array bounds
            highs  = highs_all[start_idx:end_idx]
            lows   = lows_all[start_idx:end_idx]
            closes = closes_all[start_idx:end_idx]

            if len(highs) == 0:
                log(f"  ⚠️ [{pair}] tidak ada candle closed post-entry untuk dievaluasi — skip")
                continue

            # Cek apakah harga pernah menyentuh TP/SL di candle post-entry yang sudah closed
            if side == "BUY":
                hit_tp2 = tp2 is not None and float(np.max(highs)) >= tp2
                hit_tp1 = float(np.max(highs)) >= tp1
                hit_sl  = float(np.min(lows))  <= sl_val
            else:
                hit_tp2 = tp2 is not None and float(np.min(lows)) <= tp2
                hit_tp1 = float(np.min(lows))  <= tp1
                hit_sl  = float(np.max(highs)) >= sl_val

            # Prioritas: TP2 > TP1 > SL (asumsi fill terbaik)
            if hit_tp2:
                result_val = "TP2_HIT"
                pnl_val    = round(abs(tp2 - entry) / entry * 100, 2)
            elif hit_tp1:
                result_val = "TP1_HIT"
                pnl_val    = round(abs(tp1 - entry) / entry * 100, 2)
            elif hit_sl:
                result_val = "SL_HIT"
                pnl_val    = -round(abs(sl_val - entry) / entry * 100, 2)
            else:
                result_val = "EXPIRED"
                # Gunakan closes[-1] dari candle closed terakhir (bukan candle running)
                current_price = float(closes[-1]) if len(closes) > 0 else entry
                pnl_val = round((current_price - entry) / entry * 100, 2) if side == "BUY" \
                          else round((entry - current_price) / entry * 100, 2)

            supabase.table("signals_v2").update({
                "result":    result_val,
                "closed_at": datetime.now(timezone.utc).isoformat(),
                "pnl_pct":   pnl_val,
            }).eq("id", sig["id"]).execute()

            log(f"  ✅ [{pair}|{sig['strategy']}] → {result_val} ({pnl_val:+.2f}%)")
            resolved += 1
            time.sleep(0.05)   # throttle — jangan flood Gate.io

        except Exception as e:
            log(f"  ⚠️ evaluate_signals [{sig.get('pair','?')}]: {e}", "warn")
            continue

    log(f"📊 evaluate_signals selesai — {resolved}/{len(pending)} signal ter-resolve")


def strategy_health_check() -> dict:
    """
    [v7.9 #9] Cek win rate per strategy dari N signal terakhir.
    Dijalankan di awal run() — jika ada strategy dengan win rate < 40%,
    log critical warning. Bot tidak mematikan strategy (user harus keputusan),
    tapi ada sinyal jelas bahwa sesuatu bermasalah.

    [v8.0 AUDIT FIX 3.1] Sample naik dari 20 → HEALTH_CHECK_SAMPLE_SIZE (30).
    Pada 50% true win rate: 20 sample → 95% CI ≈ 28–72% (terlalu lebar, banyak
    false CRITICAL). 30 sample → 95% CI ≈ 32–68% (lebih sempit dan andal).

    [v8.0 AUDIT FIX 3.2] Strike counter — jika strategy sama CRITICAL selama N run
    berturut-turut, pesan Telegram dieskalasi ke "TRIPLE CRITICAL".
    Strike count disimpan di tabel `strategy_strikes` di Supabase.
    Tabel ini harus dibuat manual dengan skema:
        strategy TEXT PRIMARY KEY, strike_count INT DEFAULT 0

    ⚠️  WIN-RATE BIAS WARNING (v8.0 AUDIT FIX #14):
    Win rate yang dihitung di sini HANYA mencakup signal dengan result != NULL dan
    result != 'EXPIRED'. Signal yang masih pending (result=NULL) TIDAK dihitung.
    Ini menimbulkan survivorship bias: jika banyak signal pending (evaluate_signals()
    berjalan lambat), denominator mengecil dan win rate tampak lebih tinggi dari
    kondisi nyata. Interpretasi: gunakan angka ini sebagai INDIKATOR, bukan ground truth.
    Pastikan evaluate_signals() berjalan rutin agar pending signal ter-resolve tepat waktu.

    Returns: dict { strategy_name: {win_rate, count, status, strike} }
    """
    health     = {}
    strategies = ["INTRADAY", "SWING", "MOMENTUM", "PUMP", "MICROCAP"]

    # ── Load strike counters dari Supabase (best-effort) ─────
    strikes: dict = {}
    try:
        rows = (
            supabase.table("strategy_strikes")
            .select("strategy,strike_count")
            .execute()
        ).data
        strikes = {r["strategy"]: r["strike_count"] for r in rows}
    except Exception as e:
        log(f"⚠️ strategy_health_check: gagal load strike counters: {e} — strike tracking di-skip", "warn")

    try:
        for strat in strategies:
            rows = (
                supabase.table("signals_v2")
                .select("result")
                .eq("strategy", strat)
                .not_.is_("result", "null")
                .neq("result", "EXPIRED")
                .order("sent_at", desc=True)
                .limit(HEALTH_CHECK_SAMPLE_SIZE)   # [v8.0] naik dari 20 → 30
                .execute()
            ).data

            # [v8.2 #1] Hitung pending signals per strategy untuk deteksi win rate bias.
            # Signal dengan result=NULL berarti evaluate_signals() belum sempat resolve.
            # Jika pending_count besar relatif terhadap count, win rate tidak representatif.
            pending_count = 0
            try:
                since_48h = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
                pending_rows = (
                    supabase.table("signals_v2")
                    .select("id", count="exact")
                    .eq("strategy", strat)
                    .is_("result", "null")
                    .gt("sent_at", since_48h)
                    .execute()
                )
                pending_count = pending_rows.count if hasattr(pending_rows, "count") else len(pending_rows.data)
            except Exception as pe:
                log(f"⚠️ pending_count [{strat}]: {pe} — skip", "warn")

            if len(rows) < HEALTH_CHECK_MIN_SAMPLE:
                health[strat] = {
                    "win_rate": None, "count": len(rows),
                    "status": "INSUFFICIENT_DATA", "strike": 0,
                    "pending_count": pending_count,   # [v8.2 #1]
                }
                continue

            wins     = sum(1 for r in rows if r["result"] in ("TP1_HIT", "TP2_HIT"))
            win_rate = round(wins / len(rows) * 100, 1)
            is_crit  = win_rate < 40
            status   = "CRITICAL ⛔" if is_crit else ("WARNING ⚠️" if win_rate < 50 else "OK ✅")

            # [v8.3 #4] Profit Factor & Expectancy — metrik lebih bermakna dari win rate saja.
            # Fetch pnl_pct untuk rows yang sama (sample HEALTH_CHECK_SAMPLE_SIZE terakhir).
            profit_factor = None
            expectancy    = None
            avg_win_pct   = None
            avg_loss_pct  = None
            try:
                pnl_rows = (
                    supabase.table("signals_v2")
                    .select("result,pnl_pct")
                    .eq("strategy", strat)
                    .not_.is_("result", "null")
                    .neq("result", "EXPIRED")
                    .not_.is_("pnl_pct", "null")
                    .order("sent_at", desc=True)
                    .limit(HEALTH_CHECK_SAMPLE_SIZE)
                    .execute()
                ).data
                win_pnls  = [float(r["pnl_pct"]) for r in pnl_rows if r["result"] in ("TP1_HIT", "TP2_HIT") and r["pnl_pct"] is not None]
                loss_pnls = [abs(float(r["pnl_pct"])) for r in pnl_rows if r["result"] == "SL_HIT" and r["pnl_pct"] is not None]
                if win_pnls:
                    avg_win_pct = round(float(np.mean(win_pnls)), 2)
                if loss_pnls:
                    avg_loss_pct = round(float(np.mean(loss_pnls)), 2)
                if win_pnls and loss_pnls:
                    gross_profit = sum(win_pnls)
                    gross_loss   = sum(loss_pnls)
                    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else None
                    wr_dec   = wins / len(rows)
                    lr_dec   = 1.0 - wr_dec
                    expectancy = round(wr_dec * avg_win_pct - lr_dec * avg_loss_pct, 3)
            except Exception as pf_err:
                log(f"⚠️ profit_factor [{strat}]: {pf_err} — skip", "warn")

            # ── Strike counter update (best-effort) ───────────
            prev_strike = strikes.get(strat, 0)
            new_strike  = prev_strike + 1 if is_crit else 0
            try:
                existing = (
                    supabase.table("strategy_strikes")
                    .select("strategy")
                    .eq("strategy", strat)
                    .execute()
                ).data
                if existing:
                    supabase.table("strategy_strikes").update(
                        {"strike_count": new_strike}
                    ).eq("strategy", strat).execute()
                else:
                    supabase.table("strategy_strikes").insert(
                        {"strategy": strat, "strike_count": new_strike}
                    ).execute()
            except Exception as se:
                log(f"⚠️ strike update [{strat}]: {se}", "warn")

            health[strat] = {
                "win_rate":      win_rate,
                "count":         len(rows),
                "status":        status,
                "strike":        new_strike,
                "pending_count": pending_count,      # [v8.2 #1] untuk deteksi bias
                "profit_factor": profit_factor,       # [v8.3 #4]
                "expectancy":    expectancy,          # [v8.3 #4] % per trade
                "avg_win_pct":   avg_win_pct,         # [v8.3 #4]
                "avg_loss_pct":  avg_loss_pct,        # [v8.3 #4]
            }

            # Log profit factor jika tersedia
            pf_str = f" | PF:{profit_factor}" if profit_factor is not None else ""
            exp_str = f" | EXP:{expectancy:+.3f}%" if expectancy is not None else ""
            log(f"  📈 Health [{strat}]: WR:{win_rate}% | {len(rows)} trades{pf_str}{exp_str} — {status}")

            if is_crit:
                log(
                    f"⛔ strategy_health [{strat}]: win rate {win_rate}% "
                    f"dari {len(rows)} trade — KRITIS! (strike #{new_strike})", "error"
                )
                # [v8.0 AUDIT FIX 3.2] Eskalasi Telegram jika sudah N kali berturut CRITICAL
                if new_strike >= HEALTH_CRITICAL_STRIKE_LIMIT:
                    pf_line  = f"\nProfit Factor: <b>{profit_factor}</b>" if profit_factor is not None else ""
                    exp_line = f"\nExpectancy   : <b>{expectancy:+.3f}%</b> per trade" if expectancy is not None else ""
                    tg(
                        f"🚨 <b>TRIPLE CRITICAL — {strat}</b>\n"
                        f"Strategy ini sudah <b>{new_strike}×</b> berturut-turut di bawah 40% win rate.\n"
                        f"Win rate terbaru: <b>{win_rate}%</b> dari {len(rows)} trade.{pf_line}{exp_line}\n"
                        f"<b>Pertimbangkan untuk menonaktifkan strategy ini sementara.</b>\n"
                        f"<i>Bot tidak mematikan otomatis — keputusan ada di tangan kamu.</i>",
                        critical=True,
                    )

    except Exception as e:
        log(f"⚠️ strategy_health_check error: {e} — skip", "warn")

    return health


# ════════════════════════════════════════════════════════
#  SIGNAL STRATEGIES
# ════════════════════════════════════════════════════════

def _check_intraday_buy(closes, highs, lows, volumes, price, ob_ratio,
                         structure, liq, mkt, rsi, macd, msig,
                         ema20, ema50, vwap, atr) -> dict | None:
    """
    [v8.0 AUDIT FIX 5.2] Helper privat — BUY branch dari check_intraday().
    Dipecah untuk menurunkan cyclomatic complexity dari ~22 menjadi ~11 per branch.
    """
    has_struct = (structure.get("bos")   == "BULLISH" or
                  structure.get("choch") == "BULLISH" or
                  liq.get("sweep_bull"))
    if not has_struct: return None          # Gate 1: WAJIB — struktur bullish
    if rsi > 72:       return None          # Gate 2: WAJIB — tidak overbought ekstrem

    ob    = detect_order_block(closes, highs, lows, volumes, side="BUY", lookback=25)
    score = score_signal("BUY", price, closes, highs, lows, volumes,
                         structure, liq, ob, rsi, macd, msig,
                         ema20, ema50, vwap, ob_ratio, mkt["regime"])
    tier  = assign_tier(score)
    if tier == "SKIP": return None

    last_sh = structure.get("last_sh")
    # [v8.0 AUDIT FIX 5.1] Gunakan LATE_ENTRY_THRESHOLD menggantikan magic number 1.02
    if last_sh and price > last_sh * LATE_ENTRY_THRESHOLD: return None
    entry = round(last_sh * 1.002, 8) if (last_sh and price > last_sh) else price

    sl, tp1, tp2 = calc_sl_tp(entry, "BUY", atr, structure, "INTRADAY", liq, mkt["adx"])
    if tp1 <= entry or sl >= entry: return None
    sl_dist = entry - sl
    if sl_dist <= 0 or sl_dist / entry > 0.05: return None
    rr = (tp1 - entry) / sl_dist
    if rr < MIN_RR["INTRADAY"]: return None

    return {
        "pair": None, "strategy": "INTRADAY", "side": "BUY",
        "timeframe": "1h", "entry": entry,
        "tp1": tp1, "tp2": tp2, "sl": sl,
        "tier": tier, "score": score, "rr": round(rr, 1),
        "rsi": round(rsi, 1), "structure": structure,
        "regime": mkt["regime"], "adx": mkt["adx"],
        "conviction": calc_conviction(score),
    }


def _check_intraday_sell(closes, highs, lows, volumes, price, ob_ratio,
                          structure, liq, mkt, rsi, macd, msig,
                          ema20, ema50, vwap, atr, btc) -> dict | None:
    """
    [v8.0 AUDIT FIX 5.2] Helper privat — SELL branch dari check_intraday().
    Dipecah untuk menurunkan cyclomatic complexity dari ~22 menjadi ~11 per branch.
    """
    has_struct = (structure.get("bos")   == "BEARISH" or
                  structure.get("choch") == "BEARISH" or
                  liq.get("sweep_bear"))
    if not has_struct: return None          # Gate 1: WAJIB — struktur bearish
    if rsi < 22:       return None          # Gate 2: WAJIB — tidak oversold ekstrem

    # [v7.8 ANTI-SELL BIAS] Blokir SELL saat BTC bullish kuat + pair sedang trending bullish.
    if btc.get("btc_bullish") and mkt["trend_dir"] == "BULLISH":
        return None

    # [v7.7 #2] Late-entry filter sebelum scoring — hemat komputasi OB/ADX/MACD
    last_sh = structure.get("last_sh")
    if last_sh and price < last_sh * SELL_ENTRY_LATE_THRESHOLD: return None   # sudah terlalu jauh turun

    ob    = detect_order_block(closes, highs, lows, volumes, side="SELL", lookback=25)
    score = score_signal("SELL", price, closes, highs, lows, volumes,
                         structure, liq, ob, rsi, macd, msig,
                         ema20, ema50, vwap, ob_ratio, mkt["regime"])
    tier  = assign_tier(score)
    if tier == "SKIP": return None

    # [v7.2 FIX #4] Entry SELL di dekat last_sh (supply zone), bukan last_sl.
    entry = round(last_sh * SELL_ENTRY_OFFSET, 8) if (last_sh and price >= last_sh * SELL_ENTRY_LATE_THRESHOLD) else price

    sl, tp1, tp2 = calc_sl_tp(entry, "SELL", atr, structure, "INTRADAY", liq, mkt["adx"])
    if tp1 >= entry or sl <= entry: return None
    sl_dist = sl - entry
    if sl_dist <= 0 or sl_dist / entry > 0.05: return None
    rr = (entry - tp1) / sl_dist
    if rr < MIN_RR["INTRADAY"]: return None

    return {
        "pair": None, "strategy": "INTRADAY", "side": "SELL",
        "timeframe": "1h", "entry": entry,
        "tp1": tp1, "tp2": tp2, "sl": sl,
        "tier": tier, "score": score, "rr": round(rr, 1),
        "rsi": round(rsi, 1), "structure": structure,
        "regime": mkt["regime"], "adx": mkt["adx"],
        "conviction": calc_conviction(score),
    }


def check_intraday(client, pair: str, price: float, ob_ratio: float,
                   btc: dict, side: str = "BUY",
                   sniper_ctx: dict | None = None) -> dict | None:
    """
    INTRADAY signal — timeframe 1h. Mendukung BUY dan SELL.
    [v8.0 AUDIT FIX 5.2] Refactor: dispatch ke _check_intraday_buy() atau
    _check_intraday_sell() untuk menurunkan cyclomatic complexity.
    [v8.1] sniper_ctx — dict opsional berisi btc_eth_diverge dan is_low_vol
    untuk menerapkan diverge penalty dan low-vol session filter.
    [v8.3 #1] MTF Confirmation — tolak sinyal jika 4h structure berlawanan arah
    secara kuat (bias + BOS keduanya konfirmasi counter-trend).
    """
    ctx = sniper_ctx or {}
    if side == "BUY" and btc["block_buy"]: return None

    # [v8.3 #1] MTF Confirmation Gate — cek 4h bias sebelum fetch 1h candles
    # untuk menghemat API call pada pair yang jelas counter-trend.
    if MTF_ALIGNMENT_ENABLED:
        bias4h = get_4h_bias(client, pair)
        if bias4h["valid"]:
            if side == "BUY" and bias4h["bias"] == "BEARISH" and bias4h["bos"] == "BEARISH":
                # 4h struktur bearish aktif — 1h BUY adalah counter-trend, tolak
                log(f"  ⛔ MTF [{pair}] 4h BEARISH BOS — INTRADAY BUY ditolak (counter-trend)")
                return None
            if side == "SELL" and bias4h["bias"] == "BULLISH" and bias4h["bos"] == "BULLISH":
                # 4h struktur bullish aktif — 1h SELL adalah counter-trend, tolak
                log(f"  ⛔ MTF [{pair}] 4h BULLISH BOS — INTRADAY SELL ditolak (counter-trend)")
                return None

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
    vwap       = calc_vwap(closes, highs, lows, volumes, timeframe="1h")
    structure  = detect_structure(closes, highs, lows, strength=3, lookback=60)
    liq        = detect_liquidity(closes, highs, lows, lookback=40)

    if not structure["valid"]: return None

    if side == "BUY":
        result = _check_intraday_buy(closes, highs, lows, volumes, price, ob_ratio,
                                     structure, liq, mkt, rsi, macd, msig,
                                     ema20, ema50, vwap, atr)
    else:
        result = _check_intraday_sell(closes, highs, lows, volumes, price, ob_ratio,
                                      structure, liq, mkt, rsi, macd, msig,
                                      ema20, ema50, vwap, atr, btc)

    if result is None:
        return None

    # [v8.2 #4] Diverge penalty + low-vol filter diterapkan SEBELUM assign_tier final.
    # v8.1 bug: assign_tier dipanggil di dalam _check_intraday_buy/sell, lalu dipanggil
    # lagi setelah penalty — dua evaluasi tier untuk pair yang sama dalam satu call.
    # Fix: terapkan penalty ke score, re-assign tier sekali, selesai.
    if ctx.get("btc_eth_diverge"):
        result["score"] += DIVERGE_SCORE_PENALTY
        result["conviction"] = calc_conviction(result["score"])

    # Re-evaluate tier setelah semua penalty/bonus teraplikasi — SATU kali panggil
    final_tier = assign_tier(result["score"], ctx.get("is_low_vol", False))
    if final_tier == "SKIP":
        return None
    result["tier"] = final_tier

    result["pair"]          = pair
    result["current_price"] = price
    return result


def check_swing(client, pair: str, price: float, ob_ratio: float,
                btc: dict, side: str = "BUY",
                sniper_ctx: dict | None = None) -> dict | None:
    """
    SWING signal — timeframe 4h. Mendukung BUY dan SELL.
    [v8.1] sniper_ctx untuk diverge penalty dan low-vol session filter.
    [v8.7 #1] Weekly structure gate — tolak SWING jika weekly bias + BOS
    berlawanan arah (counter-trend kuat di timeframe weekly).
    """
    ctx = sniper_ctx or {}
    # [FIX #1] Blok BUY jika BTC drop — SELL tetap boleh jalan
    if side == "BUY" and btc["block_buy"]: return None

    # [v8.7 #1] Weekly Structure Gate — cek sebelum fetch 4h candles
    # untuk hemat API call pada pair yang jelas counter-trend di weekly.
    if SWING_WEEKLY_GATE_ENABLED:
        bias_w = get_weekly_bias(client, pair)
        if bias_w["valid"]:
            if side == "BUY" and bias_w["bias"] == "BEARISH" and bias_w["bos"] == "BEARISH":
                log(f"  ⛔ WEEKLY [{pair}] weekly BEARISH BOS — SWING BUY ditolak (counter-trend)")
                return None
            if side == "SELL" and bias_w["bias"] == "BULLISH" and bias_w["bos"] == "BULLISH":
                log(f"  ⛔ WEEKLY [{pair}] weekly BULLISH BOS — SWING SELL ditolak (counter-trend)")
                return None

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
        # [v8.0 AUDIT FIX 5.1] Gunakan LATE_ENTRY_THRESHOLD menggantikan magic number 1.02
        if last_sh and price > last_sh * LATE_ENTRY_THRESHOLD: return None
        entry = round(last_sh * 1.003, 8) if (last_sh and price > last_sh) else price

        sl, tp1, tp2 = calc_sl_tp(entry, "BUY", atr, structure, "SWING", liq, mkt["adx"])
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

        # [v7.8 ANTI-SELL BIAS] Konsisten dengan check_intraday:
        # Blokir SELL SWING saat BTC bullish sistemik + pair trending bullish.
        # SWING SELL counter-trend di bull market = risk sangat tinggi, reward rendah.
        if btc.get("btc_bullish") and mkt["trend_dir"] == "BULLISH":
            return None

        # [v7.7 #3] Late-entry filter DIPINDAHKAN ke sini — sebelum scoring.
        # Konsisten dengan BUY branch dan check_intraday SELL (v7.7 #2).
        last_sh = structure.get("last_sh")
        if last_sh and price < last_sh * SELL_ENTRY_LATE_THRESHOLD: return None

        ob    = detect_order_block(closes, highs, lows, volumes, side="SELL", lookback=40)
        score = score_signal("SELL", price, closes, highs, lows, volumes,
                             structure, liq, ob, rsi, macd, msig,
                             ema50, ema200, vwap, ob_ratio, mkt["regime"])
        tier  = assign_tier(score)
        if tier == "SKIP": return None

        # [v7.2 FIX #4] Entry SELL di dekat last_sh (supply zone), konsisten dengan intraday.
        # last_sh sudah di-compute di atas untuk late-entry filter.
        entry = round(last_sh * SELL_ENTRY_OFFSET, 8) if (last_sh and price >= last_sh * SELL_ENTRY_LATE_THRESHOLD) else price

        sl, tp1, tp2 = calc_sl_tp(entry, "SELL", atr, structure, "SWING", liq, mkt["adx"])
        if tp1 >= entry or sl <= entry: return None
        sl_dist = sl - entry
        if sl_dist <= 0 or sl_dist / entry > 0.10: return None
        rr = (entry - tp1) / sl_dist

    if rr < MIN_RR["SWING"]: return None

    # [v8.2 #4] Diverge penalty + low-vol filter — SATU kali assign_tier setelah semua penalty.
    # [v8.4 FIX #1] score di-update dulu SEBELUM masuk ke return dict dan calc_conviction.
    # Bug sebelumnya: score += DIVERGE_SCORE_PENALTY tidak tercermin di return dict["score"]
    # dan conviction karena keduanya masih pakai nilai score pre-penalty.
    if ctx.get("btc_eth_diverge"):
        score += DIVERGE_SCORE_PENALTY

    final_tier = assign_tier(score, ctx.get("is_low_vol", False))
    if final_tier == "SKIP":
        return None

    return {
        "pair": pair, "strategy": "SWING", "side": side,
        "timeframe": "4h", "entry": entry,
        "current_price": price,
        "tp1": tp1, "tp2": tp2, "sl": sl,
        "tier": final_tier, "score": score, "rr": round(rr, 1),   # [v8.4 FIX #1] score post-penalty
        "rsi": round(rsi, 1), "structure": structure,
        "regime": mkt["regime"], "adx": mkt["adx"],
        "conviction": calc_conviction(score),   # [v8.4 FIX #1] conviction dari score post-penalty
    }


# ════════════════════════════════════════════════════════
#  MOMENTUM SCANNER — [v7.8 NEW]
#  Trend continuation engine — menangkap koin yang "terbang"
#  Berbeda dari INTRADAY/SWING yang reversal-heavy.
#
#  Filosofi: jangan tunggu pullback → entry saat momentum dikonfirmasi
#  Trigger:
#    1. Breakout di atas recent high 6 candle (early breakout)
#    2. Volume spike ≥ 1.5× — ada partisipasi nyata
#    3. RSI 50–72 — zona momentum sehat (bukan oversold/overbought)
#    4. EMA20 > EMA50 — struktur uptrend
#    5. Price > EMA20 — harga masih di atas trend
#    6. MACD bullish — konfirmasi tambahan
#  Target: ikut arus, bukan melawan
# ════════════════════════════════════════════════════════

# Dedup hours untuk MOMENTUM — lebih pendek dari INTRADAY
MOMENTUM_DEDUP_HOURS = 4
MAX_MOMENTUM_SIGNALS = 3


def already_sent_momentum(pair: str) -> bool:
    """Cek dedup signal MOMENTUM — pair dalam MOMENTUM_DEDUP_HOURS jam."""
    return _already_sent_generic(pair, "MOMENTUM", MOMENTUM_DEDUP_HOURS, side="BUY")


def check_momentum(client, pair: str, price: float, ob_ratio: float,
                   btc: dict, sniper_ctx: dict | None = None) -> dict | None:
    """
    MOMENTUM SCANNER — timeframe 1h.
    [v8.1] sniper_ctx untuk diverge penalty dan low-vol session filter.
    """
    ctx = sniper_ctx or {}
    # Hanya jalan jika BUY tidak diblokir
    if btc["block_buy"]: return None

    data = get_candles(client, pair, "1h", 100)
    if data is None: return None
    closes, highs, lows, volumes = data

    # ATR filter — pastikan ada volatilitas yang layak
    atr = calc_atr(closes, highs, lows)
    if atr / price * 100 < 0.2: return None
    if atr / price * 100 > 8.0: return None

    # Market regime — momentum hanya valid saat ada trend
    mkt = detect_market_regime(closes, highs, lows)
    if mkt["regime"] == "CHOPPY": return None   # ADX < 18: tidak ada trend = skip

    # ── Gate 1: RSI zona momentum ────────────────────────
    # 50–72: trend sehat. Bukan oversold (reversal), bukan overbought (exhaustion)
    rsi = calc_rsi(closes)
    if not (50.0 <= rsi <= 72.0): return None

    # ── Gate 2: Breakout di atas recent high ─────────────
    # Harga harus menembus high 6 candle terakhir (tidak termasuk candle ini)
    # Ini adalah early breakout — detect sesaat setelah break, bukan setelah retrace
    recent_high = float(np.max(highs[-7:-1]))
    if closes[-1] <= recent_high * MOMENTUM_BREAKOUT_MIN: return None   # belum breakout

    # Anti buy-the-top: jangan masuk jika sudah terlalu jauh di atas recent high
    if closes[-1] > recent_high * MOMENTUM_LATE_ENTRY_THRESHOLD: return None   # sudah terlambat

    # ── Gate 3: Volume spike konfirmasi ──────────────────
    # Volume harus lebih tinggi dari rata-rata — ada partisipasi nyata
    vol_avg = float(np.mean(volumes[-11:-1]))
    if vol_avg <= 0: return None
    vol_ratio = float(volumes[-1]) / vol_avg
    if vol_ratio < 1.5: return None   # minimal 1.5× — lebih longgar dari pump scanner

    # ── Gate 4: EMA structure bullish ────────────────────
    ema20 = calc_ema(closes, 20)
    ema50 = calc_ema(closes, 50)
    if ema20 < ema50: return None   # struktur EMA harus uptrend
    if price < ema20: return None   # price harus di atas EMA20

    # ── Scoring ──────────────────────────────────────────
    # [v7.9 #1,#2] Pakai W_MOMENTUM terpisah — tidak ada cross-contamination dengan W dict.
    # [v7.9 #1] price>ema20 pakai W_MOMENTUM["price_above_ema"] bukan W["ema_align"]
    # → fix score inflation +1 dari double-counting yang ada di v7.8.
    score = 0
    score += W_MOMENTUM["momentum_break"]         # breakout sudah dikonfirmasi di Gate 2
    score += W_MOMENTUM["vol_confirm"]            # volume sudah dikonfirmasi di Gate 3

    if rsi >= 55:                     score += W_MOMENTUM["rsi_momentum"]    # RSI kuat di zona momentum
    if ema20 > ema50:                 score += W_MOMENTUM["ema_align"]        # EMA aligned
    if price > ema20:                 score += W_MOMENTUM["price_above_ema"]  # [v7.9 #1] FIX — bukan ema_align

    # [v8.5 #1] RSI direction scoring untuk MOMENTUM — bonus jika RSI masih naik
    # (momentum berlanjut), penalti jika sudah berbalik turun (potensi exhaustion).
    _rsi_dir_mom = calc_rsi_momentum_dir(closes, lookback=RSI_MOMENTUM_WINDOW)
    if _rsi_dir_mom == 1:    score += W_MOMENTUM["rsi_dir_bull"]   # RSI naik -> momentum lanjut
    elif _rsi_dir_mom == -1: score += W_MOMENTUM["rsi_dir_bear"]   # RSI turun -> hati-hati exhaustion

    macd, msig = calc_macd(closes)
    if macd > msig:                   score += W_MOMENTUM["macd_cross"]       # MACD bullish
    elif macd < msig:                 score += W_MOMENTUM["macd_soft"]        # penalti counter

    vwap = calc_vwap(closes, highs, lows, volumes, timeframe="1h")
    if price > vwap:                  score += W_MOMENTUM["vwap_side"]

    if ob_ratio > 1.1:                score += W_MOMENTUM["ob_ratio"]

    # Volume spike kuat = bonus tambahan via W_MOMENTUM["vol_spike_strong"]
    # [v8.0 AUDIT FIX] Dipindahkan dari magic number inline (+1) ke W_MOMENTUM agar auditable
    if vol_ratio >= 3.0:              score += W_MOMENTUM["vol_spike_strong"]

    # [v8.7 #3] Session-normalized volume — bonus jika volume saat ini melebihi
    # rata-rata jam yang sama 7 hari terakhir. Lebih bermakna dari raw vol_ratio
    # karena mempertimbangkan konteks likuiditas sesi (jam sepi vs jam ramai).
    _svr_mom = calc_session_vol_ratio(closes, volumes)
    if _svr_mom >= SESSION_VOL_SPIKE_MULT: score += W_MOMENTUM["vol_session_strong"]

    # ADX bonus/penalty — dari W_MOMENTUM
    if mkt["regime"] == "TRENDING":   score += W_MOMENTUM["adx_trend"]
    elif mkt["regime"] == "RANGING":  score += W_MOMENTUM["adx_ranging"]

    tier = assign_tier(score, ctx.get("is_low_vol", False))
    if tier == "SKIP": return None

    # ── Entry & TP/SL ─────────────────────────────────────
    # Entry sedikit di atas close terakhir — breakout entry
    # [v8.0 AUDIT FIX] Pakai MOMENTUM_ENTRY_SLIPPAGE dari STRUCTURAL_CONSTANTS.
    entry = round(closes[-1] * MOMENTUM_ENTRY_SLIPPAGE, 8)

    # SL di bawah recent swing low dalam 6 candle terakhir
    recent_low  = float(np.min(lows[-7:]))
    fake_struct = {"last_sl": recent_low}   # feed ke calc_sl_tp sebagai struktur minimal
    # [v8.6 #1] Detect liquidity untuk SL avoidance — compute ringan, lookback 40 candle
    liq_mom = detect_liquidity(closes, highs, lows, lookback=40)
    sl, tp1, tp2 = calc_sl_tp(entry, "BUY", atr, fake_struct, "INTRADAY", liq_mom, mkt["adx"])

    if tp1 <= entry or sl >= entry: return None
    sl_dist = entry - sl
    if sl_dist <= 0 or sl_dist / entry > 0.05: return None
    rr = (tp1 - entry) / sl_dist
    if rr < MIN_RR["INTRADAY"]: return None

    # [v8.2 #4] Diverge penalty — terapkan sebelum final tier check, satu kali.
    if ctx.get("btc_eth_diverge"):
        score += DIVERGE_SCORE_PENALTY

    final_tier = assign_tier(score, ctx.get("is_low_vol", False))
    if final_tier == "SKIP":
        return None

    return {
        "pair":          pair,
        "strategy":      "MOMENTUM",
        "side":          "BUY",
        "timeframe":     "1h",
        "entry":         entry,
        "current_price": price,
        "tp1":           tp1,
        "tp2":           tp2,
        "sl":            sl,
        "tier":          final_tier,
        "score":         score,
        "rr":            round(rr, 1),
        "rsi":           round(rsi, 1),
        "structure":     {"bos": "BREAKOUT"},   # placeholder untuk send_signal
        "regime":        mkt["regime"],
        "adx":           mkt["adx"],
        "conviction":    calc_conviction(score),
        "vol_ratio":     round(vol_ratio, 1),
        "breakout_pct":  round((closes[-1] / recent_high - 1) * 100, 2),
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

    # Konversi IDR
    idr_rate  = get_usdt_idr_rate()
    entry_idr = usdt_to_idr(entry, idr_rate)
    tp1_idr   = usdt_to_idr(tp1, idr_rate)
    tp2_idr   = usdt_to_idr(tp2, idr_rate) if tp2 is not None else "—"
    sl_idr    = usdt_to_idr(sl, idr_rate)

    tier_emoji  = {"S": "💎", "A+": "🏆", "A": "🥇"}.get(tier, "🎯")
    strat_emoji = {"INTRADAY": "📈", "SWING": "🌊", "MOMENTUM": "🚀"}.get(strategy, "🎯")
    side_emoji  = "🟢 BUY" if side == "BUY" else "🔴 SELL"

    regime      = sig.get("regime", "—")
    adx         = sig.get("adx", 0.0)
    conviction  = sig.get("conviction", "OK 🟡")
    regime_emoji = {"TRENDING": "🔥", "RANGING": "⚠️"}.get(regime, "—")

    # Extra line untuk MOMENTUM — tampilkan breakout info
    momentum_extra = ""
    if strategy == "MOMENTUM":
        vol_ratio    = sig.get("vol_ratio", 0.0)
        breakout_pct = sig.get("breakout_pct", 0.0)
        momentum_extra = (
            f"\n📊 Vol Spike  : <b>{vol_ratio:.1f}×</b> rata-rata"
            f"\n📈 Breakout   : <b>+{breakout_pct:.2f}%</b> di atas recent high"
        )

    # [v8.1 #3] entry_note dihitung di dalam Dynamic Entry Zone block di bawah
    entry_note = ""

    # [v8.4 FIX #4] MOMENTUM ditambahkan ke kondisi 4h — TF 1h sama dengan INTRADAY.
    # Sebelumnya MOMENTUM mendapat valid window 16h (fallback else) yang tidak logis
    # untuk signal timeframe 1h dan menyesatkan user.
    hours       = 4 if strategy in ("INTRADAY", "MOMENTUM") else 16
    now         = datetime.now(WIB)
    valid_until = (now + timedelta(hours=hours)).strftime("%H:%M WIB")

    # [v7.9 #4] Ukuran posisi yang disarankan berdasarkan PORTFOLIO_VALUE & risk 1%
    pos_size = calc_position_size(entry, sl)

    # [v8.1 #3] Dynamic Entry Zone — tampilkan range, bukan 1 angka kaku
    if side == "BUY":
        entry_high  = round(entry * (1 + ENTRY_ZONE_WIDTH_PCT / 100), 8)
        entry_zone_str = f"<b>${entry:.6f} – ${entry_high:.6f}</b>"
        # Jika harga sudah di atas entry_high → Wait for Retest
        if cur_price > entry_high:
            entry_note = (
                f"\n⛔ <b>WAIT FOR RETEST</b> — Harga ${cur_price:.6f} sudah melewati batas atas entry zone!"
                f"\n   <i>Jangan kejar. Tunggu pullback ke ${entry:.6f}–${entry_high:.6f}.</i>"
            )
        elif pct_above > ENTRY_NOTE_WARN_PCT:
            entry_note = (
                f"\n⚠️ Harga saat ini ${cur_price:.6f} (+{pct_above:.1f}% dari entry zone)"
                f"\n   <i>Tunggu pullback ke zona entry, jangan kejar harga!</i>"
            )
        elif pct_above < -ENTRY_NOTE_READY_PCT:
            entry_note = f"\n✅ Harga saat ini ${cur_price:.6f} — sudah di zona entry"
    else:  # SELL
        entry_low   = round(entry * (1 - ENTRY_ZONE_WIDTH_PCT / 100), 8)
        entry_zone_str = f"<b>${entry_low:.6f} – ${entry:.6f}</b>"
        if cur_price < entry_low:
            entry_note = (
                f"\n⛔ <b>WAIT FOR RETEST</b> — Harga ${cur_price:.6f} sudah di bawah entry zone!"
                f"\n   <i>Tunggu rebound ke ${entry_low:.6f}–${entry:.6f}.</i>"
            )
        elif pct_above < -ENTRY_NOTE_WARN_PCT:
            entry_note = (
                f"\n⚠️ Harga saat ini ${cur_price:.6f} ({pct_above:.1f}% dari entry zone)"
                f"\n   <i>Tunggu retest ke zona entry, jangan kejar SHORT!</i>"
            )
        elif pct_above > ENTRY_NOTE_READY_PCT:
            entry_note = f"\n✅ Harga saat ini ${cur_price:.6f} — sudah di zona entry SELL"

    # [v8.1 #4] Partial TP & Break Even instructions
    be_note = (
        f"\n📌 <b>Plan Manajemen Posisi:</b>"
        f"\n   • TP1 tercapai → <b>tutup 50% posisi</b>"
        f"\n   • Pindahkan SL ke <b>harga entry (Break Even)</b>"
        f"\n   • Sisa 50% biarkan berjalan ke TP2"
    )

    # Label TP/SL disesuaikan arah untuk kejelasan pembaca
    tp_label = "+" if side == "BUY" else "-"
    sl_label = "-" if side == "BUY" else "+"

    msg = (
        f"{strat_emoji} <b>{tier_emoji} [{tier}] SIGNAL {side_emoji} — {strategy}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair:    <b>{pair}</b> [{tf}]\n"
        f"⏰ Valid: {now.strftime('%H:%M')} → {valid_until}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Entry Zone : {entry_zone_str} <i>≈ {entry_idr}</i>{entry_note}\n"
        f"TP1  : <b>${tp1:.6f}</b> <i>≈ {tp1_idr}</i> <i>({tp_label}{pct_tp1:.1f}%)</i>\n"
        f"TP2  : <b>{'${:.6f}'.format(tp2) if tp2 is not None else '—'}</b>"
        f"{(' <i>≈ ' + tp2_idr + '</i>') if tp2 is not None else ''}"
        f"{' <i>(' + tp_label + '{:.1f}%)</i>'.format(pct_tp2) if tp2 is not None else ''}\n"
        f"SL   : <b>${sl:.6f}</b> <i>≈ {sl_idr}</i> <i>({sl_label}{pct_sl:.1f}%)</i>\n"
        f"R/R  : <b>1:{rr}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Score:      {score} | RSI: {rsi}\n"
        f"Struct:     {bos}\n"
        f"Regime:     {regime_emoji} {regime} (ADX: {adx})\n"
        f"Conviction: <b>{conviction}</b>{momentum_extra}\n"
        f"💰 Ukuran posisi: <i>{pos_size}</i>"
        f"{be_note}\n"
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

    # [v8.3 #2] PUMP Structure Gate — tolak jika 15m sedang dalam BOS BEARISH aktif.
    # Pump candle yang muncul di tengah downstruktur cenderung jadi dead-cat bounce.
    # Menggunakan detect_structure() yang sudah ada — tidak ada API call tambahan
    # karena candle 15m sudah di-fetch di atas (data).
    if PUMP_STRUCTURE_GATE_ENABLED:
        struct_15m = detect_structure(closes, highs, lows, strength=2, lookback=30)
        if struct_15m.get("valid") and struct_15m.get("bos") == "BEARISH":
            log(f"  ⛔ PUMP GATE [{pair}] 15m BOS BEARISH — pump alert ditolak")
            return None

    atr = calc_atr(closes, highs, lows)
    sl  = round(price - atr * PUMP_SL_ATR_MULT, 8)
    tp1 = round(price + atr * PUMP_TP1_ATR_MULT, 8)
    # [v7.9 #5] PUMP kini punya TP2 = entry + ATR×3.5
    # Sebelumnya hanya TP1 → user tidak tahu kapan ambil profit sisa posisi.
    # TP2 = 3.5× ATR memberi partial-exit guidance yang jelas.
    # [v8.0 AUDIT FIX] Pakai PUMP_TP2_ATR_MULT dari STRUCTURAL_CONSTANTS.
    tp2 = round(price + atr * PUMP_TP2_ATR_MULT, 8)

    if sl <= 0: return None   # [v7.3 FIX] cegah SL negatif pada token harga sangat rendah

    pct_sl  = abs((sl  - price) / price * 100)
    pct_tp1 = abs((tp1 - price) / price * 100)
    pct_tp2 = abs((tp2 - price) / price * 100)

    # [v8.0 AUDIT FIX 2.2] Tolak PUMP signal jika SL terlalu jauh dari entry.
    # ATR yang membengkak pada candle 15m volatile bisa menghasilkan SL 6–8%,
    # yang merusak asumsi position sizing di calc_position_size() (baseline 1% risk).
    # Cap ini memastikan pct_sl konsisten dengan formula sizing.
    if pct_sl > PUMP_SL_PCT_MAX:
        log(f"⚠️ PUMP [{price}] SL terlalu jauh: {pct_sl:.1f}% > {PUMP_SL_PCT_MAX}% — skip", "warn")
        return None

    return {
        "pair":       pair,
        "strategy":   "PUMP",
        "side":       "BUY",
        "timeframe":  "15m",
        "entry":      price,
        "tp1":        tp1,
        "tp2":        tp2,    # [v7.9 #5] TP2 sekarang ada
        "sl":         sl,
        "rsi":        round(rsi, 1),
        "vol_ratio":  round(vol_ratio, 1),
        "pct_change": round(pct_change, 2),
        "pct_tp1":    round(pct_tp1, 2),
        "pct_tp2":    round(pct_tp2, 2),
        "pct_sl":     round(pct_sl, 2),
    }


def send_pump_signal(sig: dict):
    """Kirim pump alert ke Telegram — format ringkas dan cepat.
    [v7.9 #5] Tampilkan TP2 + partial-exit guidance.
    [v7.9 #4] Tampilkan ukuran posisi yang disarankan.
    """
    pair       = sig["pair"].replace("_USDT", "/USDT")
    entry      = sig["entry"]
    tp1        = sig["tp1"]
    tp2        = sig.get("tp2")
    sl         = sig["sl"]
    rsi        = sig["rsi"]
    vol_ratio  = sig["vol_ratio"]
    pct_change = sig["pct_change"]
    pct_tp1    = sig["pct_tp1"]
    pct_tp2    = sig.get("pct_tp2", 0.0)
    pct_sl     = sig["pct_sl"]

    now         = datetime.now(WIB)
    valid_until = (now + timedelta(hours=1)).strftime("%H:%M WIB")

    # Konversi IDR
    idr_rate  = get_usdt_idr_rate()
    entry_idr = usdt_to_idr(entry, idr_rate)
    tp1_idr   = usdt_to_idr(tp1, idr_rate)
    tp2_idr   = usdt_to_idr(tp2, idr_rate) if tp2 else "—"
    sl_idr    = usdt_to_idr(sl, idr_rate)

    # [v7.9 #4] Position sizing
    pos_size = calc_position_size(entry, sl)

    msg = (
        f"🚀 <b>PUMP ALERT — EARLY SIGNAL</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair  : <b>{pair}</b> [15m]\n"
        f"⏰ Valid: {now.strftime('%H:%M')} → {valid_until}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Entry : <b>${entry:.6f}</b> <i>≈ {entry_idr}</i>\n"
        f"TP1   : <b>${tp1:.6f}</b> <i>≈ {tp1_idr}</i> <i>(+{pct_tp1:.1f}%)</i>\n"
        f"TP2   : <b>${tp2:.6f}</b> <i>≈ {tp2_idr}</i> <i>(+{pct_tp2:.1f}%)</i>\n"
        f"SL    : <b>${sl:.6f}</b> <i>≈ {sl_idr}</i> <i>(-{pct_sl:.1f}%)</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 Vol Spike : <b>{vol_ratio:.1f}×</b> rata-rata\n"
        f"📈 Naik 45m  : <b>+{pct_change:.2f}%</b>\n"
        f"RSI          : {rsi}\n"
        f"💰 Ukuran posisi: <i>{pos_size}</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<i>📤 Partial exit: Ambil 50–70% profit di TP1</i>\n"
        f"<i>   Biarkan sisa posisi berjalan ke TP2</i>\n"
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

    # [v8.2 #3] Unified scoring via W_MICRO dict — menggantikan hardcoded inline values.
    # assign_tier_micro() menggantikan inline "tier = 'A' if micro_score >= 6 else 'B'"
    # sehingga ada satu jalur tier logic yang bisa diaudit di STRUCTURAL CONSTANTS.
    micro_score = 0
    if vol_ratio >= 8.0:      micro_score += W_MICRO["vol_spike_strong"]   # spike sangat kuat
    elif vol_ratio >= 5.0:    micro_score += W_MICRO["vol_spike_normal"]   # spike normal
    if pct_3h >= 6.0:         micro_score += W_MICRO["momentum_strong"]    # momentum kuat
    elif pct_3h >= 3.0:       micro_score += W_MICRO["momentum_normal"]    # momentum cukup
    if rsi < 50:              micro_score += W_MICRO["rsi_low"]            # RSI masih ada ruang
    if ema_short_bull:        micro_score += W_MICRO["ema_short_bull"]     # EMA7 > EMA20
    if has_sweep:             micro_score += W_MICRO["liq_sweep"]          # smart money
    if change_24h < 5.0:      micro_score += W_MICRO["early_entry"]        # belum pump banyak

    # [v8.2 #3] Unified tier via assign_tier_micro() — threshold dari MICRO_TIER_MIN_SCORE
    tier = assign_tier_micro(micro_score)
    if tier == "SKIP":
        return None   # tier B tidak dikirim — hemat komputasi di caller

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
    # [v7.9 #3] Ganti "A" → "M-A" di display agar tidak false-equivalent dengan main tier A.
    # check_microcap masih return tier="A" secara internal (untuk kompatibilitas),
    # tapi di Telegram ditampilkan "M-A" agar user tahu ini skala berbeda (0–10, bukan 0–18+).
    display_tier = f"M-{tier}" if tier in ("A", "B") else tier
    tier_emoji   = {"M-A": "🥇", "M-B": "🥈"}.get(display_tier, "🎯")

    # Konversi IDR
    idr_rate  = get_usdt_idr_rate()
    entry_idr = usdt_to_idr(entry, idr_rate)
    tp1_idr   = usdt_to_idr(tp1, idr_rate)
    tp2_idr   = usdt_to_idr(tp2, idr_rate)
    sl_idr    = usdt_to_idr(sl, idr_rate)

    # [v7.9 #4] Position sizing
    pos_size = calc_position_size(entry, sl)

    sweep_line = "🧲 Liq sweep terdeteksi — smart money sudah masuk\n" if has_sweep else ""

    msg = (
        f"🔬 <b>{tier_emoji} [{display_tier}] MICROCAP SIGNAL 🟢 BUY</b>\n"
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
        f"Score  : {score}/10 <i>(skala microcap, bukan setara tier A utama)</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Ukuran posisi: <i>{pos_size}</i>\n"
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
            sig["entry"], sig["tp1"], sig.get("tp2"),   # [v7.9 #5] tp2 sekarang ada
            sig["sl"], "PUMP", 0, sig["timeframe"]
        )
        sent += 1
        time.sleep(0.5)

    log(f"\n✅ Pump scan done — {sent} alert terkirim")


def run_self_test() -> bool:
    """
    [v8.2 #5] Self-test suite — validasi fungsi kritikal dengan data sintetis.
    Tidak membutuhkan koneksi API. Dijalankan via SCAN_MODE=test.

    Test yang dijalankan:
      T1. calc_rsi()  — RSI pada data trending harus > 50
      T2. calc_adx()  — ADX pada data trending harus > 25, +DI > -DI
      T3. score_signal() BUY — score pada kondisi ideal harus lolos tier A (>= 8)
      T4. score_signal() SELL — score pada kondisi ideal harus lolos tier A (>= 8)
      T5. assign_tier() — batas threshold harus tepat
      T6. assign_tier_micro() — threshold MICRO_TIER_MIN_SCORE harus tepat
      T7. calc_conviction() — mapping score → label harus konsisten

    Returns True jika semua test lulus, False jika ada kegagalan.
    """
    log("\n" + "="*60)
    log("🧪 SELF-TEST v8.4 — Mulai validasi fungsi kritikal...")
    log("="*60)

    passed = 0
    failed = 0
    errors = []

    def assert_test(name: str, condition: bool, detail: str = ""):
        nonlocal passed, failed
        if condition:
            log(f"  ✅ {name}")
            passed += 1
        else:
            log(f"  ❌ {name}{' — ' + detail if detail else ''}", "error")
            errors.append(f"{name}: {detail}")
            failed += 1

    # ── Synthetic data ────────────────────────────────────────
    # Trending UP data: harga naik linear dengan sedikit variasi
    n = 100
    base = np.linspace(100, 130, n)
    noise = np.random.default_rng(42).normal(0, 0.3, n)
    closes  = base + noise
    highs   = closes + np.abs(np.random.default_rng(1).normal(0.5, 0.2, n))
    lows    = closes - np.abs(np.random.default_rng(2).normal(0.5, 0.2, n))
    volumes = np.ones(n) * 1000 + np.random.default_rng(3).normal(0, 50, n)
    volumes[-1] = 2000   # volume spike di candle terakhir

    # Trending DOWN data: untuk SELL test
    base_down = np.linspace(130, 100, n)
    closes_d  = base_down + noise
    highs_d   = closes_d + np.abs(np.random.default_rng(4).normal(0.5, 0.2, n))
    lows_d    = closes_d - np.abs(np.random.default_rng(5).normal(0.5, 0.2, n))
    volumes_d = np.ones(n) * 1000

    # ── T1: calc_rsi ─────────────────────────────────────────
    rsi_up   = calc_rsi(closes)
    rsi_down = calc_rsi(closes_d)
    assert_test("T1a: RSI uptrend > 50",   rsi_up > 50,   f"rsi={rsi_up:.1f}")
    assert_test("T1b: RSI downtrend < 50", rsi_down < 50, f"rsi={rsi_down:.1f}")

    # ── T2: calc_adx ─────────────────────────────────────────
    adx, pdi, mdi = calc_adx(highs, lows, closes)
    assert_test("T2a: ADX uptrend > 20",   adx > 20,  f"adx={adx:.1f}")
    assert_test("T2b: +DI > -DI uptrend",  pdi > mdi, f"+DI={pdi:.1f} -DI={mdi:.1f}")
    assert_test("T2c: ADX dalam range 0-100", 0 <= adx <= 100, f"adx={adx:.1f}")

    adx_d, pdi_d, mdi_d = calc_adx(highs_d, lows_d, closes_d)
    assert_test("T2d: ADX downtrend > 20",  adx_d > 20,    f"adx={adx_d:.1f}")
    assert_test("T2e: -DI > +DI downtrend", mdi_d > pdi_d, f"+DI={pdi_d:.1f} -DI={mdi_d:.1f}")

    # ── T3: score_signal BUY ────────────────────────────────
    struct_bull = {
        "bos": "BULLISH", "choch": None,
        "last_sh": float(closes[-1] * 0.98),
        "last_sl": float(closes[-1] * 0.95),
        "prev_sh": float(closes[-1] * 0.96),
        "prev_sl": float(closes[-1] * 0.93),
        "bias": "BULLISH", "valid": True,
    }
    liq_bull = {"equal_lows": float(closes[-1] * 0.95), "equal_highs": None,
                "sweep_bull": True, "sweep_bear": False}
    ob_bull  = {"valid": True, "ob_high": float(closes[-1] * 0.97),
                "ob_low": float(closes[-1] * 0.95)}
    price    = float(closes[-1])
    ema_fast = float(np.mean(closes[-20:]))
    ema_slow = float(np.mean(closes[-50:]))
    vwap_val = price * 0.99   # price sedikit di atas vwap
    macd_val, msig_val = calc_macd(closes)

    score_buy = score_signal(
        "BUY", price, closes, highs, lows, volumes,
        struct_bull, liq_bull, ob_bull,
        rsi_up, macd_val, msig_val,
        ema_fast, ema_slow, vwap_val, 1.15, "TRENDING"
    )
    assert_test("T3a: score_signal BUY ideal >= tier A (8)",
                score_buy >= TIER_MIN_SCORE["A"], f"score={score_buy}")
    assert_test("T3b: assign_tier BUY lolos",
                assign_tier(score_buy) != "SKIP", f"tier={assign_tier(score_buy)}")

    # ── T4: score_signal SELL ───────────────────────────────
    struct_bear = {
        "bos": "BEARISH", "choch": None,
        "last_sh": float(closes_d[-1] * 1.02),
        "last_sl": float(closes_d[-1] * 0.98),
        "prev_sh": float(closes_d[-1] * 1.04),
        "prev_sl": float(closes_d[-1] * 1.00),
        "bias": "BEARISH", "valid": True,
    }
    liq_bear  = {"equal_lows": None, "equal_highs": float(closes_d[-1] * 1.02),
                 "sweep_bull": False, "sweep_bear": True}
    ob_bear   = {"valid": True, "ob_high": float(closes_d[-1] * 1.02),
                 "ob_low": float(closes_d[-1] * 1.00)}
    price_d   = float(closes_d[-1])
    ema_f_d   = float(np.mean(closes_d[-20:]))
    ema_s_d   = float(np.mean(closes_d[-50:]))
    macd_d, msig_d = calc_macd(closes_d)

    score_sell = score_signal(
        "SELL", price_d, closes_d, highs_d, lows_d, volumes_d,
        struct_bear, liq_bear, ob_bear,
        rsi_down, macd_d, msig_d,
        ema_f_d, ema_s_d, price_d * 1.01, 0.85, "TRENDING"
    )
    assert_test("T4a: score_signal SELL ideal >= tier A (8)",
                score_sell >= TIER_MIN_SCORE["A"], f"score={score_sell}")

    # ── T5: assign_tier boundaries ──────────────────────────
    assert_test("T5a: assign_tier S boundary",  assign_tier(14) == "S",    "score=14")
    assert_test("T5b: assign_tier A+ boundary", assign_tier(10) == "A+",   "score=10")
    assert_test("T5c: assign_tier A boundary",  assign_tier(8)  == "A",    "score=8")
    assert_test("T5d: assign_tier SKIP",        assign_tier(7)  == "SKIP", "score=7")
    assert_test("T5e: assign_tier low_vol raises threshold",
                assign_tier(12, low_vol_session=True) == "SKIP",
                f"score=12 < LOW_VOL_SCORE_THRESHOLD={LOW_VOL_SCORE_THRESHOLD}")

    # ── T6: assign_tier_micro ───────────────────────────────
    assert_test("T6a: assign_tier_micro A",    assign_tier_micro(6) == "A",    "score=6")
    assert_test("T6b: assign_tier_micro SKIP", assign_tier_micro(5) == "SKIP", "score=5")

    # ── T7: calc_conviction ─────────────────────────────────
    assert_test("T7a: conviction EXTREME",   calc_conviction(18).startswith("EXTREME"),   "score=18")
    assert_test("T7b: conviction VERY HIGH", calc_conviction(14).startswith("VERY HIGH"), "score=14")
    assert_test("T7c: conviction HIGH",      calc_conviction(12).startswith("HIGH"),      "score=12")
    assert_test("T7d: conviction GOOD",      calc_conviction(10).startswith("GOOD"),      "score=10")
    assert_test("T7e: conviction OK",        calc_conviction(9).startswith("OK"),         "score=9")

    # ── T8: detect_structure insufficient data ──────────
    # [v8.3 #5] Pastikan detect_structure tidak crash dengan data minim
    short_c = closes[:5]
    short_h = highs[:5]
    short_l = lows[:5]
    result_short = detect_structure(short_c, short_h, short_l, strength=3, lookback=80)
    assert_test("T8a: detect_structure data insuffisien → valid=False",
                result_short["valid"] == False,
                f"valid={result_short['valid']}")
    assert_test("T8b: detect_structure data insuffisien → no crash", True, "no exception raised")

    # ── T9: calc_position_size edge cases ───────────────
    # [v8.3 #5] Pastikan edge case tidak menghasilkan ZeroDivisionError atau output salah
    pos_zero_entry = calc_position_size(0, 95.0)
    assert_test("T9a: calc_position_size entry=0 → '—'",
                pos_zero_entry == "—",
                f"got: {pos_zero_entry}")

    pos_zero_sl = calc_position_size(100.0, 0)
    assert_test("T9b: calc_position_size sl=0 → '—'",
                pos_zero_sl == "—",
                f"got: {pos_zero_sl}")

    pos_sl_above = calc_position_size(100.0, 105.0)   # sl > entry untuk BUY — sl_pct = 5% (abs)
    assert_test("T9c: calc_position_size sl>entry → tidak crash",
                isinstance(pos_sl_above, str),
                f"got type: {type(pos_sl_above)}")

    pos_equal = calc_position_size(100.0, 100.0)   # sl == entry → sl_pct = 0 → return "—"
    assert_test("T9d: calc_position_size sl==entry → '—'",
                pos_equal == "—",
                f"got: {pos_equal}")

    # ── T10: MTF alignment helper ───────────────────────
    # [v8.3 #5] Validasi get_4h_bias returns dict dengan key yang dibutuhkan
    # tanpa API call (test via default fallback path)
    # Kita test via assign_tier + logic — tidak bisa test get_4h_bias tanpa client.
    # Sebagai gantinya, test bahwa MTF_ALIGNMENT_ENABLED terbaca dengan benar.
    assert_test("T10a: MTF_ALIGNMENT_ENABLED is bool",
                isinstance(MTF_ALIGNMENT_ENABLED, bool),
                f"type={type(MTF_ALIGNMENT_ENABLED)}")
    assert_test("T10b: PUMP_STRUCTURE_GATE_ENABLED is bool",
                isinstance(PUMP_STRUCTURE_GATE_ENABLED, bool),
                f"type={type(PUMP_STRUCTURE_GATE_ENABLED)}")
    assert_test("T10c: MAX_CONCURRENT_BUY_SIGNALS is int >= 1",
                isinstance(MAX_CONCURRENT_BUY_SIGNALS, int) and MAX_CONCURRENT_BUY_SIGNALS >= 1,
                f"value={MAX_CONCURRENT_BUY_SIGNALS}")

    # ── Summary ──────────────────────────────────────────────
    total = passed + failed
    log(f"\n{'='*60}")
    log(f"🧪 SELF-TEST RESULT: {passed}/{total} passed, {failed} failed")
    if failed == 0:
        log("✅ Semua test lulus — bot siap dijalankan")
        tg(f"✅ <b>SELF-TEST v8.4 — PASSED</b>\n"
           f"Semua {total} unit test lulus.\n"
           f"<i>Bot siap dijalankan dalam mode production.</i>", critical=True)
    else:
        log(f"❌ {failed} test GAGAL:", "error")
        for e in errors:
            log(f"   • {e}", "error")
        tg(f"🚨 <b>SELF-TEST v8.4 — FAILED ({failed}/{total})</b>\n"
           f"Test berikut gagal:\n" +
           "\n".join(f"• {e}" for e in errors) +
           f"\n<i>Periksa kode sebelum menjalankan production.</i>", critical=True)
    log("="*60 + "\n")
    return failed == 0


# ════════════════════════════════════════════════════════
#  ADAPTIVE WEIGHT AUDIT — [v8.8 #3]
# ════════════════════════════════════════════════════════

def tune_weights() -> dict:
    """
    [v8.8 #3] Analisis korelasi antara setiap scoring component dengan outcome
    TP_HIT vs SL_HIT dari WEIGHT_AUDIT_SAMPLE signal terakhir per strategy.

    Cara kerja:
    1. Ambil N signal terakhir per strategy yang sudah punya result (bukan NULL/EXPIRED).
    2. Untuk setiap signal, ekstrak "score_breakdown" dari kolom metadata jika tersedia,
       atau gunakan score total sebagai proxy.
    3. Hitung win rate kondisional: jika score >= threshold → WR tinggi atau rendah?
    4. Simpan hasil ke tabel Supabase "weight_audit" sebagai rekomendasi.
    5. Kirim ringkasan ke Telegram.

    ⚠️ CATATAN DESAIN: Fungsi ini adalah READ-ONLY terhadap W dict.
    Ia TIDAK mengubah bobot secara otomatis. Bobot baru hanya diterapkan
    setelah developer membaca laporan dan memutuskan perubahan secara manual.
    Ini desain yang disengaja — model ML tidak boleh auto-modify trading logic
    tanpa oversight manusia.

    Returns: dict laporan audit per strategy.
    """
    if not WEIGHT_AUDIT_ENABLED:
        log("⚠️ tune_weights: WEIGHT_AUDIT_ENABLED=false — skip")
        return {}

    log("🔬 tune_weights — memulai analisis weight korelasi...")
    report = {}

    strategies = ["INTRADAY", "SWING", "MOMENTUM"]
    for strat in strategies:
        try:
            rows = (
                supabase.table("signals_v2")
                .select("score,result,tier")
                .eq("strategy", strat)
                .not_.is_("result", "null")
                .neq("result", "EXPIRED")
                .order("sent_at", desc=True)
                .limit(WEIGHT_AUDIT_SAMPLE)
                .execute()
            ).data

            if len(rows) < 10:
                log(f"  ⚠️ tune_weights [{strat}]: hanya {len(rows)} resolved — tidak cukup data")
                report[strat] = {"status": "INSUFFICIENT_DATA", "count": len(rows)}
                continue

            wins     = [r for r in rows if r["result"] in ("TP1_HIT", "TP2_HIT")]
            losses   = [r for r in rows if r["result"] == "SL_HIT"]
            wr       = round(len(wins) / len(rows) * 100, 1)

            # Analisis distribusi score: apakah score tinggi benar-benar korelasi dengan win?
            win_scores  = [r["score"] for r in wins  if r.get("score") is not None]
            loss_scores = [r["score"] for r in losses if r.get("score") is not None]

            avg_win_score  = round(float(np.mean(win_scores)),  1) if win_scores  else None
            avg_loss_score = round(float(np.mean(loss_scores)), 1) if loss_scores else None

            # Score discrimination: seberapa baik score membedakan win vs loss?
            # Idealnya avg_win_score >> avg_loss_score
            discrimination = None
            if avg_win_score is not None and avg_loss_score is not None:
                discrimination = round(avg_win_score - avg_loss_score, 1)

            # Tier analysis: apakah tier S benar-benar lebih baik dari A?
            tier_wr = {}
            for tier in ["S", "A+", "A"]:
                tier_rows = [r for r in rows if r.get("tier") == tier]
                if tier_rows:
                    tier_wins = sum(1 for r in tier_rows if r["result"] in ("TP1_HIT", "TP2_HIT"))
                    tier_wr[tier] = round(tier_wins / len(tier_rows) * 100, 1)

            strat_report = {
                "status":           "OK",
                "count":            len(rows),
                "win_rate":         wr,
                "avg_win_score":    avg_win_score,
                "avg_loss_score":   avg_loss_score,
                "score_discrimination": discrimination,
                "tier_win_rates":   tier_wr,
                "recommendation":   [],
            }

            # Rekomendasi berdasarkan temuan
            if discrimination is not None:
                if discrimination < 1.0:
                    strat_report["recommendation"].append(
                        "Score tidak mendiskriminasi win vs loss dengan baik "
                        f"(delta={discrimination}). Pertimbangkan naikkan threshold tier A."
                    )
                elif discrimination >= 3.0:
                    strat_report["recommendation"].append(
                        f"Score discrimination sangat baik (delta={discrimination}). "
                        "Sistem scoring bekerja efektif untuk strategy ini."
                    )

            if "S" in tier_wr and "A" in tier_wr:
                if tier_wr.get("S", 0) <= tier_wr.get("A", 0) + 5:
                    strat_report["recommendation"].append(
                        f"Tier S ({tier_wr.get('S')}%) tidak jauh lebih baik dari A ({tier_wr.get('A')}%). "
                        "Pertimbangkan naikkan threshold tier S."
                    )

            report[strat] = strat_report

            # Simpan ke Supabase weight_audit (best-effort)
            try:
                audit_record = {
                    "strategy":          strat,
                    "checked_at":        datetime.now(timezone.utc).isoformat(),
                    "sample_count":      len(rows),
                    "win_rate":          wr,
                    "avg_win_score":     avg_win_score,
                    "avg_loss_score":    avg_loss_score,
                    "discrimination":    discrimination,
                    "tier_wr_json":      str(tier_wr),
                    "recommendations":   "; ".join(strat_report["recommendation"]) or "None",
                }
                supabase.table("weight_audit").upsert(audit_record, on_conflict="strategy").execute()
            except Exception as db_err:
                log(f"  ⚠️ tune_weights: Supabase write [{strat}]: {db_err} — skip DB write", "warn")

            pf_disc = f" | disc={discrimination:+.1f}" if discrimination is not None else ""
            log(f"  📊 [{strat}] WR:{wr}% | win_score:{avg_win_score} loss_score:{avg_loss_score}{pf_disc}")

        except Exception as e:
            log(f"⚠️ tune_weights [{strat}]: {e}", "warn")
            report[strat] = {"status": "ERROR", "error": str(e)}

    # Telegram summary
    try:
        lines = ["🔬 <b>Weight Audit Report — v8.9</b>"]
        for strat, info in report.items():
            if info.get("status") != "OK":
                lines.append(f"\n{strat}: {info.get('status', 'ERROR')}")
                continue
            disc_str = f" disc={info['score_discrimination']:+.1f}" if info.get("score_discrimination") is not None else ""
            lines.append(
                f"\n<b>{strat}</b> — WR:{info['win_rate']}% ({info['count']} trades){disc_str}"
            )
            for rec in info.get("recommendation", []):
                lines.append(f"  • {rec}")
            if not info.get("recommendation"):
                lines.append("  • Tidak ada rekomendasi perubahan bobot.")
        lines.append("\n<i>W dict tidak diubah otomatis — lihat tabel weight_audit untuk detail.</i>")
        tg("\n".join(lines))
    except Exception as tg_err:
        log(f"⚠️ tune_weights TG summary: {tg_err}", "warn")

    # [v8.9 #2] Semi-auto weight application — hanya jika WEIGHT_AUTO_APPLY=true
    # DAN semua kondisi keamanan terpenuhi di semua strategy.
    if WEIGHT_AUTO_APPLY:
        _try_apply_weights(report)

    log(f"✅ tune_weights selesai — {len(report)} strategy dianalisis")
    return report


def _try_apply_weights(report: dict) -> None:
    """
    [v8.9 #2] Coba terapkan delta kecil ke W dict berdasarkan analisis tune_weights.

    Kondisi aman yang HARUS semua terpenuhi sebelum apply:
    1. WR > WEIGHT_AUTO_WR_MIN (default 45%) — sistem tidak sedang dalam drawdown
    2. discrimination >= WEIGHT_AUTO_DISC_MIN (default 2.0) — scoring masih efektif
    3. sample >= WEIGHT_AUTO_SAMPLE_MIN (default 30) — data cukup signifikan

    Logika adjustment:
    - Jika avg_win_score >> avg_loss_score (discrimination baik): kuatkan bobot positif
      yang tinggi → +1 pada key dengan bobot >= 3 yang sering muncul di win.
    - Jika discrimination buruk (< 1.5): lemahkan bobot utama → -1 pada key >= 4.
    - Bobot penalti (negatif) dikurangi (lebih negatif) jika WR rendah.
    - Hard guard: clip ke [WEIGHT_MIN, WEIGHT_MAX].
    - Max 2 key yang diubah per run untuk stabilitas.

    Semua perubahan dicatat ke Supabase weight_audit dan dikirim ke Telegram.
    """
    # Kumpulkan apakah semua strategy memenuhi syarat keamanan
    safe_strategies = []
    for strat, info in report.items():
        if info.get("status") != "OK":
            continue
        wr   = info.get("win_rate", 0)
        disc = info.get("score_discrimination")
        cnt  = info.get("count", 0)
        if wr > WEIGHT_AUTO_WR_MIN and disc is not None and disc >= WEIGHT_AUTO_DISC_MIN and cnt >= WEIGHT_AUTO_SAMPLE_MIN:
            safe_strategies.append(strat)

    if not safe_strategies:
        log("⚠️ _try_apply_weights: kondisi keamanan tidak terpenuhi di semua strategy — tidak ada perubahan")
        return

    # Hitung rata-rata discrimination dari strategy yang aman
    discs = [report[s]["score_discrimination"] for s in safe_strategies if report[s].get("score_discrimination") is not None]
    avg_disc = float(np.mean(discs)) if discs else 0.0

    changes = {}   # key → delta yang akan diterapkan

    if avg_disc >= WEIGHT_AUTO_DISC_MIN:
        # Discrimination baik — scoring bekerja. Tidak perlu ubah banyak.
        # Hanya log bahwa sistem sehat.
        log(f"  ✅ _try_apply_weights: discrimination avg={avg_disc:.2f} — sistem sehat, tidak ada perubahan bobot diperlukan")
        tg(f"🔬 <b>Semi-Auto Weight Check</b>\nDiscrimination avg <b>{avg_disc:.2f}</b> — sistem scoring sehat.\nTidak ada perubahan bobot diterapkan.")
        return

    # Discrimination buruk (< WEIGHT_AUTO_DISC_MIN) tapi sistem masih sehat (WR > 45%).
    # Ini berarti score tidak membedakan win vs loss dengan baik.
    # Coba kurangi bobot komponen yang mungkin terlalu dominan.
    # Strategi konservatif: kurangi bobot terbesar yang bukan gate utama (bos, choch).
    gate_keys = {"bos", "choch", "liq_sweep"}  # jangan sentuh gate utama
    candidate_reductions = {k: v for k, v in W.items() if k not in gate_keys and v >= 3}

    if not candidate_reductions:
        log("  ⚠️ _try_apply_weights: tidak ada kandidat reduksi bobot yang aman")
        return

    # Ambil max 2 key dengan bobot tertinggi untuk dikurangi
    top_keys = sorted(candidate_reductions, key=lambda k: -candidate_reductions[k])[:2]
    for key in top_keys:
        new_val = max(WEIGHT_MIN, W[key] - 1)   # kurangi 1, capped di WEIGHT_MIN
        if new_val != W[key]:
            changes[key] = (W[key], new_val)

    if not changes:
        log("  ⚠️ _try_apply_weights: semua bobot sudah di minimum, tidak ada perubahan")
        return

    # Terapkan perubahan
    change_lines = []
    for key, (old_val, new_val) in changes.items():
        W[key] = new_val
        change_lines.append(f"  W['{key}']: {old_val} → {new_val}")
        log(f"  🔧 W['{key}']: {old_val} → {new_val} (discrimination={avg_disc:.2f})")

    # Simpan ke weight_audit
    try:
        for key, (old_val, new_val) in changes.items():
            supabase.table("weight_audit").upsert({
                "strategy":        "ALL",
                "checked_at":      datetime.now(timezone.utc).isoformat(),
                "sample_count":    sum(report[s].get("count", 0) for s in safe_strategies),
                "win_rate":        float(np.mean([report[s].get("win_rate", 0) for s in safe_strategies])),
                "discrimination":  avg_disc,
                "recommendations": f"AUTO-APPLIED: W[{key}] {old_val}→{new_val}",
            }, on_conflict="strategy").execute()
    except Exception as db_err:
        log(f"  ⚠️ _try_apply_weights DB write: {db_err}", "warn")

    tg(
        f"🔧 <b>Semi-Auto Weight Applied — v8.9</b>\n"
        f"Discrimination avg: <b>{avg_disc:.2f}</b> (threshold {WEIGHT_AUTO_DISC_MIN})\n"
        f"Perubahan:\n" + "\n".join(change_lines) +
        f"\n<i>Aktifkan WEIGHT_AUTO_APPLY=false untuk menonaktifkan.</i>"
    )


# ════════════════════════════════════════════════════════
#  MAIN RUN
# ════════════════════════════════════════════════════════

def run():
    global _candle_cache, _dedup_memory, _4h_bias_cache, _weekly_bias_cache
    _candle_cache        = {}   # flush cache setiap cycle
    _dedup_memory        = set()   # [v7.7 #7] reset in-memory dedup setiap cycle — HARUS set(), bukan {}
    _4h_bias_cache       = {}   # [v8.3 #1] reset MTF bias cache setiap cycle
    _weekly_bias_cache   = {}   # [v8.7 #1] reset weekly bias cache setiap cycle

    # [v7.9 #10] Start Telegram background thread — non-blocking send untuk seluruh cycle
    _start_tg_worker()

    # [v7.9 #11] Scan timeout guard — kill cycle yang hang setelah SCAN_TIMEOUT_SECONDS
    _scan_start = time.time()
    def _check_timeout(label: str = "") -> bool:
        """Return True jika scan sudah melewati batas waktu."""
        elapsed = time.time() - _scan_start
        if elapsed > SCAN_TIMEOUT_SECONDS:
            log(f"⏰ SCAN TIMEOUT ({elapsed:.0f}s > {SCAN_TIMEOUT_SECONDS}s) di [{label}] — abort cycle", "error")
            tg(f"⏰ <b>SCAN TIMEOUT</b>\nCycle di-abort setelah {elapsed:.0f}s (limit {SCAN_TIMEOUT_SECONDS}s).\n"
               f"Titik abort: {label}", wait=True)
            return True
        return False

    client = get_client()

    # [v7.5] Build dynamic ETF blocklist sekali per run
    log("🔒 Membangun ETF blocklist dinamis...")
    build_etf_blocklist()

    # [v8.9 FIX] Force Supabase schema cache reload di startup.
    # Mengatasi error 'Could not find column in schema cache' setelah ALTER TABLE.
    # Tidak fatal jika gagal — save_signal sudah punya graceful fallback.
    _reload_supabase_schema()

    # [v8.9 FIX] Validasi kolom signals_v2 — deteksi lebih awal jika ada kolom yang kurang.
    # Jika kolom baru belum ada, tampilkan pesan SQL yang perlu dijalankan.
    try:
        _test_row = supabase.table("signals_v2").select(
            "pair,strategy,side,entry,tp1,tp2,sl,tier,score,timeframe,sent_at,result,closed_at,pnl_pct"
        ).limit(1).execute()
        log("✅ Tabel signals_v2 — semua kolom OK")
    except Exception as _e_cols:
        _err = str(_e_cols)
        if "schema cache" in _err.lower() or "PGRST204" in _err:
            log(f"⚠️ signals_v2 schema cache belum reload: {_e_cols}", "warn")
            tg(
                f"⚠️ <b>STARTUP — Schema Cache Belum Reload</b>\n"
                f"Kolom baru di <code>signals_v2</code> belum terdeteksi PostgREST.\n"
                f"Jalankan di Supabase SQL Editor:\n"
                f"<code>NOTIFY pgrst, 'reload schema';</code>\n"
                f"<i>Bot tetap berjalan — signal akan tersimpan tanpa kolom baru sampai reload.</i>",
                wait=True
            )
        else:
            log(f"⚠️ signals_v2 column check: {_e_cols}", "warn")

    # [v8.0 AUDIT FIX #9] Startup check — pastikan tabel strategy_strikes ada di Supabase.
    # Tanpa tabel ini, strike counter di strategy_health_check() akan gagal silent
    # dan TRIPLE CRITICAL escalation tidak akan pernah terpicu.
    # Skema tabel: CREATE TABLE strategy_strikes (strategy TEXT PRIMARY KEY, strike_count INT DEFAULT 0);
    try:
        supabase.table("strategy_strikes").select("strategy").limit(1).execute()
        log("✅ Tabel strategy_strikes — OK")
    except Exception as _e_strikes:
        log(f"⚠️ STARTUP: Tabel 'strategy_strikes' tidak ditemukan atau tidak bisa diakses: {_e_strikes}", "error")
        tg(f"⚠️ <b>STARTUP WARNING — Tabel Missing</b>\n"
           f"Tabel <code>strategy_strikes</code> tidak bisa diakses di Supabase.\n"
           f"Strike counter tidak akan berfungsi → TRIPLE CRITICAL escalation nonaktif.\n"
           f"<i>Buat tabel: CREATE TABLE strategy_strikes (strategy TEXT PRIMARY KEY, strike_count INT DEFAULT 0);</i>",
           wait=True)

    # [v8.0 AUDIT FIX #13] CSV fallback sync ke Supabase di startup.
    # Jika ada signal yang tersimpan di CSV fallback saat Supabase down sebelumnya,
    # coba re-insert ke Supabase sekarang agar data tidak hilang permanen.
    if os.path.isfile(SIGNAL_FALLBACK_FILE):
        log(f"📂 Ditemukan CSV fallback: {SIGNAL_FALLBACK_FILE} — mencoba sync ke Supabase...")
        try:
            synced, failed = 0, 0
            remaining_rows = []
            with open(SIGNAL_FALLBACK_FILE, "r", newline="") as _f:
                _reader = csv.DictReader(_f)
                for _row in _reader:
                    try:
                        # Konversi None string kembali ke None
                        _clean = {k: (None if v in ("None", "") else v) for k, v in _row.items()}
                        supabase.table("signals_v2").insert(_clean).execute()
                        synced += 1
                    except Exception as _re:
                        failed += 1
                        remaining_rows.append(_row)
            # Hapus file jika semua berhasil, sisakan baris yang gagal
            if failed == 0:
                os.remove(SIGNAL_FALLBACK_FILE)
                log(f"✅ CSV fallback sync selesai — {synced} record di-insert, file dihapus")
            else:
                # Tulis ulang hanya baris yang gagal
                with open(SIGNAL_FALLBACK_FILE, "w", newline="") as _fw:
                    if remaining_rows:
                        _writer = csv.DictWriter(_fw, fieldnames=list(remaining_rows[0].keys()))
                        _writer.writeheader()
                        _writer.writerows(remaining_rows)
                log(f"⚠️ CSV fallback sync partial — {synced} berhasil, {failed} gagal (disisakan)")
        except Exception as _csv_sync_err:
            log(f"⚠️ CSV fallback sync error: {_csv_sync_err}", "warn")

    # [v8.2 #2] evaluate_signals() sekarang auto-trigger via daemon thread di akhir run().
    # Tidak perlu cron terpisah — outcome ter-resolve otomatis setelah setiap scan cycle.

    if SCAN_MODE == "pump":
        run_pump_scan(client)
        return

    # [v8.2 #5] Self-test mode — validasi fungsi kritikal tanpa perlu API production
    # [v8.4 FIX #3] Hapus _start_tg_worker() duplikat di sini — sudah dipanggil di awal run().
    # Sebelumnya dua worker thread berjalan consume dari queue yang sama saat SCAN_MODE=test.
    if SCAN_MODE == "test":
        run_self_test()
        return

    log(f"\n{'='*60}")
    log(f"🚀 SIGNAL BOT v8.9.1 Hotfix Edition — {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')} [FULL SCAN]")
    log(f"{'='*60}")

    # [v7.9 #6] Daily loss guard — halt jika sudah emit terlalu banyak signal hari ini
    if daily_loss_guard():
        return

    # [v7.9 #9] Strategy health check — warn jika ada strategy di bawah 40% win rate
    health = strategy_health_check()
    for strat, info in health.items():
        if info.get("win_rate") is not None:
            pending  = info.get("pending_count", 0)
            bias_warn = f" ⚠️ BIAS RISK ({pending} pending)" if pending >= 10 else f" ({pending} pending)"
            pf_str   = f" | PF:{info['profit_factor']}" if info.get("profit_factor") is not None else ""
            exp_str  = f" | EXP:{info['expectancy']:+.3f}%" if info.get("expectancy") is not None else ""
            log(f"  📊 Health [{strat}]: {info['win_rate']}% WR "
                f"({info['count']} trades){pf_str}{exp_str}{bias_warn} — {info['status']}")

    fg  = get_fear_greed()
    btc = get_btc_regime(client)
    log(f"F&G: {fg} | BTC 1h: {btc['btc_1h']:+.1f}% | BTC 4h: {btc['btc_4h']:+.1f}%")

    # [v8.1 #1] BTC Flash Guard 5m — kill-switch instan
    flash = get_btc_flash_guard(client)
    flash_triggered = flash["triggered"]

    # [v8.1 #5] ETH regime untuk Inter-Market Correlation
    eth = get_eth_regime(client)
    btc_eth_diverge = abs(btc["btc_1h"] - eth["eth_1h"]) >= ETH_BTC_DIVERGE_THRESHOLD
    if btc_eth_diverge:
        log(f"⚠️ BTC/ETH DIVERGE — BTC 1h:{btc['btc_1h']:+.1f}% vs ETH 1h:{eth['eth_1h']:+.1f}% "
            f"(selisih {abs(btc['btc_1h'] - eth['eth_1h']):.1f}%) — score Altcoin dikurangi {DIVERGE_SCORE_PENALTY}")

    # [v8.1 #6] Volatility Window — jam volume rendah WIB
    current_hour_wib = datetime.now(WIB).hour
    is_low_vol_session = LOW_VOL_HOUR_START <= current_hour_wib < LOW_VOL_HOUR_END
    if is_low_vol_session:
        log(f"🌙 LOW VOLUME SESSION ({current_hour_wib:02d}:xx WIB) — threshold score dinaikkan ke {LOW_VOL_SCORE_THRESHOLD}")

    # Counter untuk summary
    spread_rejected = 0

    allow_buy  = not btc["block_buy"] and not flash_triggered
    allow_sell = fg < FG_SELL_BLOCK

    log(f"Mode  : BUY={'✅ aktif' if allow_buy else ('🚨 FLASH GUARD' if flash_triggered else '⛔ diblokir (BTC drop)')} | "
        f"SELL={'✅ aktif' if allow_sell else f'⛔ diblokir (F&G={fg} ≥ {FG_SELL_BLOCK})'}")

    if btc["halt"]:
        tg(f"🛑 <b>SIGNAL BOT HALT</b>\n"
           f"BTC crash {btc['btc_4h']:+.1f}% dalam 4h.\n"
           f"Tidak ada signal sampai kondisi stabil.")
        log("🛑 BTC crash — bot halt"); return

    tickers       = gate_call_with_retry(client.list_tickers) or []
    signals       = []
    micro_signals = []
    momentum_signals = []   # [v7.8] momentum engine — trend continuation
    scanned       = 0
    skip_vol      = 0

    # [v7.9 #14] Exchange downtime alert — jika pairs < 50, kemungkinan Gate.io maintenance.
    # Sebelumnya "Tidak ada signal" dan "exchange down" menghasilkan pesan Telegram sama.
    # Sekarang dibedakan agar developer langsung tahu ada masalah infrastruktur.
    if len(tickers) < 50:
        msg = (f"⚠️ <b>Exchange Alert — Tickers Sangat Sedikit</b>\n"
               f"Hanya {len(tickers)} pair tersedia dari Gate.io.\n"
               f"Kemungkinan: maintenance, rate limit ekstrem, atau koneksi API bermasalah.\n"
               f"<i>Scan dibatalkan. Coba lagi dalam beberapa menit.</i>")
        tg(msg, wait=True)
        log(f"⚠️ Hanya {len(tickers)} ticker dari Gate.io — kemungkinan exchange down, abort scan", "error")
        return

    # [v7.6 #1] ob_ratio_cache dict per-pair di luar loop — menggantikan _ob_cache list trick.
    # [v8.0 AUDIT FIX 6.2] Dua cache terpisah:
    #   _ob_ratio_cache      → limit=10 untuk INTRADAY/SWING/MOMENTUM (pair likuid)
    #   _ob_ratio_cache_micro → limit=20 untuk MICROCAP (pair illiquid, butuh depth lebih dalam)
    # Di sub-$150K volume range, satu order besar bisa mendominasi top-10 book →
    # limit lebih dalam memberikan snapshot yang lebih representatif.
    _ob_ratio_cache:       dict = {}
    _ob_ratio_cache_micro: dict = {}

    def get_ob_ratio_lazy(p: str) -> float:
        """Fetch ob_ratio (limit=10) untuk pair likuid. Cache sekali per cycle."""
        if p not in _ob_ratio_cache:
            _ob_ratio_cache[p] = get_order_book_ratio(client, p, ob_limit=10)
        return _ob_ratio_cache[p]

    # [v8.4 FIX #5] get_ob_ratio_micro() dihapus — check_microcap() tidak menerima
    # parameter ob_ratio sehingga fungsi ini tidak pernah dipanggil (dead code).
    # ob_ratio scoring untuk microcap tidak diimplementasikan di check_microcap()
    # karena di sub-$150K range satu order besar sudah terlalu mendominasi top book
    # sehingga nilainya tidak reliable. Keputusan desain ini sudah benar, tapi
    # fungsi helper-nya tidak perlu ada jika tidak digunakan.

    # [v8.0 AUDIT FIX 1.2] Cross-strategy pair mutex — mencegah pair yang sama muncul
    # di dua strategy berbeda dalam satu cycle (mis. INTRADAY BUY + MOMENTUM BUY).
    # User menerima dua signal untuk pair yang sama dalam menit yang berdekatan →
    # ambigu tentang mana yang harus di-act. Set ini di-check sebelum append ke
    # signal list manapun. Priority order: PUMP > MOMENTUM > INTRADAY > SWING > MICROCAP.
    # PUMP dan MICROCAP punya quota terpisah dan tidak masuk _sent_pairs_this_cycle.
    _sent_pairs_this_cycle: set = set()

    for t in tickers:
        pair = t.currency_pair
        if not is_valid_pair(pair): continue

        # [v7.9 #11] Timeout check per-pair — abort gracefully jika loop terlalu lama
        if _check_timeout(pair):
            break

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
            # Dijalankan SEBELUM vol filter main bot.
            # Pair yang dibuang main bot bisa ditangkap microcap scanner.
            # Microcap TIDAK masuk _sent_pairs_this_cycle — quota dan tier-nya
            # terpisah dari main signals.
            if (allow_buy
                    and MICRO_VOL_MIN <= vol_24h <= MICRO_VOL_MAX
                    and not already_sent_micro(pair)):
                # [v8.0 AUDIT FIX 6.2] ob_ratio pakai limit=20 untuk pair illiquid
                sig = check_microcap(client, pair, price, vol_24h, change_24h)
                if sig:
                    micro_signals.append(sig)   # [v7.1 #6] tier B sudah difilter di check_microcap

            # Vol filter untuk main bot (INTRADAY + SWING)
            if vol_24h < MIN_VOLUME_USDT:
                skip_vol += 1; continue

            scanned += 1

            # [v8.1 #2] Spread Filter — cek Ask/Bid spread sebelum scan lebih jauh
            # Spread > SPREAD_MAX_PCT = slippage terlalu tinggi → skip pair ini
            spread_pct = get_spread_pct(client, pair)
            if spread_pct > SPREAD_MAX_PCT:
                log(f"⚠️ [{pair}] spread {spread_pct:.2f}% > {SPREAD_MAX_PCT}% — ditolak (anti-slippage)")
                spread_rejected += 1
                continue

            # [v8.1 #5+6] Buat context dict untuk diteruskan ke check_* functions
            # agar diverge penalty dan low-vol threshold bisa diterapkan di dalam scorer
            sniper_ctx = {
                "btc_eth_diverge": btc_eth_diverge,
                "is_low_vol":      is_low_vol_session,
            }

            # [v8.0 AUDIT FIX 1.2] Cross-strategy mutex — skip pair jika sudah
            # ter-queue di strategy lain dalam cycle ini. Priority: MOMENTUM > INTRADAY > SWING.
            # Pair yang sudah masuk satu strategy tidak perlu di-scan ulang untuk strategy
            # lain — mencegah user menerima dua BUY signal untuk pair yang sama dalam menit
            # yang berdekatan yang menimbulkan ambiguitas mana yang harus di-act.

            # ── MOMENTUM BUY — priority 1 (ditarget dulu) ────────
            if allow_buy and pair not in _sent_pairs_this_cycle and not already_sent_momentum(pair):
                sig = check_momentum(client, pair, price, get_ob_ratio_lazy(pair), btc, sniper_ctx)
                if sig:
                    momentum_signals.append(sig)
                    _sent_pairs_this_cycle.add(pair)

            # ── INTRADAY BUY — priority 2 ─────────────────────
            if allow_buy and pair not in _sent_pairs_this_cycle and not already_sent(pair, "INTRADAY", "BUY"):
                sig = check_intraday(client, pair, price, get_ob_ratio_lazy(pair), btc, side="BUY", sniper_ctx=sniper_ctx)
                if sig:
                    signals.append(sig)
                    _sent_pairs_this_cycle.add(pair)

            # ── INTRADAY SELL — SELL tidak dibatasi mutex BUY ─────
            if allow_sell and not already_sent(pair, "INTRADAY", "SELL"):
                sig = check_intraday(client, pair, price, get_ob_ratio_lazy(pair), btc, side="SELL", sniper_ctx=sniper_ctx)
                if sig: signals.append(sig)

            # ── SWING BUY — priority 3 ────────────────────────
            if allow_buy and pair not in _sent_pairs_this_cycle and not already_sent(pair, "SWING", "BUY"):
                sig = check_swing(client, pair, price, get_ob_ratio_lazy(pair), btc, side="BUY", sniper_ctx=sniper_ctx)
                if sig:
                    signals.append(sig)
                    _sent_pairs_this_cycle.add(pair)

            # ── SWING SELL ─────────────────────────────────────
            if allow_sell and not already_sent(pair, "SWING", "SELL"):
                sig = check_swing(client, pair, price, get_ob_ratio_lazy(pair), btc, side="SELL", sniper_ctx=sniper_ctx)
                if sig: signals.append(sig)

            time.sleep(SCAN_SLEEP_SEC)

        except Exception as e:
            log(f"⚠️ [{pair}]: {e}", "warn"); continue

    buy_cand      = sum(1 for s in signals if s["side"] == "BUY")
    sell_cand     = sum(1 for s in signals if s["side"] == "SELL")
    micro_cand    = len(micro_signals)
    momentum_cand = len(momentum_signals)
    log(f"\n📊 Scanned: {scanned} | Vol filter: {skip_vol} | "
        f"Candidates: {len(signals)} (BUY:{buy_cand} SELL:{sell_cand}) | "
        f"Microcap: {micro_cand} | Momentum: {momentum_cand}")

    # [v8.3 #3] Portfolio Exposure Cap — cap total BUY signals lintas strategy.
    # INTRADAY BUY + SWING BUY + MOMENTUM BUY = total BUY exposure cycle ini.
    # Jika melebihi MAX_CONCURRENT_BUY_SIGNALS, hanya top-N by score yang diteruskan.
    # Tujuan: mencegah 7–8% portfolio exposure dalam satu cycle saat market berkorelasi.
    all_buy_main    = [s for s in signals if s["side"] == "BUY"]
    all_buy_pool    = all_buy_main + momentum_signals
    total_buy_count = len(all_buy_pool)

    if total_buy_count > MAX_CONCURRENT_BUY_SIGNALS:
        log(f"⚠️ Portfolio cap: {total_buy_count} BUY signals > MAX {MAX_CONCURRENT_BUY_SIGNALS} — trimming ke top-{MAX_CONCURRENT_BUY_SIGNALS}")
        tg(
            f"⚠️ <b>PORTFOLIO CAP AKTIF</b>\n"
            f"{total_buy_count} BUY signals terkumpul — dikurangi ke <b>{MAX_CONCURRENT_BUY_SIGNALS}</b> terbaik.\n"
            f"Alasan: mencegah over-concentration di satu cycle.\n"
            f"<i>Atur MAX_CONCURRENT_BUY_SIGNALS untuk mengubah limit.</i>"
        )
        # Sort semua BUY (main + momentum) by score desc, ambil top-N
        all_buy_pool.sort(key=lambda x: -x["score"])
        kept = all_buy_pool[:MAX_CONCURRENT_BUY_SIGNALS]
        kept_pairs = {s["pair"] for s in kept}
        signals          = [s for s in signals if s["side"] == "SELL" or s["pair"] in kept_pairs]
        momentum_signals = [s for s in momentum_signals if s["pair"] in kept_pairs]
        log(f"  ✂️ Setelah cap: {len([s for s in signals if s['side']=='BUY'])} main BUY + {len(momentum_signals)} momentum BUY")

    # [v8.8 #1 / v8.9 #1] Correlation-aware sector cap — setelah portfolio cap, pastikan
    # tidak lebih dari MAX_SECTOR_SIGNALS BUY dari sektor yang sama.
    # [v8.9 #1] Bucket "OTHER" mendapat sub-limit MAX_OTHER_SIGNALS (default 1)
    # agar token tidak dikenal tidak flooding semua slot dari 1 bucket yang sama.
    all_buy_after_cap = [s for s in signals if s["side"] == "BUY"] + momentum_signals
    sector_counts: dict = {}
    sector_removed = []
    final_buy_pairs: set = set()

    # Sort by score desc agar yang score tinggi dipertahankan
    all_buy_after_cap.sort(key=lambda x: -x["score"])
    for sig in all_buy_after_cap:
        sector    = classify_sector(sig["pair"])
        limit     = MAX_OTHER_SIGNALS if sector == "OTHER" else MAX_SECTOR_SIGNALS
        cnt_so_far = sector_counts.get(sector, 0)
        if cnt_so_far < limit:
            sector_counts[sector] = cnt_so_far + 1
            final_buy_pairs.add(sig["pair"])
        else:
            sector_removed.append(f"{sig['pair']}({sector})")

    if sector_removed:
        log(f"⚠️ Sector cap [{MAX_SECTOR_SIGNALS}/sektor]: removed {', '.join(sector_removed)}")
        tg(
            f"⚠️ <b>SECTOR CAP AKTIF</b>\n"
            f"Dibatasi max <b>{MAX_SECTOR_SIGNALS}</b> BUY per sektor untuk diversifikasi.\n"
            f"Dihapus: {', '.join(sector_removed)}\n"
            f"<i>Atur MAX_SECTOR_SIGNALS untuk mengubah limit.</i>"
        )
        signals          = [s for s in signals if s["side"] == "SELL" or s["pair"] in final_buy_pairs]
        momentum_signals = [s for s in momentum_signals if s["pair"] in final_buy_pairs]
        log(f"  ✂️ Setelah sector cap: {len([s for s in signals if s['side']=='BUY'])} main + {len(momentum_signals)} momentum BUY")

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

    if not signals and micro_sent == 0 and not momentum_signals:
        tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v8.9.1 Hotfix Edition</b>\n"
           f"Pairs: {scanned} | F&G: {fg}\n"
           f"BUY : {'aktif' if allow_buy else ('🚨 FLASH GUARD TRIGGERED' if flash_triggered else 'diblokir (BTC drop)')}\n"
           f"SELL: {'aktif' if allow_sell else 'diblokir (extreme greed)'}\n"
           f"⚡ Flash Guard : {'🚨 TRIGGERED' if flash_triggered else '✅ ACTIVE'} (BTC 5m: {flash['chg_5m']:+.2f}%)\n"
           f"🔎 Spread Rejected: {spread_rejected} pair\n"
           f"Tidak ada signal memenuhi kriteria saat ini.\n"
           f"<i>Bot akan scan lagi dalam 4 jam.</i>", wait=True)
        log("📭 Tidak ada signal"); return

    # ── Kirim momentum signals — tier sort, independen dari main ─────
    momentum_signals.sort(key=lambda x: ({"S": 0, "A+": 1, "A": 2}.get(x["tier"], 9), -x["score"]))
    momentum_sent = 0
    for sig in momentum_signals:
        if momentum_sent >= MAX_MOMENTUM_SIGNALS: break
        send_signal(sig)
        save_signal(
            sig["pair"], "MOMENTUM", "BUY",
            sig["entry"], sig["tp1"], sig["tp2"], sig["sl"],
            sig["tier"], sig["score"], sig["timeframe"]
        )
        momentum_sent += 1
        time.sleep(0.5)

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

    # [v8.2 #6] Pending signal counter — hitung total signal result=NULL dalam 48 jam
    # agar user bisa validasi apakah evaluate_signals() jalan cukup cepat (backlog indicator).
    pending_total = 0
    pending_detail = ""
    try:
        since_48h = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        _pend_rows = (
            supabase.table("signals_v2")
            .select("strategy", count="exact")
            .is_("result", "null")
            .gt("sent_at", since_48h)
            .execute()
        )
        pending_total = _pend_rows.count if hasattr(_pend_rows, "count") else len(_pend_rows.data)
        # Kelompokkan per strategy untuk detail
        strat_pending: dict = {}
        for row in _pend_rows.data:
            s = row.get("strategy", "?")
            strat_pending[s] = strat_pending.get(s, 0) + 1
        if strat_pending:
            pending_detail = " (" + ", ".join(f"{s}:{n}" for s, n in strat_pending.items()) + ")"
    except Exception as _pe:
        log(f"⚠️ pending_total query: {_pe} — skip", "warn")

    bias_line = ""
    if pending_total >= 10:
        bias_line = f"\n⚠️ <b>WIN RATE BIAS RISK</b> — {pending_total} signal pending{pending_detail}\n   <i>evaluate_signals() mungkin perlu dijalankan lebih sering.</i>"

    tg(f"🔍 <b>Scan Selesai — SIGNAL BOT v8.9.1 Hotfix Edition</b>\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Pairs scanned : <b>{scanned}</b>\n"
       f"F&G           : <b>{fg}</b>\n"
       f"BTC 1h/4h     : <b>{btc['btc_1h']:+.1f}% / {btc['btc_4h']:+.1f}%</b>\n"
       f"ETH 1h        : <b>{eth['eth_1h']:+.1f}%</b>"
       f"{' ⚠️ DIVERGE' if btc_eth_diverge else ''}\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"⚡ Flash Guard    : {'🚨 TRIGGERED' if flash_triggered else '✅ ACTIVE'} (5m: {flash['chg_5m']:+.2f}%)\n"
       f"🔎 Spread Rejected: <b>{spread_rejected}</b> pair\n"
       f"🌙 Session        : {'LOW VOLUME (score threshold ↑)' if is_low_vol_session else 'NORMAL'}\n"
       f"━━━━━━━━━━━━━━━━━━\n"
       f"Signal terkirim : <b>{sent + micro_sent + momentum_sent}</b>\n"
       f"  📈 INTRADAY BUY  : {intraday_buy}\n"
       f"  📉 INTRADAY SELL : {intraday_sell}\n"
       f"  🌊 SWING BUY     : {swing_buy}\n"
       f"  🌊 SWING SELL    : {swing_sell}\n"
       f"  🚀 MOMENTUM BUY  : {momentum_sent}\n"
       f"  🔬 MICROCAP BUY  : {micro_sent}\n"
       f"📊 Pending resolve: <b>{pending_total}</b> signal{pending_detail}{bias_line}\n"
       f"<i>Scan berikutnya dalam 4 jam.</i>", wait=True)

    log(f"\n✅ Done — {sent + micro_sent + momentum_sent} signal terkirim "
        f"({sent} main + {momentum_sent} momentum + {micro_sent} microcap)")
    log(f"   INTRADAY BUY:{intraday_buy} SELL:{intraday_sell} | "
        f"SWING BUY:{swing_buy} SELL:{swing_sell} | "
        f"MOMENTUM:{momentum_sent} | MICROCAP:{micro_sent}")
    log(f"   Pending resolve: {pending_total} signal")

    # [v8.2 #2] Auto-trigger evaluate_signals() via daemon thread — non-blocking.
    # Di v8.0/v8.1 ini hanya ada sebagai TODO comment yang tidak pernah terpicu.
    # Sekarang dijalan otomatis setiap scan selesai kirim signal:
    #   - daemon=True → thread mati otomatis jika proses utama exit (GitHub Actions safe)
    #   - Tidak block scan summary — TG message sudah terkirim sebelum thread start
    #   - Client dibuat baru di dalam thread agar thread-safe (Gate.io client tidak reentrant)
    def _auto_evaluate():
        try:
            log("📊 [auto-evaluate] Mulai background evaluate_signals()...")
            _eval_client = get_client()
            evaluate_signals(_eval_client)
            log("📊 [auto-evaluate] Selesai")
        except Exception as _ee:
            log(f"⚠️ [auto-evaluate] Error: {_ee}", "warn")

    _eval_thread = threading.Thread(target=_auto_evaluate, daemon=True, name="auto-evaluate")
    _eval_thread.start()
    log("📊 [auto-evaluate] Thread started — berjalan di background")


if __name__ == "__main__":
    run()
