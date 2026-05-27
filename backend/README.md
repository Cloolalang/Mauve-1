# 5G ModemTestDriver — backend (serial AT engine)

**Version 4.4** — see root [`README.md`](../README.md) for the full feature overview. **GitHub:** [Cloolalang/Mauve-1](https://github.com/Cloolalang/Mauve-1). FastAPI/Swagger title: **5G ModemTestDriver** (OpenAPI version **4.4**).

Notes for **v4.4**:

- **`dashboard.html` — Access / Operator**: **`#access-qeng-search-rf`** toggle (default off) — when on, **`primaryCellDataAvailable`** accepts valid QENG LTE serving metrics without **`QNWINFO`** in-service; RAT/band fallback from QENG; SINR from **`QSINR`** or **`lte.sinr_raw`**; re-applies last KPI sample on change; **`collectUiControlsForRun()`** **`access_qeng_search_rf`**.
- OpenAPI / page header: **v4.4**.

Notes for **v4.3**:

- **`kpi_service.py` — registration reject causes**: **`_parse_reg_status()`** for **`+CEREG:`** / **`+C5GREG:`**; **`AT+CEREG=3`** / **`AT+C5GREG=3`** on first Data Service read; EMM / 5GMM cause label maps; **`refresh_data_service_snapshot()`** exposes **`eps_reject_cause_*`**, **`nr5g_reg_stat`**, **`nr5g_reject_cause_*`** on **`sample.data_service`**.
- **`dashboard.html` — Access / Operator**: **EPS reject (EMM)** and **5GS reject (5GMM)** rows under **Registration** (shown when modem reports a reject cause).
- OpenAPI / page header: **v4.3**.

Notes for **v4.2**:

- **`dashboard.html` — correlator**: **`historyRetentionMs()`** (`max(chartWindowMs, corrChartWindowMs)`); expanded **`CORR_KPI_CATALOG`** and trace **`GROUPS`** (**Modem TCP Setup**, neighbours, CA PCC, NR ARFCN/PCI, resel rates); MTCP trend uses retention window; axis **`data-axis-suffix`** / **`data-use-retention-window`**; **`corrZeroOnFail`** pushes **9999** to **`modemTcpHistory`**; **`collectUiControlsForRun()`** **`correlator`** block.
- OpenAPI / page header: **v4.2**.

Notes for **v4.1**:

- **`GET /api/sim/qmbncfg-list`**: **`AT+QMBNCFG="List"`**; response **`text`** / **`lines`** for SIM inspector UI.
- **`POST /api/network/locks`**: **AUTO** clears **`nr5g_band`** and default **NRDC off**; **LTE** forces **`nr5g_band=0`**, **`nrdc_mode=0`**; **`radio_refresh`** (optional, default on after **`mode_pref`**) — **`CFUN=0`** → **`CFUN=1`**; **`recovery_hints`** when deregistered; lock guard pauses re-apply when **SEARCH** / no service; ordered apply **`mode_pref`** → bands → **NRDC**.
- **`LockSetBody.radio_refresh`**: set **`false`** to skip CFUN cycle.
- **`dashboard.html`**: **List MBN (QMBNCFG)** button; **Apply Locks** sets **NRDC OFF** for **AUTO/LTE**.
- OpenAPI / page header: **v4.1**.

Notes for **v4.0**:

- **`modem_tcp_connect.py`**: **`run_modem_tcp_connect()`** — **`AT+QIOPEN`** → **`+QIOPEN`** URC timing; pre-close inside critical section; post-close skipped on success; **`AT`** sync after failed close timeout.
- **`serial_engine.py`**: timestamped **`at_trace`**; **`command_port_lock`**; **`modem_socket_critical_section()`**; **`max_wait_sec`** on **`_send_command_unlocked()`**; **`< TIMEOUT …>`** trace lines.
- **`kpi_service.py`**: cooperative **`at_exclusive_hold_depth`** / **`KpiCyclePreempted`**; background **`data_service_poll_loop`**; **`_kpi_at`** capped waits.
- **`test_runner.py`**: **`test_type`** **`tcp_connect`**; **`smoke_tcp_connect.json`**; CSV **`tcp_*`** columns; **`tcp_skip_pre_close_next()`**.
- **`main.py`**: **`POST /api/tools/modem-tcp-connect`**; test-runner branch for **`tcp_connect`**.
- **`dashboard.html`**: Modem TCP connect card, sweep, registration hold, reduced VoLTE/CLCC during TCP.
- OpenAPI / page header: **v4.0**.

Notes for **v3.9**:

- **Correlator scale modes** (`dashboard.html`): new `corrScaleMode` state variable (default `"stacked"`). `CORR_KPI_CATALOG` entries gain `rangeMin`/`rangeMax` fields for all 24 KPIs. In `drawCorrChart()`: `traceData` map now computes `yMin`/`yMax` from `corrScaleMode` — `"fixed"` uses catalog `rangeMin`/`rangeMax`; `"normalize"` uses window auto-range with 8 % padding; `"stacked"` uses auto-range but draws each trace in its own horizontal band (`bandH = floor((y0−y1−(N−1)×2)/N)`). The shared Y-axis grid (0–100 %) is suppressed in stacked mode; per-band separator lines, KPI labels, and actual min/max tick values are drawn inside the loop. The top-left legend is skipped in stacked mode. `#corr-scale-mode` `<select>` HTML element defaults to `"stacked"`.
- **Correlator threshold lines** (`dashboard.html`): after `yFor` is defined per-trace, a dashed horizontal line is drawn at `kpiDef.threshold` if the value falls within the band/chart boundary. Rendered at 45 % opacity in the trace colour, with a small text label on the right edge. Applies to all three scale modes.
- **Per-trace Y overrides** (`dashboard.html`): `corrTraceYMin[6]` and `corrTraceYMax[6]` arrays (default all `null`). `buildCorrTraceSelects()` appends ↓/↑ `<input type="number" placeholder="auto">` fields to each trace row. Override values are applied after auto/fixed range calculation in `traceData` map — `null` means auto. `#corr-scale-mode` HTML updated to select `"stacked"` by default.
- **Record 9999 on fail**: changed all `v: 0` failure pushes to `v: 9999` so failed tests spike visibly rather than dropping to zero.
- OpenAPI / page header: **v3.9**.

Notes for **v3.8**:

- **Auto-recover PDP on fail** (`dashboard.html`): two new boolean state variables — `pingAutoRecoverPdp` (default `false`) and `iperfAutoRecoverPdp` (default `false`) — toggled by `#ph-auto-recover-pdp` and `#iperf-auto-recover-pdp` checkboxes. A shared async helper `autoRecoverPdpContext(msgEl)` reads the Allow Data password from `#data-gate-password`, POSTs to `/api/network/data-gate` with `{inhibit: false, password}`, and updates the caller's message element with status. In `runPingSweepTest()` and `runIperfTest()` catch blocks: when the error message contains `"no active PDP context"` or `"Packet data is inhibited"` and the respective auto-recover flag is set, `pingSweepBusy`/`iperfBusy` is cleared, `autoRecoverPdpContext` is awaited, a 2.5 s stabilisation delay is applied, then the test function calls itself recursively for one retry. If recovery fails the failure path (history zeroing, chart redraw) proceeds as normal. No backend changes.
- OpenAPI / page header: **v3.8**.

Notes for **v3.7**:

- **Correlator "Record 0 on test fail"** (`dashboard.html`): `corrZeroOnFail` boolean (default `false`) gated by `#corr-zero-on-fail` checkbox in the correlator controls. In `runIperfTest()` catch block: when enabled, computes the current cell key from `currentServingEarfcn`/`currentServingPci` and pushes `{t, v:0, c?}` to the appropriate DL/UL/SE history arrays based on the `direction` variable captured at function entry. In the ping sweep catch block: pushes `{t, v:0}` to `phAvgHistory` and `phJitHistory`. All zero samples are immediately pruned by `pruneHistoryByAge`. Main chart consumers (`iperfEventHistory`, `phEventHistory`) are unchanged.
- OpenAPI / page header: **v3.7**.

Notes for **v3.6**:

- **KPI Correlator tab** (`dashboard.html`): new second browser tab with a full-width chart for multi-KPI correlation analysis.
  - State: `corrKpiKeys[6]`, `corrTraceDotEls[6]`, `corrTraceSmooth[6]`, `corrTraceSmoothWindow[6]`, `corrCurrentTab`, `corrGapModeEnabled`, `corrChartWindowMs`.
  - `CORR_KPI_CATALOG`: 24 KPI definitions mapping keys to history getters, catalog colours, units, and thresholds.
  - `drawCorrChart()`: collects up to 6 active traces, normalises each to 0–100 % of its own windowed range, draws per-segment with `colorForCellKey(p1.c, kpiDef.color)` so handovers show as colour changes. Canvas buffer sized to `clientWidth × devicePixelRatio` with `ctx.setTransform(dpr,0,0,dpr,0,0)` for sharp rendering. Live value callouts clamp inside the chart boundary (flip left when near right edge).
  - `buildCorrTraceSelects()`: dynamically creates 6 trace rows from the catalog — each row has a coloured dot (updated live to reflect current cell colour), KPI dropdown, smoothing checkbox (≈), and window-size input.
  - Hoisting/TDZ fix: all six `let corr*` state variables are declared **before** `redrawAllCharts()` is called so `drawCorrChart()` (a hoisted function) never hits the Temporal Dead Zone.
  - Scoping fix: entire correlator block is at the **top-level script scope** (not inside any nested IIFE) so `redrawAllCharts()` and `drawRfCharts()` can call `drawCorrChart()`.
- OpenAPI / page header: **v3.6**.

Notes for **v3.5**:

- **Spectral efficiency KPI** (`dashboard.html`, `test_runner.py`): SE computed as throughput (Mbps) ÷ aggregated bandwidth (MHz) = bps/Hz for DL and UL after every iperf run. Bandwidth resolved from `qcainfo.dl_bw_aggregate_mhz` (CA aggregate), falling back to `servingcell.lte.dl_bw` / `ul_bw`, then `nr_rf.primary.dl_bw`.
- **SE gauges**: `drawSingleSeGauge()` — semi-circular canvas gauges for DL SE (green `#7cffb2`) and UL SE (yellow `#ffe066`) in the *Iperf Latest Gauges* card.
- **SE trend chart** (dual Y-axis): `drawIperfChart()` extended with a right-axis bps/Hz scale; SE plotted as dashed series; `iperfDlSeHistory` / `iperfUlSeHistory` arrays track history and are pruned/reset with other iperf histories.
- **CSV columns**: `iperf_spectral_efficiency_dl_bps_hz`, `iperf_spectral_efficiency_ul_bps_hz` added after `iperf_throughput_ul_mbps` in `CSV_HEADER` and `build_csv_row()`. `_fmt_se_bps_hz()` helper in `test_runner.py`. `summarize_kpi_samples` now also tracks `lte_pcell_dl_bw_mhz_avg` / `lte_pcell_ul_bw_mhz_avg` as internal fallback bandwidth sources.
- OpenAPI / page header: **v3.5**.

Notes for **v3.4**:

- **Separate UL / DL parallel streams**: `iperf_download_upload` runner now resolves `parallel_streams_dl` and `parallel_streams_ul` from the profile independently (falls back to `parallel_streams`). Combined result dict stores both. CSV columns split into `iperf_parallel_streams_ul` and `iperf_parallel_streams_dl`.
- **Ookla-equivalent profiles**: `ookla_equiv_dl`, `ookla_equiv_ul`, `ookla_equiv_dlul` — 10 s duration, TCP, 3 streams DL / 1 stream UL.
- **Test runner pre-flight check**: `runTestRunnerProfile()` now blocks and reports if iperf, iperf sweep, ping sweep, ping auto-repeat, or VoLTE test is active at run start.
- **Pass/fail status strip on charts**: coloured 3 px baseline strip (green/red) on iperf and ping trend charts replaces the earlier dashed vertical markers; clears with "Clear All Charts".
- OpenAPI / page header: **v3.4**.

Notes for **v3.3**:

- **Test-complete / fail markers on iperf & ping charts**: after every iperf test and every ICMP ping sweep, a coloured status strip is drawn along the bottom of the respective trend chart — green on success, red on error. Renders from the very first result even before any throughput samples exist. Markers age out with the chart's rolling time window.
- OpenAPI / page header: **v3.3**.

Notes for **v3.2**:

- **iperf3 binary upgraded**: bundled binary replaced from 3.1.1 (2015, Cygwin 32-bit) → **3.21** (Cygwin 64-bit). Enables TCP bitrate pacing on servers running iperf3 ≥ 3.2; fixes `--connect-timeout` support detection.
- **UDP protocol support** (`POST /api/tools/iperf-test`): `protocol: "udp"` now accepted; adds `-u` flag; when no bitrate limit is set, passes `-b 0` (wire speed) instead of iperf3's 1 Mbit/s UDP default. Dashboard protocol selector includes **UDP** option.
- **Speed limit fix**: `-b` value divided by `parallel_streams` so the **total** aggregate bandwidth matches the user-entered limit (was per-stream before, causing the cap to be exceeded by a factor of N streams).
- **Iperf server presets dropdown**: four UK presets built into the dashboard (AAISP Maidenhead, AA.net.uk London, Clouvider London, Jisc Slough 10G); selecting a preset fills host + port instantly.
- **Continuous iperf sweep mode**: checkbox repeats the configured iperf test every 5 seconds; auto-stops at **5 minutes** to protect data usage; live MM:SS stopwatch displayed while active.
- **Iperf & ping chart hover tooltips**: crosshair + floating value tooltip on the iperf throughput trend and ICMP ping trend canvases (matches existing RF chart tooltip style).
- **UDP smoke test profiles**: three new bundled profiles — `smoke_iperf_dl_udp`, `smoke_iperf_ul_udp`, `smoke_iperf_dlul_udp` — targeting Clouvider London with UDP and 1 parallel stream.
- **UI notes**: parallel-streams hint (recommends 1 for UDP); speed-limit note explains TCP vs UDP enforcement; server preset pre-selects AAISP on load.
- OpenAPI / page header: **v3.2**.

Notes for **v3.1**:

- **CA CC RF charts (dashboard)**: four new per-component-carrier trend charts — RSRP, RSRQ, RSSI, SINR — drawn from `AT+QCAINFO` carrier rows. Only plot while CA is active (SCC present). RF smoothing (rolling avg) applied per-carrier series. EARFCN/PCI colour palette shared with primary-cell charts.
- **`registration_state` CSV**: simplified to `Home network` / `Roaming` / `MOCN` (with PLMN + operator prefix) — matches dashboard display.
- OpenAPI / page header: **v3.1**.

Notes for **v3.0**:

- **Serial engine hardened**: `asyncio.shield` prevents `wait_for` timeout from cancelling `req.done`; writer loop enforces half-duplex (waits for OK/ERROR before next command). Charts update faster; no more session-breaking AT failures after a timeout.
- **`main.py` structural split**: HTML/JS → `app/static/dashboard.html`; models → `app/models.py`; persist helpers → `app/persist.py`; singletons → `app/state.py`; serial/AT routes → `app/routes/serial.py`. `main.py` reduced from 10,085 to 3,617 lines.
- **Dashboard JS escape fixes**: `\\n` / `\\d` / `\\s` corrected to single-escaped in standalone HTML file — restores AT console newlines, interface IP validation, and band token parsing.
- **PyInstaller spec**: bundles `app/static/*.html` alongside `app/mocn/*.json`.
- OpenAPI / page header: **v3.0**.

Notes for **v2.2.6**:

- **UK MOCN-style heuristic**, **`sample.registration`** / **`sample.mocn`**, dashboard **Registered network (PLMN)** / **Registration** tooltip / **Registration trend** chart (`app/kpi_service.py`, `app/mocn_detect.py`, `app/mocn/*.json`; PyInstaller **`modemtestdriver.spec`** data bundle).
- **Test runner summary CSV**: **`registration_state`** aggregate column (after **`rat_most_common`** — root **Changes in v2.2.6**).
- OpenAPI / page header: **v2.2.6**.

Notes for **v2.2.5**:

- **Test runner summary CSV**: VoLTE **`volte_ceer`**, **`volte_modem_call_messages`** columns; **`AT+CEER`** parse joins multiple **`CEER`** lines when present (root **Changes in v2.2.5**).
- OpenAPI / page header: **v2.2.5**.

Notes for **v2.2.4**:

- **KPI / APN readback**: **`AT+QICSGP?`** fallback to **`AT+QICSGP=<cid>`**; relaxed **`+CGAUTH`/`+QICSGP`** line parsing (root **Changes in v2.2.4** / **Common issues**).
- OpenAPI / page header: **v2.2.4**.

Notes for **v2.2.3**:

- **Test runner CSV lock columns**: **`AT+QNWPREFCFG`** reads retried / parser relaxed (root **Changes in v2.2.3**).
- OpenAPI / page header was **v2.2.3** for that release line.

Notes for **v2.2.2**:

- **Windows iperf**: mobile-only bind probes **`cid1_ip`** + all mobile-like adapter IPv4s; rejects manual **`bind_ip`** that fails local bind (see root **Changes in v2.2.2** / **Common issues**).
- OpenAPI / page header was **v2.2.2** for that release line.

Notes for **v2.2.1**:

- **Test runner CSV**: **`band_locked`** removed; **`lock_rat_mode`**, **`lock_lte_bands`**, **`lock_ca_policy`**, **`lock_nr_bands`**, **`lock_nrdc`** added (modem **`AT+QNWPREFCFG`** readback after each run). See root **Changes in v2.2.1**.
- OpenAPI / page header was **v2.2.1** for that release line.

Notes for **v2.2**:

- **`POST /api/test/run`**: **`unlock_password`** required for **all** profile types; must match **Allow Data** / **`DATA_GATE_UNLOCK_PASSWORD`**.
- **Dashboard** (**`GET /`**): **RF smoothing** defaults **on**.
- **Summary CSV**: **`iperf_throughput_dl_mbps`** / **`iperf_throughput_ul_mbps`** replace **`iperf_throughput_mbps`** (see root **Changes in v2.2**).
- OpenAPI / page header was **v2.2** for that release line.

Notes for **v2.1**:

- **iperf**: optional **`port_range_max`** on **`POST /api/tools/iperf-test`** and profiles; TCP pre-connect when **`--connect-timeout`** unsupported (root **Changes in v2.1**). **`iperf_download_upload`** test type (DL then UL, same port); bundled **`smoke_iperf_dlul`**.
- OpenAPI / page header was **v2.1** for that release line.

Notes for **v2.0**:

- **Test runner**: **`app/test_runner.py`**; bundled profiles **`automated_tests/test_cases/*.json`** merged with **`.state/test_profiles.json`**. Root **Changes in v2.0** describes **`smoke_iperf_ul`**, **`connect_timeout_sec`**, and **`MD_IPERF_BIN`** / **`--connect-timeout`** behaviour.
- **QCAINFO**: **`kpi_service`** formats PCC/SCC active text as **`EARFCN/PCI(ROLE)`** (comma-separated).
- OpenAPI / page header was **v2.0** for that release line.

Notes for **v1.21**:

- **`POST /api/network/apn`**: **`AT+CGDCONT`** + **`AT+CGAUTH`** + **`AT+QICSGP`**; body **`pdp_auth_type`**, **`pdp_username`**, **`pdp_password`**; response **`auth_profile_read`**. KPI adds **`AT+CGAUTH?`** / **`AT+QICSGP?`** for **`sample.data_service`** (root **Changes in v1.21**).
- OpenAPI / page header: **v1.21**.

Notes for **v1.20**:

- **`GET /api/serial/status`**: rolling **`at_cmd_*`** telemetry (command counts/rates, latency avg/last/max over ~60s) plus existing **queue_depth** / **active_command**; see root **Changes in v1.20**.
- **Voice**: shared **`AT+CLCC`** path for **`GET /api/tools/voice-call-status`** and host auto-answer; tuned UI/autopoll intervals (root changelog).
- **`serial_engine`**: records completed-command timings for status metrics; **`GET /`**: Serial card shows baud/AT metrics; NR5G methodology blurb removed from NR card.
- OpenAPI / page header: **v1.20**.

Notes for **v1.19**:

- **`GET /`**: **Primary Cell** adds **Duplex (FDD/TDD)** from **`AT+QENG`** LTE serving. **NR5G RF KPI** adds **NR serving**, **Duplex**, and **NR band** display (QENG SA band preferred; see root **Changes in v1.19**).
- **`POST /api/network/locks`**: Longer **`AT+QNWPREFCFG`** timeouts, settle + re-verify after **`mode_pref`**; serial **`send_command`** uses extra **wait_for** slack (`serial_engine`).
- **PyInstaller (dev)**: from **`backend/`**, `pip install -r requirements.txt -r requirements-build.txt`, then **`.\build_exe.ps1`** → **`dist\5GModemTestDriver\`**. Entry: **`run_desktop.py`**; spec: **`modemtestdriver.spec`**. **Download:** prebuilt **`5GModemTestDriver-windows-amd64.zip`** on [GitHub Releases](https://github.com/Cloolalang/Mauve-1/releases) (see root **Download**). Planned: Windows **installer** (root **Planned features**).
- OpenAPI / page header: **v1.19**.

Notes for **v1.18**:

- **`GET /`**: Page header includes the Lord Kelvin quotation next to **v1.18**; default unlock password for gated API actions is **kelvin** (see root changelog).
- **`sample.qcainfo`**: **AT+QCAINFO** parse (`carriers`, `earfcn_active`, `dl_bw_aggregate_mhz`, …) on **`GET /api/kpi/latest`** / **`WS /ws/kpi`**; dashboard **Primary Cell** rows **EARFCN active (CA)** and **CA aggregated DL BW** plus combined CA trend chart (see root **Changes in v1.18**).
- **iperf3** **`POST /api/tools/iperf-test`**: body may include **`parallel_streams`** (default in UI **10**).

Notes for **v1.17**:

- **`GET /`**: RF threshold lines **RSRP −126 dBm** / **RSSI −95 dBm**; KPI dashboard suppresses **RF / neighbour / band / BW** trend samples when **SEARCH** or no valid negative **LTE RSRP**; **Data Service** **CID1** / **IP** hidden when **no service**, **SEARCH**, or **EPS** not registered.

Notes for **v1.16**:

- **`GET /`**: VoLTE card adds **in-call stopwatch** (holds last duration after hang-up until the next call); **in-call** UI/timer uses **`voice-call-status`** **`hook`** plus **`line_state`** for **CLCC** progressing states so **MT VoLTE** timers work when **`hook`** lags. **`POST /api/tools/volte-test`**: optional **`connect_timeout_sec`** (default **120**); connect detection uses **voice-only** **`+CLCC`**. **Host auto-answer** (**`POST /api/tools/host-auto-answer`**) is the dashboard path for **`ATA`**.

Notes for **v1.15**:

- **`GET /`**: embedded dashboard combines **band + DL BW** and **intra/inter neighbour counts** into single canvases each; neighbour-count UI labels omit **QENG**; default **Chart window** is **10m**.

Notes for **v1.14** (historical):

- **`GET /api/kpi/latest`** / **`/ws/kpi`**: **`sample.nr_rf`** NR5G RF KPI block (multi-row **`AT+QNWINFO`**, serving NR from **`AT+QENG`**, NR5G rows of **`AT+QRSRP`/`AT+QRSRQ`/`AT+QSINR`**, strongest intra NR neighbour when listed).
- **`GET /`**: **NR5G RF KPI** dashboard card; VoLTE card with **auto-answer** (PC `ATA`), call test, and **Answer** / **Hang up**.

Notes for **v1.13** (historical):

- **`GET /`**: toolbar **σ samples (N)** (2–600) caps how many recent primary-cell points in the RF chart window are used for **σ** KPIs; **Inter-frequency neighbour EARFCN** card shows **`inter_text`** only (intra UI removed). **`/api/kpi/neighbour-channels`** response shape unchanged.

Notes for **v1.12** (historical):

- **`GET /`** embedded dashboard: **Primary Cell** adds text **σ** (sample standard deviation, \(n-1\)) for **RSRP**, **RSRQ**, **SNIR (QSINR PRX)**, and **RSSI** over the RF chart window; computed client-side from raw history, serving **EARFCN/PCI**–filtered. No new API fields.

Notes for **v1.11** (historical):

- **`GET /api/kpi/neighbour-channels`**: returns **`intra_text`** / **`inter_text`** (newline-separated distinct LTE **EARFCNs** from QENG neighbourcell intra/inter, server-preformatted; **`sample_ts`** from the KPI poll). Not part of **`GET /api/kpi/latest`** or WebSocket KPI JSON.
- **KPI WebSocket loop**: **`json.dumps`** errors are caught and logged so the broadcaster task keeps running.

Notes for **v1.10** (historical):

- **KPI poll rate** is **fixed at 2.0 Hz** (no UI control, no `MD_KPI_POLL_HZ`). **`POST /api/kpi/poll`** only accepts **`poll_hz: 2.0`**; handler always sets **`poll_hz`** to **2.0**. **`/ws/kpi`** push cadence **0.5 s**.

Notes for **v1.9**:

- **Dashboard embedded in `GET /`**: chart title tweaks (carrier re-selection rate; **Primary cell** bandwidth / band trends); **`drawMetricChart` Y-floor option** plus fixed **yMin = 0** on iperf, ping sweep, and carrier-reselection canvases so non-negative traces stay grounded at zero.

Notes for **v1.8** (historical):

- **`GET /api/kpi/latest`**: **`intra_neighbour_count`** / **`inter_neighbour_count`** on **`sample.neighbour`** (distinct LTE rows on **`neighbourcell intra` / `inter`**, with serving-cell echo suppressed when EARFCN+PCI identity matches).
- **Parsing/UI**: **`_qeng_lte_row_echoes_serving_cell`** aligns strongest intra/inter choice and counts; dominance and RF history skip **`null`** (no bogus **0 dB** dominance when there is no neighbour).

Notes for **v1.7** (historical):

- **`GET /api/kpi/latest`**: strongest **inter-frequency** neighbour extracted from **`+QENG: "neighbourcell","inter"`** when EARFCN differs from the serving cell (**`inter_strongest_*`** fields).
- **Dashboard**: intra-neighbour overlays on serving RSRP/RSRQ/RSSI/dominance; inter-neighbour RSSI/RSRQ/RSRP + dominance charts; redundant PCI/neighbour-only trend cards removed; **Apply UI defaults** for window/smoothing/RAT bands.

Notes for **v1.6** (historical):

- **`GET /`**: Dashboard layout — compact **Serial Port** tile; **Access / Operator** includes **Registration Control (COPS)** inline; **Primary Cell** merges serving + RF + neighbour + dominance KPI rows; mobility card text trimmed.

Notes for **v1.5** (historical):

- Modem AT failures (**`+CME`/`+CMS`/generic ERROR/TIMEOUT**) are decoded in **`backend/app/at_modem_errors.py`** (**`+CME`** scanned across response lines); network endpoints include **`modem_detail`** plus clearer **`error`** strings. **`POST /api/network/data-gate`** returns accurate **`ok`** and **allow-data** retry path for **`AT+QIACT=1`**. **`POST /api/network/apn`** (**`reactivate`**) validates reattach (**CGATT**/**QIACT**); apn handler typo fix for **500** eliminated.
- **Robustel** gateways: **modem mode** in the web UI → APN (**`AT+CGDCONT`**) → MNO, so the router stack does not override serial AT.

Notes for **v1.4** (historical):

- First release of decoded **`modem_detail`** on network/tool failures; **`POST /api/network/data-gate`** truthful **`ok`**; APN reattach validation; dashboard **`userFacingBackendError`** wiring.

## Quick start

```powershell
cd backend
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8011
```

Preferred launcher (clears stale listener on target port):

```powershell
cd backend
.\start.cmd
```

Or **`.\start.ps1`** if your execution policy allows it.

Connect the **PC** to the router **USB** port with a **data** cable so Windows exposes the AT **COM** port (see root **[`README.md`](../README.md)** → **Quick start** step **1**).

## Dependency audit (pip-audit)

After **`pip install -r requirements.txt`** into **`.venv`**:

```powershell
cd backend
.\audit_deps.ps1
```

Optional **`.\audit_deps.ps1 -IncludeBuildDeps`** installs **`requirements-build.txt`** into the venv first (PyInstaller stack). **`audit_deps.cmd`** is the same with **`ExecutionPolicy Bypass`** if **`.ps1`** is blocked.

Scripts and tool pin: **`audit_deps.ps1`**, **`audit_deps.cmd`**, **`requirements-audit.txt`**.

Full notes: root **[`README.md`](../README.md)** → **Python dependency vulnerability scan**.

Default serial target is `COM49 @ 115200`.
Default KPI poll rate is `2.0 Hz`.

AT command catalog from current modem `AT+CLAC` capture:

- `../AT_COMMAND_REFERENCE.md`

Override defaults before start:

```powershell
$env:MD_SERIAL_PORT="COM49"
$env:MD_BAUDRATE="115200"
```

## API

Failures from the modem (**`+CME ERROR`**, **`ERROR`**, **TIMEOUT**, etc.) surface as **`ok: false`**, **`error`**, and often **`modem_detail`** on **`/api/network/*`**, **`/api/network/apn`**, **`/api/tools/modem-reset`**, etc. (decoded in **`app/at_modem_errors.py`**).

- `GET /` **5G ModemTestDriver** KPI page (**v1.17** in browser tab title and main heading): compact **Serial Port** tile; **Access / Operator** + **Registration Control (COPS)** combined; modem reset; lock controls + re-apply guard; roaming MNO [Vodafone/VMO2/EE/H3G/Auto]; data gate; CA/NRDC; **Primary Cell** (serving + RF + **σ** variability KPIs + **σ** sample count **N** + neighbour + dominance + neighbour counts); **NR5G RF KPI** card; **Inter-frequency neighbour EARFCN** card (**inter** list via separate API poll); intra overlay + **inter-frequency** neighbour trends; **LTE carrier re-selection** KPI + dual-trace chart; combined **band + DL BW** and **intra/inter neighbour count** trend charts; Data Service KPI (CID/IP gated when searching / not registered); SIM + PLMN; AT console; **iperf3** + ICMP sweep; VoLTE test + **host auto-answer** + live **Answer** / **Hang up** + **in-call stopwatch**; charts (default **10m** window, gaps, thresholds **RSRP −126 / RSSI −95**, smoothing, hover tooltips); **Apply UI defaults**
- `GET /api/serial/status`
- `GET /api/serial/ports`
- `POST /api/at/send`
  - body: `{ "command": "AT", "timeout_sec": 2.0 }`
- `GET /api/at/log` (recent AT TX/RX trace)
- `POST /api/serial/reopen`
  - body: `{ "port": "COM49", "baudrate": 115200 }`
- `GET /api/kpi/latest`
- `GET /api/kpi/neighbour-channels` — distinct LTE **EARFCN** lines (**intra_text** / **inter_text**) as preformatted text; small JSON, not included in WebSocket KPI; dashboard shows **inter** list only
- `POST /api/kpi/poll`
  - body: `{ "poll_hz": 2.0 }` only (rate is fixed; compatibility endpoint)
- `POST /api/kpi/poll/start`
- `POST /api/kpi/poll/stop`
- `GET /api/network/cops` (read registration/operator status)
- `POST /api/network/cops`
  - body: `{ "mode": 0 }` for auto register
  - body: `{ "mode": 2 }` for deregister
- `GET /api/network/cops/scan`
  - runs `AT+COPS=?` and returns parsed operator list with status/name/PLMN/AcT
  - optional query: `uk_only=1` to run scan with temporary UK band scope, then restore prior values
  - during scan, KPI polling is paused/resumed to avoid AT command queue starvation
- `GET /api/network/mno`
  - returns current COPS view plus named profiles (Vodafone/VMO2/EE/H3G/Auto)
- `POST /api/network/mno`
  - body example: `{ "profile": "vodafone", "cops_manual_registration": 4, "deregister_before_apply": true }` (**deregister_before_apply** default **true**: **`AT+COPS=2`** then manual PLMN; set **false** to skip)
  - manual PLMN for named profiles: `AT+COPS=<1|4>,2,"<PLMN>"` (often preceded by `AT+COPS=2`); profile **auto**: `AT+COPS=0`
- `GET /api/network/data-gate`
  - reads packet-data gate status (attach + active PDP contexts)
- `POST /api/network/apn`
  - body example: `{ "apn": "key", "cid": 1, "pdp_type": "IP", "password": "kelvin", "reactivate": true }`
  - writes `AT+CGDCONT`, mirrors **`AT+QICSGP`** (Quectel), optionally `QIDEACT`/`QIACT`; wrong password → `403`
- `POST /api/network/data-gate`
  - body example: `{ "inhibit": true }` to inhibit data
  - body example: `{ "inhibit": false, "password": "kelvin" }` to allow data
  - `password` is required for allow-data operation; wrong password returns `403`
- `GET /api/network/locks` (read QNWPREFCFG RAT/LTE/NR lock state)
- `POST /api/network/locks`
  - body example: `{ "rat_mode": "AUTO", "lte_band": "0", "nr5g_band": "78:77", "nrdc_mode": 1 }`
  - `lte_band="0"` is treated as "all LTE bands" even if modem readback expands to an explicit list
- `POST /api/tools/modem-reset`
  - sends `AT+CFUN=1,1` and returns reset command status
- `GET /api/tools/bind-interfaces`
  - Windows: IPv4 adapters from `ipconfig` for iperf / ICMP bind dropdowns
- `POST /api/tools/iperf-test`
  - bundled `iperf3` TCP client (JSON `-J`); optional `bind_ip`, `bitrate_limit_mbps`, `direction`, etc.
- `POST /api/tools/icmp-ping`
  - host OS ICMP ping sweep; body defaults `host` `8.8.8.8`, `count` `10`; optional `bind_ipv4` (Windows `ping -S`)
- `GET /api/tools/auto-answer` / `POST /api/tools/auto-answer` — optional modem **`ATS0`** (not wired to dashboard card)
- `GET /api/tools/host-auto-answer`
  - **auto-answer** watcher status (dashboard VoLTE card)
- `POST /api/tools/host-auto-answer`
  - body example: `{ "enabled": true, "rings": 2, "password": "kelvin" }` — PC **`ATA`** after N rings (URC or **`CLCC`** timed fallback)
- `POST /api/tools/volte-test`
  - password-gated call test (`password: "kelvin"`)
  - body example: `{ "number": "+447700900123", "hold_sec": 10, "connect_timeout_sec": 120, "password": "kelvin" }` — optional **`connect_timeout_sec`** (20–300, default **120**) waits for voice **CLCC** active/held
  - dials, monitors via `AT+CLCC`, holds, hangs up (with retry), and returns call KPIs + release info (`AT+CEER`)
- `GET /api/sim/high-level`
  - high-level SIM/operator reads (`AT+CGSN`, `AT+CIMI`, `AT+QSPN`, `AT+COPS?`, `AT+CPOL?`)
  - returns parsed summary + raw command outputs
- `GET /api/sim/inspector`
- `GET /api/sim/qmbncfg-list` (modem `AT+QMBNCFG="List"` — MBN profile list as plain text)
  - read-only SIM EF inspection via `AT+CRSM=176,...`
  - optional query: **`verbose=1`** — adds short EF descriptions, EF_EPSLOCI byte length, and `label_reference` (TS 31.102 numbering note); KPI UI requests verbose reads by default
  - includes PLMN/mobility-oriented files:
    - `EF_PLMNwAcT`, `EF_OPLMNwAcT`, `EF_HPLMN`, `EF_FPLMN`, `EF_SPDI`
    - `EF_AD`, `EF_EHPLMN`, `EF_UST`, `EF_PNN`, `EF_OPL`, `EF_EPSLOCI`, `EF_5GSLOCI` (where accessible)
  - decodes PLMN lists, MNC length hint, HPLMN timer, and **`EF_UST` enabled bits as USIM service n° plus labels** (`decoded.ef_ust.enabled_services_verbose`, aligned with 3GPP TS 31.102 Annex E Rel‑19-style numbering)
- `WS /ws/kpi` (live KPI snapshots)

Data Service KPI fields are available in `GET /api/kpi/latest` at `sample.data_service` and are derived from:

- `AT+CGDCONT?` (APN/PDP contexts)
- `AT+QIACT?` (active contexts + CID1 IP)
- `AT+CGATT?` (packet attach)
- `AT+CEREG?` (EPS registration)
- `AT+QCFG="usbnet"` (USB data-stack mode, e.g. ECM/RNDIS/QMI-style)
- `AT+QNETDEVSTATUS?` (network-device status)

UK-only COPS scan scope currently applies:

- LTE bands: `1:3:7:8:20:28:32:38`
- NR bands: `1:3:8:28:78`

VoLTE call testing and **`POST /api/tools/host-auto-answer`** use the same unlock secret as data allow (`kelvin`). **`POST /api/tools/auto-answer`** (modem S0) does too if used.

Example:

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8011/api/at/send" `
  -ContentType "application/json" `
  -Body '{"command":"AT"}'
```

KPI snapshot check:

```powershell
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8011/api/kpi/latest"
```

`sample.carrier_reselection` (v1.3+): rolling-window LTE PCell mobility counts from QENG (`primary_earfcn_reselections_per_min`, `intra_freq_pci_reselections_per_min`, `window_sec`).

`sample.nr_rf` (v1.14+): NR5G RF KPI summary (`available`, `primary`, `neighbour`) for the dashboard NR card.
