# 5G ModemTestDriver — backend (serial AT engine)

**Version 2.1** — see root [`README.md`](../README.md) for the full feature overview. FastAPI/Swagger title: **5G ModemTestDriver** (OpenAPI version **2.1.0**).

Notes for **v2.1**:

- **iperf**: optional **`port_range_max`** on **`POST /api/tools/iperf-test`** and profiles; TCP pre-connect when **`--connect-timeout`** unsupported (root **Changes in v2.1**). **`iperf_download_upload`** test type (DL then UL, same port); bundled **`smoke_iperf_dlul`**.
- OpenAPI / page header: **v2.1**.

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
.\start.ps1
```

Connect the **PC** to the router **USB** port with a **data** cable so Windows exposes the AT **COM** port (see root **[`README.md`](../README.md)** → **Quick start** step **1**).

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
