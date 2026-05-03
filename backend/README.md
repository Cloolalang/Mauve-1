# 5G ModemTestDriver — backend (serial AT engine)

**Version 1.13** — see root [`README.md`](../README.md) for the full feature overview. FastAPI/Swagger title: **5G ModemTestDriver** (OpenAPI version **1.13.0**).

Notes for **v1.13**:

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

- `GET /` **5G ModemTestDriver** KPI page (**v1.13** in browser tab title and main heading): compact **Serial Port** tile; **Access / Operator** + **Registration Control (COPS)** combined; modem reset; lock controls + re-apply guard; roaming MNO [Vodafone/VMO2/EE/H3G/Auto]; data gate; CA/NRDC; **Primary Cell** (serving + RF + **σ** variability KPIs + **σ** sample count **N** + neighbour + dominance + neighbour counts); **Inter-frequency neighbour EARFCN** card (**inter** list via separate API poll); intra overlay + **inter-frequency** neighbour trends; **LTE carrier re-selection** KPI + dual-trace chart; Data Service KPI; SIM + PLMN; AT console; **iperf3** + ICMP sweep; VoLTE test; charts (window, gaps, thresholds, smoothing, hover tooltips); **Apply UI defaults**
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
  - body example: `{ "apn": "key", "cid": 1, "pdp_type": "IP", "password": "nacelle", "reactivate": true }`
  - writes `AT+CGDCONT`, mirrors **`AT+QICSGP`** (Quectel), optionally `QIDEACT`/`QIACT`; wrong password → `403`
- `POST /api/network/data-gate`
  - body example: `{ "inhibit": true }` to inhibit data
  - body example: `{ "inhibit": false, "password": "nacelle" }` to allow data
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
- `POST /api/tools/volte-test`
  - password-gated call test (`password: "nacelle"`)
  - body example: `{ "number": "+447700900123", "hold_sec": 10, "password": "nacelle" }`
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

VoLTE call testing uses the same unlock secret as data allow (`nacelle`).

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
