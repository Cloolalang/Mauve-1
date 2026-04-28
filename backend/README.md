# 5G ModemTestDriver — backend (serial AT engine)

**Version 1.3** — see root [`README.md`](../README.md) for the full feature overview. FastAPI/Swagger title: **5G ModemTestDriver** (OpenAPI version **1.3.0**).

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

- `GET /` **5G ModemTestDriver** KPI page (**v1.3** in browser tab title and main heading): Serial Port tools, modem reset, COPS + lock controls with runtime re-apply guard, roaming MNO selector [Vodafone/VMO2/EE/H3G/Auto], packet-data inhibit/allow controls, CA policy switch, NRDC switch, neighbour-cell KPI, intra-cell dominance KPI, **LTE carrier re-selection KPI + dual-trace chart** (PCell EARFCN vs PCI rates), Data Service KPI [APN/PDP/CID1/attach/registration/usbnet/netdev], SIM High-Level + PLMN Inspector, TX power KPI when reported, AT console, **iperf3** + **host ICMP ping sweep** (bind interfaces, trends, optional repeat every 15 s), VoLTE call test, clear-all charts, 60s–60m chart window, optional time-roll gap mode, `EARFCN/PCI` color mapping, RF threshold lines, optional RF smoothing, **RF chart hover tooltips** (value + EARFCN/PCI per sample on RSRP/RSRQ/SINR/RSSI/dominance charts)
- `GET /api/serial/status`
- `GET /api/serial/ports`
- `POST /api/at/send`
  - body: `{ "command": "AT", "timeout_sec": 2.0 }`
- `GET /api/at/log` (recent AT TX/RX trace)
- `POST /api/serial/reopen`
  - body: `{ "port": "COM49", "baudrate": 115200 }`
- `GET /api/kpi/latest`
- `POST /api/kpi/poll`
  - body: `{ "poll_hz": 2.0 }`
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
  - body example: `{ "profile": "vodafone" }`
  - uses manual/auto PLMN selection (`AT+COPS=4,2,"<PLMN>"`) for named MNOs
- `GET /api/network/data-gate`
  - reads packet-data gate status (attach + active PDP contexts)
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
