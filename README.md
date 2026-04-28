# 5G ModemTestDriver

**Version 1.0**

Local web app and backend for Quectel modem control and live LTE/NSA KPI monitoring over serial AT commands. The browser UI and OpenAPI docs use the product name **5G ModemTestDriver** (release **v1.0** is shown in the page header).

Development/test platform: **Robustel R5010 router**.

Current default setup:
- Serial: `COM49`
- Baud: `115200`
- KPI poll: `2.0 Hz`
- Local server: `http://127.0.0.1:8011`

AT command catalog from current modem firmware:
- `AT_COMMAND_REFERENCE.md`

## What it does

- Opens a serial AT session to the modem
- Polls core KPI commands continuously:
  - `AT+QENG="servingcell"`
  - `AT+QNWINFO`
  - `AT+QRSRP`
  - `AT+QRSRQ`
  - `AT+QSINR`
- Reads modem firmware with `AT+CGMR`
- Serves the live KPI page at `/` (**5G ModemTestDriver**) with:
  - Serial Port tools (refresh ports, auto-select likely Quectel AT port, reconnect, remembers last successful port)
  - Modem reset button (`AT+CFUN=1,1`) with auto-recovery attempts
  - Registration control (`AT+COPS` auto/deregister)
  - Operator scan (`AT+COPS=?`) with optional UK-only LTE+NR band scope
  - Roaming MNO selector (Vodafone / VMO2 / EE / H3G / Auto) using manual PLMN selection
  - Data gate controls to inhibit/allow packet data (deactivate/activate PDP context)
  - RAT / band lock control (`AT+QNWPREFCFG` for mode, LTE bands, NR bands)
  - Runtime lock guard that re-applies desired RAT/band/NRDC settings if modem drifts
  - CA policy switch for LTE (single-band vs multi/all)
  - NRDC on/off switch
  - PCI lock controls (`AT+QNWLOCK "common/4g"`) including lock current/input and unlock
  - Neighbour Cells RF KPI section (strongest intra-frequency neighbour RSRP + PCI + EARFCN)
  - Data Service KPI section:
    - APN
    - PDP contexts (active/total)
    - CID1 state (UP/DOWN)
    - CID1 IP address
    - Packet attach state
    - EPS registration state
    - USB data stack mode (`AT+QCFG="usbnet"`; e.g. ECM/RNDIS/QMI)
    - Netdev status (`AT+QNETDEVSTATUS?`)
    - Built-in note when USB data stack (NDIS/QMI-like) may contend with modem traffic
  - SIM High-Level + PLMN Inspector section:
    - IMEI (`AT+CGSN`)
    - IMSI (`AT+CIMI`)
    - SPN (`AT+QSPN`, when available)
    - Current operator view (`AT+COPS?`)
    - Preferred PLMN count (`AT+CPOL?`)
    - Read-only SIM EF inspector via `AT+CRSM` for PLMN-related files
  - Live AT TX/RX console
  - Host ICMP ping sweep (`ping` from the OS; default 10 probes to `8.8.8.8`), optional bind/source IPv4 on Windows, gauges + trend, optional repeat every 15 s
  - iperf3 throughput test (TCP, DL/UL, bind interface, optional bitrate limit) with gauges and trend charts
  - VoLTE call test (`ATD...;` + `AT+CLCC` + `ATH` + `AT+CEER`) with user dial number and auto hangup after 10s
  - `Clear All Charts` control (also clears Data Service KPI display)
  - Trend charts for iperf throughput, ICMP ping sweep, RSRP, RSRQ, SINR, RSSI, State, Band, DL bandwidth, PCI, neighbour RSRP, neighbour PCI, intra-cell dominance
  - Selectable chart window (60s to 60m)
  - Dynamic chart axis label shows time span (for example, `Time axis: last 10m`)
  - Optional `Time-roll gaps` mode to scroll by wall-clock time and show blank gaps when samples pause
  - Serving-cell color mapping on KPI trend lines using current `EARFCN/PCI` with a stable high-contrast palette (historical segments keep prior cell colors)
  - State and Band trend charts now use the same per-cell color-changing segmented plotting as RF/BW/PCI charts
  - Intra-cell dominance trend is hidden when primary serving-cell data is unavailable
  - Thin red threshold lines on RF charts:
    - RSRP min `-105 dBm`
    - RSRQ min `-15 dB`
    - SINR min `0 dB`
    - RSSI max `-25 dBm`
    - Intra-cell dominance min `6 dB`
  - Optional RF smoothing toggle (rolling average of last 10 samples) for RSRP/RSRQ/SINR/RSSI/dominance
  - Primary Cell bandwidth KPI (`DL/UL BW`)
  - Primary Cell TX power KPI (`QENG` tail field, when reported by modem)
  - Primary cell intra-cell dominance KPI (`Primary RSRP - strongest intra-frequency neighbour RSRP` on serving EARFCN)
- Exposes REST and WebSocket endpoints for control and integration

## Planned RF features

- Neighbor cell RF table from `AT+QENG="neighbourcell"` (PCI, EARFCN/ARFCN, RSRP, RSRQ, SINR where available)
- Per-chain RF charts from `AT+QRSRP`, `AT+QRSRQ`, `AT+QSINR` (`PRX/DRX/RX2/RX3`)
- Dedicated NR serving KPI panel (NSA/SA `RSRP/RSRQ/SINR`, ARFCN, band)
- Carrier aggregation details from `AT+QCAINFO` (PCC/SCC, band, bandwidth, channel)
- Mobility/context timeline for serving cell changes (PCI, Cell ID, TAC, band, EARFCN/ARFCN)

## Requirements

- Windows with Python 3.12 installed
- Access to modem AT port (`COM49` by default)
- Modem not locked by another serial terminal app

## Install

From project root:

```powershell
cd backend
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Start the app

From `backend` folder:

```powershell
cd backend
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8011
```

Safer start helper (auto-clears stale listener on selected port before launch):

```powershell
cd backend
.\start.ps1
```

Optional custom port/host:

```powershell
cd backend
.\start.ps1 -Port 8012 -BindHost 127.0.0.1
```

From project root:

```powershell
.\backend\.venv\Scripts\python.exe -m uvicorn app.main:app --app-dir .\backend --host 127.0.0.1 --port 8011
```

Open:
- KPI page: `http://127.0.0.1:8011/`
- API docs: `http://127.0.0.1:8011/docs`

## Stop the app

- If running in current terminal: press `Ctrl + C`
- If running in another terminal:

```powershell
netstat -ano | findstr :8011
taskkill /PID <PID> /F
```

## Configuration

Set environment variables before start:

```powershell
$env:MD_SERIAL_PORT="COM49"
$env:MD_BAUDRATE="115200"
$env:MD_KPI_POLL_HZ="2.0"
```

## Getting started

1. Start backend with `uvicorn` command above.
2. Open KPI page (`/`) and confirm values are updating.
3. Optional UI checks:
   - In **Serial Port**, use **Refresh Ports** and **Auto-select AT Port**, then **Reconnect** after USB replug.
   - Use **Reset Modem** and allow up to ~90s for re-enumeration/recovery.
   - Use **Read COPS / Auto Register / Deregister** in Registration Control.
   - Use **Read Locks / Apply Locks** in RAT / Band Lock and confirm readback values.
   - If lock values drift during runtime, verify they are automatically re-applied by the lock guard.
   - Validate **CA policy** behavior: CA ON uses multi/all LTE bands, CA OFF uses a single LTE band.
   - Toggle **NRDC** and confirm the readback state changes.
   - Use **PCI Lock** controls (`Read`, `Lock Current Cell`, `Lock Input`, `Unlock`) and confirm state/readback.
   - Check **Neighbour Cells RF KPI** values (strongest intra-frequency neighbour RSRP + PCI + EARFCN).
   - Check **Primary cell intra-cell dominance** value and trend behavior.
   - Run **ICMP Ping Sweep** (and optional **repeat every 15 s**) and confirm gauges/trend update.
   - Watch RF, neighbour, dominance, State/Band/PCI, and Bandwidth charts update with live polling.
   - Use the **Chart window** selector to switch retention from `60s` up to `60m`.
   - Confirm RF charts show red threshold guide lines and auto-scroll over the selected window.
   - Optional: enable **RF smoothing** and verify 10-sample rolling average behavior.
   - Use **Clear All Charts** and confirm all chart histories reset.
4. Check serial status:

```powershell
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8011/api/serial/status"
```

5. Send a manual AT command:

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8011/api/at/send" `
  -ContentType "application/json" `
  -Body '{"command":"AT","timeout_sec":2.0}'
```

6. Read latest parsed KPI snapshot:

```powershell
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8011/api/kpi/latest"
```

## API quick reference

- `GET /` live KPI + charts page
- `GET /api/serial/status`
- `GET /api/serial/ports`
- `POST /api/serial/reopen`
- `POST /api/at/send`
- `GET /api/at/log`
- `GET /api/kpi/latest`
- `POST /api/kpi/poll`
- `POST /api/kpi/poll/start`
- `POST /api/kpi/poll/stop`
- `GET /api/network/cops`
- `POST /api/network/cops`
- `GET /api/network/cops/scan` (operator scan)
- `GET /api/network/mno`
- `POST /api/network/mno`
- `GET /api/network/data-gate`
- `POST /api/network/data-gate`
- `GET /api/network/locks`
- `POST /api/network/locks`
- `GET /api/network/pci-lock`
- `POST /api/network/pci-lock`
- `POST /api/tools/modem-reset`
- `GET /api/tools/bind-interfaces` (Windows IPv4 adapters for bind dropdowns)
- `POST /api/tools/iperf-test` (TCP iperf3 client; optional bind IP and bitrate limit)
- `POST /api/tools/icmp-ping` (host OS ICMP ping sweep; optional Windows `-S` bind)
- `POST /api/tools/volte-test`
- `GET /api/sim/high-level`
- `GET /api/sim/inspector`
- `WS /ws/kpi`

## Data Service KPI details

The Data Service KPI panel is populated from periodic AT reads inside the KPI poll loop:

- `AT+CGDCONT?` -> APN, PDP type, total PDP contexts
- `AT+QIACT?` -> active PDP contexts, CID1 state/IP
- `AT+CGATT?` -> packet attach state
- `AT+CEREG?` -> EPS registration status
- `AT+QCFG="usbnet"` -> USB data-stack mode (ECM/RNDIS/QMI-style)
- `AT+QNETDEVSTATUS?` -> network-device runtime status

Values are exposed in `GET /api/kpi/latest` under `sample.data_service`.

## Roaming MNO and Data Gate controls

- `GET /api/network/mno`
  - Returns current COPS view plus supported named profiles:
    - `Vodafone` (`23415`)
    - `VMO2` (`23410`)
    - `EE` (`23430`)
    - `H3G` (`23420`)
    - `Auto`

- `POST /api/network/mno`
  - body: `{ "profile": "vodafone|vmo2|ee|h3g|auto" }`
  - Uses `AT+COPS=4,2,"<PLMN>"` for named profiles and `AT+COPS=0` for auto.

- `GET /api/network/data-gate`
  - Reads packet-attach and active PDP state.

- `POST /api/network/data-gate`
  - body: `{ "inhibit": true }` -> deactivates active PDP contexts (`AT+QIDEACT=<cid>`)
  - body: `{ "inhibit": false, "password": "nacelle" }` -> allows packet data (`AT+CGATT=1`, `AT+QIACT=1`)
  - `password` is mandatory for `inhibit=false`; invalid password returns HTTP `403`.
- `POST /api/tools/volte-test` also requires the same unlock password (`nacelle`).

## COPS scan behavior

- `GET /api/network/cops/scan`
  - Runs `AT+COPS=?` and parses operator tuples into:
    - `status` / `status_label` (`available`, `current`, `forbidden`, etc.)
    - `long_name`, `short_name`, `plmn`, `act`
  - Query option: `uk_only=1` to constrain scan scope before scanning, then restore prior settings.
  - During scan, KPI polling is temporarily paused to avoid AT queue lockups.

UK-only scan scope:
- LTE bands: `1:3:7:8:20:28:32:38`
- NR bands: `1:3:8:28:78`

The backend reads existing `QNWPREFCFG` values, applies UK scope for the scan, then restores original values after completion.

## ICMP ping sweep (host OS)

`POST /api/tools/icmp-ping` runs the OS `ping` command (not modem AT). Body defaults: `host` `8.8.8.8`, `count` `10`. On Windows, optional `bind_ipv4` maps to `ping -S`. Response includes per-reply RTTs, `avg_ms`, `min_ms`, `max_ms`, `jitter_ms`, and parsed stdout tail.

## SIM High-Level and Inspector

Two SIM-focused endpoints are available:

- `GET /api/sim/high-level`
  - Executes: `AT+CGSN`, `AT+CIMI`, `AT+QSPN`, `AT+COPS?`, `AT+CPOL?`
  - Returns parsed summary (`imei`, `imsi`, `spn`, `cops`, `cpol_count`) and raw command outputs.

## VoLTE call test behavior

- `POST /api/tools/volte-test`
  - body: `{ "number": "+447700900123", "hold_sec": 10, "password": "nacelle" }`
  - Flow:
    - pre-hangup guard (`ATH`)
    - dial (`ATD<number>;`)
    - call-state polling (`AT+CLCC`)
    - hold connected call for configured duration
    - hangup with retry logic (`ATH`)
    - release context (`AT+CEER`)
  - Returns:
    - `dial_ok`, `call_connected`, `setup_time_ms`, `call_duration_s`
    - `ceer`, `clcc_states`, `clcc_after_hangup`, post-hang samples
    - network context before/during/after call from `AT+QNWINFO`

- `GET /api/sim/inspector`
  - Read-only SIM EF reads via `AT+CRSM=176,...`:
    - `EF_PLMNwAcT` (`6F60`)
    - `EF_OPLMNwAcT` (`6F61`)
    - `EF_HPLMN` (`6F31`)
    - `EF_FPLMN` (`6F7B`)
    - `EF_SPDI` (`6FCD`)
    - `EF_AD` (`6FAD`)
    - `EF_EHPLMN` (`6FD9`)
    - `EF_UST` (`6F38`)
    - `EF_PNN` (`6FC5`) and `EF_OPL` (`6FC6`)
    - `EF_EPSLOCI` (`6FE3`) and `EF_5GSLOCI` (`4F01`, module/profile permitting)
  - Decodes:
    - PLMN entries where applicable (MCC-MNC and access-technology hex for `*wAcT` files)
    - `EF_AD` MNC length hint
    - `EF_HPLMN` search timer (minutes)
    - `EF_UST` enabled service IDs
  - Keeps raw hex + SW status for files that require deeper BER-TLV parsing.

## Common issues

- `WinError 10048` when starting server:
  - Port already in use. Switch to another port (for example `8012`) or stop the existing listener.
- `{"detail":"Not Found"}`:
  - Usually means wrong URL path. Use `/`, `/docs`, or the `/api/...` routes.
- Serial open fails / port busy:
  - Close other tools using the same COM port and restart backend.
