# 5G ModemTestDriver

**Version 1.18**

Local web app and backend for Quectel modem control and live LTE/NSA KPI monitoring over serial AT commands. The browser UI and OpenAPI docs use the product name **5G ModemTestDriver** (release **v1.18** is shown in the page header with a **Lord Kelvin** quotation on measurement).

License: [GNU General Public License v2.0](LICENSE).

## Quick start

Do these in order on **Windows** with a **Robustel** modem/router (tested on **R5010**). COM port names and router labels vary by model.

### 1. Set up the hardware

- Install **SIM**.
- For a single antenna, prefer **Antenna Port 0** (see your hardware guide).
- Power on the router. Connect an **Ethernet** cable between the **PC** and the router. In the **PC** browser, open the router **web UI** and **log in**. Then enable **modem mode** plus **serial AT** so the Quectel modem appears as a **COM** port (baud is commonly **`115200`**).
- Use a **USB data** cable from **PC → router USB**. (The router can be powered from the **PC USB-C** port.)

### 2. Install Python and application dependencies

**Install Python 3.12** from [python.org/downloads](https://www.python.org/downloads/) (**Windows installer**, not the Store, avoids most “wrong Python” problems). Run the installer as administrator if your PC is locked down. Enable **Add python.exe to PATH** and the **py** launcher. Finish the wizard, then **close every PowerShell window** and open a **new** one so PATH updates apply.

Check that Windows is really using **3.12** for the commands below:

```powershell
py --list
py -3.12 --version
```

You should see **`Python 3.12.x`**. If **`py -3.12`** is missing, install/repair **Python 3.12** from python.org. If **`py`** opens the wrong version, don’t use bare **`python`** for the venv—always **`py -3.12`** as shown. If the Store’s **`python.exe`** steals the name, turn off **Settings → Apps → Advanced app settings → App execution aliases** for **python.exe** / **python3.exe**, then open a new PowerShell.

**Put the project on local disk.** Unzip the GitHub ZIP into a folder that is **not** synced by **OneDrive**, **SharePoint**, Dropbox, or similar (avoid **Desktop** / **Documents** when those point at cloud drives). Sync and “files on demand” often cause **`venv`** or **`pip install`** to fail (locks, long paths, half-written files). Example: create **`C:\dev`**, unzip there—you should get **`C:\dev\Mauve-1-main`** (GitHub adds **`-main`**).

Download from GitHub: **[repository page](https://github.com/Cloolalang/Mauve-1)** → **Code** → **Download ZIP** → unzip into your **local** folder.

In PowerShell, create the virtual environment and install packages (change **`C:\dev`** if you used another location):

```powershell
cd C:\dev\Mauve-1-main\backend
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

**PowerShell blocking scripts:** If you see *running scripts is disabled on this system*, check **`Get-ExecutionPolicy`**. For your own login you can allow locally created scripts with:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

Files from a **Download ZIP** may be marked as from the internet; from **`backend`**, run **`Unblock-File .\start.ps1`** (or unzip after unblocking the ZIP in **File Explorer → Properties → Unblock**). Locked-down PCs may disable script execution entirely via Group Policy—your administrator has to allow PowerShell or you run the **`uvicorn`** line under **Start the app** instead of **`start.ps1`**.

### 3. Confirm the Quectel AT COM port, then start the server

In **Windows Device Manager**, open **Ports (COM & LPT)** and find **Quectel USB AT Port** (wording may vary slightly by driver). Note the **COM** number (example **`COM49`**).

In PowerShell, start the backend from the **`backend`** folder:

```powershell
cd C:\dev\Mauve-1-main\backend
.\start.ps1
```

### 4. Open the dashboard and connect to the modem

In the browser go to **`http://127.0.0.1:8011/`**.

Under **Serial Port**, choose your modem COM port and click **Reconnect**. Use **Refresh Ports** or **Auto-select AT Port** if needed.

### 5. Stop the app

In **PowerShell**, click the window where **`.\start.ps1`** is running and press `Ctrl + C` to stop the backend.

## Requirements

- Windows with Python 3.12 installed
- **USB:** PC connected to the router’s **USB** port with a **data-capable** cable (see **Quick start**, step **1**)
- Access to modem AT port (`COM49` by default)
- Modem not locked by another serial terminal app

**Changes in v1.18**

- **Page header**: Lord Kelvin quotation beside the release version (*“When you can measure what you are speaking about, and express it in numbers, you know something about it”* — **Lord Kelvin**).
- **Default unlock password** for Allow Data, **Set APN**, VoLTE test, answer/hang-up, and host auto-answer: **`kelvin`** (set `DATA_GATE_UNLOCK_PASSWORD` in `backend/app/main.py` to change it; update docs/examples if you do).
- **`AT+QCAINFO` in the KPI poll**: parsed snapshot under **`sample.qcainfo`** on **`GET /api/kpi/latest`** and **`WS /ws/kpi`**, including:
  - **`carriers`**: per-row PCC/SCC objects (`role`, `earfcn`, `pci`, `dl_bw_rb`, `band`, RF fields when present).
  - **`earfcn_active`**: numeric EARFCN list; **`earfcn_active_text`**: human-readable PCC/SCC summary for the **EARFCN active (CA)** KPI row.
  - **`dl_bw_aggregate_mhz`**: sum of decoded DL bandwidth (MHz) across carriers whose RB/index maps to MHz; **`dl_bw_components_mhz`**: per-carrier MHz list when present.
  - **`query_ok`**: whether the AT query completed successfully (no service / failures still yield a structured object; row gating follows **`QNWINFO`** / service like other primary-cell fields).
- **Primary Cell KPI rows**: **EARFCN active (CA)** (QCAINFO text/list) and **CA aggregated DL BW** (aggregate MHz, with tooltip on partial decode).
- **CA EARFCN config & aggregated bandwidth** (single combined chart):
  - **Top**: EARFCN **step trend** only when QCAINFO reports at least one **SCC**; line segments are **two-tone striped** using the same **`colorForCellKey`** palette as primary (**PCC** EARFCN/PCI) vs neighbour-style (**SCC**) fallbacks; **split** markers at each SCC sample.
  - **Bottom**: **aggregated DL BW (MHz)** over time: **striped** while CA (SCC) is active; **solid** primary colour when the modem returns to **PCC-only** after CA release so the trace **drops** to single-carrier bandwidth instead of freezing on the last CA value.
  - Hover tooltips on both panes (same RF-style tooltips as other metric charts).
- **iperf3**: dashboard default **parallel streams** (**`-P`**) is **10** (range 1–64).

**Changes in v1.17**

- **RF chart thresholds** (red guide lines): **RSRP** **−126 dBm**, **RSSI** **−95 dBm** (primary + inter-neighbour traces).
- **Search / no usable LTE RF**: trend buffers no longer gain points from placeholder zeros — **primary RF**, **intra neighbour overlays**, **inter-neighbour** RSRP/RSRQ/RSSI/dominance, **neighbour counts**, **band**, and **DL bandwidth** samples are appended only when the modem has plausible **camp** data (**QNWINFO** in service, **`AT+QENG` “servingcell”** not **SEARCH**, finite **RSRP &lt; 0 dBm**). **State** and **carrier re-selection** trends still update. Primary **Neighbour Cells RF** summary rows show **“—”** in that case.
- **Data Service** card: **CID1** and **CID1 IP** show **“—”** when there is **no network service**, serving state is **SEARCH**, or **EPS** is **not registered** (avoids stale **QIACT** UP/IP while searching).

**Changes in v1.16**

- **VoLTE / voice dashboard**: **in-call stopwatch** under the phone widget (counts while connected; after hang-up the display **keeps the last duration** until the **next** call starts, then resets to **0:00**). **In call** for both the handset state and the timer uses **`hook`** from **`/api/tools/voice-call-status`** plus **`line_state`** when **`AT+CLCC`** reports **active / held / dialing / alerting / waiting** (excluding pure **incoming_ring**), so **incoming VoLTE** paths that stay in alerting without an active stat still time correctly.
- **`POST /api/tools/volte-test`**: optional **`connect_timeout_sec`** (20–300 seconds, default **120**); outbound connect detection waits on **voice-only** **`+CLCC`** rows so **data**-context lines do not hide a voice call still in setup.
- **Auto-answer**: dashboard drives **host** **`POST /api/tools/host-auto-answer`** (PC-side **`ATA`** watcher); captions stay **Idle** / **Incoming** / **In call**.

**Changes in v1.15**

- **Dashboard charts**: single combined **Primary cell band & DL bandwidth trend** (dual Y-axis: categorical band + MHz); single **Neighbour cell count trend — intra & inter (LTE)** with fixed intra/inter colours (distinct from carrier EARFCN/PCI re-selection chart colours). **State trend** remains separate.
- **Neighbour count chart** title and legend use **intra-frequency** / **inter-frequency** wording only (no **QENG** in UI labels).
- **Default chart window** is **10 minutes** (toolbar **Chart window** defaults to **10m**; placeholder axis text matches).

**Changes in v1.14**

- **NR5G RF KPI** dashboard card plus **`sample.nr_rf`** on **`GET /api/kpi/latest`** and **`/ws/kpi`**: primary NR band/channel from multi-row **`AT+QNWINFO`**, serving identity/RF from **`AT+QENG`** NR NSA/SA, and PRX metrics from the **NR5G** rows of **`AT+QRSRP`**, **`AT+QRSRQ`**, **`AT+QSINR`**; strongest **intra** NR neighbour from **`AT+QENG="neighbourcell"`** when listed (modem-dependent).

**Changes in v1.13**

- **σ samples (N)**: toolbar **number control** (2–600, default **60**) sets how many **most recent** primary-cell **raw** samples **inside the chart time window** feed each **σ** KPI; **Apply UI defaults** resets **N** to 60.
- **Inter-frequency neighbour EARFCN** dashboard card: renamed and reduced to **inter-frequency** distinct EARFCNs only (intra list and long helper text removed). **`GET /api/kpi/neighbour-channels`** unchanged and still returns **`intra_text`** / **`inter_text`**.

**Changes in v1.12**

- **Primary Cell variability KPIs**: text-only **sample standard deviation** (**σ**, \(n-1\)) for **RSRP**, **RSRQ**, **SNIR (QSINR PRX)**, and **RSSI** over the **same sliding window** as the RF trend charts. Values use **raw** samples (not RF smoothing), require **at least two** points in-window, and include only samples tagged for the **current serving** **EARFCN/PCI** so cell changes do not mix populations.

**Changes in v1.11**

- **LTE neighbour EARFCN card**: dashboard lists **distinct LTE EARFCNs** from `AT+QENG="neighbourcell"` for **intra** and **inter** (strongest-measurement order, capped). Data is **pre-formatted on the server** and exposed only via **`GET /api/kpi/neighbour-channels`**; the UI polls it about every **3 s** so the main KPI snapshot and **`/ws/kpi`** payload stay lean.
- **Reliability**: KPI WebSocket broadcast wraps **`json.dumps`** failures (logs and continues). WebSocket and HTTP poll paths surface **parse / `applySnap` errors** on the status line instead of failing silently.

**Changes in v1.10**

- **KPI polling**: fixed at **2.0 Hz** (removed dashboard **KPI poll** control). **`MD_KPI_POLL_HZ`** is no longer read. **`POST /api/kpi/poll`** remains for compatibility; request body must use **`"poll_hz": 2.0`**; the rate is always **2 Hz**. WebSocket live KPI pushes use a **0.5 s** interval (2 Hz).

**Changes in v1.9**

- **Chart labels**: carrier re-selection trend title → **Primary Carrier re-selection rate — LTE PCell /min**; **Primary cell** prefix on **Bandwidth Trend (DL BW)** and **Band Trend** chart titles.
- **Non-negative trend Y-axis**: for metrics that are never below zero (throughput, ping sweep, carrier re-selection rates, primary DL bandwidth, neighbour **cell counts**), the vertical scale now **anchors at zero** on the bottom so the axis does not float above zero when values are clustered high (**RF plots with negative dBm / dB** unchanged).

**Changes in v1.8**

- **Neighbour counts**: KPI rows plus trend charts for **distinct LTE intra‑frequency** and **inter‑frequency** neighbour cells from `AT+QENG="neighbourcell"` (`sample.neighbour.intra_neighbour_count`, `inter_neighbour_count`).
- **PCell echo handling**: parsers and neighbour **counts** share `_qeng_lte_row_echoes_serving_cell` so the modem repeating the serving **EARFCN/PCI** in neighbour lists does not inflate neighbours or overlays; intra **strongest neighbour** rejects echo-only fallback; **inter strongest** rejects the same.
- **Intra‑cell dominance KPI and trend**: no value / no chart point unless there is a **real distinct** intra strongest neighbour comparable to primary ( **`addRfSample` no longer treats `null` as 0**, so bogus **0 dB** is not plotted).

**Changes in v1.7**

- **Inter-frequency neighbour KPIs**: `AT+QENG="neighbourcell","inter"` is parsed for the strongest inter-carrier neighbour **distinct from the serving cell EARFCN** (`inter_strongest_*` JSON fields alongside existing intra-frequency neighbour fields).
- **RF charts**: Serving **RSRP/RSRQ/RSSI/dominance** trends plot the **primary** series with a dashed **first intra-frequency neighbour** overlay; gaps vs continuous time-axis respects **Roll chart gaps over time-roll**. **Primary SNIR Trend (dB)** stays single-series only.
- **Inter-carrier RF trends**: charts for strongest inter-neighbour **RSSI**, **RSRP**, **RSRQ**, and **primary − inter RSRP dominance** (`nbr-*-inter*` canvases).
- **Removed** redundant standalone charts: PCI trend, standalone neighbour RSRP, RSRP primary vs intra comparison, neighbour PCI trend.
- **Neighbour KPI** labels clarified as intra-frequency where applicable.
- **Apply UI defaults** button: 10 minute chart window, RF smoothing on, MNO **auto**, RAT **AUTO**, UK-style LTE/NR band presets, CA and NRDC checkboxes on.

**Changes in v1.6**

- **`GET /` (KPI page)**: Layout cleanup — compact **Serial Port** tile only; **Access / Operator** card combines live operator/registration/fw summary with **Registration Control (COPS)** (read / scan / auto register / deregister); **Primary Cell** card groups identity, RF KPIs, intra-cell dominance, and **Neighbour Cells RF KPI** together; optional **TX power** line removed from UI; mobility card drops the verbose “rolling 60s…” explanatory paragraph (underlying KPI/API unchanged).

**Changes in v1.5**

- **`POST /api/network/apn`**: fixed **`500`** from a typo (`reatach_errs` / `reattach_errs`) when evaluating reattach results.
- **`POST /api/network/data-gate`** (**allow packet data**): short settle after **`AT+CGATT=1`**, **`AT+QIACT=1`** timeout **45 s**, recovery attempt **`AT+QIDEACT=1`** then second **`AT+QIACT=1`** when **`CGATT`** **`OK`** but first activate fails; success reflects final attach/activate (**`modem_detail`** on failure).
- **`app/at_modem_errors.py`**: decode **`+CME ERROR`** when it appears on **any** line before a trailing **`ERROR`**; add **`QIACT`** hints when PDP activate fails without a numbered CME.
- **Docs/UI**: README notes **Robustel** gateways should use **modem mode** before **APN** then **MNO** (device web UI); removed the extra static yellow APN advisory box from the KPI page.

**Changes in v1.4**

- **Modem AT error reporting**: Responses from **`engine.send_command()`** are summarized for **`+CME ERROR`**, **`+CMS ERROR`**, generic **`ERROR`**, **TIMEOUT**, and related cases via **`app/at_modem_errors.py`** (`describe_modem_send_result`). Network APIs (**MNO**, **COPS** read/set/scan, **APN**, **data-gate**, **locks**, **modem reset**) return **`modem_detail`** (and enriched **`error`**) where the modem rejects or misbehaves, so **`+CME ERROR: 30`** and similar show human-readable hints (e.g. no network service) instead of opaque “apply failed”.
- **`POST /api/network/data-gate`**: **`ok`** now reflects whether AT steps and the inferred inhibit/allowed state succeeded (no longer unconditionally `true` on failures).
- **`POST /api/network/apn`** with **`reactivate: true`**: **`ok`** is false if **CGATT**/**QIACT** reattachment fails after a successful **`AT+CGDCONT`**.
- **UI**: `userFacingBackendError()` surfaces **`modem_detail`** consistently in status messages across the embedded dashboard **`fetch`** paths.

**Changes in v1.3**

- **LTE carrier re-selection KPI** (mobility proxy): rolling 60 s windows count **primary EARFCN** changes and **intra-frequency PCI** changes on the LTE PCell from `AT+QENG="servingcell"`, exposed as events per minute in `GET /api/kpi/latest` under `sample.carrier_reselection` (`primary_earfcn_reselections_per_min`, `intra_freq_pci_reselections_per_min`). Intended for **camped (NOCONN) and RRC connected (CONNECT)** snapshots when the modem reports LTE PCell identity; NR SA–only periods without an LTE anchor do not drive these LTE counters.
- **UI**: KPI card **Mobility · LTE carrier re-selection** plus a **dual-trace trend chart** (light blue = PCI / min, pink = EARFCN / min) with the same chart window and clear-all behavior as other trends.
- **Parsing**: more resilient Quectel QENG LTE extraction (CONNECT/shorter lines, optional-space `+QENG:"LTE"`, case-insensitive `"LTE"`, prefer `servingcell`+LTE line then first standalone LTE line for PCell). Baseline identity is retained across transient missing LTE fields so connected-mode gaps do not zero out tracking.

**Changes in v1.2**

- Removed the **PCI lock** UI panel and **`/api/network/pci-lock`** endpoints (`AT+QNWLOCK "common/4g"`); use AT console if you still need to experiment with cell lock on supported firmware.

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
  - `AT+QCAINFO` (LTE carrier aggregation — PCC/SCC rows, active EARFCNs, decoded DL bandwidth aggregate)
- Reads modem firmware with `AT+CGMR`
- Serves the live KPI page at `/` (**5G ModemTestDriver**) with:
  - Serial Port tools (refresh ports, auto-select likely Quectel AT port, reconnect, remembers last successful port)
  - Modem reset button (`AT+CFUN=1,1`) with auto-recovery attempts
  - Registration control (`AT+COPS` auto/deregister) and scan (`AT+COPS=?`, optional UK-only LTE+NR band scope), colocated under **Access / Operator**
  - Roaming MNO selector (Vodafone / VMO2 / EE / H3G / Auto) using manual PLMN selection
  - Data gate controls to inhibit/allow packet data (deactivate/activate PDP context)
  - RAT / band lock control (`AT+QNWPREFCFG` for mode, LTE bands, NR bands)
  - Runtime lock guard that re-applies desired RAT/band/NRDC settings if modem drifts
  - CA policy switch for LTE (single-band vs multi/all)
  - NRDC on/off switch
  - Neighbour Cells RF KPI (strongest intra-frequency neighbour RSRP + PCI + EARFCN, plus intra/inter neighbour counts) under **Primary Cell**
  - **LTE CA (QCAINFO)** under **Primary Cell**: live **EARFCN active (CA)** and **CA aggregated DL BW** (MHz); combined **CA EARFCN config & aggregated bandwidth** trend chart (see **Changes in v1.18**)
  - **Static-UE congestion proxy** (primary cell only): KPI compares current RSRQ to a session **RSRQ baseline** built when RSRP is stable vs a rolling median; trend chart **RSRQ vs RSRP-stable session baseline** (dB); resets on serving-cell change
  - **NR5G RF KPI** card (primary + strongest intra NR neighbour when data is available; see **`sample.nr_rf`**)
  - **LTE neighbour channels** card: distinct **EARFCN** lists (intra / inter) via **`GET /api/kpi/neighbour-channels`** (~3 s refresh); not merged into live WebSocket KPI JSON
  - **Band lock and inter-cell neighbours:** With **RAT/band lock** applied (`AT+QNWPREFCFG`), firmware commonly omits or clears **inter-frequency** (**inter-cell**) neighbour rows on **`AT+QENG="neighbourcell"`**. Expect **inter-cell** KPIs (strongest inter neighbour, inter neighbour count, **`nbr-*-inter*`** trend charts, **Inter-frequency neighbour EARFCN**) to show **no data** or **—**; **intra-frequency** neighbours may still be reported.
  - Mobility / LTE carrier re-selection KPI (PCell EARFCN vs intra-frequency PCI rates) with dual-trace chart
  - Data Service KPI section:
    - APN (live read plus **Set APN** form using `AT+CGDCONT`, same unlock password as Allow Data)
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
  - iperf3 throughput test (TCP, DL/UL, **parallel streams** default **10**, bind interface, optional bitrate limit) with gauges and trend charts
  - VoLTE call test and **auto-answer:** outbound test (`ATD...;` + hangup); **auto-answer** — set **Rings**, tick **On** (unlock password required; tab out of password field or change **Rings** to push settings if you typed password after **On**); **Off** stops without password; **Answer** / **Hang up** on the same card
  - `Clear All Charts` control (also clears Data Service KPI display)
  - Trend charts for iperf throughput, ICMP ping sweep, primary and NR5G RF (RSRP/RSRQ/SNIR/RSSI/dominance where applicable), **RSRQ congestion proxy**, RSRP/RSRQ/RSSI + **intra** neighbour overlays, **inter**-frequency neighbour RSRP/RSRQ/RSSI + dominance, **neighbour cell counts**, **State**, **RAT**, **CA EARFCN + QCAINFO aggregated DL BW** (combined card), **primary cell band & DL bandwidth** (dual Y-axis), **NR band & DL bandwidth**, LTE **carrier re-selection** (dual trace)
  - Selectable chart window (60s to 60m)
  - Dynamic chart axis label shows time span (for example, `Time axis: last 10m`)
  - Optional `Time-roll gaps` mode to scroll by wall-clock time and show blank gaps when samples pause
  - Serving-cell color mapping on KPI trend lines using current `EARFCN/PCI` with a stable high-contrast palette (historical segments keep prior cell colors)
  - State and Band trend charts now use the same per-cell color-changing segmented plotting as RF/BW/PCI charts
  - Intra-cell dominance trend is hidden when primary serving-cell data is unavailable
  - Thin red threshold lines on RF charts:
    - RSRP min `-126 dBm`
    - RSRQ min `-15 dB`
    - SINR min `0 dB`
    - RSSI min `-95 dBm`
    - Intra-cell dominance min `6 dB`
  - **Near-cell RF (field observation):** when very close to the site and **RSSI is above ~−25 dBm**, reported RF levels may look **compressed or capped**, as if **receiver gain reduction** (AGC / RF front-end behaviour) is limiting how strong the modem reports the signal—not an artefact of the dashboard.
  - Optional RF smoothing toggle (rolling average of last 10 samples) for RSRP/RSRQ/SINR/RSSI/dominance
  - **RF trend hover tooltips**: on the RSRP / RSRQ / SNIR / RSSI / dominance / congestion proxy / intra & inter neighbour / neighbour count / combined band+BW / RAT / CA combo / NR RF canvases, moving the pointer near a plotted sample shows a tooltip with the metric value and **EARFCN/PCI** where applicable (same cell key used for segment colouring).
  - Primary Cell bandwidth KPI (`DL/UL BW`)
  - Primary cell intra-cell dominance KPI (`Primary RSRP - strongest intra-frequency neighbour RSRP` on serving EARFCN)
  - **Congestion proxy** KPI (dB): session RSRQ baseline vs current RSRQ when RSRP is stable (see chart card title); not network scheduler load
  - Primary cell **σ** KPIs (sample stdev of RSRP, RSRQ, SNIR, RSSI; **last N** primary-cell samples in the chart window with configurable **N**, current cell only)
- Exposes REST and WebSocket endpoints for control and integration

## Planned RF features

- **Tiered KPI polling**: refresh primary LTE serving metrics (`AT+QENG="servingcell"`, `AT+QRSRP` / `AT+QRSRQ` / `AT+QSINR`, `AT+QNWINFO`, `AT+QCAINFO`) more often than heavy `AT+QENG="neighbourcell"` and periodic data-service queries; reuse or timestamp stale neighbour-derived fields between neighbour polls.
- Full neighbour-cell RF table from `AT+QENG="neighbourcell"` (PCI, EARFCN/ARFCN, RSRP, RSRQ, SINR where available); v1.11 adds distinct **EARFCN** lists only
- Per-chain RF charts from `AT+QRSRP`, `AT+QRSRQ`, `AT+QSINR` (`PRX/DRX/RX2/RX3`)
- Extend NR KPIs (e.g. NR **inter** neighbour rows, band on neighbour, fuller RSSI coverage) beyond the current **`sample.nr_rf`** card
- Mobility/context timeline for serving cell changes (PCI, Cell ID, TAC, band, EARFCN/ARFCN)

## Install

Same as **Quick start**, step **2** (Python 3.12 + GitHub ZIP + `backend\.venv` + `pip install -r requirements.txt`). Copy-paste from step **2**; minimal recap:

```powershell
cd C:\dev\Mauve-1-main\backend
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Start the app

Default: **Quick start**, steps **3**–**4** (COM port + **`.\start.ps1`** + browser **Reconnect**). Stop with step **5**. Additional options from `backend`:

From `backend` folder (adjust path if your unzip location differs):

```powershell
cd C:\dev\Mauve-1-main\backend
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8011
```

Safer start helper (auto-clears stale listener on selected port before launch):

```powershell
cd C:\dev\Mauve-1-main\backend
.\start.ps1
```

Optional custom port/host:

```powershell
cd C:\dev\Mauve-1-main\backend
.\start.ps1 -Port 8012 -BindHost 127.0.0.1
```

From the unzipped project folder (parent of `backend`):

```powershell
cd C:\dev\Mauve-1-main
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
```

## Getting started

1. Start backend with `uvicorn` command above.
2. Open KPI page (`/`) and confirm values are updating.
3. Optional UI checks:
   - In **Serial Port**, use **Refresh Ports** and **Auto-select AT Port**, then **Reconnect** after USB replug.
   - Use **Reset Modem** and allow up to ~90s for re-enumeration/recovery.
   - Under **Access / Operator**, use **Read COPS / Auto Register / Deregister**.
   - Use **Read Locks / Apply Locks** in RAT / Band Lock and confirm readback values.
   - If lock values drift during runtime, verify they are automatically re-applied by the lock guard.
   - Validate **CA policy** behavior: CA ON uses multi/all LTE bands, CA OFF uses a single LTE band.
   - Toggle **NRDC** and confirm the readback state changes.
   - Check **Neighbour Cells RF KPI** values (strongest intra-frequency neighbour RSRP + PCI + EARFCN; intra/inter neighbour counts).
   - After a throughput run, confirm **CA aggregated DL BW** and the **CA EARFCN config & aggregated bandwidth** chart return to **PCC-only** behaviour when the modem drops SCC.
   - Hover the **congestion proxy** and **CA combo** charts to confirm tooltips.
   - Run **ICMP Ping Sweep** (and optional **repeat every 15 s**) and confirm gauges/trend update.
   - Watch RF, neighbour, dominance, State/Band/PCI, and Bandwidth charts update with live polling.
   - Use the **Chart window** selector to switch retention from `60s` up to `60m`.
   - Confirm RF charts show red threshold guide lines and auto-scroll over the selected window.
   - Hover near points on the RF trend charts (RSRP/RSRQ/SINR/RSSI/dominance) to verify the tooltip shows the value and **EARFCN/PCI** for that sample.
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

The JSON includes `sample.carrier_reselection` with `window_sec` (60), `primary_earfcn_reselections_per_min`, and `intra_freq_pci_reselections_per_min` when the KPI poll has parsed LTE PCell identity from QENG. When NR data is available, **`sample.nr_rf`** carries **NR5G RF KPI** fields for the dashboard card (`available`, `primary`, `neighbour`). **`sample.qcainfo`** carries **AT+QCAINFO** parsing (`carriers`, `earfcn_active`, `earfcn_active_text`, `dl_bw_aggregate_mhz`, `dl_bw_components_mhz`, `query_ok`); see **Changes in v1.18**.

## API quick reference

- `GET /` live KPI + charts page
- `GET /api/serial/status`
- `GET /api/serial/ports`
- `POST /api/serial/reopen`
- `POST /api/at/send`
- `GET /api/at/log`
- `GET /api/kpi/latest`
- `GET /api/kpi/neighbour-channels` (distinct LTE EARFCNs intra/inter as text; not in WebSocket KPI)
- `POST /api/kpi/poll`
- `POST /api/kpi/poll/start`
- `POST /api/kpi/poll/stop`
- `GET /api/network/cops`
- `POST /api/network/cops`
- `GET /api/network/cops/scan` (operator scan)
- `GET /api/network/mno`
- `POST /api/network/mno`
- `POST /api/network/apn` (set `AT+CGDCONT` APN; unlock password; CID 1 default)
- `GET /api/network/data-gate`
- `POST /api/network/data-gate`
- `GET /api/network/locks`
- `POST /api/network/locks`
- `POST /api/tools/modem-reset`
- `GET /api/tools/bind-interfaces` (Windows IPv4 adapters for bind dropdowns)
- `POST /api/tools/iperf-test` (TCP iperf3 client; optional `parallel_streams`, bind IP, bitrate limit; UI default **10** streams)
- `POST /api/tools/icmp-ping` (host OS ICMP ping sweep; optional Windows `-S` bind)
- `GET /api/tools/auto-answer` / `POST /api/tools/auto-answer` — optional **modem `ATS0`** (legacy); not used by the dashboard VoLTE card
- `GET /api/tools/host-auto-answer` / `POST /api/tools/host-auto-answer` — **auto-answer** used by the dashboard (**`ATA`** from PC; body **`enabled`**, **`rings`**, **`password`**)
- `POST /api/tools/volte-test`
- `GET /api/sim/high-level`
- `GET /api/sim/inspector`
- `WS /ws/kpi`

## Data Service KPI details

The Data Service KPI panel is populated from periodic AT reads inside the KPI poll loop:

- **`POST /api/network/apn`** (password `"kelvin"` or your configured unlock; same gate as Allow Data) updates **`AT+CGDCONT`**, mirrors the same APN into Quectel **`AT+QICSGP`** for the internal PDP path when the firmware supports it, and optionally reattaches with **`AT+QIACT`**. If the context is active, **`AT+QIDEACT=<cid>`** may run first so the APN can be changed (this can briefly disturb the USB WAN path). Set **`reactivate": false`** to skip **`CGATT`/`QIACT`** and reconnect later via **Allow Data**. APN must be letters/digits/`.`/`-`/`_` only. On **Robustel** gateways, use the router’s **modem mode** before changing APN, then select MNO—see the device web UI.

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
  - body: `{ "profile": "vodafone|vmo2|ee|h3g|auto", "cops_manual_registration": 4, "deregister_before_apply": true }` (defaults: **4**, **true**)
  - Named UK PLMNs: by default sends **`AT+COPS=2`** (deregister), short pause, then **`AT+COPS=<mode>,2,"<PLMN>"`** — many modems need deregister before switching manual PLMN quickly. Set **`deregister_before_apply`: `false`** only if you intentionally skip that step.
  - **`AT+COPS=<mode>,2,"…​"** with **mode 1** or **4**; use **mode 1** when steering among UK networks on a roaming non-steered SIM; **`AT+COPS=0`** for **auto**.

- `GET /api/network/data-gate`
  - Reads packet-attach and active PDP state.

- `POST /api/network/data-gate`
  - body: `{ "inhibit": true }` -> deactivates active PDP contexts (`AT+QIDEACT=<cid>`)
  - body: `{ "inhibit": false, "password": "kelvin" }` -> allows packet data (`AT+CGATT=1`, `AT+QIACT=1`)
  - `password` is mandatory for `inhibit=false`; invalid password returns HTTP `403`.
- `POST /api/tools/volte-test` and **`POST /api/tools/host-auto-answer`** use the same unlock password (`kelvin`). **`POST /api/tools/auto-answer`** (modem **S0**) uses it too if you call that API.

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

## VoLTE call test and auto-answer

The dashboard **VoLTE** card controls **auto-answer** via **`GET`/`POST /api/tools/host-auto-answer`** only (PC sends **`ATA`** after ring URCs or timed **`CLCC`** fallback).

- `GET /api/tools/auto-answer` / `POST /api/tools/auto-answer` — optional **modem `ATS0`** for scripts/integration; VoLTE usually ignores **S0**.

- `GET /api/tools/host-auto-answer` — watcher **enabled** / **rings** / live hints (`ring_urcs`, `elapsed_s`, `note`).
- `POST /api/tools/host-auto-answer` — body: `{ "enabled": true|false, "rings": 2, "password": "kelvin" }`.

- **`GET /api/tools/voice-call-status`** includes **`host_auto_answer`**.

- `POST /api/tools/volte-test`
  - body: `{ "number": "+447700900123", "hold_sec": 10, "connect_timeout_sec": 120, "password": "kelvin" }` — **`connect_timeout_sec`** optional (default **120**, max **300**): time to wait for voice **CLCC** active/held after dial.
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

## Common issues

- `WinError 10048` when starting server:
  - Port already in use. Switch to another port (for example `8012`) or stop the existing listener.
- `{"detail":"Not Found"}`:
  - Usually means wrong URL path. Use `/`, `/docs`, or the `/api/...` routes.
- Serial open fails / port busy:
  - Close other tools using the same COM port and restart backend.
