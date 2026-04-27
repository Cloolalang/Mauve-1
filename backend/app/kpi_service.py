from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Any

from app.serial_engine import SerialEngine


def _parse_csv_payload(line: str) -> list[str]:
    # Simple CSV split is enough for current command outputs.
    return [x.strip().strip('"') for x in line.split(",")]


def _safe_int(value: str) -> int | None:
    try:
        v = int(value)
    except Exception:  # noqa: BLE001
        return None
    if v == -32768:
        return None
    return v


def _decode_lte_bw_mhz(value: str) -> int | None:
    raw = _safe_int(value)
    if raw is None:
        return None
    # Quectel LTE QENG bandwidth encoding:
    # 0=1.4MHz, 1=3MHz, 2=5MHz, 3=10MHz, 4=15MHz, 5=20MHz
    # We round 1.4MHz to 1 for integer KPI display.
    bw_map = {
        0: 1,
        1: 3,
        2: 5,
        3: 10,
        4: 15,
        5: 20,
    }
    return bw_map.get(raw, raw)


def _parse_qnwinfo(lines: list[str]) -> dict[str, Any] | None:
    out: dict[str, Any] | None = None
    for raw in lines:
        if not raw.startswith("+QNWINFO:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        if payload.upper() == "NO SERVICE":
            out = {"service": "NO SERVICE"}
            continue
        parts = _parse_csv_payload(payload)
        if len(parts) >= 4:
            out = {
                "act": parts[0],
                "operator": parts[1],
                "band": parts[2],
                "channel": _safe_int(parts[3]),
            }
    return out


def _parse_cgmr(lines: list[str]) -> str | None:
    # AT+CGMR typically returns one FW line between echo and OK.
    fw_line: str | None = None
    for raw in lines:
        s = (raw or "").strip()
        if not s:
            continue
        up = s.upper()
        if up in ("OK", "ERROR"):
            continue
        if up.startswith("AT+"):
            continue
        if up.startswith("+CME ERROR") or up.startswith("+CMS ERROR"):
            continue
        fw_line = s
    return fw_line


def _parse_four_path_metric(lines: list[str], prefix: str) -> dict[str, Any] | None:
    # Expected: +QRSRP: <PRX>,<DRX>,<RX2>,<RX3>,<sysmode>
    parsed: dict[str, Any] | None = None
    for raw in lines:
        if not raw.startswith(prefix):
            continue
        payload = raw.split(":", 1)[1].strip()
        parts = _parse_csv_payload(payload)
        if len(parts) < 5:
            continue
        parsed = {
            "prx": _safe_int(parts[0]),
            "drx": _safe_int(parts[1]),
            "rx2": _safe_int(parts[2]),
            "rx3": _safe_int(parts[3]),
            "sysmode": parts[4],
        }
    return parsed


def _parse_qeng_servingcell(lines: list[str]) -> dict[str, Any] | None:
    # Handle common shapes:
    # 1) +QENG: "servingcell","NOCONN","LTE",...
    # 2) +QENG: "servingcell","NOCONN"
    #    +QENG: "LTE",...
    #    +QENG: "NR5G-NSA",...
    parsed: dict[str, Any] = {}
    joined = "\n".join(lines)

    m = re.search(r'\+QENG:\s*"servingcell","([^"]+)"(?:,"([^"]+)")?', joined)
    if m:
        parsed["state"] = m.group(1)
        if m.group(2):
            parsed["mode"] = m.group(2)

    lte_line = None
    nsa_line = None
    sa_line = None
    for line in lines:
        if line.startswith('+QENG: "servingcell"') and '"LTE"' in line:
            lte_line = line
        elif line.startswith('+QENG: "LTE"'):
            lte_line = line
        elif line.startswith('+QENG:"NR5G-NSA"') or line.startswith('+QENG: "NR5G-NSA"'):
            nsa_line = line
        elif '"NR5G-SA"' in line:
            sa_line = line

    if lte_line:
        payload = lte_line.split(":", 1)[1].strip()
        p = _parse_csv_payload(payload)
        # Shape A: "servingcell",state,"LTE",is_tdd,...
        # Shape B: "LTE",is_tdd,...
        base = 0
        if len(p) >= 4 and "servingcell" in p[0].lower():
            base = 2
        # "LTE",is_tdd,MCC,MNC,cellID,PCID,earfcn,band,UL_bw,DL_bw,TAC,RSRP,RSRQ,RSSI,SINR,...
        if len(p) >= (15 + base):
            parsed["lte"] = {
                "rat": p[0 + base],
                "duplex": p[1 + base],
                "mcc": _safe_int(p[2 + base]),
                "mnc": _safe_int(p[3 + base]),
                "cell_id_hex": p[4 + base],
                "pcid": _safe_int(p[5 + base]),
                "earfcn": _safe_int(p[6 + base]),
                "band": _safe_int(p[7 + base]),
                "ul_bw": _decode_lte_bw_mhz(p[8 + base]),
                "dl_bw": _decode_lte_bw_mhz(p[9 + base]),
                "rsrp": _safe_int(p[11 + base]),
                "rsrq": _safe_int(p[12 + base]),
                "rssi": _safe_int(p[13 + base]),
                "sinr_raw": _safe_int(p[14 + base]),
                # Typical tail in many Quectel LTE QENG formats:
                # ...,<RSRP>,<RSRQ>,<RSSI>,<SINR>,<CQI>,<TX_power>,<SRXLEV>
                "tx_power": _safe_int(p[16 + base]) if len(p) > (16 + base) else None,
            }

    if nsa_line:
        payload = nsa_line.split(":", 1)[1].strip()
        p = _parse_csv_payload(payload)
        # "NR5G-NSA",MCC,MNC,PCID,RSRP,SINR,RSRQ,ARFCN,band,...
        if len(p) >= 9:
            parsed["nr_nsa"] = {
                "rat": p[0],
                "mcc": _safe_int(p[1]),
                "mnc": _safe_int(p[2]),
                "pcid": _safe_int(p[3]),
                "rsrp": _safe_int(p[4]),
                "sinr": _safe_int(p[5]),
                "rsrq": _safe_int(p[6]),
                "arfcn": _safe_int(p[7]),
                "band": _safe_int(p[8]),
            }

    if sa_line:
        payload = sa_line.split(":", 1)[1].strip()
        p = _parse_csv_payload(payload)
        # "servingcell",state,"NR5G-SA",duplex,MCC,MNC,cellID,PCID,TAC,ARFCN,band,dl_bw,RSRP,RSRQ,SINR,...
        if len(p) >= 15:
            parsed["nr_sa"] = {
                "mode": p[2],
                "duplex": p[3],
                "mcc": _safe_int(p[4]),
                "mnc": _safe_int(p[5]),
                "cell_id_hex": p[6],
                "pcid": _safe_int(p[7]),
                "tac_hex": p[8],
                "arfcn": _safe_int(p[9]),
                "band": _safe_int(p[10]),
                "rsrp": _safe_int(p[12]),
                "rsrq": _safe_int(p[13]),
                "sinr": _safe_int(p[14]),
            }

    return parsed or None


def _parse_qeng_strongest_neighbour(
    lines: list[str], serving_pci: int | None = None, serving_earfcn: int | None = None
) -> dict[str, int] | None:
    # Intra-frequency neighbour line shape:
    # +QENG: "neighbourcell intra","LTE",<earfcn>,<pcid>,<rsrq>,<rsrp>,...
    # For intra-cell dominance, only compare against intra LTE neighbours on same EARFCN.
    best: dict[str, int] | None = None
    fallback: dict[str, int] | None = None
    for raw in lines:
        if not raw.startswith("+QENG:"):
            continue
        low = raw.lower()
        if "neighbourcell intra" not in low:
            continue
        parts = _parse_csv_payload(raw.split(":", 1)[1].strip())
        # Find RAT token and map to likely RSRP position.
        for i, token in enumerate(parts):
            rat = token.upper()
            if rat == "LTE" and len(parts) > i + 4:
                # ... "LTE",earfcn,pcid,rsrq,rsrp,...
                earfcn = _safe_int(parts[i + 1]) if len(parts) > i + 1 else None
                pci = _safe_int(parts[i + 2]) if len(parts) > i + 2 else None
                rsrp = _safe_int(parts[i + 4])
                if rsrp is None or pci is None:
                    continue
                if serving_earfcn is not None and earfcn is not None and earfcn != serving_earfcn:
                    continue
                cand = {"pci": pci, "rsrp": rsrp, "earfcn": earfcn}
                if serving_pci is not None and pci == serving_pci:
                    # Some firmware lists serving cell first in neighbour table; keep only as fallback.
                    if fallback is None or rsrp > fallback["rsrp"]:
                        fallback = cand
                    continue
                if best is None or rsrp > best["rsrp"]:
                    best = cand
    return best or fallback


def _parse_cgatt(lines: list[str]) -> int | None:
    for raw in lines:
        if not raw.startswith("+CGATT:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        parts = _parse_csv_payload(payload)
        if not parts:
            continue
        return _safe_int(parts[0])
    return None


def _parse_cereg(lines: list[str]) -> dict[str, Any] | None:
    for raw in lines:
        if not raw.startswith("+CEREG:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        parts = _parse_csv_payload(payload)
        if not parts:
            continue
        stat = _safe_int(parts[-1] if len(parts) == 1 else parts[1])
        return {"stat": stat}
    return None


def _parse_qiact(lines: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in lines:
        if not raw.startswith("+QIACT:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        parts = _parse_csv_payload(payload)
        if len(parts) < 4:
            continue
        cid = _safe_int(parts[0])
        state = _safe_int(parts[2])
        if cid is None:
            continue
        out.append(
            {
                "cid": cid,
                "active": state == 1,
                "ip": parts[3] or None,
            }
        )
    return out


def _parse_cgdcont(lines: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in lines:
        if not raw.startswith("+CGDCONT:"):
            continue
        m = re.match(r'\+CGDCONT:\s*(\d+)\s*,\s*"([^"]*)"\s*,\s*"([^"]*)"', raw)
        if not m:
            continue
        out.append(
            {
                "cid": int(m.group(1)),
                "pdp_type": m.group(2) or None,
                "apn": m.group(3) or None,
            }
        )
    return out


def _parse_qcfg_usbnet(lines: list[str]) -> int | None:
    for raw in lines:
        if not raw.startswith("+QCFG:"):
            continue
        if '"usbnet"' not in raw:
            continue
        payload = raw.split(":", 1)[1].strip()
        parts = _parse_csv_payload(payload)
        # Expected: "usbnet",<mode>
        if len(parts) < 2:
            continue
        return _safe_int(parts[1])
    return None


def _usbnet_mode_label(mode: int | None) -> str | None:
    # Mode mapping can vary slightly by module/firmware.
    if mode is None:
        return None
    known = {
        0: "ECM",
        1: "RNDIS/NDIS",
        2: "MBIM",
        5: "QMI",
    }
    return known.get(mode, f"mode {mode}")


def _parse_qnetdevstatus(lines: list[str]) -> str | None:
    for raw in lines:
        if not raw.startswith("+QNETDEVSTATUS:"):
            continue
        return raw.split(":", 1)[1].strip() or None
    return None


def _parse_qgdcnt(lines: list[str]) -> dict[str, int] | None:
    # Common shape: +QGDCNT: <tx_kb>,<rx_kb>
    for raw in lines:
        if not raw.startswith("+QGDCNT:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        parts = _parse_csv_payload(payload)
        if len(parts) < 2:
            continue
        tx_kb = _safe_int(parts[0])
        rx_kb = _safe_int(parts[1])
        if tx_kb is None or rx_kb is None:
            continue
        return {"tx_kb": tx_kb, "rx_kb": rx_kb}
    return None


@dataclass
class KpiRuntime:
    snapshot: dict[str, Any] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    poll_running: bool = False
    poll_hz: float = 1.0
    last_error: str | None = None
    modem_fw: str | None = None
    modem_fw_at: float = 0.0
    data_service: dict[str, Any] = field(default_factory=dict)
    data_service_at: float = 0.0
    traffic_prev_tx_kb: int | None = None
    traffic_prev_rx_kb: int | None = None
    traffic_prev_at: float = 0.0


async def kpi_poll_loop(engine: SerialEngine, runtime: KpiRuntime) -> None:
    runtime.poll_running = True
    while runtime.poll_running:
        started = time.time()
        try:
            now = time.time()
            need_fw = (now - runtime.modem_fw_at) > 60.0 or not runtime.modem_fw
            if need_fw:
                cgmr = await engine.send_command("AT+CGMR", timeout_sec=2.0)
                fw = _parse_cgmr(cgmr.get("lines", []))
                if fw:
                    runtime.modem_fw = fw
                runtime.modem_fw_at = now

            qeng = await engine.send_command('AT+QENG="servingcell"', timeout_sec=2.0)
            qnwinfo = await engine.send_command("AT+QNWINFO", timeout_sec=1.5)
            net = _parse_qnwinfo(qnwinfo.get("lines", []))
            in_service = bool(
                net
                and str(net.get("service", "")).upper() != "NO SERVICE"
                and str(net.get("act", "")).upper() not in ("", "NONE")
            )

            if in_service:
                qrsrp = await engine.send_command("AT+QRSRP", timeout_sec=1.5)
                qrsrq = await engine.send_command("AT+QRSRQ", timeout_sec=1.5)
                qsinr = await engine.send_command("AT+QSINR", timeout_sec=1.5)
                qeng_nb = await engine.send_command('AT+QENG="neighbourcell"', timeout_sec=2.0)
            else:
                # Avoid command spam/errors when modem is deregistered/no-service.
                qrsrp = {"ok": False, "command": "AT+QRSRP", "final": "SKIPPED_NO_SERVICE", "lines": []}
                qrsrq = {"ok": False, "command": "AT+QRSRQ", "final": "SKIPPED_NO_SERVICE", "lines": []}
                qsinr = {"ok": False, "command": "AT+QSINR", "final": "SKIPPED_NO_SERVICE", "lines": []}
                qeng_nb = {"ok": False, "command": 'AT+QENG="neighbourcell"', "final": "SKIPPED_NO_SERVICE", "lines": []}

            refresh_ds = (now - runtime.data_service_at) > 5.0 or not runtime.data_service
            qgdcnt = await engine.send_command("AT+QGDCNT?", timeout_sec=1.5)
            qgdcnt_v = _parse_qgdcnt(qgdcnt.get("lines", []))
            eps_ul_kbps: float | None = None
            eps_dl_kbps: float | None = None
            if qgdcnt_v is not None:
                prev_ts = runtime.traffic_prev_at
                dt = now - prev_ts if prev_ts else 0.0
                prev_tx = runtime.traffic_prev_tx_kb
                prev_rx = runtime.traffic_prev_rx_kb
                tx_kb = qgdcnt_v["tx_kb"]
                rx_kb = qgdcnt_v["rx_kb"]
                if dt > 0.2 and prev_tx is not None and prev_rx is not None:
                    dtx = tx_kb - prev_tx
                    drx = rx_kb - prev_rx
                    # Counter reset or wrap: skip one sample and re-baseline.
                    if dtx >= 0 and drx >= 0:
                        # Counters are kB; convert delta kB/s to kbps.
                        eps_ul_kbps = (dtx * 8.192) / dt
                        eps_dl_kbps = (drx * 8.192) / dt
                runtime.traffic_prev_tx_kb = tx_kb
                runtime.traffic_prev_rx_kb = rx_kb
                runtime.traffic_prev_at = now

            if refresh_ds:
                cgatt = await engine.send_command("AT+CGATT?", timeout_sec=1.5)
                cereg = await engine.send_command("AT+CEREG?", timeout_sec=1.5)
                cgdcont = await engine.send_command("AT+CGDCONT?", timeout_sec=2.0)
                qiact = await engine.send_command("AT+QIACT?", timeout_sec=2.0)
                qcfg_usbnet = await engine.send_command('AT+QCFG="usbnet"', timeout_sec=2.0)
                qnetdev = await engine.send_command("AT+QNETDEVSTATUS?", timeout_sec=2.0)

                cgatt_v = _parse_cgatt(cgatt.get("lines", []))
                cereg_v = _parse_cereg(cereg.get("lines", [])) or {}
                contexts = _parse_cgdcont(cgdcont.get("lines", []))
                active = _parse_qiact(qiact.get("lines", []))
                usbnet_mode = _parse_qcfg_usbnet(qcfg_usbnet.get("lines", []))
                qnetdev_status = _parse_qnetdevstatus(qnetdev.get("lines", []))
                active_by_cid = {x.get("cid"): x for x in active if isinstance(x.get("cid"), int)}
                primary_ctx = next((c for c in contexts if c.get("cid") == 1), contexts[0] if contexts else {})
                cid1 = active_by_cid.get(1) or {}

                runtime.data_service = {
                    "apn": primary_ctx.get("apn"),
                    "pdp_type": primary_ctx.get("pdp_type"),
                    "pdp_contexts": len(contexts),
                    "active_pdp_contexts": sum(1 for x in active if x.get("active")),
                    "packet_attached": cgatt_v == 1 if cgatt_v is not None else None,
                    "eps_reg_stat": cereg_v.get("stat"),
                    "eps_registered": cereg_v.get("stat") in (1, 5) if cereg_v.get("stat") is not None else None,
                    "cid1_active": cid1.get("active"),
                    "cid1_ip": cid1.get("ip"),
                    "usbnet_mode": usbnet_mode,
                    "usbnet_mode_label": _usbnet_mode_label(usbnet_mode),
                    "qnetdev_status": qnetdev_status,
                    "qgdcnt_tx_kb": qgdcnt_v.get("tx_kb") if qgdcnt_v else None,
                    "qgdcnt_rx_kb": qgdcnt_v.get("rx_kb") if qgdcnt_v else None,
                    "eps_ul_kbps": eps_ul_kbps,
                    "eps_dl_kbps": eps_dl_kbps,
                }
                runtime.data_service_at = now
            else:
                runtime.data_service["qgdcnt_tx_kb"] = qgdcnt_v.get("tx_kb") if qgdcnt_v else None
                runtime.data_service["qgdcnt_rx_kb"] = qgdcnt_v.get("rx_kb") if qgdcnt_v else None
                runtime.data_service["eps_ul_kbps"] = eps_ul_kbps
                runtime.data_service["eps_dl_kbps"] = eps_dl_kbps

            serving = _parse_qeng_servingcell(qeng.get("lines", []))
            serving_pci = None
            serving_earfcn = None
            if isinstance(serving, dict):
                lte = serving.get("lte")
                if isinstance(lte, dict):
                    serving_pci = lte.get("pcid")
                    serving_earfcn = lte.get("earfcn")

            parsed = {
                "sample_ts": time.time(),
                "servingcell": serving,
                "network": net,
                "modem": {
                    "firmware": runtime.modem_fw,
                    "firmware_updated_at": runtime.modem_fw_at or None,
                },
                "data_service": runtime.data_service,
                "qrsrp": _parse_four_path_metric(qrsrp.get("lines", []), "+QRSRP:"),
                "qrsrq": _parse_four_path_metric(qrsrq.get("lines", []), "+QRSRQ:"),
                "qsinr": _parse_four_path_metric(qsinr.get("lines", []), "+QSINR:"),
                "neighbour": {
                    **(
                        (lambda n: {"strongest_rsrp": n["rsrp"], "strongest_pci": n["pci"], "strongest_earfcn": n.get("earfcn")})(n)
                        if (
                            n := _parse_qeng_strongest_neighbour(
                                qeng_nb.get("lines", []), serving_pci=serving_pci, serving_earfcn=serving_earfcn
                            )
                        )
                        else {"strongest_rsrp": None, "strongest_pci": None, "strongest_earfcn": None}
                    ),
                },
                "raw": {
                    "cgmr": cgmr if need_fw else None,
                    "qeng": qeng,
                    "qeng_neighbourcell": qeng_nb,
                    "qnwinfo": qnwinfo,
                    "qrsrp": qrsrp,
                    "qrsrq": qrsrq,
                    "qsinr": qsinr,
                    "qgdcnt": qgdcnt,
                },
            }
            async with runtime.lock:
                runtime.snapshot = parsed
                runtime.last_error = None
        except Exception as exc:  # noqa: BLE001
            async with runtime.lock:
                runtime.last_error = str(exc)

        elapsed = time.time() - started
        interval = max(0.2, 1.0 / max(0.1, runtime.poll_hz))
        wait_sec = max(0.0, interval - elapsed)
        await asyncio.sleep(wait_sec)

