from __future__ import annotations

import asyncio
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from app.serial_engine import SerialEngine

# Rolling window for LTE serving-cell identity change counts (EARFCN/PCI deltas between polls).
# Applies in camped / idle-style reporting and in RRC_CONNECTED (CONNECT) whenever AT+QENG exposes LTE PCell.
CARRIER_RESEL_WINDOW_SEC = 60.0

# Pre-formatted neighbour channel card (served only via /api/kpi/neighbour-channels, not WebSocket).
NEIGHBOUR_CHANNEL_CARD_MAX_ROWS = 32
NEIGHBOUR_CHANNEL_CARD_MAX_CHARS = 12_000


def _prune_ts_window(ts_deque: deque[float], now: float, window_sec: float) -> None:
    cutoff = now - window_sec
    while ts_deque and ts_deque[0] < cutoff:
        ts_deque.popleft()


@dataclass
class CarrierReselTracker:
    """Tracks LTE PCell EARFCN vs PCI changes between KPI polls (AT+QENG ``servingcell``)."""

    earfcn_change_ts: deque[float] = field(default_factory=deque)
    pci_intr_change_ts: deque[float] = field(default_factory=deque)
    last_earfcn: int | None = None
    last_pci: int | None = None


def _carrier_reselection_step(
    tracker: CarrierReselTracker,
    serving: dict[str, Any] | None,
    now: float,
) -> dict[str, Any]:
    """
    Count identity transitions over a rolling window:
    - Primary EARFCN change: LTE carrier frequency changed (inter-frequency / new anchor).
    - Intra-frequency PCI change: same EARFCN, different PCI (reselection on same carrier).

    Covers NOCONN/camped and CONNECT (RRC_Connected) snapshots; requires parseable LTE PCell identity.
    """
    _prune_ts_window(tracker.earfcn_change_ts, now, CARRIER_RESEL_WINDOW_SEC)
    _prune_ts_window(tracker.pci_intr_change_ts, now, CARRIER_RESEL_WINDOW_SEC)

    lte = serving.get("lte") if isinstance(serving, dict) else None
    ear = lte.get("earfcn") if isinstance(lte, dict) else None
    pci = lte.get("pcid") if isinstance(lte, dict) else None

    def _ok_cell_id(v: Any) -> bool:
        return isinstance(v, int) and not isinstance(v, bool)

    if not _ok_cell_id(ear) or not _ok_cell_id(pci):
        # Keep last_* across transient blanks (e.g. CONNECT layout quirks, NSA/NR gaps) so mobility
        # still registers when identity returns; does not fabricate events this poll.
        return {
            "window_sec": int(CARRIER_RESEL_WINDOW_SEC),
            "primary_earfcn_reselections_per_min": len(tracker.earfcn_change_ts),
            "intra_freq_pci_reselections_per_min": len(tracker.pci_intr_change_ts),
        }

    if tracker.last_earfcn is not None and tracker.last_pci is not None:
        if ear != tracker.last_earfcn:
            tracker.earfcn_change_ts.append(now)
        elif pci != tracker.last_pci:
            tracker.pci_intr_change_ts.append(now)

    tracker.last_earfcn = ear
    tracker.last_pci = pci

    return {
        "window_sec": int(CARRIER_RESEL_WINDOW_SEC),
        "primary_earfcn_reselections_per_min": len(tracker.earfcn_change_ts),
        "intra_freq_pci_reselections_per_min": len(tracker.pci_intr_change_ts),
    }


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


def _qeng_lte_row_echoes_serving_cell(
    earfcn: int | None,
    pci: int | None,
    serving_pci: int | None,
    serving_earfcn: int | None,
) -> bool:
    """True when a neighbour-table LTE row is a PCell echo, not a distinct neighbour.

    Some firmware repeats serving PCI (and matching EARFCN on intra lists) with no separate
    neighbours. When both neighbour and serving EARFCN are known and differ, the same PCI is
    treated as reuse on another carrier and is not suppressed.
    """
    if pci is None or serving_pci is None or pci != serving_pci:
        return False
    if serving_earfcn is not None and earfcn is not None:
        return earfcn == serving_earfcn
    return True


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


def _qcainfo_bandwidth_field_to_mhz(raw: Any) -> int | None:
    """Map ``+QCAINFO`` per-carrier bandwidth field to DL MHz (integer).

    Quectel often reports **resource block count** (e.g. 50 → 10 MHz); some firmware uses
    the same **0–5 index** as ``AT+QENG`` LTE ``dl_bw``.
    """
    if raw is None:
        return None
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return None
    rb_to_mhz = {6: 1, 15: 3, 25: 5, 50: 10, 75: 15, 100: 20}
    if v in rb_to_mhz:
        return rb_to_mhz[v]
    if 0 <= v <= 5:
        idx = {0: 1, 1: 3, 2: 5, 3: 10, 4: 15, 5: 20}
        return idx.get(v)
    return None


def _split_qcainfo_comma_fields(rest: str) -> list[str]:
    """Split QCAINFO payload after role field; respect double-quoted segments (band strings)."""
    rest = (rest or "").strip()
    if not rest:
        return []
    parts: list[str] = []
    cur: list[str] = []
    in_quote = False
    for ch in rest:
        if ch == '"':
            in_quote = not in_quote
            cur.append(ch)
        elif ch == "," and not in_quote:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur).strip())
    return [p.strip().strip('"') for p in parts]


def format_qcainfo_carriers_pcc_scc_rows(carriers: list[Any]) -> str | None:
    """Build ``6300/123(PCC), 223/456(SCC)`` from ``qcainfo.carriers`` rows (same as EARFCN active CA UI)."""
    if not isinstance(carriers, list) or not carriers:
        return None
    parts_txt: list[str] = []
    for c in carriers:
        if not isinstance(c, dict):
            continue
        e = c.get("earfcn")
        r = c.get("role")
        pci_v = c.get("pci")
        if isinstance(e, int) and r:
            role_s = str(r).strip().upper()
            if isinstance(pci_v, int):
                parts_txt.append(f"{e}/{pci_v}({role_s})")
            else:
                parts_txt.append(f"{e}({role_s})")
    return ", ".join(parts_txt) if parts_txt else None


def format_qcainfo_carriers_pcc_scc(qcainfo: dict[str, Any]) -> str:
    """PCC/SCC line for dashboard and CSV: prefer structured ``carriers``, else legacy ``earfcn_active_text``."""
    carriers = qcainfo.get("carriers")
    rows = carriers if isinstance(carriers, list) else []
    row_fmt = format_qcainfo_carriers_pcc_scc_rows(rows)
    if row_fmt:
        return row_fmt
    txt = qcainfo.get("earfcn_active_text")
    if txt is not None and str(txt).strip():
        return str(txt).strip()
    return ""


def _parse_qcainfo_for_snapshot(lines: list[str]) -> dict[str, Any]:
    """
    Parse ``AT+QCAINFO`` lines (Quectel). Typical LTE CA form per manual:

      ``+QCAINFO: "PCC",<EARFCN>,<bandwidth>,<band>,<state>,<PCI>,<RSRP>,<RSRQ>,<RSSI>,<SINR>``
      ``+QCAINFO: "SCC",...`` (same tail layout; zero or more SCC lines).

    ``bandwidth`` is often RB count (e.g. 50 for 10 MHz), not megahertz.

    ``earfcn_active_text`` lists each component as ``EARFCN/PCI(PCC)`` or ``EARFCN/PCI(SCC)``,
    comma-separated (PCI omitted from the slash pair only if the modem did not report one).
    """
    carriers: list[dict[str, Any]] = []
    for raw in lines:
        line = str(raw or "").strip()
        if not line.upper().startswith("+QCAINFO:"):
            continue
        payload = line.split(":", 1)[1].strip()
        if not payload:
            continue
        m = re.match(r'^"([^"]+)"\s*,\s*(.*)$', payload, re.DOTALL)
        if not m:
            continue
        role = m.group(1).strip().upper()
        if role not in ("PCC", "SCC"):
            continue
        fields = _split_qcainfo_comma_fields(m.group(2))
        if len(fields) < 5:
            continue
        ear = _safe_int(fields[0])
        bw_rb = _safe_int(fields[1])
        band_txt = fields[2] if fields[2] else None
        state = _safe_int(fields[3])
        pci = _safe_int(fields[4])
        c_row: dict[str, Any] = {
            "role": role,
            "earfcn": ear,
            "dl_bw_rb": bw_rb,
            "band": band_txt,
            "state": state,
            "pci": pci,

        }
        if len(fields) > 5:
            c_row["rsrp"] = _safe_int(fields[5])
        if len(fields) > 6:
            c_row["rsrq"] = _safe_int(fields[6])
        if len(fields) > 7:
            c_row["rssi"] = _safe_int(fields[7])
        if len(fields) > 8:
            c_row["sinr_raw"] = _safe_int(fields[8])
        carriers.append(c_row)

    earfcns = [c.get("earfcn") for c in carriers if isinstance(c.get("earfcn"), int)]
    component_mhz: list[int] = []
    for c in carriers:
        mw = _qcainfo_bandwidth_field_to_mhz(c.get("dl_bw_rb"))
        if isinstance(mw, int) and mw > 0:
            component_mhz.append(mw)
    text = format_qcainfo_carriers_pcc_scc_rows(carriers)
    agg: int | None = sum(component_mhz) if component_mhz else None
    return {
        "carriers": carriers,
        "earfcn_active": earfcns,
        "earfcn_active_text": text,
        "dl_bw_aggregate_mhz": agg,
        "dl_bw_components_mhz": component_mhz if component_mhz else None,
    }


def _parse_qnwinfo(lines: list[str]) -> dict[str, Any] | None:
    """Parse all ``+QNWINFO`` lines (LTE + NR5G may both appear). Primary ``act``/``band``/``channel`` stay LTE-first for backward compatibility."""
    entries: list[dict[str, Any]] = []
    for raw in lines:
        if not raw.startswith("+QNWINFO:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        if payload.upper() == "NO SERVICE":
            return {"service": "NO SERVICE", "entries": []}
        parts = _parse_csv_payload(payload)
        if len(parts) >= 4:
            entries.append(
                {
                    "act": parts[0],
                    "operator": parts[1],
                    "band": parts[2],
                    "channel": _safe_int(parts[3]),
                }
            )
    if not entries:
        return None
    lte_e = next((e for e in entries if "LTE" in str(e.get("act") or "").upper()), None)
    nr_e = next((e for e in entries if "NR" in str(e.get("act") or "").upper()), None)
    primary = lte_e or entries[0]
    out: dict[str, Any] = {
        "act": primary["act"],
        "operator": primary["operator"],
        "band": primary["band"],
        "channel": primary["channel"],
        "entries": entries,
    }
    if nr_e:
        out["nr_act"] = nr_e["act"]
        out["nr_band"] = nr_e["band"]
        out["nr_channel"] = nr_e["channel"]
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


def _pick_nr5g_four_path(modes: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    return modes.get("NR5G") or modes.get("NR")


def _parse_four_path_metrics_by_sysmode(lines: list[str], prefix: str) -> dict[str, dict[str, Any]]:
    """Quectel may return multiple ``+QRSRP`` / ``+QRSRQ`` / ``+QSINR`` lines (LTE and NR5G). Keyed by upper ``sysmode`` (e.g. ``LTE``, ``NR5G``)."""
    out: dict[str, dict[str, Any]] = {}
    for raw in lines:
        if not raw.startswith(prefix):
            continue
        payload = raw.split(":", 1)[1].strip()
        parts = _parse_csv_payload(payload)
        if len(parts) < 5:
            continue
        mode = str(parts[4]).strip().upper()
        out[mode] = {
            "prx": _safe_int(parts[0]),
            "drx": _safe_int(parts[1]),
            "rx2": _safe_int(parts[2]),
            "rx3": _safe_int(parts[3]),
            "sysmode": parts[4],
        }
    return out


def _parse_four_path_metric(lines: list[str], prefix: str) -> dict[str, Any] | None:
    # Prefer LTE row when multiple ``sysmode`` lines exist.
    modes = _parse_four_path_metrics_by_sysmode(lines, prefix)
    if not modes:
        return None
    if "LTE" in modes:
        return modes["LTE"]
    return next(iter(modes.values()), None)


def _parse_qeng_servingcell(lines: list[str]) -> dict[str, Any] | None:
    # Handle common shapes:
    # 1) +QENG: "servingcell","NOCONN"|"CONNECT"|…,"LTE",...
    # 2) +QENG: "servingcell","NOCONN"|"CONNECT"
    #    +QENG: "LTE",...   (PCell usually first; skip extra LTE lines = SCells)
    #    +QENG: "NR5G-NSA",...
    parsed: dict[str, Any] = {}
    joined = "\n".join(lines)

    m = re.search(r'\+QENG:\s*"servingcell","([^"]+)"(?:,"([^"]+)")?', joined)
    if m:
        parsed["state"] = m.group(1)
        if m.group(2):
            parsed["mode"] = m.group(2)

    lte_line_preferred: str | None = None
    lte_line_fallback: str | None = None
    nsa_line = None
    sa_line = None
    for line in lines:
        raw = line.strip()
        if not raw.startswith("+QENG:"):
            continue
        low = raw.lower()
        if lte_line_preferred is None and "servingcell" in low and re.search(r'"LTE"', raw, re.I):
            lte_line_preferred = line
        elif lte_line_fallback is None and re.match(r"^\+QENG:\s*\"LTE\"", raw, re.I):
            lte_line_fallback = line
        elif raw.startswith('+QENG:"NR5G-NSA"') or raw.startswith('+QENG: "NR5G-NSA"'):
            nsa_line = line
        elif '"NR5G-SA"' in line:
            sa_line = line

    lte_line = lte_line_preferred or lte_line_fallback

    if lte_line:
        payload = lte_line.split(":", 1)[1].strip()
        p = _parse_csv_payload(payload)
        # Shape A: "servingcell",state,"LTE",is_tdd,...
        # Shape B: "LTE",is_tdd,...
        base = 0
        if len(p) >= 4 and "servingcell" in (p[0] or "").lower():
            base = 2
        # "LTE",is_tdd,MCC,MNC,cellID,PCID,earfcn,band,UL_bw,DL_bw,<TAC/skipped>,RSRP,RSRQ,RSSI,SINR,...
        # CONNECT responses sometimes omit trailing RF fields — always parse PCID/EARFCN when present.
        min_full = 15 + base
        min_id = 7 + base
        if len(p) >= min_full:
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
        elif len(p) >= min_id:
            parsed["lte"] = {
                "rat": p[0 + base],
                "duplex": p[1 + base] if len(p) > 1 + base else None,
                "mcc": _safe_int(p[2 + base]) if len(p) > 2 + base else None,
                "mnc": _safe_int(p[3 + base]) if len(p) > 3 + base else None,
                "cell_id_hex": p[4 + base] if len(p) > 4 + base else None,
                "pcid": _safe_int(p[5 + base]),
                "earfcn": _safe_int(p[6 + base]),
                "band": _safe_int(p[7 + base]) if len(p) > 7 + base else None,
                "ul_bw": _decode_lte_bw_mhz(p[8 + base]) if len(p) > 8 + base else None,
                "dl_bw": _decode_lte_bw_mhz(p[9 + base]) if len(p) > 9 + base else None,
                "rsrp": _safe_int(p[11 + base]) if len(p) > 11 + base else None,
                "rsrq": _safe_int(p[12 + base]) if len(p) > 12 + base else None,
                "rssi": _safe_int(p[13 + base]) if len(p) > 13 + base else None,
                "sinr_raw": _safe_int(p[14 + base]) if len(p) > 14 + base else None,
                "tx_power": _safe_int(p[16 + base]) if len(p) > 16 + base else None,
            }

    if nsa_line:
        payload = nsa_line.split(":", 1)[1].strip()
        p = _parse_csv_payload(payload)
        # "NR5G-NSA",MCC,MNC,PCID,RSRP,SINR,RSRQ,ARFCN,band,...
        if len(p) >= 9:
            nsa: dict[str, Any] = {
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
            if len(p) >= 10:
                nsa["dl_bw"] = _decode_lte_bw_mhz(p[9])
            parsed["nr_nsa"] = nsa

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
                "dl_bw": _decode_lte_bw_mhz(p[11]),
                "rsrp": _safe_int(p[12]),
                "rsrq": _safe_int(p[13]),
                "sinr": _safe_int(p[14]),
            }

    return parsed or None


def _parse_qeng_strongest_neighbour(
    lines: list[str], serving_pci: int | None = None, serving_earfcn: int | None = None
) -> dict[str, int] | None:
    # Intra-frequency neighbour line shape (Quectel LTE, common):
    # ... "LTE",<EARFCN>,<PCID>,<RSRQ>,<RSRP>[,optional extra fields ...]
    # Selection: strongest RSRP among intra rows on serving EARFCN (excluding serving PCI when known).
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
                rsrq = _safe_int(parts[i + 3]) if len(parts) > i + 3 else None
                rsrp = _safe_int(parts[i + 4])
                rssi = _safe_int(parts[i + 5]) if len(parts) > i + 5 else None
                sinr = _safe_int(parts[i + 6]) if len(parts) > i + 6 else None
                if rsrp is None or pci is None:
                    continue
                if serving_earfcn is not None and earfcn is not None and earfcn != serving_earfcn:
                    continue
                cand: dict[str, int | None] = {
                    "pci": pci,
                    "rsrp": rsrp,
                    "earfcn": earfcn,
                    "rsrq": rsrq,
                    "rssi": rssi,
                    "sinr": sinr,
                }
                if serving_pci is not None and pci == serving_pci:
                    # Some firmware lists serving cell first in neighbour table; keep only as fallback.
                    if fallback is None or rsrp > fallback["rsrp"]:
                        fallback = cand
                    continue
                if best is None or rsrp > best["rsrp"]:
                    best = cand
    chosen = best if best is not None else fallback
    if chosen is None:
        return None
    # When only the serving cell appears in intra neighbour list — omit KPI/charts.
    if _qeng_lte_row_echoes_serving_cell(
        int(chosen["earfcn"]) if chosen.get("earfcn") is not None else None,
        int(chosen["pci"]) if chosen.get("pci") is not None else None,
        serving_pci,
        serving_earfcn,
    ):
        return None
    return chosen


def _parse_qeng_strongest_inter_neighbour(
    lines: list[str], serving_pci: int | None = None, serving_earfcn: int | None = None
) -> dict[str, int | None] | None:
    """Inter-frequency (inter-cell) LTE neighbours from +QENG neighbourcell inter lines."""
    best: dict[str, int | None] | None = None
    fallback: dict[str, int | None] | None = None
    for raw in lines:
        if not raw.startswith("+QENG:"):
            continue
        low = raw.lower()
        if "neighbourcell inter" not in low:
            continue
        parts = _parse_csv_payload(raw.split(":", 1)[1].strip())
        for i, token in enumerate(parts):
            rat = token.upper()
            if rat == "LTE" and len(parts) > i + 4:
                earfcn = _safe_int(parts[i + 1]) if len(parts) > i + 1 else None
                pci = _safe_int(parts[i + 2]) if len(parts) > i + 2 else None
                rsrq = _safe_int(parts[i + 3]) if len(parts) > i + 3 else None
                rsrp = _safe_int(parts[i + 4])
                rssi = _safe_int(parts[i + 5]) if len(parts) > i + 5 else None
                sinr = _safe_int(parts[i + 6]) if len(parts) > i + 6 else None
                if rsrp is None or pci is None:
                    continue
                if serving_earfcn is not None and earfcn is not None and earfcn == serving_earfcn:
                    continue
                cand: dict[str, int | None] = {
                    "pci": pci,
                    "rsrp": rsrp,
                    "earfcn": earfcn,
                    "rsrq": rsrq,
                    "rssi": rssi,
                    "sinr": sinr,
                }
                if serving_pci is not None and pci == serving_pci:
                    if fallback is None or rsrp > fallback["rsrp"]:
                        fallback = cand
                    continue
                if best is None or rsrp > best["rsrp"]:
                    best = cand
    chosen = best if best is not None else fallback
    if chosen is None:
        return None
    if _qeng_lte_row_echoes_serving_cell(
        int(chosen["earfcn"]) if chosen.get("earfcn") is not None else None,
        int(chosen["pci"]) if chosen.get("pci") is not None else None,
        serving_pci,
        serving_earfcn,
    ):
        return None
    return chosen


def _parse_qeng_strongest_nr_neighbour(
    lines: list[str],
    serving_pci: int | None = None,
    serving_arfcn: int | None = None,
) -> dict[str, int | None] | None:
    """Strongest NR neighbour on ``neighbourcell intra`` (NR5G/NR rows), same NR ARFCN as serving when known."""
    best: dict[str, int | None] | None = None
    fallback: dict[str, int | None] | None = None
    for raw in lines:
        if not raw.startswith("+QENG:"):
            continue
        low = raw.lower()
        if "neighbourcell intra" not in low:
            continue
        parts = _parse_csv_payload(raw.split(":", 1)[1].strip())
        for i, token in enumerate(parts):
            rat = token.upper()
            if rat not in ("NR5G", "NR"):
                continue
            if len(parts) <= i + 4:
                continue
            arfcn = _safe_int(parts[i + 1])
            pci = _safe_int(parts[i + 2])
            rsrq = _safe_int(parts[i + 3])
            rsrp = _safe_int(parts[i + 4])
            rssi = _safe_int(parts[i + 5]) if len(parts) > i + 5 else None
            sinr = _safe_int(parts[i + 6]) if len(parts) > i + 6 else None
            if rsrp is None or pci is None:
                continue
            if serving_arfcn is not None and arfcn is not None and arfcn != serving_arfcn:
                continue
            cand: dict[str, int | None] = {
                "pci": pci,
                "rsrp": rsrp,
                "arfcn": arfcn,
                "rsrq": rsrq,
                "rssi": rssi,
                "sinr": sinr,
            }
            if serving_pci is not None and pci == serving_pci:
                if fallback is None or rsrp > fallback["rsrp"]:
                    fallback = cand
                continue
            if best is None or rsrp > best["rsrp"]:
                best = cand
    chosen = best if best is not None else fallback
    if chosen is None:
        return None
    if _qeng_lte_row_echoes_serving_cell(
        int(chosen["arfcn"]) if chosen.get("arfcn") is not None else None,
        int(chosen["pci"]) if chosen.get("pci") is not None else None,
        serving_pci,
        serving_arfcn,
    ):
        return None
    return chosen


def _nr_primary_band_label(
    net: dict[str, Any] | None,
    nr_serv: dict[str, Any] | None,
    *,
    prefer_qeng_serving: bool,
) -> str | None:
    """Human-readable NR band: optional QNWINFO NR row and/or 3GPP index from QENG serving NR."""

    def from_nwinfo() -> str | None:
        if not isinstance(net, dict):
            return None
        nb = net.get("nr_band")
        if nb is None:
            return None
        s = str(nb).strip()
        return s or None

    def from_qeng() -> str | None:
        if not isinstance(nr_serv, dict):
            return None
        b = nr_serv.get("band")
        if isinstance(b, int):
            return f"n{b}"
        if b is not None:
            s = str(b).strip()
            return s or None
        return None

    q = from_qeng()
    n = from_nwinfo()
    if prefer_qeng_serving:
        if q is not None:
            return q
        return n
    if n is not None:
        return n
    return q


def _nr_duplex_from_serving(nr_serv: dict[str, Any] | None) -> str | None:
    """FDD/TDD from ``AT+QENG`` NR5G-SA serving cell (field after RAT). Not present on NR5G-NSA rows."""
    if not isinstance(nr_serv, dict):
        return None
    d = nr_serv.get("duplex")
    if d is None:
        return None
    s = str(d).strip().upper()
    return s or None


def _compose_nr_rf_kpi(
    net: dict[str, Any] | None,
    serving: dict[str, Any] | None,
    qrsrp_lines: list[str],
    qrsrq_lines: list[str],
    qsinr_lines: list[str],
    nr_neighbour: dict[str, int | None] | None,
) -> dict[str, Any]:
    mr = _parse_four_path_metrics_by_sysmode(qrsrp_lines, "+QRSRP:")
    mq = _parse_four_path_metrics_by_sysmode(qrsrq_lines, "+QRSRQ:")
    ms = _parse_four_path_metrics_by_sysmode(qsinr_lines, "+QSINR:")
    nr_r = _pick_nr5g_four_path(mr)
    nr_q = _pick_nr5g_four_path(mq)
    nr_s = _pick_nr5g_four_path(ms)

    nr_serv: dict[str, Any] | None = None
    is_nr_sa = bool(isinstance(serving, dict) and isinstance(serving.get("nr_sa"), dict))
    if isinstance(serving, dict):
        nn = serving.get("nr_nsa")
        ns = serving.get("nr_sa")
        if isinstance(nn, dict):
            nr_serv = nn
        elif isinstance(ns, dict):
            nr_serv = ns

    has_nr_net = bool(isinstance(net, dict) and (net.get("nr_band") is not None or net.get("nr_channel") is not None))
    has_nr = bool(nr_serv or nr_r or nr_q or nr_s or has_nr_net)

    serving_nr_type: str | None = None
    if isinstance(serving, dict):
        nsa_d = serving.get("nr_sa")
        nnsa_d = serving.get("nr_nsa")
        if isinstance(nsa_d, dict):
            serving_nr_type = nsa_d.get("mode") if isinstance(nsa_d.get("mode"), str) else None
        elif isinstance(nnsa_d, dict):
            serving_nr_type = nnsa_d.get("rat") if isinstance(nnsa_d.get("rat"), str) else None

    primary: dict[str, Any] = {
        "serving_nr_type": serving_nr_type,
        "band": _nr_primary_band_label(
            net if isinstance(net, dict) else None,
            nr_serv,
            prefer_qeng_serving=is_nr_sa,
        ),
        "arfcn": None,
        "pci": None,
        "dl_bw": None,
        "duplex": _nr_duplex_from_serving(nr_serv),
        "rsrp": None,
        "rsrq": None,
        "sinr": None,
    }
    if isinstance(nr_serv, dict):
        primary["arfcn"] = nr_serv.get("arfcn")
        primary["pci"] = nr_serv.get("pcid")
        primary["dl_bw"] = nr_serv.get("dl_bw")
        primary["rsrp"] = nr_serv.get("rsrp")
        primary["rsrq"] = nr_serv.get("rsrq")
        primary["sinr"] = nr_serv.get("sinr")

    if primary["arfcn"] is None and isinstance(net, dict):
        primary["arfcn"] = net.get("nr_channel")

    if nr_r and nr_r.get("prx") is not None:
        primary["rsrp"] = nr_r["prx"]
    if nr_q and nr_q.get("prx") is not None:
        primary["rsrq"] = nr_q["prx"]
    if nr_s and nr_s.get("prx") is not None:
        primary["sinr"] = nr_s["prx"]

    nbr_out: dict[str, Any] | None = None
    if nr_neighbour:
        nbr_out = {
            "pci": nr_neighbour.get("pci"),
            "arfcn": nr_neighbour.get("arfcn"),
            "dl_bw": nr_neighbour.get("dl_bw"),
            "rsrp": nr_neighbour.get("rsrp"),
            "rsrq": nr_neighbour.get("rsrq"),
            "sinr": nr_neighbour.get("sinr"),
        }

    return {"available": has_nr, "primary": primary, "neighbour": nbr_out}


def _count_qeng_intra_neighbours(
    lines: list[str], serving_pci: int | None = None, serving_earfcn: int | None = None
) -> int:
    """Distinct intra-frequency LTE neighbours on ``neighbourcell intra`` lines.

    Rows must match serving EARFCN when known; excludes the serving PCI (modem often echoes PCell).
    """
    keys: set[tuple[int, int]] = set()
    for raw in lines:
        if not raw.startswith("+QENG:"):
            continue
        if "neighbourcell intra" not in raw.lower():
            continue
        parts = _parse_csv_payload(raw.split(":", 1)[1].strip())
        for i, token in enumerate(parts):
            rat = token.upper()
            if rat == "LTE" and len(parts) > i + 4:
                earfcn = _safe_int(parts[i + 1]) if len(parts) > i + 1 else None
                pci = _safe_int(parts[i + 2]) if len(parts) > i + 2 else None
                rsrp = _safe_int(parts[i + 4])
                if pci is None or rsrp is None:
                    continue
                if serving_earfcn is not None and earfcn is not None and earfcn != serving_earfcn:
                    continue
                if _qeng_lte_row_echoes_serving_cell(earfcn, pci, serving_pci, serving_earfcn):
                    continue
                if earfcn is not None and pci is not None:
                    keys.add((earfcn, pci))
    return len(keys)


def _count_qeng_inter_neighbours(
    lines: list[str], serving_pci: int | None = None, serving_earfcn: int | None = None
) -> int:
    """Distinct inter-frequency LTE neighbours on ``neighbourcell inter`` lines (not serving EARFCN)."""
    keys: set[tuple[int, int]] = set()
    for raw in lines:
        if not raw.startswith("+QENG:"):
            continue
        if "neighbourcell inter" not in raw.lower():
            continue
        parts = _parse_csv_payload(raw.split(":", 1)[1].strip())
        for i, token in enumerate(parts):
            rat = token.upper()
            if rat == "LTE" and len(parts) > i + 4:
                earfcn = _safe_int(parts[i + 1]) if len(parts) > i + 1 else None
                pci = _safe_int(parts[i + 2]) if len(parts) > i + 2 else None
                rsrp = _safe_int(parts[i + 4])
                if pci is None or rsrp is None:
                    continue
                if serving_earfcn is not None and earfcn is not None and earfcn == serving_earfcn:
                    continue
                if _qeng_lte_row_echoes_serving_cell(earfcn, pci, serving_pci, serving_earfcn):
                    continue
                if earfcn is not None and pci is not None:
                    keys.add((earfcn, pci))
    return len(keys)


def _qeng_channel_row_fill_score(row: dict[str, int | None]) -> int:
    return sum(1 for k in ("pci", "rsrq", "rsrp", "rssi", "sinr") if row.get(k) is not None)


def _merge_qeng_channel_rows(
    prev: dict[str, int | None] | None, cand: dict[str, int | None]
) -> dict[str, int | None]:
    if prev is None:
        return cand
    pr, cr = prev.get("rsrp"), cand.get("rsrp")
    if cr is not None and (pr is None or cr > pr):
        return cand
    if pr is not None and cr is not None:
        if cr > pr:
            return cand
        if cr < pr:
            return prev
        if _qeng_channel_row_fill_score(cand) > _qeng_channel_row_fill_score(prev):
            return cand
        return prev
    if pr is None and cr is None and _qeng_channel_row_fill_score(cand) > _qeng_channel_row_fill_score(prev):
        return cand
    if pr is None and cr is not None:
        return cand
    return prev


def _list_qeng_neighbour_lte_channel_rows(
    lines: list[str],
    *,
    inter: bool,
    serving_pci: int | None,
    serving_earfcn: int | None,
) -> list[dict[str, int | None]]:
    """LTE rows from QENG neighbourcell intra/inter; partial '-' fields allowed; capped for UI text."""
    tag = "neighbourcell inter" if inter else "neighbourcell intra"
    best: dict[tuple[int, int], dict[str, int | None]] = {}
    for raw in lines:
        if not raw.startswith("+QENG:"):
            continue
        if tag not in raw.lower():
            continue
        parts = _parse_csv_payload(raw.split(":", 1)[1].strip())
        for i, token in enumerate(parts):
            if token.upper() != "LTE":
                continue
            if len(parts) <= i + 1:
                continue
            earfcn = _safe_int(parts[i + 1]) if len(parts) > i + 1 else None
            if earfcn is None:
                continue
            pci = _safe_int(parts[i + 2]) if len(parts) > i + 2 else None
            rsrq = _safe_int(parts[i + 3]) if len(parts) > i + 3 else None
            rsrp = _safe_int(parts[i + 4]) if len(parts) > i + 4 else None
            rssi = _safe_int(parts[i + 5]) if len(parts) > i + 5 else None
            sinr = _safe_int(parts[i + 6]) if len(parts) > i + 6 else None
            if inter:
                if serving_earfcn is not None and earfcn == serving_earfcn:
                    continue
            else:
                if serving_earfcn is not None and earfcn != serving_earfcn:
                    continue
            if _qeng_lte_row_echoes_serving_cell(earfcn, pci, serving_pci, serving_earfcn):
                continue
            cand: dict[str, int | None] = {
                "earfcn": earfcn,
                "pci": pci,
                "rsrq": rsrq,
                "rsrp": rsrp,
                "rssi": rssi,
                "sinr": sinr,
            }
            dedupe_pci = pci if pci is not None else -1
            key = (earfcn, dedupe_pci)
            best[key] = _merge_qeng_channel_rows(best.get(key), cand)
    out = list(best.values())

    def _rsrp_sort(r: dict[str, int | None]) -> int:
        v = r.get("rsrp")
        return v if v is not None else -10**9

    out.sort(key=_rsrp_sort, reverse=True)
    return out[:NEIGHBOUR_CHANNEL_CARD_MAX_ROWS]


def neighbour_channel_rows_to_text(rows: list[dict[str, int | None]]) -> str:
    """Distinct LTE EARFCNs, one per line (order: strongest RSRP first among input rows)."""
    if not rows:
        return "-"

    seen: set[int] = set()
    lines: list[str] = []
    for r in rows:
        e = r.get("earfcn")
        if e is None or e in seen:
            continue
        seen.add(e)
        lines.append(str(e))
    if not lines:
        return "-"
    text = "\n".join(lines)
    if len(text) > NEIGHBOUR_CHANNEL_CARD_MAX_CHARS:
        return text[: NEIGHBOUR_CHANNEL_CARD_MAX_CHARS] + "\n..."
    return text


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


_QIACT_LINE_RE = re.compile(
    # Quectel: +QIACT: <cid>,<context_state>,<IP_version>,<address>
    # context_state: 0=deactivated, 1=activated
    # IP_version: 1=IPv4, 2=IPv6, 3=IPv4v6 (do NOT use this field as "active")
    r'\+QIACT:\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*"([^"]*)"',
    re.IGNORECASE,
)


def _parse_qiact(lines: list[str]) -> list[dict[str, Any]]:
    """Parse AT+QIACT? lines. Activation is field 2; field 3 is IP version (IPv4/IPv6/v6v4)."""
    out: list[dict[str, Any]] = []
    for raw in lines:
        if not raw.startswith("+QIACT:"):
            continue
        m = _QIACT_LINE_RE.search(raw)
        if m:
            cid = _safe_int(m.group(1))
            ctx_state = _safe_int(m.group(2))
            ip_ver = _safe_int(m.group(3))
            addr = (m.group(4) or "").strip() or None
            if cid is None:
                continue
            out.append(
                {
                    "cid": cid,
                    "active": ctx_state == 1,
                    "ip_version": ip_ver,
                    "ip": addr,
                }
            )
            continue
        # Fallback: comma-split (IPv4-only lines without commas inside quotes).
        payload = raw.split(":", 1)[1].strip()
        parts = _parse_csv_payload(payload)
        if len(parts) < 4:
            continue
        cid = _safe_int(parts[0])
        ctx_state = _safe_int(parts[1])
        ip_ver = _safe_int(parts[2])
        if cid is None:
            continue
        out.append(
            {
                "cid": cid,
                "active": ctx_state == 1,
                "ip_version": ip_ver,
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


_CGAUTH_LINE_RE = re.compile(
    r'^\+CGAUTH:\s*(\d+)\s*,\s*(\d+)(?:\s*,\s*"([^"]*)"(?:\s*,\s*"([^"]*)")?)?',
    re.IGNORECASE,
)


def _parse_cgauth(lines: list[str]) -> list[dict[str, Any]]:
    """Parse ``+CGAUTH:`` (3GPP 27.007): cid, auth_prot[, userid[, password]]."""
    out: list[dict[str, Any]] = []
    for raw in lines:
        s = str(raw or "").strip().lstrip("\ufeff")
        if not s.upper().startswith("+CGAUTH:"):
            continue
        m = _CGAUTH_LINE_RE.match(s)
        if not m:
            continue
        cid = int(m.group(1))
        auth_type = int(m.group(2))
        uid = m.group(3)
        pwd = m.group(4)
        out.append(
            {
                "cid": cid,
                "auth_type": auth_type,
                "username": uid or "",
                "password_present_in_response": bool(pwd),
            }
        )
    return out


_QICSGP_FULL_RE = re.compile(
    r'^\+QICSGP:\s*(\d+)\s*,\s*(\d+)\s*,\s*"([^"]*)"\s*,\s*"([^"]*)"\s*,\s*"([^"]*)"\s*,\s*(\d+)\s*$',
    re.IGNORECASE,
)
_QICSGP_SHORT_RE = re.compile(
    r'^\+QICSGP:\s*(\d+)\s*,\s*"([^"]*)"\s*,\s*"([^"]*)"\s*,\s*"([^"]*)"\s*,\s*(\d+)\s*$',
    re.IGNORECASE,
)


async def _read_qicsgp_best_effort(
    engine: SerialEngine,
    primary_cid: int,
    *,
    timeout_sec: float = 2.0,
    initial: dict[str, Any] | None = None,
    probe_append: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Quectel PDP profile read: ``AT+QICSGP?`` or per-context ``AT+QICSGP=<cid>`` when ``?`` is empty/ERROR."""
    qicsgp = (
        initial
        if initial is not None
        else await engine.send_command("AT+QICSGP?", timeout_sec=timeout_sec)
    )
    lines = qicsgp.get("lines", []) or []
    if qicsgp.get("ok") and _parse_qicsgp(lines):
        return qicsgp
    seen: set[int] = set()
    for cid in (int(primary_cid), 1, 2, 3):
        if cid < 1 or cid in seen:
            continue
        seen.add(cid)
        q2 = await engine.send_command(f"AT+QICSGP={cid}", timeout_sec=timeout_sec)
        if probe_append is not None:
            probe_append.append({"cmd": f"AT+QICSGP={cid} (readback fallback)", "res": q2})
        if q2.get("ok") and _parse_qicsgp(q2.get("lines", []) or []):
            return q2
    return qicsgp


def _parse_qicsgp(lines: list[str]) -> list[dict[str, Any]]:
    """Parse ``+QICSGP:`` query lines (Quectel). Prefer context_type + apn + user + pass + auth."""
    out: list[dict[str, Any]] = []
    for raw in lines:
        s = str(raw or "").strip().lstrip("\ufeff")
        if not s.upper().startswith("+QICSGP:"):
            continue
        m = _QICSGP_FULL_RE.match(s)
        if m:
            out.append(
                {
                    "cid": int(m.group(1)),
                    "context_type": int(m.group(2)),
                    "apn": m.group(3) or None,
                    "username": m.group(4) or "",
                    "password_present_in_response": bool(m.group(5)),
                    "auth_type": int(m.group(6)),
                }
            )
            continue
        m2 = _QICSGP_SHORT_RE.match(s)
        if m2:
            out.append(
                {
                    "cid": int(m2.group(1)),
                    "context_type": None,
                    "apn": m2.group(2) or None,
                    "username": m2.group(3) or "",
                    "password_present_in_response": bool(m2.group(4)),
                    "auth_type": int(m2.group(5)),
                }
            )
    return out


def _parse_cgcontrdp(lines: list[str]) -> list[dict[str, Any]]:
    """Parse ``+CGCONTRDP:`` (3GPP TS 27.007) — EPS bearer id and **QCI** when the modem includes them."""
    out: list[dict[str, Any]] = []
    for raw in lines:
        s = str(raw or "").strip().lstrip("\ufeff")
        if not s.upper().startswith("+CGCONTRDP:"):
            continue
        parts = _parse_csv_payload(s.split(":", 1)[1].strip())
        if not parts:
            continue
        cid = _safe_int(parts[0])
        if cid is None:
            continue
        bearer_id: int | None
        qci: int | None
        if len(parts) >= 3:
            bearer_id = _safe_int(parts[1])
            qci = _safe_int(parts[2])
        elif len(parts) == 2:
            v1 = _safe_int(parts[1])
            if v1 is not None and 0 <= v1 <= 85:
                bearer_id = None
                qci = v1
            else:
                bearer_id = v1
                qci = None
        else:
            bearer_id = None
            qci = None
        out.append({"cid": cid, "bearer_id": bearer_id, "qci": qci, "field_count": len(parts)})
    return out


def _format_eps_qci_label(rows: list[dict[str, Any]], primary_cid: int) -> str | None:
    """Short dashboard string: prefer **primary** CID; ``CID/EBI:QCI`` tokens (EBI omitted when unknown)."""
    if not rows:
        return None
    want = [r for r in rows if r.get("cid") == primary_cid] or list(rows)
    bits: list[str] = []
    for r in want:
        qi = r.get("qci")
        if qi is None:
            continue
        cid = r.get("cid")
        bi = r.get("bearer_id")
        if bi is not None:
            bits.append(f"{cid}/{bi}:{qi}")
        else:
            bits.append(f"CID{cid}:{qi}")
    if not bits:
        return None
    out: list[str] = []
    for b in bits:
        if b not in out:
            out.append(b)
    return " ".join(out[:8])


async def _read_cgcontrdp_best_effort(
    engine: SerialEngine,
    primary_cid: int,
    *,
    timeout_sec: float = 2.5,
) -> dict[str, Any]:
    """``AT+CGCONTRDP?`` or ``AT+CGCONTRDP=<cid>`` when bulk read is empty or unsupported."""
    r = await engine.send_command("AT+CGCONTRDP?", timeout_sec=timeout_sec)
    if r.get("ok") and _parse_cgcontrdp(r.get("lines", []) or []):
        return r
    last: dict[str, Any] = r
    seen: set[int] = set()
    for cid in (int(primary_cid), 1, 2, 3):
        if cid < 1 or cid in seen:
            continue
        seen.add(cid)
        r2 = await engine.send_command(f"AT+CGCONTRDP={cid}", timeout_sec=timeout_sec)
        last = r2
        if r2.get("ok") and _parse_cgcontrdp(r2.get("lines", []) or []):
            return r2
    return last


def _pdp_auth_type_label(auth_type: int | None) -> str:
    if auth_type is None:
        return "-"
    labels = {0: "none", 1: "PAP", 2: "CHAP", 3: "PAP or CHAP"}
    return labels.get(int(auth_type), f"type {auth_type}")


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


@dataclass
class KpiRuntime:
    snapshot: dict[str, Any] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    poll_running: bool = False
    poll_hz: float = 2.0
    last_error: str | None = None
    modem_fw: str | None = None
    modem_fw_at: float = 0.0
    data_service: dict[str, Any] = field(default_factory=dict)
    data_service_at: float = 0.0
    carrier_resel: CarrierReselTracker = field(default_factory=CarrierReselTracker)
    neighbour_channel_card: dict[str, Any] = field(
        default_factory=lambda: {"intra_text": "-", "inter_text": "-", "sample_ts": None}
    )


async def kpi_poll_loop(engine: SerialEngine, runtime: KpiRuntime) -> None:
    runtime.poll_running = True
    try:
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
                    qcainfo_res = await engine.send_command("AT+QCAINFO", timeout_sec=2.0)
                else:
                    # Avoid command spam/errors when modem is deregistered/no-service.
                    qrsrp = {"ok": False, "command": "AT+QRSRP", "final": "SKIPPED_NO_SERVICE", "lines": []}
                    qrsrq = {"ok": False, "command": "AT+QRSRQ", "final": "SKIPPED_NO_SERVICE", "lines": []}
                    qsinr = {"ok": False, "command": "AT+QSINR", "final": "SKIPPED_NO_SERVICE", "lines": []}
                    qeng_nb = {"ok": False, "command": 'AT+QENG="neighbourcell"', "final": "SKIPPED_NO_SERVICE", "lines": []}
                    qcainfo_res = {"ok": False, "command": "AT+QCAINFO", "final": "SKIPPED_NO_SERVICE", "lines": []}

                refresh_ds = (now - runtime.data_service_at) > 5.0 or not runtime.data_service

                if refresh_ds:
                    cgatt = await engine.send_command("AT+CGATT?", timeout_sec=1.5)
                    cereg = await engine.send_command("AT+CEREG?", timeout_sec=1.5)
                    cgdcont = await engine.send_command("AT+CGDCONT?", timeout_sec=2.0)
                    contexts = _parse_cgdcont(cgdcont.get("lines", []))
                    primary_ctx_ds = next((c for c in contexts if c.get("cid") == 1), contexts[0] if contexts else {})
                    primary_cid_ds = int(primary_ctx_ds.get("cid") or 1)
                    cgauth = await engine.send_command("AT+CGAUTH?", timeout_sec=2.0)
                    qicsgp = await _read_qicsgp_best_effort(engine, primary_cid_ds, timeout_sec=2.0)
                    qiact = await engine.send_command("AT+QIACT?", timeout_sec=2.0)
                    cgcontrdp_res = await _read_cgcontrdp_best_effort(engine, primary_cid_ds, timeout_sec=2.5)
                    cgcontrdp_rows = _parse_cgcontrdp(cgcontrdp_res.get("lines", []) or [])
                    qcfg_usbnet = await engine.send_command('AT+QCFG="usbnet"', timeout_sec=2.0)
                    qnetdev = await engine.send_command("AT+QNETDEVSTATUS?", timeout_sec=2.0)

                    cgatt_v = _parse_cgatt(cgatt.get("lines", []))
                    cereg_v = _parse_cereg(cereg.get("lines", [])) or {}
                    auth_rows = _parse_cgauth(cgauth.get("lines", []))
                    qicsgp_rows = _parse_qicsgp(qicsgp.get("lines", []))
                    active = _parse_qiact(qiact.get("lines", []))
                    usbnet_mode = _parse_qcfg_usbnet(qcfg_usbnet.get("lines", []))
                    qnetdev_status = _parse_qnetdevstatus(qnetdev.get("lines", []))
                    active_by_cid = {x.get("cid"): x for x in active if isinstance(x.get("cid"), int)}
                    primary_ctx = primary_ctx_ds
                    cid1 = active_by_cid.get(1) or {}
                    primary_cid = primary_cid_ds
                    ca_one = next((r for r in auth_rows if r.get("cid") == primary_cid), None)
                    qi_one = next((r for r in qicsgp_rows if r.get("cid") == primary_cid), None)
                    pdp_user = None
                    pdp_auth = None
                    pwd_hint = False
                    if ca_one:
                        u = str(ca_one.get("username") or "").strip()
                        if u:
                            pdp_user = u
                        pdp_auth = ca_one.get("auth_type")
                        pwd_hint = pwd_hint or bool(ca_one.get("password_present_in_response"))
                    if qi_one:
                        if not pdp_user:
                            u2 = str(qi_one.get("username") or "").strip()
                            if u2:
                                pdp_user = u2
                        if pdp_auth is None:
                            pdp_auth = qi_one.get("auth_type")
                        pwd_hint = pwd_hint or bool(qi_one.get("password_present_in_response"))
                    pdp_auth_label = _pdp_auth_type_label(int(pdp_auth) if pdp_auth is not None else None)
                    eps_qci_rows = [
                        {"cid": int(r["cid"]), "bearer_id": r.get("bearer_id"), "qci": r.get("qci")}
                        for r in cgcontrdp_rows
                    ]
                    eps_qci_label = _format_eps_qci_label(cgcontrdp_rows, primary_cid_ds)

                    _creg_stat = cereg_v.get("stat")
                    # 3GPP TS 27.007 +CEREG stat: 1=home, 5=roaming (when registered on EPS).
                    _eps_scope: str | None = (
                        "roaming"
                        if _creg_stat == 5
                        else "home"
                        if _creg_stat == 1
                        else None
                    )
                    runtime.data_service = {
                        "apn": primary_ctx.get("apn"),
                        "pdp_type": primary_ctx.get("pdp_type"),
                        "pdp_auth_type": pdp_auth,
                        "pdp_auth_label": pdp_auth_label,
                        "pdp_username": pdp_user,
                        "pdp_password_reported": pwd_hint,
                        "pdp_contexts": len(contexts),
                        "active_pdp_contexts": sum(1 for x in active if x.get("active")),
                        "packet_attached": cgatt_v == 1 if cgatt_v is not None else None,
                        "eps_reg_stat": _creg_stat,
                        "eps_registered": _creg_stat in (1, 5) if _creg_stat is not None else None,
                        "eps_roaming": True if _creg_stat == 5 else False if _creg_stat == 1 else None,
                        "eps_reg_scope": _eps_scope,
                        "cid1_active": cid1.get("active"),
                        "cid1_ip": cid1.get("ip"),
                        "usbnet_mode": usbnet_mode,
                        "usbnet_mode_label": _usbnet_mode_label(usbnet_mode),
                        "qnetdev_status": qnetdev_status,
                        "eps_qci_rows": eps_qci_rows,
                        "eps_qci_label": eps_qci_label,
                        "eps_qci_query_ok": bool(cgcontrdp_res.get("ok")),
                    }
                    runtime.data_service_at = now

                serving = _parse_qeng_servingcell(qeng.get("lines", []))
                serving_pci = None
                serving_earfcn = None
                if isinstance(serving, dict):
                    lte = serving.get("lte")
                    if isinstance(lte, dict):
                        serving_pci = lte.get("pcid")
                        serving_earfcn = lte.get("earfcn")

                serving_nr_pci = None
                serving_nr_arfcn = None
                if isinstance(serving, dict):
                    nr_nsa = serving.get("nr_nsa")
                    nr_sa = serving.get("nr_sa")
                    if isinstance(nr_nsa, dict):
                        serving_nr_pci = nr_nsa.get("pcid")
                        serving_nr_arfcn = nr_nsa.get("arfcn")
                    if isinstance(nr_sa, dict):
                        if serving_nr_pci is None:
                            serving_nr_pci = nr_sa.get("pcid")
                        if serving_nr_arfcn is None:
                            serving_nr_arfcn = nr_sa.get("arfcn")

                sample_ts = time.time()
                carrier_resel = _carrier_reselection_step(
                    runtime.carrier_resel,
                    serving if isinstance(serving, dict) else None,
                    sample_ts,
                )

                nb_lines = qeng_nb.get("lines", [])
                intra_n = _parse_qeng_strongest_neighbour(nb_lines, serving_pci=serving_pci, serving_earfcn=serving_earfcn)
                inter_n = _parse_qeng_strongest_inter_neighbour(
                    nb_lines, serving_pci=serving_pci, serving_earfcn=serving_earfcn
                )
                intra_n_count = _count_qeng_intra_neighbours(nb_lines, serving_pci=serving_pci, serving_earfcn=serving_earfcn)
                inter_n_count = _count_qeng_inter_neighbours(nb_lines, serving_pci=serving_pci, serving_earfcn=serving_earfcn)
                nr_intra_n = _parse_qeng_strongest_nr_neighbour(
                    nb_lines, serving_pci=serving_nr_pci, serving_arfcn=serving_nr_arfcn
                )
                try:
                    intra_rows = _list_qeng_neighbour_lte_channel_rows(
                        nb_lines, inter=False, serving_pci=serving_pci, serving_earfcn=serving_earfcn
                    )
                    inter_rows = _list_qeng_neighbour_lte_channel_rows(
                        nb_lines, inter=True, serving_pci=serving_pci, serving_earfcn=serving_earfcn
                    )
                    ch_intra = neighbour_channel_rows_to_text(intra_rows)
                    ch_inter = neighbour_channel_rows_to_text(inter_rows)
                except Exception:  # noqa: BLE001
                    ch_intra, ch_inter = "-", "-"
                neighbour_channel_card = {
                    "intra_text": ch_intra,
                    "inter_text": ch_inter,
                    "sample_ts": sample_ts,
                }
                neighbour = {
                    "strongest_rsrp": intra_n.get("rsrp") if intra_n else None,
                    "strongest_pci": intra_n.get("pci") if intra_n else None,
                    "strongest_earfcn": intra_n.get("earfcn") if intra_n else None,
                    "strongest_rsrq": intra_n.get("rsrq") if intra_n else None,
                    "strongest_rssi": intra_n.get("rssi") if intra_n else None,
                    "strongest_sinr": intra_n.get("sinr") if intra_n else None,
                    "inter_strongest_rsrp": inter_n.get("rsrp") if inter_n else None,
                    "inter_strongest_pci": inter_n.get("pci") if inter_n else None,
                    "inter_strongest_earfcn": inter_n.get("earfcn") if inter_n else None,
                    "inter_strongest_rsrq": inter_n.get("rsrq") if inter_n else None,
                    "inter_strongest_rssi": inter_n.get("rssi") if inter_n else None,
                    "inter_strongest_sinr": inter_n.get("sinr") if inter_n else None,
                    "intra_neighbour_count": intra_n_count,
                    "inter_neighbour_count": inter_n_count,
                }

                nr_rf = _compose_nr_rf_kpi(
                    net if isinstance(net, dict) else None,
                    serving if isinstance(serving, dict) else None,
                    list(qrsrp.get("lines") or []),
                    list(qrsrq.get("lines") or []),
                    list(qsinr.get("lines") or []),
                    nr_intra_n,
                )

                qcainfo_parsed = _parse_qcainfo_for_snapshot(list(qcainfo_res.get("lines") or []))
                qcainfo_parsed["query_ok"] = bool(qcainfo_res.get("ok"))

                parsed = {
                    "sample_ts": sample_ts,
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
                    "neighbour": neighbour,
                    "nr_rf": nr_rf,
                    "qcainfo": qcainfo_parsed,
                    "carrier_reselection": carrier_resel,
                    "raw": {
                        "cgmr": cgmr if need_fw else None,
                        "qeng": qeng,
                        "qeng_neighbourcell": qeng_nb,
                        "qnwinfo": qnwinfo,
                        "qcainfo": qcainfo_res,
                        "qrsrp": qrsrp,
                        "qrsrq": qrsrq,
                        "qsinr": qsinr,
                    },
                }
                async with runtime.lock:
                    runtime.snapshot = parsed
                    runtime.neighbour_channel_card = neighbour_channel_card
                    runtime.last_error = None
            except Exception as exc:  # noqa: BLE001
                async with runtime.lock:
                    runtime.last_error = str(exc)
                    runtime.neighbour_channel_card = {
                        "intra_text": "-",
                        "inter_text": "-",
                        "sample_ts": None,
                    }

            elapsed = time.time() - started
            interval = max(0.05, 1.0 / max(1.0, float(runtime.poll_hz)))
            wait_sec = max(0.0, interval - elapsed)
            await asyncio.sleep(wait_sec)
    finally:
        runtime.poll_running = False

