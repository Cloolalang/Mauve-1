from __future__ import annotations

import asyncio
import ipaddress
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from serial.tools import list_ports

from app.kpi_service import KpiRuntime, _parse_cgdcont, kpi_poll_loop
from app.serial_engine import SerialEngine
from app.at_modem_errors import combine_errors, describe_modem_send_result
from app.sim_usim_services import (
    SIM_EF_DESCRIPTIONS,
    SIM_INSPECTOR_LABEL_REFERENCE,
    label_usim_service,
)

APP_VERSION = "1.6"


def _serial_state_file_path() -> str:
    # Keep per-project state under backend/.state
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".state", "serial_last.json"))


def _load_last_serial_state() -> dict | None:
    path = _serial_state_file_path()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        return None


def _save_last_serial_state(port: str, baudrate: int) -> None:
    path = _serial_state_file_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {"port": str(port), "baudrate": int(baudrate)}
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception:
        # Non-fatal: app should continue even if state persistence fails.
        pass


_last_serial = _load_last_serial_state() or {}
DEFAULT_PORT = os.getenv("MD_SERIAL_PORT", str(_last_serial.get("port") or "COM49"))
DEFAULT_BAUD = int(os.getenv("MD_BAUDRATE", str(_last_serial.get("baudrate") or "115200")))

engine = SerialEngine(port=DEFAULT_PORT, baudrate=DEFAULT_BAUD)
kpi_runtime = KpiRuntime(poll_hz=float(os.getenv("MD_KPI_POLL_HZ", "2.0")))
_kpi_task: asyncio.Task[None] | None = None
_ws_push_task: asyncio.Task[None] | None = None
_lock_guard_task: asyncio.Task[None] | None = None
ws_clients: list[WebSocket] = []
_instance_lock_file = None
_desired_locks: dict[str, str] = {}
_desired_locks_lock = asyncio.Lock()
_lock_guard_paused: bool = False
_modem_exclusive_lock = asyncio.Lock()


def _exclusive_section_resume_kpi_snapshot() -> bool:
    """
    Whether to restart KPI after a modem exclusive section.
    Uses poll_running and the live asyncio task: avoids missing resume when poll_running is briefly stale
    (e.g. startup before kpi_poll_loop sets it) while a poll task is already scheduled.
    """
    global _kpi_task
    if kpi_runtime.poll_running:
        return True
    t = _kpi_task
    return t is not None and not t.done()


async def _stop_kpi_poll_task_hard() -> None:
    """Clear KPI polling and wait for the poll task to exit (no overlapping AT)."""
    global _kpi_task
    kpi_runtime.poll_running = False
    t = _kpi_task
    if t is not None and not t.done():
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
    _kpi_task = None


async def _pause_exclusive_modem_access() -> None:
    """Pause lock-guard AT traffic and stop KPI poll task until resume."""
    global _lock_guard_paused
    _lock_guard_paused = True
    await _stop_kpi_poll_task_hard()


def _resume_exclusive_modem_access(resume_kpi: bool) -> None:
    """Allow lock guard and optionally restart KPI polling."""
    global _lock_guard_paused, _kpi_task
    _lock_guard_paused = False
    if not resume_kpi:
        return
    kpi_runtime.poll_running = True
    if _kpi_task is None or _kpi_task.done():
        _kpi_task = asyncio.create_task(kpi_poll_loop(engine, kpi_runtime))


def _lock_file_path() -> str:
    return os.path.join(tempfile.gettempdir(), "mobiledriver_backend.lock")


def _acquire_instance_lock() -> None:
    global _instance_lock_file
    path = _lock_file_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    f = open(path, "a+", encoding="utf-8")
    try:
        if os.name == "nt":
            import msvcrt

            f.seek(0)
            # Lock a single byte for process-wide singleton behavior.
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        f.close()
        raise RuntimeError(
            "Another ModemTestDriver backend instance is already running. "
            "Stop the other instance before starting a new one."
        ) from exc
    _instance_lock_file = f


def _release_instance_lock() -> None:
    global _instance_lock_file
    if not _instance_lock_file:
        return
    try:
        if os.name == "nt":
            import msvcrt

            _instance_lock_file.seek(0)
            msvcrt.locking(_instance_lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(_instance_lock_file.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        _instance_lock_file.close()
    except Exception:
        pass
    _instance_lock_file = None


class SendAtBody(BaseModel):
    command: str = Field(min_length=1, description="AT command without CRLF")
    timeout_sec: float = Field(default=2.0, ge=0.2, le=30.0)


class ReopenBody(BaseModel):
    port: str = Field(min_length=1)
    baudrate: int = Field(default=115200, ge=300, le=4000000)


class KpiPollBody(BaseModel):
    poll_hz: float = Field(default=2.0, ge=0.1, le=5.0)


class CopsSetBody(BaseModel):
    mode: int = Field(description="AT+COPS mode (0=auto register, 2=deregister)")


class MnoSelectBody(BaseModel):
    profile: str = Field(description="One of: vodafone, vmo2, ee, h3g, auto")
    cops_manual_registration: int = Field(
        default=4,
        description="For named profiles: AT+COPS mode — 1 = manual (stay on selected PLMN); 4 = manual with automatic fallback (default). Ignored for profile=auto.",
    )
    deregister_before_apply: bool = Field(
        default=True,
        description="For named profiles: if True, send AT+COPS=2 (deregister from network) before manual PLMN selection so switches complete quickly on many routers/modems.",
    )


class DataGateBody(BaseModel):
    inhibit: bool = Field(description="True=inhibit packet data, False=allow packet data")
    password: str | None = Field(default=None, description="Required when inhibit=false")


class ApnSetBody(BaseModel):
    apn: str = Field(min_length=1, max_length=100, description="PDP APN string for AT+CGDCONT")
    cid: int = Field(default=1, ge=1, le=15, description="PDP context ID (typically 1)")
    pdp_type: str = Field(
        default="IP",
        description='PDP type passed to AT+CGDCONT, e.g. "IP", "IPV6", "IPV4V6"',
    )
    password: str | None = Field(default=None, description="Unlock password (same as data allow)")
    reactivate: bool = Field(
        default=True,
        description="After CGDCONT, reattach data (CGATT/QIACT when needed). Disable to only write CGDCONT (+QICSGP) if the context is inactive.",
    )


class LockSetBody(BaseModel):
    rat_mode: str | None = Field(default=None, description='QNWPREFCFG mode_pref, e.g. AUTO/LTE/NR5G')
    lte_band: str | None = Field(default=None, description='QNWPREFCFG lte_band string')
    nr5g_band: str | None = Field(default=None, description='QNWPREFCFG nr5g_band or nsa_nr5g_band string')
    nrdc_mode: int | None = Field(default=None, description='QNWPREFCFG nrdc_mode (0=off,1=on)')


class VolteTestBody(BaseModel):
    number: str = Field(min_length=3, max_length=40, description="Dial number, e.g. +447700900123")
    hold_sec: int = Field(default=10, ge=3, le=120, description="Call hold duration before hangup")
    password: str | None = Field(default=None, description="Unlock password (same as data allow password)")


class IperfTestBody(BaseModel):
    host: str = Field(default="iperf.as42831.net", min_length=1)
    port: int = Field(default=5361, ge=1, le=65535)
    duration_sec: int = Field(default=10, ge=1, le=300)
    direction: str = Field(default="download", description="download=server->client, upload=client->server")
    protocol: str = Field(default="tcp", description="Traffic mode. Currently only tcp is supported.")
    mobile_only: bool = Field(default=True, description="Bind iperf to mobile data interface/IP only.")
    bind_ip: str | None = Field(default=None, description="Optional local IPv4 to bind using iperf -B.")
    bitrate_limit_mbps: float | None = Field(
        default=None,
        gt=0,
        description="Optional TCP bitrate limit for iperf -b (Mbit/s), e.g. 10 → -b 10M.",
    )


class IcmpPingSweepBody(BaseModel):
    host: str = Field(default="8.8.8.8", min_length=1, max_length=253)
    count: int = Field(default=10, ge=1, le=100)
    bind_ipv4: str | None = Field(default=None, description="Windows: ping -S source IPv4 (optional).")


def _parse_icmp_ping_rtts_windows(text: str) -> list[float]:
    out: list[float] = []
    sub1 = re.compile(r"time\s*[<≤]\s*1\s*ms", re.I)
    rtt_eq = re.compile(r"time[=]\s*(\d+)\s*ms", re.I)
    rtt_angle = re.compile(r"time[=<]\s*(\d+)\s*ms", re.I)
    temps_fr = re.compile(r"temps[=]\s*(\d+)\s*ms", re.I)
    for line in text.splitlines():
        if sub1.search(line):
            out.append(0.5)
            continue
        ls = line.replace(" ", "").lower()
        if "time<1ms" in ls:
            out.append(0.5)
            continue
        m = rtt_eq.search(line) or rtt_angle.search(line)
        if m:
            out.append(float(m.group(1)))
            continue
        m2 = temps_fr.search(line)
        if m2:
            out.append(float(m2.group(1)))
    return out


def _parse_icmp_ping_rtts_unix(text: str) -> list[float]:
    out: list[float] = []
    rtt_re = re.compile(r"time=([\d.]+)\s*ms", re.I)
    for line in text.splitlines():
        m = rtt_re.search(line)
        if m:
            out.append(float(m.group(1)))
    return out


def _icmp_jitter_ms(rtts: list[float]) -> float | None:
    if len(rtts) < 2:
        return 0.0 if rtts else None
    diffs = [abs(rtts[i] - rtts[i - 1]) for i in range(1, len(rtts))]
    return round(sum(diffs) / len(diffs), 3)


def _discover_iperf_binary(explicit: str | None = None) -> str | None:
    candidates: list[str] = []
    if explicit:
        candidates.append(str(explicit).strip())
    env_bin = os.getenv("MD_IPERF_BIN", "").strip()
    if env_bin:
        candidates.append(env_bin)
    # Common local locations first (project root and backend root).
    here = os.path.abspath(os.path.dirname(__file__))
    candidates.extend(
        [
            os.path.abspath(os.path.join(here, "..", "vendor", "iperf", "iperf3.exe")),
            os.path.abspath(os.path.join(here, "..", "..", "iperf3.exe")),
            os.path.abspath(os.path.join(here, "..", "iperf3.exe")),
            os.path.abspath(os.path.join(here, "..", "..", "..", "iperf3.exe")),
            os.path.abspath(os.path.join(here, "..", "..", "..", "iperf3.1.1_32", "iperf3.exe")),
        ]
    )
    # Then PATH lookups.
    for exe in ("iperf3.exe", "iperf3", "iperf.exe", "iperf"):
        p = shutil.which(exe)
        if p:
            candidates.append(p)
    for c in candidates:
        if not c:
            continue
        if os.path.isfile(c):
            return c
        p2 = shutil.which(c)
        if p2:
            return p2
    # Last-resort search in common user locations on Windows.
    if os.name == "nt":
        search_roots = []
        for key in ("USERPROFILE", "HOMEDRIVE"):
            v = os.getenv(key, "").strip()
            if v:
                if key == "HOMEDRIVE":
                    hp = f"{v}\\"
                    if os.path.isdir(hp):
                        search_roots.append(hp)
                elif os.path.isdir(v):
                    search_roots.append(v)
        # De-duplicate while preserving order.
        uniq_roots: list[str] = []
        for r in search_roots:
            if r not in uniq_roots:
                uniq_roots.append(r)
        hit = _find_iperf_under_roots(uniq_roots)
        if hit:
            return hit
    return None


def _find_iperf_under_roots(roots: list[str], max_depth: int = 4) -> str | None:
    names = {"iperf3.exe", "iperf3", "iperf.exe", "iperf"}
    for root in roots:
        try:
            root_abs = os.path.abspath(root)
            base_depth = root_abs.rstrip("\\/").count(os.sep)
            for dirpath, dirnames, filenames in os.walk(root_abs):
                cur_depth = dirpath.rstrip("\\/").count(os.sep) - base_depth
                if cur_depth > max_depth:
                    dirnames[:] = []
                    continue
                low_files = {str(x).lower(): str(x) for x in filenames}
                for n in names:
                    key = n.lower()
                    if key in low_files:
                        return os.path.join(dirpath, low_files[key])
        except Exception:
            continue
    return None


def _compose_iperf_error(exit_code: int, stderr: str, stdout: str, parse_error: str | None) -> str:
    chunks = [f"iperf failed (exit={exit_code})"]
    s_err = (stderr or "").strip()
    s_out = (stdout or "").strip()
    if s_err:
        tail = s_err if len(s_err) < 6000 else f"{s_err[:6000]}..."
        chunks.append(tail)
    elif s_out and not s_out.startswith("{"):
        chunks.append(s_out[:4000])
    if parse_error:
        chunks.append(f"JSON parse: {parse_error}")
    return "\n".join(chunks)


def _extract_iperf_bits_per_second(report: dict, reverse: bool) -> tuple[float | None, str | None]:
    end = report.get("end") if isinstance(report, dict) else None
    if not isinstance(end, dict):
        return None, None
    sum_sent = end.get("sum_sent") if isinstance(end.get("sum_sent"), dict) else {}
    sum_received = end.get("sum_received") if isinstance(end.get("sum_received"), dict) else {}
    sum_any = end.get("sum") if isinstance(end.get("sum"), dict) else {}
    # In reverse mode the client is receiver, so prefer sum_received.
    pref = [("sum_received", sum_received), ("sum_sent", sum_sent)] if reverse else [("sum_sent", sum_sent), ("sum_received", sum_received)]
    pref.append(("sum", sum_any))
    for label, bucket in pref:
        bps = bucket.get("bits_per_second") if isinstance(bucket, dict) else None
        if isinstance(bps, (int, float)) and bps >= 0:
            return float(bps), label
    return None, None


def _parse_ipconfig_windows_blocks() -> list[dict[str, str]]:
    """Split ipconfig output into adapter blocks (header line without trailing colon + body)."""
    try:
        proc = subprocess.run(
            ["ipconfig"],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )
    except Exception:
        return []
    txt = str(proc.stdout or "")
    if not txt.strip():
        return []
    blocks: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for raw in txt.splitlines():
        line = raw.rstrip()
        s = line.strip()
        if not s:
            continue
        if (line == s) and s.endswith(":"):
            if current:
                blocks.append(current)
            current = {"header": s[:-1], "body": ""}
            continue
        if current is not None:
            current["body"] += s + "\n"
    if current:
        blocks.append(current)
    return blocks


def _enumerate_windows_ipv4_adapters() -> list[dict[str, str]]:
    """All IPv4 addresses Windows reports under adapter sections (for iperf -B)."""
    if os.name != "nt":
        return []
    blocks = _parse_ipconfig_windows_blocks()
    ip_re = re.compile(r"IPv4[^:]*:\s*([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)")
    rows: list[dict[str, str]] = []
    for b in blocks:
        header = str(b.get("header", "")).strip()
        body = str(b.get("body", ""))
        if not header:
            continue
        for m in ip_re.finditer(body):
            ip = m.group(1)
            try:
                ipaddress.IPv4Address(ip)
            except Exception:
                continue
            rows.append({"adapter": header, "ipv4": ip})

    mobile_kw = ("mobile", "cellular", "wwan", "rndis", "quectel", "usb ethernet", "internet sharing")

    def sort_key(r: dict[str, str]) -> tuple[int, str, str]:
        hay = f'{r.get("adapter", "")} {r.get("ipv4", "")}'.lower()
        mobile_first = 0 if any(k in hay for k in mobile_kw) else 1
        return (mobile_first, str(r.get("adapter", "")).lower(), str(r.get("ipv4", "")))

    rows.sort(key=sort_key)
    return rows


def _detect_mobile_bind_ip_windows() -> tuple[str | None, str | None]:
    if os.name != "nt":
        return None, None
    blocks = _parse_ipconfig_windows_blocks()
    if not blocks:
        return None, None

    keywords = (
        "mobile",
        "cellular",
        "wwan",
        "rndis",
        "quectel",
        "usb ethernet",
        "internet sharing",
    )
    ip_re = re.compile(r"IPv4[^:]*:\s*([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)")
    for b in blocks:
        header = str(b.get("header", ""))
        body = str(b.get("body", ""))
        hay = f"{header}\n{body}".lower()
        if not any(k in hay for k in keywords):
            continue
        m = ip_re.search(body)
        if not m:
            continue
        ip = m.group(1)
        try:
            ipaddress.IPv4Address(ip)
        except Exception:
            continue
        return ip, header
    return None, None


def _parse_cops_lines(lines: list[str]) -> dict:
    # Expected: +COPS: <mode>[,<format>[,<oper>[,<AcT>]]]
    for raw in lines:
        if not raw.startswith("+COPS:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        if not payload:
            return {}
        parts = [p.strip().strip('"') for p in payload.split(",")]

        def _to_int(s: str) -> int | None:
            try:
                return int(s)
            except Exception:  # noqa: BLE001
                return None

        out = {"mode": _to_int(parts[0])}
        if len(parts) > 1:
            out["format"] = _to_int(parts[1])
        if len(parts) > 2:
            out["operator"] = parts[2] or None
        if len(parts) > 3:
            out["act"] = _to_int(parts[3])
        return out
    return {}


def _sanitize_dial_number(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    keep = "+#*0123456789"
    s = "".join(ch for ch in s if ch in keep)
    if s.count("+") > 1:
        return ""
    if "+" in s and not s.startswith("+"):
        return ""
    return s


def _parse_clcc_lines(lines: list[str]) -> list[dict]:
    out: list[dict] = []

    def _to_int(v: str) -> int | None:
        try:
            return int(v)
        except Exception:  # noqa: BLE001
            return None

    for raw in lines:
        if not raw.startswith("+CLCC:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        parts = [p.strip().strip('"') for p in payload.split(",")]
        if len(parts) < 5:
            continue
        out.append(
            {
                "idx": _to_int(parts[0]),
                "dir": _to_int(parts[1]),
                "stat": _to_int(parts[2]),
                "mode": _to_int(parts[3]),
                "mpty": _to_int(parts[4]),
                "number": parts[5] if len(parts) > 5 and parts[5] else None,
                "type": _to_int(parts[6]) if len(parts) > 6 else None,
            }
        )
    return out


def _clcc_stat_label(stat: int | None) -> str:
    m = {
        0: "active",
        1: "held",
        2: "dialing",
        3: "alerting",
        4: "incoming",
        5: "waiting",
        6: "disconnect",
    }
    if stat is None:
        return "unknown"
    return m.get(stat, f"stat_{stat}")


def _parse_ceer(lines: list[str]) -> str | None:
    for raw in lines:
        if not raw.startswith("+CEER:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        return payload or None
    return None


def _parse_qnwinfo_line(lines: list[str]) -> dict:
    for raw in lines:
        if not raw.startswith("+QNWINFO:"):
            continue
        m = re.findall(r'"([^"]*)"', raw)
        if len(m) >= 4:
            return {"act": m[0], "operator": m[1], "band": m[2], "channel": m[3]}
    return {}


def _parse_cops_scan_lines(lines: list[str]) -> list[dict]:
    out: list[dict] = []
    seen: set[tuple[str, str | None]] = set()
    status_map = {
        0: "unknown",
        1: "available",
        2: "current",
        3: "forbidden",
    }
    # Typical tuple inside +COPS=?:
    # (2,"Vodafone UK","voda UK","23415",7)
    # (1,"EE","EE","23430",0)
    rx = re.compile(r'\((\d+),"([^"]*)","([^"]*)","([^"]*)"(?:,(\d+))?\)')
    for raw in lines:
        if "+COPS:" not in raw:
            continue
        for m in rx.finditer(raw):
            stat_i = int(m.group(1))
            long_name = m.group(2) or None
            short_name = m.group(3) or None
            plmn = m.group(4) or None
            act = int(m.group(5)) if m.group(5) is not None else None
            key = (plmn or "", long_name or short_name or "")
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "status": stat_i,
                    "status_label": status_map.get(stat_i, f"status_{stat_i}"),
                    "long_name": long_name,
                    "short_name": short_name,
                    "plmn": plmn,
                    "act": act,
                }
            )
    return out


def _parse_qnwprefcfg_value(lines: list[str], key: str) -> str | None:
    key_l = key.lower()
    for raw in lines:
        if not raw.startswith("+QNWPREFCFG:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        parts = [p.strip().strip('"') for p in payload.split(",")]
        if len(parts) < 2:
            continue
        if parts[0].lower() != key_l:
            continue
        return ",".join(parts[1:]).strip() or None
    return None


async def _read_lock_status() -> dict:
    keys = ["mode_pref", "lte_band", "nr5g_band", "nsa_nr5g_band", "nrdc_mode"]
    raw_map: dict[str, dict] = {}
    out: dict[str, str | None] = {}
    for k in keys:
        res = await engine.send_command(f'AT+QNWPREFCFG="{k}"', timeout_sec=4.0)
        raw_map[k] = res
        out[k] = _parse_qnwprefcfg_value(res.get("lines", []), k)
    return {"values": out, "raw": raw_map}


def _normalize_band_pref(value: str | None) -> str:
    if value is None:
        return ""
    s = str(value).strip().strip('"')
    if not s:
        return ""
    if s == "0":
        return "0"
    parts = [p.strip() for p in s.replace(",", ":").split(":") if p.strip()]
    nums: list[str] = []
    for p in parts:
        if p.isdigit():
            nums.append(str(int(p)))
        else:
            nums.append(p)
    # Keep deterministic ordering when numeric.
    if nums and all(x.isdigit() for x in nums):
        uniq_sorted = sorted({int(x) for x in nums})
        return ":".join(str(x) for x in uniq_sorted)
    return ":".join(nums)


def _normalize_lock_value(key: str, value: str | None) -> str:
    if value is None:
        return ""
    s = str(value).strip().strip('"')
    if key == "mode_pref":
        return s.upper()
    if key in {"lte_band", "nr5g_band", "nsa_nr5g_band"}:
        return _normalize_band_pref(s)
    if key == "nrdc_mode":
        if not s:
            return ""
        if s in {"true", "TRUE", "on", "ON"}:
            return "1"
        try:
            return "1" if int(s) else "0"
        except ValueError:
            return s
    return s


def _lock_value_matches(key: str, want: str, current: dict[str, str | None]) -> bool:
    def _band_match_all_ok(wanted: str, got: str) -> bool:
        # Modems often echo "all bands" as either literal 0 or an expanded list.
        if wanted == "0":
            return bool(got)
        return wanted == got

    if key == "nr5g_band":
        got_nr = _normalize_lock_value("nr5g_band", current.get("nr5g_band"))
        got_nsa = _normalize_lock_value("nsa_nr5g_band", current.get("nsa_nr5g_band"))
        return _band_match_all_ok(want, got_nr) or _band_match_all_ok(want, got_nsa)
    if key in {"lte_band", "nsa_nr5g_band"}:
        got = _normalize_lock_value(key, current.get(key))
        return _band_match_all_ok(want, got)
    got = _normalize_lock_value(key, current.get(key))
    return want == got


async def _apply_lock_requests(requested: dict[str, str]) -> dict[str, dict]:
    set_results: dict[str, dict] = {}
    if "mode_pref" in requested:
        rat = requested["mode_pref"]
        set_results["mode_pref"] = await engine.send_command(f'AT+QNWPREFCFG="mode_pref",{rat}', timeout_sec=8.0)
    if "lte_band" in requested:
        band = requested["lte_band"]
        set_results["lte_band"] = await engine.send_command(f'AT+QNWPREFCFG="lte_band",{band}', timeout_sec=8.0)
    if "nr5g_band" in requested:
        band = requested["nr5g_band"]
        set_results["nr5g_band"] = await engine.send_command(f'AT+QNWPREFCFG="nr5g_band",{band}', timeout_sec=8.0)
        final_nr = str(set_results["nr5g_band"].get("final", "")).upper()
        if final_nr == "OK":
            set_results["nsa_nr5g_band"] = await engine.send_command(
                f'AT+QNWPREFCFG="nsa_nr5g_band",{band}', timeout_sec=8.0
            )
    if "nrdc_mode" in requested:
        mode = requested["nrdc_mode"]
        set_results["nrdc_mode"] = await engine.send_command(f'AT+QNWPREFCFG="nrdc_mode",{mode}', timeout_sec=8.0)
    return set_results


async def _lock_guard_loop() -> None:
    while True:
        await asyncio.sleep(12.0)
        if _lock_guard_paused:
            continue
        async with _desired_locks_lock:
            wanted = dict(_desired_locks)
        if not wanted:
            continue
        try:
            lock_status = await _read_lock_status()
            current = lock_status["values"]
            drift: dict[str, str] = {}
            for key, want in wanted.items():
                if not _lock_value_matches(key, want, current):
                    drift[key] = want
            if drift:
                await _apply_lock_requests(drift)
        except Exception:
            # Non-fatal: guard should keep trying.
            continue


def _parse_cgatt_attached(lines: list[str]) -> bool | None:
    for raw in lines:
        if not raw.startswith("+CGATT:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        m = re.search(r"-?\d+", payload)
        if not m:
            continue
        return int(m.group(0)) == 1
    return None


def _parse_cereg_stat(lines: list[str]) -> int | None:
    for raw in lines:
        if not raw.startswith("+CEREG:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        nums = [int(x) for x in re.findall(r"-?\d+", payload)]
        if not nums:
            continue
        # +CEREG: <stat> or +CEREG: <n>,<stat>[,...]
        return nums[0] if len(nums) == 1 else nums[1]
    return None


def _parse_qiact_contexts(lines: list[str]) -> list[dict]:
    out: list[dict] = []
    for raw in lines:
        if not raw.startswith("+QIACT:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        m = re.match(r'(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*"([^"]*)"', payload)
        if not m:
            continue
        out.append(
            {
                "cid": int(m.group(1)),
                "active": int(m.group(3)) == 1,
                "ip": m.group(4) or None,
            }
        )
    return out


MNO_PROFILES: dict[str, dict[str, str | None]] = {
    # UK profiles requested by user; values are numeric PLMN for COPS format 2.
    "vodafone": {"label": "Vodafone", "plmn": "23415"},
    "vmo2": {"label": "VMO2", "plmn": "23410"},
    "ee": {"label": "EE", "plmn": "23430"},
    "h3g": {"label": "H3G", "plmn": "23420"},
    "auto": {"label": "Auto", "plmn": None},
}
DATA_GATE_UNLOCK_PASSWORD = "nacelle"

_ALLOWED_APN_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_")


def _sanitize_apn_for_at(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="APN must not be empty.")
    if len(s) > 100:
        raise HTTPException(status_code=400, detail="APN exceeds maximum length.")
    if any(ch not in _ALLOWED_APN_CHARS for ch in s):
        raise HTTPException(
            status_code=400,
            detail="APN may only contain letters, digits, dot, hyphen, and underscore.",
        )
    return s


def _normalize_cgdcont_pdp_type(raw: str | None) -> str:
    u = str(raw or "IP").strip().upper()
    if u not in {"IP", "IPV6", "IPV4V6"}:
        raise HTTPException(
            status_code=400,
            detail='pdp_type must be one of: "IP", "IPV6", "IPV4V6".',
        )
    return u


UK_LTE_SCAN_BANDS = "1:3:7:8:20:28:32:38"
UK_NR_SCAN_BANDS = "1:3:8:28:78"
MNO_OPERATOR_ALIASES: dict[str, set[str]] = {
    # Common long/short names seen from AT+COPS? format 0 responses.
    "vodafone": {"VODAFONE", "VODAFONE UK", "VODA UK"},
    "vmo2": {"O2", "O2-UK", "TELEFONICA UK", "VMO2"},
    "ee": {"EE", "EE LIMITED", "EE LTD", "TMOBILE UK", "T-MOBILE UK", "ORANGE"},
    "h3g": {"3", "3 UK", "H3G", "THREE", "THREE UK", "HUTCHISON 3G"},
}


def _first_payload_line(lines: list[str]) -> str | None:
    for raw in lines:
        s = str(raw or "").strip()
        if not s:
            continue
        up = s.upper()
        if up in {"OK", "ERROR"}:
            continue
        if up.startswith("AT+"):
            continue
        return s
    return None


def _normalize_operator_token(raw: str | None) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(raw or "").strip().upper()).strip()


def _profile_key_from_cops_operator(operator: str | None) -> str | None:
    op = str(operator or "").strip()
    if not op:
        return None
    for key, cfg in MNO_PROFILES.items():
        plmn = str(cfg.get("plmn") or "").strip()
        if plmn and op == plmn:
            return key
    op_norm = _normalize_operator_token(op)
    if not op_norm:
        return None
    for key, aliases in MNO_OPERATOR_ALIASES.items():
        if op_norm in {_normalize_operator_token(a) for a in aliases}:
            return key
    return None


def _mno_label_for_numeric_plmn(plmn: str | None) -> str | None:
    """Return profile label (e.g. EE) for a numeric PLMN like 23430, or None."""
    p = str(plmn or "").strip()
    if not p:
        return None
    for _name, cfg in MNO_PROFILES.items():
        if str(cfg.get("plmn") or "") == p:
            lab = cfg.get("label")
            return str(lab).strip() if lab else None
    return None


def _parse_qspn(lines: list[str]) -> str | None:
    for raw in lines:
        if not raw.startswith("+QSPN:"):
            continue
        m = re.findall(r'"([^"]*)"', raw)
        # Common shape: +QSPN: "<disp_cond>","<spn_disp_cond>","<spn>",...
        if len(m) >= 3:
            return m[2] or None
        for item in m:
            if item and not item.isdigit():
                return item
    return None


def _parse_cpol(lines: list[str]) -> list[dict]:
    out: list[dict] = []
    for raw in lines:
        if not raw.startswith("+CPOL:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        parts = [p.strip().strip('"') for p in payload.split(",")]
        if len(parts) < 3:
            continue
        try:
            idx = int(parts[0])
        except Exception:  # noqa: BLE001
            idx = None
        try:
            fmt = int(parts[1])
        except Exception:  # noqa: BLE001
            fmt = None
        oper = parts[2] or None
        out.append({"index": idx, "format": fmt, "operator": oper, "raw": payload})
    return out


def _parse_crsm_hex(lines: list[str]) -> dict:
    for raw in lines:
        if not raw.startswith("+CRSM:"):
            continue
        payload = raw.split(":", 1)[1].strip()
        nums = re.findall(r"-?\d+", payload)
        sw1 = int(nums[0]) if len(nums) >= 1 else None
        sw2 = int(nums[1]) if len(nums) >= 2 else None
        m_hex = re.search(r'"([0-9A-Fa-f]*)"', payload)
        hex_data = m_hex.group(1).upper() if m_hex else ""
        return {"sw1": sw1, "sw2": sw2, "hex": hex_data}
    return {"sw1": None, "sw2": None, "hex": ""}


def _decode_plmn_bcd(raw6: str) -> dict | None:
    try:
        b = bytes.fromhex(raw6)
    except Exception:  # noqa: BLE001
        return None
    if len(b) != 3:
        return None
    d = [
        b[0] & 0x0F,  # mcc1
        (b[0] >> 4) & 0x0F,  # mcc2
        b[1] & 0x0F,  # mcc3
        b[2] & 0x0F,  # mnc1
        (b[2] >> 4) & 0x0F,  # mnc2
        (b[1] >> 4) & 0x0F,  # mnc3 (or F filler)
    ]
    if all(x == 0xF for x in d):
        return None
    if any(x > 9 for x in d[:5]):
        return None
    mcc = f"{d[0]}{d[1]}{d[2]}"
    if d[5] == 0xF:
        mnc = f"{d[3]}{d[4]}"
    elif d[5] <= 9:
        mnc = f"{d[3]}{d[4]}{d[5]}"
    else:
        return None
    return {"mcc": mcc, "mnc": mnc, "plmn": f"{mcc}-{mnc}"}


def _decode_plmn_file(hex_data: str, with_act: bool) -> list[dict]:
    s = re.sub(r"[^0-9A-Fa-f]", "", hex_data or "").upper()
    rec_len = 10 if with_act else 6
    out: list[dict] = []
    if rec_len <= 0:
        return out
    n = len(s) // rec_len
    for i in range(n):
        rec = s[i * rec_len : (i + 1) * rec_len]
        if not rec or all(ch == "F" for ch in rec):
            continue
        core = _decode_plmn_bcd(rec[:6])
        if not core:
            continue
        if with_act and len(rec) >= 10:
            core["act_hex"] = rec[6:10]
        out.append(core)
    return out


def _decode_mnc_len_from_ad(hex_data: str) -> int | None:
    s = re.sub(r"[^0-9A-Fa-f]", "", hex_data or "").upper()
    if len(s) < 8:
        return None
    # For many USIM profiles, byte 4 lower nibble stores MNC length (2 or 3).
    b4 = int(s[6:8], 16)
    mnc_len = b4 & 0x0F
    return mnc_len if mnc_len in (2, 3) else None


def _decode_hplmn_timer_minutes(hex_data: str) -> int | None:
    s = re.sub(r"[^0-9A-Fa-f]", "", hex_data or "").upper()
    if len(s) < 2:
        return None
    raw = int(s[:2], 16)
    if raw in (0x00, 0xFF):
        return None
    # 3GPP: value in units of 6 minutes.
    return raw * 6


def _decode_ust_enabled_services(hex_data: str) -> list[int]:
    s = re.sub(r"[^0-9A-Fa-f]", "", hex_data or "").upper()
    out: list[int] = []
    if len(s) < 2:
        return out
    data = bytes.fromhex(s)
    for i, b in enumerate(data):
        for bit in range(8):
            if b & (1 << bit):
                out.append(i * 8 + bit + 1)
    return out


async def _broadcast_kpi_loop() -> None:
    while True:
        await asyncio.sleep(0.5)
        if not ws_clients:
            continue
        async with kpi_runtime.lock:
            payload = json.dumps(
                {
                    "sample": kpi_runtime.snapshot,
                    "poll_running": kpi_runtime.poll_running,
                    "poll_hz": kpi_runtime.poll_hz,
                    "last_error": kpi_runtime.last_error,
                }
            )
        dead: list[WebSocket] = []
        for ws in ws_clients:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in ws_clients:
                ws_clients.remove(ws)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _kpi_task, _ws_push_task, _lock_guard_task
    _acquire_instance_lock()
    try:
        await engine.start()
        st = await engine.status()
        if st.get("serial_open"):
            _save_last_serial_state(str(st.get("port") or engine.port), int(st.get("baudrate") or engine.baudrate))
        _kpi_task = asyncio.create_task(kpi_poll_loop(engine, kpi_runtime))
        _ws_push_task = asyncio.create_task(_broadcast_kpi_loop())
        _lock_guard_task = asyncio.create_task(_lock_guard_loop())
        yield
        kpi_runtime.poll_running = False
        if _kpi_task:
            _kpi_task.cancel()
        if _ws_push_task:
            _ws_push_task.cancel()
        if _lock_guard_task:
            _lock_guard_task.cancel()
        await engine.stop()
    finally:
        _release_instance_lock()


app = FastAPI(
    title="5G ModemTestDriver",
    version="1.6.0",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse)
async def home() -> HTMLResponse:
    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>5G ModemTestDriver · v__APP_VERSION__</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background: #111; color: #f3f3f3; }
    h1 { margin: 0 0 12px 0; font-size: 22px; }
    .grid { display: grid; gap: 10px; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); }
    .card { border: 1px solid #333; border-radius: 10px; padding: 12px; background: #1b1b1b; }
    .card-compact-tile {
      align-self: start;
      max-width: min(264px, 100%);
      width: 100%;
      aspect-ratio: 1 / 1;
      overflow-x: hidden;
      overflow-y: auto;
      padding: 10px;
      box-sizing: border-box;
    }
    .card-compact-tile .row { margin-top: 6px; }
    .label { color: #9aa0a6; font-size: 12px; }
    .value { font-size: 20px; font-weight: 700; margin-top: 4px; }
    .row { display: flex; justify-content: space-between; margin-top: 8px; }
    .mono { font-family: Consolas, monospace; font-size: 12px; white-space: pre-wrap; word-break: break-word; }
    .ok { color: #39d353; }
    .warn { color: #ffcc66; }
    .err { color: #ff7070; }
  </style>
</head>
<body>
  <h1 style="display:flex; align-items:baseline; gap:10px; flex-wrap:wrap;">
    5G ModemTestDriver
    <span class="label" style="font-size:13px; font-weight:600; letter-spacing:0.02em;">v__APP_VERSION__</span>
  </h1>
  <div class="label">Live modem snapshot from COM AT engine</div>
  <div id="status" class="label" style="margin-top:8px;">Connecting...</div>
  <div style="margin-top:10px; display:flex; gap:14px; align-items:center; flex-wrap:wrap;">
    <button id="btn-clear-charts">Clear All Charts</button>
    <button id="btn-chart-gap-mode">Time-roll gaps: OFF</button>
    <label style="display:flex; align-items:center; gap:6px; font-size:12px; color:#9aa0a6;">
      Chart window
      <select id="chart-window-select" style="background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:3px 6px;">
        <option value="60">60s</option>
        <option value="120">2m</option>
        <option value="300">5m</option>
        <option value="600">10m</option>
        <option value="900">15m</option>
        <option value="1800">30m</option>
        <option value="3600">60m</option>
      </select>
    </label>
    <label style="display:flex; align-items:center; gap:6px; font-size:12px; color:#9aa0a6;">
      <input id="rf-smooth-toggle" type="checkbox" />
      RF smoothing (rolling avg, last 10 samples)
    </label>
  </div>

  <div class="grid" style="margin-top:12px;">
    <div class="card card-compact-tile">
      <div class="label">Serial Port</div>
      <div class="row"><span class="label">Current</span><span id="serial-current">-</span></div>
      <div class="row"><span class="label">Open</span><span id="serial-open">-</span></div>
      <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
        <select id="serial-port-select" style="min-width:180px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:4px 6px;"></select>
        <button id="btn-serial-refresh">Refresh Ports</button>
        <button id="btn-serial-autopick">Auto-select AT Port</button>
        <button id="btn-serial-reconnect">Reconnect</button>
        <button id="btn-modem-reset">Reset Modem</button>
      </div>
      <div id="serialmsg" class="label" style="margin-top:8px;">-</div>
    </div>

    <div class="card">
      <div class="label">Access / Operator</div>
      <div class="row"><span class="label">Operator</span><span id="operator">-</span></div>
      <div class="row"><span class="label">Registration</span><span id="access-eps-scope">-</span></div>
      <div class="row"><span class="label">Modem FW</span><span id="modemfw">-</span></div>
      <div class="row"><span class="label">Updated</span><span id="updated">-</span></div>
      <div class="label" style="margin-top:12px;">Registration Control (COPS)</div>
      <div class="row"><span class="label">Mode</span><span id="copsmode">-</span></div>
      <div class="row"><span class="label">COPS operator</span><span id="copsoperator">-</span></div>
      <div class="row"><span class="label">AcT</span><span id="copsact">-</span></div>
      <div style="margin-top:8px;">
        <label style="display:flex; align-items:center; gap:8px;">
          <input id="cops-scan-uk-only" type="checkbox" checked />
          <span>UK-only scan bands (LTE+NR)</span>
        </label>
      </div>
      <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
        <button id="btn-cops-read">Read COPS</button>
        <button id="btn-cops-scan">Scan Operators</button>
        <button id="btn-cops-auto">Auto Register</button>
        <button id="btn-cops-dereg">Deregister</button>
      </div>
      <div class="label" style="margin-top:8px;">Scan result</div>
      <div id="copsscan" class="mono">-</div>
      <div id="copsmsg" class="label" style="margin-top:8px;">-</div>
    </div>

    <div class="card">
      <div class="label">Data Service KPI</div>
      <div class="row"><span class="label">APN</span><span id="ds-apn">-</span></div>
      <div class="row"><span class="label">PDP Contexts (active/total)</span><span id="ds-pdp">-</span></div>
      <div class="row"><span class="label">CID1</span><span id="ds-cid1">-</span></div>
      <div class="row"><span class="label">CID1 IP</span><span id="ds-ip">-</span></div>
      <div class="row"><span class="label">Packet Attach</span><span id="ds-attach">-</span></div>
      <div class="row"><span class="label">EPS Registration</span><span id="ds-reg">-</span></div>
      <div class="row"><span class="label">USB data stack</span><span id="ds-usbnet">-</span></div>
      <div class="row"><span class="label">Netdev status</span><span id="ds-netdev">-</span></div>
      <div style="margin-top:12px;">
        <div class="label">Set APN (AT+CGDCONT)</div>
        <div style="margin-top:6px; display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
          <input id="ds-apn-set" placeholder="e.g. internet" style="flex:1; min-width:160px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;" />
          <select id="ds-pdp-type" style="background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;">
            <option value="IP" selected>IP</option>
            <option value="IPV4V6">IPV4V6</option>
            <option value="IPV6">IPV6</option>
          </select>
          <button id="btn-ds-apn-apply" type="button">Apply APN</button>
        </div>
        <div style="margin-top:8px;">
          <div class="label">Unlock password (same as Allow Data)</div>
          <input id="ds-apn-password" type="password" placeholder="Enter password" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
        </div>
        <div style="margin-top:8px; display:flex; align-items:flex-start; gap:8px;">
          <input id="ds-apn-reactivate" type="checkbox" checked />
          <span class="label" style="flex:1; font-size:11px;">
            Reactivate packet data after change (AT+QIACT). Uncheck to only write APN; use Allow Data to reconnect. Deactivating PDP can briefly drop the USB data path (may look like a modem reset).
          </span>
        </div>
        <div id="ds-apn-msg" class="label" style="margin-top:8px;">-</div>
      </div>
      <div id="ds-warn" class="label" style="margin-top:8px;">-</div>
    </div>

    <div class="card">
      <div class="label">Primary Cell</div>
      <div class="row"><span class="label">RAT</span><span id="rat">-</span></div>
      <div class="row"><span class="label">State</span><span id="state">-</span></div>
      <div class="row"><span class="label">Band</span><span id="band">-</span></div>
      <div class="row"><span class="label">DL/UL BW</span><span id="bwpair">-</span></div>
      <div class="row"><span class="label">EARFCN/PCI</span><span id="earfcnpci">-</span></div>
      <div class="row"><span class="label">Cell ID</span><span id="cellid">-</span></div>
      <div class="label" style="margin-top:10px;">Primary cell RF KPI</div>
      <div class="row"><span class="label">RSRP</span><span id="rsrp">-</span></div>
      <div class="row"><span class="label">RSRQ</span><span id="rsrq">-</span></div>
      <div class="row"><span class="label">SINR (QSINR PRX)</span><span id="sinr">-</span></div>
      <div class="row"><span class="label">RSSI</span><span id="rssi">-</span></div>
      <div class="row"><span class="label">Primary cell intra-cell dominance</span><span id="dominance">-</span></div>
      <div class="label" style="margin-top:10px;">Neighbour Cells RF KPI</div>
      <div class="row"><span class="label">1st strongest neighbour RSRP</span><span id="nrsrp1">-</span></div>
      <div class="row"><span class="label">1st strongest neighbour PCI</span><span id="npci1">-</span></div>
      <div class="row"><span class="label">1st strongest neighbour EARFCN (intra)</span><span id="nearfcn1">-</span></div>
    </div>

    <div class="card">
      <div class="label">Mobility · LTE carrier re-selection (camped and RRC connected)</div>
      <div class="row" style="margin-top:8px;"><span class="label">Intra-freq PCI re-selections / min</span><span id="idle-pci-rate">-</span></div>
      <div class="row"><span class="label">Primary EARFCN re-selections / min</span><span id="idle-earfcn-rate">-</span></div>
    </div>

    <div class="card">
      <div class="label">RSRP Trend (dBm)</div>
      <canvas id="rsrpchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">RSRQ Trend (dB)</div>
      <canvas id="rsrqchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">SNIR Trend (dB)</div>
      <canvas id="sinrchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">RSSI Trend (dBm)</div>
      <canvas id="rssichart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">Intra-cell Dominance Trend (dB)</div>
      <canvas id="dominancechart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">Bandwidth Trend (DL BW)</div>
      <canvas id="bwchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">State Trend</div>
      <canvas id="statechart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">Band Trend</div>
      <canvas id="bandchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">PCI Trend</div>
      <canvas id="pcichart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">Neighbour RSRP Trend (dBm)</div>
      <canvas id="nbrsrpchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">Neighbour PCI Trend</div>
      <canvas id="nbpcichart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">Primary Carrier re-selection rate — LTE PCell (camped and connected, /min)</div>
      <canvas id="carrier-resel-chart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">Per-Chain Metrics</div>
      <div id="chains" class="mono">-</div>
    </div>

    <div class="card">
      <div class="label">Roaming MNO + Data Gate</div>
      <div class="row"><span class="label">Selected profile</span><span id="mno-selected">-</span></div>
      <div class="row"><span class="label">Current PLMN</span><span id="mno-current-plmn">-</span></div>
      <div style="margin-top:8px;">
        <div class="label">MNO profile:</div>
        <select id="mno-select" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;">
          <option value="vodafone">Vodafone</option>
          <option value="vmo2">VMO2</option>
          <option value="ee">EE</option>
          <option value="h3g">H3G</option>
          <option value="auto">Auto</option>
        </select>
      </div>
      <div style="margin-top:8px;">
        <div class="label">Manual COPS mode (VF / VM / EE / three only)</div>
        <select id="mno-cops-mode" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" title="Non-steered roaming: mode 1 holds the PLMN; mode 4 can fall back automatically.">
          <option value="4" selected>4 — Manual PLMN + automatic fallback</option>
          <option value="1">1 — Manual PLMN hold (often better when roaming)</option>
        </select>
        <div style="margin-top:8px; display:flex; align-items:flex-start; gap:8px;">
          <input id="mno-skip-dereg" type="checkbox" />
          <span class="label" style="flex:1; font-size:11px;">
            Skip pre-step deregister (AT+COPS=2). Leaving this unchecked sends deregister before manual PLMN — usually needed for fast, reliable switches.
          </span>
        </div>
      </div>
      <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
        <button id="btn-mno-read">Read MNO</button>
        <button id="btn-mno-apply">Apply MNO</button>
      </div>
      <div class="row" style="margin-top:10px;"><span class="label">Data gate</span><span id="data-gate-state">-</span></div>
      <div class="row"><span class="label">Active PDP contexts</span><span id="data-gate-active">-</span></div>
      <div style="margin-top:8px;">
        <div class="label">Unlock password (Allow Data):</div>
        <input id="data-gate-password" type="password" placeholder="Enter password" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
        <button id="btn-data-inhibit">Inhibit Data</button>
        <button id="btn-data-allow">Allow Data</button>
      </div>
      <div id="mnomsg" class="label" style="margin-top:8px;">-</div>
    </div>

    <div class="card">
      <div class="label">RAT / Band Lock (QNWPREFCFG)</div>
      <div class="row"><span class="label">RAT mode</span><span id="lock-ratmode">-</span></div>
      <div class="row"><span class="label">LTE bands</span><span id="lock-lteband">-</span></div>
      <div class="row"><span class="label">CA policy</span><span id="lock-ca">-</span></div>
      <div class="row"><span class="label">NR bands</span><span id="lock-nrband">-</span></div>
      <div class="row"><span class="label">NRDC</span><span id="lock-nrdc">-</span></div>
      <div style="margin-top:10px;">
        <div class="label">Set RAT mode (AUTO/LTE/NR5G):</div>
        <select id="input-ratmode" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;">
          <option value="">(no change)</option>
          <option value="AUTO">AUTO</option>
          <option value="LTE">LTE</option>
          <option value="NR5G">NR5G</option>
          <option value="LTE:NR5G">LTE:NR5G</option>
          <option value="NR5G:LTE">NR5G:LTE</option>
        </select>
      </div>
      <div style="margin-top:8px;">
        <div class="label">CA switch (LTE):</div>
        <label style="display:flex; align-items:center; gap:8px; margin-top:4px;">
          <input id="input-ca-enable" type="checkbox" checked />
          <span>CA ON (multi/all LTE bands)</span>
        </label>
      </div>
      <div style="margin-top:8px;">
        <div class="label">CA ON bands (use 0 for all):</div>
        <input id="input-ca-on-bands" placeholder="optional (e.g. 3:20 or 0)" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:8px;">
        <div class="label">CA OFF single LTE band (example: 8):</div>
        <input id="input-ca-single-band" placeholder="8" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:8px;">
        <div class="label">Set LTE bands (example: 1:3:7:20 or 0 for all):</div>
        <input id="input-lteband" placeholder="optional override" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:8px;">
        <div class="label">Set NR bands (example: 78:77 or 0 for all):</div>
        <input id="input-nrband" placeholder="0" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:8px;">
        <div class="label">NRDC switch:</div>
        <label style="display:flex; align-items:center; gap:8px; margin-top:4px;">
          <input id="input-nrdc-enable" type="checkbox" />
          <span>NRDC ON</span>
        </label>
      </div>
      <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
        <button id="btn-lock-read">Read Locks</button>
        <button id="btn-lock-set">Apply Locks</button>
      </div>
      <div id="lockmsg" class="label" style="margin-top:8px;">-</div>
    </div>

    <div class="card">
      <div class="label">SIM High-Level + PLMN Inspector</div>
      <div class="row"><span class="label">IMEI</span><span id="sim-imei">-</span></div>
      <div class="row"><span class="label">IMSI</span><span id="sim-imsi">-</span></div>
      <div class="row"><span class="label">SPN</span><span id="sim-spn">-</span></div>
      <div class="row"><span class="label">Current COPS</span><span id="sim-cops">-</span></div>
      <div class="row"><span class="label">Preferred PLMN entries</span><span id="sim-cpol-count">-</span></div>
      <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
        <button id="btn-sim-high-read">Read SIM High-Level</button>
        <button id="btn-sim-inspect-read">Read SIM Inspector</button>
      </div>
      <div id="simmsg" class="label" style="margin-top:8px;">-</div>
      <pre id="siminspect" class="mono" style="max-height:420px; overflow:auto; margin-top:8px;">-</pre>
    </div>

    <div class="card">
      <div class="label">VoLTE Call Test</div>
      <div class="row"><span class="label">Hold time</span><span>10 seconds</span></div>
      <div style="margin-top:8px;">
        <div class="label">Dial number:</div>
        <input id="volte-number" placeholder="+447700900123" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:8px;">
        <div class="label">Unlock password:</div>
        <input id="volte-password" type="password" placeholder="Enter password" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
        <button id="btn-volte-test">Run VoLTE Call Test</button>
      </div>
      <div id="volte-msg" class="label" style="margin-top:8px;">-</div>
      <pre id="volte-trace" class="mono" style="max-height:140px; overflow:auto; margin-top:8px;">-</pre>
    </div>

    <div class="card">
      <div class="label">Iperf3 Test</div>
      <div style="margin-top:8px;">
        <div class="label">Endpoint host:</div>
        <input id="iperf-host" value="iperf.as42831.net" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px; margin-top:8px;">
        <div>
          <div class="label">Port:</div>
          <input id="iperf-port" type="number" min="1" max="65535" value="5361" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
        </div>
        <div>
          <div class="label">Duration (s):</div>
          <input id="iperf-duration" type="number" min="1" max="300" value="10" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
        </div>
      </div>
      <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px; margin-top:8px;">
        <div>
          <div class="label">Direction:</div>
          <select id="iperf-direction" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;">
            <option value="both" selected>Download then Upload</option>
            <option value="download">Download (-R)</option>
            <option value="upload">Upload</option>
          </select>
        </div>
        <div>
          <div class="label">Protocol:</div>
          <select id="iperf-protocol" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;">
            <option value="tcp" selected>TCP</option>
          </select>
        </div>
      </div>
      <div style="margin-top:8px;">
        <div class="label">Bind interface / IPv4 (iperf -B)</div>
        <div style="display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-top:4px;">
          <select id="iperf-bind-select" style="flex:1; min-width:220px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;">
            <option value="auto" selected>Auto-detect mobile broadband IPv4</option>
            <option value="manual">Manual IPv4…</option>
          </select>
          <button type="button" id="btn-iperf-refresh-ifaces">Refresh interfaces</button>
        </div>
        <input id="iperf-bind-ip" placeholder="Enter IPv4 when Manual is selected" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:8px; display:none;" />
      </div>
      <div style="margin-top:8px;">
        <div class="label">Speed limit (Mbit/s, optional — maps to iperf -b):</div>
        <input id="iperf-speed-limit" type="number" min="0" step="0.1" placeholder="Unlimited" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
        <button id="btn-iperf-test">Run Iperf3 Test</button>
      </div>
      <div id="iperf-msg" class="label" style="margin-top:8px;">-</div>
      <pre id="iperf-trace" class="mono" style="max-height:160px; overflow:auto; margin-top:8px;">-</pre>
    </div>

    <div class="card">
      <div class="label">Iperf Latest Gauges</div>
      <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px; margin-top:8px;">
        <div>
          <div class="label">DL speed</div>
          <canvas id="iperf-dl-gauge" width="220" height="130" style="width:100%; height:130px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
        </div>
        <div>
          <div class="label">UL speed</div>
          <canvas id="iperf-ul-gauge" width="220" height="130" style="width:100%; height:130px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
        </div>
      </div>
      <div class="label" id="iperf-gauge-note" style="margin-top:6px;">Latest results (Mbps). Scale auto-ranges from recent max.</div>
    </div>

    <div class="card">
      <div class="label">Iperf Throughput Trend (Mbps)</div>
      <canvas id="iperfchart" width="420" height="180" style="width:100%; height:180px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card">
      <div class="label">ICMP Ping Sweep (host OS)</div>
      <div style="margin-top:8px;">
        <div class="label">Target host:</div>
        <input id="ph-host" value="8.8.8.8" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:8px;">
        <div class="label">Count:</div>
        <input id="ph-count" type="number" min="1" max="100" value="10" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:8px;">
        <div class="label">Bind source IPv4 (Windows ping -S)</div>
        <div style="display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-top:4px;">
          <select id="ph-bind-select" style="flex:1; min-width:220px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;">
            <option value="auto" selected>Auto (OS default route)</option>
            <option value="manual">Manual IPv4…</option>
          </select>
          <button type="button" id="btn-ph-refresh-ifaces">Refresh interfaces</button>
        </div>
        <input id="ph-bind-ip" placeholder="Used when Manual is selected" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:8px; display:none;" />
      </div>
      <div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
        <button id="btn-ph-run" type="button">Run ICMP Ping Sweep</button>
      </div>
      <div class="row" style="margin-top:10px;">
        <span class="label">Repeat sweep every 15 s</span>
        <label style="display:flex; align-items:center; gap:8px; cursor:pointer;">
          <input id="ph-repeat-toggle" type="checkbox" />
          <span id="ph-repeat-state">OFF</span>
        </label>
      </div>
      <div id="ph-msg" class="label" style="margin-top:8px;">-</div>
      <pre id="ph-trace" class="mono" style="max-height:140px; overflow:auto; margin-top:8px;">-</pre>
    </div>

    <div class="card">
      <div class="label">ICMP Ping Gauges</div>
      <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px; margin-top:8px;">
        <div>
          <div class="label">Avg RTT</div>
          <canvas id="ph-lat-gauge" width="220" height="130" style="width:100%; height:130px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
        </div>
        <div>
          <div class="label">Jitter</div>
          <canvas id="ph-jit-gauge" width="220" height="130" style="width:100%; height:130px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
        </div>
      </div>
      <div class="label" id="ph-gauge-note" style="margin-top:6px;">Latest sweep (ms). Scale from recent runs.</div>
    </div>

    <div class="card">
      <div class="label">ICMP Ping Trend (ms)</div>
      <canvas id="ph-sweep-chart" width="420" height="180" style="width:100%; height:180px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 60s</div>
    </div>

    <div class="card" style="grid-column: 1 / -1;">
      <div class="label">AT Console</div>
      <pre id="atlog" class="mono" style="max-height:220px; overflow:auto; margin-top:8px;">-</pre>
    </div>
  </div>

  <script>
    const el = (id) => document.getElementById(id);
    const fmt = (v, unit = "") => (v === null || v === undefined ? "-" : `${v}${unit}`);
    const fmtTs = (s) => (!s ? "-" : new Date(s * 1000).toLocaleTimeString());
    const fmtKbps = (v) => {
      const n = Number(v);
      if (!Number.isFinite(n) || n < 0) return "-";
      if (n < 1000) return `${n.toFixed(1)} kbps`;
      return `${(n / 1000).toFixed(2)} Mbps`;
    };
    const CELL_COLOR_PALETTE = [
      "#e6194b", // red
      "#4363d8", // blue
      "#3cb44b", // green
      "#f58231", // orange
      "#911eb4", // purple
      "#46f0f0", // cyan
      "#ffe119", // yellow
      "#f032e6", // magenta
      "#008080", // teal-dark
      "#9a6324", // brown
      "#3a86ff", // bright blue
      "#808000", // olive
      "#800000", // maroon
      "#aaffc3", // mint
      "#000000"  // black
    ];
    const cellColorMap = new Map();
    let nextColorSeed = 0;
    const colorForCellKey = (cellKey, fallback = "#4da3ff") => {
      const s = String(cellKey || "").trim();
      if (!s) return fallback;
      if (cellColorMap.has(s)) return cellColorMap.get(s) || fallback;
      // Step through palette with a prime-like jump to reduce adjacent similarity.
      const idx = (nextColorSeed * 7 + 3) % CELL_COLOR_PALETTE.length;
      nextColorSeed += 1;
      const c = CELL_COLOR_PALETTE[idx] || fallback;
      cellColorMap.set(s, c);
      return c;
    };
    let iperfBusy = false;
    let serialBaud = 115200;
    let serialPorts = [];
    let currentServingEarfcn = null;
    let currentServingPci = null;
    let lastDataService = {};
    const iperfHistory = [];
    const iperfDlHistory = [];
    const iperfUlHistory = [];
    let lastIperfDlMbps = null;
    let lastIperfUlMbps = null;
    let pingSweepBusy = false;
    let phRepeatTimer = null;
    const PH_REPEAT_INTERVAL_MS = 15000;
    const phAvgHistory = [];
    const phJitHistory = [];
    let lastPhAvgMs = null;
    let lastPhJitMs = null;
    const rfHistory = { rsrp: [], rsrq: [], sinr: [], rssi: [], dominance: [] };
    const bwHistory = [];
    const pciHistory = [];
    const neighbourHistory = { rsrp: [], pci: [] };
    const carrierReselPciHistory = [];
    const carrierReselEarfcnHistory = [];
    const categoryHistory = { state: [], band: [] };
    let chartWindowMs = 60 * 1000;
    const RF_SMOOTH_WINDOW = 10;
    let rfSmoothingEnabled = false;
    let chartGapModeEnabled = false;
    let currentPollHz = 2.0;
    let primaryCellDataAvailable = false;
    let lastTrendSampleTs = null;
    let rfChartTooltipEl = null;
    const RF_HOVER_CANVAS_IDS = ["rsrpchart", "rsrqchart", "sinrchart", "rssichart", "dominancechart"];
    const RF_CHART_TITLE_BY_ID = {
      rsrpchart: "RSRP",
      rsrqchart: "RSRQ",
      sinrchart: "SINR",
      rssichart: "RSSI",
      dominancechart: "Intra-cell dominance"
    };
    const copsModeName = (m) => {
      if (m === 0) return "0 (Auto)";
      if (m === 1) return "1 (Manual)";
      if (m === 2) return "2 (Deregistered)";
      if (m === 3) return "3 (Format only)";
      if (m === 4) return "4 (Manual/auto)";
      return m === null || m === undefined ? "-" : String(m);
    };
    const UK_MNO_BY_PLMN = {
      "23415": "Vodafone",
      "23410": "VMO2",
      "23430": "EE",
      "23420": "H3G"
    };

    function formatOperatorName(raw) {
      const s = String(raw || "").trim();
      if (!s) return "-";
      const mno = UK_MNO_BY_PLMN[s];
      if (mno) return `${mno} (${s})`;
      return s;
    }

    /** Prefer server *error*, append *modem_detail* only when distinct (CME/CMS hints). */
    function userFacingBackendError(j, fallback) {
      if (!j || typeof j !== "object") return fallback || "";
      const md = typeof j.modem_detail === "string" ? j.modem_detail.trim() : "";
      let er = "";
      if (typeof j.error === "string") er = j.error.trim();
      if (!er && typeof j.detail === "string") er = j.detail.trim();
      if (!er && Array.isArray(j.detail)) {
        er = j.detail.map((d) => `${d.loc || "?"} ${d.msg || ""}`).join("; ");
      }
      if (er && md && er.includes(md)) return er || fallback || "";
      if (er && md) return `${er} — ${md}`;
      return er || md || fallback || "";
    }

    function pruneHistoryByAge(history, nowMs = Date.now()) {
      if (!Array.isArray(history) || history.length === 0) return;
      const cutoff = nowMs - chartWindowMs;
      while (history.length && Number(history[0]?.t || 0) < cutoff) history.shift();
    }

    function pruneAllHistory(nowMs = Date.now()) {
      pruneHistoryByAge(iperfHistory, nowMs);
      pruneHistoryByAge(iperfDlHistory, nowMs);
      pruneHistoryByAge(iperfUlHistory, nowMs);
      pruneHistoryByAge(phAvgHistory, nowMs);
      pruneHistoryByAge(phJitHistory, nowMs);
      Object.values(rfHistory).forEach((h) => pruneHistoryByAge(h, nowMs));
      pruneHistoryByAge(bwHistory, nowMs);
      pruneHistoryByAge(pciHistory, nowMs);
      Object.values(neighbourHistory).forEach((h) => pruneHistoryByAge(h, nowMs));
      pruneHistoryByAge(carrierReselPciHistory, nowMs);
      pruneHistoryByAge(carrierReselEarfcnHistory, nowMs);
      Object.values(categoryHistory).forEach((h) => pruneHistoryByAge(h, nowMs));
    }

    function formatAxisDuration(ms) {
      const sec = Math.max(1, Math.round(Number(ms || 0) / 1000));
      if (sec < 60) return `${sec}s`;
      const min = Math.round(sec / 60);
      if (min < 60) return `${min}m`;
      const hrs = Math.round(min / 60);
      return `${hrs}h`;
    }

    function updateChartAxisLabels() {
      const txt = `Time axis: last ${formatAxisDuration(chartWindowMs)}`;
      document.querySelectorAll(".chart-axis-label").forEach((node) => {
        node.textContent = txt;
      });
    }

    function redrawAllCharts() {
      drawIperfChart();
      drawIperfGauges();
      drawPhSweepChart();
      drawPhGauges();
      drawRfCharts();
      drawBwChart();
      drawPciChart();
      drawNeighbourCharts();
      drawCarrierReselChart();
      drawCategoryCharts();
    }

    function updateChartGapButton() {
      const b = el("btn-chart-gap-mode");
      if (!b) return;
      b.textContent = chartGapModeEnabled ? "Time-roll gaps: ON" : "Time-roll gaps: OFF";
    }

    function setChartGapMode(enabled) {
      chartGapModeEnabled = !!enabled;
      updateChartGapButton();
      pruneAllHistory(Date.now());
      redrawAllCharts();
    }

    function applyChartWindowSec(seconds) {
      const s = Number(seconds);
      if (!Number.isFinite(s) || s < 60 || s > 3600) return;
      chartWindowMs = Math.round(s * 1000);
      updateChartAxisLabels();
      pruneAllHistory(Date.now());
      redrawAllCharts();
    }

    function smoothSeries(samples, windowSize) {
      if (!Array.isArray(samples) || !samples.length) return [];
      const out = [];
      let rollingSum = 0;
      for (let i = 0; i < samples.length; i++) {
        const v = Number(samples[i].v);
        if (!Number.isFinite(v)) continue;
        rollingSum += v;
        if (i >= windowSize) rollingSum -= Number(samples[i - windowSize].v) || 0;
        const count = Math.min(i + 1, windowSize);
        out.push({ t: samples[i].t, v: rollingSum / count, c: samples[i].c });
      }
      return out;
    }

    function applySnap(payload) {
      const sample = payload?.sample || {};
      const net = sample.network || {};
      const modem = sample.modem || {};
      const ds = sample.data_service || {};
      lastDataService = ds;
      const srv = sample.servingcell || {};
      const lte = srv.lte || {};
      const qrsrp = sample.qrsrp || {};
      const qrsrq = sample.qrsrq || {};
      const qsinr = sample.qsinr || {};
      const nb = sample.neighbour || {};
      const idleMob = sample.carrier_reselection || {};
      const inService =
        String(net.service || "").toUpperCase() !== "NO SERVICE" &&
        !!net.act &&
        String(net.act).toUpperCase() !== "NONE";

      el("operator").textContent = formatOperatorName(net.operator);

      const epsScope = ds.eps_reg_scope;
      const epsStat = ds.eps_reg_stat;
      const epsTip =
        "EPS registration from AT+CEREG (3GPP: stat 1 = home PLMN, 5 = roaming). Refreshes with Data Service KPI (~5s).";
      const scopeEl = el("access-eps-scope");
      scopeEl.title = epsTip;
      if (epsScope === "home") {
        scopeEl.textContent = "Home network";
        scopeEl.className = "ok";
      } else if (epsScope === "roaming") {
        scopeEl.textContent = "Roaming";
        scopeEl.className = "warn";
      } else if (epsStat !== null && epsStat !== undefined && `${epsStat}`.length) {
        scopeEl.textContent = `Not home/roaming (${epsStat})`;
        scopeEl.className = "";
      } else {
        scopeEl.textContent = "-";
        scopeEl.className = "";
      }

      el("band").textContent = net.band || "-";
      el("modemfw").textContent = modem.firmware || "-";
      el("ds-apn").textContent = ds.apn || "-";
      if (ds.active_pdp_contexts === null || ds.active_pdp_contexts === undefined || ds.pdp_contexts === null || ds.pdp_contexts === undefined) {
        el("ds-pdp").textContent = "-";
      } else {
        el("ds-pdp").textContent = `${ds.active_pdp_contexts}/${ds.pdp_contexts}`;
      }

      const cid1State = ds.cid1_active === true ? "UP" : ds.cid1_active === false ? "DOWN" : "-";
      el("ds-cid1").textContent = cid1State;
      el("ds-cid1").className = ds.cid1_active === true ? "ok" : ds.cid1_active === false ? "warn" : "";
      el("ds-ip").textContent = ds.cid1_ip || "-";

      const attachText =
        ds.packet_attached === true ? "Attached" : ds.packet_attached === false ? "Detached" : "-";
      el("ds-attach").textContent = attachText;
      el("ds-attach").className = ds.packet_attached === true ? "ok" : ds.packet_attached === false ? "warn" : "";

      const regStat = ds.eps_reg_stat === null || ds.eps_reg_stat === undefined ? "-" : ` (${ds.eps_reg_stat})`;
      const regText =
        ds.eps_registered === true
          ? `Registered${regStat}`
          : ds.eps_registered === false
            ? `Not registered${regStat}`
            : "-";
      el("ds-reg").textContent = regText;
      el("ds-reg").className = ds.eps_registered === true ? "ok" : ds.eps_registered === false ? "warn" : "";
      el("ds-usbnet").textContent =
        ds.usbnet_mode_label || (ds.usbnet_mode === null || ds.usbnet_mode === undefined ? "-" : `mode ${ds.usbnet_mode}`);
      el("ds-netdev").textContent = ds.qnetdev_status || "-";
      if (ds.usbnet_mode_label && /RNDIS|NDIS|QMI/i.test(String(ds.usbnet_mode_label))) {
        el("ds-warn").textContent = "Note: USB data stack active (NDIS/QMI-like mode). Host WAN usage may contend with modem traffic.";
        el("ds-warn").className = "label warn";
      } else {
        el("ds-warn").textContent = "-";
        el("ds-warn").className = "label";
      }

      const ratVal = inService ? (lte.rat || srv.mode || "-") : "-";
      el("rat").textContent = ratVal;
      el("state").textContent = srv.state || "-";
      const dlBw = lte.dl_bw;
      const ulBw = lte.ul_bw;
      if (dlBw === null || dlBw === undefined || ulBw === null || ulBw === undefined) {
        el("bwpair").textContent = "-";
      } else {
        el("bwpair").textContent = `${dlBw}/${ulBw} MHz`;
      }
      const earfcn = lte.earfcn;
      const pci = lte.pcid;
      currentServingEarfcn = Number.isFinite(Number(earfcn)) ? Number(earfcn) : null;
      currentServingPci = Number.isFinite(Number(pci)) ? Number(pci) : null;
      primaryCellDataAvailable =
        inService &&
        Number.isFinite(currentServingEarfcn) &&
        Number.isFinite(currentServingPci) &&
        Number.isFinite(Number(lte.rsrp));
      if (earfcn === null || earfcn === undefined || pci === null || pci === undefined) {
        el("earfcnpci").textContent = "-";
      } else {
        el("earfcnpci").textContent = `${earfcn}/${pci}`;
      }
      el("cellid").textContent = lte.cell_id_hex || "-";

      el("rsrp").textContent = fmt(lte.rsrp, " dBm");
      el("nrsrp1").textContent = fmt(nb.strongest_rsrp, " dBm");
      el("npci1").textContent = fmt(nb.strongest_pci);
      el("nearfcn1").textContent = fmt(nb.strongest_earfcn);
      if (idleMob.intra_freq_pci_reselections_per_min === undefined || idleMob.intra_freq_pci_reselections_per_min === null) {
        el("idle-pci-rate").textContent = "-";
      } else {
        el("idle-pci-rate").textContent = String(idleMob.intra_freq_pci_reselections_per_min);
      }
      if (idleMob.primary_earfcn_reselections_per_min === undefined || idleMob.primary_earfcn_reselections_per_min === null) {
        el("idle-earfcn-rate").textContent = "-";
      } else {
        el("idle-earfcn-rate").textContent = String(idleMob.primary_earfcn_reselections_per_min);
      }
      el("rsrq").textContent = fmt(lte.rsrq, " dB");
      el("sinr").textContent = fmt(qsinr.prx, " dB");
      el("rssi").textContent = fmt(lte.rssi, " dBm");
      const dominance = (lte.rsrp === null || lte.rsrp === undefined || nb.strongest_rsrp === null || nb.strongest_rsrp === undefined)
        ? null
        : Number(lte.rsrp) - Number(nb.strongest_rsrp);
      el("dominance").textContent = fmt(dominance, " dB");
      el("updated").textContent = fmtTs(sample.sample_ts);

      const trendTs = sample.sample_ts || null;
      if (trendTs !== lastTrendSampleTs) {
        lastTrendSampleTs = trendTs;
        addRfSample("rsrp", lte.rsrp, trendTs);
        addRfSample("rsrq", lte.rsrq, trendTs);
        addRfSample("sinr", qsinr.prx, trendTs);
        addRfSample("rssi", lte.rssi, trendTs);
        addRfSample("dominance", dominance, trendTs);
        addBwSample(lte.dl_bw, trendTs);
        addPciSample(lte.pcid, trendTs);
        addNeighbourSample("rsrp", nb.strongest_rsrp, trendTs);
        addNeighbourSample("pci", nb.strongest_pci, trendTs);
        addCategorySample("state", srv.state || "-", trendTs);
        addCategorySample("band", net.band || "-", trendTs);
        addCarrierReselSamples(idleMob, trendTs);
      }
      const hz = Number(payload?.poll_hz);
      if (Number.isFinite(hz) && hz > 0) currentPollHz = hz;

      el("chains").textContent =
        `QRSRP: PRX=${fmt(qrsrp.prx)} DRX=${fmt(qrsrp.drx)} RX2=${fmt(qrsrp.rx2)} RX3=${fmt(qrsrp.rx3)}\\n` +
        `QRSRQ: PRX=${fmt(qrsrq.prx)} DRX=${fmt(qrsrq.drx)} RX2=${fmt(qrsrq.rx2)} RX3=${fmt(qrsrq.rx3)}\\n` +
        `QSINR: PRX=${fmt(qsinr.prx)} DRX=${fmt(qsinr.drx)} RX2=${fmt(qsinr.rx2)} RX3=${fmt(qsinr.rx3)}`;

      const st = el("status");
      if (payload.last_error) {
        st.textContent = `Poll warning: ${payload.last_error}`;
        st.className = "label warn";
      } else {
        st.textContent = `Connected. Poll ${payload.poll_hz} Hz`;
        st.className = "label ok";
      }
    }

    function applyCops(data, msg = "") {
      const c = data?.cops || {};
      el("copsmode").textContent = copsModeName(c.mode);
      el("copsoperator").textContent = formatOperatorName(c.operator);
      el("copsact").textContent = c.act === null || c.act === undefined ? "-" : String(c.act);
      if (msg) el("copsmsg").textContent = msg;
    }

    function applyCopsScan(data, msg = "") {
      const items = Array.isArray(data?.operators) ? data.operators : [];
      if (!items.length) {
        el("copsscan").textContent = "No operators parsed (scan may be unsupported or timed out).";
      } else {
        const lines = items.map((it) => {
          const label = it.long_name || it.short_name || "Unknown";
          const plmn = it.plmn ? ` (${it.plmn})` : "";
          const act = it.act === null || it.act === undefined ? "" : ` AcT=${it.act}`;
          const st = it.status_label || "-";
          return `${label}${plmn} - ${st}${act}`;
        });
        el("copsscan").textContent = lines.join("\\n");
      }
      if (msg) el("copsmsg").textContent = msg;
    }

    function applyLocks(data, msg = "") {
      const v = data?.locks || {};
      el("lock-ratmode").textContent = v.mode_pref || "-";
      el("lock-lteband").textContent = v.lte_band || "-";
      const lteVal = String(v.lte_band || "");
      const caPolicy = !lteVal ? "-" : (lteVal === "0" || lteVal.includes(":") ? "ON (multi/all)" : "OFF (single band)");
      el("lock-ca").textContent = caPolicy;
      el("lock-nrband").textContent = v.nr5g_band || v.nsa_nr5g_band || "-";
      el("lock-nrdc").textContent = String(v.nrdc_mode || "0") === "1" ? "ON" : "OFF";
      el("input-nrdc-enable").checked = String(v.nrdc_mode || "0") === "1";
      const ratSel = el("input-ratmode");
      if (v.mode_pref && Array.from(ratSel.options).some((o) => o.value === v.mode_pref)) {
        ratSel.value = v.mode_pref;
      }
      if (msg) el("lockmsg").textContent = msg;
    }

    function applyMnoState(data, msg = "") {
      const sel = String(data?.selected_profile || "auto");
      const profiles = data?.profiles || {};
      const label = profiles?.[sel]?.label || sel.toUpperCase();
      const cops = data?.cops || {};
      const plmn = cops?.operator || "-";
      const mode = cops?.mode;
      el("mno-selected").textContent = `${label}${mode === 0 ? " (auto)" : ""}`;
      el("mno-current-plmn").textContent = formatOperatorName(plmn);
      const selEl = el("mno-select");
      if (selEl && Array.from(selEl.options).some((o) => o.value === sel)) selEl.value = sel;
      if (msg) el("mnomsg").textContent = msg;
    }

    function applyDataGateState(data, msg = "") {
      const inhibited = !!data?.inhibited;
      const active = Array.isArray(data?.active_contexts) ? data.active_contexts : [];
      el("data-gate-state").textContent = inhibited ? "INHIBITED" : "ALLOWED";
      el("data-gate-state").className = inhibited ? "warn" : "ok";
      el("data-gate-active").textContent = String(active.length);
      if (msg) el("mnomsg").textContent = msg;
    }

    async function readMnoState(msg = "") {
      try {
        const r = await fetch("/api/network/mno");
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "MNO read failed"));
        applyMnoState(j, msg);
      } catch (e) {
        el("mnomsg").textContent = `MNO read error: ${e.message || e}`;
      }
    }

    async function applyMnoSelection() {
      const profile = String(el("mno-select").value || "auto");
      const cops_manual_registration = Number(el("mno-cops-mode")?.value || "4");
      const deregister_before_apply = profile === "auto" ? true : !el("mno-skip-dereg")?.checked;
      try {
        el("mnomsg").textContent = `Applying ${profile.toUpperCase()}...`;
        const r = await fetch("/api/network/mno", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ profile, cops_manual_registration, deregister_before_apply })
        });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "MNO apply failed"));
        applyMnoState(j, `MNO profile applied: ${profile.toUpperCase()}`);
      } catch (e) {
        el("mnomsg").textContent = `MNO apply error: ${e.message || e}`;
      } finally {
        await readMnoState();
      }
    }

    async function readDataGate() {
      try {
        const r = await fetch("/api/network/data-gate");
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "Data gate read failed"));
        applyDataGateState(j);
      } catch (e) {
        el("mnomsg").textContent = `Data gate read error: ${e.message || e}`;
      }
    }

    async function setDataGate(inhibit) {
      try {
        el("mnomsg").textContent = inhibit ? "Inhibiting packet data..." : "Allowing packet data...";
        const password = String(el("data-gate-password")?.value || "");
        const r = await fetch("/api/network/data-gate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ inhibit, password })
        });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "Data gate update failed"));
        applyDataGateState(j.after || {}, inhibit ? "Packet data inhibited." : "Packet data allowed.");
      } catch (e) {
        el("mnomsg").textContent = `Data gate error: ${e.message || e}`;
      } finally {
        await readDataGate();
      }
    }

    async function applyDsApn() {
      const apn = String(el("ds-apn-set")?.value || "").trim();
      const pdp_type = String(el("ds-pdp-type")?.value || "IP");
      const password = String(el("ds-apn-password")?.value || "");
      const msgEl = el("ds-apn-msg");
      if (!apn) {
        msgEl.textContent = "Enter an APN.";
        msgEl.className = "label warn";
        return;
      }
      if (!password) {
        msgEl.textContent = "Enter unlock password.";
        msgEl.className = "label warn";
        return;
      }
      try {
        msgEl.textContent = "Applying APN...";
        msgEl.className = "label";
        const reactivate = !!el("ds-apn-reactivate")?.checked;
        const r = await fetch("/api/network/apn", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ apn, cid: 1, pdp_type, password, reactivate })
        });
        const j = await r.json().catch(() => ({}));
        if (!r.ok) {
          const d = j?.detail;
          const errTxt =
            typeof d === "string"
              ? d
              : Array.isArray(d)
                ? d.map((x) => `${x.loc || "?"} ${x.msg || ""}`).join("; ")
                : "";
          throw new Error(
            userFacingBackendError(j, errTxt || r.statusText || "APN update failed")
          );
        }
        if (!j.ok) throw new Error(userFacingBackendError(j, "CGDCONT was rejected."));
        msgEl.textContent = j.message || "APN updated.";
        msgEl.className = "label ok";
        el("ds-apn-password").value = "";
        await pollFallback();
      } catch (e) {
        msgEl.textContent = `APN error: ${e.message || e}`;
        msgEl.className = "label warn";
      }
    }

    function applySimHighLevel(data, msg = "") {
      el("sim-imei").textContent = data?.imei || "-";
      el("sim-imsi").textContent = data?.imsi || "-";
      el("sim-spn").textContent = data?.spn || "-";
      const c = data?.cops || {};
      const act = c?.act === null || c?.act === undefined ? "" : ` (AcT ${c.act})`;
      el("sim-cops").textContent = c?.operator ? `${formatOperatorName(c.operator)}${act}` : "-";
      const cpolCount = data?.cpol_count;
      el("sim-cpol-count").textContent = cpolCount === null || cpolCount === undefined ? "-" : String(cpolCount);
      if (msg) el("simmsg").textContent = msg;
    }

    function applySimInspector(data, msg = "") {
      const d = data?.decoded || {};
      const verbose = !!data?.verbose;
      const lines = [];
      if (verbose && data?.label_reference) {
        lines.push(`Reference: ${data.label_reference}`);
        lines.push("");
      }
      const pushPlmnFile = (key, title) => {
        const f = d?.[key] || {};
        const entries = Array.isArray(f?.entries) ? f.entries : [];
        lines.push(`${title} (${f?.fileid || "-"}) count=${entries.length}`);
        if (verbose && f?.description) lines.push(`  Note: ${f.description}`);
        entries.slice(0, 12).forEach((e) => {
          const act = e?.act_hex ? ` act=${e.act_hex}` : "";
          lines.push(`  - ${e?.plmn || "-"}${act}`);
        });
        if (entries.length > 12) lines.push(`  ... ${entries.length - 12} more`);
        if (verbose) lines.push("");
      };
      pushPlmnFile("ef_plmnwact", "EF_PLMNwAcT");
      pushPlmnFile("ef_oplmnwact", "EF_OPLMNwAcT");
      pushPlmnFile("ef_ehplmn", "EF_EHPLMN");
      pushPlmnFile("ef_fplmn", "EF_FPLMN");
      const adMncLen = d?.ef_ad?.mnc_length;
      lines.push(`EF_AD (${d?.ef_ad?.fileid || "-"}) mnc_length=${adMncLen === null || adMncLen === undefined ? "-" : adMncLen}`);
      if (verbose && d?.ef_ad?.description) lines.push(`  Note: ${d.ef_ad.description}`);
      const hplmnTimer = d?.ef_hplmn?.hplmn_search_timer_min;
      lines.push(`EF_HPLMN (${d?.ef_hplmn?.fileid || "-"}) timer_min=${hplmnTimer === null || hplmnTimer === undefined ? "-" : hplmnTimer}`);
      if (verbose && d?.ef_hplmn?.description) lines.push(`  Note: ${d.ef_hplmn.description}`);
      const ustCount = d?.ef_ust?.enabled_services_count;
      lines.push(`EF_UST (${d?.ef_ust?.fileid || "-"}) enabled_services=${ustCount === null || ustCount === undefined ? "-" : ustCount}`);
      if (verbose && d?.ef_ust?.description) lines.push(`  Note: ${d.ef_ust.description}`);
      const ustVerbose = Array.isArray(d?.ef_ust?.enabled_services_verbose) ? d.ef_ust.enabled_services_verbose : [];
      if (ustVerbose.length) {
        ustVerbose.forEach((row) => {
          const n = row?.service_no;
          const lb = row?.label || "";
          lines.push(`  - n°${n}: ${lb}`);
        });
      } else {
        const ustList = Array.isArray(d?.ef_ust?.enabled_services) ? d.ef_ust.enabled_services : [];
        if (ustList.length) lines.push(`  - service IDs: ${ustList.slice(0, 48).join(", ")}${ustList.length > 48 ? " ..." : ""}`);
      }
      if (verbose) lines.push("");
      const spdiHex = d?.ef_spdi?.hex || "";
      lines.push(`EF_SPDI (${d?.ef_spdi?.fileid || "-"}) hex=${spdiHex || "-"}`);
      if (verbose && d?.ef_spdi?.description) lines.push(`  Note: ${d.ef_spdi.description}`);
      lines.push(`EF_PNN (${d?.ef_pnn?.fileid || "-"}) hex=${d?.ef_pnn?.hex || "-"}`);
      if (verbose && d?.ef_pnn?.description) lines.push(`  Note: ${d.ef_pnn.description}`);
      lines.push(`EF_OPL (${d?.ef_opl?.fileid || "-"}) hex=${d?.ef_opl?.hex || "-"}`);
      if (verbose && d?.ef_opl?.description) lines.push(`  Note: ${d.ef_opl.description}`);
      lines.push(`EF_EPSLOCI (${d?.ef_epsloci?.fileid || "-"}) hex=${d?.ef_epsloci?.hex || "-"}`);
      if (verbose && d?.ef_epsloci?.hex_byte_length != null) {
        lines.push(`  (${d.ef_epsloci.hex_byte_length} byte(s) hex payload)`);
      }
      if (verbose && d?.ef_epsloci?.description) lines.push(`  Note: ${d.ef_epsloci.description}`);
      lines.push(`EF_5GSLOCI (${d?.ef_5gsloci?.fileid || "-"}) hex=${d?.ef_5gsloci?.hex || "-"} sw=${d?.ef_5gsloci?.sw1 || "-"},${d?.ef_5gsloci?.sw2 || "-"}`);
      if (verbose && d?.ef_5gsloci?.description) lines.push(`  Note: ${d.ef_5gsloci.description}`);
      el("siminspect").textContent = lines.length ? lines.join("\\n") : "-";
      if (msg) el("simmsg").textContent = msg;
    }

    async function readSimHighLevel() {
      try {
        const r = await fetch("/api/sim/high-level");
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "SIM high-level read failed"));
        applySimHighLevel(j, "SIM high-level read OK");
      } catch (e) {
        el("simmsg").textContent = `SIM high-level read error: ${e.message || e}`;
      }
    }

    async function readSimInspector() {
      try {
        const r = await fetch("/api/sim/inspector?verbose=1");
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "SIM inspector read failed"));
        applySimInspector(j, "SIM inspector read OK");
      } catch (e) {
        el("simmsg").textContent = `SIM inspector read error: ${e.message || e}`;
      }
    }

    async function readSerialStatus(showMessage = false) {
      try {
        const r = await fetch("/api/serial/status");
        const j = await r.json();
        if (!r.ok) throw new Error(j.detail || "Serial status read failed");
        serialBaud = Number(j.baudrate) || serialBaud;
        el("serial-current").textContent = j.port || "-";
        const openText = j.serial_open ? "Yes" : "No";
        el("serial-open").textContent = openText;
        el("serial-open").className = j.serial_open ? "ok" : "warn";
        if (showMessage) {
          el("serialmsg").textContent = j.last_open_error
            ? `Serial warning: ${j.last_open_error}`
            : `Serial OK on ${j.port || "-"}`;
        }
        return j;
      } catch (e) {
        el("serial-open").textContent = "No";
        el("serial-open").className = "err";
        if (showMessage) el("serialmsg").textContent = `Serial status error: ${e.message || e}`;
        return null;
      }
    }

    async function refreshSerialPorts(selectCurrent = true) {
      try {
        const r = await fetch("/api/serial/ports");
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(j.detail || j.error || "Port scan failed");
        const ports = Array.isArray(j.ports) ? j.ports : [];
        serialPorts = ports;
        const sel = el("serial-port-select");
        const previous = sel.value;
        sel.innerHTML = "";
        if (!ports.length) {
          const op = document.createElement("option");
          op.value = "";
          op.textContent = "No COM ports found";
          sel.appendChild(op);
          el("serialmsg").textContent = "No serial ports detected.";
          return;
        }
        for (const p of ports) {
          const op = document.createElement("option");
          op.value = p.device || "";
          const desc = p.description ? ` - ${p.description}` : "";
          op.textContent = `${p.device || "?"}${desc}`;
          sel.appendChild(op);
        }
        const st = await readSerialStatus(false);
        if (selectCurrent && st?.port) {
          sel.value = st.port;
        } else if (previous) {
          sel.value = previous;
        }
        if (!sel.value && ports[0]?.device) sel.value = ports[0].device;
        el("serialmsg").textContent = `Detected ${ports.length} serial port(s).`;
      } catch (e) {
        el("serialmsg").textContent = `Port refresh error: ${e.message || e}`;
      }
    }

    function chooseLikelyAtPort(ports) {
      if (!Array.isArray(ports) || !ports.length) return null;
      const score = (p) => {
        const d = String(p?.description || "").toLowerCase();
        const m = String(p?.manufacturer || "").toLowerCase();
        const name = `${d} ${m}`;
        if (name.includes("quectel") && d.includes("usb at port")) return 100;
        if (name.includes("quectel") && d.includes(" at ")) return 90;
        if (d.includes("at port")) return 70;
        if (name.includes("quectel")) return 50;
        return 10;
      };
      const sorted = [...ports].sort((a, b) => score(b) - score(a));
      return sorted[0] || null;
    }

    async function autoPickSerialPort() {
      await refreshSerialPorts(false);
      const sel = el("serial-port-select");
      const best = chooseLikelyAtPort(serialPorts);
      if (!best?.device) {
        el("serialmsg").textContent = "No likely AT port found.";
        return;
      }
      sel.value = best.device;
      el("serialmsg").textContent = `Auto-selected ${best.device}${best.description ? ` (${best.description})` : ""}.`;
    }

    async function reconnectSerial() {
      const port = el("serial-port-select").value || "";
      if (!port) {
        el("serialmsg").textContent = "Select a serial port first.";
        return;
      }
      try {
        el("serialmsg").textContent = `Reconnecting to ${port}...`;
        const r = await fetch("/api/serial/reopen", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ port, baudrate: serialBaud })
        });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(j.detail || j.error || j.last_open_error || "Reconnect failed");
        await readSerialStatus(false);
        await refreshSerialPorts(false);
        el("serialmsg").textContent = `Reconnected on ${j.port} @ ${j.baudrate}`;
      } catch (e) {
        el("serialmsg").textContent = `Reconnect error: ${e.message || e}`;
      }
    }

    const sleepMs = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

    async function waitForModemRecovery(maxWaitSec = 90) {
      const deadline = Date.now() + maxWaitSec * 1000;
      while (Date.now() < deadline) {
        await refreshSerialPorts(false);
        const st = await readSerialStatus(false);
        if (st?.serial_open) return st;

        const best = chooseLikelyAtPort(serialPorts);
        if (best?.device) {
          try {
            await fetch("/api/serial/reopen", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ port: best.device, baudrate: serialBaud })
            });
            const st2 = await readSerialStatus(false);
            if (st2?.serial_open) return st2;
          } catch (_) {}
        }
        await sleepMs(2000);
      }
      return null;
    }

    async function resetModem() {
      const ok = window.confirm("Reset modem now? This will drop serial and network for a short period.");
      if (!ok) return;
      try {
        el("serialmsg").textContent = "Sending modem reset (AT+CFUN=1,1)...";
        await fetch("/api/kpi/poll/stop", { method: "POST" });
        const r = await fetch("/api/tools/modem-reset", { method: "POST" });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "Modem reset failed"));
        el("serialmsg").textContent = "Reset accepted. Waiting for modem recovery (up to 90s)...";
        const recovered = await waitForModemRecovery(90);
        if (recovered?.serial_open) {
          el("serialmsg").textContent = `Modem recovered on ${recovered.port}.`;
        } else {
          el("serialmsg").textContent = "Modem not fully recovered yet. Use Refresh/Auto-select/Reconnect.";
        }
        await readSerialStatus(false);
        await readCops();
        await readLocks();
      } catch (e) {
        el("serialmsg").textContent = `Reset error: ${e.message || e}`;
      } finally {
        await fetch("/api/kpi/poll/start", { method: "POST" });
      }
    }

    async function readCops() {
      try {
        const r = await fetch("/api/network/cops");
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "COPS read failed"));
        applyCops(j, "COPS read OK");
      } catch (e) {
        el("copsmsg").textContent = `COPS read error: ${e.message || e}`;
      }
    }

    async function setCops(mode) {
      try {
        const r = await fetch("/api/network/cops", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ mode })
        });
        const j = await r.json();
        const setFinal = j?.set?.final || "";
        const setLines = Array.isArray(j?.set?.lines) ? j.set.lines.join(" | ") : "";
        if (!r.ok || !j.ok) {
          throw new Error(
            userFacingBackendError(j, `COPS set failed (${setFinal || "no final"}) ${setLines}`.trim())
          );
        }
        applyCops(j, `COPS mode set to ${mode} (${setFinal || "OK"})`);
      } catch (e) {
        el("copsmsg").textContent = `COPS set error: ${e.message || e}`;
      }
    }

    async function scanCops() {
      const scanBtn = el("btn-cops-scan");
      try {
        if (scanBtn) scanBtn.disabled = true;
        const ukOnly = !!el("cops-scan-uk-only")?.checked;
        el("copsmsg").textContent = `Scanning operators via AT+COPS=? (up to ~35s)${ukOnly ? " with UK LTE+NR bands" : ""}. KPI polling is paused during scan.`;
        const r = await fetch(`/api/network/cops/scan?uk_only=${ukOnly ? "1" : "0"}`);
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "COPS scan failed"));
        applyCopsScan(j, `COPS scan complete: ${Array.isArray(j.operators) ? j.operators.length : 0} operator(s)`);
      } catch (e) {
        el("copsmsg").textContent = `COPS scan error: ${e.message || e}`;
      } finally {
        if (scanBtn) scanBtn.disabled = false;
      }
    }

    async function readLocks() {
      try {
        const r = await fetch("/api/network/locks");
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "Lock read failed"));
        applyLocks(j, "Lock config read OK");
      } catch (e) {
        el("lockmsg").textContent = `Lock read error: ${e.message || e}`;
      }
    }

    async function setLocks() {
      const ratMode = (el("input-ratmode").value || "").trim();
      const lteBandManual = (el("input-lteband").value || "").trim();
      const caOn = !!el("input-ca-enable").checked;
      const caOnBands = (el("input-ca-on-bands").value || "").trim();
      const caSingle = (el("input-ca-single-band").value || "").trim();
      const nrBand = (el("input-nrband").value || "").trim();
      const body = {};
      if (ratMode) body.rat_mode = ratMode;
      if (lteBandManual) {
        body.lte_band = lteBandManual;
      } else if (caOn && caOnBands) {
        body.lte_band = caOnBands;
      } else if (caSingle) {
        body.lte_band = caSingle;
      }
      if (nrBand) body.nr5g_band = nrBand;
      body.nrdc_mode = el("input-nrdc-enable").checked ? 1 : 0;
      if (Object.keys(body).length === 0) {
        el("lockmsg").textContent = "Enter at least one value before applying.";
        return;
      }
      try {
        const r = await fetch("/api/network/locks", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body)
        });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "Lock set failed"));
        applyLocks(j, "Locks applied and verified.");
      } catch (e) {
        el("lockmsg").textContent = `Lock set error: ${e.message || e}`;
      }
    }

    async function runVolteTest() {
      const number = String(el("volte-number")?.value || "").trim();
      const password = String(el("volte-password")?.value || "");
      if (!number) {
        el("volte-msg").textContent = "Enter a dial number first.";
        return;
      }
      try {
        el("volte-msg").textContent = `Running VoLTE call test to ${number}...`;
        el("volte-trace").textContent = "Running...";
        await fetch("/api/kpi/poll/stop", { method: "POST" });
        const r = await fetch("/api/tools/volte-test", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ number, hold_sec: 10, password })
        });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "VoLTE test failed"));
        const setupMs = Number(j.setup_time_ms);
        const setupTxt = Number.isFinite(setupMs) ? `${Math.round(setupMs)} ms` : "-";
        const durS = Number(j.call_duration_s);
        const durTxt = Number.isFinite(durS) ? `${durS.toFixed(1)} s` : "-";
        const rat = j?.nwinfo_during_call?.act || j?.nwinfo_after?.act || "-";
        el("volte-msg").textContent = `VoLTE test OK: connected=${j.call_connected ? "yes" : "no"}, setup=${setupTxt}, duration=${durTxt}, RAT=${rat}`;
        const clccStates = Array.isArray(j?.clcc_states)
          ? j.clcc_states.map((x) => `${x.t_s}s: ${x.status || "-"}`).join("\\n")
          : "-";
        const clccAfter = Array.isArray(j?.clcc_after_samples)
          ? j.clcc_after_samples.map((x) => `${x.t_s}s: ${(Array.isArray(x.states) && x.states.length) ? x.states.join(",") : "-"}`).join("\\n")
          : "-";
        const urc = Array.isArray(j?.call_urc_lines) ? j.call_urc_lines.join("\\n") : "-";
        const ceer = j?.ceer || "-";
        el("volte-trace").textContent = [
          `Number: ${j.number || number}`,
          `Connected: ${j.call_connected ? "yes" : "no"}`,
          `Setup time: ${setupTxt}`,
          `Call duration: ${durTxt}`,
          `NW during call: ${JSON.stringify(j?.nwinfo_during_call || {})}`,
          `NW after call: ${JSON.stringify(j?.nwinfo_after || {})}`,
          `CEER: ${ceer}`,
          "",
          "CLCC states:",
          clccStates,
          "",
          "Post-hangup CLCC:",
          clccAfter,
          "",
          "Call URCs:",
          urc
        ].join("\\n");
      } catch (e) {
        const msg = String(e?.message || e || "VoLTE test failed");
        el("volte-msg").textContent = `VoLTE error: ${msg}`;
        el("volte-trace").textContent = `VoLTE error: ${msg}`;
      } finally {
        await fetch("/api/kpi/poll/start", { method: "POST" });
      }
    }

    function syncIperfBindUi() {
      const sel = el("iperf-bind-select");
      const inp = el("iperf-bind-ip");
      if (!sel || !inp) return;
      const v = sel.value;
      if (v === "manual") {
        inp.style.display = "block";
      } else {
        inp.style.display = "none";
        if (v === "auto") inp.value = "";
      }
    }

    function resolveIperfBindIp() {
      const sel = el("iperf-bind-select");
      const inp = el("iperf-bind-ip");
      if (!sel) return "";
      const v = sel.value;
      if (v === "manual") return String(inp?.value || "").trim();
      if (v === "auto") return "";
      return String(v || "").trim();
    }

    function syncPhBindUi() {
      const sel = el("ph-bind-select");
      const inp = el("ph-bind-ip");
      if (!sel || !inp) return;
      const v = sel.value;
      if (v === "manual") {
        inp.style.display = "block";
      } else {
        inp.style.display = "none";
        if (v === "auto") inp.value = "";
      }
    }

    function resolvePhBindIp() {
      const sel = el("ph-bind-select");
      const inp = el("ph-bind-ip");
      if (!sel) return "";
      const v = sel.value;
      if (v === "manual") return String(inp?.value || "").trim();
      if (v === "auto") return "";
      return String(v || "").trim();
    }

    async function loadBindInterfaces() {
      try {
        const r = await fetch("/api/tools/bind-interfaces");
        const j = await r.json();
        const opts = Array.isArray(j.interfaces) ? j.interfaces : [];

        function populateBindSelect(sel, prevVal, autoLabel) {
          if (!sel) return;
          sel.innerHTML = "";
          const optAuto = document.createElement("option");
          optAuto.value = "auto";
          optAuto.textContent = autoLabel;
          sel.appendChild(optAuto);
          for (const row of opts) {
            const ip = String(row.ipv4 || "").trim();
            const ad = String(row.adapter || "").trim();
            const okIp =
              /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.test(ip) &&
              ip.split(".").every((x) => {
                const n = Number(x);
                return Number.isFinite(n) && n >= 0 && n <= 255;
              });
            if (!ip || !okIp) continue;
            const o = document.createElement("option");
            o.value = ip;
            o.textContent = ad ? `${ad} — ${ip}` : ip;
            sel.appendChild(o);
          }
          const optMan = document.createElement("option");
          optMan.value = "manual";
          optMan.textContent = "Manual IPv4…";
          sel.appendChild(optMan);
          let restored = false;
          for (let i = 0; i < sel.options.length; i++) {
            if (sel.options[i].value === prevVal) {
              sel.selectedIndex = i;
              restored = true;
              break;
            }
          }
          if (!restored) sel.value = "auto";
        }

        const selIp = el("iperf-bind-select");
        const selPh = el("ph-bind-select");
        const prevIp = selIp ? selIp.value : "auto";
        const prevPh = selPh ? selPh.value : "auto";
        populateBindSelect(selIp, prevIp, "Auto-detect mobile broadband IPv4");
        populateBindSelect(selPh, prevPh, "Auto (OS default route)");
        syncIperfBindUi();
        syncPhBindUi();
      } catch (_) {}
    }

    async function runIperfTest() {
      if (iperfBusy) return;
      iperfBusy = true;
      const host = String(el("iperf-host")?.value || "").trim();
      const port = Number(el("iperf-port")?.value || 5361);
      const durationSec = Number(el("iperf-duration")?.value || 10);
      const direction = String(el("iperf-direction")?.value || "both").trim().toLowerCase();
      const protocol = String(el("iperf-protocol")?.value || "tcp").trim().toLowerCase();
      const bindIp = resolveIperfBindIp();
      const speedLimitRaw = String(el("iperf-speed-limit")?.value || "").trim();
      const speedLimit = speedLimitRaw === "" ? null : Number(speedLimitRaw);
      if (!host) {
        el("iperf-msg").textContent = "Enter endpoint host.";
        iperfBusy = false;
        return;
      }
      if (!Number.isFinite(port) || port < 1 || port > 65535) {
        el("iperf-msg").textContent = "Port must be 1..65535.";
        iperfBusy = false;
        return;
      }
      if (!Number.isFinite(durationSec) || durationSec < 1 || durationSec > 300) {
        el("iperf-msg").textContent = "Duration must be 1..300 seconds.";
        iperfBusy = false;
        return;
      }
      if (speedLimit !== null && (!Number.isFinite(speedLimit) || speedLimit <= 0)) {
        el("iperf-msg").textContent = "Speed limit must be empty or a positive Mbit/s value.";
        iperfBusy = false;
        return;
      }
      if (String(el("iperf-bind-select")?.value || "") === "manual" && !bindIp) {
        el("iperf-msg").textContent = "Manual bind selected: enter an IPv4 address.";
        iperfBusy = false;
        return;
      }
      try {
        el("iperf-msg").textContent = `Running iperf ${direction} test...`;
        el("iperf-trace").textContent = "Running...";
        const runOne = async (dir) => {
          const body = {
            host,
            port: Math.trunc(port),
            duration_sec: Math.trunc(durationSec),
            direction: dir,
            protocol,
            mobile_only: true
          };
          if (bindIp) body.bind_ip = bindIp;
          if (speedLimit !== null) body.bitrate_limit_mbps = speedLimit;
          const r = await fetch("/api/tools/iperf-test", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body)
          });
          const j = await r.json();
          if (!r.ok || !j.ok) {
            const detail =
              typeof j.detail === "string"
                ? j.detail
                : Array.isArray(j.detail)
                  ? JSON.stringify(j.detail)
                  : j.detail
                    ? JSON.stringify(j.detail)
                    : "";
            throw new Error(j.error || detail || `Iperf ${dir} failed`);
          }
          return j;
        };

        const results = [];
        const dirs = direction === "both" ? ["download", "upload"] : [direction];
        for (const dir of dirs) {
          el("iperf-msg").textContent = `Running iperf ${dir} test...`;
          const j = await runOne(dir);
          const mbps = Number(j.throughput_mbps);
          if (Number.isFinite(mbps) && mbps >= 0) {
            addIperfSample(mbps, dir);
            if (dir === "download") lastIperfDlMbps = mbps;
            if (dir === "upload") lastIperfUlMbps = mbps;
            drawIperfGauges();
          }
          results.push(j);
        }

        const dlTxt = Number.isFinite(lastIperfDlMbps) ? `${lastIperfDlMbps.toFixed(3)} Mbps` : "-";
        const ulTxt = Number.isFinite(lastIperfUlMbps) ? `${lastIperfUlMbps.toFixed(3)} Mbps` : "-";
        const modeTxt = direction === "both" ? "download+upload" : direction;
        el("iperf-msg").textContent = `Iperf ${modeTxt} complete: DL ${dlTxt}, UL ${ulTxt}`;

        const lines = [];
        for (const j of results) {
          const mbps = Number(j.throughput_mbps);
          const shownMbps = Number.isFinite(mbps) ? `${mbps.toFixed(3)} Mbps` : "-";
          const cmd = Array.isArray(j.command) ? j.command.join(" ") : "-";
          const src = j.throughput_source || "-";
          const stderrTail = (j.stderr_tail || "").trim() || "-";
          lines.push(
            `Command: ${cmd}`,
            `Direction: ${j.direction || "-"}`,
            `Protocol: ${j.protocol || protocol}`,
            `Bound IP: ${j.bind_ip || "-"}`,
            `Mobile adapter: ${j.detected_mobile_adapter || "-"}`,
            `Measured throughput: ${shownMbps}`,
            `Result source: ${src}`,
            `Exit code: ${j.exit_code}`,
            "",
            "stderr tail:",
            stderrTail,
            "",
            "----",
            ""
          );
        }
        while (lines.length && !String(lines[lines.length - 1]).trim()) lines.pop();
        el("iperf-trace").textContent = lines.join("\\n");
      } catch (e) {
        const msg = String(e?.message || e || "Iperf test failed");
        el("iperf-msg").textContent = `Iperf error: ${msg}`;
        el("iperf-trace").textContent = `Iperf error: ${msg}`;
      } finally {
        iperfBusy = false;
      }
    }

    function addPhSweepSample(avgMs, jitMs) {
      const a = Number(avgMs);
      const j = Number(jitMs);
      if (!Number.isFinite(a) || a < 0) return;
      const t = Date.now();
      const jv = Number.isFinite(j) && j >= 0 ? j : 0;
      phAvgHistory.push({ t, v: a });
      phJitHistory.push({ t, v: jv });
      pruneHistoryByAge(phAvgHistory, t);
      pruneHistoryByAge(phJitHistory, t);
      drawPhSweepChart();
      drawPhGauges();
    }

    function setPhRepeatPing(enabled) {
      const state = el("ph-repeat-state");
      const toggle = el("ph-repeat-toggle");
      if (phRepeatTimer) {
        clearInterval(phRepeatTimer);
        phRepeatTimer = null;
      }
      if (toggle) toggle.checked = !!enabled;
      if (enabled) {
        if (state) {
          state.textContent = "ON";
          state.className = "ok";
        }
        runPingSweepTest();
        phRepeatTimer = setInterval(() => runPingSweepTest(), PH_REPEAT_INTERVAL_MS);
      } else if (state) {
        state.textContent = "OFF";
        state.className = "";
      }
    }

    async function runPingSweepTest() {
      if (pingSweepBusy) return;
      pingSweepBusy = true;
      const hostRaw = String(el("ph-host")?.value || "").trim();
      const host = hostRaw || "8.8.8.8";
      const count = Number(el("ph-count")?.value ?? 10);
      const bindIp = resolvePhBindIp();
      if (!Number.isFinite(count) || count < 1 || count > 100) {
        el("ph-msg").textContent = "Count must be 1..100.";
        pingSweepBusy = false;
        return;
      }
      if (String(el("ph-bind-select")?.value || "") === "manual" && !bindIp) {
        el("ph-msg").textContent = "Manual bind selected: enter an IPv4 address.";
        pingSweepBusy = false;
        return;
      }
      try {
        el("ph-msg").textContent = "Running ICMP ping sweep...";
        el("ph-trace").textContent = "Running...";
        const body = { host, count: Math.trunc(count) };
        if (bindIp) body.bind_ipv4 = bindIp;
        const r = await fetch("/api/tools/icmp-ping", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body)
        });
        const j = await r.json();
        if (!r.ok || !j.ok) {
          const detail =
            typeof j.detail === "string"
              ? j.detail
              : Array.isArray(j.detail)
                ? JSON.stringify(j.detail)
                : j.detail
                  ? JSON.stringify(j.detail)
                  : "";
          throw new Error(j.error || detail || "ICMP ping sweep failed");
        }
        const avg = Number(j.avg_ms);
        const jit = Number(j.jitter_ms);
        lastPhAvgMs = Number.isFinite(avg) ? avg : null;
        lastPhJitMs = Number.isFinite(jit) ? jit : null;
        if (lastPhAvgMs !== null) {
          addPhSweepSample(lastPhAvgMs, lastPhJitMs ?? 0);
        } else {
          drawPhGauges();
        }
        const rcv = j.received != null ? j.received : "-";
        el("ph-msg").textContent = `ICMP sweep OK: ${host}, ${j.count} probes, ${rcv} replies, avg ${lastPhAvgMs != null ? `${lastPhAvgMs.toFixed(2)} ms` : "-"}, jitter ${lastPhJitMs != null ? `${lastPhJitMs.toFixed(2)} ms` : "-"}`;
        const cmd = Array.isArray(j.command) ? j.command.join(" ") : "-";
        const tail = String(j.stdout_tail || "").trim() || "-";
        el("ph-trace").textContent = [`Command: ${cmd}`, `Exit: ${j.exit_code}`, "", "stdout tail:", tail].join("\\n");
      } catch (e) {
        const msg = String(e?.message || e || "ICMP ping sweep failed");
        el("ph-msg").textContent = `ICMP sweep error: ${msg}`;
        el("ph-trace").textContent = `ICMP sweep error: ${msg}`;
      } finally {
        pingSweepBusy = false;
      }
    }

    function addIperfSample(value, direction = null) {
      const v = Number(value);
      if (!Number.isFinite(v) || v < 0) return;
      const t = Date.now();
      iperfHistory.push({ t, v });
      const d = String(direction || "").toLowerCase();
      if (d === "download") iperfDlHistory.push({ t, v });
      if (d === "upload") iperfUlHistory.push({ t, v });
      pruneHistoryByAge(iperfHistory, t);
      pruneHistoryByAge(iperfDlHistory, t);
      pruneHistoryByAge(iperfUlHistory, t);
      drawIperfChart();
    }

    function addRfSample(kind, value, tsSec = null) {
      const v = Number(value);
      if (!Number.isFinite(v) || !rfHistory[kind]) return;
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      rfHistory[kind].push({ t, v, c: cellKey });
      pruneHistoryByAge(rfHistory[kind], t);
      drawRfCharts();
    }

    function addBwSample(value, tsSec = null) {
      const v = Number(value);
      if (!Number.isFinite(v)) return;
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      bwHistory.push({ t, v, c: cellKey });
      pruneHistoryByAge(bwHistory, t);
      drawBwChart();
    }

    function addPciSample(value, tsSec = null) {
      const v = Number(value);
      if (!Number.isFinite(v)) return;
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      pciHistory.push({ t, v, c: cellKey });
      pruneHistoryByAge(pciHistory, t);
      drawPciChart();
    }

    function addNeighbourSample(kind, value, tsSec = null) {
      const v = Number(value);
      if (!Number.isFinite(v) || !neighbourHistory[kind]) return;
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      neighbourHistory[kind].push({ t, v, c: cellKey });
      pruneHistoryByAge(neighbourHistory[kind], t);
      drawNeighbourCharts();
    }

    function addCategorySample(kind, value, tsSec = null) {
      if (!categoryHistory[kind]) return;
      const v = String(value || "-").trim() || "-";
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      categoryHistory[kind].push({ t, v, c: cellKey });
      pruneHistoryByAge(categoryHistory[kind], t);
      drawCategoryCharts();
    }

    function addCarrierReselSamples(idleMob, tsSec) {
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const pci = Number(idleMob?.intra_freq_pci_reselections_per_min);
      const ear = Number(idleMob?.primary_earfcn_reselections_per_min);
      if (Number.isFinite(pci)) {
        carrierReselPciHistory.push({ t, v: pci });
        pruneHistoryByAge(carrierReselPciHistory, t);
      }
      if (Number.isFinite(ear)) {
        carrierReselEarfcnHistory.push({ t, v: ear });
        pruneHistoryByAge(carrierReselEarfcnHistory, t);
      }
      drawCarrierReselChart();
    }

    function drawCarrierReselChart() {
      const canvas = el("carrier-resel-chart");
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      const all = [...carrierReselPciHistory, ...carrierReselEarfcnHistory];
      if (!all.length) {
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No primary carrier re-selection samples yet", 12, 24);
        return;
      }

      const values = all.map((p) => Number(p?.v)).filter((x) => Number.isFinite(x));
      const minV = Math.min(...values);
      const maxV = Math.max(...values);
      const pad = Math.max(0.25, (maxV - minV) * 0.15);
      const yMin = Math.max(0, minV - pad);
      const yMax = maxV + pad;
      const span = Math.max(1e-6, yMax - yMin);

      ctx.strokeStyle = "#2a2a2a";
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i++) {
        const y = 10 + (i * (h - 20)) / 4;
        ctx.beginPath();
        ctx.moveTo(44, y);
        ctx.lineTo(w - 12, y);
        ctx.stroke();
      }

      ctx.fillStyle = "#aaa";
      ctx.font = "11px Arial";
      ctx.fillText(`${yMax.toFixed(1)} /min`, 4, 14);
      ctx.fillText(`${yMin.toFixed(1)} /min`, 4, h - 8);

      const x0 = 44;
      const x1 = w - 12;
      const y0 = h - 12;
      const y1 = 10;
      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const xFor = (p) => {
        const tt = Number(p?.t);
        if (!Number.isFinite(tt)) return x0;
        const ratio = Math.max(0, Math.min(1, (tt - windowStartMs) / chartWindowMs));
        return x0 + ratio * (x1 - x0);
      };
      const yFor = (v) => y0 - ((v - yMin) / span) * (y0 - y1);

      const drawSeries = (samples, lineColor, pointColor) => {
        if (!samples.length) return;
        const sorted = [...samples].sort((a, b) => Number(a.t) - Number(b.t));
        ctx.strokeStyle = lineColor;
        ctx.lineWidth = 2;
        for (let i = 1; i < sorted.length; i++) {
          const p0 = sorted[i - 1];
          const p1 = sorted[i];
          const xA = xFor(p0);
          const yA = yFor(p0.v);
          const xB = xFor(p1);
          const yB = yFor(p1.v);
          ctx.beginPath();
          ctx.moveTo(xA, yA);
          ctx.lineTo(xB, yB);
          ctx.stroke();
        }
        ctx.fillStyle = pointColor;
        sorted.forEach((p) => {
          const x = xFor(p);
          const y = yFor(p.v);
          ctx.beginPath();
          ctx.arc(x, y, 2.1, 0, Math.PI * 2);
          ctx.fill();
        });
      };

      // Pink (EARFCN) first, light blue (PCI) on top for readability when values coincide.
      drawSeries(carrierReselEarfcnHistory, "#ff8ec8", "#ffc8e4");
      drawSeries(carrierReselPciHistory, "#87ceeb", "#c8ecff");

      ctx.font = "11px Arial";
      ctx.fillStyle = "#87ceeb";
      ctx.fillRect(w - 118, 8, 10, 3);
      ctx.fillStyle = "#dff4ff";
      ctx.fillText("PCI /min", w - 104, 12);
      ctx.fillStyle = "#ff8ec8";
      ctx.fillRect(w - 118, 20, 10, 3);
      ctx.fillStyle = "#ffe8f2";
      ctx.fillText("EARFCN /min", w - 104, 24);
    }

    function drawIperfChart() {
      const canvas = el("iperfchart");
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      const all = [...iperfDlHistory, ...iperfUlHistory];
      if (!all.length) {
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No iperf samples yet", 12, 24);
        return;
      }

      const values = all.map((p) => Number(p?.v)).filter((x) => Number.isFinite(x));
      const minV = Math.min(...values);
      const maxV = Math.max(...values);
      const pad = Math.max(1, (maxV - minV) * 0.15);
      const yMin = Math.max(0, minV - pad);
      const yMax = maxV + pad;
      const span = Math.max(1, yMax - yMin);

      ctx.strokeStyle = "#2a2a2a";
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i++) {
        const y = 10 + (i * (h - 20)) / 4;
        ctx.beginPath();
        ctx.moveTo(44, y);
        ctx.lineTo(w - 12, y);
        ctx.stroke();
      }

      ctx.fillStyle = "#aaa";
      ctx.font = "11px Arial";
      ctx.fillText(`${yMax.toFixed(1)} Mbps`, 4, 14);
      ctx.fillText(`${yMin.toFixed(1)} Mbps`, 4, h - 8);

      const x0 = 44;
      const x1 = w - 12;
      const y0 = h - 12;
      const y1 = 10;
      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const xFor = (p) => {
        const t = Number(p?.t);
        if (!Number.isFinite(t)) return x0;
        const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / chartWindowMs));
        return x0 + ratio * (x1 - x0);
      };
      const yFor = (v) => y0 - ((v - yMin) / span) * (y0 - y1);

      const drawSeries = (samples, lineColor, pointColor) => {
        if (!samples.length) return;
        const sorted = [...samples].sort((a, b) => Number(a.t) - Number(b.t));
        ctx.strokeStyle = lineColor;
        ctx.lineWidth = 2;
        for (let i = 1; i < sorted.length; i++) {
          const p0 = sorted[i - 1];
          const p1 = sorted[i];
          const xA = xFor(p0);
          const yA = yFor(p0.v);
          const xB = xFor(p1);
          const yB = yFor(p1.v);
          ctx.beginPath();
          ctx.moveTo(xA, yA);
          ctx.lineTo(xB, yB);
          ctx.stroke();
        }
        ctx.fillStyle = pointColor;
        sorted.forEach((p) => {
          const x = xFor(p);
          const y = yFor(p.v);
          ctx.beginPath();
          ctx.arc(x, y, 2.1, 0, Math.PI * 2);
          ctx.fill();
        });
      };

      drawSeries(iperfDlHistory, "#00e5ff", "#8cf7ff");
      drawSeries(iperfUlHistory, "#ffb86c", "#ffd7ad");

      // Legend
      ctx.font = "11px Arial";
      ctx.fillStyle = "#00e5ff";
      ctx.fillRect(w - 122, 10, 10, 3);
      ctx.fillStyle = "#cdefff";
      ctx.fillText("DL", w - 108, 15);
      ctx.fillStyle = "#ffb86c";
      ctx.fillRect(w - 70, 10, 10, 3);
      ctx.fillStyle = "#ffe2c3";
      ctx.fillText("UL", w - 56, 15);
    }

    function drawSingleGauge(canvasId, valueMbps, color, label) {
      const canvas = el(canvasId);
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      const cx = w / 2;
      const cy = h - 14;
      const r = Math.min(w * 0.44, h * 0.78);
      const start = Math.PI;
      const end = 2 * Math.PI;
      const span = end - start;
      const finiteVals = iperfHistory.map((x) => Number(x?.v)).filter((x) => Number.isFinite(x) && x >= 0);
      const histMax = finiteVals.length ? Math.max(...finiteVals) : 10;
      const gaugeMax = Math.max(10, Math.ceil(histMax / 5) * 5);
      const v = Number.isFinite(valueMbps) && valueMbps >= 0 ? valueMbps : null;
      const ratio = v === null ? 0 : Math.max(0, Math.min(1, v / gaugeMax));

      ctx.lineWidth = 10;
      ctx.strokeStyle = "#2a2a2a";
      ctx.beginPath();
      ctx.arc(cx, cy, r, start, end, false);
      ctx.stroke();

      if (v !== null) {
        ctx.strokeStyle = color;
        ctx.beginPath();
        ctx.arc(cx, cy, r, start, start + span * ratio, false);
        ctx.stroke();
      }

      const valueText = v === null ? "-" : `${v.toFixed(2)} Mbps`;
      ctx.fillStyle = "#e6e6e6";
      ctx.font = "bold 14px Arial";
      const valueW = ctx.measureText(valueText).width;
      ctx.fillText(valueText, cx - valueW / 2, cy - 16);

      ctx.fillStyle = "#999";
      ctx.font = "11px Arial";
      const minText = "0";
      const maxText = `${gaugeMax}`;
      ctx.fillText(minText, cx - r + 2, cy + 1);
      const maxW = ctx.measureText(maxText).width;
      ctx.fillText(maxText, cx + r - maxW - 2, cy + 1);
      const lblW = ctx.measureText(label).width;
      ctx.fillText(label, cx - lblW / 2, cy - r - 6);
    }

    function drawIperfGauges() {
      drawSingleGauge("iperf-dl-gauge", lastIperfDlMbps, "#00e5ff", "Download");
      drawSingleGauge("iperf-ul-gauge", lastIperfUlMbps, "#ffb86c", "Upload");
      const dlTxt = Number.isFinite(lastIperfDlMbps) ? `${lastIperfDlMbps.toFixed(3)} Mbps` : "-";
      const ulTxt = Number.isFinite(lastIperfUlMbps) ? `${lastIperfUlMbps.toFixed(3)} Mbps` : "-";
      const note = el("iperf-gauge-note");
      if (note) note.textContent = `Latest results: DL ${dlTxt}, UL ${ulTxt}`;
    }

    function drawPhSweepChart() {
      const canvas = el("ph-sweep-chart");
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      const all = [...phAvgHistory, ...phJitHistory];
      if (!all.length) {
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No ICMP sweep samples yet", 12, 24);
        return;
      }

      const values = all.map((p) => Number(p?.v)).filter((x) => Number.isFinite(x));
      const minV = Math.min(...values);
      const maxV = Math.max(...values);
      const pad = Math.max(0.5, (maxV - minV) * 0.15);
      const yMin = Math.max(0, minV - pad);
      const yMax = maxV + pad;
      const span = Math.max(1, yMax - yMin);

      ctx.strokeStyle = "#2a2a2a";
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i++) {
        const y = 10 + (i * (h - 20)) / 4;
        ctx.beginPath();
        ctx.moveTo(44, y);
        ctx.lineTo(w - 12, y);
        ctx.stroke();
      }

      ctx.fillStyle = "#aaa";
      ctx.font = "11px Arial";
      ctx.fillText(`${yMax.toFixed(1)} ms`, 4, 14);
      ctx.fillText(`${yMin.toFixed(1)} ms`, 4, h - 8);

      const x0 = 44;
      const x1 = w - 12;
      const y0 = h - 12;
      const y1 = 10;
      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const xFor = (p) => {
        const t = Number(p?.t);
        if (!Number.isFinite(t)) return x0;
        const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / chartWindowMs));
        return x0 + ratio * (x1 - x0);
      };
      const yFor = (v) => y0 - ((v - yMin) / span) * (y0 - y1);

      const drawSeries = (samples, lineColor, pointColor) => {
        if (!samples.length) return;
        const sorted = [...samples].sort((a, b) => Number(a.t) - Number(b.t));
        ctx.strokeStyle = lineColor;
        ctx.lineWidth = 2;
        for (let i = 1; i < sorted.length; i++) {
          const p0 = sorted[i - 1];
          const p1 = sorted[i];
          const xA = xFor(p0);
          const yA = yFor(p0.v);
          const xB = xFor(p1);
          const yB = yFor(p1.v);
          ctx.beginPath();
          ctx.moveTo(xA, yA);
          ctx.lineTo(xB, yB);
          ctx.stroke();
        }
        ctx.fillStyle = pointColor;
        sorted.forEach((p) => {
          const x = xFor(p);
          const y = yFor(p.v);
          ctx.beginPath();
          ctx.arc(x, y, 2.1, 0, Math.PI * 2);
          ctx.fill();
        });
      };

      drawSeries(phAvgHistory, "#7cffd4", "#c6fff0");
      drawSeries(phJitHistory, "#ff9edb", "#ffd6ef");

      ctx.font = "11px Arial";
      ctx.fillStyle = "#7cffd4";
      ctx.fillRect(w - 148, 10, 10, 3);
      ctx.fillStyle = "#e8fff8";
      ctx.fillText("Avg RTT", w - 132, 15);
      ctx.fillStyle = "#ff9edb";
      ctx.fillRect(w - 62, 10, 10, 3);
      ctx.fillStyle = "#ffeaf7";
      ctx.fillText("Jitter", w - 46, 15);
    }

    function drawLatencyGauge(canvasId, valueMs, color, label, scaleHistory) {
      const canvas = el(canvasId);
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      const cx = w / 2;
      const cy = h - 14;
      const r = Math.min(w * 0.44, h * 0.78);
      const start = Math.PI;
      const end = 2 * Math.PI;
      const arcSpan = end - start;
      const finiteVals = (scaleHistory || []).map((x) => Number(x?.v)).filter((x) => Number.isFinite(x) && x >= 0);
      const histMax = finiteVals.length ? Math.max(...finiteVals) : 50;
      const gaugeMax = Math.max(5, Math.ceil(histMax / 5) * 5);
      const v = Number.isFinite(valueMs) && valueMs >= 0 ? valueMs : null;
      const ratio = v === null ? 0 : Math.max(0, Math.min(1, v / gaugeMax));

      ctx.lineWidth = 10;
      ctx.strokeStyle = "#2a2a2a";
      ctx.beginPath();
      ctx.arc(cx, cy, r, start, end, false);
      ctx.stroke();

      if (v !== null) {
        ctx.strokeStyle = color;
        ctx.beginPath();
        ctx.arc(cx, cy, r, start, start + arcSpan * ratio, false);
        ctx.stroke();
      }

      const valueText = v === null ? "-" : `${v.toFixed(2)} ms`;
      ctx.fillStyle = "#e6e6e6";
      ctx.font = "bold 14px Arial";
      const valueW = ctx.measureText(valueText).width;
      ctx.fillText(valueText, cx - valueW / 2, cy - 16);

      ctx.fillStyle = "#999";
      ctx.font = "11px Arial";
      ctx.fillText("0", cx - r + 2, cy + 1);
      const maxText = `${gaugeMax}`;
      const maxW = ctx.measureText(maxText).width;
      ctx.fillText(maxText, cx + r - maxW - 2, cy + 1);
      const lblW = ctx.measureText(label).width;
      ctx.fillText(label, cx - lblW / 2, cy - r - 6);
    }

    function drawPhGauges() {
      drawLatencyGauge("ph-lat-gauge", lastPhAvgMs, "#7cffd4", "Avg RTT", phAvgHistory);
      drawLatencyGauge("ph-jit-gauge", lastPhJitMs, "#ff9edb", "Jitter", phJitHistory);
      const aTxt = Number.isFinite(lastPhAvgMs) ? `${lastPhAvgMs.toFixed(2)} ms` : "-";
      const jTxt = Number.isFinite(lastPhJitMs) ? `${lastPhJitMs.toFixed(2)} ms` : "-";
      const note = el("ph-gauge-note");
      if (note) note.textContent = `Latest sweep: avg ${aTxt}, jitter ${jTxt} (gauge scale from recent runs).`;
    }

    function drawMetricChart(canvasId, samples, unitLabel, color, thresholdValue = null) {
      const canvas = el(canvasId);
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      if (!samples.length) {
        canvas._metricHover = null;
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No samples yet", 12, 24);
        return;
      }

      const values = samples.map((p) => p.v);
      const minV = Math.min(...values);
      const maxV = Math.max(...values);
      const pad = Math.max(1, (maxV - minV) * 0.15);
      let yMin = minV - pad;
      let yMax = maxV + pad;
      if (Number.isFinite(thresholdValue)) {
        yMin = Math.min(yMin, Number(thresholdValue) - 0.5);
        yMax = Math.max(yMax, Number(thresholdValue) + 0.5);
      }
      const span = Math.max(1, yMax - yMin);

      ctx.strokeStyle = "#2a2a2a";
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i++) {
        const y = 10 + (i * (h - 20)) / 4;
        ctx.beginPath();
        ctx.moveTo(40, y);
        ctx.lineTo(w - 8, y);
        ctx.stroke();
      }

      ctx.fillStyle = "#aaa";
      ctx.font = "11px Arial";
      ctx.fillText(`${yMax.toFixed(1)} ${unitLabel}`, 4, 14);
      ctx.fillText(`${yMin.toFixed(1)} ${unitLabel}`, 4, h - 8);

      const n = samples.length;
      const x0 = 44;
      const x1 = w - 12;
      const y0 = h - 12;
      const y1 = 10;
      const xStep = n > 1 ? (x1 - x0) / (n - 1) : 0;
      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const expectedStepMs = Math.max(200, 1000 / Math.max(0.1, Number(currentPollHz) || 2));
      const gapBreakMs = expectedStepMs * 1.8;
      const xFor = (p, i) => {
        if (!chartGapModeEnabled) return x0 + i * xStep;
        const t = Number(p?.t);
        if (!Number.isFinite(t)) return x0 + i * xStep;
        const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / chartWindowMs));
        return x0 + ratio * (x1 - x0);
      };

      if (Number.isFinite(thresholdValue)) {
        const yThreshold = y0 - ((Number(thresholdValue) - yMin) / span) * (y0 - y1);
        ctx.strokeStyle = "#ff4d4f";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x0, yThreshold);
        ctx.lineTo(x1, yThreshold);
        ctx.stroke();
      }

      const sampleColor = (p) => colorForCellKey(p?.c, color);
      ctx.lineWidth = 2;
      for (let i = 1; i < samples.length; i++) {
        const p0 = samples[i - 1];
        const p1 = samples[i];
        const t0 = Number(p0?.t);
        const t1 = Number(p1?.t);
        if (chartGapModeEnabled && Number.isFinite(t0) && Number.isFinite(t1) && (t1 - t0) > gapBreakMs) continue;
        const xA = xFor(p0, i - 1);
        const yA = y0 - ((p0.v - yMin) / span) * (y0 - y1);
        const xB = xFor(p1, i);
        const yB = y0 - ((p1.v - yMin) / span) * (y0 - y1);
        ctx.strokeStyle = sampleColor(p1);
        ctx.beginPath();
        ctx.moveTo(xA, yA);
        ctx.lineTo(xB, yB);
        ctx.stroke();
      }

      samples.forEach((p, i) => {
        const x = xFor(p, i);
        const y = y0 - ((p.v - yMin) / span) * (y0 - y1);
        ctx.fillStyle = sampleColor(p);
        ctx.beginPath();
        ctx.arc(x, y, 2.1, 0, Math.PI * 2);
        ctx.fill();
      });

      canvas._metricHover = {
        canvasId,
        samples,
        unitLabel,
        x0,
        x1,
        y0,
        y1,
        yMin,
        yMax,
        span,
        gapBreakMs,
        chartNowMs: nowMs,
        cwMs: chartWindowMs,
        gapMode: chartGapModeEnabled
      };
    }

    function metricHoverXFor(p, i, h) {
      const { x0, x1, samples, cwMs, chartNowMs, gapMode } = h;
      const n = samples.length;
      const xStep = n > 1 ? (x1 - x0) / (n - 1) : 0;
      const windowStartMs = chartNowMs - cwMs;
      if (!gapMode) return x0 + i * xStep;
      const t = Number(p?.t);
      if (!Number.isFinite(t)) return x0 + i * xStep;
      const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / cwMs));
      return x0 + ratio * (x1 - x0);
    }

    function ensureRfChartTooltipEl() {
      if (rfChartTooltipEl) return rfChartTooltipEl;
      rfChartTooltipEl = document.createElement("div");
      rfChartTooltipEl.id = "rf-chart-tooltip";
      rfChartTooltipEl.setAttribute("role", "tooltip");
      rfChartTooltipEl.style.cssText = [
        "position:fixed",
        "display:none",
        "z-index:99999",
        "pointer-events:none",
        "background:#252525",
        "border:1px solid #444",
        "border-radius:6px",
        "padding:6px 10px",
        "font:12px Consolas,monospace",
        "color:#eee",
        "box-shadow:0 2px 8px rgba(0,0,0,0.45)",
        "white-space:pre-line",
        "max-width:280px",
        "line-height:1.35"
      ].join(";");
      document.body.appendChild(rfChartTooltipEl);
      return rfChartTooltipEl;
    }

    function hideRfChartTooltip() {
      if (rfChartTooltipEl) rfChartTooltipEl.style.display = "none";
    }

    function showRfChartTooltip(clientX, clientY, title, valueStr, unitLabel, cellKey) {
      const tip = ensureRfChartTooltipEl();
      const ck =
        cellKey !== null && cellKey !== undefined && String(cellKey).trim() !== ""
          ? String(cellKey).trim()
          : "—";
      tip.textContent = `${title}\\n${valueStr} ${unitLabel}\\nEARFCN/PCI: ${ck}`;
      tip.style.display = "block";
      const pad = 14;
      let left = clientX + pad;
      let top = clientY + pad;
      const tw = tip.offsetWidth;
      const th = tip.offsetHeight;
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      if (left + tw > vw - 8) left = vw - tw - 8;
      if (top + th > vh - 8) top = vh - th - 8;
      tip.style.left = `${Math.max(8, left)}px`;
      tip.style.top = `${Math.max(8, top)}px`;
    }

    function handleRfMetricChartHoverMove(ev) {
      const canvas = ev.currentTarget;
      if (!canvas || !RF_HOVER_CANVAS_IDS.includes(canvas.id)) return;
      const hover = canvas._metricHover;
      if (!hover || !Array.isArray(hover.samples) || hover.samples.length === 0) {
        hideRfChartTooltip();
        return;
      }
      const rect = canvas.getBoundingClientRect();
      const scaleX = canvas.width / rect.width;
      const scaleY = canvas.height / rect.height;
      const mx = (ev.clientX - rect.left) * scaleX;
      const my = (ev.clientY - rect.top) * scaleY;
      const { samples, y0, y1, yMin, span } = hover;
      let best = null;
      let bestD = Infinity;
      const hitR = 22;
      const hitR2 = hitR * hitR;
      for (let i = 0; i < samples.length; i++) {
        const p = samples[i];
        const x = metricHoverXFor(p, i, hover);
        const y = y0 - ((p.v - yMin) / span) * (y0 - y1);
        const dx = mx - x;
        const dy = my - y;
        const d2 = dx * dx + dy * dy;
        if (d2 < bestD) {
          bestD = d2;
          best = { p, v: Number(p?.v) };
        }
      }
      if (!best || bestD > hitR2) {
        hideRfChartTooltip();
        return;
      }
      const title = RF_CHART_TITLE_BY_ID[canvas.id] || canvas.id;
      const shown =
        Number.isFinite(best.v) ? (Math.abs(best.v % 1) < 0.05 ? best.v.toFixed(1) : best.v.toFixed(2)) : "-";
      showRfChartTooltip(ev.clientX, ev.clientY, title, shown, hover.unitLabel || "", best.p?.c);
    }

    function handleRfMetricChartHoverLeave() {
      hideRfChartTooltip();
    }

    function installRfChartHoverListeners() {
      for (const id of RF_HOVER_CANVAS_IDS) {
        const canvas = el(id);
        if (!canvas || canvas._rfHoverInstalled) continue;
        canvas._rfHoverInstalled = true;
        canvas.addEventListener("mousemove", handleRfMetricChartHoverMove);
        canvas.addEventListener("mouseleave", handleRfMetricChartHoverLeave);
      }
    }

    function drawRfCharts() {
      const rsrp = rfSmoothingEnabled ? smoothSeries(rfHistory.rsrp, RF_SMOOTH_WINDOW) : rfHistory.rsrp;
      const rsrq = rfSmoothingEnabled ? smoothSeries(rfHistory.rsrq, RF_SMOOTH_WINDOW) : rfHistory.rsrq;
      const sinr = rfSmoothingEnabled ? smoothSeries(rfHistory.sinr, RF_SMOOTH_WINDOW) : rfHistory.sinr;
      const rssi = rfSmoothingEnabled ? smoothSeries(rfHistory.rssi, RF_SMOOTH_WINDOW) : rfHistory.rssi;
      const dominanceSource = rfSmoothingEnabled ? smoothSeries(rfHistory.dominance, RF_SMOOTH_WINDOW) : rfHistory.dominance;
      const dominance = primaryCellDataAvailable ? dominanceSource : [];
      const currentCellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      const pciColor = colorForCellKey(currentCellKey, "#4da3ff");
      drawMetricChart("rsrpchart", rsrp, "dBm", pciColor, -105);
      drawMetricChart("rsrqchart", rsrq, "dB", pciColor, -15);
      drawMetricChart("sinrchart", sinr, "dB", pciColor, 0);
      drawMetricChart("rssichart", rssi, "dBm", pciColor, -25);
      drawMetricChart("dominancechart", dominance, "dB", "#50fa7b", 6);
    }

    function drawBwChart() {
      const currentCellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      const cellColor = colorForCellKey(currentCellKey, "#00d1b2");
      drawMetricChart("bwchart", bwHistory, "MHz", cellColor);
    }

    function drawPciChart() {
      const currentCellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      const cellColor = colorForCellKey(currentCellKey, "#ff7f50");
      drawMetricChart("pcichart", pciHistory, "", cellColor);
    }

    function drawNeighbourCharts() {
      const currentCellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      const cellColor = colorForCellKey(currentCellKey, "#61dafb");
      drawMetricChart("nbrsrpchart", neighbourHistory.rsrp, "dBm", cellColor);
      drawMetricChart("nbpcichart", neighbourHistory.pci, "", cellColor);
    }

    function drawCategoryChart(canvasId, samples, color) {
      const canvas = el(canvasId);
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      if (!samples.length) {
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No samples yet", 12, 24);
        return;
      }

      const labels = [];
      for (const s of samples) {
        if (!labels.includes(s.v)) labels.push(s.v);
      }
      const levels = Math.max(1, labels.length - 1);
      const leftPad = 92;
      const rightPad = 12;
      const topPad = 10;
      const bottomPad = 12;
      const x0 = leftPad;
      const x1 = w - rightPad;
      const y0 = h - bottomPad;
      const y1 = topPad;

      ctx.strokeStyle = "#2a2a2a";
      ctx.lineWidth = 1;
      labels.forEach((lbl, idx) => {
        const y = y0 - (idx / Math.max(1, levels)) * (y0 - y1);
        ctx.beginPath();
        ctx.moveTo(x0, y);
        ctx.lineTo(x1, y);
        ctx.stroke();
        ctx.fillStyle = "#aaa";
        ctx.font = "11px Arial";
        const shown = lbl.length > 12 ? `${lbl.slice(0, 12)}...` : lbl;
        ctx.fillText(shown, 4, y + 4);
      });

      const n = samples.length;
      const xStep = n > 1 ? (x1 - x0) / (n - 1) : 0;
      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const expectedStepMs = Math.max(200, 1000 / Math.max(0.1, Number(currentPollHz) || 2));
      const gapBreakMs = expectedStepMs * 1.8;
      const xFor = (p, i) => {
        if (!chartGapModeEnabled) return x0 + i * xStep;
        const t = Number(p?.t);
        if (!Number.isFinite(t)) return x0 + i * xStep;
        const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / chartWindowMs));
        return x0 + ratio * (x1 - x0);
      };
      const sampleColor = (p) => colorForCellKey(p?.c, color);
      ctx.lineWidth = 2;
      for (let i = 1; i < samples.length; i++) {
        const p0 = samples[i - 1];
        const p1 = samples[i];
        const t0 = Number(p0?.t);
        const t1 = Number(p1?.t);
        if (chartGapModeEnabled && Number.isFinite(t0) && Number.isFinite(t1) && (t1 - t0) > gapBreakMs) continue;
        const idx0 = labels.indexOf(p0.v);
        const idx1 = labels.indexOf(p1.v);
        const y0p = y0 - (idx0 / Math.max(1, levels)) * (y0 - y1);
        const y1p = y0 - (idx1 / Math.max(1, levels)) * (y0 - y1);
        const xA = xFor(p0, i - 1);
        const xB = xFor(p1, i);
        ctx.strokeStyle = sampleColor(p1);
        ctx.beginPath();
        ctx.moveTo(xA, y0p);
        ctx.lineTo(xB, y1p);
        ctx.stroke();
      }

      samples.forEach((p, i) => {
        const idx = labels.indexOf(p.v);
        const y = y0 - (idx / Math.max(1, levels)) * (y0 - y1);
        const x = xFor(p, i);
        ctx.fillStyle = sampleColor(p);
        ctx.beginPath();
        ctx.arc(x, y, 2.1, 0, Math.PI * 2);
        ctx.fill();
      });
    }

    function drawCategoryCharts() {
      const currentCellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      const cellColor = colorForCellKey(currentCellKey, "#8be9fd");
      drawCategoryChart("statechart", categoryHistory.state, cellColor);
      drawCategoryChart("bandchart", categoryHistory.band, cellColor);
    }

    function clearDataServiceKpi() {
      lastDataService = {};
      el("ds-apn").textContent = "-";
      el("ds-pdp").textContent = "-";
      el("ds-cid1").textContent = "-";
      el("ds-cid1").className = "";
      el("ds-ip").textContent = "-";
      el("ds-attach").textContent = "-";
      el("ds-attach").className = "";
      el("ds-reg").textContent = "-";
      el("ds-reg").className = "";
      el("ds-usbnet").textContent = "-";
      el("ds-netdev").textContent = "-";
      el("ds-warn").textContent = "-";
      el("ds-warn").className = "label";
      el("ds-apn-msg").textContent = "-";
      el("ds-apn-msg").className = "label";
    }

    function clearAllCharts() {
      iperfHistory.length = 0;
      iperfDlHistory.length = 0;
      iperfUlHistory.length = 0;
      lastIperfDlMbps = null;
      lastIperfUlMbps = null;
      phAvgHistory.length = 0;
      phJitHistory.length = 0;
      lastPhAvgMs = null;
      lastPhJitMs = null;
      rfHistory.rsrp.length = 0;
      rfHistory.rsrq.length = 0;
      rfHistory.sinr.length = 0;
      rfHistory.rssi.length = 0;
      rfHistory.dominance.length = 0;
      bwHistory.length = 0;
      pciHistory.length = 0;
      neighbourHistory.rsrp.length = 0;
      neighbourHistory.pci.length = 0;
      carrierReselPciHistory.length = 0;
      carrierReselEarfcnHistory.length = 0;
      categoryHistory.state.length = 0;
      categoryHistory.band.length = 0;
      drawIperfChart();
      drawIperfGauges();
      drawPhSweepChart();
      drawPhGauges();
      drawRfCharts();
      drawBwChart();
      drawPciChart();
      drawNeighbourCharts();
      drawCarrierReselChart();
      drawCategoryCharts();
      clearDataServiceKpi();
    }

    async function pollFallback() {
      try {
        const r = await fetch("/api/kpi/latest");
        if (!r.ok) return;
        applySnap(await r.json());
      } catch (_) {}
    }

    async function pollAtLog() {
      try {
        const r = await fetch("/api/at/log?limit=400");
        if (!r.ok) return;
        const j = await r.json();
        const lines = Array.isArray(j.lines) ? j.lines : [];
        const host = el("atlog");
        host.textContent = lines.length ? lines.join("\\n") : "-";
        host.scrollTop = host.scrollHeight;
      } catch (_) {}
    }

    const wsProto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${wsProto}//${location.host}/ws/kpi`);
    ws.onopen = () => { el("status").textContent = "WebSocket connected."; el("status").className = "label ok"; };
    ws.onmessage = (ev) => { try { applySnap(JSON.parse(ev.data)); } catch (_) {} };
    ws.onclose = () => { el("status").textContent = "WebSocket disconnected; polling fallback."; el("status").className = "label warn"; };

    el("btn-cops-read").addEventListener("click", () => readCops());
    el("btn-cops-scan").addEventListener("click", () => scanCops());
    el("btn-cops-auto").addEventListener("click", () => setCops(0));
    el("btn-cops-dereg").addEventListener("click", () => setCops(2));
    el("btn-lock-read").addEventListener("click", () => readLocks());
    el("btn-lock-set").addEventListener("click", () => setLocks());
    el("btn-mno-read").addEventListener("click", () => readMnoState("MNO state read OK"));
    el("btn-mno-apply").addEventListener("click", () => applyMnoSelection());
    el("btn-data-inhibit").addEventListener("click", () => setDataGate(true));
    el("btn-data-allow").addEventListener("click", () => setDataGate(false));
    const btnDsApn = el("btn-ds-apn-apply");
    if (btnDsApn) btnDsApn.addEventListener("click", () => applyDsApn());
    el("btn-sim-high-read").addEventListener("click", () => readSimHighLevel());
    el("btn-sim-inspect-read").addEventListener("click", () => readSimInspector());
    el("btn-volte-test").addEventListener("click", () => runVolteTest());
    el("btn-iperf-test").addEventListener("click", () => runIperfTest());
    el("iperf-bind-select").addEventListener("change", () => syncIperfBindUi());
    el("btn-iperf-refresh-ifaces").addEventListener("click", () => loadBindInterfaces());
    const phBindSel = el("ph-bind-select");
    if (phBindSel) phBindSel.addEventListener("change", () => syncPhBindUi());
    const btnPhRefresh = el("btn-ph-refresh-ifaces");
    if (btnPhRefresh) btnPhRefresh.addEventListener("click", () => loadBindInterfaces());
    const btnPhRun = el("btn-ph-run");
    if (btnPhRun) btnPhRun.addEventListener("click", () => runPingSweepTest());
    const phRepeatToggle = el("ph-repeat-toggle");
    if (phRepeatToggle) phRepeatToggle.addEventListener("change", (ev) => setPhRepeatPing(!!ev.target.checked));
    el("btn-clear-charts").addEventListener("click", () => clearAllCharts());
    el("btn-chart-gap-mode").addEventListener("click", () => setChartGapMode(!chartGapModeEnabled));
    el("chart-window-select").addEventListener("change", (ev) => {
      applyChartWindowSec(Number(ev.target?.value || 60));
    });
    el("rf-smooth-toggle").addEventListener("change", (ev) => {
      rfSmoothingEnabled = !!ev.target.checked;
      drawRfCharts();
    });
    el("btn-serial-refresh").addEventListener("click", () => refreshSerialPorts(false));
    el("btn-serial-autopick").addEventListener("click", () => autoPickSerialPort());
    el("btn-serial-reconnect").addEventListener("click", () => reconnectSerial());
    el("btn-modem-reset").addEventListener("click", () => resetModem());

    setInterval(pollFallback, 2000);
    setInterval(pollAtLog, 1200);
    setInterval(() => readSerialStatus(false), 3000);
    setInterval(() => {
      if (!chartGapModeEnabled) return;
      pruneAllHistory(Date.now());
      redrawAllCharts();
    }, 400);
    pollFallback();
    pollAtLog();
    readSerialStatus(true);
    refreshSerialPorts(true);
    readCops();
    readLocks();
    readMnoState();
    readDataGate();
    readSimHighLevel();
    applyChartWindowSec(Number(el("chart-window-select")?.value || 60));
    updateChartGapButton();
    redrawAllCharts();
    loadBindInterfaces();
    installRfChartHoverListeners();
  </script>
</body>
</html>"""
    return HTMLResponse(content=html.replace("__APP_VERSION__", APP_VERSION))


@app.get("/api/serial/status")
async def serial_status() -> dict:
    return await engine.status()


@app.get("/api/serial/ports")
async def serial_ports() -> dict:
    items = list_ports.comports()
    ports = [
        {
            "device": p.device,
            "description": p.description,
            "hwid": p.hwid,
            "manufacturer": p.manufacturer,
            "product": p.product,
            "serial_number": p.serial_number,
        }
        for p in items
    ]
    ports.sort(key=lambda x: x.get("device") or "")
    return {"ok": True, "ports": ports}


@app.post("/api/at/send")
async def send_at(body: SendAtBody) -> dict:
    try:
        return await engine.send_command(body.command, timeout_sec=body.timeout_sec)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"AT command failed: {exc}") from exc


@app.get("/api/at/log")
async def at_log(limit: int = 120) -> dict:
    return await engine.at_log(limit=limit)


@app.get("/api/sim/high-level")
async def sim_high_level() -> dict:
    imei_res = await engine.send_command("AT+CGSN", timeout_sec=4.0)
    cimi_res = await engine.send_command("AT+CIMI", timeout_sec=4.0)
    qspn_res = await engine.send_command("AT+QSPN", timeout_sec=4.0)
    cops_res = await engine.send_command("AT+COPS?", timeout_sec=4.0)
    cpol_res = await engine.send_command("AT+CPOL?", timeout_sec=8.0)

    imei = _first_payload_line(imei_res.get("lines", []))
    imsi = _first_payload_line(cimi_res.get("lines", []))
    spn = _parse_qspn(qspn_res.get("lines", []))
    cops = _parse_cops_lines(cops_res.get("lines", []))
    cpol_entries = _parse_cpol(cpol_res.get("lines", []))

    return {
        "ok": True,
        "imei": imei,
        "imsi": imsi,
        "spn": spn,
        "cops": cops,
        "cpol_count": len(cpol_entries),
        "cpol_entries": cpol_entries,
        "raw": {
            "imei": imei_res,
            "cimi": cimi_res,
            "qspn": qspn_res,
            "cops": cops_res,
            "cpol": cpol_res,
        },
    }


@app.get("/api/sim/inspector")
async def sim_inspector(verbose: bool = False) -> dict:
    # Read-only EF inspection via CRSM.
    files = [
        ("ef_plmnwact", "EF_PLMNwAcT", 28512),
        ("ef_oplmnwact", "EF_OPLMNwAcT", 28513),
        ("ef_hplmn", "EF_HPLMN", 28465),
        ("ef_fplmn", "EF_FPLMN", 28539),
        ("ef_spdi", "EF_SPDI", 28621),
        ("ef_ad", "EF_AD", 28589),
        ("ef_ehplmn", "EF_EHPLMN", 28633),
        ("ef_ust", "EF_UST", 28472),
        ("ef_pnn", "EF_PNN", 28613),
        ("ef_opl", "EF_OPL", 28614),
        ("ef_epsloci", "EF_EPSLOCI", 28643),
        ("ef_5gsloci", "EF_5GSLOCI", 20225),
    ]
    decoded: dict[str, dict] = {}
    raw: dict[str, dict] = {}

    for key, name, fileid in files:
        cmd = f"AT+CRSM=176,{fileid},0,0,0"
        res = await engine.send_command(cmd, timeout_sec=6.0)
        raw[key] = res
        crsm = _parse_crsm_hex(res.get("lines", []))
        if key in {"ef_plmnwact", "ef_oplmnwact"}:
            entries = _decode_plmn_file(crsm.get("hex", ""), with_act=True)
            decoded[key] = {"name": name, "fileid": fileid, "entries": entries, "count": len(entries), **crsm}
        elif key == "ef_fplmn":
            entries = _decode_plmn_file(crsm.get("hex", ""), with_act=False)
            decoded[key] = {"name": name, "fileid": fileid, "entries": entries, "count": len(entries), **crsm}
        elif key == "ef_ehplmn":
            entries = _decode_plmn_file(crsm.get("hex", ""), with_act=False)
            decoded[key] = {"name": name, "fileid": fileid, "entries": entries, "count": len(entries), **crsm}
        elif key == "ef_ad":
            decoded[key] = {
                "name": name,
                "fileid": fileid,
                "mnc_length": _decode_mnc_len_from_ad(crsm.get("hex", "")),
                **crsm,
            }
        elif key == "ef_hplmn":
            decoded[key] = {
                "name": name,
                "fileid": fileid,
                "hplmn_search_timer_min": _decode_hplmn_timer_minutes(crsm.get("hex", "")),
                **crsm,
            }
        elif key == "ef_ust":
            enabled = _decode_ust_enabled_services(crsm.get("hex", ""))
            decoded[key] = {
                "name": name,
                "fileid": fileid,
                "enabled_services_count": len(enabled),
                "enabled_services": enabled,
                "enabled_services_verbose": [
                    {"service_no": sid, "label": label_usim_service(sid)} for sid in enabled
                ],
                **crsm,
            }
        else:
            # Keep raw hex for files that require BER-TLV-specific decoding.
            decoded[key] = {"name": name, "fileid": fileid, **crsm}

        if verbose:
            desc = SIM_EF_DESCRIPTIONS.get(key)
            if desc:
                decoded[key]["description"] = desc

    if verbose:
        eo = decoded.get("ef_epsloci")
        if isinstance(eo, dict):
            hx = eo.get("hex") or ""
            clean = re.sub(r"[^0-9A-Fa-f]", "", str(hx)).upper()
            if clean:
                eo["hex_byte_length"] = len(clean) // 2

    out: dict = {
        "ok": True,
        "verbose": verbose,
        "decoded": decoded,
        "raw": raw,
    }
    if verbose:
        out["label_reference"] = SIM_INSPECTOR_LABEL_REFERENCE
    return out


@app.post("/api/serial/reopen")
async def reopen_serial(body: ReopenBody) -> dict:
    try:
        await engine.reopen(body.port, body.baudrate)
        st = await engine.status()
        if st.get("serial_open"):
            _save_last_serial_state(str(st.get("port") or body.port), int(st.get("baudrate") or body.baudrate))
        return {"ok": True, **st}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to reopen serial: {exc}") from exc


@app.get("/api/kpi/latest")
async def kpi_latest() -> dict:
    async with kpi_runtime.lock:
        return {
            "ok": True,
            "poll_running": kpi_runtime.poll_running,
            "poll_hz": kpi_runtime.poll_hz,
            "last_error": kpi_runtime.last_error,
            "sample": kpi_runtime.snapshot,
        }


@app.post("/api/kpi/poll")
async def kpi_poll_config(body: KpiPollBody) -> dict:
    async with kpi_runtime.lock:
        kpi_runtime.poll_hz = body.poll_hz
    return await kpi_latest()


@app.post("/api/kpi/poll/start")
async def kpi_poll_start() -> dict:
    global _kpi_task
    if _kpi_task is None or _kpi_task.done():
        kpi_runtime.poll_running = True
        _kpi_task = asyncio.create_task(kpi_poll_loop(engine, kpi_runtime))
    return await kpi_latest()


@app.post("/api/kpi/poll/stop")
async def kpi_poll_stop() -> dict:
    kpi_runtime.poll_running = False
    return await kpi_latest()


async def _poll_cops_state(total_wait_sec: float = 60.0, step_sec: float = 2.0) -> dict:
    deadline = asyncio.get_running_loop().time() + max(2.0, float(total_wait_sec))
    last_res: dict = {"ok": False, "lines": [], "final": "NO_READ"}
    last_cops: dict = {}
    while asyncio.get_running_loop().time() < deadline:
        res = await engine.send_command("AT+COPS?", timeout_sec=6.0)
        last_res = res
        cops = _parse_cops_lines(res.get("lines", []))
        if cops:
            last_cops = cops
            # If operator is present we have a stable registration state.
            if cops.get("operator"):
                return {"res": res, "cops": cops}
        await asyncio.sleep(max(0.4, float(step_sec)))
    return {"res": last_res, "cops": last_cops}


@app.get("/api/network/cops")
async def network_cops_get() -> dict:
    res = await engine.send_command("AT+COPS?", timeout_sec=4.0)
    return {
        "ok": res.get("ok", False),
        "cops": _parse_cops_lines(res.get("lines", [])),
        "raw": res,
    }


@app.post("/api/network/cops")
async def network_cops_set(body: CopsSetBody) -> dict:
    if body.mode not in (0, 2):
        raise HTTPException(status_code=400, detail="Only mode 0 (auto) and 2 (deregister) are enabled in this UI.")

    resume_kpi = bool(kpi_runtime.poll_running)
    async with _modem_exclusive_lock:
        await _pause_exclusive_modem_access()
        try:
            # Operator registration can take long time on live networks.
            set_timeout = 180.0 if body.mode == 0 else 45.0
            set_res = await engine.send_command(f"AT+COPS={body.mode}", timeout_sec=set_timeout)
            read_res = await engine.send_command("AT+COPS?", timeout_sec=4.0)
            set_ok = bool(set_res.get("ok", False))
            set_final = str(set_res.get("final", ""))
            set_lines = set_res.get("lines", [])

            error_msg = None
            modem_detail = describe_modem_send_result(set_res) if not set_ok else None
            if not set_ok:
                tail = ""
                if isinstance(set_lines, list) and set_lines:
                    tail = set_lines[-1]
                error_msg = f"COPS set failed ({set_final or 'no final'})"
                if tail and tail.upper() not in ("OK", "ERROR"):
                    error_msg += f": {tail}"
                error_msg = combine_errors(error_msg, modem_detail) or error_msg

            return {
                "ok": set_ok,
                "error": error_msg,
                "modem_detail": modem_detail,
                "set": set_res,
                "cops": _parse_cops_lines(read_res.get("lines", [])),
                "raw": read_res,
            }
        finally:
            _resume_exclusive_modem_access(resume_kpi)


@app.get("/api/network/cops/scan")
async def network_cops_scan(uk_only: bool = False) -> dict:
    # Operator scan monopolizes AT for ~35s; pause KPI + lock-guard so nothing else sends.
    resume_kpi = _exclusive_section_resume_kpi_snapshot()
    original_lte_band: str | None = None
    original_nr_band: str | None = None
    nr_band_key_used: str | None = None
    lte_band_changed = False
    nr_band_changed = False
    lte_band_set_res: dict | None = None
    nr_band_set_res: dict | None = None
    lte_band_restore_res: dict | None = None
    nr_band_restore_res: dict | None = None
    lte_band_read_res: dict | None = None
    nr5g_band_read_res: dict | None = None
    nsa_nr5g_band_read_res: dict | None = None
    lte_band_restore_error: str | None = None
    nr_band_restore_error: str | None = None

    async with _modem_exclusive_lock:
        await _pause_exclusive_modem_access()
        try:
            if uk_only:
                lte_band_read_res = await engine.send_command('AT+QNWPREFCFG="lte_band"', timeout_sec=5.0)
                original_lte_band = _parse_qnwprefcfg_value(lte_band_read_res.get("lines", []), "lte_band")
                if original_lte_band and original_lte_band != UK_LTE_SCAN_BANDS:
                    lte_band_set_res = await engine.send_command(
                        f'AT+QNWPREFCFG="lte_band",{UK_LTE_SCAN_BANDS}',
                        timeout_sec=8.0,
                    )
                    lte_band_changed = bool(lte_band_set_res.get("ok", False))

                nr5g_band_read_res = await engine.send_command('AT+QNWPREFCFG="nr5g_band"', timeout_sec=5.0)
                nsa_nr5g_band_read_res = await engine.send_command('AT+QNWPREFCFG="nsa_nr5g_band"', timeout_sec=5.0)
                original_nr_band = _parse_qnwprefcfg_value(nr5g_band_read_res.get("lines", []), "nr5g_band")
                nr_band_key_used = "nr5g_band"
                if not original_nr_band:
                    original_nr_band = _parse_qnwprefcfg_value(nsa_nr5g_band_read_res.get("lines", []), "nsa_nr5g_band")
                    nr_band_key_used = "nsa_nr5g_band"
                if original_nr_band and original_nr_band != UK_NR_SCAN_BANDS:
                    nr_band_set_res = await engine.send_command(
                        f'AT+QNWPREFCFG="{nr_band_key_used}",{UK_NR_SCAN_BANDS}',
                        timeout_sec=8.0,
                    )
                    nr_band_changed = bool(nr_band_set_res.get("ok", False))

            res = await engine.send_command("AT+COPS=?", timeout_sec=35.0)
            ops = _parse_cops_scan_lines(res.get("lines", []))
            ok = bool(res.get("ok", False))
            err = None
            modem_detail = describe_modem_send_result(res) if not ok else None
            if not ok:
                base = f"COPS scan failed ({res.get('final') or 'no final'})"
                err = combine_errors(base, modem_detail) or base
            return {
                "ok": ok,
                "error": err,
                "modem_detail": modem_detail,
                "uk_only": uk_only,
                "scan_scope": (
                    f"LTE bands {UK_LTE_SCAN_BANDS}; NR bands {UK_NR_SCAN_BANDS}"
                    if uk_only
                    else "default modem scope"
                ),
                "operators": ops,
                "raw": {
                    "scan": res,
                    "lte_band_read": lte_band_read_res,
                    "lte_band_set": lte_band_set_res,
                    "nr5g_band_read": nr5g_band_read_res,
                    "nsa_nr5g_band_read": nsa_nr5g_band_read_res,
                    "nr_band_key_used": nr_band_key_used,
                    "nr_band_set": nr_band_set_res,
                },
            }
        finally:
            if uk_only and lte_band_changed and original_lte_band:
                lte_band_restore_res = await engine.send_command(
                    f'AT+QNWPREFCFG="lte_band",{original_lte_band}',
                    timeout_sec=8.0,
                )
                if not lte_band_restore_res.get("ok", False):
                    lte_band_restore_error = (
                        f"Failed restoring lte_band to {original_lte_band} "
                        f"({lte_band_restore_res.get('final') or 'no final'})"
                    )
            if uk_only and nr_band_changed and original_nr_band and nr_band_key_used:
                nr_band_restore_res = await engine.send_command(
                    f'AT+QNWPREFCFG="{nr_band_key_used}",{original_nr_band}',
                    timeout_sec=8.0,
                )
                if not nr_band_restore_res.get("ok", False):
                    nr_band_restore_error = (
                        f"Failed restoring {nr_band_key_used} to {original_nr_band} "
                        f"({nr_band_restore_res.get('final') or 'no final'})"
                    )
            if lte_band_restore_error or nr_band_restore_error:
                kpi_runtime.last_error = " | ".join(
                    [x for x in [lte_band_restore_error, nr_band_restore_error] if x]
                )
            _resume_exclusive_modem_access(resume_kpi)


@app.get("/api/network/mno")
async def network_mno_get() -> dict:
    cops_res = await engine.send_command("AT+COPS?", timeout_sec=4.0)
    cops = _parse_cops_lines(cops_res.get("lines", []))
    selected = "auto" if cops.get("mode") == 0 else (_profile_key_from_cops_operator(cops.get("operator")) or "auto")
    return {
        "ok": True,
        "selected_profile": selected,
        "profiles": {k: {"label": v["label"], "plmn": v["plmn"]} for k, v in MNO_PROFILES.items()},
        "cops": cops,
        "raw": {"cops": cops_res},
    }


@app.post("/api/network/mno")
async def network_mno_set(body: MnoSelectBody) -> dict:
    key = str(body.profile or "").strip().lower()
    if key not in MNO_PROFILES:
        raise HTTPException(status_code=400, detail="Invalid profile. Use: vodafone, vmo2, ee, h3g, auto.")

    if key != "auto" and body.cops_manual_registration not in (1, 4):
        raise HTTPException(
            status_code=400,
            detail="cops_manual_registration must be 1 (manual, hold PLMN) or 4 (manual + auto fallback).",
        )

    cfg = MNO_PROFILES[key]
    resume_kpi = _exclusive_section_resume_kpi_snapshot()
    ok = False
    err: str | None = None
    set_res: dict = {}
    read_res: dict = {}
    cops: dict = {}
    recover_res: dict | None = None
    dereg_res: dict | None = None

    async with _modem_exclusive_lock:
        await _pause_exclusive_modem_access()
        try:
            if key == "auto":
                set_res = await engine.send_command("AT+COPS=0", timeout_sec=180.0)
                polled = await _poll_cops_state(total_wait_sec=60.0, step_sec=2.0)
                read_res = polled["res"]
                cops = polled["cops"]
                ok = bool(set_res.get("ok", False) and cops.get("operator"))
                err = None if ok else "Auto registration did not produce an operator within timeout."
            else:
                plmn = str(cfg["plmn"])
                cops_mode = int(body.cops_manual_registration)
                # Mode 4: manual select with automatic fallback; mode 1: manual until loss (often better for roaming / non-steered SIM).
                if body.deregister_before_apply:
                    dereg_res = await engine.send_command("AT+COPS=2", timeout_sec=45.0)
                    # Clear previous manual registration before new PLMN; required on many modems/router stacks.
                    await asyncio.sleep(2.5)

                # Operator selection can legitimately exceed 3 minutes (poor RF, PLMN roaming).
                cops_set_timeout = 360.0
                set_res = await engine.send_command(
                    f'AT+COPS={cops_mode},2,"{plmn}"', timeout_sec=cops_set_timeout
                )
                polled = await _poll_cops_state(total_wait_sec=75.0, step_sec=2.5)
                read_res = polled["res"]
                cops = polled["cops"]
                current_profile = _profile_key_from_cops_operator(cops.get("operator"))
                target_hit = bool(str(cops.get("operator") or "") == plmn or current_profile == key)
                ok = bool(set_res.get("ok", False) and target_hit)
                err = None
                recover_res = None
                if not ok:
                    want_label = str(cfg.get("label") or key)
                    cop = cops.get("operator")
                    cm = cops.get("mode") if cops else "-"
                    cur_name = _mno_label_for_numeric_plmn(str(cop)) if cop else None
                    tail_prof = f", profile≈{current_profile}" if current_profile else ""
                    snap_tail = (
                        f" Snapshot from follow-up polls: operator={cop or '-'}, COPS mode={cm}{tail_prof}."
                    )
                    sr_ok = bool(set_res.get("ok"))
                    fin_raw = str(set_res.get("final") or "").strip()
                    fin_u = fin_raw.upper()
                    if not sr_ok:
                        # Distinguish command timeout/no OK from "registered on wrong PLMN".
                        if fin_u == "TIMEOUT":
                            err = (
                                f"AT+COPS (manual PLMN {plmn}, mode={cops_mode}) did not return OK "
                                "before the serial timeout (typically several minutes)."
                                + snap_tail
                                + " The modem may still be searching; retry with better RF, or firmware may need longer."
                            )
                        else:
                            dm = describe_modem_send_result(set_res)
                            err = (dm or "AT+COPS was rejected.") + snap_tail
                    elif sr_ok and cop and not target_hit:
                        got = f"{cop}" + (f" ({cur_name})" if cur_name else "") + tail_prof
                        err = (
                            f"Requested {want_label} (PLMN {plmn}); modem registered on {got} (COPS mode {cm}) before the wait ended. "
                            "Try COPS mode 4 (manual + auto fallback), Auto, or relax RAT/band locks; "
                            "the network or SIM may refuse that PLMN until coverage/steering allows it."
                        )
                    elif sr_ok and not cop:
                        err = (
                            f"{want_label} (PLMN {plmn}): AT+COPS OK but no operator in COPS? before timeout. "
                            "Retry or check registration / flight mode."
                        )
                    else:
                        err = (
                            f"MNO select did not settle on {plmn} within poll window "
                            f"(current={cop or '-'} mode={cm}{tail_prof})"
                        )
                    # Auto-recover so user is not stranded in a de-registered state.
                    recover_res = await engine.send_command("AT+COPS=0", timeout_sec=120.0)
                    recovered = await _poll_cops_state(total_wait_sec=45.0, step_sec=2.0)
                    read_res = recovered["res"]
                    cops = recovered["cops"]
        finally:
            _resume_exclusive_modem_access(resume_kpi)

    md_set = describe_modem_send_result(set_res) if isinstance(set_res, dict) and not set_res.get("ok") else None
    md_rec = describe_modem_send_result(recover_res) if recover_res is not None and not recover_res.get("ok") else None
    md_dereg = describe_modem_send_result(dereg_res) if dereg_res is not None and not dereg_res.get("ok") else None
    modem_detail = combine_errors(md_set, md_dereg, md_rec, sep=" | ")
    # Avoid repeating the AT+COPS failure text: err usually already incorporates set_res semantics.
    if modem_detail:
        if not err:
            err = modem_detail
        elif md_rec and md_rec.strip() not in err:
            err = combine_errors(err, md_rec.strip(), sep=" — ")

    return {
        "ok": ok,
        "error": err,
        "modem_detail": modem_detail,
        "selected_profile": key,
        "cops_mode_used": None if key == "auto" else int(body.cops_manual_registration),
        "profile": {"label": cfg["label"], "plmn": cfg["plmn"]},
        "set": set_res,
        "cops": cops,
        "raw": {
            "read": read_res,
            "recover_auto": recover_res,
            "deregister": dereg_res,
        },
        "deregister_before_apply_used": (
            False if key == "auto" else bool(body.deregister_before_apply)
        ),
    }


@app.get("/api/network/data-gate")
async def network_data_gate_get() -> dict:
    cgatt_res = await engine.send_command("AT+CGATT?", timeout_sec=3.0)
    qiact_res = await engine.send_command("AT+QIACT?", timeout_sec=4.0)
    attached = _parse_cgatt_attached(cgatt_res.get("lines", []))
    contexts = _parse_qiact_contexts(qiact_res.get("lines", []))
    active = [c for c in contexts if c.get("active")]
    inhibited = len(active) == 0
    return {
        "ok": True,
        "inhibited": inhibited,
        "packet_attached": attached,
        "active_contexts": active,
        "raw": {"cgatt": cgatt_res, "qiact": qiact_res},
    }


@app.post("/api/network/apn")
async def network_apn_set(body: ApnSetBody) -> dict:
    """Set PDP APN via AT+CGDCONT (password-gated). Optionally QIDEACT, CGATT, QIACT."""
    if (body.password or "") != DATA_GATE_UNLOCK_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password for APN change.")

    apn = _sanitize_apn_for_at(body.apn)
    pdp = _normalize_cgdcont_pdp_type(body.pdp_type)
    cid = int(body.cid)

    resume_kpi = _exclusive_section_resume_kpi_snapshot()
    actions: list[dict] = []
    reattach_errs: list[dict] = []

    async with _modem_exclusive_lock:
        await _pause_exclusive_modem_access()
        try:
            cgatt_before_res = await engine.send_command("AT+CGATT?", timeout_sec=4.0)
            attached_before = _parse_cgatt_attached(cgatt_before_res.get("lines", []))
            actions.append({"cmd": "AT+CGATT?", "res": cgatt_before_res})

            qiact_res = await engine.send_command("AT+QIACT?", timeout_sec=4.0)
            contexts = _parse_qiact_contexts(qiact_res.get("lines", []))
            active_here = next((c for c in contexts if c.get("cid") == cid and c.get("active")), None)
            did_ideact = False
            if active_here:
                ideact = await engine.send_command(f"AT+QIDEACT={cid}", timeout_sec=25.0)
                actions.append({"cmd": f"AT+QIDEACT={cid}", "res": ideact})
                did_ideact = True

            cmd = f'AT+CGDCONT={cid},"{pdp}","{apn}"'
            cgd_set = await engine.send_command(cmd, timeout_sec=15.0)
            actions.append({"cmd": cmd, "res": cgd_set})
            set_ok = bool(cgd_set.get("ok", False))

            qic_cmd = f'AT+QICSGP={cid},1,"{apn}","","",0'
            qic_res = await engine.send_command(qic_cmd, timeout_sec=15.0)
            actions.append(
                {
                    "cmd": qic_cmd + " (Quectel PDP stack mirror; OK if modem supports)",
                    "res": qic_res,
                }
            )

            if body.reactivate and set_ok:
                need_attach = bool(did_ideact or attached_before is not True)
                if need_attach:
                    att = await engine.send_command("AT+CGATT=1", timeout_sec=35.0)
                    actions.append({"cmd": "AT+CGATT=1", "res": att})
                    if not att.get("ok"):
                        reattach_errs.append(att)
                qi = await engine.send_command(f"AT+QIACT={cid}", timeout_sec=45.0)
                actions.append({"cmd": f"AT+QIACT={cid}", "res": qi})
                if not qi.get("ok"):
                    reattach_errs.append(qi)

            read_res = await engine.send_command("AT+CGDCONT?", timeout_sec=4.0)
            contexts_parsed = _parse_cgdcont(read_res.get("lines", []))
            primary = next((c for c in contexts_parsed if c.get("cid") == cid), None)

            if not set_ok:
                msg = "AT+CGDCONT did not complete successfully."
            elif reattach_errs:
                msg = (
                    "APN saved (CGDCONT + mirror) but CGATT/QIACT reattachment did not complete successfully. "
                    "Use Allow Data or retry reconnect."
                )
            elif body.reactivate:
                msg = "APN updated (CGDCONT + Quectel QICSGP); packet data reattached (QIACT)."
            elif did_ideact:
                msg = (
                    "APN stored; PDP context was deactivated to apply CGDCONT + QICSGP. "
                    "Press Allow Data to reconnect with the new APN."
                )
            else:
                msg = "APN stored (CGDCONT + QICSGP). Use Allow Data if you need an immediate reconnect."

            md_parts = []
            if not set_ok:
                md_parts.append(describe_modem_send_result(cgd_set))
            for rr in reattach_errs:
                md_parts.append(describe_modem_send_result(rr))
            modem_detail = combine_errors(*md_parts, sep=" | ")

            apn_ok = bool(
                set_ok
                and (not body.reactivate or len(reattach_errs) == 0)
            )
            api_err_text = None if apn_ok else (modem_detail or "APN update failed.")

            return {
                "ok": apn_ok,
                "error": api_err_text,
                "modem_detail": modem_detail,
                "apn": apn,
                "cid": cid,
                "pdp_type": pdp,
                "primary_context": primary,
                "cgdcont_contexts": contexts_parsed,
                "reactivate_requested": bool(body.reactivate),
                "did_pdp_detach": bool(did_ideact),
                "message": msg,
                "actions": actions,
                "raw": {
                    "cgatt_before": cgatt_before_res,
                    "qiact_before": qiact_res,
                    "cgdcont_read": read_res,
                },
            }
        finally:
            _resume_exclusive_modem_access(resume_kpi)


@app.post("/api/network/data-gate")
async def network_data_gate_set(body: DataGateBody) -> dict:
    if not body.inhibit:
        if (body.password or "") != DATA_GATE_UNLOCK_PASSWORD:
            raise HTTPException(status_code=403, detail="Invalid password for data allow operation.")
    actions: list[dict] = []
    before = await network_data_gate_get()
    if body.inhibit:
        for c in before.get("active_contexts", []):
            cid = c.get("cid")
            if cid is None:
                continue
            res = await engine.send_command(f"AT+QIDEACT={int(cid)}", timeout_sec=15.0)
            actions.append({"cmd": f"AT+QIDEACT={int(cid)}", "res": res})
    else:
        # Allow packet data: ensure attach then activate primary CID 1.
        res_attach = await engine.send_command("AT+CGATT=1", timeout_sec=20.0)
        actions.append({"cmd": "AT+CGATT=1", "res": res_attach})
        if bool(res_attach.get("ok")):
            await asyncio.sleep(0.4)
        res_activate = await engine.send_command("AT+QIACT=1", timeout_sec=45.0)
        actions.append({"cmd": "AT+QIACT=1", "res": res_activate})
        if bool(res_attach.get("ok")) and not bool(res_activate.get("ok")):
            recover = await engine.send_command("AT+QIDEACT=1", timeout_sec=20.0)
            actions.append({"cmd": "AT+QIDEACT=1", "res": recover, "note": "after failed QIACT=1"})
            await asyncio.sleep(0.35)
            res_activate = await engine.send_command("AT+QIACT=1", timeout_sec=45.0)
            actions.append({"cmd": "AT+QIACT=1 (retry)", "res": res_activate})
    after = await network_data_gate_get()

    cmds_ok: bool
    md_parts: list[str | None]
    if body.inhibit:
        cmds_ok = True
        md_parts = []
        for a in actions:
            rr = a.get("res") or {}
            if not rr.get("ok", False):
                cmds_ok = False
                md_parts.append(describe_modem_send_result(rr))
        modem_detail = combine_errors(*md_parts, sep=" | ")
    else:
        cmds_ok = bool(res_attach.get("ok") and res_activate.get("ok"))
        md_parts = []
        if not res_attach.get("ok"):
            md_parts.append(describe_modem_send_result(res_attach))
        if not res_activate.get("ok"):
            md_parts.append(describe_modem_send_result(res_activate))
        modem_detail = combine_errors(*md_parts, sep=" | ")

    desired_inhibited = bool(body.inhibit)
    achieved = bool(after.get("inhibited")) if desired_inhibited else not bool(after.get("inhibited"))
    overall_ok = bool(cmds_ok and achieved)

    err = None
    if not overall_ok:
        err = combine_errors(
            "Data gate command(s) did not complete as expected." if not cmds_ok else "Data gate state mismatch after AT.",
            modem_detail,
            sep=" ",
        )
        if not err:
            err = "Data gate update failed."

    return {
        "ok": overall_ok,
        "error": err,
        "modem_detail": modem_detail,
        "requested_inhibit": bool(body.inhibit),
        "before": before,
        "after": after,
        "actions": actions,
    }


@app.get("/api/network/locks")
async def network_locks_get() -> dict:
    lock_state = await _read_lock_status()
    async with _desired_locks_lock:
        desired = dict(_desired_locks)
    return {
        "ok": True,
        "locks": lock_state["values"],
        "desired_locks": desired,
        "raw": lock_state["raw"],
    }


@app.post("/api/network/locks")
async def network_locks_set(body: LockSetBody) -> dict:
    requested: dict[str, str] = {}

    if body.rat_mode:
        rat = body.rat_mode.strip().upper()
        requested["mode_pref"] = rat

    if body.lte_band:
        band = body.lte_band.strip()
        requested["lte_band"] = band

    if body.nr5g_band:
        band = body.nr5g_band.strip()
        requested["nr5g_band"] = band

    if body.nrdc_mode is not None:
        mode = 1 if int(body.nrdc_mode) else 0
        requested["nrdc_mode"] = str(mode)

    if not requested:
        raise HTTPException(status_code=400, detail="No lock values provided.")

    normalized_requested = {
        k: _normalize_lock_value(k, v)
        for k, v in requested.items()
    }

    set_results = await _apply_lock_requests(requested)
    lock_state = await _read_lock_status()
    locks = lock_state["values"]
    errors: list[str] = []

    if "mode_pref" in normalized_requested:
        want = normalized_requested["mode_pref"]
        if not _lock_value_matches("mode_pref", want, locks):
            final = set_results.get("mode_pref", {}).get("final", "UNKNOWN")
            got = _normalize_lock_value("mode_pref", locks.get("mode_pref"))
            errors.append(f"mode_pref verify failed (wanted {want}, got {got or '-'}, final={final})")

    if "lte_band" in normalized_requested:
        want = normalized_requested["lte_band"]
        if not _lock_value_matches("lte_band", want, locks):
            final = set_results.get("lte_band", {}).get("final", "UNKNOWN")
            got = _normalize_lock_value("lte_band", locks.get("lte_band"))
            errors.append(f"lte_band verify failed (wanted {want}, got {got or '-'}, final={final})")

    if "nr5g_band" in normalized_requested:
        want = normalized_requested["nr5g_band"]
        if not _lock_value_matches("nr5g_band", want, locks):
            final_nr = set_results.get("nr5g_band", {}).get("final", "UNKNOWN")
            final_nsa = set_results.get("nsa_nr5g_band", {}).get("final", "N/A")
            got_nr = _normalize_lock_value("nr5g_band", locks.get("nr5g_band"))
            got_nsa = _normalize_lock_value("nsa_nr5g_band", locks.get("nsa_nr5g_band"))
            errors.append(
                f"nr5g_band verify failed (wanted {want}, got nr5g={got_nr or '-'}, "
                f"nsa_nr5g={got_nsa or '-'}, finals={final_nr}/{final_nsa})"
            )

    if "nrdc_mode" in normalized_requested:
        want = normalized_requested["nrdc_mode"]
        if not _lock_value_matches("nrdc_mode", want, locks):
            final = set_results.get("nrdc_mode", {}).get("final", "UNKNOWN")
            got = _normalize_lock_value("nrdc_mode", locks.get("nrdc_mode"))
            errors.append(f"nrdc_mode verify failed (wanted {want}, got {got or '-'}, final={final})")

    if not errors:
        async with _desired_locks_lock:
            _desired_locks.update(normalized_requested)

    modem_hints: list[str] = []
    for k2, rr in set_results.items():
        if isinstance(rr, dict) and not rr.get("ok"):
            hint = describe_modem_send_result(rr)
            if hint:
                modem_hints.append(f"{k2}: {hint}")
    modem_detail_l = " | ".join(modem_hints) if modem_hints else None
    err_out = "; ".join(errors) if errors else None
    if modem_detail_l:
        err_out = combine_errors(err_out, modem_detail_l, sep=" — ")

    return {
        "ok": len(errors) == 0,
        "error": err_out if err_out else None,
        "modem_detail": modem_detail_l,
        "set": set_results,
        "locks": locks,
        "desired_locks": normalized_requested if not errors else None,
        "raw": lock_state["raw"],
    }


@app.get("/api/tools/bind-interfaces")
async def tools_bind_interfaces() -> dict:
    if os.name != "nt":
        return {"ok": True, "interfaces": [], "platform": "non-windows"}
    interfaces = _enumerate_windows_ipv4_adapters()
    return {"ok": True, "interfaces": interfaces, "platform": "windows"}


@app.post("/api/tools/modem-reset")
async def tools_modem_reset() -> dict:
    # Quectel modem reboot/reset. Some firmware may drop port before final response.
    cmd_res = await engine.send_command("AT+CFUN=1,1", timeout_sec=8.0)
    final = str(cmd_res.get("final", "")).upper()
    accepted = bool(cmd_res.get("ok", False) or final == "TIMEOUT")
    # Give the modem a short grace period to detach/re-enumerate.
    await asyncio.sleep(1.5)
    status = await engine.status()
    md_fail = describe_modem_send_result(cmd_res) if not accepted else None
    fallback = f"Reset command rejected ({cmd_res.get('final', 'UNKNOWN')})"
    return {
        "ok": accepted,
        "error": None if accepted else (md_fail or fallback),
        "modem_detail": md_fail,
        "cmd": cmd_res,
        "status": status,
    }


@app.post("/api/tools/iperf-test")
async def tools_iperf_test(body: IperfTestBody) -> dict:
    host = str(body.host or "").strip()
    if not host:
        raise HTTPException(status_code=400, detail="host is required.")
    direction = str(body.direction or "download").strip().lower()
    if direction not in ("download", "upload"):
        raise HTTPException(status_code=400, detail="direction must be 'download' or 'upload'.")
    protocol = str(body.protocol or "tcp").strip().lower()
    if protocol != "tcp":
        raise HTTPException(status_code=400, detail="Only protocol='tcp' is currently supported.")
    reverse = direction == "download"
    limit_mbps = body.bitrate_limit_mbps
    bind_ip = str(body.bind_ip or "").strip() or None
    if bind_ip:
        try:
            ipaddress.IPv4Address(bind_ip)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"Invalid bind_ip: {bind_ip}") from exc
    detected_adapter = None
    if body.mobile_only and not bind_ip:
        bind_ip, detected_adapter = _detect_mobile_bind_ip_windows()
        if not bind_ip:
            return {
                "ok": False,
                "error": "Mobile-only mode could not detect mobile interface IPv4. Set bind_ip manually.",
                "host": host,
                "port": int(body.port),
                "duration_sec": int(body.duration_sec),
                "direction": direction,
                "protocol": protocol,
                "mobile_only": bool(body.mobile_only),
                "bind_ip": None,
                "bitrate_limit_mbps": limit_mbps,
            }
    binary = _discover_iperf_binary()
    if not binary:
        return {
            "ok": False,
            "error": "iperf3 binary not found. Place iperf3.exe in project/backend root or set MD_IPERF_BIN.",
            "host": host,
            "port": int(body.port),
            "duration_sec": int(body.duration_sec),
            "direction": direction,
            "protocol": protocol,
            "mobile_only": bool(body.mobile_only),
            "bind_ip": bind_ip,
            "bitrate_limit_mbps": limit_mbps,
        }
    cmd = [
        binary,
        "-c",
        host,
        # Force IPv4: hostname may resolve to IPv6 while -B binds an IPv4, which yields exit=1.
        "-4",
        "-p",
        str(int(body.port)),
        "-t",
        str(int(body.duration_sec)),
        "-J",
    ]
    if reverse:
        cmd.append("-R")
    if bind_ip:
        cmd.extend(["-B", bind_ip])
    if limit_mbps is not None:
        cmd.extend(["-b", f"{float(limit_mbps):g}M"])
    timeout_sec = int(body.duration_sec) + 15
    try:
        proc = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "error": f"iperf timed out after {timeout_sec}s",
            "command": cmd,
            "host": host,
            "port": int(body.port),
            "duration_sec": int(body.duration_sec),
            "direction": direction,
            "protocol": protocol,
            "mobile_only": bool(body.mobile_only),
            "bind_ip": bind_ip,
            "bitrate_limit_mbps": limit_mbps,
        }
    stdout = str(proc.stdout or "")
    stderr = str(proc.stderr or "")
    report = None
    parse_error = None
    if stdout.strip():
        try:
            report = json.loads(stdout)
        except Exception as exc:  # noqa: BLE001
            parse_error = str(exc)
    bps, source = _extract_iperf_bits_per_second(report or {}, reverse=reverse)
    mbps = (bps / 1_000_000.0) if isinstance(bps, (int, float)) else None
    ok = proc.returncode == 0 and mbps is not None
    err_detail = None if ok else _compose_iperf_error(proc.returncode, stderr, stdout, parse_error)
    return {
        "ok": ok,
        "error": err_detail,
        "host": host,
        "port": int(body.port),
        "duration_sec": int(body.duration_sec),
        "direction": direction,
        "protocol": protocol,
        "mobile_only": bool(body.mobile_only),
        "bind_ip": bind_ip,
        "detected_mobile_adapter": detected_adapter,
        "bitrate_limit_mbps": limit_mbps,
        "throughput_mbps": round(float(mbps), 3) if mbps is not None else None,
        "throughput_source": source,
        "command": cmd,
        "exit_code": proc.returncode,
        "json_parse_error": parse_error,
        "stderr_tail": "\n".join(stderr.splitlines()[-40:]) if stderr else "",
        "stdout_head": (stdout[:1200] + "...") if len(stdout) > 1200 else stdout if stdout else "",
        "raw": report,
    }


@app.post("/api/tools/icmp-ping")
async def tools_icmp_ping(body: IcmpPingSweepBody) -> dict:
    host = body.host.strip() or "8.8.8.8"
    count = int(body.count)
    bind = str(body.bind_ipv4 or "").strip() or None
    if bind:
        try:
            ipaddress.IPv4Address(bind)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"Invalid bind_ipv4: {bind}") from exc

    if os.name == "nt":
        cmd = ["ping", "-4", "-n", str(count), "-w", "3000"]
        if bind:
            cmd.extend(["-S", bind])
        cmd.append(host)
    else:
        cmd = ["ping", "-c", str(count), "-W", "5", host]

    timeout_sec = min(120, max(15, count * 4 + 10))
    try:
        proc = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "error": f"ping subprocess timed out after {timeout_sec}s",
            "host": host,
            "count": count,
            "bind_ipv4": bind,
            "command": cmd,
        }

    stdout = str(proc.stdout or "")
    stderr = str(proc.stderr or "")
    combined = stdout + "\n" + stderr
    if os.name == "nt":
        rtts = _parse_icmp_ping_rtts_windows(combined)
    else:
        rtts = _parse_icmp_ping_rtts_unix(combined)

    jitter = _icmp_jitter_ms(rtts) if rtts else None
    avg_ms = round(sum(rtts) / len(rtts), 3) if rtts else None
    min_ms = round(min(rtts), 3) if rtts else None
    max_ms = round(max(rtts), 3) if rtts else None
    ok_success = bool(rtts)
    err = None
    if not ok_success:
        tail = (stderr or stdout)[-1500:] if (stderr or stdout) else ""
        err = f"ICMP ping failed (exit={proc.returncode})"
        if tail.strip():
            err = f"{err}\n{tail.strip()}"

    return {
        "ok": ok_success,
        "error": None if ok_success else err,
        "host": host,
        "count": count,
        "received": len(rtts),
        "rtt_ms": rtts,
        "avg_ms": avg_ms,
        "min_ms": min_ms,
        "max_ms": max_ms,
        "jitter_ms": jitter,
        "bind_ipv4": bind,
        "bind_supported": os.name == "nt",
        "command": cmd,
        "exit_code": proc.returncode,
        "stdout_tail": stdout[-4000:] if stdout else "",
    }


@app.post("/api/tools/volte-test")
async def tools_volte_test(body: VolteTestBody) -> dict:
    if (body.password or "") != DATA_GATE_UNLOCK_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password for VoLTE call test.")

    number = _sanitize_dial_number(body.number)
    if not number:
        raise HTTPException(status_code=400, detail="Invalid dial number.")

    hold_sec = int(body.hold_sec or 10)

    # Ensure no stale call exists before test.
    pre_hang = await engine.send_command("ATH", timeout_sec=4.0)
    await asyncio.sleep(0.25)

    before_urc = list(engine.urc_log)
    nw_before_res = await engine.send_command("AT+QNWINFO", timeout_sec=3.0)
    nw_before = _parse_qnwinfo_line(nw_before_res.get("lines", []))

    dial_started = time.time()
    dial_res = await engine.send_command(f"ATD{number};", timeout_sec=8.0)
    dial_ok = bool(dial_res.get("ok", False))

    deadline = asyncio.get_running_loop().time() + 35.0
    call_connected = False
    connect_ts: float | None = None
    clcc_states: list[dict] = []
    last_clcc: list[dict] = []
    while asyncio.get_running_loop().time() < deadline:
        clcc_res = await engine.send_command("AT+CLCC", timeout_sec=3.0)
        clcc = _parse_clcc_lines(clcc_res.get("lines", []))
        last_clcc = clcc
        stat = clcc[0].get("stat") if clcc else None
        clcc_states.append(
            {
                "t_s": round(time.time() - dial_started, 1),
                "status": _clcc_stat_label(stat),
                "raw_stat": stat,
            }
        )
        if stat in (0, 1):
            call_connected = True
            connect_ts = time.time()
            break
        if stat is None:
            # No call entries anymore.
            break
        await asyncio.sleep(0.8)

    nw_during_res = await engine.send_command("AT+QNWINFO", timeout_sec=3.0)
    nw_during = _parse_qnwinfo_line(nw_during_res.get("lines", []))

    call_duration_s = 0.0
    if call_connected and connect_ts is not None:
        await asyncio.sleep(max(1, hold_sec))
        call_duration_s = max(0.0, time.time() - connect_ts)

    hang_attempts: list[dict] = []
    hang_res = await engine.send_command("ATH", timeout_sec=5.0)
    hang_attempts.append(hang_res)
    await asyncio.sleep(0.35)

    def _has_active_or_held(rows: list[dict]) -> bool:
        # stat 0/1 means active/held and should be treated as still connected.
        return any((r.get("stat") in (0, 1)) for r in rows)

    end_deadline = asyncio.get_running_loop().time() + 15.0
    clcc_after: list[dict] = []
    clcc_after_samples: list[dict] = []
    while asyncio.get_running_loop().time() < end_deadline:
        clcc_after_res = await engine.send_command("AT+CLCC", timeout_sec=3.0)
        clcc_after = _parse_clcc_lines(clcc_after_res.get("lines", []))
        clcc_after_samples.append(
            {
                "t_s": round(time.time() - dial_started, 1),
                "states": [_clcc_stat_label(x.get("stat")) for x in clcc_after],
                "raw_states": [x.get("stat") for x in clcc_after],
            }
        )
        if not clcc_after:
            break
        # Retry hangup once if call still truly active/held after initial ATH.
        if _has_active_or_held(clcc_after) and len(hang_attempts) < 2:
            hang_attempts.append(await engine.send_command("ATH", timeout_sec=5.0))
            await asyncio.sleep(0.4)
            continue
        await asyncio.sleep(0.8)

    ceer_res = await engine.send_command("AT+CEER", timeout_sec=3.0)
    ceer = _parse_ceer(ceer_res.get("lines", []))

    nw_after_res = await engine.send_command("AT+QNWINFO", timeout_sec=3.0)
    nw_after = _parse_qnwinfo_line(nw_after_res.get("lines", []))

    now_urc = list(engine.urc_log)
    if len(now_urc) >= len(before_urc):
        delta_urc = now_urc[len(before_urc):]
    else:
        delta_urc = now_urc
    call_urc_lines = [
        x
        for x in delta_urc
        if any(tok in str(x).upper() for tok in ("NO CARRIER", "+CLCC", "+CEER", "BUSY", "NO ANSWER", "NO DIALTONE"))
    ]

    setup_time_ms = int((connect_ts - dial_started) * 1000) if connect_ts else None
    active_after_hang = _has_active_or_held(clcc_after)
    ok = bool(dial_ok and call_connected and not active_after_hang)
    error = None
    if not ok:
        if not dial_ok:
            error = f"Dial command failed ({dial_res.get('final') or 'no final'})"
        elif not call_connected:
            error = "Call did not reach connected state within timeout."
        elif active_after_hang:
            error = "Call still appears active/held after hangup retries."

    return {
        "ok": ok,
        "error": error,
        "number": number,
        "hold_sec": hold_sec,
        "dial_ok": dial_ok,
        "call_connected": call_connected,
        "setup_time_ms": setup_time_ms,
        "call_duration_s": round(call_duration_s, 1) if call_connected else 0.0,
        "ceer": ceer,
        "nwinfo_before": nw_before,
        "nwinfo_during_call": nw_during,
        "nwinfo_after": nw_after,
        "clcc_states": clcc_states,
        "clcc_last": last_clcc,
        "clcc_after_hangup": clcc_after,
        "clcc_after_samples": clcc_after_samples,
        "active_after_hang": active_after_hang,
        "call_urc_lines": call_urc_lines,
        "raw": {
            "pre_hang": pre_hang,
            "dial": dial_res,
            "hang": hang_res,
            "hang_attempts": hang_attempts,
            "ceer": ceer_res,
            "qnwinfo_before": nw_before_res,
            "qnwinfo_during": nw_during_res,
            "qnwinfo_after": nw_after_res,
        },
    }


@app.websocket("/ws/kpi")
async def ws_kpi(ws: WebSocket) -> None:
    await ws.accept()
    ws_clients.append(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in ws_clients:
            ws_clients.remove(ws)
