from __future__ import annotations

import asyncio
import functools
import ipaddress
import json
import logging
import math
import os
import random
import sys
import re
import shutil
import socket
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path

from typing import Any, Literal

from fastapi import Body, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse

from app.kpi_service import (
    _parse_cgdcont,
    _parse_cgauth,
    _parse_qiact,
    _parse_qicsgp,
    _read_qicsgp_best_effort,
    kpi_poll_loop,
)
from app import test_runner as tr
from app.at_modem_errors import combine_errors, describe_modem_send_result
from app.models import (
    ApnSetBody,
    AutoAnswerSetBody,
    CopsSetBody,
    DataGateBody,
    HostAutoAnswerBody,
    IcmpPingSweepBody,
    IperfTestBody,
    KpiPollBody,
    LockSetBody,
    MnoSelectBody,
    TestCancelBody,
    TestRunBody,
    VolteTestBody,
    VoiceAnswerBody,
    VoiceHangupBody,
)
from app.persist import save_last_serial_state
from app.routes import serial as serial_routes
from app.sim_usim_services import (
    SIM_EF_DESCRIPTIONS,
    SIM_INSPECTOR_LABEL_REFERENCE,
    label_usim_service,
)
from app.state import engine, kpi_runtime

logger = logging.getLogger(__name__)

APP_VERSION = "3.6"


_kpi_task: asyncio.Task[None] | None = None
_ws_push_task: asyncio.Task[None] | None = None
_lock_guard_task: asyncio.Task[None] | None = None
_host_auto_answer_task: asyncio.Task[None] | None = None
_host_aa_rings: int = 2
_host_aa_status_lock = asyncio.Lock()
_host_aa_status: dict[str, Any] = {"ring_urcs": 0, "note": ""}
ws_clients: list[WebSocket] = []
_instance_lock_file = None
_desired_locks: dict[str, str] = {}
_desired_locks_lock = asyncio.Lock()
_lock_guard_paused: bool = False
_modem_exclusive_lock = asyncio.Lock()

# In-flight `POST /api/test/run` session (single active run for cancel from UI/API).
_test_run_session_lock = asyncio.Lock()
_test_run_session: dict[str, Any] | None = None

# Shared AT+CLCC for `/api/tools/voice-call-status` and host auto-answer (single serial queue).
_voice_clcc_cache_ts: float = 0.0
_voice_clcc_rows: list[dict] | None = None
_voice_clcc_res_ok: bool = False
_voice_clcc_data_lock = asyncio.Lock()
_voice_clcc_fetch_lock = asyncio.Lock()
VOICE_CLCC_CACHE_TTL_SEC = 0.7
VOICE_CLCC_TIMEOUT_SEC = 2.5
HOST_AUTO_ANSWER_POLL_SEC = 0.85
VOICE_STATUS_POLL_MS = 1700


async def _voice_clcc_snapshot(*, force: bool = False) -> tuple[list[dict], bool]:
    """
    One AT+CLCC path for VoLTE widget + host auto-answer.

    When *force* is False, reuse a snapshot younger than ``VOICE_CLCC_CACHE_TTL_SEC`` so the
    dashboard poll often skips the modem while the auto-answer worker is already polling CLCC.
    """
    global _voice_clcc_cache_ts, _voice_clcc_rows, _voice_clcc_res_ok
    if not force:
        now = time.time()
        async with _voice_clcc_data_lock:
            if _voice_clcc_rows is not None and (now - _voice_clcc_cache_ts) <= VOICE_CLCC_CACHE_TTL_SEC:
                return list(_voice_clcc_rows), _voice_clcc_res_ok
    async with _voice_clcc_fetch_lock:
        now2 = time.time()
        if not force:
            async with _voice_clcc_data_lock:
                if _voice_clcc_rows is not None and (now2 - _voice_clcc_cache_ts) <= VOICE_CLCC_CACHE_TTL_SEC:
                    return list(_voice_clcc_rows), _voice_clcc_res_ok
        clcc_res = await engine.send_command("AT+CLCC", timeout_sec=VOICE_CLCC_TIMEOUT_SEC)
        rows = _parse_clcc_lines(clcc_res.get("lines", []))
        ok = bool(clcc_res.get("ok"))
        async with _voice_clcc_data_lock:
            _voice_clcc_cache_ts = time.time()
            _voice_clcc_rows = rows
            _voice_clcc_res_ok = ok
        return list(rows), ok


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


@functools.lru_cache(maxsize=16)
def _iperf_supports_connect_timeout(binary: str) -> bool:
    """True if *binary* accepts ``--connect-timeout`` (iperf3 newer than ~3.1.x)."""
    try:
        proc = subprocess.run(
            [binary, "--help"],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )
        blob = f"{proc.stdout or ''}\n{proc.stderr or ''}".lower()
        return "connect-timeout" in blob
    except Exception:
        return False


def _iperf_preflight_tcp_ipv4(
    host: str, port: int, *, timeout_sec: float, bind_ip: str | None
) -> str | None:
    """
    When iperf3 has no ``--connect-timeout``, the OS can keep TCP SYN attempts alive far
    longer than *timeout_sec*. Try one IPv4 control connection (same bind as iperf ``-B``)
    with *timeout_sec* as the socket deadline so manual tests return near the UI budget.
    Returns an error string, or ``None`` if the TCP handshake completes.
    """
    p = int(port)
    if p < 1 or p > 65535:
        return f"Invalid port: {port}"
    deadline = time.perf_counter() + max(0.25, float(timeout_sec))
    try:
        infos = socket.getaddrinfo(
            host,
            p,
            family=socket.AF_INET,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
    except socket.gaierror as exc:
        return f"Host resolution failed: {exc}"
    if not infos:
        return f"No IPv4 address for host {host!r}."
    last_err = "Could not connect."
    for _fa, _ty, _proto, _canon, sockaddr in infos:
        remain = deadline - time.perf_counter()
        if remain <= 0:
            return f"TCP connect to {host}:{p} timed out after {float(timeout_sec):g}s."
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(remain)
            if bind_ip:
                sock.bind((str(bind_ip), 0))
            sock.connect(sockaddr)
            return None
        except OSError as exc:
            last_err = f"TCP connect failed: {exc}"
        finally:
            try:
                sock.close()
            except Exception:
                pass
    return last_err


def _iperf_connect_timeout_sec_clamped(raw: Any) -> float | None:
    """Return 1..120 seconds or None when unset / invalid / non-positive."""
    if raw is None or raw == "":
        return None
    try:
        f = float(raw)
    except (TypeError, ValueError):
        return None
    if f <= 0:
        return None
    return max(1.0, min(120.0, f))


def _iperf_connect_timeout_for_profile(cfg: dict[str, Any]) -> float:
    """Default 10 s when profile omits ``connect_timeout_sec``; otherwise clamp or fall back to 10 if unset/null."""
    if "connect_timeout_sec" not in cfg:
        return 10.0
    c = _iperf_connect_timeout_sec_clamped(cfg.get("connect_timeout_sec"))
    return 10.0 if c is None else float(c)


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


def _windows_ipv4_assignable_for_bind(ip: str) -> bool:
    """True if *ip* is a local IPv4 this process may bind (``iperf3 -B`` / TCP pre-connect)."""
    s = str(ip or "").strip()
    if not s or s in ("0.0.0.0", "127.0.0.1"):
        return False
    try:
        ipaddress.IPv4Address(s)
    except Exception:
        return False
    # UDP bind is enough to validate local assignment; avoids listener state from TCP.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind((s, 0))
        return True
    except OSError:
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


def _iter_mobile_bind_candidates_windows() -> list[tuple[str, str]]:
    """(ipv4, source_label) in preference order for ``mobile_only`` iperf bind on Windows."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add(ip: str, label: str) -> None:
        s = str(ip or "").strip()
        if not s or s in seen:
            return
        try:
            ipaddress.IPv4Address(s)
        except Exception:
            return
        if s in ("0.0.0.0", "127.0.0.1"):
            return
        seen.add(s)
        out.append((s, str(label or "").strip() or "interface"))

    try:
        ds = getattr(kpi_runtime, "data_service", None) or {}
        if isinstance(ds, dict):
            cid_ip = ds.get("cid1_ip")
            if cid_ip:
                add(str(cid_ip), "modem cid1_ip (QIACT)")
    except Exception:
        pass

    mobile_kw = ("mobile", "cellular", "wwan", "rndis", "quectel", "usb ethernet", "internet sharing")
    for r in _enumerate_windows_ipv4_adapters():
        ip = str(r.get("ipv4") or "").strip()
        ad = str(r.get("adapter") or "").strip()
        hay = f"{ad} {ip}".lower()
        if any(k in hay for k in mobile_kw):
            add(ip, ad)

    tagged = [(0 if not ip.startswith("169.254.") else 1, i, ip, lab) for i, (ip, lab) in enumerate(out)]
    tagged.sort()
    return [(ip, lab) for _, _, ip, lab in tagged]


def _detect_mobile_bind_ip_windows() -> tuple[str | None, str | None]:
    """Pick first mobile-related IPv4 that actually binds locally (avoids WinError 10049 on ``-B``)."""
    if os.name != "nt":
        return None, None
    for ip, label in _iter_mobile_bind_candidates_windows():
        if _windows_ipv4_assignable_for_bind(ip):
            return ip, label
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
        s = str(raw or "").strip().lstrip("\ufeff")
        if not s.upper().startswith("+CLCC:"):
            continue
        payload = s.split(":", 1)[1].strip()
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


def _clcc_rows_voice_only(rows: list[dict]) -> list[dict]:
    """
    Drop ``+CLCC`` rows that are **data** bearers (``mode`` **1** per 3GPP TS 27.007).
    Packet data often shows ``stat`` active (0) with ``mode`` data (1); that must not look like a voice call.
    """
    return [r for r in rows if r.get("mode") != 1]


def _clcc_voice_active_or_held(rows: list[dict]) -> bool:
    """True only for **voice** contexts in active (0) or held (1) state — excludes packet PDP rows in ``+CLCC``."""
    v = _clcc_rows_voice_only(rows)
    return any((r.get("stat") in (0, 1)) for r in v)


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


def _summarize_voice_call_state(rows: list[dict]) -> dict[str, Any]:
    """Derive hook / ringing / line state from ``AT+CLCC`` rows (3GPP TS 27.007)."""
    if not rows:
        return {
            "call_present": False,
            "incoming_ringing": False,
            "hook": "on_hook",
            "line_state": "idle",
            "primary_number": None,
        }
    stats: list[int | None] = [r.get("stat") for r in rows]
    incoming = any(s == 4 for s in stats)
    off_hook = any(s in (0, 1) for s in stats)
    if incoming:
        line_state = "incoming_ring"
    elif any(s == 0 for s in stats):
        line_state = "active"
    elif any(s == 1 for s in stats):
        line_state = "held"
    elif any(s == 2 for s in stats):
        line_state = "dialing"
    elif any(s == 3 for s in stats):
        line_state = "alerting"
    elif any(s == 5 for s in stats):
        line_state = "waiting"
    else:
        line_state = "other"
    num = next((r.get("number") for r in rows if r.get("number")), None)
    return {
        "call_present": True,
        "incoming_ringing": incoming,
        "hook": "off_hook" if off_hook else "on_hook",
        "line_state": line_state,
        "primary_number": num,
    }


def _urc_recent_incoming_ring(urc_entries: list[tuple[float, str]], max_age_sec: float = 12.0) -> bool:
    """True only if the newest RING / +CRING URC is within *max_age_sec* (avoids stale ring UI)."""
    now = time.time()
    for ts, raw in reversed(urc_entries[-96:]):
        s = str(raw or "").strip().upper()
        if not s:
            continue
        if s == "RING" or s.startswith("RING") or "+CRING:" in s:
            return (now - float(ts)) <= max_age_sec
    return False


def _is_ring_urc_line(raw: str) -> bool:
    s = str(raw or "").strip().upper()
    if not s:
        return False
    return s == "RING" or s.startswith("RING") or "+CRING:" in s


def _count_ring_urcs_since(urc_entries: list[tuple[float, str]], since_ts: float) -> int:
    return sum(1 for ts, raw in urc_entries if float(ts) >= since_ts and _is_ring_urc_line(raw))


async def _host_auto_answer_worker(rings_target: int, password: str) -> None:
    """
    Poll CLCC + URC log; send ``ATA`` when either:
    - at least *rings_target* ``RING`` / ``+CRING`` lines since incoming started, or
    - ``CLCC`` shows incoming voice long enough (~4.5s per missing ring) with no URC pulses (VoLTE).
    """
    global _host_aa_status
    session_start: float | None = None
    sent_ata = False
    try:
        while True:
            await asyncio.sleep(HOST_AUTO_ANSWER_POLL_SEC)
            if (password or "") != DATA_GATE_UNLOCK_PASSWORD:
                break
            try:
                entries = list(engine.urc_log)
                rows, _ = await _voice_clcc_snapshot(force=True)
                voice_rows = _clcc_rows_voice_only(rows)
                summary = _summarize_voice_call_state(voice_rows)
                stats = [r.get("stat") for r in voice_rows]
                in_voice = any(s in (0, 1) for s in stats)
                recent_ring = _urc_recent_incoming_ring(entries)
                clcc_incoming = bool(summary["incoming_ringing"])
                incoming = clcc_incoming or recent_ring
            except asyncio.CancelledError:
                raise
            except Exception:
                async with _host_aa_status_lock:
                    _host_aa_status = {"ring_urcs": 0, "note": "poll_error"}
                continue

            if in_voice or not incoming:
                session_start = None
                sent_ata = False
                async with _host_aa_status_lock:
                    _host_aa_status = {"ring_urcs": 0, "note": ""}
                continue

            if session_start is None:
                session_start = time.time() - 2.0
                sent_ata = False

            ring_n = _count_ring_urcs_since(entries, session_start)
            elapsed = time.time() - float(session_start)
            need = max(1, min(255, rings_target))

            urc_ready = ring_n >= need
            min_elapsed = (3.0 if need > 1 else 1.2) + (need - 1) * 4.5
            time_ready = clcc_incoming and elapsed >= min_elapsed

            async with _host_aa_status_lock:
                _host_aa_status = {
                    "ring_urcs": ring_n,
                    "elapsed_s": round(elapsed, 1),
                    "note": "waiting" if not sent_ata else "answered",
                }

            if sent_ata:
                continue

            if urc_ready or time_ready:
                sent_ata = True
                await engine.send_command("ATA", timeout_sec=10.0)
                await asyncio.sleep(0.35)
    except asyncio.CancelledError:
        pass
    finally:
        global _host_auto_answer_task
        ct = asyncio.current_task()
        if _host_auto_answer_task is ct:
            _host_auto_answer_task = None
        async with _host_aa_status_lock:
            _host_aa_status = {"ring_urcs": 0, "note": "stopped"}


async def _stop_host_auto_answer_task() -> None:
    global _host_auto_answer_task
    t = _host_auto_answer_task
    _host_auto_answer_task = None
    if t is not None and not t.done():
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass


def _parse_ats0_rings(lines: list[str]) -> int | None:
    """Parse ``ATS0?`` response: S0 ring count before auto-answer (0 = disabled)."""
    for raw in lines:
        s = raw.strip()
        ul = s.upper()
        if ul in ("OK", "ERROR", ">", ".") or not s:
            continue
        if ul.startswith("+CME ERROR") or ul.startswith("+CMS ERROR"):
            continue
        m = re.match(r"^ATS0:\s*(\d+)\s*$", s, re.I)
        if m:
            return int(m.group(1))
        m2 = re.match(r"^S0:\s*(\d+)\s*$", s, re.I)
        if m2:
            return int(m2.group(1))
        if re.fullmatch(r"\d{1,3}", s):
            return int(s)
    return None


def _parse_ceer(lines: list[str]) -> str | None:
    """Collect ``+CEER:`` / ``CEER:`` payloads from modem response lines (joined if multiple)."""
    parts: list[str] = []
    for raw in lines:
        s = str(raw).strip()
        ul = s.upper()
        if ul.startswith("+CEER:") or ul.startswith("CEER:"):
            payload = s.split(":", 1)[1].strip()
            if payload:
                parts.append(payload)
    if not parts:
        return None
    return "; ".join(parts)


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
    """Parse ``+QNWPREFCFG: "<key>",<value...>`` from modem response lines (case-insensitive header)."""
    key_l = key.lower()
    hdr = re.compile(r"^\s*\+QNWPREFCFG:\s*(.*)$", re.IGNORECASE)
    for raw in lines:
        line = ((raw or "").replace("\ufeff", "")).strip()
        if not line:
            continue
        m = hdr.match(line)
        if not m:
            continue
        payload = m.group(1).strip()
        parts = [p.strip().strip('"') for p in payload.split(",")]
        if len(parts) < 2:
            continue
        if parts[0].lower() != key_l:
            continue
        return ",".join(parts[1:]).strip() or None
    return None


def _lock_read_values_nonempty(values: Any) -> bool:
    if not isinstance(values, dict):
        return False
    for v in values.values():
        if v is None:
            continue
        if str(v).strip():
            return True
    return False


async def _read_lock_status_for_run_csv(*, attempts: int = 4, pause_sec: float = 0.45) -> dict[str, Any]:
    """Read QNWPREFCFG with retries; modem/serial can be busy right after iperf/ping/VoLTE."""
    last: dict[str, Any] = {"values": {}, "raw": {}}
    for attempt in range(int(attempts)):
        try:
            last = await _read_lock_status(timeout_per_key=12.0)
            vals = last.get("values")
            if _lock_read_values_nonempty(vals):
                return last
        except Exception as exc:  # noqa: BLE001
            logger.warning("test run: lock status read attempt %s failed: %s", attempt + 1, exc)
        if attempt + 1 < int(attempts):
            await asyncio.sleep(float(pause_sec))
    if not _lock_read_values_nonempty(last.get("values")):
        raw_hint = ""
        try:
            r0 = (last.get("raw") or {}).get("mode_pref") or {}
            ln = r0.get("lines") if isinstance(r0, dict) else None
            if isinstance(ln, list) and ln:
                raw_hint = " mode_pref_lines=" + repr(ln[-4:])
        except Exception:
            pass
        logger.warning("test run: lock status still empty after %s attempts.%s", attempts, raw_hint)
    return last


async def _read_lock_status(timeout_per_key: float = 8.0) -> dict:
    keys = ["mode_pref", "lte_band", "nr5g_band", "nsa_nr5g_band", "nrdc_mode"]
    raw_map: dict[str, dict] = {}
    out: dict[str, str | None] = {}
    for k in keys:
        res = await engine.send_command(f'AT+QNWPREFCFG="{k}"', timeout_sec=float(timeout_per_key))
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
    # RAT / band preference changes can take many seconds; short timeouts produce false
    # TIMEOUT results, confuse verify readback, and interleave badly with follow-up ATs.
    if "mode_pref" in requested:
        rat = requested["mode_pref"]
        set_results["mode_pref"] = await engine.send_command(
            f'AT+QNWPREFCFG="mode_pref",{rat}', timeout_sec=75.0
        )
    if "lte_band" in requested:
        band = requested["lte_band"]
        set_results["lte_band"] = await engine.send_command(
            f'AT+QNWPREFCFG="lte_band",{band}', timeout_sec=25.0
        )
    if "nr5g_band" in requested:
        band = requested["nr5g_band"]
        set_results["nr5g_band"] = await engine.send_command(
            f'AT+QNWPREFCFG="nr5g_band",{band}', timeout_sec=25.0
        )
        final_nr = str(set_results["nr5g_band"].get("final", "")).upper()
        if final_nr == "OK":
            set_results["nsa_nr5g_band"] = await engine.send_command(
                f'AT+QNWPREFCFG="nsa_nr5g_band",{band}', timeout_sec=25.0
            )
    if "nrdc_mode" in requested:
        mode = requested["nrdc_mode"]
        set_results["nrdc_mode"] = await engine.send_command(
            f'AT+QNWPREFCFG="nrdc_mode",{mode}', timeout_sec=25.0
        )
    return set_results


def _collect_lock_verify_errors(
    normalized_requested: dict[str, str],
    set_results: dict[str, dict],
    locks: dict[str, str | None],
) -> list[str]:
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
    return errors


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


MNO_PROFILES: dict[str, dict[str, str | None]] = {
    # UK profiles requested by user; values are numeric PLMN for COPS format 2.
    "vodafone": {"label": "Vodafone", "plmn": "23415"},
    "vmo2": {"label": "VMO2", "plmn": "23410"},
    "ee": {"label": "EE", "plmn": "23430"},
    "h3g": {"label": "H3G", "plmn": "23420"},
    "auto": {"label": "Auto", "plmn": None},
}
DATA_GATE_UNLOCK_PASSWORD = "kelvin"

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


def _sanitize_pdp_user_or_password(raw: str | None, field: str, *, max_len: int = 64) -> str:
    s = "" if raw is None else str(raw)
    if len(s) > max_len:
        raise HTTPException(status_code=400, detail=f"{field} exceeds maximum length ({max_len}).")
    for ch in s:
        if ch in '"\\\r\n':
            raise HTTPException(
                status_code=400,
                detail=f'{field} must not contain double quotes, backslash, or newlines.',
            )
    return s


UK_LTE_SCAN_BANDS = "1:3:7:8:20:28:32:38"
UK_NR_SCAN_BANDS = "1:3:8:28:78"
MNO_OPERATOR_ALIASES: dict[str, set[str]] = {
    # Common long/short names seen from AT+COPS? format 0 responses.
    "vodafone": {"VODAFONE", "VODAFONE UK", "VODA UK"},
    "vmo2": {"O2", "O2-UK", "TELEFONICA UK", "VMO2"},
    "ee": {"EE", "EE LIMITED", "EE LTD", "TMOBILE UK", "T-MOBILE UK", "ORANGE"},
    "h3g": {"3", "3 UK", "H3G", "THREE", "THREE UK", "HUTCHISON 3G"},
}


def _parse_imei_from_cgsn_lines(lines: list[str]) -> str | None:
    """IMEI from AT+CGSN; skip interleaved URCs (+COPS, +CREG, …) captured in the same response."""
    for raw in lines:
        s = str(raw or "").strip()
        if not s:
            continue
        up = s.upper()
        if up in {"OK", "ERROR"} or up.startswith("AT+"):
            continue
        if s.lstrip().startswith("+"):
            continue
        cand = "".join(ch for ch in s if ch.isdigit())
        if 14 <= len(cand) <= 16:
            return cand
    return None


def _parse_imsi_from_cimi_lines(lines: list[str]) -> str | None:
    """IMSI from AT+CIMI; skip interleaved URCs (+COPS, …)."""
    for raw in lines:
        s = str(raw or "").strip()
        if not s:
            continue
        up = s.upper()
        if up in {"OK", "ERROR"} or up.startswith("AT+"):
            continue
        if s.lstrip().startswith("+"):
            continue
        cand = "".join(ch for ch in s if ch.isdigit())
        if 14 <= len(cand) <= 15:
            return cand
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
        try:
            async with kpi_runtime.lock:
                payload = json.dumps(
                    {
                        "sample": kpi_runtime.snapshot,
                        "poll_running": kpi_runtime.poll_running,
                        "poll_hz": kpi_runtime.poll_hz,
                        "last_error": kpi_runtime.last_error,
                    }
                )
        except Exception:
            logger.exception("KPI WebSocket broadcast: json.dumps failed (snapshot not JSON-serializable?)")
            continue
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
            save_last_serial_state(str(st.get("port") or engine.port), int(st.get("baudrate") or engine.baudrate))
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
        await _stop_host_auto_answer_task()
        await engine.stop()
    finally:
        _release_instance_lock()


app = FastAPI(
    title="5G ModemTestDriver",
    version="3.5",
    lifespan=lifespan,
)
app.include_router(serial_routes.router)


@app.get("/", response_class=HTMLResponse)
async def home() -> HTMLResponse:
    _html_file = Path(__file__).resolve().parent / "static" / "dashboard.html"
    html = _html_file.read_text(encoding="utf-8")
    return HTMLResponse(content=html.replace("__APP_VERSION__", APP_VERSION))




@app.get("/api/sim/high-level")
async def sim_high_level() -> dict:
    imei_res = await engine.send_command("AT+CGSN", timeout_sec=4.0)
    cimi_res = await engine.send_command("AT+CIMI", timeout_sec=4.0)
    qspn_res = await engine.send_command("AT+QSPN", timeout_sec=4.0)
    cops_res = await engine.send_command("AT+COPS?", timeout_sec=4.0)
    cpol_res = await engine.send_command("AT+CPOL?", timeout_sec=8.0)

    imei = _parse_imei_from_cgsn_lines(imei_res.get("lines", []))
    imsi = _parse_imsi_from_cimi_lines(cimi_res.get("lines", []))
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


@app.get("/api/kpi/neighbour-channels")
async def kpi_neighbour_channels() -> dict:
    """Small payload: pre-formatted LTE neighbour lines (not included in WebSocket KPI)."""
    async with kpi_runtime.lock:
        c = kpi_runtime.neighbour_channel_card
        return {
            "ok": True,
            "intra_text": c.get("intra_text") if isinstance(c.get("intra_text"), str) else "-",
            "inter_text": c.get("inter_text") if isinstance(c.get("inter_text"), str) else "-",
            "sample_ts": c.get("sample_ts"),
        }


@app.post("/api/kpi/poll")
async def kpi_poll_config(_body: KpiPollBody = KpiPollBody()) -> dict:
    async with kpi_runtime.lock:
        kpi_runtime.poll_hz = 2.0
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
    contexts = _parse_qiact(qiact_res.get("lines", []))
    active = [c for c in contexts if c.get("active")]
    inhibited = len(active) == 0
    return {
        "ok": True,
        "inhibited": inhibited,
        "packet_attached": attached,
        "active_contexts": active,
        "raw": {"cgatt": cgatt_res, "qiact": qiact_res},
    }


async def _require_packet_data_for_host_traffic_tests() -> None:
    """Reject ping/iperf when PDP is down. Uses AT+QIACT? (same parsing as KPI)."""
    qiact_res = await engine.send_command("AT+QIACT?", timeout_sec=4.0)
    contexts = _parse_qiact(qiact_res.get("lines", []))
    active = [c for c in contexts if c.get("active")]

    if qiact_res.get("ok") and len(active) > 0:
        return
    if qiact_res.get("ok") and len(active) == 0:
        raise HTTPException(
            status_code=409,
            detail="Packet data is inhibited (no active PDP context). Use Allow Data before running ping or iperf.",
        )

    # QIACT? failed (busy modem, timeout) — avoid false "inhibited" using recent KPI snapshot.
    async with kpi_runtime.lock:
        ds = dict(kpi_runtime.data_service or {})
        ds_at = float(kpi_runtime.data_service_at or 0.0)
    now = time.time()
    fresh = ds_at > 0 and (now - ds_at) <= 30.0
    ap = ds.get("active_pdp_contexts")
    c1a = ds.get("cid1_active")
    if fresh and (isinstance(ap, int) and ap > 0 or c1a is True):
        return
    if fresh and (ap == 0 or ap is None) and c1a is False:
        raise HTTPException(
            status_code=409,
            detail="Packet data is inhibited (no active PDP context). Use Allow Data before running ping or iperf.",
        )


@app.post("/api/network/apn")
async def network_apn_set(body: ApnSetBody) -> dict:
    """Set PDP APN via AT+CGDCONT, AT+CGAUTH, AT+QICSGP (password-gated). Optionally QIDEACT, CGATT, QIACT."""
    if (body.password or "") != DATA_GATE_UNLOCK_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password for APN change.")

    apn = _sanitize_apn_for_at(body.apn)
    pdp = _normalize_cgdcont_pdp_type(body.pdp_type)
    cid = int(body.cid)
    auth_t = int(body.pdp_auth_type)
    if auth_t not in (0, 1, 2, 3):
        raise HTTPException(status_code=400, detail="pdp_auth_type must be 0..3.")
    pdp_user = _sanitize_pdp_user_or_password(body.pdp_username, "pdp_username")
    pdp_pass = _sanitize_pdp_user_or_password(body.pdp_password, "pdp_password")

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
            contexts = _parse_qiact(qiact_res.get("lines", []))
            active_here = next((c for c in contexts if c.get("cid") == cid and c.get("active")), None)
            did_ideact = False
            if active_here:
                ideact = await engine.send_command(f"AT+QIDEACT={cid}", timeout_sec=25.0)
                actions.append({"cmd": f"AT+QIDEACT={cid}", "res": ideact})
                did_ideact = True

            cmd = f'AT+CGDCONT={cid},"{pdp}","{apn}"'
            cgd_set = await engine.send_command(cmd, timeout_sec=15.0)
            actions.append({"cmd": cmd, "res": cgd_set})

            if auth_t == 0:
                cgauth_cmd = f"AT+CGAUTH={cid},0"
            else:
                cgauth_cmd = f'AT+CGAUTH={cid},{auth_t},"{pdp_user}","{pdp_pass}"'
            cgauth_res = await engine.send_command(cgauth_cmd, timeout_sec=15.0)
            actions.append({"cmd": cgauth_cmd, "res": cgauth_res})

            q_user = "" if auth_t == 0 else pdp_user
            q_pass = "" if auth_t == 0 else pdp_pass
            qic_cmd = f'AT+QICSGP={cid},1,"{apn}","{q_user}","{q_pass}",{auth_t}'
            qic_res = await engine.send_command(qic_cmd, timeout_sec=15.0)
            actions.append(
                {
                    "cmd": qic_cmd + " (Quectel PDP stack mirror; OK if modem supports)",
                    "res": qic_res,
                }
            )

            cgd_ok = bool(cgd_set.get("ok", False))
            cgauth_ok = bool(cgauth_res.get("ok", False))
            qic_ok = bool(qic_res.get("ok", False))
            set_ok = cgd_ok and cgauth_ok and qic_ok

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

            cgauth_read_res = await engine.send_command("AT+CGAUTH?", timeout_sec=4.0)
            actions.append({"cmd": "AT+CGAUTH? (readback)", "res": cgauth_read_res})
            qicsgp_read_res = await engine.send_command("AT+QICSGP?", timeout_sec=4.0)
            actions.append({"cmd": "AT+QICSGP? (readback)", "res": qicsgp_read_res})
            qicsgp_read_res = await _read_qicsgp_best_effort(
                engine,
                cid,
                timeout_sec=4.0,
                initial=qicsgp_read_res,
                probe_append=actions,
            )
            auth_rows = _parse_cgauth(cgauth_read_res.get("lines", []))
            qicsgp_rows = _parse_qicsgp(qicsgp_read_res.get("lines", []))
            ca_one = next((r for r in auth_rows if r.get("cid") == cid), None)
            qi_one = next((r for r in qicsgp_rows if r.get("cid") == cid), None)

            if not set_ok:
                parts = []
                if not cgd_ok:
                    parts.append("AT+CGDCONT failed.")
                if not cgauth_ok:
                    parts.append("AT+CGAUTH failed.")
                if not qic_ok:
                    parts.append("AT+QICSGP failed.")
                msg = " ".join(parts) if parts else "APN profile update failed."
            elif reattach_errs:
                msg = (
                    "APN + auth saved (CGDCONT + CGAUTH + QICSGP) but CGATT/QIACT reattachment did not complete successfully. "
                    "Use Allow Data or retry reconnect."
                )
            elif body.reactivate:
                msg = "APN updated (CGDCONT + CGAUTH + Quectel QICSGP); packet data reattached (QIACT)."
            elif did_ideact:
                msg = (
                    "APN + auth stored; PDP context was deactivated to apply profile. "
                    "Press Allow Data to reconnect with the new settings."
                )
            else:
                msg = "APN + auth stored (CGDCONT + CGAUTH + QICSGP). Use Allow Data if you need an immediate reconnect."

            md_parts = []
            if not cgd_ok:
                md_parts.append(describe_modem_send_result(cgd_set))
            if not cgauth_ok:
                md_parts.append(describe_modem_send_result(cgauth_res))
            if not qic_ok:
                md_parts.append(describe_modem_send_result(qic_res))
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
                "pdp_auth_type": auth_t,
                "pdp_username": pdp_user,
                "primary_context": primary,
                "cgdcont_contexts": contexts_parsed,
                "auth_profile_read": {"cgauth": ca_one, "qicsgp": qi_one, "rows_cgauth": auth_rows, "rows_qicsgp": qicsgp_rows},
                "reactivate_requested": bool(body.reactivate),
                "did_pdp_detach": bool(did_ideact),
                "message": msg,
                "actions": actions,
                "raw": {
                    "cgatt_before": cgatt_before_res,
                    "qiact_before": qiact_res,
                    "cgdcont_read": read_res,
                    "cgauth_read": cgauth_read_res,
                    "qicsgp_read": qicsgp_read_res,
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
    if "mode_pref" in requested:
        await asyncio.sleep(2.5)
    lock_state = await _read_lock_status()
    locks = lock_state["values"]
    errors = _collect_lock_verify_errors(normalized_requested, set_results, locks)
    if errors:
        await asyncio.sleep(2.8)
        lock_state = await _read_lock_status()
        locks = lock_state["values"]
        errors = _collect_lock_verify_errors(normalized_requested, set_results, locks)

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


def _iperf_resolve_port(body: IperfTestBody) -> tuple[int, dict[str, int] | None]:
    """Pick the TCP server port for this run; optional inclusive high bound."""
    lo = int(body.port)
    hi = body.port_range_max
    if hi is None:
        return lo, None
    hi_i = int(hi)
    if hi_i < lo:
        raise HTTPException(status_code=400, detail="port_range_max must be greater than or equal to port.")
    return random.randint(lo, hi_i), {"min": lo, "max": hi_i}


@app.post("/api/tools/iperf-test")
async def tools_iperf_test(body: IperfTestBody) -> dict:
    await _require_packet_data_for_host_traffic_tests()
    host = str(body.host or "").strip()
    if not host:
        raise HTTPException(status_code=400, detail="host is required.")
    direction = str(body.direction or "download").strip().lower()
    if direction not in ("download", "upload"):
        raise HTTPException(status_code=400, detail="direction must be 'download' or 'upload'.")
    protocol = str(body.protocol or "tcp").strip().lower()
    if protocol not in ("tcp", "udp"):
        raise HTTPException(status_code=400, detail="protocol must be 'tcp' or 'udp'.")
    reverse = direction == "download"
    parallel_streams = int(body.parallel_streams)
    ct_sec = max(1.0, min(120.0, float(body.connect_timeout_sec)))
    effective_port, port_range_requested = _iperf_resolve_port(body)
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
                "error": (
                    "Mobile-only mode could not find a usable mobile IPv4 on this Windows host "
                    "(modem cid1_ip from KPI plus ipconfig adapters matching USB/cellular/WWAN; "
                    "each candidate must pass a local bind test — avoids WinError 10049 when iperf -B uses "
                    "an address that is not on an active NIC). Use *Refresh ifaces*, set Manual bind_ip, "
                    "or turn off Mobile-only."
                ),
                "host": host,
                "port": effective_port,
                "port_range_requested": port_range_requested,
                "duration_sec": int(body.duration_sec),
                "direction": direction,
                "protocol": protocol,
                "mobile_only": bool(body.mobile_only),
                "bind_ip": None,
                "bitrate_limit_mbps": limit_mbps,
                "parallel_streams": parallel_streams,
                "connect_timeout_sec": ct_sec,
            }
    if bind_ip and os.name == "nt" and not _windows_ipv4_assignable_for_bind(bind_ip):
        return {
            "ok": False,
            "error": (
                f"bind_ip {bind_ip!r} is not assigned to an active local adapter on Windows "
                "(local bind check failed — the same situation as iperf WinError 10049). "
                "Choose an IPv4 from *Refresh ifaces* / ipconfig, ensure packet data is up, "
                "or disable Mobile-only."
            ),
            "host": host,
            "port": effective_port,
            "port_range_requested": port_range_requested,
            "duration_sec": int(body.duration_sec),
            "direction": direction,
            "protocol": protocol,
            "mobile_only": bool(body.mobile_only),
            "bind_ip": bind_ip,
            "detected_mobile_adapter": detected_adapter,
            "bitrate_limit_mbps": limit_mbps,
            "parallel_streams": parallel_streams,
            "connect_timeout_sec": ct_sec,
        }
    binary = _discover_iperf_binary()
    if not binary:
        return {
            "ok": False,
            "error": "iperf3 binary not found. Place iperf3.exe in project/backend root or set MD_IPERF_BIN.",
            "host": host,
            "port": effective_port,
            "port_range_requested": port_range_requested,
            "duration_sec": int(body.duration_sec),
            "direction": direction,
            "protocol": protocol,
            "mobile_only": bool(body.mobile_only),
            "bind_ip": bind_ip,
            "bitrate_limit_mbps": limit_mbps,
            "parallel_streams": parallel_streams,
            "connect_timeout_sec": ct_sec,
        }
    cmd = [
        binary,
        "-c",
        host,
        # Force IPv4: hostname may resolve to IPv6 while -B binds an IPv4, which yields exit=1.
        "-4",
        "-p",
        str(int(effective_port)),
        "-t",
        str(int(body.duration_sec)),
        "-J",
    ]
    if reverse:
        cmd.append("-R")
    if protocol == "udp":
        cmd.append("-u")
    if bind_ip:
        cmd.extend(["-B", bind_ip])
    if limit_mbps is not None and float(limit_mbps) > 0:
        # iperf3 -b is per-stream; divide total limit across parallel streams so the
        # aggregate bandwidth matches what the user entered.
        # For UDP this is a hard application-level cap; for TCP it is a pacing hint
        # (effective on Linux only — Windows/Cygwin ignores it for TCP).
        per_stream_mbps = float(limit_mbps) / max(1, parallel_streams)
        cmd.extend(["-b", f"{per_stream_mbps:g}M"])
    elif protocol == "udp":
        # iperf3 UDP defaults to 1 Mbit/s which is far too low for a throughput test.
        # Send at wire speed and let the network be the bottleneck.
        cmd.extend(["-b", "0"])
    cmd.extend(["-P", str(parallel_streams)])
    supports_ct = _iperf_supports_connect_timeout(binary)
    if supports_ct:
        ms = max(1, int(round(float(ct_sec) * 1000.0)))
        cmd.extend(["--connect-timeout", str(ms)])
    else:
        # Bundled iperf 3.1.x: no --connect-timeout; OS TCP can stall far longer than ct_sec.
        # Skip TCP preflight for UDP — the preflight opens a TCP socket which is irrelevant.
        probe_err = None if protocol == "udp" else await asyncio.to_thread(
            _iperf_preflight_tcp_ipv4,
            host,
            int(effective_port),
            timeout_sec=float(ct_sec),
            bind_ip=bind_ip,
        )
        if probe_err:
            return {
                "ok": False,
                "error": probe_err,
                "host": host,
                "port": effective_port,
                "port_range_requested": port_range_requested,
                "duration_sec": int(body.duration_sec),
                "direction": direction,
                "protocol": protocol,
                "mobile_only": bool(body.mobile_only),
                "bind_ip": bind_ip,
                "detected_mobile_adapter": detected_adapter,
                "bitrate_limit_mbps": limit_mbps,
                "parallel_streams": parallel_streams,
                "connect_timeout_sec": ct_sec,
                "throughput_mbps": None,
                "throughput_source": None,
                "command": cmd,
                "exit_code": None,
                "json_parse_error": None,
                "stderr_tail": "",
                "stdout_head": "",
                "raw": None,
            }
    dur = int(body.duration_sec)
    # Wall-clock often exceeds iperf -t: TCP slow-start, JSON flush, cellular UL teardown.
    # Upload (no -R) is slower and needs more headroom than download (-R).
    slack_dl = 40
    slack_ul = max(75, dur // 2 + 60)
    stream_slack = min(120, max(0, parallel_streams - 1) * 10)
    connect_slack = int(math.ceil(float(ct_sec)))
    timeout_sec = dur + (slack_dl if reverse else slack_ul) + stream_slack + connect_slack
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
            "port": effective_port,
            "port_range_requested": port_range_requested,
            "duration_sec": int(body.duration_sec),
            "direction": direction,
            "protocol": protocol,
            "mobile_only": bool(body.mobile_only),
            "bind_ip": bind_ip,
            "bitrate_limit_mbps": limit_mbps,
            "parallel_streams": parallel_streams,
            "connect_timeout_sec": ct_sec,
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
        "port": effective_port,
        "port_range_requested": port_range_requested,
        "duration_sec": int(body.duration_sec),
        "direction": direction,
        "protocol": protocol,
        "mobile_only": bool(body.mobile_only),
        "bind_ip": bind_ip,
        "detected_mobile_adapter": detected_adapter,
        "bitrate_limit_mbps": limit_mbps,
        "parallel_streams": parallel_streams,
        "connect_timeout_sec": ct_sec,
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
    await _require_packet_data_for_host_traffic_tests()
    host = body.host.strip() or "8.8.8.8"
    count = int(body.count)
    bind = str(body.bind_ipv4 or "").strip() or None
    if bind:
        try:
            ipaddress.IPv4Address(bind)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"Invalid bind_ipv4: {bind}") from exc

    ping_timeout_ms: int | None = None
    if os.name == "nt":
        w_ms = int(body.timeout_ms) if body.timeout_ms is not None else 3000
        w_ms = max(500, min(60000, w_ms))
        ping_timeout_ms = int(w_ms)
        cmd = ["ping", "-4", "-n", str(count), "-w", str(w_ms)]
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
        "timeout_ms": ping_timeout_ms,
        "command": cmd,
        "exit_code": proc.returncode,
        "stdout_tail": stdout[-4000:] if stdout else "",
    }


@app.get("/api/tools/auto-answer")
async def tools_auto_answer_read() -> dict[str, Any]:
    """Read S0 (rings before auto-answer). ``ATS0=0`` means auto-answer disabled."""
    res = await engine.send_command("ATS0?", timeout_sec=4.0)
    rings = _parse_ats0_rings(res.get("lines") or [])
    ok_cmd = bool(res.get("ok"))
    return {
        "ok": ok_cmd,
        "s0_rings": rings,
        "auto_answer_enabled": rings is not None and rings > 0,
        "raw": res,
    }


@app.post("/api/tools/auto-answer")
async def tools_auto_answer_set(body: AutoAnswerSetBody) -> dict[str, Any]:
    if (body.password or "") != DATA_GATE_UNLOCK_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password for auto-answer control.")

    cmd = "ATS0=0" if not body.enabled else f"ATS0={int(body.rings)}"
    set_res = await engine.send_command(cmd, timeout_sec=4.0)
    read_res = await engine.send_command("ATS0?", timeout_sec=4.0)
    rings_after = _parse_ats0_rings(read_res.get("lines") or [])
    ok = bool(set_res.get("ok")) and bool(read_res.get("ok"))
    return {
        "ok": ok,
        "s0_rings": rings_after,
        "auto_answer_enabled": rings_after is not None and rings_after > 0,
        "set_command": cmd,
        "raw_set": set_res,
        "raw_readback": read_res,
    }


@app.get("/api/tools/host-auto-answer")
async def tools_host_auto_answer_read() -> dict[str, Any]:
    """Whether the PC-side ``ATA`` watcher is running (VoLTE-friendly auto-answer)."""
    global _host_auto_answer_task, _host_aa_rings
    running = _host_auto_answer_task is not None and not _host_auto_answer_task.done()
    async with _host_aa_status_lock:
        st = dict(_host_aa_status)
    return {
        "ok": True,
        "enabled": running,
        "rings": _host_aa_rings,
        **st,
    }


@app.post("/api/tools/host-auto-answer")
async def tools_host_auto_answer_set(body: HostAutoAnswerBody) -> dict[str, Any]:
    global _host_auto_answer_task, _host_aa_rings
    if body.enabled:
        if (body.password or "") != DATA_GATE_UNLOCK_PASSWORD:
            raise HTTPException(status_code=403, detail="Invalid password for host auto-answer.")
        await _stop_host_auto_answer_task()
        _host_aa_rings = int(body.rings)
        _host_auto_answer_task = asyncio.create_task(
            _host_auto_answer_worker(_host_aa_rings, str(body.password or ""))
        )
        return {"ok": True, "enabled": True, "rings": _host_aa_rings}
    await _stop_host_auto_answer_task()
    return {"ok": True, "enabled": False, "rings": _host_aa_rings}


@app.get("/api/tools/voice-call-status")
async def tools_voice_call_status() -> dict[str, Any]:
    """Live voice indication from ``AT+CLCC`` plus *recent* URCs (RING / +CRING, time-limited)."""
    rows, clcc_cmd_ok = await _voice_clcc_snapshot(force=False)
    voice_rows = _clcc_rows_voice_only(rows)
    summary = _summarize_voice_call_state(voice_rows)
    urc_entries = list(engine.urc_log)
    recent_ring = _urc_recent_incoming_ring(urc_entries)
    clcc_incoming = bool(summary["incoming_ringing"])
    incoming = clcc_incoming or recent_ring
    stats = [r.get("stat") for r in voice_rows]
    in_voice_call = any(s in (0, 1) for s in stats)
    can_answer = incoming and not in_voice_call
    can_hangup = bool(summary["call_present"]) or recent_ring or clcc_incoming
    running_haa = _host_auto_answer_task is not None and not _host_auto_answer_task.done()
    async with _host_aa_status_lock:
        haa_snap = dict(_host_aa_status)
    return {
        "ok": True,
        "clcc_ok": clcc_cmd_ok,
        "clcc": rows,
        "hook": summary["hook"],
        "line_state": summary["line_state"],
        "primary_number": summary["primary_number"],
        "incoming_ringing": incoming,
        "incoming_clcc": clcc_incoming,
        "recent_ring_urc": recent_ring,
        "can_answer": can_answer,
        "can_hangup": can_hangup,
        "host_auto_answer": {
            "enabled": running_haa,
            "rings": _host_aa_rings,
            **haa_snap,
        },
    }


@app.post("/api/tools/voice-answer")
async def tools_voice_answer(body: VoiceAnswerBody) -> dict[str, Any]:
    """Answer incoming voice with ``ATA`` (27.007). Password-gated."""
    if (body.password or "") != DATA_GATE_UNLOCK_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password for answer call.")

    ata_res = await engine.send_command("ATA", timeout_sec=10.0)
    await asyncio.sleep(0.4)
    clcc_res = await engine.send_command("AT+CLCC", timeout_sec=3.0)
    rows = _parse_clcc_lines(clcc_res.get("lines", []))
    connected = any((r.get("stat") in (0, 1)) for r in _clcc_rows_voice_only(rows))
    ok = bool(ata_res.get("ok")) or connected
    err = None if ok else "ATA did not complete OK and CLCC still shows no active call."
    return {
        "ok": ok,
        "error": err,
        "ata": ata_res,
        "clcc_after": rows,
    }


@app.post("/api/tools/voice-hangup")
async def tools_voice_hangup(body: VoiceHangupBody) -> dict[str, Any]:
    """Send ``ATH`` to release or reject the current voice session (password-gated)."""
    if (body.password or "") != DATA_GATE_UNLOCK_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password for hang up.")

    hang_attempts: list[dict[str, Any]] = []
    hang_attempts.append(await engine.send_command("ATH", timeout_sec=5.0))
    await asyncio.sleep(0.35)

    def _still_live_voice(rows_in: list[dict]) -> bool:
        v = _clcc_rows_voice_only(rows_in)
        return any((r.get("stat") in (0, 1, 2, 3, 4, 5)) for r in v)

    end_deadline = asyncio.get_running_loop().time() + 14.0
    last_rows: list[dict] = []
    while asyncio.get_running_loop().time() < end_deadline:
        clcc_res = await engine.send_command("AT+CLCC", timeout_sec=3.0)
        last_rows = _parse_clcc_lines(clcc_res.get("lines", []))
        if not last_rows:
            break
        if _still_live_voice(last_rows) and len(hang_attempts) < 3:
            hang_attempts.append(await engine.send_command("ATH", timeout_sec=5.0))
            await asyncio.sleep(0.35)
            continue
        break

    still = bool(last_rows) and _still_live_voice(last_rows)
    ok = not still
    err = None if ok else "Call may still be active; check AT+CLCC or retry ATH."
    return {
        "ok": ok,
        "error": err,
        "hang_attempts": hang_attempts,
        "clcc_after": last_rows,
    }


@app.post("/api/tools/volte-test")
async def tools_volte_test(body: VolteTestBody) -> dict:
    if (body.password or "") != DATA_GATE_UNLOCK_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password for VoLTE call test.")

    number = _sanitize_dial_number(body.number)
    if not number:
        raise HTTPException(status_code=400, detail="Invalid dial number.")

    hold_sec = int(body.hold_sec or 10)
    connect_timeout_sec = int(body.connect_timeout_sec or 120)

    # Ensure no stale call exists before test.
    pre_hang = await engine.send_command("ATH", timeout_sec=4.0)
    await asyncio.sleep(0.25)

    before_urc = list(engine.urc_log)
    nw_before_res = await engine.send_command("AT+QNWINFO", timeout_sec=3.0)
    nw_before = _parse_qnwinfo_line(nw_before_res.get("lines", []))

    dial_started = time.time()
    dial_res = await engine.send_command(f"ATD{number};", timeout_sec=8.0)
    dial_ok = bool(dial_res.get("ok", False))

    deadline = asyncio.get_running_loop().time() + float(connect_timeout_sec)
    call_connected = False
    connect_ts: float | None = None
    clcc_states: list[dict] = []
    last_clcc: list[dict] = []
    while asyncio.get_running_loop().time() < deadline:
        clcc_res = await engine.send_command("AT+CLCC", timeout_sec=3.0)
        clcc = _parse_clcc_lines(clcc_res.get("lines", []))
        last_clcc = clcc
        voice_rows = _clcc_rows_voice_only(clcc)
        if any((r.get("stat") in (0, 1)) for r in voice_rows):
            call_connected = True
            connect_ts = time.time()
            break
        rep_stat = voice_rows[0].get("stat") if voice_rows else None
        clcc_states.append(
            {
                "t_s": round(time.time() - dial_started, 1),
                "status": _clcc_stat_label(rep_stat),
                "raw_stat": rep_stat,
            }
        )
        if not clcc:
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

    end_deadline = asyncio.get_running_loop().time() + 18.0
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
        # Voice-only: packet contexts (mode 1) often stay stat active after ATH and must not fail the test.
        if not _clcc_voice_active_or_held(clcc_after):
            break
        if len(hang_attempts) < 5:
            nxt = "AT+CHUP" if len(hang_attempts) % 2 == 1 else "ATH"
            hang_attempts.append(await engine.send_command(nxt, timeout_sec=5.0))
            await asyncio.sleep(0.5)
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
        ln
        for _, ln in delta_urc
        if any(tok in str(ln).upper() for tok in ("NO CARRIER", "+CLCC", "+CEER", "BUSY", "NO ANSWER", "NO DIALTONE"))
    ]

    setup_time_ms = int((connect_ts - dial_started) * 1000) if connect_ts else None
    active_after_hang = _clcc_voice_active_or_held(clcc_after)
    ok = bool(dial_ok and call_connected and not active_after_hang)
    error = None
    if not ok:
        if not dial_ok:
            error = f"Dial command failed ({dial_res.get('final') or 'no final'})"
        elif not call_connected:
            error = f"Call did not reach connected state within {connect_timeout_sec}s."
        elif active_after_hang:
            error = "Call still appears active/held after hangup retries."

    return {
        "ok": ok,
        "error": error,
        "number": number,
        "hold_sec": hold_sec,
        "connect_timeout_sec": connect_timeout_sec,
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


async def _server_ui_state_for_test_export() -> dict[str, Any]:
    st = await engine.status()
    async with kpi_runtime.lock:
        return {
            "serial_port": st.get("port"),
            "serial_baudrate": st.get("baudrate"),
            "serial_open": st.get("serial_open"),
            "serial_queue_depth": st.get("queue_depth"),
            "kpi_poll_hz": kpi_runtime.poll_hz,
            "kpi_poll_running": kpi_runtime.poll_running,
            "kpi_last_error": kpi_runtime.last_error,
        }


async def _apply_modem_requirements(mr: Any, test_type: str) -> None:
    if not isinstance(mr, dict) or not mr:
        return
    if mr.get("require_packet_data") and test_type in (
        "ping",
        "iperf_download",
        "iperf_upload",
        "iperf_download_upload",
    ):
        await _require_packet_data_for_host_traffic_tests()
    if mr.get("require_serving_cell"):
        deadline = time.time() + float(mr.get("max_start_wait_sec") or 60)
        min_r = mr.get("min_rsrp_dbm")
        while time.time() < deadline:
            async with kpi_runtime.lock:
                net = kpi_runtime.snapshot.get("network") or {}
                srv = kpi_runtime.snapshot.get("servingcell") or {}
                lte = srv.get("lte") or {}
                service_ok = str(net.get("service") or "").upper() != "NO SERVICE" and bool(net.get("act"))
                rsrp = lte.get("rsrp")
            if service_ok and (min_r is None or (isinstance(rsrp, (int, float)) and float(rsrp) >= float(min_r))):
                return
            await asyncio.sleep(1.0)
        raise HTTPException(
            status_code=400,
            detail="modem_requirements: serving cell / RSRP gate not met within max_start_wait_sec.",
        )


@app.get("/api/ui/state")
async def api_ui_state() -> dict[str, Any]:
    s = await _server_ui_state_for_test_export()
    return {"ok": True, "ui_controls": {"server": s}}


@app.get("/api/test/profiles")
async def api_test_profiles_list() -> dict[str, Any]:
    merged = tr.list_merged_profiles()
    names = [str(p.get("name") or "").strip() for p in merged if isinstance(p, dict)]
    return {
        "ok": True,
        "profiles": merged,
        "names": names,
        "example_profile_names": tr.example_only_profile_names(),
        "bundled_examples_dir": tr.bundled_example_profiles_dir(),
        "test_cases_dir": tr.test_case_profiles_dir(),
        "test_results_root_dir": tr.test_results_root_dir(),
        "automated_tests_root": tr.automated_tests_root(),
    }


@app.get("/api/test/profiles/{name}")
async def api_test_profiles_get(name: str) -> dict[str, Any]:
    p = tr.get_profile_by_name(name)
    if not p:
        raise HTTPException(status_code=404, detail="Profile not found.")
    return {"ok": True, "profile": p}


@app.post("/api/test/profiles")
async def api_test_profiles_upsert(body: dict[str, Any]) -> dict[str, Any]:
    try:
        tr.upsert_profile(body)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, "name": body.get("name")}


@app.delete("/api/test/profiles/{name}")
async def api_test_profiles_delete(name: str) -> dict[str, Any]:
    if not tr.delete_profile(name):
        raise HTTPException(status_code=404, detail="Profile not found.")
    return {"ok": True}


_RUN_DOWNLOAD_KIND = {"summary": "_summary.csv", "kpi": "_kpi.jsonl", "ui": "_ui.json"}


@app.get("/api/test/download/{run_id}/{kind}")
async def api_test_download(run_id: str, kind: str) -> FileResponse:
    if not re.fullmatch(r"[0-9a-fA-F]{8}", run_id or ""):
        raise HTTPException(status_code=400, detail="Invalid run_id.")
    low = kind.lower()
    if low not in _RUN_DOWNLOAD_KIND:
        raise HTTPException(status_code=400, detail="kind must be summary, kpi, or ui.")
    parent = tr.resolve_run_artifacts_dir(run_id)
    if not parent:
        raise HTTPException(status_code=404, detail="Artifact not found.")
    path = os.path.join(parent, f"run_{run_id.lower()}{_RUN_DOWNLOAD_KIND[low]}")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Artifact not found.")
    media = "text/csv" if low == "summary" else "application/x-ndjson" if low == "kpi" else "application/json"
    fname = os.path.basename(path)
    return FileResponse(path, filename=fname, media_type=media)


async def _interruptible_sleep_test_delay(total_sec: float, cancel_ev: asyncio.Event) -> bool:
    """Return True if *cancel_ev* was set before the full delay elapsed."""
    total_sec = max(0.0, float(total_sec))
    deadline = time.time() + total_sec
    while True:
        if cancel_ev.is_set():
            return True
        now = time.time()
        if now >= deadline:
            return False
        await asyncio.sleep(min(0.25, deadline - now))


@app.get("/api/test/active")
async def api_test_active() -> dict[str, Any]:
    idle: dict[str, Any] = {
        "active": False,
        "run_id": None,
        "phase": None,
        "iterations_total": None,
        "iteration_running": None,
        "iteration_next": None,
        "seconds_until_next": None,
    }
    async with _test_run_session_lock:
        if not _test_run_session:
            return idle
        sess = _test_run_session
        rid = str(sess.get("run_id") or "")
        st = sess.get("state")
        if not isinstance(st, dict):
            return {
                "active": True,
                "run_id": rid,
                "phase": None,
                "iterations_total": None,
                "iteration_running": None,
                "iteration_next": None,
                "seconds_until_next": None,
            }
        phase = st.get("phase")
        it_total = st.get("iterations_total")
        it_run = st.get("iteration_running")
        it_next = st.get("iteration_next")
        sec_until: float | None = None
        if phase == "delay":
            ddl = st.get("delay_deadline_ts")
            if isinstance(ddl, (int, float)):
                sec_until = max(0.0, float(ddl) - time.time())
        return {
            "active": True,
            "run_id": rid,
            "phase": phase,
            "iterations_total": it_total,
            "iteration_running": it_run,
            "iteration_next": it_next,
            "seconds_until_next": round(sec_until, 1) if sec_until is not None else None,
        }


@app.post("/api/test/cancel")
async def api_test_cancel(body: TestCancelBody = Body(default_factory=TestCancelBody)) -> dict[str, Any]:
    want = (body.run_id or "").strip().lower()
    async with _test_run_session_lock:
        sess = _test_run_session
        if not sess:
            raise HTTPException(status_code=404, detail="No test run in progress.")
        cur = str(sess.get("run_id") or "").lower()
        if want and want != cur:
            raise HTTPException(status_code=409, detail="run_id does not match the active test run.")
        ev: asyncio.Event = sess["cancel"]
        ev.set()
    return {
        "ok": True,
        "run_id": cur,
        "message": "Cancellation requested; the run stops after the current tool step and any interruptible delay.",
    }


@app.post("/api/test/run")
async def api_test_run(body: TestRunBody) -> dict[str, Any]:
    global _test_run_session
    prof = tr.get_profile_by_name(body.profile_name.strip())
    if not prof:
        raise HTTPException(status_code=404, detail="Unknown profile_name.")
    errs = tr.validate_profile(prof)
    if errs:
        raise HTTPException(status_code=400, detail="; ".join(errs))
    if not (body.unlock_password or "").strip():
        raise HTTPException(status_code=400, detail="unlock_password is required for test runs.")
    if (body.unlock_password or "") != DATA_GATE_UNLOCK_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid unlock_password for test run.")
    test_type = str(prof.get("test_type") or "").strip()
    cfg = prof.get("test_config") if isinstance(prof.get("test_config"), dict) else {}
    mr = prof.get("modem_requirements") if isinstance(prof.get("modem_requirements"), dict) else {}

    project_name = (body.project_name or "").strip() or str(prof.get("project_name") or "").strip()
    test_location = (body.test_location or "").strip() or str(prof.get("test_location") or "").strip()
    engineer = (body.engineer or "").strip() or str(prof.get("engineer") or "").strip()
    run_note = (body.note or "").strip()

    run_id = tr.new_run_id()
    started = time.time()
    out_dir = tr.prepare_run_artifacts_dir(
        project_name=project_name,
        test_location=test_location,
        started_ts=started,
        run_id=run_id,
    )
    csv_path = os.path.join(out_dir, f"run_{run_id}_summary.csv")
    kpi_path = os.path.join(out_dir, f"run_{run_id}_kpi.jsonl")
    ui_path = os.path.join(out_dir, f"run_{run_id}_ui.json")

    n_it = max(1, min(100, int(body.test_iterations)))
    delay_s = max(10.0, min(3600.0, float(body.test_iteration_delay_sec)))
    if delay_s == int(delay_s):
        delay_csv = str(int(delay_s))
    else:
        delay_csv = str(round(delay_s, 3)).rstrip("0").rstrip(".")

    cancel_ev = asyncio.Event()
    run_state: dict[str, Any] = {"cancelled": False}
    iteration_log: list[dict[str, Any]] = []
    cancel_tool_result: dict[str, Any] = {"ok": False, "error": "Test run cancelled.", "cancelled": True}
    tr_progress_state: dict[str, Any] = {
        "phase": "queued",
        "iterations_total": n_it,
        "iteration_running": None,
        "iteration_next": None,
        "delay_deadline_ts": None,
    }

    async def run_one_tool() -> dict[str, Any]:
        tr_progress_state["phase"] = "modem_requirements"
        tr_progress_state["iteration_running"] = None
        tr_progress_state["iteration_next"] = None
        tr_progress_state["delay_deadline_ts"] = None
        await _apply_modem_requirements(mr, test_type)
        iteration_log.clear()
        last_tr: dict[str, Any] = {"ok": False, "error": "no result"}
        for i in range(n_it):
            if cancel_ev.is_set():
                run_state["cancelled"] = True
                return cancel_tool_result
            tr_progress_state["phase"] = "tool"
            tr_progress_state["iteration_running"] = i + 1
            tr_progress_state["iteration_next"] = None
            tr_progress_state["delay_deadline_ts"] = None
            t_iter0 = time.time()
            if test_type == "ping":
                bind_ip: str | None
                if body.ping_bind_ipv4_override is not None:
                    bind_ip = str(body.ping_bind_ipv4_override).strip() or None
                else:
                    bind_ip = str(cfg.get("bind_ipv4") or "").strip() or None
                last_tr = await tools_icmp_ping(
                    IcmpPingSweepBody(
                        host=str(cfg["host"]),
                        count=int(cfg["count"]),
                        bind_ipv4=bind_ip,
                        timeout_ms=int(cfg["timeout_ms"]),
                    )
                )
            elif test_type == "iperf_download":
                lim = cfg.get("bitrate_limit_mbps")
                lim_f = float(lim) if lim is not None else None
                if lim_f is not None and lim_f <= 0:
                    lim_f = None
                prm_raw = cfg.get("port_range_max")
                prm_i: int | None
                if prm_raw is None or prm_raw == "":
                    prm_i = None
                else:
                    prm_i = int(prm_raw)
                last_tr = await tools_iperf_test(
                    IperfTestBody(
                        host=str(cfg["host"]),
                        port=int(cfg["port"]),
                        port_range_max=prm_i,
                        duration_sec=int(cfg["duration_sec"]),
                        direction="download",
                        protocol=str(cfg.get("protocol") or "tcp").lower(),
                        mobile_only=bool(cfg["mobile_only"]),
                        bind_ip=None,
                        bitrate_limit_mbps=lim_f,
                        parallel_streams=int(cfg.get("parallel_streams_dl") or cfg["parallel_streams"]),
                        connect_timeout_sec=_iperf_connect_timeout_for_profile(cfg),
                    )
                )
            elif test_type == "iperf_upload":
                lim = cfg.get("bitrate_limit_mbps")
                lim_f = float(lim) if lim is not None else None
                if lim_f is not None and lim_f <= 0:
                    lim_f = None
                prm_raw = cfg.get("port_range_max")
                prm_i = None if prm_raw is None or prm_raw == "" else int(prm_raw)
                last_tr = await tools_iperf_test(
                    IperfTestBody(
                        host=str(cfg["host"]),
                        port=int(cfg["port"]),
                        port_range_max=prm_i,
                        duration_sec=int(cfg["duration_sec"]),
                        direction="upload",
                        protocol=str(cfg.get("protocol") or "tcp").lower(),
                        mobile_only=bool(cfg["mobile_only"]),
                        bind_ip=None,
                        bitrate_limit_mbps=lim_f,
                        parallel_streams=int(cfg.get("parallel_streams_ul") or cfg["parallel_streams"]),
                        connect_timeout_sec=_iperf_connect_timeout_for_profile(cfg),
                    )
                )
            elif test_type == "iperf_download_upload":
                lim = cfg.get("bitrate_limit_mbps")
                lim_f = float(lim) if lim is not None else None
                if lim_f is not None and lim_f <= 0:
                    lim_f = None
                prm_raw = cfg.get("port_range_max")
                prm_i = None if prm_raw is None or prm_raw == "" else int(prm_raw)
                conn_to = _iperf_connect_timeout_for_profile(cfg)
                ps_dl = int(cfg.get("parallel_streams_dl") or cfg["parallel_streams"])
                ps_ul = int(cfg.get("parallel_streams_ul") or cfg["parallel_streams"])
                j_dl = await tools_iperf_test(
                    IperfTestBody(
                        host=str(cfg["host"]),
                        port=int(cfg["port"]),
                        port_range_max=prm_i,
                        duration_sec=int(cfg["duration_sec"]),
                        direction="download",
                        protocol=str(cfg.get("protocol") or "tcp").lower(),
                        mobile_only=bool(cfg["mobile_only"]),
                        bind_ip=None,
                        bitrate_limit_mbps=lim_f,
                        parallel_streams=ps_dl,
                        connect_timeout_sec=conn_to,
                    )
                )
                if not j_dl.get("ok"):
                    last_tr = {
                        "ok": False,
                        "error": str(j_dl.get("error") or "").strip() or "iperf download failed",
                        "phase": "download",
                        "download": j_dl,
                        "upload": None,
                        "host": str(j_dl.get("host") or cfg.get("host") or ""),
                        "port": int(j_dl.get("port") or cfg.get("port") or 0),
                        "duration_sec": int(cfg["duration_sec"]),
                        "parallel_streams_dl": ps_dl,
                        "parallel_streams_ul": ps_ul,
                        "protocol": str(cfg.get("protocol") or "tcp").lower(),
                        "mobile_only": bool(cfg["mobile_only"]),
                        "connect_timeout_sec": conn_to,
                        "direction": "download_upload",
                        "throughput_mbps_dl": j_dl.get("throughput_mbps"),
                        "throughput_mbps_ul": None,
                    }
                else:
                    used_port = int(j_dl.get("port") or cfg["port"])
                    await asyncio.sleep(0.8)
                    j_ul = await tools_iperf_test(
                        IperfTestBody(
                            host=str(cfg["host"]),
                            port=used_port,
                            port_range_max=None,
                            duration_sec=int(cfg["duration_sec"]),
                            direction="upload",
                            protocol=str(cfg.get("protocol") or "tcp").lower(),
                            mobile_only=bool(cfg["mobile_only"]),
                            bind_ip=None,
                            bitrate_limit_mbps=lim_f,
                            parallel_streams=ps_ul,
                            connect_timeout_sec=conn_to,
                        )
                    )
                    ul_err = ""
                    if not j_ul.get("ok"):
                        ul_err = str(j_ul.get("error") or "").strip() or "iperf upload failed"
                    last_tr = {
                        "ok": bool(j_dl.get("ok") and j_ul.get("ok")),
                        "error": ul_err or None,
                        "host": str(j_dl.get("host") or cfg.get("host") or ""),
                        "port": used_port,
                        "port_range_requested": j_dl.get("port_range_requested"),
                        "duration_sec": int(cfg["duration_sec"]),
                        "parallel_streams_dl": ps_dl,
                        "parallel_streams_ul": ps_ul,
                        "protocol": str(cfg.get("protocol") or "tcp").lower(),
                        "mobile_only": bool(cfg["mobile_only"]),
                        "connect_timeout_sec": conn_to,
                        "direction": "download_upload",
                        "throughput_mbps_dl": j_dl.get("throughput_mbps"),
                        "throughput_mbps_ul": j_ul.get("throughput_mbps"),
                        "download": j_dl,
                        "upload": j_ul,
                    }
            elif test_type == "volte_call_outbound":
                hold = max(1, min(int(cfg.get("call_duration_sec") or 10), 120))
                if not bool(cfg.get("auto_hangup", True)):
                    hold = 1
                conn_to = max(20, min(int(cfg.get("answer_wait_sec") or 120), 300))
                last_tr = await tools_volte_test(
                    VolteTestBody(
                        number=str(cfg["phone_number"]),
                        hold_sec=hold,
                        connect_timeout_sec=conn_to,
                        password=body.unlock_password,
                    )
                )
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported test_type: {test_type}")
            iteration_log.append(
                {
                    "iteration": i + 1,
                    "started": t_iter0,
                    "ended": time.time(),
                    "tool_result": last_tr,
                }
            )
            if i < n_it - 1:
                if cancel_ev.is_set():
                    run_state["cancelled"] = True
                    tr_progress_state["delay_deadline_ts"] = None
                    tr_progress_state["iteration_next"] = None
                    return cancel_tool_result
                tr_progress_state["phase"] = "delay"
                tr_progress_state["iteration_running"] = None
                tr_progress_state["iteration_next"] = i + 2
                tr_progress_state["delay_deadline_ts"] = time.time() + delay_s
                interrupted = await _interruptible_sleep_test_delay(delay_s, cancel_ev)
                tr_progress_state["delay_deadline_ts"] = None
                tr_progress_state["iteration_next"] = None
                if interrupted or cancel_ev.is_set():
                    run_state["cancelled"] = True
                    return cancel_tool_result
        tr_progress_state["phase"] = "complete"
        tr_progress_state["iteration_running"] = None
        tr_progress_state["iteration_next"] = None
        tr_progress_state["delay_deadline_ts"] = None
        return last_tr

    def current_snap() -> dict[str, Any]:
        return kpi_runtime.snapshot if isinstance(kpi_runtime.snapshot, dict) else {}

    try:
        async with _test_run_session_lock:
            _test_run_session = {"run_id": run_id.lower(), "cancel": cancel_ev, "state": tr_progress_state}
        kpi_pre, kpi_post, tool_result, samples = await tr.run_with_kpi_sampling(
            kpi_jsonl_path=kpi_path,
            kpi_lock=kpi_runtime.lock,
            get_snapshot=current_snap,
            interval_sec=1.0,
            execute_test=run_one_tool,
        )
    finally:
        async with _test_run_session_lock:
            if _test_run_session is not None and _test_run_session.get("run_id") == run_id.lower():
                _test_run_session = None

    ended = time.time()
    run_ended_utc = tr._utc_iso(ended)

    tc_json = json.dumps(cfg, separators=(",", ":"))
    agg = tr.aggregate_snapshots(samples)
    agg["lock_rat_mode"] = ""
    agg["lock_lte_bands"] = ""
    agg["lock_ca_policy"] = ""
    agg["lock_nr_bands"] = ""
    agg["lock_nrdc"] = ""
    try:
        lock_st = await _read_lock_status_for_run_csv()
        locks = lock_st.get("values") if isinstance(lock_st, dict) else None
        if isinstance(locks, dict):
            agg["lock_rat_mode"] = str(locks.get("mode_pref") or "").strip()
            lte_b = locks.get("lte_band")
            agg["lock_lte_bands"] = str(lte_b or "").strip()
            agg["lock_ca_policy"] = tr.format_ca_policy_from_lte_band(lte_b)
            nr_b = str(locks.get("nr5g_band") or "").strip()
            if not nr_b:
                nr_b = str(locks.get("nsa_nr5g_band") or "").strip()
            agg["lock_nr_bands"] = nr_b
            agg["lock_nrdc"] = tr.format_nrdc_mode_csv(locks.get("nrdc_mode"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("test run: unexpected error reading lock status for CSV: %s", exc)

    agg_str = {k: str(v) for k, v in agg.items()}
    logs: list[dict[str, Any]] = list(iteration_log)
    if not logs:
        logs = [{"iteration": 1, "started": started, "ended": ended, "tool_result": tool_result}]

    tool_results_list = [dict(e["tool_result"]) if isinstance(e.get("tool_result"), dict) else {"ok": False, "error": "no result"} for e in logs]
    if run_state["cancelled"]:
        ok_run = False
        errs = [str(x.get("error") or "").strip() for x in tool_results_list if not bool(x.get("ok"))]
        err_run = "; ".join(e for e in errs if e)
        err_run = (err_run + "; " if err_run else "") + "Run cancelled by user."
    else:
        ok_run = all(bool(x.get("ok")) for x in tool_results_list) if tool_results_list else bool(tool_result.get("ok"))
        errs = [str(x.get("error") or "").strip() for x in tool_results_list if not bool(x.get("ok"))]
        err_run = "; ".join(e for e in errs if e) or (str(tool_result.get("error") or "").strip() if not ok_run else "")

    first_csv = True
    for entry in logs:
        tr_one = entry.get("tool_result")
        if not isinstance(tr_one, dict):
            tr_one = {"ok": False, "error": "invalid tool_result"}
        ping_cols, iperf_cols, volte_cols = tr.tool_csv_columns_for_test_type(test_type, cfg, tr_one)
        t0 = entry.get("started", started)
        t1 = entry.get("ended", ended)
        if not isinstance(t0, (int, float)):
            t0 = started
        if not isinstance(t1, (int, float)):
            t1 = ended
        iter_dur_ms = int(round((float(t1) - float(t0)) * 1000))
        row = tr.build_csv_row(
            project_name=project_name,
            test_location=test_location,
            engineer=engineer,
            modem_antenna_config=tr.profile_modem_antenna_config(prof),
            note=run_note,
            run_started_utc=tr._utc_iso(float(t0)),
            run_ended_utc=tr._utc_iso(float(t1)),
            profile_name=body.profile_name.strip(),
            test_type=test_type,
            run_success=bool(tr_one.get("ok")),
            run_error=str(tr_one.get("error") or "").strip(),
            run_duration_ms=iter_dur_ms,
            test_config_json=tc_json,
            test_iteration_index=int(entry.get("iteration") or 1),
            test_iterations_total=n_it,
            test_iteration_delay_sec=delay_csv,
            ping_cols=ping_cols,
            iperf_cols=iperf_cols,
            volte_cols=volte_cols,
            agg=agg_str,
        )
        if first_csv:
            tr.write_summary_csv(csv_path, row)
            first_csv = False
        else:
            tr.append_summary_csv_row(csv_path, row)

    ui_payload: dict[str, Any] = {
        "run_id": run_id,
        "captured_utc": run_ended_utc,
        "modem_antenna_config": tr.profile_modem_antenna_config(prof),
        "note": run_note,
    }
    server_ui = await _server_ui_state_for_test_export()
    if body.include_ui_snapshot:
        client_ui: dict[str, Any] = {}
        if isinstance(body.ui_controls, dict):
            client_ui = tr.redact_ui_controls(body.ui_controls)
        ui_payload["server"] = server_ui
        ui_payload["client_ui"] = client_ui
    else:
        ui_payload["server"] = server_ui
        ui_payload["client_ui"] = None
    with open(ui_path, "w", encoding="utf-8") as uf:
        json.dump({"ui_controls": ui_payload}, uf, indent=2, default=tr._json_default)

    rel = os.path.basename
    return {
        "ok": True,
        "run_id": run_id,
        "modem_antenna_config": tr.profile_modem_antenna_config(prof),
        "note": run_note,
        "run_success": ok_run,
        "error": err_run if not ok_run else None,
        "tool_result": tool_result,
        "tool_results": tool_results_list,
        "test_iterations": n_it,
        "test_iteration_delay_sec": delay_s,
        "run_cancelled": bool(run_state["cancelled"]),
        "kpi_pre": kpi_pre,
        "kpi_post": kpi_post,
        "kpi_sample_count": len(samples),
        "artifacts_dir": os.path.abspath(out_dir),
        "run_folder": os.path.basename(out_dir),
        "artifacts": {
            "summary_csv": rel(csv_path),
            "kpi_jsonl": rel(kpi_path),
            "ui_json": rel(ui_path),
        },
        "download_paths": {
            "summary": f"/api/test/download/{run_id}/summary",
            "kpi": f"/api/test/download/{run_id}/kpi",
            "ui": f"/api/test/download/{run_id}/ui",
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
