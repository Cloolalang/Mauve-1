from __future__ import annotations

import asyncio
import functools
import ipaddress
import json
import logging
import math
import os
import sys
import re
import shutil
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager

from typing import Any, Literal

from fastapi import Body, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field
from serial.tools import list_ports

from app.kpi_service import KpiRuntime, _parse_cgdcont, _parse_cgauth, _parse_qiact, _parse_qicsgp, kpi_poll_loop
from app import test_runner as tr
from app.serial_engine import SerialEngine
from app.at_modem_errors import combine_errors, describe_modem_send_result
from app.sim_usim_services import (
    SIM_EF_DESCRIPTIONS,
    SIM_INSPECTOR_LABEL_REFERENCE,
    label_usim_service,
)

logger = logging.getLogger(__name__)

APP_VERSION = "2.0"


def _serial_state_file_path() -> str:
    """Persist last-used serial port. Dev: ``backend/.state``; PyInstaller: ``%LOCALAPPDATA%\\5GModemTestDriver``."""
    if getattr(sys, "frozen", False):
        base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
        d = os.path.join(base, "5GModemTestDriver")
    else:
        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".state"))
    return os.path.join(d, "serial_last.json")


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
kpi_runtime = KpiRuntime(poll_hz=2.0)
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


class SendAtBody(BaseModel):
    command: str = Field(min_length=1, description="AT command without CRLF")
    timeout_sec: float = Field(default=2.0, ge=0.2, le=30.0)


class ReopenBody(BaseModel):
    port: str = Field(min_length=1)
    baudrate: int = Field(default=115200, ge=300, le=4000000)


class KpiPollBody(BaseModel):
    """KPI sampling is fixed at 2 Hz; `poll_hz` must be 2.0 (accepted for API compatibility)."""

    poll_hz: Literal[2.0] = Field(default=2.0, description="Fixed at 2.0 Hz")


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
    pdp_auth_type: int = Field(
        default=0,
        ge=0,
        le=3,
        description="3GPP +CGAUTH / Quectel +QICSGP: 0=none, 1=PAP, 2=CHAP, 3=PAP or CHAP",
    )
    pdp_username: str | None = Field(default=None, max_length=64, description="PDP username (optional)")
    pdp_password: str | None = Field(
        default=None,
        max_length=64,
        description="PDP password for PAP/CHAP; omit or null for empty. Not the dashboard unlock password.",
    )
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
    hold_sec: int = Field(default=10, ge=1, le=120, description="Call hold duration before hangup")
    connect_timeout_sec: int = Field(
        default=120,
        ge=20,
        le=300,
        description="Max seconds to wait for CLCC active/held (voice) after dial",
    )
    password: str | None = Field(default=None, description="Unlock password (same as data allow password)")


class VoiceHangupBody(BaseModel):
    password: str | None = Field(default=None, description="Unlock password (same as VoLTE / data allow)")


class VoiceAnswerBody(BaseModel):
    password: str | None = Field(default=None, description="Unlock password (same as VoLTE / data allow)")


class AutoAnswerSetBody(BaseModel):
    enabled: bool = Field(description="False → ATS0=0 (no auto-answer); True → ATS0=rings")
    rings: int = Field(
        default=2,
        ge=1,
        le=255,
        description="Rings before auto-answer (only when enabled=True)",
    )
    password: str | None = Field(default=None, description="Unlock password (same as data allow / VoLTE test)")


class HostAutoAnswerBody(BaseModel):
    """Enable/disable background watcher that sends ``ATA`` after N rings (VoLTE-friendly)."""

    enabled: bool
    rings: int = Field(default=2, ge=1, le=255)
    password: str | None = Field(default=None, description="Required when enabled=True")


class IperfTestBody(BaseModel):
    host: str = Field(default="iperf.as42831.net", min_length=1)
    port: int = Field(default=5361, ge=1, le=65535)
    duration_sec: int = Field(default=1, ge=1, le=300)
    direction: str = Field(default="download", description="download=server->client, upload=client->server")
    protocol: str = Field(default="tcp", description="Traffic mode. Currently only tcp is supported.")
    mobile_only: bool = Field(default=True, description="Bind iperf to mobile data interface/IP only.")
    bind_ip: str | None = Field(default=None, description="Optional local IPv4 to bind using iperf -B.")
    bitrate_limit_mbps: float | None = Field(
        default=None,
        ge=0,
        description="Optional TCP bitrate limit for iperf -b (Mbit/s); 0 or None = unlimited.",
    )
    parallel_streams: int = Field(
        default=10,
        ge=1,
        le=64,
        description="iperf3 parallel streams (-P), 1–64.",
    )
    connect_timeout_sec: float = Field(
        default=10.0,
        ge=1.0,
        le=120.0,
        description=(
            "iperf3 control-connection startup budget in seconds (maps to --connect-timeout in ms when the binary supports it). "
            "Default 10. Bundled iperf 3.1.1 omits the flag but the subprocess wall-clock still allows this headroom."
        ),
    )


class IcmpPingSweepBody(BaseModel):
    host: str = Field(default="8.8.8.8", min_length=1, max_length=253)
    count: int = Field(default=10, ge=1, le=100)
    bind_ipv4: str | None = Field(default=None, description="Windows: ping -S source IPv4 (optional).")
    timeout_ms: int | None = Field(
        default=None,
        ge=500,
        le=60000,
        description="Windows: per-reply timeout for ping -w (ms). Default 3000 when omitted.",
    )


class TestRunBody(BaseModel):
    profile_name: str = Field(min_length=1, max_length=120)
    project_name: str = Field(default="", max_length=200)
    test_location: str = Field(default="", max_length=400)
    engineer: str = Field(default="", max_length=200)
    note: str = Field(default="", max_length=4000, description="Optional free-text note stored on the run (CSV + UI snapshot).")
    ping_bind_ipv4_override: str | None = Field(
        default=None,
        description="ping profiles only: set to force bind (-S on Windows). Empty string = OS default route (no bind). Omit to use profile test_config.bind_ipv4.",
    )
    include_ui_snapshot: bool = True
    ui_controls: dict[str, Any] | None = Field(
        default=None,
        description="Optional client dashboard control values; password-like keys are redacted server-side.",
    )
    unlock_password: str | None = Field(default=None, description="Required for volte_call_outbound profiles.")
    test_iterations: int = Field(default=1, ge=1, le=100, description="Run the profile tool this many times; CSV gets one row per iteration.")
    test_iteration_delay_sec: float = Field(
        default=10.0,
        ge=10.0,
        le=3600.0,
        description="Seconds to wait between iterations (minimum 10; not applied after the last).",
    )


class TestCancelBody(BaseModel):
    run_id: str | None = Field(
        default=None,
        max_length=32,
        description="If set, must match the active test run id or the cancel request is rejected.",
    )


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


def _clcc_rows_voice_only(rows: list[dict]) -> list[dict]:
    """
    Drop ``+CLCC`` rows that are **data** bearers (``mode`` **1** per 3GPP TS 27.007).
    Packet data often shows ``stat`` active (0) with ``mode`` data (1); that must not look like a voice call.
    """
    return [r for r in rows if r.get("mode") != 1]


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
        await _stop_host_auto_answer_task()
        await engine.stop()
    finally:
        _release_instance_lock()


app = FastAPI(
    title="5G ModemTestDriver",
    version="2.0.0",
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
    .label { color: #9aa0a6; font-size: 12px; }
    .value { font-size: 20px; font-weight: 700; margin-top: 4px; }
    .row { display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; margin-top: 8px; }
    .modemfw-value {
      font-size: 10px;
      line-height: 1.3;
      font-family: Consolas, monospace;
      text-align: right;
      flex: 1;
      min-width: 0;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    .mono { font-family: Consolas, monospace; font-size: 12px; white-space: pre-wrap; word-break: break-word; }
    .nbr-channels-pre {
      font-family: Consolas, monospace;
      font-size: 11px;
      line-height: 1.35;
      margin: 6px 0 0;
      padding: 8px;
      max-height: 220px;
      overflow: auto;
      background: #0e0e0e;
      border: 1px solid #333;
      border-radius: 6px;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .ok { color: #39d353; }
    .warn { color: #ffcc66; }
    .err { color: #ff7070; }
    @keyframes voice-ring-pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.35; }
    }
    .voice-ringing {
      animation: voice-ring-pulse 0.85s ease-in-out infinite;
      color: #ffcc66 !important;
      font-weight: 700;
    }
    .volte-phone-panel {
      margin-top: 14px;
      padding: 12px;
      border-radius: 10px;
      border: 1px solid #333;
      background: #141414;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 10px;
    }
    .volte-phone-widget {
      width: 88px;
      height: 88px;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #d64545;
      transition: color 0.25s ease;
    }
    .volte-phone-widget svg {
      display: block;
    }
    .volte-phone-widget .volte-handset {
      transition: transform 0.35s ease;
      transform: translate(0, 5px);
      transform-origin: 36px 46px;
    }
    .volte-phone-widget.volte-phone--active {
      color: #39d353;
    }
    .volte-phone-widget.volte-phone--active .volte-handset {
      transform: translate(-6px, -10px) rotate(-26deg);
    }
    .volte-phone-widget.volte-phone--ringing {
      color: #e6b800;
      animation: volte-phone-flash 0.75s ease-in-out infinite;
    }
    .volte-phone-widget.volte-phone--ringing .volte-handset {
      transform: translate(0, 5px);
    }
    @keyframes volte-phone-flash {
      0%, 100% { opacity: 1; filter: drop-shadow(0 0 4px rgba(230, 184, 0, 0.55)); }
      50% { opacity: 0.38; filter: drop-shadow(0 0 2px rgba(230, 184, 0, 0.25)); }
    }
    .volte-phone-caption {
      font-size: 13px;
      font-weight: 600;
      color: #c8c8c8;
    }
    .page-title-row {
      display: flex;
      flex-wrap: wrap;
      align-items: baseline;
      gap: 8px 18px;
      margin: 0 0 8px 0;
    }
    .page-title-row h1 {
      margin: 0;
      display: flex;
      align-items: baseline;
      gap: 10px;
      flex-wrap: wrap;
    }
    .header-quote {
      margin: 0;
      font-size: 12px;
      font-style: italic;
      color: #9aa0a6;
      line-height: 1.45;
      max-width: 44em;
    }
    .header-quote-attrib {
      font-style: normal;
      color: #8a9099;
    }
  </style>
</head>
<body>
  <div class="page-title-row">
    <h1>5G ModemTestDriver <span class="label" style="font-size:13px; font-weight:600; letter-spacing:0.02em;">v__APP_VERSION__</span></h1>
    <p class="header-quote">"When you can measure what you are speaking about, and express it in numbers, you know something about it" <span class="header-quote-attrib">— Lord Kelvin</span></p>
  </div>
  <div class="label">Live modem snapshot from COM AT engine</div>
  <div id="status" class="label" style="margin-top:8px;">Connecting...</div>
  <div style="margin-top:10px; display:flex; gap:14px; align-items:center; flex-wrap:wrap;">
    <button id="btn-clear-charts">Clear All Charts</button>
    <button id="btn-ui-defaults" type="button" title="Chart 10m, RF smoothing on, MNO Auto, RAT AUTO, LTE/NR bands, CA multi-band on, NRDC on">Apply UI defaults</button>
    <button id="btn-chart-gap-mode">Time-roll gaps: OFF</button>
    <label style="display:flex; align-items:center; gap:6px; font-size:12px; color:#9aa0a6;">
      Chart window
      <select id="chart-window-select" style="background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:3px 6px;">
        <option value="60">60s</option>
        <option value="120">2m</option>
        <option value="300">5m</option>
        <option value="600" selected>10m</option>
        <option value="900">15m</option>
        <option value="1800">30m</option>
        <option value="3600">60m</option>
      </select>
    </label>
    <label style="display:flex; align-items:center; gap:6px; font-size:12px; color:#9aa0a6;">
      <input id="rf-smooth-toggle" type="checkbox" />
      RF smoothing (rolling avg, last 10 samples — primary + intra overlay)
    </label>
    <label style="display:flex; align-items:center; gap:6px; font-size:12px; color:#9aa0a6;" title="Last N primary-cell raw samples inside the chart window used for σ (sample stdev, n−1).">
      σ samples (N)
      <input id="rf-std-sample-count" type="number" min="2" max="600" step="1" value="60"
        style="width:4.2em; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:3px 6px;" />
    </label>
  </div>

  <div class="grid" style="margin-top:12px;">
    <div class="card">
      <div class="label">Serial Port</div>
      <div class="row"><span class="label">Current</span><span id="serial-current">-</span></div>
      <div class="row"><span class="label">Baud</span><span id="serial-baud">-</span></div>
      <div class="row"><span class="label">Open</span><span id="serial-open">-</span></div>
      <div class="row"><span class="label">Queue depth</span><span id="serial-queue">-</span></div>
      <div class="row" title="Command currently owning the AT port (if any)."><span class="label">AT active</span><span id="serial-at-active" class="mono" style="font-size:11px;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:right;">-</span></div>
      <div class="row" title="Estimated command rate from the last 60s window; /s uses recent 10s span."><span class="label">AT rate</span><span id="serial-at-rate">-</span></div>
      <div class="row" title="Mean time from enqueue until OK/ERROR/+CME (includes waiting in queue)."><span class="label">AT avg turn</span><span id="serial-at-avg-ms">-</span></div>
      <div class="row" title="Last completed command and worst in the rolling 60s window."><span class="label">AT last / max</span><span id="serial-at-last-max">-</span></div>
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
      <div class="row"><span class="label">Modem FW</span><span id="modemfw" class="modemfw-value">-</span></div>
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
      <div class="row"><span class="label">PDP type</span><span id="ds-pdp-type-kpi">-</span></div>
      <div class="row"><span class="label">PDP username</span><span id="ds-pdp-user-kpi">-</span></div>
      <div class="row"><span class="label">PDP auth</span><span id="ds-pdp-auth-kpi">-</span></div>
      <div class="row" title="Whether +CGAUTH? / +QICSGP? included a non-empty password field (many firmwares hide password on read)."><span class="label">PDP pwd in AT read</span><span id="ds-pdp-pw-hint">-</span></div>
      <div class="row"><span class="label">PDP Contexts (active/total)</span><span id="ds-pdp">-</span></div>
      <div class="row"><span class="label">CID1</span><span id="ds-cid1">-</span></div>
      <div class="row"><span class="label">CID1 IP</span><span id="ds-ip">-</span></div>
      <div class="row"><span class="label">Packet Attach</span><span id="ds-attach">-</span></div>
      <div class="row"><span class="label">EPS Registration</span><span id="ds-reg">-</span></div>
      <div class="row"><span class="label">USB data stack</span><span id="ds-usbnet">-</span></div>
      <div class="row"><span class="label">Netdev status</span><span id="ds-netdev">-</span></div>
      <div style="margin-top:12px;">
        <div class="label">Set APN / PDP auth (AT+CGDCONT, +CGAUTH, +QICSGP)</div>
        <div style="margin-top:6px; display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
          <input id="ds-apn-set" placeholder="e.g. internet" style="flex:1; min-width:160px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;" />
          <select id="ds-pdp-type" style="background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;">
            <option value="IP" selected>IP</option>
            <option value="IPV4V6">IPV4V6</option>
            <option value="IPV6">IPV6</option>
          </select>
          <select id="ds-pdp-auth-type" title="3GPP auth protocol for this CID" style="background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;">
            <option value="0" selected>Auth: none</option>
            <option value="1">Auth: PAP</option>
            <option value="2">Auth: CHAP</option>
            <option value="3">Auth: PAP or CHAP</option>
          </select>
          <button id="btn-ds-apn-apply" type="button">Apply</button>
        </div>
        <div style="margin-top:8px; display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
          <input id="ds-pdp-net-user" placeholder="PDP username (optional)" style="flex:1; min-width:160px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;" />
          <input id="ds-pdp-net-pass" type="password" placeholder="PDP password (optional)" style="flex:1; min-width:160px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;" />
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
      <div class="row" title="FDD or TDD from AT+QENG serving LTE cell (field after RAT on the LTE row)."><span class="label">Duplex</span><span id="lte-duplex">-</span></div>
      <div class="row"><span class="label">DL/UL BW</span><span id="bwpair">-</span></div>
      <div class="row"><span class="label">EARFCN/PCI</span><span id="earfcnpci">-</span></div>
      <div class="row" title="LTE CA components from AT+QCAINFO: EARFCN/PCI per PCC and SCC, comma-separated."><span class="label">EARFCN active (CA)</span><span id="earfcn-active-ca">-</span></div>
      <div class="row" title="Sum of decoded DL bandwidths (MHz) for each PCC/SCC row in AT+QCAINFO where the bandwidth field maps to RB count or QENG-style 0–5 index. — if a component cannot be decoded it is omitted from the sum."><span class="label">CA aggregated DL BW</span><span id="ca-agg-dl-bw">-</span></div>
      <div class="row"><span class="label">Cell ID</span><span id="cellid">-</span></div>
      <div class="label" style="margin-top:10px;">Primary cell RF KPI</div>
      <div class="row"><span class="label">RSRP</span><span id="rsrp">-</span></div>
      <div class="row"><span class="label">RSRQ</span><span id="rsrq">-</span></div>
      <div class="row"><span class="label">SINR (QSINR PRX)</span><span id="sinr">-</span></div>
      <div class="row"><span class="label">RSSI</span><span id="rssi">-</span></div>
      <div class="label" style="margin-top:8px; font-size:11px; line-height:1.35;">Sample σ (n−1) from the last <strong>N</strong> primary-cell raw samples in the chart time window (toolbar <strong>σ samples (N)</strong>); EARFCN/PCI tag per sample. Not RF-smoothed.</div>
      <div class="row"><span class="label">σ RSRP</span><span id="rsrp-std">-</span></div>
      <div class="row"><span class="label">σ RSRQ</span><span id="rsrq-std">-</span></div>
      <div class="row"><span class="label">σ SNIR (QSINR PRX)</span><span id="sinr-std">-</span></div>
      <div class="row"><span class="label">σ RSSI</span><span id="rssi-std">-</span></div>
      <div class="row"><span class="label">Primary cell intra-cell RSRP dominance</span><span id="dominance">-</span></div>
      <div class="row" title="Same LTE cell only. Builds median RSRQ from samples where |RSRP − rolling median RSRP| ≤ 3 dB. Value = baseline − current RSRQ (dB). Positive ⇒ current RSRQ worse than session baseline — RF/airtime congestion proxy for a static UE; not scheduler load. Resets on cell change.">
        <span class="label">RSRQ vs baseline (static UE proxy)</span><span id="rsrq-static-proxy">-</span>
      </div>
      <div class="label" style="margin-top:10px;">Neighbour Cells RF KPI</div>
      <div class="row"><span class="label">1st strongest neighbour RSRP (intra)</span><span id="nrsrp1">-</span></div>
      <div class="row"><span class="label">1st strongest neighbour PCI (intra)</span><span id="npci1">-</span></div>
      <div class="row"><span class="label">1st strongest neighbour EARFCN (intra)</span><span id="nearfcn1">-</span></div>
      <div class="row"><span class="label">Intra-frequency neighbour count (LTE)</span><span id="nbr-intra-count">-</span></div>
      <div class="row"><span class="label">Inter-frequency neighbour count (LTE)</span><span id="nbr-inter-count">-</span></div>
    </div>

    <div class="card">
      <div class="label">NR5G RF KPI</div>
      <div class="label" style="margin-top:10px;">Primary NR cell</div>
      <div class="row" title="NR layer reported on AT+QENG serving cell: NR5G-SA or NR5G-NSA."><span class="label">NR serving</span><span id="nr-rf-serving-type">-</span></div>
      <div class="row" title="In NR5G-SA this is the band index from the same AT+QENG line (shown as nNN). In NR5G-NSA, AT+QNWINFO is preferred when present, otherwise QENG."><span class="label">NR band</span><span id="nr-rf-band">-</span></div>
      <div class="row" title="Duplex mode from the AT+QENG NR5G-SA serving line (FDD or TDD). Not reported on NR5G-NSA rows."><span class="label">Duplex</span><span id="nr-rf-duplex">-</span></div>
      <div class="row"><span class="label">ARFCN</span><span id="nr-rf-arfcn">-</span></div>
      <div class="row"><span class="label">PCI</span><span id="nr-rf-pci">-</span></div>
      <div class="row"><span class="label">DL bandwidth</span><span id="nr-rf-dl-bw">-</span></div>
      <div class="row"><span class="label">RSRP</span><span id="nr-rf-rsrp">-</span></div>
      <div class="row"><span class="label">RSRQ</span><span id="nr-rf-rsrq">-</span></div>
      <div class="row"><span class="label">SNIR (QSINR PRX)</span><span id="nr-rf-sinr">-</span></div>
      <div class="label" style="margin-top:10px;">1st strongest NR neighbour (intra)</div>
      <div class="row"><span class="label">ARFCN</span><span id="nr-nbr-arfcn">-</span></div>
      <div class="row"><span class="label">PCI</span><span id="nr-nbr-pci">-</span></div>
      <div class="row"><span class="label">DL bandwidth</span><span id="nr-nbr-dl-bw">-</span></div>
      <div class="row"><span class="label">RSRP</span><span id="nr-nbr-rsrp">-</span></div>
      <div class="row"><span class="label">RSRQ</span><span id="nr-nbr-rsrq">-</span></div>
      <div class="row"><span class="label">SNIR</span><span id="nr-nbr-sinr">-</span></div>
    </div>

    <div class="card">
      <div class="label">Mobility · LTE carrier re-selection (camped and RRC connected)</div>
      <div class="row" style="margin-top:8px;"><span class="label">Intra-freq PCI re-selections / min</span><span id="idle-pci-rate">-</span></div>
      <div class="row"><span class="label">Primary EARFCN re-selections / min</span><span id="idle-earfcn-rate">-</span></div>
    </div>

    <div class="card">
      <div class="label">Inter-frequency neighbour EARFCN</div>
      <pre id="nbr-inter-channels" class="nbr-channels-pre">-</pre>
    </div>

    <div class="card">
      <div class="label">Primary and 1st strongest intra-cell neighbour RSRP Trend (dBm)</div>
      <canvas id="rsrpchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">Primary and 1st strongest intra-cell neighbour RSRQ Trend (dB)</div>
      <canvas id="rsrqchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">Primary SNIR Trend (dB)</div>
      <canvas id="sinrchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">Primary and 1st strongest intra-cell neighbour RSSI Trend (dBm)</div>
      <canvas id="rssichart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">Primary and 1st strongest intra-cell neighbour RSRP dominance Trend (dB)</div>
      <canvas id="dominancechart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">RSRQ vs RSRP-stable session baseline — static UE congestion proxy (dB)</div>
      <canvas id="congestionproxychart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">1st strongest inter-cell neighbour RSRP Trend (dBm)</div>
      <canvas id="nbrintersrpchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">1st strongest inter-cell neighbour RSRQ Trend (dB)</div>
      <canvas id="nbrintersrqchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">1st strongest inter-cell neighbour RSSI Trend (dBm)</div>
      <canvas id="nbrinterrssichart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">Primary and 1st strongest inter-cell neighbour RSRP dominance Trend (dB)</div>
      <canvas id="nbridomchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">Neighbour cell count trend — intra &amp; inter (LTE)</div>
      <canvas id="nbrcountcombinedchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">CA EARFCN config &amp; aggregated bandwidth</div>
      <canvas id="ca-combo-chart" width="420" height="180" style="width:100%; height:180px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">State Trend</div>
      <canvas id="statechart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">RAT Trend</div>
      <div class="label" style="font-size:11px; margin-top:4px; line-height:1.35;">
        Same value as the RAT field above: LTE row from <code>AT+QENG</code> when present, else serving mode string (NR5G-NSA / NR5G-SA / …).
      </div>
      <canvas id="ratchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">Primary cell band &amp; DL bandwidth trend</div>
      <canvas id="bandbwcombinedchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">Primary Carrier re-selection rate — LTE PCell /min</div>
      <canvas id="carrier-resel-chart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card">
      <div class="label">NR5G — Primary &amp; 1st strongest intra NR neighbour RSRP (dBm)</div>
      <canvas id="nr-rsrpchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>
    <div class="card">
      <div class="label">NR5G — Primary &amp; 1st strongest intra NR neighbour RSRQ (dB)</div>
      <canvas id="nr-rsrqchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>
    <div class="card">
      <div class="label">NR5G — Primary &amp; 1st strongest intra NR neighbour SNIR (dB)</div>
      <canvas id="nr-sinrchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>
    <div class="card">
      <div class="label">NR5G — Primary &amp; 1st strongest intra NR neighbour RSRP dominance (dB)</div>
      <canvas id="nr-dominancechart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>
    <div class="card">
      <div class="label">NR5G — Primary ARFCN trend</div>
      <canvas id="nr-arfcnchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>
    <div class="card">
      <div class="label">NR5G — Primary PCI trend</div>
      <canvas id="nr-pcichart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>
    <div class="card">
      <div class="label">NR5G — Primary band &amp; DL bandwidth trend</div>
      <canvas id="nr-bandbwcombinedchart" width="420" height="160" style="width:100%; height:160px; background:#101010; border:1px solid #333; border-radius:8px;"></canvas>
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
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
      <div class="label">VoLTE Call Controller</div>

      <div style="margin-top:10px;">
        <div class="label">Unlock password</div>
        <input id="volte-password" type="password" placeholder="Enter password" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>

      <div class="label" style="margin-top:14px;">Outbound test call</div>
      <div style="margin-top:6px;">
        <div class="label">Dial number</div>
        <input id="volte-number" placeholder="+447700900123" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:8px;">
        <div class="label">Call hold time (seconds)</div>
        <input id="volte-hold-sec" type="number" min="3" max="120" value="10"
          style="width:100%; max-width:140px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:8px;">
        <div class="label">Max wait for connect (seconds)</div>
        <input id="volte-connect-timeout" type="number" min="20" max="300" value="120"
          style="width:100%; max-width:140px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="margin-top:10px;">
        <button id="btn-volte-test" type="button">Run VoLTE call test</button>
      </div>

      <div class="label" style="margin-top:14px;">Incoming calls — auto-answer</div>
      <div class="row" style="margin-top:8px;"><span class="label">Auto-answer</span>
        <label style="display:flex; align-items:center; gap:8px; cursor:pointer;">
          <input id="autoanswer-enabled" type="checkbox" />
          <span class="label" style="margin:0;">On</span>
        </label>
      </div>
      <div class="row" style="margin-top:6px;"><span class="label">Rings before answer (1–255)</span>
        <input id="autoanswer-rings" type="number" min="1" max="255" value="2"
          style="width:72px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:4px 6px;" />
      </div>

      <div class="volte-phone-panel">
        <div id="volte-phone-widget" class="volte-phone-widget volte-phone--idle" title="Call status">
          <svg viewBox="0 0 72 72" width="72" height="72" aria-hidden="true" focusable="false">
            <g class="volte-phone-base" opacity="0.4" fill="currentColor">
              <path d="M10 54 Q36 64 62 54 L60 58 Q36 68 12 58 Z"/>
              <ellipse cx="36" cy="54" rx="20" ry="5"/>
            </g>
            <g class="volte-handset" fill="currentColor">
              <rect x="20" y="10" width="32" height="38" rx="11" ry="11"/>
              <rect x="33" y="44" width="6" height="12" rx="2"/>
            </g>
          </svg>
        </div>
        <div id="volte-phone-caption" class="volte-phone-caption">Idle</div>
        <div id="volte-call-timer-row" style="text-align:center; margin-top:6px;" title="Elapsed in this call; holds the last duration after hang-up; resets to 0:00 when the next call starts.">
          <span class="mono" id="volte-call-timer" style="font-size:20px; font-weight:600; letter-spacing:0.05em; color:#555;">0:00</span>
        </div>
        <div style="display:flex; gap:10px; flex-wrap:wrap; justify-content:center; margin-top:4px;">
          <button type="button" id="btn-voice-answer" disabled>Answer (ATA)</button>
          <button type="button" id="btn-voice-hangup" disabled>Hang up (ATH)</button>
        </div>
      </div>

      <div id="volte-msg" class="label" style="margin-top:10px;">-</div>
      <pre id="volte-trace" class="mono" style="max-height:140px; overflow:auto; margin-top:8px;">-</pre>
    </div>

    <div class="card">
      <div class="label">Iperf3 Test</div>
      <div style="margin-top:8px;">
        <div class="label">Endpoint host:</div>
        <input id="iperf-host" value="iperf.as42831.net" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="display:grid; grid-template-columns: 1fr 1fr 1fr; gap:8px; margin-top:8px;">
        <div>
          <div class="label">Port:</div>
          <input id="iperf-port" type="number" min="1" max="65535" value="5361" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
        </div>
        <div>
          <div class="label">Duration (s):</div>
          <input id="iperf-duration" type="number" min="1" max="300" value="1" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
        </div>
        <div>
          <div class="label">Parallel streams (-P):</div>
          <input id="iperf-parallel" type="number" min="1" max="64" value="10" title="iperf3 -P: number of parallel client streams" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
        </div>
      </div>
      <div style="margin-top:8px;">
        <div class="label" title="iperf3 control-connection startup timeout (--connect-timeout in ms). Default 10 s. Applied only if your iperf3 build supports the flag (bundled 3.1.1 skips it; subprocess still allows this headroom).">Connect timeout (s)</div>
        <input id="iperf-connect-timeout" type="number" min="1" max="120" step="1" value="10" style="width:100%; max-width:220px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;" />
      </div>
      <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px; margin-top:8px;">
        <div>
          <div class="label">Direction:</div>
          <select id="iperf-direction" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:4px;">
            <option value="both" selected>Upload then Download</option>
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
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
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
      <div class="label chart-axis-label" style="margin-top:6px;">Time axis: last 10m</div>
    </div>

    <div class="card" style="grid-column: 1 / -1;">
      <div class="label">Test Runner (saved profiles)</div>
      <div class="label" style="margin-top:6px; font-size:11px; line-height:1.4;">
        Runs a saved profile; each run creates a folder under <code>backend/automated_tests/test_results/</code> (name: project + location + UTC time + run id) containing <code>run_*_summary.csv</code>, <code>run_*_kpi.jsonl</code>, and <code>run_*_ui.json</code>. Bundled profiles live in <code>automated_tests/test_cases/</code>.
        To exercise <strong>specific modem radio settings</strong> (for example LTE/NR band lock, RAT mode, CA on/off, single-band or neighbour locks), configure them <strong>manually in this dashboard first</strong>—the Test Runner applies each profile’s optional modem requirements (if any) but does not replace full lock/MNO setup you want for the test.
        For <strong>ping</strong> profiles, use <strong>Ping bind</strong> (same IPv4 list as ICMP sweep) so traffic uses the modem interface; <strong>Auto</strong> skips <code>-S</code>. <strong>Profile bind_ipv4</strong> uses the JSON value only when that option is selected.
        For <strong>iperf</strong> profiles, optional <code>test_config.connect_timeout_sec</code> (1–120; default <strong>10</strong> when omitted) maps to iperf3 <code>--connect-timeout</code> when the binary supports it (bundled 3.1.1 skips the flag; subprocess still allows the timeout headroom).
        VoLTE runs require unlock password below. Password fields in UI snapshot are redacted server-side.
        <strong>Delay between iterations</strong> is at least 10 seconds when a profile runs more than once. Use <strong>Cancel run</strong> (or <code>POST /api/test/cancel</code>) to stop after the current tool step or during the delay; the modem cannot abort a ping/iperf/VoLTE call mid-flight.
      </div>
      <div style="display:flex; flex-wrap:wrap; gap:10px; align-items:flex-end; margin-top:10px;">
        <div style="min-width:220px; flex:1;">
          <div class="label">Profile</div>
          <select id="test-runner-profile" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;"></select>
        </div>
        <div style="min-width:140px;">
          <div class="label">Project</div>
          <input id="test-runner-project" placeholder="Project name" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;" />
        </div>
        <div style="min-width:140px;">
          <div class="label">Location / zone</div>
          <input id="test-runner-location" placeholder="Site, room, …" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;" />
        </div>
        <div style="min-width:120px;">
          <div class="label">Engineer</div>
          <input id="test-runner-engineer" placeholder="Name or ID" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;" />
          <div style="display:flex; gap:10px; flex-wrap:wrap; margin-top:6px;">
            <label style="flex:1; min-width:140px;">Iterations
              <input id="test-runner-iterations" type="number" min="1" max="100" value="1" style="width:100%; margin-top:4px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;" />
            </label>
            <label style="flex:1; min-width:160px;">Delay between (sec)
              <input id="test-runner-iter-delay" type="number" min="10" max="3600" step="1" value="10" style="width:100%; margin-top:4px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;" />
            </label>
          </div>
        </div>
        <div style="min-width:280px; flex:1;">
          <div class="label">Ping bind (Windows <code>-S</code>, Test Runner)</div>
          <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
            <select id="test-runner-bind-select" style="flex:1; min-width:200px; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;"></select>
            <button id="btn-test-runner-refresh-ifaces" type="button">Refresh ifaces</button>
          </div>
          <input id="test-runner-bind-ip" placeholder="Manual IPv4 when Manual is selected" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px; margin-top:6px; display:none;" />
        </div>
        <div style="min-width:180px;">
          <div class="label">Unlock (VoLTE runs)</div>
          <input id="test-runner-unlock" type="password" autocomplete="off" placeholder="Same as Allow Data" style="width:100%; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:6px;" />
        </div>
        <button id="btn-test-runner-refresh" type="button">Refresh profiles</button>
        <button id="btn-test-runner-run" type="button">Run test</button>
        <button id="btn-test-runner-cancel" type="button" disabled>Cancel run</button>
      </div>
      <div style="margin-top:10px; width:100%; max-width:900px;">
        <div class="label">Note</div>
        <textarea id="test-runner-note" rows="2" placeholder="Optional run note (saved with results)" style="width:100%; box-sizing:border-box; background:#111; color:#f3f3f3; border:1px solid #333; border-radius:6px; padding:8px; resize:vertical; font-family:inherit; font-size:13px;"></textarea>
      </div>
      <div id="test-runner-msg" class="label" style="margin-top:8px;">-</div>
      <div id="test-runner-progress" class="label" style="margin-top:6px; font-size:12px; min-height:1.3em; color:#9cf;"> </div>
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
    /** Line/point colours for dark (#101010) canvas — avoid black and very dark RGBs (invisible/low contrast). */
    const CELL_COLOR_PALETTE = [
      "#e6194b",
      "#4363d8",
      "#42d982",
      "#f58231",
      "#bf7bff",
      "#46f0f0",
      "#ffe119",
      "#ff5cd6",
      "#2dd4bf",
      "#daa520",
      "#5c9dff",
      "#b5cf2e",
      "#ff7f7f",
      "#baffd0",
      "#eabfff"
    ];
    /** Fixed chart colours for combined KPI charts — avoid carrier re-selection EARFCN/PCI (#ff8ec8 / #87ceeb) and cell-key palette. */
    const CHART_COLOR_BAND_TREND = "#f39c12";
    const CHART_COLOR_DL_BW_TREND = "#1abc9c";
    const CHART_COLOR_NR_BAND_TREND = "#7dcea0";
    const CHART_COLOR_NR_DL_BW_TREND = "#58d68d";
    const CHART_COLOR_NBR_COUNT_INTRA = "#b8e986";
    const CHART_COLOR_NBR_COUNT_INTER = "#af7ac5";
    const cellColorMap = new Map();
    let nextColorSeed = 0;
    function liftRgbForDarkBg(r, g, b, minMax = 108) {
      let mx = Math.max(r, g, b);
      if (mx >= minMax) return { r, g, b };
      const s = mx < 1 ? 2.85 : Math.min(3.4, (minMax + 24) / mx);
      return {
        r: Math.min(255, Math.round(r * s + 38)),
        g: Math.min(255, Math.round(g * s + 38)),
        b: Math.min(255, Math.round(b * s + 38))
      };
    }
    function hex6(r, g, b) {
      return [r, g, b].map((x) => Math.max(0, Math.min(255, x)).toString(16).padStart(2, "0")).join("");
    }
    /** Normalize any hex (palette or fallback) so traces stay visible on dark chart background */
    function traceColorForCanvas(hex) {
      const raw = String(hex || "").trim();
      const m = /^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(raw);
      if (!m) return "#62c8ff";
      let r = parseInt(m[1], 16);
      let g = parseInt(m[2], 16);
      let b = parseInt(m[3], 16);
      const lifted = liftRgbForDarkBg(r, g, b);
      r = lifted.r;
      g = lifted.g;
      b = lifted.b;
      const mx2 = Math.max(r, g, b);
      if (mx2 < 100) {
        r = Math.min(255, r + (100 - mx2));
        g = Math.min(255, g + (100 - mx2));
        b = Math.min(255, b + (100 - mx2));
      }
      return `#${hex6(r, g, b)}`;
    }
    const colorForCellKey = (cellKey, fallback = "#62c8ff") => {
      const s = String(cellKey || "").trim();
      const fb = traceColorForCanvas(fallback);
      if (!s) return fb;
      if (cellColorMap.has(s)) return cellColorMap.get(s) || fb;
      const idx = (nextColorSeed * 7 + 3) % CELL_COLOR_PALETTE.length;
      nextColorSeed += 1;
      const c = traceColorForCanvas(CELL_COLOR_PALETTE[idx] || fb);
      cellColorMap.set(s, c);
      return c;
    };
    let iperfBusy = false;
    let serialBaud = 115200;
    let serialPorts = [];
    let currentServingEarfcn = null;
    let currentServingPci = null;
    let currentNrArfcn = null;
    let currentNrPci = null;
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
    /** RSRQ session baseline vs current (RSRP-stable gated); UE-side congestion proxy — see stepCongestionProxy. */
    const CONGESTION_RSRP_MEDIAN_WINDOW = 21;
    const CONGESTION_RSRP_STABLE_DB = 3;
    const CONGESTION_BASELINE_MAX = 160;
    const CONGESTION_BASELINE_MIN_SAMPLES = 10;
    const congestionRsrpRing = [];
    const congestionBaselineRsrq = [];
    const congestionProxyHistory = [];
    let congestionProxyCellKey = null;
    let lastCongestionUi = { proxy: null, baselineCount: 0 };
    /** Intra-EARFCN strongest neighbour (QENG neighbourcell intra) per metric for RF trend overlays. */
    const rfNeighborOverlap = { rsrp: [], rsrq: [], rssi: [] };
    const nrRfHistory = { rsrp: [], rsrq: [], sinr: [], dominance: [] };
    const nrRfNeighborOverlap = { rsrp: [], rsrq: [], sinr: [] };
    const nrBwHistory = [];
    const nrArfcnHistory = [];
    const nrPciHistory = [];
    const nbrInterRsrpHistory = [];
    const nbrInterRsrqHistory = [];
    const nbrInterRssiHistory = [];
    const nInterDomHistory = [];
    const nbrIntraCountHistory = [];
    const nbrInterCountHistory = [];
    const bwHistory = [];
    const caAggBwHistory = [];
    const carrierReselPciHistory = [];
    const carrierReselEarfcnHistory = [];
    const categoryHistory = { state: [], rat: [], band: [], caEarfcn: [], nrBand: [] };
    let chartWindowMs = 600 * 1000;
    const RF_SMOOTH_WINDOW = 10;
    const RF_STD_SAMPLE_MIN = 2;
    const RF_STD_SAMPLE_MAX = 600;
    let rfSmoothingEnabled = false;
    let chartGapModeEnabled = false;
    let currentPollHz = 2.0;
    let primaryCellDataAvailable = false;
    let nrCellDataAvailable = false;
    let lastTrendSampleTs = null;
    let rfChartTooltipEl = null;
    const RF_HOVER_CANVAS_IDS = [
      "rsrpchart",
      "rsrqchart",
      "sinrchart",
      "rssichart",
      "dominancechart",
      "congestionproxychart",
      "nbrintersrpchart",
      "nbrintersrqchart",
      "nbrinterrssichart",
      "nbridomchart",
      "nbrcountcombinedchart",
      "bandbwcombinedchart",
      "nr-rsrpchart",
      "nr-rsrqchart",
      "nr-sinrchart",
      "nr-dominancechart",
      "nr-arfcnchart",
      "nr-pcichart",
      "nr-bandbwcombinedchart",
      "ratchart",
      "ca-combo-chart"
    ];
    const RF_CHART_TITLE_BY_ID = {
      rsrpchart: "Primary and 1st strongest intra-cell neighbour RSRP Trend (dBm)",
      rsrqchart: "Primary and 1st strongest intra-cell neighbour RSRQ Trend (dB)",
      sinrchart: "Primary SNIR Trend (dB)",
      rssichart: "Primary and 1st strongest intra-cell neighbour RSSI Trend (dBm)",
      dominancechart: "Primary and 1st strongest intra-cell neighbour RSRP dominance Trend (dB)",
      congestionproxychart: "RSRQ vs RSRP-stable session baseline — static UE congestion proxy (dB)",
      nbrintersrpchart: "1st strongest inter-cell neighbour RSRP Trend (dBm)",
      nbrintersrqchart: "1st strongest inter-cell neighbour RSRQ Trend (dB)",
      nbrinterrssichart: "1st strongest inter-cell neighbour RSSI Trend (dBm)",
      nbridomchart: "Primary and 1st strongest inter-cell neighbour RSRP dominance Trend (dB)",
      nbrcountcombinedchart: "Neighbour cell count trend — intra & inter (LTE)",
      bandbwcombinedchart: "Primary cell band & DL bandwidth trend",
      ratchart: "RAT Trend",
      "ca-combo-chart": "CA EARFCN config & aggregated bandwidth",
      "nr-rsrpchart": "NR5G — Primary & 1st strongest intra NR neighbour RSRP (dBm)",
      "nr-rsrqchart": "NR5G — Primary & 1st strongest intra NR neighbour RSRQ (dB)",
      "nr-sinrchart": "NR5G — Primary & 1st strongest intra NR neighbour SNIR (dB)",
      "nr-dominancechart": "NR5G — Primary & 1st strongest intra NR neighbour RSRP dominance (dB)",
      "nr-arfcnchart": "NR5G — Primary ARFCN trend",
      "nr-pcichart": "NR5G — Primary PCI trend",
      "nr-bandbwcombinedchart": "NR5G — Primary band & DL bandwidth trend"
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
      Object.values(rfNeighborOverlap).forEach((h) => pruneHistoryByAge(h, nowMs));
      pruneHistoryByAge(nbrInterRsrpHistory, nowMs);
      pruneHistoryByAge(nbrInterRsrqHistory, nowMs);
      pruneHistoryByAge(nbrInterRssiHistory, nowMs);
      pruneHistoryByAge(nInterDomHistory, nowMs);
      pruneHistoryByAge(congestionProxyHistory, nowMs);
      pruneHistoryByAge(nbrIntraCountHistory, nowMs);
      pruneHistoryByAge(nbrInterCountHistory, nowMs);
      pruneHistoryByAge(bwHistory, nowMs);
      pruneHistoryByAge(caAggBwHistory, nowMs);
      pruneHistoryByAge(carrierReselPciHistory, nowMs);
      pruneHistoryByAge(carrierReselEarfcnHistory, nowMs);
      Object.values(nrRfHistory).forEach((h) => pruneHistoryByAge(h, nowMs));
      Object.values(nrRfNeighborOverlap).forEach((h) => pruneHistoryByAge(h, nowMs));
      pruneHistoryByAge(nrBwHistory, nowMs);
      pruneHistoryByAge(nrArfcnHistory, nowMs);
      pruneHistoryByAge(nrPciHistory, nowMs);
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
      drawInterNbrRfCharts();
      drawNeighbourCountCharts();
      drawBandBwCombinedChart();
      drawCaCombinedChart();
      drawCarrierReselChart();
      drawCategoryCharts();
      drawNrRfCharts();
      drawNrBandBwCombinedChart();
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
        const src = samples[i];
        const row = { t: src.t, v: rollingSum / count, c: src.c };
        if (Array.isArray(src.carriers)) row.carriers = src.carriers;
        out.push(row);
      }
      return out;
    }

    function nbrRfKey(nb) {
      const row = nb || {};
      const e = row.strongest_earfcn;
      const p = row.strongest_pci;
      if (e === null || e === undefined || p === null || p === undefined) return null;
      const en = Number(e);
      const pn = Number(p);
      if (!Number.isFinite(en) || !Number.isFinite(pn)) return null;
      return `${en}/${pn}`;
    }

    /** True when neighbour intra strongest row is on primary EARFCN and PCI is not a PCell echo. */
    function intraStrongestDistinctFromServing(nb, lte) {
      const row = nb || {};
      const L = lte || {};
      const pe = Number(L.earfcn);
      const ne = Number(row.strongest_earfcn);
      if (!Number.isFinite(pe) || !Number.isFinite(ne) || pe !== ne) return false;
      const spci = L.pcid;
      const npci = row.strongest_pci;
      if (
        spci !== null &&
        spci !== undefined &&
        npci !== null &&
        npci !== undefined &&
        Number.isFinite(Number(spci)) &&
        Number.isFinite(Number(npci)) &&
        Number(spci) === Number(npci)
      ) {
        return false;
      }
      return true;
    }

    function nrServingKey() {
      if (Number.isFinite(currentNrArfcn) && Number.isFinite(currentNrPci)) return `${currentNrArfcn}/${currentNrPci}`;
      return null;
    }

    function nbrNrKey(nrn) {
      const row = nrn || {};
      const a = row.arfcn;
      const p = row.pci;
      if (a === null || a === undefined || p === null || p === undefined) return null;
      const an = Number(a);
      const pn = Number(p);
      if (!Number.isFinite(an) || !Number.isFinite(pn)) return null;
      return `${an}/${pn}`;
    }

    /** Same NR ARFCN as primary; exclude same PCI (modem may echo serving row in neighbour list). */
    function intraNrStrongestDistinctFromServing(nrn, nrp) {
      const N = nrn || {};
      const P = nrp || {};
      const pa = Number(P.arfcn);
      const na = Number(N.arfcn);
      if (!Number.isFinite(pa) || !Number.isFinite(na) || pa !== na) return false;
      const sp = Number(P.pci);
      const np = Number(N.pci);
      if (
        sp !== null &&
        sp !== undefined &&
        np !== null &&
        np !== undefined &&
        Number.isFinite(sp) &&
        Number.isFinite(np) &&
        sp === np
      ) {
        return false;
      }
      return true;
    }

    function pushNrNeighborIntraOverlap(nrn, nrp, trendTsSec) {
      if (!intraNrStrongestDistinctFromServing(nrn, nrp)) return;
      const nk = nbrNrKey(nrn);
      if (!nk) return;
      const t = Number(trendTsSec);
      const tMs = Number.isFinite(t) ? t * 1000 : Date.now();
      const pushOne = (kind, raw) => {
        const vv = Number(raw);
        if (!Number.isFinite(vv) || !nrRfNeighborOverlap[kind]) return;
        nrRfNeighborOverlap[kind].push({ t: tMs, v: vv, c: nk });
        pruneHistoryByAge(nrRfNeighborOverlap[kind], tMs);
      };
      pushOne("rsrp", nrn && nrn.rsrp);
      pushOne("rsrq", nrn && nrn.rsrq);
      pushOne("sinr", nrn && nrn.sinr);
    }

    /** Same EARFCN as LTE primary: push strongest intra-cell neighbour RF samples for overlay charts. */
    function pushRfNeighborIntraOverlap(nb, lte, trendTsSec) {
      if (!intraStrongestDistinctFromServing(nb, lte)) return;
      const nk = nbrRfKey(nb);
      if (!nk) return;
      const t = Number(trendTsSec);
      const tMs = Number.isFinite(t) ? t * 1000 : Date.now();
      const pushOne = (kind, raw) => {
        const vv = Number(raw);
        if (!Number.isFinite(vv) || !rfNeighborOverlap[kind]) return;
        rfNeighborOverlap[kind].push({ t: tMs, v: vv, c: nk });
        pruneHistoryByAge(rfNeighborOverlap[kind], tMs);
      };
      pushOne("rsrp", nb.strongest_rsrp);
      pushOne("rsrq", nb.strongest_rsrq);
      pushOne("rssi", nb.strongest_rssi);
    }

    function nbrInterRssiKey(nb) {
      const row = nb || {};
      const e = row.inter_strongest_earfcn;
      const p = row.inter_strongest_pci;
      if (e === null || e === undefined || p === null || p === undefined) return null;
      const en = Number(e);
      const pn = Number(p);
      if (!Number.isFinite(en) || !Number.isFinite(pn)) return null;
      return `${en}/${pn}`;
    }

    /** strongest inter-freq neighbour (QENG neighbourcell inter): RSRQ/RSRP/RSSI + primary−inter RSRP dominance. */
    function addInterNeighbourTrendSamples(nb, lte, tsSec) {
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const ck = nbrInterRssiKey(nb);
      const pushIf = (arr, raw) => {
        const v = Number(raw);
        if (!Number.isFinite(v)) return;
        arr.push({ t, v, c: ck });
        pruneHistoryByAge(arr, t);
      };
      pushIf(nbrInterRsrpHistory, nb && nb.inter_strongest_rsrp);
      pushIf(nbrInterRsrqHistory, nb && nb.inter_strongest_rsrq);
      pushIf(nbrInterRssiHistory, nb && nb.inter_strongest_rssi);
      const pr = Number(lte && lte.rsrp);
      const ir = Number(nb && nb.inter_strongest_rsrp);
      if (Number.isFinite(pr) && Number.isFinite(ir)) {
        const d = pr - ir;
        nInterDomHistory.push({ t, v: d, c: ck });
        pruneHistoryByAge(nInterDomHistory, t);
      }
      drawInterNbrRfCharts();
    }

    /** Distinct LTE neighbour counts from QENG neighbourcell intra/inter (see API neighbour.intra_neighbour_count). */
    function addNeighbourCountTrendSamples(nb, tsSec) {
      const row = nb || {};
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const ck =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      const pushCount = (arr, raw) => {
        if (raw === null || raw === undefined) return;
        const v = Number(raw);
        if (!Number.isFinite(v) || v < 0) return;
        arr.push({ t, v, c: ck });
        pruneHistoryByAge(arr, t);
      };
      pushCount(nbrIntraCountHistory, row.intra_neighbour_count);
      pushCount(nbrInterCountHistory, row.inter_neighbour_count);
      drawNeighbourCountCharts();
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
      {
        const dpx = lte.duplex;
        let dpxText = "-";
        if (dpx !== null && dpx !== undefined && `${dpx}`.trim()) {
          const s = String(dpx).trim();
          const n = Number(s);
          if (s === "0" || (Number.isFinite(n) && n === 0)) dpxText = "FDD";
          else if (s === "1" || (Number.isFinite(n) && n === 1)) dpxText = "TDD";
          else dpxText = s.toUpperCase();
        }
        el("lte-duplex").textContent = dpxText;
      }
      el("modemfw").textContent = modem.firmware || "-";
      el("ds-apn").textContent = ds.apn || "-";
      const pdpTk = el("ds-pdp-type-kpi");
      if (pdpTk) pdpTk.textContent = ds.pdp_type || "—";
      const pdpUk = el("ds-pdp-user-kpi");
      if (pdpUk) pdpUk.textContent = ds.pdp_username != null && String(ds.pdp_username).length ? String(ds.pdp_username) : "—";
      const pdpAk = el("ds-pdp-auth-kpi");
      if (pdpAk) pdpAk.textContent = ds.pdp_auth_label != null && String(ds.pdp_auth_label).length ? String(ds.pdp_auth_label) : "—";
      const pdpPh = el("ds-pdp-pw-hint");
      if (pdpPh) {
        if (ds.pdp_password_reported === true) pdpPh.textContent = "yes";
        else if (ds.pdp_password_reported === false) pdpPh.textContent = "no";
        else pdpPh.textContent = "—";
      }
      if (ds.active_pdp_contexts === null || ds.active_pdp_contexts === undefined || ds.pdp_contexts === null || ds.pdp_contexts === undefined) {
        el("ds-pdp").textContent = "-";
      } else {
        el("ds-pdp").textContent = `${ds.active_pdp_contexts}/${ds.pdp_contexts}`;
      }

      /* QIACT can stay UP with a stale IP while RRC is searching — hide CID1/IP when not plausibly on-net. */
      const suppressCid1IpKpi =
        !inService ||
        /\bSEARCH\b/i.test(String(srv.state || "")) ||
        ds.eps_registered === false;

      const cid1StateRaw = ds.cid1_active === true ? "UP" : ds.cid1_active === false ? "DOWN" : "-";
      el("ds-cid1").textContent = suppressCid1IpKpi ? "-" : cid1StateRaw;
      el("ds-cid1").className = suppressCid1IpKpi
        ? ""
        : ds.cid1_active === true
          ? "ok"
          : ds.cid1_active === false
            ? "warn"
            : "";
      el("ds-ip").textContent = suppressCid1IpKpi ? "-" : ds.cid1_ip || "-";

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
      const lteRsrpN = Number(lte.rsrp);
      const lteStateNoRf = /\bSEARCH\b/i.test(String(srv.state || ""));
      /* Camped / connected LTE RF: RSRP must be a finite negative dBm. Non‑negative (e.g. 0) appears during invalid parse; SEARCH has no usable serving RF — do not feed charts or derivative KPIs. */
      primaryCellDataAvailable =
        inService &&
        !lteStateNoRf &&
        Number.isFinite(currentServingEarfcn) &&
        Number.isFinite(currentServingPci) &&
        Number.isFinite(lteRsrpN) &&
        lteRsrpN < 0;
      if (earfcn === null || earfcn === undefined || pci === null || pci === undefined) {
        el("earfcnpci").textContent = "-";
      } else {
        el("earfcnpci").textContent = `${earfcn}/${pci}`;
      }
      const qca = sample.qcainfo || {};
      const qcaTxt = qca.earfcn_active_text;
      const qcaEl = el("earfcn-active-ca");
      if (qcaEl) {
        if (qcaTxt !== null && qcaTxt !== undefined && String(qcaTxt).trim()) {
          qcaEl.textContent = String(qcaTxt).trim();
        } else if (!inService || !qca.query_ok) {
          qcaEl.textContent = "-";
        } else {
          qcaEl.textContent = "—";
        }
      }
      const caAggEl = el("ca-agg-dl-bw");
      if (caAggEl) {
        const am = qca.dl_bw_aggregate_mhz;
        if (am !== null && am !== undefined && Number.isFinite(Number(am)) && Number(am) > 0) {
          caAggEl.textContent = `${Number(am)} MHz`;
        } else if (!inService || !qca.query_ok) {
          caAggEl.textContent = "-";
        } else {
          caAggEl.textContent = "—";
        }
      }
      el("cellid").textContent = lte.cell_id_hex || "-";

      el("rsrp").textContent = primaryCellDataAvailable ? fmt(lte.rsrp, " dBm") : "-";
      el("nrsrp1").textContent = primaryCellDataAvailable ? fmt(nb.strongest_rsrp, " dBm") : "-";
      el("npci1").textContent = primaryCellDataAvailable ? fmt(nb.strongest_pci) : "-";
      el("nearfcn1").textContent = primaryCellDataAvailable ? fmt(nb.strongest_earfcn) : "-";
      const ic = nb.intra_neighbour_count;
      const xc = nb.inter_neighbour_count;
      el("nbr-intra-count").textContent =
        primaryCellDataAvailable && ic !== null && ic !== undefined && Number.isFinite(Number(ic))
          ? String(ic)
          : "-";
      el("nbr-inter-count").textContent =
        primaryCellDataAvailable && xc !== null && xc !== undefined && Number.isFinite(Number(xc))
          ? String(xc)
          : "-";

      const nrf = sample.nr_rf || {};
      const nrp = nrf.primary || {};
      const nrn = nrf.neighbour || {};
      const nrDash = "-";
      el("nr-rf-serving-type").textContent =
        nrp.serving_nr_type !== null &&
        nrp.serving_nr_type !== undefined &&
        String(nrp.serving_nr_type).trim().length
          ? String(nrp.serving_nr_type).trim()
          : nrDash;
      el("nr-rf-band").textContent =
        nrp.band !== null && nrp.band !== undefined && `${nrp.band}`.length ? String(nrp.band) : nrDash;
      el("nr-rf-duplex").textContent =
        nrp.duplex !== null && nrp.duplex !== undefined && String(nrp.duplex).trim().length
          ? String(nrp.duplex).trim().toUpperCase()
          : nrDash;
      el("nr-rf-arfcn").textContent =
        nrp.arfcn !== null && nrp.arfcn !== undefined && Number.isFinite(Number(nrp.arfcn)) ? String(nrp.arfcn) : nrDash;
      el("nr-rf-pci").textContent = fmt(nrp.pci);
      el("nr-rf-dl-bw").textContent =
        nrp.dl_bw !== null && nrp.dl_bw !== undefined && Number.isFinite(Number(nrp.dl_bw)) ? `${nrp.dl_bw} MHz` : nrDash;
      el("nr-rf-rsrp").textContent = fmt(nrp.rsrp, " dBm");
      el("nr-rf-rsrq").textContent = fmt(nrp.rsrq, " dB");
      el("nr-rf-sinr").textContent = fmt(nrp.sinr, " dB");
      el("nr-nbr-arfcn").textContent =
        nrn && nrn.arfcn !== null && nrn.arfcn !== undefined && Number.isFinite(Number(nrn.arfcn)) ? String(nrn.arfcn) : nrDash;
      el("nr-nbr-pci").textContent = nrn ? fmt(nrn.pci) : nrDash;
      el("nr-nbr-dl-bw").textContent =
        nrn && nrn.dl_bw !== null && nrn.dl_bw !== undefined && Number.isFinite(Number(nrn.dl_bw))
          ? `${nrn.dl_bw} MHz`
          : nrDash;
      el("nr-nbr-rsrp").textContent = nrn ? fmt(nrn.rsrp, " dBm") : nrDash;
      el("nr-nbr-rsrq").textContent = nrn ? fmt(nrn.rsrq, " dB") : nrDash;
      el("nr-nbr-sinr").textContent = nrn ? fmt(nrn.sinr, " dB") : nrDash;

      const nrRsrpGate = Number(nrp.rsrp);
      currentNrArfcn =
        nrp.arfcn !== null && nrp.arfcn !== undefined && Number.isFinite(Number(nrp.arfcn)) ? Number(nrp.arfcn) : null;
      currentNrPci =
        nrp.pci !== null && nrp.pci !== undefined && Number.isFinite(Number(nrp.pci)) ? Number(nrp.pci) : null;
      nrCellDataAvailable =
        !!nrf.available &&
        currentNrArfcn !== null &&
        currentNrPci !== null &&
        Number.isFinite(nrRsrpGate) &&
        nrRsrpGate < 0;
      const nrDominance =
        intraNrStrongestDistinctFromServing(nrn, nrp) &&
        nrp.rsrp !== null &&
        nrp.rsrp !== undefined &&
        nrn &&
        nrn.rsrp !== null &&
        nrn.rsrp !== undefined &&
        Number.isFinite(Number(nrp.rsrp)) &&
        Number.isFinite(Number(nrn.rsrp))
          ? Number(nrp.rsrp) - Number(nrn.rsrp)
          : null;

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
      el("rsrq").textContent = primaryCellDataAvailable ? fmt(lte.rsrq, " dB") : "-";
      el("sinr").textContent = primaryCellDataAvailable ? fmt(qsinr.prx, " dB") : "-";
      el("rssi").textContent = primaryCellDataAvailable ? fmt(lte.rssi, " dBm") : "-";
      const trendTs = sample.sample_ts || null;
      const advanceRfHistory = trendTs !== null && trendTs !== lastTrendSampleTs;
      const dominance =
        intraStrongestDistinctFromServing(nb, lte) &&
        lte.rsrp !== null &&
        lte.rsrp !== undefined &&
        nb.strongest_rsrp !== null &&
        nb.strongest_rsrp !== undefined &&
        Number.isFinite(Number(lte.rsrp)) &&
        Number.isFinite(Number(nb.strongest_rsrp))
          ? Number(lte.rsrp) - Number(nb.strongest_rsrp)
          : null;
      el("dominance").textContent = primaryCellDataAvailable ? fmt(dominance, " dB") : "-";
      const spEl = el("rsrq-static-proxy");
      if (!primaryCellDataAvailable) {
        resetCongestionProxyState();
        if (spEl) spEl.textContent = "-";
      } else if (advanceRfHistory) {
        lastCongestionUi = stepCongestionProxy(lte, trendTs, primaryCellDataAvailable);
        if (spEl) {
          if (lastCongestionUi.proxy !== null && Number.isFinite(lastCongestionUi.proxy)) {
            spEl.textContent = `${Number(lastCongestionUi.proxy).toFixed(1)} dB`;
          } else if (
            lastCongestionUi.baselineCount > 0 &&
            lastCongestionUi.baselineCount < CONGESTION_BASELINE_MIN_SAMPLES
          ) {
            spEl.textContent = `${lastCongestionUi.baselineCount}/${CONGESTION_BASELINE_MIN_SAMPLES}`;
          } else {
            spEl.textContent = "-";
          }
        }
      } else if (spEl) {
        if (lastCongestionUi.proxy !== null && Number.isFinite(lastCongestionUi.proxy)) {
          spEl.textContent = `${Number(lastCongestionUi.proxy).toFixed(1)} dB`;
        } else if (
          lastCongestionUi.baselineCount > 0 &&
          lastCongestionUi.baselineCount < CONGESTION_BASELINE_MIN_SAMPLES
        ) {
          spEl.textContent = `${lastCongestionUi.baselineCount}/${CONGESTION_BASELINE_MIN_SAMPLES}`;
        } else {
          spEl.textContent = "-";
        }
      }
      el("updated").textContent = fmtTs(sample.sample_ts);

      if (trendTs !== lastTrendSampleTs) {
        lastTrendSampleTs = trendTs;
        addCategorySample("state", srv.state || "-", trendTs);
        addCategorySample("rat", ratVal, trendTs);
        addCarrierReselSamples(idleMob, trendTs);
        if (primaryCellDataAvailable) {
          addRfSample("rsrp", lte.rsrp, trendTs, true);
          addRfSample("rsrq", lte.rsrq, trendTs, true);
          addRfSample("sinr", qsinr.prx, trendTs, true);
          addRfSample("rssi", lte.rssi, trendTs, true);
          addRfSample("dominance", dominance, trendTs, true);
          pushRfNeighborIntraOverlap(nb, lte, trendTs);
          drawRfCharts();
          addBwSample(lte.dl_bw, trendTs);
          addCategorySample("band", net.band || "-", trendTs);
          {
            const qcaTr = sample.qcainfo || {};
            const ears = Array.isArray(qcaTr.earfcn_active)
              ? qcaTr.earfcn_active.filter((x) => Number.isFinite(Number(x)))
              : [];
            const caTrVal = ears.length ? ears.map((x) => String(x)).join("+") : "-";
            addCategorySample("caEarfcn", caTrVal, trendTs, { carriers: qcaTr.carriers });
            const caAggMhz = Number(qcaTr.dl_bw_aggregate_mhz);
            if (Number.isFinite(caAggMhz) && caAggMhz > 0) addCaAggBwSample(caAggMhz, trendTs, qcaTr.carriers);
          }
          addInterNeighbourTrendSamples(nb, lte, trendTs);
          addNeighbourCountTrendSamples(nb, trendTs);
        } else {
          drawRfCharts();
          drawInterNbrRfCharts();
          drawNeighbourCountCharts();
          drawBandBwCombinedChart();
          drawCaCombinedChart();
          drawCategoryCharts();
        }
        if (nrCellDataAvailable) {
          addNrRfSample("rsrp", nrp.rsrp, trendTs, true);
          addNrRfSample("rsrq", nrp.rsrq, trendTs, true);
          addNrRfSample("sinr", nrp.sinr, trendTs, true);
          addNrRfSample("dominance", nrDominance, trendTs, true);
          pushNrNeighborIntraOverlap(nrn, nrp, trendTs);
          addNrBwSample(nrp.dl_bw, trendTs);
          const nbStr =
            nrp.band !== null && nrp.band !== undefined && String(nrp.band).trim() ? String(nrp.band).trim() : "-";
          addCategorySample("nrBand", nbStr, trendTs);
          addNrNumericSample(nrArfcnHistory, nrp.arfcn, trendTs);
          addNrNumericSample(nrPciHistory, nrp.pci, trendTs);
        }
        drawNrRfCharts();
        drawNrBandBwCombinedChart();
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
      const lteNorm = lteVal.replace(/\s/g, "");
      const bandTokens = lteVal
        .split(/[:,]/)
        .map((s) => s.trim())
        .filter((s) => /^\d+$/.test(s));
      const caPolicy =
        !lteVal
          ? "-"
          : lteNorm === "0"
            ? "ON (multi/all)"
            : bandTokens.length > 1
              ? "ON (multi/all)"
              : "OFF (single band)";
      el("lock-ca").textContent = caPolicy;
      el("lock-nrband").textContent = v.nr5g_band || v.nsa_nr5g_band || "-";
      el("lock-nrdc").textContent = String(v.nrdc_mode || "0") === "1" ? "ON" : "OFF";
      el("input-nrdc-enable").checked = String(v.nrdc_mode || "0") === "1";
      const caChk = el("input-ca-enable");
      if (caChk) {
        caChk.checked = caPolicy === "ON (multi/all)";
      }
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
        const pdpAuthRaw = Number(el("ds-pdp-auth-type")?.value ?? 0);
        const pdpAuthType = Number.isFinite(pdpAuthRaw) ? Math.max(0, Math.min(3, Math.trunc(pdpAuthRaw))) : 0;
        const netUserRaw = String(el("ds-pdp-net-user")?.value || "").trim();
        const netPassRaw = String(el("ds-pdp-net-pass")?.value || "");
        const r = await fetch("/api/network/apn", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            apn,
            cid: 1,
            pdp_type,
            password,
            reactivate,
            pdp_auth_type: pdpAuthType,
            pdp_username: netUserRaw ? netUserRaw : null,
            pdp_password: netPassRaw.length ? netPassRaw : null
          })
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
        if (!j.ok) throw new Error(userFacingBackendError(j, "APN/auth profile was rejected."));
        msgEl.textContent = j.message || "APN updated.";
        msgEl.className = "label ok";
        el("ds-apn-password").value = "";
        el("ds-pdp-net-pass").value = "";
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
        const baudEl = el("serial-baud");
        if (baudEl) baudEl.textContent = j.baudrate != null && j.baudrate !== undefined ? String(j.baudrate) : "-";
        const openText = j.serial_open ? "Yes" : "No";
        el("serial-open").textContent = openText;
        el("serial-open").className = j.serial_open ? "ok" : "warn";

        const fmtMsDisp = (v) =>
          v === null || v === undefined || !Number.isFinite(Number(v)) ? "—" : `${Math.round(Number(v))} ms`;
        const qn = j.queue_depth;
        const qEl = el("serial-queue");
        if (qEl) qEl.textContent = qn !== null && qn !== undefined ? String(qn) : "—";
        const act = j.active_command;
        const actEl = el("serial-at-active");
        if (actEl) {
          const s = act != null && String(act).length ? String(act) : "—";
          actEl.textContent = s.length > 36 ? `${s.slice(0, 34)}…` : s;
          actEl.title = act ? String(act) : "";
        }
        const rpm = Number(j.at_cmd_per_min_est);
        const rps = Number(j.at_cmd_per_sec_10s);
        const rateEl = el("serial-at-rate");
        if (rateEl) {
          const n60 = Number(j.at_cmd_count_60s);
          if (!Number.isFinite(n60) || n60 <= 0) rateEl.textContent = "—";
          else
            rateEl.textContent = `${Number.isFinite(rpm) ? rpm.toFixed(1) : "—"}/min · ${
              Number.isFinite(rps) ? rps.toFixed(2) : "—"
            }/s`;
        }
        const avgEl = el("serial-at-avg-ms");
        if (avgEl) avgEl.textContent = fmtMsDisp(j.at_cmd_latency_avg_ms);
        const lmEl = el("serial-at-last-max");
        if (lmEl) {
          const last = fmtMsDisp(j.at_cmd_latency_last_ms);
          const max = fmtMsDisp(j.at_cmd_latency_max_ms);
          lmEl.textContent = last === "—" && max === "—" ? "—" : `${last} / ${max}`;
        }
        if (showMessage) {
          el("serialmsg").textContent = j.last_open_error
            ? `Serial warning: ${j.last_open_error}`
            : `Serial OK on ${j.port || "-"}`;
        }
        return j;
      } catch (e) {
        el("serial-open").textContent = "No";
        el("serial-open").className = "err";
        const baudEl = el("serial-baud");
        if (baudEl) baudEl.textContent = "-";
        const qEl = el("serial-queue");
        if (qEl) qEl.textContent = "—";
        const actEl = el("serial-at-active");
        if (actEl) {
          actEl.textContent = "—";
          actEl.title = "";
        }
        const rateEl = el("serial-at-rate");
        if (rateEl) rateEl.textContent = "—";
        const avgEl = el("serial-at-avg-ms");
        if (avgEl) avgEl.textContent = "—";
        const lmEl = el("serial-at-last-max");
        if (lmEl) lmEl.textContent = "—";
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
      /* CA ON: multi-band / all bands. CA OFF: single band only — must NOT send Set LTE bands list or modem stays in CA. */
      if (caOn) {
        if (lteBandManual) body.lte_band = lteBandManual;
        else if (caOnBands) body.lte_band = caOnBands;
        else body.lte_band = "0";
      } else {
        if (caSingle) {
          body.lte_band = caSingle;
        } else if (lteBandManual) {
          const parts = lteBandManual.split(/[:,]/).map((s) => s.trim()).filter(Boolean);
          if (parts.length === 1) body.lte_band = parts[0];
          else {
            el("lockmsg").textContent =
              "CA OFF: enter one LTE band in 'CA OFF single LTE band', or clear 'Set LTE bands' to a single value.";
            return;
          }
        }
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

    const UI_DEFAULT_LTE_BANDS = "1:3:7:8:20:28:38:32";
    const UI_DEFAULT_NR_BANDS = "1:3:7:8:28:78";

    /** Local chart prefs + Modem QNWPREFCFG (+ MNO profile) presets for UK-style lab use. */
    async function applyUiDefaults() {
      const btn = el("btn-ui-defaults");
      if (btn) btn.disabled = true;
      try {
        const cw = el("chart-window-select");
        if (cw) {
          cw.value = "600";
          applyChartWindowSec(600);
        }
        const rst = el("rf-smooth-toggle");
        if (rst) {
          rst.checked = true;
          rfSmoothingEnabled = true;
          redrawAllCharts();
        }

        el("input-ratmode").value = "AUTO";
        el("input-lteband").value = UI_DEFAULT_LTE_BANDS;
        el("input-ca-enable").checked = true;
        el("input-ca-on-bands").value = "";
        el("input-ca-single-band").value = "";
        el("input-nrband").value = UI_DEFAULT_NR_BANDS;
        el("input-nrdc-enable").checked = true;

        el("mno-select").value = "auto";

        await setLocks();
        await applyMnoSelection();

        const rstd = el("rf-std-sample-count");
        if (rstd) rstd.value = "60";
        updatePrimaryRfStdDevKpis();

        el("status").textContent =
          "UI defaults: 10m chart window, smoothing on, locks (AUTO LTE " +
          UI_DEFAULT_LTE_BANDS +
          ", NR " +
          UI_DEFAULT_NR_BANDS +
          ", NRDC ON), MNO Auto — see RAT/Band lock and roaming messages.";
        el("status").className = "label ok";
      } finally {
        if (btn) btn.disabled = false;
      }
    }

    async function runVolteTest() {
      const number = String(el("volte-number")?.value || "").trim();
      const password = String(el("volte-password")?.value || "");
      let holdSec = Math.round(Number(el("volte-hold-sec")?.value ?? 10));
      if (!Number.isFinite(holdSec)) holdSec = 10;
      holdSec = Math.max(3, Math.min(120, holdSec));
      let connectTimeoutSec = Math.round(Number(el("volte-connect-timeout")?.value ?? 120));
      if (!Number.isFinite(connectTimeoutSec)) connectTimeoutSec = 120;
      connectTimeoutSec = Math.max(20, Math.min(300, connectTimeoutSec));
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
          body: JSON.stringify({ number, hold_sec: holdSec, connect_timeout_sec: connectTimeoutSec, password })
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

    let autoAnswerApplyBusy = false;

    async function readAutoAnswerStatus(silent = false) {
      try {
        const r = await fetch("/api/tools/host-auto-answer");
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "Read failed"));
        const cb = el("autoanswer-enabled");
        if (cb) cb.checked = !!j.enabled;
        const rin = el("autoanswer-rings");
        if (rin && j.rings != null && j.rings !== undefined) {
          rin.value = String(j.rings);
        }
        if (!silent) el("volte-msg").textContent = "-";
      } catch (e) {
        if (!silent) el("volte-msg").textContent = `Auto-answer read error: ${e?.message || e}`;
      }
    }

    async function applyAutoAnswer() {
      if (autoAnswerApplyBusy) return;
      const password = String(el("volte-password")?.value || "");
      const enabled = !!el("autoanswer-enabled")?.checked;
      let rings = Math.max(1, Math.min(255, Math.floor(Number(el("autoanswer-rings")?.value || 2))));
      if (!Number.isFinite(rings)) rings = 2;
      if (enabled && !password) {
        el("volte-msg").textContent = "-";
        return;
      }
      autoAnswerApplyBusy = true;
      try {
        const r = await fetch("/api/tools/host-auto-answer", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled, rings, password }),
        });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, "Apply failed"));
        await readAutoAnswerStatus(true);
        el("volte-msg").textContent = "-";
      } catch (e) {
        el("volte-msg").textContent = `Auto-answer: ${e?.message || e}`;
        await readAutoAnswerStatus(true);
      } finally {
        autoAnswerApplyBusy = false;
      }
    }

    let volteCallTimerPrevInCall = false;
    let volteCallTimerStartMs = null;
    let volteCallTimerFrozenMs = null;
    let volteCallTimerInterval = null;

    function formatVolteCallElapsed(ms) {
      const totalSec = Math.floor(ms / 1000);
      const h = Math.floor(totalSec / 3600);
      const m = Math.floor((totalSec % 3600) / 60);
      const s = totalSec % 60;
      if (h > 0) return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
      return `${m}:${String(s).padStart(2, "0")}`;
    }

    function syncVolteCallTimerDisplay() {
      const elT = el("volte-call-timer");
      if (!elT) return;
      if (volteCallTimerStartMs != null) {
        elT.textContent = formatVolteCallElapsed(Date.now() - volteCallTimerStartMs);
        elT.style.color = "#8fd491";
        return;
      }
      if (volteCallTimerFrozenMs != null) {
        elT.textContent = formatVolteCallElapsed(volteCallTimerFrozenMs);
        elT.style.color = "#555";
        return;
      }
      elT.textContent = "0:00";
      elT.style.color = "#555";
    }

    function ensureVolteCallTimerTick() {
      if (volteCallTimerInterval != null) return;
      volteCallTimerInterval = setInterval(syncVolteCallTimerDisplay, 250);
    }

    function stopVolteCallTimerTick() {
      if (volteCallTimerInterval != null) {
        clearInterval(volteCallTimerInterval);
        volteCallTimerInterval = null;
      }
    }

    /**
     * True when a voice session is in progress (connected or progressing).
     * Some MT / VoLTE modems leave AT+CLCC in alerting/dialing without stat active (0/1), so hook stays on-hook;
     * the stopwatch and handset must follow line_state as well, but not pure incoming_ring (unanswered ring).
     */
    function volteStatusInCall(j) {
      if (!j) return false;
      if (j.hook === "off_hook") return true;
      const line = String(j.line_state || "");
      if (line === "idle" || line === "incoming_ring" || line === "other") return false;
      return (
        line === "active" ||
        line === "held" ||
        line === "dialing" ||
        line === "alerting" ||
        line === "waiting"
      );
    }

    function updateVoltePhoneWidget(j) {
      const w = el("volte-phone-widget");
      const cap = el("volte-phone-caption");
      if (!w || !j) return;
      const inCall = volteStatusInCall(j);
      /* RING URC stays recent (~12s) after answer; CLCC active must win over that for the handset icon. */
      const incoming = !!j.incoming_ringing && !inCall;
      w.classList.remove("volte-phone--idle", "volte-phone--ringing", "volte-phone--active");
      if (inCall) {
        w.classList.add("volte-phone--active");
        if (cap) cap.textContent = "In call";
      } else if (incoming) {
        w.classList.add("volte-phone--ringing");
        const num = j.primary_number ? String(j.primary_number).trim() : "";
        if (cap) cap.textContent = num ? `Incoming — ${num}` : "Incoming";
      } else {
        w.classList.add("volte-phone--idle");
        if (cap) cap.textContent = "Idle";
      }
    }

    async function pollVoiceCallStatus() {
      try {
        const r = await fetch("/api/tools/voice-call-status");
        const j = await r.json();
        if (!r.ok || !j.ok) return;
        updateVoltePhoneWidget(j);
        const inCall = volteStatusInCall(j);
        if (inCall && !volteCallTimerPrevInCall) {
          volteCallTimerFrozenMs = null;
          volteCallTimerStartMs = Date.now();
          ensureVolteCallTimerTick();
          syncVolteCallTimerDisplay();
        } else if (!inCall && volteCallTimerPrevInCall) {
          stopVolteCallTimerTick();
          if (volteCallTimerStartMs != null)
            volteCallTimerFrozenMs = Date.now() - volteCallTimerStartMs;
          volteCallTimerStartMs = null;
          syncVolteCallTimerDisplay();
        }
        volteCallTimerPrevInCall = inCall;
        const btnHu = el("btn-voice-hangup");
        const btnAn = el("btn-voice-answer");
        if (btnAn && !voiceCallActionBusy) btnAn.disabled = !j.can_answer;
        if (btnHu && !voiceCallActionBusy) btnHu.disabled = !j.can_hangup;
      } catch (_) {}
    }

    let voiceCallActionBusy = false;

    async function voiceAnswerCall() {
      if (voiceCallActionBusy) return;
      const password = String(el("volte-password")?.value || "");
      if (!password) {
        el("volte-msg").textContent = "Enter unlock password to answer.";
        return;
      }
      voiceCallActionBusy = true;
      const btnAn = el("btn-voice-answer");
      const btnHu = el("btn-voice-hangup");
      if (btnAn) btnAn.disabled = true;
      if (btnHu) btnHu.disabled = true;
      try {
        const r = await fetch("/api/tools/voice-answer", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ password }),
        });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, j.error || "Answer failed"));
        el("volte-msg").textContent = "Answer (ATA) sent.";
      } catch (e) {
        el("volte-msg").textContent = `Answer: ${e?.message || e}`;
      } finally {
        voiceCallActionBusy = false;
        await pollVoiceCallStatus();
        for (let i = 0; i < 4; i++) {
          await new Promise((r) => setTimeout(r, 200));
          await pollVoiceCallStatus();
        }
      }
    }

    async function voiceHangupCall() {
      if (voiceCallActionBusy) return;
      const password = String(el("volte-password")?.value || "");
      if (!password) {
        el("volte-msg").textContent = "Enter unlock password to hang up.";
        return;
      }
      voiceCallActionBusy = true;
      const btn = el("btn-voice-hangup");
      const btnAn = el("btn-voice-answer");
      if (btn) btn.disabled = true;
      if (btnAn) btnAn.disabled = true;
      try {
        const r = await fetch("/api/tools/voice-hangup", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ password }),
        });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(userFacingBackendError(j, j.error || "Hang up failed"));
        el("volte-msg").textContent = "Hang up (ATH) completed.";
      } catch (e) {
        el("volte-msg").textContent = `Hang up: ${e?.message || e}`;
      } finally {
        voiceCallActionBusy = false;
        await pollVoiceCallStatus();
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

    function syncTestRunnerBindUi() {
      const sel = el("test-runner-bind-select");
      const inp = el("test-runner-bind-ip");
      if (!sel || !inp) return;
      const v = sel.value;
      if (v === "manual") {
        inp.style.display = "block";
      } else {
        inp.style.display = "none";
        if (v === "auto" || v === "__profile__") inp.value = "";
      }
    }

    function resolveTestRunnerBindIp() {
      const sel = el("test-runner-bind-select");
      const inp = el("test-runner-bind-ip");
      if (!sel) return "";
      const v = sel.value;
      if (v === "__profile__") return "";
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
        const selTr = el("test-runner-bind-select");
        if (selTr) {
          const prevTr = selTr.value || "auto";
          populateBindSelect(selTr, prevTr, "Auto (OS default route)");
          const optProf = document.createElement("option");
          optProf.value = "__profile__";
          optProf.textContent = "Profile bind_ipv4 (JSON)";
          const manIdx = Array.from(selTr.options).findIndex((o) => o.value === "manual");
          if (manIdx >= 0) selTr.insertBefore(optProf, selTr.options[manIdx]);
          else selTr.appendChild(optProf);
          syncTestRunnerBindUi();
        }
        syncIperfBindUi();
        syncPhBindUi();
      } catch (_) {}
    }

    async function runIperfTest() {
      if (iperfBusy) return;
      iperfBusy = true;
      const host = String(el("iperf-host")?.value || "").trim();
      const port = Number(el("iperf-port")?.value || 5361);
      const durationSec = Number(el("iperf-duration")?.value || 1);
      const parallelStreams = Number(el("iperf-parallel")?.value || 10);
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
      if (!Number.isFinite(parallelStreams) || parallelStreams < 1 || parallelStreams > 64 || parallelStreams !== Math.trunc(parallelStreams)) {
        el("iperf-msg").textContent = "Parallel streams must be an integer 1..64.";
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
      const connectToRaw = String(el("iperf-connect-timeout")?.value || "").trim();
      const cCt = Number(connectToRaw || 10);
      if (!Number.isFinite(cCt) || cCt < 1 || cCt > 120) {
        el("iperf-msg").textContent = "Connect timeout must be 1..120 seconds.";
        iperfBusy = false;
        return;
      }
      const connectTimeoutSec = Math.trunc(cCt);
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
            mobile_only: true,
            parallel_streams: Math.trunc(parallelStreams)
          };
          if (bindIp) body.bind_ip = bindIp;
          if (speedLimit !== null) body.bitrate_limit_mbps = speedLimit;
          body.connect_timeout_sec = connectTimeoutSec;
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
        const dirs = direction === "both" ? ["upload", "download"] : [direction];
        for (let i = 0; i < dirs.length; i++) {
          const dir = dirs[i];
          if (direction === "both" && i > 0) {
            await new Promise((r) => setTimeout(r, 800));
          }
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
        const modeTxt = direction === "both" ? "upload then download" : direction;
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
            `Parallel streams: ${j.parallel_streams != null ? j.parallel_streams : parallelStreams}`,
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

    function medianNumeric(vals) {
      const xs = vals.filter((x) => Number.isFinite(x)).slice().sort((a, b) => a - b);
      const n = xs.length;
      if (!n) return null;
      const m = Math.floor(n / 2);
      if (n % 2) return xs[m];
      return (xs[m - 1] + xs[m]) / 2;
    }

    function resetCongestionProxyState() {
      congestionRsrpRing.length = 0;
      congestionBaselineRsrq.length = 0;
      congestionProxyHistory.length = 0;
      congestionProxyCellKey = null;
      lastCongestionUi = { proxy: null, baselineCount: 0 };
    }

    /**
     * Session RSRQ baseline from RSRP-stable samples on the same LTE cell; proxy = baseline − RSRQ (+ ⇒ worse than baseline).
     */
    function stepCongestionProxy(lte, tsSec, primaryOk) {
      if (!primaryOk || !lte) {
        return { proxy: null, gated: false, baselineCount: 0 };
      }
      const rsrp = Number(lte.rsrp);
      const rsrq = Number(lte.rsrq);
      if (!Number.isFinite(rsrp) || !Number.isFinite(rsrq)) {
        return { proxy: null, gated: false, baselineCount: congestionBaselineRsrq.length };
      }
      const cellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      if (!cellKey) return { proxy: null, gated: false, baselineCount: 0 };

      if (congestionProxyCellKey !== cellKey) {
        congestionRsrpRing.length = 0;
        congestionBaselineRsrq.length = 0;
        congestionProxyCellKey = cellKey;
      }

      congestionRsrpRing.push(rsrp);
      while (congestionRsrpRing.length > CONGESTION_RSRP_MEDIAN_WINDOW) congestionRsrpRing.shift();

      const medRsrp = medianNumeric(congestionRsrpRing);
      const gated =
        congestionRsrpRing.length >= 7 &&
        medRsrp !== null &&
        Math.abs(rsrp - medRsrp) <= CONGESTION_RSRP_STABLE_DB;

      if (gated) {
        congestionBaselineRsrq.push(rsrq);
        while (congestionBaselineRsrq.length > CONGESTION_BASELINE_MAX) congestionBaselineRsrq.shift();
      }

      const baseline = medianNumeric(congestionBaselineRsrq);
      const okN = congestionBaselineRsrq.length >= CONGESTION_BASELINE_MIN_SAMPLES;
      let proxy = null;
      if (gated && baseline !== null && okN) {
        proxy = baseline - rsrq;
        const t = tsSec ? Number(tsSec) * 1000 : Date.now();
        congestionProxyHistory.push({ t, v: proxy, c: cellKey });
        pruneHistoryByAge(congestionProxyHistory, t);
      }
      return { proxy, gated, baselineCount: congestionBaselineRsrq.length };
    }

    function addRfSample(kind, value, tsSec = null, deferDraw = false) {
      if (value === null || value === undefined) return;
      const v = Number(value);
      if (!Number.isFinite(v) || !rfHistory[kind]) return;
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      rfHistory[kind].push({ t, v, c: cellKey });
      pruneHistoryByAge(rfHistory[kind], t);
      if (!deferDraw) drawRfCharts();
    }

    function addNrRfSample(kind, value, tsSec = null, deferDraw = false) {
      if (value === null || value === undefined) return;
      const v = Number(value);
      if (!Number.isFinite(v) || !nrRfHistory[kind]) return;
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey = nrServingKey();
      nrRfHistory[kind].push({ t, v, c: cellKey });
      pruneHistoryByAge(nrRfHistory[kind], t);
      if (!deferDraw) drawNrRfCharts();
    }

    function addNrBwSample(value, tsSec = null) {
      const v = Number(value);
      if (!Number.isFinite(v)) return;
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey = nrServingKey();
      nrBwHistory.push({ t, v, c: cellKey });
      pruneHistoryByAge(nrBwHistory, t);
      drawNrBandBwCombinedChart();
    }

    function addNrNumericSample(historyArr, rawVal, tsSec) {
      if (!Array.isArray(historyArr)) return;
      const v = Number(rawVal);
      if (!Number.isFinite(v)) return;
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey = nrServingKey();
      historyArr.push({ t, v, c: cellKey });
      pruneHistoryByAge(historyArr, t);
    }

    function sampleStdDev(values) {
      if (!Array.isArray(values) || values.length < 2) return null;
      let sum = 0;
      for (const x of values) sum += x;
      const mean = sum / values.length;
      let acc = 0;
      for (const x of values) {
        const d = x - mean;
        acc += d * d;
      }
      return Math.sqrt(acc / (values.length - 1));
    }

    function getRfStdSampleCount() {
      const raw = Number(el("rf-std-sample-count")?.value);
      if (!Number.isFinite(raw)) return 60;
      return Math.max(RF_STD_SAMPLE_MIN, Math.min(RF_STD_SAMPLE_MAX, Math.round(raw)));
    }

    function primaryRfValuesForCellInWindow(history, cellKey, nowMs = Date.now()) {
      if (!cellKey || !Array.isArray(history) || !history.length) return [];
      const cutoff = nowMs - chartWindowMs;
      const out = [];
      for (const p of history) {
        if (p.c !== cellKey) continue;
        if (Number(p.t) < cutoff) continue;
        const v = Number(p.v);
        if (!Number.isFinite(v)) continue;
        out.push(v);
      }
      const cap = getRfStdSampleCount();
      if (out.length > cap) return out.slice(-cap);
      return out;
    }

    function updatePrimaryRfStdDevKpis() {
      const nowMs = Date.now();
      const cellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      const apply = (elemId, kind) => {
        const node = el(elemId);
        if (!node) return;
        if (!cellKey) {
          node.textContent = "-";
          return;
        }
        const vals = primaryRfValuesForCellInWindow(rfHistory[kind], cellKey, nowMs);
        const s = sampleStdDev(vals);
        node.textContent = s !== null && Number.isFinite(s) ? `${s.toFixed(2)} dB` : "-";
      };
      apply("rsrp-std", "rsrp");
      apply("rsrq-std", "rsrq");
      apply("sinr-std", "sinr");
      apply("rssi-std", "rssi");
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
      drawBandBwCombinedChart();
    }

    function addCaAggBwSample(value, tsSec = null, carriers = null) {
      const v = Number(value);
      if (!Number.isFinite(v) || v <= 0) return;
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      const row = { t, v, c: cellKey };
      if (Array.isArray(carriers)) row.carriers = carriers;
      caAggBwHistory.push(row);
      pruneHistoryByAge(caAggBwHistory, t);
      drawCaCombinedChart();
    }

    function addCategorySample(kind, value, tsSec = null, meta = null) {
      if (!categoryHistory[kind]) return;
      const v = String(value || "-").trim() || "-";
      const t = tsSec ? Number(tsSec) * 1000 : Date.now();
      const cellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      const row = { t, v, c: cellKey };
      if (kind === "caEarfcn" && meta && Array.isArray(meta.carriers)) row.carriers = meta.carriers;
      categoryHistory[kind].push(row);
      pruneHistoryByAge(categoryHistory[kind], t);
      drawCategoryCharts();
      if (kind === "band") drawBandBwCombinedChart();
      if (kind === "nrBand") drawNrBandBwCombinedChart();
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
      const yMin = 0;
      const yMax = Math.max(maxV + pad, 0.25);
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
      const yMin = 0;
      const yMax = Math.max(maxV + pad, 1);
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
      const yMin = 0;
      const yMax = Math.max(maxV + pad, 1);
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

    function drawMetricChart(canvasId, samples, unitLabel, color, thresholdValue = null, yFloorAtZero = false) {
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
      if (yFloorAtZero) {
        yMin = 0;
        yMax = Math.max(yMax, 1);
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
      const expectedStepMs = Math.max(50, 1000 / Math.max(1, Number(currentPollHz) || 2));
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

    /** Primary + strongest intra-cell (same-EARFCN) neighbour overlay; shared scale; time-aligned x when overlay exists. */
    function drawMetricChartWithIntraNeighbour(
      canvasId,
      primarySamples,
      overlapSamples,
      unitLabel,
      primaryColor,
      thresholdValue = null
    ) {
      const overlap = Array.isArray(overlapSamples) ? overlapSamples : [];
      if (!overlap.length) {
        drawMetricChart(canvasId, primarySamples, unitLabel, primaryColor, thresholdValue);
        return;
      }

      const canvas = el(canvasId);
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      const prim = primarySamples || [];
      if (!prim.length && !overlap.length) {
        canvas._metricHover = null;
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No samples yet", 12, 24);
        return;
      }

      const valueList = [...prim, ...overlap]
        .map((p) => Number(p?.v))
        .filter((x) => Number.isFinite(x));
      if (!valueList.length) {
        canvas._metricHover = null;
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No samples yet", 12, 24);
        return;
      }

      const minV = Math.min(...valueList);
      const maxV = Math.max(...valueList);
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

      const n = prim.length || 1;
      const x0 = 44;
      const x1 = w - 12;
      const y0 = h - 12;
      const y1 = 10;
      const xStep = n > 1 ? (x1 - x0) / (n - 1) : 0;
      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const expectedStepMs = Math.max(50, 1000 / Math.max(1, Number(currentPollHz) || 2));
      const gapBreakMsLocal = expectedStepMs * 1.8;
      const intraOvTolMs = Math.max(50, expectedStepMs / 2);
      const primArr = prim;

      /** Match drawMetricChart: time-X only when Time-roll gaps is ON — never infer from intra overlay alone. */
      const xPrim = (p, i) => {
        if (!chartGapModeEnabled) return x0 + i * xStep;
        const t = Number(p?.t);
        if (!Number.isFinite(t)) return x0 + i * xStep;
        const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / chartWindowMs));
        return x0 + ratio * (x1 - x0);
      };

      function slotNearestPrim(tt) {
        const t = Number(tt);
        if (!Number.isFinite(t) || !primArr.length) return -1;
        let bestIx = -1;
        let bestAbs = Infinity;
        for (let q = 0; q < primArr.length; q++) {
          const d = Math.abs(Number(primArr[q].t) - t);
          if (d < bestAbs) {
            bestAbs = d;
            bestIx = q;
          }
        }
        return bestAbs <= intraOvTolMs ? bestIx : -1;
      }

      /** When gaps OFF, neighbour traces share primary column (same poll time). When gaps ON, use time-roll like primary. */
      const xNeighbourPt = (p) => {
        if (chartGapModeEnabled) return xPrim(p, 0);
        const sx = slotNearestPrim(p?.t);
        if (sx < 0) return null;
        return x0 + sx * xStep;
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

      const neighbourFb = "#61dafb";
      const primColorFn = (p) => colorForCellKey(p?.c, primaryColor);
      const nbrColorFn = (p) => colorForCellKey(p?.c, neighbourFb);

      ctx.lineWidth = 2;
      for (let i = 1; i < primArr.length; i++) {
        const p0 = primArr[i - 1];
        const p1 = primArr[i];
        const t0 = Number(p0?.t);
        const t1 = Number(p1?.t);
        if (chartGapModeEnabled && Number.isFinite(t0) && Number.isFinite(t1) && t1 - t0 > gapBreakMsLocal)
          continue;
        const xA = xPrim(p0, i - 1);
        const yA = y0 - ((Number(p0.v) - yMin) / span) * (y0 - y1);
        const xB = xPrim(p1, i);
        const yB = y0 - ((Number(p1.v) - yMin) / span) * (y0 - y1);
        ctx.strokeStyle = primColorFn(p1);
        ctx.beginPath();
        ctx.moveTo(xA, yA);
        ctx.lineTo(xB, yB);
        ctx.stroke();
      }

      const overlapSorted = [...overlap].sort((a, b) => Number(a.t) - Number(b.t));
      ctx.setLineDash([5, 4]);
      if (chartGapModeEnabled) {
        for (let i = 1; i < overlapSorted.length; i++) {
          const p0 = overlapSorted[i - 1];
          const p1 = overlapSorted[i];
          const t0 = Number(p0?.t);
          const t1 = Number(p1?.t);
          if (Number.isFinite(t0) && Number.isFinite(t1) && t1 - t0 > gapBreakMsLocal) continue;
          const xA = xNeighbourPt(p0);
          const xB = xNeighbourPt(p1);
          if (xA === null || xB === null || !Number.isFinite(xA) || !Number.isFinite(xB)) continue;
          const yA = y0 - ((Number(p0.v) - yMin) / span) * (y0 - y1);
          const yB = y0 - ((Number(p1.v) - yMin) / span) * (y0 - y1);
          ctx.strokeStyle = nbrColorFn(p1);
          ctx.beginPath();
          ctx.moveTo(xA, yA);
          ctx.lineTo(xB, yB);
          ctx.stroke();
        }
      } else {
        const nPts = overlapSorted
          .map((p) => {
            const xv = xNeighbourPt(p);
            if (xv === null || !Number.isFinite(xv)) return null;
            return { p, xv, yv: y0 - ((Number(p.v) - yMin) / span) * (y0 - y1) };
          })
          .filter(Boolean)
          .sort((a, b) => a.xv - b.xv || Number(a.p.t) - Number(b.p.t));
        for (let i = 1; i < nPts.length; i++) {
          const a = nPts[i - 1];
          const b = nPts[i];
          if (Math.abs(a.xv - b.xv) < 1e-9) continue;
          ctx.strokeStyle = nbrColorFn(b.p);
          ctx.beginPath();
          ctx.moveTo(a.xv, a.yv);
          ctx.lineTo(b.xv, b.yv);
          ctx.stroke();
        }
      }
      ctx.setLineDash([]);

      primArr.forEach((p, i) => {
        const x = xPrim(p, i);
        const y = y0 - ((Number(p.v) - yMin) / span) * (y0 - y1);
        ctx.fillStyle = primColorFn(p);
        ctx.beginPath();
        ctx.arc(x, y, 2.1, 0, Math.PI * 2);
        ctx.fill();
      });
      overlapSorted.forEach((p) => {
        const x = xNeighbourPt(p);
        if (x === null || !Number.isFinite(x)) return;
        const y = y0 - ((Number(p.v) - yMin) / span) * (y0 - y1);
        ctx.fillStyle = nbrColorFn(p);
        ctx.beginPath();
        ctx.arc(x, y, 2.0, 0, Math.PI * 2);
        ctx.fill();
      });

      ctx.fillStyle = "#888";
      ctx.font = "10px Arial";
      ctx.fillText("··· intra-neighbour", w - 102, h - 4);

      const primSamples = primArr;
      const nbrSamples = overlapSorted;
      canvas._metricHover = {
        dualCmp: true,
        canvasId,
        rows: primSamples,
        samples: primSamples,
        primSamples,
        nbrSamples,
        intraOvTolMs,
        unitLabel,
        x0,
        x1,
        y0,
        y1,
        yMin,
        yMax,
        span,
        gapBreakMs: gapBreakMsLocal,
        chartNowMs: nowMs,
        cwMs: chartWindowMs,
        gapMode: chartGapModeEnabled
      };
    }

    function metricHoverXFor(p, i, h) {
      const { x0, x1, cwMs, chartNowMs, gapMode } = h;
      let n = 1;
      if (Number(h.xStepCount) > 0) n = Number(h.xStepCount);
      else if (Array.isArray(h.samples) && h.samples.length) n = h.samples.length;
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

    function showRfCategoryHoverTooltip(clientX, clientY, title, lines, cellKey) {
      const tip = ensureRfChartTooltipEl();
      const ck =
        cellKey !== null && cellKey !== undefined && String(cellKey).trim() !== ""
          ? String(cellKey).trim()
          : "—";
      const parts = Array.isArray(lines) ? lines.map((x) => String(x ?? "").trim()).filter(Boolean) : [String(lines || "").trim()].filter(Boolean);
      const body = parts.join("\\n");
      tip.textContent = body ? `${title}\\n${body}\\nEARFCN/PCI: ${ck}` : `${title}\\nEARFCN/PCI: ${ck}`;
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
      const rect = canvas.getBoundingClientRect();
      const scaleX = canvas.width / rect.width;
      const scaleY = canvas.height / rect.height;
      const mx = (ev.clientX - rect.left) * scaleX;
      const my = (ev.clientY - rect.top) * scaleY;

      if (
        hover &&
        hover.dualCmp &&
        Array.isArray(hover.primSamples) &&
        Array.isArray(hover.nbrSamples) &&
        ((hover.primSamples && hover.primSamples.length) || (hover.nbrSamples && hover.nbrSamples.length))
      ) {
        const { primSamples, nbrSamples, y0, y1, yMin, span } = hover;
        let best = null;
        let bestD = Infinity;
        const hitR = 22;
        const hitR2 = hitR * hitR;
        const tol = Number(hover.intraOvTolMs);
        const useNbrIxMatch =
          !hover.gapMode && Number.isFinite(tol) && Array.isArray(hover.primSamples) && hover.primSamples.length;
        const ixMatchXForNbr = (p) => {
          const tt = Number(p?.t);
          if (!Number.isFinite(tt)) return null;
          const ps = hover.primSamples;
          let bi = -1;
          let bd = Infinity;
          for (let q = 0; q < ps.length; q++) {
            const d = Math.abs(Number(ps[q].t) - tt);
            if (d < bd) {
              bd = d;
              bi = q;
            }
          }
          if (bi < 0 || bd > tol) return null;
          const pn = ps.length;
          const xSpr = pn > 1 ? (hover.x1 - hover.x0) / (pn - 1) : 0;
          return hover.x0 + bi * xSpr;
        };
        const trySeries = (arr, seriesTag) => {
          for (let i = 0; i < arr.length; i++) {
            const p = arr[i];
            let x =
              seriesTag === "nbr" && useNbrIxMatch ? ixMatchXForNbr(p) : metricHoverXFor(p, i, hover);
            if (seriesTag === "nbr" && useNbrIxMatch && (x === null || !Number.isFinite(x))) continue;
            const vy = Number(p?.v);
            if (!Number.isFinite(vy)) continue;
            const y = y0 - ((vy - yMin) / span) * (y0 - y1);
            const dx = mx - x;
            const dy = my - y;
            const d2 = dx * dx + dy * dy;
            if (d2 < bestD) {
              bestD = d2;
              best = { p, v: vy, seriesTag };
            }
          }
        };
        trySeries(primSamples, "prim");
        trySeries(nbrSamples, "nbr");
        if (!best || bestD > hitR2) {
          hideRfChartTooltip();
          return;
        }
        const chartTitle = RF_CHART_TITLE_BY_ID[canvas.id] || canvas.id;
        const sub = best.seriesTag === "prim" ? "Primary" : "Strongest intra";
        const shown =
          Number.isFinite(best.v) ? (Math.abs(best.v % 1) < 0.05 ? best.v.toFixed(1) : best.v.toFixed(2)) : "-";
        showRfChartTooltip(
          ev.clientX,
          ev.clientY,
          `${chartTitle} — ${sub}`,
          shown,
          hover.unitLabel || "",
          best.p?.c
        );
        return;
      }

      if (
        hover &&
        hover.countDual &&
        Array.isArray(hover.intraSamples) &&
        Array.isArray(hover.interSamples) &&
        (hover.intraSamples.length || hover.interSamples.length)
      ) {
        const { intraSamples, interSamples, y0, y1, yMin, span } = hover;
        let best = null;
        let bestD = Infinity;
        const hitR = 22;
        const hitR2 = hitR * hitR;
        const tryCnt = (arr, tag) => {
          for (let i = 0; i < arr.length; i++) {
            const p = arr[i];
            const x = metricHoverXFor(p, i, hover);
            const vy = Number(p?.v);
            if (!Number.isFinite(vy)) continue;
            const y = y0 - ((vy - yMin) / span) * (y0 - y1);
            const dx = mx - x;
            const dy = my - y;
            const d2 = dx * dx + dy * dy;
            if (d2 < bestD) {
              bestD = d2;
              best = { p, v: vy, tag };
            }
          }
        };
        tryCnt(intraSamples, "intra");
        tryCnt(interSamples, "inter");
        if (!best || bestD > hitR2) {
          hideRfChartTooltip();
          return;
        }
        const chartTitle = RF_CHART_TITLE_BY_ID[canvas.id] || canvas.id;
        const sub = best.tag === "intra" ? "Intra-frequency" : "Inter-frequency";
        const shown =
          Number.isFinite(best.v) ? (Math.abs(best.v % 1) < 0.05 ? best.v.toFixed(1) : best.v.toFixed(2)) : "-";
        showRfChartTooltip(
          ev.clientX,
          ev.clientY,
          `${chartTitle} — ${sub}`,
          shown,
          hover.unitLabel || "",
          best.p?.c
        );
        return;
      }

      if (hover && hover.bandBw && typeof hover.xFor === "function" && Array.isArray(hover.bandBwRows)) {
        const rows = hover.bandBwRows;
        let best = null;
        let bestD = Infinity;
        const hitR = 22;
        const hitR2 = hitR * hitR;
        for (let i = 0; i < rows.length; i++) {
          const r = rows[i];
          const x = hover.xFor(r, i);
          const ix = hover.labels ? hover.labels.indexOf(r.bandEff) : -1;
          const idx = ix < 0 ? 0 : ix;
          const yBandPt = hover.yForBand(idx);
          const dxb = mx - x;
          const dyb = my - yBandPt;
          const d2b = dxb * dxb + dyb * dyb;
          if (d2b < bestD) {
            bestD = d2b;
            best = { row: r, curve: "band" };
          }
          if (r.bw !== null && Number.isFinite(Number(r.bw))) {
            const yw = hover.yForBw(Number(r.bw));
            const dxw = mx - x;
            const dyw = my - yw;
            const d2w = dxw * dxw + dyw * dyw;
            if (d2w < bestD) {
              bestD = d2w;
              best = { row: r, curve: "bw" };
            }
          }
        }
        if (!best || bestD > hitR2) {
          hideRfChartTooltip();
          return;
        }
        const chartTitle = RF_CHART_TITLE_BY_ID[canvas.id] || canvas.id;
        const bwTxt =
          best.row.bw !== null && Number.isFinite(Number(best.row.bw)) ? `${Number(best.row.bw).toFixed(1)} MHz` : "—";
        const tipBody = `Band: ${best.row.bandEff || "—"}\\nDL BW: ${bwTxt}`;
        showRfChartTooltip(ev.clientX, ev.clientY, `${chartTitle} — sample`, tipBody, "", best.row.c);
        return;
      }

      if (hover && hover.caCombo) {
        const {
          earfcnSamples,
          catLabels,
          catLevels,
          topY0,
          topY1,
          mhzSamples,
          mhzYMin,
          mhzSpan,
          botY0,
          botY1,
          x0,
          x1,
          gapBreakMs,
          chartNowMs,
          cwMs,
          gapMode
        } = hover;
        const chartTitle = RF_CHART_TITLE_BY_ID[canvas.id] || canvas.id;
        const inTop = my >= topY1 - 2 && my <= topY0 + 2;
        const inBot = my >= botY1 - 2 && my <= botY0 + 2;
        if (inTop && Array.isArray(earfcnSamples) && earfcnSamples.length && catLabels.length) {
          const samples = earfcnSamples;
          const labels = catLabels;
          const levels = catLevels;
          const y0 = topY0;
          const y1 = topY1;
          const vlab = "EARFCN (CA)";
          const lv = Math.max(1, levels);
          const n = samples.length;
          const xStep = n > 1 ? (x1 - x0) / (n - 1) : 0;
          const windowStartMs = chartNowMs - cwMs;
          const xFor = (p, i) => {
            if (!gapMode) return x0 + i * xStep;
            const t = Number(p?.t);
            if (!Number.isFinite(t)) return x0 + i * xStep;
            const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / cwMs));
            return x0 + ratio * (x1 - x0);
          };
          const yForVal = (vStr) => {
            const idx = labels.indexOf(vStr);
            const ix = idx < 0 ? 0 : idx;
            return y0 - (ix / lv) * (y0 - y1);
          };
          const distPointToSeg = (px, py, xa, ya, xb, yb) => {
            const dx = xb - xa;
            const dy = yb - ya;
            const len2 = dx * dx + dy * dy;
            if (len2 < 1e-12) return Math.hypot(px - xa, py - ya);
            let t = ((px - xa) * dx + (py - ya) * dy) / len2;
            t = Math.max(0, Math.min(1, t));
            const nx = xa + t * dx;
            const ny = ya + t * dy;
            return Math.hypot(px - nx, py - ny);
          };
          const fmtTime = (tsMs) => {
            const t = Number(tsMs);
            if (!Number.isFinite(t)) return "—";
            return new Date(t).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
          };
          let bestPtD2 = Infinity;
          let bestPt = null;
          for (let i = 0; i < samples.length; i++) {
            const p = samples[i];
            if (!caSampleHasScc(p)) continue;
            const x = xFor(p, i);
            const y = yForVal(p.v);
            const d2 = (mx - x) * (mx - x) + (my - y) * (my - y);
            if (d2 < bestPtD2) {
              bestPtD2 = d2;
              bestPt = { p, x, y };
            }
          }
          const hitPtR = 22;
          let bestSegD = Infinity;
          let bestSeg = null;
          for (let i = 1; i < samples.length; i++) {
            const p0 = samples[i - 1];
            const p1 = samples[i];
            if (!caSampleHasScc(p0) || !caSampleHasScc(p1)) continue;
            if (labels.indexOf(p0.v) < 0 || labels.indexOf(p1.v) < 0) continue;
            const t0 = Number(p0?.t);
            const t1 = Number(p1?.t);
            if (gapMode && Number.isFinite(t0) && Number.isFinite(t1) && t1 - t0 > gapBreakMs) continue;
            const xA = xFor(p0, i - 1);
            const yA = yForVal(p0.v);
            const xB = xFor(p1, i);
            const yB = yForVal(p1.v);
            const d = distPointToSeg(mx, my, xA, yA, xB, yB);
            if (d < bestSegD) {
              bestSegD = d;
              bestSeg = { p0, p1 };
            }
          }
          const hitLineR = 12;
          if (bestPt && bestPtD2 <= hitPtR * hitPtR) {
            const p = bestPt.p;
            const rat = String(p.v ?? "—");
            showRfCategoryHoverTooltip(ev.clientX, ev.clientY, `${chartTitle} — EARFCN active`, [`${vlab}: ${rat}`, `Time: ${fmtTime(p.t)}`], p.c);
            return;
          }
          if (bestSeg && bestSegD <= hitLineR) {
            const { p0, p1 } = bestSeg;
            const a = String(p0.v ?? "—");
            const b = String(p1.v ?? "—");
            const ratLine = a === b ? `${vlab}: ${a}` : `${vlab}: ${a} → ${b}`;
            showRfCategoryHoverTooltip(
              ev.clientX,
              ev.clientY,
              `${chartTitle} — EARFCN active`,
              [ratLine, `Time: ${fmtTime(p0.t)} – ${fmtTime(p1.t)}`],
              p1.c || p0.c
            );
            return;
          }
        }
        if (inBot && Array.isArray(mhzSamples) && mhzSamples.length) {
          const samples = mhzSamples;
          const y0 = botY0;
          const y1 = botY1;
          const yMin = mhzYMin;
          const span = mhzSpan;
          const subHover = { samples, x0, x1, cwMs, chartNowMs, gapMode };
          let best = null;
          let bestD = Infinity;
          const hitR = 22;
          const hitR2 = hitR * hitR;
          for (let i = 0; i < samples.length; i++) {
            const p = samples[i];
            const x = metricHoverXFor(p, i, subHover);
            const y = y0 - ((p.v - yMin) / span) * (y0 - y1);
            const dx = mx - x;
            const dy = my - y;
            const d2 = dx * dx + dy * dy;
            if (d2 < bestD) {
              bestD = d2;
              best = { p, v: Number(p?.v) };
            }
          }
          if (best && bestD <= hitR2) {
            const shown =
              Number.isFinite(best.v) ? (Math.abs(best.v % 1) < 0.05 ? best.v.toFixed(1) : best.v.toFixed(2)) : "-";
            showRfChartTooltip(ev.clientX, ev.clientY, `${chartTitle} — Aggregated DL BW`, shown, "MHz", best.p?.c);
            return;
          }
        }
        hideRfChartTooltip();
        return;
      }

      if (hover && hover.categoryStep && Array.isArray(hover.samples) && hover.samples.length) {
        const { samples, labels, levels, x0, x1, y0, y1, gapBreakMs, chartNowMs, cwMs, gapMode } = hover;
        const vlab = hover.categoryValueLabel && String(hover.categoryValueLabel).trim() ? String(hover.categoryValueLabel).trim() : "RAT";
        const lv = Math.max(1, levels);
        const n = samples.length;
        const xStep = n > 1 ? (x1 - x0) / (n - 1) : 0;
        const windowStartMs = chartNowMs - cwMs;
        const xFor = (p, i) => {
          if (!gapMode) return x0 + i * xStep;
          const t = Number(p?.t);
          if (!Number.isFinite(t)) return x0 + i * xStep;
          const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / cwMs));
          return x0 + ratio * (x1 - x0);
        };
        const yForVal = (vStr) => {
          const idx = labels.indexOf(vStr);
          const ix = idx < 0 ? 0 : idx;
          return y0 - (ix / lv) * (y0 - y1);
        };
        const distPointToSeg = (px, py, xa, ya, xb, yb) => {
          const dx = xb - xa;
          const dy = yb - ya;
          const len2 = dx * dx + dy * dy;
          if (len2 < 1e-12) return Math.hypot(px - xa, py - ya);
          let t = ((px - xa) * dx + (py - ya) * dy) / len2;
          t = Math.max(0, Math.min(1, t));
          const nx = xa + t * dx;
          const ny = ya + t * dy;
          return Math.hypot(px - nx, py - ny);
        };
        const fmtTime = (tsMs) => {
          const t = Number(tsMs);
          if (!Number.isFinite(t)) return "—";
          return new Date(t).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
        };
        let bestPtD2 = Infinity;
        let bestPt = null;
        for (let i = 0; i < samples.length; i++) {
          const p = samples[i];
          const x = xFor(p, i);
          const y = yForVal(p.v);
          const d2 = (mx - x) * (mx - x) + (my - y) * (my - y);
          if (d2 < bestPtD2) {
            bestPtD2 = d2;
            bestPt = { p, x, y };
          }
        }
        const hitPtR = 22;
        let bestSegD = Infinity;
        let bestSeg = null;
        for (let i = 1; i < samples.length; i++) {
          const p0 = samples[i - 1];
          const p1 = samples[i];
          const t0 = Number(p0?.t);
          const t1 = Number(p1?.t);
          if (gapMode && Number.isFinite(t0) && Number.isFinite(t1) && t1 - t0 > gapBreakMs) continue;
          const xA = xFor(p0, i - 1);
          const yA = yForVal(p0.v);
          const xB = xFor(p1, i);
          const yB = yForVal(p1.v);
          const d = distPointToSeg(mx, my, xA, yA, xB, yB);
          if (d < bestSegD) {
            bestSegD = d;
            bestSeg = { p0, p1 };
          }
        }
        const hitLineR = 12;
        const chartTitle = RF_CHART_TITLE_BY_ID[canvas.id] || canvas.id;
        if (bestPt && bestPtD2 <= hitPtR * hitPtR) {
          const p = bestPt.p;
          const rat = String(p.v ?? "—");
          showRfCategoryHoverTooltip(ev.clientX, ev.clientY, chartTitle, [`${vlab}: ${rat}`, `Time: ${fmtTime(p.t)}`], p.c);
          return;
        }
        if (bestSeg && bestSegD <= hitLineR) {
          const { p0, p1 } = bestSeg;
          const a = String(p0.v ?? "—");
          const b = String(p1.v ?? "—");
          const ratLine = a === b ? `${vlab}: ${a}` : `${vlab}: ${a} → ${b}`;
          showRfCategoryHoverTooltip(
            ev.clientX,
            ev.clientY,
            chartTitle,
            [ratLine, `Time: ${fmtTime(p0.t)} – ${fmtTime(p1.t)}`],
            p1.c || p0.c
          );
          return;
        }
        hideRfChartTooltip();
        return;
      }

      if (!hover || !Array.isArray(hover.samples) || hover.samples.length === 0) {
        hideRfChartTooltip();
        return;
      }
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
      const ovRsrp = rfSmoothingEnabled ? smoothSeries(rfNeighborOverlap.rsrp, RF_SMOOTH_WINDOW) : rfNeighborOverlap.rsrp;
      const ovRsrq = rfSmoothingEnabled ? smoothSeries(rfNeighborOverlap.rsrq, RF_SMOOTH_WINDOW) : rfNeighborOverlap.rsrq;
      const ovRssi = rfSmoothingEnabled ? smoothSeries(rfNeighborOverlap.rssi, RF_SMOOTH_WINDOW) : rfNeighborOverlap.rssi;
      drawMetricChartWithIntraNeighbour("rsrpchart", rsrp, ovRsrp, "dBm", pciColor, -126);
      drawMetricChartWithIntraNeighbour("rsrqchart", rsrq, ovRsrq, "dB", pciColor, -15);
      drawMetricChart("sinrchart", sinr, "dB", pciColor, 0);
      drawMetricChartWithIntraNeighbour("rssichart", rssi, ovRssi, "dBm", pciColor, -95);
      drawMetricChart("dominancechart", dominance, "dB", "#50fa7b", 6);
      drawMetricChart("congestionproxychart", primaryCellDataAvailable ? congestionProxyHistory : [], "dB", "#ffb86c", 0);
      updatePrimaryRfStdDevKpis();
    }

    function drawNrRfCharts() {
      const rsrp = rfSmoothingEnabled ? smoothSeries(nrRfHistory.rsrp, RF_SMOOTH_WINDOW) : nrRfHistory.rsrp;
      const rsrq = rfSmoothingEnabled ? smoothSeries(nrRfHistory.rsrq, RF_SMOOTH_WINDOW) : nrRfHistory.rsrq;
      const sinr = rfSmoothingEnabled ? smoothSeries(nrRfHistory.sinr, RF_SMOOTH_WINDOW) : nrRfHistory.sinr;
      const dominanceSource = rfSmoothingEnabled ? smoothSeries(nrRfHistory.dominance, RF_SMOOTH_WINDOW) : nrRfHistory.dominance;
      const dominance = nrCellDataAvailable ? dominanceSource : [];
      const primKey = nrServingKey();
      const pciColor = colorForCellKey(primKey, "#39ff14");
      const ovRsrp = rfSmoothingEnabled ? smoothSeries(nrRfNeighborOverlap.rsrp, RF_SMOOTH_WINDOW) : nrRfNeighborOverlap.rsrp;
      const ovRsrq = rfSmoothingEnabled ? smoothSeries(nrRfNeighborOverlap.rsrq, RF_SMOOTH_WINDOW) : nrRfNeighborOverlap.rsrq;
      const ovSinr = rfSmoothingEnabled ? smoothSeries(nrRfNeighborOverlap.sinr, RF_SMOOTH_WINDOW) : nrRfNeighborOverlap.sinr;
      drawMetricChartWithIntraNeighbour("nr-rsrpchart", rsrp, ovRsrp, "dBm", pciColor, -126);
      drawMetricChartWithIntraNeighbour("nr-rsrqchart", rsrq, ovRsrq, "dB", pciColor, -15);
      drawMetricChartWithIntraNeighbour("nr-sinrchart", sinr, ovSinr, "dB", pciColor, 0);
      drawMetricChart("nr-dominancechart", dominance, "dB", "#adff2f", 6);
      drawMetricChart("nr-arfcnchart", nrCellDataAvailable ? nrArfcnHistory : [], "ARFCN", "#dda0dd", null, false);
      drawMetricChart("nr-pcichart", nrCellDataAvailable ? nrPciHistory : [], "PCI", "#f0e68c", null, true);
    }

    function drawInterNbrRfCharts() {
      const base = "#d4a017";
      const rsrp = rfSmoothingEnabled ? smoothSeries(nbrInterRsrpHistory, RF_SMOOTH_WINDOW) : nbrInterRsrpHistory;
      const rsrq = rfSmoothingEnabled ? smoothSeries(nbrInterRsrqHistory, RF_SMOOTH_WINDOW) : nbrInterRsrqHistory;
      const rssi = rfSmoothingEnabled ? smoothSeries(nbrInterRssiHistory, RF_SMOOTH_WINDOW) : nbrInterRssiHistory;
      const domSource = rfSmoothingEnabled ? smoothSeries(nInterDomHistory, RF_SMOOTH_WINDOW) : nInterDomHistory;
      const dominanceInter = primaryCellDataAvailable ? domSource : [];
      drawMetricChart("nbrintersrpchart", rsrp, "dBm", base, -126);
      drawMetricChart("nbrintersrqchart", rsrq, "dB", base, -15);
      drawMetricChart("nbrinterrssichart", rssi, "dBm", base, -95);
      drawMetricChart("nbridomchart", dominanceInter, "dB", "#bd93f9", 6);
    }

    function drawNeighbourCountCharts() {
      const canvas = el("nbrcountcombinedchart");
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      const intra = nbrIntraCountHistory;
      const inter = nbrInterCountHistory;
      if (!intra.length && !inter.length) {
        canvas._metricHover = null;
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No neighbour count samples yet", 12, 24);
        return;
      }

      const values = [...intra, ...inter]
        .map((p) => Number(p?.v))
        .filter((x) => Number.isFinite(x));
      const minV = values.length ? Math.min(...values) : 0;
      const maxV = values.length ? Math.max(...values) : 0;
      const pad = Math.max(0.25, (maxV - minV) * 0.15);
      const yMin = 0;
      const yMax = Math.max(maxV + pad, 1);
      const span = Math.max(1e-6, yMax - yMin);

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
      ctx.fillText(`${yMax.toFixed(1)} cells`, 4, 14);
      ctx.fillText(`${yMin.toFixed(1)} cells`, 4, h - 8);

      const refN = Math.max(intra.length, inter.length, 1);
      const x0 = 44;
      const x1 = w - 12;
      const y0 = h - 12;
      const y1 = 10;
      const xStep = refN > 1 ? (x1 - x0) / (refN - 1) : 0;
      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const expectedStepMs = Math.max(50, 1000 / Math.max(1, Number(currentPollHz) || 2));
      const gapBreakMs = expectedStepMs * 1.8;
      const xFor = (p, i) => {
        if (!chartGapModeEnabled) return x0 + i * xStep;
        const t = Number(p?.t);
        if (!Number.isFinite(t)) return x0 + i * xStep;
        const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / chartWindowMs));
        return x0 + ratio * (x1 - x0);
      };
      const yFor = (v) => y0 - ((Number(v) - yMin) / span) * (y0 - y1);

      const drawSeries = (samples, lineHex, pointHex) => {
        if (!samples.length) return;
        ctx.strokeStyle = lineHex;
        ctx.lineWidth = 2;
        for (let i = 1; i < samples.length; i++) {
          const p0 = samples[i - 1];
          const p1 = samples[i];
          const t0 = Number(p0?.t);
          const t1 = Number(p1?.t);
          if (chartGapModeEnabled && Number.isFinite(t0) && Number.isFinite(t1) && t1 - t0 > gapBreakMs) continue;
          const xA = xFor(p0, i - 1);
          const xB = xFor(p1, i);
          const yA = yFor(p0.v);
          const yB = yFor(p1.v);
          ctx.beginPath();
          ctx.moveTo(xA, yA);
          ctx.lineTo(xB, yB);
          ctx.stroke();
        }
        ctx.fillStyle = pointHex;
        samples.forEach((p, i) => {
          ctx.beginPath();
          ctx.arc(xFor(p, i), yFor(p.v), 2.1, 0, Math.PI * 2);
          ctx.fill();
        });
      };

      drawSeries(intra, CHART_COLOR_NBR_COUNT_INTRA, CHART_COLOR_NBR_COUNT_INTRA);
      drawSeries(inter, CHART_COLOR_NBR_COUNT_INTER, CHART_COLOR_NBR_COUNT_INTER);

      ctx.font = "11px Arial";
      ctx.fillStyle = CHART_COLOR_NBR_COUNT_INTRA;
      ctx.fillRect(w - 148, 8, 10, 3);
      ctx.fillStyle = "#dff7cf";
      ctx.fillText("Intra-frequency", w - 134, 12);
      ctx.fillStyle = CHART_COLOR_NBR_COUNT_INTER;
      ctx.fillRect(w - 148, 20, 10, 3);
      ctx.fillStyle = "#efd9f5";
      ctx.fillText("Inter-frequency", w - 134, 24);

      canvas._metricHover = {
        countDual: true,
        intraSamples: intra,
        interSamples: inter,
        xStepCount: refN,
        unitLabel: "cells",
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

    function mergeBandBwTimelineRows() {
      const nowMs = Date.now();
      const cutoff = nowMs - chartWindowMs;
      const bands = categoryHistory.band.filter((p) => Number(p?.t) >= cutoff);
      const bws = bwHistory.filter((p) => Number(p?.t) >= cutoff);
      const map = new Map();
      for (const p of bands) {
        const k = Number(p.t);
        if (!Number.isFinite(k)) continue;
        const row = map.get(k) || { t: k, band: null, bw: null, c: null };
        row.band = p.v;
        row.c = p.c;
        map.set(k, row);
      }
      for (const p of bws) {
        const k = Number(p.t);
        if (!Number.isFinite(k)) continue;
        const row = map.get(k) || { t: k, band: null, bw: null, c: null };
        const bv = Number(p.v);
        if (Number.isFinite(bv)) row.bw = bv;
        row.c = row.c || p.c;
        map.set(k, row);
      }
      const sorted = [...map.values()].sort((a, b) => a.t - b.t);
      let lastBand = "-";
      return sorted.map((m) => {
        const b = m.band !== null && m.band !== undefined && String(m.band).trim() ? String(m.band).trim() : null;
        if (b) lastBand = b;
        return { t: m.t, bandEff: lastBand, bw: Number.isFinite(Number(m.bw)) ? Number(m.bw) : null, c: m.c };
      });
    }

    function mergeNrBandBwTimelineRows() {
      const nowMs = Date.now();
      const cutoff = nowMs - chartWindowMs;
      const bands = categoryHistory.nrBand.filter((p) => Number(p?.t) >= cutoff);
      const bws = nrBwHistory.filter((p) => Number(p?.t) >= cutoff);
      const map = new Map();
      for (const p of bands) {
        const k = Number(p.t);
        if (!Number.isFinite(k)) continue;
        const row = map.get(k) || { t: k, band: null, bw: null, c: null };
        row.band = p.v;
        row.c = p.c;
        map.set(k, row);
      }
      for (const p of bws) {
        const k = Number(p.t);
        if (!Number.isFinite(k)) continue;
        const row = map.get(k) || { t: k, band: null, bw: null, c: null };
        const bv = Number(p.v);
        if (Number.isFinite(bv)) row.bw = bv;
        row.c = row.c || p.c;
        map.set(k, row);
      }
      const sorted = [...map.values()].sort((a, b) => a.t - b.t);
      let lastBand = "-";
      return sorted.map((m) => {
        const b = m.band !== null && m.band !== undefined && String(m.band).trim() ? String(m.band).trim() : null;
        if (b) lastBand = b;
        return { t: m.t, bandEff: lastBand, bw: Number.isFinite(Number(m.bw)) ? Number(m.bw) : null, c: m.c };
      });
    }

    function drawBandBwCombinedChart() {
      const canvas = el("bandbwcombinedchart");
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      const rows = mergeBandBwTimelineRows();
      if (!rows.length) {
        canvas._metricHover = null;
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No band / DL BW samples yet", 12, 24);
        return;
      }

      const labels = [];
      for (const r of rows) {
        const lb = r.bandEff || "-";
        if (!labels.includes(lb)) labels.push(lb);
      }
      const levels = Math.max(1, labels.length - 1);

      const bwVals = rows.map((r) => r.bw).filter((v) => v !== null && Number.isFinite(v));
      const padBw = bwVals.length ? Math.max(0.5, (Math.max(...bwVals) - Math.min(...bwVals)) * 0.12) : 1;
      const yMinBw = 0;
      const yMaxBw = bwVals.length ? Math.max(Math.max(...bwVals) + padBw, 1) : 1;
      const spanBw = Math.max(1e-6, yMaxBw - yMinBw);

      const leftPad = 92;
      const rightPad = 52;
      const x0 = leftPad;
      const x1 = w - rightPad;
      const y0 = h - 12;
      const y1 = 10;
      const n = rows.length;
      const xStep = n > 1 ? (x1 - x0) / (n - 1) : 0;
      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const expectedStepMs = Math.max(50, 1000 / Math.max(1, Number(currentPollHz) || 2));
      const gapBreakMs = expectedStepMs * 1.8;
      const xFor = (row, i) => {
        if (!chartGapModeEnabled) return x0 + i * xStep;
        const t = Number(row?.t);
        if (!Number.isFinite(t)) return x0 + i * xStep;
        const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / chartWindowMs));
        return x0 + ratio * (x1 - x0);
      };
      const yForBand = (idx) => y0 - (idx / Math.max(1, levels)) * (y0 - y1);
      const yForBw = (mhz) => y0 - ((mhz - yMinBw) / spanBw) * (y0 - y1);

      ctx.strokeStyle = "#2a2a2a";
      ctx.lineWidth = 1;
      labels.forEach((lbl, idx) => {
        const y = yForBand(idx);
        ctx.beginPath();
        ctx.moveTo(x0, y);
        ctx.lineTo(x1, y);
        ctx.stroke();
        ctx.fillStyle = "#888";
        ctx.font = "10px Arial";
        const shown = lbl.length > 14 ? `${lbl.slice(0, 14)}…` : lbl;
        ctx.fillText(shown, 4, y + 3);
      });

      ctx.fillStyle = "#aaa";
      ctx.font = "11px Arial";
      ctx.fillText(`${yMaxBw.toFixed(1)} MHz`, w - rightPad + 6, 14);
      ctx.fillText(`${yMinBw.toFixed(1)} MHz`, w - rightPad + 6, h - 8);

      ctx.lineWidth = 2;
      ctx.strokeStyle = CHART_COLOR_BAND_TREND;
      for (let i = 1; i < rows.length; i++) {
        const p0 = rows[i - 1];
        const p1 = rows[i];
        const t0 = Number(p0?.t);
        const t1 = Number(p1?.t);
        if (chartGapModeEnabled && Number.isFinite(t0) && Number.isFinite(t1) && t1 - t0 > gapBreakMs) continue;
        const i0 = labels.indexOf(p0.bandEff);
        const i1 = labels.indexOf(p1.bandEff);
        const idx0 = i0 < 0 ? 0 : i0;
        const idx1 = i1 < 0 ? 0 : i1;
        ctx.beginPath();
        ctx.moveTo(xFor(p0, i - 1), yForBand(idx0));
        ctx.lineTo(xFor(p1, i), yForBand(idx1));
        ctx.stroke();
      }
      rows.forEach((p, i) => {
        const ix = labels.indexOf(p.bandEff);
        const idx = ix < 0 ? 0 : ix;
        ctx.fillStyle = CHART_COLOR_BAND_TREND;
        ctx.beginPath();
        ctx.arc(xFor(p, i), yForBand(idx), 2.1, 0, Math.PI * 2);
        ctx.fill();
      });

      ctx.strokeStyle = CHART_COLOR_DL_BW_TREND;
      for (let i = 1; i < rows.length; i++) {
        const p0 = rows[i - 1];
        const p1 = rows[i];
        if (p0.bw === null || p1.bw === null) continue;
        const t0 = Number(p0?.t);
        const t1 = Number(p1?.t);
        if (chartGapModeEnabled && Number.isFinite(t0) && Number.isFinite(t1) && t1 - t0 > gapBreakMs) continue;
        ctx.beginPath();
        ctx.moveTo(xFor(p0, i - 1), yForBw(p0.bw));
        ctx.lineTo(xFor(p1, i), yForBw(p1.bw));
        ctx.stroke();
      }
      rows.forEach((p, i) => {
        if (p.bw === null) return;
        ctx.fillStyle = CHART_COLOR_DL_BW_TREND;
        ctx.beginPath();
        ctx.arc(xFor(p, i), yForBw(p.bw), 2.1, 0, Math.PI * 2);
        ctx.fill();
      });

      ctx.font = "11px Arial";
      ctx.fillStyle = CHART_COLOR_BAND_TREND;
      ctx.fillRect(w - 132, 8, 10, 3);
      ctx.fillStyle = "#fdebd0";
      ctx.fillText("Band", w - 118, 12);
      ctx.fillStyle = CHART_COLOR_DL_BW_TREND;
      ctx.fillRect(w - 132, 20, 10, 3);
      ctx.fillStyle = "#d5f5ee";
      ctx.fillText("DL BW", w - 118, 24);

      canvas._metricHover = {
        bandBw: true,
        bandBwRows: rows,
        labels,
        levels,
        x0,
        x1,
        y0,
        y1,
        yMinBw,
        yMaxBw,
        spanBw,
        xFor,
        yForBand,
        yForBw,
        gapBreakMs,
        chartNowMs: nowMs,
        cwMs: chartWindowMs,
        gapMode: chartGapModeEnabled
      };
    }

    /** True when QCAINFO snapshot lists at least one SCC carrier (LTE CA active). */
    function caSampleHasScc(sample) {
      const arr = sample?.carriers;
      if (!Array.isArray(arr)) return false;
      return arr.some((c) => String(c?.role || "").toUpperCase() === "SCC");
    }

    /** PCC / first SCC: primary RF (`#4da3ff`) vs neighbour-style (`#61dafb` fallback; same keys → same map as KPI overlays). */
    function caStripeColorsFromCarriers(carriers) {
      const fbPcc = "#4da3ff";
      const fbScc = "#61dafb";
      if (!Array.isArray(carriers)) return [fbPcc, fbScc];
      const pcc = carriers.find((c) => String(c?.role || "").toUpperCase() === "PCC");
      const scc = carriers.find((c) => String(c?.role || "").toUpperCase() === "SCC");
      const pk =
        pcc != null && Number.isFinite(Number(pcc.earfcn)) && Number.isFinite(Number(pcc.pci))
          ? `${Number(pcc.earfcn)}/${Number(pcc.pci)}`
          : null;
      const sk =
        scc != null && Number.isFinite(Number(scc.earfcn)) && Number.isFinite(Number(scc.pci))
          ? `${Number(scc.earfcn)}/${Number(scc.pci)}`
          : null;
      const a = pk ? colorForCellKey(pk, fbPcc) : fbPcc;
      const b = sk ? colorForCellKey(sk, fbScc) : fbScc;
      return [a, b];
    }

    function caPccOnlyStrokeColor(sample) {
      const carriers = sample?.carriers;
      if (Array.isArray(carriers)) {
        const pcc = carriers.find((c) => String(c?.role || "").toUpperCase() === "PCC");
        const pk =
          pcc != null && Number.isFinite(Number(pcc.earfcn)) && Number.isFinite(Number(pcc.pci))
            ? `${Number(pcc.earfcn)}/${Number(pcc.pci)}`
            : null;
        if (pk) return colorForCellKey(pk, "#4da3ff");
      }
      return colorForCellKey(sample?.c, "#4da3ff");
    }

    function strokeStripedSegment(ctx, xA, yA, xB, yB, colA, colB, stripePx) {
      const dx = xB - xA;
      const dy = yB - yA;
      const len = Math.hypot(dx, dy);
      if (len < 1e-6) return;
      const ux = dx / len;
      const uy = dy / len;
      let acc = 0;
      let flip = 0;
      const stripe = Math.max(2, stripePx);
      ctx.lineWidth = 2;
      ctx.lineCap = "butt";
      while (acc < len) {
        const seg = Math.min(stripe, len - acc);
        const xs0 = xA + ux * acc;
        const ys0 = yA + uy * acc;
        const xs1 = xA + ux * (acc + seg);
        const ys1 = yA + uy * (acc + seg);
        ctx.strokeStyle = flip % 2 === 0 ? colA : colB;
        ctx.beginPath();
        ctx.moveTo(xs0, ys0);
        ctx.lineTo(xs1, ys1);
        ctx.stroke();
        acc += seg;
        flip++;
      }
    }

    function fillSplitCaMarker(ctx, x, y, r, colA, colB) {
      ctx.beginPath();
      ctx.arc(x, y, r, -Math.PI / 2, Math.PI / 2);
      ctx.lineTo(x, y);
      ctx.closePath();
      ctx.fillStyle = colA;
      ctx.fill();
      ctx.beginPath();
      ctx.arc(x, y, r, Math.PI / 2, -Math.PI / 2);
      ctx.lineTo(x, y);
      ctx.closePath();
      ctx.fillStyle = colB;
      ctx.fill();
    }

    function drawCaCombinedChart() {
      const canvas = el("ca-combo-chart");
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      const splitY = Math.round(h * 0.5);
      const bandGap = 8;
      const topY1 = 10;
      const topY0 = splitY - bandGap;
      const botY1 = splitY + bandGap;
      const botY0 = h - 12;

      const leftPad = 100;
      const rightPad = 12;
      const x0 = leftPad;
      const x1 = w - rightPad;

      const earfcnSamples = categoryHistory.caEarfcn || [];
      const mhzSource = primaryCellDataAvailable ? caAggBwHistory : [];
      const mhzSeries = rfSmoothingEnabled ? smoothSeries(mhzSource, RF_SMOOTH_WINDOW) : mhzSource;

      if (!earfcnSamples.length && !mhzSeries.length) {
        canvas._metricHover = null;
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No CA / QCAINFO samples yet", 12, h / 2);
        return;
      }

      ctx.strokeStyle = "#3a3a3a";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x0 - 8, splitY);
      ctx.lineTo(x1, splitY);
      ctx.stroke();

      ctx.fillStyle = "#666";
      ctx.font = "9px Arial";
      ctx.fillText("EARFCN", 6, topY1 + 10);
      ctx.fillText("Σ DL BW", 6, botY1 + 11);

      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const expectedStepMs = Math.max(50, 1000 / Math.max(1, Number(currentPollHz) || 2));
      const gapBreakMs = expectedStepMs * 1.8;

      let catLabels = [];
      let catLevels = 1;
      if (earfcnSamples.length) {
        for (const s of earfcnSamples) {
          if (!caSampleHasScc(s)) continue;
          if (!catLabels.includes(s.v)) catLabels.push(s.v);
        }
        catLevels = Math.max(1, catLabels.length - 1);

        if (catLabels.length) {
          ctx.strokeStyle = "#2a2a2a";
          ctx.lineWidth = 1;
          const labelMax = 22;
          catLabels.forEach((lbl, idx) => {
            const y = topY0 - (idx / Math.max(1, catLevels)) * (topY0 - topY1);
            ctx.beginPath();
            ctx.moveTo(x0, y);
            ctx.lineTo(x1, y);
            ctx.stroke();
            ctx.fillStyle = "#888";
            ctx.font = "10px Arial";
            const shown = lbl.length > labelMax ? `${lbl.slice(0, labelMax)}...` : lbl;
            ctx.fillText(shown, 4, y + 3);
          });

          const nCat = earfcnSamples.length;
          const xStepCat = nCat > 1 ? (x1 - x0) / (nCat - 1) : 0;
          const xForCat = (p, i) => {
            if (!chartGapModeEnabled) return x0 + i * xStepCat;
            const t = Number(p?.t);
            if (!Number.isFinite(t)) return x0 + i * xStepCat;
            const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / chartWindowMs));
            return x0 + ratio * (x1 - x0);
          };
          for (let i = 1; i < earfcnSamples.length; i++) {
            const p0 = earfcnSamples[i - 1];
            const p1 = earfcnSamples[i];
            if (!caSampleHasScc(p0) || !caSampleHasScc(p1)) continue;
            const t0 = Number(p0?.t);
            const t1 = Number(p1?.t);
            if (chartGapModeEnabled && Number.isFinite(t0) && Number.isFinite(t1) && (t1 - t0) > gapBreakMs) continue;
            const idx0 = catLabels.indexOf(p0.v);
            const idx1 = catLabels.indexOf(p1.v);
            if (idx0 < 0 || idx1 < 0) continue;
            const y0p = topY0 - (idx0 / Math.max(1, catLevels)) * (topY0 - topY1);
            const y1p = topY0 - (idx1 / Math.max(1, catLevels)) * (topY0 - topY1);
            const xA = xForCat(p0, i - 1);
            const xB = xForCat(p1, i);
            const [c0, c1] = caStripeColorsFromCarriers(p1.carriers);
            strokeStripedSegment(ctx, xA, y0p, xB, y1p, c0, c1, 4);
          }
          earfcnSamples.forEach((p, i) => {
            if (!caSampleHasScc(p)) return;
            const idx = catLabels.indexOf(p.v);
            if (idx < 0) return;
            const y = topY0 - (idx / Math.max(1, catLevels)) * (topY0 - topY1);
            const x = xForCat(p, i);
            const [c0, c1] = caStripeColorsFromCarriers(p.carriers);
            fillSplitCaMarker(ctx, x, y, 2.3, c0, c1);
          });
        } else {
          ctx.fillStyle = "#555";
          ctx.font = "11px Arial";
          ctx.fillText("No SCC — no CA component plotted", x0, topY0 - 12);
        }
      } else {
        ctx.fillStyle = "#555";
        ctx.font = "11px Arial";
        ctx.fillText("No EARFCN samples", x0, topY0 - 12);
      }

      let mhzYMin = 0;
      let mhzYMax = 1;
      let mhzSpan = 1;
      if (mhzSeries.length) {
        const values = mhzSeries.map((p) => p.v);
        const minV = Math.min(...values);
        const maxV = Math.max(...values);
        const pad = Math.max(0.5, (maxV - minV) * 0.15);
        mhzYMin = 0;
        mhzYMax = Math.max(maxV + pad, 1);
        mhzSpan = Math.max(1e-6, mhzYMax - mhzYMin);

        ctx.strokeStyle = "#2a2a2a";
        ctx.lineWidth = 1;
        for (let i = 0; i <= 4; i++) {
          const y = botY1 + (i * (botY0 - botY1)) / 4;
          ctx.beginPath();
          ctx.moveTo(x0, y);
          ctx.lineTo(x1, y);
          ctx.stroke();
        }
        ctx.fillStyle = "#888";
        ctx.font = "10px Arial";
        ctx.fillText(`${mhzYMax.toFixed(1)} MHz`, 4, botY1 + 12);
        ctx.fillText(`${mhzYMin.toFixed(1)} MHz`, 4, botY0 - 2);

        const nMhz = mhzSeries.length;
        const xForMhz = (p, i) => {
          if (!chartGapModeEnabled) {
            const xs = nMhz > 1 ? (x1 - x0) / (nMhz - 1) : 0;
            return x0 + i * xs;
          }
          const t = Number(p?.t);
          if (!Number.isFinite(t)) return x0;
          const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / chartWindowMs));
          return x0 + ratio * (x1 - x0);
        };
        for (let i = 1; i < mhzSeries.length; i++) {
          const p0 = mhzSeries[i - 1];
          const p1 = mhzSeries[i];
          const t0 = Number(p0?.t);
          const t1 = Number(p1?.t);
          if (chartGapModeEnabled && Number.isFinite(t0) && Number.isFinite(t1) && (t1 - t0) > gapBreakMs) continue;
          const xA = xForMhz(p0, i - 1);
          const xB = xForMhz(p1, i);
          const yA = botY0 - ((p0.v - mhzYMin) / mhzSpan) * (botY0 - botY1);
          const yB = botY0 - ((p1.v - mhzYMin) / mhzSpan) * (botY0 - botY1);
          const s0 = caSampleHasScc(p0);
          const s1 = caSampleHasScc(p1);
          if (s0 && s1) {
            const [c0, c1] = caStripeColorsFromCarriers(p1.carriers);
            strokeStripedSegment(ctx, xA, yA, xB, yB, c0, c1, 4);
          } else {
            ctx.strokeStyle = caPccOnlyStrokeColor(p1);
            ctx.lineWidth = 2;
            ctx.beginPath();
            ctx.moveTo(xA, yA);
            ctx.lineTo(xB, yB);
            ctx.stroke();
          }
        }
        mhzSeries.forEach((p, i) => {
          const x = xForMhz(p, i);
          const y = botY0 - ((p.v - mhzYMin) / mhzSpan) * (botY0 - botY1);
          if (caSampleHasScc(p)) {
            const [c0, c1] = caStripeColorsFromCarriers(p.carriers);
            fillSplitCaMarker(ctx, x, y, 2.3, c0, c1);
          } else {
            ctx.fillStyle = caPccOnlyStrokeColor(p);
            ctx.beginPath();
            ctx.arc(x, y, 2.3, 0, Math.PI * 2);
            ctx.fill();
          }
        });
      } else {
        ctx.fillStyle = "#555";
        ctx.font = "11px Arial";
        ctx.fillText("No aggregated DL BW samples", x0, botY0 - 8);
      }

      canvas._metricHover = {
        caCombo: true,
        earfcnSamples,
        catLabels,
        catLevels,
        topY0,
        topY1,
        mhzSamples: mhzSeries,
        mhzYMin,
        mhzYMax,
        mhzSpan,
        botY0,
        botY1,
        x0,
        x1,
        gapBreakMs,
        chartNowMs: nowMs,
        cwMs: chartWindowMs,
        gapMode: chartGapModeEnabled
      };
    }

    function drawNrBandBwCombinedChart() {
      const canvas = el("nr-bandbwcombinedchart");
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#101010";
      ctx.fillRect(0, 0, w, h);

      const rows = mergeNrBandBwTimelineRows();
      if (!rows.length) {
        canvas._metricHover = null;
        ctx.fillStyle = "#777";
        ctx.font = "12px Arial";
        ctx.fillText("No NR band / DL BW samples yet", 12, 24);
        return;
      }

      const labels = [];
      for (const r of rows) {
        const lb = r.bandEff || "-";
        if (!labels.includes(lb)) labels.push(lb);
      }
      const levels = Math.max(1, labels.length - 1);

      const bwVals = rows.map((r) => r.bw).filter((v) => v !== null && Number.isFinite(v));
      const padBw = bwVals.length ? Math.max(0.5, (Math.max(...bwVals) - Math.min(...bwVals)) * 0.12) : 1;
      const yMinBw = 0;
      const yMaxBw = bwVals.length ? Math.max(Math.max(...bwVals) + padBw, 1) : 1;
      const spanBw = Math.max(1e-6, yMaxBw - yMinBw);

      const leftPad = 92;
      const rightPad = 52;
      const x0 = leftPad;
      const x1 = w - rightPad;
      const y0 = h - 12;
      const y1 = 10;
      const n = rows.length;
      const xStep = n > 1 ? (x1 - x0) / (n - 1) : 0;
      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const expectedStepMs = Math.max(50, 1000 / Math.max(1, Number(currentPollHz) || 2));
      const gapBreakMs = expectedStepMs * 1.8;
      const xFor = (row, i) => {
        if (!chartGapModeEnabled) return x0 + i * xStep;
        const t = Number(row?.t);
        if (!Number.isFinite(t)) return x0 + i * xStep;
        const ratio = Math.max(0, Math.min(1, (t - windowStartMs) / chartWindowMs));
        return x0 + ratio * (x1 - x0);
      };
      const yForBand = (idx) => y0 - (idx / Math.max(1, levels)) * (y0 - y1);
      const yForBw = (mhz) => y0 - ((mhz - yMinBw) / spanBw) * (y0 - y1);

      ctx.strokeStyle = "#2a2a2a";
      ctx.lineWidth = 1;
      labels.forEach((lbl, idx) => {
        const y = yForBand(idx);
        ctx.beginPath();
        ctx.moveTo(x0, y);
        ctx.lineTo(x1, y);
        ctx.stroke();
        ctx.fillStyle = "#888";
        ctx.font = "10px Arial";
        const shown = lbl.length > 14 ? `${lbl.slice(0, 14)}…` : lbl;
        ctx.fillText(shown, 4, y + 3);
      });

      ctx.fillStyle = "#aaa";
      ctx.font = "11px Arial";
      ctx.fillText(`${yMaxBw.toFixed(1)} MHz`, w - rightPad + 6, 14);
      ctx.fillText(`${yMinBw.toFixed(1)} MHz`, w - rightPad + 6, h - 8);

      ctx.lineWidth = 2;
      ctx.strokeStyle = CHART_COLOR_NR_BAND_TREND;
      for (let i = 1; i < rows.length; i++) {
        const p0 = rows[i - 1];
        const p1 = rows[i];
        const t0 = Number(p0?.t);
        const t1 = Number(p1?.t);
        if (chartGapModeEnabled && Number.isFinite(t0) && Number.isFinite(t1) && t1 - t0 > gapBreakMs) continue;
        const i0 = labels.indexOf(p0.bandEff);
        const i1 = labels.indexOf(p1.bandEff);
        const idx0 = i0 < 0 ? 0 : i0;
        const idx1 = i1 < 0 ? 0 : i1;
        ctx.beginPath();
        ctx.moveTo(xFor(p0, i - 1), yForBand(idx0));
        ctx.lineTo(xFor(p1, i), yForBand(idx1));
        ctx.stroke();
      }
      rows.forEach((p, i) => {
        const ix = labels.indexOf(p.bandEff);
        const idx = ix < 0 ? 0 : ix;
        ctx.fillStyle = CHART_COLOR_NR_BAND_TREND;
        ctx.beginPath();
        ctx.arc(xFor(p, i), yForBand(idx), 2.1, 0, Math.PI * 2);
        ctx.fill();
      });

      ctx.strokeStyle = CHART_COLOR_NR_DL_BW_TREND;
      for (let i = 1; i < rows.length; i++) {
        const p0 = rows[i - 1];
        const p1 = rows[i];
        if (p0.bw === null || p1.bw === null) continue;
        const t0 = Number(p0?.t);
        const t1 = Number(p1?.t);
        if (chartGapModeEnabled && Number.isFinite(t0) && Number.isFinite(t1) && t1 - t0 > gapBreakMs) continue;
        ctx.beginPath();
        ctx.moveTo(xFor(p0, i - 1), yForBw(p0.bw));
        ctx.lineTo(xFor(p1, i), yForBw(p1.bw));
        ctx.stroke();
      }
      rows.forEach((p, i) => {
        if (p.bw === null) return;
        ctx.fillStyle = CHART_COLOR_NR_DL_BW_TREND;
        ctx.beginPath();
        ctx.arc(xFor(p, i), yForBw(p.bw), 2.1, 0, Math.PI * 2);
        ctx.fill();
      });

      ctx.font = "11px Arial";
      ctx.fillStyle = CHART_COLOR_NR_BAND_TREND;
      ctx.fillRect(w - 148, 8, 10, 3);
      ctx.fillStyle = "#d5f4e6";
      ctx.fillText("NR band", w - 134, 12);
      ctx.fillStyle = CHART_COLOR_NR_DL_BW_TREND;
      ctx.fillRect(w - 148, 20, 10, 3);
      ctx.fillStyle = "#d4efdf";
      ctx.fillText("NR DL BW", w - 134, 24);

      canvas._metricHover = {
        bandBw: true,
        bandBwRows: rows,
        labels,
        levels,
        x0,
        x1,
        y0,
        y1,
        yMinBw,
        yMaxBw,
        spanBw,
        xFor,
        yForBand,
        yForBw,
        gapBreakMs,
        chartNowMs: nowMs,
        cwMs: chartWindowMs,
        gapMode: chartGapModeEnabled
      };
    }

    function drawCategoryChart(canvasId, samples, color, opts) {
      const labelMax = opts && opts.labelMax != null ? opts.labelMax : 12;
      const leftPad = opts && opts.leftPad != null ? opts.leftPad : 92;
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

      const labels = [];
      for (const s of samples) {
        if (!labels.includes(s.v)) labels.push(s.v);
      }
      const levels = Math.max(1, labels.length - 1);
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
        const shown = lbl.length > labelMax ? `${lbl.slice(0, labelMax)}...` : lbl;
        ctx.fillText(shown, 4, y + 4);
      });

      const n = samples.length;
      const xStep = n > 1 ? (x1 - x0) / (n - 1) : 0;
      const nowMs = Date.now();
      const windowStartMs = nowMs - chartWindowMs;
      const expectedStepMs = Math.max(50, 1000 / Math.max(1, Number(currentPollHz) || 2));
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
      if (opts && opts.categoryHover) {
        canvas._metricHover = {
          categoryStep: true,
          categoryValueLabel: opts.categoryValueLabel && String(opts.categoryValueLabel).trim() ? String(opts.categoryValueLabel).trim() : "RAT",
          samples,
          labels,
          levels,
          x0,
          x1,
          y0,
          y1,
          gapBreakMs,
          chartNowMs: nowMs,
          cwMs: chartWindowMs,
          gapMode: chartGapModeEnabled
        };
      } else {
        canvas._metricHover = null;
      }
    }

    function drawCategoryCharts() {
      const currentCellKey =
        Number.isFinite(currentServingEarfcn) && Number.isFinite(currentServingPci)
          ? `${currentServingEarfcn}/${currentServingPci}`
          : null;
      const cellColor = colorForCellKey(currentCellKey, "#8be9fd");
      drawCategoryChart("statechart", categoryHistory.state, cellColor);
      drawCategoryChart("ratchart", categoryHistory.rat, "#ffb86c", {
        labelMax: 16,
        leftPad: 100,
        categoryHover: true,
        categoryValueLabel: "RAT"
      });
      drawCaCombinedChart();
    }

    function clearDataServiceKpi() {
      lastDataService = {};
      el("ds-apn").textContent = "-";
      const pdpTk = el("ds-pdp-type-kpi");
      if (pdpTk) pdpTk.textContent = "—";
      const pdpUk = el("ds-pdp-user-kpi");
      if (pdpUk) pdpUk.textContent = "—";
      const pdpAk = el("ds-pdp-auth-kpi");
      if (pdpAk) pdpAk.textContent = "—";
      const pdpPh = el("ds-pdp-pw-hint");
      if (pdpPh) pdpPh.textContent = "—";
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
      resetCongestionProxyState();
      Object.keys(rfNeighborOverlap).forEach((k) => {
        rfNeighborOverlap[k].length = 0;
      });
      nbrInterRsrpHistory.length = 0;
      nbrInterRsrqHistory.length = 0;
      nbrInterRssiHistory.length = 0;
      nInterDomHistory.length = 0;
      nbrIntraCountHistory.length = 0;
      nbrInterCountHistory.length = 0;
      bwHistory.length = 0;
      caAggBwHistory.length = 0;
      carrierReselPciHistory.length = 0;
      carrierReselEarfcnHistory.length = 0;
      categoryHistory.state.length = 0;
      categoryHistory.rat.length = 0;
      categoryHistory.band.length = 0;
      categoryHistory.caEarfcn.length = 0;
      categoryHistory.nrBand.length = 0;
      Object.keys(nrRfHistory).forEach((k) => {
        nrRfHistory[k].length = 0;
      });
      Object.keys(nrRfNeighborOverlap).forEach((k) => {
        nrRfNeighborOverlap[k].length = 0;
      });
      nrBwHistory.length = 0;
      nrArfcnHistory.length = 0;
      nrPciHistory.length = 0;
      drawIperfChart();
      drawIperfGauges();
      drawPhSweepChart();
      drawPhGauges();
      drawRfCharts();
      drawInterNbrRfCharts();
      drawNeighbourCountCharts();
      drawBandBwCombinedChart();
      drawCaCombinedChart();
      drawCarrierReselChart();
      drawCategoryCharts();
      drawNrRfCharts();
      drawNrBandBwCombinedChart();
      clearDataServiceKpi();
      const _z = ["rsrp-std", "rsrq-std", "sinr-std", "rssi-std"];
      for (const id of _z) {
        const n = el(id);
        if (n) n.textContent = "-";
      }
      const eca = el("earfcn-active-ca");
      if (eca) eca.textContent = "-";
      const cab = el("ca-agg-dl-bw");
      if (cab) cab.textContent = "-";
    }

    async function pollFallback() {
      try {
        const r = await fetch("/api/kpi/latest");
        if (!r.ok) return;
        try {
          applySnap(await r.json());
        } catch (e) {
          console.error("KPI poll fallback applySnap error:", e);
          const st = el("status");
          st.textContent = `Live KPI update error: ${e && e.message ? e.message : e}`;
          st.className = "label err";
        }
      } catch (_) {}
    }

    async function pollNeighbourChannels() {
      try {
        const r = await fetch("/api/kpi/neighbour-channels");
        if (!r.ok) return;
        const j = await r.json();
        const t2 = j.inter_text;
        const p2 = document.getElementById("nbr-inter-channels");
        if (p2) p2.textContent = typeof t2 === "string" ? t2 : "-";
      } catch (e) {
        console.error("Neighbour channels poll error:", e);
      }
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

    function _trVal(id) {
      const n = el(id);
      if (!n) return null;
      if (n.type === "checkbox") return !!n.checked;
      const s = String(n.value ?? "").trim();
      return s.length ? s : null;
    }
    function collectUiControlsForRun() {
      return {
        chart: {
          window_sec: Number(el("chart-window-select")?.value || 600),
          gap_mode: !!chartGapModeEnabled,
          rf_smoothing: !!el("rf-smooth-toggle")?.checked,
          rf_std_sample_n: Number(el("rf-std-sample-count")?.value || 60),
        },
        serial: { port: _trVal("serial-port-select"), baud: serialBaud },
        cops_scan_uk_only: _trVal("cops-scan-uk-only"),
        data_service_form: {
          apn: _trVal("ds-apn-set"),
          pdp_type: _trVal("ds-pdp-type"),
          pdp_auth: _trVal("ds-pdp-auth-type"),
          pdp_username: _trVal("ds-pdp-net-user"),
          reactivate_checked: _trVal("ds-apn-reactivate"),
        },
        mno: {
          select: _trVal("mno-select"),
          cops_mode: _trVal("mno-cops-mode"),
          skip_dereg: _trVal("mno-skip-dereg"),
        },
        locks: {
          ratmode: _trVal("input-ratmode"),
          ca_enable: _trVal("input-ca-enable"),
          ca_on_bands: _trVal("input-ca-on-bands"),
          ca_single_band: _trVal("input-ca-single-band"),
          lte_bands: _trVal("input-lteband"),
          nr_bands: _trVal("input-nrband"),
          nrdc_enable: _trVal("input-nrdc-enable"),
        },
        volte_panel: {
          number: _trVal("volte-number"),
          hold_sec: _trVal("volte-hold-sec"),
          connect_timeout: _trVal("volte-connect-timeout"),
          autoanswer_enabled: _trVal("autoanswer-enabled"),
          autoanswer_rings: _trVal("autoanswer-rings"),
        },
        iperf: {
          host: _trVal("iperf-host"),
          port: _trVal("iperf-port"),
          duration: _trVal("iperf-duration"),
          parallel: _trVal("iperf-parallel"),
          connect_timeout_sec: _trVal("iperf-connect-timeout"),
          direction: _trVal("iperf-direction"),
          protocol: _trVal("iperf-protocol"),
          bind_select: _trVal("iperf-bind-select"),
          bind_ip: _trVal("iperf-bind-ip"),
          speed_limit: _trVal("iperf-speed-limit"),
        },
        ping_sweep: {
          host: _trVal("ph-host"),
          count: _trVal("ph-count"),
          bind_select: _trVal("ph-bind-select"),
          bind_ip: _trVal("ph-bind-ip"),
          repeat: _trVal("ph-repeat-toggle"),
        },
        test_runner: {
          bind_select: _trVal("test-runner-bind-select"),
          bind_ip: _trVal("test-runner-bind-ip"),
          note: String(el("test-runner-note")?.value || "").trim() || null,
          test_iterations: Number(el("test-runner-iterations")?.value || 1),
          test_iteration_delay_sec: Number(el("test-runner-iter-delay")?.value || 10),
        },
      };
    }
    async function refreshTestRunnerProfiles() {
      const sel = el("test-runner-profile");
      if (!sel) return;
      try {
        const r = await fetch("/api/test/profiles");
        const j = await r.json();
        const names = Array.isArray(j.names) ? j.names : [];
        sel.innerHTML = "";
        for (const n of names) {
          const o = document.createElement("option");
          o.value = n;
          o.textContent = n;
          sel.appendChild(o);
        }
        if (!names.length) {
          const o = document.createElement("option");
          o.value = "";
          o.textContent = "(no profiles)";
          sel.appendChild(o);
        }
      } catch (e) {
        sel.innerHTML = "";
        const o = document.createElement("option");
        o.value = "";
        o.textContent = "Load failed";
        sel.appendChild(o);
      }
    }
    let _testRunnerProgressTimer = null;
    function stopTestRunnerProgressPoll() {
      if (_testRunnerProgressTimer) {
        clearInterval(_testRunnerProgressTimer);
        _testRunnerProgressTimer = null;
      }
      const pr = el("test-runner-progress");
      if (pr) pr.textContent = "";
    }
    function startTestRunnerProgressPoll() {
      stopTestRunnerProgressPoll();
      const pr = el("test-runner-progress");
      const tick = async () => {
        try {
          const r = await fetch("/api/test/active");
          if (!r.ok) return;
          const j = await r.json();
          if (!pr) return;
          if (!j.active) {
            pr.textContent = "";
            return;
          }
          const tot = Number(j.iterations_total) || 1;
          if (tot < 2) {
            pr.textContent = "";
            return;
          }
          if (j.phase === "delay" && typeof j.seconds_until_next === "number") {
            const next = Number(j.iteration_next);
            const s = Math.max(0, Math.ceil(j.seconds_until_next));
            pr.textContent = Number.isFinite(next)
              ? `Next iteration ${next}/${tot} in ${s}s`
              : `Next iteration in ${s}s`;
          } else if (j.phase === "tool" && j.iteration_running) {
            pr.textContent = `Running iteration ${j.iteration_running}/${tot}`;
          } else if (j.phase === "modem_requirements") {
            pr.textContent = "Preparing modem for test…";
          } else if (j.phase === "complete") {
            pr.textContent = "Finishing…";
          } else {
            pr.textContent = "";
          }
        } catch (_) {}
      };
      tick();
      _testRunnerProgressTimer = setInterval(tick, 400);
    }
    async function cancelTestRunnerRun() {
      const msg = el("test-runner-msg");
      try {
        const r = await fetch("/api/test/cancel", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
        });
        const j = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(userFacingBackendError(j, `HTTP ${r.status}`));
        const rid = j.run_id ? ` run_id=${j.run_id}` : "";
        if (msg) msg.textContent = `${msg.textContent || ""} Cancel requested.${rid}`.trim();
      } catch (e) {
        if (msg) msg.textContent = `Cancel: ${e?.message || e}`;
      }
    }
    async function runTestRunnerProfile() {
      const msg = el("test-runner-msg");
      const sel = el("test-runner-profile");
      const btnRun = el("btn-test-runner-run");
      const btnCancel = el("btn-test-runner-cancel");
      if (!sel || !String(sel.value || "").trim()) {
        if (msg) msg.textContent = "Select a profile first (create one via POST /api/test/profiles).";
        return;
      }
      if (msg) msg.textContent = "Running…";
      if (btnRun) btnRun.disabled = true;
      if (btnCancel) btnCancel.disabled = false;
      try {
        let nIt = Math.max(1, Math.min(100, Math.floor(Number(el("test-runner-iterations")?.value) || 1)));
        let dIt = Math.max(10, Math.min(3600, Number(el("test-runner-iter-delay")?.value) || 10));
        if (!Number.isFinite(nIt)) nIt = 1;
        if (!Number.isFinite(dIt)) dIt = 10;
        const body = {
          profile_name: String(sel.value).trim(),
          project_name: String(el("test-runner-project")?.value || "").trim(),
          test_location: String(el("test-runner-location")?.value || "").trim(),
          engineer: String(el("test-runner-engineer")?.value || "").trim(),
          note: String(el("test-runner-note")?.value || "").trim(),
          test_iterations: nIt,
          test_iteration_delay_sec: dIt,
          include_ui_snapshot: true,
          ui_controls: collectUiControlsForRun(),
          unlock_password: String(el("test-runner-unlock")?.value || "") || null,
        };
        const trBind = el("test-runner-bind-select");
        if (trBind) {
          if (trBind.value === "__profile__") {
            /* omit ping_bind_ipv4_override → server uses profile test_config.bind_ipv4 */
          } else if (trBind.value === "manual") {
            const ip = String(el("test-runner-bind-ip")?.value || "").trim();
            if (!ip) {
              if (msg) msg.textContent = "Ping bind: choose an interface or enter a manual IPv4.";
              return;
            }
            body.ping_bind_ipv4_override = ip;
          } else if (trBind.value === "auto") {
            body.ping_bind_ipv4_override = "";
          } else {
            body.ping_bind_ipv4_override = String(trBind.value || "").trim();
          }
        }
        startTestRunnerProgressPoll();
        const r = await fetch("/api/test/run", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const j = await r.json();
        if (!r.ok) throw new Error(userFacingBackendError(j, `HTTP ${r.status}`));
        if (msg) {
          const folder = j.run_folder ? ` folder=${j.run_folder}` : "";
          const dirHint = j.artifacts_dir ? ` Files: ${j.artifacts_dir}` : "";
          const cx = j.run_cancelled ? " cancelled=true" : "";
          msg.textContent = `OK run_id=${j.run_id} success=${j.run_success}.${cx}${folder}${dirHint}`;
        }
      } catch (e) {
        if (msg) msg.textContent = `Error: ${e?.message || e}`;
      } finally {
        stopTestRunnerProgressPoll();
        if (btnRun) btnRun.disabled = false;
        if (btnCancel) btnCancel.disabled = true;
      }
    }

    const wsProto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${wsProto}//${location.host}/ws/kpi`);
    ws.onopen = () => { el("status").textContent = "WebSocket connected."; el("status").className = "label ok"; };
    ws.onmessage = (ev) => {
      try {
        applySnap(JSON.parse(ev.data));
      } catch (e) {
        console.error("KPI WebSocket message error:", e);
        const st = el("status");
        st.textContent = `Live KPI parse/update error: ${e && e.message ? e.message : e}`;
        st.className = "label err";
      }
    };
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
    const aaEn = el("autoanswer-enabled");
    if (aaEn) aaEn.addEventListener("change", () => applyAutoAnswer());
    const aaRings = el("autoanswer-rings");
    if (aaRings) aaRings.addEventListener("change", () => applyAutoAnswer());
    const voltePwd = el("volte-password");
    if (voltePwd) {
      const tryAaAfterPassword = () => {
        if (el("autoanswer-enabled")?.checked && String(el("volte-password")?.value || "")) applyAutoAnswer();
      };
      voltePwd.addEventListener("change", tryAaAfterPassword);
      voltePwd.addEventListener("blur", tryAaAfterPassword);
    }
    const btnVoiceHangup = el("btn-voice-hangup");
    if (btnVoiceHangup) btnVoiceHangup.addEventListener("click", () => voiceHangupCall());
    const btnVoiceAnswer = el("btn-voice-answer");
    if (btnVoiceAnswer) btnVoiceAnswer.addEventListener("click", () => voiceAnswerCall());
    el("btn-iperf-test").addEventListener("click", () => runIperfTest());
    el("iperf-bind-select").addEventListener("change", () => syncIperfBindUi());
    el("btn-iperf-refresh-ifaces").addEventListener("click", () => loadBindInterfaces());
    const phBindSel = el("ph-bind-select");
    if (phBindSel) phBindSel.addEventListener("change", () => syncPhBindUi());
    const btnPhRefresh = el("btn-ph-refresh-ifaces");
    if (btnPhRefresh) btnPhRefresh.addEventListener("click", () => loadBindInterfaces());
    const btnPhRun = el("btn-ph-run");
    if (btnPhRun) btnPhRun.addEventListener("click", () => runPingSweepTest());
    const btnTrRefresh = el("btn-test-runner-refresh");
    if (btnTrRefresh) btnTrRefresh.addEventListener("click", () => refreshTestRunnerProfiles());
    const btnTrIf = el("btn-test-runner-refresh-ifaces");
    if (btnTrIf) btnTrIf.addEventListener("click", () => loadBindInterfaces());
    const trBindSel = el("test-runner-bind-select");
    if (trBindSel) trBindSel.addEventListener("change", () => syncTestRunnerBindUi());
    const btnTrRun = el("btn-test-runner-run");
    if (btnTrRun) btnTrRun.addEventListener("click", () => runTestRunnerProfile());
    const btnTrCancel = el("btn-test-runner-cancel");
    if (btnTrCancel) btnTrCancel.addEventListener("click", () => cancelTestRunnerRun());
    const phRepeatToggle = el("ph-repeat-toggle");
    if (phRepeatToggle) phRepeatToggle.addEventListener("change", (ev) => setPhRepeatPing(!!ev.target.checked));
    el("btn-clear-charts").addEventListener("click", () => clearAllCharts());
    const btnUiDefaults = el("btn-ui-defaults");
    if (btnUiDefaults) btnUiDefaults.addEventListener("click", () => applyUiDefaults());
    el("btn-chart-gap-mode").addEventListener("click", () => setChartGapMode(!chartGapModeEnabled));
    el("chart-window-select").addEventListener("change", (ev) => {
      applyChartWindowSec(Number(ev.target?.value || 600));
    });
    el("rf-smooth-toggle").addEventListener("change", (ev) => {
      rfSmoothingEnabled = !!ev.target.checked;
      redrawAllCharts();
    });
    const rfStdSampleInput = el("rf-std-sample-count");
    if (rfStdSampleInput) {
      const onStdN = () => updatePrimaryRfStdDevKpis();
      rfStdSampleInput.addEventListener("change", onStdN);
      rfStdSampleInput.addEventListener("input", onStdN);
    }
    el("btn-serial-refresh").addEventListener("click", () => refreshSerialPorts(false));
    el("btn-serial-autopick").addEventListener("click", () => autoPickSerialPort());
    el("btn-serial-reconnect").addEventListener("click", () => reconnectSerial());
    el("btn-modem-reset").addEventListener("click", () => resetModem());

    setInterval(pollFallback, 2000);
    setInterval(pollNeighbourChannels, 3000);
    setInterval(pollVoiceCallStatus, 1700);
    setInterval(pollAtLog, 1200);
    setInterval(() => readSerialStatus(false), 3000);
    setInterval(() => {
      if (!chartGapModeEnabled) return;
      pruneAllHistory(Date.now());
      redrawAllCharts();
    }, 400);
    pollFallback();
    pollNeighbourChannels();
    pollVoiceCallStatus();
    pollAtLog();
    readSerialStatus(true);
    refreshSerialPorts(true);
    readCops();
    readLocks();
    readMnoState();
    readDataGate();
    readAutoAnswerStatus(true);
    readSimHighLevel();
    applyChartWindowSec(Number(el("chart-window-select")?.value || 600));
    updateChartGapButton();
    redrawAllCharts();
    loadBindInterfaces();
    refreshTestRunnerProfiles();
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
    if protocol != "tcp":
        raise HTTPException(status_code=400, detail="Only protocol='tcp' is currently supported.")
    reverse = direction == "download"
    parallel_streams = int(body.parallel_streams)
    ct_sec = max(1.0, min(120.0, float(body.connect_timeout_sec)))
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
                "parallel_streams": parallel_streams,
                "connect_timeout_sec": ct_sec,
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
        str(int(body.port)),
        "-t",
        str(int(body.duration_sec)),
        "-J",
    ]
    if reverse:
        cmd.append("-R")
    if bind_ip:
        cmd.extend(["-B", bind_ip])
    if limit_mbps is not None and float(limit_mbps) > 0:
        cmd.extend(["-b", f"{float(limit_mbps):g}M"])
    cmd.extend(["-P", str(parallel_streams)])
    if _iperf_supports_connect_timeout(binary):
        ms = max(1, int(round(float(ct_sec) * 1000.0)))
        cmd.extend(["--connect-timeout", str(ms)])
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
            "port": int(body.port),
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
        "port": int(body.port),
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
        ln
        for _, ln in delta_urc
        if any(tok in str(ln).upper() for tok in ("NO CARRIER", "+CLCC", "+CEER", "BUSY", "NO ANSWER", "NO DIALTONE"))
    ]

    setup_time_ms = int((connect_ts - dial_started) * 1000) if connect_ts else None
    active_after_hang = _has_active_or_held(clcc_after)
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
    if mr.get("require_packet_data") and test_type in ("ping", "iperf_download", "iperf_upload"):
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
                last_tr = await tools_iperf_test(
                    IperfTestBody(
                        host=str(cfg["host"]),
                        port=int(cfg["port"]),
                        duration_sec=int(cfg["duration_sec"]),
                        direction="download",
                        protocol=str(cfg.get("protocol") or "tcp").lower(),
                        mobile_only=bool(cfg["mobile_only"]),
                        bind_ip=None,
                        bitrate_limit_mbps=lim_f,
                        parallel_streams=int(cfg["parallel_streams"]),
                        connect_timeout_sec=_iperf_connect_timeout_for_profile(cfg),
                    )
                )
            elif test_type == "iperf_upload":
                lim = cfg.get("bitrate_limit_mbps")
                lim_f = float(lim) if lim is not None else None
                if lim_f is not None and lim_f <= 0:
                    lim_f = None
                last_tr = await tools_iperf_test(
                    IperfTestBody(
                        host=str(cfg["host"]),
                        port=int(cfg["port"]),
                        duration_sec=int(cfg["duration_sec"]),
                        direction="upload",
                        protocol=str(cfg.get("protocol") or "tcp").lower(),
                        mobile_only=bool(cfg["mobile_only"]),
                        bind_ip=None,
                        bitrate_limit_mbps=lim_f,
                        parallel_streams=int(cfg["parallel_streams"]),
                        connect_timeout_sec=_iperf_connect_timeout_for_profile(cfg),
                    )
                )
            elif test_type == "volte_call_outbound":
                if not body.unlock_password:
                    raise HTTPException(status_code=400, detail="unlock_password is required for VoLTE test runs.")
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
    try:
        lock_st = await _read_lock_status()
        locks = lock_st.get("values") if isinstance(lock_st, dict) else None
        agg["band_locked"] = tr.format_band_locked(locks)
    except Exception:
        pass

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
