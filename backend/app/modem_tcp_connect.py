"""Modem-side TCP connect timing via Quectel AT+QIOPEN / AT+QICLOSE."""
from __future__ import annotations

import asyncio
import re
import time
from typing import Any

from app.at_modem_errors import describe_modem_send_result
from app.kpi_service import KpiRuntime, _parse_qiact, kpi_socket_hold_enter, kpi_socket_hold_leave

_QIOPEN_URC_RE = re.compile(r"\+QIOPEN:\s*(\d+)\s*,\s*(-?\d+)", re.I)

_QICLOSE_TIMEOUT_SEC = 1.5
_QICLOSE_MAX_WAIT_SEC = 2.0

_QIOPEN_ERR_HINTS: dict[int, str] = {
    0: "TCP connection established",
    -1: "Unknown error",
    565: "Operation blocked",
    566: "Failed to create socket",
    567: "Failed to bind socket",
    568: "Failed to listen on socket",
    569: "Failed to accept connection",
    570: "Failed to connect to network",
    571: "Network not available",
    572: "Remote refused connection",
    573: "Timeout",
    574: "PDP context not active",
}


def _qiopen_err_label(err: int | None) -> str:
    if err is None:
        return "no +QIOPEN URC"
    if err in _QIOPEN_ERR_HINTS:
        return _QIOPEN_ERR_HINTS[err]
    return f"QIOPEN error code {err}"


def _parse_qiopen_in_line(line: str, connect_id: int) -> int | None:
    m = _QIOPEN_URC_RE.search(str(line or ""))
    if not m:
        return None
    if int(m.group(1)) != int(connect_id):
        return None
    return int(m.group(2))


def _scan_qiopen(engine: Any, connect_id: int, urc_from: int, extra_lines: list[str]) -> int | None:
    for ln in extra_lines:
        err = _parse_qiopen_in_line(ln, connect_id)
        if err is not None:
            return err
    urc = getattr(engine, "urc_log", None)
    if not urc:
        return None
    for _ts, ln in list(urc)[urc_from:]:
        err = _parse_qiopen_in_line(ln, connect_id)
        if err is not None:
            return err
    return None


def _host_ok_for_qiopen(host: str) -> str:
    h = str(host or "").strip()
    if not h or len(h) > 127:
        raise ValueError("host must be 1..127 characters")
    if any(c in h for c in ('"', "\r", "\n")):
        raise ValueError("host must not contain quotes or newlines")
    return h


async def run_modem_tcp_connect(
    engine: Any,
    *,
    host: str,
    port: int,
    timeout_sec: float,
    pdp_cid: int = 1,
    connect_id: int = 0,
    skip_pre_close: bool = False,
    kpi_runtime: KpiRuntime | None = None,
) -> dict[str, Any]:
    """
    Open a TCP socket on the modem (AT+QIOPEN), measure until +QIOPEN URC, then QICLOSE.
    KPI and the serial port are held for the full pre-close → QIOPEN → post-close block.
    Post-close uses a short wait cap so a slow modem OK does not block RF for ~9s.
    """
    actions: list[dict[str, Any]] = []
    host_s = _host_ok_for_qiopen(host)
    p = int(port)
    if p < 1 or p > 65535:
        raise ValueError(f"Invalid port: {port}")
    cid = max(1, min(15, int(pdp_cid)))
    sock_id = max(0, min(11, int(connect_id)))
    budget = max(1.0, min(120.0, float(timeout_sec)))

    pdp_active = False
    qiact: dict[str, Any] = {"ok": True, "final": "SKIPPED_KPI_CACHE", "lines": []}
    use_cached_pdp = False
    if kpi_runtime is not None and cid == 1:
        ds = kpi_runtime.data_service or {}
        ds_age = time.time() - float(kpi_runtime.data_service_at or 0.0)
        if ds.get("cid1_active") is True and ds_age <= 25.0:
            use_cached_pdp = True
            pdp_active = True
    if not use_cached_pdp:
        qiact = await engine.send_command("AT+QIACT?", timeout_sec=4.0)
        contexts = _parse_qiact(qiact.get("lines") or [])
        pdp_active = bool(
            qiact.get("ok")
            and next((c for c in contexts if c.get("cid") == cid and c.get("active")), None)
        )
    actions.append(
        {
            "cmd": "AT+QIACT?" if not use_cached_pdp else "AT+QIACT? (KPI cache)",
            "res": qiact,
        }
    )

    if not pdp_active:
        return {
            "ok": False,
            "error": (
                f"PDP context {cid} is not active (AT+QIACT?). "
                "Use Allow Data before running modem TCP connect."
            ),
            "host": host_s,
            "port": p,
            "pdp_cid": cid,
            "connect_id": sock_id,
            "connect_setup_ms": None,
            "qiopen_err": None,
            "qiopen_detail": None,
            "actions": actions,
        }

    urc_before = len(getattr(engine, "urc_log", []))
    qiopen_cmd = f'AT+QIOPEN={cid},{sock_id},"TCP","{host_s}",{p}'
    t0 = time.perf_counter()
    err_code: int | None = None
    connect_ms: float | None = None
    qiopen_res: dict[str, Any] = {"ok": False, "final": "NOT_RUN", "lines": []}
    qclose_res: dict[str, Any] = {"ok": False, "final": "NOT_RUN", "lines": []}

    critical = getattr(engine, "modem_socket_critical_section", None)
    if critical is None:
        raise RuntimeError("Serial engine missing modem_socket_critical_section")

    if kpi_runtime is not None:
        kpi_socket_hold_enter(kpi_runtime)
    try:
        async with critical():
            if not skip_pre_close:
                close_res = await engine._send_command_unlocked(
                    f"AT+QICLOSE={sock_id}",
                    timeout_sec=_QICLOSE_TIMEOUT_SEC,
                    max_wait_sec=_QICLOSE_MAX_WAIT_SEC,
                )
                actions.append(
                    {"cmd": f"AT+QICLOSE={sock_id} (pre-open cleanup)", "res": close_res}
                )
                await asyncio.sleep(0.15)

            qiopen_res = await engine._send_command_unlocked(
                qiopen_cmd, timeout_sec=min(8.0, budget), max_wait_sec=min(10.0, budget + 2.0)
            )
            if not qiopen_res.get("ok"):
                md = describe_modem_send_result(qiopen_res)
                actions.append({"cmd": qiopen_cmd, "res": qiopen_res})
                return {
                    "ok": False,
                    "error": md or str(qiopen_res.get("final") or "AT+QIOPEN failed"),
                    "modem_detail": md,
                    "host": host_s,
                    "port": p,
                    "pdp_cid": cid,
                    "connect_id": sock_id,
                    "connect_setup_ms": None,
                    "qiopen_err": None,
                    "qiopen_detail": None,
                    "actions": actions,
                }

            extra = list(qiopen_res.get("lines") or [])
            err_code = _scan_qiopen(engine, sock_id, urc_before, extra)
            deadline = asyncio.get_running_loop().time() + min(budget, 8.0)
            while err_code is None and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.08)
                err_code = _scan_qiopen(engine, sock_id, urc_before, [])

            t1 = time.perf_counter()
            connect_ms = round((t1 - t0) * 1000.0, 1)

            # Timing ends at +QIOPEN URC. Post-close often never returns OK while the
            # modem is busy and then blocks the next KPI command for seconds.
            if err_code is not None and int(err_code) == 0:
                qclose_res = {
                    "ok": True,
                    "final": "SKIPPED_OK_QIOPEN",
                    "lines": [],
                    "elapsed_ms": 0,
                }
                actions.append(
                    {
                        "cmd": f"AT+QICLOSE={sock_id} (skipped — +QIOPEN:0,0)",
                        "res": qclose_res,
                    }
                )
                await asyncio.sleep(0.25)
            else:
                qclose_res = await engine._send_command_unlocked(
                    f"AT+QICLOSE={sock_id}",
                    timeout_sec=_QICLOSE_TIMEOUT_SEC,
                    max_wait_sec=_QICLOSE_MAX_WAIT_SEC,
                )
                actions.append({"cmd": f"AT+QICLOSE={sock_id}", "res": qclose_res})
                if qclose_res.get("final") == "TIMEOUT":
                    sync = await engine._send_command_unlocked(
                        "AT",
                        timeout_sec=1.0,
                        max_wait_sec=1.5,
                    )
                    actions.append({"cmd": "AT (sync after QICLOSE timeout)", "res": sync})
    finally:
        if kpi_runtime is not None:
            kpi_socket_hold_leave(kpi_runtime)

    actions.append({"cmd": qiopen_cmd, "res": qiopen_res})

    if err_code is None:
        return {
            "ok": False,
            "error": f"No +QIOPEN URC within {budget:g}s (modem may not support socket AT on this port).",
            "host": host_s,
            "port": p,
            "pdp_cid": cid,
            "connect_id": sock_id,
            "connect_setup_ms": connect_ms,
            "qiopen_err": None,
            "qiopen_detail": "timeout waiting for +QIOPEN",
            "actions": actions,
        }

    detail = _qiopen_err_label(err_code)
    ok = int(err_code) == 0
    err_msg = None if ok else detail
    return {
        "ok": ok,
        "error": err_msg,
        "host": host_s,
        "port": p,
        "pdp_cid": cid,
        "connect_id": sock_id,
        "connect_setup_ms": connect_ms if ok else connect_ms,
        "qiopen_err": int(err_code),
        "qiopen_detail": detail,
        "actions": actions,
    }
