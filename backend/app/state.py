"""Application-level singletons shared across route handlers and background tasks.

Importing this module has no side effects other than initialising the serial engine
and KPI runtime objects — no event loop, no I/O.  ``engine.start()`` and the KPI
poll task are launched in the FastAPI lifespan inside ``main.py``.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any

from fastapi import WebSocket

from app.kpi_service import KpiRuntime
from app.persist import load_last_serial_state
from app.serial_engine import SerialEngine

_last_serial = load_last_serial_state() or {}
DEFAULT_PORT: str = os.getenv("MD_SERIAL_PORT", str(_last_serial.get("port") or "COM49"))
DEFAULT_BAUD: int = int(os.getenv("MD_BAUDRATE", str(_last_serial.get("baudrate") or "115200")))

engine: SerialEngine = SerialEngine(port=DEFAULT_PORT, baudrate=DEFAULT_BAUD)
kpi_runtime: KpiRuntime = KpiRuntime(poll_hz=2.0)

# Background task handles (rebound during lifespan).
kpi_task: asyncio.Task[None] | None = None
ws_push_task: asyncio.Task[None] | None = None
lock_guard_task: asyncio.Task[None] | None = None
host_auto_answer_task: asyncio.Task[None] | None = None

# WebSocket broadcast subscribers.
ws_clients: list[WebSocket] = []

# Host auto-answer state.
host_aa_rings: int = 2
host_aa_status_lock: asyncio.Lock = asyncio.Lock()
host_aa_status: dict[str, Any] = {"ring_urcs": 0, "note": ""}

# Single-instance lock file handle.
instance_lock_file: Any = None

# Band / RAT lock guardian.
desired_locks: dict[str, str] = {}
desired_locks_lock: asyncio.Lock = asyncio.Lock()
lock_guard_paused: bool = False

# Modem exclusive-access (e.g. factory-reset, COPS scan).
modem_exclusive_lock: asyncio.Lock = asyncio.Lock()

# Active test run session.
test_run_session_lock: asyncio.Lock = asyncio.Lock()
test_run_session: dict[str, Any] | None = None

# Shared AT+CLCC result cache (voice call status + host auto-answer share one serial slot).
voice_clcc_cache_ts: float = 0.0
voice_clcc_rows: list[dict[str, Any]] | None = None
voice_clcc_res_ok: bool = False
voice_clcc_data_lock: asyncio.Lock = asyncio.Lock()
voice_clcc_fetch_lock: asyncio.Lock = asyncio.Lock()

VOICE_CLCC_CACHE_TTL_SEC: float = 0.7
VOICE_CLCC_TIMEOUT_SEC: float = 2.5
HOST_AUTO_ANSWER_POLL_SEC: float = 0.85
VOICE_STATUS_POLL_MS: int = 1700
