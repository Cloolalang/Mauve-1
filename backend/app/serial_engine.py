from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import serial


FINAL_TOKENS = ("OK", "ERROR")


def _is_final_line(line: str) -> bool:
    up = line.strip().upper()
    if not up:
        return False
    if up in FINAL_TOKENS:
        return True
    if "+CME ERROR" in up or "+CMS ERROR" in up:
        return True
    return False


@dataclass
class CommandRequest:
    command: str
    timeout_sec: float
    created_at: float = field(default_factory=time.time)
    lines: list[str] = field(default_factory=list)
    done: asyncio.Future[dict[str, Any]] | None = None


class SerialEngine:
    def __init__(
        self,
        port: str = "COM49",
        baudrate: int = 115200,
        read_timeout_sec: float = 0.1,
        max_log_lines: int = 500,
    ) -> None:
        self.port = port
        self.baudrate = baudrate
        self.read_timeout_sec = read_timeout_sec
        self._max_log_lines = max_log_lines

        self._serial: serial.Serial | None = None
        self._running = False
        self._last_open_error: str | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._queue: asyncio.Queue[CommandRequest] = asyncio.Queue()
        self._active_request: CommandRequest | None = None
        self._lock = asyncio.Lock()
        self._reopen_lock = asyncio.Lock()
        self._last_reopen_attempt_at = 0.0
        self._reopen_interval_sec = 2.0

        self.rx_log: deque[str] = deque(maxlen=max_log_lines)
        self.urc_log: deque[tuple[float, str]] = deque(maxlen=max_log_lines)
        self.tx_log: deque[str] = deque(maxlen=max_log_lines)
        self.at_trace: deque[str] = deque(maxlen=max_log_lines * 2)

    @property
    def running(self) -> bool:
        return self._running

    async def start(self) -> None:
        async with self._lock:
            if self._running:
                return
            self._running = True
            self._writer_task = asyncio.create_task(self._writer_loop())
            self._reader_task = asyncio.create_task(self._reader_loop())
            try:
                await self._open_serial()
            except Exception as exc:  # noqa: BLE001
                # Keep API alive even if COM port is busy/unavailable.
                self._last_open_error = str(exc)

    async def stop(self) -> None:
        async with self._lock:
            if not self._running:
                return
            self._running = False

            if self._writer_task:
                self._writer_task.cancel()
            if self._reader_task:
                self._reader_task.cancel()

            if self._serial:
                await asyncio.to_thread(self._serial.close)
                self._serial = None
            self._last_open_error = None

    async def reopen(self, port: str, baudrate: int) -> None:
        await self.stop()
        self.port = port
        self.baudrate = baudrate
        await self.start()

    async def send_command(self, command: str, timeout_sec: float = 2.0) -> dict[str, Any]:
        if not self._running:
            raise RuntimeError("Serial engine is not running.")

        req = CommandRequest(command=command.strip(), timeout_sec=max(0.2, timeout_sec))
        req.done = asyncio.get_running_loop().create_future()
        await self._queue.put(req)

        try:
            return await asyncio.wait_for(req.done, timeout=req.timeout_sec + 0.5)
        except asyncio.TimeoutError:
            if self._active_request is req:
                self._active_request = None
            return {
                "ok": False,
                "command": req.command,
                "final": "TIMEOUT",
                "lines": list(req.lines),
                "elapsed_ms": int((time.time() - req.created_at) * 1000),
            }

    async def status(self) -> dict[str, Any]:
        serial_open = bool(self._serial and getattr(self._serial, "is_open", False))
        return {
            "running": self._running,
            "port": self.port,
            "baudrate": self.baudrate,
            "serial_open": serial_open,
            "queue_depth": self._queue.qsize(),
            "active_command": self._active_request.command if self._active_request else None,
            "last_open_error": self._last_open_error,
            "recent_tx": list(self.tx_log)[-30:],
            "recent_rx": list(self.rx_log)[-30:],
            "recent_urc": [{"ts": ts, "line": ln} for ts, ln in list(self.urc_log)[-30:]],
            "recent_at_trace": list(self.at_trace)[-80:],
        }

    async def at_log(self, limit: int = 120) -> dict[str, Any]:
        lim = max(1, min(int(limit), self._max_log_lines * 2))
        return {
            "ok": True,
            "lines": list(self.at_trace)[-lim:],
        }

    async def _open_serial(self) -> None:
        try:
            self._serial = await asyncio.to_thread(
                serial.Serial,
                self.port,
                self.baudrate,
                timeout=self.read_timeout_sec,
            )
            self._last_open_error = None
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to open serial port {self.port}: {exc}") from exc

    async def _try_reopen_serial(self) -> bool:
        # Throttle reconnect attempts to avoid hot-looping when port is absent.
        now = time.monotonic()
        if now - self._last_reopen_attempt_at < self._reopen_interval_sec:
            return False
        self._last_reopen_attempt_at = now
        async with self._reopen_lock:
            if self._serial and getattr(self._serial, "is_open", False):
                return True
            try:
                await self._open_serial()
                self.at_trace.append(f"< URC SERIAL_REOPENED {self.port}@{self.baudrate}")
                return True
            except Exception as exc:  # noqa: BLE001
                self._last_open_error = str(exc)
                return False

    async def _mark_serial_disconnected(self, reason: str) -> None:
        self._last_open_error = reason
        self.at_trace.append(f"< URC SERIAL_DISCONNECTED {reason}")
        if self._serial:
            try:
                await asyncio.to_thread(self._serial.close)
            except Exception:
                pass
        self._serial = None

    async def _writer_loop(self) -> None:
        while self._running:
            req = await self._queue.get()
            if not self._running:
                break
            self._active_request = req
            payload = req.command if req.command.endswith("\r\n") else req.command + "\r\n"
            try:
                while self._running and not self._serial:
                    await self._try_reopen_serial()
                    if not self._serial:
                        await asyncio.sleep(0.25)
                if not self._running:
                    break
                if not self._serial:
                    raise RuntimeError(f"Serial port {self.port} is not open")
                tx = req.command.strip()
                self.tx_log.append(tx)
                self.at_trace.append(f"> {tx}")
                await asyncio.to_thread(self._serial.write, payload.encode("utf-8", errors="replace"))
                await asyncio.to_thread(self._serial.flush)
            except Exception as exc:  # noqa: BLE001
                if req.done and not req.done.done():
                    req.done.set_result(
                        {
                            "ok": False,
                            "command": req.command,
                            "final": f"WRITE_ERROR: {exc}",
                            "lines": list(req.lines),
                            "elapsed_ms": int((time.time() - req.created_at) * 1000),
                        }
                    )
                self._active_request = None

    async def _reader_loop(self) -> None:
        while self._running:
            if not self._serial:
                await self._try_reopen_serial()
                await asyncio.sleep(0.2)
                continue

            try:
                raw = await asyncio.to_thread(self._serial.readline)
            except Exception as exc:
                await self._mark_serial_disconnected(f"read error: {exc}")
                await asyncio.sleep(0.1)
                continue

            if not raw:
                await asyncio.sleep(0.01)
                continue

            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue

            self.rx_log.append(line)
            req = self._active_request
            if req is None:
                self.urc_log.append((time.time(), line))
                self.at_trace.append(f"< URC {line}")
                continue

            req.lines.append(line)
            self.at_trace.append(f"< {line}")
            if _is_final_line(line):
                final = line.strip()
                if req.done and not req.done.done():
                    req.done.set_result(
                        {
                            "ok": final.upper() == "OK",
                            "command": req.command,
                            "final": final,
                            "lines": list(req.lines),
                            "elapsed_ms": int((time.time() - req.created_at) * 1000),
                        }
                    )
                self._active_request = None
