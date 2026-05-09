"""Serial port and raw AT command routes."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from serial.tools import list_ports

from app.models import ReopenBody, SendAtBody
from app.persist import save_last_serial_state
from app.state import engine

router = APIRouter()


@router.get("/api/serial/status")
async def serial_status() -> dict:
    return await engine.status()


@router.get("/api/serial/ports")
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


@router.post("/api/at/send")
async def send_at(body: SendAtBody) -> dict:
    try:
        return await engine.send_command(body.command, timeout_sec=body.timeout_sec)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"AT command failed: {exc}") from exc


@router.get("/api/at/log")
async def at_log(limit: int = 120) -> dict:
    return await engine.at_log(limit=limit)


@router.post("/api/serial/reopen")
async def reopen_serial(body: ReopenBody) -> dict:
    try:
        await engine.reopen(body.port, body.baudrate)
        st = await engine.status()
        if st.get("serial_open"):
            save_last_serial_state(
                str(st.get("port") or body.port),
                int(st.get("baudrate") or body.baudrate),
            )
        return {"ok": True, **st}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to reopen serial: {exc}") from exc
