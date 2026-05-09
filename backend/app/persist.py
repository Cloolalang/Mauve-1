"""Persistence helpers for user-configurable runtime settings (serial port, baud rate).

Stored as JSON files so selections survive application restarts without needing a config UI.
"""
from __future__ import annotations

import json
import os
import sys


def _serial_state_file_path() -> str:
    """Persist last-used serial port. Dev: ``backend/.state``; PyInstaller: ``%LOCALAPPDATA%\\5GModemTestDriver``."""
    if getattr(sys, "frozen", False):
        base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
        d = os.path.join(base, "5GModemTestDriver")
    else:
        d = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".state"))
    return os.path.join(d, "serial_last.json")


def load_last_serial_state() -> dict | None:
    path = _serial_state_file_path()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        return None


def save_last_serial_state(port: str, baudrate: int) -> None:
    path = _serial_state_file_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {"port": str(port), "baudrate": int(baudrate)}
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception:
        # Non-fatal: app continues even if state persistence fails.
        pass
