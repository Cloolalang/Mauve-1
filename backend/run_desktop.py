"""
Desktop / frozen entry point: run Uvicorn without requiring ``python -m uvicorn``.

Build (from ``backend/``): see ``build_exe.ps1`` or ``pyinstaller modemtestdriver.spec``.
"""
from __future__ import annotations

import argparse
import os
import sys


def _maybe_chdir_to_bundle() -> None:
    if getattr(sys, "frozen", False):
        exe_dir = os.path.dirname(os.path.abspath(sys.executable))
        try:
            os.chdir(exe_dir)
        except OSError:
            pass


def main() -> None:
    _maybe_chdir_to_bundle()
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if isinstance(meipass, str) and meipass and meipass not in sys.path:
            sys.path.insert(0, meipass)

    parser = argparse.ArgumentParser(description="5G ModemTestDriver — local HTTP server")
    parser.add_argument(
        "--host",
        default=os.environ.get("MODEM_DRIVER_BIND", "127.0.0.1"),
        help="Bind address (default 127.0.0.1 or MODEM_DRIVER_BIND)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("MODEM_DRIVER_PORT", "8011")),
        help="Port (default 8011 or MODEM_DRIVER_PORT)",
    )
    args = parser.parse_args()

    # Load ASGI app by object — uvicorn's string import ("app.main:app") often fails in PyInstaller bundles.
    from app.main import app as asgi_app

    import uvicorn

    uvicorn.run(
        asgi_app,
        host=args.host,
        port=args.port,
        log_level="info",
        access_log=False,
        use_colors=False,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise
    except SystemExit:
        raise
    except Exception:
        import traceback

        traceback.print_exc()
        if os.name == "nt":
            input("Press Enter to exit...")
        raise
