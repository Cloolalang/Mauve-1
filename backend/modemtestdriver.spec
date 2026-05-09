# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for 5G ModemTestDriver (onedir, console).
# Run from repository ``backend/``: pyinstaller modemtestdriver.spec

import pathlib

from PyInstaller.utils.hooks import collect_submodules

BACKEND = pathlib.Path(SPEC).resolve().parent

hiddenimports: list[str] = [
    "app",
    "app.main",
    "app.kpi_service",
    "app.serial_engine",
    "app.at_modem_errors",
    "app.sim_usim_services",
]

try:
    hiddenimports.extend(collect_submodules("app"))
except Exception:
    pass

for pkg in ("uvicorn", "starlette", "pydantic", "anyio"):
    try:
        hiddenimports.extend(collect_submodules(pkg))
    except Exception:
        pass

# Deduplicate while keeping order
seen: set[str] = set()
_hi: list[str] = []
for m in hiddenimports:
    if m not in seen:
        seen.add(m)
        _hi.append(m)
hiddenimports = _hi

mocn_dir = BACKEND / "app" / "mocn"
mocn_datas = [(str(p), str(pathlib.Path("app") / "mocn")) for p in sorted(mocn_dir.glob("*.json"))]

a = Analysis(
    [str(BACKEND / "run_desktop.py")],
    pathex=[str(BACKEND)],
    binaries=[],
    datas=mocn_datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="5GModemTestDriver",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="5GModemTestDriver",
)
