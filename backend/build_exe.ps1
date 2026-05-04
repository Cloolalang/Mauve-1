param(
    [switch]$Clean
)

$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

$venvPy = Join-Path $here ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPy)) {
    Write-Error "Python venv not found at $venvPy. From backend: py -3.12 -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt -r requirements-build.txt"
    exit 1
}

Write-Host "Installing build deps (PyInstaller) if needed..."
& $venvPy -m pip install -q -r requirements-build.txt

if ($Clean) {
    $d1 = Join-Path $here "build"
    $d2 = Join-Path $here "dist"
    if (Test-Path $d1) { Remove-Item -Recurse -Force $d1 }
    if (Test-Path $d2) { Remove-Item -Recurse -Force $d2 }
    Write-Host "Removed build/ and dist/"
}

Write-Host "Running PyInstaller (onedir)..."
& $venvPy -m PyInstaller --noconfirm modemtestdriver.spec

$outDir = Join-Path $here "dist\5GModemTestDriver"
$exe = Join-Path $outDir "5GModemTestDriver.exe"
if (Test-Path $exe) {
    Write-Host ""
    Write-Host "OK: $exe"
    Write-Host "Run: .\dist\5GModemTestDriver\5GModemTestDriver.exe"
    Write-Host "Then open: http://127.0.0.1:8011/"
} else {
    Write-Warning "Expected exe missing: $exe"
    exit 1
}
