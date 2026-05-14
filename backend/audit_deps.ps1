param(
    [switch]$IncludeBuildDeps
)

$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

$venvPy = Join-Path $here ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPy)) {
    Write-Error "Python venv not found at $venvPy. From backend: py -3.12 -m venv .venv; .\.venv\Scripts\python.exe -m pip install -r requirements.txt"
    exit 1
}

if ($IncludeBuildDeps) {
    Write-Host "Syncing optional build requirements into venv..."
    & $venvPy -m pip install -q -r requirements-build.txt
}

Write-Host "Installing pip-audit (from requirements-audit.txt)..."
& $venvPy -m pip install -q -r requirements-audit.txt

Write-Host "Running pip-audit (OSV) on this venv..."
& $venvPy -m pip_audit
exit $LASTEXITCODE
