param(
    [int]$Port = 8011,
    [string]$BindHost = "127.0.0.1"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$pythonExe = Join-Path $scriptDir ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    Write-Error "Virtual environment Python not found at '$pythonExe'. Create venv first."
    exit 1
}

try {
    $listeners = Get-NetTCPConnection -LocalAddress $BindHost -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
} catch {
    $listeners = @()
}

if ($listeners) {
    $pids = $listeners | Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($procId in $pids) {
        try {
            $proc = Get-Process -Id $procId -ErrorAction Stop
            Write-Host "Stopping listener on $BindHost`:$Port (PID $procId - $($proc.ProcessName))..."
            Stop-Process -Id $procId -Force
        } catch {
            Write-Warning "Could not stop PID ${procId}: $($_.Exception.Message)"
        }
    }
    Start-Sleep -Milliseconds 400
}

Write-Host "Starting 5G ModemTestDriver backend on http://$BindHost`:$Port ..."
& $pythonExe -m uvicorn app.main:app --host $BindHost --port $Port
