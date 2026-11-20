# Hourly data.gov.in fetch and Spark preparation.
# The project root is the parent of this script's directory.
# This file does not delete snapshots or normalized history.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Protect-DataGovLogText {
    param([AllowEmptyString()][string]$Text)
    if ([string]::IsNullOrEmpty($Text)) {
        return ''
    }
    $Text = [regex]::Replace($Text, '(?i)(api-key=)[^&\s]+', '${1}[REDACTED]')
    $Text = [regex]::Replace($Text, '(?i)(api_key=)[^&\s]+', '${1}[REDACTED]')
    $Text = [regex]::Replace($Text, '(?i)(DATA_GOV_IN_API_KEY\s*=\s*)\S+', '${1}[REDACTED]')
    return $Text
}

function Write-DataGovLog {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Message
    )
    $stamp = Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK'
    $line = Protect-DataGovLogText "$stamp $Message"
    Add-Content -LiteralPath $Path -Value $line -Encoding UTF8
}

function Test-DataGovApiKeyConfigured {
    param([Parameter(Mandatory = $true)][string]$ProjectRoot)
    $fromEnv = [Environment]::GetEnvironmentVariable('DATA_GOV_IN_API_KEY')
    if (-not [string]::IsNullOrWhiteSpace($fromEnv)) {
        return $true
    }
    $envFile = Join-Path $ProjectRoot '.env'
    if (-not (Test-Path -LiteralPath $envFile)) {
        return $false
    }
    foreach ($line in [System.IO.File]::ReadAllLines($envFile)) {
        $trimmed = $line.Trim()
        if ($trimmed.Length -eq 0 -or $trimmed.StartsWith('#')) {
            continue
        }
        if ($trimmed -match '^(?i)DATA_GOV_IN_API_KEY\s*=\s*(.+)$') {
            $raw = $Matches[1].Trim()
            if ($raw.Length -ge 2) {
                $quoted = ($raw.StartsWith('"') -and $raw.EndsWith('"')) -or ($raw.StartsWith("'") -and $raw.EndsWith("'"))
                if ($quoted) {
                    $raw = $raw.Substring(1, $raw.Length - 2).Trim()
                }
            }
            $configured = -not [string]::IsNullOrWhiteSpace($raw)
            Remove-Variable raw
            Remove-Variable Matches -ErrorAction SilentlyContinue
            return $configured
        }
    }
    return $false
}

function Get-DataGovMutexName {
    param([Parameter(Mandatory = $true)][string]$ProjectRoot)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = $sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($ProjectRoot.Trim().ToLowerInvariant()))
    } finally {
        $sha.Dispose()
    }
    $hex = -join ($bytes | ForEach-Object { $_.ToString('x2') })
    return "AEROSCOPE_DataGov_$($hex.Substring(0, 16))"
}

function New-DataGovMutex {
    param([Parameter(Mandatory = $true)][string]$Name)
    try {
        return New-Object System.Threading.Mutex($false, "Global\$Name")
    } catch {
        return New-Object System.Threading.Mutex($false, "Local\$Name")
    }
}

function Remove-OldDataGovLogs {
    param([Parameter(Mandatory = $true)][string]$LogDir)
    $cutoff = (Get-Date).AddDays(-14)
    $files = @(Get-ChildItem -LiteralPath $LogDir -Filter 'pipeline-*.log' -File -ErrorAction SilentlyContinue)
    foreach ($file in $files) {
        if ($file.LastWriteTime -lt $cutoff) {
            Remove-Item -LiteralPath $file.FullName -Force
        }
    }
    $remaining = @(Get-ChildItem -LiteralPath $LogDir -Filter 'pipeline-*.log' -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending)
    if ($remaining.Count -gt 400) {
        foreach ($file in $remaining | Select-Object -Skip 400) {
            Remove-Item -LiteralPath $file.FullName -Force
        }
    }
    $stale = (Get-Date).AddMinutes(-10)
    foreach ($pattern in @('stdout-*.tmp', 'stderr-*.tmp')) {
        foreach ($file in @(Get-ChildItem -LiteralPath $LogDir -Filter $pattern -File -ErrorAction SilentlyContinue)) {
            if ($file.LastWriteTime -lt $stale) {
                Remove-Item -LiteralPath $file.FullName -Force -ErrorAction SilentlyContinue
            }
        }
    }
}

function Read-DataGovCapturedText {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return ''
    }
    $text = ''
    for ($attempt = 1; $attempt -le 40; $attempt++) {
        try {
            $stream = [System.IO.File]::Open(
                $Path,
                [System.IO.FileMode]::Open,
                [System.IO.FileAccess]::Read,
                [System.IO.FileShare]::ReadWrite
            )
            try {
                $reader = New-Object System.IO.StreamReader($stream)
                $text = $reader.ReadToEnd()
                $reader.Dispose()
            } finally {
                $stream.Dispose()
            }
            break
        } catch {
            Start-Sleep -Milliseconds 250
        }
    }
    try {
        Remove-Item -LiteralPath $Path -Force -ErrorAction Stop
    } catch {
        # A shutdown helper may still hold the file. The text is already copied.
    }
    return $text
}

function Invoke-DataGovPython {
    param(
        [Parameter(Mandatory = $true)][string]$Python,
        [Parameter(Mandatory = $true)][string]$Main,
        [Parameter(Mandatory = $true)][string]$CommandName,
        [Parameter(Mandatory = $true)][string]$LogPath
    )
    $env:PYTHONUNBUFFERED = '1'
    $env:PYTHONIOENCODING = 'utf-8'
    # Native stderr must not flow through 2>&1 while ErrorAction is Stop.
    # Spark writes warnings there, and Windows PowerShell turns the first
    # warning into a terminating error and abandons the process.
    $stdoutPath = Join-Path (Split-Path -Parent $LogPath) ('stdout-' + [guid]::NewGuid().ToString('N') + '.tmp')
    $stderrPath = Join-Path (Split-Path -Parent $LogPath) ('stderr-' + [guid]::NewGuid().ToString('N') + '.tmp')
    Write-DataGovLog $LogPath "Python process starting: $CommandName"
    $workDir = Split-Path -Parent $Main
    # Start-Process splits a path at the space in this Windows account name
    # before the virtual-environment launcher sees it. cmd.exe keeps each
    # quoted path as one argument and returns the Python exit code.
    $quotedPython = '"' + ($Python -replace '"', '""') + '"'
    $quotedMain = '"' + ($Main -replace '"', '""') + '"'
    $quotedOut = '"' + ($stdoutPath -replace '"', '""') + '"'
    $quotedErr = '"' + ($stderrPath -replace '"', '""') + '"'
    $commandLine = "$quotedPython $quotedMain $CommandName 1>$quotedOut 2>$quotedErr"
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $previousLocation = Get-Location
    $code = $null
    try {
        Set-Location -LiteralPath $workDir
        & cmd.exe /d /c $commandLine
        if ($null -ne $LASTEXITCODE) {
            $code = $LASTEXITCODE
        }
    } finally {
        Set-Location -LiteralPath $previousLocation.Path
        $ErrorActionPreference = $previousPreference
    }
    if ($null -eq $code) {
        $code = 1
    }
    # Spark's Windows shutdown can keep the redirected files open for a
    # moment after Python has already exited. The exit code is already known.
    $output = Read-DataGovCapturedText $stdoutPath
    $errorText = Read-DataGovCapturedText $stderrPath
    if (-not [string]::IsNullOrWhiteSpace($errorText)) {
        $output = $output.TrimEnd() + "`r`n" + $errorText.TrimEnd()
    }
    Write-DataGovLog $LogPath "Python process finished: $CommandName exit $code"
    $protected = Protect-DataGovLogText $output
    if ($protected.Length -gt 200000) {
        $protected = $protected.Substring(0, 200000) + "`r`n... log output truncated ..."
    }
    if (-not [string]::IsNullOrWhiteSpace($protected)) {
        Add-Content -LiteralPath $LogPath -Value $protected.TrimEnd() -Encoding UTF8
    }
    return [int]$code
}

function Write-DataGovStatus {
    param(
        [Parameter(Mandatory = $true)][string]$LogDir,
        [Parameter(Mandatory = $true)][bool]$Success,
        [bool]$FetchRan = $false,
        [bool]$PrepareRan = $false,
        [object]$FetchExitCode = $null,
        [object]$PrepareExitCode = $null,
        [Parameter(Mandatory = $true)][string]$Message
    )
    $statusPath = Join-Path $LogDir 'status.json'
    $previousSuccess = $null
    if (Test-Path -LiteralPath $statusPath) {
        try {
            $previous = Get-Content -LiteralPath $statusPath -Raw -Encoding UTF8 | ConvertFrom-Json
            if ($previous.PSObject.Properties.Name -contains 'last_success_at') {
                $previousSuccess = $previous.last_success_at
            }
        } catch {
            $previousSuccess = $null
        }
    }
    $finished = Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK'
    $lastSuccess = $previousSuccess
    if ($Success) {
        $lastSuccess = $finished
    }
    $payload = [ordered]@{
        finished_at = $finished
        success = $Success
        fetch_ran = $FetchRan
        prepare_ran = $PrepareRan
        fetch_exit_code = $FetchExitCode
        prepare_exit_code = $PrepareExitCode
        last_success_at = $lastSuccess
        message = (Protect-DataGovLogText $Message)
    }
    $json = $payload | ConvertTo-Json -Depth 3
    [System.IO.File]::WriteAllText($statusPath, $json + "`r`n")
}

function Invoke-AeroscopeDataGovPipeline {
    param([Parameter(Mandatory = $true)][string]$ProjectRoot)

    $root = (Resolve-Path -LiteralPath $ProjectRoot).Path
    $logDir = Join-Path $root 'logs\data_gov_automation'
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    Remove-OldDataGovLogs -LogDir $logDir
    $logPath = Join-Path $logDir ('pipeline-' + (Get-Date -Format 'yyyyMMddTHHmmss') + '.log')
    New-Item -ItemType File -Path $logPath -Force | Out-Null
    Write-DataGovLog $logPath "Runner process $PID started. This line is written before API key and Python checks. Project root: $root"

    $python = Join-Path $root '.venv\Scripts\python.exe'
    if (-not (Test-Path -LiteralPath $python)) {
        $message = 'The virtual environment interpreter was not found at .venv\Scripts\python.exe.'
        Write-DataGovLog $logPath $message
        Write-DataGovStatus -LogDir $logDir -Success $false -Message $message
        return 3
    }
    if (-not (Test-DataGovApiKeyConfigured -ProjectRoot $root)) {
        $message = 'DATA_GOV_IN_API_KEY is not set in the environment or in .env.'
        Write-DataGovLog $logPath $message
        Write-DataGovStatus -LogDir $logDir -Success $false -Message $message
        return 4
    }
    Write-DataGovLog $logPath 'API key configuration is available. The key is not written to this log.'

    $mutex = $null
    $acquired = $false
    try {
        $mutex = New-DataGovMutex -Name (Get-DataGovMutexName -ProjectRoot $root)
        try {
            $acquired = $mutex.WaitOne(0)
        } catch [System.Threading.AbandonedMutexException] {
            $acquired = $true
        }
    } catch {
        $message = 'A pipeline lock could not be created. This run was not started.'
        Write-DataGovLog $logPath $message
        return 5
    }
    if (-not $acquired) {
        Write-DataGovLog $logPath 'A pipeline run is already active. This run was not started.'
        if ($null -ne $mutex) {
            $mutex.Dispose()
        }
        return 0
    }

    $lockPath = Join-Path $logDir 'pipeline.lock'
    Set-Content -LiteralPath $lockPath -Value "$PID" -Encoding ASCII
    $fetchRan = $false
    $prepareRan = $false
    $fetchCode = $null
    $prepareCode = $null
    $message = 'completed'
    $exitCode = 0
    try {
        Set-Location -LiteralPath $root
        $main = Join-Path $root 'main.py'
        Write-DataGovLog $logPath 'Starting fetch-data-gov.'
        $fetchRan = $true
        $fetchCode = Invoke-DataGovPython -Python $python -Main $main -CommandName 'fetch-data-gov' -LogPath $logPath
        Write-DataGovLog $logPath "fetch-data-gov exit $fetchCode"
        if ($fetchCode -ne 0) {
            $message = 'fetch-data-gov failed. prepare-data-gov was not started.'
            $exitCode = $fetchCode
        } else {
            Write-DataGovLog $logPath 'Starting prepare-data-gov.'
            $prepareRan = $true
            $prepareCode = Invoke-DataGovPython -Python $python -Main $main -CommandName 'prepare-data-gov' -LogPath $logPath
            Write-DataGovLog $logPath "prepare-data-gov exit $prepareCode"
            if ($prepareCode -ne 0) {
                $message = 'prepare-data-gov failed.'
                $exitCode = $prepareCode
            }
        }
    } catch {
        $detail = [string]$_.Exception.Message
        if ($detail.Length -gt 500) {
            $detail = $detail.Substring(0, 500)
        }
        $message = "The pipeline stopped because of an unexpected error. $detail"
        $exitCode = 1
        Write-DataGovLog $logPath $message
    } finally {
        Write-DataGovStatus -LogDir $logDir -Success ($exitCode -eq 0) -FetchRan $fetchRan -PrepareRan $prepareRan -FetchExitCode $fetchCode -PrepareExitCode $prepareCode -Message $message
        if (Test-Path -LiteralPath $lockPath) {
            Remove-Item -LiteralPath $lockPath -Force
        }
        if ($acquired -and $null -ne $mutex) {
            try {
                $mutex.ReleaseMutex()
            } catch {
                # The mutex may already be abandoned if the process is shutting down.
            }
            $mutex.Dispose()
        }
    }
    return $exitCode
}

if ($MyInvocation.InvocationName -ne '.') {
    $scriptDir = Split-Path -Parent $PSCommandPath
    $projectRoot = Split-Path -Parent $scriptDir
    $code = Invoke-AeroscopeDataGovPipeline -ProjectRoot $projectRoot
    exit $code
}
