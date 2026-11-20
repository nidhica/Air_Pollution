# Register the hourly AEROSCOPE data.gov.in task for the current user.
# Running this file creates the task. Tests dot-source it and do not register it.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$AeroscopeDataGovTaskName = 'AEROSCOPE_DataGov_Hourly'

function Get-AeroscopeNextHour {
    $now = Get-Date
    return $now.Date.AddHours($now.Hour + 1)
}

function Install-AeroscopeDataGovTask {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [scriptblock]$Registrar = $null
    )

    $root = (Resolve-Path -LiteralPath $ProjectRoot).Path
    $runner = Join-Path $root 'scripts\run_data_gov_pipeline.ps1'
    if (-not (Test-Path -LiteralPath $runner)) {
        Write-Host "The pipeline script was not found: $runner"
        return 1
    }
    $powershell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    # Do not pass -WindowStyle Hidden. Task Scheduler can deliver a console
    # close to that hidden host and record 0xC000013A before the script runs.
    $argument = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$runner`""
    $start = Get-AeroscopeNextHour
    $action = New-ScheduledTaskAction -Execute $powershell -Argument $argument -WorkingDirectory $root
    $trigger = New-ScheduledTaskTrigger -Once -At $start -RepetitionInterval (New-TimeSpan -Hours 1) -RepetitionDuration (New-TimeSpan -Days 900)
    $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 50) -RunOnlyIfNetworkAvailable
    $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
    $description = 'Hourly AEROSCOPE data.gov.in fetch and Spark preparation. The API key stays in the project .env file.'
    # Tests pass -Registrar. A normal run registers the task for real.
    if ($null -eq $Registrar) {
        Register-ScheduledTask -TaskName $AeroscopeDataGovTaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description $description -Force | Out-Null
    } else {
        & $Registrar -TaskName $AeroscopeDataGovTaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description $description -Force | Out-Null
    }
    Write-Host "Scheduled task $AeroscopeDataGovTaskName is registered."
    Write-Host "First run is the next local hour: $($start.ToString('yyyy-MM-dd HH:mm'))"
    Write-Host 'After that, it repeats every hour.'
    Write-Host 'It runs as the current Windows user while that user is logged on to Windows.'
    Write-Host 'Streamlit does not need to be open.'
    Write-Host 'It does not run after Windows sign-out, because Task Scheduler is not given a password.'
    Write-Host 'The API key is not stored in the task.'
    try {
        $info = Get-ScheduledTaskInfo -TaskName $AeroscopeDataGovTaskName -ErrorAction Stop
        Write-Host "Next scheduled run: $($info.NextRunTime)"
    } catch {
        Write-Host "Next scheduled run: $($start.ToString('yyyy-MM-dd HH:mm'))"
    }
    return 0
}

if ($MyInvocation.InvocationName -ne '.') {
    $scriptDir = Split-Path -Parent $PSCommandPath
    $projectRoot = Split-Path -Parent $scriptDir
    $code = Install-AeroscopeDataGovTask -ProjectRoot $projectRoot
    exit $code
}
