# Remove only the AEROSCOPE data.gov.in scheduled task.
# This file does not delete snapshots, normalized data, logs, or any other task.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$AeroscopeDataGovTaskName = 'AEROSCOPE_DataGov_Hourly'

function Uninstall-AeroscopeDataGovTask {
    $task = Get-ScheduledTask -TaskName $AeroscopeDataGovTaskName -ErrorAction SilentlyContinue
    if ($null -eq $task) {
        Write-Host "$AeroscopeDataGovTaskName is not registered."
        return 0
    }
    Unregister-ScheduledTask -TaskName $AeroscopeDataGovTaskName -Confirm:$false
    Write-Host "Removed scheduled task $AeroscopeDataGovTaskName."
    Write-Host 'Project data, raw snapshots, and logs were not deleted.'
    return 0
}

if ($MyInvocation.InvocationName -ne '.') {
    $code = Uninstall-AeroscopeDataGovTask
    exit $code
}
