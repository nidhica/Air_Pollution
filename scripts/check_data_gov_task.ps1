# Read the AEROSCOPE data.gov.in scheduled task. This does not run the pipeline.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$AeroscopeDataGovTaskName = 'AEROSCOPE_DataGov_Hourly'

function Get-AeroscopeNormalizedCode {
    param($Code)
    $text = ''
    if ($null -ne $Code) {
        $text = ([string]$Code).Trim()
    }
    if ($text -notmatch '^-?\d+$') {
        return $null
    }
    $number = [System.Numerics.BigInteger]::Parse($text)
    $width = [System.Numerics.BigInteger]::Pow(2, 32)
    $signedFloor = [System.Numerics.BigInteger]::Parse('-2147483648')
    if ($number -lt 0 -and $number -ge $signedFloor) {
        $number = [System.Numerics.BigInteger]::Add($number, $width)
    }
    if ($number -lt 0) {
        return $null
    }
    return $number
}

function Get-AeroscopeCodeHex {
    param([System.Numerics.BigInteger]$Number)
    if ($Number -eq 0) {
        return '0x00000000'
    }
    $digits = '0123456789ABCDEF'
    $sixteen = [System.Numerics.BigInteger]16
    $chars = New-Object System.Collections.Generic.List[char]
    $value = $Number
    while ($value -gt 0) {
        $remainder = [int]([System.Numerics.BigInteger]::Remainder($value, $sixteen))
        $chars.Add($digits[$remainder])
        $value = [System.Numerics.BigInteger]::Divide($value, $sixteen)
    }
    while ($chars.Count -lt 8) {
        $chars.Add('0')
    }
    $hex = '0x'
    for ($index = $chars.Count - 1; $index -ge 0; $index--) {
        $hex += $chars[$index]
    }
    return $hex
}

function Format-AeroscopeTaskResult {
    param($Code)
    try {
        $number = Get-AeroscopeNormalizedCode -Code $Code
        if ($null -eq $number) {
            if ($null -eq $Code -or [string]::IsNullOrWhiteSpace([string]$Code)) {
                return 'not available'
            }
            return ("{0} (unrecognized)" -f ([string]$Code))
        }
        $decimalText = $number.ToString()
        $hexText = Get-AeroscopeCodeHex -Number $number
        switch ($decimalText) {
            '0' { return '0 (success)' }
            '267009' { return '267009 (currently running)' }
            '267011' { return '267011 (has not run yet)' }
            '3221225786' { return "3221225786 (interrupted or terminated) $hexText" }
            default { return "$decimalText $hexText" }
        }
    } catch {
        return 'not available (the result could not be read)'
    }
}

function Show-AeroscopeDataGovTask {
    $task = Get-ScheduledTask -TaskName $AeroscopeDataGovTaskName -ErrorAction SilentlyContinue
    if ($null -eq $task) {
        Write-Host "Task: $AeroscopeDataGovTaskName"
        Write-Host 'Exists: no'
        Write-Host 'Enabled: no'
        Write-Host 'Last run time: not available'
        Write-Host 'Last run result: not available'
        Write-Host 'Next run: not available'
        return 1
    }
    $info = Get-ScheduledTaskInfo -TaskName $AeroscopeDataGovTaskName
    $enabled = $task.Settings.Enabled
    $lastRun = $info.LastRunTime
    if ($lastRun.Year -lt 2000) {
        $lastRunText = 'has not run'
    } else {
        $lastRunText = $lastRun.ToString('yyyy-MM-dd HH:mm:ss')
    }
    Write-Host "Task: $AeroscopeDataGovTaskName"
    Write-Host 'Exists: yes'
    Write-Host "Enabled: $enabled"
    Write-Host "State: $($task.State)"
    Write-Host "Last run time: $lastRunText"
    $resultText = 'not available (the result could not be read)'
    try {
        $resultText = Format-AeroscopeTaskResult -Code $info.LastTaskResult
    } catch {
        $resultText = 'not available (the result could not be read)'
    }
    Write-Host "Last run result: $resultText"
    Write-Host "Next run: $($info.NextRunTime.ToString('yyyy-MM-dd HH:mm:ss'))"
    return 0
}

if ($MyInvocation.InvocationName -ne '.') {
    $code = Show-AeroscopeDataGovTask
    exit $code
}
