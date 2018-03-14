$createdMutex = ""
$mutex = New-Object -TypeName system.threading.mutex($true, "Global\HpcMesos", [ref] $CreatedMutex)
if (!$CreatedMutex) {
    $mutex.WaitOne()
}

$daemonScript = [IO.File]::ReadAllText(".\daemon.ps1")
$bytes = [System.Text.Encoding]::Unicode.GetBytes($daemonScript)
$encodeddaemonScript = [Convert]::ToBase64String($bytes)

$daemon = Start-Process powershell.exe -ArgumentList "-EncodedCommand $encodedCommand" -PassThru

$setupProc = Start-Process "C:\HPCPack2016\private.20180308.251b491.release.debug\release.debug\setup.exe" -ArgumentList "-unattend -computenode:mesoswinagent -sslthumbprint:0386B1198B956BBAAA4154153B6CA1F44B6D1016" -PassThru
$setupProc.WaitForExit()

Add-PSSnapin microsoft.hpc
$hstnm = hostname

$broughtOnline = $false
$retryCount = 0
while (!$broughtOnline -and ($retryCount -lt 120)) {
    try {
        $node = Get-HpcNode -Name $hstnm
        Set-HpcNodeState -Node $node -State online
        $broughtOnline = $true
    }
    catch {
        $_
        Write-Output "Wait for 5 secs and then retry"
        ++$retryCount
        Start-Sleep 5
    }
}

$heartBeatParams = @{"hostname" = hostname} | ConvertTo-Json

while ($true) {
    try {
        # We check daemon is still running first
        if (!$daemon -or $daemon.HasExited) {
            $daemon = Start-Process powershell.exe -ArgumentList "-EncodedCommand $encodedCommand" -PassThru
        }

        Invoke-WebRequest -Method Post http://localhost:8088 -Body $heartBeatParams
    }
    catch {
        $_
    }
    finally {
        start-sleep 60
    }    
}

$mutex.ReleaseMutex()
$mutex.Close()

exit 0

