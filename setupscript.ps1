$setupProc = Start-Process "C:\HPCPack2016\5.1.6086.0\setup.exe" -ArgumentList "-unattend -computenode:mesoswinagent -sslthumbprint:0386B1198B956BBAAA4154153B6CA1F44B6D1016" -PassThru
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
        Write-host "Wait for 5 secs and then retry"
        ++$retryCount
        Start-Sleep 5
    }
}

exit 0