$hstnm = hostname
$broughtOnline = $false
$retryCount = 0

while (!$broughtOnline) {
    try {
        Add-PSSnapin microsoft.hpc
        Write-Output "HPC PSSnapin loaded."    
        $node = Get-HpcNode -Name $hstnm
        Set-HpcNodeState -Node $node -State online
        $broughtOnline = $true
    }
    catch {
        $broughtOnline = $false
        $_
        Write-Output "Wait for 5 secs and then retry"
        Start-Sleep 5
    }           
}

schtasks /delete /tn mesoshpconline /f