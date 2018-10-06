$hstnm = hostname
$broughtOnline = $false
$nodeReachable = $false
$retryCount = 180 # retry 15 minutes

while (!$broughtOnline -and $retryCount -gt 0) {
    try {
        Add-PSSnapin microsoft.hpc
        Write-Output "HPC PSSnapin loaded."    
        $node = Get-HpcNode -Name $hstnm
        Set-HpcNodeState -Node $node -State online
        $broughtOnline = $true
    }
    catch {
        $broughtOnline = $false
        $retryCount--
        $_
        Write-Output "Wait for 5 secs and then retry"
        Start-Sleep 5
    }           
}

Write-Output "Brought node online. Waiting for node being reachable."

$retryCount = 180
$okState = [Microsoft.ComputeCluster.CCPPSH.NodeHealthState]::OK
while ($broughtOnline -and !$nodeReachable -and $retryCount -gt 0) {
    try {
        $node = Get-HpcNode -Name $hstnm
        $nodeState = $node.HealthState
        if ($nodeState -eq $okState) {
            $nodeReachable = $true
        }
        else {
            Start-Sleep 5
        }
    }
    catch {
        $nodeReachable = $false
        $retryCount--
        $_
        Write-Output "Wait for 5 secs and then retry"
        Start-Sleep 5
    }
}


schtasks /delete /tn mesoshpconline /f
