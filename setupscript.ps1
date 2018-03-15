param([string]$setupPath = "C:\HPCPack2016\setup.exe", [string]$headnode = "", [string]$sslthumbprint = "", [string]$frameworkUri = "localhost")

function Start-Daemon {	
    $script = 'powershell.exe -noexit -EncodedCommand ' + $encodeddaemonScript + ' > daemon.log' 
    $srcipt
    Invoke-WmiMethod -path win32_process -name create -argumentlist $script
}

$createdMutex = ""
$mutex = New-Object -TypeName system.threading.mutex($true, "Global\HpcMesos", [ref] $CreatedMutex)
if (!$CreatedMutex) {
    $mutex.WaitOne()
}

Write-Output "Mutex entered"

$s = "powershell -WindowStyle Hidden -file " + (Split-Path -parent $myinvocation.mycommand.path) + "\daemon.ps1 > C:\deamon.log"
$s
schtasks /create /tn mesoshpcdaemon /tr $s /sc onstart /f
schtasks /run /tn mesoshpcdaemon

$setupPath
$setupProc = Start-Process $setupPath -ArgumentList "-unattend -computenode:$headnode -sslthumbprint:$sslthumbprint" -PassThru
#$setupProc = Start-Process "C:\HPCPack2016\private.20180308.251b491.release.debug\release.debug\setup.exe" -ArgumentList "-unattend -computenode:mesoswinagent -sslthumbprint:0386B1198B956BBAAA4154153B6CA1F44B6D1016" -PassThru
$setupProc.WaitForExit()

Write-Output "Start HPC Services if not already"
# HPC Head node Service
sc.exe start HpcMonitoringServer 
sc.exe start HpcScheduler 
sc.exe start HpcManagement 
sc.exe start HpcSession 
sc.exe start HpcDiagnostics 
sc.exe start HpcReporting 
sc.exe start HpcWebService 
sc.exe start HpcNamingService 
sc.exe start HpcFrontendService 

# HPC Compute node service
sc.exe start HpcMonitoringClient
sc.exe start HpcNodeManager
sc.exe start HpcSoaDiagMon
sc.exe start HpcBroker

# Other HPC service depend on SDM
sc.exe start HpcSdm 

Write-Output "Bring HPC Node online"

<#
$daemonScript = '$hstnm = hostname
$broughtOnline = $false
$retryCount = 0

while (!$broughtOnline -and ($retryCount -lt 120)) {
    try {
		Add-PSSnapin microsoft.hpc
		Write-Output "HPC PSSnapin loaded."        
        Set-HpcNodeState -Name $hstnm -State online
        $broughtOnline = $true
    }
    catch {
        $_
        Write-Output "Wait for 5 secs and then retry"
        ++$retryCount
        Start-Sleep 5
    }
}'
$bytes = [System.Text.Encoding]::Unicode.GetBytes($daemonScript)
$encodeddaemonScript = [Convert]::ToBase64String($bytes)

$bringOnlineProc = Start-Process "Powershell.exe" -ArgumentList ('-noexit -encodedCommand ' + $encodeddaemonScript) -PassThru

$bringOnlineProc.WaitForExit()
#>

$s = "powershell -file " + (Split-Path -parent $myinvocation.mycommand.path) + "\bringOnline.ps1"
#$s = "powershell -noexit -file c:\hpcpack2016\bringOnline.ps1"
$s
schtasks /create /tn mesoshpconline /tr $s /sc onstart /f
schtasks /run /tn mesoshpconline

$bringingOnline = schtasks /query /tn mesoshpconline | findstr Running
while ($bringingOnline) {
    Write-Output "Still bringing node online. Waiting."
    Start-Sleep 10
    $bringingOnline = schtasks /query /tn mesoshpconline | findstr Running
}

$heartBeatParams = @{"hostname" = hostname} | ConvertTo-Json
$url = "http://" + $frameworkUri + ":8088"

while ($true) {
    try {
        # We check daemon is still running first
        # if (!$daemon -or $daemon.HasExited) {
        #     $daemon = Start-Daemon
        #     $daemon
        # }
        $daemonRunning = schtasks /query /tn mesoshpcdaemon | findstr Running
        if (!$daemonRunning) {
            Write-Output "Daemon script not found. Restart."
            schtasks /run /tn mesoshpcdaemon
        }
		
        Write-Output "Send HeartBeat"
        Invoke-WebRequest -Method Post $url -Body $heartBeatParams
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