param([string]$setupPath = "C:\HPCPack2016\setup.exe", [string]$headnode = "", [string]$sslthumbprint = "", [string]$frameworkUri = "localhost")

$createdMutex = ""
$mutex = New-Object -TypeName system.threading.mutex($true, "Global\HpcMesos", [ref] $CreatedMutex)
if (!$CreatedMutex) {
    $mutex.WaitOne()
}

Write-Output "Mutex entered"

# Clean up any left over scheduled tasks
schtasks /delete /tn mesoshpcdaemon /f

$s = "cmd /c powershell -WindowStyle Hidden -file " + (Split-Path -parent $myinvocation.mycommand.path) + "\daemon.ps1 > %temp%\hpcmesos_deamon.log"
$s
schtasks /create /tn mesoshpcdaemon /tr $s /sc onstart /f
schtasks /run /tn mesoshpcdaemon

$setupPath
$setupProc = Start-Process $setupPath -ArgumentList "-unattend -computenode:$headnode -sslthumbprint:$sslthumbprint" -PassThru
#$setupProc = Start-Process "C:\HPCPack2016\private.20180308.251b491.release.debug\release.debug\setup.exe" -ArgumentList "-unattend -computenode:mesoswinagent -sslthumbprint:0386B1198B956BBAAA4154153B6CA1F44B6D1016" -PassThru
$setupProc.WaitForExit()

Write-Output "Set CCP_ env vars and registry"
try {
    [Environment]::SetEnvironmentVariable("CCP_CONNECTIONSTRING", $headnode, "Machine")
    [Environment]::SetEnvironmentVariable("CCP_SCHEDULER", $headnode, "Machine")
    Set-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\HPC" -Name "ClusterConnectionString" -Value $headnode
}
catch {
    $_
}

Write-Output "Start HPC Services if not already"
# Other HPC service depend on SDM
sc.exe start HpcSdm 
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

$heartBeatParams = @{"hostname" = hostname} | ConvertTo-Json
$url = "http://" + $frameworkUri + ":8088"

while ($true) {
    try {
        $daemonRunning = schtasks /query /tn mesoshpcdaemon | findstr Running
        if (!$daemonRunning) {
            Write-Output "Daemon script not found. Restart."
            schtasks /run /tn mesoshpcdaemon
        }
		
        Write-Output "Send HeartBeat to"  $url
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