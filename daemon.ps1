$createdMutex = ""
$mutex = New-Object -TypeName system.threading.mutex($true, "Global\HpcMesos", [ref] $CreatedMutex)
if (!$CreatedMutex) {
    $mutex.WaitOne()
}

write-output "Mutex entered. Deleting HPC Node"

try {
    Add-PSSnapin microsoft.hpc
    $hstnm = hostname
    $node = Get-HpcNode -Name $hstnm
    Set-HpcNodeState -Node $node -State offline
    Remove-HpcNode -Node $node        
}
catch {
    $_        
}

write-output "Stopping HPC Service"
    
# HPC Head node Service
sc.exe stop HpcMonitoringServer 
sc.exe stop HpcScheduler 
sc.exe stop HpcManagement 
sc.exe stop HpcSession 
sc.exe stop HpcDiagnostics 
sc.exe stop HpcReporting 
sc.exe stop HpcWebService 
sc.exe stop HpcNamingService 
sc.exe stop HpcFrontendService 

# HPC Compute node service
sc.exe stop HpcMonitoringClient
sc.exe stop HpcNodeManager
sc.exe stop HpcSoaDiagMon
sc.exe stop HpcBroker

# Other HPC service depend on SDM
sc.exe stop HpcSdm 

schtasks /delete /tn mesoshpcdaemon /f

$mutex.ReleaseMutex()
$mutex.Close()