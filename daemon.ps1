$createdMutex = ""
$mutex = New-Object -TypeName system.threading.mutex($true, "Global\HpcMesos", [ref] $CreatedMutex)
if (!$CreatedMutex) {
    $mutex.WaitOne()
}

write-output "Mutex entered. Stopping HPC Services"
    
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
	
$mutex.ReleaseMutex()
$mutex.Close()