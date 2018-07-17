Add-PSSnapin Microsoft.Hpc
$scriptRoot = $PSScriptRoot

Describe "HPC Mesos Integration Test" {
    Context "Basic 1 node Grow Shrink" {
        It "checks no nodes online at the begining" {
            $retries = 180
            while ($retries -gt 0) {
                $nodes = Get-HpcNode -State Online -ErrorAction SilentlyContinue
                $ans = $nodes.Count
                if ($ans -eq 0) {
                    break
                }
                $retries--
                Start-Sleep 10
            }
            $ans | Should -Be 0
        }

        It "submits a new 1 core job and waits for it to success"   {
            $1CoreJobXml = Join-Path $scriptRoot "JobXml\1Core.Xml"
            $job = New-HpcJob -JobFile $1CoreJobXml
            Submit-HpcJob -Job $job
            $retries = 180
            while ($retries -gt 0) {
                $job.Refresh()
                $ans = $job.State -eq "Finished"
                if ($ans) {
                    break
                }
                $retries--
                Start-Sleep 10
            }
            $ans | Should -BeTrue
        }

        It "checks if over grown" {
            $nodes = Get-HpcNode -State Online -ErrorAction SilentlyContinue
            $nodes.Count | Should -Be 1
        }

        It "checks nodes get shrunk in the end" {
            $retries = 180
            while ($retries -gt 0) {
                $nodes = Get-HpcNode -State Online -ErrorAction SilentlyContinue
                $ans = $nodes.Count
                if ($ans -eq 0) {
                    break
                }
                $retries--
                Start-Sleep 10
            }
            $ans | Should -Be 0
        }
    }

    Context "Basic 2 nodes Grow Shrink" {
        It "checks no nodes online at the begining" {
            $retries = 180
            while ($retries -gt 0) {
                $nodes = Get-HpcNode -State Online -ErrorAction SilentlyContinue
                $ans = $nodes.Count
                if ($ans -eq 0) {
                    break
                }
                $retries--
                Start-Sleep 10
            }
            $ans | Should -Be 0
        }

        It "submits a new 9 cores job and waits for that to success"   {
            $1CoreJobXml = Join-Path $scriptRoot "JobXml\9Cores.Xml"
            $job = New-HpcJob -JobFile $1CoreJobXml
            Submit-HpcJob -Job $job
            $retries = 180
            while ($retries -gt 0) {
                $job.Refresh()
                $ans = $job.State -eq "Finished"
                if ($ans) {
                    break
                }
                $retries--
                Start-Sleep 10
            }
            $ans | Should -BeTrue
        }

        It "checks if over grown" {
            $nodes = Get-HpcNode -State Online -ErrorAction SilentlyContinue
            $nodes.Count | Should -Be 2
        }

        It "checks nodes get shrunk in the end" {
            $retries = 180
            while ($retries -gt 0) {
                $nodes = Get-HpcNode -State Online -ErrorAction SilentlyContinue
                $ans = $nodes.Count
                if ($ans -eq 0) {
                    break
                }
                $retries--
                Start-Sleep 10
            }
            $ans | Should -Be 0
        }
    }

    Context "1 node required node group Grow Shrink" {
        It "checks no nodes online at the begining" {
            $retries = 180
            while ($retries -gt 0) {
                $nodes = Get-HpcNode -State Online -ErrorAction SilentlyContinue
                $ans = $nodes.Count
                if ($ans -eq 0) {
                    break
                }
                $retries--
                Start-Sleep 10
            }
            $ans | Should -Be 0
        }

        It "submits a new 1 core job and waits for it to success"   {
            $1CoreJobXml = Join-Path $scriptRoot "JobXml\NeedNg1.Xml"
            $job = New-HpcJob -JobFile $1CoreJobXml
            Submit-HpcJob -Job $job
            $retries = 180
            while ($retries -gt 0) {
                $job.Refresh()
                $ans = $job.State -eq "Finished"
                if ($ans) {
                    break
                }
                $retries--
                Start-Sleep 10
            }
            $ans | Should -BeTrue
        }

        It "checks if over grown" {
            $nodes = Get-HpcNode -State Online -ErrorAction SilentlyContinue
            $nodes.Count | Should -Be 1
        }

        It "checks nodes get shrunk in the end" {
            $retries = 180
            while ($retries -gt 0) {
                $nodes = Get-HpcNode -State Online -ErrorAction SilentlyContinue
                $ans = $nodes.Count
                if ($ans -eq 0) {
                    break
                }
                $retries--
                Start-Sleep 10
            }
            $ans | Should -Be 0
        }
    }
}



