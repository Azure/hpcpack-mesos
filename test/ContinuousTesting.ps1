while ($true) {
    Invoke-Pester $PSScriptRoot -OutputFile "result-$(((get-date).ToUniversalTime()).ToString("yyyyMMddThhmmssZ")).log"
    Start-Sleep 300
}