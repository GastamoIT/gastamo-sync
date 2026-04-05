# ================================
# SOCi Review Summary Rebuild
# Calls Supabase Edge Function
# ================================

$supabaseUrl = $env:SUPABASE_URL
$supabaseKey = $env:SUPABASE_KEY

Write-Host "Triggering summary rebuild..."

try {
    $response = Invoke-RestMethod "$supabaseUrl/functions/v1/rebuild-review-summary" `
        -Method POST `
        -Headers @{
            "apikey"        = $supabaseKey
            "Authorization" = "Bearer $supabaseKey"
            "Content-Type"  = "application/json"
        } `
        -Body "{}"

    Write-Host "Status: $($response.status)"
    Write-Host ""
    Write-Host "================================"
    Write-Host "Summary rebuild complete"
    Write-Host "================================"
    
    $response.summary.PSObject.Properties | ForEach-Object {
        Write-Host "$($_.Name): $($_.Value.reviews) reviews across $($_.Value.count) date/location combos"
    }
}
catch {
    Write-Host "ERROR: $_"
    exit 1
}
