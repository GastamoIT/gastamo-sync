# ================================
# SOCi Review Summary Rebuild
# ================================

$supabaseUrl = $env:SUPABASE_URL
$supabaseKey = $env:SUPABASE_KEY

$headers = @{
    "apikey"        = $supabaseKey
    "Authorization" = "Bearer $supabaseKey"
    "Content-Type"  = "application/json"
    "Prefer"        = "return=minimal"
}

Write-Host "Truncating soci_review_summary..."

try {
    Invoke-RestMethod "$supabaseUrl/rest/v1/rpc/truncate_review_summary" `
        -Method POST `
        -Headers $headers `
        -Body "{}" | Out-Null
    Write-Host "Truncate complete"
}
catch {
    Write-Host "ERROR truncating: $_"
    exit 1
}

Write-Host "Rebuilding summary..."

try {
    Invoke-RestMethod "$supabaseUrl/rest/v1/rpc/populate_review_summary" `
        -Method POST `
        -Headers $headers `
        -Body "{}" | Out-Null
    Write-Host "Populate complete"
}
catch {
    Write-Host "ERROR populating: $_"
    exit 1
}

# Verify results - paginate to get all rows
Write-Host "Verifying rebuild..."

$allRows = @()
$verifyStart = 0
$verifyBatch = 1000

do {
    $batch = Invoke-RestMethod "$supabaseUrl/rest/v1/soci_review_summary?select=network,review_count&limit=$verifyBatch&offset=$verifyStart" `
        -Headers @{
            "apikey"        = $supabaseKey
            "Authorization" = "Bearer $supabaseKey"
            "Range-Unit"    = "items"
        }
    
    $allRows += $batch
    $verifyStart += $verifyBatch

} while ($batch.Count -eq $verifyBatch)

$grouped = $allRows | Group-Object network | ForEach-Object {
    [PSCustomObject]@{
        network       = $_.Name
        date_combos   = $_.Count
        total_reviews = ($_.Group | Measure-Object review_count -Sum).Sum
    }
} | Sort-Object total_reviews -Descending

Write-Host ""
Write-Host "================================"
Write-Host "Summary rebuild complete"
Write-Host "================================"
$grouped | ForEach-Object {
    Write-Host "$($_.network): $($_.total_reviews) reviews across $($_.date_combos) date/location combos"
}
