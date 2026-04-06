# ============================================================
# Gastamo Group — Deferred Sales Sync
# Populates deferred_sales table for a rolling window of dates.
# Runs after the main daily sync.
# ============================================================

$supabaseUrl = $env:SUPABASE_URL
$supabaseKey = $env:SUPABASE_KEY

$lookbackDays = if ($env:DEFERRED_LOOKBACK_DAYS) { [int]$env:DEFERRED_LOOKBACK_DAYS } else { 4 }

$logDir  = "logs"
$logFile = "$logDir/deferred-sales-$(Get-Date -Format 'yyyy-MM-dd-HHmm').log"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

function Write-Log($message, $color = "White") {
  $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[$timestamp] $message"
  Add-Content -Path $logFile -Value $line
  Write-Host $line -ForegroundColor $color
}

$headers = @{
  "apikey"        = $supabaseKey
  "Authorization" = "Bearer $supabaseKey"
  "Content-Type"  = "application/json"
  "Prefer"        = "return=minimal"
}

# Compute date range in Mountain Time
function Get-MountainTimeZone {
  foreach ($tzId in @("America/Denver", "Mountain Standard Time")) {
    try { return [TimeZoneInfo]::FindSystemTimeZoneById($tzId) } catch {}
  }
  throw "Unable to resolve Mountain Time zone."
}

$mountainTz  = Get-MountainTimeZone
$mountainNow = [TimeZoneInfo]::ConvertTime([DateTimeOffset]::UtcNow, $mountainTz)
$endDate     = [int]$mountainNow.Date.ToString("yyyyMMdd")
$startDate   = [int]$mountainNow.Date.AddDays(-1 * $lookbackDays).ToString("yyyyMMdd")

Write-Log "============================================"
Write-Log "Deferred Sales Sync Started"
Write-Log "============================================"
Write-Log "Date range: $startDate to $endDate ($lookbackDays day lookback)"

try {
  $body = @{ p_start_date = $startDate; p_end_date = $endDate } | ConvertTo-Json
  $result = Invoke-RestMethod "$supabaseUrl/rest/v1/rpc/populate_deferred_sales" `
    -Method POST `
    -Headers $headers `
    -Body $body
  Write-Log "Deferred sales populated: $result rows upserted" Green
} catch {
  Write-Log "ERROR populating deferred sales: $_" Red
  exit 1
}

Write-Log "============================================"
Write-Log "Deferred Sales Sync Complete"
Write-Log "============================================"
