# ============================================================
# Gastamo Group — Deferred Sales Backfill
# One-time script to populate deferred_sales for all history.
# Run manually — not part of the daily workflow.
# ============================================================

$supabaseUrl = $env:SUPABASE_URL
$supabaseKey = $env:SUPABASE_KEY

# Set your earliest business date across all locations
$startDate = 20230101
$endDate   = [int](Get-Date -Format "yyyyMMdd")
$chunkDays = 30  # Process 30 days at a time to avoid timeouts

$logDir  = "logs"
$logFile = "$logDir/backfill-deferred-$(Get-Date -Format 'yyyy-MM-dd-HHmm').log"
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

function Add-Days($ymd, $days) {
  $date = [datetime]::ParseExact($ymd.ToString(), "yyyyMMdd", $null)
  return [int]$date.AddDays($days).ToString("yyyyMMdd")
}

Write-Log "============================================"
Write-Log "Deferred Sales Backfill Started"
Write-Log "============================================"
Write-Log "Full range: $startDate to $endDate"
Write-Log "Chunk size: $chunkDays days"

$totalRows  = 0
$chunkStart = $startDate
$chunkNum   = 0

while ($chunkStart -le $endDate) {
  $chunkEnd = Add-Days $chunkStart ($chunkDays - 1)
  if ($chunkEnd -gt $endDate) { $chunkEnd = $endDate }
  $chunkNum++

  Write-Log "Chunk $chunkNum: $chunkStart to $chunkEnd" Cyan
  try {
    $body   = @{ p_start_date = $chunkStart; p_end_date = $chunkEnd } | ConvertTo-Json
    $result = Invoke-RestMethod "$supabaseUrl/rest/v1/rpc/populate_deferred_sales" `
      -Method POST `
      -Headers $headers `
      -Body $body
    Write-Log "  Chunk $chunkNum complete: $result rows upserted" Green
    $totalRows += $result
  } catch {
    Write-Log "  ERROR on chunk $chunkStart-$chunkEnd: $_" Red
    Write-Log "  Continuing with next chunk..." Yellow
  }

  $chunkStart = Add-Days $chunkStart $chunkDays
  Start-Sleep -Milliseconds 500
}

Write-Log "============================================"
Write-Log "Backfill Complete — $totalRows total rows upserted"
Write-Log "============================================"
