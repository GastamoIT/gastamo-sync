# ============================================================
# Gastamo Group — Daily Sync Script (Optimized)
# Incremental sync with rolling overlap windows to avoid missed late updates.
# ============================================================

# CREDENTIALS
$clientId     = $env:TOAST_CLIENT_ID
$clientSecret = $env:TOAST_CLIENT_SECRET
$toastApiUrl  = $env:TOAST_API_URL
$supabaseUrl  = $env:SUPABASE_URL
$supabaseKey  = $env:SUPABASE_KEY

# SYNC WINDOW CONFIG
$syncLookbackHours = if ($env:SYNC_LOOKBACK_HOURS) { [int]$env:SYNC_LOOKBACK_HOURS } else { 72 }
$cashLookbackDays  = if ($env:CASH_LOOKBACK_DAYS)  { [int]$env:CASH_LOOKBACK_DAYS }  else { 4 }

# LOG FILE
$logDir  = "logs"
$logFile = "$logDir/daily-sync-$(Get-Date -Format 'yyyy-MM-dd-HHmm').log"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

function Write-Log($message, $color = "White") {
  $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[$timestamp] $message"
  Add-Content -Path $logFile -Value $line
  Write-Host $line -ForegroundColor $color
}

function Get-MountainTimeZone {
  foreach ($tzId in @("America/Denver", "Mountain Standard Time")) {
    try {
      return [TimeZoneInfo]::FindSystemTimeZoneById($tzId)
    } catch {}
  }
  throw "Unable to resolve America/Denver / Mountain Standard Time on this runner."
}

$mountainTz = Get-MountainTimeZone
$utcNow = [DateTimeOffset]::UtcNow
$mountainNow = [TimeZoneInfo]::ConvertTime($utcNow, $mountainTz)
$startUtcRaw = $utcNow.AddHours(-1 * $syncLookbackHours).UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "+0000"
$endUtcRaw   = $utcNow.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "+0000"
$startUtc    = [Uri]::EscapeDataString($startUtcRaw)
$endUtc      = [Uri]::EscapeDataString($endUtcRaw)
$cashBusinessDates = @()
for ($i = 0; $i -lt $cashLookbackDays; $i++) {
  $cashBusinessDates += $mountainNow.Date.AddDays(-$i).ToString("yyyyMMdd")
}

Write-Log "============================================"
Write-Log "Gastamo Daily Sync Started"
Write-Log "============================================"
Write-Log "Mountain now: $($mountainNow.ToString('yyyy-MM-dd HH:mm:ss zzz'))"
Write-Log "Rolling overlap window (UTC): $startUtcRaw to $endUtcRaw"
Write-Log "Cash business dates: $($cashBusinessDates -join ', ')"

# ============================================================
# AUTH
# ============================================================
try {
  $authBody = @{ clientId = $clientId; clientSecret = $clientSecret; userAccessType = "TOAST_MACHINE_CLIENT" } | ConvertTo-Json
  $authResponse = Invoke-RestMethod -Uri "$toastApiUrl/authentication/v1/authentication/login" -Method POST -Body $authBody -ContentType "application/json"

  # Try both known response shapes
  $authJson = $authResponse | ConvertTo-Json -Depth 5
  $token = $authResponse.token.accessToken
  if (-not $token) { $token = $authResponse.accessToken }
  if (-not $token) { $token = $authResponse.token }

  # Validate token before proceeding
  if (-not $token -or $token.Length -lt 20) {
    Write-Log "FATAL: Auth returned 200 but token is null/empty. Response:" Red
    Write-Log $authJson Red
    exit 1
  }

  $tokenAcquiredAt = Get-Date
  Write-Log "Toast token acquired (length=$($token.Length))" Green
} catch {
  Write-Log "FATAL: Toast auth failed — $(Get-ToastErrorDetail $_)" Red
  exit 1
}

$supabaseHeaders = @{
  "apikey"        = $supabaseKey
  "Authorization" = "Bearer $supabaseKey"
  "Content-Type"  = "application/json"
  "Prefer"        = "resolution=merge-duplicates,return=minimal"
}

# ============================================================
# LOCATIONS
# ============================================================
$locations = @(
  @{ guid = "5ed76b7f-7d30-472f-bc5c-8ea17ae31954"; name = "HG-Westminster" },
  @{ guid = "15ed9231-4d0f-4838-9c2e-2f085b5b18f9"; name = "Perdida-Westminster" },
  @{ guid = "2693b094-44de-42de-9d0a-24b27aa17687"; name = "Park-Co" },
  @{ guid = "ee3ff73b-3b0b-4ccb-b351-9ef006c3d664"; name = "HG-CastleRock" },
  @{ guid = "263eb56c-977c-465d-ad6d-46ddf5800e63"; name = "LN-Arvada" },
  @{ guid = "15db461b-a5d9-463e-8e76-b10443f9f451"; name = "PB-Holly" },
  @{ guid = "a96580b0-0906-4611-a2ea-08159717d80b"; name = "HG-Parker" },
  @{ guid = "72eb5f11-7674-4ffe-91a3-d6b5688748d7"; name = "HG-Arvada" },
  @{ guid = "689d4903-ed5d-4c9d-ac2d-9ade4e783d46"; name = "HG-WashingtonPark" },
  @{ guid = "8c4d1096-fb86-4f4f-a975-036219328145"; name = "PB-Highlands" },
  @{ guid = "c5de1411-9fdf-43c4-94b3-1700a9d2b68a"; name = "HG-KenCaryl" },
  @{ guid = "a7555f87-91ea-484e-be76-d4a46ddaf4d7"; name = "PB-RiNo" },
  @{ guid = "e95f033a-ef10-4004-96d9-0d949427eca3"; name = "Perdida-WashingtonPark" },
  @{ guid = "b438f92d-ec44-41c4-a765-816f3cd902d9"; name = "LN-Belmar" },
  @{ guid = "d842fb61-9607-4752-990b-9ed08aecb8a9"; name = "PB-Pearl" },
  @{ guid = "c021581f-bb16-45cd-b69a-1c1b2416a8f1"; name = "LN-CentralPark" }
)

$locResponse = Invoke-RestMethod -Uri "$supabaseUrl/rest/v1/locations?select=id,toast_restaurant_guid&limit=100" -Headers $supabaseHeaders
$locationIdMap = @{}
foreach ($loc in $locResponse) { $locationIdMap[$loc.toast_restaurant_guid] = $loc.id }
$script:HadErrors = $false

# ============================================================
# HELPERS
# ============================================================
function Get-ToastErrorDetail($errorRecord) {
  # Extract the response body from Toast 4xx/5xx errors
  # Works across PowerShell 5.1 and 7
  try {
    # PS7: ErrorDetails.Message often contains the response body
    if ($errorRecord.ErrorDetails -and $errorRecord.ErrorDetails.Message) {
      return "$($errorRecord.Exception.Message) | Body: $($errorRecord.ErrorDetails.Message)"
    }
    # PS5.1: WebException has Response.GetResponseStream()
    if ($errorRecord.Exception.Response -and $errorRecord.Exception.Response.GetResponseStream) {
      $reader = New-Object System.IO.StreamReader($errorRecord.Exception.Response.GetResponseStream())
      $reader.BaseStream.Position = 0
      $body = $reader.ReadToEnd()
      $reader.Close()
      return "$($errorRecord.Exception.Message) | Body: $body"
    }
  } catch {}
  return $errorRecord.Exception.Message
}

function Get-NextPageUrl($linkHeader) {
  if (-not $linkHeader) { return $null }
  foreach ($part in $linkHeader -split ",") {
    if ($part -match '<([^>]+)>;\s*rel="next"') { return $matches[1] }
  }
  return $null
}

function Invoke-WithRetry {
  param(
    [scriptblock]$ScriptBlock,
    [int]$MaxRetries = 3,
    [int]$BaseDelaySeconds = 5,
    [string]$Label = "API call"
  )
  for ($attempt = 1; $attempt -le ($MaxRetries + 1); $attempt++) {
    try {
      return (& $ScriptBlock)
    } catch {
      $statusCode = $null
      # PS7: HttpRequestException has .StatusCode directly
      if ($null -ne $_.Exception.StatusCode) {
        $statusCode = [int]$_.Exception.StatusCode
      }
      # PS5.1: WebException has .Response.StatusCode
      elseif ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
        $statusCode = [int]$_.Exception.Response.StatusCode
      }
      # Fallback: parse 3-digit code from error message
      elseif ($_.Exception.Message -match '\b(\d{3})\b') {
        $statusCode = [int]$matches[1]
      }
      # Only retry on 500, 502, 503, 504, 429
      $retryable = $statusCode -in @(500, 502, 503, 504, 429)
      if (-not $retryable -or $attempt -gt $MaxRetries) {
        throw $_
      }
      $delay = $BaseDelaySeconds * [Math]::Pow(2, ($attempt - 1))
      Write-Log "  RETRY $attempt/$MaxRetries for $Label (HTTP $statusCode) — waiting ${delay}s" Yellow
      Start-Sleep -Seconds $delay
    }
  }
}

function Write-ToSupabase {
  param(
    [Parameter(Mandatory = $true)]$batch,
    [Parameter(Mandatory = $true)][string]$table,
    [string]$conflict = $null,
    [string]$select = $null,
    [int]$batchSize = 100
  )

  $rows = @($batch)
  if ($rows.Count -eq 0) {
    if ($select) { return @() }
    return 0
  }

  $written = 0
  $returnedRows = @()

  for ($j = 0; $j -lt $rows.Count; $j += $batchSize) {
    $end = [Math]::Min($j + $batchSize - 1, $rows.Count - 1)
    $slice = @($rows[$j..$end])
    $json = $slice | ConvertTo-Json -Depth 8 -Compress
    if ($slice.Count -eq 1) { $json = "[$json]" }

    $queryParams = @()
    if ($conflict) { $queryParams += "on_conflict=$([Uri]::EscapeDataString($conflict))" }
    if ($select)   { $queryParams += "select=$([Uri]::EscapeDataString($select))" }

    $url = "$supabaseUrl/rest/v1/$table"
    if ($queryParams.Count -gt 0) {
      $url += "?" + ($queryParams -join "&")
    }

    $headers = @{}
    foreach ($key in $supabaseHeaders.Keys) {
      $headers[$key] = $supabaseHeaders[$key]
    }
    if ($select) {
      $headers["Prefer"] = "resolution=merge-duplicates,return=representation"
    } else {
      $headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    }

    for ($retry = 0; $retry -lt 3; $retry++) {
      try {
        $response = Invoke-RestMethod -Uri $url -Method POST -Headers $headers -Body $json
        $written += $slice.Count
        if ($select -and $null -ne $response) {
          $returnedRows += @($response)
        }
        break
      } catch {
        $errorBody = ""
        try {
          if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
            $errorBody = $_.ErrorDetails.Message
          } elseif ($_.Exception.Response -and $_.Exception.Response.GetResponseStream) {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $reader.BaseStream.Position = 0
            $errorBody = $reader.ReadToEnd()
            $reader.Close()
          }
        } catch {}

        $isTimeout = $errorBody -match "57014" -or $errorBody -match "statement timeout"
        $statusCode = $null
        if ($null -ne $_.Exception.StatusCode) { $statusCode = [int]$_.Exception.StatusCode }
        elseif ($_.Exception.Response -and $_.Exception.Response.StatusCode) { $statusCode = [int]$_.Exception.Response.StatusCode }
        elseif ($_.Exception.Message -match '\b(\d{3})\b') { $statusCode = [int]$matches[1] }

        if (($isTimeout -or $statusCode -in @(500, 503)) -and $retry -lt 2) {
          $delay = 5 * [Math]::Pow(2, $retry)
          Write-Log "  SUPABASE RETRY $($retry+1)/3 on $table (timeout) — waiting ${delay}s" Yellow
          Start-Sleep -Seconds $delay
        } else {
          throw ("Supabase write failed on {0}: {1}" -f $table, $_.Exception.Message) $($_.Exception.Message) | $errorBody"
        }
      }
    }
  }

  if ($select) { return $returnedRows }
  return $written
}

function Strip-HelperFields($rows) {
  return $rows | ForEach-Object {
    $clean = @{}
    foreach ($key in $_.Keys) {
      if ($key -notlike '_*') { $clean[$key] = $_[$key] }
    }
    $clean
  }
}

function Refresh-TokenIfNeeded {
  $minutesElapsed = ((Get-Date) - $script:tokenAcquiredAt).TotalMinutes
  if ($minutesElapsed -gt 50) {
    try {
      $authBody = @{ clientId = $clientId; clientSecret = $clientSecret; userAccessType = "TOAST_MACHINE_CLIENT" } | ConvertTo-Json
      $authResponse = Invoke-RestMethod -Uri "$toastApiUrl/authentication/v1/authentication/login" -Method POST -Body $authBody -ContentType "application/json"
      $newToken = $authResponse.token.accessToken
      if (-not $newToken) { $newToken = $authResponse.accessToken }
      if (-not $newToken) { $newToken = $authResponse.token }
      if (-not $newToken -or $newToken.Length -lt 20) {
        Write-Log "WARNING: Token refresh returned empty token — keeping old token" Red
        return
      }
      $script:token = $newToken
      $script:tokenAcquiredAt = Get-Date
      Write-Log "Toast token refreshed (length=$($newToken.Length))" Green
    } catch {
      Write-Log "WARNING: Token refresh failed — $(Get-ToastErrorDetail $_)" Red
    }
  }
}

function Build-SelectionRow($sel, $checkId, $orderId, $locationId, $parentToastGuid) {
  $appliedTaxes = $null
  if ($sel.appliedTaxes -and $sel.appliedTaxes.Count -gt 0) {
    $appliedTaxes = $sel.appliedTaxes | ConvertTo-Json -Compress -Depth 5
  }
  return @{
    toast_selection_guid       = $sel.guid
    check_id                   = $checkId
    order_id                   = $orderId
    location_id                = $locationId
    parent_item_id             = $null
    _parent_toast_guid         = $parentToastGuid
    item_guid                  = if ($sel.item.guid) { $sel.item.guid } else { $null }
    item_group_guid            = if ($sel.itemGroup.guid) { $sel.itemGroup.guid } else { $null }
    option_group_guid          = if ($sel.optionGroup.guid) { $sel.optionGroup.guid } else { $null }
    sales_category_guid        = if ($sel.salesCategory.guid) { $sel.salesCategory.guid } else { $null }
    display_name               = if ($sel.displayName) { $sel.displayName } else { $null }
    quantity                   = if ($null -ne $sel.quantity) { $sel.quantity } else { $null }
    seat_number                = if ($null -ne $sel.seatNumber) { $sel.seatNumber } else { $null }
    selection_type             = if ($sel.selectionType) { $sel.selectionType } else { $null }
    price                      = if ($null -ne $sel.price) { $sel.price } else { $null }
    pre_discount_price         = if ($null -ne $sel.preDiscountPrice) { $sel.preDiscountPrice } else { $null }
    tax                        = if ($null -ne $sel.tax) { $sel.tax } else { $null }
    voided                     = if ($null -ne $sel.voided) { $sel.voided } else { $false }
    void_date                  = if ($sel.voidDate) { $sel.voidDate } else { $null }
    void_reason_guid           = if ($sel.voidReason.guid) { $sel.voidReason.guid } else { $null }
    applied_taxes              = $appliedTaxes
    dining_option_guid         = if ($sel.diningOption.guid) { $sel.diningOption.guid } else { $null }
    created_date               = if ($sel.createdDate) { $sel.createdDate } else { $null }
    modified_date              = if ($sel.modifiedDate) { $sel.modifiedDate } else { $null }
    updated_at                 = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}

function Get-AllSelections($selections, $checkId, $orderId, $locationId, $parentToastGuid) {
  $rows = @()
  foreach ($sel in $selections) {
    if (-not $sel.guid) { continue }
    $rows += Build-SelectionRow $sel $checkId $orderId $locationId $parentToastGuid
    if ($sel.modifiers -and $sel.modifiers.Count -gt 0) {
      $rows += Get-AllSelections $sel.modifiers $checkId $orderId $locationId $sel.guid
    }
  }
  return $rows
}

function Get-ToastOrdersForLocation {
  param(
    $Location,
    [string]$StartUtc,
    [string]$EndUtc
  )

  $cacheKey = "$($Location.guid)|$StartUtc|$EndUtc"
  if ($script:ordersCache.ContainsKey($cacheKey)) {
    return $script:ordersCache[$cacheKey]
  }

  $allOrders = @()
  $url = "$toastApiUrl/orders/v2/ordersBulk?startDate=$StartUtc&endDate=$EndUtc&pageSize=100"
  do {
    $response = Invoke-WithRetry -Label "$($Location.name) ordersBulk" -ScriptBlock {
      Invoke-WebRequest -Uri $url -UseBasicParsing -Headers @{
        Authorization                  = "Bearer $token"
        "Toast-Restaurant-External-ID" = $Location.guid
      }
    }
    $allOrders += ($response.Content | ConvertFrom-Json)
    $url = Get-NextPageUrl $response.Headers["link"]
  } while ($url)

  $script:ordersCache[$cacheKey] = $allOrders
  return $allOrders
}

function Get-ItemLevelDiscountRows {
  param(
    $Selections,
    $SupabaseCheckId,
    $SupabaseOrderId,
    $LocationId,
    $ItemIdMap
  )

  $rows = @()
  foreach ($sel in @($Selections)) {
    if (-not $sel.guid) { continue }

    $supabaseItemId = $null
    if ($ItemIdMap.ContainsKey($sel.guid)) {
      $supabaseItemId = $ItemIdMap[$sel.guid]
    }

    foreach ($discount in @($sel.appliedDiscounts)) {
      if (-not $discount.guid) { continue }
      $rows += @{
        toast_discount_guid  = $discount.guid
        check_id             = $SupabaseCheckId
        order_item_id        = $supabaseItemId
        order_id             = $SupabaseOrderId
        location_id          = $LocationId
        name                 = if ($discount.name) { $discount.name } else { $null }
        discount_amount      = if ($null -ne $discount.discountAmount) { $discount.discountAmount } else { $null }
        discount_guid        = if ($discount.discount.guid) { $discount.discount.guid } else { $null }
        discount_type        = if ($discount.discountType) { $discount.discountType } else { $null }
        processing_state     = if ($discount.processingState) { $discount.processingState } else { $null }
        approver_guid        = if ($discount.approver.guid) { $discount.approver.guid } else { $null }
        applied_at_level     = "ITEM"
        updated_at           = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
      }
    }

    if ($sel.modifiers -and $sel.modifiers.Count -gt 0) {
      $rows += Get-ItemLevelDiscountRows -Selections $sel.modifiers -SupabaseCheckId $SupabaseCheckId -SupabaseOrderId $SupabaseOrderId -LocationId $LocationId -ItemIdMap $ItemIdMap
    }
  }

  return $rows
}

# ============================================================
# MAIN SYNC
# ============================================================
$grandTotalOrders       = 0
$grandTotalChecks       = 0
$grandTotalItems        = 0
$grandTotalPayments     = 0
$grandTotalDiscounts    = 0
$grandTotalSvcCharges   = 0
$grandTotalShifts       = 0
$grandTotalTimeEntries  = 0
$grandTotalCashEntries  = 0
$grandTotalDeposits     = 0

Refresh-TokenIfNeeded

$script:ordersCache = @{}
$orderIdMap = @{}
$checkIdMap = @{}
$itemIdMapsByLocation = @{}

  # ============================================================
  # 1. TOAST ORDERS
  # ============================================================
  Write-Log "--- Orders ---" Cyan
  $dayOrderTotal = 0
  foreach ($loc in $locations) {
    $locationId = $locationIdMap[$loc.guid]
    if (-not $locationId) { Write-Log "  ERROR $($loc.name): missing locations.id mapping for Toast GUID $($loc.guid)" Red; $script:HadErrors = $true; continue }
    try {
      $allOrders = Get-ToastOrdersForLocation -Location $loc -StartUtc $startUtc -EndUtc $endUtc

      $batch = @()
      foreach ($order in $allOrders) {
        if (-not $order.guid) { continue }
        $batch += @{
          toast_order_guid           = $order.guid
          location_id                = $locationId
          toast_external_id          = if ($order.externalId) { $order.externalId } else { $null }
          business_date              = if ($null -ne $order.businessDate) { $order.businessDate } else { $null }
          opened_date                = if ($order.openedDate) { $order.openedDate } else { $null }
          modified_date              = if ($order.modifiedDate) { $order.modifiedDate } else { $null }
          promised_date              = if ($order.promisedDate) { $order.promisedDate } else { $null }
          estimated_fulfillment_date = if ($order.estimatedFulfillmentDate) { $order.estimatedFulfillmentDate } else { $null }
          paid_date                  = if ($order.paidDate) { $order.paidDate } else { $null }
          closed_date                = if ($order.closedDate) { $order.closedDate } else { $null }
          deleted_date               = if ($order.deletedDate) { $order.deletedDate } else { $null }
          void_date                  = if ($order.voidDate) { $order.voidDate } else { $null }
          void_business_date         = if ($null -ne $order.voidBusinessDate) { $order.voidBusinessDate } else { $null }
          created_date               = if ($order.createdDate) { $order.createdDate } else { $null }
          display_number             = if ($order.displayNumber) { $order.displayNumber } else { $null }
          number_of_guests           = if ($null -ne $order.numberOfGuests) { $order.numberOfGuests } else { $null }
          source                     = if ($order.source) { $order.source } else { $null }
          approval_status            = if ($order.approvalStatus) { $order.approvalStatus } else { $null }
          dining_option_guid         = if ($order.diningOption.guid) { $order.diningOption.guid } else { $null }
          dining_option_entity_type  = if ($order.diningOption.entityType) { $order.diningOption.entityType } else { $null }
          table_guid                 = if ($order.table.guid) { $order.table.guid } else { $null }
          service_area_guid          = if ($order.serviceArea.guid) { $order.serviceArea.guid } else { $null }
          restaurant_service_guid    = if ($order.restaurantService.guid) { $order.restaurantService.guid } else { $null }
          revenue_center_guid        = if ($order.revenueCenter.guid) { $order.revenueCenter.guid } else { $null }
          server_guid                = if ($order.server.guid) { $order.server.guid } else { $null }
          channel_guid               = if ($order.channelGuid) { $order.channelGuid } else { $null }
          duration_seconds           = if ($null -ne $order.duration) { $order.duration } else { $null }
          voided                     = if ($null -ne $order.voided) { $order.voided } else { $false }
          deleted                    = if ($null -ne $order.deleted) { $order.deleted } else { $false }
          created_in_test_mode       = if ($null -ne $order.createdInTestMode) { $order.createdInTestMode } else { $false }
          excess_food                = if ($null -ne $order.excessFood) { $order.excessFood } else { $false }
          delivery_address1          = if ($order.deliveryInfo.address1) { $order.deliveryInfo.address1 } else { $null }
          delivery_city              = if ($order.deliveryInfo.city) { $order.deliveryInfo.city } else { $null }
          delivery_state             = if ($order.deliveryInfo.state) { $order.deliveryInfo.state } else { $null }
          delivery_zip               = if ($order.deliveryInfo.zipCode) { $order.deliveryInfo.zipCode } else { $null }
          delivery_state_status      = if ($order.deliveryInfo.deliveryState) { $order.deliveryInfo.deliveryState } else { $null }
          created_device_id          = if ($order.createdDevice.id) { $order.createdDevice.id } else { $null }
          updated_at                 = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        }
      }

      $writtenRows = @(Write-ToSupabase -batch $batch -table "toast_orders" -conflict "location_id,toast_order_guid" -select "id,toast_order_guid" -batchSize 100)
      foreach ($row in $writtenRows) {
        if ($row.toast_order_guid) {
          $orderIdMap[$row.toast_order_guid] = $row.id
        }
      }
      $dayOrderTotal += $writtenRows.Count
    } catch {
      $script:HadErrors = $true
      Write-Log "  ERROR $($loc.name) orders: $(Get-ToastErrorDetail $_)" Red
    }
    Start-Sleep -Milliseconds 100
  }
  Write-Log "  Orders: $dayOrderTotal" Green
  $grandTotalOrders += $dayOrderTotal

  # ============================================================
  # 2. TOAST CHECKS
  # ============================================================
  Write-Log "--- Checks ---" Cyan
  $dayCheckTotal = 0
  foreach ($loc in $locations) {
    $locationId = $locationIdMap[$loc.guid]
    if (-not $locationId) { Write-Log "  ERROR $($loc.name): missing locations.id mapping for Toast GUID $($loc.guid)" Red; $script:HadErrors = $true; continue }
    try {
      $allOrders = Get-ToastOrdersForLocation -Location $loc -StartUtc $startUtc -EndUtc $endUtc
      $batch = @()
      foreach ($order in $allOrders) {
        if (-not $order.guid) { continue }
        $supabaseOrderId = $orderIdMap[$order.guid]
        if (-not $supabaseOrderId) { continue }
        foreach ($check in @($order.checks)) {
          if (-not $check.guid) { continue }
          $batch += @{
            toast_check_guid        = $check.guid
            order_id                = $supabaseOrderId
            location_id             = $locationId
            toast_external_id       = if ($check.externalId) { $check.externalId } else { $null }
            display_number          = if ($check.displayNumber) { $check.displayNumber } else { $null }
            tab_name                = if ($check.tabName) { $check.tabName } else { $null }
            opened_date             = if ($check.openedDate) { $check.openedDate } else { $null }
            created_date            = if ($check.createdDate) { $check.createdDate } else { $null }
            modified_date           = if ($check.modifiedDate) { $check.modifiedDate } else { $null }
            closed_date             = if ($check.closedDate) { $check.closedDate } else { $null }
            paid_date               = if ($check.paidDate) { $check.paidDate } else { $null }
            void_date               = if ($check.voidDate) { $check.voidDate } else { $null }
            void_business_date      = if ($null -ne $check.voidBusinessDate) { $check.voidBusinessDate } else { $null }
            payment_status          = if ($check.paymentStatus) { $check.paymentStatus } else { $null }
            voided                  = if ($null -ne $check.voided) { $check.voided } else { $false }
            deleted                 = if ($null -ne $check.deleted) { $check.deleted } else { $false }
            tax_exempt              = if ($null -ne $check.taxExempt) { $check.taxExempt } else { $false }
            duration_seconds        = if ($null -ne $check.duration) { $check.duration } else { $null }
            amount                  = if ($null -ne $check.amount) { $check.amount } else { $null }
            tax_amount              = if ($null -ne $check.taxAmount) { $check.taxAmount } else { $null }
            total_amount            = if ($null -ne $check.totalAmount) { $check.totalAmount } else { $null }
            opened_by_guid          = if ($check.openedBy.guid) { $check.openedBy.guid } else { $null }
            customer_guid           = if ($check.customer.guid) { $check.customer.guid } else { $null }
            customer_first_name     = if ($check.customer.firstName) { $check.customer.firstName } else { $null }
            customer_last_name      = if ($check.customer.lastName) { $check.customer.lastName } else { $null }
            customer_phone          = if ($check.customer.phone) { $check.customer.phone } else { $null }
            customer_email          = if ($check.customer.email) { $check.customer.email } else { $null }
            loyalty_vendor          = if ($check.appliedLoyaltyInfo.vendor) { $check.appliedLoyaltyInfo.vendor } else { $null }
            updated_at              = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
          }
        }
      }

      $writtenRows = @(Write-ToSupabase -batch $batch -table "toast_checks" -conflict "toast_check_guid" -select "id,toast_check_guid" -batchSize 100)
      foreach ($row in $writtenRows) {
        if ($row.toast_check_guid) {
          $checkIdMap[$row.toast_check_guid] = $row.id
        }
      }
      $dayCheckTotal += $writtenRows.Count
    } catch {
      $script:HadErrors = $true
      Write-Log "  ERROR $($loc.name) checks: $(Get-ToastErrorDetail $_)" Red
    }
  }
  Write-Log "  Checks: $dayCheckTotal" Green
  $grandTotalChecks += $dayCheckTotal

  # ============================================================
  # 3. TOAST ORDER ITEMS
  # ============================================================
  Write-Log "--- Order Items ---" Cyan
  $dayItemTotal = 0
  foreach ($loc in $locations) {
    $locationId = $locationIdMap[$loc.guid]
    if (-not $locationId) { Write-Log "  ERROR $($loc.name): missing locations.id mapping for Toast GUID $($loc.guid)" Red; $script:HadErrors = $true; continue }
    try {
      $allOrders = Get-ToastOrdersForLocation -Location $loc -StartUtc $startUtc -EndUtc $endUtc
      $allSelections = @()
      foreach ($order in $allOrders) {
        if (-not $order.guid) { continue }
        $supabaseOrderId = $orderIdMap[$order.guid]
        if (-not $supabaseOrderId) { continue }
        foreach ($check in @($order.checks)) {
          if (-not $check.guid) { continue }
          $supabaseCheckId = $checkIdMap[$check.guid]
          if (-not $supabaseCheckId) { continue }
          if ($check.selections -and $check.selections.Count -gt 0) {
            $allSelections += Get-AllSelections $check.selections $supabaseCheckId $supabaseOrderId $locationId $null
          }
        }
      }

      $pending = @($allSelections)
      $locationItemIdMap = @{}
      while ($pending.Count -gt 0) {
        $insertable = @()
        $remaining = @()

        foreach ($row in $pending) {
          if ($null -eq $row['_parent_toast_guid']) {
            $insertable += Strip-HelperFields @($row)
          } elseif ($locationItemIdMap.ContainsKey($row['_parent_toast_guid'])) {
            $row['parent_item_id'] = $locationItemIdMap[$row['_parent_toast_guid']]
            $insertable += Strip-HelperFields @($row)
          } else {
            $remaining += $row
          }
        }

        if ($insertable.Count -eq 0) {
          Write-Log "  WARNING $($loc.name) items: unable to resolve $($remaining.Count) modifier parent ids" Yellow
          break
        }

        $insertedRows = @(Write-ToSupabase -batch $insertable -table "toast_order_items" -conflict "toast_selection_guid" -select "id,toast_selection_guid" -batchSize 50)
        foreach ($row in $insertedRows) {
          if ($row.toast_selection_guid) {
            $locationItemIdMap[$row.toast_selection_guid] = $row.id
          }
        }
        $dayItemTotal += $insertedRows.Count
        $pending = @($remaining)
      }

      $itemIdMapsByLocation[$loc.guid] = $locationItemIdMap
    } catch {
      $script:HadErrors = $true
      Write-Log "  ERROR $($loc.name) items: $(Get-ToastErrorDetail $_)" Red
    }
  }
  Write-Log "  Items: $dayItemTotal" Green
  $grandTotalItems += $dayItemTotal

  # ============================================================
  # 4. TOAST PAYMENTS
  # ============================================================
  Write-Log "--- Payments ---" Cyan
  $dayPaymentTotal = 0
  foreach ($loc in $locations) {
    $locationId = $locationIdMap[$loc.guid]
    if (-not $locationId) { Write-Log "  ERROR $($loc.name): missing locations.id mapping for Toast GUID $($loc.guid)" Red; $script:HadErrors = $true; continue }
    try {
      $allOrders = Get-ToastOrdersForLocation -Location $loc -StartUtc $startUtc -EndUtc $endUtc
      $batch = @()
      foreach ($order in $allOrders) {
        if (-not $order.guid) { continue }
        $supabaseOrderId = $orderIdMap[$order.guid]
        if (-not $supabaseOrderId) { continue }
        foreach ($check in @($order.checks)) {
          if (-not $check.guid) { continue }
          $supabaseCheckId = $checkIdMap[$check.guid]
          if (-not $supabaseCheckId) { continue }
          foreach ($payment in @($check.payments)) {
            if (-not $payment.guid) { continue }
            $batch += @{
              toast_payment_guid      = $payment.guid
              check_id                = $supabaseCheckId
              order_id                = $supabaseOrderId
              location_id             = $locationId
              toast_order_guid        = $order.guid
              toast_check_guid        = $check.guid
              payment_type            = if ($payment.type) { $payment.type } else { $null }
              card_entry_mode         = if ($payment.cardEntryMode) { $payment.cardEntryMode } else { $null }
              card_type               = if ($payment.cardType) { $payment.cardType } else { $null }
              last4_digits            = if ($payment.last4Digits) { $payment.last4Digits } else { $null }
              other_payment_guid      = if ($payment.otherPayment.guid) { $payment.otherPayment.guid } else { $null }
              server_guid             = if ($payment.server.guid) { $payment.server.guid } else { $null }
              amount                  = if ($null -ne $payment.amount) { $payment.amount } else { $null }
              tip_amount              = if ($null -ne $payment.tipAmount) { $payment.tipAmount } else { $null }
              amount_tendered         = if ($null -ne $payment.amountTendered) { $payment.amountTendered } else { $null }
              original_processing_fee = if ($null -ne $payment.originalProcessingFee) { $payment.originalProcessingFee } else { $null }
              payment_status          = if ($payment.paymentStatus) { $payment.paymentStatus } else { $null }
              refund_status           = if ($payment.refundStatus) { $payment.refundStatus } else { $null }
              refund_amount           = if ($null -ne $payment.refund.refundAmount) { $payment.refund.refundAmount } else { $null }
              refund_date             = if ($payment.refund.refundDate) { $payment.refund.refundDate } else { $null }
              paid_date               = if ($payment.paidDate) { $payment.paidDate } else { $null }
              paid_business_date      = if ($null -ne $payment.paidBusinessDate) { $payment.paidBusinessDate } else { $null }
              updated_at              = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            }
          }
        }
      }
      $written = Write-ToSupabase -batch $batch -table "toast_payments" -conflict "toast_payment_guid" -batchSize 100
      $dayPaymentTotal += $written
    } catch {
      $script:HadErrors = $true
      Write-Log "  ERROR $($loc.name) payments: $(Get-ToastErrorDetail $_)" Red
    }
  }
  Write-Log "  Payments: $dayPaymentTotal" Green
  $grandTotalPayments += $dayPaymentTotal

  # ============================================================
  # 5. TOAST DISCOUNTS
  # ============================================================
  Write-Log "--- Discounts ---" Cyan
  $dayDiscountTotal = 0
  foreach ($loc in $locations) {
    $locationId = $locationIdMap[$loc.guid]
    if (-not $locationId) { Write-Log "  ERROR $($loc.name): missing locations.id mapping for Toast GUID $($loc.guid)" Red; $script:HadErrors = $true; continue }
    try {
      $allOrders = Get-ToastOrdersForLocation -Location $loc -StartUtc $startUtc -EndUtc $endUtc
      $itemIdMap = @{}
      if ($itemIdMapsByLocation.ContainsKey($loc.guid)) {
        $itemIdMap = $itemIdMapsByLocation[$loc.guid]
      }

      $batch = @()
      foreach ($order in $allOrders) {
        if (-not $order.guid) { continue }
        $supabaseOrderId = $orderIdMap[$order.guid]
        if (-not $supabaseOrderId) { continue }
        foreach ($check in @($order.checks)) {
          if (-not $check.guid) { continue }
          $supabaseCheckId = $checkIdMap[$check.guid]
          if (-not $supabaseCheckId) { continue }

          foreach ($discount in @($check.appliedDiscounts)) {
            if (-not $discount.guid) { continue }
            $batch += @{
              toast_discount_guid  = $discount.guid
              check_id             = $supabaseCheckId
              order_item_id        = $null
              order_id             = $supabaseOrderId
              location_id          = $locationId
              name                 = if ($discount.name) { $discount.name } else { $null }
              discount_amount      = if ($null -ne $discount.discountAmount) { $discount.discountAmount } else { $null }
              discount_guid        = if ($discount.discount.guid) { $discount.discount.guid } else { $null }
              discount_type        = if ($discount.discountType) { $discount.discountType } else { $null }
              processing_state     = if ($discount.processingState) { $discount.processingState } else { $null }
              approver_guid        = if ($discount.approver.guid) { $discount.approver.guid } else { $null }
              applied_at_level     = "CHECK"
              updated_at           = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            }
          }

          if ($check.selections -and $check.selections.Count -gt 0) {
            $batch += Get-ItemLevelDiscountRows -Selections $check.selections -SupabaseCheckId $supabaseCheckId -SupabaseOrderId $supabaseOrderId -LocationId $locationId -ItemIdMap $itemIdMap
          }
        }
      }

      $written = Write-ToSupabase -batch $batch -table "toast_discounts" -conflict "toast_discount_guid" -batchSize 100
      $dayDiscountTotal += $written
    } catch {
      $script:HadErrors = $true
      Write-Log "  ERROR $($loc.name) discounts: $(Get-ToastErrorDetail $_)" Red
    }
  }
  Write-Log "  Discounts: $dayDiscountTotal" Green
  $grandTotalDiscounts += $dayDiscountTotal

  # ============================================================
  # 6. TOAST SERVICE CHARGES
  # ============================================================
  Write-Log "--- Service Charges ---" Cyan
  $daySvcChargeTotal = 0
  foreach ($loc in $locations) {
    $locationId = $locationIdMap[$loc.guid]
    if (-not $locationId) { Write-Log "  ERROR $($loc.name): missing locations.id mapping for Toast GUID $($loc.guid)" Red; $script:HadErrors = $true; continue }
    try {
      $allOrders = Get-ToastOrdersForLocation -Location $loc -StartUtc $startUtc -EndUtc $endUtc
      $batch = @()
      foreach ($order in $allOrders) {
        if (-not $order.guid) { continue }
        $supabaseOrderId = $orderIdMap[$order.guid]
        if (-not $supabaseOrderId) { continue }
        foreach ($check in @($order.checks)) {
          if (-not $check.guid) { continue }
          $supabaseCheckId = $checkIdMap[$check.guid]
          if (-not $supabaseCheckId) { continue }
          foreach ($sc in @($check.appliedServiceCharges)) {
            if (-not $sc.guid) { continue }
            $batch += @{
              toast_service_charge_guid  = $sc.guid
              check_id                   = $supabaseCheckId
              order_id                   = $supabaseOrderId
              location_id                = $locationId
              service_charge_config_guid = if ($sc.serviceCharge.guid) { $sc.serviceCharge.guid } else { $null }
              name                       = if ($sc.name) { $sc.name } else { $null }
              charge_type                = if ($sc.chargeType) { $sc.chargeType } else { $null }
              charge_amount              = if ($null -ne $sc.chargeAmount) { $sc.chargeAmount } else { $null }
              service_charge_category    = if ($sc.serviceChargeCategory) { $sc.serviceChargeCategory } else { $null }
              is_delivery                = if ($null -ne $sc.delivery) { $sc.delivery } else { $false }
              is_takeout                 = if ($null -ne $sc.takeout) { $sc.takeout } else { $false }
              is_dine_in                 = if ($null -ne $sc.dineIn) { $sc.dineIn } else { $false }
              is_gratuity                = if ($null -ne $sc.gratuity) { $sc.gratuity } else { $false }
              is_taxable                 = if ($null -ne $sc.taxable) { $sc.taxable } else { $false }
              updated_at                 = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            }
          }
        }
      }
      $written = Write-ToSupabase -batch $batch -table "toast_service_charges" -conflict "toast_service_charge_guid" -batchSize 100
      $daySvcChargeTotal += $written
    } catch {
      $script:HadErrors = $true
      Write-Log "  ERROR $($loc.name) service charges: $(Get-ToastErrorDetail $_)" Red
    }
  }
  Write-Log "  Service Charges: $daySvcChargeTotal" Green
  $grandTotalSvcCharges += $daySvcChargeTotal

  # ============================================================
  # 7. TOAST SHIFTS
  # ============================================================
  Write-Log "--- Shifts ---" Cyan
  $empIdMap = @{}
  $offset = 0
  do {
    $empResponse = Invoke-RestMethod -Uri "$supabaseUrl/rest/v1/employees?select=id,location_id,toast_employee_guid&limit=1000&offset=$offset" -Headers $supabaseHeaders
    foreach ($e in $empResponse) { $empIdMap["$($e.location_id)-$($e.toast_employee_guid)"] = $e.id }
    $offset += 1000
  } while ($empResponse.Count -eq 1000)

  $jobIdMap = @{}
  $offset = 0
  do {
    $jobResponse = Invoke-RestMethod -Uri "$supabaseUrl/rest/v1/jobs?select=id,location_id,toast_job_guid&limit=1000&offset=$offset" -Headers $supabaseHeaders
    foreach ($j in $jobResponse) { $jobIdMap["$($j.location_id)-$($j.toast_job_guid)"] = $j.id }
    $offset += 1000
  } while ($jobResponse.Count -eq 1000)

  $dayShiftTotal = 0
  foreach ($loc in $locations) {
    $locationId = $locationIdMap[$loc.guid]
    if (-not $locationId) { Write-Log "  ERROR $($loc.name): missing locations.id mapping for Toast GUID $($loc.guid)" Red; $script:HadErrors = $true; continue }
    try {
      $shifts = Invoke-WithRetry -Label "$($loc.name) shifts" -ScriptBlock {
        Invoke-RestMethod -Uri "$toastApiUrl/labor/v1/shifts?startDate=$startUtc&endDate=$endUtc" -Headers @{
          Authorization                  = "Bearer $token"
          "Toast-Restaurant-External-ID" = $loc.guid
        }
      }
      $batch = @()
      foreach ($shift in $shifts) {
        if (-not $shift.guid) { continue }
        $batch += @{
          toast_shift_guid     = $shift.guid
          location_id          = $locationId
          employee_id          = $empIdMap["$locationId-$($shift.employeeReference.guid)"]
          job_id               = $jobIdMap["$locationId-$($shift.jobReference.guid)"]
          toast_employee_guid  = if ($shift.employeeReference.guid) { $shift.employeeReference.guid } else { $null }
          toast_job_guid       = if ($shift.jobReference.guid) { $shift.jobReference.guid } else { $null }
          external_id          = if ($shift.externalId) { $shift.externalId } else { $null }
          in_date              = if ($shift.inDate) { $shift.inDate } else { $null }
          out_date             = if ($shift.outDate) { $shift.outDate } else { $null }
          deleted              = if ($null -ne $shift.deleted) { $shift.deleted } else { $false }
          created_date         = if ($shift.createdDate) { $shift.createdDate } else { $null }
          modified_date        = if ($shift.modifiedDate) { $shift.modifiedDate } else { $null }
        }
      }
      $written = Write-ToSupabase $batch "toast_shifts" "location_id,toast_shift_guid"
      $dayShiftTotal += $written
    } catch {
      $script:HadErrors = $true
      Write-Log "  ERROR $($loc.name) shifts: $(Get-ToastErrorDetail $_)" Red
    }
  }
  Write-Log "  Shifts: $dayShiftTotal" Green
  $grandTotalShifts += $dayShiftTotal

  # ============================================================
  # 8. TOAST TIME ENTRIES
  # ============================================================
  Write-Log "--- Time Entries ---" Cyan
  $dayTimeEntryTotal = 0
  foreach ($loc in $locations) {
    $locationId = $locationIdMap[$loc.guid]
    if (-not $locationId) { Write-Log "  ERROR $($loc.name): missing locations.id mapping for Toast GUID $($loc.guid)" Red; $script:HadErrors = $true; continue }
    try {
      $entries = Invoke-WithRetry -Label "$($loc.name) timeEntries" -ScriptBlock {
        Invoke-RestMethod -Uri "$toastApiUrl/labor/v1/timeEntries?modifiedStartDate=$startUtc&modifiedEndDate=$endUtc" -Headers @{
          Authorization                  = "Bearer $token"
          "Toast-Restaurant-External-ID" = $loc.guid
        }
      }
      $batch = @()
      foreach ($entry in $entries) {
        if (-not $entry.guid) { continue }
        $breaksJson = $null
        if ($entry.breaks -and $entry.breaks.Count -gt 0) {
          $breaksJson = $entry.breaks | ConvertTo-Json -Compress -Depth 5
        }
        $batch += @{
          toast_time_entry_guid             = $entry.guid
          location_id                       = $locationId
          employee_id                       = $empIdMap["$locationId-$($entry.employeeReference.guid)"]
          job_id                            = $jobIdMap["$locationId-$($entry.jobReference.guid)"]
          toast_employee_guid               = if ($entry.employeeReference.guid) { $entry.employeeReference.guid } else { $null }
          toast_job_guid                    = if ($entry.jobReference.guid) { $entry.jobReference.guid } else { $null }
          toast_shift_guid                  = if ($entry.shiftReference.guid) { $entry.shiftReference.guid } else { $null }
          in_date                           = if ($entry.inDate) { $entry.inDate } else { $null }
          out_date                          = if ($entry.outDate) { $entry.outDate } else { $null }
          business_date                     = if ($entry.businessDate) { $entry.businessDate } else { $null }
          auto_clocked_out                  = if ($null -ne $entry.autoClockedOut) { $entry.autoClockedOut } else { $false }
          regular_hours                     = if ($null -ne $entry.regularHours) { $entry.regularHours } else { $null }
          overtime_hours                    = if ($null -ne $entry.overtimeHours) { $entry.overtimeHours } else { $null }
          hourly_wage                       = if ($null -ne $entry.hourlyWage) { $entry.hourlyWage } else { $null }
          declared_cash_tips                = if ($null -ne $entry.declaredCashTips) { $entry.declaredCashTips } else { $null }
          non_cash_tips                     = if ($null -ne $entry.nonCashTips) { $entry.nonCashTips } else { $null }
          cash_gratuity_service_charges     = if ($null -ne $entry.cashGratuityServiceCharges) { $entry.cashGratuityServiceCharges } else { $null }
          non_cash_gratuity_service_charges = if ($null -ne $entry.nonCashGratuityServiceCharges) { $entry.nonCashGratuityServiceCharges } else { $null }
          tips_withheld                     = if ($null -ne $entry.tipsWithheld) { $entry.tipsWithheld } else { $null }
          non_cash_sales                    = if ($null -ne $entry.nonCashSales) { $entry.nonCashSales } else { $null }
          cash_sales                        = if ($null -ne $entry.cashSales) { $entry.cashSales } else { $null }
          breaks                            = $breaksJson
          deleted                           = if ($null -ne $entry.deleted) { $entry.deleted } else { $false }
          created_date                      = if ($entry.createdDate) { $entry.createdDate } else { $null }
          modified_date                     = if ($entry.modifiedDate) { $entry.modifiedDate } else { $null }
        }
      }
      $written = Write-ToSupabase $batch "toast_time_entries" "location_id,toast_time_entry_guid"
      $dayTimeEntryTotal += $written
    } catch {
      $script:HadErrors = $true
      Write-Log "  ERROR $($loc.name) time entries: $(Get-ToastErrorDetail $_)" Red
    }
  }
  Write-Log "  Time Entries: $dayTimeEntryTotal" Green
  $grandTotalTimeEntries += $dayTimeEntryTotal

  # ============================================================
  # 9. CASH ENTRIES + DEPOSITS
  # ============================================================
  Write-Log "--- Cash ---" Cyan
  $dayCashEntryTotal  = 0
  $dayDepositTotal    = 0
  foreach ($loc in $locations) {
    $locationId = $locationIdMap[$loc.guid]
    if (-not $locationId) { Write-Log "  ERROR $($loc.name): missing locations.id mapping for Toast GUID $($loc.guid)" Red; $script:HadErrors = $true; continue }
    try {
      $locCashEntryTotal = 0
      $locDepositTotal = 0
      foreach ($cashBusinessDate in $cashBusinessDates) {
        $entries = Invoke-WithRetry -Label "$($loc.name) cashEntries $cashBusinessDate" -ScriptBlock {
          Invoke-RestMethod -Uri "$toastApiUrl/cashmgmt/v1/entries?businessDate=$cashBusinessDate" -Headers @{
            Authorization                  = "Bearer $token"
            "Toast-Restaurant-External-ID" = $loc.guid
          }
        }
        $entryBatch = @()
        foreach ($entry in @($entries)) {
          if (-not $entry.guid) { continue }
          $entryBatch += @{
            toast_cash_entry_guid = $entry.guid
            location_id           = $locationId
            amount                = if ($null -ne $entry.amount) { $entry.amount } else { $null }
            reason                = if ($entry.reason) { $entry.reason } else { $null }
            entry_date            = if ($entry.date) { $entry.date } else { $null }
            entry_type            = if ($entry.type) { $entry.type } else { $null }
            business_date         = $cashBusinessDate
            cash_drawer_guid      = if ($entry.cashDrawer.guid) { $entry.cashDrawer.guid } else { $null }
            payout_reason_guid    = if ($entry.payoutReason.guid) { $entry.payoutReason.guid } else { $null }
            no_sale_reason_guid   = if ($entry.noSaleReason.guid) { $entry.noSaleReason.guid } else { $null }
            undoes                = if ($entry.undoes) { $entry.undoes } else { $null }
            employee1_guid        = if ($entry.employee1.guid) { $entry.employee1.guid } else { $null }
            employee2_guid        = if ($entry.employee2.guid) { $entry.employee2.guid } else { $null }
          }
        }
        $writtenE = Write-ToSupabase $entryBatch "cash_entries" "location_id,toast_cash_entry_guid"
        $locCashEntryTotal += $writtenE
        $dayCashEntryTotal += $writtenE

        $deposits = Invoke-WithRetry -Label "$($loc.name) deposits $cashBusinessDate" -ScriptBlock {
          Invoke-RestMethod -Uri "$toastApiUrl/cashmgmt/v1/deposits?businessDate=$cashBusinessDate" -Headers @{
            Authorization                  = "Bearer $token"
            "Toast-Restaurant-External-ID" = $loc.guid
          }
        }
        $depositBatch = @()
        foreach ($deposit in @($deposits)) {
          if (-not $deposit.guid) { continue }
          $depositBatch += @{
            toast_deposit_guid = $deposit.guid
            location_id        = $locationId
            business_date      = $cashBusinessDate
            amount             = if ($null -ne $deposit.amount) { $deposit.amount } else { $null }
            deposit_date       = if ($deposit.date) { $deposit.date } else { $null }
            undoes             = if ($deposit.undoes) { $deposit.undoes } else { $null }
            employee_guid      = if ($deposit.employee.guid) { $deposit.employee.guid } else { $null }
            creator_guid       = if ($deposit.creator.guid) { $deposit.creator.guid } else { $null }
          }
        }
        $writtenD = Write-ToSupabase $depositBatch "cash_deposits" "location_id,toast_deposit_guid"
        $locDepositTotal += $writtenD
        $dayDepositTotal += $writtenD
      }
      Write-Log "  $($loc.name): $locCashEntryTotal cash entries, $locDepositTotal deposits" Cyan
    } catch {
      $script:HadErrors = $true
      Write-Log "  ERROR $($loc.name) cash: $(Get-ToastErrorDetail $_)" Red
    }
  }
  Write-Log "  Cash Entries: $dayCashEntryTotal | Deposits: $dayDepositTotal" Green
  $grandTotalCashEntries += $dayCashEntryTotal
  $grandTotalDeposits    += $dayDepositTotal


# ============================================================
# GRAND TOTALS
# ============================================================
Write-Log "============================================"
Write-Log "Gastamo Daily Sync Complete"
Write-Log "============================================"
Write-Log "Orders:         $grandTotalOrders"
Write-Log "Checks:         $grandTotalChecks"
Write-Log "Order Items:    $grandTotalItems"
Write-Log "Payments:       $grandTotalPayments"
Write-Log "Discounts:      $grandTotalDiscounts"
Write-Log "Svc Charges:    $grandTotalSvcCharges"
Write-Log "Shifts:         $grandTotalShifts"
Write-Log "Time Entries:   $grandTotalTimeEntries"
Write-Log "Cash Entries:   $grandTotalCashEntries"
Write-Log "Deposits:       $grandTotalDeposits"
Write-Log "============================================"
if ($script:HadErrors) {
  Write-Log "Daily sync completed with one or more errors. Failing workflow to prevent silent data loss." Red
  exit 1
}
