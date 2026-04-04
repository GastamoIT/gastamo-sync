# ============================================================
# Gastamo Group — Daily Sync Master Script
# Runs every morning at 6:00 AM via GitHub Actions
# ============================================================

# CREDENTIALS — loaded from GitHub Actions secrets
$clientId     = $env:TOAST_CLIENT_ID
$clientSecret = $env:TOAST_CLIENT_SECRET
$toastApiUrl  = $env:TOAST_API_URL
$supabaseUrl  = $env:SUPABASE_URL
$supabaseKey  = $env:SUPABASE_KEY

# LOG FILE — GitHub Actions compatible path
$logDir  = "logs"
$logFile = "$logDir/sync-$(Get-Date -Format 'yyyy-MM-dd').log"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

function Write-Log($message, $color = "White") {
  $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[$timestamp] $message"
  Add-Content -Path $logFile -Value $line
  Write-Host $line -ForegroundColor $color
}

Write-Log "============================================"
Write-Log "Gastamo Daily Sync Started"
Write-Log "============================================"

# ============================================================
# CALCULATE DATE RANGE
# Yesterday in Mountain Time = UTC-7 (MDT) or UTC-6 (MST)
# Business day: yesterday 07:00 UTC to today 07:00 UTC
# ============================================================
$yesterday    = (Get-Date).AddDays(-1).ToString("yyyyMMdd")
$businessDate = $yesterday
$startUtc     = [Uri]::EscapeDataString("$(Get-Date -Format 'yyyy-MM-dd' -Date (Get-Date).AddDays(-0))T07:00:00.000+0000")
$endUtc       = [Uri]::EscapeDataString("$(Get-Date -Format 'yyyy-MM-dd' -Date (Get-Date).AddDays(1))T07:00:00.000+0000")

Write-Log "Business date: $businessDate"
Write-Log "UTC range: $startUtc to $endUtc"

# ============================================================
# AUTH
# ============================================================
try {
  $authBody = @{ clientId = $clientId; clientSecret = $clientSecret; userAccessType = "TOAST_MACHINE_CLIENT" } | ConvertTo-Json
  $authResponse = Invoke-RestMethod -Uri "$toastApiUrl/authentication/v1/authentication/login" -Method POST -Body $authBody -ContentType "application/json"
  $token = $authResponse.token.accessToken
  Write-Log "Toast token acquired" Green
} catch {
  Write-Log "FATAL: Toast auth failed — $($_.Exception.Message)" Red
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

# ============================================================
# HELPER FUNCTIONS
# ============================================================
function Get-NextPageUrl($linkHeader) {
  if (-not $linkHeader) { return $null }
  foreach ($part in $linkHeader -split ",") {
    if ($part -match '<([^>]+)>;\s*rel="next"') { return $matches[1] }
  }
  return $null
}

function Write-ToSupabase($batch, $table, $conflict) {
  if ($batch.Count -eq 0) { return 0 }
  $written = 0
  for ($j = 0; $j -lt $batch.Count; $j += 50) {
    $slice = $batch[$j..([Math]::Min($j+49, $batch.Count-1))]
    $json = $slice | ConvertTo-Json -Depth 5
    if ($slice.Count -eq 1) { $json = "[$json]" }
    $url = "$supabaseUrl/rest/v1/$table"
    if ($conflict) { $url += "?on_conflict=$conflict" }
    try {
      Invoke-RestMethod -Uri $url -Method POST -Headers $supabaseHeaders -Body $json | Out-Null
      $written += $slice.Count
    } catch {
      $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
      $reader.BaseStream.Position = 0
      Write-Log "  SUPABASE ERROR on $table : $($reader.ReadToEnd())" Red
    }
  }
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

# ============================================================
# 1. TOAST ORDERS
# ============================================================
Write-Log "--- Syncing toast_orders ---" Cyan
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $allOrders = @()
    $url = "$toastApiUrl/orders/v2/ordersBulk?startDate=$startUtc&endDate=$endUtc&pageSize=100"
    do {
      $response = Invoke-WebRequest -Uri $url -UseBasicParsing -Headers @{
        Authorization                  = "Bearer $token"
        "Toast-Restaurant-External-ID" = $loc.guid
      }
      $allOrders += ($response.Content | ConvertFrom-Json)
      $url = Get-NextPageUrl $response.Headers["link"]
    } while ($url)

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
    $written = Write-ToSupabase $batch "toast_orders" "location_id,toast_order_guid"
    $grandTotal += $written
    Write-Log "  $($loc.name): $written orders"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "toast_orders total: $grandTotal" Green

# ============================================================
# 2. TOAST CHECKS
# ============================================================
Write-Log "--- Syncing toast_checks ---" Cyan

$orderIdMap = @{}
$offset = 0
do {
  $orderResponse = Invoke-RestMethod -Uri "$supabaseUrl/rest/v1/toast_orders?select=id,toast_order_guid&limit=1000&offset=$offset" -Headers $supabaseHeaders
  foreach ($o in $orderResponse) { $orderIdMap[$o.toast_order_guid] = $o.id }
  $offset += 1000
} while ($orderResponse.Count -eq 1000)

$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $allOrders = @()
    $url = "$toastApiUrl/orders/v2/ordersBulk?startDate=$startUtc&endDate=$endUtc&pageSize=100"
    do {
      $response = Invoke-WebRequest -Uri $url -UseBasicParsing -Headers @{
        Authorization                  = "Bearer $token"
        "Toast-Restaurant-External-ID" = $loc.guid
      }
      $allOrders += ($response.Content | ConvertFrom-Json)
      $url = Get-NextPageUrl $response.Headers["link"]
    } while ($url)

    $batch = @()
    foreach ($order in $allOrders) {
      if (-not $order.guid) { continue }
      $supabaseOrderId = $orderIdMap[$order.guid]
      if (-not $supabaseOrderId) { continue }
      foreach ($check in $order.checks) {
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
    $written = Write-ToSupabase $batch "toast_checks" "toast_check_guid"
    $grandTotal += $written
    Write-Log "  $($loc.name): $written checks"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "toast_checks total: $grandTotal" Green

# ============================================================
# 3. TOAST ORDER ITEMS
# ============================================================
Write-Log "--- Syncing toast_order_items ---" Cyan

$checkIdMap = @{}
$offset = 0
do {
  $checkResponse = Invoke-RestMethod -Uri "$supabaseUrl/rest/v1/toast_checks?select=id,toast_check_guid&limit=1000&offset=$offset" -Headers $supabaseHeaders
  foreach ($c in $checkResponse) { $checkIdMap[$c.toast_check_guid] = $c.id }
  $offset += 1000
} while ($checkResponse.Count -eq 1000)

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

$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $allOrders = @()
    $url = "$toastApiUrl/orders/v2/ordersBulk?startDate=$startUtc&endDate=$endUtc&pageSize=100"
    do {
      $response = Invoke-WebRequest -Uri $url -UseBasicParsing -Headers @{
        Authorization                  = "Bearer $token"
        "Toast-Restaurant-External-ID" = $loc.guid
      }
      $allOrders += ($response.Content | ConvertFrom-Json)
      $url = Get-NextPageUrl $response.Headers["link"]
    } while ($url)

    $allSelections = @()
    foreach ($order in $allOrders) {
      if (-not $order.guid) { continue }
      $supabaseOrderId = $orderIdMap[$order.guid]
      if (-not $supabaseOrderId) { continue }
      foreach ($check in $order.checks) {
        if (-not $check.guid) { continue }
        $supabaseCheckId = $checkIdMap[$check.guid]
        if (-not $supabaseCheckId) { continue }
        if ($check.selections -and $check.selections.Count -gt 0) {
          $allSelections += Get-AllSelections $check.selections $supabaseCheckId $supabaseOrderId $locationId $null
        }
      }
    }

    $topLevel  = @($allSelections | Where-Object { $null -eq $_['_parent_toast_guid'] })
    $modifiers = @($allSelections | Where-Object { $null -ne $_['_parent_toast_guid'] })

    $topLevelClean = Strip-HelperFields $topLevel
    Write-ToSupabase $topLevelClean "toast_order_items" "toast_selection_guid" | Out-Null

    $itemIdMap2 = @{}
    $offset2 = 0
    do {
      $itemResponse = Invoke-RestMethod -Uri "$supabaseUrl/rest/v1/toast_order_items?select=id,toast_selection_guid&limit=1000&offset=$offset2" -Headers $supabaseHeaders
      foreach ($item in $itemResponse) { $itemIdMap2[$item.toast_selection_guid] = $item.id }
      $offset2 += 1000
    } while ($itemResponse.Count -eq 1000)

    $resolvedModifiers = $modifiers | ForEach-Object {
      $_['parent_item_id'] = $itemIdMap2[$_['_parent_toast_guid']]
      $_
    }
    $resolvedClean = Strip-HelperFields $resolvedModifiers
    Write-ToSupabase $resolvedClean "toast_order_items" "toast_selection_guid" | Out-Null

    $locTotal = $topLevel.Count + $resolvedModifiers.Count
    $grandTotal += $locTotal
    Write-Log "  $($loc.name): $($topLevel.Count) items + $($resolvedModifiers.Count) modifiers"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "toast_order_items total: $grandTotal" Green

# ============================================================
# 4. TOAST PAYMENTS
# ============================================================
Write-Log "--- Syncing toast_payments ---" Cyan
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $allOrders = @()
    $url = "$toastApiUrl/orders/v2/ordersBulk?startDate=$startUtc&endDate=$endUtc&pageSize=100"
    do {
      $response = Invoke-WebRequest -Uri $url -UseBasicParsing -Headers @{
        Authorization                  = "Bearer $token"
        "Toast-Restaurant-External-ID" = $loc.guid
      }
      $allOrders += ($response.Content | ConvertFrom-Json)
      $url = Get-NextPageUrl $response.Headers["link"]
    } while ($url)

    $batch = @()
    foreach ($order in $allOrders) {
      if (-not $order.guid) { continue }
      $supabaseOrderId = $orderIdMap[$order.guid]
      if (-not $supabaseOrderId) { continue }
      foreach ($check in $order.checks) {
        if (-not $check.guid) { continue }
        $supabaseCheckId = $checkIdMap[$check.guid]
        if (-not $supabaseCheckId) { continue }
        foreach ($payment in $check.payments) {
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
    $written = Write-ToSupabase $batch "toast_payments" "toast_payment_guid"
    $grandTotal += $written
    Write-Log "  $($loc.name): $written payments"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "toast_payments total: $grandTotal" Green

# ============================================================
# 5. TOAST DISCOUNTS
# ============================================================
Write-Log "--- Syncing toast_discounts ---" Cyan
$itemIdMap = @{}
$offset = 0
do {
  $itemResponse = Invoke-RestMethod -Uri "$supabaseUrl/rest/v1/toast_order_items?select=id,toast_selection_guid&limit=1000&offset=$offset" -Headers $supabaseHeaders
  foreach ($i in $itemResponse) { $itemIdMap[$i.toast_selection_guid] = $i.id }
  $offset += 1000
} while ($itemResponse.Count -eq 1000)

$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $allOrders = @()
    $url = "$toastApiUrl/orders/v2/ordersBulk?startDate=$startUtc&endDate=$endUtc&pageSize=100"
    do {
      $response = Invoke-WebRequest -Uri $url -UseBasicParsing -Headers @{
        Authorization                  = "Bearer $token"
        "Toast-Restaurant-External-ID" = $loc.guid
      }
      $allOrders += ($response.Content | ConvertFrom-Json)
      $url = Get-NextPageUrl $response.Headers["link"]
    } while ($url)

    $batch = @()
    foreach ($order in $allOrders) {
      if (-not $order.guid) { continue }
      $supabaseOrderId = $orderIdMap[$order.guid]
      if (-not $supabaseOrderId) { continue }
      foreach ($check in $order.checks) {
        if (-not $check.guid) { continue }
        $supabaseCheckId = $checkIdMap[$check.guid]
        if (-not $supabaseCheckId) { continue }
        foreach ($discount in $check.appliedDiscounts) {
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
        foreach ($sel in $check.selections) {
          if (-not $sel.guid) { continue }
          $supabaseItemId = $itemIdMap[$sel.guid]
          foreach ($discount in $sel.appliedDiscounts) {
            if (-not $discount.guid) { continue }
            $batch += @{
              toast_discount_guid  = $discount.guid
              check_id             = $supabaseCheckId
              order_item_id        = $supabaseItemId
              order_id             = $supabaseOrderId
              location_id          = $locationId
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
        }
      }
    }
    $written = Write-ToSupabase $batch "toast_discounts" "toast_discount_guid"
    $grandTotal += $written
    Write-Log "  $($loc.name): $written discounts"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "toast_discounts total: $grandTotal" Green

# ============================================================
# 6. TOAST SERVICE CHARGES
# ============================================================
Write-Log "--- Syncing toast_service_charges ---" Cyan
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $allOrders = @()
    $url = "$toastApiUrl/orders/v2/ordersBulk?startDate=$startUtc&endDate=$endUtc&pageSize=100"
    do {
      $response = Invoke-WebRequest -Uri $url -UseBasicParsing -Headers @{
        Authorization                  = "Bearer $token"
        "Toast-Restaurant-External-ID" = $loc.guid
      }
      $allOrders += ($response.Content | ConvertFrom-Json)
      $url = Get-NextPageUrl $response.Headers["link"]
    } while ($url)

    $batch = @()
    foreach ($order in $allOrders) {
      if (-not $order.guid) { continue }
      $supabaseOrderId = $orderIdMap[$order.guid]
      if (-not $supabaseOrderId) { continue }
      foreach ($check in $order.checks) {
        if (-not $check.guid) { continue }
        $supabaseCheckId = $checkIdMap[$check.guid]
        if (-not $supabaseCheckId) { continue }
        foreach ($sc in $check.appliedServiceCharges) {
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
    $written = Write-ToSupabase $batch "toast_service_charges" "toast_service_charge_guid"
    $grandTotal += $written
    Write-Log "  $($loc.name): $written service charges"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "toast_service_charges total: $grandTotal" Green

# ============================================================
# 7. TOAST SHIFTS
# ============================================================
Write-Log "--- Syncing toast_shifts ---" Cyan
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

$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $shifts = Invoke-RestMethod -Uri "$toastApiUrl/labor/v1/shifts?startDate=$startUtc&endDate=$endUtc" -Headers @{
      Authorization                  = "Bearer $token"
      "Toast-Restaurant-External-ID" = $loc.guid
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
    $grandTotal += $written
    Write-Log "  $($loc.name): $written shifts"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "toast_shifts total: $grandTotal" Green

# ============================================================
# 8. TOAST TIME ENTRIES
# ============================================================
Write-Log "--- Syncing toast_time_entries ---" Cyan
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $entries = Invoke-RestMethod -Uri "$toastApiUrl/labor/v1/timeEntries?businessDate=$businessDate" -Headers @{
      Authorization                  = "Bearer $token"
      "Toast-Restaurant-External-ID" = $loc.guid
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
    $grandTotal += $written
    Write-Log "  $($loc.name): $written time entries"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "toast_time_entries total: $grandTotal" Green

# ============================================================
# 9. CASH ENTRIES + DEPOSITS
# ============================================================
Write-Log "--- Syncing cash_entries + cash_deposits ---" Cyan
$totalEntries  = 0
$totalDeposits = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $entries = Invoke-RestMethod -Uri "$toastApiUrl/cashmgmt/v1/entries?businessDate=$businessDate" -Headers @{
      Authorization                  = "Bearer $token"
      "Toast-Restaurant-External-ID" = $loc.guid
    }
    $entryBatch = @()
    foreach ($entry in $entries) {
      if (-not $entry.guid) { continue }
      $entryBatch += @{
        toast_cash_entry_guid = $entry.guid
        location_id           = $locationId
        amount                = if ($null -ne $entry.amount) { $entry.amount } else { $null }
        reason                = if ($entry.reason) { $entry.reason } else { $null }
        entry_date            = if ($entry.date) { $entry.date } else { $null }
        entry_type            = if ($entry.type) { $entry.type } else { $null }
        business_date         = $businessDate
        cash_drawer_guid      = if ($entry.cashDrawer.guid) { $entry.cashDrawer.guid } else { $null }
        payout_reason_guid    = if ($entry.payoutReason.guid) { $entry.payoutReason.guid } else { $null }
        no_sale_reason_guid   = if ($entry.noSaleReason.guid) { $entry.noSaleReason.guid } else { $null }
        undoes                = if ($entry.undoes) { $entry.undoes } else { $null }
        employee1_guid        = if ($entry.employee1.guid) { $entry.employee1.guid } else { $null }
        employee2_guid        = if ($entry.employee2.guid) { $entry.employee2.guid } else { $null }
      }
    }
    $writtenE = Write-ToSupabase $entryBatch "cash_entries" "location_id,toast_cash_entry_guid"
    $totalEntries += $writtenE

    $deposits = Invoke-RestMethod -Uri "$toastApiUrl/cashmgmt/v1/deposits?businessDate=$businessDate" -Headers @{
      Authorization                  = "Bearer $token"
      "Toast-Restaurant-External-ID" = $loc.guid
    }
    $depositBatch = @()
    foreach ($deposit in $deposits) {
      if (-not $deposit.guid) { continue }
      $depositBatch += @{
        toast_deposit_guid   = $deposit.guid
        location_id          = $locationId
        business_date        = $businessDate
        amount               = if ($null -ne $deposit.amount) { $deposit.amount } else { $null }
        deposit_date         = if ($deposit.date) { $deposit.date } else { $null }
        undoes               = if ($deposit.undoes) { $deposit.undoes } else { $null }
        employee_guid        = if ($deposit.employee.guid) { $deposit.employee.guid } else { $null }
        creator_guid         = if ($deposit.creator.guid) { $deposit.creator.guid } else { $null }
      }
    }
    $writtenD = Write-ToSupabase $depositBatch "cash_deposits" "location_id,toast_deposit_guid"
    $totalDeposits += $writtenD

    Write-Log "  $($loc.name): $writtenE entries, $writtenD deposits"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "cash_entries total: $totalEntries | cash_deposits total: $totalDeposits" Green

# ============================================================
# DONE
# ============================================================
Write-Log "============================================"
Write-Log "Gastamo Daily Sync Complete"
Write-Log "============================================"
