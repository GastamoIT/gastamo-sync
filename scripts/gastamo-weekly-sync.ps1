# ============================================================
# Gastamo Group — Weekly Config Sync
# Runs every Sunday at 2:00 AM via GitHub Actions
# Updates all reference/config tables 1-26
# ============================================================

# CREDENTIALS — loaded from GitHub Actions secrets
$clientId     = $env:TOAST_CLIENT_ID
$clientSecret = $env:TOAST_CLIENT_SECRET
$toastApiUrl  = $env:TOAST_API_URL
$supabaseUrl  = $env:SUPABASE_URL
$supabaseKey  = $env:SUPABASE_KEY

# LOG FILE — GitHub Actions compatible path
$logDir  = "logs"
$logFile = "$logDir/weekly-sync-$(Get-Date -Format 'yyyy-MM-dd').log"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

function Write-Log($message, $color = "White") {
  $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[$timestamp] $message"
  Add-Content -Path $logFile -Value $line
  Write-Host $line -ForegroundColor $color
}

Write-Log "============================================"
Write-Log "Gastamo Weekly Config Sync Started"
Write-Log "============================================"

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
# HELPERS
# ============================================================
function Write-ToSupabase($batch, $table, $conflict) {
  if (-not $batch -or $batch.Count -eq 0) { return 0 }
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

function Sync-SimpleConfigTable($endpoint, $table, $conflictKey, $mapRow) {
  $grandTotal = 0
  foreach ($loc in $locations) {
    $locationId = $locationIdMap[$loc.guid]
    if (-not $locationId) { continue }
    try {
      $items = Invoke-RestMethod -Uri "$toastApiUrl$endpoint" -Headers @{
        Authorization                  = "Bearer $token"
        "Toast-Restaurant-External-ID" = $loc.guid
      }
      $batch = @()
      foreach ($item in $items) {
        if (-not $item.guid) { continue }
        $row = & $mapRow $item $locationId
        $batch += $row
      }
      $written = Write-ToSupabase $batch $table $conflictKey
      $grandTotal += $written
      Write-Log "  $($loc.name): $written rows"
    } catch {
      Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
    }
    Start-Sleep -Milliseconds 500
  }
  return $grandTotal
}

# ============================================================
# 1. LOCATIONS
# ============================================================
Write-Log "--- Syncing locations ---" Cyan
try {
  $batch = @()
  foreach ($loc in $locations) {
    try {
      $info = Invoke-RestMethod -Uri "$toastApiUrl/restaurants/v1/restaurants/$($loc.guid)" -Headers @{
        Authorization                  = "Bearer $token"
        "Toast-Restaurant-External-ID" = $loc.guid
      }
      $general = $info.general
      $batch += @{
        toast_restaurant_guid = $loc.guid
        name                  = if ($general.name) { $general.name } else { $null }
        store_number          = if ($general.locationCode) { $general.locationCode } else { $null }
        address1              = if ($info.location.address1) { $info.location.address1 } else { $null }
        address2              = if ($info.location.address2) { $info.location.address2 } else { $null }
        city                  = if ($info.location.city) { $info.location.city } else { $null }
        state                 = if ($info.location.state) { $info.location.state } else { $null }
        zip                   = if ($info.location.zip) { $info.location.zip } else { $null }
        phone                 = if ($general.phone) { $general.phone } else { $null }
        timezone              = if ($general.timeZone) { $general.timeZone } else { $null }
        closeout_hour         = if ($null -ne $general.closeoutHour) { $general.closeoutHour } else { $null }
        updated_at            = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
      }
    } catch {
      Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
    }
    Start-Sleep -Milliseconds 500
  }
  $written = Write-ToSupabase $batch "locations" "toast_restaurant_guid"
  Write-Log "locations total: $written" Green
} catch {
  Write-Log "ERROR syncing locations: $($_.Exception.Message)" Red
}

# ============================================================
# 2. EMPLOYEES
# ============================================================
Write-Log "--- Syncing employees ---" Cyan
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $employees = Invoke-RestMethod -Uri "$toastApiUrl/labor/v1/employees" -Headers @{
      Authorization                  = "Bearer $token"
      "Toast-Restaurant-External-ID" = $loc.guid
    }
    $batch = @()
    foreach ($emp in $employees) {
      if (-not $emp.guid) { continue }
      $batch += @{
        toast_employee_guid = $emp.guid
        location_id         = $locationId
        external_id         = if ($emp.externalId) { $emp.externalId } else { $null }
        first_name          = if ($emp.firstName) { $emp.firstName } else { $null }
        last_name           = if ($emp.lastName) { $emp.lastName } else { $null }
        chosen_name         = if ($emp.chosenName) { $emp.chosenName } else { $null }
        email               = if ($emp.email) { $emp.email } else { $null }
        phone_mobile        = if ($emp.phoneMobile) { $emp.phoneMobile } else { $null }
        deleted             = if ($null -ne $emp.deleted) { $emp.deleted } else { $false }
        disabled            = if ($null -ne $emp.disabled) { $emp.disabled } else { $false }
        archived            = if ($null -ne $emp.archived) { $emp.archived } else { $false }
        created_date        = if ($emp.createdDate) { $emp.createdDate } else { $null }
        modified_date       = if ($emp.modifiedDate) { $emp.modifiedDate } else { $null }
        deleted_date        = if ($emp.deletedDate) { $emp.deletedDate } else { $null }
        updated_at          = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
      }
    }
    $written = Write-ToSupabase $batch "employees" "location_id,toast_employee_guid"
    $grandTotal += $written
    Write-Log "  $($loc.name): $written employees"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "employees total: $grandTotal" Green

# ============================================================
# 3. JOBS
# ============================================================
Write-Log "--- Syncing jobs ---" Cyan
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $jobs = Invoke-RestMethod -Uri "$toastApiUrl/labor/v1/jobs" -Headers @{
      Authorization                  = "Bearer $token"
      "Toast-Restaurant-External-ID" = $loc.guid
    }
    $batch = @()
    foreach ($job in $jobs) {
      if (-not $job.guid) { continue }
      $batch += @{
        toast_job_guid = $job.guid
        location_id    = $locationId
        external_id    = if ($job.externalId) { $job.externalId } else { $null }
        title          = if ($job.title) { $job.title } else { $null }
        code           = if ($job.code) { $job.code } else { $null }
        default_wage   = if ($null -ne $job.defaultWage) { $job.defaultWage } else { $null }
        tipped         = if ($null -ne $job.tipped) { $job.tipped } else { $false }
        deleted        = if ($null -ne $job.deleted) { $job.deleted } else { $false }
        created_date   = if ($job.createdDate) { $job.createdDate } else { $null }
        modified_date  = if ($job.modifiedDate) { $job.modifiedDate } else { $null }
        updated_at     = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
      }
    }
    $written = Write-ToSupabase $batch "jobs" "location_id,toast_job_guid"
    $grandTotal += $written
    Write-Log "  $($loc.name): $written jobs"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 1
}
Write-Log "jobs total: $grandTotal" Green

# ============================================================
# 4-10. MENUS
# ============================================================
Write-Log "--- Syncing menus ---" Cyan
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $menuData = Invoke-RestMethod -Uri "$toastApiUrl/menus/v2/menus" -Headers @{
      Authorization                  = "Bearer $token"
      "Toast-Restaurant-External-ID" = $loc.guid
    }

    $menuBatch        = @()
    $groupBatch       = @()
    $itemBatch        = @()
    $modGroupBatch    = @()
    $modOptionBatch   = @()
    $preModGroupBatch = @()
    $preModBatch      = @()

    foreach ($menu in $menuData) {
      if (-not $menu.guid) { continue }
      $menuBatch += @{
        toast_menu_guid  = $menu.guid
        location_id      = $locationId
        name             = if ($menu.name) { $menu.name } else { $null }
        description      = if ($menu.description) { $menu.description } else { $null }
        orderable_online = if ($null -ne $menu.orderableOnline) { $menu.orderableOnline } else { $null }
        updated_at       = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
      }

      foreach ($group in $menu.menuGroups) {
        if (-not $group.guid) { continue }
        $groupBatch += @{
          toast_menu_group_guid = $group.guid
          location_id           = $locationId
          toast_menu_guid       = $menu.guid
          name                  = if ($group.name) { $group.name } else { $null }
          updated_at            = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        }

        foreach ($item in $group.menuItems) {
          if (-not $item.guid) { continue }
          $itemBatch += @{
            toast_menu_item_guid = $item.guid
            location_id          = $locationId
            toast_menu_group_id  = $null
            menu_group_guid      = $group.guid
            name                 = if ($item.name) { $item.name } else { $null }
            description          = if ($item.description) { $item.description } else { $null }
            sku                  = if ($item.sku) { $item.sku } else { $null }
            price                = if ($null -ne $item.price) { $item.price } else { $null }
            plu                  = if ($item.plu) { $item.plu } else { $null }
            unit_of_measure      = if ($item.unitOfMeasure) { $item.unitOfMeasure } else { $null }
            updated_at           = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
          }
        }

        foreach ($modGroup in $group.modifierGroups) {
          if (-not $modGroup.guid) { continue }
          $modGroupBatch += @{
            toast_modifier_group_guid = $modGroup.guid
            location_id               = $locationId
            name                      = if ($modGroup.name) { $modGroup.name } else { $null }
            min_selections            = if ($null -ne $modGroup.minSelections) { $modGroup.minSelections } else { $null }
            max_selections            = if ($null -ne $modGroup.maxSelections) { $modGroup.maxSelections } else { $null }
            updated_at                = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
          }

          foreach ($option in $modGroup.modifierOptions) {
            if (-not $option.guid) { continue }
            $modOptionBatch += @{
              toast_modifier_option_guid = $option.guid
              location_id                = $locationId
              toast_modifier_group_guid  = $modGroup.guid
              name                       = if ($option.name) { $option.name } else { $null }
              price                      = if ($null -ne $option.price) { $option.price } else { $null }
              updated_at                 = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            }
          }
        }
      }

      foreach ($preModGroup in $menu.preModifierGroups) {
        if (-not $preModGroup.guid) { continue }
        $preModGroupBatch += @{
          toast_pre_modifier_group_guid = $preModGroup.guid
          location_id                   = $locationId
          name                          = if ($preModGroup.name) { $preModGroup.name } else { $null }
          updated_at                    = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        }

        foreach ($preMod in $preModGroup.preModifiers) {
          if (-not $preMod.guid) { continue }
          $preModBatch += @{
            toast_pre_modifier_guid       = $preMod.guid
            location_id                   = $locationId
            toast_pre_modifier_group_guid = $preModGroup.guid
            name                          = if ($preMod.name) { $preMod.name } else { $null }
            updated_at                    = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
          }
        }
      }
    }

    Write-ToSupabase $menuBatch        "menus"                "location_id,toast_menu_guid"                 | Out-Null
    Write-ToSupabase $groupBatch       "menu_groups"          "location_id,toast_menu_group_guid"           | Out-Null
    Write-ToSupabase $itemBatch        "menu_items"           "location_id,toast_menu_item_guid"            | Out-Null
    Write-ToSupabase $modGroupBatch    "modifier_groups"      "location_id,toast_modifier_group_guid"       | Out-Null
    Write-ToSupabase $modOptionBatch   "modifier_options"     "location_id,toast_modifier_option_guid"      | Out-Null
    Write-ToSupabase $preModGroupBatch "pre_modifier_groups"  "location_id,toast_pre_modifier_group_guid"   | Out-Null
    Write-ToSupabase $preModBatch      "pre_modifiers"        "location_id,toast_pre_modifier_guid"         | Out-Null

    $locTotal = $menuBatch.Count + $groupBatch.Count + $itemBatch.Count + $modGroupBatch.Count + $modOptionBatch.Count + $preModGroupBatch.Count + $preModBatch.Count
    $grandTotal += $locTotal
    Write-Log "  $($loc.name): $($menuBatch.Count) menus, $($groupBatch.Count) groups, $($itemBatch.Count) items, $($modGroupBatch.Count) mod groups, $($modOptionBatch.Count) mod options"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Seconds 2
}
Write-Log "menus total: $grandTotal rows across all menu tables" Green

# ============================================================
# 11. REVENUE CENTERS
# ============================================================
Write-Log "--- Syncing revenue_centers ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/revenueCenters" "revenue_centers" "location_id,toast_revenue_center_guid" {
  param($item, $locationId)
  @{
    toast_revenue_center_guid = $item.guid
    location_id               = $locationId
    entity_type               = if ($item.entityType) { $item.entityType } else { $null }
    name                      = if ($item.name) { $item.name } else { $null }
    updated_at                = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "revenue_centers total: $total" Green

# ============================================================
# 12. PREP STATIONS
# ============================================================
Write-Log "--- Syncing prep_stations ---" Cyan
$total = Sync-SimpleConfigTable "/kitchen/v1/prepStations" "prep_stations" "location_id,toast_prep_station_guid" {
  param($item, $locationId)
  @{
    toast_prep_station_guid = $item.guid
    location_id             = $locationId
    entity_type             = if ($item.entityType) { $item.entityType } else { $null }
    name                    = if ($item.name) { $item.name } else { $null }
    updated_at              = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "prep_stations total: $total" Green

# ============================================================
# 13. DINING OPTIONS
# ============================================================
Write-Log "--- Syncing dining_options ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/diningOptions" "dining_options" "location_id,toast_dining_option_guid" {
  param($item, $locationId)
  @{
    toast_dining_option_guid = $item.guid
    location_id              = $locationId
    toast_external_id        = if ($item.externalId) { $item.externalId } else { $null }
    entity_type              = if ($item.entityType) { $item.entityType } else { $null }
    name                     = if ($item.name) { $item.name } else { $null }
    behavior                 = if ($item.behavior) { $item.behavior } else { $null }
    curbside                 = if ($null -ne $item.curbside) { $item.curbside } else { $false }
    updated_at               = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "dining_options total: $total" Green

# ============================================================
# 14. SERVICE AREAS
# ============================================================
Write-Log "--- Syncing service_areas ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/serviceAreas" "service_areas" "location_id,toast_service_area_guid" {
  param($item, $locationId)
  @{
    toast_service_area_guid    = $item.guid
    location_id                = $locationId
    toast_external_id          = if ($item.externalId) { $item.externalId } else { $null }
    entity_type                = if ($item.entityType) { $item.entityType } else { $null }
    name                       = if ($item.name) { $item.name } else { $null }
    revenue_center_guid        = if ($item.revenueCenter.guid) { $item.revenueCenter.guid } else { $null }
    revenue_center_entity_type = if ($item.revenueCenter.entityType) { $item.revenueCenter.entityType } else { $null }
    updated_at                 = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "service_areas total: $total" Green

# ============================================================
# 15. RESTAURANT TABLES
# ============================================================
Write-Log "--- Syncing restaurant_tables ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/tables" "restaurant_tables" "location_id,toast_table_guid" {
  param($item, $locationId)
  @{
    toast_table_guid    = $item.guid
    location_id         = $locationId
    entity_type         = if ($item.entityType) { $item.entityType } else { $null }
    name                = if ($item.name) { $item.name } else { $null }
    service_area_guid   = if ($item.serviceArea.guid) { $item.serviceArea.guid } else { $null }
    revenue_center_guid = if ($item.revenueCenter.guid) { $item.revenueCenter.guid } else { $null }
    updated_at          = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "restaurant_tables total: $total" Green

# ============================================================
# 16. TAX RATES
# ============================================================
Write-Log "--- Syncing tax_rates ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/taxRates" "tax_rates" "location_id,toast_tax_rate_guid" {
  param($item, $locationId)
  @{
    toast_tax_rate_guid   = $item.guid
    location_id           = $locationId
    entity_type           = if ($item.entityType) { $item.entityType } else { $null }
    name                  = if ($item.name) { $item.name } else { $null }
    is_default            = if ($null -ne $item.isDefault) { $item.isDefault } else { $false }
    rate                  = if ($null -ne $item.rate) { $item.rate } else { $null }
    type                  = if ($item.type) { $item.type } else { $null }
    rounding_type         = if ($item.roundingType) { $item.roundingType } else { $null }
    tax_table             = if ($item.taxTable -and $item.taxTable.Count -gt 0) { $item.taxTable | ConvertTo-Json -Compress -Depth 3 } else { $null }
    conditional_tax_rates = if ($item.conditionalTaxRates -and $item.conditionalTaxRates.Count -gt 0) { $item.conditionalTaxRates | ConvertTo-Json -Compress -Depth 3 } else { $null }
    updated_at            = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "tax_rates total: $total" Green

# ============================================================
# 17. VOID REASONS
# ============================================================
Write-Log "--- Syncing void_reasons ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/voidReasons" "void_reasons" "location_id,toast_void_reason_guid" {
  param($item, $locationId)
  @{
    toast_void_reason_guid = $item.guid
    location_id            = $locationId
    entity_type            = if ($item.entityType) { $item.entityType } else { $null }
    name                   = if ($item.name) { $item.name } else { $null }
    updated_at             = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "void_reasons total: $total" Green

# ============================================================
# 18. NO SALE REASONS
# ============================================================
Write-Log "--- Syncing no_sale_reasons ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/noSaleReasons" "no_sale_reasons" "location_id,toast_no_sale_reason_guid" {
  param($item, $locationId)
  @{
    toast_no_sale_reason_guid = $item.guid
    location_id               = $locationId
    entity_type               = if ($item.entityType) { $item.entityType } else { $null }
    name                      = if ($item.name) { $item.name } else { $null }
    updated_at                = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "no_sale_reasons total: $total" Green

# ============================================================
# 19. PAYOUT REASONS
# ============================================================
Write-Log "--- Syncing payout_reasons ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/payoutReasons" "payout_reasons" "location_id,toast_payout_reason_guid" {
  param($item, $locationId)
  @{
    toast_payout_reason_guid = $item.guid
    location_id              = $locationId
    entity_type              = if ($item.entityType) { $item.entityType } else { $null }
    name                     = if ($item.name) { $item.name } else { $null }
    updated_at               = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "payout_reasons total: $total" Green

# ============================================================
# 20. RESTAURANT SERVICES
# ============================================================
Write-Log "--- Syncing restaurant_services ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/restaurantServices" "restaurant_services" "location_id,toast_restaurant_service_guid" {
  param($item, $locationId)
  @{
    toast_restaurant_service_guid = $item.guid
    location_id                   = $locationId
    entity_type                   = if ($item.entityType) { $item.entityType } else { $null }
    name                          = if ($item.name) { $item.name } else { $null }
    updated_at                    = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "restaurant_services total: $total" Green

# ============================================================
# 21. PRICE GROUPS
# ============================================================
Write-Log "--- Syncing price_groups ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/priceGroups" "price_groups" "location_id,toast_price_group_guid" {
  param($item, $locationId)
  @{
    toast_price_group_guid = $item.guid
    location_id            = $locationId
    entity_type            = if ($item.entityType) { $item.entityType } else { $null }
    name                   = if ($item.name) { $item.name } else { $null }
    updated_at             = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "price_groups total: $total" Green

# ============================================================
# 22. ALTERNATE PAYMENT TYPES
# ============================================================
Write-Log "--- Syncing alternate_payment_types ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/alternatePaymentTypes" "alternate_payment_types" "location_id,toast_alt_payment_type_guid" {
  param($item, $locationId)
  @{
    toast_alt_payment_type_guid = $item.guid
    location_id                 = $locationId
    toast_external_id           = if ($item.externalId) { $item.externalId } else { $null }
    entity_type                 = if ($item.entityType) { $item.entityType } else { $null }
    name                        = if ($item.name) { $item.name } else { $null }
    updated_at                  = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "alternate_payment_types total: $total" Green

# ============================================================
# 23. TIP WITHHOLDING
# ============================================================
Write-Log "--- Syncing tip_withholding ---" Cyan
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $locationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $item = Invoke-RestMethod -Uri "$toastApiUrl/config/v2/tipWithholding" -Headers @{
      Authorization                  = "Bearer $token"
      "Toast-Restaurant-External-ID" = $loc.guid
    }
    if (-not $item) { continue }
    $row = @{
      location_id = $locationId
      toast_guid  = if ($item.guid) { $item.guid } else { $null }
      entity_type = if ($item.entityType) { $item.entityType } else { $null }
      enabled     = if ($null -ne $item.enabled) { $item.enabled } else { $false }
      percentage  = if ($null -ne $item.percentage) { $item.percentage } else { $null }
      updated_at  = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    } | ConvertTo-Json -Depth 3
    Invoke-RestMethod -Uri "$supabaseUrl/rest/v1/tip_withholding?on_conflict=location_id" -Method POST -Headers $supabaseHeaders -Body $row | Out-Null
    $grandTotal++
    Write-Log "  $($loc.name): enabled=$($item.enabled)"
  } catch {
    Write-Log "  ERROR $($loc.name): $($_.Exception.Message)" Red
  }
  Start-Sleep -Milliseconds 500
}
Write-Log "tip_withholding total: $grandTotal" Green

# ============================================================
# 24. BREAK TYPES
# ============================================================
Write-Log "--- Syncing break_types ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/breakTypes" "break_types" "location_id,toast_break_type_guid" {
  param($item, $locationId)
  @{
    toast_break_type_guid       = $item.guid
    location_id                 = $locationId
    entity_type                 = if ($item.entityType) { $item.entityType } else { $null }
    name                        = if ($item.name) { $item.name } else { $null }
    active                      = if ($null -ne $item.active) { $item.active } else { $true }
    paid                        = if ($null -ne $item.paid) { $item.paid } else { $false }
    duration                    = if ($null -ne $item.duration) { $item.duration } else { $null }
    enforce_minimum_time        = if ($null -ne $item.enforceMinimumTime) { $item.enforceMinimumTime } else { $false }
    track_missed_breaks         = if ($null -ne $item.trackMissedBreaks) { $item.trackMissedBreaks } else { $false }
    break_interval_hrs          = if ($null -ne $item.breakIntervalHrs) { $item.breakIntervalHrs } else { $null }
    break_interval_mins         = if ($null -ne $item.breakIntervalMins) { $item.breakIntervalMins } else { $null }
    track_break_acknowledgement = if ($null -ne $item.trackBreakAcknowledgement) { $item.trackBreakAcknowledgement } else { $false }
    updated_at                  = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "break_types total: $total" Green

# ============================================================
# 25. CASH DRAWERS
# ============================================================
Write-Log "--- Syncing cash_drawers ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/cashDrawers" "cash_drawers" "location_id,toast_cash_drawer_guid" {
  param($item, $locationId)
  @{
    toast_cash_drawer_guid = $item.guid
    location_id            = $locationId
    entity_type            = if ($item.entityType) { $item.entityType } else { $null }
    printer_guid           = if ($item.printer.guid) { $item.printer.guid } else { $null }
    printer_entity_type    = if ($item.printer.entityType) { $item.printer.entityType } else { $null }
    updated_at             = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "cash_drawers total: $total" Green

# ============================================================
# 26. DISCOUNT CONFIGS
# ============================================================
Write-Log "--- Syncing discount_configs ---" Cyan
$total = Sync-SimpleConfigTable "/config/v2/discounts" "discount_configs" "location_id,toast_discount_guid" {
  param($item, $locationId)
  @{
    toast_discount_guid   = $item.guid
    location_id           = $locationId
    entity_type           = if ($item.entityType) { $item.entityType } else { $null }
    name                  = if ($item.name) { $item.name } else { $null }
    active                = if ($null -ne $item.active) { $item.active } else { $true }
    type                  = if ($item.type) { $item.type } else { $null }
    percentage            = if ($null -ne $item.percentage) { $item.percentage } else { $null }
    amount                = if ($null -ne $item.amount) { $item.amount } else { $null }
    selection_type        = if ($item.selectionType) { $item.selectionType } else { $null }
    non_exclusive         = if ($null -ne $item.nonExclusive) { $item.nonExclusive } else { $false }
    item_picking_priority = if ($item.itemPickingPriority) { $item.itemPickingPriority } else { $null }
    fixed_total           = if ($null -ne $item.fixedTotal) { $item.fixedTotal } else { $null }
    promo_codes           = if ($item.promoCodes -and $item.promoCodes.Count -gt 0) { $item.promoCodes | ConvertTo-Json -Compress -Depth 3 } else { $null }
    updated_at            = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
  }
}
Write-Log "discount_configs total: $total" Green

# ============================================================
# DONE
# ============================================================
Write-Log "============================================"
Write-Log "Gastamo Weekly Config Sync Complete"
Write-Log "============================================"
