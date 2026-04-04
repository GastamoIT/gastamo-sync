# ============================================================
# Gastamo Group - Weekly Config Sync
# Updated to match Supabase schema (April 2026)
# ============================================================

$ErrorActionPreference = 'Stop'

# CREDENTIALS
$clientId     = $env:TOAST_CLIENT_ID
$clientSecret = $env:TOAST_CLIENT_SECRET
$toastApiUrl  = $env:TOAST_API_URL
$supabaseUrl  = $env:SUPABASE_URL
$supabaseKey  = $env:SUPABASE_KEY

if ([string]::IsNullOrWhiteSpace($clientId) -or
    [string]::IsNullOrWhiteSpace($clientSecret) -or
    [string]::IsNullOrWhiteSpace($toastApiUrl) -or
    [string]::IsNullOrWhiteSpace($supabaseUrl) -or
    [string]::IsNullOrWhiteSpace($supabaseKey)) {
  throw 'Missing one or more required environment variables.'
}

# LOG FILE
$logDir  = 'logs'
$logFile = Join-Path $logDir ("weekly-sync-{0}.log" -f (Get-Date -Format 'yyyy-MM-dd'))
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

function Write-Log {
  param(
    [Parameter(Mandatory=$true)][string]$Message,
    [string]$Color = 'White'
  )
  $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  $line = "[$timestamp] $Message"
  Add-Content -Path $logFile -Value $line
  Write-Host $line -ForegroundColor $Color
}

Write-Log '============================================'
Write-Log 'Gastamo Weekly Config Sync Started'
Write-Log '============================================'

# AUTH
try {
  $authBody = @{
    clientId       = $clientId
    clientSecret   = $clientSecret
    userAccessType = 'TOAST_MACHINE_CLIENT'
  } | ConvertTo-Json

  $authUri = '{0}/authentication/v1/authentication/login' -f $toastApiUrl.TrimEnd('/')
  $authResponse = Invoke-RestMethod -Uri $authUri -Method POST -Body $authBody -ContentType 'application/json'

  $token = $authResponse.token.accessToken
  Write-Log ("Toast token acquired (length={0})" -f $token.Length) 'Green'
} catch {
  Write-Log ("FATAL: Toast auth failed - {0}" -f $_.Exception.Message) 'Red'
  throw
}

$supabaseHeaders = @{
  'apikey'        = $supabaseKey
  'Authorization' = "Bearer $supabaseKey"
  'Content-Type'  = 'application/json'
  'Prefer'        = 'resolution=merge-duplicates,return=minimal'
}

$toastHeadersBase = @{
  Authorization = "Bearer $token"
}

$locations = @(
  @{ guid = '5ed76b7f-7d30-472f-bc5c-8ea17ae31954'; name = 'HG-Westminster' },
  @{ guid = '15ed9231-4d0f-4838-9c2e-2f085b5b18f9'; name = 'Perdida-Westminster' },
  @{ guid = '2693b094-44de-42de-9d0a-24b27aa17687'; name = 'Park-Co' },
  @{ guid = 'ee3ff73b-3b0b-4ccb-b351-9ef006c3d664'; name = 'HG-CastleRock' },
  @{ guid = '263eb56c-977c-465d-ad6d-46ddf5800e63'; name = 'LN-Arvada' },
  @{ guid = '15db461b-a5d9-463e-8e76-b10443f9f451'; name = 'PB-Holly' },
  @{ guid = 'a96580b0-0906-4611-a2ea-08159717d80b'; name = 'HG-Parker' },
  @{ guid = '72eb5f11-7674-4ffe-91a3-d6b5688748d7'; name = 'HG-Arvada' },
  @{ guid = '689d4903-ed5d-4c9d-ac2d-9ade4e783d46'; name = 'HG-WashingtonPark' },
  @{ guid = '8c4d1096-fb86-4f4f-a975-036219328145'; name = 'PB-Highlands' },
  @{ guid = 'c5de1411-9fdf-43c4-94b3-1700a9d2b68a'; name = 'HG-KenCaryl' },
  @{ guid = 'a7555f87-91ea-484e-be76-d4a46ddaf4d7'; name = 'PB-RiNo' },
  @{ guid = 'e95f033a-ef10-4004-96d9-0d949427eca3'; name = 'Perdida-WashingtonPark' },
  @{ guid = 'b438f92d-ec44-41c4-a765-816f3cd902d9'; name = 'LN-Belmar' },
  @{ guid = 'd842fb61-9607-4752-990b-9ed08aecb8a9'; name = 'PB-Pearl' },
  @{ guid = 'c021581f-bb16-45cd-b69a-1c1b2416a8f1'; name = 'LN-CentralPark' }
)

$script:HadErrors = $false

function Get-NowIsoUtc {
  return (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
}

function Get-HttpErrorBody {
  param([Parameter(Mandatory=$true)]$Err)

  $resp = $Err.Exception.Response
  if (-not $resp) { return $Err.Exception.Message }

  if ($resp -is [System.Net.Http.HttpResponseMessage]) {
    try {
      return $resp.Content.ReadAsStringAsync().GetAwaiter().GetResult()
    } catch {
      return $Err.Exception.Message
    }
  }

  try {
    $stream = $resp.GetResponseStream()
    if ($stream) {
      $reader = [System.IO.StreamReader]::new($stream)
      try { return $reader.ReadToEnd() }
      finally {
        $reader.Dispose()
        $stream.Dispose()
      }
    }
  } catch {}

  return $Err.Exception.Message
}

function New-ToastHeaders {
  param([Parameter(Mandatory=$true)][string]$RestaurantGuid)
  return @{
    Authorization                  = "Bearer $token"
    'Toast-Restaurant-External-ID' = $RestaurantGuid
  }
}

function Invoke-ToastJson {
  param(
    [Parameter(Mandatory=$true)][string]$Uri,
    [Parameter(Mandatory=$true)][string]$RestaurantGuid,
    [string]$Method = 'GET'
  )

  return Invoke-RestMethod -Uri $Uri -Method $Method -Headers (New-ToastHeaders -RestaurantGuid $RestaurantGuid)
}

function Get-ToastConfigItems {
  param(
    [Parameter(Mandatory=$true)][string]$Endpoint,
    [Parameter(Mandatory=$true)][string]$RestaurantGuid
  )

  $items = [System.Collections.Generic.List[object]]::new()
  $pageToken = $null

  do {
    $uri = "$toastApiUrl$Endpoint"
    if ($pageToken) {
      $separator = if ($uri.Contains('?')) { '&' } else { '?' }
      $uri = "${uri}${separator}pageToken=$([uri]::EscapeDataString($pageToken))"
    }

    $response = Invoke-WebRequest -Uri $uri -Headers (New-ToastHeaders -RestaurantGuid $RestaurantGuid) -Method GET -UseBasicParsing
    $content = if ([string]::IsNullOrWhiteSpace($response.Content)) { @() } else { ($response.Content | ConvertFrom-Json) }

    if ($content -is [System.Array]) {
      foreach ($obj in $content) { $items.Add($obj) }
    } elseif ($null -ne $content) {
      $items.Add($content)
    }

    $pageToken = $response.Headers['Toast-Next-Page-Token']
  } while ($pageToken)

  return @($items)
}

function Get-KitchenItems {
  param(
    [Parameter(Mandatory=$true)][string]$Endpoint,
    [Parameter(Mandatory=$true)][string]$RestaurantGuid
  )

  $items = [System.Collections.Generic.List[object]]::new()
  $pageToken = $null

  do {
    $uri = "$toastApiUrl$Endpoint"
    if ($pageToken) {
      $separator = if ($uri.Contains('?')) { '&' } else { '?' }
      $uri = "${uri}${separator}pageToken=$([uri]::EscapeDataString($pageToken))"
    }

    $response = Invoke-WebRequest -Uri $uri -Headers (New-ToastHeaders -RestaurantGuid $RestaurantGuid) -Method GET -UseBasicParsing
    $content = if ([string]::IsNullOrWhiteSpace($response.Content)) { @() } else { ($response.Content | ConvertFrom-Json) }

    if ($content -is [System.Array]) {
      foreach ($obj in $content) { $items.Add($obj) }
    } elseif ($null -ne $content) {
      $items.Add($content)
    }

    $pageToken = $response.Headers['Toast-Next-Page-Token']
  } while ($pageToken)

  return @($items)
}

function Write-ToSupabase {
  param(
    [Parameter(Mandatory=$true)][array]$Batch,
    [Parameter(Mandatory=$true)][string]$Table,
    [string]$Conflict
  )

  if (-not $Batch -or $Batch.Count -eq 0) { return 0 }

  $written = 0
  $batchSize = 100

  for ($j = 0; $j -lt $Batch.Count; $j += $batchSize) {
    $end = [Math]::Min($j + $batchSize - 1, $Batch.Count - 1)
    $slice = @($Batch[$j..$end])
    $json = $slice | ConvertTo-Json -Depth 10 -Compress
    if ($slice.Count -eq 1) { $json = "[$json]" }

    $url = "$supabaseUrl/rest/v1/$Table"
    if ($Conflict) { $url += "?on_conflict=$([uri]::EscapeDataString($Conflict))" }

    $retry = 0
    while ($true) {
      try {
        Invoke-RestMethod -Uri $url -Method POST -Headers $supabaseHeaders -Body $json | Out-Null
        $written += $slice.Count
        break
      } catch {
        $body = Get-HttpErrorBody $_
        $statusCode = $null
        try { $statusCode = [int]$_.Exception.Response.StatusCode } catch {}
        $isRetryable = $statusCode -in @(408, 429, 500, 502, 503, 504)

        if ($isRetryable -and $retry -lt 2) {
          $retry++
          $delay = 2 * [Math]::Pow(2, $retry)
          Write-Log ("  SUPABASE RETRY {0}/3 on {1} after HTTP {2}; waiting {3}s" -f $retry, $Table, $statusCode, $delay) 'Yellow'
          Start-Sleep -Seconds $delay
          continue
        }

        throw ("Supabase write failed on {0}: {1}`n{2}" -f $Table, $_.Exception.Message, $body)
      }
    }
  }

  return $written
}

function Get-LocationIdMap {
  $resp = Invoke-RestMethod -Uri "$supabaseUrl/rest/v1/locations?select=id,toast_restaurant_guid&limit=100" -Headers $supabaseHeaders
  $map = @{}
  foreach ($loc in $resp) {
    $map[$loc.toast_restaurant_guid] = [string]$loc.id
  }
  return $map
}

function Escape-InListValue {
  param([Parameter(Mandatory=$true)][string]$Value)
  return '"' + ($Value -replace '"', '\"') + '"'
}

function Get-SupabaseIdMapByGuids {
  param(
    [Parameter(Mandatory=$true)][string]$Table,
    [Parameter(Mandatory=$true)][string]$GuidColumn,
    [Parameter(Mandatory=$true)][string[]]$Guids
  )

  $map = @{}
  if (-not $Guids -or $Guids.Count -eq 0) { return $map }

  $uniqueGuids = $Guids | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique
  if (-not $uniqueGuids -or $uniqueGuids.Count -eq 0) { return $map }

  $chunkSize = 100
  for ($i = 0; $i -lt $uniqueGuids.Count; $i += $chunkSize) {
    $end = [Math]::Min($i + $chunkSize - 1, $uniqueGuids.Count - 1)
    $slice = @($uniqueGuids[$i..$end])
    $inList = ($slice | ForEach-Object { Escape-InListValue $_ }) -join ','
    $uri = "$supabaseUrl/rest/v1/$Table?select=id,$GuidColumn&$GuidColumn=in.($inList)"
    $rows = Invoke-RestMethod -Uri $uri -Headers $supabaseHeaders
    foreach ($row in $rows) {
      $map[[string]$row.$GuidColumn] = [string]$row.id
    }
  }

  return $map
}

function Sync-SimpleConfigTable {
  param(
    [Parameter(Mandatory=$true)][string]$Endpoint,
    [Parameter(Mandatory=$true)][string]$Table,
    [Parameter(Mandatory=$true)][string]$ConflictKey,
    [Parameter(Mandatory=$true)][scriptblock]$MapRow,
    [switch]$KitchenApi
  )

  $grandTotal = 0
  foreach ($loc in $locations) {
    $locationId = $script:LocationIdMap[$loc.guid]
    if (-not $locationId) {
      Write-Log ("  SKIP {0}: no location_id mapping" -f $loc.name) 'Yellow'
      continue
    }

    try {
      $items = if ($KitchenApi) {
        Get-KitchenItems -Endpoint $Endpoint -RestaurantGuid $loc.guid
      } else {
        Get-ToastConfigItems -Endpoint $Endpoint -RestaurantGuid $loc.guid
      }

      $batch = @()
      foreach ($item in $items) {
        if (-not $item.guid) { continue }
        $row = & $MapRow $item $locationId
        if ($row) { $batch += $row }
      }

      $written = Write-ToSupabase -Batch $batch -Table $Table -Conflict $ConflictKey
      $grandTotal += $written
      Write-Log ("  {0}: {1} rows" -f $loc.name, $written)
    } catch {
      $script:HadErrors = $true
      Write-Log ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message) 'Red'
    }

    Start-Sleep -Milliseconds 250
  }

  return $grandTotal
}

$script:LocationIdMap = Get-LocationIdMap

# 1. LOCATIONS
Write-Log '--- Syncing locations ---' 'Cyan'
try {
  $batch = @()
  foreach ($loc in $locations) {
    try {
      $info = Invoke-ToastJson -Uri "$toastApiUrl/restaurants/v1/restaurants/$($loc.guid)" -RestaurantGuid $loc.guid
      $general = $info.general
      $location = $info.location
      $urls = $info.urls

      $batch += @{
        toast_restaurant_guid   = $loc.guid
        name                    = $general.name
        location_name           = $general.locationName
        location_code           = $general.locationCode
        description             = $general.description
        timezone                = $general.timeZone
        closeout_hour           = $general.closeoutHour
        management_group_guid   = $general.managementGroupGuid
        currency_code           = $general.currencyCode
        first_business_date     = $general.firstBusinessDate
        archived                = if ($null -ne $general.archived) { $general.archived } else { $false }
        address1                = $location.address1
        address2                = $location.address2
        city                    = $location.city
        state                   = $location.administrativeArea
        zip_code                = $location.zipCode
        country                 = $location.country
        phone                   = $location.phone
        phone_country_code      = $location.phoneCountryCode
        latitude                = $location.latitude
        longitude               = $location.longitude
        url                     = $urls.website
        facebook_url            = $urls.facebook
        twitter_url             = $urls.twitter
        store_number            = $general.locationCode
        updated_at              = Get-NowIsoUtc
      }
    } catch {
      $script:HadErrors = $true
      Write-Log ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message) 'Red'
    }
    Start-Sleep -Milliseconds 250
  }

  $written = Write-ToSupabase -Batch $batch -Table 'locations' -Conflict 'toast_restaurant_guid'
  Write-Log ("locations total: {0}" -f $written) 'Green'
  $script:LocationIdMap = Get-LocationIdMap
} catch {
  $script:HadErrors = $true
  Write-Log ("ERROR syncing locations: {0}" -f $_.Exception.Message) 'Red'
}

# 2. EMPLOYEES
Write-Log '--- Syncing employees ---' 'Cyan'
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $script:LocationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $employees = Invoke-ToastJson -Uri "$toastApiUrl/labor/v1/employees" -RestaurantGuid $loc.guid
    $batch = @()
    foreach ($emp in $employees) {
      if (-not $emp.guid) { continue }
      $batch += @{
        toast_employee_guid       = $emp.guid
        location_id               = $locationId
        external_id               = $emp.externalId
        first_name                = $emp.firstName
        chosen_name               = $emp.chosenName
        last_name                 = $emp.lastName
        email                     = $emp.email
        phone_number              = $emp.phoneNumber
        phone_number_country_code = $emp.phoneNumberCountryCode
        external_employee_id      = $emp.externalEmployeeId
        deleted                   = if ($null -ne $emp.deleted) { $emp.deleted } else { $false }
        job_references            = if ($emp.jobReferences) { $emp.jobReferences | ConvertTo-Json -Compress -Depth 10 } else { $null }
        wage_overrides            = if ($emp.wageOverrides) { $emp.wageOverrides | ConvertTo-Json -Compress -Depth 10 } else { $null }
        v2_employee_guid          = $emp.v2EmployeeGuid
        created_date              = $emp.createdDate
        modified_date             = $emp.modifiedDate
        deleted_date              = $emp.deletedDate
        updated_at                = Get-NowIsoUtc
      }
    }
    $written = Write-ToSupabase -Batch $batch -Table 'employees' -Conflict 'location_id,toast_employee_guid'
    $grandTotal += $written
    Write-Log ("  {0}: {1} employees" -f $loc.name, $written)
  } catch {
    $script:HadErrors = $true
    Write-Log ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message) 'Red'
  }
  Start-Sleep -Milliseconds 250
}
Write-Log ("employees total: {0}" -f $grandTotal) 'Green'

# 3. JOBS
Write-Log '--- Syncing jobs ---' 'Cyan'
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $script:LocationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $jobs = Invoke-ToastJson -Uri "$toastApiUrl/labor/v1/jobs" -RestaurantGuid $loc.guid
    $batch = @()
    foreach ($job in $jobs) {
      if (-not $job.guid) { continue }
      $batch += @{
        toast_job_guid           = $job.guid
        location_id              = $locationId
        external_id              = $job.externalId
        title                    = $job.title
        code                     = $job.code
        wage_frequency           = $job.wageFrequency
        default_wage             = $job.defaultWage
        tipped                   = if ($null -ne $job.tipped) { $job.tipped } else { $false }
        exclude_from_reporting   = if ($null -ne $job.excludeFromReporting) { $job.excludeFromReporting } else { $false }
        deleted                  = if ($null -ne $job.deleted) { $job.deleted } else { $false }
        created_date             = $job.createdDate
        modified_date            = $job.modifiedDate
        deleted_date             = $job.deletedDate
        updated_at               = Get-NowIsoUtc
      }
    }
    $written = Write-ToSupabase -Batch $batch -Table 'jobs' -Conflict 'location_id,toast_job_guid'
    $grandTotal += $written
    Write-Log ("  {0}: {1} jobs" -f $loc.name, $written)
  } catch {
    $script:HadErrors = $true
    Write-Log ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message) 'Red'
  }
  Start-Sleep -Milliseconds 250
}
Write-Log ("jobs total: {0}" -f $grandTotal) 'Green'

# 4-10. MENUS (normalized schema)
function Add-MenuGroupRows {
  param(
    [Parameter(Mandatory=$true)][array]$Groups,
    [Parameter(Mandatory=$true)][string]$ToastMenuGuid,
    [string]$ParentGroupGuid,
    [ref]$GroupRows,
    [ref]$ItemRows
  )

  foreach ($group in $Groups) {
    if (-not $group.guid) { continue }

    $GroupRows.Value.Add([ordered]@{
      toast_menu_group_guid   = $group.guid
      menu_id                 = $null
      toast_multi_location_id = $group.multiLocationId
      toast_master_id         = $group.masterId
      name                    = $group.name
      description             = $group.description
      pos_name                = $group.posName
      pos_button_color_light  = $group.posButtonColorLight
      pos_button_color_dark   = $group.posButtonColorDark
      image_url               = $group.image
      visibility              = if ($group.visibility) { $group.visibility | ConvertTo-Json -Compress -Depth 10 } else { $null }
      item_tags               = if ($group.itemTags) { $group.itemTags | ConvertTo-Json -Compress -Depth 10 } else { $null }
      toast_parent_group_guid = $ParentGroupGuid
      _toast_menu_guid        = $ToastMenuGuid
      updated_at              = Get-NowIsoUtc
    })

    if ($group.menuItems) {
      foreach ($item in $group.menuItems) {
        if (-not $item.guid) { continue }
        $ItemRows.Value.Add([ordered]@{
          toast_menu_item_guid          = $item.guid
          menu_group_id                 = $null
          toast_multi_location_id       = $item.multiLocationId
          toast_master_id               = $item.masterId
          name                          = $item.name
          kitchen_name                  = $item.kitchenName
          description                   = $item.description
          pos_name                      = $item.posName
          pos_button_color_light        = $item.posButtonColorLight
          pos_button_color_dark         = $item.posButtonColorDark
          image_url                     = $item.image
          images                        = if ($item.images) { $item.images | ConvertTo-Json -Compress -Depth 10 } else { $null }
          visibility                    = if ($item.visibility) { $item.visibility | ConvertTo-Json -Compress -Depth 10 } else { $null }
          price                         = $item.price
          pricing_strategy              = $item.pricingStrategy
          pricing_rules                 = if ($item.pricingRules) { $item.pricingRules | ConvertTo-Json -Compress -Depth 10 } else { $null }
          tax_info                      = if ($item.taxInfo) { $item.taxInfo | ConvertTo-Json -Compress -Depth 10 } else { $null }
          tax_inclusion                 = $item.taxInclusion
          sales_category_guid           = $item.salesCategory.guid
          sales_category_name           = $item.salesCategory.name
          plu                           = $item.plu
          sku                           = $item.sku
          calories                      = $item.calories
          unit_of_measure               = $item.unitOfMeasure
          is_deferred                   = $item.isDeferred
          is_discountable               = $item.isDiscountable
          prep_time_seconds             = $item.prepTimeSeconds
          prep_stations                 = if ($item.prepStations) { $item.prepStations | ConvertTo-Json -Compress -Depth 10 } else { $null }
          modifier_group_references     = if ($item.modifierGroupReferences) { $item.modifierGroupReferences | ConvertTo-Json -Compress -Depth 10 } else { $null }
          portions                      = if ($item.portions) { $item.portions | ConvertTo-Json -Compress -Depth 10 } else { $null }
          item_tags                     = if ($item.itemTags) { $item.itemTags | ConvertTo-Json -Compress -Depth 10 } else { $null }
          eligible_payment_assistance   = if ($item.eligiblePaymentAssistance) { $item.eligiblePaymentAssistance | ConvertTo-Json -Compress -Depth 10 } else { $null }
          content_advisories            = if ($item.contentAdvisories) { $item.contentAdvisories | ConvertTo-Json -Compress -Depth 10 } else { $null }
          allergens                     = if ($item.allergens) { $item.allergens | ConvertTo-Json -Compress -Depth 10 } else { $null }
          length                        = $item.length
          height                        = $item.height
          width                         = $item.width
          dimension_unit_of_measure     = $item.dimensionUnitOfMeasure
          weight                        = $item.weight
          weight_unit_of_measure        = $item.weightUnitOfMeasure
          guest_count                   = $item.guestCount
          sort_order                    = $item.sortOrder
          _toast_menu_group_guid        = $group.guid
          updated_at                    = Get-NowIsoUtc
        })
      }
    }

    if ($group.menuGroups) {
      Add-MenuGroupRows -Groups $group.menuGroups -ToastMenuGuid $ToastMenuGuid -ParentGroupGuid $group.guid -GroupRows $GroupRows -ItemRows $ItemRows
    }
  }
}

Write-Log '--- Syncing menus ---' 'Cyan'
$grandTotal = 0
foreach ($loc in $locations) {
  try {
    $menuData = Invoke-ToastJson -Uri "$toastApiUrl/menus/v2/menus" -RestaurantGuid $loc.guid

    $menuBatch = [System.Collections.Generic.List[object]]::new()
    $groupRows = [System.Collections.Generic.List[object]]::new()
    $itemRows = [System.Collections.Generic.List[object]]::new()
    $modGroupBatch = [System.Collections.Generic.List[object]]::new()
    $modOptionBatch = [System.Collections.Generic.List[object]]::new()
    $preModGroupBatch = [System.Collections.Generic.List[object]]::new()
    $preModBatch = [System.Collections.Generic.List[object]]::new()

    foreach ($menu in @($menuData.menus)) {
      if (-not $menu.guid) { continue }
      $menuBatch.Add([ordered]@{
        toast_menu_guid        = $menu.guid
        toast_multi_location_id= $menu.multiLocationId
        toast_master_id        = $menu.masterId
        name                   = $menu.name
        description            = $menu.description
        pos_name               = $menu.posName
        pos_button_color_light = $menu.posButtonColorLight
        pos_button_color_dark  = $menu.posButtonColorDark
        image_url              = $menu.image
        visibility             = if ($menu.visibility) { $menu.visibility | ConvertTo-Json -Compress -Depth 10 } else { $null }
        availability           = if ($menu.availability) { $menu.availability | ConvertTo-Json -Compress -Depth 10 } else { $null }
        updated_at             = Get-NowIsoUtc
      })

      if ($menu.menuGroups) {
        Add-MenuGroupRows -Groups $menu.menuGroups -ToastMenuGuid $menu.guid -ParentGroupGuid $null -GroupRows ([ref]$groupRows) -ItemRows ([ref]$itemRows)
      }
    }

    if ($menuData.modifierGroupReferences) {
      foreach ($prop in $menuData.modifierGroupReferences.PSObject.Properties) {
        $mg = $prop.Value
        if (-not $mg.guid) { continue }
        $preModGroupGuid = $null
        if ($mg.preModifierGroup) { $preModGroupGuid = $mg.preModifierGroup.guid }
        $modGroupBatch.Add([ordered]@{
          toast_modifier_group_guid   = $mg.guid
          toast_reference_id          = $mg.referenceId
          toast_multi_location_id     = $mg.multiLocationId
          toast_master_id             = $mg.masterId
          pre_modifier_group_id       = $null
          _toast_pre_modifier_group_guid = $preModGroupGuid
          name                        = $mg.name
          description                 = $mg.description
          pos_name                    = $mg.posName
          pos_button_color_light      = $mg.posButtonColorLight
          pos_button_color_dark       = $mg.posButtonColorDark
          image_url                   = $mg.image
          visibility                  = if ($mg.visibility) { $mg.visibility | ConvertTo-Json -Compress -Depth 10 } else { $null }
          item_tags                   = if ($mg.itemTags) { $mg.itemTags | ConvertTo-Json -Compress -Depth 10 } else { $null }
          pricing_strategy            = $mg.pricingStrategy
          pricing_rules               = if ($mg.pricingRules) { $mg.pricingRules | ConvertTo-Json -Compress -Depth 10 } else { $null }
          default_options_charge_price      = $mg.defaultOptionsChargePrice
          default_options_substitution_pricing = $mg.defaultOptionsSubstitutionPricing
          min_selections              = $mg.minSelections
          max_selections              = $mg.maxSelections
          required_mode               = $mg.requiredMode
          is_multi_select             = $mg.isMultiSelect
          updated_at                  = Get-NowIsoUtc
        })
      }
    }

    if ($menuData.preModifierGroupReferences) {
      foreach ($prop in $menuData.preModifierGroupReferences.PSObject.Properties) {
        $pmg = $prop.Value
        if (-not $pmg.guid) { continue }
        $preModGroupBatch.Add([ordered]@{
          toast_pre_modifier_group_guid = $pmg.guid
          toast_multi_location_id       = $pmg.multiLocationId
          name                          = $pmg.name
          updated_at                    = Get-NowIsoUtc
        })

        foreach ($pm in @($pmg.preModifiers)) {
          if (-not $pm.guid) { continue }
          $preModBatch.Add([ordered]@{
            toast_pre_modifier_guid       = $pm.guid
            pre_modifier_group_id         = $null
            _toast_pre_modifier_group_guid = $pmg.guid
            toast_multi_location_id       = $pm.multiLocationId
            name                          = $pm.name
            pos_name                      = $pm.posName
            display_mode                  = $pm.displayMode
            fixed_price                   = $pm.fixedPrice
            multiplication_factor         = $pm.multiplicationFactor
            charge_as_extra               = $pm.chargeAsExtra
            plu                           = $pm.plu
            pos_button_color_light        = $pm.posButtonColorLight
            pos_button_color_dark         = $pm.posButtonColorDark
            updated_at                    = Get-NowIsoUtc
          })
        }
      }
    }

    if ($menuData.modifierOptionReferences) {
      foreach ($prop in $menuData.modifierOptionReferences.PSObject.Properties) {
        $mo = $prop.Value
        if (-not $mo.guid) { continue }
        $parentGroupRef = $null
        if ($mo.modifierGroupReferenceId) {
          $parentGroupRef = $menuData.modifierGroupReferences."$($mo.modifierGroupReferenceId)".guid
        }
        $modOptionBatch.Add([ordered]@{
          toast_modifier_option_guid = $mo.guid
          modifier_group_id          = $null
          _toast_modifier_group_guid = $parentGroupRef
          name                       = $mo.name
          kitchen_name               = $mo.kitchenName
          price                      = $mo.price
          is_default                 = $mo.isDefault
          allows_duplicates          = $mo.allowsDuplicates
          sort_order                 = $mo.sortOrder
          calories                   = $mo.calories
          updated_at                 = Get-NowIsoUtc
        })
      }
    }

    $menuWritten = Write-ToSupabase -Batch @($menuBatch) -Table 'menus' -Conflict 'toast_menu_guid'
    $menuIdMap = Get-SupabaseIdMapByGuids -Table 'menus' -GuidColumn 'toast_menu_guid' -Guids (@($menuBatch | ForEach-Object { $_.toast_menu_guid }))

    foreach ($row in $groupRows) {
      $row.menu_id = $menuIdMap[$row._toast_menu_guid]
      $null = $row.Remove('_toast_menu_guid')
    }
    $groupWritten = Write-ToSupabase -Batch @($groupRows) -Table 'menu_groups' -Conflict 'toast_menu_group_guid'
    $groupIdMap = Get-SupabaseIdMapByGuids -Table 'menu_groups' -GuidColumn 'toast_menu_group_guid' -Guids (@($groupRows | ForEach-Object { $_.toast_menu_group_guid }))

    foreach ($row in $itemRows) {
      $row.menu_group_id = $groupIdMap[$row._toast_menu_group_guid]
      $null = $row.Remove('_toast_menu_group_guid')
    }
    $itemWritten = Write-ToSupabase -Batch @($itemRows) -Table 'menu_items' -Conflict 'toast_menu_item_guid'

    $preModGroupWritten = Write-ToSupabase -Batch @($preModGroupBatch) -Table 'pre_modifier_groups' -Conflict 'toast_pre_modifier_group_guid'
    $preModGroupIdMap = Get-SupabaseIdMapByGuids -Table 'pre_modifier_groups' -GuidColumn 'toast_pre_modifier_group_guid' -Guids (@($preModGroupBatch | ForEach-Object { $_.toast_pre_modifier_group_guid }))

    foreach ($row in $modGroupBatch) {
      if ($row._toast_pre_modifier_group_guid) {
        $row.pre_modifier_group_id = $preModGroupIdMap[$row._toast_pre_modifier_group_guid]
      }
      $null = $row.Remove('_toast_pre_modifier_group_guid')
    }
    $modGroupWritten = Write-ToSupabase -Batch @($modGroupBatch) -Table 'modifier_groups' -Conflict 'toast_modifier_group_guid'
    $modGroupIdMap = Get-SupabaseIdMapByGuids -Table 'modifier_groups' -GuidColumn 'toast_modifier_group_guid' -Guids (@($modGroupBatch | ForEach-Object { $_.toast_modifier_group_guid }))

    foreach ($row in $modOptionBatch) {
      if ($row._toast_modifier_group_guid) {
        $row.modifier_group_id = $modGroupIdMap[$row._toast_modifier_group_guid]
      }
      $null = $row.Remove('_toast_modifier_group_guid')
    }
    $modOptionWritten = Write-ToSupabase -Batch @($modOptionBatch) -Table 'modifier_options' -Conflict 'toast_modifier_option_guid'

    foreach ($row in $preModBatch) {
      $row.pre_modifier_group_id = $preModGroupIdMap[$row._toast_pre_modifier_group_guid]
      $null = $row.Remove('_toast_pre_modifier_group_guid')
    }
    $preModWritten = Write-ToSupabase -Batch @($preModBatch) -Table 'pre_modifiers' -Conflict 'toast_pre_modifier_guid'

    $locTotal = $menuWritten + $groupWritten + $itemWritten + $modGroupWritten + $modOptionWritten + $preModGroupWritten + $preModWritten
    $grandTotal += $locTotal
    Write-Log ("  {0}: {1} menus, {2} groups, {3} items, {4} mod groups, {5} mod options, {6} pre-mod groups, {7} pre-mods" -f $loc.name, $menuWritten, $groupWritten, $itemWritten, $modGroupWritten, $modOptionWritten, $preModGroupWritten, $preModWritten)
  } catch {
    $script:HadErrors = $true
    Write-Log ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message) 'Red'
  }
  Start-Sleep -Milliseconds 500
}
Write-Log ("menus total: {0} rows across all menu tables" -f $grandTotal) 'Green'

# 11. REVENUE CENTERS
Write-Log '--- Syncing revenue_centers ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/revenueCenters' -Table 'revenue_centers' -ConflictKey 'location_id,toast_revenue_center_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_revenue_center_guid = $item.guid
    location_id               = $locationId
    entity_type               = $item.entityType
    name                      = $item.name
    description               = $item.description
    updated_at                = Get-NowIsoUtc
  }
}
Write-Log ("revenue_centers total: {0}" -f $total) 'Green'

# 12. PREP STATIONS
Write-Log '--- Syncing prep_stations ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/kitchen/v1/published/prepStations' -Table 'prep_stations' -ConflictKey 'location_id,toast_prep_station_guid' -KitchenApi -MapRow {
  param($item, $locationId)
  @{
    toast_prep_station_guid   = $item.guid
    location_id               = $locationId
    entity_type               = $item.entityType
    name                      = $item.name
    printing_mode             = $item.printingMode
    include_with_expediter    = $item.includeWithExpediter
    expo_routing              = $item.expoRouting
    kitchen_printer_guid      = $item.kitchenPrinter.guid
    kitchen_printer_entity_type = $item.kitchenPrinter.entityType
    connected_prep_stations   = if ($item.connectedPrepStations) { $item.connectedPrepStations | ConvertTo-Json -Compress -Depth 10 } else { $null }
    updated_at                = Get-NowIsoUtc
  }
}
Write-Log ("prep_stations total: {0}" -f $total) 'Green'

# 13. DINING OPTIONS
Write-Log '--- Syncing dining_options ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/diningOptions' -Table 'dining_options' -ConflictKey 'location_id,toast_dining_option_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_dining_option_guid = $item.guid
    location_id              = $locationId
    toast_external_id        = $item.externalId
    entity_type              = $item.entityType
    name                     = $item.name
    behavior                 = $item.behavior
    curbside                 = if ($null -ne $item.curbside) { $item.curbside } else { $false }
    updated_at               = Get-NowIsoUtc
  }
}
Write-Log ("dining_options total: {0}" -f $total) 'Green'

# 14. SERVICE AREAS
Write-Log '--- Syncing service_areas ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/serviceAreas' -Table 'service_areas' -ConflictKey 'location_id,toast_service_area_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_service_area_guid    = $item.guid
    location_id                = $locationId
    toast_external_id          = $item.externalId
    entity_type                = $item.entityType
    name                       = $item.name
    revenue_center_guid        = $item.revenueCenter.guid
    revenue_center_entity_type = $item.revenueCenter.entityType
    updated_at                 = Get-NowIsoUtc
  }
}
Write-Log ("service_areas total: {0}" -f $total) 'Green'

# 15. RESTAURANT TABLES
Write-Log '--- Syncing restaurant_tables ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/tables' -Table 'restaurant_tables' -ConflictKey 'location_id,toast_table_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_table_guid             = $item.guid
    location_id                  = $locationId
    entity_type                  = $item.entityType
    name                         = $item.name
    service_area_guid            = $item.serviceArea.guid
    service_area_entity_type     = $item.serviceArea.entityType
    revenue_center_guid          = $item.revenueCenter.guid
    revenue_center_entity_type   = $item.revenueCenter.entityType
    updated_at                   = Get-NowIsoUtc
  }
}
Write-Log ("restaurant_tables total: {0}" -f $total) 'Green'

# 16. TAX RATES
Write-Log '--- Syncing tax_rates ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/taxRates' -Table 'tax_rates' -ConflictKey 'location_id,toast_tax_rate_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_tax_rate_guid   = $item.guid
    location_id           = $locationId
    entity_type           = $item.entityType
    name                  = $item.name
    is_default            = if ($null -ne $item.isDefault) { $item.isDefault } else { $false }
    rate                  = $item.rate
    type                  = $item.type
    rounding_type         = $item.roundingType
    tax_table             = if ($item.taxTable) { $item.taxTable | ConvertTo-Json -Compress -Depth 10 } else { $null }
    conditional_tax_rates = if ($item.conditionalTaxRates) { $item.conditionalTaxRates | ConvertTo-Json -Compress -Depth 10 } else { $null }
    updated_at            = Get-NowIsoUtc
  }
}
Write-Log ("tax_rates total: {0}" -f $total) 'Green'

# 17. VOID REASONS
Write-Log '--- Syncing void_reasons ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/voidReasons' -Table 'void_reasons' -ConflictKey 'location_id,toast_void_reason_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_void_reason_guid = $item.guid
    location_id            = $locationId
    entity_type            = $item.entityType
    name                   = $item.name
    updated_at             = Get-NowIsoUtc
  }
}
Write-Log ("void_reasons total: {0}" -f $total) 'Green'

# 18. NO SALE REASONS
Write-Log '--- Syncing no_sale_reasons ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/noSaleReasons' -Table 'no_sale_reasons' -ConflictKey 'location_id,toast_no_sale_reason_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_no_sale_reason_guid = $item.guid
    location_id               = $locationId
    entity_type               = $item.entityType
    name                      = $item.name
    updated_at                = Get-NowIsoUtc
  }
}
Write-Log ("no_sale_reasons total: {0}" -f $total) 'Green'

# 19. PAYOUT REASONS
Write-Log '--- Syncing payout_reasons ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/payoutReasons' -Table 'payout_reasons' -ConflictKey 'location_id,toast_payout_reason_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_payout_reason_guid = $item.guid
    location_id              = $locationId
    entity_type              = $item.entityType
    name                     = $item.name
    updated_at               = Get-NowIsoUtc
  }
}
Write-Log ("payout_reasons total: {0}" -f $total) 'Green'

# 20. RESTAURANT SERVICES
Write-Log '--- Syncing restaurant_services ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/restaurantServices' -Table 'restaurant_services' -ConflictKey 'location_id,toast_restaurant_service_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_restaurant_service_guid = $item.guid
    location_id                   = $locationId
    entity_type                   = $item.entityType
    name                          = $item.name
    updated_at                    = Get-NowIsoUtc
  }
}
Write-Log ("restaurant_services total: {0}" -f $total) 'Green'

# 21. PRICE GROUPS
Write-Log '--- Syncing price_groups ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/priceGroups' -Table 'price_groups' -ConflictKey 'location_id,toast_price_group_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_price_group_guid = $item.guid
    location_id            = $locationId
    entity_type            = $item.entityType
    name                   = $item.name
    updated_at             = Get-NowIsoUtc
  }
}
Write-Log ("price_groups total: {0}" -f $total) 'Green'

# 22. ALTERNATE PAYMENT TYPES
Write-Log '--- Syncing alternate_payment_types ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/alternatePaymentTypes' -Table 'alternate_payment_types' -ConflictKey 'location_id,toast_alt_payment_type_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_alt_payment_type_guid = $item.guid
    location_id                 = $locationId
    toast_external_id           = $item.externalId
    entity_type                 = $item.entityType
    name                        = $item.name
    updated_at                  = Get-NowIsoUtc
  }
}
Write-Log ("alternate_payment_types total: {0}" -f $total) 'Green'

# 23. TIP WITHHOLDING
Write-Log '--- Syncing tip_withholding ---' 'Cyan'
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $script:LocationIdMap[$loc.guid]
  if (-not $locationId) { continue }
  try {
    $item = Invoke-ToastJson -Uri "$toastApiUrl/config/v2/tipWithholding" -RestaurantGuid $loc.guid
    if (-not $item) { continue }

    $row = @{
      location_id = $locationId
      toast_guid  = $item.guid
      entity_type = $item.entityType
      enabled     = if ($null -eq $item.enabled) { $false } else { [bool]$item.enabled }
      percentage  = $item.percentage
      updated_at  = Get-NowIsoUtc
    }

    $written = Write-ToSupabase -Batch @($row) -Table 'tip_withholding' -Conflict 'location_id'
    $grandTotal += $written
    Write-Log ("  {0}: enabled={1}" -f $loc.name, $row.enabled)
  } catch {
    $script:HadErrors = $true
    Write-Log ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message) 'Red'
  }
  Start-Sleep -Milliseconds 250
}
Write-Log ("tip_withholding total: {0}" -f $grandTotal) 'Green'

# 24. BREAK TYPES
Write-Log '--- Syncing break_types ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/breakTypes' -Table 'break_types' -ConflictKey 'location_id,toast_break_type_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_break_type_guid       = $item.guid
    location_id                 = $locationId
    entity_type                 = $item.entityType
    name                        = $item.name
    active                      = if ($null -ne $item.active) { $item.active } else { $true }
    paid                        = if ($null -ne $item.paid) { $item.paid } else { $false }
    duration                    = $item.duration
    enforce_minimum_time        = if ($null -ne $item.enforceMinimumTime) { $item.enforceMinimumTime } else { $false }
    track_missed_breaks         = if ($null -ne $item.trackMissedBreaks) { $item.trackMissedBreaks } else { $false }
    break_interval_hrs          = $item.breakIntervalHrs
    break_interval_mins         = $item.breakIntervalMins
    track_break_acknowledgement = if ($null -ne $item.trackBreakAcknowledgement) { $item.trackBreakAcknowledgement } else { $false }
    updated_at                  = Get-NowIsoUtc
  }
}
Write-Log ("break_types total: {0}" -f $total) 'Green'

# 25. CASH DRAWERS
Write-Log '--- Syncing cash_drawers ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/cashDrawers' -Table 'cash_drawers' -ConflictKey 'location_id,toast_cash_drawer_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_cash_drawer_guid = $item.guid
    location_id            = $locationId
    entity_type            = $item.entityType
    printer_guid           = $item.printer.guid
    printer_entity_type    = $item.printer.entityType
    updated_at             = Get-NowIsoUtc
  }
}
Write-Log ("cash_drawers total: {0}" -f $total) 'Green'

# 26. DISCOUNT CONFIGS
Write-Log '--- Syncing discount_configs ---' 'Cyan'
$total = Sync-SimpleConfigTable -Endpoint '/config/v2/discounts' -Table 'discount_configs' -ConflictKey 'location_id,toast_discount_guid' -MapRow {
  param($item, $locationId)
  @{
    toast_discount_guid   = $item.guid
    location_id           = $locationId
    entity_type           = $item.entityType
    name                  = $item.name
    active                = if ($null -ne $item.active) { $item.active } else { $true }
    type                  = $item.type
    percentage            = $item.percentage
    amount                = $item.amount
    selection_type        = $item.selectionType
    non_exclusive         = if ($null -ne $item.nonExclusive) { $item.nonExclusive } else { $false }
    item_picking_priority = $item.itemPickingPriority
    fixed_total           = $item.fixedTotal
    promo_codes           = if ($item.promoCodes) { $item.promoCodes | ConvertTo-Json -Compress -Depth 10 } else { $null }
    updated_at            = Get-NowIsoUtc
  }
}
Write-Log ("discount_configs total: {0}" -f $total) 'Green'

Write-Log '============================================'
if ($script:HadErrors) {
  Write-Log 'Gastamo Weekly Config Sync Complete WITH ERRORS' 'Yellow'
} else {
  Write-Log 'Gastamo Weekly Config Sync Complete' 'Green'
}
Write-Log '============================================'

if ($script:HadErrors) {
  throw 'Weekly config sync completed with errors. Review the log output above.'
}
