# ============================================================
# Gastamo Group — Weekly Config Sync (fixed + optimized)
# Runs every Sunday via GitHub Actions
# Updates reference/config tables
# ============================================================

$ErrorActionPreference = "Stop"

# CREDENTIALS — loaded from GitHub Actions secrets
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
  throw "Missing one or more required environment variables."
}

# LOG FILE — GitHub Actions compatible path
$logDir  = "logs"
$logFile = Join-Path $logDir ("weekly-sync-{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

$script:HadErrors = $false
$script:ToastToken = $null
$script:LocationIdMap = @{}
$script:UtcNowStamp = $null

function Write-Log($message, $color = "White") {
  $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[{0}] {1}" -f $timestamp, $message
  Add-Content -Path $logFile -Value $line
  Write-Host $line -ForegroundColor $color
}

function Mark-Error($message) {
  $script:HadErrors = $true
  Write-Log $message "Red"
}

function Get-UtcStamp() {
  return (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
}

function Get-PropValue($Object, [string]$Name, $Default = $null) {
  if ($null -eq $Object) { return $Default }
  $prop = $Object.PSObject.Properties[$Name]
  if ($null -eq $prop) { return $Default }
  $value = $prop.Value
  if ($null -eq $value) { return $Default }
  if (($value -is [string]) -and [string]::IsNullOrWhiteSpace($value)) { return $Default }
  return $value
}

function Get-NestedPropValue($Object, [string[]]$Path, $Default = $null) {
  $current = $Object
  foreach ($segment in $Path) {
    if ($null -eq $current) { return $Default }
    $prop = $current.PSObject.Properties[$segment]
    if ($null -eq $prop) { return $Default }
    $current = $prop.Value
  }
  if ($null -eq $current) { return $Default }
  if (($current -is [string]) -and [string]::IsNullOrWhiteSpace($current)) { return $Default }
  return $current
}

function Coalesce {
  param([Parameter(ValueFromRemainingArguments = $true)][object[]]$Values)
  foreach ($value in $Values) {
    if ($null -eq $value) { continue }
    if (($value -is [string]) -and [string]::IsNullOrWhiteSpace($value)) { continue }
    return $value
  }
  return $null
}

function Get-ArrayOrEmpty($Value) {
  if ($null -eq $Value) { return @() }
  if (($Value -is [System.Collections.IEnumerable]) -and ($Value -isnot [string])) {
    return @($Value)
  }
  return @($Value)
}

function Get-ReferenceMapValue($Map, $Key) {
  if ($null -eq $Map) { return $null }
  $prop = $Map.PSObject.Properties[[string]$Key]
  if ($null -eq $prop) { return $null }
  return $prop.Value
}

function Add-QueryParams([string]$BaseUri, [hashtable]$Query) {
  if ($null -eq $Query -or $Query.Count -eq 0) { return $BaseUri }

  $pairs = New-Object System.Collections.Generic.List[string]
  foreach ($key in $Query.Keys) {
    $value = $Query[$key]
    if ($null -eq $value) { continue }
    $pairs.Add(("{0}={1}" -f [uri]::EscapeDataString([string]$key), [uri]::EscapeDataString([string]$value)))
  }

  if ($pairs.Count -eq 0) { return $BaseUri }

  $separator = if ($BaseUri.Contains("?")) { "&" } else { "?" }
  return $BaseUri + $separator + ($pairs -join "&")
}

function Get-HttpErrorBody($Err) {
  if ($null -ne $Err.ErrorDetails -and -not [string]::IsNullOrWhiteSpace($Err.ErrorDetails.Message)) {
    return $Err.ErrorDetails.Message
  }

  $resp = $Err.Exception.Response
  if ($null -eq $resp) { return $Err.Exception.Message }

  if ($resp -is [System.Net.Http.HttpResponseMessage]) {
    try {
      return $resp.Content.ReadAsStringAsync().GetAwaiter().GetResult()
    } catch {
      return $Err.Exception.Message
    }
  }

  try {
    $stream = $resp.GetResponseStream()
    if ($null -ne $stream) {
      $reader = [System.IO.StreamReader]::new($stream)
      try {
        return $reader.ReadToEnd()
      } finally {
        $reader.Dispose()
        $stream.Dispose()
      }
    }
  } catch {}

  return $Err.Exception.Message
}

function Invoke-ToastWebRequest {
  param(
    [string]$Uri,
    [string]$RestaurantGuid,
    [string]$Method = "GET",
    [string]$Body = $null,
    [int]$MaxRetries = 3
  )

  $headers = @{
    "Authorization" = "Bearer $script:ToastToken"
  }
  if (-not [string]::IsNullOrWhiteSpace($RestaurantGuid)) {
    $headers["Toast-Restaurant-External-ID"] = $RestaurantGuid
  }

  for ($attempt = 0; $attempt -lt $MaxRetries; $attempt++) {
    try {
      $params = @{
        Uri     = $Uri
        Method  = $Method
        Headers = $headers
      }
      if ($null -ne $Body) {
        $params["Body"] = $Body
        $params["ContentType"] = "application/json"
      }
      return Invoke-WebRequest @params
    } catch {
      $statusCode = $null
      try {
        if ($null -ne $_.Exception.Response -and $null -ne $_.Exception.Response.StatusCode) {
          $statusCode = [int]$_.Exception.Response.StatusCode
        }
      } catch {}

      $responseBody = Get-HttpErrorBody $_
      if ($statusCode -in @(429, 500, 502, 503, 504) -and $attempt -lt ($MaxRetries - 1)) {
        $delay = [int]([Math]::Pow(2, $attempt) * 2)
        Write-Log ("  TOAST RETRY {0}/{1} after HTTP {2} for {3}" -f ($attempt + 1), $MaxRetries, $statusCode, $Uri) "Yellow"
        Start-Sleep -Seconds $delay
        continue
      }

      if ([string]::IsNullOrWhiteSpace($responseBody)) {
        $responseBody = $_.Exception.Message
      }

      throw ("Toast request failed [{0}] {1} :: {2}" -f $Method, $Uri, $responseBody)
    }
  }
}

function Invoke-ToastJson {
  param(
    [string]$Path,
    [string]$RestaurantGuid,
    [hashtable]$Query = $null
  )

  $uri = Add-QueryParams -BaseUri ("{0}{1}" -f $toastApiUrl, $Path) -Query $Query
  $response = Invoke-ToastWebRequest -Uri $uri -RestaurantGuid $RestaurantGuid

  if ([string]::IsNullOrWhiteSpace($response.Content)) { return $null }
  return ($response.Content | ConvertFrom-Json -Depth 50)
}

function Invoke-ToastPagedGet {
  param(
    [string]$Path,
    [string]$RestaurantGuid,
    [hashtable]$Query = $null
  )

  $results = New-Object System.Collections.Generic.List[object]
  $pageToken = $null

  do {
    $pageQuery = @{}
    if ($null -ne $Query) {
      foreach ($key in $Query.Keys) { $pageQuery[$key] = $Query[$key] }
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$pageToken)) {
      $pageQuery["pageToken"] = [string]$pageToken
    }

    $uri = Add-QueryParams -BaseUri ("{0}{1}" -f $toastApiUrl, $Path) -Query $pageQuery
    $response = Invoke-ToastWebRequest -Uri $uri -RestaurantGuid $RestaurantGuid

    if (-not [string]::IsNullOrWhiteSpace($response.Content)) {
      $payload = $response.Content | ConvertFrom-Json -Depth 50
      foreach ($item in (Get-ArrayOrEmpty $payload)) {
        if ($null -ne $item) { [void]$results.Add($item) }
      }
    }

    $nextHeader = $response.Headers["Toast-Next-Page-Token"]
    if ($nextHeader -is [System.Array]) {
      $pageToken = $nextHeader[0]
    } else {
      $pageToken = $nextHeader
    }
  } while (-not [string]::IsNullOrWhiteSpace([string]$pageToken))

  return $results.ToArray()
}

$script:SupabaseHeaders = @{
  "apikey"        = $supabaseKey
  "Authorization" = "Bearer $supabaseKey"
  "Content-Type"  = "application/json"
  "Prefer"        = "resolution=merge-duplicates,return=minimal"
}

function Write-ToSupabase {
  param(
    [object[]]$Rows,
    [string]$Table,
    [string]$Conflict,
    [int]$BatchSize = 200
  )

  if ($null -eq $Rows -or $Rows.Count -eq 0) { return 0 }

  $written = 0
  for ($start = 0; $start -lt $Rows.Count; $start += $BatchSize) {
    $end = [Math]::Min($start + $BatchSize - 1, $Rows.Count - 1)
    $slice = @($Rows[$start..$end])

    $json = if ($slice.Count -eq 1) {
      "[" + ($slice[0] | ConvertTo-Json -Depth 20 -Compress) + "]"
    } else {
      $slice | ConvertTo-Json -Depth 20 -Compress
    }

    $uri = "{0}/rest/v1/{1}" -f $supabaseUrl, $Table
    if (-not [string]::IsNullOrWhiteSpace($Conflict)) {
      $uri = $uri + "?on_conflict=" + $Conflict
    }

    for ($attempt = 0; $attempt -lt 3; $attempt++) {
      try {
        Invoke-WebRequest -Uri $uri -Method POST -Headers $script:SupabaseHeaders -Body $json -ContentType "application/json" | Out-Null
        $written += $slice.Count
        break
      } catch {
        $statusCode = $null
        try {
          if ($null -ne $_.Exception.Response -and $null -ne $_.Exception.Response.StatusCode) {
            $statusCode = [int]$_.Exception.Response.StatusCode
          }
        } catch {}

        $body = Get-HttpErrorBody $_
        if ($statusCode -in @(408, 409, 425, 429, 500, 502, 503, 504) -and $attempt -lt 2) {
          $delay = [int]([Math]::Pow(2, $attempt) * 2)
          Write-Log ("  SUPABASE RETRY {0}/3 on {1} after HTTP {2}" -f ($attempt + 1), $Table, $statusCode) "Yellow"
          Start-Sleep -Seconds $delay
        } else {
          if ([string]::IsNullOrWhiteSpace($body)) {
            $body = $_.Exception.Message
          }
          throw ("Supabase write failed on {0}: {1}" -f $Table, $body)
        }
      }
    }
  }

  return $written
}

function Get-LocationIdMap {
  $map = @{}
  $resp = Invoke-RestMethod -Uri ("{0}/rest/v1/locations?select=id,toast_restaurant_guid&limit=1000" -f $supabaseUrl) -Headers $script:SupabaseHeaders
  foreach ($loc in (Get-ArrayOrEmpty $resp)) {
    $guid = Get-PropValue $loc "toast_restaurant_guid"
    $id   = Get-PropValue $loc "id"
    if (-not [string]::IsNullOrWhiteSpace([string]$guid) -and $null -ne $id) {
      $map[[string]$guid] = $id
    }
  }
  return $map
}

function Sync-SimpleConfigTable {
  param(
    [string]$Endpoint,
    [string]$Table,
    [string]$ConflictKey,
    [scriptblock]$MapRow,
    [int]$BatchSize = 200
  )

  $grandTotal = 0
  foreach ($loc in $locations) {
    $locationId = $script:LocationIdMap[$loc.guid]
    if (-not $locationId) {
      Mark-Error ("  ERROR {0}: missing location_id in Supabase" -f $loc.name)
      continue
    }

    try {
      $items = Invoke-ToastPagedGet -Path $Endpoint -RestaurantGuid $loc.guid
      $rows = New-Object System.Collections.Generic.List[object]

      foreach ($item in $items) {
        if (-not (Get-PropValue $item "guid")) { continue }
        $row = & $MapRow $item $locationId
        if ($null -ne $row) { [void]$rows.Add($row) }
      }

      $written = Write-ToSupabase -Rows $rows.ToArray() -Table $Table -Conflict $ConflictKey -BatchSize $BatchSize
      $grandTotal += $written
      Write-Log ("  {0}: {1} rows" -f $loc.name, $written)
    } catch {
      Mark-Error ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message)
    }

    Start-Sleep -Milliseconds 100
  }

  return $grandTotal
}

function Add-MenuGroupRecursive {
  param(
    $Group,
    [string]$MenuGuid,
    [int]$LocationId,
    [ref]$GroupRows,
    [ref]$ItemRows,
    [hashtable]$SeenGroups,
    [hashtable]$SeenItems
  )

  if ($null -eq $Group) { return }

  $groupGuid = Get-PropValue $Group "guid"
  if ([string]::IsNullOrWhiteSpace([string]$groupGuid)) { return }

  if (-not $SeenGroups.ContainsKey($groupGuid)) {
    $SeenGroups[$groupGuid] = $true
    [void]$GroupRows.Value.Add(@{
      toast_menu_group_guid = $groupGuid
      location_id           = $LocationId
      toast_menu_guid       = $MenuGuid
      name                  = Get-PropValue $Group "name"
      updated_at            = $script:UtcNowStamp
    })
  }

  foreach ($item in (Get-ArrayOrEmpty (Get-PropValue $Group "menuItems"))) {
    $itemGuid = Get-PropValue $item "guid"
    if ([string]::IsNullOrWhiteSpace([string]$itemGuid)) { continue }

    if (-not $SeenItems.ContainsKey($itemGuid)) {
      $SeenItems[$itemGuid] = $true
      [void]$ItemRows.Value.Add(@{
        toast_menu_item_guid = $itemGuid
        location_id          = $LocationId
        toast_menu_group_id  = $null
        menu_group_guid      = $groupGuid
        name                 = Get-PropValue $item "name"
        description          = Get-PropValue $item "description"
        sku                  = Get-PropValue $item "sku"
        price                = Get-PropValue $item "price"
        plu                  = Get-PropValue $item "plu"
        unit_of_measure      = Get-PropValue $item "unitOfMeasure"
        updated_at           = $script:UtcNowStamp
      })
    }
  }

  foreach ($child in (Get-ArrayOrEmpty (Get-PropValue $Group "menuGroups"))) {
    Add-MenuGroupRecursive -Group $child -MenuGuid $MenuGuid -LocationId $LocationId -GroupRows $GroupRows -ItemRows $ItemRows -SeenGroups $SeenGroups -SeenItems $SeenItems
  }
}

Write-Log "============================================"
Write-Log "Gastamo Weekly Config Sync Started"
Write-Log "============================================"

# AUTH
try {
  $authBody = @{
    clientId       = $clientId
    clientSecret   = $clientSecret
    userAccessType = "TOAST_MACHINE_CLIENT"
  } | ConvertTo-Json -Compress

  $authResponse = Invoke-RestMethod -Uri ("{0}/authentication/v1/authentication/login" -f $toastApiUrl) -Method POST -Body $authBody -ContentType "application/json"
  $script:ToastToken = $authResponse.token.accessToken
  Write-Log ("Toast token acquired (length={0})" -f $script:ToastToken.Length) "Green"
} catch {
  throw ("FATAL: Toast auth failed :: {0}" -f $_.Exception.Message)
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

# 1. LOCATIONS
Write-Log "--- Syncing locations ---" "Cyan"
$script:UtcNowStamp = Get-UtcStamp
$locationRows = New-Object System.Collections.Generic.List[object]

foreach ($loc in $locations) {
  try {
    $info = Invoke-ToastJson -Path ("/restaurants/v1/restaurants/{0}" -f $loc.guid) -RestaurantGuid $loc.guid -Query @{ includeArchived = "false" }
    $general = Get-PropValue $info "general"
    $location = Get-PropValue $info "location"

    [void]$locationRows.Add(@{
      toast_restaurant_guid = $loc.guid
      name                  = Get-PropValue $general "name"
      store_number          = Get-PropValue $general "locationCode"
      address1              = Get-PropValue $location "address1"
      address2              = Get-PropValue $location "address2"
      city                  = Get-PropValue $location "city"
      state                 = Coalesce (Get-PropValue $location "administrativeArea") (Get-PropValue $location "stateCode")
      zip                   = Get-PropValue $location "zipCode"
      phone                 = Get-PropValue $location "phone"
      timezone              = Get-PropValue $general "timeZone"
      closeout_hour         = Get-PropValue $general "closeoutHour"
      updated_at            = $script:UtcNowStamp
    })
  } catch {
    Mark-Error ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message)
  }

  Start-Sleep -Milliseconds 100
}

try {
  $written = Write-ToSupabase -Rows $locationRows.ToArray() -Table "locations" -Conflict "toast_restaurant_guid"
  Write-Log ("locations total: {0}" -f $written) "Green"
} catch {
  Mark-Error ("ERROR syncing locations: {0}" -f $_.Exception.Message)
}

$script:LocationIdMap = Get-LocationIdMap

# 2. EMPLOYEES
Write-Log "--- Syncing employees ---" "Cyan"
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $script:LocationIdMap[$loc.guid]
  if (-not $locationId) {
    Mark-Error ("  ERROR {0}: missing location_id in Supabase" -f $loc.name)
    continue
  }

  try {
    $employees = Invoke-ToastJson -Path "/labor/v1/employees" -RestaurantGuid $loc.guid
    $rows = New-Object System.Collections.Generic.List[object]
    foreach ($emp in (Get-ArrayOrEmpty $employees)) {
      $guid = Get-PropValue $emp "guid"
      if (-not $guid) { continue }

      [void]$rows.Add(@{
        toast_employee_guid = $guid
        location_id         = $locationId
        external_id         = Get-PropValue $emp "externalId"
        first_name          = Get-PropValue $emp "firstName"
        last_name           = Get-PropValue $emp "lastName"
        chosen_name         = Get-PropValue $emp "chosenName"
        email               = Get-PropValue $emp "email"
        phone_mobile        = Coalesce (Get-PropValue $emp "phoneMobile") (Get-PropValue $emp "phoneNumber")
        deleted             = [bool](Get-PropValue $emp "deleted" $false)
        disabled            = [bool](Get-PropValue $emp "disabled" $false)
        archived            = [bool](Get-PropValue $emp "archived" $false)
        created_date        = Get-PropValue $emp "createdDate"
        modified_date       = Get-PropValue $emp "modifiedDate"
        deleted_date        = Get-PropValue $emp "deletedDate"
        updated_at          = Get-UtcStamp
      })
    }

    $written = Write-ToSupabase -Rows $rows.ToArray() -Table "employees" -Conflict "location_id,toast_employee_guid"
    $grandTotal += $written
    Write-Log ("  {0}: {1} employees" -f $loc.name, $written)
  } catch {
    Mark-Error ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message)
  }

  Start-Sleep -Milliseconds 100
}
Write-Log ("employees total: {0}" -f $grandTotal) "Green"

# 3. JOBS
Write-Log "--- Syncing jobs ---" "Cyan"
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $script:LocationIdMap[$loc.guid]
  if (-not $locationId) {
    Mark-Error ("  ERROR {0}: missing location_id in Supabase" -f $loc.name)
    continue
  }

  try {
    $jobs = Invoke-ToastJson -Path "/labor/v1/jobs" -RestaurantGuid $loc.guid
    $rows = New-Object System.Collections.Generic.List[object]
    foreach ($job in (Get-ArrayOrEmpty $jobs)) {
      $guid = Get-PropValue $job "guid"
      if (-not $guid) { continue }

      [void]$rows.Add(@{
        toast_job_guid = $guid
        location_id    = $locationId
        external_id    = Get-PropValue $job "externalId"
        title          = Get-PropValue $job "title"
        code           = Get-PropValue $job "code"
        default_wage   = Get-PropValue $job "defaultWage"
        tipped         = [bool](Get-PropValue $job "tipped" $false)
        deleted        = [bool](Get-PropValue $job "deleted" $false)
        created_date   = Get-PropValue $job "createdDate"
        modified_date  = Get-PropValue $job "modifiedDate"
        updated_at     = Get-UtcStamp
      })
    }

    $written = Write-ToSupabase -Rows $rows.ToArray() -Table "jobs" -Conflict "location_id,toast_job_guid"
    $grandTotal += $written
    Write-Log ("  {0}: {1} jobs" -f $loc.name, $written)
  } catch {
    Mark-Error ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message)
  }

  Start-Sleep -Milliseconds 100
}
Write-Log ("jobs total: {0}" -f $grandTotal) "Green"

# 4-10. MENUS (Menus V2 single pull per location; fixed response parsing)
Write-Log "--- Syncing menus ---" "Cyan"
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $script:LocationIdMap[$loc.guid]
  if (-not $locationId) {
    Mark-Error ("  ERROR {0}: missing location_id in Supabase" -f $loc.name)
    continue
  }

  try {
    $menuData = Invoke-ToastJson -Path "/menus/v2/menus" -RestaurantGuid $loc.guid

    $script:UtcNowStamp = Get-UtcStamp

    $menuRows        = New-Object System.Collections.Generic.List[object]
    $groupRows       = New-Object System.Collections.Generic.List[object]
    $itemRows        = New-Object System.Collections.Generic.List[object]
    $modGroupRows    = New-Object System.Collections.Generic.List[object]
    $modOptionRows   = New-Object System.Collections.Generic.List[object]
    $preGroupRows    = New-Object System.Collections.Generic.List[object]
    $preRows         = New-Object System.Collections.Generic.List[object]

    $seenMenus       = @{}
    $seenGroups      = @{}
    $seenItems       = @{}
    $seenModGroups   = @{}
    $seenModOptions  = @{}
    $seenPreGroups   = @{}
    $seenPreMods     = @{}

    $menus = Get-ArrayOrEmpty (Get-PropValue $menuData "menus")
    foreach ($menu in $menus) {
      $menuGuid = Get-PropValue $menu "guid"
      if (-not $menuGuid) { continue }

      if (-not $seenMenus.ContainsKey($menuGuid)) {
        $seenMenus[$menuGuid] = $true
        [void]$menuRows.Add(@{
          toast_menu_guid  = $menuGuid
          location_id      = $locationId
          name             = Get-PropValue $menu "name"
          description      = Get-PropValue $menu "description"
          orderable_online = Get-PropValue $menu "orderableOnline"
          updated_at       = $script:UtcNowStamp
        })
      }

      foreach ($group in (Get-ArrayOrEmpty (Get-PropValue $menu "menuGroups"))) {
        Add-MenuGroupRecursive -Group $group -MenuGuid $menuGuid -LocationId $locationId -GroupRows ([ref]$groupRows) -ItemRows ([ref]$itemRows) -SeenGroups $seenGroups -SeenItems $seenItems
      }
    }

    $modifierGroupMap = Get-PropValue $menuData "modifierGroupReferences"
    $modifierOptionMap = Get-PropValue $menuData "modifierOptionReferences"
    if ($null -ne $modifierGroupMap) {
      foreach ($prop in $modifierGroupMap.PSObject.Properties) {
        $modGroup = $prop.Value
        $modGroupGuid = Get-PropValue $modGroup "guid"
        if (-not $modGroupGuid) { continue }

        if (-not $seenModGroups.ContainsKey($modGroupGuid)) {
          $seenModGroups[$modGroupGuid] = $true
          [void]$modGroupRows.Add(@{
            toast_modifier_group_guid = $modGroupGuid
            location_id               = $locationId
            name                      = Get-PropValue $modGroup "name"
            min_selections            = Get-PropValue $modGroup "minSelections"
            max_selections            = Get-PropValue $modGroup "maxSelections"
            updated_at                = $script:UtcNowStamp
          })
        }

        foreach ($refId in (Get-ArrayOrEmpty (Get-PropValue $modGroup "modifierOptionReferences"))) {
          $modOption = Get-ReferenceMapValue -Map $modifierOptionMap -Key $refId
          $modOptionGuid = Get-PropValue $modOption "guid"
          if (-not $modOptionGuid) { continue }

          if (-not $seenModOptions.ContainsKey($modOptionGuid)) {
            $seenModOptions[$modOptionGuid] = $true
            [void]$modOptionRows.Add(@{
              toast_modifier_option_guid = $modOptionGuid
              location_id                = $locationId
              toast_modifier_group_guid  = $modGroupGuid
              name                       = Get-PropValue $modOption "name"
              price                      = Get-PropValue $modOption "price"
              updated_at                 = $script:UtcNowStamp
            })
          }
        }
      }
    }

    $preGroupMap = Get-PropValue $menuData "preModifierGroupReferences"
    if ($null -ne $preGroupMap) {
      foreach ($prop in $preGroupMap.PSObject.Properties) {
        $preGroup = $prop.Value
        $preGroupGuid = Get-PropValue $preGroup "guid"
        if (-not $preGroupGuid) { continue }

        if (-not $seenPreGroups.ContainsKey($preGroupGuid)) {
          $seenPreGroups[$preGroupGuid] = $true
          [void]$preGroupRows.Add(@{
            toast_pre_modifier_group_guid = $preGroupGuid
            location_id                   = $locationId
            name                          = Get-PropValue $preGroup "name"
            updated_at                    = $script:UtcNowStamp
          })
        }

        foreach ($preMod in (Get-ArrayOrEmpty (Get-PropValue $preGroup "preModifiers"))) {
          $preModGuid = Get-PropValue $preMod "guid"
          if (-not $preModGuid) { continue }

          if (-not $seenPreMods.ContainsKey($preModGuid)) {
            $seenPreMods[$preModGuid] = $true
            [void]$preRows.Add(@{
              toast_pre_modifier_guid       = $preModGuid
              location_id                   = $locationId
              toast_pre_modifier_group_guid = $preGroupGuid
              name                          = Get-PropValue $preMod "name"
              updated_at                    = $script:UtcNowStamp
            })
          }
        }
      }
    }

    [void](Write-ToSupabase -Rows $menuRows.ToArray()      -Table "menus"               -Conflict "location_id,toast_menu_guid")
    [void](Write-ToSupabase -Rows $groupRows.ToArray()     -Table "menu_groups"         -Conflict "location_id,toast_menu_group_guid")
    [void](Write-ToSupabase -Rows $itemRows.ToArray()      -Table "menu_items"          -Conflict "location_id,toast_menu_item_guid")
    [void](Write-ToSupabase -Rows $modGroupRows.ToArray()  -Table "modifier_groups"     -Conflict "location_id,toast_modifier_group_guid")
    [void](Write-ToSupabase -Rows $modOptionRows.ToArray() -Table "modifier_options"    -Conflict "location_id,toast_modifier_option_guid")
    [void](Write-ToSupabase -Rows $preGroupRows.ToArray()  -Table "pre_modifier_groups" -Conflict "location_id,toast_pre_modifier_group_guid")
    [void](Write-ToSupabase -Rows $preRows.ToArray()       -Table "pre_modifiers"       -Conflict "location_id,toast_pre_modifier_guid")

    $locTotal = $menuRows.Count + $groupRows.Count + $itemRows.Count + $modGroupRows.Count + $modOptionRows.Count + $preGroupRows.Count + $preRows.Count
    $grandTotal += $locTotal

    Write-Log ("  {0}: {1} menus, {2} groups, {3} items, {4} mod groups, {5} mod options, {6} pre-mod groups, {7} pre-modifiers" -f `
      $loc.name, $menuRows.Count, $groupRows.Count, $itemRows.Count, $modGroupRows.Count, $modOptionRows.Count, $preGroupRows.Count, $preRows.Count)
  } catch {
    Mark-Error ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message)
  }

  Start-Sleep -Milliseconds 100
}
Write-Log ("menus total: {0} rows across all menu tables" -f $grandTotal) "Green"

# 11. REVENUE CENTERS
Write-Log "--- Syncing revenue_centers ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/revenueCenters" "revenue_centers" "location_id,toast_revenue_center_guid" {
  param($item, $locationId)
  @{
    toast_revenue_center_guid = Get-PropValue $item "guid"
    location_id               = $locationId
    entity_type               = Get-PropValue $item "entityType"
    name                      = Get-PropValue $item "name"
    updated_at                = Get-UtcStamp
  }
}
Write-Log ("revenue_centers total: {0}" -f $total) "Green"

# 12. PREP STATIONS
Write-Log "--- Syncing prep_stations ---" "Cyan"
$total = Sync-SimpleConfigTable "/kitchen/v1/published/prepStations" "prep_stations" "location_id,toast_prep_station_guid" {
  param($item, $locationId)
  @{
    toast_prep_station_guid = Get-PropValue $item "guid"
    location_id             = $locationId
    entity_type             = Get-PropValue $item "entityType"
    name                    = Get-PropValue $item "name"
    updated_at              = Get-UtcStamp
  }
}
Write-Log ("prep_stations total: {0}" -f $total) "Green"

# 13. DINING OPTIONS
Write-Log "--- Syncing dining_options ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/diningOptions" "dining_options" "location_id,toast_dining_option_guid" {
  param($item, $locationId)
  @{
    toast_dining_option_guid = Get-PropValue $item "guid"
    location_id              = $locationId
    toast_external_id        = Get-PropValue $item "externalId"
    entity_type              = Get-PropValue $item "entityType"
    name                     = Get-PropValue $item "name"
    behavior                 = Get-PropValue $item "behavior"
    curbside                 = [bool](Get-PropValue $item "curbside" $false)
    updated_at               = Get-UtcStamp
  }
}
Write-Log ("dining_options total: {0}" -f $total) "Green"

# 14. SERVICE AREAS
Write-Log "--- Syncing service_areas ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/serviceAreas" "service_areas" "location_id,toast_service_area_guid" {
  param($item, $locationId)
  @{
    toast_service_area_guid    = Get-PropValue $item "guid"
    location_id                = $locationId
    toast_external_id          = Get-PropValue $item "externalId"
    entity_type                = Get-PropValue $item "entityType"
    name                       = Get-PropValue $item "name"
    revenue_center_guid        = Get-NestedPropValue $item @("revenueCenter", "guid")
    revenue_center_entity_type = Get-NestedPropValue $item @("revenueCenter", "entityType")
    updated_at                 = Get-UtcStamp
  }
}
Write-Log ("service_areas total: {0}" -f $total) "Green"

# 15. RESTAURANT TABLES
Write-Log "--- Syncing restaurant_tables ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/tables" "restaurant_tables" "location_id,toast_table_guid" {
  param($item, $locationId)
  @{
    toast_table_guid    = Get-PropValue $item "guid"
    location_id         = $locationId
    entity_type         = Get-PropValue $item "entityType"
    name                = Get-PropValue $item "name"
    service_area_guid   = Get-NestedPropValue $item @("serviceArea", "guid")
    revenue_center_guid = Get-NestedPropValue $item @("revenueCenter", "guid")
    updated_at          = Get-UtcStamp
  }
}
Write-Log ("restaurant_tables total: {0}" -f $total) "Green"

# 16. TAX RATES
Write-Log "--- Syncing tax_rates ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/taxRates" "tax_rates" "location_id,toast_tax_rate_guid" {
  param($item, $locationId)
  @{
    toast_tax_rate_guid   = Get-PropValue $item "guid"
    location_id           = $locationId
    entity_type           = Get-PropValue $item "entityType"
    name                  = Get-PropValue $item "name"
    is_default            = [bool](Get-PropValue $item "isDefault" $false)
    rate                  = Get-PropValue $item "rate"
    type                  = Get-PropValue $item "type"
    rounding_type         = Get-PropValue $item "roundingType"
    tax_table             = if ((Get-ArrayOrEmpty (Get-PropValue $item "taxTable")).Count -gt 0) { (Get-PropValue $item "taxTable" | ConvertTo-Json -Compress -Depth 10) } else { $null }
    conditional_tax_rates = if ((Get-ArrayOrEmpty (Get-PropValue $item "conditionalTaxRates")).Count -gt 0) { (Get-PropValue $item "conditionalTaxRates" | ConvertTo-Json -Compress -Depth 10) } else { $null }
    updated_at            = Get-UtcStamp
  }
}
Write-Log ("tax_rates total: {0}" -f $total) "Green"

# 17. VOID REASONS
Write-Log "--- Syncing void_reasons ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/voidReasons" "void_reasons" "location_id,toast_void_reason_guid" {
  param($item, $locationId)
  @{
    toast_void_reason_guid = Get-PropValue $item "guid"
    location_id            = $locationId
    entity_type            = Get-PropValue $item "entityType"
    name                   = Get-PropValue $item "name"
    updated_at             = Get-UtcStamp
  }
}
Write-Log ("void_reasons total: {0}" -f $total) "Green"

# 18. NO SALE REASONS
Write-Log "--- Syncing no_sale_reasons ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/noSaleReasons" "no_sale_reasons" "location_id,toast_no_sale_reason_guid" {
  param($item, $locationId)
  @{
    toast_no_sale_reason_guid = Get-PropValue $item "guid"
    location_id               = $locationId
    entity_type               = Get-PropValue $item "entityType"
    name                      = Get-PropValue $item "name"
    updated_at                = Get-UtcStamp
  }
}
Write-Log ("no_sale_reasons total: {0}" -f $total) "Green"

# 19. PAYOUT REASONS
Write-Log "--- Syncing payout_reasons ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/payoutReasons" "payout_reasons" "location_id,toast_payout_reason_guid" {
  param($item, $locationId)
  @{
    toast_payout_reason_guid = Get-PropValue $item "guid"
    location_id              = $locationId
    entity_type              = Get-PropValue $item "entityType"
    name                     = Get-PropValue $item "name"
    updated_at               = Get-UtcStamp
  }
}
Write-Log ("payout_reasons total: {0}" -f $total) "Green"

# 20. RESTAURANT SERVICES
Write-Log "--- Syncing restaurant_services ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/restaurantServices" "restaurant_services" "location_id,toast_restaurant_service_guid" {
  param($item, $locationId)
  @{
    toast_restaurant_service_guid = Get-PropValue $item "guid"
    location_id                   = $locationId
    entity_type                   = Get-PropValue $item "entityType"
    name                          = Get-PropValue $item "name"
    updated_at                    = Get-UtcStamp
  }
}
Write-Log ("restaurant_services total: {0}" -f $total) "Green"

# 21. PRICE GROUPS
Write-Log "--- Syncing price_groups ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/priceGroups" "price_groups" "location_id,toast_price_group_guid" {
  param($item, $locationId)
  @{
    toast_price_group_guid = Get-PropValue $item "guid"
    location_id            = $locationId
    entity_type            = Get-PropValue $item "entityType"
    name                   = Get-PropValue $item "name"
    updated_at             = Get-UtcStamp
  }
}
Write-Log ("price_groups total: {0}" -f $total) "Green"

# 22. ALTERNATE PAYMENT TYPES
Write-Log "--- Syncing alternate_payment_types ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/alternatePaymentTypes" "alternate_payment_types" "location_id,toast_alt_payment_type_guid" {
  param($item, $locationId)
  @{
    toast_alt_payment_type_guid = Get-PropValue $item "guid"
    location_id                 = $locationId
    toast_external_id           = Get-PropValue $item "externalId"
    entity_type                 = Get-PropValue $item "entityType"
    name                        = Get-PropValue $item "name"
    updated_at                  = Get-UtcStamp
  }
}
Write-Log ("alternate_payment_types total: {0}" -f $total) "Green"

# 23. TIP WITHHOLDING
Write-Log "--- Syncing tip_withholding ---" "Cyan"
$grandTotal = 0
foreach ($loc in $locations) {
  $locationId = $script:LocationIdMap[$loc.guid]
  if (-not $locationId) {
    Mark-Error ("  ERROR {0}: missing location_id in Supabase" -f $loc.name)
    continue
  }

  try {
    $item = Invoke-ToastJson -Path "/config/v2/tipWithholding" -RestaurantGuid $loc.guid
    if ($null -eq $item) {
      Write-Log ("  {0}: no tip withholding config returned" -f $loc.name) "Yellow"
      continue
    }

    $rows = @(@{
      location_id = $locationId
      toast_guid  = Get-PropValue $item "guid"
      entity_type = Get-PropValue $item "entityType"
      enabled     = [bool](Get-PropValue $item "enabled" $false)
      percentage  = Get-PropValue $item "percentage"
      updated_at  = Get-UtcStamp
    })

    [void](Write-ToSupabase -Rows $rows -Table "tip_withholding" -Conflict "location_id" -BatchSize 1)
    $grandTotal++
    Write-Log ("  {0}: enabled={1}" -f $loc.name, ([bool](Get-PropValue $item "enabled" $false)))
  } catch {
    Mark-Error ("  ERROR {0}: {1}" -f $loc.name, $_.Exception.Message)
  }

  Start-Sleep -Milliseconds 100
}
Write-Log ("tip_withholding total: {0}" -f $grandTotal) "Green"

# 24. BREAK TYPES
Write-Log "--- Syncing break_types ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/breakTypes" "break_types" "location_id,toast_break_type_guid" {
  param($item, $locationId)
  @{
    toast_break_type_guid       = Get-PropValue $item "guid"
    location_id                 = $locationId
    entity_type                 = Get-PropValue $item "entityType"
    name                        = Get-PropValue $item "name"
    active                      = [bool](Get-PropValue $item "active" $true)
    paid                        = [bool](Get-PropValue $item "paid" $false)
    duration                    = Get-PropValue $item "duration"
    enforce_minimum_time        = [bool](Get-PropValue $item "enforceMinimumTime" $false)
    track_missed_breaks         = [bool](Get-PropValue $item "trackMissedBreaks" $false)
    break_interval_hrs          = Get-PropValue $item "breakIntervalHrs"
    break_interval_mins         = Get-PropValue $item "breakIntervalMins"
    track_break_acknowledgement = [bool](Get-PropValue $item "trackBreakAcknowledgement" $false)
    updated_at                  = Get-UtcStamp
  }
}
Write-Log ("break_types total: {0}" -f $total) "Green"

# 25. CASH DRAWERS
Write-Log "--- Syncing cash_drawers ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/cashDrawers" "cash_drawers" "location_id,toast_cash_drawer_guid" {
  param($item, $locationId)
  @{
    toast_cash_drawer_guid = Get-PropValue $item "guid"
    location_id            = $locationId
    entity_type            = Get-PropValue $item "entityType"
    printer_guid           = Get-NestedPropValue $item @("printer", "guid")
    printer_entity_type    = Get-NestedPropValue $item @("printer", "entityType")
    updated_at             = Get-UtcStamp
  }
}
Write-Log ("cash_drawers total: {0}" -f $total) "Green"

# 26. DISCOUNT CONFIGS
Write-Log "--- Syncing discount_configs ---" "Cyan"
$total = Sync-SimpleConfigTable "/config/v2/discounts" "discount_configs" "location_id,toast_discount_guid" {
  param($item, $locationId)
  @{
    toast_discount_guid   = Get-PropValue $item "guid"
    location_id           = $locationId
    entity_type           = Get-PropValue $item "entityType"
    name                  = Get-PropValue $item "name"
    active                = [bool](Get-PropValue $item "active" $true)
    type                  = Get-PropValue $item "type"
    percentage            = Get-PropValue $item "percentage"
    amount                = Get-PropValue $item "amount"
    selection_type        = Get-PropValue $item "selectionType"
    non_exclusive         = [bool](Get-PropValue $item "nonExclusive" $false)
    item_picking_priority = Get-PropValue $item "itemPickingPriority"
    fixed_total           = Get-PropValue $item "fixedTotal"
    promo_codes           = if ((Get-ArrayOrEmpty (Get-PropValue $item "promoCodes")).Count -gt 0) { (Get-PropValue $item "promoCodes" | ConvertTo-Json -Compress -Depth 10) } else { $null }
    updated_at            = Get-UtcStamp
  }
}
Write-Log ("discount_configs total: {0}" -f $total) "Green"

Write-Log "============================================"
if ($script:HadErrors) {
  Write-Log "Gastamo Weekly Config Sync Complete WITH ERRORS" "Yellow"
  Write-Log "============================================"
  throw "Weekly config sync completed with errors. Review the log output above."
} else {
  Write-Log "Gastamo Weekly Config Sync Complete" "Green"
  Write-Log "============================================"
}
