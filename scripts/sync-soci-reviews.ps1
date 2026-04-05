# ================================
# SOCi Reviews Daily Delta Sync
# ================================

$sociApiKey     = $env:SOCI_API_KEY
$sociAccountId  = $env:SOCI_ACCOUNT_ID
$supabaseUrl    = $env:SUPABASE_URL
$supabaseKey    = $env:SUPABASE_KEY

$sociHeaders = @{ "soci-key" = $sociApiKey }
$supabaseHeaders = @{
    "apikey"        = $supabaseKey
    "Authorization" = "Bearer $supabaseKey"
    "Content-Type"  = "application/json"
    "Prefer"        = "resolution=merge-duplicates,return=minimal"
    "X-Upsert"      = "true"
}

# ================================
# SOCi Daily Delta Sync
# Run once per day
# ================================

$batchSize = 100
$start = 0
$totalProcessed = 0
$totalInserted = 0
$totalErrors = 0
$cutoffHours = 26 # slightly more than 24 to avoid missing edge cases

$cutoffDate = (Get-Date).ToUniversalTime().AddHours(-$cutoffHours)
Write-Host "Syncing reviews updated since: $cutoffDate"

function Parse-Date($val) {
    if (-not $val -or $val -eq "0000-00-00 00:00:00") { return $null }
    try { return [datetime]::Parse($val).ToString("yyyy-MM-ddTHH:mm:ss") }
    catch { return $null }
}

function Parse-Bool($val) {
    if ($val -eq $null) { return $false }
    if ($val -is [bool]) { return $val }
    return $val -eq "1"
}

function Sanitize-Text($val) {
    if (-not $val) { return $null }
    return [System.Text.RegularExpressions.Regex]::Replace($val, '[^\x09\x0A\x0D\x20-\x7E]', '')
}

$sociHeaders = @{ "soci-key" = $sociApiKey }
$supabaseHeaders = @{
    "apikey"        = $supabaseKey
    "Authorization" = "Bearer $supabaseKey"
    "Content-Type"  = "application/json"
    "Prefer"        = "resolution=merge-duplicates,return=minimal"
}

# Load location map
$locResponse = Invoke-RestMethod "$supabaseUrl/rest/v1/soci_locations?select=location_id,soci_remote_id" `
    -Headers @{ "apikey" = $supabaseKey; "Authorization" = "Bearer $supabaseKey" }

$locationMap = @{}
foreach ($loc in $locResponse) { $locationMap[$loc.soci_remote_id] = $loc.location_id }
Write-Host "Loaded $($locationMap.Count) locations"

$keepGoing = $true

while ($keepGoing) {
    Write-Host "Fetching $start to $($start + $batchSize)..."

    $response = Invoke-RestMethod "https://app.meetsoci.com/api/reviews/0/get_feed?network=gmb&start=$start&limit=$batchSize&account_id=$sociAccountId&sort_by=last_updated&sort_dir=desc" `
        -Method 'GET' -Headers $sociHeaders

    if ($response.status -ne "ok" -or -not $response.data -or $response.data.Count -eq 0) {
        Write-Host "No more data"
        break
    }

    $reviews = $response.data

    # Check if oldest record in this batch is older than cutoff
    $oldestInBatch = $reviews | 
        Where-Object { $_.last_updated } | 
        Sort-Object last_updated | 
        Select-Object -First 1

    if ($oldestInBatch) {
        $oldestDate = [datetime]::Parse($oldestInBatch.last_updated)
        if ($oldestDate -lt $cutoffDate) {
            # Filter to only records within cutoff
            $reviews = $reviews | Where-Object { 
                $_.last_updated -and [datetime]::Parse($_.last_updated) -ge $cutoffDate 
            }
            Write-Host "Reached cutoff - processing $($reviews.Count) remaining records then stopping"
            $keepGoing = $false
        }
    }

    if ($reviews.Count -eq 0) { break }

    $batch = @()

    foreach ($r in $reviews) {
        $projectRemoteId = $null
        $projectId = $null
        $profileName = $null

        if ($r.projects -and $r.projects.Count -gt 0) {
            $projectRemoteId = $r.projects[0].project_remote_id
            $projectId = $r.projects[0].id
            $profileName = $r.projects[0].profile_name
        }

        $locationId = $null
        if ($projectRemoteId -and $locationMap.ContainsKey($projectRemoteId)) {
            $locationId = $locationMap[$projectRemoteId]
        }

        $hasResponse = $false
        $responseMessage = $null
        $responseStatus = $null
        $responseAuthor = $null
        $responseAdminId = $null
        $responseAdminName = $null
        $responseTimestamp = $null
        $responsePublishedAt = $null
        $responseApproved = $false
        $responseIsDeleted = $false

        if ($r.responses -and $r.responses.Count -gt 0) {
            $hasResponse = $true
            $resp = $r.responses[0]
            $responseMessage     = Sanitize-Text $resp.message
            $responseStatus      = $resp.status
            $responseAuthor      = Sanitize-Text $resp.author
            $responseAdminId     = $resp.admin_id
            $responseAdminName   = Sanitize-Text $resp.admin_name
            $responseTimestamp   = Parse-Date $resp.timestamp
            $responsePublishedAt = Parse-Date $resp.published_at
            $responseApproved    = Parse-Bool $resp.approved
            $responseIsDeleted   = Parse-Bool $resp.is_deleted
        }

        $batch += @{
            location_id              = $locationId
            soci_review_id           = $r.id
            soci_stream_remote_id    = $r.stream_remote_id
            soci_remote_id           = $r.remote_id
            soci_project_id          = $projectId
            soci_project_remote_id   = $projectRemoteId
            soci_notif_id            = $r.notif_id
            soci_base_notif_id       = $r.base_notif_id
            soci_reviewer_id         = $r.reviewer_id
            network                  = $r.site
            network_url              = $r.network_url
            remote_score             = if ($r.remote_score) { [decimal]$r.remote_score } else { $null }
            profile_name             = Sanitize-Text $profileName
            review_text              = Sanitize-Text $r.review
            rating                   = $r.rating
            recommendation           = $r.recommendation
            sentiment                = $r.sentiment
            is_recommendation        = Parse-Bool $r.is_recommendation
            formatted_recommendation = Sanitize-Text $r.formatted_recommendation
            tags                     = $r.tags
            image_count              = if ($r.image_count) { [int]$r.image_count } else { 0 }
            images                   = ($r.images | ConvertTo-Json -Compress)
            author                   = Sanitize-Text $r.author
            portrait                 = $r.portrait
            reviewer_location        = $r.location
            reviewer_data            = $r.reviewer_data
            review_timestamp         = Parse-Date $r.timestamp
            last_updated             = Parse-Date $r.last_updated
            created_at_soci          = Parse-Date $r.created_at
            row_updated_at           = Parse-Date $r.row_updated_at
            has_response             = $hasResponse
            response_message         = $responseMessage
            response_status          = $responseStatus
            response_author          = $responseAuthor
            response_admin_id        = $responseAdminId
            response_admin_name      = $responseAdminName
            response_timestamp       = $responseTimestamp
            response_published_at    = $responsePublishedAt
            response_approved        = $responseApproved
            response_is_deleted      = $responseIsDeleted
            first_response_time      = Parse-Date $r.first_response_time
            first_response_admin_id  = $r.first_response_admin_id
            first_responder          = Sanitize-Text $r.first_responder
            response_time_seconds    = if ($r.response_time_seconds) { [int]$r.response_time_seconds } else { $null }
            review_response_override = $r.review_response_override
            resolved_by              = Sanitize-Text $r.resolved_by
            resolved_at              = Parse-Date $r.resolved_at
            state                    = $r.state
            is_spam                  = Parse-Bool $r.is_spam
            is_updated               = Parse-Bool $r.is_updated
            exclude_from_summary     = Parse-Bool $r.exclude_from_summary
            nlp_processed            = Parse-Bool $r.nlp_processed
            deleted                  = Parse-Bool $r.deleted
            deleted_at               = Parse-Date $r.deleted_at
            rdr_network_status       = $r.rdr_network_status
            chat_comments_count      = if ($r.chat_comments_count) { [int]$r.chat_comments_count } else { 0 }
            responses                = ($r.responses | ConvertTo-Json -Compress -Depth 5)
            updated_at               = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss")
        }
    }

    if ($batch.Count -eq 0) { 
        $start += $batchSize
        continue 
    }

    $json = $batch | ConvertTo-Json -Depth 5 -Compress
    $json = [System.Text.RegularExpressions.Regex]::Replace($json, '\\u[dD][89aAbB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2}', '')

    try {
        Invoke-RestMethod "$supabaseUrl/rest/v1/soci_reviews" `
            -Method POST `
            -Headers $supabaseHeaders `
            -Body $json | Out-Null

        $totalInserted += $batch.Count
        Write-Host "Upserted $($batch.Count) | Total: $totalInserted"
    }
    catch {
        $totalErrors++
        Write-Host "ERROR at start=$start : $_"
    }

    $totalProcessed += $reviews.Count
    $start += $batchSize
}

Write-Host ""
Write-Host "================================"
Write-Host "Delta sync complete"
Write-Host "Cutoff          : $cutoffDate"
Write-Host "Total processed : $totalProcessed"
Write-Host "Total upserted  : $totalInserted"
Write-Host "Total errors    : $totalErrors"
Write-Host "================================"
