param([string]$BaseUrl = 'http://127.0.0.1:58000')

$ErrorActionPreference = 'Stop'
function Check($value, $message) {
    if (-not $value) { throw $message }
}
function Send-Json($path, $payload, $headers = @{}) {
    Invoke-RestMethod -Method POST -Uri "$BaseUrl/ipaklink/v1/analytics/$path" -Headers $headers -ContentType 'application/json' -Body ($payload | ConvertTo-Json -Depth 12 -Compress) -TimeoutSec 20
}
$device = @{
    installId = [guid]::NewGuid().ToString()
    identityEpoch = [guid]::NewGuid().ToString()
    platform = 'android'
    appVersion = '1.2.0'
}
$token = Send-Json 'anonymous-tokens' $device
Check ($token.code -eq 201 -and $token.data.expiresIn -eq 900) 'Anonymous token contract failed'
$headers = @{ 'X-Analytics-Token' = $token.data.analyticsToken }
$eventId = [guid]::NewGuid().ToString()
$event = @{
    eventId = $eventId
    eventName = 'screen_view'
    schemaVersion = 1
    occurredAt = [DateTime]::UtcNow.ToString('o')
    sessionId = [guid]::NewGuid().ToString()
    sequence = 1
    screenName = 'FactoryDetail'
    pageViewId = [guid]::NewGuid().ToString()
    properties = @{ entrySource = 'search' }
}
$batch = $device + @{
    batchId = [guid]::NewGuid().ToString()
    identityType = 'anonymous'
    policyVersion = $token.data.policyVersion
    events = @($event)
}
$accepted = Send-Json 'events/report' $batch $headers
Check ($accepted.data.results[0].status -eq 'accepted') "Expected a Kafka ACK, got $($accepted | ConvertTo-Json -Depth 8 -Compress)"
$duplicate = Send-Json 'events/report' $batch $headers
Check ($duplicate.data.results[0].status -eq 'duplicate') 'Duplicate event was not recognized'
$event.properties = @{ entrySource = 'home' }
$conflict = Send-Json 'events/report' $batch $headers
Check ($conflict.data.results[0].errorCode -eq 'eventIdConflict') 'Changed payload reused an event ID'
$event.eventId = [guid]::NewGuid().ToString()
$event.properties = @{ entrySource = 'unregistered' }
$invalid = Send-Json 'events/report' $batch $headers
Check ($invalid.data.results[0].errorCode -eq 'invalidProperties') 'Invalid properties were not rejected'
$checkAuth = Invoke-WebRequest -SkipHttpErrorCheck -Method POST -Uri "$BaseUrl/ipaklink/v1/analytics/events/report" -ContentType 'application/json' -Body ($batch | ConvertTo-Json -Depth 12 -Compress)
Check ($checkAuth.StatusCode -eq 401) 'Unauthenticated batch did not return 401'

function Read-Count($id) {
    $sql = "SELECT count() FROM analytics.events_raw WHERE event_id = toUUID('$id') FORMAT TSV"
    $result = docker compose exec -T clickhouse clickhouse-client --user analytics --password analytics_local --query $sql
    if ($LASTEXITCODE -ne 0) { throw 'ClickHouse query failed' }
    return [int]$result
}
$deadline = [DateTime]::UtcNow.AddSeconds(35)
do {
    $rows = Read-Count $eventId
    if ($rows -gt 0) { break }
    Start-Sleep -Milliseconds 500
} while ([DateTime]::UtcNow -lt $deadline)
Check ($rows -eq 1) "Expected exactly one ClickHouse row for this smoke event, got $rows"
Check ((Read-Count $event.eventId) -eq 0) 'Rejected event reached ClickHouse'
[pscustomobject]@{ eventId = $eventId; firstReport = 'accepted'; repeat = 'duplicate'; conflict = 'eventIdConflict'; invalid = 'invalidProperties'; missingToken = 401; clickHouseRows = $rows }
