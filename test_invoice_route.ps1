$ErrorActionPreference = "Stop"
$Base = if ($args.Count -ge 1 -and $args[0]) { $args[0].TrimEnd('/') } else { "https://invoice-agent-3j7m.onrender.com" }
$Uri = "$Base/a2a/message:send"
$TestId = "ROUTE-TEST-$([guid]::NewGuid().ToString('N'))"
$Principal = "route-diagnostic-$([guid]::NewGuid().ToString('N'))"
$OutputPath = Join-Path $PSScriptRoot "route_test_result.txt"

Write-Host "Invoice Agent route diagnostic"
Write-Host "URL: $Uri"
Write-Host "Test ID: $TestId"
Write-Host "Content-Type: application/a2a+json"
Write-Host "Safety: synthetic invoice with approval_status=pending; no payment is requested."
Write-Host "Sending request; a free Render instance may need to wake up...`n"

$invoicePackage = @{
    invoice_id = $TestId
    invoice_number = $TestId
    vendor = "Route Test Vendor"
    amount = 1.25
    currency = "USD"
    approval_status = "pending"
    receipt_required = $false
    evidence = @(
        @{ id = "EV-$TestId-INVOICE"; source = "route-test.json"; locator = "$.invoice_number"; quote = "Invoice number $TestId" },
        @{ id = "EV-$TestId-APPROVAL"; source = "route-test.json"; locator = "$.approval_status"; quote = "Payment approval status: pending" }
    )
}
$payload = @{
    message = @{
        messageId = "msg-$TestId"
        role = "ROLE_USER"
        parts = @(@{ data = @{ invoice_package = $invoicePackage } })
    }
}
$json = ConvertTo-Json -InputObject $payload -Depth 30 -Compress
$headers = @{ "A2A-Version" = "1.0"; "X-Principal-ID" = $Principal; "Accept" = "application/a2a+json" }
$status = 0
$body = ""
$contentType = ""
try {
    $response = Invoke-WebRequest -Uri $Uri -Method POST -ContentType "application/a2a+json" -Headers $headers -Body $json -TimeoutSec 110 -UseBasicParsing
    $status = [int]$response.StatusCode
    $body = [string]$response.Content
    if ($response.Headers["Content-Type"]) { $contentType = [string]$response.Headers["Content-Type"] }
} catch {
    $body = [string]$_.ErrorDetails.Message
    if ($_.Exception.Response) {
        try { $status = [int]$_.Exception.Response.StatusCode } catch {}
        try {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $body = $reader.ReadToEnd()
            $reader.Dispose()
        } catch {}
        try { $contentType = [string]$_.Exception.Response.ContentType } catch {}
    }
}
@("Time: $([DateTime]::UtcNow.ToString('o'))", "URL: $Uri", "Test ID: $TestId", "HTTP status: $status", "Content-Type: $contentType", "Response body:", $body) | Set-Content -LiteralPath $OutputPath -Encoding UTF8

Write-Host "HTTP status: $status"
Write-Host "Content-Type: $contentType"
if ($body) { Write-Host "Response body:"; Write-Host $body }
if ($status -ne 200) { throw "A2A route did not return HTTP 200. See $OutputPath" }
$result = $body | ConvertFrom-Json
if (-not $result.task) { throw "Expected a task envelope. See $OutputPath" }
$task = $result.task
Write-Host "Task state: $($task.status.state)"
if (-not $task.artifacts -or -not $task.artifacts[0].parts -or -not $task.artifacts[0].parts[0].text) {
    throw "No decision artifact returned. See $OutputPath"
}
$decisionResult = $task.artifacts[0].parts[0].text | ConvertFrom-Json
$decision = $decisionResult.decisions[0]
Write-Host "Action: $($decision.action)"
Write-Host "Analysis method: $($decision.analysis_method)"
Write-Host "Authorized: $($decision.authorized)"
Write-Host "Action status: $($decision.action_status)"
if ($decision.authorized -ne $false -or $decision.action_status -ne "not_executed") {
    throw "Safety assertion failed: pending invoice was not safely blocked. See $OutputPath"
}
Write-Host "PASS: Exact A2A route returned HTTP 200 and did not authorize the pending invoice."
Write-Host "Full response saved to: $OutputPath"
