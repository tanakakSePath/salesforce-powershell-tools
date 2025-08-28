param (
    [string[]]$envs = @("Production", "Sandbox2")  # デフォルト
)

# ===== 共通設定 =====
$apiVersion = "v64.0"  # 必要に応じて変更
function Get-Utf8EncodingParam {
    if ($PSVersionTable.PSVersion.Major -ge 7) { return "utf8BOM" }
    else { return "UTF8" } # Windows PowerShell 5 はBOM付き
}

# ===== 設定ファイル読み込み =====
$connectionSettings = Get-Content -Raw -Encoding UTF8 -Path  "../Config/connectionSettings.json" | ConvertFrom-Json
$soqlSettings       = Get-Content -Raw -Encoding UTF8 -Path "../Config/soqlSettings.json"       | ConvertFrom-Json

# ===== 指定環境でフィルタ =====
$targetEnvs = $connectionSettings.environments | Where-Object { $_.name -in $envs }
if ($targetEnvs.Count -eq 0) {
    Write-Host "❌ 指定された環境が connectionSettings.json に見つかりません。"
    exit 1
}

# ===== Bulk 2.0 実行関数 =====
function Invoke-SFBulkQuery {
    param(
        [Parameter(Mandatory=$true)][string]$InstanceUrl,
        [Parameter(Mandatory=$true)][hashtable]$Headers,
        [Parameter(Mandatory=$true)][string]$Soql,
        [Parameter(Mandatory=$true)][string]$CsvPath,
        [int]$PollSeconds = 2,
        [int]$TimeoutSeconds = 600   # 10分でタイムアウト
    )

    $createBody = @{
        operation       = "query"
        query           = $Soql
        columnDelimiter = "COMMA"
        lineEnding      = "CRLF"
    } | ConvertTo-Json

    $jobUrl = "$InstanceUrl/services/data/v64.0/jobs/query"
    $job = Invoke-RestMethod -Method Post -Uri $jobUrl -Headers ($Headers + @{ "Content-Type" = "application/json" }) -Body $createBody
    if (-not $job.id) { throw "ジョブ作成に失敗しました。" }

    $jobId = $job.id
    Write-Host "   🆔 JobId: $jobId"

    $statusUrl = "$InstanceUrl/services/data/v64.0/jobs/query/$jobId"
    $elapsed = 0
    $lastState = ""

    while ($true) {
        Start-Sleep -Seconds $PollSeconds
        $elapsed += $PollSeconds

        $st = Invoke-RestMethod -Method Get -Uri $statusUrl -Headers $Headers

        if ($st.state -ne $lastState -or ($elapsed % 10) -eq 0) {
            $prog = @()
            if ($st.numberRecordsProcessed -ne $null) { $prog += "processed=$($st.numberRecordsProcessed)" }
            if ($st.numberRecordsFailed -ne $null)    { $prog += "failed=$($st.numberRecordsFailed)" }
            $p = if ($prog.Count) { " (" + ($prog -join ", ") + ")" } else { "" }
            Write-Host ("   ⏳ {0} {1}s{2}" -f $st.state, $elapsed, $p)
            $lastState = $st.state
        }

        if ($st.state -eq "JobComplete") {
            Write-Host "   ✅ Completed."
            break   # ここで while を抜ける
        } elseif ($st.state -eq "Aborted") {
            throw "ジョブが中止されました。"
        } elseif ($st.state -eq "Failed") {
            $failedUrl = "$InstanceUrl/services/data/v64.0/jobs/query/$jobId/failedResults"
            try {
                $failedCsv = Invoke-WebRequest -Method Get -Uri $failedUrl -Headers ($Headers + @{ "Accept" = "text/csv" })
                $sample = ($failedCsv.Content -split "`n") | Select-Object -First 5
                throw "ジョブが失敗しました: $($st.errorMessage)`n--- failedResults sample ---`n$($sample -join "`n")"
            } catch {
                throw "ジョブが失敗しました: $($st.errorMessage)"
            }
        }

        if ($elapsed -ge $TimeoutSeconds) {
            throw "ジョブがタイムアウトしました（$TimeoutSeconds 秒）。状態: $($st.state)"
        }
    }

    # --- 結果取得（CSV）---
    $resultsBase = "$InstanceUrl/services/data/v64.0/jobs/query/$jobId/results"
    $locator = $null
    $firstChunk = $true

    do {
        $resultsUrl = if ($locator) { "$resultsBase?locator=$locator" } else { $resultsBase }

        $resp = Invoke-WebRequest -Method Get -Uri $resultsUrl -Headers ($Headers + @{ "Accept" = "text/csv" })
        # PS5 互換: 三項演算子を使わない
        if ($resp.ContentEncoding) {
            $text = [System.Text.Encoding]::UTF8.GetString($resp.RawContentStream.ToArray())
        } else {
            $text = $resp.Content
        }

        if ($firstChunk) {
            if ($PSVersionTable.PSVersion.Major -ge 7) {
                $text | Out-File -FilePath $CsvPath -Encoding utf8BOM
            } else {
                $text | Out-File -FilePath $CsvPath -Encoding UTF8
            }
            $firstChunk = $false
        } else {
            Add-Content -Path $CsvPath -Value $text -Encoding UTF8
        }

        $locator = $resp.Headers["Sforce-Locator"]
    } while ($locator -and $locator -ne "null")
}


# ===== 実行本体 =====
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

foreach ($env in $targetEnvs) {
    Write-Host "`n🔄 接続先：$($env.name)"

    # 認証（パスワードフロー）
    $loginUrl = $env.instanceUrl
    $response = Invoke-RestMethod -Method Post -Uri "$loginUrl/services/oauth2/token" -Body @{
        grant_type    = 'password'
        client_id     = $env.clientId
        client_secret = $env.clientSecret
        username      = $env.username
        password      = "$($env.password)$($env.securityToken)"
    }

    if (-not $response.instance_url) {
        Write-Host "❌ instance_url が取得できませんでした。"
        $response | ConvertTo-Json -Depth 5
        continue
    }

    $instanceUrl = $response.instance_url
    $accessToken = $response.access_token
    $headers = @{ Authorization = "Bearer $accessToken" }

    $exportDir = ".\Output\$($env.name)\$timestamp"
    New-Item -ItemType Directory -Path $exportDir -Force | Out-Null

    foreach ($setting in $soqlSettings) {
        try {
            Write-Host "📦 取得中（Bulk 2.0）: $($setting.objectName)"
            $csvPath = Join-Path $exportDir $setting.outputFileName

            Invoke-SFBulkQuery -InstanceUrl $instanceUrl -Headers $headers -Soql $setting.soql -CsvPath $csvPath

            Write-Host "✅ 出力完了: $csvPath"
        } catch {
            Write-Host "❌ 取得失敗: $($setting.objectName) - $($_.Exception.Message)"
        }
    }
}

Write-Host "`n🎉 指定されたすべての環境の処理が完了しました。"
