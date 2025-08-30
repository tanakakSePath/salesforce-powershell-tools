param (
    [string[]]$envs = @("Sandbox1（apxmigutt2）", "Sandbox2（apxmigbft1）")  # デフォルト
)

# =========================
# 基本設定
# =========================
$apiVersion = "v64.0"   # 必要に応じて調整
$progressPollSeconds = 2
$jobTimeoutSeconds   = 600  # 10分
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$baseOut  = Join-Path $PSScriptRoot "Output"   # スクリプト直下に固定

# =========================
# 設定ファイル読み込み
# =========================
$connectionSettings = Get-Content -Raw -Encoding UTF8 -Path  "../Config/connectionSettings.json" | ConvertFrom-Json
$soqlSettings       = Get-Content -Raw -Encoding UTF8 -Path "../Config/soqlSettings.json"       | ConvertFrom-Json

# 指定環境フィルタ
$targetEnvs = $connectionSettings.environments | Where-Object { $_.name -in $envs }
if ($targetEnvs.Count -eq 0) {
    Write-Host "❌ 指定された環境が connectionSettings.json に見つかりません。"
    exit 1
}

# =========================
# ユーティリティ
# =========================

function Get-SafeFileName {
    param([string]$Name)
    $invalid = [System.IO.Path]::GetInvalidFileNameChars()
    $sb = New-Object System.Text.StringBuilder
    foreach ($ch in $Name.ToCharArray()) {
        if ($invalid -contains $ch) { [void]$sb.Append('_') } else { [void]$sb.Append($ch) }
    }
    return $sb.ToString().TrimEnd('.', ' ')
}

function Ensure-ParentDirectory {
    param([string]$FilePath)
    $dir = Split-Path -Parent $FilePath
    if ($dir -and -not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}

# File.Open を使わず BOM を書いて空ファイルを作成
function New-Utf8BomFile {
    param([Parameter(Mandatory=$true)][string]$Path)
    Ensure-ParentDirectory -FilePath $Path
    $bom = [byte[]](0xEF,0xBB,0xBF)
    Set-Content -Path $Path -Value $bom -Encoding Byte
}

function Get-BytesFromResponse {
    param([Parameter(Mandatory=$true)]$Response)

    # まずレスポンスを素でバイト化
    $msIn = New-Object System.IO.MemoryStream
    if ($Response.RawContentStream) {
        $Response.RawContentStream.CopyTo($msIn)
    } else {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Response.Content)
        $msIn.Write($bytes, 0, $bytes.Length)
    }
    $msIn.Position = 0
    $raw = $msIn.ToArray()

    # 先頭数バイトを見て実体を判定
    $isGzip    = ($raw.Length -ge 2 -and $raw[0] -eq 0x1F -and $raw[1] -eq 0x8B)          # 1F 8B
    $isDeflate = ($raw.Length -ge 2 -and $raw[0] -eq 0x78 -and (0x01,0x5E,0x9C,0xDA) -contains $raw[1]) # 78 01/5E/9C/DA

    if ($isGzip) {
        try {
            $msOut = New-Object System.IO.MemoryStream
            $msIn.Position = 0
            $gz = New-Object System.IO.Compression.GZipStream($msIn, [System.IO.Compression.CompressionMode]::Decompress)
            $gz.CopyTo($msOut)
            $gz.Dispose()
            $bytesOut = $msOut.ToArray()
            $msOut.Dispose()
            $msIn.Dispose()
            return $bytesOut
        } catch {
            # もし失敗したら生データを返す（二重判定の保険）
            $msIn.Dispose()
            return $raw
        }
    } elseif ($isDeflate) {
        try {
            $msOut = New-Object System.IO.MemoryStream
            $msIn.Position = 0
            $df = New-Object System.IO.Compression.DeflateStream($msIn, [System.IO.Compression.CompressionMode]::Decompress)
            $df.CopyTo($msOut)
            $df.Dispose()
            $bytesOut = $msOut.ToArray()
            $msOut.Dispose()
            $msIn.Dispose()
            return $bytesOut
        } catch {
            $msIn.Dispose()
            return $raw
        }
    } else {
        # ヘッダーがgzipでも中身がプレーンならそのまま返す
        $msIn.Dispose()
        return $raw
    }
}


function Invoke-SFBulkQuery {
    <#
        Bulk API 2.0 Query
        - ジョブ作成 → 完了待ち（進捗/タイムアウト/失敗詳細）→ CSV を UTF-8+BOM で保存（BOMは先頭1回、本文はバイト追記）
        - File.Open を使用せず Set-Content / Add-Content (Encoding Byte) で安全に書き込み
    #>
    param(
        [Parameter(Mandatory=$true)][string]$InstanceUrl,
        [Parameter(Mandatory=$true)][hashtable]$Headers,
        [Parameter(Mandatory=$true)][string]$Soql,
        [Parameter(Mandatory=$true)][string]$CsvPath,
        [int]$PollSeconds = $progressPollSeconds,
        [int]$TimeoutSeconds = $jobTimeoutSeconds
    )

    # --- ジョブ作成 ---
    $createBody = @{
        operation       = "query"
        query           = $Soql
        columnDelimiter = "COMMA"
        lineEnding      = "CRLF"
    } | ConvertTo-Json

    $jobUrl = "$InstanceUrl/services/data/$apiVersion/jobs/query"
    $job = Invoke-RestMethod -Method Post -Uri $jobUrl -Headers ($Headers + @{ "Content-Type" = "application/json" }) -Body $createBody
    if (-not $job.id) { throw "ジョブ作成に失敗しました。" }
    $jobId = $job.id
    Write-Host ("   🆔 JobId: {0}" -f $jobId)

    # --- ジョブ完了待ち ---
    $statusUrl = "$InstanceUrl/services/data/$apiVersion/jobs/query/$jobId"
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
            break
        } elseif ($st.state -eq "Aborted") {
            throw "ジョブが中止されました。"
        } elseif ($st.state -eq "Failed") {
            # 失敗詳細（先頭数行）を取得
            $failedUrl = "$InstanceUrl/services/data/$apiVersion/jobs/query/$jobId/failedResults"
            try {
                $failedResp = Invoke-WebRequest -Method Get -Uri $failedUrl -Headers ($Headers + @{ "Accept" = "text/csv" })
                $sample = ($failedResp.Content -split "`n") | Select-Object -First 5
                throw "ジョブが失敗しました: $($st.errorMessage)`n--- failedResults sample ---`n$($sample -join "`n")"
            } catch {
                throw "ジョブが失敗しました: $($st.errorMessage)"
            }
        }

        if ($elapsed -ge $TimeoutSeconds) {
            throw "ジョブがタイムアウトしました（$TimeoutSeconds 秒）。状態: $($st.state)"
        }
    }

    # --- 結果取得（CSV：UTF-8 + BOM、Byte追記）---
    $resultsBase = "$InstanceUrl/services/data/$apiVersion/jobs/query/$jobId/results"
    $locator = $null

    # 先頭1回だけ BOM を出力
    New-Utf8BomFile -Path $CsvPath

    do {
        $resultsUrl = if ($locator) { "$resultsBase?locator=$locator" } else { $resultsBase }

        # 自動解凍の揺れ対策として Accept-Encoding を明示
        $resp = Invoke-WebRequest -Method Get -Uri $resultsUrl `
                -Headers ($Headers + @{ "Accept" = "text/csv"; "Accept-Encoding" = "gzip, deflate, identity" })

        # 必要なら次行を有効化してヘッダ確認
        # Write-Host ("   Content-Encoding: {0}" -f $resp.Headers["Content-Encoding"])

        $bytes = Get-BytesFromResponse -Response $resp

        # 本文はバイトで追記（File.Open 不使用）
        Add-Content -Path $CsvPath -Value $bytes -Encoding Byte

        $locator = $resp.Headers["Sforce-Locator"]
    } while ($locator -and $locator -ne "null")
}

# =========================
# 実行本体
# =========================

foreach ($env in $targetEnvs) {
    Write-Host "`n🛰  接続先: $($env.name)"

    # 認証（Resource Owner Password）
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

    $exportDir = Join-Path $baseOut (Join-Path $env.name $timestamp)
    New-Item -ItemType Directory -Path $exportDir -Force | Out-Null

    foreach ($setting in $soqlSettings) {
        try {
            Write-Host ("📦 取得中（Bulk 2.0）: {0}" -f $setting.objectName)

            # ファイル名はサニタイズ（サブフォルダが欲しい場合はここを調整）
            $rawName  = $setting.outputFileName
            $fileName = Split-Path -Leaf $rawName
            $safeName = Get-SafeFileName $fileName
            $csvPath  = Join-Path $exportDir $safeName

            Invoke-SFBulkQuery -InstanceUrl $instanceUrl -Headers $headers -Soql $setting.soql -CsvPath $csvPath `
                               -PollSeconds $progressPollSeconds -TimeoutSeconds $jobTimeoutSeconds

            Write-Host "✅ 出力完了: $csvPath"
        } catch {
            Write-Host ("❌ 取得失敗: {0} - {1}" -f $setting.objectName, $_.Exception.Message)
        }
    }
}

Write-Host "`n🎉 指定されたすべての環境の処理が完了しました。"
