param (
    [string[]]$envs = @("Production", "Sandbox2")  # デフォルト
)

# 設定ファイル読み込み
$connectionSettings = Get-Content -Raw -Encoding UTF8 -Path  "../Config/connectionSettings.json" | ConvertFrom-Json
$soqlSettings = Get-Content -Raw -Encoding UTF8 -Path "../Config/soqlSettings.json" | ConvertFrom-Json


# 指定環境でフィルタ
$targetEnvs = $connectionSettings.environments | Where-Object { $_.name -in $envs }

if ($targetEnvs.Count -eq 0) {
    Write-Host "❌ 指定された環境が connectionSettings.json に見つかりません。"
    exit
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

foreach ($env in $targetEnvs) {
    Write-Host "`n🔄 接続先：$($env.name)"

    # 認証
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
        Write-Host "📦 取得中: $($setting.objectName)"
        $query = $setting.soql
        $fieldOrder = @()

        # SOQLのSELECT句から順番を取得
        if ($query -match "(?i)^SELECT\s+(.*?)\s+FROM\s") {
            $fieldOrder = $matches[1].Split(',').ForEach{ $_.Trim() }
        }

        $encodedQuery = [System.Web.HttpUtility]::UrlEncode($query)
        $url = "$instanceUrl/services/data/v58.0/query?q=$encodedQuery"

        try {
            $result = Invoke-RestMethod -Method Get -Uri $url -Headers $headers
            $records = $result.records

            if (-not $records -or $records.Count -eq 0) {
                Write-Host "⚠️ データなし: $($setting.objectName)"
                continue
            }

            # フィールド順に並べ替え
            $orderedRecords = foreach ($rec in $records) {
                $ordered = [ordered]@{}
                foreach ($field in $fieldOrder) {
                    if ($rec.PSObject.Properties[$field]) {
                        $ordered[$field] = $rec.$field
                    }
                }
                [PSCustomObject]$ordered
            }

            $csvPath = Join-Path $exportDir $setting.outputFileName
            $orderedRecords | ConvertTo-Csv -NoTypeInformation | Out-File -Encoding UTF8 -FilePath $csvPath

            Write-Host "✅ 出力完了: $csvPath"
        } catch {
            Write-Host "❌ 取得失敗: $($setting.objectName) - $($_.Exception.Message)"
        }
    }
}

Write-Host "`n🎉 指定されたすべての環境の処理が完了しました。"
