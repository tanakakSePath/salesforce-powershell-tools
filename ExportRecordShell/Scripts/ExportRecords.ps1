
# ExportRecords.ps1
param (
    [string]$soqlConfigPath = "../Config/soqlSettings.json",
    [string]$connectionConfigPath = "../Config/connectionSettings.json"
)

function Select-Environment {
    param ([array]$environments)
    Write-Host "接続先を選択してください:"
    for ($i = 0; $i -lt $environments.Count; $i++) {
        Write-Host "$($i + 1): $($environments[$i].name)"
    }
    do {
        $selection = Read-Host "番号を入力"
    } while (-not ($selection -as [int]) -or $selection -lt 1 -or $selection -gt $environments.Count)

    return $environments[$selection - 1]
}

function Get-AccessToken {
    param (
        [string]$instanceUrl,
        [string]$clientId,
        [string]$clientSecret,
        [string]$username,
        [string]$password,
        [string]$securityToken
    )

    $body = @{
        grant_type    = "password"
        client_id     = $clientId
        client_secret = $clientSecret
        username      = $username
        password      = "$password$securityToken"
    }

    $response = Invoke-RestMethod -Method Post -Uri "$instanceUrl/services/oauth2/token" -Body $body
    return $response.access_token, $response.instance_url
}

function Export-Records {
    param (
        [string]$accessToken,
        [string]$instanceUrl,
        [array]$soqlList,
        [string]$outputDir
    )

    foreach ($entry in $soqlList) {
        $soqlEncoded = [System.Web.HttpUtility]::UrlEncode($entry.soql)
        $uri = "$instanceUrl/services/data/v60.0/query?q=$soqlEncoded"

        $headers = @{ Authorization = "Bearer $accessToken" }

        try {
            $result = Invoke-RestMethod -Method Get -Uri $uri -Headers $headers
            $records = $result.records | Where-Object { $_ -ne $null }

            if ($records.Count -eq 0) {
                Write-Host "レコードなし: $($entry.objectName)"
                continue
            }

            $csvPath = Join-Path $outputDir $entry.outputFileName
            $records | ConvertTo-Csv -NoTypeInformation | Set-Content -Path $csvPath -Encoding UTF8
            Write-Host "出力完了: $csvPath"
        } catch {
            Write-Warning "取得失敗: $($entry.objectName) - $($_.Exception.Message)"
        }
    }
}

# ===== メイン処理 =====
$connectionSettings = Get-Content $connectionConfigPath | ConvertFrom-Json
$soqlSettings = Get-Content $soqlConfigPath | ConvertFrom-Json

$env = Select-Environment -environments $connectionSettings.environments
$accessInfo = Get-AccessToken -instanceUrl $env.instanceUrl -clientId $env.clientId -clientSecret $env.clientSecret -username $env.username -password $env.password -securityToken $env.securityToken
$accessToken = $accessInfo[0]
$instanceUrl = $accessInfo[1]

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputDir = "../Output/$($env.name)/$timestamp"
New-Item -Path $outputDir -ItemType Directory -Force | Out-Null

Export-Records -accessToken $accessToken -instanceUrl $instanceUrl -soqlList $soqlSettings -outputDir $outputDir

Write-Host "`nすべての処理が完了しました。"
