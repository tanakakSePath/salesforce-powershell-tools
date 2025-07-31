# salesforce-powershell-tools
PowerShell scripts for Salesforce API integration

■Salesforceの前提
設定→OAuth および OpenID Connect 設定を開き、[OAuth ユーザ名パスワードフローを許可] をオンに変更します。

パワーシェルを管理者権限で立ち上げる。
cdで、シェルを置いているディレクトリへ移動する。
./ExportRecords_MultiEnv.ps1で実行する。
※セキュリティエラーの場合は、「Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass」で一時許可

環境しても可能
./ExportRecords_MultiEnv.ps1 -envs "Sandbox1","Production"

PS1は、UTF-8のBOM付で保存する。
