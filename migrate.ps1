class ConnectionStrings {
	[string] $Source
	[string] $Target
}

class AppSettings {
	[ConnectionStrings] $ConnectionStrings
}

class JsonConfig {
	[AppSettings] $AppSettings
}

function Execute {
	param([string] $cmd)
	sqlcmd -S $server -U $user -P $password -b -i ".\$cmd.sql" -o ".\logs\$cmd.log"
	if ($LASTEXITCODE -ne 0) {
		echo "$cmd error!"
	}
	else {
		echo "$cmd success."
	}
}

$dbInfo = @{}
$serializer = [System.Web.Script.Serialization.JavaScriptSerializer]::new()
$content = $serializer.Deserialize((Get-Content -Path .\appsettings.json), [JsonConfig])

$content.AppSettings.ConnectionStrings.Target -split ";" | ForEach-Object {
	$parts = $_ -split "="
	$key = $parts[0]
	$value = $parts[1]
	$dbInfo[$key] = $value
}

$server = $dbInfo["Server"]
$user = $dbInfo["User ID"]
$password = $dbInfo["Password"]

if(!(Test-Path ".\logs")) {
	mkdir logs > $null
}

echo "Migration started: $((Get-Date).ToString())"
Execute "patch"
Execute "create"
Execute "update"
Execute "alter"
Execute "seed"
echo "Migration finished: $((Get-Date).ToString())"