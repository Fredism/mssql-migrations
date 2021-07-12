param (
    [parameter(Position=0)]
	[string]
	$path = ".\appsettings.json"
)

class ConnectionStrings {
	[string] $Source
	[string] $Target
}

class AppSettings {
	[ConnectionStrings] $ConnectionStrings
}

function Execute {
	param([string] $cmd)
	sqlcmd -I -S $server -U $user -P $password -b -i ".\$cmd.sql" -o ".\logs\$cmd.log" -f 65001
	if ($LASTEXITCODE -ne 0) {
		echo "$cmd error!"
	}
	else {
		echo "$cmd success."
	}
}

$dbInfo = @{}

Add-Type -AssemblyName System.Web.Extensions
$serializer = [System.Web.Script.Serialization.JavaScriptSerializer]::new()
$content = $serializer.Deserialize((Get-Content -Path $path), [AppSettings])

$content.ConnectionStrings.Target -split ";" | ForEach-Object {
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
Execute "seed"
Execute "update"
Execute "alter"
echo "Migration finished: $((Get-Date).ToString())"