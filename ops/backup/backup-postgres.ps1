[CmdletBinding()]
param(
    [ValidateSet("event", "registration")]
    [string]$Database = "event",
    [ValidateSet("docker", "k8s")]
    [string]$Runtime = "docker",
    [string]$OutputPath,
    [string]$KubeContext = "eventflow",
    [switch]$InsecureSkipTlsVerify
)

$ErrorActionPreference = "Stop"
$service = "event-db-primary"
$dbName = "eventflow"
$schema = if ($Database -eq "event") { "event_service" } else { "registration_service" }

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path (Get-Location) ("backup-{0}-{1}.sql" -f $Database, (Get-Date -Format "yyyyMMdd-HHmmss"))
}
if ($OutputPath -match '[\r\n"]') {
    throw "OutputPath must not contain quotes or newlines"
}
$OutputPath = [IO.Path]::GetFullPath($OutputPath)
$parent = Split-Path -Parent $OutputPath
if (-not (Test-Path -LiteralPath $parent)) {
    New-Item -ItemType Directory -Path $parent | Out-Null
}

if ($Runtime -eq "docker") {
    $command = 'docker compose exec -T {0} pg_dump --clean --if-exists --no-owner --no-privileges --schema {2} -U eventflow -d {1} > "{3}"' -f $service, $dbName, $schema, $OutputPath
    cmd.exe /d /s /c $command
} else {
    $kubectlArgs = @("--context", $KubeContext)
    if ($InsecureSkipTlsVerify) { $kubectlArgs += "--insecure-skip-tls-verify=true" }
    $podOutput = & kubectl @kubectlArgs -n eventflow get pods -l "app=shared-db-primary" -o jsonpath="{.items[0].metadata.name}" 2>&1
    if ($LASTEXITCODE -ne 0) { throw "kubectl could not locate a pod for ${service}: $($podOutput -join ' ')" }
    $pod = ($podOutput -join "").Trim()
    if ([string]::IsNullOrWhiteSpace($pod)) { throw "No pod found for $service" }
    $tls = if ($InsecureSkipTlsVerify) { " --insecure-skip-tls-verify=true" } else { "" }
    $command = 'kubectl --context {0}{1} -n eventflow exec -i {2} -- pg_dump --clean --if-exists --no-owner --no-privileges --schema {4} -U eventflow -d {3} > "{5}"' -f $KubeContext, $tls, $pod, $dbName, $schema, $OutputPath
    cmd.exe /d /s /c $command
}
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL backup failed with exit code $LASTEXITCODE" }
Write-Output "Backup written to $OutputPath"
