[CmdletBinding()]
param(
    [ValidateSet("event", "registration")]
    [string]$Database = "event",
    [ValidateSet("docker", "k8s")]
    [string]$Runtime = "docker",
    [Parameter(Mandatory = $true)]
    [string]$InputPath,
    [string]$KubeContext = "eventflow",
    [switch]$InsecureSkipTlsVerify,
    [switch]$ConfirmRestore
)

$ErrorActionPreference = "Stop"
if (-not $ConfirmRestore) {
    throw "Restore is destructive. Re-run with -ConfirmRestore after validating the dump and target."
}
if (-not (Test-Path -LiteralPath $InputPath -PathType Leaf)) {
    throw "Backup file not found: $InputPath"
}
if ($InputPath -match '[\r\n"]') {
    throw "InputPath must not contain quotes or newlines"
}
$service = if ($Database -eq "event") { "event-db" } else { "registration-db" }
$dbName = if ($Database -eq "event") { "eventflow_event" } else { "eventflow_registration" }
$InputPath = [IO.Path]::GetFullPath($InputPath)

if ($Runtime -eq "docker") {
    $command = 'type "{0}" | docker compose exec -T {1} psql -v ON_ERROR_STOP=1 -U eventflow -d {2}' -f $InputPath, $service, $dbName
    cmd.exe /d /s /c $command
} else {
    $kubectlArgs = @("--context", $KubeContext)
    if ($InsecureSkipTlsVerify) { $kubectlArgs += "--insecure-skip-tls-verify=true" }
    $podOutput = & kubectl @kubectlArgs -n eventflow get pods -l "app=$service" -o jsonpath="{.items[0].metadata.name}" 2>&1
    if ($LASTEXITCODE -ne 0) { throw "kubectl could not locate a pod for ${service}: $($podOutput -join ' ')" }
    $pod = ($podOutput -join "").Trim()
    if ([string]::IsNullOrWhiteSpace($pod)) { throw "No pod found for $service" }
    $tls = if ($InsecureSkipTlsVerify) { " --insecure-skip-tls-verify=true" } else { "" }
    $command = 'type "{0}" | kubectl --context {1}{2} -n eventflow exec -i {3} -- psql -v ON_ERROR_STOP=1 -U eventflow -d {4}' -f $InputPath, $KubeContext, $tls, $pod, $dbName
    cmd.exe /d /s /c $command
}
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL restore failed with exit code $LASTEXITCODE" }
Write-Output "Restore completed for $Database"
