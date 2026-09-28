[CmdletBinding()]
param(
    [ValidateSet("docker", "k8s")]
    [string]$Runtime = "docker",
    [string]$KubeContext = "eventflow",
    [string]$Topic = "eventflow.domain-events",
    [string]$ConsumerGroup = "eventflow-analytics",
    [switch]$ConfirmLocalReset
)

$ErrorActionPreference = "Stop"

if (-not $ConfirmLocalReset) {
    throw "This command clears only the local analytics projection and resets the local Kafka consumer group. Re-run with -ConfirmLocalReset."
}

function Invoke-Checked {
    param(
        [string]$Executable,
        [string[]]$Arguments
    )

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & $Executable @Arguments 2>&1
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed ($LASTEXITCODE): $Executable $($Arguments -join ' ') $([Environment]::NewLine)$($output -join [Environment]::NewLine)"
    }
    return ($output -join [Environment]::NewLine)
}

$truncate = "TRUNCATE analytics_event_ledger, analytics_projection"
$consumerGroupArgs = @(
    "/opt/kafka/bin/kafka-consumer-groups.sh",
    "--bootstrap-server", "kafka:9092",
    "--group", $ConsumerGroup,
    "--topic", $Topic,
    "--reset-offsets", "--to-earliest", "--execute"
)

if ($Runtime -eq "docker") {
    Invoke-Checked "docker" @("compose", "stop", "registration-service") | Out-Null
    Invoke-Checked "docker" @("compose", "exec", "-T", "registration-db", "psql", "-U", "eventflow",
        "-d", "eventflow_registration", "-c", $truncate) | Out-Null
    Invoke-Checked "docker" (@("compose", "exec", "-T", "kafka") + $consumerGroupArgs) | Out-Null
    Invoke-Checked "docker" @("compose", "start", "registration-service") | Out-Null
} else {
    $kubectlPrefix = @("--context", $KubeContext, "--insecure-skip-tls-verify=true")
    $dbPod = Invoke-Checked "kubectl" ($kubectlPrefix + @(
        "-n", "eventflow", "get", "pods", "-l", "app=registration-db",
        "-o", "jsonpath={.items[0].metadata.name}"
    ))

    Invoke-Checked "kubectl" ($kubectlPrefix + @("scale", "deployment/registration-service", "-n", "eventflow", "--replicas=0")) | Out-Null
    Start-Sleep -Seconds 3
    Invoke-Checked "kubectl" ($kubectlPrefix + @(
        "-n", "eventflow", "exec", $dbPod, "--", "psql", "-U", "eventflow",
        "-d", "eventflow_registration", "-c", $truncate
    )) | Out-Null
    Invoke-Checked "kubectl" ($kubectlPrefix + @(
        "-n", "eventflow", "exec", "deployment/kafka", "--"
    ) + $consumerGroupArgs) | Out-Null
    Invoke-Checked "kubectl" ($kubectlPrefix + @("scale", "deployment/registration-service", "-n", "eventflow", "--replicas=1")) | Out-Null
    Invoke-Checked "kubectl" ($kubectlPrefix + @(
        "-n", "eventflow", "wait", "--for=condition=available",
        "deployment/registration-service", "--timeout=120s"
    )) | Out-Null
}

[pscustomobject]@{
    status = "PASS"
    runtime = $Runtime
    topic = $Topic
    consumerGroup = $ConsumerGroup
    reset = "earliest"
    projection = "cleared-and-rebuilt-by-consumer"
} | ConvertTo-Json
