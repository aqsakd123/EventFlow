$ErrorActionPreference = "Continue"
$Region = "us-east-1"
$LogFile = "C:\Users\PC\Downloads\Files\3\AWSProjectDemo\ops\cost-guard-log.txt"

"=== Run: $(Get-Date) ===" | Out-File -FilePath $LogFile -Append

# 1. Xoa toan bo NAT Gateway dang chay (va release EIP kem theo)
$nats = aws ec2 describe-nat-gateways --region $Region `
  --filter "Name=state,Values=available,pending" `
  --query "NatGateways[].{Id:NatGatewayId,Eip:NatGatewayAddresses[0].AllocationId}" `
  --output json | ConvertFrom-Json

if ($nats.Count -gt 0) {
    foreach ($nat in $nats) {
        "Deleting NAT Gateway $($nat.Id)" | Out-File -FilePath $LogFile -Append
        aws ec2 delete-nat-gateway --nat-gateway-id $nat.Id --region $Region | Out-Null
    }
    aws ec2 wait nat-gateway-deleted --nat-gateway-ids ($nats.Id) --region $Region

    foreach ($nat in $nats) {
        if ($nat.Eip) {
            "Releasing EIP $($nat.Eip)" | Out-File -FilePath $LogFile -Append
            aws ec2 release-address --allocation-id $nat.Eip --region $Region | Out-Null
        }
    }
} else {
    "No NAT Gateway found." | Out-File -FilePath $LogFile -Append
}

# 2. Tat (stop, khong terminate) toan bo EC2 dang chay
$instances = aws ec2 describe-instances --region $Region `
  --filters "Name=instance-state-name,Values=running" `
  --query "Reservations[].Instances[].InstanceId" --output text

if ($instances) {
    "Stopping EC2 instances: $instances" | Out-File -FilePath $LogFile -Append
    aws ec2 stop-instances --instance-ids $instances.Split() --region $Region | Out-Null
} else {
    "No running EC2 instances." | Out-File -FilePath $LogFile -Append
}

# 3. Tat toan bo RDS instance dang chay
$rdsInstances = aws rds describe-db-instances --region $Region `
  --query "DBInstances[?DBInstanceStatus=='available'].DBInstanceIdentifier" --output text

if ($rdsInstances) {
    foreach ($db in $rdsInstances.Split()) {
        "Stopping RDS instance: $db" | Out-File -FilePath $LogFile -Append
        aws rds stop-db-instance --db-instance-identifier $db --region $Region | Out-Null
    }
} else {
    "No running RDS instances." | Out-File -FilePath $LogFile -Append
}

"=== Done: $(Get-Date) ===" | Out-File -FilePath $LogFile -Append
