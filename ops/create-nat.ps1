$ErrorActionPreference = "Stop"

$Region = "us-east-1"
$SubnetId = "subnet-07b2409b80688dc83"   # learning-vpc-subnet-public1-us-east-1a

Write-Host "Allocating Elastic IP..."
$AllocId = aws ec2 allocate-address --domain vpc --region $Region --query "AllocationId" --output text
Write-Host "Elastic IP AllocationId: $AllocId"

Write-Host "Creating NAT Gateway..."
$NatId = aws ec2 create-nat-gateway `
  --subnet-id $SubnetId `
  --allocation-id $AllocId `
  --tag-specifications "ResourceType=natgateway,Tags=[{Key=Name,Value=learning-natgw}]" `
  --region $Region `
  --query "NatGateway.NatGatewayId" --output text

$NatId | Out-File -FilePath "nat-gateway-id.txt" -Encoding ascii -NoNewline
$AllocId | Out-File -FilePath "eip-allocation-id.txt" -Encoding ascii -NoNewline

Write-Host ""
Write-Host "NAT Gateway ID: $NatId (initializing, takes 1-3 minutes)"
Write-Host "Check status with:"
Write-Host "aws ec2 describe-nat-gateways --nat-gateway-ids $NatId --region $Region --query 'NatGateways[0].State'"