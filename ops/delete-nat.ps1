$ErrorActionPreference = "Stop"

$Region = "us-east-1"

$NatId = Get-Content "nat-gateway-id.txt"
$AllocId = Get-Content "eip-allocation-id.txt"

Write-Host "Deleting NAT Gateway $NatId ..."
aws ec2 delete-nat-gateway --nat-gateway-id $NatId --region $Region

Write-Host "Waiting for NAT Gateway to finish deleting (about 1-2 minutes)..."
aws ec2 wait nat-gateway-deleted --nat-gateway-ids $NatId --region $Region

Write-Host "Releasing Elastic IP $AllocId ..."
aws ec2 release-address --allocation-id $AllocId --region $Region

Write-Host "Done. NAT Gateway deleted and Elastic IP released - billing stopped."