#!/bin/sh
set -eu
awslocal s3 mb s3://eventflow-media 2>/dev/null || true
awslocal s3api put-public-access-block --bucket eventflow-media \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
echo "LocalStack: private bucket eventflow-media is ready"
