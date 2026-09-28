# LocalStack S3 evidence

Date: 2026-09-16

A standalone container was started from localstack/localstack:3.8.1 on host port 4567.
The health endpoint reported S3 available. The following AWS-style operations succeeded:

~~~powershell
docker exec eventflow-localstack awslocal s3 mb s3://eventflow-media
docker exec eventflow-localstack awslocal s3api put-public-access-block --bucket eventflow-media --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
docker exec eventflow-localstack awslocal s3api get-public-access-block --bucket eventflow-media
~~~

Observed flags:
- BlockPublicAcls: true
- IgnorePublicAcls: true
- BlockPublicPolicy: true
- RestrictPublicBuckets: true

The container was removed after the check. This proves the local emulator flow only; it is not AWS IAM/S3 evidence.

