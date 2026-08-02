# CloudFormation

Provisions the AWS side of the lakehouse: one encrypted S3 bucket (with
lifecycle rules), the three Glue catalog databases (`cryptostream_bronze`,
`cryptostream_silver`, `cryptostream_gold`), the Bronze `market_trades`
external table (partition-projected on year/month/day/hour, no crawler or
manual `MSCK REPAIR` needed), the `cryptostream-dev` Athena workgroup, and
two IAM users: one scoped to writing into the Bronze S3 prefix (for Kafka
Connect), one scoped to Athena/Glue/S3 access on all three layers (for
dbt-athena).

## Validate

```
aws cloudformation validate-template \
  --template-body file://infrastructure/cloudformation/template.yaml
```

## Deploy

```
aws cloudformation create-stack \
  --stack-name cryptostream-dev \
  --template-body file://infrastructure/cloudformation/template.yaml \
  --parameters file://infrastructure/cloudformation/parameters-dev.json \
  --capabilities CAPABILITY_NAMED_IAM

aws cloudformation wait stack-create-complete --stack-name cryptostream-dev

aws cloudformation describe-stacks --stack-name cryptostream-dev --query "Stacks[0].Outputs"
```

## Get Kafka Connect / dbt-athena credentials

The stack only creates the IAM users, not access keys (keys shouldn't sit in
stack outputs). Generate them separately:

```
aws iam create-access-key --user-name cryptostream-dev-kafka-connect
aws iam create-access-key --user-name cryptostream-dev-dbt-athena
```

## Tear down

```
aws cloudformation delete-stack --stack-name cryptostream-dev
```
