# Week 9 Runbook - Real AWS Scheduled Pipeline Proof

Week 9 proves the Current Proven AWS Path:

```text
EventBridge Scheduler -> ECS / Fargate Batch Container -> Flight API Ingestion -> S3 Bronze -> Redshift Serverless -> dbt staging/marts/tests -> CloudWatch Logs
```

Proof status: complete. A manual ECS/Fargate run and an EventBridge Scheduler
triggered run both completed with `WEEK9_BATCH_SUCCESS` in CloudWatch Logs.

The recurring daily schedule was disabled after proof collection to avoid
unwanted daily AWS runs and cost.

## What Was Added

- `Dockerfile.batch` builds a Python 3.11 batch image for AWS.
- `scripts/run_aws_batch_pipeline.py` composes the existing commands:
  - S3 Bronze ingestion with `python -m ingestion.ingest_api_to_s3 --mode s3`
  - Redshift load with `python warehouse/run_redshift_sql.py`
  - dbt deps/build with a runtime-generated `flight_fares` profile that reads
    Redshift settings from environment variables and ECS Secrets Manager
  - optional Redshift mart verification with `verify_marts.sql`
- `aws/*.template.json` files provide AWS CLI templates for ECR, ECS, Fargate,
  EventBridge Scheduler, and CloudWatch Logs.

The runner uses environment variables and ECS secrets. It does not hard-code AWS
credentials, account IDs, Redshift passwords, or API keys.

## Completed Proof Summary

- ECR image tag `week9` was pushed for the batch image.
- ECS cluster `cloud-flight-fare-pipeline-week9` was used for Fargate execution.
- ECS task definition `cloud-flight-fare-pipeline-batch:2` ran the batch
  container.
- The manual ECS/Fargate task stopped with container exit code `0`.
- Manual CloudWatch logs showed `WEEK9_BATCH_SUCCESS`.
- EventBridge Scheduler triggered an ECS RunTask target using the same task
  definition.
- The one-time proof schedule
  `cloud-flight-fare-pipeline-week9-once-proof` used
  `ActionAfterCompletion=DELETE`.
- Scheduled CloudWatch logs showed `WEEK9_BATCH_SUCCESS`.
- The recurring daily schedule `cloud-flight-fare-pipeline-week9-daily` was
  disabled after proof collection.

## Required AWS Resources

Prepare these before the proof run:

- ECR repository for the batch image.
- S3 bucket with the Week 7 Bronze prefix, or permissions for the task to write it.
- Redshift Serverless namespace/workgroup from Week 8.
- Redshift COPY IAM role attached to Redshift and allowed to read the Bronze prefix.
- ECS cluster.
- ECS task execution role with the managed ECS execution policy and permission to
  read required Secrets Manager secrets.
- ECS task role with S3 permissions for Bronze ingestion.
- Redshift password stored in Secrets Manager.
- CloudWatch log group for the ECS task logs.
- VPC subnets and security group for Fargate.
- Network path from Fargate to ECR, CloudWatch Logs, Secrets Manager, S3, and
  Redshift. Public subnets with `AssignPublicIp=ENABLED` are acceptable for a
  proof; private subnets need NAT or VPC endpoints.
- EventBridge Scheduler execution role allowed to run the ECS task and pass the
  task execution/task roles.

Do not commit filled-in templates containing real account IDs, private subnet
IDs, security group IDs, or secret ARNs.

## Build and Push the ECR Image

PowerShell example:

```powershell
$env:AWS_REGION="us-east-1"
$env:AWS_ACCOUNT_ID="<account-id>"
$env:ECR_REPOSITORY="cloud-flight-fare-pipeline-batch"
$env:IMAGE_TAG="week9"
$env:ECR_URI="$env:AWS_ACCOUNT_ID.dkr.ecr.$env:AWS_REGION.amazonaws.com/$env:ECR_REPOSITORY"

aws ecr create-repository --cli-input-json file://aws/ecr-repository.template.json

aws ecr get-login-password --region $env:AWS_REGION |
  docker login --username AWS --password-stdin "$env:AWS_ACCOUNT_ID.dkr.ecr.$env:AWS_REGION.amazonaws.com"

docker build -f Dockerfile.batch -t "$env:ECR_REPOSITORY:$env:IMAGE_TAG" .
docker tag "$env:ECR_REPOSITORY:$env:IMAGE_TAG" "$env:ECR_URI:$env:IMAGE_TAG"
docker push "$env:ECR_URI:$env:IMAGE_TAG"
```

If the repository already exists, continue after confirming the repository name
matches the image URI used in the ECS task definition.

## Create CloudWatch Log Group

```powershell
aws logs create-log-group --cli-input-json file://aws/cloudwatch-log-group.template.json
aws logs put-retention-policy `
  --log-group-name "/ecs/cloud-flight-fare-pipeline/week9" `
  --retention-in-days 14
```

If the log group already exists, keep it and verify the task definition uses the
same log group name.

## Create ECS Cluster

```powershell
aws ecs create-cluster --cli-input-json file://aws/ecs-cluster.template.json
```

## Register ECS Task Definition

Copy the template to a local file and replace placeholders:

```powershell
Copy-Item aws\ecs-task-definition.template.json aws\ecs-task-definition.local.json
```

Set these values in the local copy:

- ECR image URI: `<account-id>.dkr.ecr.<region>.amazonaws.com/...:week9`
- `executionRoleArn` and `taskRoleArn`
- AWS Region
- S3 bucket and `S3_COPY_URI`
- Redshift host, database, user, and schema
- `IAM_ROLE_ARN` for the Redshift COPY role
- Secrets Manager ARN for `REDSHIFT_PASSWORD`
- CloudWatch log group and region

Register it:

```powershell
aws ecs register-task-definition --cli-input-json file://aws/ecs-task-definition.local.json
```

The committed template intentionally stores `REDSHIFT_PASSWORD` as an ECS secret,
not as a plain environment variable.

## Run ECS/Fargate Manually

Copy the run-task template, replace subnet and security group placeholders, then
run one Fargate task:

```powershell
Copy-Item aws\ecs-run-task.template.json aws\ecs-run-task.local.json
aws ecs run-task --cli-input-json file://aws/ecs-run-task.local.json
```

Watch task status:

```powershell
aws ecs list-tasks --cluster cloud-flight-fare-pipeline-week9
aws ecs describe-tasks `
  --cluster cloud-flight-fare-pipeline-week9 `
  --tasks <task-arn>
```

The task should stop with exit code `0`. If it fails, inspect the stopped reason
and CloudWatch logs first.

## Verify CloudWatch Logs

Tail the log group:

```powershell
aws logs tail "/ecs/cloud-flight-fare-pipeline/week9" --follow
```

Successful logs should include:

```text
WEEK9_BATCH_START
WEEK9_STEP_SUCCESS s3_ingest
WEEK9_STEP_SUCCESS redshift_load
WEEK9_STEP_SUCCESS dbt_build
WEEK9_STEP_SUCCESS redshift_verify
WEEK9_BATCH_SUCCESS
```

These logs are the main proof that ECS/Fargate executed the Dockerized batch
runner and wrote output to CloudWatch.

## Create EventBridge Scheduler

After the manual Fargate run works, create the schedule.

Copy and fill in the template:

```powershell
Copy-Item aws\eventbridge-scheduler.template.json aws\eventbridge-scheduler.local.json
```

The scheduler template is disabled by default for cost safety; enable it only
for proof runs.

Replace:

- `<region>`
- `<account-id>`
- task definition revision
- scheduler execution role ARN
- subnet IDs
- security group IDs

Create the schedule:

```powershell
aws scheduler create-schedule --cli-input-json file://aws/eventbridge-scheduler.local.json
```

For proof, prefer a separate one-time schedule instead of modifying the daily
schedule. The completed proof used:

```text
Name: cloud-flight-fare-pipeline-week9-once-proof
ScheduleExpression: at(2026-05-15T15:30:00)
ScheduleExpressionTimezone: America/New_York
ActionAfterCompletion: DELETE
Target: ECS RunTask / Fargate
Task definition: cloud-flight-fare-pipeline-batch:2
```

If a recurring daily schedule is enabled for testing, disable or delete it after
proof collection unless daily AWS runs are intentional.

## Screenshots to Save

Save proof under:

```text
docs/screenshots/week9/
```

Capture:

- `01-ecr-pushed-image-tag.png` - ECR repository showing pushed `week9` tag.
- `02-ecs-cluster.png` - ECS cluster.
- `03-ecs-task-definition-revision.png` - Fargate task definition revision.
- `04-manual-fargate-task-exit-code-0.png` - manual ECS task exit code `0`.
- `05-cloudwatch-manual-run-week9-success.png` - manual CloudWatch success log.
- `06-eventbridge-scheduler-enabled-target.png` - scheduler enabled with ECS
  target.
- `07-cloudwatch-scheduled-run-week9-success.png` - scheduled CloudWatch
  success log.

## Risks and Notes

- The current Redshift helper runs the Week 8 proof SQL by default, including
  `00_reset_schemas.sql`. Use this only in the proof warehouse environment.
- Redshift connectivity failures are usually VPC/security-group/routing issues
  when moving from a local laptop to Fargate.
- If Fargate runs in private subnets, make sure it can reach ECR, CloudWatch
  Logs, Secrets Manager, S3, and Redshift.
- `PIPELINE_START_DATE` and `PIPELINE_DAYS` can be used for controlled backfills.
  The scheduled proof should normally leave `PIPELINE_START_DATE` empty so the
  runner uses the current UTC date.
- Keep `aws/*.local.json`, `.env`, `dbt/profiles.yml`, and real credential
  values out of git. The batch image generates a dbt profile at runtime and gets
  `REDSHIFT_PASSWORD` through ECS Secrets Manager.
