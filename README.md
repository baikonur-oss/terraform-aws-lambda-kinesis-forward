# Amazon Kinesis to Kinesis log forwarding Terraform module

Terraform module and Lambda for saving JSON log records from Kinesis Data Streams to S3.

![terraform v0.12.x](https://img.shields.io/badge/terraform-v0.12.x-brightgreen.svg)

## Prerequisites
1. Records in Kinesis stream must be valid JSON data. Non-JSON data will be **ignored**.
    1. gzipped JSON, [CloudWatch Logs subscription filters log format](https://docs.aws.amazon.com/ja_jp/AmazonCloudWatch/latest/logs/SubscriptionFilters.html) are supported.
    2. Broken JSON logs or logs without log type will be saved to S3 as `unknown`.
2. JSON data must have the following keys (key names are modifiable via variables):
    1. `log_type`: Log type identifier. Used for applying log type whitelist 
3. Recommended keys (necessary if target stream has [lambda-kinesis-to-s3](https://github.com/baikonur-oss/terraform-aws-lambda-kinesis-to-s3) or other modules attached):
    1. `log_id`: Any unique identifier. Used to avoid file overwrites on S3. Also is useful to search for a specific log record.
    2. `time`: Any timestamp supported by [dateutil.parser.parse](https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse). ISO8601 with milli/microseconds recommended.

## Usage
```HCL
resource "aws_kinesis_stream" "stream" {
  name             = "stream"
  shard_count      = "1"
  retention_period = "24"
}

resource "aws_kinesis_stream" "target" {
  name             = "target"
  shard_count      = "1"
  retention_period = "24"
}

module "kinesis_forward" {
  source  = "baikonur-oss/lambda-kinesis-forward/aws"

  lambda_package_url = "https://github.com/baikonur-oss/terraform-aws-lambda-kinesis-forward/releases/download/v1.0.0/lambda_package.zip"
  name               = "kinesis_forward"

  memory     = "1024"
  batch_size = "100"

  source_stream_name = "${aws_kinesis_stream.source.name}"
  target_stream_name = "${aws_kinesis_stream.target.name}"

  failed_log_s3_bucket = "failed-logs"
  failed_log_s3_prefix = "forward"
}
```

Warning: use same module and package versions!

### Version pinning
#### Terraform Module Registry
Use `version` parameter to pin to a specific version, or to specify a version constraint when pulling from [Terraform Module Registry](https://registry.terraform.io) (`source = baikonur-oss/lambda-kinesis-forward/aws`).
For more information, refer to [Module Versions](https://www.terraform.io/docs/configuration/modules.html#module-versions) section of Terraform Modules documentation.

#### GitHub URI
Make sure to use `?ref=` version pinning in module source URI when pulling from GitHub.
Pulling from GitHub is especially useful for development, as you can pin to a specific branch, tag or commit hash.
Example: `source = github.com/baikonur-oss/terraform-aws-lambda-kinesis-forward?ref=v1.0.0`

For more information on module version pinning, see [Selecting a Revision](https://www.terraform.io/docs/modules/sources.html#selecting-a-revision) section of Terraform Modules documentation.


<!-- Documentation below is generated by pre-commit, do not overwrite manually -->
<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|:----:|:-----:|:-----:|
| batch_size | Maximum number of records passed for a single Lambda invocation | string | - | yes |
| enable_kinesis_mapping | Determines if the event source mapping will be enabled | string | `true` | no |
| failed_log_s3_bucket | S3 bucket name for saving failed logs (ES API errors etc.) | string | - | yes |
| failed_log_s3_prefix | Path prefix for failed logs | string | - | yes |
| handler | Lambda Function handler (entrypoint) | string | `main.handler` | no |
| lambda_package_url | Lambda package URL (see Usage in README) | string | - | yes |
| log_id_field | Key name for unique log ID | string | `log_id` | no |
| log_retention_in_days | Lambda Function log retention in days | string | `30` | no |
| log_timestamp_field | Key name for log timestamp | string | `time` | no |
| log_type_field | Key name for log type | string | `log_type` | no |
| log_type_field_whitelist | Log type whitelist (if empty, all types will be processed) | list(string) | `[]` | no |
| log_type_unknown_prefix | Log type prefix for logs without log type field | string | `unknown` | no |
| memory | Lambda Function memory in megabytes | string | `256` | no |
| name | Resource name | string | - | yes |
| runtime | Lambda Function runtime | string | `python3.7` | no |
| source_stream_name | Source Kinesis Data Stream name | string | - | yes |
| starting_position | Kinesis ShardIterator type (see: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html ) | string | `TRIM_HORIZON` | no |
| tags | Tags for Lambda Function | map(string) | `{}` | no |
| target_stream_name | Target Kinesis Data Stream name | string | - | yes |
| timeout | Lambda Function timeout in seconds | string | `60` | no |
| timezone | tz database timezone name (e.g. Asia/Tokyo) | string | `UTC` | no |
| tracing_mode | X-Ray tracing mode (see: https://docs.aws.amazon.com/lambda/latest/dg/API_TracingConfig.html ) | string | `PassThrough` | no |

<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->

## Contributing

Make sure to have following tools installed:
- [Terraform](https://www.terraform.io/)
- [terraform-docs](https://github.com/segmentio/terraform-docs)
- [pre-commit](https://pre-commit.com/)

### macOS
```bash
brew install pre-commit terraform terraform-docs

# set up pre-commit hooks by running below command in repository root
pre-commit install
```
