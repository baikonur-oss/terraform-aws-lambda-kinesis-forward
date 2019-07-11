resource "aws_kinesis_stream" "source" {
  name             = "source"
  shard_count      = "1"
  retention_period = "24"
}

resource "aws_kinesis_stream" "target" {
  name             = "target"
  shard_count      = "1"
  retention_period = "24"
}

module "kinesis_forward" {
  source = "github.com/baikonur-oss/terraform-aws-lambda-kinesis-forward?ref=init"

  lambda_package_url = "github.com/baikonur-oss/terraform-aws-lambda-kinesis-forward/releases/download/v1.0.0/lambda_package.zip"
  name               = "kinesis_forward"

  memory     = "1024"
  batch_size = "100"

  source_stream_name = "${aws_kinesis_stream.source.name}"
  target_stream_name = "${aws_kinesis_stream.target.name}"

  failed_log_s3_bucket = "failed-logs"
  failed_log_s3_prefix = "forward"
}
