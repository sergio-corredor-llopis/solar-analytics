output "raw_bucket_name" {
  description = "Name of the raw data S3 bucket"
  value       = aws_s3_bucket.solar_raw.id
}

output "raw_bucket_arn" {
  description = "ARN of the raw data S3 bucket"
  value       = aws_s3_bucket.solar_raw.arn
}

output "raw_bucket_region" {
  description = "Region of the raw data S3 bucket"
  value       = aws_s3_bucket.solar_raw.region
}

output "pipeline_role_arn" {
  description = "ARN of the pipeline IAM role"
  value       = aws_iam_role.pipeline_role.arn
}

output "pipeline_role_name" {
  description = "Name of the pipeline IAM role"
  value       = aws_iam_role.pipeline_role.name
}