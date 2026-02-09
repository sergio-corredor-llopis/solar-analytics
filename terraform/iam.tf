# -----------------------------------------------------------------------------
# IAM ROLE: Pipeline Execution Role
# -----------------------------------------------------------------------------
# This role will be assumed by Airflow/Lambda to access S3

resource "aws_iam_role" "pipeline_role" {
  name = "${var.project_name}-pipeline-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "Solar Pipeline Role"
    Environment = var.environment
    Project     = var.project_name
  }
}

# -----------------------------------------------------------------------------
# IAM POLICY: S3 Access for Pipeline
# -----------------------------------------------------------------------------
# Grants read/write access to our specific bucket only

resource "aws_iam_policy" "pipeline_s3_policy" {
  name        = "${var.project_name}-s3-access-${var.environment}"
  description = "Allow pipeline to read/write to solar analytics S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.solar_raw.arn
      },
      {
        Sid    = "ReadWriteObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.solar_raw.arn}/*"
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# ATTACH POLICY TO ROLE
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy_attachment" "pipeline_s3_attachment" {
  role       = aws_iam_role.pipeline_role.name
  policy_arn = aws_iam_policy.pipeline_s3_policy.arn
}