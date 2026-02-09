# -----------------------------------------------------------------------------
# S3 BUCKET: Raw Data Layer
# -----------------------------------------------------------------------------
# This bucket stores raw CSV files from the solar monitoring system
# Data will be organized by: raw/year=YYYY/month=MM/

resource "aws_s3_bucket" "solar_raw" {
  bucket = "${var.project_name}-raw-${var.owner}-${var.environment}"

  tags = {
    Name        = "Solar Pipeline Raw Data"
    Environment = var.environment
    Project     = var.project_name
    Owner       = var.owner
  }
}

# -----------------------------------------------------------------------------
# Bucket Versioning
# -----------------------------------------------------------------------------
# Enables version history - useful for recovering from accidental overwrites

resource "aws_s3_bucket_versioning" "solar_raw_versioning" {
  bucket = aws_s3_bucket.solar_raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

# -----------------------------------------------------------------------------
# Block Public Access
# -----------------------------------------------------------------------------
# Security best practice - ensures bucket is never accidentally made public

resource "aws_s3_bucket_public_access_block" "solar_raw_public_access" {
  bucket = aws_s3_bucket.solar_raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}