variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-central-1"  # Frankfurt - good for Zurich latency
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project identifier"
  type        = string
  default     = "solar-analytics"
}

variable "owner" {
  description = "Owner initials"
  type        = string
  default     = "scl"
}