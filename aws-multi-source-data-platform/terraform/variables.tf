variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "kafka_version" {
  description = "MSK Kafka version"
  type        = string
  default     = "3.5.1"
}

variable "kafka_broker_instance" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "redshift_password" {
  description = "Redshift master password — injected from Secrets Manager at runtime"
  type        = string
  sensitive   = true
}

variable "rds_password" {
  description = "RDS master password — injected from Secrets Manager at runtime"
  type        = string
  sensitive   = true
}
