terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket         = "tfstate-aws-data-platform"
    key            = "aws-data-platform/terraform.tfstate"
    region         = "eu-west-1"
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "aws-multi-source-data-platform"
      ManagedBy   = "terraform"
      Environment = var.environment
      Owner       = "yassine-fetoui"
    }
  }
}

module "networking" {
  source      = "./modules/networking"
  environment = var.environment
  vpc_cidr    = var.vpc_cidr
}

module "security" {
  source      = "./modules/security"
  environment = var.environment
  vpc_id      = module.networking.vpc_id
}

module "storage" {
  source          = "./modules/storage"
  environment     = var.environment
  kms_key_arn     = module.security.kms_key_arn
}

module "streaming" {
  source             = "./modules/streaming"
  environment        = var.environment
  vpc_id             = module.networking.vpc_id
  private_subnet_ids = module.networking.private_subnet_ids
  security_group_id  = module.networking.msk_security_group_id
  kafka_version      = var.kafka_version
  broker_instance    = var.kafka_broker_instance
}

module "compute" {
  source              = "./modules/compute"
  environment         = var.environment
  raw_bucket_arn      = module.storage.raw_bucket_arn
  curated_bucket_arn  = module.storage.curated_bucket_arn
  glue_role_arn       = module.security.glue_role_arn
  lambda_role_arn     = module.security.lambda_role_arn
  kms_key_arn         = module.security.kms_key_arn
}

module "warehouse" {
  source             = "./modules/warehouse"
  environment        = var.environment
  vpc_id             = module.networking.vpc_id
  private_subnet_ids = module.networking.private_subnet_ids
  redshift_password  = var.redshift_password
  rds_password       = var.rds_password
  kms_key_arn        = module.security.kms_key_arn
}
