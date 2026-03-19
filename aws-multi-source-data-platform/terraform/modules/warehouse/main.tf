resource "aws_redshift_cluster" "main" {
  cluster_identifier        = "data-platform-redshift-${var.environment}"
  database_name             = "dataplatform"
  master_username           = "admin"
  master_password           = var.redshift_password
  node_type                 = var.environment == "prod" ? "ra3.4xlarge" : "dc2.large"
  number_of_nodes           = var.environment == "prod" ? 4 : 1
  cluster_type              = var.environment == "prod" ? "multi-node" : "single-node"

  vpc_security_group_ids    = [aws_security_group.redshift.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.main.name

  encrypted  = true
  kms_key_id = var.kms_key_arn

  enhanced_vpc_routing = true   # forces traffic through VPC — no public internet

  # Snapshot schedule for prod
  automated_snapshot_retention_period = var.environment == "prod" ? 7 : 1

  # Prevent accidental deletion on prod
  skip_final_snapshot = var.environment != "prod"

  logging {
    enable        = true
    log_destination_type = "cloudwatch"
    log_exports   = ["useractivitylog", "userlog", "connectionlog"]
  }

  lifecycle {
    prevent_destroy = false   # set to true for prod in real deployment
    ignore_changes  = [master_password]
  }

  tags = { Name = "data-platform-redshift-${var.environment}" }
}

resource "aws_redshift_subnet_group" "main" {
  name       = "data-platform-redshift-sg-${var.environment}"
  subnet_ids = var.private_subnet_ids
}

resource "aws_security_group" "redshift" {
  name        = "data-platform-redshift-sg-${var.environment}"
  description = "Redshift cluster — VPC access only"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
    description = "Redshift port — internal network only"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ── RDS PostgreSQL — OLTP source ──────────────────────────────────────────────

resource "aws_db_instance" "main" {
  identifier        = "data-platform-rds-${var.environment}"
  engine            = "postgres"
  engine_version    = "15.4"
  instance_class    = "db.t3.medium"
  allocated_storage = 100
  storage_encrypted = true
  kms_key_id        = var.kms_key_arn

  db_name  = "dataplatform"
  username = "admin"
  password = var.rds_password

  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name

  backup_retention_period    = 7
  deletion_protection        = var.environment == "prod"
  skip_final_snapshot        = var.environment != "prod"
  performance_insights_enabled = true

  # Enable logical replication for Debezium CDC
  parameter_group_name = aws_db_parameter_group.postgres.name

  tags = { Name = "data-platform-rds-${var.environment}" }
}

resource "aws_db_parameter_group" "postgres" {
  name   = "data-platform-postgres15-${var.environment}"
  family = "postgres15"

  parameter {
    name  = "rds.logical_replication"
    value = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    name  = "wal_level"
    value = "logical"
    apply_method = "pending-reboot"
  }
}

resource "aws_db_subnet_group" "main" {
  name       = "data-platform-rds-subnet-${var.environment}"
  subnet_ids = var.private_subnet_ids
}

resource "aws_security_group" "rds" {
  name        = "data-platform-rds-sg-${var.environment}"
  description = "RDS PostgreSQL — VPC access only"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
    description = "PostgreSQL — internal only"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

output "redshift_endpoint" { value = aws_redshift_cluster.main.endpoint }
output "rds_endpoint"      { value = aws_db_instance.main.endpoint }
