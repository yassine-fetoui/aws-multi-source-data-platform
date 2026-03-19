resource "aws_msk_cluster" "main" {
  cluster_name           = "data-platform-kafka-${var.environment}"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = var.broker_instance
    client_subnets  = var.private_subnet_ids
    security_groups = [var.security_group_id]

    storage_info {
      ebs_storage_info {
        volume_size = 100   # GB per broker
      }
    }
  }

  # Enable exactly-once semantics and idempotent production
  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  # IAM authentication — no username/password
  client_authentication {
    sasl { iam = true }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = { Name = "data-platform-kafka-${var.environment}" }
}

resource "aws_msk_configuration" "main" {
  name              = "data-platform-kafka-config-${var.environment}"
  kafka_versions    = [var.kafka_version]

  server_properties = <<-EOT
    # Enable idempotent production — required for exactly-once semantics
    enable.idempotence=true

    # Replication factor for exactly-once guarantees
    transaction.state.log.replication.factor=3
    transaction.state.log.min.isr=2
    default.replication.factor=3
    min.insync.replicas=2

    # Partition count — must match consumer group size for full parallelism
    num.partitions=32

    # Log retention
    log.retention.hours=168
    log.retention.bytes=107374182400

    # Compression
    compression.type=snappy
  EOT
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/data-platform-${var.environment}"
  retention_in_days = 14
}

output "bootstrap_brokers_tls" { value = aws_msk_cluster.main.bootstrap_brokers_sasl_iam }
output "cluster_arn"           { value = aws_msk_cluster.main.arn }
