resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = { Name = "data-platform-vpc-${var.environment}" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "data-platform-igw-${var.environment}" }
}

# ── Private Subnets (3 AZs) ──────────────────────────────────────────────────

resource "aws_subnet" "private" {
  count             = 3
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = { Name = "data-platform-private-${count.index}-${var.environment}" }
}

data "aws_availability_zones" "available" { state = "available" }

# ── VPC Endpoints — keep S3 and Glue traffic inside private network ───────────

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = { Name = "data-platform-s3-endpoint-${var.environment}" }
}

resource "aws_vpc_endpoint" "glue" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.glue"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = { Name = "data-platform-glue-endpoint-${var.environment}" }
}

resource "aws_vpc_endpoint" "secrets_manager" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = { Name = "data-platform-sm-endpoint-${var.environment}" }
}

# ── Security Groups ───────────────────────────────────────────────────────────

resource "aws_security_group" "msk" {
  name        = "data-platform-msk-sg-${var.environment}"
  description = "MSK Kafka broker security group"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Kafka plaintext — internal VPC only"
  }

  ingress {
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Kafka TLS — internal VPC only"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "data-platform-msk-sg-${var.environment}" }
}

resource "aws_security_group" "vpc_endpoints" {
  name        = "data-platform-endpoints-sg-${var.environment}"
  description = "VPC interface endpoint security group"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "HTTPS from VPC"
  }

  tags = { Name = "data-platform-endpoints-sg-${var.environment}" }
}

# ── Route Tables ──────────────────────────────────────────────────────────────

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "data-platform-private-rt-${var.environment}" }
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

data "aws_region" "current" {}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "vpc_id"                 { value = aws_vpc.main.id }
output "private_subnet_ids"    { value = aws_subnet.private[*].id }
output "msk_security_group_id" { value = aws_security_group.msk.id }
