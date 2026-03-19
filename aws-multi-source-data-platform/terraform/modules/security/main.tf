resource "aws_kms_key" "data_platform" {
  description             = "KMS key for data platform encryption at rest"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = {
    Name = "data-platform-kms-${var.environment}"
  }
}

resource "aws_kms_alias" "data_platform" {
  name          = "alias/data-platform-${var.environment}"
  target_key_id = aws_kms_key.data_platform.key_id
}

# ── Glue IAM Role ──────────────────────────────────────────────────────────

resource "aws_iam_role" "glue_role" {
  name = "data-platform-glue-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

data "aws_iam_policy_document" "glue_s3_policy" {
  # Read from raw zone only
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      "arn:aws:s3:::data-platform-raw-${var.environment}",
      "arn:aws:s3:::data-platform-raw-${var.environment}/*"
    ]
    condition {
      test     = "StringEquals"
      variable = "aws:SourceVpc"
      values   = [var.vpc_id]
    }
  }
  # Write to curated zone only
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = [
      "arn:aws:s3:::data-platform-curated-${var.environment}/*"
    ]
    condition {
      test     = "StringEquals"
      variable = "aws:SourceVpc"
      values   = [var.vpc_id]
    }
  }
  # KMS decrypt/encrypt
  statement {
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data_platform.arn]
  }
}

resource "aws_iam_policy" "glue_s3" {
  name   = "data-platform-glue-s3-${var.environment}"
  policy = data.aws_iam_policy_document.glue_s3_policy.json
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3.arn
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# ── Lambda IAM Role ──────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda_role" {
  name = "data-platform-lambda-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

data "aws_iam_policy_document" "lambda_s3_policy" {
  # Read and write within raw zone prefix only — for compaction
  statement {
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [
      "arn:aws:s3:::data-platform-raw-${var.environment}",
      "arn:aws:s3:::data-platform-raw-${var.environment}/compaction/*"
    ]
  }
  statement {
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data_platform.arn]
  }
}

resource "aws_iam_policy" "lambda_s3" {
  name   = "data-platform-lambda-s3-${var.environment}"
  policy = data.aws_iam_policy_document.lambda_s3_policy.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3.arn
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# ── Outputs ──────────────────────────────────────────────────────────────────

output "kms_key_arn"     { value = aws_kms_key.data_platform.arn }
output "glue_role_arn"   { value = aws_iam_role.glue_role.arn }
output "lambda_role_arn" { value = aws_iam_role.lambda_role.arn }
