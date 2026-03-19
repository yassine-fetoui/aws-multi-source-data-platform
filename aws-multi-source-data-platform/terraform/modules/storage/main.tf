locals {
  buckets = ["raw", "curated", "consumption", "tfstate"]
}

resource "aws_s3_bucket" "raw" {
  bucket        = "data-platform-raw-${var.environment}"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket" "curated" {
  bucket        = "data-platform-curated-${var.environment}"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket" "consumption" {
  bucket        = "data-platform-consumption-${var.environment}"
  force_destroy = var.environment != "prod"
}

# ── Versioning ───────────────────────────────────────────────────────────────

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_versioning" "curated" {
  bucket = aws_s3_bucket.curated.id
  versioning_configuration { status = "Enabled" }
}

# ── Server-Side Encryption ───────────────────────────────────────────────────

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

# ── Block Public Access ───────────────────────────────────────────────────────

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "curated" {
  bucket                  = aws_s3_bucket.curated.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Lifecycle Policy — archive micro-files after compaction ──────────────────

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "archive-compacted-originals"
    status = "Enabled"
    filter { prefix = "compaction/archive/" }
    transition {
      days          = 30
      storage_class = "GLACIER"
    }
    expiration { days = 90 }
  }

  rule {
    id     = "expire-temp-files"
    status = "Enabled"
    filter { prefix = "tmp/" }
    expiration { days = 7 }
  }
}

# ── Prevent destroy on prod buckets ─────────────────────────────────────────

resource "aws_s3_bucket" "tfstate" {
  bucket = "data-platform-tfstate-${var.environment}"
  lifecycle { prevent_destroy = true }
}

# ── Outputs ──────────────────────────────────────────────────────────────────

output "raw_bucket_arn"         { value = aws_s3_bucket.raw.arn }
output "raw_bucket_id"          { value = aws_s3_bucket.raw.id }
output "curated_bucket_arn"     { value = aws_s3_bucket.curated.arn }
output "consumption_bucket_arn" { value = aws_s3_bucket.consumption.arn }
