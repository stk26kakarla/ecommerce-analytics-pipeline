output "bronze_bucket" {
  description = "Bronze layer S3 bucket name"
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket" {
  description = "Silver layer S3 bucket name"
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket" {
  description = "Gold layer S3 bucket name"
  value       = aws_s3_bucket.gold.bucket
}
