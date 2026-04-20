# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-04-20

### Added
- Data quality validation CLI for CSV and Parquet files
- Eight built-in check types: `not_null`, `not_empty`, `min`, `max`, `between`, `unique`, `regex`, `custom`
- Optional `threshold` field for per-rule violation tolerance (0.0–1.0)
- JSON Lines and table output formats
- `--dry-run` flag to validate rules without executing them
- `--verbose` flag to print full error chains
- Azure Blob Storage support via `az://` URLs
- Amazon S3 support via `s3://` URLs
