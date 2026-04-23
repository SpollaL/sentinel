# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `validate` and `schema` subcommands — CLI now uses `sentinel validate <file> --rules <rules>` (breaking change from flat invocation)
- `sentinel schema <file>` command — outputs per-column type, null count, distinct count, min/max, and total row count as JSON
- `severity` field on rules (`error` | `warning`, default `error`) — warning rules fail with exit code `2` instead of `1`
- Granular exit codes: `0` all pass, `1` error failure, `2` warning-only failure, `3` config/schema error, `4` file error
- `--show-violations [N]` flag — attaches first N violating rows to each failed rule result (`sample_rows` in JSON, extra column in table output)
- `--agent` flag / `SENTINEL_AGENT` env var — streams JSON Lines with per-rule timing and a summary line, structured error objects on stderr
- Parallel rule execution via `tokio::task::JoinSet` — all rules run concurrently, results returned in original order

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
