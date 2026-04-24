# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `sentinel profile` now emits per-column quantiles (P01/P25/P50/P75/P99) for numeric columns via DataFusion's `approx_percentile_cont` (t-digest), and a top-10 frequent-values block for non-numeric / bounded-cardinality columns (2 ≤ unique ≤ 50, NULLs excluded).
- `sentinel profile` suggests a new `{col}_typical_range` rule for numeric columns on datasets of ≥ 100 rows — a `between` check bounded by P01/P99 with a 2% violation threshold and warning severity. Robust to single-outlier min/max values that wreck the raw `{col}_range` bounds.
- `sentinel profile --format json` — structured JSON output alongside the default human-readable text. JSON shape: `{row_count, columns[], suggested_rules[]}`, with each column flattening the `schema` fields plus optional `top_values`.
- `sentinel query <file> --sql "<SQL>"` command — runs arbitrary SQL against the registered `data` table and streams rows as JSONL. Defaults to capping output at 1000 rows via `--max-rows` for agent token-budget safety; the cap is applied as a `DataFrame::limit` rather than SQL subquery wrapping, so user `WITH`/`UNION`/`ORDER BY` clauses pass through unchanged.
- `sentinel head <file> [-n N]` command — returns the first N rows of the dataset as JSONL (default 10). Thin wrapper over `query`.
- `sentinel validate --rule <SPEC>` — repeatable inline rule flag with compact syntax `check:column[:arg...]`. Supported forms: `not_null:<col>`, `not_empty:<col>`, `unique:<col>`, `min:<col>:<n>`, `max:<col>:<n>`, `between:<col>:<min>:<max>`, `regex:<col>:<pattern>`. Inline rules always have severity `error`; use YAML for `warning`, `threshold`, or `custom`.
- `sentinel validate --rules -` — read rules YAML from stdin. Combines with `--rule` flags; empty stdin is tolerated when inline rules are present. Duplicate rule names across sources are disambiguated with `_2`, `_3`, … suffixes.

### Changed
- `sentinel schema` JSON now also emits `p01/p25/p50/p75/p99` approximate quantile fields for numeric columns (computed via the same `approx_percentile_cont` pass added for `profile`).
- `--show-violations` sample rows now cover more Arrow types (timestamps, dates, decimals) — the row-to-JSON conversion was rebuilt on `arrow::json::WriterBuilder`.
- `sentinel profile <file>` command — prints per-column stats (type, nulls %, unique, min/max/mean) and a ready-to-use `rules.yaml` block with suggested rules inferred from the data
- `mean` field added to `sentinel schema` output for numeric columns
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
