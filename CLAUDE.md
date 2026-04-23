# sentinel — Claude guidance

## After implementing any feature

Always update docs before closing the task:

1. **README.md** — add or update the relevant section (usage, flags, output format, examples). If a flag or subcommand changes its interface, update all code blocks that reference it.
2. **CHANGELOG.md** — add a bullet under `## [Unreleased]` describing what was added, changed, or fixed. One bullet per logical change.

This applies to every PR, no matter how small. A flag with no docs is a feature that doesn't exist to users.

## Project layout

- `src/main.rs` — CLI definition (clap), entry point, `run_validate`, `run_schema`, agent mode
- `src/runner.rs` — rule execution, `run_rule`, `run_rules_parallel`, `fetch_violation_samples`
- `src/rules.rs` — YAML schema: `Rule`, `Check`, `Severity`
- `src/output.rs` — JSON and table formatting
- `src/schema.rs` — dataset introspection (`sentinel schema`)
- `src/storage.rs` — file registration (CSV, Parquet, Azure, S3)
- `examples/` — sample data and rules files

## Exit codes

| Code | Meaning |
|---|---|
| `0` | All rules passed |
| `1` | Error-severity rule failed, or empty input |
| `2` | Warning-severity rule(s) failed only |
| `3` | Config/rules/schema error |
| `4` | Data file not found or unreadable |

## Testing

Run `cargo test` — all tests are unit/integration tests in-process using DataFusion in-memory tables. Cloud storage tests (`test_azure_csv_source`, `test_s3_csv_source`) are ignored by default; they require live credentials.

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs on every push and PR to `master`:
- `cargo fmt --check`
- `cargo clippy -- -D warnings`
- `rustsec/audit-check` for dependency vulnerabilities
- `cargo test`
