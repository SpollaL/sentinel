# sentinel — Claude guidance

## After implementing any feature

Update docs before closing task:

1. **README.md** — add/update relevant section (usage, flags, output format, examples). Flag or subcommand interface changes → update all referencing code blocks.
2. **CHANGELOG.md** — bullet under `## [Unreleased]`: what added, changed, fixed. One bullet per logical change.

Every PR, no matter how small. Flag with no docs = feature that don't exist to users.

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

Run `cargo test` — unit/integration tests in-process, DataFusion in-memory tables. Cloud storage tests (`test_azure_csv_source`, `test_s3_csv_source`) ignored by default; need live credentials.

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs on every push and PR to `master`:
- `cargo fmt --check`
- `cargo clippy -- -D warnings`
- `rustsec/audit-check` for dependency vulnerabilities
- `cargo test`