# Contributing to Sentinel

Thanks for your interest in contributing! Here's everything you need to get started.

## Reporting bugs

Open an issue and include:
- The command you ran
- Your rules YAML file (or a minimal reproduction)
- The data file format (CSV or Parquet) and a sample if possible
- The error output

## Suggesting features

Open an issue describing:
- The use case you're trying to solve
- What you'd expect the behaviour to look like (example YAML, CLI flags, output)

## Submitting a pull request

1. Fork the repo and create a branch from `master`
2. Make your changes
3. Add or update tests — all check types must have coverage
4. Run the full check suite locally before pushing:

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```

4. Open a PR with a clear description of what changed and why

CI will run automatically on your PR. It must pass before merging.

## Development setup

```bash
git clone https://github.com/Spollal/sentinel
cd sentinel
cargo build
cargo test
```

Rust stable is required. Install it via [rustup](https://rustup.rs).

## Code structure

| File | Responsibility |
|---|---|
| `src/main.rs` | CLI parsing, file loading, orchestration |
| `src/rules.rs` | YAML rule definitions and validation |
| `src/runner.rs` | SQL generation and DataFusion execution |
| `src/output.rs` | JSON and table formatting |

## Adding a new check type

1. Add a variant to the `Check` enum in `src/rules.rs`
2. Add a `build_sql` arm in `src/runner.rs`
3. Add both a pass and fail test in the `#[cfg(test)]` block in `src/runner.rs`
4. Document it in the `README.md` supported checks table
