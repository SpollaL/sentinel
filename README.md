# sentinel

A data quality validation CLI — define rules in YAML, run them against CSV or Parquet files.

## Build

```bash
cargo build --release
```

## Usage

```bash
cargo run -- <data-file> --rules <rules-file>
```

Example:

```bash
cargo run -- data.csv --rules rules.yaml
```

Sentinel exits with code `0` if all rules pass, `1` if any fail — making it easy to use in CI pipelines.

## Rules file

Rules are defined in a YAML file. Each rule targets a column and applies a check.

```yaml
rules:
  - name: no_nulls_in_age
    column: age
    check: not_null

  - name: no_empty_names
    column: name
    check: not_empty

  - name: age_is_positive
    column: age
    check: min
    value: 0

  - name: age_is_realistic
    column: age
    check: max
    value: 120
```

## Supported checks

| Check       | Description                          | Requires `value` |
|-------------|--------------------------------------|------------------|
| `not_null`  | Column must have no null values      | No               |
| `not_empty` | Column must have no empty strings    | No               |
| `min`       | All values must be >= value          | Yes              |
| `max`       | All values must be <= value          | Yes              |

## Supported file formats

- CSV (`.csv`)
- Parquet (`.parquet`)
