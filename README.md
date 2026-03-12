# sentinel

A data quality validation CLI — define rules in YAML, run them against CSV or Parquet files.

## Build

```bash
cargo build --release
```

## Usage

```bash
cargo run -- <data-file> --rules <rules-file> [--format table]
```

Example:

```bash
cargo run -- data.csv --rules rules.yaml
cargo run -- data.csv --rules rules.yaml --format table
```

Sentinel exits with code `0` if all rules pass, `1` if any fail — making it easy to use in CI pipelines.

## Output

By default sentinel outputs one JSON object per rule (JSONL):

```json
{"name":"no_nulls_in_age","status":"pass","violations":0,"total_rows":100,"violation_rate":0.0}
{"name":"age_is_positive","status":"fail","violations":3,"total_rows":100,"violation_rate":0.03}
```

Use `--format table` for a human-readable table:

```
+--------------------+--------+------------+-------+------+
| RULE               | STATUS | VIOLATIONS | TOTAL | RATE |
+--------------------+--------+------------+-------+------+
| no_nulls_in_age    | pass   | 0          | 100   | 0.0% |
| age_is_positive    | fail   | 3          | 100   | 3.0% |
+--------------------+--------+------------+-------+------+
```

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
    min: 0

  - name: age_is_realistic
    column: age
    check: max
    max: 120

  - name: age_in_range
    column: age
    check: between
    min: 18
    max: 99

  - name: name_unique
    column: name
    check: unique

  - name: valid_email
    column: email
    check: regex
    pattern: '^[^@]+@[^@]+\.[^@]+'

  - name: mostly_valid_ages
    column: age
    check: not_null
    threshold: 0.05  # allow up to 5% nulls
```

## Supported checks

| Check       | Description                                  | Parameters         |
|-------------|----------------------------------------------|--------------------|
| `not_null`  | Column must have no null values              | —                  |
| `not_empty` | Column must have no empty strings            | —                  |
| `min`       | All values must be >= min                    | `min`              |
| `max`       | All values must be <= max                    | `max`              |
| `between`   | All values must be between min and max       | `min`, `max`       |
| `unique`    | Column must have no duplicate values         | —                  |
| `regex`     | All values must match the pattern            | `pattern`          |

## Threshold

All rules support an optional `threshold` field — a violation rate (0.0 to 1.0) below which the rule still passes:

```yaml
- name: mostly_filled
  column: age
  check: not_null
  threshold: 0.05  # pass if fewer than 5% of rows are null
```

## Supported file formats

- CSV (`.csv`)
- Parquet (`.parquet`)
