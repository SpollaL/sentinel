# sentinel

![CI](https://github.com/Spollal/sentinel/actions/workflows/ci.yml/badge.svg)

A data quality validation CLI — define rules in YAML, run them against CSV or Parquet files.

## Install

```bash
cargo install --path .
```

Or run directly without installing:

```bash
cargo run -- <data-file> --rules <rules-file>
```

Try the included examples:

```bash
sentinel examples/data.csv --rules examples/rules.yaml --format table
```

## Usage

```bash
sentinel <data-file> --rules <rules-file> [--format table] [--dry-run] [--verbose]
```

Sentinel exits with code `0` if all rules pass, `1` if any fail — making it easy to use in CI pipelines.

Use `--verbose` to print the full error chain on failure, useful for debugging rules.

## Output

By default sentinel outputs one JSON object per rule (JSONL), followed by a summary:

```json
{"name":"no_nulls_in_age","status":"pass","violations":0,"total_rows":100,"violation_rate":0.0}
{"name":"age_is_positive","status":"fail","violations":3,"total_rows":100,"violation_rate":0.03}
// 1 passed, 1 failed out of 2 rules
```

Use `--format table` for a human-readable table:

```
+--------------------+--------+------------+-------+------+
| RULE               | STATUS | VIOLATIONS | TOTAL | RATE |
+--------------------+--------+------------+-------+------+
| no_nulls_in_age    | pass   | 0          | 100   | 0.0% |
| age_is_positive    | fail   | 3          | 100   | 3.0% |
+--------------------+--------+------------+-------+------+
1 passed, 1 failed out of 2 rules
```

## Dry run

Use `--dry-run` to validate your rules file and data schema without running any checks:

```bash
sentinel data.csv --rules rules.yaml --dry-run
```

This loads the file, checks that all rule columns exist in the schema, and validates that each rule is well-formed (e.g. a `min` check has a `min` value). No queries are executed against the data.

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

  - name: discount_exceeds_price
    column: _unused  # column is required but ignored for custom checks
    check: custom
    sql: "SELECT COUNT(*) FROM data WHERE discount > price"
```

> **Custom SQL contract**: the query must return a single integer representing the number of **violating rows** — not total rows, not a boolean. `threshold` works the same as for built-in checks.

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
| `custom`    | Run arbitrary SQL — must return the number of **violating** rows as a single integer | `sql` |

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

## Cloud storage

Sentinel can read files directly from Azure Blob Storage and Amazon S3. Credentials are read from environment variables — no code changes needed.

### Azure Blob Storage

Use the `az://` scheme:

```bash
sentinel az://my-container/path/to/data.csv --rules rules.yaml
```

Set these environment variables before running:

| Variable | Description |
|---|---|
| `AZURE_STORAGE_ACCOUNT_NAME` | Storage account name |
| `AZURE_STORAGE_ACCOUNT_KEY` | Storage account key |

Or use a connection string:

| Variable | Description |
|---|---|
| `AZURE_STORAGE_CONNECTION_STRING` | Full connection string |

### Amazon S3

Use the `s3://` scheme:

```bash
sentinel s3://my-bucket/path/to/data.parquet --rules rules.yaml
```

Set these environment variables before running:

| Variable | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_DEFAULT_REGION` | Bucket region (e.g. `us-east-1`) |

For S3-compatible stores (MinIO, etc.), also set `AWS_ENDPOINT` to point to your endpoint.
