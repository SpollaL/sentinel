# sentinel

![CI](https://github.com/Spollal/sentinel/actions/workflows/ci.yml/badge.svg)

A data quality validation CLI — define rules in YAML, run them against CSV or Parquet files.

## Install

```bash
cargo install --path .
```

Or run directly without installing:

```bash
cargo run -- validate <data-file> --rules <rules-file>
```

Try the included examples:

```bash
sentinel validate examples/data.csv --rules examples/rules.yaml --format table
```

## Commands

Sentinel has two subcommands: `validate` and `schema`.

### validate

Run data quality rules against a file.

```bash
sentinel validate <data-file> --rules <rules-file> [OPTIONS]
```

| Flag | Description |
|---|---|
| `-r, --rules <file>` | Path to rules YAML file (required) |
| `-f, --format <fmt>` | Output format: `json` (default) or `table` |
| `--dry-run` | Validate rules file and schema without running checks |
| `--verbose` | Print full error chain on failure |
| `--show-violations [N]` | Attach first N violating rows to each failed rule (default 5) |
| `--agent` | Stream JSON Lines output for machine consumption (see Agent mode) |

### schema

Inspect the schema and basic stats of a dataset — no rules file needed.

```bash
sentinel schema <data-file>
```

Outputs JSON with per-column info (type, null count, distinct count, min/max for numeric columns) and total row count:

```json
{
  "columns": [
    { "name": "age",  "type": "int64",  "nulls": 2,  "unique": 87, "min": 18.0, "max": 99.0 },
    { "name": "name", "type": "utf8",   "nulls": 0,  "unique": 100 },
    { "name": "flag", "type": "bool",   "nulls": 1,  "unique": 2 }
  ],
  "row_count": 100
}
```

## Exit codes

| Code | Meaning |
|---|---|
| `0` | All rules passed |
| `1` | At least one `error`-severity rule failed, or input file is empty |
| `2` | Only `warning`-severity rules failed (no errors) |
| `3` | Invalid rules file or schema mismatch |
| `4` | Data file not found or unreadable |

## Output

By default sentinel outputs one JSON object per rule (JSONL), followed by a summary:

```json
{"name":"no_nulls_in_age","status":"pass","severity":"error","violations":0,"total_rows":100,"violation_rate":0.0}
{"name":"age_is_positive","status":"fail","severity":"warning","violations":3,"total_rows":100,"violation_rate":0.03}
// 1 passed, 1 failed out of 2 rules
```

Use `--format table` for a human-readable table:

```
+--------------------+--------+----------+------------+-------+------+
| RULE               | STATUS | SEVERITY | VIOLATIONS | TOTAL | RATE |
+--------------------+--------+----------+------------+-------+------+
| no_nulls_in_age    | pass   | error    | 0          | 100   | 0.0% |
| age_is_positive    | fail   | warning  | 3          | 100   | 3.0% |
+--------------------+--------+----------+------------+-------+------+
1 passed, 1 failed out of 2 rules
```

### Violation samples

Pass `--show-violations` to attach the first N violating rows to each failed rule:

```bash
sentinel validate data.csv --rules rules.yaml --show-violations 3
```

In JSON output, failed rules gain a `sample_rows` array:

```json
{"name":"age_is_positive","status":"fail","severity":"error","violations":3,"total_rows":100,"violation_rate":0.03,"sample_rows":[{"age":-1},{"age":0},{"age":-5}]}
```

In table output, a **SAMPLE VIOLATIONS** column is added automatically.

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

| Check       | Description                                                                           | Parameters         |
|-------------|---------------------------------------------------------------------------------------|--------------------|
| `not_null`  | Column must have no null values                                                       | —                  |
| `not_empty` | Column must have no empty strings                                                     | —                  |
| `min`       | All values must be >= min                                                             | `min`              |
| `max`       | All values must be <= max                                                             | `max`              |
| `between`   | All values must be between min and max                                                | `min`, `max`       |
| `unique`    | Column must have no duplicate values                                                  | —                  |
| `regex`     | All values must match the pattern                                                     | `pattern`          |
| `custom`    | Run arbitrary SQL — must return the number of **violating** rows as a single integer  | `sql`              |

## Severity

Each rule has an optional `severity` field (`error` or `warning`, default `error`).

- `error` rules that fail cause exit code `1`.
- `warning` rules that fail cause exit code `2` (only if no error rules also failed).

```yaml
rules:
  - name: no_nulls_in_id
    column: id
    check: not_null
    severity: error    # pipeline fails hard

  - name: phone_format
    column: phone
    check: regex
    pattern: '^\+?[0-9]{7,15}$'
    severity: warning  # flag it but don't block the pipeline
```

## Threshold

All rules support an optional `threshold` field — a violation rate (0.0 to 1.0) below which the rule still passes:

```yaml
- name: mostly_filled
  column: age
  check: not_null
  threshold: 0.05  # pass if fewer than 5% of rows are null
```

## Dry run

Use `--dry-run` to validate your rules file and data schema without running any checks:

```bash
sentinel validate data.csv --rules rules.yaml --dry-run
```

## Agent mode

Pass `--agent` (or set `SENTINEL_AGENT=1`) to stream results as JSON Lines for use in scripts or pipelines. Results are emitted one per rule as they complete, followed by a summary line.

```bash
sentinel validate data.csv --rules rules.yaml --agent
```

```json
{"type":"result","rule":"no_nulls_in_age","status":"pass","violations":0,"total_rows":100,"duration_ms":12}
{"type":"result","rule":"age_is_positive","status":"fail","violations":3,"total_rows":100,"duration_ms":8}
{"type":"summary","passed":1,"failed":1,"quality_score":0.5,"duration_ms":21}
```

On error, a structured error object is written to stderr:

```json
{"type":"error","code":"file_not_found","message":"Could not read file: data.csv"}
```

Error codes: `file_not_found`, `rules_parse_error`, `schema_mismatch`, `rule_execution_error`, `validation_error`.

## Supported file formats

- CSV (`.csv`)
- Parquet (`.parquet`)

## Cloud storage

Sentinel can read files directly from Azure Blob Storage and Amazon S3. Credentials are read from environment variables — no code changes needed.

### Azure Blob Storage

Use the `az://` scheme:

```bash
sentinel validate az://my-container/path/to/data.csv --rules rules.yaml
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
sentinel validate s3://my-bucket/path/to/data.parquet --rules rules.yaml
```

Set these environment variables before running:

| Variable | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_DEFAULT_REGION` | Bucket region (e.g. `us-east-1`) |

For S3-compatible stores (MinIO, etc.), also set `AWS_ENDPOINT` to point to your endpoint.
