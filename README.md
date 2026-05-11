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

Sentinel has five subcommands: `validate`, `schema`, `profile`, `query`, and `head`.

### validate

Run data quality rules against a file.

```bash
sentinel validate <data-file> --rules <rules-file> [OPTIONS]
```

| Flag | Description |
|---|---|
| `-r, --rules <file>` | Path to rules YAML file (use `-` for stdin). Optional if `--rule` is used. |
| `--rule <SPEC>` | Inline rule spec (repeatable). See [Inline rules](#inline-rules). |
| `-f, --format <fmt>` | Output format: `json` (default) or `table` |
| `--dry-run` | Validate rules file and schema without running checks |
| `--verbose` | Print full error chain on failure |
| `--show-violations [N]` | Attach first N violating rows to each failed rule (default 5) |
| `--agent` | Stream JSON Lines output for machine consumption (see Agent mode) |

At least one of `--rules` or `--rule` must be provided.

#### Inline rules

Pass rules directly on the command line with `--rule` using the compact syntax `check:column[:arg...]`:

```bash
sentinel validate data.csv \
  --rule not_null:id \
  --rule between:age:18:99 \
  --rule regex:email:'^[^@]+@[^@]+$'
```

Supported forms:

| Form | Example |
|---|---|
| `not_null:<column>` | `not_null:id` |
| `not_empty:<column>` | `not_empty:name` |
| `unique:<column>` | `unique:id` |
| `min:<column>:<value>` | `min:age:0` |
| `max:<column>:<value>` | `max:age:120` |
| `between:<column>:<min>:<max>` | `between:age:18:99` |
| `regex:<column>:<pattern>` | `regex:email:^[^@]+@[^@]+$` (pattern may contain `:`) |

Each inline rule is named `{column}_{check}`; duplicates across `--rule` flags or against YAML rules are disambiguated with `_2`, `_3`, … suffixes.

Inline rules are always severity `error`. For `warning`, `threshold`, or `custom` SQL rules, use a rules YAML file. You can combine both: `--rules rules.yaml --rule not_null:id`.

#### Reading rules from stdin

Pipe YAML rules into sentinel with `--rules -`:

```bash
cat rules.yaml | sentinel validate data.csv --rules -
```

Empty stdin is accepted when at least one `--rule` flag is also present — useful for coding agents that prefer passing everything via flags:

```bash
echo '' | sentinel validate data.csv --rules - --rule not_null:id
```

### profile

Profile a dataset — no rules file needed. Prints per-column stats (including quantiles and top-K frequent values) and emits a ready-to-use `rules.yaml` block you can paste straight into a rules file.

```bash
sentinel profile <data-file> [--format text|json]
```

| Flag | Description |
|---|---|
| `-f, --format <fmt>` | Output format: `text` (default, human-readable) or `json` (structured, for agents) |

```
Column: age
  type:        int64
  nulls:       0 (0.0%)
  unique:      71
  min:         18
  max:         92
  mean:        34.70
  p01:         19.00
  p25:         27.00
  p50:         34.00
  p75:         42.00
  p99:         88.00

Column: status
  type:        utf8
  nulls:       0 (0.0%)
  unique:      3
  top values:
    active × 720
    pending × 210
    closed × 70

---
Suggested rules (1000 rows):

rules:
- name: age_not_null
  column: age
  check: not_null
- name: age_range
  column: age
  check: between
  min: 18.0
  max: 92.0
- name: age_typical_range
  column: age
  check: between
  min: 19.0
  max: 88.0
  threshold: 0.02
  severity: warning
- name: status_not_null
  column: status
  check: not_null
```

Stats emitted:
- **All columns** — type, null count/rate, unique count
- **Numeric columns** (int/float) — min, max, mean, plus P01/P25/P50/P75/P99 quantiles (via t-digest approximation)
- **Low-cardinality columns** (2 ≤ unique ≤ 50) — top 10 most-frequent non-null values with counts

Rule suggestion logic:
- **`not_null`** — suggested for any column with 0% nulls (error severity)
- **`not_null` + threshold** — suggested for columns with ≤ 20% nulls (warning severity, threshold = observed null rate rounded up)
- **`between`** (min/max) — suggested for numeric columns using observed min/max as bounds
- **`between`** (typical range) — additionally suggested for numeric columns on datasets of ≥ 100 rows, using P01/P99 bounds with a 2% violation threshold (warning severity) — more robust to outliers than the raw min/max rule
- **`unique`** — suggested when all values in the column are distinct

Use `--format json` for structured output, useful for coding agents or automation:

```bash
sentinel profile data.csv --format json
```

### query

Run arbitrary SQL against the dataset and stream rows as JSONL. The dataset is registered as the table named `data`, so queries must reference `FROM data`.

```bash
sentinel query <data-file> --sql "<SQL>" [--max-rows <N>]
```

| Flag | Description |
|---|---|
| `-s, --sql <sql>` | SQL to execute (required) |
| `--max-rows <N>` | Cap on rows returned (default `1000`) — applied via `LIMIT` on top of the user query, safe with `WITH`/`UNION`/etc. |

One JSON object per row is written to stdout, keyed by column name. Nulls are emitted explicitly.

```bash
sentinel query examples/data.csv --sql "SELECT * FROM data WHERE age IS NULL OR age > 27"
```

```json
{"age":30,"name":"alice"}
{"age":null,"name":"bob"}
```

### head

Return the first N rows of the dataset as JSONL — a convenience wrapper over `query`.

```bash
sentinel head <data-file> [-n <N>]
```

| Flag | Description |
|---|---|
| `-n <N>` | Number of rows to return (default `10`) |

```bash
sentinel head examples/data.csv -n 2
```

```json
{"age":30,"name":"alice"}
{"age":null,"name":"bob"}
```

### schema

Inspect the schema and basic stats of a dataset — no rules file needed.

```bash
sentinel schema <data-file>
```

Outputs JSON with per-column info (type, null count, distinct count, min/max/mean and P01/P25/P50/P75/P99 quantiles for numeric columns) and total row count:

```json
{
  "columns": [
    { "name": "age",  "type": "int64",  "nulls": 2,  "unique": 87, "min": 18.0, "max": 99.0, "mean": 34.7,
      "p01": 19.0, "p25": 27.0, "p50": 34.0, "p75": 42.0, "p99": 88.0 },
    { "name": "name", "type": "utf8",   "nulls": 0,  "unique": 100 },
    { "name": "flag", "type": "bool",   "nulls": 1,  "unique": 2 }
  ],
  "row_count": 100
}
```

Quantiles are approximate (DataFusion's `approx_percentile_cont` / t-digest) and omitted for non-numeric columns.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | All rules passed |
| `1` | At least one `error`-severity rule failed, or input file is empty |
| `2` | Only `warning`-severity rules failed (no errors) |
| `3` | Invalid rules file or schema mismatch (also: bad SQL for `query`) |
| `4` | Data file not found or unreadable |

Codes `1` and `2` apply to `validate` only; `query`, `head`, `schema`, and `profile` exit with `0`, `3`, or `4`.

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

#### Azurite (local emulator)

To point at a local [Azurite](https://github.com/Azure/Azurite) instance instead of real Azure, set:

| Variable | Description |
|---|---|
| `AZURE_STORAGE_USE_EMULATOR` | `true` — uses the well-known `devstoreaccount1` credentials and HTTP |
| `AZURITE_BLOB_STORAGE_URL` | Optional; defaults to `http://127.0.0.1:10000`. Override only if Azurite runs on a different host/port. |

`AZURE_STORAGE_ACCOUNT_NAME` / `AZURE_STORAGE_ACCOUNT_KEY` are **not** needed in emulator mode — they're filled in automatically. Example:

```bash
export AZURE_STORAGE_USE_EMULATOR=true
sentinel schema az://my-container/data.csv
```

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
