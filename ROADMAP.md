# Sentinel Roadmap

This document tracks planned features and their priority. Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Agent-Optimized Use

Sentinel is designed to be used by coding agents exploring and validating datasets programmatically. This section covers features specifically for that use case.

### A. Schema Introspection Command

`sentinel schema data.csv` — emit column names, types, and basic stats as JSON. Lets an agent understand the data before writing rules.

```json
{
  "columns": [
    {"name": "id",    "type": "int64",  "nulls": 0,   "unique": 1000},
    {"name": "email", "type": "utf8",   "nulls": 12,  "unique": 988},
    {"name": "age",   "type": "int64",  "nulls": 0,   "min": 18, "max": 92}
  ],
  "row_count": 1000
}
```

An agent can call this first, then generate a rules YAML, then validate — all in one workflow.

### B. Machine-Readable Output by Default (Agent Mode)

`--agent` flag (or `SENTINEL_AGENT=1` env var) that:
- Forces JSON output (no colors, no tables, no human summaries)
- Adds `row_count`, `schema`, and `duration_ms` to every response
- Emits a structured `summary` object as the final line with overall pass/fail and quality score

```json
{"type": "result",  "rule": "not_null_id", "status": "PASS", "violations": 0, "total": 1000}
{"type": "result",  "rule": "email_format","status": "FAIL", "violations": 12,"total": 1000}
{"type": "summary", "passed": 1, "failed": 1, "quality_score": 0.5, "duration_ms": 310}
```

Agents can parse stdout line-by-line without special handling.

### C. Rule Generation from Natural Language Description

`sentinel generate --description "email must be valid, age must be between 18 and 99, id must be unique"` — use an LLM or pattern-matching to emit a ready-to-use `rules.yaml`.

Agents can pass a plain-English constraint description and get back a valid rules file to execute immediately.

### D. Inline Rules (No Rules File Required)

Pass rules directly as a CLI argument instead of a YAML file — better for agent-generated one-shot checks.

```bash
sentinel validate data.csv \
  --rule 'not_null:id' \
  --rule 'between:age:18:99' \
  --rule 'regex:email:^[^@]+@[^@]+\.[^@]+$'
```

Avoids the agent needing to write a temp file to disk.

### E. Granular Exit Codes

Current: `0` = pass, `1` = fail. For agents:

| Code | Meaning                               |
|------|---------------------------------------|
| 0    | All rules passed                      |
| 1    | One or more error-severity rules failed|
| 2    | One or more warning-severity rules triggered (no errors) |
| 3    | Invalid rules file or schema mismatch |
| 4    | Data file not found or unreadable     |

Agents can branch on exit code without parsing stdout.

### F. Structured Error Output

Today errors go to stderr as plain text. In agent mode, errors should also be JSON:

```json
{"type": "error", "code": "schema_mismatch", "rule": "not_null_id", "message": "Column 'id' not found. Available: [user_id, email, age]"}
```

Agents can surface the exact error back to the user or retry with corrected rules.

### G. Validation Dry-Run with Rule Explanation

`sentinel explain rules.yaml --data data.csv` — for each rule, emit the SQL it will run and what it checks. Already partially implemented via `--dry-run`; extend to emit the generated SQL as JSON.

```json
{
  "rule": "not_null_id",
  "sql": "SELECT COUNT(*) FROM data WHERE \"id\" IS NULL",
  "explanation": "Counts rows where column 'id' is NULL. Fails if count > 0."
}
```

Agents can log this for debugging or show it to the user for transparency.

---

## Tier 1 — Critical User Value

These are table-stakes for a data quality tool. Without them, users hit walls fast.

### 1. Violation Row Export

`--show-violations [N]` — show first N rows that failed each rule.

Users today know *that* a rule failed but not *which rows*. This is the #1 friction point.

```json
{
  "rule": "price_not_negative",
  "status": "FAIL",
  "violations": 3,
  "sample_rows": [
    {"row": 42, "price": -5.00},
    {"row": 107, "price": -0.01}
  ]
}
```

Useful for both engineers (root cause analysis) and analysts (stakeholder reports).

### 2. Rule Severity Levels

`severity: warning | error` — warnings print but do not fail the exit code.

```yaml
rules:
  - name: no_nulls_in_id
    column: id
    check: not_null
    severity: error      # fails pipeline

  - name: phone_format
    column: phone
    check: regex
    pattern: '^\+?[\d\s\-]{7,15}$'
    severity: warning    # alerts but does not block
```

Engineers need this for CI/CD (block on critical, warn on soft issues). Analysts need it to distinguish blockers from informational checks.

### 3. Parallel Rule Execution

Run rules concurrently via `tokio::spawn` + `join_all` instead of sequentially.

10–50 rules on a large Parquet file = significant wall-clock savings. Zero CLI change — pure internal improvement. `SessionContext` would be `Arc`-shared across tasks.

### 4. Data Profiling Mode

`sentinel profile data.csv` — auto-generate column stats and candidate rule YAML.

```
Column: age
  type:        int64
  nulls:       2.1%
  min:         18
  max:         92
  mean:        34.7
  cardinality: 71

Suggested rules:
  - {name: age_not_null, column: age, check: not_null}
  - {name: age_range, column: age, check: between, min: 0, max: 120}
```

Removes the cold-start problem. Analysts benefit most — no SQL knowledge needed to get started.

---

## Tier 2 — Strong Differentiators

Features that separate Sentinel from basic scripts.

### 5. Built-in Rule Templates

First-class check types for common patterns — no custom SQL needed.

| Check type    | What it validates            |
|---------------|------------------------------|
| `email`       | Valid email format            |
| `url`         | Valid HTTP/HTTPS URL          |
| `iso_date`    | ISO 8601 date (YYYY-MM-DD)   |
| `phone`       | International phone number    |
| `uuid`        | UUID v4 format               |
| `positive`    | Value > 0                    |
| `non_negative`| Value >= 0                   |

Implemented as pre-baked regex/SQL patterns in the existing `build_sql()` dispatch. No new architecture required.

### 6. Multi-Column Rules

Cross-column validation — the most common reason users fall back to `custom` SQL.

```yaml
- name: discount_not_exceed_price
  check: less_than_or_equal
  column: discount
  compare_column: price

- name: end_after_start
  check: greater_than
  column: end_date
  compare_column: start_date
```

New check variants: `less_than_column`, `greater_than_column`, `less_than_or_equal_column`, `not_equal_column`.

### 7. Rich Output Formats

| Format     | Flag                | Use case                              |
|------------|---------------------|---------------------------------------|
| JSON Lines | default             | Machine consumption (existing)        |
| Table      | `--output table`    | Terminal review (existing)            |
| HTML       | `--output html`     | Stakeholder reports, shareable        |
| Markdown   | `--output markdown` | GitHub PRs, Notion, wikis             |
| CSV        | `--output csv`      | Violation export to spreadsheets      |
| JUnit XML  | `--output junit`    | Native CI/CD test report integration  |

JUnit XML is highest value for engineers — GitHub Actions, Jenkins, and GitLab all render it as native test results.

### 8. Overall Data Quality Score

Summary metric at end of run: `Quality Score: 87.5% (7/8 rules passed)`.

Table and HTML output would show a header with total pass/fail counts, score percentage, and total execution time. Gives analysts a single KPI to report upstream.

### 9. Execution Timing Per Rule

`duration_ms` field on each rule result. The `tracing` infrastructure already exists — add `Instant::now()` around `run_sql()`.

```json
{"rule": "not_null_id", "status": "PASS", "duration_ms": 142}
```

Lets engineers identify slow rules on large datasets.

---

## Tier 3 — Ecosystem Integration

Features that grow adoption and reduce friction.

### 10. Config File Support

`sentinel.toml` (project-level) and `~/.sentinel.toml` (user-level) for persisting defaults.

```toml
[defaults]
output = "table"
show_violations = 5

[storage.s3]
region = "us-east-1"
endpoint = "http://localhost:4566"
```

### 11. Watch Mode

`--watch` — re-run validation automatically when the input file changes (via the `notify` crate).

```
sentinel validate data.csv rules.yaml --watch
[14:32:01] PASS  7/8 rules
[14:32:45] File changed. Re-running...
[14:32:46] FAIL  6/8 rules
```

Good for analysts cleaning data interactively.

### 12. Native GitHub Actions Action

`uses: sentinel-data/sentinel-action@v1` — zero-install CI integration with pre-built binaries cached between runs.

```yaml
- uses: sentinel-data/sentinel-action@v1
  with:
    data: data/customers.parquet
    rules: rules/customers.yaml
    output: junit
```

### 13. Pre-built Docker Image

```bash
docker run ghcr.io/sentinel-data/sentinel validate data.csv rules.yaml
```

Enables use in any CI/CD without a Rust toolchain.

### 14. Delta / Incremental Validation

`--since <timestamp|row_id>` — validate only rows added since the last run.

Uses a state file (`.sentinel_state.json`) tracking the last-run timestamp or max ID. Primarily useful for append-only data pipelines.

### 15. Rule Groups / Tags

```yaml
rules:
  - name: id_not_null
    column: id
    check: not_null
    tags: [critical, pii]
```

```bash
sentinel validate data.csv rules.yaml --tags critical
```

Enables fast critical-only checks on every commit and full suite runs nightly.

---

## Priority Matrix

| Feature                   | Impact | Effort | Audience  | Priority |
|---------------------------|--------|--------|-----------|----------|
| Violation Row Export      | High   | Med    | Both      | P0       |
| Rule Severity Levels      | High   | Low    | Both      | P0       |
| Parallel Execution        | High   | Med    | Engineers | P0       |
| Data Profiling Mode       | High   | High   | Analysts  | P1       |
| Built-in Rule Templates   | Med    | Low    | Both      | P1       |
| Multi-Column Rules        | High   | Med    | Engineers | P1       |
| Rich Output Formats       | High   | Med    | Both      | P1       |
| Quality Score Summary     | Med    | Low    | Analysts  | P1       |
| Execution Timing          | Med    | Low    | Engineers | P1       |
| Config File               | Med    | Med    | Both      | P2       |
| Watch Mode                | Med    | Med    | Analysts  | P2       |
| GitHub Actions Action     | High   | Med    | Engineers | P2       |
| Docker Image              | Med    | Low    | Engineers | P2       |
| Delta Validation          | High   | High   | Engineers | P3       |
| Rule Groups / Tags        | Med    | Med    | Both      | P3       |
| **Agent features**        |        |        |           |          |
| Schema Introspection      | High   | Low    | Agents    | P0       |
| Agent Mode / JSON output  | High   | Low    | Agents    | P0       |
| Granular Exit Codes       | High   | Low    | Agents    | P0       |
| Structured Error Output   | High   | Low    | Agents    | P0       |
| Inline Rules (no file)    | Med    | Med    | Agents    | P1       |
| Dry-run with SQL explain  | Med    | Low    | Agents    | P1       |
| Rule Generation (NL)      | High   | High   | Agents    | P2       |
