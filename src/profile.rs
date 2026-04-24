use crate::schema::{ColumnInfo, SchemaOutput};
use anyhow::Context;
use datafusion::prelude::*;
use serde::Serialize;
use serde_json::Value as JsonValue;

// Top-K is suppressed past 50 distinct values — at that point "which values
// appear most" stops being a useful agent signal and becomes noise.
const TOP_K_MAX_UNIQUE: u64 = 50;
const TOP_K_LIMIT: usize = 10;

// P01/P99-based `typical_range` is gated on a minimum row count. On tiny
// datasets the t-digest endpoints collapse toward the observed min/max, which
// makes the suggested rule identical to `{col}_range` (redundant noise).
const TYPICAL_RANGE_MIN_ROWS: u64 = 100;

#[derive(Debug, Serialize, Clone)]
pub struct SuggestedRule {
    pub name: String,
    pub column: String,
    pub check: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
}

#[derive(Serialize)]
struct SuggestedRulesFile<'a> {
    rules: &'a [SuggestedRule],
}

#[derive(Debug, Serialize, Clone)]
pub struct TopKValue {
    pub value: JsonValue,
    pub count: u64,
}

#[derive(Debug, Serialize)]
pub struct ColumnProfile {
    #[serde(flatten)]
    pub info: ColumnInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_values: Option<Vec<TopKValue>>,
}

#[derive(Debug, Serialize)]
pub struct ProfileOutput {
    pub row_count: u64,
    pub columns: Vec<ColumnProfile>,
    pub suggested_rules: Vec<SuggestedRule>,
}

async fn run_top_k(
    ctx: &SessionContext,
    col: &str,
    table: &str,
    limit: usize,
) -> anyhow::Result<Vec<TopKValue>> {
    use crate::arrow_json::record_batches_to_json_rows;
    use datafusion::arrow::array::{Array, Int64Array};

    // Exclude NULLs — otherwise GROUP BY returns NULL as its own group and
    // crowds out the real signal.
    let sql = format!(
        "SELECT \"{col}\" AS value, COUNT(*) AS count \
         FROM {table} \
         WHERE \"{col}\" IS NOT NULL \
         GROUP BY \"{col}\" \
         ORDER BY count DESC, value ASC \
         LIMIT {limit}",
        col = col,
        table = table,
        limit = limit
    );
    let df = ctx.sql(&sql).await.context("top-k query failed")?;
    let batches = df.collect().await.context("Failed to collect top-k")?;
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(vec![]);
    }

    // Convert to JSON rows so the `value` column preserves its original type
    // (string / int / bool) without us having to dispatch per Arrow type.
    let rows = record_batches_to_json_rows(&batches)?;

    // Pull COUNT(*) from its native Int64 array into u64 directly.
    // COUNT(*) is Int64 and non-null per SQL standard, so no null check needed.
    let counts: Vec<u64> = batches
        .iter()
        .map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("Expected Int64 count column")
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .iter()
        .flat_map(|arr| (0..arr.len()).map(|i| arr.value(i) as u64))
        .collect();

    Ok(rows
        .into_iter()
        .zip(counts)
        .map(|(row, count)| TopKValue {
            value: row.get("value").cloned().unwrap_or(JsonValue::Null),
            count,
        })
        .collect())
}

pub async fn build_profile(
    ctx: &SessionContext,
    schema: SchemaOutput,
    table: &str,
) -> anyhow::Result<ProfileOutput> {
    let suggested_rules = generate_suggested_rules(&schema);
    let row_count = schema.row_count;

    let mut column_profiles = Vec::with_capacity(schema.columns.len());
    for info in schema.columns {
        // Top-K is meaningful when there's a real distribution: at least 2
        // distinct values (so there's something to rank) and not more than
        // TOP_K_MAX_UNIQUE (past which it's just a sample of a free-text column).
        let top_values = if info.unique > 1 && info.unique <= TOP_K_MAX_UNIQUE {
            let top = run_top_k(ctx, &info.name, table, TOP_K_LIMIT).await?;
            if top.is_empty() {
                None
            } else {
                Some(top)
            }
        } else {
            None
        };

        column_profiles.push(ColumnProfile { info, top_values });
    }

    Ok(ProfileOutput {
        row_count,
        columns: column_profiles,
        suggested_rules,
    })
}

pub fn generate_suggested_rules(schema: &SchemaOutput) -> Vec<SuggestedRule> {
    let mut rules = Vec::new();

    for col in &schema.columns {
        let null_rate = if schema.row_count > 0 {
            col.nulls as f64 / schema.row_count as f64
        } else {
            0.0
        };

        let col_slug = col.name.replace(' ', "_").to_lowercase();

        if col.nulls == 0 {
            rules.push(SuggestedRule {
                name: format!("{col_slug}_not_null"),
                column: col.name.clone(),
                check: "not_null".to_string(),
                min: None,
                max: None,
                threshold: None,
                severity: None,
            });
        } else if null_rate <= 0.20 {
            // threshold = null_rate rounded up to 2 dp, giving a little headroom
            let threshold = (null_rate * 100.0).ceil() / 100.0;
            rules.push(SuggestedRule {
                name: format!("{col_slug}_mostly_not_null"),
                column: col.name.clone(),
                check: "not_null".to_string(),
                min: None,
                max: None,
                threshold: Some(threshold),
                severity: Some("warning".to_string()),
            });
        }

        if let (Some(min), Some(max)) = (col.min, col.max) {
            rules.push(SuggestedRule {
                name: format!("{col_slug}_range"),
                column: col.name.clone(),
                check: "between".to_string(),
                min: Some(min),
                max: Some(max),
                threshold: None,
                severity: None,
            });
        }

        // Typical-range: P01..P99 with a 2% violation budget. Suggested only
        // when there's enough data for the t-digest endpoints to move off the
        // exact min/max — otherwise it duplicates `{col}_range`. Skipped when
        // p01 == p99 (constant column), since that produces a degenerate
        // `between: X..X` rule that adds no signal over the raw range.
        if schema.row_count >= TYPICAL_RANGE_MIN_ROWS {
            if let (Some(p01), Some(p99)) = (col.p01, col.p99) {
                if p01 < p99 {
                    rules.push(SuggestedRule {
                        name: format!("{col_slug}_typical_range"),
                        column: col.name.clone(),
                        check: "between".to_string(),
                        min: Some(p01),
                        max: Some(p99),
                        threshold: Some(0.02),
                        severity: Some("warning".to_string()),
                    });
                }
            }
        }

        // column looks like an ID — values are all distinct
        if schema.row_count > 1 && col.unique == schema.row_count {
            rules.push(SuggestedRule {
                name: format!("{col_slug}_unique"),
                column: col.name.clone(),
                check: "unique".to_string(),
                min: None,
                max: None,
                threshold: None,
                severity: None,
            });
        }
    }

    rules
}

pub fn format_profile_text(profile: &ProfileOutput) -> String {
    let mut out = String::new();

    for cp in &profile.columns {
        let col = &cp.info;
        let null_pct = if profile.row_count > 0 {
            col.nulls as f64 / profile.row_count as f64 * 100.0
        } else {
            0.0
        };

        out.push_str(&format!("Column: {}\n", col.name));
        out.push_str(&format!("  type:        {}\n", col.data_type));
        out.push_str(&format!(
            "  nulls:       {} ({:.1}%)\n",
            col.nulls, null_pct
        ));
        out.push_str(&format!("  unique:      {}\n", col.unique));
        // min/max render as integers when the source column was integer (no
        // forced .2f); mean and quantiles always render with 2-decimal
        // precision so small rounding differences are visible.
        if let Some(v) = col.min {
            out.push_str(&format!("  {:<13}{v}\n", "min:"));
        }
        if let Some(v) = col.max {
            out.push_str(&format!("  {:<13}{v}\n", "max:"));
        }
        for (label, v) in [
            ("mean:", col.mean),
            ("p01:", col.p01),
            ("p25:", col.p25),
            ("p50:", col.p50),
            ("p75:", col.p75),
            ("p99:", col.p99),
        ] {
            if let Some(v) = v {
                out.push_str(&format!("  {label:<13}{v:.2}\n"));
            }
        }

        if let Some(values) = &cp.top_values {
            if !values.is_empty() {
                out.push_str("  top values:\n");
                for tv in values {
                    out.push_str(&format!(
                        "    {} × {}\n",
                        format_json_value(&tv.value),
                        tv.count
                    ));
                }
            }
        }

        out.push('\n');
    }

    out.push_str("---\n");
    if profile.suggested_rules.is_empty() {
        out.push_str("No rules suggested.\n");
    } else {
        out.push_str(&format!(
            "Suggested rules ({} rows):\n\n",
            profile.row_count
        ));
        let wrapper = SuggestedRulesFile {
            rules: &profile.suggested_rules,
        };
        out.push_str(&serde_yaml::to_string(&wrapper).unwrap_or_default());
    }

    out
}

fn format_json_value(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Null => "null".to_string(),
        other => other.to_string(),
    }
}

pub fn format_profile_json(profile: &ProfileOutput) -> anyhow::Result<String> {
    serde_json::to_string_pretty(profile).context("Failed to serialize profile to JSON")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnInfo, SchemaOutput};

    fn make_schema(columns: Vec<ColumnInfo>, row_count: u64) -> SchemaOutput {
        SchemaOutput { columns, row_count }
    }

    fn col(
        name: &str,
        nulls: u64,
        unique: u64,
        min: Option<f64>,
        max: Option<f64>,
        mean: Option<f64>,
    ) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: "int64".to_string(),
            nulls,
            unique,
            min,
            max,
            mean,
            p01: None,
            p25: None,
            p50: None,
            p75: None,
            p99: None,
        }
    }

    fn col_with_quantiles(
        name: &str,
        nulls: u64,
        unique: u64,
        min: Option<f64>,
        max: Option<f64>,
        p01: f64,
        p99: f64,
    ) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: "int64".to_string(),
            nulls,
            unique,
            min,
            max,
            mean: None,
            p01: Some(p01),
            p25: Some((p01 + p99) / 4.0),
            p50: Some((p01 + p99) / 2.0),
            p75: Some((p01 + p99) * 3.0 / 4.0),
            p99: Some(p99),
        }
    }

    #[test]
    fn test_no_nulls_suggests_not_null() {
        let schema = make_schema(vec![col("id", 0, 100, None, None, None)], 100);
        let rules = generate_suggested_rules(&schema);
        assert!(rules
            .iter()
            .any(|r| r.check == "not_null" && r.threshold.is_none()));
    }

    #[test]
    fn test_some_nulls_suggests_not_null_with_threshold() {
        let schema = make_schema(vec![col("age", 5, 80, None, None, None)], 100);
        let rules = generate_suggested_rules(&schema);
        let r = rules.iter().find(|r| r.check == "not_null").unwrap();
        assert!(r.threshold.is_some());
        assert_eq!(r.severity, Some("warning".to_string()));
    }

    #[test]
    fn test_high_null_rate_no_rule() {
        let schema = make_schema(vec![col("notes", 60, 40, None, None, None)], 100);
        let rules = generate_suggested_rules(&schema);
        assert!(!rules.iter().any(|r| r.check == "not_null"));
    }

    #[test]
    fn test_numeric_suggests_between() {
        let schema = make_schema(
            vec![col("age", 0, 71, Some(18.0), Some(92.0), Some(34.7))],
            100,
        );
        let rules = generate_suggested_rules(&schema);
        let r = rules.iter().find(|r| r.check == "between").unwrap();
        assert_eq!(r.min, Some(18.0));
        assert_eq!(r.max, Some(92.0));
    }

    #[test]
    fn test_all_distinct_suggests_unique() {
        let schema = make_schema(vec![col("id", 0, 100, None, None, None)], 100);
        let rules = generate_suggested_rules(&schema);
        assert!(rules.iter().any(|r| r.check == "unique"));
    }

    #[test]
    fn test_single_row_no_unique_suggestion() {
        let schema = make_schema(vec![col("id", 0, 1, None, None, None)], 1);
        let rules = generate_suggested_rules(&schema);
        assert!(!rules.iter().any(|r| r.check == "unique"));
    }

    #[test]
    fn test_typical_range_suggested_when_quantiles_and_enough_rows() {
        let schema = make_schema(
            vec![col_with_quantiles(
                "age",
                0,
                80,
                Some(0.0),
                Some(200.0),
                18.0,
                95.0,
            )],
            500,
        );
        let rules = generate_suggested_rules(&schema);
        let r = rules
            .iter()
            .find(|r| r.name == "age_typical_range")
            .unwrap();
        assert_eq!(r.check, "between");
        assert_eq!(r.min, Some(18.0));
        assert_eq!(r.max, Some(95.0));
        assert_eq!(r.threshold, Some(0.02));
        assert_eq!(r.severity, Some("warning".to_string()));
    }

    #[test]
    fn test_typical_range_skipped_below_row_threshold() {
        let schema = make_schema(
            vec![col_with_quantiles(
                "age",
                0,
                80,
                Some(0.0),
                Some(200.0),
                18.0,
                95.0,
            )],
            50,
        );
        let rules = generate_suggested_rules(&schema);
        assert!(!rules.iter().any(|r| r.name == "age_typical_range"));
    }

    #[test]
    fn test_typical_range_absent_without_quantiles() {
        let schema = make_schema(vec![col("age", 0, 80, Some(0.0), Some(200.0), None)], 500);
        let rules = generate_suggested_rules(&schema);
        assert!(!rules.iter().any(|r| r.name == "age_typical_range"));
    }

    #[test]
    fn test_typical_range_absent_when_p01_equals_p99() {
        // Constant-valued numeric column — every percentile collapses to the
        // same value. Emitting `between: X..X` with a 2% threshold would be
        // a degenerate rule that adds no signal over the raw `{col}_range`.
        let schema = make_schema(
            vec![col_with_quantiles(
                "constant_col",
                0,
                1,
                Some(42.0),
                Some(42.0),
                42.0,
                42.0,
            )],
            500,
        );
        let rules = generate_suggested_rules(&schema);
        assert!(!rules.iter().any(|r| r.name == "constant_col_typical_range"));
    }

    #[tokio::test]
    async fn test_build_profile_emits_top_k_for_bounded_cardinality() {
        let ctx = SessionContext::new();
        ctx.sql(
            "CREATE TABLE data AS SELECT * FROM \
             (VALUES ('a'), ('a'), ('a'), ('b'), ('b'), ('c')) AS t(category)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let schema = crate::schema::introspect(&ctx, "data").await.unwrap();
        let profile = build_profile(&ctx, schema, "data").await.unwrap();
        let col = &profile.columns[0];
        assert!(
            col.top_values.is_some(),
            "expected top_values for low-cardinality string column"
        );
        let top = col.top_values.as_ref().unwrap();
        // 'a' should be first with count 3
        assert_eq!(top[0].count, 3);
        assert_eq!(top[0].value, serde_json::json!("a"));
    }

    #[tokio::test]
    async fn test_build_profile_skips_top_k_above_cardinality_cap() {
        // 51 distinct integers — above TOP_K_MAX_UNIQUE
        let ctx = SessionContext::new();
        let values: Vec<String> = (1..=51).map(|i| format!("({i})")).collect();
        let sql = format!(
            "CREATE TABLE data AS SELECT * FROM (VALUES {}) AS t(v)",
            values.join(", ")
        );
        ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let schema = crate::schema::introspect(&ctx, "data").await.unwrap();
        let profile = build_profile(&ctx, schema, "data").await.unwrap();
        let col = &profile.columns[0];
        assert!(col.top_values.is_none(), "expected no top_values above cap");
    }

    #[tokio::test]
    async fn test_build_profile_top_k_excludes_nulls() {
        let ctx = SessionContext::new();
        ctx.sql(
            "CREATE TABLE data AS SELECT * FROM \
             (VALUES ('a'), ('a'), (NULL), (NULL), (NULL), ('b')) AS t(cat)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        let schema = crate::schema::introspect(&ctx, "data").await.unwrap();
        let profile = build_profile(&ctx, schema, "data").await.unwrap();
        let col = &profile.columns[0];
        let top = col.top_values.as_ref().expect("top_values present");
        for tv in top {
            assert!(!tv.value.is_null(), "top-k should not include NULL values");
        }
    }

    #[test]
    fn test_profile_json_flattens_column_info() {
        let info = col("x", 0, 10, Some(1.0), Some(9.0), Some(5.0));
        let profile = ProfileOutput {
            row_count: 10,
            columns: vec![ColumnProfile {
                info,
                top_values: Some(vec![TopKValue {
                    value: serde_json::json!("a"),
                    count: 3,
                }]),
            }],
            suggested_rules: vec![],
        };
        let json = format_profile_json(&profile).unwrap();
        // Flattened: `name` at top level of column object, not nested under `info`
        assert!(json.contains("\"name\": \"x\""));
        assert!(!json.contains("\"info\":"));
        assert!(json.contains("\"top_values\":"));
    }
}
