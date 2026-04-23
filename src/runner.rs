use crate::rules::{Check, Rule};
use anyhow::Context;
use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;
use serde::Serialize;
use serde_json::{json, Value as JsonValue};

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleStatus {
    Pass,
    Fail,
}

impl std::fmt::Display for RuleStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuleStatus::Pass => write!(f, "pass"),
            RuleStatus::Fail => write!(f, "fail"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RuleResult {
    pub name: String,
    pub status: RuleStatus,
    pub violations: u64,
    pub total_rows: u64,
    pub violation_rate: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_rows: Option<Vec<JsonValue>>,
}

fn build_sql(rule: &Rule) -> anyhow::Result<String> {
    match &rule.check {
        Check::NotNull => Ok(format!(
            "SELECT COUNT(*) FROM data WHERE \"{}\" IS NULL",
            rule.column
       )),
        Check::Min => {
            let thr = rule.min.context("Min check requires a min value")?;
            Ok(format!(
                "SELECT COUNT(*) FROM data WHERE \"{}\" < {}",
                rule.column, thr
            ))
        }
        Check::Max => {
            let thr = rule.max.context("Max check requires a max value")?;
            Ok(format!(
                "SELECT COUNT(*) FROM data WHERE \"{}\" > {}",
                rule.column, thr
            ))
        }
        Check::NotEmpty => Ok(format!(
            "SELECT COUNT(*) FROM data WHERE \"{}\" = ''",
            rule.column
        )),
        Check::Between => {
            let min = rule.min.context("Between check requires a min value")?;
            let max = rule.max.context("Between check requires a max value")?;
            Ok(format!(
                "SELECT COUNT(*) FROM data WHERE \"{}\" < {} OR \"{}\" > {}",
                rule.column, min, rule.column, max
            ))
        }
        Check::Unique => Ok(format!(
            "SELECT COALESCE(SUM(cnt), 0) FROM (SELECT COUNT(\"{}\") AS cnt FROM data GROUP BY \"{}\" HAVING COUNT(\"{}\")>1)",
            rule.column, rule.column, rule.column
        )),
        Check::Regex => {
            let pattern = rule
                .pattern
                .clone()
                .context("Regex check requires a pattern value")?;
            let escaped = pattern.replace('\'', "''");
            Ok(format!(
                "SELECT COUNT(*) FROM data WHERE REGEXP_MATCH(\"{}\", '{}') IS NULL",
                rule.column, escaped
            ))
        },
        Check::Custom => {
            let sql = rule.sql.clone().context("Custom check requires an sql value")?;
            Ok(sql)
        }
    }
}

/// Build a SQL query that SELECTs the violating rows (not a COUNT).
/// Returns `Ok(None)` for `Custom` checks, which cannot produce sample rows.
pub fn build_violation_sample_sql(rule: &Rule, limit: u64) -> anyhow::Result<Option<String>> {
    match &rule.check {
        Check::NotNull => Ok(Some(format!(
            "SELECT * FROM data WHERE \"{}\" IS NULL LIMIT {}",
            rule.column, limit
        ))),
        Check::NotEmpty => Ok(Some(format!(
            "SELECT * FROM data WHERE \"{}\" = '' LIMIT {}",
            rule.column, limit
        ))),
        Check::Min => {
            let min = rule.min.context("Min check requires a min value")?;
            Ok(Some(format!(
                "SELECT * FROM data WHERE \"{}\" < {} LIMIT {}",
                rule.column, min, limit
            )))
        }
        Check::Max => {
            let max = rule.max.context("Max check requires a max value")?;
            Ok(Some(format!(
                "SELECT * FROM data WHERE \"{}\" > {} LIMIT {}",
                rule.column, max, limit
            )))
        }
        Check::Between => {
            let min = rule.min.context("Between check requires a min value")?;
            let max = rule.max.context("Between check requires a max value")?;
            Ok(Some(format!(
                "SELECT * FROM data WHERE \"{}\" < {} OR \"{}\" > {} LIMIT {}",
                rule.column, min, rule.column, max, limit
            )))
        }
        Check::Unique => Ok(Some(format!(
            "SELECT * FROM data WHERE \"{}\" IN (SELECT \"{}\" FROM data GROUP BY \"{}\" HAVING COUNT(*) > 1) LIMIT {}",
            rule.column, rule.column, rule.column, limit
        ))),
        Check::Regex => {
            let pattern = rule
                .pattern
                .clone()
                .context("Regex check requires a pattern value")?;
            let escaped = pattern.replace('\'', "''");
            Ok(Some(format!(
                "SELECT * FROM data WHERE REGEXP_MATCH(\"{}\", '{}') IS NULL LIMIT {}",
                rule.column, escaped, limit
            )))
        }
        Check::Custom => Ok(None),
    }
}

pub fn validate_rule(rule: &Rule) -> anyhow::Result<()> {
    build_sql(rule)?;
    Ok(())
}

pub fn validate_threshold(rules: &[Rule]) -> anyhow::Result<()> {
    for rule in rules {
        if let Some(t) = rule.threshold {
            if !(0.0..=1.0).contains(&t) {
                anyhow::bail!(
                    "Rule '{}' has an invalid threshold {}: must be between 0.0 and 1.0",
                    rule.name,
                    t
                );
            }
        }
    }
    Ok(())
}

pub async fn run_sql(ctx: &SessionContext, sql: String) -> anyhow::Result<u64> {
    let df = ctx.sql(&sql).await.context("SQL query failed")?;
    let batches = df.collect().await.context("Failed to collect results")?;
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("Expected Int64 column")?
        .value(0) as u64;
    Ok(values)
}

/// Fetch up to `limit` rows that violate the rule, as JSON objects.
/// Returns an empty vec for Custom checks or when there are no violations.
pub async fn fetch_violation_samples(
    ctx: &SessionContext,
    rule: &Rule,
    limit: u64,
) -> anyhow::Result<Vec<JsonValue>> {
    let Some(sql) = build_violation_sample_sql(rule, limit)? else {
        return Ok(vec![]);
    };
    let df = ctx.sql(&sql).await.context("Sample SQL query failed")?;
    let batches = df
        .collect()
        .await
        .context("Failed to collect sample results")?;

    let mut rows: Vec<JsonValue> = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            let mut obj = serde_json::Map::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let field = schema.field(col_idx);
                let col_name = field.name().clone();
                let value: JsonValue = match field.data_type() {
                    DataType::Int8 => {
                        if col.is_null(row_idx) {
                            JsonValue::Null
                        } else {
                            json!(col
                                .as_any()
                                .downcast_ref::<Int8Array>()
                                .unwrap()
                                .value(row_idx))
                        }
                    }
                    DataType::Int16 => {
                        if col.is_null(row_idx) {
                            JsonValue::Null
                        } else {
                            json!(col
                                .as_any()
                                .downcast_ref::<Int16Array>()
                                .unwrap()
                                .value(row_idx))
                        }
                    }
                    DataType::Int32 => {
                        if col.is_null(row_idx) {
                            JsonValue::Null
                        } else {
                            json!(col
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap()
                                .value(row_idx))
                        }
                    }
                    DataType::Int64 => {
                        if col.is_null(row_idx) {
                            JsonValue::Null
                        } else {
                            json!(col
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(row_idx))
                        }
                    }
                    DataType::Float32 => {
                        if col.is_null(row_idx) {
                            JsonValue::Null
                        } else {
                            json!(col
                                .as_any()
                                .downcast_ref::<Float32Array>()
                                .unwrap()
                                .value(row_idx))
                        }
                    }
                    DataType::Float64 => {
                        if col.is_null(row_idx) {
                            JsonValue::Null
                        } else {
                            json!(col
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(row_idx))
                        }
                    }
                    DataType::Utf8 => {
                        if col.is_null(row_idx) {
                            JsonValue::Null
                        } else {
                            json!(col
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(row_idx))
                        }
                    }
                    DataType::Boolean => {
                        if col.is_null(row_idx) {
                            JsonValue::Null
                        } else {
                            json!(col
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .unwrap()
                                .value(row_idx))
                        }
                    }
                    _ => {
                        if col.is_null(row_idx) {
                            JsonValue::Null
                        } else {
                            json!(format!("{:?}", col.slice(row_idx, 1)))
                        }
                    }
                };
                obj.insert(col_name, value);
            }
            rows.push(JsonValue::Object(obj));
        }
    }
    Ok(rows)
}

pub async fn run_rule(
    ctx: &SessionContext,
    rule: &Rule,
    total_rows: u64,
) -> anyhow::Result<RuleResult> {
    let sql = build_sql(rule)?;
    let violations = run_sql(ctx, sql).await?;
    let violation_rate = violations as f64 / total_rows as f64;
    let status = {
        if violation_rate <= rule.threshold.unwrap_or(0.0) {
            RuleStatus::Pass
        } else {
            RuleStatus::Fail
        }
    };
    Ok(RuleResult {
        name: rule.name.clone(),
        status,
        violations,
        total_rows,
        violation_rate,
        sample_rows: None,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    async fn make_ctx(sql: &str) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        ctx
    }

    fn make_rule(name: &str, column: &str, check: Check) -> Rule {
        Rule {
            name: name.to_string(),
            column: column.to_string(),
            check,
            min: None,
            max: None,
            pattern: None,
            threshold: None,
            sql: None,
        }
    }

    #[tokio::test]
    async fn test_not_null_fails_when_nulls_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (NULL, 'carol')) AS t(age, name)").await;
        let rule = make_rule("age_not_null", "age", Check::NotNull);
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 1)
    }

    #[tokio::test]
    async fn test_not_null_pass_when_nulls_not_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (NULL, 'carol')) AS t(age, name)").await;
        let rule = make_rule("name_not_null", "name", Check::NotNull);
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
        assert!(res.violations == 0)
    }

    #[tokio::test]
    async fn test_min_fails_when_smaller_values_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (3, ''), (2, 'bob'), (4, 'carol')) AS t(age, name)").await;
        let rule = Rule {
            min: Some(3.0),
            ..make_rule("age_gt_3", "age", Check::Min)
        };
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 1)
    }

    #[tokio::test]
    async fn test_min_pass_when_smaller_values_not_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (NULL, 'carol')) AS t(age, name)").await;
        let rule = Rule {
            min: Some(1.0),
            ..make_rule("age_gt_1", "age", Check::Min)
        };
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
        assert!(res.violations == 0)
    }

    #[tokio::test]
    async fn test_not_empty_fails_when_empty_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, ''), (2, 'bob'), (NULL, 'carol')) AS t(age, name)").await;
        let rule = make_rule("name_not_empty", "name", Check::NotEmpty);
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 1)
    }

    #[tokio::test]
    async fn test_not_em_pass_when_empty_not_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (NULL, 'carol')) AS t(age, name)").await;
        let rule = make_rule("name_not_empty", "name", Check::NotEmpty);
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
        assert!(res.violations == 0)
    }

    #[tokio::test]
    async fn test_max_fails_when_larger_values_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (3, ''), (2, 'bob'), (4, 'carol')) AS t(age, name)").await;
        let rule = Rule {
            max: Some(2.0),
            ..make_rule("age_st_2", "age", Check::Max)
        };
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 2)
    }

    #[tokio::test]
    async fn test_max_pass_when_larger_values_not_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (NULL, 'carol')) AS t(age, name)").await;
        let rule = Rule {
            max: Some(2.0),
            ..make_rule("age_st_2", "age", Check::Max)
        };
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
        assert!(res.violations == 0)
    }

    #[tokio::test]
    async fn test_between_fails_when_out_of_range() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1), (5), (10)) AS t(age)").await;
        let rule = Rule {
            min: Some(2.0),
            max: Some(8.0),
            ..make_rule("age_between", "age", Check::Between)
        };
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 2); // 1 and 10 are out of range
    }

    #[tokio::test]
    async fn test_between_passes_when_all_in_range() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (3), (5), (7)) AS t(age)").await;
        let rule = Rule {
            min: Some(1.0),
            max: Some(10.0),
            ..make_rule("age_between", "age", Check::Between)
        };
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
        assert_eq!(res.violations, 0);
    }

    #[tokio::test]
    async fn test_unique_fails_when_duplicates_present() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('a'), ('b'), ('a')) AS t(name)")
                .await;
        let rule = make_rule("name_unique", "name", Check::Unique);
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 2); // both 'a' rows are duplicates
    }

    #[tokio::test]
    async fn test_unique_passes_when_all_distinct() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('a'), ('b'), ('c')) AS t(name)")
                .await;
        let rule = make_rule("name_unique", "name", Check::Unique);
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
        assert_eq!(res.violations, 0);
    }

    #[tokio::test]
    async fn test_regex_fails_when_pattern_not_matched() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('foo@bar.com'), ('notanemail')) AS t(email)").await;
        let rule = Rule {
            pattern: Some("^[^@]+@[^@]+$".to_string()),
            ..make_rule("email_regex", "email", Check::Regex)
        };
        let res = run_rule(&ctx, &rule, 2).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 1);
    }

    #[tokio::test]
    async fn test_regex_passes_when_all_match() {
        let ctx = make_ctx(
            "CREATE TABLE data AS SELECT * FROM (VALUES ('foo@bar.com'), ('x@y.com')) AS t(email)",
        )
        .await;
        let rule = Rule {
            pattern: Some("^[^@]+@[^@]+$".to_string()),
            ..make_rule("email_regex", "email", Check::Regex)
        };
        let res = run_rule(&ctx, &rule, 2).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
        assert_eq!(res.violations, 0);
    }

    #[tokio::test]
    async fn test_threshold_allows_tolerance() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1), (2), (NULL)) AS t(age)")
                .await;
        let rule = Rule {
            threshold: Some(0.5), // allow up to 50% nulls — 1/3 = 33% should pass
            ..make_rule("age_not_null", "age", Check::NotNull)
        };
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
    }

    #[tokio::test]
    async fn test_min_without_min_value_returns_error() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1)) AS t(age)").await;
        let rule = make_rule("bad_rule", "age", Check::Min); // no min set
        let res = run_rule(&ctx, &rule, 1).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_max_without_max_value_returns_error() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1)) AS t(age)").await;
        let rule = make_rule("bad_rule", "age", Check::Max); // no max set
        let res = run_rule(&ctx, &rule, 1).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_regex_without_pattern_returns_error() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('hello')) AS t(name)").await;
        let rule = make_rule("bad_rule", "name", Check::Regex); // no pattern set
        let res = run_rule(&ctx, &rule, 1).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_regex_with_single_quote_in_pattern() {
        let ctx = make_ctx(
            "CREATE TABLE data AS SELECT * FROM (VALUES ('it''s valid'), ('nope')) AS t(name)",
        )
        .await;
        let rule = Rule {
            pattern: Some("it's".to_string()),
            ..make_rule("quote_test", "name", Check::Regex)
        };
        let res = run_rule(&ctx, &rule, 2).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 1);
    }

    #[tokio::test]
    async fn test_custom_fails() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (NULL, 'carol')) AS t(age, name)").await;
        let rule = Rule {
            sql: Some("SELECT COUNT(*) FROM data WHERE age IS NULL".into()),
            ..make_rule("age_not_null", "age", Check::Custom)
        };
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 1)
    }

    #[tokio::test]
    async fn test_custom_passes() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')) AS t(age, name)").await;
        let rule = Rule {
            sql: Some("SELECT COUNT(*) FROM data WHERE age IS NULL".into()),
            ..make_rule("age_not_null", "age", Check::Custom)
        };
        let res = run_rule(&ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
        assert_eq!(res.violations, 0)
    }

    #[test]
    fn test_threshold_above_one_is_invalid() {
        let rules = vec![Rule {
            name: "bad".to_string(),
            column: "age".to_string(),
            check: Check::NotNull,
            min: None,
            max: None,
            pattern: None,
            threshold: Some(1.5),
            sql: None,
        }];
        assert!(validate_threshold(&rules).is_err());
    }

    #[test]
    fn test_threshold_at_boundaries_is_valid() {
        let rules = vec![
            Rule {
                name: "a".to_string(),
                column: "x".to_string(),
                check: Check::NotNull,
                min: None,
                max: None,
                pattern: None,
                threshold: Some(0.0),
                sql: None,
            },
            Rule {
                name: "b".to_string(),
                column: "x".to_string(),
                check: Check::NotNull,
                min: None,
                max: None,
                pattern: None,
                threshold: Some(1.0),
                sql: None,
            },
        ];
        assert!(validate_threshold(&rules).is_ok());
    }

    // ------------------------------------------------------------------
    // fetch_violation_samples tests
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_fetch_samples_not_null() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1), (NULL), (3)) AS t(age)")
                .await;
        let rule = make_rule("age_not_null", "age", Check::NotNull);
        let samples = fetch_violation_samples(&ctx, &rule, 5).await.unwrap();
        assert_eq!(samples.len(), 1);
        assert!(samples[0]["age"].is_null());
    }

    #[tokio::test]
    async fn test_fetch_samples_min() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1), (5), (10)) AS t(age)").await;
        let rule = Rule {
            min: Some(3.0),
            ..make_rule("age_min", "age", Check::Min)
        };
        let samples = fetch_violation_samples(&ctx, &rule, 5).await.unwrap();
        assert_eq!(samples.len(), 1);
        // value 1 is below min 3
        assert_eq!(samples[0]["age"], json!(1_i64));
    }

    #[tokio::test]
    async fn test_fetch_samples_max() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1), (5), (10)) AS t(age)").await;
        let rule = Rule {
            max: Some(4.0),
            ..make_rule("age_max", "age", Check::Max)
        };
        let samples = fetch_violation_samples(&ctx, &rule, 5).await.unwrap();
        // values 5 and 10 are above max 4
        assert_eq!(samples.len(), 2);
    }

    #[tokio::test]
    async fn test_fetch_samples_between() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1), (5), (10)) AS t(age)").await;
        let rule = Rule {
            min: Some(2.0),
            max: Some(8.0),
            ..make_rule("age_between", "age", Check::Between)
        };
        let samples = fetch_violation_samples(&ctx, &rule, 5).await.unwrap();
        // values 1 and 10 are out of range
        assert_eq!(samples.len(), 2);
    }

    #[tokio::test]
    async fn test_fetch_samples_not_empty() {
        let ctx = make_ctx(
            "CREATE TABLE data AS SELECT * FROM (VALUES ('hello'), (''), ('world')) AS t(name)",
        )
        .await;
        let rule = make_rule("name_not_empty", "name", Check::NotEmpty);
        let samples = fetch_violation_samples(&ctx, &rule, 5).await.unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0]["name"], json!(""));
    }

    #[tokio::test]
    async fn test_fetch_samples_unique() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('a'), ('b'), ('a')) AS t(name)")
                .await;
        let rule = make_rule("name_unique", "name", Check::Unique);
        let samples = fetch_violation_samples(&ctx, &rule, 5).await.unwrap();
        // both 'a' rows are duplicates
        assert_eq!(samples.len(), 2);
        assert!(samples.iter().all(|r| r["name"] == json!("a")));
    }

    #[tokio::test]
    async fn test_fetch_samples_regex() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('foo@bar.com'), ('notanemail')) AS t(email)").await;
        let rule = Rule {
            pattern: Some("^[^@]+@[^@]+$".to_string()),
            ..make_rule("email_regex", "email", Check::Regex)
        };
        let samples = fetch_violation_samples(&ctx, &rule, 5).await.unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0]["email"], json!("notanemail"));
    }

    #[tokio::test]
    async fn test_fetch_samples_limit() {
        // 10 violating rows, request only 3
        let ctx = make_ctx(
            "CREATE TABLE data AS SELECT * FROM (VALUES \
             (NULL),(NULL),(NULL),(NULL),(NULL),(NULL),(NULL),(NULL),(NULL),(NULL)\
             ) AS t(age)",
        )
        .await;
        let rule = make_rule("age_not_null", "age", Check::NotNull);
        let samples = fetch_violation_samples(&ctx, &rule, 3).await.unwrap();
        assert_eq!(samples.len(), 3);
    }

    #[tokio::test]
    async fn test_fetch_samples_no_violations() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1), (2), (3)) AS t(age)").await;
        let rule = make_rule("age_not_null", "age", Check::NotNull);
        let samples = fetch_violation_samples(&ctx, &rule, 5).await.unwrap();
        assert!(samples.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_samples_custom_returns_empty() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1), (NULL)) AS t(age)").await;
        let rule = Rule {
            sql: Some("SELECT COUNT(*) FROM data WHERE age IS NULL".into()),
            ..make_rule("age_not_null", "age", Check::Custom)
        };
        // Custom check cannot produce sample rows — must return empty vec
        let samples = fetch_violation_samples(&ctx, &rule, 5).await.unwrap();
        assert!(samples.is_empty());
    }
}
