use crate::rules::{Check, Rule};
use anyhow::Context;
use datafusion::arrow::array::Int64Array;
use datafusion::prelude::*;
use serde::Serialize;
use std::sync::Arc;

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

pub async fn run_rule(
    ctx: Arc<SessionContext>,
    rule: &Rule,
    total_rows: u64,
) -> anyhow::Result<RuleResult> {
    let sql = build_sql(rule)?;
    let violations = run_sql(&ctx, sql).await?;
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
    })
}

/// Run all rules concurrently via `tokio::task::JoinSet` and return results in
/// the same order as the input `rules` slice.
pub async fn run_rules_parallel(
    ctx: Arc<SessionContext>,
    rules: Vec<Rule>,
    total_rows: u64,
) -> anyhow::Result<Vec<RuleResult>> {
    // Build a position map so we can restore original order after concurrent execution.
    let rule_order: std::collections::HashMap<String, usize> = rules
        .iter()
        .enumerate()
        .map(|(i, r)| (r.name.clone(), i))
        .collect();

    let mut set = tokio::task::JoinSet::new();
    for rule in rules {
        let ctx = Arc::clone(&ctx);
        set.spawn(async move {
            let name = rule.name.clone();
            run_rule(ctx, &rule, total_rows)
                .await
                .with_context(|| format!("Rule '{}' failed to execute", name))
        });
    }

    let mut results: Vec<RuleResult> = Vec::new();
    while let Some(res) = set.join_next().await {
        match res {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(e)) => return Err(e),
            Err(join_err) => return Err(anyhow::anyhow!("Task panicked: {}", join_err)),
        }
    }

    // Restore original submission order.
    results.sort_by_key(|r| rule_order.get(&r.name).copied().unwrap_or(usize::MAX));
    Ok(results)
}

#[cfg(test)]
mod test {
    use super::*;

    async fn make_ctx(sql: &str) -> Arc<SessionContext> {
        let ctx = SessionContext::new();
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        Arc::new(ctx)
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
        let res = run_rule(ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 1)
    }

    #[tokio::test]
    async fn test_not_null_pass_when_nulls_not_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (NULL, 'carol')) AS t(age, name)").await;
        let rule = make_rule("name_not_null", "name", Check::NotNull);
        let res = run_rule(ctx, &rule, 3).await.unwrap();
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
        let res = run_rule(ctx, &rule, 3).await.unwrap();
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
        let res = run_rule(ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
        assert!(res.violations == 0)
    }

    #[tokio::test]
    async fn test_not_empty_fails_when_empty_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, ''), (2, 'bob'), (NULL, 'carol')) AS t(age, name)").await;
        let rule = make_rule("name_not_empty", "name", Check::NotEmpty);
        let res = run_rule(ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 1)
    }

    #[tokio::test]
    async fn test_not_em_pass_when_empty_not_present() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob'), (NULL, 'carol')) AS t(age, name)").await;
        let rule = make_rule("name_not_empty", "name", Check::NotEmpty);
        let res = run_rule(ctx, &rule, 3).await.unwrap();
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
        let res = run_rule(ctx, &rule, 3).await.unwrap();
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
        let res = run_rule(ctx, &rule, 3).await.unwrap();
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
        let res = run_rule(ctx, &rule, 3).await.unwrap();
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
        let res = run_rule(ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
        assert_eq!(res.violations, 0);
    }

    #[tokio::test]
    async fn test_unique_fails_when_duplicates_present() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('a'), ('b'), ('a')) AS t(name)")
                .await;
        let rule = make_rule("name_unique", "name", Check::Unique);
        let res = run_rule(ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Fail));
        assert_eq!(res.violations, 2); // both 'a' rows are duplicates
    }

    #[tokio::test]
    async fn test_unique_passes_when_all_distinct() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('a'), ('b'), ('c')) AS t(name)")
                .await;
        let rule = make_rule("name_unique", "name", Check::Unique);
        let res = run_rule(ctx, &rule, 3).await.unwrap();
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
        let res = run_rule(ctx, &rule, 2).await.unwrap();
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
        let res = run_rule(ctx, &rule, 2).await.unwrap();
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
        let res = run_rule(ctx, &rule, 3).await.unwrap();
        assert!(matches!(res.status, RuleStatus::Pass));
    }

    #[tokio::test]
    async fn test_min_without_min_value_returns_error() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1)) AS t(age)").await;
        let rule = make_rule("bad_rule", "age", Check::Min); // no min set
        let res = run_rule(ctx, &rule, 1).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_max_without_max_value_returns_error() {
        let ctx = make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1)) AS t(age)").await;
        let rule = make_rule("bad_rule", "age", Check::Max); // no max set
        let res = run_rule(ctx, &rule, 1).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_regex_without_pattern_returns_error() {
        let ctx =
            make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES ('hello')) AS t(name)").await;
        let rule = make_rule("bad_rule", "name", Check::Regex); // no pattern set
        let res = run_rule(ctx, &rule, 1).await;
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
        let res = run_rule(ctx, &rule, 2).await.unwrap();
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
        let res = run_rule(ctx, &rule, 3).await.unwrap();
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
        let res = run_rule(ctx, &rule, 3).await.unwrap();
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

    // ── Parallel execution tests ──────────────────────────────────────────────

    /// Create a shared in-memory context with a simple numeric table.
    async fn make_numeric_ctx() -> Arc<SessionContext> {
        make_ctx("CREATE TABLE data AS SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(age)")
            .await
    }

    #[tokio::test]
    async fn test_parallel_execution_all_pass() {
        let ctx = make_numeric_ctx().await;
        // 5 rules that all pass (age >= 1, age <= 10, etc.)
        let rules: Vec<Rule> = (0..5)
            .map(|i| Rule {
                max: Some(10.0),
                ..make_rule(&format!("rule_{}", i), "age", Check::Max)
            })
            .collect();
        let total_rows = 5;
        let results = run_rules_parallel(ctx, rules.clone(), total_rows)
            .await
            .unwrap();
        assert_eq!(results.len(), 5);
        for res in &results {
            assert!(matches!(res.status, RuleStatus::Pass));
            assert_eq!(res.violations, 0);
        }
        // Verify ordering matches input order
        for (i, res) in results.iter().enumerate() {
            assert_eq!(res.name, format!("rule_{}", i));
        }
    }

    #[tokio::test]
    async fn test_parallel_execution_with_failure() {
        let ctx = make_numeric_ctx().await;
        // Mix: rule_0 passes (max=10), rule_1 fails (max=2, values 3/4/5 violate), rule_2 passes
        let rules = vec![
            Rule {
                max: Some(10.0),
                ..make_rule("pass_rule", "age", Check::Max)
            },
            Rule {
                max: Some(2.0),
                ..make_rule("fail_rule", "age", Check::Max)
            },
            Rule {
                min: Some(1.0),
                ..make_rule("pass_rule_2", "age", Check::Min)
            },
        ];
        let results = run_rules_parallel(ctx, rules, 5).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(matches!(results[0].status, RuleStatus::Pass));
        assert!(matches!(results[1].status, RuleStatus::Fail));
        assert_eq!(results[1].violations, 3); // values 3, 4, 5 exceed max=2
        assert!(matches!(results[2].status, RuleStatus::Pass));
        // Names preserved in order
        assert_eq!(results[0].name, "pass_rule");
        assert_eq!(results[1].name, "fail_rule");
        assert_eq!(results[2].name, "pass_rule_2");
    }

    #[tokio::test]
    async fn test_parallel_execution_preserves_order() {
        let ctx = make_numeric_ctx().await;
        // Submit 10 rules; completion order is non-deterministic, but output must match input order.
        let rules: Vec<Rule> = (0..10)
            .map(|i| Rule {
                max: Some(100.0),
                ..make_rule(&format!("ordered_rule_{:02}", i), "age", Check::Max)
            })
            .collect();
        let expected_names: Vec<String> = rules.iter().map(|r| r.name.clone()).collect();
        let results = run_rules_parallel(ctx, rules, 5).await.unwrap();
        assert_eq!(results.len(), 10);
        let actual_names: Vec<String> = results.iter().map(|r| r.name.clone()).collect();
        assert_eq!(actual_names, expected_names);
    }
}
