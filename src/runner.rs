use crate::rules::{Check, Rule};
use datafusion::arrow::array::Int64Array;
use datafusion::prelude::*;

pub struct RuleResult {
    pub name: String,
    pub passed: bool,
    pub violations: u64,
}

fn build_sql(rule: &Rule) -> Result<String, String> {
    match &rule.check {
        Check::NotNull => Ok(format!(
            "SELECT COUNT(*) FROM data WHERE \"{}\" IS NULL",
            rule.column
        )),
        Check::Min => {
            let thr = rule.min.ok_or("Min check requires a min value")?;
            Ok(format!(
                "SELECT COUNT(*) FROM data WHERE \"{}\" < {}",
                rule.column, thr
            ))
        }
        Check::Max => {
            let thr = rule.max.ok_or("Max check requires a max value")?;
            Ok(format!(
                "SELECT COUNT(*) FROM data WHERE \"{}\" > {}",
                rule.column, thr
            ))
        }
        Check::NotEmpty => {
            Ok(format!("SELECT COUNT(*) FROM data WHERE \"{}\" = ''", rule.column))
        }
        Check::Between => {
            let min = rule.min.ok_or("Between check requires a min value")?;
            let max = rule.max.ok_or("Between check requires a max value")?;
            Ok(format!(
                "SELECT COUNT(*) FROM data WHERE \"{}\" < {} OR \"{}\" > {}",
                rule.column, min, rule.column, max
            ))
        }
        Check::Unique => {
            Ok(format!(
                "SELECT COALESCE(SUM(cnt), 0) FROM (SELECT COUNT(\"{}\") AS cnt FROM data GROUP BY \"{}\" HAVING COUNT(\"{}\")>1)",
                rule.column, rule.column, rule.column
            ))
        }
        Check::Regex => {
            let pattern = rule
                .pattern
                .clone()
                .ok_or("Regex check requires a pattern value")?;
            Ok(format!(
                "SELECT COUNT(*) FROM data WHERE REGEXP_MATCH(\"{}\", '{}') IS NULL",
                rule.column, pattern
            ))
        }
    }
}

pub async fn run_rule(ctx: &SessionContext, rule: &Rule) -> Result<RuleResult, String> {
    let sql = build_sql(rule)?;

    let df = ctx.sql(&sql).await.expect("SQL query failed");
    let batches = df.collect().await.expect("Failed to collect results");
    let violations = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64 column")
        .value(0) as u64;

    Ok(RuleResult {
        name: rule.name.clone(),
        passed: violations == 0,
        violations: violations,
    })
}
