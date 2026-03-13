use crate::rules::{Check, Rule};
use anyhow::Context;
use datafusion::arrow::array::Int64Array;
use datafusion::prelude::*;
use serde::Serialize;

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
            Ok(format!(
                "SELECT COUNT(*) FROM data WHERE REGEXP_MATCH(\"{}\", '{}') IS NULL",
                rule.column, pattern
            ))
        }
    }
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
        status: status,
        violations: violations,
        total_rows: total_rows,
        violation_rate: violation_rate,
    })
}
