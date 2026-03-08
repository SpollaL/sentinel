use crate::rules::{Check, Rule};
use datafusion::prelude::*;
use datafusion::arrow::array::Int64Array;

pub struct RuleResult {
    pub name: String,
    pub passed: bool,
    pub violations: u64,
}

fn build_sql(rule: &Rule) -> String {
    match &rule.check {
        Check::NotNull => format!(
            "SELECT COUNT(*) FROM data WHERE \"{}\" IS NULL",
            rule.column
        ),
        Check::Min => {
            let thr = rule.value.expect("Min check requires a value");
            format!(
                "SELECT COUNT(*) FROM data WHERE \"{}\" < {}",
                rule.column, thr
            )
        }
        Check::Max => {
            let thr = rule.value.expect("Min check requires a value");
            format!(
                "SELECT COUNT(*) FROM data WHERE \"{}\" > {}",
                rule.column, thr
            )
        }
    }
}

pub async fn run_rule(ctx: &SessionContext, rule: &Rule) -> RuleResult {
    let sql = build_sql(rule);

    let df = ctx.sql(&sql).await.expect("SQL query failed");
    let batches = df.collect().await.expect("Failed to collect results");
    let violations = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64 column")
        .value(0) as u64;

    RuleResult {
        name: rule.name.clone(),
        passed: violations == 0,
        violations: violations
    }
}
