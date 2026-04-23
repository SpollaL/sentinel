use comfy_table::Table;
use serde::Deserialize;

use crate::runner::{RuleResult, RuleStatus};

#[derive(Debug, Deserialize, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Json,
    Table,
}

pub fn format_results(results: &[RuleResult], format: &OutputFormat) -> String {
    let passed = results
        .iter()
        .filter(|r| matches!(r.status, RuleStatus::Pass))
        .count();
    let failed = results.len() - passed;
    let summary = format!(
        "{} passed, {} failed out of {}",
        passed,
        failed,
        results.len()
    );
    match format {
        OutputFormat::Json => {
            let mut out = build_json(results);
            out.push_str(&format!("// {}\n", summary));
            out
        }
        OutputFormat::Table => {
            format!("{}\n{}", build_table(results), summary)
        }
    }
}

pub fn build_json(results: &[RuleResult]) -> String {
    let mut out: String = String::new();
    results.iter().for_each(|res| {
        if let Ok(line) = serde_json::to_string(res) {
            out.push_str(&line);
            out.push('\n');
        }
    });
    out
}

pub fn build_table(results: &[RuleResult]) -> String {
    let has_samples = results
        .iter()
        .any(|r| r.sample_rows.as_ref().is_some_and(|s| !s.is_empty()));

    let mut table = Table::new();
    if has_samples {
        table.set_header([
            "RULE",
            "STATUS",
            "VIOLATIONS",
            "TOTAL",
            "RATE",
            "SAMPLE VIOLATIONS",
        ]);
    } else {
        table.set_header(["RULE", "STATUS", "VIOLATIONS", "TOTAL", "RATE"]);
    }
    results.iter().for_each(|res| {
        if has_samples {
            let sample_str = match &res.sample_rows {
                Some(rows) if !rows.is_empty() => {
                    let parts: Vec<String> = rows
                        .iter()
                        .map(|row| {
                            // Render each row as compact JSON; truncate long values
                            let s = serde_json::to_string(row).unwrap_or_default();
                            if s.len() > 60 {
                                format!("{}…", &s[..60])
                            } else {
                                s
                            }
                        })
                        .collect();
                    let joined = parts.join(", ");
                    if joined.len() > 120 {
                        format!("{}…", &joined[..120])
                    } else {
                        joined
                    }
                }
                _ => String::new(),
            };
            table.add_row([
                res.name.clone(),
                format!("{}", res.status),
                res.violations.to_string(),
                res.total_rows.to_string(),
                format!("{:.1}%", res.violation_rate * 100.0),
                sample_str,
            ]);
        } else {
            table.add_row([
                res.name.clone(),
                format!("{}", res.status),
                res.violations.to_string(),
                res.total_rows.to_string(),
                format!("{:.1}%", res.violation_rate * 100.0),
            ]);
        }
    });
    table.to_string()
}
