use crate::schema::SchemaOutput;
use serde::Serialize;

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

pub fn format_profile_text(schema: &SchemaOutput, rules: &[SuggestedRule]) -> String {
    let mut out = String::new();

    for col in &schema.columns {
        let null_pct = if schema.row_count > 0 {
            col.nulls as f64 / schema.row_count as f64 * 100.0
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
        if let Some(v) = col.min {
            out.push_str(&format!("  min:         {v}\n"));
        }
        if let Some(v) = col.max {
            out.push_str(&format!("  max:         {v}\n"));
        }
        if let Some(v) = col.mean {
            out.push_str(&format!("  mean:        {v:.2}\n"));
        }
        out.push('\n');
    }

    out.push_str("---\n");
    if rules.is_empty() {
        out.push_str("No rules suggested.\n");
    } else {
        out.push_str(&format!("Suggested rules ({} rows):\n\n", schema.row_count));
        let wrapper = SuggestedRulesFile { rules };
        out.push_str(&serde_yaml::to_string(&wrapper).unwrap_or_default());
    }

    out
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
}
