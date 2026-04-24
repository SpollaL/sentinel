//! Convert Arrow `RecordBatch`es into JSON rows.
//!
//! Used by `runner::fetch_violation_samples` and by agent-facing data-exploration
//! commands (`query`, `head`) to surface rows as JSONL-friendly
//! `serde_json::Value` objects keyed by column name.
//!
//! Explicit nulls are preserved so downstream consumers can rely on every row
//! having every column key.

use anyhow::Context;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::json::writer::JsonArray;
use datafusion::arrow::json::WriterBuilder;
use serde_json::Value as JsonValue;

/// Serialize `batches` to a `Vec<JsonValue>` with one object per row.
///
/// Empty input (or all-empty batches) returns an empty vec without invoking the writer.
pub fn record_batches_to_json_rows(batches: &[RecordBatch]) -> anyhow::Result<Vec<JsonValue>> {
    if batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(vec![]);
    }
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(&mut buf);
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    writer
        .write_batches(&refs)
        .context("Failed to serialize RecordBatches to JSON")?;
    writer.finish().context("Failed to finalize JSON writer")?;
    serde_json::from_slice(&buf).context("Failed to parse arrow-json output into serde_json::Value")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;
    use std::sync::Arc;

    fn make_batch(schema: Schema, columns: Vec<datafusion::arrow::array::ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(schema), columns).expect("valid batch")
    }

    #[test]
    fn empty_batches_produce_empty_vec() {
        let rows = record_batches_to_json_rows(&[]).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn single_batch_yields_object_per_row() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let ids = Int32Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["a", "b", "c"]);
        let batch = make_batch(schema, vec![Arc::new(ids), Arc::new(names)]);

        let rows = record_batches_to_json_rows(&[batch]).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], json!({"id": 1, "name": "a"}));
        assert_eq!(rows[1], json!({"id": 2, "name": "b"}));
        assert_eq!(rows[2], json!({"id": 3, "name": "c"}));
    }

    #[test]
    fn nulls_are_preserved_explicitly() {
        let schema = Schema::new(vec![Field::new("age", DataType::Int32, true)]);
        let ages = Int32Array::from(vec![Some(30), None, Some(25)]);
        let batch = make_batch(schema, vec![Arc::new(ages)]);

        let rows = record_batches_to_json_rows(&[batch]).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["age"], json!(30));
        assert!(rows[1]["age"].is_null());
        assert_eq!(rows[2]["age"], json!(25));
    }

    #[test]
    fn mixed_types_round_trip() {
        let schema = Schema::new(vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new("score", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
        ]);
        let flags = BooleanArray::from(vec![Some(true), Some(false)]);
        let scores = Float64Array::from(vec![Some(1.5), None]);
        let labels = StringArray::from(vec![Some("ok"), Some("")]);
        let batch = make_batch(
            schema,
            vec![Arc::new(flags), Arc::new(scores), Arc::new(labels)],
        );

        let rows = record_batches_to_json_rows(&[batch]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], json!({"flag": true, "score": 1.5, "label": "ok"}));
        assert_eq!(rows[1]["flag"], json!(false));
        assert!(rows[1]["score"].is_null());
        assert_eq!(rows[1]["label"], json!(""));
    }

    #[test]
    fn multiple_batches_concatenate() {
        let schema = Schema::new(vec![Field::new("n", DataType::Int32, false)]);
        let b1 = make_batch(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]);
        let b2 = make_batch(schema, vec![Arc::new(Int32Array::from(vec![3]))]);

        let rows = record_batches_to_json_rows(&[b1, b2]).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["n"], json!(1));
        assert_eq!(rows[2]["n"], json!(3));
    }
}
