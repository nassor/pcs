//! Canonical `Order` component for the polyglot example.
//!
//! This crate is the *single* Rust definition of the schema that all four
//! stages of `examples/polyglot/` agree on. The driver
//! (`crates/pcs-service/examples/polyglot_orders.rs`), the Rust stage
//! (`examples/polyglot/stages/rust-settle/`) and the code generator that feeds
//! the Go/Python/JavaScript stages all consume it, so there is no second
//! definition to drift.
//!
//! # Why every column exists up front
//!
//! The non-Rust stages mutate Arrow IPC bytes in place: they overwrite
//! fixed-width value slots inside the `RecordBatch` body they were handed. A
//! guest that only rewrites bytes cannot *add* a column, so every downstream
//! column is present (zeroed / empty) from the moment the driver seeds the
//! dataset. Field order is load-bearing — it feeds both the schema fingerprint
//! and the buffer walk every hand-rolled codec performs.
//!
//! | # | field         | Arrow type | written by       |
//! |---|---------------|------------|------------------|
//! | 0 | `id`          | `Int64`    | input only       |
//! | 1 | `region`      | `Utf8`     | input only       |
//! | 2 | `currency`    | `Utf8`     | input only       |
//! | 3 | `amount`      | `Float64`  | input only       |
//! | 4 | `valid`       | `Boolean`  | Go stage         |
//! | 5 | `usd_amount`  | `Float64`  | Python stage     |
//! | 6 | `risk_score`  | `Float64`  | JavaScript stage |
//! | 7 | `flagged`     | `Boolean`  | JavaScript stage |
//! | 8 | `settlement`  | `Utf8`     | Rust stage       |
//!
//! `settlement` is the only variable-length *output*, which is why it is
//! assigned to the Rust stage: that stage has the full arrow-rs writer, while
//! the byte-mutating stages can only rewrite fixed-width slots.

#![deny(missing_docs)]

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use pcs_core::component::Component;

/// One order, in the shape every polyglot stage reads and writes.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Order {
    /// Stable row identity. Input only.
    pub id: i64,
    /// Originating region (`emea` / `apac` / `amer`). Input only.
    pub region: String,
    /// ISO currency code of `amount`. Input only.
    pub currency: String,
    /// Order amount in `currency`. Input only.
    pub amount: f64,
    /// `amount > min_amount`. Written by the **Go** stage.
    pub valid: bool,
    /// `amount` converted to USD, or `0.0` when invalid. Written by the
    /// **Python** stage.
    pub usd_amount: f64,
    /// `usd_amount / risk_threshold`. Written by the **JavaScript** stage.
    pub risk_score: f64,
    /// `risk_score >= 1.0`. Written by the **JavaScript** stage.
    pub flagged: bool,
    /// `REJECTED` / `HOLD` / `SETTLED`. Written by the **Rust** stage.
    pub settlement: String,
}

impl Component for Order {
    fn name() -> &'static str {
        "Order"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("valid", DataType::Boolean, false),
            Field::new("usd_amount", DataType::Float64, false),
            Field::new("risk_score", DataType::Float64, false),
            Field::new("flagged", DataType::Boolean, false),
            Field::new("settlement", DataType::Utf8, false),
        ]))
    }
}

impl Order {
    /// Construct an input row with every derived column zeroed.
    fn seed(id: i64, region: &str, currency: &str, amount: f64) -> Self {
        Self {
            id,
            region: region.to_string(),
            currency: currency.to_string(),
            amount,
            valid: false,
            usd_amount: 0.0,
            risk_score: 0.0,
            flagged: false,
            settlement: String::new(),
        }
    }
}

/// The five-row fixture every polyglot verification path uses.
///
/// The driver, the integration test, the emitted `fixture_input.pcs` /
/// `fixture_input.json` pair, and the three native codec test suites all start
/// from these exact rows, so the expected output values documented in
/// `examples/polyglot/README.md` are reproducible everywhere.
pub fn fixture_rows() -> Vec<Order> {
    vec![
        Order::seed(1, "emea", "EUR", 100.0),
        Order::seed(2, "emea", "GBP", -5.0),
        Order::seed(3, "apac", "JPY", 1_000_000.0),
        Order::seed(4, "amer", "USD", 60_000.0),
        Order::seed(5, "emea", "EUR", 0.0),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_field_order_is_the_documented_one() {
        let schema = Order::schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "id",
                "region",
                "currency",
                "amount",
                "valid",
                "usd_amount",
                "risk_score",
                "flagged",
                "settlement",
            ]
        );
        assert!(
            schema.fields().iter().all(|f| !f.is_nullable()),
            "every Order field must be non-nullable: the hand-rolled codecs \
             assume a fixed buffer-slot count per Arrow type"
        );
    }

    #[test]
    fn fixture_rows_seed_only_input_columns() {
        let rows = fixture_rows();
        assert_eq!(rows.len(), 5);
        assert!(rows.iter().all(|r| !r.valid
            && r.usd_amount == 0.0
            && r.risk_score == 0.0
            && !r.flagged
            && r.settlement.is_empty()));
        assert_eq!(
            rows.iter().map(|r| r.amount).collect::<Vec<_>>(),
            [100.0, -5.0, 1_000_000.0, 60_000.0, 0.0]
        );
    }
}
