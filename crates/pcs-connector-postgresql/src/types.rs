//! Which PostgreSQL types each declared Arrow type accepts, and the load-time
//! check that enforces it.
//!
//! The connector never coerces: a declared `int32` fed by an `int8` column is a
//! configuration error, not a narrowing cast. [`validate_columns`] runs once per
//! connection against the real catalog — the statement's result columns for the
//! source, `pg_attribute` for the sink — and reports every mismatch at once so
//! one restart fixes the whole config.

use pcs_core::error::PcsError;
use tokio_postgres::types::Type;

use crate::config::{FieldSpec, PgFieldType};

const OID_BOOL: u32 = 16;
const OID_BYTEA: u32 = 17;
const OID_NAME: u32 = 19;
const OID_INT8: u32 = 20;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_TEXT: u32 = 25;
const OID_JSON: u32 = 114;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_BPCHAR: u32 = 1042;
const OID_VARCHAR: u32 = 1043;
const OID_DATE: u32 = 1082;
const OID_TIME: u32 = 1083;
const OID_TIMESTAMP: u32 = 1114;
const OID_TIMESTAMPTZ: u32 = 1184;
const OID_NUMERIC: u32 = 1700;
const OID_UUID: u32 = 2950;
pub(crate) const OID_JSONB: u32 = 3802;

/// Whether a column of PostgreSQL type `oid` can fill a declared `ty`.
pub(crate) fn accepts(ty: PgFieldType, oid: u32) -> bool {
    match ty {
        PgFieldType::Boolean => oid == OID_BOOL,
        PgFieldType::Int16 => oid == OID_INT2,
        PgFieldType::Int32 => oid == OID_INT4,
        PgFieldType::Int64 => oid == OID_INT8,
        PgFieldType::Float32 => oid == OID_FLOAT4,
        PgFieldType::Float64 => oid == OID_FLOAT8,
        PgFieldType::Utf8 => {
            matches!(oid, OID_TEXT | OID_VARCHAR | OID_BPCHAR | OID_NAME)
        }
        PgFieldType::Binary => oid == OID_BYTEA,
        PgFieldType::Date32 => oid == OID_DATE,
        PgFieldType::Time64Micros => oid == OID_TIME,
        PgFieldType::TimestampMicros => oid == OID_TIMESTAMP,
        PgFieldType::TimestampMicrosUtc => oid == OID_TIMESTAMPTZ,
        PgFieldType::Uuid => oid == OID_UUID,
        PgFieldType::Json => matches!(oid, OID_JSON | OID_JSONB),
        PgFieldType::Decimal128 => oid == OID_NUMERIC,
    }
}

/// The server-side name of a type OID, for error messages.
///
/// `Type::from_oid` covers the built-ins; anything else (a domain, an enum, a
/// composite) has no static name here and is reported by OID alone.
pub(crate) fn type_name(oid: u32) -> String {
    match Type::from_oid(oid) {
        Some(ty) => ty.name().to_string(),
        None => format!("oid {oid}"),
    }
}

/// Reject a declared schema the server's actual column types cannot fill.
///
/// Matches by name, never by position, and collects every problem before
/// returning so a misconfigured schema is fixed in one pass. Columns the server
/// has but the config does not declare are ignored.
///
/// # Errors
///
/// Returns [`PcsError::Configuration`] listing every declared field that is
/// absent from `actual` or whose type `actual` cannot fill.
pub(crate) fn validate_columns(
    what: &str,
    declared: &[FieldSpec],
    actual: &[(String, u32)],
) -> Result<(), PcsError> {
    let mut problems: Vec<String> = Vec::new();

    for spec in declared {
        let Some((_, oid)) = actual.iter().find(|(name, _)| *name == spec.name) else {
            problems.push(format!(
                "column '{}' is declared but the server has no such column (server columns: {})",
                spec.name,
                if actual.is_empty() {
                    "none".to_string()
                } else {
                    actual
                        .iter()
                        .map(|(name, _)| name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            ));
            continue;
        };

        if accepts(spec.ty, *oid) {
            continue;
        }

        let hint = if *oid == OID_NUMERIC {
            "; declare type = \"decimal128\" with explicit 'precision' and 'scale', or select the \
             column with a ::text cast and declare type = \"utf8\""
        } else {
            ""
        };
        problems.push(format!(
            "column '{}' is declared type \"{}\" but the server type is {} (oid {}){hint}",
            spec.name,
            spec.ty.as_str(),
            type_name(*oid),
            oid
        ));
    }

    if problems.is_empty() {
        return Ok(());
    }
    Err(PcsError::configuration(format!(
        "{what}: declared schema does not match the server: {}",
        problems.join("; ")
    )))
}

/// The declared columns of `statement`, as `(name, oid)` pairs.
pub(crate) fn statement_columns(statement: &tokio_postgres::Statement) -> Vec<(String, u32)> {
    statement
        .columns()
        .iter()
        .map(|column| (column.name().to_string(), column.type_().oid()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PgFieldType;

    fn spec(name: &str, ty: PgFieldType) -> FieldSpec {
        FieldSpec {
            name: name.to_string(),
            ty,
            nullable: true,
            precision: if ty == PgFieldType::Decimal128 {
                Some(18)
            } else {
                None
            },
            scale: if ty == PgFieldType::Decimal128 {
                Some(4)
            } else {
                None
            },
        }
    }

    #[test]
    fn every_declared_type_accepts_its_postgres_types() {
        assert!(accepts(PgFieldType::Boolean, OID_BOOL));
        assert!(accepts(PgFieldType::Int16, OID_INT2));
        assert!(accepts(PgFieldType::Int32, OID_INT4));
        assert!(accepts(PgFieldType::Int64, OID_INT8));
        assert!(accepts(PgFieldType::Float32, OID_FLOAT4));
        assert!(accepts(PgFieldType::Float64, OID_FLOAT8));
        for oid in [OID_TEXT, OID_VARCHAR, OID_BPCHAR, OID_NAME] {
            assert!(accepts(PgFieldType::Utf8, oid), "utf8 should accept {oid}");
        }
        assert!(accepts(PgFieldType::Binary, OID_BYTEA));
        assert!(accepts(PgFieldType::Date32, OID_DATE));
        assert!(accepts(PgFieldType::Time64Micros, OID_TIME));
        assert!(accepts(PgFieldType::TimestampMicros, OID_TIMESTAMP));
        assert!(accepts(PgFieldType::TimestampMicrosUtc, OID_TIMESTAMPTZ));
        assert!(accepts(PgFieldType::Uuid, OID_UUID));
        assert!(accepts(PgFieldType::Json, OID_JSON));
        assert!(accepts(PgFieldType::Json, OID_JSONB));
        assert!(accepts(PgFieldType::Decimal128, OID_NUMERIC));
    }

    #[test]
    fn integer_widths_are_not_interchangeable() {
        assert!(!accepts(PgFieldType::Int32, OID_INT8));
        assert!(!accepts(PgFieldType::Int64, OID_INT4));
        assert!(!accepts(PgFieldType::TimestampMicros, OID_TIMESTAMPTZ));
        assert!(!accepts(PgFieldType::TimestampMicrosUtc, OID_TIMESTAMP));
    }

    #[test]
    fn missing_and_mismatched_columns_are_reported_together() {
        let declared = [
            spec("id", PgFieldType::Int64),
            spec("label", PgFieldType::Int32),
            spec("gone", PgFieldType::Utf8),
        ];
        let actual = vec![
            ("id".to_string(), OID_INT8),
            ("label".to_string(), OID_TEXT),
        ];
        let err = validate_columns("PostgresSource", &declared, &actual).unwrap_err();
        assert_eq!(err.category(), "configuration");
        let message = err.message();
        assert!(message.contains("'gone'"), "{message}");
        assert!(message.contains("'label'"), "{message}");
        assert!(message.contains("text"), "{message}");
        assert!(!message.contains("'id'"), "{message}");
    }

    #[test]
    fn numeric_into_a_non_decimal_field_suggests_the_alternatives() {
        let declared = [spec("amount", PgFieldType::Float64)];
        let actual = vec![("amount".to_string(), OID_NUMERIC)];
        let err = validate_columns("PostgresSink", &declared, &actual).unwrap_err();
        let message = err.message();
        assert!(message.contains("decimal128"), "{message}");
        assert!(message.contains("::text"), "{message}");
        assert!(message.contains("numeric"), "{message}");
    }

    #[test]
    fn undeclared_server_columns_are_ignored() {
        let declared = [spec("id", PgFieldType::Int64)];
        let actual = vec![
            ("id".to_string(), OID_INT8),
            ("extra".to_string(), OID_TEXT),
        ];
        validate_columns("PostgresSource", &declared, &actual).unwrap();
    }
}
