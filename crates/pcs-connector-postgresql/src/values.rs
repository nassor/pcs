//! PostgreSQL binary values into Arrow arrays.
//!
//! One decoder serves all three source modes. `tokio-postgres` requests binary
//! result formats unconditionally, and `pgoutput` with `binary = 'true'` emits
//! tuple columns through the same type-send functions, so a query result column
//! and a logical-decoding `'b'` tuple value are byte-identical. [`RawValue`] is
//! the hook that gets at those bytes: a `FromSql` newtype that accepts every
//! type and hands back the slice unchanged.
//!
//! Read columns as `Option<RawValue>` so `Option`'s blanket `FromSql` handles
//! SQL NULL; [`RawValue`] itself never implements `from_sql_null`.
//!
//! Every integer on the wire is big-endian. PostgreSQL's date and timestamp
//! epoch is 2000-01-01, Arrow's is 1970-01-01, so both gain a fixed offset. The
//! `±infinity` sentinels have no Arrow representation and are rejected rather
//! than folded into a real timestamp.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use pcs_core::error::PcsError;
use tokio_postgres::types::{FromSql, Type};

use crate::config::{FieldSpec, PgFieldType};
use crate::numeric::numeric_to_i128;
use crate::types::OID_JSONB;

/// Days between the Arrow `Date32` epoch (1970-01-01) and PostgreSQL's
/// (2000-01-01).
pub(crate) const DATE_EPOCH_OFFSET_DAYS: i32 = 10_957;

/// Microseconds between the Arrow timestamp epoch (1970-01-01T00:00:00Z) and
/// PostgreSQL's (2000-01-01T00:00:00Z).
pub(crate) const TIMESTAMP_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// A column value as the server sent it, with no interpretation.
///
/// Accepts every type so one read path covers every column, which is what lets
/// the pgoutput decoder share this module with the query path.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RawValue<'a>(pub(crate) &'a [u8]);

impl<'a> FromSql<'a> for RawValue<'a> {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RawValue(raw))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

/// One Arrow builder per declared column, dispatched once at construction.
pub(crate) enum ColumnBuilder {
    /// `bool`.
    Boolean(BooleanBuilder),
    /// `int2`.
    Int16(Int16Builder),
    /// `int4`.
    Int32(Int32Builder),
    /// `int8`.
    Int64(Int64Builder),
    /// `float4`.
    Float32(Float32Builder),
    /// `float8`.
    Float64(Float64Builder),
    /// `text`, `varchar`, `bpchar`, `name`.
    Utf8(StringBuilder),
    /// `json`, or `jsonb` when the flag is set. `jsonb` prefixes the document
    /// with a version byte that `json` does not carry, and the two share an
    /// OID-indistinguishable Arrow type, so the flag comes from the server's
    /// column type rather than from a guess about the payload.
    Json(StringBuilder, bool),
    /// `bytea`.
    Binary(BinaryBuilder),
    /// `date`.
    Date32(Date32Builder),
    /// `time`.
    Time64(Time64MicrosecondBuilder),
    /// `timestamp` and `timestamptz`; the timezone lives in the data type.
    Timestamp(TimestampMicrosecondBuilder),
    /// `uuid`.
    Uuid(FixedSizeBinaryBuilder),
    /// `numeric`, rescaled to the declared scale.
    Decimal128(Decimal128Builder, i8),
}

impl ColumnBuilder {
    /// A builder for `spec`, sized for `capacity` rows.
    ///
    /// `server_oid` is the PostgreSQL type OID of the column this builder is
    /// filled from, already checked by
    /// [`types::validate_columns`](crate::types::validate_columns). Pass 0 for
    /// the reserved metadata columns, which no server column backs.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] when `spec` declares an illegal
    /// `decimal128` precision or scale.
    pub(crate) fn new(
        spec: &FieldSpec,
        capacity: usize,
        server_oid: u32,
    ) -> Result<Self, PcsError> {
        Ok(match spec.ty {
            PgFieldType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            PgFieldType::Int16 => ColumnBuilder::Int16(Int16Builder::with_capacity(capacity)),
            PgFieldType::Int32 => ColumnBuilder::Int32(Int32Builder::with_capacity(capacity)),
            PgFieldType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            PgFieldType::Float32 => ColumnBuilder::Float32(Float32Builder::with_capacity(capacity)),
            PgFieldType::Float64 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            // 16 bytes per value is a starting guess for the data buffer; the
            // builder grows it as needed.
            PgFieldType::Utf8 => {
                ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 16))
            }
            PgFieldType::Json => ColumnBuilder::Json(
                StringBuilder::with_capacity(capacity, capacity * 16),
                server_oid == OID_JSONB,
            ),
            PgFieldType::Binary => {
                ColumnBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 16))
            }
            PgFieldType::Date32 => ColumnBuilder::Date32(Date32Builder::with_capacity(capacity)),
            PgFieldType::Time64Micros => {
                ColumnBuilder::Time64(Time64MicrosecondBuilder::with_capacity(capacity))
            }
            PgFieldType::TimestampMicros => {
                ColumnBuilder::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            PgFieldType::TimestampMicrosUtc => ColumnBuilder::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone("UTC"),
            ),
            PgFieldType::Uuid => {
                ColumnBuilder::Uuid(FixedSizeBinaryBuilder::with_capacity(capacity, 16))
            }
            PgFieldType::Decimal128 => {
                let (precision, scale) = spec.decimal_params()?;
                ColumnBuilder::Decimal128(
                    Decimal128Builder::with_capacity(capacity)
                        .with_precision_and_scale(precision, scale)
                        .map_err(|e| {
                            PcsError::configuration(format!(
                                "field '{}': invalid decimal128 precision/scale: {e}",
                                spec.name
                            ))
                        })?,
                    scale,
                )
            }
        })
    }

    /// Decode one PostgreSQL binary value, or append NULL for `None`.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Generic`] naming `column` when the value has the
    /// wrong length, holds an out-of-range discriminant, is not valid UTF-8, or
    /// is one of the temporal infinity sentinels.
    pub(crate) fn push(&mut self, column: &str, raw: Option<&[u8]>) -> Result<(), PcsError> {
        let Some(raw) = raw else {
            self.push_null();
            return Ok(());
        };

        match self {
            ColumnBuilder::Boolean(builder) => {
                let byte = one(column, raw, "bool")?;
                match byte {
                    0 => builder.append_value(false),
                    1 => builder.append_value(true),
                    other => {
                        return Err(PcsError::generic(format!(
                            "column '{column}': bool value is {other}, expected 0 or 1"
                        )));
                    }
                }
            }
            ColumnBuilder::Int16(builder) => {
                builder.append_value(i16::from_be_bytes(fixed::<2>(column, raw, "int2")?));
            }
            ColumnBuilder::Int32(builder) => {
                builder.append_value(i32::from_be_bytes(fixed::<4>(column, raw, "int4")?));
            }
            ColumnBuilder::Int64(builder) => {
                builder.append_value(i64::from_be_bytes(fixed::<8>(column, raw, "int8")?));
            }
            ColumnBuilder::Float32(builder) => {
                builder.append_value(f32::from_bits(u32::from_be_bytes(fixed::<4>(
                    column, raw, "float4",
                )?)));
            }
            ColumnBuilder::Float64(builder) => {
                builder.append_value(f64::from_bits(u64::from_be_bytes(fixed::<8>(
                    column, raw, "float8",
                )?)));
            }
            ColumnBuilder::Utf8(builder) => builder.append_value(utf8(column, raw)?),
            ColumnBuilder::Json(builder, jsonb) => {
                // `jsonb` on the wire is a one-byte version followed by the
                // document; `json` is the document alone. Version 1 is the only
                // one PostgreSQL has ever emitted, and a later version would
                // have a layout this decoder does not know.
                let text = if *jsonb {
                    match raw.split_first() {
                        Some((1, rest)) => utf8(column, rest)?,
                        Some((version, _)) => {
                            return Err(PcsError::generic(format!(
                                "column '{column}': jsonb version byte is {version}, expected 1"
                            )));
                        }
                        None => {
                            return Err(PcsError::generic(format!(
                                "column '{column}': jsonb value is empty, expected at least a \
                                 version byte"
                            )));
                        }
                    }
                } else {
                    utf8(column, raw)?
                };
                builder.append_value(text);
            }
            ColumnBuilder::Binary(builder) => builder.append_value(raw),
            ColumnBuilder::Date32(builder) => {
                let days = i32::from_be_bytes(fixed::<4>(column, raw, "date")?);
                if days == i32::MIN || days == i32::MAX {
                    return Err(PcsError::generic(format!(
                        "column '{column}': date is {}infinity, which has no Date32 \
                         representation; exclude the row or select the column with a ::text cast",
                        if days == i32::MIN { "-" } else { "" }
                    )));
                }
                builder.append_value(days.checked_add(DATE_EPOCH_OFFSET_DAYS).ok_or_else(
                    || {
                        PcsError::generic(format!(
                            "column '{column}': date {days} overflows Date32 after rebasing to the \
                         1970-01-01 epoch"
                        ))
                    },
                )?);
            }
            ColumnBuilder::Time64(builder) => {
                builder.append_value(i64::from_be_bytes(fixed::<8>(column, raw, "time")?));
            }
            ColumnBuilder::Timestamp(builder) => {
                let micros = i64::from_be_bytes(fixed::<8>(column, raw, "timestamp")?);
                if micros == i64::MIN || micros == i64::MAX {
                    return Err(PcsError::generic(format!(
                        "column '{column}': timestamp is {}infinity, which has no Arrow \
                         representation; exclude the row or select the column with a ::text cast",
                        if micros == i64::MIN { "-" } else { "" }
                    )));
                }
                builder.append_value(
                    micros
                        .checked_add(TIMESTAMP_EPOCH_OFFSET_MICROS)
                        .ok_or_else(|| {
                            PcsError::generic(format!(
                                "column '{column}': timestamp {micros} overflows an i64 after \
                                 rebasing to the 1970-01-01 epoch"
                            ))
                        })?,
                );
            }
            ColumnBuilder::Uuid(builder) => {
                let bytes = fixed::<16>(column, raw, "uuid")?;
                builder.append_value(bytes).map_err(|e| {
                    PcsError::generic(format!("column '{column}': cannot append uuid: {e}"))
                })?;
            }
            ColumnBuilder::Decimal128(builder, scale) => {
                builder.append_value(numeric_to_i128(raw, *scale, column)?);
            }
        }
        Ok(())
    }

    /// Append NULL.
    ///
    /// Used for pgoutput's `'n'` and `'u'` tuple markers, and for the columns a
    /// `DELETE` old tuple does not carry.
    pub(crate) fn push_null(&mut self) {
        match self {
            ColumnBuilder::Boolean(b) => b.append_null(),
            ColumnBuilder::Int16(b) => b.append_null(),
            ColumnBuilder::Int32(b) => b.append_null(),
            ColumnBuilder::Int64(b) => b.append_null(),
            ColumnBuilder::Float32(b) => b.append_null(),
            ColumnBuilder::Float64(b) => b.append_null(),
            ColumnBuilder::Utf8(b) | ColumnBuilder::Json(b, _) => b.append_null(),
            ColumnBuilder::Binary(b) => b.append_null(),
            ColumnBuilder::Date32(b) => b.append_null(),
            ColumnBuilder::Time64(b) => b.append_null(),
            ColumnBuilder::Timestamp(b) => b.append_null(),
            ColumnBuilder::Uuid(b) => b.append_null(),
            ColumnBuilder::Decimal128(b, _) => b.append_null(),
        }
    }

    /// Append a `&str`, for the reserved metadata columns the connector fills
    /// itself rather than reading from a tuple.
    pub(crate) fn push_str(&mut self, column: &str, value: &str) -> Result<(), PcsError> {
        match self {
            ColumnBuilder::Utf8(b) | ColumnBuilder::Json(b, _) => {
                b.append_value(value);
                Ok(())
            }
            _ => Err(PcsError::generic(format!(
                "column '{column}': expected a utf8 builder for a connector-supplied string"
            ))),
        }
    }

    /// Append an `i64`, for the reserved metadata columns.
    pub(crate) fn push_i64(&mut self, column: &str, value: i64) -> Result<(), PcsError> {
        match self {
            ColumnBuilder::Int64(b) => {
                b.append_value(value);
                Ok(())
            }
            ColumnBuilder::Timestamp(b) => {
                b.append_value(value);
                Ok(())
            }
            _ => Err(PcsError::generic(format!(
                "column '{column}': expected an int64 or timestamp builder for a \
                 connector-supplied integer"
            ))),
        }
    }

    /// Finish the builder, resetting it for the next batch.
    pub(crate) fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Boolean(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Int16(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Int32(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Int64(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Float32(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Float64(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Utf8(b) | ColumnBuilder::Json(b, _) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Binary(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Date32(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Time64(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Timestamp(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Uuid(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Decimal128(b, _) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

/// Read a fixed-width value, naming the column and both lengths on mismatch.
fn fixed<const N: usize>(column: &str, raw: &[u8], what: &str) -> Result<[u8; N], PcsError> {
    raw.try_into().map_err(|_| {
        PcsError::generic(format!(
            "column '{column}': {what} value is {} byte(s), expected {N}",
            raw.len()
        ))
    })
}

fn one(column: &str, raw: &[u8], what: &str) -> Result<u8, PcsError> {
    Ok(fixed::<1>(column, raw, what)?[0])
}

fn utf8<'a>(column: &str, raw: &'a [u8]) -> Result<&'a str, PcsError> {
    std::str::from_utf8(raw)
        .map_err(|e| PcsError::generic(format!("column '{column}': value is not valid UTF-8: {e}")))
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float64Array, Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    };

    use super::*;
    use crate::config::FieldSpec;

    fn spec(ty: PgFieldType) -> FieldSpec {
        FieldSpec {
            name: "c".to_string(),
            ty,
            nullable: true,
            precision: matches!(ty, PgFieldType::Decimal128).then_some(18),
            scale: matches!(ty, PgFieldType::Decimal128).then_some(4),
        }
    }

    /// The PostgreSQL type OID a declared type is normally fed from. Only the
    /// `json`/`jsonb` split actually changes decoding.
    fn oid(ty: PgFieldType) -> u32 {
        match ty {
            PgFieldType::Boolean => Type::BOOL.oid(),
            PgFieldType::Int16 => Type::INT2.oid(),
            PgFieldType::Int32 => Type::INT4.oid(),
            PgFieldType::Int64 => Type::INT8.oid(),
            PgFieldType::Float32 => Type::FLOAT4.oid(),
            PgFieldType::Float64 => Type::FLOAT8.oid(),
            PgFieldType::Utf8 => Type::TEXT.oid(),
            PgFieldType::Binary => Type::BYTEA.oid(),
            PgFieldType::Date32 => Type::DATE.oid(),
            PgFieldType::Time64Micros => Type::TIME.oid(),
            PgFieldType::TimestampMicros => Type::TIMESTAMP.oid(),
            PgFieldType::TimestampMicrosUtc => Type::TIMESTAMPTZ.oid(),
            PgFieldType::Uuid => Type::UUID.oid(),
            PgFieldType::Json => Type::JSON.oid(),
            PgFieldType::Decimal128 => Type::NUMERIC.oid(),
        }
    }

    fn builder_for(ty: PgFieldType, capacity: usize) -> ColumnBuilder {
        ColumnBuilder::new(&spec(ty), capacity, oid(ty)).expect("builder")
    }

    fn build(ty: PgFieldType, values: &[Option<&[u8]>]) -> ArrayRef {
        let mut builder = builder_for(ty, values.len());
        for value in values {
            builder.push("c", *value).expect("push");
        }
        builder.finish()
    }

    #[test]
    fn decodes_integers_and_floats() {
        let array = build(PgFieldType::Int64, &[Some(&7i64.to_be_bytes()), None]);
        let ints = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 7);
        assert!(ints.is_null(1));

        let array = build(
            PgFieldType::Float64,
            &[Some(&2.5f64.to_bits().to_be_bytes())],
        );
        let floats = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(floats.value(0), 2.5);
    }

    #[test]
    fn decodes_booleans_and_rejects_other_bytes() {
        let array = build(PgFieldType::Boolean, &[Some(&[1]), Some(&[0])]);
        let bools = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.value(0));
        assert!(!bools.value(1));

        let mut builder = builder_for(PgFieldType::Boolean, 1);
        let err = builder.push("flag", Some(&[2])).unwrap_err();
        assert!(err.message().contains("flag"), "{}", err.message());
        assert!(
            err.message().contains("expected 0 or 1"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn timestamptz_zero_is_the_year_2000_in_arrow_epoch_micros() {
        let array = build(
            PgFieldType::TimestampMicrosUtc,
            &[Some(&0i64.to_be_bytes())],
        );
        let stamps = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(stamps.value(0), TIMESTAMP_EPOCH_OFFSET_MICROS);
        assert_eq!(stamps.value(0), 946_684_800_000_000);
    }

    #[test]
    fn timestamp_infinity_sentinels_are_rejected() {
        for sentinel in [i64::MIN, i64::MAX] {
            let mut builder = builder_for(PgFieldType::TimestampMicros, 1);
            let err = builder
                .push("created_at", Some(&sentinel.to_be_bytes()))
                .unwrap_err();
            assert!(err.message().contains("created_at"), "{}", err.message());
            assert!(err.message().contains("infinity"), "{}", err.message());
        }
    }

    #[test]
    fn date_zero_is_the_year_2000_in_arrow_epoch_days() {
        let array = build(PgFieldType::Date32, &[Some(&0i32.to_be_bytes())]);
        let dates = array.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(dates.value(0), DATE_EPOCH_OFFSET_DAYS);
    }

    #[test]
    fn date_infinity_sentinels_are_rejected() {
        for sentinel in [i32::MIN, i32::MAX] {
            let mut builder = builder_for(PgFieldType::Date32, 1);
            let err = builder
                .push("day", Some(&sentinel.to_be_bytes()))
                .unwrap_err();
            assert!(err.message().contains("day"), "{}", err.message());
            assert!(err.message().contains("infinity"), "{}", err.message());
        }
    }

    #[test]
    fn time_is_microseconds_since_midnight_unchanged() {
        let array = build(
            PgFieldType::Time64Micros,
            &[Some(&3_600_000_000i64.to_be_bytes())],
        );
        let times = array
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(times.value(0), 3_600_000_000);
    }

    #[test]
    fn uuid_must_be_sixteen_bytes() {
        let bytes = [7u8; 16];
        let array = build(PgFieldType::Uuid, &[Some(&bytes)]);
        let uuids = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(uuids.value(0), &bytes);

        let mut builder = builder_for(PgFieldType::Uuid, 1);
        let err = builder.push("uid", Some(&[0u8; 15])).unwrap_err();
        assert!(err.message().contains("uid"), "{}", err.message());
        assert!(err.message().contains("expected 16"), "{}", err.message());
    }

    #[test]
    fn invalid_utf8_in_text_is_rejected() {
        let mut builder = builder_for(PgFieldType::Utf8, 1);
        let err = builder.push("label", Some(&[0xff, 0xfe])).unwrap_err();
        assert!(err.message().contains("label"), "{}", err.message());
        assert!(err.message().contains("UTF-8"), "{}", err.message());
    }

    #[test]
    fn json_and_jsonb_both_yield_the_document_text() {
        let json = br#"{"a":1}"#;
        let array = build(PgFieldType::Json, &[Some(json)]);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), r#"{"a":1}"#);

        let mut jsonb = vec![1u8];
        jsonb.extend_from_slice(json);
        let mut builder =
            ColumnBuilder::new(&spec(PgFieldType::Json), 1, Type::JSONB.oid()).unwrap();
        builder.push("doc", Some(&jsonb)).unwrap();
        let array = builder.finish();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), r#"{"a":1}"#);

        // A jsonb version byte the decoder does not know is an error, not a
        // document that silently keeps its header.
        let mut builder =
            ColumnBuilder::new(&spec(PgFieldType::Json), 1, Type::JSONB.oid()).unwrap();
        let mut future = vec![2u8];
        future.extend_from_slice(json);
        let err = builder.push("doc", Some(&future)).unwrap_err();
        assert!(
            err.message().contains("version byte is 2"),
            "{}",
            err.message()
        );
        assert!(err.message().contains("doc"), "{}", err.message());
    }

    #[test]
    fn bytea_is_carried_verbatim() {
        let array = build(PgFieldType::Binary, &[Some(&[0u8, 1, 2, 255])]);
        let blobs = array.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(blobs.value(0), &[0u8, 1, 2, 255]);
    }

    #[test]
    fn numeric_reaches_the_declared_scale() {
        let mut raw = bytes::BytesMut::new();
        crate::numeric::i128_to_numeric(123_456, 4, &mut raw);
        let array = build(PgFieldType::Decimal128, &[Some(&raw)]);
        let decimals = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(decimals.value(0), 123_456);
        assert_eq!(decimals.precision(), 18);
        assert_eq!(decimals.scale(), 4);
    }

    #[test]
    fn reserved_column_helpers_reject_the_wrong_builder() {
        let mut builder = builder_for(PgFieldType::Int64, 1);
        assert!(builder.push_str("__op", "I").is_err());
        builder.push_i64("__lsn", 5).unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 1);
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            5
        );
    }
}
