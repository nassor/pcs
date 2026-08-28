//! Arrow values into PostgreSQL binary parameters for `COPY … FORMAT binary`.
//!
//! [`PgValue`] is one enum implementing `ToSql`, so a row costs one reusable
//! `Vec` rather than a boxed trait object per value, and `to_sql` writes the
//! wire form directly rather than going through an intermediate owned type.
//!
//! [`ColumnReader`] resolves each declared column against a `RecordBatch` once,
//! so the per-row path is an index into a typed array with no schema lookup and
//! no downcast.
//!
//! The inverse of [`crate::values`]: the same epochs, the same `jsonb` version
//! byte, the same `numeric` codec.

use std::error::Error;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use bytes::{BufMut, BytesMut};
use pcs_core::error::PcsError;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};

use crate::config::{FieldSpec, PgFieldType};
use crate::numeric::i128_to_numeric;
use crate::values::{DATE_EPOCH_OFFSET_DAYS, TIMESTAMP_EPOCH_OFFSET_MICROS};

/// One column value, ready to be written in PostgreSQL binary format.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PgValue<'a> {
    /// SQL NULL.
    Null,
    /// `bool`.
    Bool(bool),
    /// `int2`.
    I16(i16),
    /// `int4`.
    I32(i32),
    /// `int8`.
    I64(i64),
    /// `float4`.
    F32(f32),
    /// `float8`.
    F64(f64),
    /// `text`, `varchar`, `bpchar`, `name`.
    Str(&'a str),
    /// `bytea`.
    Bytes(&'a [u8]),
    /// `date`, in Arrow's 1970-01-01 epoch.
    Date(i32),
    /// `time`, microseconds since midnight.
    TimeMicros(i64),
    /// `timestamp` or `timestamptz`, in Arrow's 1970-01-01 epoch.
    TimestampMicros(i64),
    /// `uuid`, exactly 16 bytes.
    Uuid(&'a [u8]),
    /// `json` or `jsonb`; the version byte is added when the target is `jsonb`.
    Json(&'a str),
    /// `numeric`, as an unscaled `i128` plus its scale.
    Numeric {
        /// The unscaled value.
        value: i128,
        /// Decimal digits after the point.
        scale: i8,
    },
}

impl ToSql for PgValue<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match *self {
            PgValue::Null => return Ok(IsNull::Yes),
            PgValue::Bool(v) => out.put_u8(u8::from(v)),
            PgValue::I16(v) => out.put_i16(v),
            PgValue::I32(v) => out.put_i32(v),
            PgValue::I64(v) => out.put_i64(v),
            PgValue::F32(v) => out.put_u32(v.to_bits()),
            PgValue::F64(v) => out.put_u64(v.to_bits()),
            PgValue::Str(v) => out.put_slice(v.as_bytes()),
            PgValue::Bytes(v) | PgValue::Uuid(v) => out.put_slice(v),
            PgValue::Date(v) => {
                let days = v.checked_sub(DATE_EPOCH_OFFSET_DAYS).ok_or_else(|| {
                    format!("date {v} does not fit PostgreSQL's 2000-01-01 epoch")
                })?;
                out.put_i32(days);
            }
            PgValue::TimeMicros(v) => out.put_i64(v),
            PgValue::TimestampMicros(v) => {
                let micros = v
                    .checked_sub(TIMESTAMP_EPOCH_OFFSET_MICROS)
                    .ok_or_else(|| {
                        format!("timestamp {v} does not fit PostgreSQL's 2000-01-01 epoch")
                    })?;
                out.put_i64(micros);
            }
            PgValue::Json(v) => {
                if *ty == Type::JSONB {
                    out.put_u8(1);
                }
                out.put_slice(v.as_bytes());
            }
            PgValue::Numeric { value, scale } => i128_to_numeric(value, scale, out),
        }
        Ok(IsNull::No)
    }

    // The target column types come from the catalog and are checked against the
    // declared schema by `types::validate_columns` before a single row is
    // encoded, so there is nothing left for a per-value type check to reject.
    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// A declared column resolved against one `RecordBatch`.
///
/// Holding the downcast array means the per-row cost is a bounds-checked index,
/// not a name lookup plus a downcast per value.
#[derive(Debug)]
pub(crate) enum ColumnReader<'a> {
    /// `boolean`.
    Boolean(&'a BooleanArray),
    /// `int16`.
    Int16(&'a Int16Array),
    /// `int32`.
    Int32(&'a Int32Array),
    /// `int64`.
    Int64(&'a Int64Array),
    /// `float32`.
    Float32(&'a Float32Array),
    /// `float64`.
    Float64(&'a Float64Array),
    /// `utf8`.
    Utf8(&'a StringArray),
    /// `json`.
    Json(&'a StringArray),
    /// `binary`.
    Binary(&'a BinaryArray),
    /// `date32`.
    Date32(&'a Date32Array),
    /// `time64_micros`.
    Time64(&'a Time64MicrosecondArray),
    /// `timestamp_micros` and `timestamp_micros_utc`.
    Timestamp(&'a TimestampMicrosecondArray),
    /// `uuid`.
    Uuid(&'a FixedSizeBinaryArray),
    /// `decimal128`, carrying the declared scale.
    Decimal128(&'a Decimal128Array, i8),
}

impl<'a> ColumnReader<'a> {
    /// The value at `row`, or [`PgValue::Null`].
    pub(crate) fn value(&self, row: usize) -> PgValue<'a> {
        macro_rules! read {
            ($array:expr, $wrap:expr) => {{
                if $array.is_null(row) {
                    PgValue::Null
                } else {
                    $wrap($array.value(row))
                }
            }};
        }
        match *self {
            ColumnReader::Boolean(a) => read!(a, PgValue::Bool),
            ColumnReader::Int16(a) => read!(a, PgValue::I16),
            ColumnReader::Int32(a) => read!(a, PgValue::I32),
            ColumnReader::Int64(a) => read!(a, PgValue::I64),
            ColumnReader::Float32(a) => read!(a, PgValue::F32),
            ColumnReader::Float64(a) => read!(a, PgValue::F64),
            ColumnReader::Utf8(a) => read!(a, PgValue::Str),
            ColumnReader::Json(a) => read!(a, PgValue::Json),
            ColumnReader::Binary(a) => read!(a, PgValue::Bytes),
            ColumnReader::Date32(a) => read!(a, PgValue::Date),
            ColumnReader::Time64(a) => read!(a, PgValue::TimeMicros),
            ColumnReader::Timestamp(a) => read!(a, PgValue::TimestampMicros),
            ColumnReader::Uuid(a) => read!(a, PgValue::Uuid),
            ColumnReader::Decimal128(a, scale) => {
                if a.is_null(row) {
                    PgValue::Null
                } else {
                    PgValue::Numeric {
                        value: a.value(row),
                        scale,
                    }
                }
            }
        }
    }
}

/// Resolve every declared column of `batch` once, in declared order.
///
/// # Errors
///
/// Returns [`PcsError::Generic`] naming the first column that `batch` does not
/// carry, or that carries a different Arrow type than the declared one.
pub(crate) fn resolve_columns<'a>(
    what: &str,
    batch: &'a RecordBatch,
    specs: &[FieldSpec],
) -> Result<Vec<ColumnReader<'a>>, PcsError> {
    let mut readers = Vec::with_capacity(specs.len());
    for spec in specs {
        let array = batch
            .column_by_name(&spec.name)
            .ok_or_else(|| {
                PcsError::generic(format!(
                    "{what}: batch has no column '{}'; the batch schema must match the declared \
                     schema_fields",
                    spec.name
                ))
            })?
            .as_ref();

        macro_rules! cast {
            ($target:ty, $variant:expr) => {{
                let typed = array.as_any().downcast_ref::<$target>().ok_or_else(|| {
                    PcsError::generic(format!(
                        "{what}: column '{}' is declared type \"{}\" but the batch holds {:?}",
                        spec.name,
                        spec.ty.as_str(),
                        array.data_type()
                    ))
                })?;
                $variant(typed)
            }};
        }

        readers.push(match spec.ty {
            PgFieldType::Boolean => cast!(BooleanArray, ColumnReader::Boolean),
            PgFieldType::Int16 => cast!(Int16Array, ColumnReader::Int16),
            PgFieldType::Int32 => cast!(Int32Array, ColumnReader::Int32),
            PgFieldType::Int64 => cast!(Int64Array, ColumnReader::Int64),
            PgFieldType::Float32 => cast!(Float32Array, ColumnReader::Float32),
            PgFieldType::Float64 => cast!(Float64Array, ColumnReader::Float64),
            PgFieldType::Utf8 => cast!(StringArray, ColumnReader::Utf8),
            PgFieldType::Json => cast!(StringArray, ColumnReader::Json),
            PgFieldType::Binary => cast!(BinaryArray, ColumnReader::Binary),
            PgFieldType::Date32 => cast!(Date32Array, ColumnReader::Date32),
            PgFieldType::Time64Micros => cast!(Time64MicrosecondArray, ColumnReader::Time64),
            PgFieldType::TimestampMicros | PgFieldType::TimestampMicrosUtc => {
                cast!(TimestampMicrosecondArray, ColumnReader::Timestamp)
            }
            PgFieldType::Uuid => cast!(FixedSizeBinaryArray, ColumnReader::Uuid),
            PgFieldType::Decimal128 => {
                let (_, scale) = spec.decimal_params()?;
                let typed = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        PcsError::generic(format!(
                            "{what}: column '{}' is declared type \"decimal128\" but the batch \
                             holds {:?}",
                            spec.name,
                            array.data_type()
                        ))
                    })?;
                if typed.scale() != scale {
                    return Err(PcsError::generic(format!(
                        "{what}: column '{}' is declared scale {scale} but the batch holds scale \
                         {}",
                        spec.name,
                        typed.scale()
                    )));
                }
                ColumnReader::Decimal128(typed, scale)
            }
        });
    }
    Ok(readers)
}

/// Refill `out` with one row's values, reusing its allocation.
pub(crate) fn row_values<'a>(readers: &[ColumnReader<'a>], row: usize, out: &mut Vec<PgValue<'a>>) {
    out.clear();
    out.extend(readers.iter().map(|reader| reader.value(row)));
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::FixedSizeBinaryBuilder;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;
    use crate::values::ColumnBuilder;

    fn spec(name: &str, ty: PgFieldType) -> FieldSpec {
        FieldSpec {
            name: name.to_string(),
            ty,
            nullable: true,
            precision: matches!(ty, PgFieldType::Decimal128).then_some(18),
            scale: matches!(ty, PgFieldType::Decimal128).then_some(4),
        }
    }

    /// Encode with `PgValue::to_sql`, decode with `ColumnBuilder::push`, and
    /// return the length the encoder wrote so a field-width regression shows up.
    fn encoded(value: PgValue<'_>, ty: &Type) -> BytesMut {
        let mut buf = BytesMut::new();
        let is_null = value.to_sql(ty, &mut buf).expect("to_sql");
        assert!(matches!(is_null, IsNull::No));
        buf
    }

    #[test]
    fn integers_and_floats_round_trip_through_the_decoder() {
        let cases: Vec<(PgFieldType, PgValue<'static>, Type)> = vec![
            (PgFieldType::Boolean, PgValue::Bool(true), Type::BOOL),
            (PgFieldType::Int16, PgValue::I16(-3), Type::INT2),
            (PgFieldType::Int32, PgValue::I32(-70_000), Type::INT4),
            (PgFieldType::Int64, PgValue::I64(i64::MIN), Type::INT8),
            (PgFieldType::Float32, PgValue::F32(-1.5), Type::FLOAT4),
            (PgFieldType::Float64, PgValue::F64(1e100), Type::FLOAT8),
        ];
        for (declared, value, ty) in cases {
            let buf = encoded(value, &ty);
            let mut builder = ColumnBuilder::new(&spec("c", declared), 1, ty.oid()).unwrap();
            builder.push("c", Some(&buf)).expect("push");
            assert_eq!(builder.finish().len(), 1, "{declared:?}");
        }
    }

    #[test]
    fn date_and_timestamp_epochs_are_symmetric() {
        // 2000-01-01 in Arrow terms, which is 0 on the wire.
        let buf = encoded(PgValue::Date(DATE_EPOCH_OFFSET_DAYS), &Type::DATE);
        assert_eq!(&buf[..], &0i32.to_be_bytes());

        let buf = encoded(
            PgValue::TimestampMicros(TIMESTAMP_EPOCH_OFFSET_MICROS),
            &Type::TIMESTAMPTZ,
        );
        assert_eq!(&buf[..], &0i64.to_be_bytes());

        // And the Unix epoch is negative on the wire.
        let buf = encoded(PgValue::TimestampMicros(0), &Type::TIMESTAMP);
        assert_eq!(
            i64::from_be_bytes(buf[..].try_into().unwrap()),
            -TIMESTAMP_EPOCH_OFFSET_MICROS
        );
    }

    #[test]
    fn json_gains_a_version_byte_only_for_jsonb() {
        let buf = encoded(PgValue::Json("{\"a\":1}"), &Type::JSON);
        assert_eq!(&buf[..], b"{\"a\":1}");

        let buf = encoded(PgValue::Json("{\"a\":1}"), &Type::JSONB);
        assert_eq!(buf[0], 1);
        assert_eq!(&buf[1..], b"{\"a\":1}");
    }

    #[test]
    fn numeric_round_trips_through_the_decoder() {
        let buf = encoded(
            PgValue::Numeric {
                value: -123_456,
                scale: 4,
            },
            &Type::NUMERIC,
        );
        let mut builder =
            ColumnBuilder::new(&spec("c", PgFieldType::Decimal128), 1, Type::NUMERIC.oid())
                .unwrap();
        builder.push("c", Some(&buf)).unwrap();
        let array = builder.finish();
        let decimals = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(decimals.value(0), -123_456);
    }

    #[test]
    fn null_writes_nothing_and_reports_is_null() {
        let mut buf = BytesMut::new();
        let is_null = PgValue::Null.to_sql(&Type::INT8, &mut buf).unwrap();
        assert!(matches!(is_null, IsNull::Yes));
        assert!(buf.is_empty());
    }

    #[test]
    fn resolve_columns_reads_by_name_and_row_values_reuses_its_vec() {
        let mut uuids = FixedSizeBinaryBuilder::with_capacity(2, 16);
        uuids.append_value([9u8; 16]).unwrap();
        uuids.append_null();

        let schema = Arc::new(Schema::new(vec![
            Field::new("label", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
            Field::new("uid", DataType::FixedSizeBinary(16), true),
            Field::new("at", DataType::Time64(TimeUnit::Microsecond), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(uuids.finish()),
                Arc::new(Time64MicrosecondArray::from(vec![Some(5), Some(6)])),
            ],
        )
        .unwrap();

        // Declared in a different order than the batch: resolution is by name.
        let specs = [
            spec("id", PgFieldType::Int64),
            spec("label", PgFieldType::Utf8),
            spec("uid", PgFieldType::Uuid),
            spec("at", PgFieldType::Time64Micros),
        ];
        let readers = resolve_columns("PostgresSink", &batch, &specs).unwrap();

        let mut row = Vec::new();
        row_values(&readers, 0, &mut row);
        assert!(matches!(row[0], PgValue::I64(1)));
        assert!(matches!(row[1], PgValue::Str("a")));
        assert!(matches!(row[2], PgValue::Uuid(_)));
        assert!(matches!(row[3], PgValue::TimeMicros(5)));

        let capacity = row.capacity();
        row_values(&readers, 1, &mut row);
        assert_eq!(row.capacity(), capacity, "the row vec must be reused");
        assert!(matches!(row[0], PgValue::I64(2)));
        assert!(matches!(row[1], PgValue::Null));
        assert!(matches!(row[2], PgValue::Null));
    }

    #[test]
    fn a_missing_batch_column_names_itself() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let specs = [spec("other", PgFieldType::Int64)];
        let err = resolve_columns("PostgresSink", &batch, &specs).unwrap_err();
        assert!(err.message().contains("'other'"), "{}", err.message());
    }

    #[test]
    fn a_decimal_scale_mismatch_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(18, 2),
            true,
        )]));
        let array = Decimal128Array::from(vec![Some(1i128)])
            .with_precision_and_scale(18, 2)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let specs = [spec("amount", PgFieldType::Decimal128)];
        let err = resolve_columns("PostgresSink", &batch, &specs).unwrap_err();
        assert!(err.message().contains("scale"), "{}", err.message());
    }
}
