//! [`PostgresSource`]: the one public source type, over three read modes.
//!
//! The Arrow schema is built from the declared `schema_fields` once, in
//! [`PostgresSource::new`], and handed out by reference from
//! [`Source::schema`]: the trait requires a schema that does not change between
//! calls, so it is never rebuilt.
//!
//! [`new`](PostgresSource::new) opens no connection. It validates the config,
//! builds the schema and the [`Connector`](crate::connection::Connector), and
//! returns; the first [`next_batch`](Source::next_batch) connects. That is what
//! keeps `pcs-service validate` free of a database, and it is forced anyway,
//! because `SourceFactory::build` is synchronous.

pub(crate) mod cursor;
pub(crate) mod logical;
pub(crate) mod pgoutput;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use pcs_core::error::PcsError;
use pcs_core::io::source::Source;

use crate::config::{PostgresSourceConfig, SourceMode};
use crate::source::cursor::CursorReader;
use crate::source::logical::LogicalReader;

/// A PostgreSQL [`Source`] in one of the three read modes.
pub struct PostgresSource {
    schema: Arc<Schema>,
    reader: Reader,
}

/// The mode-specific half.
///
/// Both readers carry their prepared statements, builders and connection state,
/// so they are boxed: an unboxed enum would make every `PostgresSource` as large
/// as the bigger of the two.
enum Reader {
    /// `polling` and `cdc_trigger`, which differ only in retention.
    Cursor(Box<CursorReader>),
    /// `cdc_logical`.
    Logical(Box<LogicalReader>),
}

impl PostgresSource {
    /// Validate `cfg` and prepare the reader. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] for any violation
    /// [`PostgresSourceConfig::validate`] reports, for a DSN that does not
    /// parse, for an unreadable `password_file`, and for a TLS configuration
    /// that cannot be built.
    pub fn new(cfg: PostgresSourceConfig) -> Result<Self, PcsError> {
        cfg.validate()?;

        let fields = cfg
            .schema_fields
            .iter()
            .map(|spec| spec.to_arrow_field())
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(fields));

        #[cfg(feature = "tracing")]
        tracing::info!(
            source = %cfg.name,
            mode = cfg.mode.label(),
            columns = cfg.schema_fields.len(),
            "postgres source configured"
        );

        let reader = match &cfg.mode {
            SourceMode::Polling(_) | SourceMode::CdcTrigger(_) => {
                Reader::Cursor(Box::new(CursorReader::new(&cfg)?))
            }
            SourceMode::CdcLogical(_) => Reader::Logical(Box::new(LogicalReader::new(&cfg)?)),
        };

        Ok(Self { schema, reader })
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        match &mut self.reader {
            Reader::Cursor(reader) => reader.next_batch(&self.schema).await,
            Reader::Logical(reader) => reader.next_batch(&self.schema).await,
        }
    }

    // The trait default of `None`. The only honest answer needs a `COUNT(*)` per
    // call, and the trait documents that callers must not rely on accuracy.
}
