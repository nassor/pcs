//! [`HttpSource`]: one HTTP GET read through a transformer.
//!
//! The request is made on the first [`Source::next_batch`], never in the
//! constructor, so `pcs-service validate` touches no network. The response body
//! is spooled to an unnamed temp file and handed to the transformer's stream
//! read surface: [`Transformer::open_reader`] takes a `std::fs::File` because
//! parquet reads its footer before any row group. One dedicated OS thread then
//! drives the [`BatchReader`](pcs_transformer::BatchReader) and pushes batches
//! down a bounded channel, so the executor never blocks on the decode.

use std::io::{Seek, Write};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use reqwest::header::HeaderMap;
use tokio::sync::mpsc;

use pcs_core::error::PcsError;
use pcs_core::io::source::Source;
use pcs_transformer::Transformer;

use crate::client;

/// Batches the reader thread may queue before it blocks. Matches
/// `pcs-connector-file`'s.
const CHANNEL_CAPACITY: usize = 4;

/// HTTP [`Source`]: one GET, decoded by a transformer, then EOF.
///
/// The body is fetched once, on the first batch, and the source reports EOF
/// when the decoded stream ends. It is finite, so every run mode can drive it:
/// `one_shot` reads the resource once, and `interval` re-reads it on the run it
/// is built for.
///
/// [`schema`](Source::schema) reports the declared schema, or an empty one when
/// the config declared none, and never changes: nothing can be known about a
/// body that has not arrived. A workflow whose format carries its own schema
/// (`parquet`, `avro`) therefore has no schema to validate the graph against,
/// so a link into a processor that declares fields is rejected at build. Those
/// formats belong in a Rust pipeline, where no graph check runs.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use arrow_schema::{DataType, Field, Schema};
/// use pcs_connector_http::HttpSource;
/// use pcs_transformer_csv::CsvTransformer;
///
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// // csv carries no schema of its own, so the declared one is required.
/// let src = HttpSource::new(
///     "https://example.invalid/orders.csv",
///     Some(schema),
///     Arc::new(CsvTransformer::new(true)),
///     vec![("accept".to_string(), "text/csv".to_string())],
///     Duration::from_secs(30),
/// )
/// .unwrap();
/// ```
pub struct HttpSource {
    client: reqwest::Client,
    url: String,
    /// Cloned onto every request: reqwest's `headers` takes the map by value.
    headers: HeaderMap,
    transformer: Arc<dyn Transformer>,
    /// The schema the config declared, `None` when it declared none. Handed to
    /// the format verbatim: csv requires it, parquet refuses it, ndjson infers
    /// without it.
    declared: Option<Arc<Schema>>,
    /// What [`Source::schema`] reports: `declared` when there is one, an empty
    /// schema when there is not. Resolved here so the accessor is one
    /// `Arc::clone`, and fixed for the source's lifetime.
    schema: Arc<Schema>,
    /// `None` until the first `next_batch` fetches the body.
    batches: Option<mpsc::Receiver<Result<RecordBatch, PcsError>>>,
    /// What the reader reported once it was opened, so it is `None` until the
    /// body has arrived.
    estimated_rows: Option<usize>,
}

impl HttpSource {
    /// Prepare the source without making a request.
    ///
    /// `url` is fetched once, on the first batch. `declared` is the schema the
    /// config named, `None` when it named none; what that means is the
    /// format's business. `headers` are sent with the request, and `timeout`
    /// is the whole-request budget, connect through body.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] when a header name or value is
    /// malformed, or when the HTTP client cannot be built. No request is made.
    pub fn new(
        url: &str,
        declared: Option<Arc<Schema>>,
        transformer: Arc<dyn Transformer>,
        headers: Vec<(String, String)>,
        timeout: Duration,
    ) -> Result<Self, PcsError> {
        Ok(Self {
            client: client::build_client("HttpSource", timeout)?,
            url: url.to_string(),
            headers: client::header_map("HttpSource", headers)?,
            transformer,
            schema: match &declared {
                Some(schema) => Arc::clone(schema),
                None => Arc::new(Schema::empty()),
            },
            declared,
            batches: None,
            estimated_rows: None,
        })
    }

    /// GET the body, spool it, open the reader, and start the decode thread.
    ///
    /// Returns the receiver the decode thread feeds and the row estimate the
    /// reader reported.
    async fn fetch(
        &self,
    ) -> Result<(mpsc::Receiver<Result<RecordBatch, PcsError>>, Option<usize>), PcsError> {
        let response = self
            .client
            .get(&self.url)
            .headers(self.headers.clone())
            .send()
            .await
            .map_err(|e| self.transport_error(&e))?;

        let status = response.status();
        if !status.is_success() {
            return Err(PcsError::generic(format!(
                "HttpSource: status {status} from {}",
                self.url
            )));
        }

        // The whole body is buffered once here: reqwest hands it over as one
        // `Bytes`, and the read surface needs a seekable file anyway.
        let body = response
            .bytes()
            .await
            .map_err(|e| self.transport_error(&e))?;

        // Spooling and the metadata read are disk IO, and a parquet footer read
        // is a seek, so both happen off the executor.
        let transformer = Arc::clone(&self.transformer);
        let declared = self.declared.clone();
        let (mut reader, estimated_rows) = tokio::task::spawn_blocking(move || {
            // Unnamed: the OS reclaims the file when the last handle closes,
            // which is when the reader below is dropped.
            let mut spool = tempfile::tempfile()
                .map_err(|e| PcsError::generic(format!("HttpSource: spool file: {e}")))?;
            spool
                .write_all(&body)
                .map_err(|e| PcsError::generic(format!("HttpSource: spool write: {e}")))?;
            spool
                .rewind()
                .map_err(|e| PcsError::generic(format!("HttpSource: spool rewind: {e}")))?;
            let reader = transformer.open_reader(spool, declared)?;
            let estimated_rows = reader.estimated_rows();
            Ok::<_, PcsError>((reader, estimated_rows))
        })
        .await
        .map_err(|e| PcsError::generic(format!("HttpSource: spawn_blocking panic: {e}")))??;

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        std::thread::spawn(move || {
            loop {
                match reader.next_batch() {
                    Ok(None) => break,
                    Ok(Some(batch)) => {
                        // A send error means the source was dropped (pipeline
                        // aborted), so this thread has nowhere left to go.
                        if tx.blocking_send(Ok(batch)).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e));
                        break;
                    }
                }
            }
        });

        Ok((rx, estimated_rows))
    }

    /// The one wording every transport failure of the GET carries: a refused
    /// connection, a TLS rejection, a timeout, and a body that stops early are
    /// all the same request failing.
    fn transport_error(&self, error: &reqwest::Error) -> PcsError {
        PcsError::generic(format!("HttpSource: cannot GET {}: {error}", self.url))
    }
}

#[async_trait]
impl Source for HttpSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        if self.batches.is_none() {
            let (rx, estimated_rows) = self.fetch().await?;
            self.batches = Some(rx);
            self.estimated_rows = estimated_rows;
        }
        let rx = self.batches.as_mut().expect("the receiver was just set");
        match rx.recv().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }
}
