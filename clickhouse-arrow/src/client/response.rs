use std::pin::Pin;

use futures_util::stream::StreamExt;
use futures_util::{Stream, TryStreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, trace};

use super::ClientFormat;
use crate::explain::ExplainResult;
use crate::prelude::{ATT_CID, ATT_QID};
use crate::{Qid, Result};

pub(crate) fn create_response_stream<T: ClientFormat>(
    rx: mpsc::Receiver<Result<T::Data>>,
    qid: Qid,
    cid: u16,
) -> impl Stream<Item = Result<T::Data>> + 'static {
    ReceiverStream::new(rx)
        .inspect_ok(move |_| trace!({ ATT_CID } = cid, { ATT_QID } = %qid, "response"))
        .inspect_err(move |error| error!(?error, { ATT_CID } = cid, { ATT_QID } = %qid, "response"))
}

pub(crate) fn handle_insert_response<T: ClientFormat>(
    rx: mpsc::Receiver<Result<T::Data>>,
    qid: Qid,
    cid: u16,
) -> impl Stream<Item = Result<()>> + 'static {
    ReceiverStream::new(rx)
        .inspect_ok(move |_| trace!({ ATT_CID } = cid, { ATT_QID } = %qid, "response"))
        .inspect_err(move |error| error!(?error, { ATT_CID } = cid, { ATT_QID } = %qid, "response"))
        .filter_map(move |response| async move {
            match response {
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            }
        })
}

/// Response from a `ClickHouse` query.
///
/// This struct wraps a stream of query results and optionally includes
/// an EXPLAIN result if explain options were provided.
#[pin_project::pin_project]
pub struct ClickHouseResponse<T> {
    #[pin]
    stream:           Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>,
    /// Receiver for the parallel EXPLAIN result, if configured.
    explain_receiver: Option<oneshot::Receiver<Result<ExplainResult>>>,
}

impl<T> ClickHouseResponse<T> {
    /// Create a new response wrapping a stream.
    pub fn new(stream: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>) -> Self {
        Self { stream, explain_receiver: None }
    }

    /// Create a new response with an explain receiver.
    pub fn with_explain(
        stream: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>,
        explain_receiver: oneshot::Receiver<Result<ExplainResult>>,
    ) -> Self {
        Self { stream, explain_receiver: Some(explain_receiver) }
    }

    /// Create a response from a stream.
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<T>> + Send + 'static,
    {
        Self::new(Box::pin(stream))
    }

    /// Create a response from a stream with explain.
    pub fn from_stream_with_explain<S>(
        stream: S,
        explain_receiver: oneshot::Receiver<Result<ExplainResult>>,
    ) -> Self
    where
        S: Stream<Item = Result<T>> + Send + 'static,
    {
        Self::with_explain(Box::pin(stream), explain_receiver)
    }

    /// Check if this response has an EXPLAIN result pending.
    #[must_use]
    pub fn has_explain(&self) -> bool { self.explain_receiver.is_some() }

    /// Get the EXPLAIN result, if one was configured.
    ///
    /// This method consumes the explain receiver, so it can only be called once.
    /// If called before the parallel explain completes, it will block until the
    /// result is available.
    ///
    /// Returns `None` if no explain was configured for this query.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut response = client.query_with_options("SELECT...", opts).await?;
    ///
    /// // Consume query results
    /// while let Some(batch) = response.next().await { ... }
    ///
    /// // Get explain result (blocks if still running)
    /// if let Some(explain) = response.explain().await {
    ///     let explain = explain?;
    ///     println!("{}", explain);
    /// }
    /// ```
    pub async fn explain(&mut self) -> Option<Result<ExplainResult>> {
        let receiver = self.explain_receiver.take()?;
        match receiver.await {
            Ok(result) => Some(result),
            Err(_) => Some(Err(crate::Error::ChannelClosed)),
        }
    }
}

impl<T> Stream for ClickHouseResponse<T>
where
    T: Send + 'static,
{
    type Item = Result<T>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}
