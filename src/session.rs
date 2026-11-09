use crate::{
    codec::Codec,
    command::Command,
    config::Config,
    error::{Result, SmuxError},
    frame::Frame,
    stream::Stream,
};
use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Notify,
    time::interval,
};
use tokio_util::codec::Framed;

/// Stream state tracked by the session for each active stream
#[derive(Debug)]
struct StreamState {
    /// Sender for data chunks to the stream
    data_tx: flume::Sender<Bytes>,
    /// Atomic flag for read closed state
    is_read_closed: Arc<AtomicBool>,
    /// Waker of the task blocked in `Stream::poll_read`
    read_waker: Arc<std::sync::Mutex<Option<std::task::Waker>>>,
}

#[derive(Debug)]
pub(crate) struct SessionState {
    die: Arc<Notify>,
    closed: Arc<AtomicBool>,
    /// Tokens available for flow control
    tokens: Arc<AtomicI64>,
    /// Notified when tokens are returned by streams
    tokens_returned: Arc<Notify>,
    /// Flag indicating if data has been received since last keepalive check
    data_received: Arc<AtomicBool>,
}

/// A multiplexed session that manages multiple streams over a single connection.
///
/// The `Session` provides the main interface for creating and managing multiple logical
/// streams over a single underlying connection. It handles frame multiplexing, flow control,
/// and keep-alive functionality.
///
/// # Examples
///
/// ## Creating a client session
///
/// ```rust,no_run
/// use smux::{Config, Session};
/// use tokio::net::TcpStream;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let conn = TcpStream::connect("127.0.0.1:8080").await?;
///     let config = Config::default();
///     let mut session = Session::client(conn, config).await?;
///     
///     let mut stream = session.open_stream().await?;
///     // Use stream...
///     Ok(())
/// }
/// ```
///
/// ## Creating a server session
///
/// ```rust,no_run
/// use smux::{Config, Session};
/// use tokio::net::TcpListener;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let listener = TcpListener::bind("127.0.0.1:8080").await?;
///     let (conn, _) = listener.accept().await?;
///     
///     let config = Config::default();
///     let mut session = Session::server(conn, config).await?;
///     
///     while let Ok(mut stream) = session.accept_stream().await {
///         // Handle incoming stream...
///     }
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct Session {
    inner: Arc<SessionInner>,
}

/// Internal session state shared between tasks
#[derive(Debug)]
struct SessionInner {
    /// Active streams mapped by stream ID to their state
    streams: DashMap<u32, StreamState>,
    /// Session configuration
    config: Arc<Config>,
    /// Sender for accepting new streams initiated by peer
    incoming_streams_tx: flume::Sender<Stream>,
    /// Receiver for accepting new streams (used by accept_stream)
    incoming_streams_rx: flume::Receiver<Stream>,
    /// Next stream ID to be used
    next_stream_id: AtomicU32,
    /// True if this is the client side of the connection
    is_client: bool,
    /// Sender for outgoing frames (to send_loop)
    frame_tx: flume::Sender<Frame>,
    /// Session state passed to Streams
    state: SessionState,
}

impl SessionState {
    /// Create a new session state
    pub fn new(buffer_size: usize) -> Self {
        Self {
            die: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
            tokens: Arc::new(AtomicI64::new(buffer_size as i64)),
            tokens_returned: Arc::new(Notify::new()),
            data_received: Arc::new(AtomicBool::new(true)), // Start with true to avoid immediate timeout
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn close_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.die)
    }

    pub fn close(&self) {
        if !self.closed.swap(true, std::sync::atomic::Ordering::Relaxed) {
            self.die.notify_waiters();
        }
    }

    /// Return tokens to the session and notify waiters
    pub fn return_tokens(&self, count: usize) {
        self.tokens.fetch_add(count as i64, Ordering::Relaxed);
        self.tokens_returned.notify_waiters();
    }

    pub fn get_tokens(&self) -> i64 {
        self.tokens.load(Ordering::Relaxed)
    }

    pub fn take_tokens(&self, count: usize) -> i64 {
        self.tokens.fetch_sub(count as i64, Ordering::Relaxed);
        self.get_tokens()
    }

    /// Get tokens returned notifier
    pub fn tokens_returned_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.tokens_returned)
    }

    /// Mark that data has been received
    pub fn mark_data_received(&self) {
        self.data_received.store(true, Ordering::Relaxed);
    }

    /// Check and reset data received flag (returns previous value)
    pub fn check_and_reset_data_received(&self) -> bool {
        self.data_received.swap(false, Ordering::Relaxed)
    }
}

impl Clone for SessionState {
    fn clone(&self) -> Self {
        Self {
            die: Arc::clone(&self.die),
            closed: Arc::clone(&self.closed),
            tokens: Arc::clone(&self.tokens),
            tokens_returned: Arc::clone(&self.tokens_returned),
            data_received: Arc::clone(&self.data_received),
        }
    }
}

impl Clone for Session {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Session {
    /// Create a new client session over the given transport.
    ///
    /// This method establishes a client-side smux session and starts the necessary
    /// background tasks for handling frames and keep-alive.
    ///
    /// # Arguments
    ///
    /// * `transport` - The underlying connection (e.g., TcpStream)
    /// * `config` - Session configuration
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use smux::{Config, Session};
    /// use tokio::net::TcpStream;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let conn = TcpStream::connect("127.0.0.1:8080").await?;
    /// let session = Session::client(conn, Config::default()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn client(
        transport: impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        config: Config,
    ) -> Result<Self> {
        Self::new(transport, config, true).await
    }

    /// Create a new server session over the given transport.
    ///
    /// This method establishes a server-side smux session and starts the necessary
    /// background tasks for handling frames and keep-alive.
    ///
    /// # Arguments
    ///
    /// * `transport` - The underlying connection (e.g., TcpStream)
    /// * `config` - Session configuration
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use smux::{Config, Session};
    /// use tokio::net::TcpListener;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let listener = TcpListener::bind("127.0.0.1:8080").await?;
    /// let (conn, _) = listener.accept().await?;
    /// let session = Session::server(conn, Config::default()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn server(
        transport: impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        config: Config,
    ) -> Result<Self> {
        Self::new(transport, config, false).await
    }

    /// Internal constructor for both client and server sessions
    async fn new(
        transport: impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        config: Config,
        is_client: bool,
    ) -> Result<Self> {
        let config = Arc::new(config);
        let codec = Codec::new((*config).clone());
        let framed = Framed::new(transport, codec);
        let (sink, stream) = framed.split();

        // Create channels
        let (frame_tx, frame_rx) = flume::bounded(config.max_receive_buffer);
        let (incoming_streams_tx, incoming_streams_rx) = flume::bounded(16);

        // Initial stream ID depends on whether we are client or server
        let initial_id = if is_client { 1 } else { 2 };

        // Create session inner
        let inner = Arc::new(SessionInner {
            streams: DashMap::new(),
            config: Arc::clone(&config),
            incoming_streams_tx,
            incoming_streams_rx,
            next_stream_id: AtomicU32::new(initial_id),
            is_client,
            frame_tx,
            state: SessionState::new(config.max_receive_buffer),
        });

        let session = Session {
            inner: Arc::clone(&inner),
        };

        // Spawn background tasks
        let recv_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            if let Err(e) = recv_loop(stream, recv_inner).await {
                tracing::error!("recv_loop error: {}", e);
            }
        });

        let send_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            if let Err(e) = send_loop(sink, frame_rx, send_inner).await {
                tracing::error!("send_loop error: {}", e);
            }
        });

        // Spawn keep-alive task
        let keep_alive_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            if let Err(e) = keep_alive_loop(keep_alive_inner).await {
                tracing::error!("keep_alive_loop error: {}", e);
            }
        });

        Ok(session)
    }

    /// Open a new outgoing stream
    /// Open a new outgoing stream.
    ///
    /// This method creates a new logical stream within the session. It can only be called
    /// on the client side of the connection. The server should use `accept_stream()` to
    /// receive incoming streams.
    ///
    /// # Returns
    ///
    /// Returns a new `Stream` that implements `AsyncRead` and `AsyncWrite`.
    ///
    /// # Errors
    ///
    /// Returns `SmuxError::SessionClosed` if the session has been closed.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use smux::{Config, Session};
    /// use tokio::net::TcpStream;
    /// use tokio::io::{AsyncReadExt, AsyncWriteExt};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let conn = TcpStream::connect("127.0.0.1:8080").await?;
    /// let mut session = Session::client(conn, Config::default()).await?;
    ///
    /// let mut stream = session.open_stream().await?;
    /// stream.write_all(b"Hello, world!").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open_stream(&self) -> Result<Stream> {
        if self.is_closed() {
            return Err(SmuxError::SessionClosed);
        }

        // Generate new stream ID
        let stream_id = self.inner.next_stream_id()?;

        // Create data channel for this stream
        let (data_tx, data_rx) = flume::unbounded();

        // Create stream state
        let read_waker = Arc::new(std::sync::Mutex::new(None));
        let stream_state = StreamState {
            data_tx,
            is_read_closed: Arc::new(AtomicBool::new(false)),
            read_waker: Arc::clone(&read_waker),
        };

        // Create stream
        let stream = Stream::new(
            stream_id,
            self.inner.frame_tx.clone(),
            data_rx,
            self.inner.state.clone(),
            Arc::clone(&self.inner.config),
            read_waker,
        );

        // Add to streams map
        self.inner.streams.insert(stream_id, stream_state);

        // Send SYN frame
        let syn_frame = Frame::new_syn(self.inner.config.version, stream_id);
        self.inner
            .frame_tx
            .send_async(syn_frame)
            .await
            .map_err(|_| SmuxError::SessionClosed)?;

        Ok(stream)
    }

    /// Accept an incoming stream initiated by the peer
    /// Accept an incoming stream from the peer.
    ///
    /// This method waits for and returns new streams initiated by the remote peer.
    /// It should typically be called in a loop on the server side to handle incoming
    /// stream requests.
    ///
    /// # Returns
    ///
    /// Returns a new `Stream` that implements `AsyncRead` and `AsyncWrite`.
    ///
    /// # Errors
    ///
    /// Returns `SmuxError::SessionClosed` if the session has been closed.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use smux::{Config, Session};
    /// use tokio::net::TcpListener;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let listener = TcpListener::bind("127.0.0.1:8080").await?;
    /// let (conn, _) = listener.accept().await?;
    /// let mut session = Session::server(conn, Config::default()).await?;
    ///
    /// while let Ok(stream) = session.accept_stream().await {
    ///     tokio::spawn(async move {
    ///         // Handle the stream...
    ///     });
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn accept_stream(&self) -> Result<Stream> {
        if self.is_closed() {
            return Err(SmuxError::SessionClosed);
        }

        let rx = &self.inner.incoming_streams_rx;
        let close_notifier = self.inner.state.close_notifier();

        tokio::select! {
            result = rx.recv_async() => {
                match result {
                    Ok(stream) => Ok(stream),
                    Err(_) => Err(SmuxError::SessionClosed),
                }
            },
            _ = close_notifier.notified() => Err(SmuxError::SessionClosed),
        }
    }

    /// Close the session gracefully
    #[inline]
    /// Close the session.
    ///
    /// This gracefully closes the session and all associated streams. After calling
    /// this method, no new streams can be opened or accepted, and existing streams
    /// will be closed.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use smux::{Config, Session};
    /// use tokio::net::TcpStream;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let conn = TcpStream::connect("127.0.0.1:8080").await?;
    /// let session = Session::client(conn, Config::default()).await?;
    ///
    /// // Use session...
    ///
    /// session.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&self) -> Result<()> {
        self.inner.state.close();
        Ok(())
    }

    /// Check if the session is closed.
    ///
    /// Returns `true` if the session has been closed, either by calling `close()`
    /// or due to an error condition.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use smux::{Config, Session};
    /// use tokio::net::TcpStream;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let conn = TcpStream::connect("127.0.0.1:8080").await?;
    /// let session = Session::client(conn, Config::default()).await?;
    ///
    /// assert!(!session.is_closed());
    /// session.close().await?;
    /// assert!(session.is_closed());
    /// # Ok(())
    /// # }
    /// ```
    pub fn is_closed(&self) -> bool {
        self.inner.state.is_closed()
    }
}

impl SessionInner {
    /// Get the next available stream ID
    fn next_stream_id(&self) -> Result<u32> {
        let current = self.next_stream_id.fetch_add(2, Ordering::Relaxed);
        if current > u32::MAX - 2 {
            return Err(SmuxError::ProtocolViolation(
                "Stream ID overflow - session should be restarted".to_string(),
            ));
        }
        Ok(current)
    }

    /// Validate a stream ID initiated by the peer
    fn validate_peer_stream_id(&self, stream_id: u32) -> Result<()> {
        if stream_id == 0 {
            return Err(SmuxError::InvalidStreamId(stream_id));
        }

        let expected_parity = if self.is_client { 0 } else { 1 };
        let actual_parity = stream_id % 2;

        if actual_parity != expected_parity {
            return Err(SmuxError::InvalidStreamId(stream_id));
        }

        Ok(())
    }
}

/// Background task that reads frames from the transport and dispatches them
async fn recv_loop<T>(
    mut stream: futures::stream::SplitStream<Framed<T, Codec>>,
    inner: Arc<SessionInner>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
    let close_notifier = inner.state.close_notifier();
    loop {
        tokio::select! {
            frame_result = stream.next() => {
                match frame_result {
                    Some(Ok(frame)) => {
                        // Mark that data has been received for keep-alive
                        inner.state.mark_data_received();

                        if let Err(e) = handle_frame(frame, &inner).await {
                            tracing::error!("Error handling frame: {}", e);
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("Frame decode error: {}", e);
                        break;
                    }
                    None => {
                        tracing::info!("Transport closed");
                        break;
                    }
                }
            }
            _ = close_notifier.notified() => {
                tracing::info!("recv_loop shutting down");
                break;
            }
        }
    }

    // Signal session closed
    inner.state.close();
    Ok(())
}

/// Background task that writes frames to the transport
async fn send_loop<T>(
    mut sink: futures::stream::SplitSink<Framed<T, Codec>, Frame>,
    frame_rx: flume::Receiver<Frame>,
    inner: Arc<SessionInner>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
    let close_notifier = inner.state.close_notifier();
    loop {
        tokio::select! {
            result = frame_rx.recv_async() => {
                match result {
                    Ok(frame) => {
                        if let Err(e) = sink.send(frame).await {
                            tracing::error!("Frame send error: {}", e);
                            break;
                        }
                    }
                    Err(_) => {
                        tracing::info!("Frame sender closed");
                        break;
                    }
                }
            }
            _ = close_notifier.notified() => {
                tracing::info!("send_loop shutting down");
                break;
            }
        }
    }

    // Signal session closed
    inner.state.close();
    Ok(())
}

/// Handle an incoming frame based on its command type
async fn handle_frame(frame: Frame, inner: &Arc<SessionInner>) -> Result<()> {
    match frame.cmd {
        Command::Syn => handle_syn_frame(frame, inner).await,
        Command::Fin => handle_fin_frame(frame, inner).await,
        Command::Psh => handle_psh_frame(frame, inner).await,
        Command::Upd { .. } => handle_upd_frame(frame, inner).await,
        Command::Nop => {
            // NOP frames are just keep-alives, no action needed
            Ok(())
        }
    }
}

/// Handle SYN frame (new stream from peer)
async fn handle_syn_frame(frame: Frame, inner: &Arc<SessionInner>) -> Result<()> {
    let stream_id = frame.stream_id;

    // Validate peer stream ID
    inner.validate_peer_stream_id(stream_id)?;

    // Check if stream already exists
    if inner.streams.contains_key(&stream_id) {
        return Err(SmuxError::StreamAlreadyExists(stream_id));
    }

    // Create data channel for this stream
    let (data_tx, data_rx) = flume::unbounded();

    // Create stream state
    let read_waker = Arc::new(std::sync::Mutex::new(None));
    let stream_state = StreamState {
        data_tx,
        is_read_closed: Arc::new(AtomicBool::new(false)),
        read_waker: Arc::clone(&read_waker),
    };

    // Create stream
    let stream = Stream::new(
        stream_id,
        inner.frame_tx.clone(),
        data_rx,
        inner.state.clone(),
        Arc::clone(&inner.config),
        read_waker,
    );

    // Add to streams map
    inner.streams.insert(stream_id, stream_state);

    // Send to accept channel
    if (inner.incoming_streams_tx.send_async(stream).await).is_err() {
        // Accept channel is closed, remove from streams map
        inner.streams.remove(&stream_id);
        return Err(SmuxError::SessionClosed);
    }

    Ok(())
}

/// Handle FIN frame (stream close)
async fn handle_fin_frame(frame: Frame, inner: &Arc<SessionInner>) -> Result<()> {
    let stream_id = frame.stream_id;

    // Find the stream and mark it as read-closed
    if let Some((_, stream_state)) = inner.streams.remove(&stream_id) {
        stream_state
            .is_read_closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        // Close the data channel to signal EOF to the stream
        drop(stream_state.data_tx);
        // Wake the reader so it observes EOF instead of sleeping forever
        if let Some(waker) = stream_state.read_waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    Ok(())
}

/// Handle PSH frame (data)
async fn handle_psh_frame(frame: Frame, inner: &Arc<SessionInner>) -> Result<()> {
    let stream_id = frame.stream_id;
    let data_len = frame.data.len();

    if data_len == 0 {
        return Ok(()); // Empty frame, nothing to do
    }

    let tokens_notifier = inner.state.tokens_returned_notifier();
    let close_notifier = inner.state.close_notifier();

    loop {
        let current_tokens = inner.state.get_tokens();
        if current_tokens > 0 {
            break;
        }
        inner.state.take_tokens(data_len);

        // Wait for tokens to be returned
        tokio::select! {
            _ = tokens_notifier.notified() => {
                // Check again after notification
                continue;
            }
            _ = close_notifier.notified() => {
                // Session is closing, return the tokens we took and drop the frame
                inner.state.return_tokens(data_len);
                return Ok(());
            }
        }
    }

    // Now process the frame
    if let Some(stream_state) = inner.streams.get(&stream_id) {
        match stream_state.data_tx.send_async(frame.data).await {
            Ok(_) => {
                // Data successfully sent to stream: wake the task waiting in poll_read
                if let Some(waker) = stream_state.read_waker.lock().unwrap().take() {
                    waker.wake();
                }
            }
            Err(_) => {
                // Stream receiver is closed, return tokens and remove from map
                inner.state.return_tokens(data_len);
                drop(stream_state);
                inner.streams.remove(&stream_id);
            }
        }
    } else {
        // Stream not found, return tokens (could be for a closed stream)
        inner.state.return_tokens(data_len);
    }

    Ok(())
}

/// Handle UPD frame (flow control update)
async fn handle_upd_frame(frame: Frame, _inner: &Arc<SessionInner>) -> Result<()> {
    let _stream_id = frame.stream_id;

    // Extract window from UPD frame
    if let Command::Upd { .. } = frame.cmd {
        // For now, we just ignore the UPD frame since our simple implementation
        // doesn't implement flow control yet. In a full implementation, we would
        // update the stream's send window and potentially wake up blocked writers.
        // TODO: Implement proper flow control
    }
    // If stream not found, ignore the frame

    Ok(())
}

/// Background task that handles keep-alive functionality
async fn keep_alive_loop(inner: Arc<SessionInner>) -> Result<()> {
    // Only run if keep-alive is enabled
    if !inner.config.enable_keep_alive {
        return Ok(());
    }

    let mut ping_interval = interval(inner.config.keep_alive_interval);
    let mut timeout_interval = interval(inner.config.keep_alive_timeout);
    let close_notifier = inner.state.close_notifier();

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                // Send NOP frame to keep connection alive
                let nop_frame = Frame::new_nop(inner.config.version);
                if inner.frame_tx.send_async(nop_frame).await.is_err() {
                    // Frame channel closed, session is shutting down
                    tracing::info!("keep_alive_loop: frame channel closed");
                    break;
                }
                tracing::debug!("Sent keep-alive NOP frame");
            }
            _ = timeout_interval.tick() => {
                // Check if data was received since last timeout check
                if !inner.state.check_and_reset_data_received() {
                    // No data received, check flow control tokens before deciding to close
                    let available_tokens = inner.state.get_tokens();

                    // Only close if we have enough tokens to receive a frame
                    // If tokens are low, the peer might be unable to send, so don't timeout
                    if available_tokens > 0  {
                        tracing::warn!(
                            "Keep-alive timeout: no data received (tokens: {})",
                            available_tokens
                        );
                        inner.state.close();
                        break;
                    } else {
                        tracing::debug!(
                            "Keep-alive timeout detected but tokens are low ({}), not closing session",
                            available_tokens
                        );
                    }
                } else {
                    tracing::debug!("Data received, keep-alive timeout reset");
                }
            }
            _ = close_notifier.notified() => {
                tracing::info!("keep_alive_loop shutting down");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    fn test_config() -> Config {
        Config::default()
    }

    #[tokio::test]
    async fn test_session_creation() {
        let (client_transport, _server_transport) = tokio::io::duplex(1024);
        let config = test_config();

        let session = Session::client(client_transport, config).await.unwrap();
        assert!(!session.is_closed());
    }

    #[tokio::test]
    async fn test_session_open_stream() {
        let (client_transport, _server_transport) = tokio::io::duplex(1024);
        let config = test_config();

        let session = Session::client(client_transport, config).await.unwrap();
        let stream = session.open_stream().await.unwrap();

        // Client should generate odd stream IDs
        assert_eq!(stream.stream_id() % 2, 1);
    }

    #[tokio::test]
    async fn test_session_close() {
        let (client_transport, _server_transport) = tokio::io::duplex(1024);
        let config = test_config();

        let session = Session::client(client_transport, config).await.unwrap();
        assert!(!session.is_closed());

        session.close().await.unwrap();
        assert!(session.is_closed());

        // Should return Error after close
        let result = session.accept_stream().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_client_server_stream_ids() {
        let (client_transport, server_transport) = tokio::io::duplex(1024);
        let config = test_config();

        let client_session = Session::client(client_transport, config.clone())
            .await
            .unwrap();
        let server_session = Session::server(server_transport, config).await.unwrap();

        let client_stream = client_session.open_stream().await.unwrap();
        let server_stream = server_session.open_stream().await.unwrap();

        // Client should generate odd IDs, server should generate even IDs
        assert_eq!(client_stream.stream_id() % 2, 1);
        assert_eq!(server_stream.stream_id() % 2, 0);
    }

    #[tokio::test]
    async fn test_multiple_streams() {
        let (client_transport, _server_transport) = tokio::io::duplex(1024);
        let config = test_config();

        let session = Session::client(client_transport, config).await.unwrap();

        let stream1 = session.open_stream().await.unwrap();
        let stream2 = session.open_stream().await.unwrap();
        let stream3 = session.open_stream().await.unwrap();

        // All should be unique odd IDs
        assert_eq!(stream1.stream_id(), 1);
        assert_eq!(stream2.stream_id(), 3);
        assert_eq!(stream3.stream_id(), 5);
    }

    #[test]
    fn test_client_stream_id_generation() {
        let inner = SessionInner {
            streams: DashMap::new(),
            config: Arc::new(test_config()),
            incoming_streams_tx: flume::bounded(1).0,
            incoming_streams_rx: flume::bounded(1).1,
            next_stream_id: AtomicU32::new(1),
            is_client: true,
            frame_tx: flume::bounded(1).0,
            state: SessionState::new(1024),
        };

        assert_eq!(inner.next_stream_id().unwrap(), 1);
        assert_eq!(inner.next_stream_id().unwrap(), 3);
        assert_eq!(inner.next_stream_id().unwrap(), 5);
    }

    #[test]
    fn test_server_stream_id_generation() {
        let inner = SessionInner {
            streams: DashMap::new(),
            config: Arc::new(test_config()),
            incoming_streams_tx: flume::bounded(1).0,
            incoming_streams_rx: flume::bounded(1).1,
            next_stream_id: AtomicU32::new(2),
            is_client: false,
            frame_tx: flume::bounded(1).0,
            state: SessionState::new(1024),
        };

        assert_eq!(inner.next_stream_id().unwrap(), 2);
        assert_eq!(inner.next_stream_id().unwrap(), 4);
        assert_eq!(inner.next_stream_id().unwrap(), 6);
    }

    #[test]
    fn test_stream_id_overflow() {
        let inner = SessionInner {
            streams: DashMap::new(),
            config: Arc::new(test_config()),
            incoming_streams_tx: flume::bounded(1).0,
            incoming_streams_rx: flume::bounded(1).1,
            next_stream_id: AtomicU32::new(u32::MAX - 1),
            is_client: true,
            frame_tx: flume::bounded(1).0,
            state: SessionState::new(1024),
        };
        assert!(inner.next_stream_id().is_err());
    }

    #[test]
    fn test_peer_stream_id_validation() {
        let client_inner = SessionInner {
            streams: DashMap::new(),
            config: Arc::new(test_config()),
            incoming_streams_tx: flume::bounded(1).0,
            incoming_streams_rx: flume::bounded(1).1,
            next_stream_id: AtomicU32::new(1),
            is_client: true,
            frame_tx: flume::bounded(1).0,
            state: SessionState::new(1024),
        };
        let server_inner = SessionInner {
            streams: DashMap::new(),
            config: Arc::new(test_config()),
            incoming_streams_tx: flume::bounded(1).0,
            incoming_streams_rx: flume::bounded(1).1,
            next_stream_id: AtomicU32::new(2),
            is_client: false,
            frame_tx: flume::bounded(1).0,
            state: SessionState::new(1024),
        };

        assert!(client_inner.validate_peer_stream_id(2).is_ok());
        assert!(client_inner.validate_peer_stream_id(1).is_err());
        assert!(server_inner.validate_peer_stream_id(1).is_ok());
        assert!(server_inner.validate_peer_stream_id(2).is_err());
        assert!(client_inner.validate_peer_stream_id(0).is_err());
        assert!(server_inner.validate_peer_stream_id(0).is_err());
    }

    #[tokio::test]
    async fn test_concurrent_id_generation() {
        use std::collections::HashSet;
        let (client_transport, _server_transport) = tokio::io::duplex(1024);
        let session = Session::client(client_transport, test_config())
            .await
            .unwrap();
        let session_inner = Arc::clone(&session.inner);

        let mut handles = vec![];
        for _ in 0..20 {
            let inner = Arc::clone(&session_inner);
            let handle = tokio::spawn(async move {
                let mut ids = Vec::new();
                for _ in 0..50 {
                    if let Ok(id) = inner.next_stream_id() {
                        ids.push(id);
                    }
                }
                ids
            });
            handles.push(handle);
        }

        let mut all_ids = Vec::new();
        for handle in handles {
            all_ids.extend(handle.await.unwrap());
        }

        let mut unique_ids = HashSet::new();
        for id in &all_ids {
            assert_eq!(*id % 2, 1, "Client ID should be odd");
            assert!(unique_ids.insert(*id), "Duplicate client ID found");
        }
        assert_eq!(unique_ids.len(), all_ids.len());
        assert!(all_ids.len() >= 1000);
    }

    #[tokio::test]
    async fn test_keep_alive_sends_nop_frames() {
        let (client_transport, server_transport) = tokio::io::duplex(1024);

        // Configure with very short keep-alive interval for testing
        let config = Config {
            keep_alive_interval: Duration::from_millis(100),
            keep_alive_timeout: Duration::from_millis(500),
            enable_keep_alive: true,
            ..Default::default()
        };

        let client_session = Session::client(client_transport, config.clone())
            .await
            .unwrap();
        let server_session = Session::server(server_transport, config).await.unwrap();

        // Wait for keep-alive interval to trigger NOP frame sending
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Both sessions should still be alive due to NOP frames
        assert!(!client_session.is_closed());
        assert!(!server_session.is_closed());

        // Wait a bit more to ensure multiple NOP frames are sent
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(!client_session.is_closed());
        assert!(!server_session.is_closed());
    }

    #[tokio::test]
    async fn test_keep_alive_timeout_closes_session() {
        let (client_transport, _server_transport) = tokio::io::duplex(1024);

        // Configure with very short timeout for testing
        let config = Config {
            keep_alive_interval: Duration::from_millis(50),
            keep_alive_timeout: Duration::from_millis(100),
            enable_keep_alive: true,
            ..Default::default()
        };

        let client_session = Session::client(client_transport, config).await.unwrap();

        // Initially session should be open
        assert!(!client_session.is_closed());

        // Server side is dropped immediately, so no responses to NOP frames
        // Wait for timeout to trigger
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Session should eventually close due to timeout
        let mut retries = 0;
        while !client_session.is_closed() && retries < 10 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            retries += 1;
        }

        assert!(client_session.is_closed());
    }

    #[tokio::test]
    async fn test_keep_alive_with_active_communication() {
        let (client_transport, server_transport) = tokio::io::duplex(1024);

        // Configure with short timeouts for testing
        let config = Config {
            keep_alive_interval: Duration::from_millis(100),
            keep_alive_timeout: Duration::from_millis(200),
            enable_keep_alive: true,
            ..Default::default()
        };

        let client_session = Session::client(client_transport, config.clone())
            .await
            .unwrap();
        let server_session = Session::server(server_transport, config).await.unwrap();

        // Create streams and send data to keep connection active
        let client_stream = client_session.open_stream().await.unwrap();

        // Accept the stream on server side
        let server_handle = tokio::spawn(async move {
            let _server_stream = server_session.accept_stream().await;
            // Keep server session alive
            tokio::time::sleep(Duration::from_millis(500)).await;
        });

        // Wait longer than timeout but with active communication
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Sessions should remain open due to active communication
        assert!(!client_session.is_closed());

        drop(client_stream);
        let _ = server_handle.await;
    }

    #[tokio::test]
    async fn test_keep_alive_disabled() {
        let (client_transport, _server_transport) = tokio::io::duplex(1024);

        // Configure with keep-alive disabled
        let config = Config {
            keep_alive_interval: Duration::from_millis(50),
            keep_alive_timeout: Duration::from_millis(100),
            enable_keep_alive: false,
            ..Default::default()
        };

        let _client_session = Session::client(client_transport, config).await.unwrap();

        // Wait longer than what would be timeout if enabled
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Session should remain open since keep-alive is disabled
        // Note: It might close due to transport issues, but not due to keep-alive timeout
        // We can't easily test this without a more sophisticated setup
    }
}
