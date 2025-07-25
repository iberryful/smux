use crate::{
    Command,
    codec::Codec,
    config::Config,
    error::{Result, SmuxError},
    frame::Frame,
    stream::Stream,
    stream_id::StreamIdGenerator,
};
use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, atomic::AtomicBool};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Notify,
};
use tokio_util::codec::Framed;

/// Stream state tracked by the session for each active stream
#[derive(Debug)]
struct StreamState {
    /// Sender for data chunks to the stream
    data_tx: flume::Sender<Bytes>,
    /// Atomic flag for read closed state
    is_read_closed: Arc<AtomicBool>,
}

/// A multiplexed session that manages multiple streams over a single connection
#[derive(Debug)]
pub struct Session<T> {
    inner: Arc<SessionInner<T>>,
}

/// Internal session state shared between tasks
#[derive(Debug)]
struct SessionInner<T> {
    /// Active streams mapped by stream ID to their state
    streams: DashMap<u32, StreamState>,
    /// Session configuration
    config: Arc<Config>,
    /// Sender for accepting new streams initiated by peer
    incoming_streams_tx: flume::Sender<Stream>,
    /// Receiver for accepting new streams (used by accept_stream)
    incoming_streams_rx: flume::Receiver<Stream>,
    /// Stream ID generator
    stream_id_gen: StreamIdGenerator,
    /// Sender for outgoing frames (to send_loop)
    frame_tx: flume::Sender<Frame>,
    /// Session shutdown signal
    die: Arc<Notify>,
    /// Flag to track if session is closed
    closed: AtomicBool,
    /// Transport type marker
    _transport: std::marker::PhantomData<T>,
}

impl<T> Clone for Session<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Session<T>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
    /// Create a new client session
    pub async fn client(transport: T, config: Config) -> Result<Self> {
        Self::new(transport, config, true).await
    }

    /// Create a new server session
    pub async fn server(transport: T, config: Config) -> Result<Self> {
        Self::new(transport, config, false).await
    }

    /// Internal constructor for both client and server sessions
    async fn new(transport: T, config: Config, is_client: bool) -> Result<Self> {
        let config = Arc::new(config);
        let codec = Codec::new((*config).clone());
        let framed = Framed::new(transport, codec);
        let (sink, stream) = framed.split();

        // Create channels
        let (frame_tx, frame_rx) = flume::bounded(config.max_receive_buffer);
        let (incoming_streams_tx, incoming_streams_rx) = flume::bounded(16);

        // Create session inner
        let inner = Arc::new(SessionInner {
            streams: DashMap::new(),
            config: Arc::clone(&config),
            incoming_streams_tx,
            incoming_streams_rx,
            stream_id_gen: StreamIdGenerator::new(is_client),
            frame_tx,
            die: Arc::new(Notify::new()),
            closed: AtomicBool::new(false),
            _transport: std::marker::PhantomData,
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

        Ok(session)
    }

    /// Open a new outgoing stream
    pub async fn open_stream(&self) -> Result<Stream> {
        if self.inner.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(SmuxError::SessionClosed);
        }

        // Generate new stream ID
        let stream_id = self.inner.stream_id_gen.next()?;

        // Create data channel for this stream
        let (data_tx, data_rx) = flume::unbounded();

        // Create stream state
        let stream_state = StreamState {
            data_tx,
            is_read_closed: Arc::new(AtomicBool::new(false)),
        };

        // Create stream
        let stream = Stream::new(stream_id, self.inner.frame_tx.clone(), data_rx);

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
    pub async fn accept_stream(&self) -> Result<Option<Stream>> {
        if self.inner.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(None);
        }

        let rx = &self.inner.incoming_streams_rx;

        tokio::select! {
            result = rx.recv_async() => {
                match result {
                    Ok(stream) => Ok(Some(stream)),
                    Err(_) => Ok(None), // Channel is closed
                }
            },
            _ = self.inner.die.notified() => Ok(None),
        }
    }

    /// Close the session gracefully
    pub async fn close(&self) -> Result<()> {
        self.inner
            .closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.inner.die.notify_waiters();
        Ok(())
    }

    /// Check if the session is closed
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Background task that reads frames from the transport and dispatches them
async fn recv_loop<T>(
    mut stream: futures::stream::SplitStream<Framed<T, Codec>>,
    inner: Arc<SessionInner<T>>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
    loop {
        tokio::select! {
            frame_result = stream.next() => {
                match frame_result {
                    Some(Ok(frame)) => {
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
            _ = inner.die.notified() => {
                tracing::info!("recv_loop shutting down");
                break;
            }
        }
    }

    // Signal session closed
    inner
        .closed
        .store(true, std::sync::atomic::Ordering::Relaxed);
    inner.die.notify_waiters();
    Ok(())
}

/// Background task that writes frames to the transport
async fn send_loop<T>(
    mut sink: futures::stream::SplitSink<Framed<T, Codec>, Frame>,
    frame_rx: flume::Receiver<Frame>,
    inner: Arc<SessionInner<T>>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
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
            _ = inner.die.notified() => {
                tracing::info!("send_loop shutting down");
                break;
            }
        }
    }

    // Signal session closed
    inner
        .closed
        .store(true, std::sync::atomic::Ordering::Relaxed);
    inner.die.notify_waiters();
    Ok(())
}

/// Handle an incoming frame based on its command type
async fn handle_frame<T>(frame: Frame, inner: &Arc<SessionInner<T>>) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
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
async fn handle_syn_frame<T>(frame: Frame, inner: &Arc<SessionInner<T>>) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
    let stream_id = frame.stream_id;

    // Validate peer stream ID
    inner.stream_id_gen.validate_peer_stream_id(stream_id)?;

    // Check if stream already exists
    if inner.streams.contains_key(&stream_id) {
        return Err(SmuxError::StreamAlreadyExists(stream_id));
    }

    // Create data channel for this stream
    let (data_tx, data_rx) = flume::unbounded();

    // Create stream state
    let stream_state = StreamState {
        data_tx,
        is_read_closed: Arc::new(AtomicBool::new(false)),
    };

    // Create stream
    let stream = Stream::new(stream_id, inner.frame_tx.clone(), data_rx);

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
async fn handle_fin_frame<T>(frame: Frame, inner: &Arc<SessionInner<T>>) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
    let stream_id = frame.stream_id;

    // Find the stream and mark it as read-closed
    if let Some((_, stream_state)) = inner.streams.remove(&stream_id) {
        stream_state
            .is_read_closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        // Close the data channel to signal EOF to the stream
        drop(stream_state.data_tx);
    }

    Ok(())
}

/// Handle PSH frame (data)
async fn handle_psh_frame<T>(frame: Frame, inner: &Arc<SessionInner<T>>) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
    let stream_id = frame.stream_id;

    // Find the stream and send data to it
    if let Some(stream_state) = inner.streams.get(&stream_id) {
        if !frame.data.is_empty() && stream_state.data_tx.try_send(frame.data).is_err() {
            // Stream receiver is closed, remove from map
            drop(stream_state);
            inner.streams.remove(&stream_id);
        }
    }
    // If stream not found, ignore the frame (could be for a closed stream)

    Ok(())
}

/// Handle UPD frame (flow control update)
async fn handle_upd_frame<T>(frame: Frame, _inner: &Arc<SessionInner<T>>) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
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

#[cfg(test)]
mod tests {
    use super::*;

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

        // Should return None after close
        let result = session.accept_stream().await.unwrap();
        assert!(result.is_none());
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
}
