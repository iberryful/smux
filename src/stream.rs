use crate::{
    config::Config,
    error::{Result, SmuxError},
    frame::Frame,
    session::SessionState,
};
use bytes::Bytes;
use flume;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A multiplexed stream within a smux session.
///
/// `Stream` represents a single logical stream within a multiplexed session.
/// It implements `AsyncRead` and `AsyncWrite` for seamless integration with
/// tokio-based applications.
///
/// Streams are created through `Session::open_stream()` (client side) or
/// `Session::accept_stream()` (server side). Each stream provides independent,
/// bidirectional communication over the shared underlying connection.
///
/// # Examples
///
/// ## Using a stream for reading and writing
///
/// ```rust,no_run
/// use smux::{Config, Session};
/// use tokio::net::TcpStream;
/// use tokio::io::{AsyncReadExt, AsyncWriteExt};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let conn = TcpStream::connect("127.0.0.1:8080").await?;
///     let mut session = Session::client(conn, Config::default()).await?;
///     
///     let mut stream = session.open_stream().await?;
///     
///     // Write some data
///     stream.write_all(b"Hello, world!").await?;
///     
///     // Read the response
///     let mut buffer = vec![0; 1024];
///     let n = stream.read(&mut buffer).await?;
///     println!("Read {} bytes: {:?}", n, &buffer[..n]);
///     
///     Ok(())
/// }
/// ```
///
/// Implements AsyncRead and AsyncWrite for seamless integration with tokio.
#[derive(Debug)]
pub struct Stream {
    /// Stream ID
    stream_id: u32,
    /// Sends frames to the session's send_loop
    frame_tx: flume::Sender<Frame>,
    /// Receives incoming data chunks from the session
    data_rx: flume::Receiver<Bytes>,
    /// A temporary buffer for when a user reads only part of a data chunk
    current_chunk: Option<Bytes>,
    /// Set when a FIN frame is received (no more data from peer)
    is_read_closed: Arc<AtomicBool>,
    /// Set when the stream is closed for writing
    is_write_closed: Arc<AtomicBool>,
    /// Session state
    session_state: SessionState,
    /// Reference to session config for max_frame_size
    config: Arc<Config>,
    /// Cumulative number of bytes consumed by the application (smux v2 flow control)
    upd_consumed: u32,
    /// Value of `upd_consumed` at the moment the last UPDATE frame was sent
    upd_last_sent: u32,
}

impl Stream {
    pub(crate) fn new(
        stream_id: u32,
        frame_tx: flume::Sender<Frame>,
        data_rx: flume::Receiver<Bytes>,
        session_state: SessionState,
        config: Arc<Config>,
    ) -> Self {
        Self {
            stream_id,
            frame_tx,
            data_rx,
            current_chunk: None,
            is_read_closed: Arc::new(AtomicBool::new(false)),
            is_write_closed: Arc::new(AtomicBool::new(false)),
            session_state,
            config,
            upd_consumed: 0,
            upd_last_sent: 0,
        }
    }

    /// Get the stream ID
    pub fn stream_id(&self) -> u32 {
        self.stream_id
    }

    /// Check if the stream is closed for reading
    pub fn is_read_closed(&self) -> bool {
        self.is_read_closed.load(Ordering::Relaxed)
    }

    /// Check if the stream is closed for writing
    pub fn is_write_closed(&self) -> bool {
        self.is_write_closed.load(Ordering::Relaxed)
    }

    /// Check if the stream is fully closed
    pub fn is_closed(&self) -> bool {
        self.is_read_closed() && self.is_write_closed()
    }

    /// Close the stream for writing (sends FIN frame)
    pub async fn close(&mut self) -> Result<()> {
        if !self.is_write_closed.swap(true, Ordering::Relaxed) {
            let fin_frame = Frame::new_fin(1, self.stream_id); // TODO: Use actual version
            self.frame_tx
                .send_async(fin_frame)
                .await
                .map_err(|_| SmuxError::SessionClosed)?;
        }
        Ok(())
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        if this.is_read_closed.load(Ordering::Relaxed) {
            return Poll::Ready(Ok(()));
        }

        if this.session_state.is_closed() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Session is closed",
            )));
        }

        if let Some(ref mut chunk) = this.current_chunk {
            let to_copy = std::cmp::min(chunk.len(), buf.remaining());
            if to_copy > 0 {
                let data = chunk.split_to(to_copy);
                buf.put_slice(&data);

                // Return tokens for consumed data
                this.session_state.return_tokens(to_copy);

                // If chunk is now empty, remove it
                if chunk.is_empty() {
                    this.current_chunk = None;
                }

                // smux v2 flow control: tell the peer how much we consumed
                this.maybe_send_update(to_copy as u32);

                return Poll::Ready(Ok(()));
            }
        }

        match this.data_rx.try_recv() {
            Ok(mut chunk) => {
                let to_copy = std::cmp::min(chunk.len(), buf.remaining());
                if to_copy > 0 {
                    let data = chunk.split_to(to_copy);
                    buf.put_slice(&data);

                    // Return tokens for consumed data
                    this.session_state.return_tokens(to_copy);

                    // smux v2 flow control: tell the peer how much we consumed
                    this.maybe_send_update(to_copy as u32);

                    if !chunk.is_empty() {
                        this.current_chunk = Some(chunk);
                    }
                } else {
                    this.current_chunk = Some(chunk);
                }
                Poll::Ready(Ok(()))
            }
            Err(flume::TryRecvError::Disconnected) => Poll::Ready(Ok(())),
            Err(flume::TryRecvError::Empty) => {
                if this.is_read_closed.load(Ordering::Relaxed) {
                    Poll::Ready(Ok(()))
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
    }
}

/// Minimum amount of consumed bytes before an UPDATE frame is sent.
/// Matches the granularity used by the Go implementation (32 KiB).
const UPDATE_THRESHOLD: u32 = 32 * 1024;

impl Stream {
    /// Sends a smux v2 `UPD` frame when the application has consumed at least
    /// `UPDATE_THRESHOLD` bytes since the previous update.
    ///
    /// Without these frames the peer's send window is never reopened: the
    /// connection works for the first ~window bytes and then goes silent.
    fn maybe_send_update(&mut self, consumed: u32) {
        if self.config.version < 2 || consumed == 0 {
            return;
        }
        self.upd_consumed = self.upd_consumed.wrapping_add(consumed);
        if self.upd_consumed.wrapping_sub(self.upd_last_sent) < UPDATE_THRESHOLD {
            return;
        }
        let frame = Frame::new_upd(
            self.config.version,
            self.stream_id,
            self.upd_consumed,
            self.config.max_stream_buffer as u32,
        );
        if self.frame_tx.try_send(frame).is_ok() {
            self.upd_last_sent = self.upd_consumed;
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        // Check if session is closed - if so, return BrokenPipe error
        if this.session_state.is_closed() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Session is closed",
            )));
        }

        if this.is_write_closed.load(Ordering::Relaxed) {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Stream is closed for writing",
            )));
        }

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        // For now, we'll use a simple approach and send all data in one frame
        // TODO: Implement proper fragmentation and flow control
        let max_frame_size = this.config.max_frame_size;
        // Account for frame header size when calculating max payload size
        let max_payload_size = max_frame_size.saturating_sub(crate::frame::HEADER_SIZE);
        let chunk_size = std::cmp::min(buf.len(), max_payload_size);
        let data = Bytes::copy_from_slice(&buf[..chunk_size]);

        let psh_frame = Frame::new_psh(1, this.stream_id, data); // TODO: Use actual version

        // Try to send the frame
        match this.frame_tx.try_send(psh_frame) {
            Ok(_) => Poll::Ready(Ok(chunk_size)),
            Err(flume::TrySendError::Full(_)) => {
                // Channel is full, return pending and will be woken when ready
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(flume::TrySendError::Disconnected(_)) => {
                // Channel closed
                Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Session is closed",
                )))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // For now, we don't buffer writes, so flush is a no-op
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        if this.is_write_closed.load(Ordering::Relaxed) {
            return Poll::Ready(Ok(()));
        }

        let fin_frame = Frame::new_fin(1, this.stream_id); // TODO: Use actual version

        match this.frame_tx.try_send(fin_frame) {
            Ok(_) => {
                this.is_write_closed.store(true, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
            Err(flume::TrySendError::Full(_)) => {
                // Channel is full, wake and try again
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(flume::TrySendError::Disconnected(_)) => {
                // Channel closed, consider it shutdown
                this.is_write_closed.store(true, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        // If the stream is not closed for writing, try to send a FIN frame
        if !self.is_write_closed.load(Ordering::Relaxed) {
            self.is_write_closed.store(true, Ordering::Relaxed);
            let fin_frame = Frame::new_fin(1, self.stream_id); // TODO: Use actual version

            // Try to send FIN frame (best effort, ignore errors)
            let _ = self.frame_tx.try_send(fin_frame);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Command;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_stream_creation() {
        let (frame_tx, _) = flume::bounded(1);
        let (_, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(65536);
        let config = Arc::new(crate::Config::default());

        let stream = Stream::new(123, frame_tx, data_rx, session_state, config);
        assert_eq!(stream.stream_id(), 123);
        assert!(!stream.is_read_closed());
        assert!(!stream.is_write_closed());
        assert!(!stream.is_closed());
    }

    #[tokio::test]
    async fn test_stream_read_with_data() {
        let (frame_tx, _) = flume::bounded(1);
        let (data_tx, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(65536);
        let config = Arc::new(crate::Config::default());

        let mut stream = Stream::new(123, frame_tx, data_rx, session_state, config);

        // Send data directly to the data channel
        let test_data = Bytes::from("hello world");
        data_tx.send(test_data.clone()).unwrap();

        // Read from stream
        let mut buf = [0u8; 20];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(n, test_data.len());
        assert_eq!(&buf[..n], test_data.as_ref());
    }

    #[tokio::test]
    async fn test_stream_read_eof() {
        let (frame_tx, _) = flume::bounded(1);
        let (data_tx, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(65536);
        let config = Arc::new(crate::Config::default());

        let mut stream = Stream::new(123, frame_tx, data_rx, session_state, config);

        // Close the data channel to simulate EOF
        drop(data_tx);

        // Read should return 0 (EOF)
        let mut buf = [0u8; 20];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_stream_write() {
        let (frame_tx, frame_rx) = flume::bounded(1);
        let (_, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(65536);
        let config = Arc::new(crate::Config::default());

        let mut stream = Stream::new(123, frame_tx, data_rx, session_state, config);

        // Write some data
        let test_data = b"hello world";
        let n = stream.write(test_data).await.unwrap();
        assert_eq!(n, test_data.len());

        // Verify PSH frame was sent
        let frame = frame_rx.recv_async().await.unwrap();
        assert_eq!(frame.cmd, Command::Psh);
        assert_eq!(frame.stream_id, 123);
        assert_eq!(frame.data.as_ref(), test_data);
    }

    #[tokio::test]
    async fn test_stream_shutdown() {
        let (frame_tx, frame_rx) = flume::bounded(1);
        let (_, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(65536);
        let config = Arc::new(crate::Config::default());

        let mut stream = Stream::new(123, frame_tx, data_rx, session_state, config);

        // Shutdown the stream
        stream.shutdown().await.unwrap();
        assert!(stream.is_write_closed());

        // Verify FIN frame was sent
        let frame = frame_rx.recv_async().await.unwrap();
        assert_eq!(frame.cmd, Command::Fin);
        assert_eq!(frame.stream_id, 123);
    }

    #[tokio::test]
    async fn test_stream_close() {
        let (frame_tx, frame_rx) = flume::bounded(1);
        let (_, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(65536);
        let config = Arc::new(crate::Config::default());

        let mut stream = Stream::new(123, frame_tx, data_rx, session_state, config);

        // Close the stream
        stream.close().await.unwrap();
        assert!(stream.is_write_closed());

        // Verify FIN frame was sent
        let frame = frame_rx.recv_async().await.unwrap();
        assert_eq!(frame.cmd, Command::Fin);
        assert_eq!(frame.stream_id, 123);
    }

    #[tokio::test]
    async fn test_stream_multiple_reads() {
        let (frame_tx, _) = flume::bounded(1);
        let (data_tx, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(65536);
        let config = Arc::new(crate::Config::default());

        let mut stream = Stream::new(123, frame_tx, data_rx, session_state, config);

        // Send multiple data chunks
        let data1 = Bytes::from("hello ");
        let data2 = Bytes::from("world");

        data_tx.send(data1).unwrap();
        data_tx.send(data2).unwrap();

        // Read all data
        let mut buf = [0u8; 20];
        let n1 = stream.read(&mut buf).await.unwrap();
        let n2 = stream.read(&mut buf[n1..]).await.unwrap();

        let total_len = n1 + n2;
        let expected = b"hello world";
        assert_eq!(total_len, expected.len());
        assert_eq!(&buf[..total_len], expected);
    }

    #[tokio::test]
    async fn test_stream_drop_sends_fin() {
        let (frame_tx, frame_rx) = flume::bounded(1);
        let (_, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(65536);
        let config = Arc::new(crate::Config::default());

        {
            let _stream = Stream::new(123, frame_tx, data_rx, session_state, config);
            // Stream is dropped here
        }

        // Verify FIN frame was sent on drop
        let frame = frame_rx.try_recv().unwrap();
        assert_eq!(frame.cmd, Command::Fin);
        assert_eq!(frame.stream_id, 123);
    }

    #[tokio::test]
    async fn test_stream_integration_with_session() {
        use crate::{Config, Session};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Create a duplex connection for client-server communication
        let (client_transport, server_transport) = tokio::io::duplex(1024);
        let config = Config::default();

        // Create client and server sessions
        let client_session = Session::client(client_transport, config.clone())
            .await
            .unwrap();
        let server_session = Session::server(server_transport, config).await.unwrap();

        // Client opens a stream
        let mut client_stream = client_session.open_stream().await.unwrap();

        // Give some time for the SYN frame to propagate
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Server accepts the stream
        let mut server_stream = server_session.accept_stream().await.unwrap();

        // Verify stream IDs match
        assert_eq!(client_stream.stream_id(), server_stream.stream_id());

        // Client writes data
        let test_data = b"Hello from client!";
        client_stream.write_all(test_data).await.unwrap();

        // Give some time for the data to propagate
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Server reads data
        let mut buffer = [0u8; 64];
        let n = server_stream.read(&mut buffer).await.unwrap();
        assert_eq!(n, test_data.len());
        assert_eq!(&buffer[..n], test_data);

        // Server writes response
        let response_data = b"Hello from server!";
        server_stream.write_all(response_data).await.unwrap();

        // Give some time for the data to propagate
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Client reads response
        let mut response_buffer = [0u8; 64];
        let n = client_stream.read(&mut response_buffer).await.unwrap();
        assert_eq!(n, response_data.len());
        assert_eq!(&response_buffer[..n], response_data);

        // Close streams
        client_stream.shutdown().await.unwrap();
        server_stream.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_immediate_close_on_session_closed() {
        let (frame_tx, _frame_rx) = flume::bounded(1);
        let (data_tx, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(65536);

        let mut stream = Stream::new(
            123,
            frame_tx,
            data_rx,
            session_state.clone(),
            Arc::new(crate::Config::default()),
        );

        // Initially, read and write should work normally
        assert!(!session_state.is_closed());

        // Send some test data for reading
        data_tx.send(Bytes::from("test data")).unwrap();
        let mut read_buf = [0u8; 10];
        let n = stream.read(&mut read_buf).await.unwrap();
        assert_eq!(n, 9); // "test data" length
        assert_eq!(&read_buf[..n], b"test data");

        // Write should work
        let write_result = stream.write(b"hello").await;
        assert!(write_result.is_ok());

        // Now simulate session closure
        session_state.close();

        // Read should immediately return EOF (0 bytes)
        let mut read_buf2 = [0u8; 10];
        assert!(stream.read(&mut read_buf2).await.is_err());

        // Write should immediately return BrokenPipe error
        let write_result = stream.write(b"should fail").await;
        assert!(write_result.is_err());
        let err = write_result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
        assert!(err.to_string().contains("Session is closed"));
    }
    #[tokio::test]
    async fn test_upd_frame_sent_after_consuming_threshold() {
        let (frame_tx, frame_rx) = flume::bounded(8);
        let (data_tx, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(1 << 20);
        let mut config = crate::Config::default();
        config.version = 2;
        config.max_stream_buffer = 256 * 1024;
        let mut stream = Stream::new(123, frame_tx, data_rx, session_state, Arc::new(config));

        // меньше порога: UPDATE не отправляется
        data_tx.send(Bytes::from(vec![7u8; 16 * 1024])).unwrap();
        let mut first = vec![0u8; 16 * 1024];
        stream.read_exact(&mut first).await.unwrap();
        assert!(
            frame_rx.try_recv().is_err(),
            "unexpected UPDATE below the threshold"
        );

        // перешли порог: ожидаем один UPDATE с накопленными 48 КБ
        data_tx.send(Bytes::from(vec![7u8; 32 * 1024])).unwrap();
        let mut second = vec![0u8; 32 * 1024];
        stream.read_exact(&mut second).await.unwrap();
        let frame = frame_rx.try_recv().expect("UPDATE frame expected");
        assert_eq!(frame.version, 2);
        assert_eq!(frame.stream_id, 123);
        assert_eq!(
            frame.cmd,
            Command::Upd {
                consumed: 48 * 1024,
                window: 256 * 1024
            }
        );
    }
    #[tokio::test]
    async fn test_no_upd_frames_for_v1() {
        let (frame_tx, frame_rx) = flume::bounded(8);
        let (data_tx, data_rx) = flume::unbounded();
        let session_state = crate::session::SessionState::new(1 << 20);
        let mut stream = Stream::new(
            123,
            frame_tx,
            data_rx,
            session_state,
            Arc::new(crate::Config::default()), // v1 by default
        );

        data_tx.send(Bytes::from(vec![1u8; 64 * 1024])).unwrap();
        let mut buf = vec![0u8; 64 * 1024];
        stream.read_exact(&mut buf).await.unwrap();

        // v1 has no window updates: nothing may be sent
        assert!(frame_rx.try_recv().is_err(), "v1 must not send UPD frames");
    }

}
