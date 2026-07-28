use std::{collections::VecDeque, io, net::SocketAddr, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex, oneshot},
    task::JoinHandle,
};

#[derive(Debug, Clone)]
pub struct ScriptedResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body_chunks: Vec<Vec<u8>>,
}

impl ScriptedResponse {
    pub fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body_chunks: vec![body.into()],
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn with_chunks(mut self, body_chunks: Vec<Vec<u8>>) -> Self {
        self.body_chunks = body_chunks;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedHttpRequest {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RecordedHttpRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn header_values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.headers
            .iter()
            .filter(move |(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Debug)]
pub struct ScriptedHttpServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedHttpRequest>>>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl ScriptedHttpServer {
    pub async fn spawn(responses: Vec<ScriptedResponse>) -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded_requests = Arc::clone(&requests);
        let (shutdown, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut responses = VecDeque::from(responses);
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => return Ok(()),
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        let (request, stream) = read_request(stream).await?;
                        recorded_requests.lock().await.push(request);
                        let response = responses.pop_front().ok_or_else(|| {
                            io::Error::new(io::ErrorKind::UnexpectedEof, "scripted response queue exhausted")
                        })?;
                        write_response(stream, response).await?;
                        if responses.is_empty() {
                            return Ok(());
                        }
                    }
                }
            }
        });

        Ok(Self {
            addr,
            requests,
            shutdown: Some(shutdown),
            task: Some(task),
        })
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    pub async fn requests(&self) -> Vec<RecordedHttpRequest> {
        self.requests.lock().await.clone()
    }

    pub async fn finish(&mut self) -> io::Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task
            .take()
            .expect("scripted HTTP server already finished")
            .await
            .expect("scripted HTTP server task panicked")
    }
}

impl Drop for ScriptedHttpServer {
    fn drop(&mut self) {
        self.shutdown.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn read_request(stream: TcpStream) -> io::Result<(RecordedHttpRequest, TcpStream)> {
    let mut reader = BufReader::new(stream);
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line).await? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed before request line",
        ));
    }
    let request_line = std::str::from_utf8(&line)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request method"))?
        .to_string();
    let target = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request target"))?
        .to_string();
    let version = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP version"))?;
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") || parts.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid HTTP request line",
        ));
    }

    let mut headers = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before header terminator",
            ));
        }
        if line == b"\n" || line == b"\r\n" {
            break;
        }
        let header_line = std::str::from_utf8(&line)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let (name, value) = header_line
            .split_once(':')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed HTTP header"))?;
        let name = name.trim();
        if name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "empty HTTP header name",
            ));
        }
        headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
    }

    let content_lengths = headers
        .iter()
        .filter(|(name, _)| name == "content-length")
        .map(|(_, value)| {
            value.parse::<usize>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid Content-Length: {error}"),
                )
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    if content_lengths
        .windows(2)
        .any(|values| values[0] != values[1])
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "conflicting Content-Length headers",
        ));
    }
    let content_length = content_lengths.first().copied().unwrap_or(0);
    if headers.iter().any(|(name, _)| name == "transfer-encoding") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunked request bodies are unsupported by the scripted server",
        ));
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).await?;

    let stream = reader.into_inner();
    Ok((
        RecordedHttpRequest {
            method,
            target,
            headers,
            body,
        },
        stream,
    ))
}

async fn write_response(mut stream: TcpStream, response: ScriptedResponse) -> io::Result<()> {
    let body_length: usize = response.body_chunks.iter().map(Vec::len).sum();
    let status = http::StatusCode::from_u16(response.status)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let reason = status.canonical_reason().unwrap_or("Test Response");
    let mut headers = response.headers;
    if let Some((_, value)) = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
    {
        let declared_length = value.parse::<usize>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid Content-Length: {error}"),
            )
        })?;
        if declared_length != body_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scripted Content-Length does not match body",
            ));
        }
    } else {
        headers.push(("content-length".to_string(), body_length.to_string()));
    }
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("connection"))
    {
        headers.push(("connection".to_string(), "close".to_string()));
    }

    stream
        .write_all(format!("HTTP/1.1 {} {}\r\n", status.as_u16(), reason).as_bytes())
        .await?;
    for (name, value) in headers {
        stream
            .write_all(format!("{}: {}\r\n", name, value).as_bytes())
            .await?;
    }
    stream.write_all(b"\r\n").await?;
    for chunk in response.body_chunks {
        stream.write_all(&chunk).await?;
        stream.flush().await?;
        tokio::task::yield_now().await;
    }
    stream.shutdown().await
}
