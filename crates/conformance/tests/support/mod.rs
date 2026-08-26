use std::collections::BTreeMap;
use std::io::{ErrorKind, Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct RecordedRequest {
    pub method: String,
    pub target: String,
    pub headers: BTreeMap<String, String>,
    pub body: String,
}

pub struct MockResponse {
    status: u16,
    content_type: Option<String>,
    body: Vec<u8>,
    declared_length: Option<usize>,
}

impl MockResponse {
    pub fn json(value: serde_json::Value) -> Self {
        Self::status_json(200, value)
    }

    pub fn status_json(status: u16, value: serde_json::Value) -> Self {
        Self {
            status,
            content_type: Some("application/json".to_owned()),
            body: serde_json::to_vec(&value).expect("mock JSON should serialize"),
            declared_length: None,
        }
    }

    #[allow(dead_code)]
    pub fn empty(status: u16) -> Self {
        Self {
            status,
            content_type: None,
            body: Vec::new(),
            declared_length: None,
        }
    }

    pub fn oversized(declared_length: usize) -> Self {
        Self {
            status: 200,
            content_type: Some("application/json".to_owned()),
            body: Vec::new(),
            declared_length: Some(declared_length),
        }
    }

    #[allow(dead_code)]
    pub fn with_content_type(mut self, content_type: &str) -> Self {
        self.content_type = Some(content_type.to_owned());
        self
    }
}

pub struct MockServer {
    address: SocketAddr,
    worker: JoinHandle<Vec<RecordedRequest>>,
}

impl MockServer {
    pub fn start(responses: Vec<MockResponse>) -> Self {
        Self::start_handler(responses.len(), {
            let mut responses = responses.into_iter();
            move |_| responses.next().expect("mock response should exist")
        })
    }

    pub fn start_handler<F>(request_count: usize, mut handler: F) -> Self
    where
        F: FnMut(&RecordedRequest) -> MockResponse + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("mock server should bind");
        let address = listener
            .local_addr()
            .expect("mock server should expose its address");
        listener
            .set_nonblocking(true)
            .expect("mock listener should become nonblocking");
        let worker = thread::spawn(move || {
            let mut requests = Vec::with_capacity(request_count);
            let mut deadline = Instant::now() + Duration::from_secs(5);
            while requests.len() < request_count {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .expect("accepted mock stream should become blocking");
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .expect("mock read timeout should apply");
                        let request = read_request(&mut stream);
                        let response = handler(&request);
                        write_response(&mut stream, response);
                        requests.push(request);
                        deadline = Instant::now() + Duration::from_secs(5);
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "mock server received {} of {request_count} expected requests",
                            requests.len()
                        );
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("mock accept failed: {error}"),
                }
            }
            requests
        });
        Self { address, worker }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn finish(self) -> Vec<RecordedRequest> {
        self.worker.join().expect("mock server should finish")
    }
}

fn read_request(stream: &mut TcpStream) -> RecordedRequest {
    const HEADER_END: &[u8] = b"\r\n\r\n";
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut chunk).expect("mock request should read");
        assert!(read > 0, "connection closed before request headers");
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(position) = find_bytes(&bytes, HEADER_END) {
            break position + HEADER_END.len();
        }
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec())
        .expect("HTTP request headers should be UTF-8");
    let mut lines = headers.split("\r\n");
    let request_line = lines.next().expect("request line should exist");
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .expect("request method should exist")
        .to_owned();
    let target = request_parts
        .next()
        .expect("request target should exist")
        .to_owned();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect::<BTreeMap<_, _>>();
    let content_length = headers
        .get("content-length")
        .map(|length| {
            length
                .parse::<usize>()
                .expect("content length should parse")
        })
        .unwrap_or_default();
    while bytes.len() - header_end < content_length {
        let read = stream.read(&mut chunk).expect("mock body should read");
        assert!(read > 0, "connection closed before request body");
        bytes.extend_from_slice(&chunk[..read]);
    }
    let body = String::from_utf8(bytes[header_end..header_end + content_length].to_vec())
        .expect("HTTP request body should be UTF-8");
    RecordedRequest {
        method,
        target,
        headers,
        body,
    }
}

fn write_response(stream: &mut TcpStream, response: MockResponse) {
    let reason = match response.status {
        200 => "OK",
        204 => "No Content",
        404 => "Not Found",
        406 => "Not Acceptable",
        409 => "Conflict",
        _ => "Test Response",
    };
    write!(stream, "HTTP/1.1 {} {reason}\r\n", response.status)
        .expect("mock response status should write");
    if let Some(content_type) = response.content_type {
        write!(stream, "Content-Type: {content_type}\r\n").expect("mock content type should write");
    }
    write!(
        stream,
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        response.declared_length.unwrap_or(response.body.len())
    )
    .expect("mock response headers should write");
    // A bounded-body test deliberately closes after reading Content-Length,
    // so a broken pipe here is an expected mock transport race.
    let _ = stream.write_all(&response.body);
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
