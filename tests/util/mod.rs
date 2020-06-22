#![allow(dead_code, unused_macros)] // Note: not all tests use all functions/types.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, Once};
use std::task::{self, Poll};
use std::thread::sleep;
use std::time::Duration;

use futures_util::task::noop_waker;
use log::LevelFilter;

/// Macro to create `start_stored` function to start a stored process.
macro_rules! start_stored_fn {
    (
        // Starts a new stored process for each configuration.
        &[ $( $conf_path: expr ),* ],
        // Removes all the old database files.
        &[ $( $remove_db_path: expr ),* ],
        // Log severity that gets printed.
        $filter: ident $(,)*
    ) => {
        /// Start the stored server.
        fn start_stored() -> $crate::util::Proc<'static> {
            lazy_static::lazy_static! {
                static ref PROC: $crate::util::ProcLock = $crate::util::ProcLock::new(None);
            }

            static REMOVE: std::sync::Once = std::sync::Once::new();
            REMOVE.call_once(|| {
                // Remove the old databases from previous tests.
                $( let _ = fs::remove_dir_all($remove_db_path); )*
            });

            $crate::util::start_stored(&[$( $conf_path ),*], &PROC, $filter)
        }
    };
}

pub fn poll_wait<Fut>(mut future: Pin<&mut Fut>) -> Fut::Output
where
    Fut: Future,
{
    // This is not great.
    let waker = noop_waker();
    let mut ctx = task::Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut ctx) {
            Poll::Ready(result) => return result,
            Poll::Pending => continue,
        }
    }
}

pub struct Proc<'a> {
    lock: &'a ProcLock,
    processes: Arc<Box<[ChildCommand]>>,
}

impl<'a> Drop for Proc<'a> {
    fn drop(&mut self) {
        // We `lock` first to create a queue of tests that is done. After we got
        // the lock we'll check if we're the last test. If we did this without
        // holding the lock two tests currently ending could both determine
        // there not the last test and never stop the process.
        let mut processes = self.lock.lock().unwrap();
        if Arc::strong_count(&self.processes) == 2 {
            // Take the (second to) last arc pointer to the process. Which means
            // that if we get dropped the process is stopped.
            processes.take();
        }
    }
}

pub type ProcLock = Mutex<Option<Arc<Box<[ChildCommand]>>>>;

/// Build and start the stored server.
///
/// It attempts to start a single process, which stops itself once all tests are
/// done (for which it needs a `ProcLock`).
///
/// If `filter` is not `LevelFilter::Off` it will set the standard out and error
/// to inherit from this process, making all logs available.
pub fn start_stored<'a>(conf_paths: &[&str], lock: &'a ProcLock, filter: LevelFilter) -> Proc<'a> {
    build_stored();

    let mut proc = lock.lock().unwrap();
    let processes = if let Some(proc) = &mut *proc {
        proc.clone()
    } else {
        let mut processes = Vec::with_capacity(conf_paths.len());

        for conf_path in conf_paths {
            let mut child = Command::new(env!("CARGO_BIN_EXE_stored"));

            if filter == LevelFilter::Off {
                child.stderr(Stdio::null()).stdout(Stdio::null());
            } else {
                child
                    .stderr(Stdio::inherit())
                    .stdout(Stdio::inherit())
                    .env("LOG_LEVEL", filter.to_string())
                    .env("LOG_TARGET", "stored");
            }

            let child = child
                .stdin(Stdio::null())
                .arg(conf_path)
                .spawn()
                .map(|inner| ChildCommand { inner })
                .expect("unable to start server");

            processes.push(child);
        }

        // Give the processes some time to start and sync up.
        if conf_paths.len() == 1 {
            sleep(Duration::from_millis(200));
        } else {
            sleep(Duration::from_millis(processes.len() as u64 * 300));
        }

        let processes = Arc::new(processes.into_boxed_slice());
        proc.replace(processes.clone());
        processes
    };
    Proc { lock, processes }
}

fn build_stored() {
    static BUILD: Once = Once::new();
    static mut BUILD_SUCCESS: bool = false;

    BUILD.call_once(|| {
        let output = Command::new("cargo")
            .args(&["build", "--bin", "stored"])
            .output()
            .expect("unable to build server");

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("failed to build example: {}\n\n{}", stdout, stderr);
        }

        unsafe { BUILD_SUCCESS = true }
    });
    assert!(unsafe { BUILD_SUCCESS }, "build failed");
}

/// Wrapper around a `command::Child` that kills the process when dropped, even
/// if the test failed. Sometimes the child command would survive the test when
/// running then in a loop (e.g. with `cargo watch`). This caused problems when
/// trying to bind to the same port again.
pub struct ChildCommand {
    inner: Child,
}

impl ChildCommand {
    /// Ensure the server process is still alive.
    pub fn assert_ok(&self) {
        const CHECK: libc::c_int = 0;
        let id = self.inner.id();
        if unsafe { libc::kill(id as libc::pid_t, CHECK) != 0 } {
            let err = io::Error::last_os_error();
            panic!("process failed: {}", err);
        }
    }
}

impl Drop for ChildCommand {
    fn drop(&mut self) {
        let _ = self.inner.kill();
        self.inner.wait().expect("can't wait on child process");
    }
}

/// Helper type that runs `F` if the thread is panicking (e.g. when an assertion
/// failed) to provided extra context to the error.
pub struct OnPanic<F>(pub F)
where
    F: FnMut();

impl<F> Drop for OnPanic<F>
where
    F: FnMut(),
{
    fn drop(&mut self) {
        if std::thread::panicking() {
            (self.0)();
        }
    }
}

#[macro_use]
pub mod http {
    //! Simple http client.

    use std::io::{Read, Write};
    use std::mem::replace;
    use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpStream};
    use std::str::{self, FromStr};
    use std::time::Duration;

    use chrono::{Datelike, Timelike, Utc};
    use http::header::{
        HeaderMap, HeaderName, CONTENT_LENGTH, CONTENT_TYPE, DATE, LAST_MODIFIED, SERVER,
    };
    use http::status::StatusCode;
    use http::{HeaderValue, Request, Response, Uri, Version};

    /// Make a request and check the response.
    macro_rules! request {
        (
            // GET request port, path and body.
            GET $port: expr, $path: expr, $body: expr,
            // The wanted status, body and headers in the response.
            expected: $want_status: expr, $want_body: expr,
            $($header_name: ident => $header_value: expr),*,
        ) => {{
            request!(
                "GET", $port, $path, $body,
                /* No request headers. */;
                expected: $want_status, $want_body,
                $($header_name => $header_value),*,
            );
        }};
        (
            // HEAD request port, path and body.
            HEAD $port: expr, $path: expr, $body: expr,
            // The wanted status and headers in the response.
            expected: $want_status: expr,
            $($header_name: ident => $header_value: expr),*,
        ) => {{
            request!(
                "HEAD", $port, $path, $body,
                /* No request headers. */;
                expected: $want_status, &[],
                $($header_name => $header_value),*,
            );
        }};
        (
            // POST request port, path, body and headers.
            POST $port: expr, $path: expr, $body: expr,
            $($r_header_name: ident => $r_header_value: expr),*;
            // The wanted status, body and headers in the response.
            expected: $want_status: expr, $want_body: expr,
            $($header_name: ident => $header_value: expr),*,
        ) => {{
            request!(
                "POST", $port, $path, $body,
                $($r_header_name => $r_header_value),*;
                expected: $want_status, $want_body,
                $($header_name => $header_value),*,
            );
        }};
        (
            // DELETE request port, path, body and headers.
            DELETE $port: expr, $path: expr, $body: expr,
            // The wanted status, body and headers in the response.
            expected: $want_status: expr, $want_body: expr,
            $($header_name: ident => $header_value: expr),*,
        ) => {{
            request!(
                "DELETE", $port, $path, $body,
                /* No request headers. */;
                expected: $want_status, $want_body,
                $($header_name => $header_value),*,
            );
        }};
        (
            $method: expr, $port: expr, $path: expr, $body: expr,
            $($r_header_name: ident => $r_header_value: expr),*;
            expected: $want_status: expr, $want_body: expr,
            $($header_name: ident => $header_value: expr),*,
        ) => {{
            let _ctx = $crate::util::OnPanic(|| {
                eprintln!("Request failed: {} to localhost:{}{}",
                    $method, $port, $path);
            });
            let response = $crate::util::http::request(
                $method, $path, $port,
                &[ $( ($r_header_name, $r_header_value), )* ],
                $body
            );
            $crate::util::http::assert_response(
                response, $want_status,
                &[ $( ($header_name, $header_value), )* ],
                $want_body
            );
        }};
    }

    pub mod body {
        //! HTTP bodies and there lengths (in text for the "Content-Length"
        //! header).

        pub const EMPTY: &[u8] = b"";
        pub const EMPTY_LEN: &str = "0";

        pub const OK: &[u8] = b"OK";
        pub const OK_LEN: &str = "2";

        pub const NOT_FOUND: &[u8] = b"Not found";
        pub const NOT_FOUND_LEN: &str = "9";

        pub const INVALID_KEY: &[u8] = b"Invalid key in URI";
        pub const INVALID_KEY_LEN: &str = "18";

        pub const LENGTH_REQUIRED: &[u8] = b"Missing required content length header";
        pub const LENGTH_REQUIRED_LEN: &str = "38";

        pub const PAYLOAD_TOO_LARGE: &[u8] = b"Blob too large";
        pub const PAYLOAD_TOO_LARGE_LEN: &str = "14";

        pub const INCOMPLETE: &[u8] = b"Incomplete blob";
        pub const INCOMPLETE_LEN: &str = "14";

        pub const UNEXPECTED_BODY: &[u8] = b"Unexpected request body";
        pub const UNEXPECTED_BODY_LEN: &str = "23";
    }

    pub mod header {
        //! Common headers values.

        pub const PLAIN_TEXT: &str = "text/plain; charset=utf-8";
        pub const CLOSE: &str = "close";
        pub const KEEP_ALIVE: &str = "keep-alive";
    }

    /// Make a single HTTP request.
    pub fn request(
        method: &'static str,
        path: &'static str,
        port: u16,
        headers: &[(HeaderName, &'static str)],
        body: &[u8],
    ) -> Response<Vec<u8>> {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let address = SocketAddr::new(ip, port);
        let mut stream = TcpStream::connect(address).expect("failed to connect");

        // Write the request and shutdown the writing side to indicate we're
        // only sending a single request.
        write_request(&mut stream, method, path, headers, body);
        stream
            .shutdown(Shutdown::Write)
            .expect("failed to shutdown writing side");

        let mut responses = read_responses(&mut stream, method == "HEAD");
        assert_eq!(responses.len(), 1, "unexpected number of responses");
        responses.pop().unwrap()
    }

    const IO_TIMEOUT: Option<Duration> = Some(Duration::from_secs(15));

    /// Write a HTTP request to `stream`.
    pub fn write_request(
        stream: &mut TcpStream,
        method: &'static str,
        path: &'static str,
        headers: &[(HeaderName, &'static str)],
        body: &[u8],
    ) {
        let mut req = Request::builder()
            .method(method)
            .uri(Uri::from_static(path))
            .version(Version::HTTP_11);

        let req_headers = req.headers_mut().unwrap();
        for (name, value) in headers {
            req_headers.insert(name, HeaderValue::from_static(value));
        }

        let req = req.body(body).unwrap();

        stream
            .set_write_timeout(IO_TIMEOUT)
            .expect("failed to set write timeout");

        // Write the Status line.
        write!(stream, "{} {} HTTP/1.1\r\n", req.method(), req.uri().path())
            .expect("failed to write request status line");
        // Headers.
        for (name, value) in req.headers() {
            write!(stream, "{}: {}\r\n", name, value.to_str().unwrap())
                .expect("failed to write request headers");
        }
        write!(stream, "\r\n").unwrap();
        // Body.
        stream.write_all(req.body()).expect("failed to write body");
        stream.flush().expect("failed to flush request");
    }

    /// Read a number of HTTP response from `stream`.
    pub fn read_responses(stream: &mut TcpStream, was_head: bool) -> Vec<Response<Vec<u8>>> {
        stream
            .set_read_timeout(IO_TIMEOUT)
            .expect("failed to set read timeout");

        // Read the all the responses.
        let mut bytes = Vec::new();
        stream
            .read_to_end(&mut bytes)
            .expect("failed to read response (WouldBlock means time out/no response)");

        let mut responses = Vec::new();
        while !bytes.is_empty() {
            // Parse a  HTTP headers.
            let mut headers = [httparse::EMPTY_HEADER; 8];
            let mut raw_resp = httparse::Response::new(&mut headers);
            let header_bytes = raw_resp
                .parse(&*bytes)
                .expect("invalid HTTP response")
                .unwrap();
            assert_eq!(raw_resp.version, Some(1));

            // Build an owned HTTP response.
            let mut resp = Response::builder()
                .version(Version::HTTP_11)
                .status(raw_resp.code.unwrap_or(0));
            {
                let headers = resp.headers_mut().unwrap();
                for header in raw_resp.headers {
                    let value = HeaderValue::from_bytes(header.value).unwrap();
                    let name = HeaderName::from_str(header.name).unwrap();
                    assert!(
                        headers.insert(name, value).is_none(),
                        "send '{}' header twice",
                        header.name
                    );
                }
            }

            let content_length: usize = if was_head {
                0
            } else {
                // Sorry, :)
                resp.headers_ref()
                    .unwrap()
                    .get(CONTENT_LENGTH)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap()
            };

            let remaining_bytes = bytes.split_off(header_bytes + content_length);
            let mut request_bytes = replace(&mut bytes, remaining_bytes);

            // Drop the header bytes and use the remainder as body.
            drop(request_bytes.drain(..header_bytes));
            let response = resp.body(request_bytes).unwrap();
            responses.push(response);
        }

        responses
    }

    /// Assert that `response` has all the `want`ed fields.
    ///
    /// # Notes
    ///
    /// Always checks the "Server" and "Date" headers. Panics on more then the
    /// wanted headers.
    pub fn assert_response(
        response: Response<Vec<u8>>,
        want_status: StatusCode,
        want_headers: &[(HeaderName, &str)],
        want_body: &[u8],
    ) {
        let want_date_header = date_header();
        assert_eq!(
            response.status(),
            want_status,
            "unexpected status: response={:?}",
            response.map(|body| String::from_utf8(body))
        );
        assert_eq!(
            response.version(),
            Version::HTTP_11,
            "unexpected HTTP version: response={:?}",
            response.map(|body| String::from_utf8(body))
        );
        for (name, value) in response.headers() {
            match name {
                &SERVER => assert_eq!(value, "stored"),
                &DATE => cmp_date_header(&http::header::DATE, value, &*want_date_header),
                name => {
                    let want = want_headers.iter().find(|want| name == want.0);
                    if let Some(want) = want {
                        if name == LAST_MODIFIED {
                            cmp_date_header(name, value, want.1);
                        } else {
                            assert_eq!(
                                value,
                                want.1,
                                "Different '{}' header: response={:?}, body={:?}",
                                name,
                                response,
                                str::from_utf8(response.body())
                            );
                        }
                    } else {
                        panic!(
                            "unexpected header: \"{}\" = {:?}, not in: {:?}: response={:?}",
                            name,
                            value.to_str(),
                            want_headers,
                            response
                        );
                    }
                }
            }
        }
        assert_eq!(
            response.headers().len(),
            want_headers.len() + 2,
            "Missing headers: {:?}",
            missing_headers(response.headers(), want_headers)
        );
        let got_content_length = response
            .headers()
            .get(CONTENT_LENGTH)
            .expect("missing 'Content-Length' header");
        let got_content_length: usize = got_content_length.to_str().unwrap().parse().unwrap();
        if got_content_length == 0 {
            // No body -> no content type.
            assert!(response.headers().get(CONTENT_TYPE).is_none());
        } else {
            // TODO:
            //assert!(response.headers().get(CONTENT_TYPE).is_some());
        }
        let got_body: &[u8] = &*response.body();
        if !want_body.is_empty() {
            // HEAD request expect an empty body, but with headers for a GET
            // request, thus the Content-Length will be >= 0, with an empty body.
            assert_eq!(
                got_content_length,
                got_body.len(),
                "'Content-Length' header and actual body length differ. Got body: {:?}, expected: {:?}. Content-Length: {}",
                str::from_utf8(got_body),
                str::from_utf8(want_body),
                got_content_length
            );
        }
        assert_eq!(
            got_body,
            want_body,
            "bodies differ: got: {:?}, want: {:?}",
            str::from_utf8(got_body),
            str::from_utf8(want_body)
        );
    }

    #[test]
    fn test_cmp_date_header() {
        let tests = &[
            (
                "Thu, 11 Jun 2020 10:35:20 GMT",
                "Thu, 11 Jun 2020 10:35:20 GMT",
            ),
            (
                "Thu, 11 Jun 2020 10:35:20 GMT",
                "Thu, 11 Jun 2020 10:35:21 GMT",
            ),
            (
                "Thu, 11 Jun 2020 10:35:39 GMT",
                "Thu, 11 Jun 2020 10:35:40 GMT",
            ),
            (
                "Thu, 11 Jun 2020 10:35:59 GMT",
                "Thu, 11 Jun 2020 10:36:00 GMT",
            ),
        ];

        for (value, want) in tests {
            cmp_date_header(&LAST_MODIFIED, &HeaderValue::from_static(value), want);
        }
    }

    fn cmp_date_header(name: &HeaderName, value: &HeaderValue, want: &str) {
        // Check a second before and after.
        let before_want = before_date_header(want);
        if value != want && value != &*before_want {
            assert_eq!(value, want, "Different '{}' header", name);
        }
    }

    fn before_date_header(date: &str) -> String {
        // Indexes for the 2 second bytes.
        // Format: `Wed, 10 Jun 2020 18:19:51 GMT`.
        const SEC1_INDEX: usize = 23;
        const SEC2_INDEX: usize = 24;
        const MIN2_INDEX: usize = 21;

        let mut date = date.to_owned();
        let bytes = unsafe { date.as_bytes_mut() };
        if bytes[SEC2_INDEX] == b'0' {
            if bytes[SEC1_INDEX] == b'0' {
                // TODO: properly support changing the minute.
                bytes[MIN2_INDEX] -= 1;
                bytes[SEC1_INDEX] = b'5';
            } else {
                bytes[SEC1_INDEX] -= 1;
            }
            bytes[SEC2_INDEX] = b'9';
        } else {
            bytes[SEC2_INDEX] -= 1;
        }
        date
    }

    /// Returns header names in `want` but not in `got`.
    fn missing_headers<'a>(got: &HeaderMap, want: &'a [(HeaderName, &str)]) -> Vec<&'a HeaderName> {
        want.iter()
            .filter_map(|(name, _)| {
                if !got.iter().any(|(n, _)| n == name) {
                    Some(name)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Current time in correct "Date" header format.
    pub fn date_header() -> String {
        let timestamp = Utc::now();
        let mut buf = Vec::with_capacity(30);
        static MONTHS: [&'static str; 12] = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ];
        write!(
            buf,
            "{}, {:02} {} {:004} {:02}:{:02}:{:02} GMT",
            timestamp.weekday(),
            timestamp.day(),
            MONTHS[timestamp.month0() as usize],
            timestamp.year(),
            timestamp.hour(),
            timestamp.minute(),
            timestamp.second(),
        )
        .unwrap();
        String::from_utf8(buf).unwrap()
    }
}
