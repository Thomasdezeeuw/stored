#![allow(dead_code)] // Note: not all tests use all functions/types.

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

pub struct Proc {
    lock: &'static ProcLock,
    child: Arc<ChildCommand>,
}

impl Drop for Proc {
    fn drop(&mut self) {
        // We `lock` first to create a queue of tests that is done. After we got
        // the lock we'll check if we're the last test. If we did this without
        // holding the lock two tests currently ending could both determine
        // there not the last test and never stop the process.
        let mut child = self.lock.lock().unwrap();
        if Arc::strong_count(&self.child) == 2 {
            // Take the (second to) last arc pointer to the process. Which means
            // that if we get dropped the process is stopped.
            child.take();
        }
    }
}

pub type ProcLock = Mutex<Option<Arc<ChildCommand>>>;

/// Build and start the stored server.
///
/// It attempts to start a single process, which stops itself once all tests are
/// done (for which it needs a `ProcLock`).
///
/// If `filter` is not `LevelFilter::Off` it will set the standard out and error
/// to inherit from this process, making all logs available.
pub fn start_stored(conf_path: &'static str, lock: &'static ProcLock, filter: LevelFilter) -> Proc {
    build_stored();

    let mut proc = lock.lock().unwrap();
    let child = if let Some(proc) = &mut *proc {
        proc.clone()
    } else {
        let mut child = Command::new("target/debug/stored");

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

        // Give the server some time to start.
        sleep(Duration::from_millis(500));

        let child = Arc::new(child);
        proc.replace(child.clone());
        child
    };
    Proc { lock, child }
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

pub mod http {
    //! Simple http client.

    use std::io::{self, Read, Write};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
    use std::str::{self, FromStr};

    use chrono::{Datelike, Timelike, Utc};
    use http::header::{
        HeaderMap, HeaderName, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, DATE, SERVER,
    };
    use http::status::StatusCode;
    use http::{HeaderValue, Request, Response, Uri, Version};

    pub mod body {
        //! HTTP bodies and there lengths (in text for the "Content-Length"
        //! header).

        pub const EMPTY: &[u8] = b"";
        pub const EMPTY_LEN: &str = "0";

        pub const NOT_FOUND: &[u8] = b"Not found";
        pub const NOT_FOUND_LEN: &str = "9";

        pub const INVALID_KEY: &[u8] = b"Invalid key";
        pub const INVALID_KEY_LEN: &str = "11";

        pub const LENGTH_REQUIRED: &[u8] = b"Missing required content length header";
        pub const LENGTH_REQUIRED_LEN: &str = "38";

        pub const PAYLOAD_TOO_LARGE: &[u8] = b"Blob too large";
        pub const PAYLOAD_TOO_LARGE_LEN: &str = "14";

        pub const INCOMPLETE: &[u8] = b"Incomplete blob";
        pub const INCOMPLETE_LEN: &str = "14";
    }

    pub mod header {
        //! Common headers values.

        pub const PLAIN_TEXT: &str = "text/plain; charset=utf-8";
        pub const CLOSE: &str = "close";
    }

    /// Make a HTTP request.
    pub fn request(
        method: &'static str,
        path: &'static str,
        port: u16,
        headers: &[(HeaderName, &'static str)],
        body: &[u8],
    ) -> io::Result<Response<Vec<u8>>> {
        let mut req = Request::builder()
            .method(method)
            .uri(Uri::from_static(path))
            .version(Version::HTTP_11);

        let req_headers = req.headers_mut().unwrap();
        for (name, value) in headers {
            req_headers.insert(name, HeaderValue::from_static(value));
        }

        let req = req.body(body).unwrap();

        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let address = SocketAddr::new(ip, port);
        let mut stream = TcpStream::connect(address)?;

        // Write the Status line.
        write!(
            &mut stream,
            "{} {} HTTP/1.1\r\n",
            req.method(),
            req.uri().path()
        )?;
        // Headers.
        for (name, value) in req.headers() {
            write!(&mut stream, "{}: {}\r\n", name, value.to_str().unwrap())?;
        }
        write!(&mut stream, "\r\n")?;
        // Body.
        stream.write(req.body())?;
        stream.flush()?;

        // Read the entire response.
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes)?;

        // Parse the HTTP headers.
        let mut headers = [httparse::EMPTY_HEADER; 8];
        let mut raw_resp = httparse::Response::new(&mut headers);
        let header_bytes = raw_resp
            .parse(&*bytes)
            .expect("invalid HTTP response")
            .unwrap();
        assert_eq!(raw_resp.version, Some(1));

        // Build an own HTTP response.
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

        // Drop the header bytes and use the remainder as body.
        drop(bytes.drain(..header_bytes));
        Ok(resp.body(bytes).unwrap())
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
        assert_eq!(response.status(), want_status);
        assert_eq!(response.version(), Version::HTTP_11);
        for (name, value) in response.headers() {
            match name {
                &SERVER => assert_eq!(value, "stored"),
                &DATE => assert_eq!(value, &*date_header()),
                name => {
                    let want = want_headers.iter().find(|want| name == want.0);
                    if let Some(want) = want {
                        assert_eq!(value, want.1, "Different '{}' header", name);
                    } else {
                        panic!(
                            "unexpected header: \"{}\" = {:?}, not in: {:?}",
                            name,
                            value.to_str(),
                            want_headers
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
        let status = response.status();
        if status.is_client_error() || status.is_server_error() {
            // In case of an error we expect an 'Connection: close' header.
            let conn_header = response
                .headers()
                .get(CONNECTION)
                .expect("missing 'Connection' header");
            assert_eq!(conn_header, "close");
        }
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
