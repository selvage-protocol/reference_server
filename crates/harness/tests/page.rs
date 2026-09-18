//! The optional page: a static file server on the same origin as `/session`
//! and `/meta`.
//!
//! The escape cases matter more than the happy path — a request must never name
//! a file outside the page root — so the traversal shapes are written as raw
//! request lines rather than through a client that would normalize them away.

use std::env;
use std::error::Error as StdError;
use std::fs;
use std::path::PathBuf;
use std::process;
use std::time::SystemTime;

use selvage_harness::{Harness, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

type Failure = Box<dyn StdError>;

/// A page root of our own, so the test never reads another test's leftovers.
struct PageDir {
    path: PathBuf,
}

impl PageDir {
    fn new() -> Result<Self, Failure> {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        let path = env::temp_dir()
            .join(format!("selvaged-page-test-{}-{unique}", process::id()));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn write(&self, name: &str, body: &str) -> Result<(), Failure> {
        fs::write(self.path.join(name), body)?;
        Ok(())
    }

    async fn start(&self) -> Harness {
        Harness::start_with(ServerConfig {
            page_root: Some(self.path.clone()),
            ..ServerConfig::default()
        })
        .await
    }
}

impl Drop for PageDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Returns (status line, headers, body) for one raw request against the harness.
async fn request(
    harness: &Harness,
    request_line: &str,
) -> Result<(String, String, Vec<u8>), Failure> {
    let addr = harness.ws_base().trim_start_matches("ws://").to_string();
    let mut stream = TcpStream::connect(&addr).await?;
    stream
        .write_all(
            format!(
                "{request_line}\r\nhost: {addr}\r\nconnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or("an HTTP response has a header terminator")?;
    let head_bytes = response
        .get(..split)
        .ok_or("a head before the terminator")?;
    let body_bytes = response
        .get(split.saturating_add(4)..)
        .ok_or("a body after the terminator")?;
    let head = String::from_utf8(head_bytes.to_vec())?;
    let body = body_bytes.to_vec();
    let (status, headers) = head.split_once("\r\n").ok_or("a status line")?;
    Ok((status.to_string(), headers.to_string(), body))
}

#[tokio::test]
async fn the_root_serves_the_index_and_assets_with_types() -> Result<(), Failure>
{
    let dir = PageDir::new()?;
    dir.write("index.html", "<!doctype html><title>selvage</title>")?;
    dir.write("app.js", "export const a = 1;")?;
    let harness = dir.start().await;

    let (status, headers, body) = request(&harness, "GET /").await?;
    assert_eq!(status, "HTTP/1.1 200 OK", "{headers}");
    assert!(
        headers.contains("content-type: text/html; charset=utf-8"),
        "{headers}"
    );
    assert!(headers.contains("cache-control: no-store"), "{headers}");
    assert_eq!(body, b"<!doctype html><title>selvage</title>");

    let (status, headers, body) = request(&harness, "GET /app.js").await?;
    assert_eq!(status, "HTTP/1.1 200 OK", "{headers}");
    assert!(
        headers.contains("content-type: text/javascript; charset=utf-8"),
        "a hashed chunk must not be HTML: {headers}"
    );
    assert_eq!(body, b"export const a = 1;");
    Ok(())
}

#[tokio::test]
async fn the_page_and_meta_share_one_origin() -> Result<(), Failure> {
    let dir = PageDir::new()?;
    dir.write("index.html", "page")?;
    let harness = dir.start().await;

    let (status, _, body) = request(&harness, "GET /meta").await?;
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        String::from_utf8(body)?.contains("wire_versions"),
        "/meta still answers beside the page"
    );
    Ok(())
}

#[tokio::test]
async fn head_carries_the_length_without_the_body() -> Result<(), Failure> {
    let dir = PageDir::new()?;
    dir.write("index.html", "twelve bytes")?;
    let harness = dir.start().await;

    let (status, headers, body) = request(&harness, "HEAD /").await?;
    assert_eq!(status, "HTTP/1.1 200 OK", "{headers}");
    assert!(headers.contains("content-length: 12"), "{headers}");
    assert!(body.is_empty(), "HEAD carries no body: {body:?}");
    Ok(())
}

#[tokio::test]
async fn a_traversal_is_the_same_answer_as_a_missing_file()
-> Result<(), Failure> {
    let dir = PageDir::new()?;
    dir.write("index.html", "page")?;
    let harness = dir.start().await;

    for line in [
        "GET /../Cargo.toml",
        "GET /%2e%2e/Cargo.toml",
        "GET /a/../../Cargo.toml",
        "GET /missing.js",
    ] {
        let (status, _, body) = request(&harness, line).await?;
        assert_eq!(status, "HTTP/1.1 404 Not Found", "{line}");
        assert!(
            String::from_utf8_lossy(&body).contains("not found"),
            "{line}: {body:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_write_method_is_refused_like_meta() -> Result<(), Failure> {
    let dir = PageDir::new()?;
    dir.write("index.html", "page")?;
    let harness = dir.start().await;

    let (status, headers, _) = request(&harness, "POST /").await?;
    assert_eq!(status, "HTTP/1.1 405 Method Not Allowed", "{headers}");
    assert!(headers.contains("allow: GET, HEAD"), "{headers}");
    Ok(())
}

#[tokio::test]
async fn without_a_page_root_an_unknown_path_is_a_plain_404()
-> Result<(), Failure> {
    let harness = Harness::start_with(ServerConfig::default()).await;
    let (status, _, body) = request(&harness, "GET /").await?;
    assert_eq!(status, "HTTP/1.1 404 Not Found");
    let text = String::from_utf8(body)?;
    assert!(text.contains("/session"), "{text}");
    assert!(text.contains("/meta"), "{text}");
    Ok(())
}
