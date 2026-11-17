use crate::error::CbltError;
use async_compression::tokio::write::GzipEncoder;
use bytes::BytesMut;
use http::header::{ACCEPT_ENCODING, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, VARY};
use http::{Request, Response, StatusCode};
#[cfg(debug_assertions)]
use log::debug;
use log::info;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "trace")]
use tracing::instrument;

#[cfg_attr(feature = "trace", instrument(level = "trace", skip_all))]
pub async fn send_response_file<S>(
    mut socket: S,
    response: Response<impl AsyncRead>,
    req: &Request<BytesMut>,
) -> Result<(), CbltError>
where
    S: AsyncWriteExt + Unpin,
{
    let (mut parts, body) = response.into_parts();
    let can_compress =
        parts.status == StatusCode::OK && !parts.headers.contains_key(CONTENT_ENCODING);
    let gzip_supported = can_compress && gzip_support_detect(req);

    if can_compress {
        parts
            .headers
            .append(VARY, http::HeaderValue::from_static("Accept-Encoding"));
    }

    if gzip_supported {
        parts.headers.remove(CONTENT_LENGTH);
        parts
            .headers
            .insert(CONTENT_ENCODING, http::HeaderValue::from_static("gzip"));
        parts
            .headers
            .insert(CONNECTION, http::HeaderValue::from_static("close"));
    }

    let mut body = pin::pin!(body);

    // Write status line without allocation
    socket.write_all(b"HTTP/1.1 ").await?;
    let mut itoa_buf = itoa::Buffer::new();
    let status_str = itoa_buf.format(parts.status.as_u16());
    socket.write_all(status_str.as_bytes()).await?;
    socket.write_all(b" ").await?;
    socket
        .write_all(parts.status.canonical_reason().unwrap_or("").as_bytes())
        .await?;
    socket.write_all(b"\r\n").await?;
    // Write headers without allocation
    for (key, value) in parts.headers.iter() {
        socket.write_all(key.as_str().as_bytes()).await?;
        socket.write_all(b": ").await?;
        socket.write_all(value.as_bytes()).await?;
        socket.write_all(b"\r\n").await?;
    }

    // End headers
    socket.write_all(b"\r\n").await?;

    // Ensure all headers are flushed
    socket.flush().await?;

    if gzip_supported {
        #[cfg(debug_assertions)]
        debug!("Gzip supported");
        let mut gzip_encoder = GzipEncoder::new(&mut socket);
        tokio::io::copy(&mut body, &mut gzip_encoder).await?;
        gzip_encoder.shutdown().await?;
    } else {
        tokio::io::copy(&mut body, &mut socket).await?;
    }

    // Ensure all data is flushed
    socket.flush().await?;

    Ok(())
}

#[cfg_attr(feature = "trace", instrument(level = "trace", skip_all))]
pub async fn ranged_file_response(
    file: File,
    file_path: &PathBuf,
    file_size: u64,
    range: (u64, u64),
) -> Result<Response<tokio::io::Take<File>>, CbltError> {
    let (start, end) = range;
    let content_length = end
        .checked_sub(start)
        .and_then(|length| length.checked_add(1))
        .filter(|_| end < file_size)
        .ok_or(CbltError::ResponseError {
            details: "Invalid byte range".to_string(),
            status_code: StatusCode::RANGE_NOT_SATISFIABLE,
        })?;

    // Seek to the start position
    let mut file = file;
    use tokio::io::AsyncSeekExt;
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let file = file.take(content_length);

    let mut content_range: heapless::String<200> = heapless::String::new();
    content_range
        .push_str("bytes ")
        .map_err(|_| CbltError::HeaplessError {})?;
    content_range
        .push_str(start.to_string().as_str())
        .map_err(|_| CbltError::HeaplessError {})?;
    content_range
        .push_str("-")
        .map_err(|_| CbltError::HeaplessError {})?;
    content_range
        .push_str(end.to_string().as_str())
        .map_err(|_| CbltError::HeaplessError {})?;
    content_range
        .push_str("/")
        .map_err(|_| CbltError::HeaplessError {})?;
    content_range
        .push_str(file_size.to_string().as_str())
        .map_err(|_| CbltError::HeaplessError {})?;

    let mime_type = mime_guess::from_path(file_path)
        .first_or_octet_stream()
        .to_string();
    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header("Content-Length", content_length)
        .header("Content-Range", content_range.as_str())
        .header("Content-Type", mime_type)
        .body(file)?)
}

#[cfg_attr(feature = "trace", instrument(level = "trace", skip_all))]
fn gzip_support_detect(req_opt: &Request<BytesMut>) -> bool {
    let mut gzip_quality = None;
    let mut wildcard_quality = None;

    for header in req_opt
        .headers()
        .get_all(ACCEPT_ENCODING)
        .iter()
        .filter_map(|value| value.to_str().ok())
    {
        for encoding in header.split(',') {
            let encoding = encoding.trim();
            let (coding, parameters) = encoding.split_once(';').unwrap_or((encoding, ""));
            let Some(quality) = encoding_quality(parameters) else {
                continue;
            };

            if coding.trim().eq_ignore_ascii_case("gzip") {
                gzip_quality = Some(quality);
            } else if coding.trim() == "*" {
                wildcard_quality = Some(quality);
            }
        }
    }

    gzip_quality
        .or(wildcard_quality)
        .map(|quality| quality > 0.0)
        .unwrap_or(false)
}

fn encoding_quality(parameters: &str) -> Option<f32> {
    let mut quality = None;

    for parameter in parameters
        .split(';')
        .filter(|value| !value.trim().is_empty())
    {
        let (name, value) = parameter.trim().split_once('=')?;
        if !name.trim().eq_ignore_ascii_case("q") || quality.is_some() {
            return None;
        }

        let parsed = value.trim().parse::<f32>().ok()?;
        if !parsed.is_finite() || !(0.0..=1.0).contains(&parsed) {
            return None;
        }
        quality = Some(parsed);
    }

    Some(quality.unwrap_or(1.0))
}

#[cfg_attr(feature = "trace", instrument(level = "trace", skip_all))]
pub fn log_request_response(
    request: &Request<BytesMut>,
    status_code: StatusCode,
    remote_addr: SocketAddr,
) {
    let method = &request.method();
    let uri = request.uri();
    let headers = request.headers();

    let host_header = headers
        .get("Host")
        .map_or("-", |v| v.to_str().unwrap_or("-"));

    info!(
        "Request: {} {} {} {} remote_addr={}",
        method,
        uri,
        host_header,
        status_code.as_u16(),
        remote_addr
    );
}

#[cfg_attr(feature = "trace", instrument(level = "trace", skip_all))]
pub async fn send_response<S>(socket: &mut S, response: Response<BytesMut>) -> Result<(), CbltError>
where
    S: AsyncWriteExt + Unpin,
{
    let (parts, body) = response.into_parts();

    // Estimate capacity to reduce reallocations
    let mut resp_bytes = Vec::with_capacity(128 + body.len());
    resp_bytes.write_all(b"HTTP/1.1 ").await?;

    let mut itoa_buf = itoa::Buffer::new();
    let status_str = itoa_buf.format(parts.status.as_u16());
    resp_bytes.write_all(status_str.as_bytes()).await?;

    resp_bytes.write_all(b" ").await?;
    resp_bytes
        .write_all(parts.status.canonical_reason().unwrap_or("").as_bytes())
        .await?;
    resp_bytes.write_all(b"\r\n").await?;

    for (key, value) in parts.headers.iter() {
        resp_bytes.extend_from_slice(key.as_str().as_bytes());
        resp_bytes.extend_from_slice(b": ");
        resp_bytes.extend_from_slice(value.as_bytes());
        resp_bytes.extend_from_slice(b"\r\n");
    }

    resp_bytes.extend_from_slice(b"\r\n");
    resp_bytes.extend_from_slice(&body);

    socket.write_all(&resp_bytes).await?;

    Ok(())
}

#[cfg_attr(feature = "trace", instrument(level = "trace", skip_all))]
pub fn error_response(status: StatusCode) -> Result<Response<BytesMut>, CbltError> {
    let msg = match status {
        StatusCode::BAD_REQUEST => "Bad request",
        StatusCode::FORBIDDEN => "Forbidden",
        StatusCode::NOT_FOUND => "Not found",
        StatusCode::METHOD_NOT_ALLOWED => "Method not allowed",
        StatusCode::INTERNAL_SERVER_ERROR => "Internal server error",
        StatusCode::BAD_GATEWAY => "Bad gateway",
        _ => "Unknown error",
    };
    let bytes = BytesMut::from(msg);
    Ok(Response::builder().status(status).body(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn request_with_encoding(encoding: &str) -> Request<BytesMut> {
        Request::builder()
            .header(ACCEPT_ENCODING, encoding)
            .body(BytesMut::new())
            .expect("valid test request")
    }

    #[test]
    fn gzip_negotiation_respects_tokens_and_quality() {
        for (encoding, expected) in [
            ("gzip", true),
            ("br, GZIP; q=0.5", true),
            ("gzip;q=0", false),
            ("gzip;q=1.1", false),
            ("xgzip", false),
            ("*", true),
            ("br", false),
        ] {
            assert_eq!(
                gzip_support_detect(&request_with_encoding(encoding)),
                expected,
                "unexpected gzip negotiation for {encoding:?}"
            );
        }
    }

    #[tokio::test]
    async fn gzip_response_has_consistent_headers_and_body(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let payload = b"response body".to_vec();
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, payload.len())
            .body(std::io::Cursor::new(payload))?;
        let request = request_with_encoding("gzip");
        let (mut client, mut server) = tokio::io::duplex(4096);

        send_response_file(&mut server, response, &request).await?;
        drop(server);

        let mut response_bytes = Vec::new();
        client.read_to_end(&mut response_bytes).await?;
        let header_end = response_bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("response headers are terminated");
        let headers = std::str::from_utf8(&response_bytes[..header_end])?;
        let body = &response_bytes[header_end + 4..];

        assert!(headers.contains("content-encoding: gzip"));
        assert!(headers.contains("connection: close"));
        assert!(headers.contains("vary: Accept-Encoding"));
        assert!(!headers.contains("content-length:"));
        assert!(body.starts_with(&[0x1f, 0x8b]));
        Ok(())
    }

    #[tokio::test]
    async fn partial_content_is_not_compressed() -> Result<(), Box<dyn std::error::Error>> {
        let payload = b"partial".to_vec();
        let response = Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(CONTENT_LENGTH, payload.len())
            .body(std::io::Cursor::new(payload))?;
        let request = request_with_encoding("gzip");
        let (mut client, mut server) = tokio::io::duplex(4096);

        send_response_file(&mut server, response, &request).await?;
        drop(server);

        let mut response_bytes = Vec::new();
        client.read_to_end(&mut response_bytes).await?;
        let header_end = response_bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("response headers are terminated");
        let headers = std::str::from_utf8(&response_bytes[..header_end])?;

        assert!(headers.contains("content-length: 7"));
        assert!(!headers.contains("content-encoding: gzip"));
        assert_eq!(&response_bytes[header_end + 4..], b"partial");
        Ok(())
    }

    #[tokio::test]
    async fn ranged_response_reads_only_the_requested_bytes(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/response.rs");
        let contents = tokio::fs::read(&path).await?;
        let file = File::open(&path).await?;
        let response = ranged_file_response(file, &path, contents.len() as u64, (2, 7)).await?;
        assert_eq!(
            response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some("6")
        );
        let mut body = response.into_body();
        let mut bytes = Vec::new();

        body.read_to_end(&mut bytes).await?;
        assert_eq!(bytes, contents[2..=7]);
        Ok(())
    }
}
