use std::io::{Cursor, Read};

use flate2::read::GzDecoder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    UnsupportedEncoding,
    InvalidGzip,
    PayloadTooLarge,
}

pub fn decode_bounded(
    encoding: Option<&str>,
    bytes: &[u8],
    max_body_bytes: usize,
) -> Result<Vec<u8>, DecodeError> {
    if bytes.len() > max_body_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }

    let encoding = encoding.unwrap_or("identity").trim().to_ascii_lowercase();
    match encoding.as_str() {
        "" | "identity" => Ok(bytes.to_vec()),
        "gzip" => decode_gzip(bytes, max_body_bytes),
        _ => Err(DecodeError::UnsupportedEncoding),
    }
}

fn decode_gzip(bytes: &[u8], max_body_bytes: usize) -> Result<Vec<u8>, DecodeError> {
    let decoder = GzDecoder::new(Cursor::new(bytes));
    let mut limited = decoder.take(max_body_bytes as u64 + 1);
    let mut decoded = Vec::with_capacity(bytes.len().min(max_body_bytes));
    limited
        .read_to_end(&mut decoded)
        .map_err(|_| DecodeError::InvalidGzip)?;
    if decoded.len() > max_body_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{write::GzEncoder, Compression};

    use super::*;

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn identity_round_trips() {
        assert_eq!(decode_bounded(None, b"abc", 3).unwrap(), b"abc");
        assert_eq!(decode_bounded(Some("identity"), b"abc", 3).unwrap(), b"abc");
    }

    #[test]
    fn gzip_round_trips_within_limit() {
        let encoded = gzip(b"otel-payload");
        assert_eq!(
            decode_bounded(Some("GZIP"), &encoded, 64).unwrap(),
            b"otel-payload"
        );
    }

    #[test]
    fn gzip_expansion_is_bounded() {
        let encoded = gzip(&vec![b'x'; 4096]);
        assert_eq!(
            decode_bounded(Some("gzip"), &encoded, 128),
            Err(DecodeError::PayloadTooLarge)
        );
    }

    #[test]
    fn malformed_gzip_is_rejected() {
        assert_eq!(
            decode_bounded(Some("gzip"), b"not-gzip", 128),
            Err(DecodeError::InvalidGzip)
        );
    }

    #[test]
    fn unsupported_or_chained_encodings_are_rejected() {
        for encoding in ["br", "deflate", "gzip, br"] {
            assert_eq!(
                decode_bounded(Some(encoding), b"payload", 128),
                Err(DecodeError::UnsupportedEncoding)
            );
        }
    }
}
