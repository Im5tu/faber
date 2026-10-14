use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Largest frame body either side accepts: 4 MiB.
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame io: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame of {len} bytes is over the limit of {max}")]
    TooLarge { len: usize, max: usize },
    #[error("frame is not valid for this message: {0}")]
    Json(#[from] serde_json::Error),
}

/// The length prefix and JSON body of `msg`.
pub fn encode_frame<T: Serialize>(msg: &T) -> Result<Vec<u8>, FrameError> {
    let body = serde_json::to_vec(msg)?;
    if body.len() > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge {
            len: body.len(),
            max: MAX_FRAME_BYTES,
        });
    }
    let len = u32::try_from(body.len()).map_err(|_| FrameError::TooLarge {
        len: body.len(),
        max: MAX_FRAME_BYTES,
    })?;
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

pub async fn write_frame<W, T>(writer: &mut W, msg: &T) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = encode_frame(msg)?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads one frame. `Ok(None)` is a clean end of stream between frames.
///
/// The length is checked against `max` before any buffer is allocated, so a
/// peer cannot make the reader allocate more than `max` bytes.
pub async fn read_frame<R, T>(reader: &mut R, max: usize) -> Result<Option<T>, FrameError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut header = [0u8; 4];
    let mut filled = 0;
    while filled < header.len() {
        let n = reader.read(&mut header[filled..]).await?;
        if n == 0 {
            if filled == 0 {
                return Ok(None);
            }
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        filled += n;
    }
    let len = u32::from_be_bytes(header) as usize;
    if len > max {
        return Err(FrameError::TooLarge { len, max });
    }
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).await?;
    Ok(Some(serde_json::from_slice(&body)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trips_a_frame() {
        let mut wire = Vec::new();
        write_frame(&mut wire, &vec![1u8, 2, 3]).await.unwrap();
        assert_eq!(&wire[..4], &(wire.len() as u32 - 4).to_be_bytes());
        let got: Option<Vec<u8>> = read_frame(&mut wire.as_slice(), 1024).await.unwrap();
        assert_eq!(got, Some(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn clean_eof_is_none_and_partial_header_is_an_error() {
        let got: Option<u8> = read_frame(&mut [].as_slice(), 16).await.unwrap();
        assert_eq!(got, None);
        let err = read_frame::<_, u8>(&mut [0u8, 0].as_slice(), 16)
            .await
            .unwrap_err();
        assert!(matches!(err, FrameError::Io(_)));
    }

    #[tokio::test]
    async fn oversized_length_is_refused_before_reading_the_body() {
        // Header only: if the reader tried to read the body it would hit EOF.
        let header = (1024u32 * 1024).to_be_bytes();
        let err = read_frame::<_, u8>(&mut header.as_slice(), 1024)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            FrameError::TooLarge {
                len: 1048576,
                max: 1024
            }
        ));
    }

    #[tokio::test]
    async fn truncated_body_and_bad_json_are_errors() {
        let mut wire = 10u32.to_be_bytes().to_vec();
        wire.extend_from_slice(b"[1,2");
        let err = read_frame::<_, Vec<u8>>(&mut wire.as_slice(), 64)
            .await
            .unwrap_err();
        assert!(matches!(err, FrameError::Io(_)));

        let mut wire = 4u32.to_be_bytes().to_vec();
        wire.extend_from_slice(b"nope");
        let err = read_frame::<_, Vec<u8>>(&mut wire.as_slice(), 64)
            .await
            .unwrap_err();
        assert!(matches!(err, FrameError::Json(_)));
    }

    #[test]
    fn encoding_refuses_an_oversized_body() {
        let big = "x".repeat(MAX_FRAME_BYTES);
        assert!(matches!(
            encode_frame(&big),
            Err(FrameError::TooLarge { .. })
        ));
    }
}
