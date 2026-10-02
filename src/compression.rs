use crate::config::Compression;
use crate::errors::{Error, Result};
use std::io::Write;

pub(crate) fn encode(input: &[u8], compression: Compression) -> Result<Vec<u8>> {
    use flate2::write::{GzEncoder, ZlibEncoder};
    use flate2::Compression as Level;

    let encoded = match compression {
        Compression::None => return Ok(input.to_vec()),
        Compression::Gz => {
            let mut encoder = GzEncoder::new(Vec::new(), Level::default());
            encoder.write_all(input).map_err(codec_error)?;
            encoder.finish().map_err(codec_error)?
        }
        // Doris' deflate option expects a zlib-wrapped DEFLATE stream.
        Compression::Deflate => {
            let mut encoder = ZlibEncoder::new(Vec::new(), Level::default());
            encoder.write_all(input).map_err(codec_error)?;
            encoder.finish().map_err(codec_error)?
        }
        Compression::Bz2 => {
            let mut encoder =
                bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
            encoder.write_all(input).map_err(codec_error)?;
            encoder.finish().map_err(codec_error)?
        }
        // Stream Load's lz4 format is an LZ4 frame, not a raw LZ4 block.
        Compression::Lz4 => {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(input).map_err(codec_error)?;
            encoder
                .finish()
                .map_err(|error| Error::InvalidConfig(format!("LZ4 compression failed: {error}")))?
        }
        // Doris routes both aliases through its LZOP stream decoder, so both
        // values require the LZOP container (raw LZO1X alone is not accepted).
        Compression::Lzo | Compression::Lzop => encode_lzop(input)?,
    };
    Ok(encoded)
}

fn codec_error(error: std::io::Error) -> Error {
    Error::InvalidConfig(format!("compression failed: {error}"))
}

fn encode_lzop(input: &[u8]) -> Result<Vec<u8>> {
    // LZOP container framing, with an LZO1X block when it is smaller and a
    // stored block otherwise. Omit optional block checksums for compatibility
    // with Doris' Stream Load LZOP decoder; the mandatory header checksum is
    // still written below.
    let mut out = Vec::new();
    out.extend_from_slice(&[0x89, b'L', b'Z', b'O', 0, 13, 10, 26, 10]);
    let mut header = Vec::new();
    for value in [0x1010u16, 0x20a0, 0x0940] {
        header.extend_from_slice(&value.to_be_bytes());
    }
    header.push(1); // LZO1X-1
    header.push(5); // compression level
    header.extend_from_slice(&0u32.to_be_bytes()); // flags
    header.extend_from_slice(&0u32.to_be_bytes()); // mode
    header.extend_from_slice(&0u32.to_be_bytes()); // mtime low
    header.extend_from_slice(&0u32.to_be_bytes()); // mtime high
    header.push(0); // filename length
    let checksum = adler32(&header);
    out.extend_from_slice(&header);
    out.extend_from_slice(&checksum.to_be_bytes());

    let compressed = lzokay::compress::compress(input)
        .map_err(|e| Error::InvalidConfig(format!("LZOP compression failed: {e:?}")))?;
    let (payload, compressed_len) = if compressed.len() < input.len() {
        (compressed.as_slice(), compressed.len())
    } else {
        (input, input.len())
    };
    let size = u32::try_from(input.len())
        .map_err(|_| Error::InvalidConfig("LZOP input exceeds 4 GiB".into()))?;
    out.extend_from_slice(&size.to_be_bytes());
    // A matching size marks a stored block; LZOP still carries this field.
    out.extend_from_slice(&(compressed_len as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(&0u32.to_be_bytes());
    Ok(out)
}

fn adler32(bytes: &[u8]) -> u32 {
    let (mut a, mut b) = (1u32, 0u32);
    for chunk in bytes.chunks(5552) {
        for byte in chunk {
            a += u32::from(*byte);
            b += a;
        }
        a %= 65_521;
        b %= 65_521;
    }
    (b << 16) | a
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn each_codec_round_trips_csv_body() {
        let input = b"1,repeat-repeat-repeat\n2,repeat-repeat-repeat\n";
        for compression in [
            Compression::Gz,
            Compression::Lzo,
            Compression::Bz2,
            Compression::Lz4,
            Compression::Lzop,
            Compression::Deflate,
        ] {
            let encoded = encode(input, compression).unwrap();
            let decoded = match compression {
                Compression::Gz => {
                    let mut decoder = flate2::read::GzDecoder::new(encoded.as_slice());
                    let mut out = Vec::new();
                    decoder.read_to_end(&mut out).unwrap();
                    out
                }
                Compression::Deflate => {
                    let mut decoder = flate2::read::ZlibDecoder::new(encoded.as_slice());
                    let mut out = Vec::new();
                    decoder.read_to_end(&mut out).unwrap();
                    out
                }
                Compression::Bz2 => {
                    let mut decoder = bzip2::read::BzDecoder::new(encoded.as_slice());
                    let mut out = Vec::new();
                    decoder.read_to_end(&mut out).unwrap();
                    out
                }
                Compression::Lz4 => {
                    let mut decoder = lz4_flex::frame::FrameDecoder::new(encoded.as_slice());
                    let mut out = Vec::new();
                    decoder.read_to_end(&mut out).unwrap();
                    out
                }
                Compression::Lzo | Compression::Lzop => decode_lzop(&encoded),
                Compression::None => unreachable!(),
            };
            assert_eq!(decoded, input, "{compression:?}");
        }
    }

    fn decode_lzop(input: &[u8]) -> Vec<u8> {
        assert_eq!(&input[..9], &[0x89, b'L', b'Z', b'O', 0, 13, 10, 26, 10]);
        let mut pos = 38; // fixed header including its checksum, no filename
        let original_len = u32::from_be_bytes(input[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let compressed_len = u32::from_be_bytes(input[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let payload = &input[pos..pos + compressed_len];
        let decoded = if compressed_len == original_len {
            payload.to_vec()
        } else {
            let mut out = vec![0; original_len];
            let len = lzokay::decompress::decompress(payload, &mut out).unwrap();
            out.truncate(len);
            out
        };
        decoded
    }

    #[test]
    fn none_preserves_body() {
        let body = b"1,plain\n";
        assert_eq!(encode(body, Compression::None).unwrap(), body);
        assert_eq!(Compression::None.as_header(), None);
    }
}
