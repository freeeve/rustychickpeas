//! Error type for format encoding/decoding.

use std::fmt;

/// Errors produced while encoding or decoding format files.
#[derive(Debug)]
pub enum FormatError {
    /// Underlying I/O failure while writing.
    Io(std::io::Error),
    /// The input bytes are not a valid file of the expected format.
    Corrupt(String),
    /// The file is a recognized format but a newer, unsupported version.
    UnsupportedVersion { format: &'static str, version: u16 },
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FormatError::Io(e) => write!(f, "i/o error: {}", e),
            FormatError::Corrupt(msg) => write!(f, "corrupt input: {}", msg),
            FormatError::UnsupportedVersion { format, version } => {
                write!(f, "unsupported {} version {}", format, version)
            }
        }
    }
}

impl std::error::Error for FormatError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FormatError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for FormatError {
    fn from(e: std::io::Error) -> Self {
        FormatError::Io(e)
    }
}
