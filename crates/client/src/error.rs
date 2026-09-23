//! Everything a session can fail with.

use std::error::Error as StdError;
use std::fmt;
use std::io;

use serde_json::Error as JsonError;
use tokio_tungstenite::tungstenite::Error as WireError;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Wire(WireError),
    Json(JsonError),
    /// The server refused the session; `code` is one of `selvage_protocol::code`.
    Protocol {
        code: String,
        message: String,
    },
    /// The connection ended.
    Closed,
    /// The session could not be started: the given string is not a connection URL.
    Invite(String),
    /// The link names the other wire version, which this engine does not speak.
    Version(String),
    /// The sync engine could not apply an operation.
    Yjs(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Wire(e) => write!(f, "websocket: {e}"),
            Self::Json(e) => write!(f, "json: {e}"),
            Self::Protocol { code, message } => write!(f, "{code}: {message}"),
            Self::Closed => write!(f, "the session is closed"),
            Self::Invite(url) => write!(f, "not a Selvage invite URL: {url}"),
            Self::Version(message) => write!(f, "{message}"),
            Self::Yjs(message) => write!(f, "sync: {message}"),
        }
    }
}

impl StdError for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<JsonError> for Error {
    fn from(e: JsonError) -> Self {
        Self::Json(e)
    }
}
