//! The optional static page: the browser client served from the same origin as
//! `/session` and `/meta`.
//!
//! Serving the page from the server's own listener removes the two defects the
//! deployment runbook documents — the cross-origin `/meta` read and the
//! mixed-content refusal of a plaintext socket from an `https` page — because
//! the page's `/meta` fetch and its WebSocket dial are both same-origin.
//!
//! A request path is a *file name under the page root*, never a path on the
//! host: only the components below the root are used, and `.`, `..` and an
//! empty component refuse the request outright. Nothing here resolves a
//! symbolic link, so the operator's own page directory is exactly what is
//! served.

use std::path::{Path, PathBuf};

/// The file a request for the root serves.
const INDEX: &str = "index.html";

/// The largest file this will read. Built web assets are small; a page root
/// holding something else is a configuration mistake, not a reason to read it
/// into memory. Over the bound the request is answered like a missing file.
pub(crate) const MAX_PAGE_BYTES: u64 = 8 * 1024 * 1024;

/// The file `path` names under `root`, or `None` when it names none this
/// serves. `path` is the origin-form request target with its query already
/// split off, so it starts with `/`; percent-encoding is deliberately not
/// decoded, which is what keeps `%2e%2e` a file name that cannot exist rather
/// than a traversal.
pub(crate) fn resolve(root: &Path, path: &str) -> Option<PathBuf> {
    let mut file = root.to_path_buf();
    let trimmed = path.strip_prefix('/')?;
    if trimmed.is_empty() {
        file.push(INDEX);
        return Some(file);
    }
    let mut components: usize = 0;
    for part in trimmed.split('/') {
        if part.is_empty()
            || part == "."
            || part == ".."
            || part.contains('\\')
            || part.contains('\0')
        {
            return None;
        }
        file.push(part);
        components = components.saturating_add(1);
    }
    (components > 0).then_some(file)
}

/// The media type for a served file, from an explicit table rather than the
/// host's mime database. A hashed chunk served as `text/html` blocks the page's
/// module load (a defect the runbook records), so the table is the whole policy
/// and an unknown extension is opaque bytes, never HTML.
pub(crate) fn content_type(file: &Path) -> &'static str {
    match file.extension().and_then(|extension| extension.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json" | "map" | "webmanifest") => {
            "application/json; charset=utf-8"
        }
        Some("wasm") => "application/wasm",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("ttf") => "font/ttf",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{INDEX, content_type, resolve};

    fn resolved(path: &str) -> String {
        resolve(Path::new("/page"), path)
            .expect("the path stays under the root")
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn the_root_serves_the_index() {
        assert_eq!(resolved("/"), format!("/page/{INDEX}"));
    }

    #[test]
    fn a_path_is_a_file_name_under_the_root() {
        assert_eq!(resolved("/app.js"), "/page/app.js");
        assert_eq!(resolved("/assets/app.js"), "/page/assets/app.js");
    }

    #[test]
    fn a_traversal_never_resolves() {
        for path in [
            "/../secret",
            "/assets/../../secret",
            "/./secret",
            "//etc/passwd",
            "secret",
            "/a//b",
        ] {
            assert!(
                resolve(Path::new("/page"), path).is_none(),
                "{path} must not resolve under the root"
            );
        }
    }

    #[test]
    fn percent_encoding_is_a_file_name_not_a_traversal() {
        // Not decoded: `%2e%2e` names a file that cannot exist, rather than a
        // parent directory. The literal stays under the root.
        assert_eq!(resolved("/%2e%2e/secret"), "/page/%2e%2e/secret");
    }

    #[test]
    fn a_backslash_or_nul_names_nothing() {
        assert!(resolve(Path::new("/page"), "/a\\b").is_none());
        assert!(resolve(Path::new("/page"), "/a\0b").is_none());
    }

    #[test]
    fn content_types_cover_the_built_page() {
        assert_eq!(
            content_type(Path::new("index.html")),
            "text/html; charset=utf-8"
        );
        assert_eq!(
            content_type(Path::new("app.js")),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(
            content_type(Path::new("app.css")),
            "text/css; charset=utf-8"
        );
        assert_eq!(
            content_type(Path::new("app.js.map")),
            "application/json; charset=utf-8"
        );
        assert_eq!(
            content_type(Path::new("editor.worker.js")),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(
            content_type(Path::new("manifest.webmanifest")),
            "application/json; charset=utf-8"
        );
        assert_eq!(content_type(Path::new("codicon.ttf")), "font/ttf");
        assert_eq!(content_type(Path::new("icon.png")), "image/png");
    }

    #[test]
    fn an_unknown_extension_is_opaque_never_html() {
        assert_eq!(content_type(Path::new("blob")), "application/octet-stream");
        assert_eq!(
            content_type(Path::new("chunk.xyz")),
            "application/octet-stream"
        );
    }
}
