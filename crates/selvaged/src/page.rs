//! The optional static page: the browser client served from the same origin as
//! `/session` and `/meta`.
//!
//! Serving the page from the server's own listener removes the two defects the
//! deployment runbook documents — the cross-origin `/meta` read and the
//! mixed-content refusal of a plaintext socket from an `https` page — because
//! the page's `/meta` fetch and its WebSocket dial are both same-origin.
//!
//! A request path is a *file name under the page root*, never a path on the
//! host: only the components below the root are used, `.`, `..` and an empty
//! component refuse the request outright, and the file that gets opened has to
//! resolve to somewhere under the same root — so a symbolic link the page
//! directory holds cannot reach out of it.

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use tokio::fs;

/// The file a request for the root serves.
const INDEX: &str = "index.html";

/// The largest file this will read. Built web assets are small; a page root
/// holding something else is a configuration mistake, not a reason to read it
/// into memory. Over the bound the request is answered like a missing file.
pub(crate) const MAX_PAGE_BYTES: u64 = 8 * 1024 * 1024;

/// The policy for a name carrying a content hash: its bytes cannot change
/// under that name, so a reload may reuse it without asking. A year is the
/// conventional "for ever" of a hashed asset, not a promise about the file.
const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// The policy for every other name. The shell and the bundler's stable names
/// change whenever the page is rebuilt, so a cached copy is revalidated rather
/// than pinned; the page root is read per request, so a re-synced build is
/// served without a restart either way.
const REVALIDATE: &str = "no-cache";

/// The page's content security policy: no default source at all, so every
/// fetch the page makes is one of the directives below.
///
/// `connect-src` admits any websocket or HTTP origin, because the page is a
/// client for whatever server an invite names — a page served from one origin
/// joining a room on another is the normal case, not a defect. `'unsafe-inline'`
/// covers the shell's inline style block and its pre-paint script, neither of
/// which can be hashed per build.
pub(crate) const CSP: &str = concat!(
    "default-src 'none'; ",
    "script-src 'self' 'unsafe-inline'; ",
    "style-src 'self' 'unsafe-inline'; ",
    "img-src 'self' data:; ",
    "font-src 'self'; ",
    "worker-src 'self' blob:; ",
    "connect-src 'self' ws: wss: http: https:; ",
    "manifest-src 'self'; ",
    "base-uri 'none'; ",
    "form-action 'none'; ",
    "frame-ancestors 'none'",
);

/// Every header one served file carries, in the order they are written.
///
/// The media type comes from the pinned table below, never the host's mime
/// database. The cache policy is the name's: a content-hashed asset is pinned,
/// everything else revalidates. The last three are the hardening the demo's
/// hand-written page server carried and a static handler forgets: an invite
/// URL carries the room token, so its referrer is withheld from every origin
/// the page visits, and `nosniff` holds a response to the media type this table
/// gave it.
pub(crate) fn headers(file: &Path) -> [(&'static str, &'static str); 5] {
    [
        ("content-type", content_type(file)),
        ("cache-control", cache_control(file)),
        ("referrer-policy", "no-referrer"),
        ("x-content-type-options", "nosniff"),
        ("content-security-policy", CSP),
    ]
}

/// How a client may cache this file.
fn cache_control(file: &Path) -> &'static str {
    if is_content_hashed(file) {
        IMMUTABLE
    } else {
        REVALIDATE
    }
}

/// Whether the name carries a content hash the bundler wrote — `-<hash>` before
/// the extension, eight or more characters of the alphabet a hash is written in:
/// the pattern the demo's page server pinned. The extensions are the ones the
/// bundler hashes, so a name that merely looks hashed is not pinned.
/// `name-1a2b3c4d.js` is; `app.js` and `app-1a2b3c.js` are not, the latter
/// because seven characters is not a hash, and `lang-255Y2KCL.js.map` is not,
/// because there the extension is `map` and the hash sits before the `.js`.
fn is_content_hashed(file: &Path) -> bool {
    let Some(name) = file.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some((stem, extension)) = name.rsplit_once('.') else {
        return false;
    };
    if !matches!(
        extension,
        "js" | "css" | "map" | "ttf" | "woff" | "woff2" | "png" | "svg"
    ) {
        return false;
    }
    let Some((_, hash)) = stem.rsplit_once('-') else {
        return false;
    };
    hash.len() >= 8
        && hash.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
        })
}

/// Opens `file` for a response, and only when the file that was opened still
/// lives under `root`.
///
/// [`resolve`] keeps the *request* under the root, but the page directory is a
/// build artifact and a symbolic link inside it can name a file outside —
/// `dist/secret -> /etc/passwd` served to anyone who can reach the page. The
/// check is on the descriptor the caller reads, not on the path that named it:
/// a component swapped between an open and a check cannot move the descriptor,
/// and the file it names is the file that is read. The root is resolved once
/// per request, which is what makes a page root that is itself a link work.
///
/// The descriptor's own path is read through `/proc/self/fd`, the only way std
/// offers to name what an open descriptor is; on a system without it every
/// request is refused rather than served unverified, and every target here
/// (Linux, the image, the Pi unit) has it.
pub(crate) async fn open_within(root: &Path, file: &Path) -> Option<fs::File> {
    let real_root = fs::canonicalize(root).await.ok()?;
    let opened = fs::File::open(file).await.ok()?;
    let real = fs::read_link(format!("/proc/self/fd/{}", opened.as_raw_fd()))
        .await
        .ok()?;
    real.starts_with(real_root).then_some(opened)
}

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

    use super::{
        CSP, IMMUTABLE, INDEX, REVALIDATE, cache_control, content_type,
        headers, resolve,
    };

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
    fn a_content_hashed_name_is_pinned_and_a_stable_one_revalidates() {
        for name in [
            "app-1a2b3c4d.js",
            "editor.worker-09f8e7d6.js",
            "index-abcdefgh.css",
            "app-1a2b3c4d.map",
            "codicon-7c6e5d4f.ttf",
            "codicon-7c6e5d4f.woff",
            "codicon-7c6e5d4f.woff2",
            "preview-deadbeef.png",
            "svp-0f1e2d3c.svg",
        ] {
            assert_eq!(cache_control(Path::new(name)), IMMUTABLE, "{name}");
        }
        for name in [
            // The shell and the bundler's unhashed names.
            "index.html",
            "app.js",
            // A hash shorter than eight characters is not one.
            "app-1a2b3c.js",
            // The extensions the bundler does not hash.
            "app-1a2b3c4d.html",
            "app-1a2b3c4d.txt",
            // A source map of a hashed chunk: the extension is `map` and the
            // hash is not next to it, so the name is not the hash's.
            "lang-255Y2KCL.js.map",
            // A hash-looking name with no extension, and a bare extension.
            "app-1a2b3c4d",
            ".js",
        ] {
            assert_eq!(cache_control(Path::new(name)), REVALIDATE, "{name}");
        }
    }

    #[test]
    fn every_served_file_carries_its_type_its_cache_policy_and_the_hardening() {
        let index = headers(Path::new("index.html"));
        assert!(index.contains(&("content-type", "text/html; charset=utf-8")));
        assert!(index.contains(&("cache-control", REVALIDATE)));
        assert!(index.contains(&("referrer-policy", "no-referrer")));
        assert!(index.contains(&("x-content-type-options", "nosniff")));
        assert!(index.contains(&("content-security-policy", CSP)));

        let hashed = headers(Path::new("app-1a2b3c4d.js"));
        assert!(hashed.contains(&("cache-control", IMMUTABLE)));
        assert!(hashed.contains(&("content-security-policy", CSP)));
    }

    #[test]
    fn the_csp_admits_what_the_page_needs_and_nothing_else() {
        // The page dials whatever server its invite names, so the socket
        // directives stay open; everything else is the page's own origin.
        assert!(CSP.contains("connect-src 'self' ws: wss: http: https:"));
        assert!(CSP.contains("script-src 'self' 'unsafe-inline'"));
        assert!(CSP.starts_with("default-src 'none'"));
        assert!(CSP.contains("frame-ancestors 'none'"));
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
