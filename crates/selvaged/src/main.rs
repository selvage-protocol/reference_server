use std::env;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::Duration;

use selvage_protocol::SERVER_NAME;
use selvaged::{Server, ServerConfig, page};

const DEFAULT_ADDRESS: &str = "127.0.0.1:8080";
/// The usage line: the flags wrapped across lines, so `--help` and an unknown-argument
/// refusal stay readable as the surface grows.
const USAGE: &str = concat!(
    "usage: selvaged [--listen ADDR] [--room-grace-ms MS] [--serve-page DIR]\n",
    "                [--max-connections N] [--max-rooms N] [--max-peers-per-room N]\n",
    "                [--max-documents-per-room N] [--outbound-queue-bytes N]\n",
    "                [--max-envelope-bytes N] [--inbound-bytes-per-sec N]\n",
    "                [--inbound-burst-bytes N] [--serve-version-1-only]",
);

#[tokio::main]
async fn main() -> io::Result<()> {
    let action = match parse_args(env::args().skip(1)) {
        Ok(action) => action,
        Err(message) => {
            eprintln!("{message}");
            exit(2);
        }
    };
    match action {
        Action::Help => {
            println!("{}", help_text());
            return Ok(());
        }
        Action::Version => {
            println!("{SERVER_NAME}");
            return Ok(());
        }
        Action::Run(plan) => run(*plan).await,
    }
}

/// What the process was asked to do: where to listen, which page to serve if any, and
/// the server's own limits.
async fn run(mut plan: Run) -> io::Result<()> {
    if let Some(root) = &plan.page
        && !root.is_dir()
    {
        eprintln!(
            "--serve-page wants a directory: {} is not one (create it, or drop \
             the flag to serve /session and /meta alone)",
            root.display()
        );
        exit(2);
    }
    // The page handler keeps a served file inside the page root by asking an open
    // descriptor what it is, through `page::FD_DIR`. A platform without it would
    // answer every page request with a 404, so it is refused once, here, instead.
    if plan.page.is_some() && !Path::new(page::FD_DIR).exists() {
        eprintln!(
            "--serve-page cannot run here: it keeps a served file inside its \
             root through {}, which this platform does not have. Drop the flag \
             to serve /session and /meta alone",
            page::FD_DIR
        );
        exit(2);
    }
    plan.config.page_root = plan.page.clone();
    let server = match Server::bind(plan.addr, plan.config.clone()).await {
        Ok(server) => server,
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            eprintln!("{}", addr_in_use_hint(plan.addr));
            exit(1);
        }
        Err(error) => return Err(error),
    };
    for line in startup_lines(server.local_addr(), &plan.config) {
        println!("{line}");
    }
    server.run().await;
    Ok(())
}

/// A parsed command line that starts a server.
#[derive(Debug, PartialEq, Eq)]
struct Run {
    addr: SocketAddr,
    page: Option<PathBuf>,
    /// Every limit the server enforces, as the flags set it.
    config: ServerConfig,
}

#[derive(Debug, PartialEq, Eq)]
enum Action {
    /// Boxed: the config is the size of the enum, and the two other variants carry
    /// nothing.
    Run(Box<Run>),
    Help,
    Version,
}

/// Reads `--listen ADDR` and `--room-grace-ms MS`, which default to localhost and the
/// reference grace period. A vector that depends on the grace period is replayed by a
/// process that can set it.
fn parse_args(raw: impl IntoIterator<Item = String>) -> Result<Action, String> {
    let mut addr: SocketAddr = DEFAULT_ADDRESS
        .parse()
        .map_err(|e| format!("bad default address: {e}"))?;
    let mut config = ServerConfig::default();
    let mut page: Option<PathBuf> = None;
    let mut args = raw.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(Action::Help),
            "--version" => return Ok(Action::Version),
            "--listen" => {
                let value = args
                    .next()
                    .ok_or("--listen wants an address, e.g. 127.0.0.1:8080")?;
                addr = address(&value)?;
            }
            "--room-grace-ms" => {
                let value = args.next().ok_or(
                    "--room-grace-ms wants a number of milliseconds, e.g. 30000",
                )?;
                config.room_grace = grace(&value)?;
            }
            // The flag that narrows the default: a server that seats `selvage/1` alone.
            // A room is pinned to the version that minted it either way.
            "--serve-version-1-only" => {
                config.serve_version_1_only = true;
            }
            "--serve-page" => {
                let value = args
                    .next()
                    .ok_or("--serve-page wants a directory, e.g. /page")?;
                page = Some(PathBuf::from(value));
            }
            // The capacity flags, each defaulting to the reference value.
            "--max-connections" => {
                config.max_connections = limit(&mut args, "--max-connections")?;
            }
            "--max-rooms" => {
                config.max_rooms = limit(&mut args, "--max-rooms")?;
            }
            "--max-peers-per-room" => {
                let flag = "--max-peers-per-room";
                config.max_peers_per_room = limit(&mut args, flag)?;
            }
            "--max-documents-per-room" => {
                let flag = "--max-documents-per-room";
                config.max_documents_per_room = limit(&mut args, flag)?;
            }
            "--outbound-queue-bytes" => {
                let flag = "--outbound-queue-bytes";
                config.max_queue_bytes = limit(&mut args, flag)?;
            }
            "--max-envelope-bytes" => {
                let flag = "--max-envelope-bytes";
                config.max_envelope_bytes = limit(&mut args, flag)?;
            }
            "--inbound-bytes-per-sec" => {
                let flag = "--inbound-bytes-per-sec";
                config.inbound_bytes_per_sec = byte_rate(&mut args, flag)?;
            }
            "--inbound-burst-bytes" => {
                let flag = "--inbound-burst-bytes";
                config.inbound_burst_bytes = byte_rate(&mut args, flag)?;
            }
            _ => return Err(format!("unknown argument: {arg}\n{USAGE}")),
        }
    }
    // The outbound queue has to hold one whole frame, and the largest frame this
    // configuration can be asked to carry is the room's open-document set echoed to every
    // peer, the whole grant, or a relayed payload. A queue below that does not bound
    // memory, it breaks sessions: a handshake frame nobody can queue seats nobody, and
    // every peer — the one that asked included — is dropped for opening a path the server
    // echoed. What a frame costs is its wire bytes, so a set of paths JSON has to escape
    // costs twice their length, which is the usual reason this fires.
    let smallest = config.smallest_queue_bytes();
    if config.max_queue_bytes < smallest {
        return Err(format!(
            "--outbound-queue-bytes {} is below the {smallest} bytes this configuration \
             needs: the queue has to hold one whole frame, which is the room's \
             open-document set echoed to every peer, the whole grant, or a relayed \
             payload, counted in the bytes the frame wires to. Raise \
             --outbound-queue-bytes, or lower --max-documents-per-room",
            config.max_queue_bytes
        ));
    }
    Ok(Action::Run(Box::new(Run { addr, page, config })))
}

/// Reads one numeric flag's value, refusing a missing or non-numeric one by naming the
/// flag and the value it defaults to.
fn limit(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<usize, String> {
    let (value, example) = demanded(args, flag)?;
    value
        .parse()
        .map_err(|_| format!("{flag} wants a whole number, e.g. {example}"))
}

/// [`limit`] for a rate or a byte count, refused in the unit a reader sizes with.
fn byte_rate(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<usize, String> {
    let (value, example) = demanded(args, flag)?;
    value.parse().map_err(|_| {
        format!("{flag} wants a whole number of bytes, e.g. {example}")
    })
}

/// The value a flag was given, and the value it defaults to, so that a refusal can show
/// both. The default is looked up from the flag's own name, which is the one place the
/// two could drift apart.
fn demanded(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<(String, String), String> {
    let default = default_for(flag);
    let value = args
        .next()
        .ok_or_else(|| format!("{flag} wants a value, e.g. {default}"))?;
    Ok((value, default))
}

/// What `flag` defaults to, printed in the unit its own help line uses.
fn default_for(flag: &str) -> String {
    let default = ServerConfig::default();
    match flag {
        "--max-connections" => default.max_connections.to_string(),
        "--max-rooms" => default.max_rooms.to_string(),
        "--max-peers-per-room" => default.max_peers_per_room.to_string(),
        "--max-documents-per-room" => {
            default.max_documents_per_room.to_string()
        }
        "--outbound-queue-bytes" => default.max_queue_bytes.to_string(),
        "--max-envelope-bytes" => default.max_envelope_bytes.to_string(),
        "--inbound-bytes-per-sec" => default.inbound_bytes_per_sec.to_string(),
        "--inbound-burst-bytes" => default.inbound_burst_bytes.to_string(),
        _ => String::new(),
    }
}

fn help_text() -> String {
    let default = ServerConfig::default();
    let mut lines = vec![
        USAGE.to_string(),
        String::new(),
        "A memory-only room server. It mints nothing to share on its own: connect a \
         client, and the client's output carries the invite link."
            .to_string(),
        String::new(),
    ];
    let mut flags = endpoint_help(&default);
    flags.extend(capacity_help(&default));
    flags.extend(abuse_help(&default));
    flags.push(("--help, -h", "print this help".to_string()));
    flags.push((
        "--version",
        format!("print the server version ({SERVER_NAME})"),
    ));
    let width = flags
        .iter()
        .map(|(name, _)| name.len())
        .max()
        .unwrap_or_default()
        .saturating_add(2);
    for (name, description) in flags {
        lines.push(format!("  {name:width$}{description}"));
    }
    lines.join("\n")
}

/// The flags that say where the server listens and what it serves.
fn endpoint_help(default: &ServerConfig) -> Vec<(&'static str, String)> {
    vec![
        (
            "--listen ADDR",
            format!("address to bind (default {DEFAULT_ADDRESS})"),
        ),
        (
            "--room-grace-ms MS",
            format!(
                "how long a room survives its host disconnecting, in milliseconds \
                 (default {}s)",
                default.room_grace.as_secs()
            ),
        ),
        (
            "--serve-page DIR",
            "serve the browser page from DIR, on the same origin as /session and /meta"
                .to_string(),
        ),
        (
            "--serve-version-1-only",
            "seat `selvage/1` alone. Every version is seated by default, so `/meta` \
             advertises `selvage/2` too; a server that must refuse a version-2 hello \
             — the version-1 corpus's own shape — names this. A room is pinned to \
             the version that minted it either way"
                .to_string(),
        ),
    ]
}

/// The flags that size what one server holds. Each is a bound on this process's own
/// memory, which only this process can enforce, and each defaults to the reference
/// value.
fn capacity_help(default: &ServerConfig) -> Vec<(&'static str, String)> {
    vec![
        (
            "--max-connections N",
            format!(
                "connections held at once, counted past the request head (default {})",
                default.max_connections
            ),
        ),
        (
            "--max-rooms N",
            format!(
                "rooms held at once; past it a room is not minted (default {})",
                default.max_rooms
            ),
        ),
        (
            "--max-peers-per-room N",
            format!(
                "peers one room seats at once (default {})",
                default.max_peers_per_room
            ),
        ),
        (
            "--max-documents-per-room N",
            format!(
                "paths one room's open-document set holds (default {})",
                default.max_documents_per_room
            ),
        ),
        (
            "--outbound-queue-bytes N",
            format!(
                "payload bytes queued but unwritten for one connection; past it the \
                 peer is dropped as one that stopped reading (default {})",
                mib_label(default.max_queue_bytes)
            ),
        ),
    ]
}

/// The flags that bound what one connection may send.
fn abuse_help(default: &ServerConfig) -> Vec<(&'static str, String)> {
    vec![
        (
            "--max-envelope-bytes N",
            format!(
                "largest inbound text envelope, judged before the JSON parse \
                 (default {})",
                mib_label(default.max_envelope_bytes)
            ),
        ),
        (
            "--inbound-bytes-per-sec N",
            format!(
                "bytes one connection may send a second, refilled continuously \
                 (default {}/s)",
                mib_label(default.inbound_bytes_per_sec)
            ),
        ),
        (
            "--inbound-burst-bytes N",
            format!(
                "how much of that rate one connection may spend at once (default {})",
                mib_label(default.inbound_burst_bytes)
            ),
        ),
    ]
}

/// A byte count in the largest whole unit that does not lie about it: a limit of
/// 1,000,000 bytes is not "0 MiB".
fn mib_label(bytes: usize) -> String {
    const MIB: usize = 1024 * 1024;
    match bytes.checked_div(MIB) {
        Some(whole) if whole.saturating_mul(MIB) == bytes => {
            format!("{whole} MiB")
        }
        _ => format!("{bytes} bytes"),
    }
}

fn address(value: &str) -> Result<SocketAddr, String> {
    value.parse().map_err(|_| {
        "--listen wants an IP address and port, e.g. 127.0.0.1:8080 (a hostname \
         is not an address)"
            .to_string()
    })
}

fn grace(value: &str) -> Result<Duration, String> {
    let ms: u64 = value.parse().map_err(|_| {
        "--room-grace-ms wants a number of milliseconds, e.g. 30000".to_string()
    })?;
    Ok(Duration::from_millis(ms))
}

/// A plain-language refusal when the bind address is taken, with the next step.
fn addr_in_use_hint(addr: SocketAddr) -> String {
    format!(
        "address {addr} is already in use — stop the process holding it, or bind \
        somewhere else with `--listen {}` (the server prints the port it got)",
        SocketAddr::new(addr.ip(), 0)
    )
}

/// The startup lines: what is listening, the limits this process enforces, how long
/// rooms outlive their host, and what the host does next. The invite itself is minted
/// host-side by the client library, so this points at it rather than printing one.
///
/// The limits are printed rather than left to `--help`: a deployment reads its own
/// container's log, and a limit that is not what its operator thought it was is exactly
/// the mistake this line exists to make visible.
fn startup_lines(local: SocketAddr, config: &ServerConfig) -> Vec<String> {
    let mut lines = vec![
        format!(
            "selvaged listening on ws://{local}/session (meta at http://{local}/meta)"
        ),
        format!(
            "limits: {} connections, {} rooms, {} peers per room, {} documents per \
             room, {} outbound per connection, {} inbound text envelope, {}/s inbound \
             with a {} burst",
            config.max_connections,
            config.max_rooms,
            config.max_peers_per_room,
            config.max_documents_per_room,
            mib_label(config.max_queue_bytes),
            mib_label(config.max_envelope_bytes),
            mib_label(config.inbound_bytes_per_sec),
            mib_label(config.inbound_burst_bytes),
        ),
    ];
    if let Some(root) = &config.page_root {
        lines.push(format!(
            "serving the page from {} at http://{local}/",
            root.display()
        ));
    }
    if local.ip().is_loopback() {
        lines.push(format!(
            "note: {local} is loopback-only, so friends cannot reach it — bind \
            `--listen 0.0.0.0:PORT` and hand them a URL that reaches your machine \
            (a tunnel works for a first test)"
        ));
    }
    if local.ip().is_unspecified() {
        lines.push(format!(
            "note: {local} listens on every interface — replace the wildcard with \
            a hostname or address your friends can reach (a tunnel URL works for \
            a first test)"
        ));
    }
    lines.push(format!(
        "rooms live {}s after their host disconnects — rejoin with the same invite \
        link within the window to keep the room",
        config.room_grace.as_secs_f64()
    ));
    lines.push(
        "connect a client to mint a room; the client prints the invite link to share. \
        Keep this process running — Ctrl-C ends all rooms."
            .to_string(),
    );
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Result<Action, String> {
        parse_args(items.iter().map(ToString::to_string))
    }

    /// A parsed run of `flags` on top of the defaults.
    fn plan(flags: &[&str]) -> Run {
        match args(flags).expect("the flags parse") {
            Action::Run(plan) => *plan,
            other @ (Action::Help | Action::Version) => {
                panic!("a run was expected, got {other:?}")
            }
        }
    }

    /// The default plan, which is what no flags at all must produce.
    fn defaults() -> Run {
        plan(&[])
    }

    /// A config differing from the defaults only in the grace period, for the startup
    /// lines of a test that does not care about the limits.
    fn config_with_grace(seconds: u64) -> ServerConfig {
        ServerConfig {
            room_grace: Duration::from_secs(seconds),
            ..ServerConfig::default()
        }
    }

    #[test]
    fn help_and_version_flags_win() {
        assert_eq!(args(&["--help"]), Ok(Action::Help));
        assert_eq!(args(&["-h"]), Ok(Action::Help));
        assert_eq!(args(&["--version"]), Ok(Action::Version));
    }

    #[test]
    fn defaults_bind_loopback_with_the_reference_grace() {
        let plan = defaults();
        assert_eq!(
            plan.addr,
            DEFAULT_ADDRESS.parse().expect("the default binds")
        );
        assert_eq!(plan.page, None);
        assert_eq!(plan.config, ServerConfig::default());
    }

    /// Both wire versions are seated by default, and the one flag that narrows it reaches
    /// the field `/meta` and the handshake both read.
    #[test]
    fn the_default_seats_both_wire_versions_and_one_flag_narrows_it() {
        assert_eq!(
            defaults().config.wire_versions(),
            vec![selvage_protocol::Version::V1, selvage_protocol::Version::V2],
            "a server with no flags seats both versions"
        );
        let only = plan(&["--serve-version-1-only"]);
        assert!(only.config.serve_version_1_only);
        assert_eq!(
            only.config.wire_versions(),
            vec![selvage_protocol::Version::V1]
        );
    }

    #[test]
    fn flags_set_the_bind_address_and_the_grace() {
        let plan =
            plan(&["--listen", "0.0.0.0:9000", "--room-grace-ms", "5000"]);
        assert_eq!(
            plan.addr,
            "0.0.0.0:9000".parse().expect("the test address parses")
        );
        assert_eq!(plan.config.room_grace, Duration::from_secs(5));
        assert_eq!(plan.page, None);
    }

    /// Every capacity flag reaches the field it names, and nothing else moves: a
    /// deployment that sizes its box passes these instead of rebuilding the server.
    #[test]
    fn the_capacity_flags_set_their_own_limits() {
        let plan = plan(&[
            "--max-connections",
            "8",
            "--max-rooms",
            "7",
            "--max-peers-per-room",
            "6",
            "--max-documents-per-room",
            "5",
            "--outbound-queue-bytes",
            "16777216",
        ]);
        assert_eq!(plan.config.max_connections, 8);
        assert_eq!(plan.config.max_rooms, 7);
        assert_eq!(plan.config.max_peers_per_room, 6);
        assert_eq!(plan.config.max_documents_per_room, 5);
        assert_eq!(plan.config.max_queue_bytes, 16 * 1024 * 1024);
        let untouched = ServerConfig {
            max_connections: 8,
            max_rooms: 7,
            max_peers_per_room: 6,
            max_documents_per_room: 5,
            max_queue_bytes: 16 * 1024 * 1024,
            ..ServerConfig::default()
        };
        assert_eq!(plan.config, untouched);
    }

    /// The two bounds on what one connection may send are flags too, and they land in
    /// the fields the server reads them from.
    #[test]
    fn the_abuse_flags_set_their_own_limits() {
        let plan = plan(&[
            "--max-envelope-bytes",
            "262144",
            "--inbound-bytes-per-sec",
            "1048576",
            "--inbound-burst-bytes",
            "2097152",
        ]);
        assert_eq!(plan.config.max_envelope_bytes, 256 * 1024);
        assert_eq!(plan.config.inbound_bytes_per_sec, 1024 * 1024);
        assert_eq!(plan.config.inbound_burst_bytes, 2 * 1024 * 1024);
    }

    /// A limit that is not a whole number names its flag and shows the value it should
    /// have taken, so the operator does not have to read `--help` to find the mistake.
    #[test]
    fn a_limit_that_is_not_a_number_names_its_flag() {
        for (flag, wanted) in [
            ("--max-connections", "1024"),
            ("--max-rooms", "1024"),
            ("--max-peers-per-room", "128"),
            ("--max-documents-per-room", "1024"),
            ("--outbound-queue-bytes", "33554432"),
            ("--max-envelope-bytes", "5242880"),
            ("--inbound-bytes-per-sec", "2097152"),
            ("--inbound-burst-bytes", "67108864"),
        ] {
            let missing = args(&[flag]).expect_err("a bare limit fails");
            assert!(
                missing.contains(flag) && missing.contains(wanted),
                "{flag} names itself and its default: {missing}"
            );
            let words =
                args(&[flag, "lots"]).expect_err("words are not limits");
            assert!(
                words.contains(flag) && words.contains(wanted),
                "{flag} names itself and its default: {words}"
            );
        }
    }

    #[test]
    fn serve_page_takes_a_directory() {
        assert_eq!(
            plan(&["--serve-page", "/page"]).page,
            Some(PathBuf::from("/page"))
        );
    }

    #[test]
    fn the_last_serve_page_wins() {
        assert_eq!(
            plan(&["--serve-page", "/a", "--serve-page", "/b"]).page,
            Some(PathBuf::from("/b"))
        );
    }

    #[test]
    fn unknown_arguments_lead_with_the_offender() {
        let error = args(&["--bogus"]).expect_err("an unknown flag fails");
        assert!(
            error.starts_with("unknown argument: --bogus"),
            "the eye lands on the offender first, not the usage: {error}"
        );
        assert!(error.contains(USAGE), "usage still follows: {error}");
    }

    #[test]
    fn missing_values_say_what_they_want() {
        let listen = args(&["--listen"]).expect_err("a bare --listen fails");
        assert!(listen.contains("127.0.0.1:8080"), "{listen}");
        let grace = args(&["--room-grace-ms"]).expect_err("a bare grace fails");
        assert!(grace.contains("30000"), "{grace}");
        let page = args(&["--serve-page"]).expect_err("a bare page fails");
        assert!(page.contains("/page"), "{page}");
        let not_a_number = args(&["--room-grace-ms", "soon"])
            .expect_err("words are not millis");
        assert!(not_a_number.contains("30000"), "{not_a_number}");
    }

    /// The help names every flag and every default a flag takes, including the ones
    /// whose default is a byte count the reader would otherwise have to look up.
    #[test]
    fn help_names_every_flag_with_its_default() {
        let help = help_text();
        assert!(help.contains(USAGE), "{help}");
        assert!(help.contains(DEFAULT_ADDRESS), "{help}");
        assert!(help.contains("30s"), "{help}");
        for flag in [
            "--serve-page",
            "--serve-version-1-only",
            "--max-connections",
            "--max-rooms",
            "--max-peers-per-room",
            "--max-documents-per-room",
            "--outbound-queue-bytes",
            "--max-envelope-bytes",
            "--inbound-bytes-per-sec",
            "--inbound-burst-bytes",
            "--version",
        ] {
            assert!(help.contains(flag), "{flag} is not in the help: {help}");
        }
        let default = ServerConfig::default();
        for wanted in [
            default.max_connections.to_string(),
            default.max_rooms.to_string(),
            default.max_peers_per_room.to_string(),
            default.max_documents_per_room.to_string(),
            "32 MiB".to_string(),
            "5 MiB".to_string(),
            "2 MiB".to_string(),
            "64 MiB".to_string(),
        ] {
            assert!(
                help.contains(&wanted),
                "{wanted} is not in the help: {help}"
            );
        }
    }

    /// An outbound queue below what one frame needs is refused at the command line, with
    /// the flag named: the failure it would otherwise cause is a handshake that seats
    /// nobody, or every peer dropped, the publisher included, for a `doc.open` the
    /// server itself echoed.
    #[test]
    fn a_queue_that_cannot_hold_a_frame_is_refused() {
        // The 8 MiB frame bound `PROTOCOL.md` §2.1 states, which a deployment reaches for
        // first and which is not the floor at the reference document cap.
        const FRAME_BOUND: usize = 8 * 1024 * 1024;
        let smallest = ServerConfig::default().smallest_queue_bytes();
        // The floor counts the frame and not the path: the reference document cap holds
        // 4 MiB of path bytes, which JSON may write as 8 MiB, so the largest frame the
        // server can echo is wider than the frame bound.
        assert!(
            smallest > FRAME_BOUND,
            "an escaped set is wider than the frame bound: {smallest}"
        );
        let refused = args(&[
            "--outbound-queue-bytes",
            &smallest.saturating_sub(1).to_string(),
        ])
        .expect_err("a queue below one frame is refused");
        assert!(refused.contains("--outbound-queue-bytes"), "{refused}");
        assert!(refused.contains(&smallest.to_string()), "{refused}");
        // The floor is not a fixed number: a document set past it raises it, so the same
        // queue is refused for one configuration and accepted for another.
        let wide = args(&[
            "--outbound-queue-bytes",
            &smallest.to_string(),
            "--max-documents-per-room",
            "8192",
        ])
        .expect_err("an open-document set wider than the queue is refused");
        assert!(wide.contains("--max-documents-per-room"), "{wide}");
        let accepted = plan(&["--outbound-queue-bytes", &smallest.to_string()]);
        assert_eq!(accepted.config.max_queue_bytes, smallest);
    }

    /// A byte count reads as the unit a reader sizes a box in, and a count that is not
    /// a whole number of them is not rounded into a lie.
    #[test]
    fn byte_labels_do_not_round_the_truth_away() {
        assert_eq!(mib_label(32 * 1024 * 1024), "32 MiB");
        assert_eq!(mib_label(1024 * 1024), "1 MiB");
        assert_eq!(mib_label(1_000_000), "1000000 bytes");
        assert_eq!(mib_label(0), "0 MiB");
    }

    #[test]
    fn version_matches_what_meta_serves() {
        assert_eq!(
            SERVER_NAME,
            "selvaged/".to_owned() + env!("CARGO_PKG_VERSION")
        );
    }

    #[test]
    fn a_taken_address_suggests_the_next_step() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().expect("parses");
        let hint = addr_in_use_hint(addr);
        assert!(hint.contains("already in use"), "{hint}");
        assert!(hint.contains("--listen 127.0.0.1:0"), "{hint}");
        let six: SocketAddr = "[::1]:8080".parse().expect("parses");
        let six_hint = addr_in_use_hint(six);
        assert!(six_hint.contains("--listen [::1]:0"), "{six_hint}");
    }

    #[test]
    fn startup_points_loopback_hosts_at_the_next_step() {
        let local: SocketAddr = "127.0.0.1:8080".parse().expect("parses");
        let joined = startup_lines(local, &config_with_grace(30)).join("\n");
        assert!(joined.contains("ws://127.0.0.1:8080/session"), "{joined}");
        assert!(joined.contains("loopback-only"), "{joined}");
        assert!(joined.contains("0.0.0.0"), "{joined}");
        assert!(
            joined.contains("rooms live 30s"),
            "seconds, not milliseconds: {joined}"
        );
        assert!(!joined.contains("30000ms"), "{joined}");
        assert!(joined.contains("same invite link"), "{joined}");
        assert!(joined.contains("Ctrl-C ends all rooms"), "{joined}");
    }

    /// The startup line reports the limits this process actually enforces, which is what
    /// makes a deployment whose limits are not what its operator thought visible in its
    /// own log.
    #[test]
    fn startup_names_the_limits_in_force() {
        let local: SocketAddr = "127.0.0.1:8080".parse().expect("parses");
        let config = ServerConfig {
            max_connections: 8,
            max_rooms: 7,
            max_peers_per_room: 6,
            max_documents_per_room: 5,
            max_queue_bytes: 4 * 1024 * 1024,
            max_envelope_bytes: 1024 * 1024,
            inbound_bytes_per_sec: 512 * 1024,
            inbound_burst_bytes: 2 * 1024 * 1024,
            ..config_with_grace(30)
        };
        let joined = startup_lines(local, &config).join("\n");
        for wanted in [
            "limits: 8 connections, 7 rooms, 6 peers per room, 5 documents per room",
            "4 MiB outbound per connection",
            "1 MiB inbound text envelope",
            "524288 bytes/s inbound with a 2 MiB burst",
        ] {
            assert!(joined.contains(wanted), "{wanted} is not named: {joined}");
        }
    }

    #[test]
    fn startup_guides_a_wildcard_bind() {
        let local: SocketAddr = "0.0.0.0:8080".parse().expect("parses");
        let joined = startup_lines(local, &config_with_grace(30)).join("\n");
        assert!(!joined.contains("loopback-only"), "{joined}");
        assert!(
            joined.contains("replace the wildcard"),
            "a wildcard is not a client URL: {joined}"
        );
    }

    #[test]
    fn startup_stays_quiet_for_a_specific_address() {
        let local: SocketAddr = "192.0.2.7:8080".parse().expect("parses");
        let joined = startup_lines(local, &config_with_grace(30)).join("\n");
        assert!(!joined.contains("loopback-only"), "{joined}");
        assert!(!joined.contains("wildcard"), "{joined}");
        assert!(joined.contains("same invite link"), "{joined}");
    }

    #[test]
    fn startup_names_the_served_page() {
        let local: SocketAddr = "127.0.0.1:8080".parse().expect("parses");
        let config = ServerConfig {
            page_root: Some(PathBuf::from("/page")),
            ..config_with_grace(30)
        };
        let joined = startup_lines(local, &config).join("\n");
        assert!(joined.contains("serving the page from /page"), "{joined}");
    }
}
