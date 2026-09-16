use std::env;
use std::io;
use std::net::SocketAddr;
use std::process::exit;
use std::time::Duration;

use selvage_protocol::Meta;
use selvaged::{Server, ServerConfig};

const DEFAULT_ADDRESS: &str = "127.0.0.1:8080";
const USAGE: &str = "usage: selvaged [--listen ADDR] [--room-grace-ms MS]";

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
            println!("{}", Meta::reference().server);
            return Ok(());
        }
        Action::Run(addr, room_grace) => run(addr, room_grace).await,
    }
}

async fn run(addr: SocketAddr, room_grace: Duration) -> io::Result<()> {
    let config = ServerConfig {
        room_grace,
        ..ServerConfig::default()
    };
    let server = match Server::bind(addr, config).await {
        Ok(server) => server,
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            eprintln!("{}", addr_in_use_hint(addr));
            exit(1);
        }
        Err(error) => return Err(error),
    };
    for line in startup_lines(server.local_addr(), room_grace) {
        println!("{line}");
    }
    server.run().await;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Run(SocketAddr, Duration),
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
    let mut room_grace = ServerConfig::default().room_grace;
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
                room_grace = grace(&value)?;
            }
            _ => return Err(format!("unknown argument: {arg}\n{USAGE}")),
        }
    }
    Ok(Action::Run(addr, room_grace))
}

fn help_text() -> String {
    format!(
        "{USAGE}\n\nA memory-only room server. It mints nothing to share on its own: \
        connect a client, and the client's output carries the invite link.\n\n  --listen ADDR       \
        address to bind (default {DEFAULT_ADDRESS})\n  --room-grace-ms MS  \
        how long a room survives its host disconnecting, in milliseconds \
        (default {}s)\n  --help, -h          print this help\n  --version           \
        print the server version",
        ServerConfig::default().room_grace.as_secs()
    )
}

fn address(value: &str) -> Result<SocketAddr, String> {
    value.parse().map_err(|e| {
        format!("--listen wants an address, e.g. 127.0.0.1:8080\n{e}")
    })
}

fn grace(value: &str) -> Result<Duration, String> {
    let ms: u64 = value.parse().map_err(|e| {
        format!(
            "--room-grace-ms wants a number of milliseconds, e.g. 30000\n{e}"
        )
    })?;
    Ok(Duration::from_millis(ms))
}

/// A plain-language refusal when the bind address is taken, with the next step.
fn addr_in_use_hint(addr: SocketAddr) -> String {
    format!(
        "address {addr} is already in use — stop the process holding it, or bind \
        somewhere else with `--listen {}:0` (the server prints the port it got)",
        addr.ip()
    )
}

/// The startup lines: what is listening, how long rooms outlive their host, and what
/// the host does next. The invite itself is minted host-side by the client library,
/// so this points at it rather than printing one.
fn startup_lines(local: SocketAddr, room_grace: Duration) -> Vec<String> {
    let mut lines = vec![format!(
        "selvaged listening on ws://{local}/session (meta at http://{local}/meta)"
    )];
    if local.ip().is_loopback() {
        lines.push(format!(
            "note: {local} is loopback-only, so friends cannot reach it — bind \
            `--listen 0.0.0.0:PORT` and hand them a URL that reaches your machine \
            (a tunnel works for a first test)"
        ));
    }
    lines.push(format!(
        "rooms live {}s after their host disconnects — rejoin with the same invite \
        link within the window to keep the room",
        room_grace.as_secs_f64()
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

    #[test]
    fn help_and_version_flags_win() {
        assert_eq!(args(&["--help"]), Ok(Action::Help));
        assert_eq!(args(&["-h"]), Ok(Action::Help));
        assert_eq!(args(&["--version"]), Ok(Action::Version));
    }

    #[test]
    fn defaults_bind_loopback_with_the_reference_grace() {
        let default_grace = ServerConfig::default().room_grace;
        assert_eq!(
            args(&[]),
            Ok(Action::Run(
                DEFAULT_ADDRESS.parse().expect("the default binds"),
                default_grace
            ))
        );
    }

    #[test]
    fn flags_set_the_bind_address_and_the_grace() {
        let action =
            args(&["--listen", "0.0.0.0:9000", "--room-grace-ms", "5000"])
                .expect("valid flags parse");
        assert_eq!(
            action,
            Action::Run(
                "0.0.0.0:9000".parse().expect("the test address parses"),
                Duration::from_secs(5)
            )
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
        let not_a_number = args(&["--room-grace-ms", "soon"])
            .expect_err("words are not millis");
        assert!(not_a_number.contains("30000"), "{not_a_number}");
    }

    #[test]
    fn help_names_every_flag_with_its_default() {
        let help = help_text();
        assert!(help.contains(USAGE), "{help}");
        assert!(help.contains(DEFAULT_ADDRESS), "{help}");
        assert!(help.contains("30s"), "{help}");
        assert!(help.contains("--version"), "{help}");
    }

    #[test]
    fn version_matches_what_meta_serves() {
        assert_eq!(
            Meta::reference().server,
            "selvaged/".to_owned() + env!("CARGO_PKG_VERSION")
        );
    }

    #[test]
    fn a_taken_address_suggests_the_next_step() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().expect("parses");
        let hint = addr_in_use_hint(addr);
        assert!(hint.contains("already in use"), "{hint}");
        assert!(hint.contains("--listen 127.0.0.1:0"), "{hint}");
    }

    #[test]
    fn startup_points_loopback_hosts_at_the_next_step() {
        let local: SocketAddr = "127.0.0.1:8080".parse().expect("parses");
        let lines = startup_lines(local, Duration::from_secs(30));
        let joined = lines.join("\n");
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

    #[test]
    fn startup_stays_quiet_for_a_public_bind() {
        let local: SocketAddr = "0.0.0.0:8080".parse().expect("parses");
        let joined = startup_lines(local, Duration::from_secs(30)).join("\n");
        assert!(!joined.contains("loopback-only"), "{joined}");
        assert!(joined.contains("same invite link"), "{joined}");
    }
}
