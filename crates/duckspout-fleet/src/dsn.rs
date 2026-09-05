//! Postgres DSN parsing and rewriting — the one home for both, shared by
//! the reachability probe ([`crate::backend_check`]) and by #204's catalog
//! fault links ([`crate::link`]), which must redirect a node's catalog
//! address through a proxy without disturbing anything else in its DSN.
//!
//! # Two real forms, both supported
//!
//! `duckspout-daemon`'s `catalog.dsn` is handed to `DuckLake`'s real
//! `ATTACH`, which accepts libpq's two connection-string shapes:
//!
//! - the **URI** form, `postgres://user@host:port/db` —
//!   `duckspout-fleet --postgres-dsn`'s own CLI default; and
//! - the **keyword/value** form, `postgres:host=… port=… dbname=…` — the
//!   one `crates/duckspout-fleet/tests/fault_injection.rs` documents as
//!   the only form `DuckLake`'s `ATTACH` actually parses (issue #212).
//!
//! Handling only the first is what forced that test file to pass
//! `--skip-backend-check`: the probe simply could not read the DSN it was
//! given. Both forms are handled here, so both the probe and the fault
//! links work against either.
//!
//! # The keyword-form limitation, stated
//!
//! libpq's keyword form allows single-quoted values with embedded spaces
//! and backslash escapes (`password='a b'`). This module splits on
//! whitespace and does not implement that quoting: a token with no `=` at
//! all is a hard error rather than a silent misparse. That is enough for
//! every DSN this fleet runner constructs or is handed in practice, and it
//! fails closed rather than quietly rewriting the wrong field.

use anyhow::{Context, bail};

/// Default Postgres port, used when a DSN names a host but no port.
const DEFAULT_PORT: u16 = 5432;

/// Parses `dsn`'s host and port (module docs: URI or keyword/value form) —
/// just enough to open a bare TCP probe or to know what a fault link must
/// forward to, not a full libpq parser (userinfo, database, and every other
/// parameter are irrelevant to "where does this connect to").
///
/// # Errors
///
/// If `dsn` is neither form (no `://`, and no `key=value` tokens), or its
/// keyword form carries an unparseable `port`.
pub fn postgres_host_port(dsn: &str) -> anyhow::Result<(String, u16)> {
    if let Some((_, rest)) = dsn.split_once("://") {
        // Authority ends at the first '/' (database path) or '?' (params);
        // userinfo (`user[:pass]@`) precedes the host, so take the part
        // after the LAST '@' (a password is never expected here in
        // practice, but a literal '@' in one would still resolve to the
        // right host).
        let authority = rest.split(['/', '?']).next().unwrap_or(rest);
        let host_port = authority.rsplit('@').next().unwrap_or(authority);
        return Ok(match host_port.rsplit_once(':') {
            Some((host, port)) => (host.to_owned(), port.parse().unwrap_or(DEFAULT_PORT)),
            None => (host_port.to_owned(), DEFAULT_PORT),
        });
    }
    let keywords = keyword_tokens(dsn)?;
    let host = keywords
        .iter()
        .find_map(|(key, value)| (*key == "host").then_some((*value).to_owned()))
        .with_context(|| format!("postgres DSN {dsn:?} names no host"))?;
    let port = match keywords
        .iter()
        .find_map(|(key, value)| (*key == "port").then_some(*value))
    {
        Some(port) => port
            .parse()
            .with_context(|| format!("postgres DSN {dsn:?} has a non-numeric port {port:?}"))?,
        None => DEFAULT_PORT,
    };
    Ok((host, port))
}

/// Returns `dsn` with its host and port replaced by `host`/`port`, keeping
/// every other field (user, database, parameters, and the form itself)
/// exactly as it was — how a node is pointed at a [`crate::link::FaultLink`]
/// in front of the real catalog (§8.4, issue #204).
///
/// # Errors
///
/// If `dsn` is neither supported form (module docs).
pub fn rewrite_postgres_host_port(dsn: &str, host: &str, port: u16) -> anyhow::Result<String> {
    if let Some((scheme, rest)) = dsn.split_once("://") {
        let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(authority_end);
        let userinfo = match authority.rsplit_once('@') {
            Some((userinfo, _host_port)) => format!("{userinfo}@"),
            None => String::new(),
        };
        return Ok(format!("{scheme}://{userinfo}{host}:{port}{tail}"));
    }
    let prefix = keyword_prefix(dsn);
    let keywords = keyword_tokens(dsn)?;
    let mut rendered: Vec<String> = keywords
        .iter()
        .filter(|(key, _)| *key != "host" && *key != "port")
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    // Host and port lead, so a rewritten DSN reads the way the CLI default
    // and `deploy/compose/`'s own examples do — the remaining fields keep
    // their original relative order.
    rendered.insert(0, format!("port={port}"));
    rendered.insert(0, format!("host={host}"));
    Ok(format!("{prefix}{}", rendered.join(" ")))
}

/// The `postgres:` / `postgresql:` prefix a keyword-form DSN may carry
/// (`DuckLake`'s `ATTACH` accepts both with and without), or `""`.
fn keyword_prefix(dsn: &str) -> &str {
    for prefix in ["postgresql:", "postgres:"] {
        if dsn.starts_with(prefix) {
            return prefix;
        }
    }
    ""
}

/// Splits a keyword/value DSN into its `(key, value)` pairs, in order.
///
/// # Errors
///
/// If any whitespace-separated token is not `key=value` — including the
/// case where the whole string is some third form entirely (a bare
/// `host:port`, say), which must fail closed rather than be misread as a
/// host (module docs).
fn keyword_tokens(dsn: &str) -> anyhow::Result<Vec<(&str, &str)>> {
    let body = dsn.strip_prefix(keyword_prefix(dsn)).unwrap_or(dsn);
    let mut pairs = Vec::new();
    for token in body.split_whitespace() {
        let (key, value) = token.split_once('=').ok_or_else(|| {
            anyhow::anyhow!(
                "postgres DSN {dsn:?} is neither a `scheme://` URI nor libpq keyword/value pairs \
                 (token {token:?} has no `=`)"
            )
        })?;
        pairs.push((key, value));
    }
    if pairs.is_empty() {
        bail!("postgres DSN {dsn:?} is empty");
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_form_host_port_parses_the_compose_dsn() {
        let (host, port) =
            postgres_host_port("postgres://duckspout@127.0.0.1:5432/duckspout_catalog").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 5432);
    }

    #[test]
    fn uri_form_defaults_the_port_when_absent() {
        let (host, port) = postgres_host_port("postgres://user@dbhost/db").unwrap();
        assert_eq!(host, "dbhost");
        assert_eq!(port, 5432);
    }

    #[test]
    fn uri_form_handles_no_userinfo() {
        let (host, port) = postgres_host_port("postgresql://127.0.0.1:5433/x").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 5433);
    }

    /// The form `crates/duckspout-fleet/tests/fault_injection.rs` documents
    /// as the only one `DuckLake`'s real `ATTACH` parses (issue #212) — the
    /// reason this module exists at all rather than the URI-only parser
    /// `backend_check` used to carry.
    #[test]
    fn keyword_form_host_port_parses_the_ducklake_attach_dsn() {
        let (host, port) = postgres_host_port(
            "postgres:host=127.0.0.1 port=5432 dbname=duckspout_catalog user=duckspout",
        )
        .unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 5432);
    }

    #[test]
    fn keyword_form_works_without_the_scheme_prefix_and_defaults_the_port() {
        let (host, port) = postgres_host_port("host=dbhost dbname=x").unwrap();
        assert_eq!(host, "dbhost");
        assert_eq!(port, 5432);
    }

    /// Fails closed rather than misreading a bare `host:port` as a host —
    /// the exact case `backend_check`'s own predecessor test pinned.
    #[test]
    fn a_dsn_in_neither_form_is_rejected() {
        assert!(postgres_host_port("127.0.0.1:5432/duckspout_catalog").is_err());
        assert!(postgres_host_port("").is_err());
    }

    #[test]
    fn keyword_form_with_a_non_numeric_port_is_rejected() {
        assert!(postgres_host_port("host=db port=main").is_err());
    }

    #[test]
    fn keyword_form_with_no_host_is_rejected() {
        assert!(postgres_host_port("dbname=x user=y").is_err());
    }

    /// A rewritten URI DSN keeps its scheme, userinfo, database and
    /// parameters — only the address moves. Anything else silently dropped
    /// here would make a faulted node connect as the wrong user, or to the
    /// wrong database, and the "fault" would be a misconfiguration.
    #[test]
    fn rewriting_a_uri_dsn_moves_only_the_address() {
        let rewritten = rewrite_postgres_host_port(
            "postgres://duckspout@10.0.0.5:5432/duckspout_catalog?sslmode=disable",
            "127.0.0.1",
            34567,
        )
        .unwrap();
        assert_eq!(
            rewritten,
            "postgres://duckspout@127.0.0.1:34567/duckspout_catalog?sslmode=disable"
        );
        // And it round-trips through this module's own parser.
        assert_eq!(
            postgres_host_port(&rewritten).unwrap(),
            ("127.0.0.1".to_owned(), 34567)
        );
    }

    #[test]
    fn rewriting_a_uri_dsn_with_no_userinfo_or_path_still_works() {
        assert_eq!(
            rewrite_postgres_host_port("postgres://db", "127.0.0.1", 1).unwrap(),
            "postgres://127.0.0.1:1"
        );
    }

    /// The keyword form's rewrite keeps every other keyword — same
    /// requirement as the URI case, and the form the real fleet
    /// integration tests use.
    #[test]
    fn rewriting_a_keyword_dsn_keeps_every_other_keyword() {
        let rewritten = rewrite_postgres_host_port(
            "postgres:host=10.0.0.5 port=5432 dbname=duckspout_catalog user=duckspout",
            "127.0.0.1",
            34567,
        )
        .unwrap();
        assert_eq!(
            rewritten,
            "postgres:host=127.0.0.1 port=34567 dbname=duckspout_catalog user=duckspout"
        );
        assert_eq!(
            postgres_host_port(&rewritten).unwrap(),
            ("127.0.0.1".to_owned(), 34567)
        );
    }

    /// A keyword DSN with no `port=` at all gains one (rather than being
    /// left pointing at the default port while its host says otherwise —
    /// which would send the node straight past the fault link to the real
    /// catalog, making every catalog fault a silent no-op).
    #[test]
    fn rewriting_a_keyword_dsn_without_a_port_adds_one() {
        let rewritten =
            rewrite_postgres_host_port("host=10.0.0.5 dbname=x", "127.0.0.1", 34567).unwrap();
        assert_eq!(rewritten, "host=127.0.0.1 port=34567 dbname=x");
    }

    #[test]
    fn rewriting_a_dsn_in_neither_form_is_rejected() {
        assert!(rewrite_postgres_host_port("127.0.0.1:5432", "127.0.0.1", 1).is_err());
    }
}
