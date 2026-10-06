//! The storage contract's conformance suite (feature `conformance`).
//!
//! Every check is an async function that opens a fresh, empty store from a
//! [`Backend`] and asserts what the contract promises, through the area traits
//! alone. A backend runs the whole suite from its own tests with
//! [`conformance_tests!`](crate::conformance_tests), which expands to one
//! `#[test]` per check:
//!
//! ```ignore
//! use fridica_core::store::conformance::Backend;
//!
//! struct Sqlite;
//! impl Backend for Sqlite {
//!     type Guard = tempfile::TempDir;
//!     type Store = my_backend::Store;
//!     async fn fresh() -> (Self::Guard, Self::Store) {
//!         let dir = tempfile::tempdir().unwrap();
//!         let store = my_backend::Store::open(dir.path().join("state")).await.unwrap();
//!         (dir, store)
//!     }
//! }
//!
//! fridica_core::conformance_tests!(Sqlite);
//! ```
//!
//! Each test runs its check on a current-thread tokio runtime of its own
//! ([`run`]), so the backend's tests need no test macros. A check fails by
//! panicking, like any test.
use super::{ArrivedMessage, ClaimedJob, ParentTurn, Store};
use crate::{
    config::{registry::Registry, Limits},
    delivery::Post,
};
use std::future::Future;

mod threads;
mod unit;
mod values;
mod work;
pub use threads::*;
pub use unit::*;
pub use values::*;
pub use work::*;

/// A storage backend under test.
pub trait Backend {
    /// What must live as long as the store: a temporary directory, a server
    /// handle, or `()`.
    type Guard;
    /// The backend's store.
    type Store: Store + 'static;
    /// A fresh, empty store, as the host would open it.
    fn fresh() -> impl Future<Output = (Self::Guard, Self::Store)>;
}

/// Run one check to completion on a current-thread tokio runtime (with its
/// I/O and time drivers enabled).
pub fn run(check: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime for the check")
        .block_on(check)
}

/// One `#[test]` per conformance check, run against `$backend`, an
/// implementation of [`store::conformance::Backend`](crate::store::conformance::Backend).
///
/// ```ignore
/// fridica_core::conformance_tests!(MyBackend);
/// ```
#[macro_export]
macro_rules! conformance_tests {
    ($backend:ty) => {
        $crate::conformance_tests!(@checks $backend;
            // unit
            a_failed_unit_of_work_leaves_nothing_behind,
            a_unit_of_work_returns_its_result,
            the_ledger_appends_in_order_and_completes_calls,
            health_events_are_deduplicated_by_time_or_by_detail,
            the_ledger_finds_intake_senders_and_repeats,
            the_ledger_finds_the_latest_attachment_context,
            a_github_pause_keeps_the_later_end,
            // values
            views_read_threads_messages_and_files,
            owner_notes_are_revised_and_audited,
            slack_names_are_kept_as_recorded,
            slack_identity_is_kept_as_reported,
            slack_scopes_are_read_when_recorded,
            the_feed_reads_posts_jobs_and_messages,
            the_socket_status_is_kept_for_the_runtime_row,
            the_runtime_row_starts_and_restarts,
            catch_up_keeps_watermarks_and_truncated_passes,
            file_lookups_find_own_uploads_and_thread_attachments,
            the_supervisor_interrupts_and_fingerprints_workers,
            // threads
            intake_keeps_messages_once_and_claims_one_item_at_a_time,
            a_reserved_reply_is_answered_once_its_obligations_are_open,
            obligations_are_signalled_disposed_and_queued_when_due,
            a_historical_review_opens_deferred_mentions_and_is_found_by_client_id,
            thread_controls_record_their_effects,
            worker_stops_are_queued_once_per_worker,
            thread_memory_keeps_decisions_and_parent_notes,
            a_turn_loads_its_thread_and_settles_only_while_current,
            a_failed_or_rate_limited_turn_keeps_its_evidence,
            a_committed_turn_records_its_effects_once_fenced,
            a_thread_without_refused_posts_has_none_to_rewrite,
            a_thread_driver_is_set_audited_and_fences_turns,
            a_failed_decision_is_retried_once_by_the_owner,
            // work
            the_outbox_queues_once_and_fences_delivery_attempts,
            a_post_being_sent_is_confirmed_once,
            jobs_are_claimed_progressed_and_completed,
            approvals_begin_settle_and_cancel,
            a_fetch_is_fenced_by_its_job_attempt,
            worker_controls_are_queued_current_and_completed,
            links_are_recorded_and_backfilled_once,
            a_job_keeps_its_tags_and_the_feed_finds_it,
        );
    };
    (@checks $backend:ty; $($check:ident),* $(,)?) => {
        $(
            #[test]
            fn $check() {
                $crate::store::conformance::run(
                    $crate::store::conformance::$check::<$backend>(),
                );
            }
        )*
    };
}

// Fixtures shared by the checks.

/// A message in thread `T:C:1.0` from `U2`; it mentions the owner `U1` when
/// its text does.
fn arrived(event: &str, ts: &str, text: &str) -> ArrivedMessage {
    ArrivedMessage {
        event_id: event.into(),
        workspace: "T".into(),
        channel: "C".into(),
        ts: ts.into(),
        root_ts: "1.0".into(),
        thread_ts: (ts != "1.0").then(|| "1.0".into()),
        sender: "U2".into(),
        text: text.into(),
        files: "[]".into(),
        source: "socket".into(),
        meta: Some(r#"{"b":1,"a":2}"#.into()),
        received_at: 2.0,
        attachments: "[]".into(),
        mentions_owner: text.contains("<@U1>"),
    }
}

/// A plain message: workspace `T`, the given channel, sender, source and
/// attachments, received at `received_at`.
#[allow(clippy::too_many_arguments)]
fn message(
    event: &str,
    channel: &str,
    ts: &str,
    root_ts: &str,
    sender: &str,
    text: &str,
    source: &str,
    received_at: f64,
    attachments: &str,
) -> ArrivedMessage {
    ArrivedMessage {
        event_id: event.into(),
        workspace: "T".into(),
        channel: channel.into(),
        ts: ts.into(),
        root_ts: root_ts.into(),
        thread_ts: (ts != root_ts).then(|| root_ts.into()),
        sender: sender.into(),
        text: text.into(),
        files: "[]".into(),
        source: source.into(),
        meta: None,
        received_at,
        attachments: attachments.into(),
        mentions_owner: false,
    }
}

fn parent_turn(call: &str, error: &str, blocked: Option<&str>) -> ParentTurn {
    ParentTurn {
        call: Some(call.into()),
        response: r#"{"b":1,"a":2}"#.into(),
        context: r#"{"call":"decide"}"#.into(),
        error: error.into(),
        created: Some(3.0),
        blocked: blocked.map(str::to_owned),
    }
}

/// A reply in thread `T:C:1`.
fn post(key: &str, text: &str, after: &str) -> Post {
    session_post(key, "T:C:1", "reply", text, after)
}

fn session_post(key: &str, session: &str, kind: &str, text: &str, after: &str) -> Post {
    Post {
        idem_key: key.into(),
        session_id: session.into(),
        kind: kind.into(),
        channel: "C".into(),
        thread_ts: Some(session.rsplit(':').next().unwrap_or_default().into()),
        text: text.into(),
        meta: None,
        filename: String::new(),
        blob: None,
        after: after.into(),
    }
}

fn machines() -> Registry {
    serde_json::from_value(serde_json::json!({"default":"m","machines":[{
        "name":"m","transport":"local","workspaces":[],"backends":["claude"],
        "default_backend":"claude","policy":{},"host":"","tags":[],"resources":{},
        "max_workers":2,"max_jobs":2,"slurm":null,"description":""}]}))
    .unwrap()
}

/// Thread `T:C:1` with worker `w1` and its queued job `j1`.
async fn thread_with_job(store: &impl Store) {
    store
        .transact(|u| {
            u.open_thread("T:C:1", "T", "C", "1", 1.0)?;
            u.add_workers(
                &serde_json::from_value::<Vec<_>>(serde_json::json!([{
                    "id":"w1","session_id":"T:C:1","machine":"m","workspace":"/w","backend":"claude"
                }]))?,
                1.0,
            )?;
            u.queue_jobs(
                &serde_json::from_value::<Vec<_>>(serde_json::json!([{
                    "id":"j1","worker_id":"w1","session_id":"T:C:1","brief":"look",
                    "fetch_repo":"r","fetch_ref":"main"
                }]))?,
                2.0,
            )
        })
        .await
        .unwrap();
}

/// Claim job `id` in `slot`.
async fn claim(store: &impl Store, id: &'static str, slot: usize) -> ClaimedJob {
    let machines = machines();
    store
        .transact(move |u| u.claim_job(id, slot, &machines, &Limits::default(), 3.0))
        .await
        .unwrap()
        .expect("the job is admitted")
}

#[cfg(test)]
mod tests {
    /// Every check is listed in `conformance_tests!`, and only checks are.
    #[test]
    fn the_macro_lists_every_check() {
        let sources = [
            include_str!("unit.rs"),
            include_str!("values.rs"),
            include_str!("threads.rs"),
            include_str!("work.rs"),
        ];
        let mut checks = sources
            .iter()
            .flat_map(|s| s.lines())
            .filter_map(|l| l.strip_prefix("pub async fn "))
            .map(|l| l.split('<').next().unwrap().to_owned())
            .collect::<Vec<_>>();
        let this = include_str!("mod.rs");
        let start = this.find("@checks $backend;\n").unwrap();
        let end = start + this[start..].find(");").unwrap();
        let mut listed = this[start..end]
            .lines()
            .skip(1)
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with("//"))
            .map(|l| l.trim_end_matches(',').to_owned())
            .collect::<Vec<_>>();
        checks.sort();
        listed.sort();
        assert_eq!(checks, listed);
    }
}
