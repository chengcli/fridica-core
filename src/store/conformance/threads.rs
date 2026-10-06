//! Threads: intake, replies and obligations, owner controls, worker stops,
//! thread memory and a turn's life.
use super::{arrived, message, parent_turn, session_post, Backend};
use crate::store::{
    ActivityView, Arrival, Backfill, Disposal, Fence, HistoricalMention, HistoricalObligation,
    InboxItem, Mention, MentionQuery, NewAsk, ObligationChange, QueuedAnswer, QueuedHandoff,
    RecentReply, ReservedReply, Route, Settlement, Store, TriageSettlement, TurnClose, TurnFailure,
    TurnRetry,
};

/// The audit entries whose action is `action`.
fn actions<'a>(
    entries: &'a [ActivityView],
    action: &'a str,
) -> impl Iterator<Item = &'a ActivityView> + 'a {
    entries.iter().filter(move |entry| entry.action == action)
}

/// A message is kept once (its meta byte for byte), a thread opened once,
/// and a thread's items are claimed one at a time, oldest first.
pub async fn intake_keeps_messages_once_and_claims_one_item_at_a_time<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let (new, again, waiting, first, second, claimed, busy) = store
        .transact(|u| {
            let new = u.keep_message(&arrived("e1", "1.0", "hi"))?;
            let again = u.keep_message(&arrived("e1", "1.0", "hi"))?;
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            u.open_thread("T:C:1.0", "T", "C", "1.0", 9.0)?;
            let waiting = u.thread_waiting("T:C:1.0")?;
            let first = u.queue_message("T:C:1.0", "e1", 2.0)?;
            let second = u.queue_message("T:C:1.0", "e2", 3.0)?;
            let claimed = u.claim_next("T:C:1.0", 5.0)?;
            let busy = u.claim_next("T:C:1.0", 5.0)?;
            Ok((new, again, waiting, first, second, claimed, busy))
        })
        .await
        .unwrap();
    assert!(new && !again && !waiting && second > first);
    assert_eq!(
        claimed,
        Some(InboxItem {
            id: first,
            kind: "message".into()
        })
    );
    assert_eq!(busy, None);
    let (messages, thread) = store
        .transact(|u| Ok((u.thread_messages("T:C:1.0", 10)?, u.thread("T:C:1.0")?)))
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].meta_json.as_deref(), Some(r#"{"b":1,"a":2}"#));
    assert_eq!(thread.unwrap().created, 2.0);
}

/// A reservation counts against the thread's replies; an answer is recorded
/// once, while its obligations are open.
pub async fn a_reserved_reply_is_answered_once_its_obligations_are_open<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let post = store
        .transact(|u| u.queue_post(&session_post("k", "T:C:1.0", "reply", "hi", ""), 1.0))
        .await
        .unwrap();
    let found = store
        .transact(move |u| {
            assert!(u.thread_active("T:C:1.0").is_err());
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            let inbox = u.queue_message("T:C:1.0", "e1", 2.0)?;
            assert!(u.thread_active("T:C:1.0")? && u.inbox_open(inbox, "T:C:1.0")?);
            assert!(!u.inbox_open(inbox, "T:C:2.0")?);
            assert_eq!(u.reservation_state(inbox)?, None);
            assert!(u.reserved_reply(inbox, "T:C:1.0").is_err());
            u.reserve_reply("r1", "T:C:1.0", inbox, "peer", 3.0)?;
            u.open_mention(&Mention {
                id: "o1".into(),
                session: "T:C:1.0".into(),
                dedup_key: "mention:T:C:1.0".into(),
                source: r#"{"event_id":"e1"}"#.into(),
                created: 2.0,
                due: 9.0,
            })?;
            let answer = QueuedAnswer {
                post,
                session: "T:C:1.0".into(),
                inbox,
                trigger: "peer".into(),
                obligations: vec!["o1".into()],
                answers: r#"["o1"]"#.into(),
                time: 4.0,
            };
            let before = (
                u.reservation_state(inbox)?,
                u.reserved_reply(inbox, "T:C:1.0")?,
                u.recent_replies("T:C:1.0", 5.0)?,
                u.thread_route("T:C:1.0")?,
            );
            let answered = u.answer_queued(&answer)?;
            let reserved = u.reserved_reply(inbox, "T:C:1.0")?;
            let again = u.answer_queued(&answer)?;
            u.defer_reply("T:C:1.0", inbox, 50.0)?;
            Ok((before, answered, reserved, again, u.obligation_state("o1")?))
        })
        .await
        .unwrap();
    let ((state, reserved, recent, route), answered, after, again, obligation) = found;
    assert_eq!(state.as_deref(), Some("reserved"));
    assert_eq!(
        reserved,
        ReservedReply {
            trigger: "peer".into(),
            post: None
        }
    );
    assert_eq!(
        recent,
        [RecentReply {
            trigger: "peer".into(),
            at: 5.0
        }]
    );
    assert_eq!(
        route,
        Route {
            channel: "C".into(),
            root_ts: "1.0".into()
        }
    );
    assert_eq!((answered, after.post), (None, Some(post)));
    assert_eq!(again.as_deref(), Some("o1"));
    assert_eq!(obligation, "awaiting_delivery");
    let (posts, thread) = store
        .transact(|u| Ok((u.thread_posts("T:C:1.0")?, u.thread("T:C:1.0")?.unwrap())))
        .await
        .unwrap();
    assert_eq!(posts[0].answers_json, r#"["o1"]"#);
    assert_eq!(thread.throttled_until, 50.0);
}

/// A signal opens once, is queued once per due time, and is disposed once,
/// with its audit kept byte for byte.
pub async fn obligations_are_signalled_disposed_and_queued_when_due<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let (opened, reopened, queued, again, disposed, closed, state) = store
        .transact(|u| {
            assert!(u.obligation_state("s1").is_err());
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            let opened = u.open_signal("s1", "T:C:1.0", 3.0)?;
            let reopened = u.open_signal("s1", "T:C:1.0", 4.0)?;
            let queued = u.queue_due(5.0)?;
            let again = u.queue_due(6.0)?;
            let disposal = Disposal {
                id: "s1".into(),
                state: "declined".into(),
                details: r#"{"kind":"declined","reason":"no"}"#.into(),
                due: None,
                actor: r#""owner""#.into(),
                time: 7.0,
            };
            let disposed = u.dispose(&disposal)?;
            let closed = u.dispose(&disposal)?;
            Ok((
                opened,
                reopened,
                queued,
                again,
                disposed,
                closed,
                u.obligation_state("s1")?,
            ))
        })
        .await
        .unwrap();
    assert!(opened && !reopened && disposed && !closed);
    assert_eq!((queued, again, state.as_str()), (1, 0, "declined"));
    let activity = store.transact(|u| u.activity(100)).await.unwrap();
    let audit = actions(&activity, "obligation.disposition")
        .map(|entry| (entry.details_json.as_str(), entry.actor.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        audit,
        [(r#"{"kind":"declined","reason":"no"}"#, r#""owner""#)]
    );
}

/// A historical review finds mentions without an obligation, opens them
/// deferred, and is found again by its client ID.
pub async fn a_historical_review_opens_deferred_mentions_and_is_found_by_client_id<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let (found, after, prior, missing) = store
        .transact(|u| {
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            u.keep_message(&arrived("e1", "1.0", "hi"))?;
            u.keep_message(&arrived("e2", "3.0", "<@U1> look"))?;
            u.keep_message(&arrived("e3", "9.0", "<@U1> later"))?;
            let query = MentionQuery {
                workspace: "T".into(),
                channels: r#"["C"]"#.into(),
                since: 0.0,
                until: 5.0,
                owner: "U1".into(),
                mention: "<@U1>".into(),
            };
            let found = u.historical_mentions(&query)?;
            let obligations = found
                .iter()
                .map(|m| HistoricalObligation {
                    id: format!("backfill:mention:T:C:{}", m.ts),
                    session: m.session.clone(),
                    dedup_key: format!("mention:T:C:{}", m.ts),
                    event_id: m.event_id.clone(),
                    source: "{}".into(),
                    created: m.received_at,
                    due: 20.0,
                    state: r#"{"kind":"deferred"}"#.into(),
                    updated: 10.0,
                })
                .collect();
            u.apply_backfill(&Backfill {
                obligations,
                time: 10.0,
                actor: "U1".into(),
                client_id: "client-1".into(),
                result: r#"{"count":1}"#.into(),
            })?;
            u.record(
                "obligations_backfill",
                10.0,
                r#"{"request":{"client_id":"client-1"},"result":{"count":1}}"#,
                true,
            )?;
            Ok((
                found,
                u.historical_mentions(&query)?,
                u.backfill_record("client-1")?,
                u.backfill_record("client-2")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        found,
        [HistoricalMention {
            event_id: "e2".into(),
            session: "T:C:1.0".into(),
            workspace: "T".into(),
            channel: "C".into(),
            ts: "3.0".into(),
            received_at: 2.0
        }]
    );
    assert_eq!(after, []);
    assert_eq!(
        prior.as_deref(),
        Some(r#"{"request":{"client_id":"client-1"},"result":{"count":1}}"#)
    );
    assert_eq!(missing, None);
    let (obligations, activity) = store
        .transact(|u| Ok((u.obligations(10)?, u.activity(10)?)))
        .await
        .unwrap();
    assert_eq!(
        obligations
            .iter()
            .map(|obligation| obligation.state.as_str())
            .collect::<Vec<_>>(),
        ["deferred"]
    );
    assert_eq!(
        actions(&activity, "obligations.backfill")
            .map(|entry| entry.details_json.as_str())
            .collect::<Vec<_>>(),
        [r#"{"count":1}"#]
    );
}

/// Owner controls: a control and its details are kept, resuming reuses the
/// first unfinished entry, owner instructions are found by client ID, and
/// cleaning wipes their text.
pub async fn thread_controls_record_their_effects<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    store
        .transact(|u| {
            u.open_thread("T:C:1", "T", "C", "1", 1.0)?;
            u.close_turn(&TurnClose {
                session: "T:C:1".into(),
                status: "blocked".into(),
                reply_key: String::new(),
                turn: 1,
                waiting: 0,
                quiet: 0,
                hash: String::new(),
                summary: String::new(),
                now: 5.0,
            })?;
            u.keep_message(&message(
                "e1", "C", "1", "1", "U", "one", "slack", 1.0, "[]",
            ))?;
            u.keep_message(&message(
                "e2", "C", "2", "1", "B", "reply", "self", 2.0, "[]",
            ))?;
            u.keep_message(&message(
                "e3", "C", "3", "1", "U", "three", "slack", 3.0, "[]",
            ))?;
            u.add_workers(
                &serde_json::from_value::<Vec<_>>(serde_json::json!([{
                    "id":"W1","session_id":"T:C:1","machine":"m","workspace":"/w","backend":"claude"
                }]))?,
                1.0,
            )?;
            u.queue_message("T:C:1", "e3", 3.0)?;
            u.queue_message("T:C:1", "e3", 3.0)?;
            Ok(())
        })
        .await
        .unwrap();
    let (state, channel, live, point) = store
        .transact(|u| {
            u.set_control("T:C:1", "paused", r#"{"b":1,"a":2}"#, "why", 6.0, false)?;
            Ok((
                u.control_state("T:C:1")?,
                u.thread_channel("T:C:1")?,
                u.has_live_workers("T:C:1")?,
                u.resume_point("T:C:1")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(state.control, "paused");
    assert_eq!(state.details, r#"{"b":1,"a":2}"#);
    assert_eq!(channel, ("T".into(), "C".into()));
    assert!(live);
    // The latest message from someone else since Fridica last posted.
    assert_eq!(point.latest, Some(("e3".into(), 3.0)));
    assert_eq!(point.newest, 3.0);
    // An unknown thread is an error.
    assert!(store.transact(|u| u.control_state("T:C:9")).await.is_err());
    assert!(store.transact(|u| u.thread_channel("T:C:9")).await.is_err());

    store
        .transact(|u| {
            u.restart_turns("T:C:1")?;
            u.reset_thread_at("T:C:1", 2.5)?;
            u.resume_message("T:C:1", "e3", r#"{"resumed":true}"#, 7.0)?;
            u.resume_message("T:C:1", "e1", r#"{"resumed":true}"#, 7.0)?;
            u.audit_control(7.0, r#""owner""#, "thread.resume", "T:C:1", r#""resume""#)
        })
        .await
        .unwrap();
    let (thread, activity) = store
        .transact(|u| Ok((u.thread("T:C:1")?.unwrap(), u.activity(10)?)))
        .await
        .unwrap();
    assert_eq!(
        (thread.status.as_str(), thread.reset_at, thread.turns),
        ("complete", 2.5, 0)
    );
    assert_eq!(actions(&activity, "thread.resume").count(), 1);

    // Owner instructions are found by client ID; cleaning wipes their text.
    let (id, found, missing) = store
        .transact(|u| {
            let id = u.queue_owner_instruction("T:C:1", "client-1", r#"{"text":"go"}"#, 8.0)?;
            Ok((
                id,
                u.owner_instruction("T:C:1", "client-1")?,
                u.owner_instruction("T:C:1", "client-2")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(found, Some((id, "go".into())));
    assert_eq!(missing, None);
    let (found, text_after, thread) = store
        .transact(|u| {
            u.wipe("T:C:1", 9.0)?;
            u.release_worker_results("T:C:1")?;
            u.unblock("T:C:1")?;
            Ok((
                u.owner_instruction("T:C:1", "client-1")?,
                u.thread_messages("T:C:1", 1)?,
                u.thread("T:C:1")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(found, Some((id, String::new())));
    assert_eq!(text_after[0].text, "");
    assert!(thread.is_some());
}

/// A stop is queued once per live worker of a closed thread (stopped ones
/// only on request), kept in the ledger, and done once completed.
pub async fn worker_stops_are_queued_once_per_worker<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    store
        .transact(|u| {
            u.open_thread("T:C:1", "T", "C", "1", 1.0)?;
            u.open_thread("T:C:2", "T", "C", "2", 1.0)?;
            u.set_control("T:C:1", "closed", "{}", "", 5.0, false)?;
            u.add_workers(
                &serde_json::from_value::<Vec<_>>(serde_json::json!([
                    {"id":"W1","session_id":"T:C:1","machine":"m","workspace":"/w","backend":"claude"},
                    {"id":"W2","session_id":"T:C:1","machine":"m","workspace":"/w","backend":"claude",
                     "status":"stopped"},
                    {"id":"W3","session_id":"T:C:2","machine":"m","workspace":"/w","backend":"claude"}
                ]))?,
                1.0,
            )
        })
        .await
        .unwrap();
    let (closed, before, pending, again, other) = store
        .transact(|u| {
            let closed = u.closed_threads_with_live_workers()?;
            let before = u.worker_stop_pending("T:C:1")?;
            u.queue_worker_stops(&closed, 2.0, false)?;
            let pending = u.pending_worker_stops()?;
            // A pending stop is not queued twice; stopped workers only on request.
            u.queue_worker_stops(&closed, 3.0, true)?;
            Ok((
                closed,
                before,
                pending,
                u.pending_worker_stops()?,
                u.worker_stop_pending("T:C:2")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(closed, ["T:C:1"]);
    assert!(!before);
    assert_eq!(
        pending
            .iter()
            .map(|s| s.worker.as_str())
            .collect::<Vec<_>>(),
        ["W1"]
    );
    assert_eq!(
        again.iter().map(|s| s.worker.as_str()).collect::<Vec<_>>(),
        ["W1", "W2"]
    );
    assert!(!other);
    let (pending, after) = store
        .transact(move |u| {
            for stop in &again {
                u.complete(stop.seq, true)?;
            }
            Ok((u.worker_stop_pending("T:C:1")?, u.pending_worker_stops()?))
        })
        .await
        .unwrap();
    assert!(!pending);
    assert!(after.is_empty());
    let events = store.transact(|u| u.events_after(0, 10)).await.unwrap();
    assert_eq!(events[0].kind, "thread_worker_stop");
    assert_eq!(events[0].payload, r#"{"session":"T:C:1","worker":"W1"}"#);
}

/// Decisions are replaced whole, the parent's notes are revised and audited,
/// and a thread's queued posts are found by key.
pub async fn thread_memory_keeps_decisions_and_parent_notes<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let (notes, decisions) = store
        .transact(|u| {
            u.open_thread("T:C:1", "T", "C", "1", 1.0)?;
            let before = u.latest_notes("T:C:1")?;
            assert_eq!(before, None);
            u.keep_decisions("T:C:1", r#"["a","b"]"#)?;
            u.write_parent_notes("T:C:1", 1, r#"{"repo":"x"}"#, 2, 9.0)?;
            u.write_parent_notes("T:C:1", 2, r#"{"z":1,"a":2}"#, 3, 10.0)?;
            Ok((u.latest_notes("T:C:1")?, u.thread_decisions("T:C:1")?))
        })
        .await
        .unwrap();
    assert_eq!(notes, Some((2, r#"{"z":1,"a":2}"#.into())));
    assert_eq!(decisions, r#"["a","b"]"#);
    assert!(store
        .transact(|u| u.thread_decisions("T:C:9"))
        .await
        .is_err());
    let activity = store.transact(|u| u.activity(10)).await.unwrap();
    let audit = actions(&activity, "notes.write").collect::<Vec<_>>();
    assert_eq!(audit.len(), 2);
    assert_eq!(audit[1].actor, "parent");
    let details: serde_json::Value = serde_json::from_str(&audit[0].details_json).unwrap();
    assert_eq!(details, serde_json::json!({"revision":2,"inbox_id":3}));
    store
        .transact(|u| u.queue_post(&session_post("2:reply", "T:C:1", "reply", "hi", ""), 1.0))
        .await
        .unwrap();
    let queued = store
        .transact(|u| {
            Ok((
                u.post_queued("T:C:1", "2:reply")?,
                u.post_queued("T:C:1", "3:reply")?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(queued, (true, false));
}

/// A turn loads its thread and item, and settles only while the thread is
/// still at the version it loaded.
pub async fn a_turn_loads_its_thread_and_settles_only_while_current<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let (input, roots, decisions, live, stale, held, done, attempts) = store
        .transact(|u| {
            u.keep_message(&arrived("e1", "1.0", "<@U1> hi"))?;
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            let first = u.queue_message("T:C:1.0", "e1", 2.0)?;
            u.claim_next("T:C:1.0", 5.0)?;
            let input = u.turn_input("T:C:1.0", first)?;
            let roots = u.earlier_roots(Some("T"), Some("C"), Some("2.0"))?;
            let decisions = u.turn_decisions("T:C:1.0")?;
            let live = u.turn_live("T:C:1.0", Some(0), first)?;
            let version: i64 = serde_json::from_str::<serde_json::Value>(&input.thread)?["version"]
                .as_i64()
                .unwrap();
            let settle = |version, until| Settlement {
                id: first,
                session: "T:C:1.0".into(),
                version,
                event: Some("e1".into()),
                verdict: "observe: test".into(),
                until,
            };
            // A stale version only puts the item back.
            let stale = u.settle_turn(&settle(version + 1, None))?;
            u.claim_next("T:C:1.0", 5.0)?;
            let held = u.settle_turn(&settle(version, Some(40.0)))?;
            u.claim_next("T:C:1.0", 50.0)?;
            let done = u.settle_turn(&settle(version, None))?;
            Ok((
                input,
                roots,
                decisions,
                live,
                stale,
                held,
                done,
                u.inbox_attempts(first)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        (input.kind.as_str(), input.reference.as_str()),
        ("message", "e1")
    );
    assert!(input.message.unwrap().contains(r#""event_id":"e1""#));
    assert_eq!(
        (input.from_peer, input.history.len(), input.review_required),
        (None, 1, false)
    );
    assert_eq!(roots.len(), 1);
    assert_eq!(decisions.1, 0);
    assert!(live && !stale && held && done);
    assert_eq!(attempts, (0, "done".into()));
}

/// A failed turn opens a signal and is audited; a rate-limited one is
/// retried later; triage settles it and bumps the thread's version.
pub async fn a_failed_or_rate_limited_turn_keeps_its_evidence<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let (current, recovered, triaged, attempts) = store
        .transact(|u| {
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            let first = u.queue_message("T:C:1.0", "e1", 2.0)?;
            u.claim_next("T:C:1.0", 5.0)?;
            u.fail_turn(&TurnFailure {
                id: first,
                session: "T:C:1.0".into(),
                state: "pending".into(),
                not_before: 35.0,
                signal: "inbox-failed:1".into(),
                source: r#"{"inbox_id":1}"#.into(),
                details: r#"{"inbox_id":1,"attempt":1}"#.into(),
                now: 5.0,
            })?;
            u.claim_next("T:C:1.0", 40.0)?;
            let current = u.retry_turn(&TurnRetry {
                id: first,
                session: "T:C:1.0".into(),
                version: Some(0),
                calls: vec![parent_turn("decide", "parent_rate_limited", None)],
                retry_at: 100.0,
                details: r#"{"retry_at":100.0}"#.into(),
                now: 40.0,
            })?;
            u.claim_next("T:C:1.0", 100.0)?;
            let recovered = u.recover_turns()?;
            u.claim_next("T:C:1.0", 100.0)?;
            let triaged = u.settle_triage(&TriageSettlement {
                id: first,
                session: "T:C:1.0".into(),
                version: Some(0),
                calls: vec![parent_turn("triage", "", None)],
                event: None,
                verdict: "ignore: triage".into(),
                now: 101.0,
            })?;
            Ok((current, recovered, triaged, u.inbox_attempts(first)?))
        })
        .await
        .unwrap();
    assert!(current && triaged);
    assert_eq!(recovered, 1);
    assert_eq!(attempts, (1, "done".into()));
    let (thread, obligations, activity) = store
        .transact(|u| {
            Ok((
                u.thread("T:C:1.0")?.unwrap(),
                u.obligations(10)?,
                u.activity(10)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(thread.version, 1);
    assert_eq!(
        obligations
            .iter()
            .map(|obligation| obligation.id.as_str())
            .collect::<Vec<_>>(),
        ["inbox-failed:1"]
    );
    // Newest first.
    assert_eq!(
        activity
            .iter()
            .map(|entry| entry.action.as_str())
            .collect::<Vec<_>>(),
        ["parent.rate_limited", "inbox.failed"]
    );
}

/// A committed turn's effects: fence, arrivals, channel and thread state,
/// post labels, hand-offs, asks and obligation changes, parent calls.
pub async fn a_committed_turn_records_its_effects_once_fenced<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let post = store
        .transact(|u| u.queue_post(&session_post("1:reply", "T:C:1.0", "reply", "hi", ""), 1.0))
        .await
        .unwrap();
    let (fence, arrival, again, running, class, changed, unchanged) = store
        .transact(move |u| {
            u.keep_message(&arrived("e1", "1.0", "hi"))?;
            u.keep_message(&arrived("e2", "3.0", "later"))?;
            u.open_thread("T:C:1.0", "T", "C", "1.0", 2.0)?;
            u.open_thread("T:C:2.0", "T", "C", "2.0", 2.0)?;
            let first = u.queue_message("T:C:1.0", "e1", 2.0)?;
            u.claim_next("T:C:1.0", 5.0)?;
            let fence = u.fence_turn("T:C:1.0", first)?;
            let arrival = u.arrival("T:C:1.0", first, Some(1.0), "U1")?;
            let again = u.arrival("T:C:1.0", first, Some(1.0), "U1")?;
            u.mark_unsolicited(Some("T"), Some("C"), 6.0)?;
            assert_eq!(u.last_unsolicited(Some("T"), Some("C"))?, Some(6.0));
            u.patch_context("T:C:1.0", r#"{"repo":"x"}"#)?;
            u.label_post(post, r#"{"v":2}"#, "e1", true)?;
            u.mark_reported("T:C:1.0", &[Some("j1".into()), None])?;
            let running = u.jobs_running("T:C:1.0")?;
            let class = u.handoff_class(first)?;
            let handoff = |target: &str| QueuedHandoff {
                target: target.into(),
                from: "T:C:1.0".into(),
                payload: "{}".into(),
                dedup_key: format!("handoff:{target}"),
                queued: r#"{"queued":true}"#.into(),
                skipped: r#"{"queued":false}"#.into(),
            };
            u.queue_handoffs(&[handoff("T:C:2.0"), handoff("T:C:9.0")], 6.0)?;
            u.open_asks(
                "T:C:1.0",
                &[NewAsk {
                    id: "ask:1:0".into(),
                    source: "{}".into(),
                    summary: "Answer".into(),
                    due: 50.0,
                }],
                6.0,
            )?;
            u.answer_for_handoff("T:C:1.0", &["ask:1:0".into(), "missing".into()], post, 6.0)?;
            u.open_asks(
                "T:C:1.0",
                &[NewAsk {
                    id: "ask:1:1".into(),
                    source: "{}".into(),
                    summary: "Later".into(),
                    due: 50.0,
                }],
                6.0,
            )?;
            let change = |id: &str| ObligationChange {
                id: id.into(),
                state: "deferred".into(),
                details: r#"{"until":60.0}"#.into(),
                due: Some(60.0),
            };
            let changed = u.change_obligations("T:C:1.0", &[change("ask:1:1")], 6.0)?;
            let unchanged = u.change_obligations("T:C:1.0", &[change("ask:1:0")], 6.0)?;
            u.open_streak_signal("streak:T:C:1.0:1", "T:C:1.0", r#"{"inbox_id":1}"#, 6.0)?;
            u.open_streak_signal("streak:T:C:1.0:1", "T:C:1.0", r#"{"inbox_id":1}"#, 7.0)?;
            u.record_parent_calls(
                "T:C:1.0",
                first,
                r#"{"reply":null}"#,
                &[parent_turn(
                    "decide",
                    "parent_unavailable",
                    Some(r#"{"reason":"parent_unavailable"}"#),
                )],
                6.0,
            )?;
            u.close_turn(&TurnClose {
                session: "T:C:1.0".into(),
                status: "complete".into(),
                reply_key: "1:reply".into(),
                turn: 1,
                waiting: 0,
                quiet: 0,
                hash: "h".into(),
                summary: String::new(),
                now: 6.0,
            })?;
            u.finish_turn(first, Some("e1"), "respond: test")?;
            Ok((fence, arrival, again, running, class, changed, unchanged))
        })
        .await
        .unwrap();
    assert_eq!(
        fence,
        Fence {
            version: 0,
            active: true
        }
    );
    assert_eq!(
        arrival,
        Arrival {
            arrived: true,
            reread: false
        }
    );
    assert_eq!(
        again,
        Arrival {
            arrived: true,
            reread: true
        }
    );
    assert!(!running && changed && !unchanged);
    assert_eq!(class, "peer");
    let (posts, thread, obligations, activity, ready) = store
        .transact(|u| {
            Ok((
                u.thread_posts("T:C:1.0")?,
                u.thread("T:C:1.0")?.unwrap(),
                u.obligations(10)?,
                u.activity(10)?,
                u.ready_threads(10.0, 10)?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        (
            posts[0].kind.as_str(),
            posts[0].meta_json.as_deref(),
            posts[0].trigger_event.as_str()
        ),
        ("report", Some(r#"{"v":2}"#), "e1")
    );
    assert_eq!(
        (
            thread.context_json.as_str(),
            thread.status.as_str(),
            thread.turns,
            thread.version,
            thread.last_reply_hash.as_str()
        ),
        (r#"{"repo":"x"}"#, "complete", 1, 1, "h")
    );
    let mut states = obligations
        .iter()
        .map(|obligation| (obligation.id.as_str(), obligation.state.as_str()))
        .collect::<Vec<_>>();
    states.sort();
    assert_eq!(
        states,
        [
            ("ask:1:0", "awaiting_delivery"),
            ("ask:1:1", "deferred"),
            ("streak:T:C:1.0:1", "open"),
        ]
    );
    // Oldest first.
    assert_eq!(
        activity
            .iter()
            .rev()
            .map(|entry| entry.details_json.as_str())
            .collect::<Vec<_>>(),
        [
            r#"{"queued":true}"#,
            r#"{"queued":false}"#,
            r#"{"reason":"parent_unavailable"}"#
        ]
    );
    // The hand-off waits in its target's inbox.
    assert_eq!(ready, ["T:C:2.0"]);
}

/// A thread without refused or undelivered posts has none.
pub async fn a_thread_without_refused_posts_has_none_to_rewrite<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let (refused, undelivered) = store
        .transact(|u| Ok((u.refused_post("W:C:1", 7)?, u.undelivered_posts("W:C:1")?)))
        .await
        .unwrap();
    assert_eq!((refused, undelivered), (None, vec![]));
}

/// A thread is driven by its parent until set otherwise; a change of driver
/// bumps the thread's version (a turn loaded before it is stale), shows on
/// its view and is audited once; an unknown driver or thread is an error.
pub async fn a_thread_driver_is_set_audited_and_fences_turns<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let (initial, before) = store
        .transact(|u| {
            u.open_thread("T:C:1", "T", "C", "1", 1.0)?;
            Ok((
                u.thread_driver("T:C:1")?,
                u.thread("T:C:1")?.unwrap().version,
            ))
        })
        .await
        .unwrap();
    assert_eq!(initial, "parent");
    let (changed, again, driver, view, invalid, unknown, missing, activity) = store
        .transact(|u| {
            let changed = u.set_thread_driver("T:C:1", "external", r#"{"kind":"owner"}"#, 2.0)?;
            let again = u.set_thread_driver("T:C:1", "external", r#"{"kind":"owner"}"#, 3.0)?;
            Ok((
                changed,
                again,
                u.thread_driver("T:C:1")?,
                u.thread("T:C:1")?.unwrap(),
                u.set_thread_driver("T:C:1", "robot", "{}", 4.0).is_err(),
                u.set_thread_driver("T:C:9", "parent", "{}", 4.0).is_err(),
                u.thread_driver("T:C:9").is_err(),
                u.activity(10)?,
            ))
        })
        .await
        .unwrap();
    assert!(changed && !again);
    assert_eq!(driver, "external");
    assert_eq!(view.driver, "external");
    assert_eq!(view.version, before + 1);
    assert!(invalid && unknown && missing);
    let audited: Vec<_> = actions(&activity, "driver").collect();
    assert_eq!(audited.len(), 1);
    assert_eq!(audited[0].target, "T:C:1");
    assert_eq!(audited[0].actor, r#"{"kind":"owner"}"#);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&audited[0].details_json).unwrap(),
        serde_json::json!({"from":"parent","to":"external"})
    );
}

/// An owner retry puts the item of a thread's last failed decision back in
/// the queue and unblocks the thread, once; a thread whose last decision did
/// not fail has nothing to retry (fridica#136).
pub async fn a_failed_decision_is_retried_once_by_the_owner<B: Backend>() {
    let (_guard, store) = B::fresh().await;
    let (nothing, retried, again, attempts, status, claimed) = store
        .transact(|u| {
            u.open_thread("T:C:1", "T", "C", "1", 1.0)?;
            let ok = u.queue_message("T:C:1", "e1", 1.0)?;
            u.claim_next("T:C:1", 1.0)?;
            u.record_parent_calls("T:C:1", ok, "{}", &[parent_turn("decide", "", None)], 2.0)?;
            u.finish_turn(ok, None, "respond: addressed")?;
            let nothing = u.retry_failed_turn("T:C:1")?;
            let failed = u.queue_message("T:C:1", "e2", 3.0)?;
            u.claim_next("T:C:1", 3.0)?;
            u.record_parent_calls(
                "T:C:1",
                failed,
                "{}",
                &[parent_turn(
                    "decide",
                    "parent_unavailable",
                    Some(r#"{"failure":"parent_exit_1"}"#),
                )],
                4.0,
            )?;
            u.close_turn(&TurnClose {
                session: "T:C:1".into(),
                status: "blocked".into(),
                reply_key: String::new(),
                turn: 1,
                waiting: 0,
                quiet: 0,
                hash: String::new(),
                summary: String::new(),
                now: 4.0,
            })?;
            u.finish_turn(failed, None, "respond: addressed")?;
            let retried = u.retry_failed_turn("T:C:1")?;
            let again = u.retry_failed_turn("T:C:1")?;
            let attempts = u.inbox_attempts(failed)?;
            let status = u.thread("T:C:1")?.unwrap().status;
            let claimed = u.claim_next("T:C:1", 5.0)?.map(|item| item.id);
            Ok((
                nothing,
                Some(failed) == retried,
                again,
                attempts,
                status,
                claimed == Some(failed),
            ))
        })
        .await
        .unwrap();
    assert_eq!(nothing, None);
    assert!(retried && claimed);
    assert_eq!(again, None);
    assert_eq!(attempts, (0, "pending".into()));
    assert_eq!(status, "complete");
}
