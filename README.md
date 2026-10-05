# fridica-core

Fridica's domain, without I/O: the types and pure logic that do not depend
on Slack, SQLite or worker processes.

It is the domain crate of [Fridica](https://github.com/chengcli/fridica), a
Slack agent that delegates work to Claude Code and Codex workers on your
machines; the `fridica` crate re-exports it as `fridica::core`.

fridica-core: general mechanism only; no names, enums, defaults or prompt text specific to one host.

- **No I/O.** Only serde, regex, sha2, uuid and unicode-casefold; `unsafe` is
  forbidden. Storage, Slack, worker processes and config loading belong to the
  host, which calls into these types and implements the adapter traits below.
- **Decisions are validated before they act.** `delegation::prepare` checks a
  parent decision against its `Scope` (whether the channel may delegate, the
  `Limits`, the machine `Registry`) and places every delegation, or names what
  to repair.
- **Placement is sticky and load-aware.** `placement` keeps a thread on its
  machine and workspace, reads the fixed load probe and assesses it.
- **Delivery outcomes are facts.** A post is `Sent`, `RateLimited`, `Rejected`
  or `Ambiguous`; `DeliveryState::after_restart` turns an interrupted `Sending`
  into `Ambiguous` instead of guessing.
- **Publication is gated.** `egress::scan` names every rule a text breaks
  (`deny_list:<line>`, `ai_trailer`) and never the matched text.
- **Attention is explicit.** `policy::legacy_gate` is the frozen v0.3 gate;
  `policy::attention_gate` is the v0.4 gate.

## Modules

- `ids`, `time`: thread IDs (`workspace:channel:root_ts`), clocks and ID sources.
- `parent`: the parent's request and decision (including hand-offs to linked
  threads of the channel), its action schema and context trimming, and the
  `Parent` adapter trait.
- `delegation`: validating a decision and placing its delegations, within a
  `Scope` (whether the channel may delegate, `Limits`, the machine `Registry`).
- `fork`: the worker's starting context as a fork of the parent's (with the
  state of the thread's linked threads): snapshot, delta and rendering, bounded
  by `limits.worker_context_chars`.
- `placement`: sticky, load-aware machine and workspace selection, and the
  fixed load probe's parser and assessment.
- `worker`, `result`, `approvals`: worker records, jobs, results and their
  format (with the host's optional `annotations` sub-schema), failures (including `RateLimited`, temporary, with a reset time),
  approval requests and automatic command rules.
- `delivery`: outbox posts and delivery outcomes, and the `Delivery` trait.
- `egress`: the deny list and the scan of text about to be published.
- `policy`, `render`, `failure`: attention gates, reply rendering, blocked
  turns, and the wait after a rate-limited parent call.
- `config`: the parent, limits, placement and attention settings, and the
  machine registry. Loading them from TOML belongs to the host.

## What you supply

| Trait | Purpose |
|---|---|
| `parent::Parent` | Decide a `ParentRequest` |
| `delivery::Delivery` | Send a `ClaimedPost` and report its `DeliveryOutcome` |
| `time::Clock` | The current time (`SystemClock` and `ReplayClock` are provided) |
| `time::Identifiers` | New IDs per namespace (`RandomIds` and `SequenceIds` are provided) |

## Storage backends

`store` is the storage contract: everything Fridica keeps, independent of how
([fridica#117](https://github.com/chengcli/fridica/issues/117)).
[fridica-store-sqlite](https://github.com/chengcli/fridica-store-sqlite) is the
first backend. Another one implements:

- **`store::Store::run`**: run a type-erased unit of work (`Work`) and return its
  result. Hosts call it through `Store::transact` (or `store::transact` on a
  `dyn Store`).
- **Every area trait for its unit.** `store::Unit` is the sum of the areas
  (`Ledger`, `Health`, `Views`, `Inbox`, `Turns`, `Outbox`, `Jobs`, ...); a
  unit of work receives `&mut dyn Unit`.

What a unit of work guarantees:

- **All or nothing.** Everything a unit wrote commits when its closure returns
  `Ok`; when it returns `Err`, none of it does, and the error comes
  back to the caller as raised.
- **One at a time.** Units are serialised, or run with equivalent isolation: a
  unit never sees another's partial writes, and a later unit sees every
  earlier committed one.

Data rules:

- **JSON is text, kept byte for byte.** Payloads, details and every `_json`
  field of a view record are stored and returned exactly as given (the replay tapes
  compare them), never re-serialised; key order and spacing survive.
- **Order is part of the contract.** The ledger's sequence numbers increase;
  each method's doc says how its results are ordered (oldest first, newest
  first, in the order given).
- **Errors are `anyhow::Error`**, with the backend's own error inside. An
  unknown record is `Ok(None)` or an error as each method documents.

A host also needs, outside the unit of work: opening the store (and locking it
against a second daemon), migrating its schema, and the weekly archives. These
are each backend's own API, not traits yet.

### Conformance suite

The `conformance` feature adds `store::conformance`: async checks that open a
fresh, empty store and assert the contract through the traits alone (the unit
of work, ledger order and payloads, the health dedupe rules, and each area's
writes and reads). Run them from the backend's tests:

```toml
[dev-dependencies]
fridica-core = { version = "0.2", features = ["conformance"] }
```

```rust,ignore
// tests/conformance.rs
use fridica_core::store::conformance::Backend;

struct MyBackend;
impl Backend for MyBackend {
    type Guard = tempfile::TempDir; // what must outlive the store, or ()
    type Store = my_backend::Store;
    async fn fresh() -> (Self::Guard, Self::Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = my_backend::Store::open(dir.path().join("state")).await.unwrap();
        (dir, store)
    }
}

// One #[test] per check, each on a tokio runtime of its own.
fridica_core::conformance_tests!(MyBackend);
```

The feature only adds tokio (`rt`); a build without it has no new dependencies.

The traits are **unstable within 0.x**: they grow (and may change) as
Fridica's queries move behind them, and the suite grows with them.

## Example

```rust,no_run
use fridica_core::egress::{scan, DenyList};

let deny = DenyList::parse("# private names\nAlice Example\nproject-\\d+\n")?;
assert_eq!(scan("see PROJECT-42", &deny), ["deny_list:3"]);
assert!(scan("ordinary text", &deny).is_empty());
# Ok::<(), anyhow::Error>(())
```

## License

MIT
