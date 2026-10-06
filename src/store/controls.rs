//! Thread controls (pause, resume, close, restore, clean and owner
//! instructions), the worker stops they queue, and the threads a thread is
//! linked to.
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// A thread's control as stored.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ControlState {
    /// `active`, `paused`, `closed`, `archived` or `cleaned`.
    pub control: String,
    /// JSON text: the control's details.
    pub details: String,
}

/// Where a resumed thread picks up again.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResumePoint {
    /// The latest message from someone else since Fridica last posted in the
    /// thread: its Slack event ID and timestamp.
    pub latest: Option<(String, f64)>,
    /// The timestamp of the thread's newest message, or 0.
    pub newest: f64,
}

/// What owner controls do to a thread. The caller decides whether a control
/// is allowed; these record its effects.
pub trait ThreadControls {
    /// The thread's control. An unknown thread is an error.
    fn control_state(&mut self, session: &str) -> Result<ControlState>;
    /// The thread's workspace and channel. An unknown thread is an error.
    fn thread_channel(&mut self, session: &str) -> Result<(String, String)>;
    /// Whether any of the thread's workers is not stopped.
    fn has_live_workers(&mut self, session: &str) -> Result<bool>;
    /// Set the thread's control (`details` is JSON text) and pause reason,
    /// and clear its wait and no-progress streaks when `reset_streaks`.
    fn set_control(
        &mut self,
        session: &str,
        control: &str,
        details: &str,
        reason: &str,
        now: f64,
        reset_streaks: bool,
    ) -> Result<()>;
    /// On resume: end a blocked or working turn and restart the turn count.
    fn restart_turns(&mut self, session: &str) -> Result<()>;
    /// On an owner instruction: a blocked thread is complete again.
    fn unblock(&mut self, session: &str) -> Result<()>;
    /// On an owner retry: when the thread's last decision call failed because
    /// the parent was unavailable or still invalid after repair, put that
    /// call's finished inbox item back to pending (attempts reset, due now)
    /// and make a blocked thread complete again, so the item gets a new
    /// parent turn (fridica#136). Returns the item, or `None` when the last
    /// decision did not fail that way or its item is not finished.
    fn retry_failed_turn(&mut self, session: &str) -> Result<Option<i64>>;
    /// Let the thread's held worker results and interruptions through now.
    fn release_worker_results(&mut self, session: &str) -> Result<()>;
    /// Clean a thread: wipe its messages, summary, decisions and owner
    /// instructions, drop its unfinished inbox, release its unposted reply
    /// reservations and close its open obligations.
    fn wipe(&mut self, session: &str, now: f64) -> Result<()>;
    /// The first owner instruction with this client ID: its inbox ID and text.
    fn owner_instruction(
        &mut self,
        session: &str,
        client_id: &str,
    ) -> Result<Option<(i64, String)>>;
    /// Queue an owner instruction (`payload` is JSON text) and return its
    /// inbox ID.
    fn queue_owner_instruction(
        &mut self,
        session: &str,
        client_id: &str,
        payload: &str,
        now: f64,
    ) -> Result<i64>;
    /// Where the thread picks up again on resume.
    fn resume_point(&mut self, session: &str) -> Result<ResumePoint>;
    /// Start the thread's next turn from messages after `boundary`.
    fn reset_thread_at(&mut self, session: &str, boundary: f64) -> Result<()>;
    /// Queue message `event` again with `payload` (JSON text), reusing its
    /// unfinished inbox entry, unless a job already holds it or holds an
    /// unreported result for it.
    fn resume_message(&mut self, session: &str, event: &str, payload: &str, now: f64)
        -> Result<()>;
    /// Who drives the thread's work: `parent` or `external`. An unknown
    /// thread is an error.
    fn thread_driver(&mut self, session: &str) -> Result<String>;
    /// Set who drives the thread's work (`parent` or `external`; anything
    /// else is an error) for `actor` (JSON text). A change bumps the thread's
    /// version, so a turn loaded under the old driver is stale, and is
    /// audited as `driver`. Returns whether it changed. An unknown thread is
    /// an error.
    fn set_thread_driver(
        &mut self,
        session: &str,
        driver: &str,
        actor: &str,
        now: f64,
    ) -> Result<bool>;
    /// Audit a control (`actor` and `details` are JSON text).
    fn audit_control(
        &mut self,
        now: f64,
        actor: &str,
        action: &str,
        session: &str,
        details: &str,
    ) -> Result<()>;
}

/// A pending worker stop: its ledger sequence number and the worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerStop {
    pub seq: i64,
    pub worker: String,
}

/// Worker stops: durable intents, kept in the replay ledger, to stop the
/// workers of closed threads. Each completes once its worker is stopped.
pub trait WorkerStops {
    /// Closed, archived or cleaned threads with a worker not yet stopped.
    fn closed_threads_with_live_workers(&mut self) -> Result<Vec<String>>;
    /// Queue a stop for every worker of `sessions` that is not stopped (or
    /// for every worker, when `include_stopped`) unless one is pending.
    fn queue_worker_stops(
        &mut self,
        sessions: &[String],
        now: f64,
        include_stopped: bool,
    ) -> Result<()>;
    /// Whether a stop is pending for one of the thread's workers.
    fn worker_stop_pending(&mut self, session: &str) -> Result<bool>;
    /// Every pending stop, oldest first.
    fn pending_worker_stops(&mut self) -> Result<Vec<WorkerStop>>;
}

/// An open ask on a linked thread.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpenAsk {
    pub kind: String,
    pub summary: String,
    pub due: f64,
}

/// A job on a linked thread.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinkedJob {
    pub id: String,
    pub status: String,
    pub role: String,
    pub machine: String,
    pub brief: String,
    /// The result's summary, or empty.
    pub summary: String,
    pub error: String,
    /// The latest progress note, or empty.
    pub progress: String,
}

/// A message on a linked thread.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinkedMessage {
    pub ts: String,
    pub sender: String,
    pub text: String,
    pub from_agent: bool,
}

/// A thread linked to another, and its state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinkedThread {
    pub id: String,
    /// The items both threads mention, comma-separated (an item with a repo
    /// is prefixed by it).
    pub shared_items: String,
    /// Whether the thread asked about refers to this one.
    pub referenced: bool,
    /// Whether this one refers to the thread asked about.
    pub references_this: bool,
    pub root_ts: String,
    pub status: String,
    pub control: String,
    pub summary: String,
    /// JSON text: the thread's decisions.
    pub decisions: String,
    /// The root message's text, or empty.
    pub root: String,
    /// Up to 3 open or deferred asks, oldest first.
    pub asks: Vec<OpenAsk>,
    /// Up to 3 jobs, most recently queued first.
    pub jobs: Vec<LinkedJob>,
    /// The last 2 messages, oldest first.
    pub messages: Vec<LinkedMessage>,
}

/// The threads linked to a thread through the channel ledger.
pub trait LinkedThreads {
    /// Up to `limit` open threads that `session` refers to, that refer to it,
    /// or that mention an item it mentions within `window` seconds of it;
    /// most recently updated first.
    fn linked_threads(
        &mut self,
        session: &str,
        limit: usize,
        window: f64,
    ) -> Result<Vec<LinkedThread>>;
}
