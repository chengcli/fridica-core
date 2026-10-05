//! Core stands alone: delegation, placement and results over plain values.
use fridica_core::{
    config::{
        registry::{Machine, Policy, Registry, Resources, Workspace},
        Limits,
    },
    delegation::{prepare, Scope, Work},
    fork::ContextMode,
    ids::ThreadId,
    parent::{Decision, ParentRequest},
    result,
    time::SequenceIds,
};
use serde_json::json;

fn registry() -> Registry {
    Registry {
        machines: vec![Machine {
            name: "local".into(),
            transport: "local".into(),
            workspaces: vec![Workspace {
                name: "project".into(),
                path: "/work".into(),
                policy: Policy::default(),
                subfolders: false,
            }],
            backends: vec!["codex".into()],
            default_backend: "codex".into(),
            policy: Policy::default(),
            host: String::new(),
            tags: vec!["cpu".into()],
            resources: Resources::default(),
            max_workers: 2,
            max_jobs: 2,
            slurm: None,
            description: String::new(),
        }],
        default: "local".into(),
    }
}
fn request() -> ParentRequest {
    serde_json::from_value(
        json!({"inbox_id":7,"call":"decide","session":{"id":"T:C:1.1","channel":"C",
        "work":{"workers":[],"busy":{}},"context":{}},"trigger":{},"history":[],"obligations":[],
        "previous":null,"errors":[]}),
    )
    .unwrap()
}

#[test]
fn delegation_places_new_workers_only_where_allowed() {
    let decision: Decision =
        serde_json::from_value(json!({"delegations":[{"brief":"Run the checks","tags":["cpu"]}]}))
            .unwrap();
    let (limits, machines) = (Limits::default(), registry());
    let scope = |allowed| {
        Some(Scope {
            allowed,
            limits: &limits,
            machines: &machines,
            roles: &[],
        })
    };
    let ids = SequenceIds::default();
    let work = prepare(&decision, &request(), scope(true), Some(&ids)).unwrap();
    assert_eq!((work.workers.len(), work.jobs.len()), (1, 1));
    assert_eq!(
        (
            work.workers[0].machine.as_str(),
            work.workers[0].workspace.as_str()
        ),
        ("local", "project")
    );
    assert_eq!(work.context["machine"], "local");
    // Validation alone allocates nothing.
    assert!(prepare(&decision, &request(), scope(true), None)
        .unwrap()
        .jobs
        .is_empty());
    let refused = prepare(&decision, &request(), scope(false), None)
        .err()
        .unwrap();
    assert!(refused.to_string().contains("disabled"), "{refused}");
    assert!(prepare(&decision, &request(), None, None).is_err());
    let unknown: Decision =
        serde_json::from_value(json!({"delegations":[{"brief":"x","machine":"gpu9"}]})).unwrap();
    assert!(prepare(&unknown, &request(), scope(true), None).is_err());
}

#[test]
fn thread_ids_and_worker_results_round_trip() {
    let id: ThreadId = "T1:C2:100.1".parse().unwrap();
    assert_eq!(
        (id.channel.0.as_str(), id.to_string().as_str()),
        ("C2", "T1:C2:100.1")
    );
    assert!("T1:C2".parse::<ThreadId>().is_err());
    let parsed = result::parse(
        "```json\n{\"status\":\"done\",\"summary\":\"ok\",\"report\":\"All checks passed.\"}\n```",
    )
    .unwrap();
    assert_eq!(parsed.status, "done");
    assert_eq!(result::fallback("free text").status, "partial");
}

#[test]
fn delegations_fork_the_turn_by_default_and_fresh_carries_no_snapshot() {
    let (limits, machines) = (Limits::default(), registry());
    let scope = Some(Scope {
        allowed: true,
        limits: &limits,
        machines: &machines,
        roles: &[],
    });
    let decision: Decision = serde_json::from_value(json!({"summary":"Plume fit","delegations":[
        {"brief":"Run the checks","tags":["cpu"]},
        {"brief":"Review the diff","tags":["cpu"],"ephemeral":true,"context":"fresh"}
    ]}))
    .unwrap();
    let ids = SequenceIds::default();
    let work = prepare(&decision, &request(), scope, Some(&ids)).unwrap();
    assert_eq!(work.jobs.len(), 2);
    assert_eq!(work.jobs[0].context, ContextMode::Fork);
    let snapshot = work.jobs[0]
        .snapshot
        .as_ref()
        .expect("a fork carries the snapshot");
    assert_eq!(snapshot.at.inbox_id, 7);
    assert_eq!(snapshot.summary, "Plume fit");
    assert_eq!(work.jobs[1].context, ContextMode::Fresh);
    assert!(work.jobs[1].snapshot.is_none());
    // Validation alone computes no snapshot and allocates nothing.
    assert!(prepare(&decision, &request(), scope, None)
        .unwrap()
        .jobs
        .is_empty());
    assert!(serde_json::from_value::<Decision>(
        json!({"delegations":[{"brief":"x","context":"bogus"}]})
    )
    .is_err());
}

#[test]
fn a_fork_worker_delegation_needs_a_live_source_session_on_the_same_placement() {
    let (limits, machines) = (Limits::default(), registry());
    let scope = Some(Scope {
        allowed: true,
        limits: &limits,
        machines: &machines,
        roles: &[],
    });
    let mut request = request();
    request.session["work"]["workers"] = json!([
        {"id":"w1","session_id":"T:C:1.1","machine":"local","workspace":"project","backend":"codex","backend_session_id":"codex-thread-1"},
        {"id":"w2","ephemeral":true,"session_id":"T:C:1.1","machine":"local","workspace":"project","backend":"codex"},
        {"id":"w3","session_id":"T:C:1.1","machine":"local","workspace":"project","backend":"codex","backend_session_id":"gone","status":"stopped"},
        {"id":"w4","ephemeral":true,"session_id":"T:C:1.1","machine":"gpu","workspace":"project","backend":"codex","backend_session_id":"codex-thread-4"},
        {"id":"w5","ephemeral":true,"session_id":"T:C:2.1","machine":"local","workspace":"project","backend":"codex","backend_session_id":"codex-thread-5"},
    ]);
    let decide = |delegation: serde_json::Value| -> anyhow::Result<Work> {
        let decision: Decision =
            serde_json::from_value(json!({"summary":"Plume fit","delegations":[delegation]}))
                .unwrap();
        prepare(&decision, &request, scope, Some(&SequenceIds::default()))
    };
    let work = decide(
        json!({"brief":"Run the second experiment","tags":["cpu"],"context":"fork_worker","fork_worker_id":"w1"}),
    )
    .unwrap();
    // A new worker on the source's placement; the job names its source and
    // keeps the thread snapshot as its fallback.
    assert_eq!(work.workers.len(), 1);
    assert_eq!(work.jobs[0].context, ContextMode::ForkWorker);
    assert_eq!(work.jobs[0].fork_from_worker, "w1");
    assert!(work.jobs[0].snapshot.is_some());
    assert_eq!(work.jobs[0].context.as_str(), "fork_worker");
    // Thread forks and fresh jobs never carry a source.
    let work = decide(json!({"brief":"Review","tags":["cpu"],"context":"fresh"})).unwrap();
    assert_eq!(work.jobs[0].fork_from_worker, "");
    for (delegation, message) in [
        (
            json!({"brief":"x","tags":["cpu"],"context":"fork_worker"}),
            "needs fork_worker_id",
        ),
        (
            json!({"brief":"x","tags":["cpu"],"context":"fork_worker","fork_worker_id":"w2"}),
            "no backend session",
        ),
        (
            json!({"brief":"x","tags":["cpu"],"context":"fork_worker","fork_worker_id":"w3"}),
            "not a live worker",
        ),
        (
            json!({"brief":"x","tags":["cpu"],"context":"fork_worker","fork_worker_id":"w5"}),
            "not a live worker",
        ),
        (
            json!({"brief":"x","tags":["cpu"],"context":"fork_worker","fork_worker_id":"w4"}),
            "same machine and backend",
        ),
        (
            json!({"brief":"x","worker_id":"w2","context":"fork_worker","fork_worker_id":"w1"}),
            "leave worker_id empty",
        ),
        (
            json!({"brief":"x","tags":["cpu"],"fork_worker_id":"w1"}),
            "needs context: fork_worker",
        ),
    ] {
        let error = match decide(delegation) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("accepted a delegation that should repair: {message}"),
        };
        assert!(
            error.contains(message) && error.contains("context: fork"),
            "{error}"
        );
    }
}

fn linked_request() -> ParentRequest {
    serde_json::from_value(json!({
        "inbox_id": 3, "call": "decide",
        "session": {"id":"T:C:200.1","status":"working","turns":1,"context":{},"summary":"","decisions":[],
            "linked_threads":[{"thread":"100.1","status":"working","summary":"Review of #269 at a655d9c",
                "decisions":["needs a clean ctest"],"open_asks":[],"latest_messages":[],
                "jobs":[{"id":"j1","status":"running","summary":"","progress":"ctest 40%","brief":"x"}]}]},
        "trigger": {"kind":"message","message":{"ts":"200.1","text":"status?"}},
        "history": [], "obligations": [], "previous": null, "errors": []
    }))
    .unwrap()
}

/// Hand-offs (#108) target only linked threads; without any, none are allowed.
#[test]
fn hand_offs_target_only_linked_threads() {
    use fridica_core::parent::{schema, HandoffKind};
    let request = linked_request();
    let choices = schema::Choices::from_session(&request.session);
    assert_eq!(choices.threads, ["100.1"]);
    let s = schema::decision(&choices);
    assert_eq!(
        s["properties"]["handoffs"]["items"]["properties"]["thread"]["enum"],
        json!(["100.1"])
    );
    assert_eq!(s["properties"]["handoffs"]["maxItems"], 3);
    assert!(s["required"]
        .as_array()
        .unwrap()
        .contains(&json!("handoffs")));
    let none = schema::decision(&schema::Choices::default());
    assert_eq!(none["properties"]["handoffs"]["maxItems"], 0);
    let d: Decision = serde_json::from_value(json!({"handoffs":[{"thread":"100.1","kind":"post","note":"Post the sign-off there.","answers":["o1"]}]})).unwrap();
    assert_eq!(
        (d.handoffs[0].kind, d.handoffs[0].answers.len()),
        (HandoffKind::Post, 1)
    );
    assert!(serde_json::from_value::<Decision>(json!({}))
        .unwrap()
        .handoffs
        .is_empty());
}

/// A worker forked from a check-up thread inherits the linked threads' state.
#[test]
fn a_fork_carries_linked_threads_and_its_delta_only_what_changed() {
    use fridica_core::fork::{delta, render, render_delta, snapshot};
    let request = linked_request();
    let bundle = snapshot(&request, &Decision::default(), 12000);
    assert_eq!(bundle.linked.len(), 1);
    assert_eq!(bundle.linked[0]["jobs"][0]["progress"], "ctest 40%");
    assert!(bundle.linked[0].get("open_asks").is_none());
    assert!(render(&bundle).contains("Linked threads of this channel"));
    assert!(delta(&bundle, &bundle).linked.is_empty());
    let mut moved = linked_request();
    moved.session["linked_threads"][0]["status"] = json!("complete");
    let update = delta(&bundle, &snapshot(&moved, &Decision::default(), 12000));
    assert_eq!(update.linked.len(), 1);
    assert!(render_delta("j0", &update, "").contains("Linked threads of this channel now"));
}

/// A usage limit is its own, temporary failure kind (#107).
#[test]
fn rate_limits_are_a_failure_kind_with_a_reset_time_and_progress_has_limits() {
    use fridica_core::{
        failure,
        parent::ParentFailure,
        worker::{retry_same_session, Failure},
    };
    let kind: Failure =
        serde_json::from_value(json!({"rate_limited":{"retry_at":1790973000}})).unwrap();
    assert_eq!(
        kind,
        Failure::RateLimited {
            retry_at: Some(1790973000)
        }
    );
    assert_eq!(
        serde_json::to_value(Failure::Execution).unwrap(),
        json!("execution")
    );
    assert!(!retry_same_session(kind, 0, "s"));
    assert!(failure::rate_limited(&ParentFailure {
        code: failure::RATE_LIMITED.into()
    }));
    let limits = Limits::default();
    assert_eq!(
        (limits.progress_interval, limits.progress_chars),
        (120., 1500)
    );
}

/// Linked threads count against the bundle's budget: history yields to them.
#[test]
fn linked_threads_count_against_the_fork_budget() {
    use fridica_core::fork::{render, snapshot};
    let mut request = linked_request();
    request.history = (0..40)
        .map(|n| json!({"ts":format!("{}.1",150+n),"sender":"U","text":"x".repeat(200)}))
        .collect();
    let with = snapshot(&request, &Decision::default(), 4000);
    let mut without = request.clone();
    without.session["linked_threads"] = json!([]);
    let plain = snapshot(&without, &Decision::default(), 4000);
    assert!(with.history.len() < plain.history.len());
    assert!(render(&with).chars().count() <= 4000 + 2000);
}

/// Roles come from the host's scope (fridica#126): `general` always, others
/// only when the scope lists them.
#[test]
fn delegation_roles_are_checked_against_the_scope() {
    use fridica_core::parent::schema;
    let (limits, machines) = (Limits::default(), registry());
    let roles = ["referee".to_string()];
    let decide = |role: &str, roles: &[String]| {
        let decision: Decision = serde_json::from_value(
            json!({"delegations":[{"brief":"Referee the draft","tags":["cpu"],"role":role}]}),
        )
        .unwrap();
        let scope = Scope {
            allowed: true,
            limits: &limits,
            machines: &machines,
            roles,
        };
        prepare(
            &decision,
            &request(),
            Some(scope),
            Some(&SequenceIds::default()),
        )
    };
    assert_eq!(
        decide("referee", &roles).unwrap().workers[0].role,
        "referee"
    );
    assert_eq!(decide("", &roles).unwrap().workers[0].role, "general");
    assert_eq!(decide("general", &[]).unwrap().workers[0].role, "general");
    for (role, roles) in [("implementer", &roles[..]), ("referee", &[])] {
        let error = decide(role, roles).err().unwrap();
        assert!(error.to_string().contains("invalid worker role"), "{error}");
    }
    let choices = schema::Choices {
        roles: vec!["referee".into(), "general".into()],
        ..Default::default()
    };
    let s = schema::decision(&choices);
    let role = &s["properties"]["delegations"]["items"]["properties"]["role"]["enum"];
    assert_eq!(role, &json!(["", "general", "referee"]));
    let none = schema::decision(&schema::Choices::default());
    let role = &none["properties"]["delegations"]["items"]["properties"]["role"]["enum"];
    assert_eq!(role, &json!(["", "general"]));
}

/// `annotations` is optional and host-defined: absent from older workers,
/// round-tripped as given, never a field core reads.
#[test]
fn worker_results_round_trip_with_and_without_annotations() {
    use fridica_core::worker::WorkerResult;
    let plain: WorkerResult =
        serde_json::from_value(json!({"status":"done","summary":"ok"})).unwrap();
    assert_eq!(plain.annotations, None);
    let value = serde_json::to_value(&plain).unwrap();
    assert!(value.get("annotations").is_none());
    assert_eq!(
        serde_json::from_value::<WorkerResult>(value).unwrap(),
        plain
    );

    let fields = json!({"confidence":0.8,"labels":["a","b"],"nested":{"k":[1,null,true]}});
    let with: WorkerResult =
        serde_json::from_value(json!({"status":"done","summary":"ok","annotations":fields}))
            .unwrap();
    assert_eq!(
        with.annotations.clone().map(Into::into),
        Some(fields.clone())
    );
    let value = serde_json::to_value(&with).unwrap();
    assert_eq!(value["annotations"], fields);
    assert_eq!(serde_json::from_value::<WorkerResult>(value).unwrap(), with);
    for bad in [json!("text"), json!([1]), json!(3)] {
        assert!(serde_json::from_value::<WorkerResult>(
            json!({"status":"done","summary":"ok","annotations":bad})
        )
        .is_err());
    }
}

/// The tolerant parser keeps valid annotations and drops invalid ones (not an
/// object, or over a cap) with the rest of the result kept.
#[test]
fn the_tolerant_parser_drops_invalid_annotations_and_keeps_the_rest() {
    let parse = |annotations: serde_json::Value| {
        let text = json!({"status":"done","summary":"ok","report":"posted",
            "unresolved":["one"],"annotations":annotations});
        result::parse(&format!("```json\n{text}\n```")).unwrap()
    };
    let keys =
        |n: usize| serde_json::Value::Object((0..n).map(|i| (format!("k{i}"), json!(i))).collect());
    let kept = parse(json!({"score":2,"note":"fine"}));
    assert_eq!(
        kept.annotations.map(Into::into),
        Some(json!({"score":2,"note":"fine"}))
    );
    assert!(parse(keys(result::ANNOTATIONS_KEYS)).annotations.is_some());
    let big = "x".repeat(result::ANNOTATIONS_BYTES);
    for bad in [
        json!("text"),
        json!(["a"]),
        json!(null),
        keys(result::ANNOTATIONS_KEYS + 1),
        json!({"long": big}),
    ] {
        let parsed = parse(bad.clone());
        assert_eq!(parsed.annotations, None, "{bad}");
        assert_eq!(
            (parsed.summary.as_str(), parsed.report.as_str()),
            ("ok", "posted")
        );
        assert_eq!(parsed.unresolved, vec!["one".to_string()]);
    }
}

/// What a backend enforcing structured output in strict mode (Codex) accepts:
/// every object lists all its properties as required and forbids others.
fn assert_strict(schema: &serde_json::Value, at: &str) {
    if let Some(properties) = schema["properties"].as_object() {
        assert_eq!(schema["additionalProperties"], json!(false), "{at}");
        let mut required: Vec<&str> = schema["required"]
            .as_array()
            .unwrap_or_else(|| panic!("{at}: no required"))
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        let mut keys: Vec<&str> = properties.keys().map(String::as_str).collect();
        required.sort();
        keys.sort();
        assert_eq!(required, keys, "{at}");
        for (key, value) in properties {
            assert_strict(value, &format!("{at}.{key}"));
        }
    }
    if let Some(items) = schema.get("items") {
        assert_strict(items, &format!("{at}[]"));
    }
}

/// Without a host sub-schema the worker schema and format note are 0.4's and
/// ask for no `annotations`; with one, `annotations` is a required, nullable
/// object of that shape. Both forms are valid in strict mode.
#[test]
fn an_injected_annotations_schema_appears_in_the_worker_schema() {
    let bare = result::schema();
    assert_eq!(result::schema_with(None), bare);
    assert!(bare["properties"].get("annotations").is_none());
    assert_strict(&bare, "schema");
    assert_eq!(result::format_note(None), result::FORMAT_NOTE);
    let line = |note: &str| -> serde_json::Value {
        serde_json::from_str(note.lines().nth(2).unwrap()).unwrap()
    };
    assert_eq!(line(result::FORMAT_NOTE), bare);

    let sub = json!({"properties":{"score":{"type":"integer"},"verdict":{"type":"string","enum":["pass","return"]}},"required":["score","verdict"]});
    let schema = result::schema_with(Some(&sub));
    assert_strict(&schema, "schema_with");
    let annotations = &schema["properties"]["annotations"];
    assert_eq!(annotations["type"], json!(["object", "null"]));
    assert_eq!(annotations["additionalProperties"], json!(false));
    assert_eq!(annotations["properties"], sub["properties"]);
    let mut rest = schema.clone();
    rest["properties"]
        .as_object_mut()
        .unwrap()
        .remove("annotations");
    rest["required"]
        .as_array_mut()
        .unwrap()
        .retain(|k| k != "annotations");
    assert_eq!(rest, bare);
    // An empty sub-schema is still a strict, nullable object.
    assert_strict(&result::schema_with(Some(&json!({}))), "empty");
    let note = result::format_note(Some(&sub));
    assert_eq!(line(&note), schema);
    assert!(note.starts_with(
        &result::FORMAT_NOTE
            .lines()
            .take(2)
            .collect::<Vec<_>>()
            .join("\n")
    ));
    assert!(note.ends_with("or null when none apply."));
}
