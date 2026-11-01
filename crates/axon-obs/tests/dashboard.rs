#![cfg(feature = "dashboard")]

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axon::{Actor, ActorSystem, Context, TraceContext};
use axon_cluster::{AccessPolicy, Cluster, MemoryDiscovery, MemoryLeases, Placement};
use axon_obs::*;
use axon_stream::{DeliveryPolicy, StreamKey, StreamNode, StreamNodeConfig};
use axon_testkit::eventually;
use serde_json::Value as Json;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

mod common;

struct Idle;

impl Actor for Idle {
    type Message = ();
    type Error = ();

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        Ok(())
    }
}

struct Fails;

impl Actor for Fails {
    type Message = ();
    type Error = ();

    fn on_message(&mut self, _: (), _: &mut Context<'_, Self>) -> Result<(), ()> {
        Err(())
    }
}

/// Fails after a nap, so what was told after it is still queued: dead letters of that generation.
struct Napper;

impl Actor for Napper {
    type Message = u64;
    type Error = ();

    fn on_message(&mut self, ms: u64, _: &mut Context<'_, Self>) -> Result<(), ()> {
        std::thread::sleep(Duration::from_millis(ms));
        Err(())
    }
}

async fn bare(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    common::loopback(id, discovery.clone(), ActorSystem::builder(), |c| c.max_control_handlers = 64).await
}

async fn node(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    let (system, cluster) = bare(id, discovery).await;
    let telemetry = TelemetryConfig {
        // Sampled every 50 ms, so the console fills within the test.
        sample_interval: Duration::from_millis(50),
        access: AccessPolicy::Nodes(["a".to_string()].into()),
        ..Default::default()
    };
    let telemetry = Telemetry::start(&system, &cluster, telemetry).await.unwrap();
    if id == "b" {
        let streams = StreamNode::start(&cluster, StreamNodeConfig::new(vec![])).await.unwrap();
        streams.open_stream(&StreamKey::new("prices", "eurusd"), 7, DeliveryPolicy::ReplayRequired).await.unwrap();
        telemetry.watch_streams(&streams);
    }
    (system, cluster)
}

/// At argon2's lowest cost. The sign-in limits refill in real time, an attempt every 20 s, and a verify at the default
/// cost took seconds on a loaded box, so the lockout ran out while the test was still hashing the attempts that fill
/// it. `bea` below keeps `hash_password`'s default cost.
fn admin() -> User {
    use argon2::password_hash::{PasswordHasher, SaltString};
    let cheapest = argon2::Params::new(8, 1, 1, None).unwrap();
    let argon2 = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, cheapest);
    let salt = SaltString::encode_b64(&[7; 16]).unwrap();
    let hash = argon2.hash_password(b"secret", &salt).unwrap();
    User { name: "admin".into(), hash: hash.to_string() }
}

fn set_cookie(response: &reqwest::Response) -> String {
    response.headers()["set-cookie"].to_str().unwrap().to_owned()
}

/// The `name=value` a browser sends back.
fn session_cookie(response: &reqwest::Response) -> String {
    set_cookie(response).split(';').next().unwrap().to_owned()
}

/// Signs in the way the login page does: the session cookie, or the refusal's status.
async fn sign_in(http: &reqwest::Client, base: &str, user: &str, password: &str) -> Result<String, u16> {
    let origin = base.split('/').take(3).collect::<Vec<_>>().join("/");
    let form = [("user", user), ("password", password)];
    let answer = http.post(format!("{base}login")).header("origin", origin).form(&form).send().await.unwrap();
    match answer.status().as_u16() {
        200 => Ok(session_cookie(&answer)),
        status => Err(status),
    }
}

fn events(overview: &[u8]) -> Vec<(String, String)> {
    let overview: Json = serde_json::from_slice(overview).unwrap();
    let events = overview["events"].as_array().unwrap().iter();
    events.map(|e| (e["kind"].as_str().unwrap().to_owned(), e["detail"].as_str().unwrap().to_owned())).collect()
}

/// The overview is rebuilt at most every other refresh, so an event shows within a few.
async fn logged(dashboard: &Dashboard, kind: &str, detail: &str) {
    let seen = async || events(&dashboard.overview().await).iter().any(|(k, d)| k == kind && d == detail);
    eventually(&format!("{kind} {detail}"), seen).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_dashboard_serves_the_cluster_over_http() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (b_system, _b)) = (node("a", &discovery).await, node("b", &discovery).await);
    eventually("membership converges", || async { a.view().members.len() == 2 }).await;
    let mut idle = Vec::new();
    for i in 0..30 {
        idle.push(b_system.spawn(Idle).name(format!("idle-{i:02}")).start().unwrap());
    }

    let open = DashboardConfig { bind: "0.0.0.0:0".parse().unwrap(), ..Default::default() };
    assert_eq!(
        Dashboard::start(&a_system, &a, open).await.err().unwrap(),
        "a non-loopback bind needs at least one user"
    );
    let bad = DashboardConfig { base_path: "/../x".into(), ..Default::default() };
    assert!(Dashboard::start(&a_system, &a, bad).await.is_err());
    let plain =
        DashboardConfig { users: vec![User { name: "admin".into(), hash: "secret".into() }], ..Default::default() };
    assert!(Dashboard::start(&a_system, &a, plain).await.is_err(), "a password is not a hash");
    let scrypt =
        User { name: "admin".into(), hash: hash_password("secret").unwrap().replacen("argon2id", "scrypt", 1) };
    let scrypt = DashboardConfig { users: vec![scrypt], ..Default::default() };
    assert!(Dashboard::start(&a_system, &a, scrypt).await.is_err(), "not an argon2 hash");
    // Past the 12 h maximum, which start refuses.
    let session = SessionConfig { idle: Duration::from_secs(13 * 3600), ..Default::default() };
    let longer = DashboardConfig { users: vec![admin()], session, ..Default::default() };
    assert!(Dashboard::start(&a_system, &a, longer).await.is_err(), "idle past max");

    let collect = MetricsConfig {
        // Collected every 100 ms, so the console fills within the test.
        collection_interval: Some(Duration::from_millis(100)),
        access: AccessPolicy::Nodes(["a".to_string()].into()),
        ..Default::default()
    };
    let store = Arc::new(MemoryMetrics::new(64 << 20));
    MetricsService::start(&a_system, &a, store.clone(), collect).await.unwrap();

    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        base_path: "/ops/axon".into(),
        users: vec![admin()],
        // Refreshed every 100 ms, so the console fills within the test.
        refresh_interval: Duration::from_millis(100),
        ..Default::default()
    };
    let dashboard = Dashboard::start(&a_system, &a, cfg).await.unwrap();
    let root = format!("http://{}", dashboard.address());
    let base = format!("{root}/ops/axon");
    let http = reqwest::Client::new();
    let cookie = sign_in(&http, &format!("{base}/"), "admin", "secret").await.unwrap();
    let get = |path: &str| http.get(format!("{base}/{path}")).header("cookie", &cookie).send();
    let body = |path: String| async move {
        let response = get(&path).await.unwrap();
        (response.status().as_u16(), response.json::<Json>().await.unwrap())
    };

    for path in ["api/overview", "api/events", "api/session", "api/connection?node_id=a&peer=b&direction=out"] {
        let refused = http.get(format!("{base}/{path}")).send().await.unwrap();
        assert_eq!(refused.status(), 401, "{path}");
        let answer = refused.json::<Json>().await.unwrap();
        assert_eq!(answer["available"], false, "{path}: a JSON answer the UI can read");
        assert_eq!(answer["error"], "sign in first", "{path}: no cookie at all");
    }
    assert_eq!(body("api/session".into()).await.1["user"], "admin");
    let wrong = http.get(format!("{base}/api/overview")).header("cookie", "axon_session=admin").send().await.unwrap();
    assert_eq!(wrong.status(), 401);
    assert_eq!(http.get(format!("{root}/api/overview")).header("cookie", &cookie).send().await.unwrap().status(), 404);

    // The page and its bundle hold no cluster data: the login page is one of its routes.
    for page in ["", "/", "/login", "/nodes/b"] {
        let index = http.get(format!("{base}{page}")).send().await.unwrap();
        assert_eq!(index.status(), 200, "{page}");
        assert_eq!(index.headers()["cache-control"], "no-store");
        assert_eq!(index.headers()["x-content-type-options"], "nosniff");
        assert_eq!(index.headers()["referrer-policy"], "no-referrer");
        assert!(index.headers().get("strict-transport-security").is_none(), "no HSTS over plain HTTP");
        assert!(index.headers()["content-security-policy"].to_str().unwrap().contains("frame-ancestors 'none'"));
        assert!(index.text().await.unwrap().contains("<base href=\"/ops/axon/\" />"), "{page}");
    }
    for missing in ["api/nothing", "assets/none.js", "favicon.ico"] {
        assert_eq!(get(missing).await.unwrap().status(), 404, "{missing}");
    }

    eventually("both nodes are observed and history is found", || async {
        let (_, overview) = body("api/overview".into()).await;
        let nodes = overview["nodes"].as_array().unwrap();
        nodes.len() == 2
            && nodes.iter().all(|n| n["snapshot"]["node_id"] == n["node"]["id"] && n["error"].is_null())
            && overview["history"]["available"] == true
            && overview["history"]["node"] == "a"
            && overview["history"]["sources"][0]["storage"] == "memory"
    })
    .await;
    // The console reads these by name, and each array by the reason names it pairs with it.
    let peers = |overview: &Json| {
        let nodes = overview["nodes"].as_array().unwrap().iter();
        nodes.flat_map(|n| n["snapshot"]["cluster"]["engine"]["peers"].as_array().unwrap().clone()).collect::<Vec<_>>()
    };
    eventually("each node's sessions show with their wire version", || async {
        let overview = body("api/overview".into()).await.1;
        let mut engines = overview["nodes"].as_array().unwrap().iter().map(|n| &n["snapshot"]["cluster"]["engine"]);
        engines.all(|e| e["peers"].as_array().is_some_and(|p| !p.is_empty()))
            && peers(&overview).iter().all(|p| p["version"].is_u64())
    })
    .await;
    let (_, overview) = body("api/overview".into()).await;
    let b = overview["nodes"].as_array().unwrap().iter().find(|n| n["node"]["id"] == "b").unwrap()["snapshot"].clone();
    let len = |v: &Json| v.as_array().unwrap().len();
    let engine = &b["cluster"]["engine"];
    assert_eq!(len(&engine["inbound_rejected"]), axon_cluster::REJECTION_NAMES.len());
    assert_eq!(len(&engine["listener"]["handshake_failures"]), axon_net::HANDSHAKE_FAILURE_REASONS.len());
    assert!(b["cluster"]["quotas"].is_null() && b["cluster"]["ownership"].is_null(), "neither runs here");
    let streams = &b["streams"];
    assert_eq!(len(&streams["hub"]["quota_refused"]), axon_stream::NAMESPACE_LIMITS.len());
    for field in ["retained_bytes", "interests_sent", "interests_failed", "interest_keys", "resyncs"] {
        assert!(streams[field].is_u64(), "{field}");
    }
    assert!(streams["disk"].is_null(), "no history on disk here");
    // The snapshots carry no single actors, and no session's timings, which api/connection serves.
    assert!(b["actors"].is_null() && b["actor_total"].is_u64() && b["members"].is_array(), "{b}");
    for peer in peers(&overview) {
        assert!(peer["session"]["timings"].is_null() && peer["request_rtt"].is_null(), "{peer}");
        assert!(peer["session"]["rx_frames"].is_u64(), "{peer}");
    }
    // A down lane has no session to time.
    let peer =
        engine["peers"].as_array().unwrap().iter().find(|p| p["down_ms"].is_null()).expect("a lane with a session");
    let direction = if peer["outgoing"] == true { "out" } else { "in" };
    let (to, lane) = (peer["node"].as_str().unwrap(), peer["lane"].as_str().unwrap());
    let path = format!("api/connection?node_id=b&peer={to}&direction={direction}&lane={lane}");
    let (status, timings) = body(path).await;
    assert_eq!(status, 200, "{timings}");
    assert!(timings["timings"]["rtt"]["count"].is_u64() && timings["request_rtt"]["count"].is_u64(), "{timings}");
    assert_eq!(body("api/connection?node_id=b&peer=zz&direction=out&lane=live".into()).await.0, 404);
    assert_eq!(body("api/connection?node_id=b&peer=a&lane=live".into()).await.0, 400);
    assert_eq!(body("api/connection?node_id=b&peer=a&direction=out&lane=fast".into()).await.0, 400);
    // Collected every 100 ms but probed every 5 s: the age counts up on the dashboard's clock between probes. A first
    // probe that beat the first collection carries no age, so this can take the next probe and 1.5 s more.
    let counts_up = async {
        loop {
            let (_, overview) = body("api/overview".into()).await;
            let ago = overview["history"]["sources"][0]["collector"]["collected_ago_ms"].as_u64();
            if ago.is_some_and(|ago| ago >= 1500) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(15), counts_up)
        .await
        .expect("the time since a collection counts up between probes");
    let (_, overview) = body("api/overview".into()).await;
    assert_eq!(overview["observer"], "a");
    assert_eq!(serde_json::from_slice::<Json>(&dashboard.overview().await).unwrap()["observer"], "a");
    let incarnation = overview["nodes"][1]["node"]["incarnation"].as_str().unwrap().to_owned();

    let mut stream = get("api/events").await.unwrap();
    assert_eq!(stream.headers()["content-type"], "text/event-stream");
    let mut seen = String::new();
    while seen.matches("\n\n").count() < 2 {
        seen.push_str(std::str::from_utf8(&stream.chunk().await.unwrap().unwrap()).unwrap());
    }
    let frame = seen.split("\n\n").next().unwrap().strip_prefix("event: overview\ndata: ").unwrap();
    assert_eq!(serde_json::from_str::<Json>(frame).unwrap()["nodes"].as_array().unwrap().len(), 2);
    // Every tab takes the overview each second: compressed when asked, and still flushed event by event.
    let request = http.get(format!("{base}/api/events")).header("cookie", &cookie).header("accept-encoding", "gzip");
    let mut zipped = request.send().await.unwrap();
    assert_eq!(zipped.headers()["content-encoding"], "gzip");
    let mut flushed = Vec::new();
    while !flushed.ends_with(&[0, 0, 0xff, 0xff]) {
        flushed.extend_from_slice(&zipped.chunk().await.unwrap().unwrap());
    }
    drop(zipped);
    // Brotli where the browser offers it, but not for a font, which is compressed already.
    let assets = std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/ui/dist/assets")).unwrap();
    let font = assets.map(|f| f.unwrap().file_name().into_string().unwrap()).find(|f| f.ends_with(".woff2")).unwrap();
    for (path, encoding) in [(format!("assets/{font}"), None), ("api/overview".into(), Some("br"))] {
        let request =
            http.get(format!("{base}/{path}")).header("cookie", &cookie).header("accept-encoding", "gzip, br");
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), 200, "{path}");
        assert_eq!(response.headers().get("content-encoding").map(|e| e.to_str().unwrap()), encoding, "{path}");
    }

    let (status, page) = body(format!("api/actors?node_id=b&incarnation={incarnation}&q=idle-0&limit=5")).await;
    assert_eq!((status, page["actors"].as_array().unwrap().len()), (200, 5), "{page}");
    let one = &page["actors"][2];
    let id = one["incarnation"].as_object().unwrap();
    let id = format!("{}:{}:{}", id["runtime"], id["slot"], id["generation"]);
    let (_, exact) = body(format!("api/actors?node_id=b&id={id}&timings=true")).await;
    assert_eq!((exact["actors"].as_array().unwrap().len(), &exact["actors"][0]["name"]), (1, &one["name"]));
    assert_eq!(body("api/actors?node_id=b&incarnation=00".into()).await.0, 409);
    assert_eq!(body("api/actors?node_id=zz".into()).await.0, 404);
    assert_eq!(body("api/actors?node_id=b&id=nope".into()).await.0, 400);

    let (_, first) = body("api/inventory?q=idle&sort=name&order=desc&limit=25".into()).await;
    assert_eq!((first["total"].as_u64(), first["truncated"].as_bool()), (Some(30), Some(false)), "{first}");
    assert_eq!(
        (&first["actors"][0]["name"], &first["actors"][0]["node_id"]),
        (&Json::from("idle-29"), &Json::from("b"))
    );
    assert_eq!(first["actors"][0]["node_incarnation"], incarnation);
    let (_, second) = body("api/inventory?q=idle&sort=name&order=desc&limit=25&offset=25".into()).await;
    // A rate is change over the time between gatherings, not between answers: a cached answer carries its gathering's time.
    assert!(first["timestamp_ms"].as_u64().is_some_and(|at| at > 0), "{first}");
    assert_eq!(first["timestamp_ms"], second["timestamp_ms"]);
    assert_eq!((second["actors"].as_array().unwrap().len(), &second["actors"][4]["name"]), (5, &Json::from("idle-00")));
    assert_eq!(body("api/inventory?node_id=a&q=idle".into()).await.1["total"], 0);
    assert_eq!(body("api/inventory?sort=secrets".into()).await.0, 400);
    // Two families, restarted in the opposite order to their spawns, then a younger actor that never restarts: Up
    // puts that actor first, Since restart the latest restart first and the earliest last, with the never-restarted
    // actors after both in either order.
    let spawned_first = b_system.supervise(|| Fails).name("idle-restarts-last").start_supervised().unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let spawned_second = b_system.supervise(|| Fails).name("idle-restarts-first").start_supervised().unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let _late = b_system.spawn(Idle).name("idle-late").start().unwrap();
    for family in [&spawned_second, &spawned_first] {
        family.tell(()).unwrap();
        eventually("the family restarts", || async { family.family.restarts() == 1 }).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let sorted = [
        ("up", "idle-late"),
        ("since_restart", "idle-restarts-last"),
        ("since_restart&order=desc", "idle-restarts-first"),
    ];
    for (sort, first) in sorted {
        let (_, page) = body(format!("api/inventory?q=idle-&sort={sort}&limit=1")).await;
        assert_eq!(page["actors"][0]["name"], first, "{sort}: {page}");
    }
    // A family's row counts the letters of its generations, as the node does.
    let napper = b_system.supervise(|| Napper).name("napper").start_supervised().unwrap();
    for ms in [50u64, 0, 0, 0] {
        napper.tell(ms).unwrap();
    }
    eventually("the inventory counts the family's letters", || async {
        let (_, page) = body("api/inventory?q=napper".into()).await;
        page["actors"][0]["diagnostics"]["dead_letters"] == 3
    })
    .await;
    let id = napper.lifecycle.incarnation();
    let (_, page) = body(format!("api/actors?node_id=b&id={}:{}:0", id.runtime, id.slot)).await;
    assert_eq!(page["actors"][0]["diagnostics"]["dead_letters"], 3, "{page}");

    let (_, streams) = body("api/streams?limit=5".into()).await;
    let listed = &streams["nodes"]["b"]["streams"][0];
    assert_eq!(
        (&listed["name"], &listed["generation"], &listed["origin"]),
        (&"eurusd".into(), &7.into(), &true.into())
    );
    assert_eq!((&streams["nodes"]["a"]["total"], &streams["nodes"]["b"]["total"]), (&0.into(), &1.into()), "{streams}");

    let root = axon::TraceContext::root();
    root.enter(|| idle[0].tell(()).unwrap());
    let id = format!("{:016x}", root.trace);
    let path = format!("api/trace?id={id}");
    // `tell` returns before the actor has handled the message.
    eventually("the span is held", || async { body(path.clone()).await.1["spans"][0]["kind"] == "handle" }).await;
    let (_, held) = body(path).await;
    let span = &held["spans"][0];
    assert_eq!(
        (&span["node_id"], &span["kind"], &span["parent"]),
        (&"b".into(), &"handle".into(), &"".into()),
        "{held}"
    );
    assert!(span["actor_type"].as_str().unwrap().ends_with("Idle"), "{held}");
    eventually("the trace is listed from the store", || async {
        let (_, list) = body("api/traces?q=idle&limit=5".into()).await;
        list["source"] == "store" && list["traces"][0]["trace"] == *id && list["traces"][0]["nodes"][0] == "b"
    })
    .await;
    assert_eq!(body("api/traces?failed=true".into()).await.1["traces"].as_array().unwrap().len(), 0);
    assert_eq!(body("api/traces?history=b".into()).await.1["source"], "live", "no store there: the nodes answer");
    assert_eq!(body("api/traces?limit=0".into()).await.0, 400);
    assert_eq!(body("api/trace?id=nope".into()).await.0, 400);
    assert_eq!(body("api/trace?id=00000000000000aa".into()).await.0, 404);

    let letter = axon::DeadLetter {
        seq: 0,
        at_us: 7,
        reason: "full",
        label: "tell",
        actor: None,
        target: Some("orders".into()),
        peer: Some("a".into()),
        trace: root.trace.get(),
        count: 3,
    };
    b_system.dead_letters().record(letter, None);
    let (_, dead) = body("api/dead-letters".into()).await;
    let letter = dead["letters"].as_array().unwrap().iter().find(|l| l["target"] == "orders").unwrap();
    assert_eq!(
        (&letter["node_id"], &letter["reason"], &letter["target"], &letter["peer"], &letter["count"], &letter["trace"]),
        (&"b".into(), &"full".into(), &"orders".into(), &"a".into(), &3.into(), &id.clone().into()),
        "{dead}"
    );
    // With the napper's letter above.
    eventually("the collector stores the letters", || async { store.dead_letters(10).await.unwrap().len() == 2 }).await;
    let (_, dead) = body("api/dead-letters".into()).await;
    assert_eq!(dead["letters"].as_array().unwrap().len(), 2, "held by the node and by the store, listed once: {dead}");

    let now = overview["timestamp_ms"].as_u64().unwrap() + 60_000;
    let range = format!("from_ms={}&to_ms={now}&step_ms=1000", now - 120_000);
    eventually("history answers a page of charts in one request", || async {
        let (status, answer) =
            body(format!("api/history?metrics=runtime.actors,no.such.metric&node_id=b&{range}")).await;
        let results = answer["results"].as_array().unwrap();
        status == 200
            && answer["source"] == "a"
            && results[0]["metric"] == "runtime.actors"
            && results[0]["result"]["series"][0]["node_id"] == "b"
            && results[1]["result"]["series"].as_array().is_some_and(Vec::is_empty)
    })
    .await;
    // In columns: one time axis, each series' header once and a value per step, and the edge before which they settled.
    let (_, answer) = body(format!("api/history?metrics=runtime.actors&{range}")).await;
    let result = &answer["results"][0]["result"];
    let from = (now - 120_000) / 1000 * 1000;
    assert_eq!(
        (&result["from"], &result["step"], &result["count"]),
        (&from.into(), &1000.into(), &121.into()),
        "{result}"
    );
    for series in result["series"].as_array().unwrap() {
        assert_eq!(series["values"].as_array().unwrap().len(), 121, "{series}");
        assert!(series.get("metric").is_none() && series.get("summed").is_none(), "{series}");
    }
    assert!(result["settled"].as_u64().is_some_and(|settled| settled <= now + 1 - 60_000), "{result}");
    eventually("a sum over the cluster adds up both nodes", || async {
        let (_, answer) = body(format!("api/history?metrics=runtime.actors&{range}&sum=cluster")).await;
        let series = answer["results"][0]["result"]["series"].as_array().unwrap().clone();
        series.len() == 1 && series[0]["node_id"] == "" && series[0]["summed"] == 2
    })
    .await;
    let (_, answer) = body(format!("api/history?metrics=runtime.actors&{range}&sum=node&by=pool")).await;
    let nodes: Vec<_> =
        answer["results"][0]["result"]["series"].as_array().unwrap().iter().map(|s| s["node_id"].clone()).collect();
    assert_eq!(nodes, ["a", "b"], "one series per node boot: {answer}");
    for hostile in ["sum=avg", "by=pool", &format!("sum=node&by={}", ["l"; 9].join(","))] {
        assert_eq!(body(format!("api/history?metrics=runtime.actors&{range}&{hostile}")).await.0, 400, "{hostile}");
    }
    let (status, listed) = body("api/series?metric=runtime.actors&node_id=b".into()).await;
    assert_eq!((status, &listed["source"]), (200, &"a".into()), "{listed}");
    let headers = listed["series"].as_array().unwrap();
    assert!(
        !headers.is_empty() && headers.iter().all(|s| s["node_id"] == "b" && s.get("values").is_none()),
        "{listed}"
    );
    assert_eq!(body("api/series?node_id=b".into()).await.0, 400, "no metric");
    assert_eq!(body("api/series?metric=runtime.actors&history=b".into()).await.0, 400, "no store there");

    assert_eq!(body(format!("api/history?metrics=runtime.actors&{range}&history=b")).await.0, 400);
    assert_eq!(body(format!("api/history?metrics=runtime.actors&{range}&history=a")).await.0, 200);
    assert_eq!(body("api/history?metrics=runtime.actors&from_ms=5&to_ms=1&step_ms=1000".into()).await.0, 400);
    assert_eq!(body(format!("api/history?{range}")).await.0, 400);
    let labels: String = (0..9).map(|n| format!("&label.l{n}=v")).collect();
    for hostile in [
        "step_ms=1",
        "step_ms=99999999999",
        "max_points=0",
        "max_points=18446744073709551616",
        "max_points=-1",
        "max_series=100000",
        "from_ms=0",
        "aggregation=median",
        "to_ms=abc",
        &labels[1..],
        &format!("node_id={}", "n".repeat(300)),
        &format!("metrics={}", ["m"; 40].join(",")),
    ] {
        let (status, answer) = body(format!("api/history?metrics=runtime.actors&{range}&{hostile}")).await;
        assert_eq!(status, 400, "{hostile}: {answer}");
    }

    // A client library would tidy these paths up before sending them, so they go out by hand.
    for path in [
        "assets/../../../../etc/passwd",
        "assets/%2e%2e/%2e%2e/%2e%2e/etc/passwd",
        "..%2f..%2fCargo.toml",
        "//etc/passwd",
    ] {
        let mut raw = TcpStream::connect(dashboard.address()).await.unwrap();
        let request =
            format!("GET /ops/axon/{path} HTTP/1.1\r\nHost: x\r\nCookie: {cookie}\r\nConnection: close\r\n\r\n");
        raw.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        raw.read_to_string(&mut answer).await.unwrap();
        let served_the_app = answer.starts_with("HTTP/1.1 200") && answer.contains("<base href=");
        assert!(served_the_app || answer.starts_with("HTTP/1.1 404"), "{path}: {}", &answer[..answer.len().min(200)]);
        assert!(!answer.contains("root:") && !answer.contains("[workspace]"), "{path} left the embedded files");
    }

    let (c_system, c) = node("c", &discovery).await;
    eventually("a join shows live and in the timeline", || async {
        let (_, overview) = body("api/overview".into()).await;
        let (_, timeline) = body(format!("api/timeline?to_ms={now}&limit=10")).await;
        let joined =
            |events: &Json| events.as_array().unwrap().iter().any(|e| e["kind"] == "joined" && e["node_id"] == "c");
        joined(&overview["events"]) && joined(&timeline)
    })
    .await;
    // Another node's own timeline reaches the live feed too, without a store; a burst on it, more than the feed
    // holds, leaves the membership changes in place.
    for i in 0..300 {
        c.events().record("acquired", format!("orders/{i}"), "epoch 1");
    }
    eventually("c's events show live beside its join", || async {
        let (_, overview) = body("api/overview".into()).await;
        let events = overview["events"].as_array().unwrap();
        let joined = events.iter().any(|e| e["kind"] == "joined" && e["node_id"] == "c");
        joined && events.iter().any(|e| e["kind"] == "acquired" && e["node_id"] == "c" && e["subject"] == "orders/299")
    })
    .await;
    assert_eq!(body("api/timeline?limit=100000".into()).await.0, 400);
    let (status, joins) = body("api/timeline?limit=100&node=c&kind=joined".into()).await;
    let joins = joins.as_array().unwrap().clone();
    assert!(status == 200 && !joins.is_empty(), "{status} {joins:?}");
    assert!(joins.iter().all(|e| e["node_id"] == "c" && e["kind"] == "joined"), "filtered on the server: {joins:?}");
    assert_eq!(body("api/timeline?limit=100&node=c&kind=left".into()).await.1, Json::Array(vec![]));
    assert_eq!(body(format!("api/timeline?node={}", "n".repeat(300))).await.0, 400);

    a_system.shutdown().await.unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(2), async { while let Ok(Some(_)) = stream.chunk().await {} });
    ended.await.expect("shutdown ends live streams");
    // The port, not `get`: its kept-alive connection may still be answered until that connection's task sees the stop.
    assert!(std::net::TcpListener::bind(dashboard.address()).is_ok(), "the port is free again");
    assert!(dashboard.overview().await.is_empty(), "a request caught by the shutdown gets nothing, not a panic");
    for system in [b_system, c_system] {
        system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_and_live_streams_past_the_limit_are_refused_until_a_place_frees_up() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (h_system, h)) = (node("a", &discovery).await, bare("h", &discovery).await);
    // `h` takes every question about its actors and answers none until it is released.
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let arrived = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (held, counted) = (release.clone(), arrived.clone());
    h.serve_control(TELEMETRY_SERVICE, AccessPolicy::Cluster, move |request| {
        let held = held.clone();
        let about_actors = matches!(serde_json::from_slice(&request.body), Ok(Request::Actors(_)));
        if about_actors {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        async move {
            if about_actors {
                let _ = held.acquire().await;
            }
            Err("released".into())
        }
    })
    .unwrap();
    eventually("membership converges", || async { a.view().members.len() == 2 }).await;

    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let dashboard = Dashboard::start(&a_system, &a, cfg).await.unwrap();
    let base = format!("http://{}", dashboard.address());
    let http = reqwest::Client::new();

    let mut live = Vec::new();
    for _ in 0..32 {
        let stream = http.get(format!("{base}/api/events")).send().await.unwrap();
        assert_eq!(stream.status(), 200);
        live.push(stream);
    }
    assert_eq!(http.get(format!("{base}/api/events")).send().await.unwrap().status(), 429);
    live.pop();
    eventually("a closed stream gives its place back", || async {
        http.get(format!("{base}/api/events")).send().await.unwrap().status() == 200
    })
    .await;

    let stuck = || http.get(format!("{base}/api/actors?node_id=h")).timeout(Duration::from_secs(20)).send();
    let waiting: Vec<_> = (0..16).map(|_| tokio::spawn(stuck())).collect();
    eventually("all sixteen are with h", || async { arrived.load(std::sync::atomic::Ordering::SeqCst) == 16 }).await;
    assert_eq!(stuck().await.unwrap().status(), 429, "the seventeenth");
    assert_eq!(http.get(format!("{base}/api/overview")).send().await.unwrap().status(), 200, "the page still loads");
    release.add_permits(1000);
    for request in waiting {
        assert_ne!(request.await.unwrap().unwrap().status(), 429);
    }
    eventually("the places come back", || async { stuck().await.unwrap().status() != 429 }).await;

    for system in [a_system, h_system] {
        system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_is_a_signed_cookie_that_slides_expires_and_rotates_with_its_key() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (b_system, b)) = (bare("a", &discovery).await, bare("b", &discovery).await);
    let bea = User { name: "bea".into(), hash: hash_password("other").unwrap() };
    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        base_path: "/ops".into(),
        users: vec![admin(), bea],
        session: SessionConfig { key: Some(vec![7; 32]), ..Default::default() },
        ..Default::default()
    };
    let short =
        DashboardConfig { session: SessionConfig { key: Some(vec![7; 31]), ..Default::default() }, ..cfg.clone() };
    assert!(Dashboard::start(&a_system, &a, short).await.is_err(), "a short key");
    // A 2 s idle and a 3 s maximum, so both lapse within the test. Only this dashboard's sessions do: the others
    // outlast any runner, so no step after its own depends on how long the steps before it took. Its sleeps of 1.1 s
    // leave each step 0.9 s to answer in.
    let session = SessionConfig { idle: Duration::from_secs(2), max: Duration::from_secs(3), ..cfg.session.clone() };
    let lapsing = Dashboard::start(&a_system, &a, DashboardConfig { session, ..cfg.clone() }).await.unwrap();
    let one = Dashboard::start(&a_system, &a, cfg.clone()).await.unwrap();
    let two = Dashboard::start(&b_system, &b, cfg.clone()).await.unwrap();
    let admin_only = Dashboard::start(&b_system, &b, DashboardConfig { users: vec![admin()], ..cfg }).await.unwrap();
    let base = |d: &Dashboard| format!("http://{}/ops/", d.address());
    let http = reqwest::Client::new();
    let whoami = async |d: &Dashboard, cookie: &str| {
        http.get(format!("{}api/session", base(d))).header("cookie", cookie).send().await.unwrap()
    };

    let origin = format!("http://{}", one.address());
    let form = [("user", "admin"), ("password", "secret")];
    let login = http.post(format!("{}login", base(&lapsing))).header("origin", format!("http://{}", lapsing.address()));
    let login = login.form(&form).send().await.unwrap();
    assert_eq!(login.status(), 200);
    let attributes = set_cookie(&login);
    for attribute in ["HttpOnly", "SameSite=Strict", "Path=/ops/", "Max-Age=2"] {
        assert!(attributes.split("; ").any(|a| a == attribute), "{attribute}: {attributes}");
    }
    assert!(!attributes.contains("Secure"), "not over plain HTTP: {attributes}");
    let first = session_cookie(&login);
    let mut idling = http.get(format!("{}api/events", base(&lapsing))).header("cookie", &first).send().await.unwrap();
    assert_eq!(idling.status(), 200);

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let used = whoami(&lapsing, &first).await;
    assert_eq!(used.status(), 200);
    let slid = session_cookie(&used);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let lapsed = whoami(&lapsing, &first).await;
    assert_eq!(lapsed.status(), 401, "idle for 2.2 s");
    assert_eq!(lapsed.json::<Json>().await.unwrap()["error"], "the session expired", "the sign-in page says why");
    let ended =
        tokio::time::timeout(Duration::from_secs(1), async { while idling.chunk().await.unwrap().is_some() {} });
    ended.await.expect("the live stream ended with its session");
    let used = whoami(&lapsing, &slid).await;
    assert_eq!(used.status(), 200, "used 1.1 s ago: the idle time slid");
    let slid = session_cookie(&used);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(whoami(&lapsing, &slid).await.status(), 401, "used 1.1 s ago, but signed in 3.3 s ago");

    // Behind one balancer: the same key, so either node takes the other's cookie.
    let fresh = sign_in(&http, &base(&one), "admin", "secret").await.unwrap();
    let answer = whoami(&two, &fresh).await;
    assert_eq!(answer.status(), 200);
    assert_eq!(answer.json::<Json>().await.unwrap()["user"], "admin");
    // Signed by a dashboard whose clock runs 13 h ahead, beyond a whole session: this one's clock slides it back
    // behind its sign-in time.
    let ahead = axon::trace::unix_micros() / 1000 + 13 * 3600 * 1000;
    let body = format!("YWRtaW4.{ahead}.{ahead}");
    let mac = ring::hmac::sign(&ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &[7; 32]), body.as_bytes());
    let mut cookie = format!(
        "axon_session={body}.{}",
        base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, mac.as_ref())
    );
    for _ in 0..2 {
        let answer = whoami(&one, &cookie).await;
        assert_eq!(answer.status(), 200, "{cookie}");
        cookie = session_cookie(&answer);
    }

    let fields: Vec<&str> = fresh.strip_prefix("axon_session=").unwrap().split('.').collect();
    let later = (fields[2].parse::<u64>().unwrap() + 600_000).to_string();
    let forged = [
        ["YmVh", fields[1], fields[2], fields[3]].join("."),
        [fields[0], fields[1], &later, fields[3]].join("."),
        [fields[0], fields[1], fields[2], "AAAA"].join("."),
        [fields[0], fields[1], fields[2]].join("."),
    ];
    for forged in forged {
        assert_eq!(whoami(&one, &format!("axon_session={forged}")).await.status(), 401, "{forged}");
    }

    let bea = sign_in(&http, &base(&one), "bea", "other").await.expect("a second user");
    assert_eq!(whoami(&two, &bea).await.status(), 200);
    assert_eq!(whoami(&admin_only, &bea).await.status(), 401, "no longer listed");
    assert_eq!(whoami(&admin_only, &fresh).await.status(), 200);
    assert_eq!(sign_in(&http, &base(&one), "bea", "secret").await, Err(401), "another user's password");
    assert_eq!(sign_in(&http, &base(&one), "nobody", "secret").await, Err(401));

    // Compressed, as a browser asks for it: the stream still ends cleanly, with the encoder's last bytes.
    let watching =
        http.get(format!("{}api/events", base(&one))).header("cookie", &fresh).header("accept-encoding", "gzip");
    let mut watching = watching.send().await.unwrap();
    assert!(watching.chunk().await.unwrap().is_some());
    one.sign_out_everyone(None).unwrap();
    let refused = whoami(&one, &fresh).await;
    assert_eq!(refused.status(), 401, "a new random key");
    let why = refused.json::<Json>().await.unwrap()["error"].clone();
    assert_eq!(why, "signed out: the console restarted or signed everyone out", "the sign-in page says why");
    logged(&one, "signed_out_everyone", "the console refuses every session that it signed before").await;
    let ended =
        tokio::time::timeout(Duration::from_secs(1), async { while watching.chunk().await.unwrap().is_some() {} });
    ended.await.expect("signing everyone out ends the live streams too");
    assert_eq!(whoami(&two, &fresh).await.status(), 200, "the other node keeps its key until it is given the new one");
    one.sign_out_everyone(Some(vec![9; 32])).unwrap();
    two.sign_out_everyone(Some(vec![9; 32])).unwrap();
    let again = sign_in(&http, &base(&one), "admin", "secret").await.unwrap();
    assert_eq!(whoami(&one, &fresh).await.status(), 401);
    assert_eq!(whoami(&two, &again).await.status(), 200);
    assert!(one.sign_out_everyone(Some(vec![9; 8])).is_err());

    let logout = async |origin: Option<&str>| {
        let request = http.post(format!("{}logout", base(&one))).header("cookie", &again);
        let request = if let Some(origin) = origin { request.header("origin", origin) } else { request };
        request.send().await.unwrap()
    };
    assert_eq!(logout(None).await.status(), 403);
    assert_eq!(logout(Some("http://elsewhere.example")).await.status(), 403);
    let out = logout(Some(&origin)).await;
    assert_eq!(out.status(), 204);
    assert!(set_cookie(&out).contains("Max-Age=0"), "{}", set_cookie(&out));

    for (kind, detail) in [
        ("login", "admin from 127.0.0.1"),
        ("login", "bea from 127.0.0.1"),
        ("login_failed", "bea from 127.0.0.1"),
        ("login_failed", "unknown user from 127.0.0.1"),
        ("logout", "admin from 127.0.0.1"),
    ] {
        logged(&one, kind, detail).await;
    }
    for system in [a_system, b_system] {
        system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sign_ins_are_limited_per_client_and_per_user_and_only_a_trusted_proxy_names_the_client() {
    let (system, cluster) = bare("a", &MemoryDiscovery::new()).await;
    let cfg = |trusted: &[&str]| DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        users: vec![admin()],
        trusted_proxies: trusted.iter().map(|ip| ip.parse::<IpAddr>().unwrap()).collect(),
        ..Default::default()
    };
    let direct = Dashboard::start(&system, &cluster, cfg(&[])).await.unwrap();
    let proxied = Dashboard::start(&system, &cluster, cfg(&["192.0.2.1", "127.0.0.1"])).await.unwrap();
    let http = reqwest::Client::new();
    let attempt = async |d: &Dashboard, user: &str, password: &str, client: &str| {
        let (url, origin) = (format!("http://{}/login", d.address()), format!("http://{}", d.address()));
        let request = http.post(url).header("origin", origin).header("x-forwarded-for", client);
        let form = [("user", user), ("password", password)];
        request.header("x-forwarded-proto", "https").form(&form).send().await.unwrap()
    };

    // From an untrusted peer the forwarded headers are ignored: five "clients" are the one peer.
    for i in 0..5 {
        assert_eq!(attempt(&direct, &format!("u{i}"), "x", &format!("10.0.0.{i}")).await.status(), 401);
    }
    let locked = attempt(&direct, "admin", "secret", "10.0.0.9").await;
    assert_eq!(locked.status(), 429, "the right password too, while the client is locked out");
    assert!(locked.headers()["retry-after"].to_str().unwrap().parse::<u64>().unwrap() > 0);
    assert_eq!(attempt(&direct, "admin", "secret", "10.0.0.9").await.status(), 429);
    logged(&direct, "login_failed", "unknown user from 127.0.0.1").await;
    logged(&direct, "login_limited", "admin from 127.0.0.1").await;
    let log = events(&direct.overview().await);
    let limited: Vec<&str> = log.iter().filter(|(kind, _)| kind == "login_limited").map(|(_, d)| d.as_str()).collect();
    assert_eq!(limited, ["admin from 127.0.0.1"], "once per lockout");

    // Through a trusted proxy the client is the nearest address no trusted proxy vouches for, and https counts.
    let signed = attempt(&proxied, "admin", "secret", "10.0.0.1, 192.0.2.1").await;
    assert_eq!(signed.status(), 200);
    assert!(set_cookie(&signed).ends_with("; Secure"), "{}", set_cookie(&signed));
    assert!(signed.headers().contains_key("strict-transport-security"));
    logged(&proxied, "login", "admin from 10.0.0.1").await;
    // A browser can send its own X-Forwarded-For; the proxy appends the address it saw.
    assert_eq!(attempt(&proxied, "admin", "secret", "6.6.6.6, 10.0.0.7").await.status(), 200);
    logged(&proxied, "login", "admin from 10.0.0.7").await;
    // Five clients guessing one user's password lock that user out; the success before took nothing for good.
    for i in 0..5 {
        assert_eq!(attempt(&proxied, "admin", "x", &format!("10.0.1.{i}")).await.status(), 401);
    }
    assert_eq!(attempt(&proxied, "admin", "secret", "10.0.1.9").await.status(), 429);

    let open = Dashboard::start(&system, &cluster, cfg(&[])).await.unwrap();
    let signed = attempt(&open, "admin", "secret", "10.0.0.1").await;
    assert!(!set_cookie(&signed).contains("Secure"), "an untrusted peer cannot claim https");
    assert!(!signed.headers().contains_key("strict-transport-security"));
    let page = format!("http://{}/login?next=nodes", open.address());
    let login = async |headers: &[(&str, &str)]| {
        let mut request = http.post(format!("http://{}/login", open.address()));
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.form(&[("user", "admin"), ("password", "secret")]).send().await.unwrap().status()
    };
    let forwarded = [("origin", "http://elsewhere.example"), ("x-forwarded-host", "elsewhere.example")];
    for refused in
        [&[][..], &[("origin", "http://elsewhere.example")], &[("origin", "null"), ("referer", &page)], &forwarded]
    {
        assert_eq!(login(refused).await, 403, "{refused:?}");
    }
    assert_eq!(login(&[("referer", &page)]).await, 200, "a Referer from the console's own page");

    let unlisted = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let unlisted = Dashboard::start(&system, &cluster, unlisted).await.unwrap();
    assert_eq!(attempt(&unlisted, "admin", "secret", "").await.status(), 404, "an open console has no sign-in");
    let whoami = reqwest::get(format!("http://{}/api/session", unlisted.address())).await.unwrap();
    assert_eq!(whoami.json::<Json>().await.unwrap()["user"], Json::Null);
    system.shutdown().await.unwrap();
}

/// A CA the client trusts, and the dashboard's certificate for `localhost` from it.
fn certificates() -> (reqwest::Certificate, Arc<axon_net::rustls::ServerConfig>, Arc<axon_net::rustls::ClientConfig>) {
    use axon_net::rustls::{ClientConfig, RootCertStore, ServerConfig, crypto::ring, pki_types::PrivatePkcs8KeyDer};
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca.self_signed(&ca_key).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let leaf = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    let leaf = leaf.signed_by(&key, &ca, &ca_key).unwrap();
    let server = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![leaf.der().clone()], PrivatePkcs8KeyDer::from(key.serialize_der()).into())
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(ca.der().clone()).unwrap();
    let client = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (reqwest::Certificate::from_der(ca.der()).unwrap(), Arc::new(server), Arc::new(client))
}

/// A client that trusts only `ca` and finds `localhost` at `address`.
fn https(ca: &reqwest::Certificate, address: SocketAddr) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder().use_rustls_tls().tls_built_in_root_certs(false);
    builder.add_root_certificate(ca.clone()).resolve("localhost", address)
}

// One worker: the aborted task that holds the TLS listener cannot be dropped behind the hooks' backs.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn the_dashboard_serves_https_and_takes_new_certificates_while_it_runs() {
    let (system, cluster) = bare("a", &MemoryDiscovery::new()).await;
    let ((first_ca, first, _), (second_ca, second, _)) = (certificates(), certificates());
    let plain = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let plain = Dashboard::start(&system, &cluster, plain).await.unwrap();
    assert!(plain.set_tls(first.clone()).is_err(), "started without TLS");
    // Between the two, so that its hook runs right after the TLS dashboard's: the plain one's would let tasks run.
    let rebind = Arc::new(common::Rebind::default());
    system.attach_extension(rebind.clone()).unwrap();

    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        users: vec![admin()],
        tls: Some(first),
        ..Default::default()
    };
    let dashboard = Dashboard::start(&system, &cluster, cfg).await.unwrap();
    let base = format!("https://localhost:{}/", dashboard.address().port());
    let client = |ca: &reqwest::Certificate| https(ca, dashboard.address()).build().unwrap();
    let http = client(&first_ca);
    let login = http
        .post(format!("{base}login"))
        .header("origin", base.trim_end_matches('/'))
        .form(&[("user", "admin"), ("password", "secret")])
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), 200);
    assert!(set_cookie(&login).ends_with("; Secure"), "{}", set_cookie(&login));
    assert!(login.headers()["strict-transport-security"].to_str().unwrap().starts_with("max-age="));
    let cookie = session_cookie(&login);
    let overview = http.get(format!("{base}api/overview")).header("cookie", &cookie).send().await.unwrap();
    assert_eq!(overview.status(), 200);
    assert!(reqwest::get(format!("http://{}/healthz", dashboard.address())).await.is_err(), "no plain HTTP");

    dashboard.set_tls(second).unwrap();
    assert!(client(&first_ca).get(format!("{base}healthz")).send().await.is_err(), "a new connection gets the new one");
    assert_eq!(client(&second_ca).get(format!("{base}healthz")).send().await.unwrap().status(), 200);
    let kept = http.get(format!("{base}api/session")).header("cookie", &cookie).send().await.unwrap();
    assert_eq!(kept.status(), 200, "a connection opened before the swap keeps its certificate");
    *rebind.0.lock() = vec![dashboard.address()];
    assert_eq!(system.shutdown().await.unwrap().failures, Vec::<String>::new(), "the port is free once it returns");
}

/// Opens 64 connections to `address` that never start a handshake, and says how many were closed at once.
async fn silent(address: SocketAddr) -> (usize, Vec<TcpStream>) {
    let mut reads = tokio::task::JoinSet::new();
    for _ in 0..64 {
        let mut tcp = TcpStream::connect(address).await.unwrap();
        reads.spawn(async move {
            let read = tokio::time::timeout(Duration::from_secs(1), tcp.read(&mut [0; 1])).await;
            (matches!(read, Ok(Ok(0) | Err(_))), tcp)
        });
    }
    let (closed, open): (Vec<_>, Vec<_>) = reads.join_all().await.into_iter().partition(|(closed, _)| *closed);
    (closed.len(), open.into_iter().map(|(_, tcp)| tcp).collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_has_16_handshakes_under_way_and_a_trusted_proxy_only_the_ports_64() {
    let (system, cluster) = bare("a", &MemoryDiscovery::new()).await;
    let (ca, tls, _) = certificates();
    let start = |trusted_proxies: Vec<IpAddr>| {
        let cfg = DashboardConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            tls: Some(tls.clone()),
            trusted_proxies,
            ..Default::default()
        };
        Dashboard::start(&system, &cluster, cfg)
    };
    let (open, proxied) = (start(Vec::new()).await.unwrap(), start(vec![[127, 0, 0, 1].into()]).await.unwrap());
    let served = |address: SocketAddr| {
        let http = https(&ca, address).timeout(Duration::from_secs(1)).build().unwrap();
        let asked = http.get(format!("https://localhost:{}/healthz", address.port())).send();
        async move { asked.await.is_ok_and(|r| r.status() == 200) }
    };

    // Past its share an address's connections are closed at once, so one client cannot hold every place.
    let (closed, held) = silent(open.address()).await;
    assert_eq!((closed, held.len()), (64 - 16, 16), "an untrusted client holds its 16");
    drop(held);
    // Their places come back once the server reads each one's end, which a busy machine does a little later.
    eventually("a real client is served once the silent ones have gone", || served(open.address())).await;

    // The same address as a trusted proxy takes every place the port has, and no more.
    let (closed, held) = silent(proxied.address()).await;
    assert_eq!(closed, 0, "a trusted proxy holds past a client's share");
    assert!(!served(proxied.address()).await, "the port's 64 bound a trusted proxy too");
    drop(held);
    eventually("the proxy is served once the silent ones have gone", || served(proxied.address())).await;
    system.shutdown().await.unwrap();
}

/// Writes `start`, then a byte a second, as a slowloris client does, and says when the server answered or ended it.
async fn trickle(mut io: impl AsyncRead + AsyncWrite + Unpin, start: &[u8]) -> Duration {
    let started = tokio::time::Instant::now();
    let mut head = start;
    while started.elapsed() < Duration::from_secs(20) {
        if io.write_all(head).await.is_err() || io.flush().await.is_err() {
            break;
        }
        head = b"a";
        if tokio::time::timeout(Duration::from_secs(1), io.read(&mut [0; 64])).await.is_ok() {
            break;
        }
    }
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_has_10_s_a_head_16_kib_and_a_sign_in_4_kib_on_every_port() {
    let (system, cluster) = bare("a", &MemoryDiscovery::new()).await;
    let (ca, tls, client) = certificates();
    let plain = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let plain = Dashboard::start(&system, &cluster, plain).await.unwrap().address();
    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        users: vec![admin()],
        tls: Some(tls),
        ..Default::default()
    };
    let secure = Dashboard::start(&system, &cluster, cfg).await.unwrap().address();
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap().address();

    let tcp = async |address| TcpStream::connect(address).await.unwrap();
    let localhost = "localhost".try_into().unwrap();
    let to_secure = tokio_rustls::TlsConnector::from(client).connect(localhost, tcp(secure).await).await.unwrap();
    let head = b"GET /healthz HTTP/1.1\r\nx-slow: ";
    let body =
        b"POST /login HTTP/1.1\r\ncontent-type: application/x-www-form-urlencoded\r\ncontent-length: 4000\r\n\r\n";
    let (to_health, to_plain, to_sign_in) = (tcp(health).await, tcp(plain).await, tcp(plain).await);
    let cut = tokio::join!(
        trickle(to_health, head),
        trickle(to_plain, head),
        trickle(to_secure, head),
        trickle(to_sign_in, body)
    );
    for (port, cut) in [("health", cut.0), ("plain", cut.1), ("tls", cut.2), ("a sign-in body", cut.3)] {
        assert!((Duration::from_secs(9)..Duration::from_secs(13)).contains(&cut), "{port}: cut after {cut:?}");
    }

    for address in [health, plain] {
        let mut tcp = tcp(address).await;
        let head = format!("GET /healthz HTTP/1.1\r\nx-big: {}", "a".repeat(17 * 1024));
        let _ = tcp.write_all(head.as_bytes()).await;
        let mut answer = [0; 12];
        let read = tokio::time::timeout(Duration::from_secs(2), tcp.read(&mut answer)).await;
        let ended = matches!(read, Ok(Ok(0) | Err(_))) || answer.starts_with(b"HTTP/1.1 431");
        assert!(ended, "a head past 16 KiB is refused, not waited on: {read:?}");
    }

    let base = format!("https://localhost:{}/", secure.port());
    let http = https(&ca, secure).build().unwrap();
    assert_eq!(sign_in(&http, &base, "admin", &"a".repeat(4096)).await, Err(413), "refused before it is checked");
    assert!(sign_in(&http, &base, "admin", "secret").await.is_ok());
    system.shutdown().await.unwrap();
}

/// A connection that takes 4 KiB at a time, so an answer longer than that waits on its reader.
async fn narrow(address: SocketAddr) -> TcpStream {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    socket.connect(address).await.unwrap()
}

/// Asks, far past what the server reads ahead, and reads nothing: how long after the ask the server cut it, or `None`
/// if it has not within 13 s. A cut closes the connection with asks unread, which resets it at once, so the cut shows
/// without draining the megabytes of answer the server's socket still holds, which on loaded cores takes seconds.
async fn stall<S: AsyncWrite + Unpin>(mut io: S, tcp: fn(&S) -> &TcpStream, ask: String) -> Option<Duration> {
    let asked = tokio::time::Instant::now();
    io.write_all(ask.as_bytes()).await.unwrap();
    io.flush().await.unwrap();
    while asked.elapsed() < Duration::from_secs(13) {
        if tcp(&io).take_error().unwrap().is_some() {
            return Some(asked.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread")]
async fn an_answer_the_client_stops_reading_is_cut_after_10_s_and_gives_its_place_back() {
    let (system, cluster) = bare("a", &MemoryDiscovery::new()).await;
    let (_, tls, client) = certificates();
    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let address = Dashboard::start(&system, &cluster, cfg).await.unwrap().address();
    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), tls: Some(tls), ..Default::default() };
    let secure = Dashboard::start(&system, &cluster, cfg).await.unwrap().address();
    let telemetry = Telemetry::start(&system, &cluster, TelemetryConfig::default()).await.unwrap();
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    health.serve_metrics(&telemetry);
    let health = health.address();
    let index = reqwest::get(format!("http://{address}/")).await.unwrap().text().await.unwrap();
    let bundle = index.split("src=\"./").nth(1).and_then(|rest| rest.split('"').next()).unwrap();
    let whole = reqwest::get(format!("http://{address}/{bundle}")).await.unwrap().bytes().await.unwrap();
    let ask = format!("GET /{bundle} HTTP/1.1\r\nhost: a\r\n\r\n");
    let probe = "GET /healthz HTTP/1.1\r\nhost: a\r\n\r\n";
    let scrape = "GET /metrics HTTP/1.1\r\nhost: a\r\n\r\n";

    // Every place this address has: one client asks for 32 MiB of the bundle and reads the answers slowly for 12 s, more
    // than the kernel's buffers take before the place is checked, and the rest ask 2000 times and read nothing. A TLS
    // port gets one that reads nothing too, as does a Health port's scraper.
    let times = (32 << 20) / whole.len() + 1;
    let started = tokio::time::Instant::now();
    let mut slow = TcpStream::connect(address).await.unwrap();
    let last = ask.replace("\r\n\r\n", "\r\nconnection: close\r\n\r\n");
    slow.write_all((ask.repeat(times - 1) + &last).as_bytes()).await.unwrap();
    let slow = tokio::spawn(async move {
        let (mut got, mut chunk) = (Vec::new(), vec![0; 64 * 1024]);
        while started.elapsed() < Duration::from_secs(12) {
            match slow.read(&mut chunk).await {
                Ok(n @ 1..) => got.extend_from_slice(&chunk[..n]),
                _ => break,
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let _ = slow.read_to_end(&mut got).await;
        got
    });
    let mut stalled = Vec::new();
    for _ in 0..63 {
        stalled.push(("plain", tokio::spawn(stall(narrow(address).await, |tcp| tcp, ask.repeat(2000)))));
    }
    let localhost = "localhost".try_into().unwrap();
    let to_secure = tokio_rustls::TlsConnector::from(client).connect(localhost, narrow(secure).await).await.unwrap();
    stalled.push(("tls", tokio::spawn(stall(to_secure, |tls| tls.get_ref().0, ask.repeat(2000)))));
    stalled.push(("health", tokio::spawn(stall(narrow(health).await, |tcp| tcp, scrape.repeat(2000)))));

    tokio::time::sleep_until(started + Duration::from_secs(8)).await;
    let mut past = TcpStream::connect(address).await.unwrap();
    let read = tokio::time::timeout(Duration::from_secs(2), past.read(&mut [0; 1])).await;
    assert!(matches!(read, Ok(Ok(0) | Err(_))), "the places are held until the deadline");
    for (port, stalled) in stalled {
        let cut = stalled.await.unwrap().unwrap_or_else(|| panic!("{port}: not cut within 13 s of its ask"));
        assert!(cut >= Duration::from_secs(10), "{port}: cut {cut:?} after its ask, before 10 s without progress");
    }
    let got = slow.await.unwrap();
    assert!(
        got.len() > times * whole.len() && got.ends_with(&whole),
        "a client that reads slowly but keeps reading is not cut"
    );
    // Every place comes back: the address holds 64 connections at once again, each served.
    eventually("the address gets all its places back", || async {
        let mut again = Vec::new();
        for _ in 0..64 {
            again.push(TcpStream::connect(address).await.unwrap());
        }
        for tcp in &mut again {
            let _ = tcp.write_all(probe.as_bytes()).await;
            let read = tokio::time::timeout(Duration::from_secs(2), tcp.read(&mut [0; 1024])).await;
            if !matches!(read, Ok(Ok(1..))) {
                return false;
            }
        }
        true
    })
    .await;
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn redaction_hides_actor_keys_and_span_names_in_every_reply() {
    let (system, cluster) = bare("a", &MemoryDiscovery::new()).await;
    let telemetry = TelemetryConfig { access: AccessPolicy::Nodes(["a".to_string()].into()), ..Default::default() };
    let telemetry = Telemetry::start(&system, &cluster, telemetry).await.unwrap();
    let orders = system.spawn(Idle).name("orders/cust-42").start().unwrap();
    let _later = system.spawn(Idle).name("orders/cust-17").start().unwrap();
    common::keyed(&cluster, "carts", ["cust-42".to_string()]).await;
    let streams = StreamNode::start(&cluster, StreamNodeConfig::new(vec![])).await.unwrap();
    telemetry.watch_streams(&streams);
    let key = StreamKey::new("prices", "cust-42");
    streams.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        redact: true,
        // Refreshed every 100 ms, so the page fills within the test.
        refresh_interval: Duration::from_millis(100),
        ..Default::default()
    };
    let dashboard = Dashboard::start(&system, &cluster, cfg).await.unwrap();
    let base = format!("http://{}/", dashboard.address());
    let get = async |path: &str| reqwest::get(format!("{base}{path}")).await.unwrap().text().await.unwrap();
    let collect = MetricsConfig {
        // Collected every 100 ms, so the timeline fills within the test.
        collection_interval: Some(Duration::from_millis(100)),
        access: AccessPolicy::Nodes(["a".to_string()].into()),
        ..Default::default()
    };
    MetricsService::start(&system, &cluster, Arc::new(MemoryMetrics::new(1 << 20)), collect).await.unwrap();
    system.events().record("acquired", "orders/cust-42", "epoch 1");

    let (handled, published) = (TraceContext::root(), TraceContext::root());
    handled.enter(|| orders.tell(()).unwrap());
    published.enter(|| streams.publish(&key, vec![bytes::Bytes::from_static(b"p")]).unwrap());
    let traces = [format!("api/trace?id={:016x}", handled.trace), format!("api/trace?id={:016x}", published.trace)];
    eventually("the actors, a keyed placement, an event and both spans are listed, without the key", || async {
        let overview = get("api/overview").await;
        overview.contains("\"subject\":\"orders/…\"")
            && overview.contains("\"kind\":\"carts\"")
            && get("api/inventory?q=orders").await.contains("\"name\":\"orders/…\"")
            && get(&traces[0]).await.contains("\"actor_name\":\"orders/…\"")
            && get(&traces[1]).await.contains("\"name\":\"prices/…\"")
            && get("api/timeline").await.contains("\"subject\":\"orders/…\"")
    })
    .await;
    let letter = axon::DeadLetter {
        seq: 0,
        at_us: 7,
        reason: "full",
        label: "tell",
        actor: None,
        target: Some("orders/cust-42".into()),
        peer: None,
        trace: 0,
        count: 1,
    };
    system.dead_letters().record(letter, None);
    let mut stream = reqwest::get(format!("{base}api/events")).await.unwrap();
    let frame = String::from_utf8(stream.chunk().await.unwrap().unwrap().to_vec()).unwrap();
    let overview = String::from_utf8(dashboard.overview().await.to_vec()).unwrap();
    let mut replies = vec![frame, overview, get("api/overview").await, get("api/inventory").await];
    replies.extend([get("api/actors?node_id=a").await, get(&traces[0]).await, get(&traces[1]).await]);
    replies.extend([get("api/traces").await, get("api/dead-letters").await, get("api/timeline").await]);
    for reply in replies {
        assert!(reply.contains("/…") && !reply.contains("cust-42"), "{reply}");
    }
    let listed = get("api/streams").await;
    assert!(listed.contains("\"namespace\":\"prices\"") && !listed.contains("cust-42"), "{listed}");
    assert_eq!(get("api/streams").await, listed, "a stream keeps the name that stands in for its own");

    // A search sees only what the reply shows, or its answers would spell the key out.
    let total = async |search: &str| {
        let page: Json = serde_json::from_str(&get(&format!("api/inventory?q={search}")).await).unwrap();
        page["total"].as_u64().unwrap()
    };
    assert_eq!((total("orders/").await, total("orders/c").await, total("cust").await), (2, 0, 0));
    // Sorted by name, hidden keys tie, and fall back to the actor order rather than telling their own.
    let order = async |sort: &str| {
        let page: Json = serde_json::from_str(&get(&format!("api/inventory?q=orders/&sort={sort}")).await).unwrap();
        page["actors"].as_array().unwrap().iter().map(|a| a["incarnation"].clone()).collect::<Vec<_>>()
    };
    assert_eq!(order("name").await, order("node_id").await);
    let found: Json = serde_json::from_str(&get("api/actors?node_id=a&q=cust").await).unwrap();
    assert_eq!(found["actors"].as_array().unwrap().len(), 0, "{found}");
    let found = async |search: &str| {
        let list: Json = serde_json::from_str(&get(&format!("api/traces?q={search}")).await).unwrap();
        list["traces"].as_array().unwrap().len()
    };
    assert!(found("orders").await > 0);
    assert_eq!(found("cust").await, 0);
    // A lookup by key would answer for a key no reply shows.
    for path in ["api/key?kind=carts&key=cust-42", "api/keys?kind=carts", "api/keys?kind=carts&prefix=cust-4"] {
        let looked = reqwest::get(format!("{base}{path}")).await.unwrap();
        assert_eq!(looked.status(), 403, "{path}");
        assert!(!looked.text().await.unwrap().contains("cust-42"));
    }
    system.shutdown().await.unwrap();
}

/// Holds spans, and says it keeps none.
struct NoTraces(MemoryMetrics);

impl MetricsQuery for NoTraces {
    fn capabilities(&self) -> Capabilities {
        Capabilities { traces: false, ..self.0.capabilities() }
    }

    fn query(&self, query: &HistoryQuery) -> MetricsFuture<'_, HistoryResult> {
        self.0.query(query)
    }

    fn events(&self, query: &EventQuery) -> MetricsFuture<'_, Vec<ClusterEvent>> {
        self.0.events(query)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_that_keeps_no_traces_is_not_asked_for_them_and_its_refusal_is_no_error() {
    let (system, cluster) = bare("a", &MemoryDiscovery::new()).await;
    let access = AccessPolicy::Nodes(["a".to_string()].into());
    Telemetry::start(&system, &cluster, TelemetryConfig { access: access.clone(), ..Default::default() })
        .await
        .unwrap();
    // A query-only host: with no sink it collects nothing.
    let collect = MetricsConfig { collection_interval: None, access, ..Default::default() };
    let provider = Arc::new(NoTraces(MemoryMetrics::new(1 << 20)));
    MetricsService::with_providers(&system, &cluster, vec![], provider, collect).await.unwrap();
    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        // Refreshed every 100 ms, so the store is probed within the test.
        refresh_interval: Duration::from_millis(100),
        ..Default::default()
    };
    let dashboard = Dashboard::start(&system, &cluster, cfg).await.unwrap();
    let base = format!("http://{}/", dashboard.address());
    let get = async |path: &str| -> Json { reqwest::get(format!("{base}{path}")).await.unwrap().json().await.unwrap() };
    let idle = system.spawn(Idle).start().unwrap();
    let root = TraceContext::root();
    root.enter(|| idle.tell(()).unwrap());
    let id = format!("{:016x}", root.trace);
    eventually("the nodes list the trace, with no store error", || async {
        // A refresh probes the stores.
        dashboard.overview().await;
        let list = get("api/traces").await;
        let listed = list["traces"].as_array().is_some_and(|t| t.iter().any(|t| t["trace"] == *id));
        list["source"] == "live" && list["store_error"].is_null() && listed
    })
    .await;
    let held = get(&format!("api/trace?id={id}")).await;
    assert!(held["spans"][0]["kind"] == "handle" && held["errors"] == serde_json::json!({}), "{held}");
    system.shutdown().await.unwrap();
}

/// `carts` is registered on `b` alone, under leases, so `a`'s console places each of its keys there and asks `b`.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_lookup_names_the_owner_whether_its_actor_runs_and_the_epoch_it_started_under() {
    let (discovery, leases) = (MemoryDiscovery::new(), MemoryLeases::new());
    let leased = |id: &'static str| {
        let authority = Arc::new(leases.authority());
        common::loopback(id, discovery.clone(), ActorSystem::builder(), move |cfg| cfg.ownership = Some(authority))
    };
    let ((a_system, a), (b_system, b)) = (leased("a").await, leased("b").await);
    common::keyed(&b, "carts", ["cust-42".to_string()]).await;
    let epoch = b.hosted(1).keys[0].epoch.expect("a leased key has an epoch");
    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let dashboard = Dashboard::start(&a_system, &a, cfg).await.unwrap();
    let base = format!("http://{}/api/key", dashboard.address());
    let get = async |query: &str| {
        let answer = reqwest::get(format!("{base}?{query}")).await.unwrap();
        (answer.status().as_u16(), answer.json::<Json>().await.unwrap())
    };
    let running = serde_json::json!({ "owner": "b", "running": true, "epoch": epoch });
    eventually("a places cust-42 on b, which says it runs", || async {
        get("kind=carts&key=cust-42").await == (200, running.clone())
    })
    .await;
    let stopped = serde_json::json!({ "owner": "b", "running": false, "epoch": null });
    assert_eq!(get("kind=carts&key=cust-7").await, (200, stopped));
    assert_eq!(b.hosted(10).total, 1, "a lookup starts nothing");
    let nowhere = serde_json::json!({ "owner": null, "running": false, "epoch": null });
    assert_eq!(get("kind=carts&key=cust-42&label.zone=mars").await, (200, nowhere.clone()));
    assert_eq!(get("kind=boats&key=cust-42").await, (200, nowhere));
    assert_eq!(get("kind=carts").await.0, 400);
    drop((a, b));
    for system in [a_system, b_system] {
        system.shutdown().await.unwrap();
    }
}

/// `account` runs on `a` and `b`; `c` is a node from before `Request::Keys`, which refuses it as an unknown variant.
/// The console pages the keys of both in key order behind one cursor, searches them by prefix, asks one node alone,
/// and names `c` among the errors.
#[tokio::test(flavor = "multi_thread")]
async fn the_keys_of_a_kind_page_across_nodes_by_prefix_and_an_older_node_is_an_error() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (b_system, b)) = (node("a", &discovery).await, node("b", &discovery).await);
    let (c_system, c) = bare("c", &discovery).await;
    c.serve_control(TELEMETRY_SERVICE, AccessPolicy::Cluster, |request| {
        let refused = serde_json::from_slice::<Json>(&request.body).map_or(true, |r| r.get("keys").is_some());
        async move { if refused { Err("bad request: unknown variant `keys`".into()) } else { Err("old".into()) } }
    })
    .unwrap();
    common::keyed(&b, "account", []).await;
    eventually("a sees b host accounts", || async {
        a.view().members.len() == 3 && a.view().get("b").is_some_and(|m| m.record.has_kind("account"))
    })
    .await;
    common::keyed(&a, "account", (0..300).map(|i| format!("k-{i:03}"))).await;
    let on_b = b.hosted(0).total;
    assert!((50..250).contains(&on_b), "the keys spread over a and b: {on_b} on b");
    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let dashboard = Dashboard::start(&a_system, &a, cfg).await.unwrap();
    let base = format!("http://{}/api/keys", dashboard.address());
    let get = async |query: &str| {
        let answer = reqwest::get(format!("{base}?{query}")).await.unwrap();
        (answer.status().as_u16(), answer.json::<Json>().await.unwrap())
    };

    let (mut rows, mut query) = (Vec::new(), "kind=account&limit=64".to_string());
    loop {
        let (status, page) = get(&query).await;
        assert_eq!(status, 200, "{page}");
        assert!(page["errors"]["c"].as_str().is_some_and(|e| e.contains("unknown variant")), "{page}");
        let keys = page["keys"].as_array().unwrap();
        rows.extend(
            keys.iter().map(|k| (k["key"].as_str().unwrap().to_owned(), k["node"].as_str().unwrap().to_owned())),
        );
        let Some((key, node)) = rows.last().filter(|_| page["more"] == true) else { break };
        query = format!("kind=account&limit=64&after={key}&after_node={node}");
    }
    let keys: Vec<_> = rows.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(keys, (0..300).map(|i| format!("k-{i:03}")).collect::<Vec<_>>(), "each key once, in order");
    for (key, node) in &rows {
        assert_eq!(a.owner("account", key, &Placement::default()).as_deref(), Some(node.as_str()), "{key}");
    }

    let (_, found) = get("kind=account&prefix=k-1&limit=1000").await;
    let found: Vec<_> =
        found["keys"].as_array().unwrap().iter().map(|k| k["key"].as_str().unwrap().to_owned()).collect();
    assert_eq!(found, (100..200).map(|i| format!("k-{i:03}")).collect::<Vec<_>>());
    let (_, one) = get("kind=account&node_id=b&limit=1000").await;
    let one = one["keys"].as_array().unwrap();
    assert!(one.len() == on_b && one.iter().all(|k| k["node"] == "b"), "{one:?}");
    let (_, old) = get("kind=account&node_id=c").await;
    assert_eq!((old["keys"].as_array().unwrap().len(), old["errors"]["c"].is_string()), (0, true));
    assert_eq!(get("prefix=k").await.0, 400);
    assert_eq!(get("kind=account&limit=0").await.0, 400);
    drop((a, b, c));
    for system in [a_system, b_system, c_system] {
        system.shutdown().await.unwrap();
    }
}

/// `r` restarts under the same id, and its new boot does not answer the console: the page shows its error, not the
/// snapshot of the boot before.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_node_that_does_not_answer_is_not_shown_with_its_last_boots_snapshot() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (r_system, r)) = (node("a", &discovery).await, node("r", &discovery).await);
    eventually("membership converges", || async { a.view().members.len() == 2 }).await;
    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let dashboard = Dashboard::start(&a_system, &a, cfg).await.unwrap();
    let r_on = async || -> Json {
        let page: Json = serde_json::from_slice(&dashboard.overview().await).unwrap();
        page["nodes"].as_array().unwrap().iter().find(|n| n["node"]["id"] == "r").cloned().unwrap_or_default()
    };
    eventually("r's snapshot is on the page", || async { r_on().await["snapshot"].is_object() }).await;
    let first = format!("{:032x}", r.identity().incarnation);

    // The new boot registers over the old one, as after a crash, before the old one leaves.
    let (again_system, again) = bare("r", &discovery).await;
    eventually("the new boot is on the page, failing", || async {
        let r = r_on().await;
        r["node"]["incarnation"] != first.as_str() && r["error"].is_string()
    })
    .await;
    let shown = r_on().await;
    assert!(shown["snapshot"].is_null(), "{shown:#}");

    drop((r, again));
    for system in [r_system, again_system, a_system] {
        system.shutdown().await.unwrap();
    }
}

/// `b` leaves cleanly with nothing to drain, far faster than the console's node polls discovery: its departure still
/// carries its reason, so the console reads a leave, not a crash.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_leaves_faster_than_a_poll_is_seen_leaving_with_its_reason() {
    let discovery = MemoryDiscovery::new();
    // The shipped 1 s: the window a leave must leave its draining record up for, against the few ms this one takes.
    let shipped = |c: &mut axon_cluster::ClusterConfig| c.poll_interval = Duration::from_secs(1);
    let (a_system, a) = common::loopback("a", discovery.clone(), ActorSystem::builder(), shipped).await;
    let (b_system, b) = common::loopback("b", discovery.clone(), ActorSystem::builder(), shipped).await;
    eventually("a sees b", || async { a.view().get("b").is_some() }).await;
    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let dashboard = Dashboard::start(&a_system, &a, cfg).await.unwrap();
    let incarnation = format!("{:032x}", b.identity().incarnation);
    drop(b);
    b_system.shutdown().await.unwrap();
    // Without its reason, as after a crash, it would read "incarnation …" alone.
    logged(&dashboard, "left", &format!("Requested · incarnation {incarnation}")).await;
    a_system.shutdown().await.unwrap();
}

/// `h` never answers a snapshot and the cluster would wait longer for it than the console does: the page comes at the
/// console's deadline, with h's error on it.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_never_answers_holds_the_page_only_until_the_snapshot_deadline() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (h_system, h)) = (node("a", &discovery).await, bare("h", &discovery).await);
    h.serve_control(TELEMETRY_SERVICE, AccessPolicy::Cluster, |_| std::future::pending()).unwrap();
    eventually("membership converges", || async { a.view().members.len() == 2 }).await;
    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let dashboard = Dashboard::start(&a_system, &a, cfg).await.unwrap();
    let asked = tokio::time::Instant::now();
    let page: Json = serde_json::from_slice(&dashboard.overview().await).unwrap();
    let took = asked.elapsed();
    let h_node = page["nodes"].as_array().unwrap().iter().find(|n| n["node"]["id"] == "h").cloned().unwrap();
    assert!(h_node["error"].as_str().is_some_and(|e| e.contains("deadline")), "{h_node:#}");
    assert!(took < Duration::from_millis(3800), "the page took {took:?}");
    for system in [a_system, h_system] {
        system.shutdown().await.unwrap();
    }
}

/// `h` answers every snapshot with more than any response may carry: the console shows its error and every other node.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_past_the_response_bound_is_that_nodes_error_and_the_page_still_loads() {
    let discovery = MemoryDiscovery::new();
    let ((a_system, a), (h_system, h)) = (node("a", &discovery).await, bare("h", &discovery).await);
    let huge = bytes::Bytes::from(vec![b' '; 17 << 20]);
    h.serve_control(TELEMETRY_SERVICE, AccessPolicy::Cluster, move |_| {
        let huge = huge.clone();
        async move { Ok(huge) }
    })
    .unwrap();
    eventually("membership converges", || async { a.view().members.len() == 2 }).await;
    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let dashboard = Dashboard::start(&a_system, &a, cfg).await.unwrap();
    let overview = format!("http://{}/api/overview", dashboard.address());
    eventually("h's error and a's snapshot are on the page", || async {
        let page: Json = reqwest::get(&overview).await.unwrap().json().await.unwrap();
        let of = |id: &str| page["nodes"].as_array().unwrap().iter().find(|n| n["node"]["id"] == id).cloned();
        let (a, h) = (of("a").unwrap_or_default(), of("h").unwrap_or_default());
        a["snapshot"].is_object()
            && h["snapshot"].is_null()
            && h["error"].as_str().is_some_and(|e| e.contains("response too large"))
    })
    .await;
    for system in [a_system, h_system] {
        system.shutdown().await.unwrap();
    }
}
