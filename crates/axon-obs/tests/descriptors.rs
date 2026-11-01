//! A process out of file descriptors: the probe and console ports keep the connections they have, refuse none for
//! good, and take new ones once descriptors free up. The test runs itself again under `ulimit -n 256`.
#![cfg(feature = "dashboard")]

use std::time::Duration;

use axon::ActorSystem;
use axon_cluster::MemoryDiscovery;
use axon_obs::{Dashboard, DashboardConfig, Health, Telemetry, TelemetryConfig};
#[cfg(feature = "clickhouse")]
use axon_obs::{MetricsSink, Sample, SampleBatch, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod common;

const LIMITED: &str = "AXON_DESCRIPTORS_LIMITED";

/// Where the first HTTP message in `bytes` ends, once all of it is there.
fn message_end(bytes: &[u8]) -> Option<usize> {
    let head = bytes.windows(4).position(|w| w == b"\r\n\r\n")?;
    let length = (String::from_utf8_lossy(&bytes[..head]).lines())
        .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse().unwrap()))
        .unwrap_or(0usize);
    Some(head + 4 + length).filter(|end| bytes.len() >= *end)
}

/// Reads until a whole message is in.
async fn read_message(connection: &mut TcpStream) -> Vec<u8> {
    let (mut message, mut buf) = (Vec::new(), vec![0; 64 << 10]);
    while message_end(&message).is_none() {
        let n = connection.read(&mut buf).await.unwrap();
        assert!(n > 0, "the connection closed");
        message.extend_from_slice(&buf[..n]);
    }
    message
}

/// One request on a connection kept open, and the status of its answer.
async fn ask(connection: &mut TcpStream, path: &str) -> u16 {
    let request = format!("GET {path} HTTP/1.1\r\nHost: axon\r\n\r\n");
    connection.write_all(request.as_bytes()).await.unwrap();
    String::from_utf8_lossy(&read_message(connection).await[9..12]).parse().unwrap()
}

/// Says yes to every statement, as a ClickHouse with nothing to report does.
#[cfg(feature = "clickhouse")]
async fn agreeable() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut peer, _)) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            tokio::spawn(async move {
                read_message(&mut peer).await;
                let _ = peer.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
            });
        }
    });
    address
}

#[tokio::test(flavor = "multi_thread")]
async fn probes_and_the_console_outlive_a_process_out_of_descriptors() {
    if std::env::var_os(LIMITED).is_none() {
        let test = "probes_and_the_console_outlive_a_process_out_of_descriptors";
        let run = std::process::Command::new("sh")
            .args(["-c", "ulimit -n 256 && exec \"$0\" --exact \"$1\" --nocapture"])
            .arg(std::env::current_exe().unwrap())
            .arg(test)
            .env(LIMITED, "1")
            .output()
            .unwrap();
        let said = String::from_utf8_lossy(&run.stdout) + String::from_utf8_lossy(&run.stderr);
        assert!(run.status.success() && said.contains("1 passed"), "{said}");
        return;
    }

    let (system, cluster) = common::loopback("a", MemoryDiscovery::new(), ActorSystem::builder(), |_| {}).await;
    let health = Health::start(&system, &cluster, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    health.serve_metrics(&Telemetry::start(&system, &cluster, TelemetryConfig::default()).await.unwrap());
    let cfg = DashboardConfig { bind: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let dashboard = Dashboard::start(&system, &cluster, cfg).await.unwrap();
    let mut probes = TcpStream::connect(health.address()).await.unwrap();
    let mut console = TcpStream::connect(dashboard.address()).await.unwrap();
    assert_eq!(ask(&mut probes, "/readyz").await, 200);
    assert_eq!(ask(&mut console, "/api/overview").await, 200);
    #[cfg(feature = "clickhouse")]
    let (sink, batch) = {
        let cfg = axon_obs::ClickHouseConfig { url: format!("http://{}", agreeable().await), ..Default::default() };
        let sample = Sample { name: "t".into(), unit: "".into(), labels: Default::default(), value: Value::Gauge(1.0) };
        let batch = SampleBatch {
            node_id: "a".into(),
            incarnation: "00ff".into(),
            sequence: 1,
            timestamp_ms: 1,
            samples: vec![sample],
        };
        (axon_obs::ClickHouseProvider::connect(cfg).await.unwrap(), [batch])
    };

    // The new connections' sockets are made first, so no descriptor is ever free for a port's accept, which retries,
    // to take before a connection could.
    let sockets = [(); 2].map(|()| tokio::net::TcpSocket::new_v4().unwrap());
    let mut held = Vec::new();
    let full = loop {
        match std::fs::File::open("/dev/null") {
            Ok(file) => held.push(file),
            Err(error) => break error,
        }
    };
    assert_eq!(full.raw_os_error(), Some(24), "{full}");
    let mut waiting = Vec::new();
    for (socket, address) in sockets.into_iter().zip([health.address(), dashboard.address()]) {
        waiting.push(socket.connect(address).await.unwrap());
    }
    for _ in 0..3 {
        assert_eq!(ask(&mut probes, "/readyz").await, 200);
        assert_eq!(ask(&mut probes, "/healthz").await, 200);
        assert_eq!(ask(&mut probes, "/metrics").await, 200);
        assert_eq!(ask(&mut console, "/readyz").await, 200);
        assert_eq!(ask(&mut console, "/api/overview").await, 200);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    #[cfg(feature = "clickhouse")]
    {
        let error = tokio::time::timeout(Duration::from_secs(4), sink.write(&batch)).await.unwrap().unwrap_err();
        assert!(error.starts_with("ClickHouse"), "{error}");
    }

    drop(held);
    for connection in &mut waiting {
        let answered = tokio::time::timeout(Duration::from_secs(5), ask(connection, "/readyz"));
        assert_eq!(answered.await, Ok(200), "a connection that waited is taken once descriptors free up");
    }
    for address in [health.address(), dashboard.address()] {
        assert_eq!(reqwest::get(format!("http://{address}/readyz")).await.unwrap().status(), 200);
    }
    #[cfg(feature = "clickhouse")]
    sink.write(&batch).await.unwrap();
    system.shutdown().await.unwrap();
}
