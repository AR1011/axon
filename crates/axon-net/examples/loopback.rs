//! Same-process TCP loopback: one-way throughput, then sequential echo RTT.
//! usage: loopback [frames] [payload bytes]
#![expect(clippy::disallowed_methods, reason = "a benchmark measures real time")]
use std::time::Instant;

use axon_net::*;
use bytes::Bytes;

fn config(name: &str, codec: Compression) -> Config {
    let mut cfg = Config::new(name, SharedSecret::new("local-benchmark-only").unwrap());
    cfg.allow_plaintext = true;
    cfg.lanes.control.compression = codec;
    cfg.compression_threshold = 0;
    cfg.compression_min_savings = 1;
    cfg
}

async fn sessions(codec: Compression) -> (Session, Session) {
    let any = "127.0.0.1:0".parse().unwrap();
    let mut listener = Listener::bind(any, None, config("server", codec), AcceptLimits::default()).await.unwrap();
    let client = connect(listener.local_addr(), Lane::Control, None, &config("client", codec)).await.unwrap();
    (client, listener.accept().await.unwrap())
}

async fn throughput(codec: Compression, payload: Bytes, frames: u64) -> (f64, f64) {
    let (client, mut server) = sessions(codec).await;
    let receiving = tokio::spawn(async move {
        for id in 0..frames {
            assert_eq!(server.receiver.recv().await.unwrap().id, id);
        }
        server
    });
    let started = Instant::now();
    for id in 0..frames {
        let frame = Frame { id, payload: payload.clone(), ..Default::default() };
        client.sender.submit(frame, None).await.unwrap();
    }
    client.sender.flush().await.unwrap();
    let _server = receiving.await.unwrap();
    let stats = client.sender.stats();
    (started.elapsed().as_secs_f64(), stats.tx_wire_bytes as f64 / stats.tx_payload_bytes as f64)
}

async fn roundtrip(codec: Compression, payload: Bytes, frames: usize) -> (f64, f64) {
    let (mut client, mut server) = sessions(codec).await;
    tokio::spawn(async move {
        while let Ok(got) = server.receiver.recv().await {
            let _ = server.sender.submit(got.into_frame(), None).await;
        }
    });
    let mut samples = Vec::with_capacity(frames);
    for id in 0..frames as u64 {
        let started = Instant::now();
        client.sender.submit(Frame { id, payload: payload.clone(), ..Default::default() }, None).await.unwrap();
        assert_eq!(client.receiver.recv().await.unwrap().id, id);
        samples.push(started.elapsed().as_nanos() as f64 / 1e3);
    }
    samples.sort_unstable_by(f64::total_cmp);
    (samples[frames / 2], samples[(frames - 1) * 99 / 100])
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1).map(|a| a.parse::<usize>().expect("number"));
    let (frames, size) = (args.next().unwrap_or(200_000), args.next().unwrap_or(256));
    println!("pattern       codec    frames/s    payload MB/s  wire/payload  RTT p50 us  RTT p99 us");
    for pattern in ["repeated", "pseudorandom"] {
        let mut state = 0x1234_5678_u32;
        let payload: Bytes = (0..size)
            .map(|i| {
                if pattern == "repeated" {
                    return (i % 16) as u8;
                }
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect();
        for codec in [Compression::None, Compression::Lz4, Compression::Zstd] {
            let (elapsed, ratio) = throughput(codec, payload.clone(), frames as u64).await;
            let (p50, p99) = roundtrip(codec, payload.clone(), frames.clamp(100, 10_000)).await;
            println!(
                "{pattern:<13} {:<6} {:>11.0} {:>15.1} {ratio:>13.3} {p50:>11.1} {p99:>11.1}",
                format!("{codec:?}"),
                frames as f64 / elapsed,
                (frames * size) as f64 / elapsed / 1e6,
            );
        }
    }
}
