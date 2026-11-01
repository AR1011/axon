//! What a node shows outside itself: the Health port with `/metrics` (HEALTH_PORT, 7401), which ops/probe.py polls,
//! and in TLS mode (TLS_DIR) its certificates, read from `TLS_DIR/current`, the pass run.sh links, at the start and
//! again on each SIGHUP (spec/fault-rig.md § Workloads).

use std::path::PathBuf;
use std::sync::Arc;

use axon::ActorSystem;
use axon_cluster::{Cluster, SeedDiscovery, SeedServer, Tls};
use axon_net::rustls::crypto::ring::default_provider;
use axon_net::rustls::pki_types::pem::PemObject;
use axon_net::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use axon_net::rustls::server::WebPkiClientVerifier;
use axon_net::rustls::{ClientConfig, RootCertStore, ServerConfig};
use axon_obs::{Health, Telemetry, TelemetryConfig};
use serde_json::json;

use crate::account::Shared;

/// Started after the `up` line, so a scrape check.py places after an `up` came from that incarnation. A port that
/// does not open is logged, not fatal: the probes then show it.
pub async fn health(system: &ActorSystem, cluster: &Cluster, sh: &Shared, ip: &str) {
    let addr = format!("{ip}:{}", crate::env("HEALTH_PORT", "7401"));
    let started = async {
        let health = Health::start(system, cluster, addr.parse().map_err(|e| format!("{e}"))?).await?;
        health.serve_metrics(&Telemetry::start(system, cluster, TelemetryConfig::default()).await?);
        Ok::<_, String>(())
    };
    let err = started.await.err();
    sh.node_log.line("health", json!({"addr": addr, "err": err}));
}

pub struct Certs {
    dir: PathBuf,
    id: String,
}

impl Certs {
    pub fn from_env(id: &str) -> Option<Self> {
        std::env::var("TLS_DIR").ok().map(|dir| Certs { dir: dir.into(), id: id.to_owned() })
    }

    /// The pass `current` links to, and mutual TLS from its `ca.pem` (every CA trusted) and `<id>.crt`/`<id>.key`.
    pub fn load(&self) -> Result<(String, Tls), String> {
        let pass = std::fs::read_link(self.dir.join("current")).map_err(|e| format!("current: {e}"))?;
        // Every file from the pass the link named once, so a rotation mid-load never pairs one pass's key with another's.
        let dir = self.dir.join(&pass);
        let pass = pass.to_string_lossy().into_owned();
        let read = |f: &str| std::fs::read(dir.join(f)).map_err(|e| format!("{f}: {e}"));
        let chain = CertificateDer::pem_slice_iter(&read(&format!("{}.crt", self.id))?)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("crt: {e:?}"))?;
        let key =
            PrivateKeyDer::from_pem_slice(&read(&format!("{}.key", self.id))?).map_err(|e| format!("key: {e:?}"))?;
        let mut roots = RootCertStore::empty();
        for ca in CertificateDer::pem_slice_iter(&read("ca.pem")?) {
            roots.add(ca.map_err(|e| format!("ca: {e:?}"))?).map_err(|e| format!("ca: {e}"))?;
        }
        let roots = Arc::new(roots);
        let provider = Arc::new(default_provider());
        let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
            .build()
            .map_err(|e| format!("verifier: {e}"))?;
        let server = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .and_then(|b| b.with_client_cert_verifier(verifier).with_single_cert(chain.clone(), key.clone_key()))
            .map_err(|e| format!("server: {e}"))?;
        let client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .and_then(|b| b.with_root_certificates(roots).with_client_auth_cert(chain, key))
            .map_err(|e| format!("client: {e}"))?;
        Ok((pass, Tls { server: Arc::new(server), client: Arc::new(client) }))
    }

    /// A SIGHUP: the pass `current` links to now, set on the cluster, the seed links and this node's seed server. Logs
    /// `tls {pass, err, at: hup}`; each set is tried even when one before it failed.
    pub fn reload(&self, cluster: &Cluster, discovery: &SeedDiscovery, seed: Option<&SeedServer>, sh: &Shared) {
        let (pass, errs) = match self.load() {
            Ok((pass, tls)) => {
                let errs: Vec<String> = [
                    cluster.set_tls(tls.clone()).err().map(|e| format!("cluster: {e}")),
                    discovery.set_tls(tls.clone()).err().map(|e| format!("seeds: {e}")),
                    seed.and_then(|s| s.set_tls(tls).err()).map(|e| format!("server: {e}")),
                ]
                .into_iter()
                .flatten()
                .collect();
                (Some(pass), errs)
            }
            Err(e) => (None, vec![e]),
        };
        let err = (!errs.is_empty()).then(|| errs.join("; "));
        sh.node_log.line("tls", json!({"pass": pass, "err": err, "at": "hup"}));
    }
}
