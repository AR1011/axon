#![allow(dead_code, reason = "each net test file uses part of it")]
use std::sync::Arc;

use axon_net::rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName};
use axon_net::rustls::{ClientConfig, RootCertStore, ServerConfig};
use axon_net::{ClientTls, Config, SharedSecret};

pub fn config(node: &str) -> Config {
    let mut cfg = Config::new(node, SharedSecret::new("a-cluster-wide-secret").unwrap());
    cfg.allow_plaintext = true;
    cfg
}

pub struct Identity {
    pub cert: CertificateDer<'static>,
    pub key: PrivatePkcs8KeyDer<'static>,
}

pub fn identity(name: &str) -> Identity {
    let made = rcgen::generate_simple_self_signed(vec![name.to_owned()]).unwrap();
    Identity { cert: made.cert.der().clone(), key: PrivatePkcs8KeyDer::from(made.key_pair.serialize_der()) }
}

pub fn trust(cert: &CertificateDer<'static>) -> Arc<RootCertStore> {
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    Arc::new(roots)
}

/// `name` serves `id` and a dialler trusts it, presenting no certificate of its own.
pub fn one_way(id: &Identity, name: &'static str) -> (Arc<ServerConfig>, ClientTls) {
    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![id.cert.clone()], id.key.clone_key().into());
    let client = ClientConfig::builder().with_root_certificates(trust(&id.cert)).with_no_client_auth();
    (Arc::new(server.unwrap()), ClientTls { name: ServerName::try_from(name).unwrap(), config: Arc::new(client) })
}
