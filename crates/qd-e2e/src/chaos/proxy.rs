//! A TCP proxy with a switchable backend set: stands in for a Kubernetes Service in front of
//! server replicas (round-robin per connection, endpoints added/removed during rollouts) and for
//! CloudNativePG's `-rw` Service in front of the current Postgres primary.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::task::AbortHandle;

#[derive(Clone)]
pub struct Proxy {
    pub addr: SocketAddr,
    inner: Arc<Inner>,
}

struct Inner {
    backends: Mutex<Vec<SocketAddr>>,
    next: AtomicUsize,
    connections: Mutex<Vec<AbortHandle>>,
    accept: Mutex<Option<AbortHandle>>,
}

impl Proxy {
    pub async fn start(backends: Vec<SocketAddr>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().unwrap();
        let inner = Arc::new(Inner {
            backends: Mutex::new(backends),
            next: AtomicUsize::new(0),
            connections: Mutex::new(Vec::new()),
            accept: Mutex::new(None),
        });
        let accept_inner = inner.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else { continue };
                let backend = {
                    let backends = accept_inner.backends.lock().unwrap();
                    if backends.is_empty() {
                        None
                    } else {
                        let i = accept_inner.next.fetch_add(1, Ordering::Relaxed);
                        Some(backends[i % backends.len()])
                    }
                };
                let Some(backend) = backend else {
                    drop(client); // no endpoints: connection reset, like a Service with no ready pods
                    continue;
                };
                let handle = tokio::spawn(async move {
                    let mut client = client;
                    if let Ok(mut server) = TcpStream::connect(backend).await {
                        let _ = client.set_nodelay(true);
                        let _ = server.set_nodelay(true);
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                    }
                });
                let mut conns = accept_inner.connections.lock().unwrap();
                conns.retain(|h| !h.is_finished());
                conns.push(handle.abort_handle());
            }
        });
        *inner.accept.lock().unwrap() = Some(task.abort_handle());
        Self { addr, inner }
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn set_backends(&self, backends: Vec<SocketAddr>) {
        *self.inner.backends.lock().unwrap() = backends;
    }

    pub fn add_backend(&self, backend: SocketAddr) {
        self.inner.backends.lock().unwrap().push(backend);
    }

    pub fn remove_backend(&self, backend: SocketAddr) {
        self.inner.backends.lock().unwrap().retain(|b| *b != backend);
    }

    /// Stops accepting and drops all connections.
    pub fn shutdown(&self) {
        if let Some(h) = self.inner.accept.lock().unwrap().take() {
            h.abort();
        }
        self.reset_connections();
    }

    /// Drops every proxied connection (e.g. the old primary is gone).
    pub fn reset_connections(&self) {
        for h in self.inner.connections.lock().unwrap().drain(..) {
            h.abort();
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(h) = self.accept.lock().unwrap().take() {
            h.abort();
        }
        for h in self.connections.lock().unwrap().drain(..) {
            h.abort();
        }
    }
}
