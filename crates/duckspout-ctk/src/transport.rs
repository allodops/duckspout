//! The in-memory network: the [`Transport`] port's deterministic double,
//! with fault-injection points accounted by the [`InjectorLedger`].

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use duckspout_types::{BoxFuture, NodeId, Transport, TransportError};

use crate::ledger::InjectorLedger;

/// One directed link's fault state.
#[derive(Debug, Clone, Copy, Default)]
struct LinkFaults {
    /// Drop every message on this link (a network partition, one direction).
    blackhole: bool,
}

struct NetInner {
    mailboxes: HashMap<NodeId, VecDeque<(NodeId, Bytes)>>,
    links: HashMap<(NodeId, NodeId), LinkFaults>,
}

/// The shared in-memory network. Endpoints are created per node; faults are
/// injected per directed link and accounted armed-vs-fired (§8.3).
pub struct InMemNetwork {
    inner: Arc<Mutex<NetInner>>,
    ledger: Arc<InjectorLedger>,
}

fn blackhole_fault_id(from: &NodeId, to: &NodeId) -> String {
    format!("net:blackhole:{from}->{to}")
}

impl InMemNetwork {
    /// A network accounting its faults in `ledger`.
    #[must_use]
    pub fn new(ledger: Arc<InjectorLedger>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(NetInner {
                mailboxes: HashMap::new(),
                links: HashMap::new(),
            })),
            ledger,
        }
    }

    /// Creates (or reattaches) the endpoint for `node`, registering its
    /// mailbox.
    #[must_use]
    pub fn endpoint(&self, node: NodeId) -> InMemTransport {
        let mut inner = self.inner.lock().expect("network lock");
        inner.mailboxes.entry(node.clone()).or_default();
        InMemTransport {
            node,
            inner: Arc::clone(&self.inner),
            ledger: Arc::clone(&self.ledger),
        }
    }

    /// Arms a one-directional blackhole: every `from → to` message is
    /// silently dropped until [`InMemNetwork::heal`]. Fault id:
    /// `net:blackhole:<from>-><to>`.
    pub fn blackhole(&self, from: &NodeId, to: &NodeId) {
        self.ledger.arm(&blackhole_fault_id(from, to));
        let mut inner = self.inner.lock().expect("network lock");
        inner
            .links
            .entry((from.clone(), to.clone()))
            .or_default()
            .blackhole = true;
    }

    /// Clears the `from → to` blackhole.
    pub fn heal(&self, from: &NodeId, to: &NodeId) {
        let mut inner = self.inner.lock().expect("network lock");
        if let Some(link) = inner.links.get_mut(&(from.clone(), to.clone())) {
            link.blackhole = false;
        }
    }
}

/// One node's endpoint on an [`InMemNetwork`].
pub struct InMemTransport {
    node: NodeId,
    inner: Arc<Mutex<NetInner>>,
    ledger: Arc<InjectorLedger>,
}

impl Transport for InMemTransport {
    fn send(&self, to: NodeId, payload: Bytes) -> BoxFuture<'_, Result<(), TransportError>> {
        let result = {
            let mut inner = self.inner.lock().expect("network lock");
            let dropped = inner
                .links
                .get(&(self.node.clone(), to.clone()))
                .is_some_and(|link| link.blackhole);
            if dropped {
                // Silent to the sender, as on a real network (the port's
                // contract): Ok, but nothing is delivered.
                self.ledger.fired(&blackhole_fault_id(&self.node, &to));
                Ok(())
            } else if let Some(mailbox) = inner.mailboxes.get_mut(&to) {
                mailbox.push_back((self.node.clone(), payload));
                Ok(())
            } else {
                Err(TransportError::UnknownPeer(to))
            }
        };
        Box::pin(async move { result })
    }

    fn recv(&self) -> BoxFuture<'_, Result<(NodeId, Bytes), TransportError>> {
        Box::pin(RecvFuture {
            node: self.node.clone(),
            inner: Arc::clone(&self.inner),
        })
    }
}

struct RecvFuture {
    node: NodeId,
    inner: Arc<Mutex<NetInner>>,
}

impl Future for RecvFuture {
    type Output = Result<(NodeId, Bytes), TransportError>;

    fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.lock().expect("network lock");
        match inner.mailboxes.get_mut(&self.node) {
            Some(mailbox) => match mailbox.pop_front() {
                Some(message) => Poll::Ready(Ok(message)),
                None => Poll::Pending,
            },
            None => Poll::Ready(Err(TransportError::Closed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn poll_once<T>(future: &mut BoxFuture<'_, T>) -> Poll<T> {
        let mut context = Context::from_waker(std::task::Waker::noop());
        future.as_mut().poll(&mut context)
    }

    #[test]
    fn delivers_between_endpoints_and_drops_on_blackhole() {
        let ledger = Arc::new(InjectorLedger::new());
        let net = InMemNetwork::new(Arc::clone(&ledger));
        let a = NodeId::new("a");
        let b = NodeId::new("b");
        let ta = net.endpoint(a.clone());
        let tb = net.endpoint(b.clone());

        assert!(matches!(
            poll_once(&mut ta.send(b.clone(), Bytes::from_static(b"one"))),
            Poll::Ready(Ok(()))
        ));
        match poll_once(&mut tb.recv()) {
            Poll::Ready(Ok((from, payload))) => {
                assert_eq!(from, a);
                assert_eq!(payload, Bytes::from_static(b"one"));
            }
            other => panic!("expected delivery, got {other:?}"),
        }

        net.blackhole(&a, &b);
        assert!(matches!(
            poll_once(&mut ta.send(b.clone(), Bytes::from_static(b"two"))),
            Poll::Ready(Ok(())),
        ));
        assert!(matches!(poll_once(&mut tb.recv()), Poll::Pending));
        assert!(ledger.vacuously_armed().is_empty(), "the blackhole fired");
    }
}
