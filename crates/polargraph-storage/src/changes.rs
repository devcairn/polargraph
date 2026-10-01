//! Change log for the `Subscribe` feed (`docs/design/change-feed.md`).
//!
//! Every commit that writes quads or changes a graph also writes one entry to
//! the `chg` column family, in the same `WriteBatch` — so the log is exactly as
//! durable as the data and replicates with it. Key: `[commit_ts BE(8)]`.
//! Value (version 1):
//!
//! ```text
//! [1u8]
//! [author_len u16][author utf8]
//! [n_ops u16]   n × [kind u8][graph u32][source u32]        kind: 1 created, 2 dropped, 3 copied
//! [n_quads u32] n × [s 16][p u32][o 16][g u32][len u32][value bytes]
//! ```
//!
//! Quad values are the codec bytes written to the quad index (out-of-line
//! values stay references into the `blob` CF), so decoding reuses the normal
//! read path. Bulk SST import, migrations, OWL materialization and retention
//! deletes don't go through commits and aren't logged.

use polargraph_core::{id::GraphId, id::NodeId, temporal::Timestamp, triple::Triple};
use rocksdb::{Direction, IteratorMode, WriteBatch};

use crate::{cf, error::StorageError, keys::QuadKey, store::TripleStore};

const VERSION: u8 = 1;

/// META key: commits before this transaction time are not (or no longer) in
/// the change log — set when the log starts and advanced by pruning.
const META_CHANGES_FLOOR: &[u8] = b"__changes__/floor";

/// A graph-level operation recorded alongside a commit's quads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphOp {
    Created(GraphId),
    Dropped(GraphId),
    /// Quads of `source` were copied into `target` (ADD / COPY / MOVE).
    Copied {
        source: GraphId,
        target: GraphId,
    },
}

/// One logged commit.
#[derive(Debug, Clone)]
pub struct ChangeRecord {
    pub commit_ts: Timestamp,
    /// The caller's user id, empty for service calls.
    pub author: String,
    pub graph_ops: Vec<GraphOp>,
    /// Quad versions written by the commit (asserted, or closed when
    /// `vt_end` is set), each with its graph.
    pub quads: Vec<(GraphId, Triple)>,
}

/// A quad version staged in a commit, as written to the quad index.
pub(crate) struct StagedQuad {
    pub key: QuadKey,
    pub value: Vec<u8>,
}

impl TripleStore {
    /// Add the change-log entry for a commit at `tt` to `batch` (nothing when
    /// the commit has no quads and no graph operations).
    pub(crate) fn batch_change(
        &self,
        batch: &mut WriteBatch,
        tt: Timestamp,
        author: &str,
        ops: &[GraphOp],
        quads: &[StagedQuad],
    ) -> Result<(), StorageError> {
        if ops.is_empty() && quads.is_empty() {
            return Ok(());
        }
        let author = &author.as_bytes()[..author.len().min(u16::MAX as usize)];
        let mut out = Vec::with_capacity(
            8 + author.len()
                + ops.len() * 9
                + quads.iter().map(|q| 60 + q.value.len()).sum::<usize>(),
        );
        out.push(VERSION);
        out.extend_from_slice(&(author.len() as u16).to_be_bytes());
        out.extend_from_slice(author);
        out.extend_from_slice(&(ops.len() as u16).to_be_bytes());
        for op in ops {
            let (kind, g, source) = match *op {
                GraphOp::Created(g) => (1u8, g, GraphId::DEFAULT),
                GraphOp::Dropped(g) => (2, g, GraphId::DEFAULT),
                GraphOp::Copied { source, target } => (3, target, source),
            };
            out.push(kind);
            out.extend_from_slice(&g.0.to_be_bytes());
            out.extend_from_slice(&source.0.to_be_bytes());
        }
        out.extend_from_slice(&(quads.len() as u32).to_be_bytes());
        for q in quads {
            out.extend_from_slice(q.key.s.as_bytes());
            out.extend_from_slice(&q.key.p.to_be_bytes());
            out.extend_from_slice(q.key.o.as_bytes());
            out.extend_from_slice(&q.key.g.0.to_be_bytes());
            out.extend_from_slice(&(q.value.len() as u32).to_be_bytes());
            out.extend_from_slice(&q.value);
        }
        batch.put_cf(&self.cf_handle(cf::CHG)?, tt.0.to_be_bytes(), out);
        Ok(())
    }

    /// Up to `limit` logged commits with `commit_ts > after`, oldest first.
    pub fn changes_after(
        &self,
        after: Timestamp,
        limit: usize,
    ) -> Result<Vec<ChangeRecord>, StorageError> {
        let cf = self.cf_handle(cf::CHG)?;
        let start = after.0.saturating_add(1).max(0).to_be_bytes();
        let mut out = Vec::new();
        for item in self
            .db_ref()
            .iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward))
        {
            if out.len() >= limit {
                break;
            }
            let (key, value) = item?;
            let ts = Timestamp(i64::from_be_bytes(key[..8].try_into().map_err(|_| {
                StorageError::KeyDecode("change-log key must be 8 bytes".into())
            })?));
            out.push(self.decode_change(ts, &value)?);
        }
        Ok(out)
    }

    /// The oldest point a subscriber can resume from: commits at or before
    /// this were never logged or have been pruned.
    pub fn changes_floor(&self) -> Result<Timestamp, StorageError> {
        let meta = self.cf_handle(cf::META)?;
        Ok(match self.db_ref().get_cf(&meta, META_CHANGES_FLOOR)? {
            Some(v) if v.len() == 8 => Timestamp(i64::from_be_bytes(v[..8].try_into().unwrap())),
            _ => Timestamp(0),
        })
    }

    /// Start the change log at the current oracle time if it hasn't started
    /// (first open after upgrade, or a new store). Primary only.
    pub(crate) fn init_changes_floor(&self) -> Result<(), StorageError> {
        let meta = self.cf_handle(cf::META)?;
        if self.db_ref().get_cf(&meta, META_CHANGES_FLOOR)?.is_none() {
            self.db_ref()
                .put_cf(&meta, META_CHANGES_FLOOR, self.oracle_ts().to_be_bytes())?;
        }
        Ok(())
    }

    /// Delete change-log entries committed before `before` and raise the
    /// floor. Returns the number of entries removed.
    pub fn prune_changes(&self, before: Timestamp) -> Result<usize, StorageError> {
        if self.is_replica() {
            return Err(StorageError::ReadOnly(
                "change-log pruning runs on the primary".into(),
            ));
        }
        let cf = self.cf_handle(cf::CHG)?;
        let end = before.0.max(0).to_be_bytes();
        let mut n = 0;
        for item in self.db_ref().iterator_cf(&cf, IteratorMode::Start) {
            let (key, _) = item?;
            if key.as_ref() >= &end[..] {
                break;
            }
            n += 1;
        }
        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&cf, 0i64.to_be_bytes(), end);
        if before > self.changes_floor()? {
            batch.put_cf(
                &self.cf_handle(cf::META)?,
                META_CHANGES_FLOOR,
                (before.0 - 1).to_be_bytes(),
            );
        }
        self.db_ref().write(batch)?;
        Ok(n)
    }

    fn decode_change(&self, commit_ts: Timestamp, v: &[u8]) -> Result<ChangeRecord, StorageError> {
        let bad =
            || StorageError::KeyDecode(format!("corrupt change-log entry at {}", commit_ts.0));
        let mut r = Reader { buf: v, pos: 0 };
        if r.u8().ok_or_else(bad)? != VERSION {
            return Err(StorageError::KeyDecode(format!(
                "unsupported change-log version at {}",
                commit_ts.0
            )));
        }
        let author_len = r.u16().ok_or_else(bad)? as usize;
        let author =
            String::from_utf8(r.bytes(author_len).ok_or_else(bad)?.to_vec()).map_err(|_| bad())?;
        let n_ops = r.u16().ok_or_else(bad)?;
        let mut graph_ops = Vec::with_capacity(n_ops as usize);
        for _ in 0..n_ops {
            let kind = r.u8().ok_or_else(bad)?;
            let g = GraphId(r.u32().ok_or_else(bad)?);
            let source = GraphId(r.u32().ok_or_else(bad)?);
            graph_ops.push(match kind {
                1 => GraphOp::Created(g),
                2 => GraphOp::Dropped(g),
                3 => GraphOp::Copied { source, target: g },
                _ => return Err(bad()),
            });
        }
        let n_quads = r.u32().ok_or_else(bad)?;
        let mut quads = Vec::with_capacity(n_quads as usize);
        for _ in 0..n_quads {
            let s = r.node().ok_or_else(bad)?;
            let p = r.u32().ok_or_else(bad)?;
            let o = r.node().ok_or_else(bad)?;
            let g = GraphId(r.u32().ok_or_else(bad)?);
            let len = r.u32().ok_or_else(bad)? as usize;
            let value = r.bytes(len).ok_or_else(bad)?;
            quads.push((g, self.reconstruct(s, p, o, commit_ts, value)?));
        }
        Ok(ChangeRecord {
            commit_ts,
            author,
            graph_ops,
            quads,
        })
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let out = self.buf.get(self.pos..self.pos.checked_add(n)?)?;
        self.pos += n;
        Some(out)
    }
    fn u8(&mut self) -> Option<u8> {
        self.bytes(1).map(|b| b[0])
    }
    fn u16(&mut self) -> Option<u16> {
        self.bytes(2).map(|b| u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Option<u32> {
        self.bytes(4)
            .map(|b| u32::from_be_bytes(b.try_into().unwrap()))
    }
    fn node(&mut self) -> Option<NodeId> {
        self.bytes(16)
            .map(|b| NodeId(uuid::Uuid::from_bytes(b.try_into().unwrap())))
    }
}
