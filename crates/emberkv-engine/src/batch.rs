use crate::types::Expiry;

/// A group of writes applied atomically by
/// [`Engine::write_batch`](crate::Engine::write_batch): after a crash either
/// every operation is visible or none is, and concurrent readers never
/// observe a partially applied batch.
///
/// ```
/// use emberkv_engine::{Engine, Options, WriteBatch};
///
/// # fn main() -> emberkv_engine::Result<()> {
/// # let dir = tempfile::tempdir()?;
/// let db = Engine::open(dir.path(), Options::default())?;
/// let mut batch = WriteBatch::new();
/// batch.put("order:7", "paid").delete("cart:42").put("outbox:7", "order:7");
/// db.write_batch(&batch)?;
/// assert_eq!(db.get(b"order:7")?.as_deref(), Some(&b"paid"[..]));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    pub(crate) ops: Vec<BatchOp>,
}

#[derive(Debug, Clone)]
pub(crate) enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8>, expiry: Expiry },
    Delete { key: Vec<u8> },
}

impl WriteBatch {
    /// Creates an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a put that never expires.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> &mut Self {
        self.put_with_expiry(key, value, Expiry::Never)
    }

    /// Adds a put with an expiry. [`Expiry::KeepExisting`] refers to the
    /// state before the batch, not to earlier operations in the same batch.
    pub fn put_with_expiry(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        expiry: Expiry,
    ) -> &mut Self {
        self.ops.push(BatchOp::Put { key: key.into(), value: value.into(), expiry });
        self
    }

    /// Adds a delete.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.ops.push(BatchOp::Delete { key: key.into() });
        self
    }

    /// Number of operations.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether the batch has no operations.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}
