use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct BufferPool {
    slab_size: usize,
    semaphore: Arc<Semaphore>,
    free_list: Arc<Mutex<Vec<Vec<u8>>>>,
}

pub struct PooledBuffer {
    buf: Option<Vec<u8>>,
    pool: BufferPool,
    _permit: OwnedSemaphorePermit,
}

impl BufferPool {
    pub fn new(slab_size: usize, max_slabs: usize) -> Self {
        let max_slabs = max_slabs.max(1);
        Self {
            slab_size,
            semaphore: Arc::new(Semaphore::new(max_slabs)),
            free_list: Arc::new(Mutex::new(Vec::with_capacity(max_slabs.min(64)))),
        }
    }

    pub async fn acquire(&self, size: usize) -> PooledBuffer {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("BufferPool semaphore never closes");
        let mut buf = self
            .free_list
            .lock()
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(size.max(self.slab_size)));
        buf.resize(size, 0);
        PooledBuffer {
            buf: Some(buf),
            pool: self.clone(),
            _permit: permit,
        }
    }

    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

impl PooledBuffer {
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buf.as_mut().unwrap().as_mut_slice()
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.buf.as_ref().unwrap().as_slice()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(mut b) = self.buf.take() {
            if b.capacity() <= self.pool.slab_size * 2 {
                b.clear();
                let mut list = self.pool.free_list.lock();
                if list.len() < 128 {
                    list.push(b);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_buffer_pool_reuse() {
        let pool = BufferPool::new(4096, 4);
        assert_eq!(pool.available_permits(), 4);
        {
            let mut b1 = pool.acquire(1024).await;
            b1.as_mut_slice()[0] = 42;
            assert_eq!(pool.available_permits(), 3);
        }
        assert_eq!(pool.available_permits(), 4);
        let b2 = pool.acquire(2048).await;
        assert_eq!(b2.as_slice().len(), 2048);
    }
}
