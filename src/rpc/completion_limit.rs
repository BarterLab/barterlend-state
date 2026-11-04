use std::{collections::VecDeque, sync::{Arc, Mutex}, time::Duration};
use tokio::{sync::Notify, time::{Instant, sleep_until}};
use crate::{Error, Result};

/// A request keeps its slot until one second after it finishes. Therefore a
/// delayed dispatch cannot join the next batch without consuming its old slot.
/// No background task is created; storage never exceeds the configured limit.
#[derive(Debug)]
pub(super) struct CompletionLimit {
    limit: usize,
    // FIFO admission: a newly ready task cannot repeatedly overtake readers
    // already waiting for a slot. Cancellation removes its place in this queue.
    admission: tokio::sync::Mutex<()>,
    state: Mutex<(usize, VecDeque<Instant>)>,
    changed: Notify,
}

impl CompletionLimit {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self { limit, admission: tokio::sync::Mutex::new(()),
            state: Mutex::new((0, VecDeque::new())), changed: Notify::new() })
    }

    pub(super) async fn acquire(self: &Arc<Self>) -> Result<CompletionPermit> {
        let _turn = self.admission.lock().await;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            // Register before checking the predicate to avoid a lost completion.
            changed.as_mut().enable();
            let until = {
                let mut state = self.state.lock().map_err(|_| Error::Transport("RPC completion limiter poisoned".into()))?;
                let now = Instant::now();
                while state.1.front().is_some_and(|at| *at <= now) { state.1.pop_front(); }
                if state.0 + state.1.len() < self.limit {
                    state.0 += 1;
                    return Ok(CompletionPermit(self.clone()));
                }
                state.1.front().copied()
            };
            if let Some(until) = until {
                tokio::select! { _ = sleep_until(until) => {}, _ = &mut changed => {} }
            } else {
                changed.await;
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct CompletionPermit(Arc<CompletionLimit>);
impl Drop for CompletionPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.0 -= 1;
            state.1.push_back(Instant::now() + Duration::from_secs(1));
        }
        self.0.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_unpolled_old_waiter_cannot_be_overtaken_by_new_work() {
        let limiter = CompletionLimit::new(1);
        let held = limiter.acquire().await.unwrap();
        let mut older = Box::pin(limiter.acquire());
        assert!(futures_util::poll!(older.as_mut()).is_pending());
        drop(held);
        tokio::time::sleep(Duration::from_millis(1050)).await;
        let mut newer = Box::pin(limiter.acquire());
        assert!(futures_util::poll!(newer.as_mut()).is_pending());
        let first = older.await.unwrap();
        assert!(futures_util::poll!(newer.as_mut()).is_pending());
        drop(first);
        tokio::time::timeout(Duration::from_millis(1200), newer).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn delayed_completion_and_cancelled_waiter_do_not_release_early() {
        let limiter = CompletionLimit::new(1);
        let permit = limiter.acquire().await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(20), limiter.acquire()).await.is_err());
        drop(permit);
        assert!(tokio::time::timeout(Duration::from_millis(850), limiter.acquire()).await.is_err());
        let next = tokio::time::timeout(Duration::from_millis(400), limiter.acquire()).await.unwrap().unwrap();
        assert_eq!(limiter.state.lock().unwrap().0, 1);
        drop(next);
        let state = limiter.state.lock().unwrap();
        assert_eq!(state.0, 0);
        assert_eq!(state.1.len(), 1);
    }
}
