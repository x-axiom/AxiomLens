use tokio::sync::watch;
use std::fmt;

#[derive(Clone)]
pub struct CancellationToken {
    sender: watch::Sender<bool>,
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl CancellationToken {
    pub fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }

    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }

    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        while !*receiver.borrow_and_update() {
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::runtime::Builder;

    #[test]
    fn cancellation_wakes_async_waiters_and_is_visible_across_clones() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let token = CancellationToken::new();
        let waiter = token.clone();
        runtime.block_on(async move {
            let wait_task = tokio::spawn(async move { waiter.cancelled().await });
            token.cancel();
            tokio::time::timeout(Duration::from_secs(1), wait_task)
                .await
                .unwrap()
                .unwrap();
            assert!(token.is_cancelled());
            token.cancelled().await;
        });
    }
}