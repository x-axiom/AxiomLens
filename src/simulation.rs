use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use async_channel::{Sender, TrySendError};
use gpui_kit::{BackgroundExecutor, Task};

pub const CHANNEL_CAPACITY: usize = 16;
pub const UI_BATCH_LIMIT: usize = 8;
pub const FEED_LIMIT: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SimulatedTaskMode {
    DelayedSuccess,
    DelayedFailure,
    Burst,
}

impl SimulatedTaskMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::DelayedSuccess => "delayed mock success",
            Self::DelayedFailure => "delayed mock failure",
            Self::Burst => "bounded queue overflow demo",
        }
    }
}

#[derive(Clone)]
pub enum SimulatedTaskMessage {
    Output {
        sequence: usize,
        level: &'static str,
        message: String,
    },
    Finished {
        dropped: usize,
    },
    Failed {
        dropped: usize,
    },
    Cancelled {
        dropped: usize,
    },
}

impl SimulatedTaskMessage {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Finished { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }
}

fn enqueue_bounded(
    sender: &Sender<SimulatedTaskMessage>,
    message: SimulatedTaskMessage,
    dropped: &mut usize,
) -> bool {
    match sender.try_send(message) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            *dropped += 1;
            true
        }
        Err(TrySendError::Closed(_)) => false,
    }
}

pub fn spawn(
    executor: BackgroundExecutor,
    sender: Sender<SimulatedTaskMessage>,
    cancelled: Arc<AtomicBool>,
    mode: SimulatedTaskMode,
) -> Task<()> {
    let timer = executor.clone();
    executor.spawn(async move {
        let message_count = if mode == SimulatedTaskMode::Burst { 64 } else { 18 };
        let mut dropped = 0;
        let mut failed = false;

        for sequence in 1..=message_count {
            if cancelled.load(Ordering::Acquire) {
                break;
            }

            if mode != SimulatedTaskMode::Burst {
                timer.timer(Duration::from_millis(120)).await;
                if cancelled.load(Ordering::Acquire) {
                    break;
                }
            }

            if mode == SimulatedTaskMode::DelayedFailure && sequence == 12 {
                failed = true;
                break;
            }

            let (level, message) = match sequence % 4 {
                0 => ("WARN", format!("sample retry {sequence}/18")),
                1 => ("INFO", format!("sample event batch {sequence}")),
                _ => ("INFO", format!("sample response chunk {sequence}")),
            };

            if !enqueue_bounded(
                &sender,
                SimulatedTaskMessage::Output {
                    sequence,
                    level,
                    message,
                },
                &mut dropped,
            ) {
                return;
            }
        }

        let terminal = if cancelled.load(Ordering::Acquire) {
            SimulatedTaskMessage::Cancelled { dropped }
        } else if failed {
            SimulatedTaskMessage::Failed { dropped }
        } else {
            SimulatedTaskMessage::Finished { dropped }
        };
        let _ = sender.send(terminal).await;
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui_kit::TestAppContext;
    use std::time::Duration;

    #[test]
    fn bounded_queue_drops_newest_messages_and_reports_count() {
        let (sender, receiver) = async_channel::bounded(CHANNEL_CAPACITY);
        let mut dropped = 0;

        for sequence in 1..=CHANNEL_CAPACITY + 3 {
            assert!(enqueue_bounded(
                &sender,
                SimulatedTaskMessage::Output {
                    sequence,
                    level: "INFO",
                    message: format!("sample {sequence}"),
                },
                &mut dropped,
            ));
        }

        let received = std::iter::from_fn(|| receiver.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(received.len(), CHANNEL_CAPACITY);
        assert_eq!(dropped, 3);
        assert!(matches!(received.first(), Some(SimulatedTaskMessage::Output { sequence: 1, .. })));
        assert!(matches!(received.last(), Some(SimulatedTaskMessage::Output { sequence: 16, .. })));
    }

    #[test]
    fn every_task_outcome_is_a_distinct_terminal_state() {
        assert!(SimulatedTaskMessage::Finished { dropped: 0 }.is_terminal());
        assert!(SimulatedTaskMessage::Failed { dropped: 0 }.is_terminal());
        assert!(SimulatedTaskMessage::Cancelled { dropped: 0 }.is_terminal());
        assert!(!SimulatedTaskMessage::Output {
            sequence: 1,
            level: "INFO",
            message: "sample".to_owned(),
        }
        .is_terminal());
    }

    #[gpui_kit::test]
    fn burst_reports_bounded_queue_overflow(cx: &mut TestAppContext) {
        let (sender, receiver) = async_channel::bounded(CHANNEL_CAPACITY);
        let worker = spawn(
            cx.executor(),
            sender,
            Arc::new(AtomicBool::new(false)),
            SimulatedTaskMode::Burst,
        );
        cx.run_until_parked();

        let mut output_count = 0;
        while let Ok(message) = receiver.try_recv() {
            if matches!(message, SimulatedTaskMessage::Output { .. }) {
                output_count += 1;
            }
        }
        cx.run_until_parked();

        let terminal = receiver.try_recv().expect("burst emits terminal outcome");
        assert_eq!(output_count, CHANNEL_CAPACITY);
        match terminal {
            SimulatedTaskMessage::Finished { dropped } => {
                assert_eq!(dropped, 64 - CHANNEL_CAPACITY);
            }
            _ => panic!("burst finishes after reporting overflow"),
        }
        drop(worker);
    }

    #[gpui_kit::test]
    fn delayed_failure_ends_as_failure_not_success(cx: &mut TestAppContext) {
        let (sender, receiver) = async_channel::bounded(CHANNEL_CAPACITY);
        let worker = spawn(
            cx.executor(),
            sender,
            Arc::new(AtomicBool::new(false)),
            SimulatedTaskMode::DelayedFailure,
        );
        cx.run_until_parked();

        for _ in 0..12 {
            cx.executor().advance_clock(Duration::from_millis(120));
            cx.run_until_parked();
        }

        let messages = std::iter::from_fn(|| receiver.try_recv().ok()).collect::<Vec<_>>();
        assert!(messages
            .iter()
            .any(|message| matches!(message, SimulatedTaskMessage::Failed { .. })));
        assert!(!messages
            .iter()
            .any(|message| matches!(message, SimulatedTaskMessage::Finished { .. })));
        drop(worker);
    }

    #[gpui_kit::test]
    fn delayed_task_cancellation_has_cancelled_terminal_state(cx: &mut TestAppContext) {
        let (sender, receiver) = async_channel::bounded(CHANNEL_CAPACITY);
        let cancellation = Arc::new(AtomicBool::new(false));
        let worker = spawn(
            cx.executor(),
            sender,
            cancellation.clone(),
            SimulatedTaskMode::DelayedSuccess,
        );
        cx.run_until_parked();

        cx.executor().advance_clock(Duration::from_millis(120));
        cx.run_until_parked();
        cancellation.store(true, Ordering::Release);
        cx.executor().advance_clock(Duration::from_millis(120));
        cx.run_until_parked();

        let messages = std::iter::from_fn(|| receiver.try_recv().ok()).collect::<Vec<_>>();
        assert!(messages
            .iter()
            .any(|message| matches!(message, SimulatedTaskMessage::Cancelled { .. })));
        assert!(!messages
            .iter()
            .any(|message| matches!(message, SimulatedTaskMessage::Finished { .. })));
        drop(worker);
    }
}