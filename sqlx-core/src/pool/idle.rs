use super::connection::Idle;
use crate::database::Database;
use crate::pool::IdleOrder;
use crossbeam_queue::ArrayQueue;
use std::sync::{Mutex, PoisonError};

/// The pool's idle connections, handed out in the pool's [`IdleOrder`].
// There is one per pool, so the size of `ArrayQueue`'s cache-padded indices does not matter.
#[allow(clippy::large_enum_variant)]
pub(super) enum IdleQueue<DB: Database> {
    Fifo(ArrayQueue<Idle<DB>>),
    /// Ordered from longest idle to most recently released.
    ///
    /// Critical sections are a push, pop or bounded scan with no `.await`,
    /// so a plain mutex does not hold up the pool.
    Lifo {
        stack: Mutex<Vec<Idle<DB>>>,
        capacity: usize,
    },
}

impl<DB: Database> IdleQueue<DB> {
    pub(super) fn new(order: IdleOrder, capacity: usize) -> Self {
        match order {
            IdleOrder::Fifo => Self::Fifo(ArrayQueue::new(capacity)),
            IdleOrder::Lifo => Self::Lifo {
                stack: Mutex::new(Vec::with_capacity(capacity)),
                capacity,
            },
        }
    }

    /// Returns the connection back if the queue is full.
    pub(super) fn push(&self, idle: Idle<DB>) -> Result<(), Idle<DB>> {
        match self {
            Self::Fifo(queue) => queue.push(idle),
            Self::Lifo { stack, capacity } => {
                let mut stack = stack.lock().unwrap_or_else(PoisonError::into_inner);
                if stack.len() >= *capacity {
                    return Err(idle);
                }
                stack.push(idle);
                Ok(())
            }
        }
    }

    /// Takes the next connection to hand out.
    pub(super) fn pop(&self) -> Option<Idle<DB>> {
        match self {
            Self::Fifo(queue) => queue.pop(),
            Self::Lifo { stack, .. } => stack.lock().unwrap_or_else(PoisonError::into_inner).pop(),
        }
    }

    /// Takes a connection for which `is_expired` holds, without changing when any connection
    /// counts as idle from.
    ///
    /// FIFO checks only the connection at the head, moving it to the tail if it has not expired,
    /// so `n` calls visit the `n` longest-idle connections. LIFO takes the longest-idle expired
    /// connection and leaves the order of the rest untouched.
    pub(super) fn pop_expired(&self, is_expired: impl Fn(&Idle<DB>) -> bool) -> Option<Idle<DB>> {
        match self {
            Self::Fifo(queue) => {
                let idle = queue.pop()?;
                if is_expired(&idle) {
                    return Some(idle);
                }
                if queue.push(idle).is_err() {
                    panic!("BUG: connection queue overflow in pop_expired()");
                }
                None
            }
            Self::Lifo { stack, .. } => {
                let mut stack = stack.lock().unwrap_or_else(PoisonError::into_inner);
                let index = stack.iter().position(is_expired)?;
                Some(stack.remove(index))
            }
        }
    }
}
