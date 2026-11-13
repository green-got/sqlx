use super::connection::Idle;
use crate::database::Database;
use crate::pool::IdleOrder;
use crossbeam_queue::ArrayQueue;
use std::cmp;
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

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

    /// Takes a connection whose `expires_at` has passed, without changing when any connection
    /// counts as idle from. Otherwise returns the earliest `expires_at` among the connections
    /// it checked, if any.
    ///
    /// FIFO checks only the connection at the head, moving it to the tail if it has not expired,
    /// so `n` calls visit the `n` longest-idle connections. LIFO takes the longest-idle expired
    /// connection and leaves the order of the rest untouched.
    pub(super) fn pop_expired(
        &self,
        expires_at: impl Fn(&Idle<DB>) -> Option<Instant>,
    ) -> Result<Idle<DB>, Option<Instant>> {
        let now = Instant::now();

        match self {
            Self::Fifo(queue) => {
                let idle = queue.pop().ok_or(None)?;
                let expiry = expires_at(&idle);
                if expiry.is_some_and(|expiry| expiry <= now) {
                    return Ok(idle);
                }
                if queue.push(idle).is_err() {
                    panic!("BUG: connection queue overflow in pop_expired()");
                }
                Err(expiry)
            }
            Self::Lifo { stack, .. } => {
                let mut stack = stack.lock().unwrap_or_else(PoisonError::into_inner);
                let mut next_expiry = None;
                for (index, idle) in stack.iter().enumerate() {
                    match expires_at(idle) {
                        Some(expiry) if expiry <= now => return Ok(stack.remove(index)),
                        Some(expiry) => {
                            next_expiry =
                                Some(next_expiry.map_or(expiry, |next| cmp::min(expiry, next)))
                        }
                        None => {}
                    }
                }
                Err(next_expiry)
            }
        }
    }
}
