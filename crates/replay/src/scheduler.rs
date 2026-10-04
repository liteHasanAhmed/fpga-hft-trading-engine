use hft_core::Ts;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Deterministic discrete-event scheduler.
///
/// Events are ordered by `(time, priority, insertion counter)`. Equal-time events
/// therefore run in a fixed priority order, and equal-priority ones in the order
/// they were scheduled — never in hash or allocation order.
pub struct Scheduler<E> {
    heap: BinaryHeap<Reverse<Entry<E>>>,
    counter: u64,
    now: Ts,
}

struct Entry<E> {
    ts: Ts,
    prio: u8,
    seq: u64,
    ev: E,
}

impl<E> PartialEq for Entry<E> {
    fn eq(&self, o: &Self) -> bool {
        (self.ts, self.prio, self.seq) == (o.ts, o.prio, o.seq)
    }
}
impl<E> Eq for Entry<E> {}
impl<E> PartialOrd for Entry<E> {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl<E> Ord for Entry<E> {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (self.ts, self.prio, self.seq).cmp(&(o.ts, o.prio, o.seq))
    }
}

impl<E> Default for Scheduler<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Scheduler<E> {
    pub fn new() -> Self {
        Scheduler { heap: BinaryHeap::new(), counter: 0, now: Ts(i64::MIN) }
    }

    /// Current virtual time (time of the last popped event).
    pub fn now(&self) -> Ts {
        self.now
    }

    /// Schedule `ev` at `ts`. Scheduling in the past is clamped to `now`, so the
    /// clock never moves backwards.
    pub fn push(&mut self, ts: Ts, prio: u8, ev: E) {
        let ts = ts.max(self.now);
        self.counter += 1;
        self.heap.push(Reverse(Entry { ts, prio, seq: self.counter, ev }));
    }

    pub fn peek_ts(&self) -> Option<Ts> {
        self.heap.peek().map(|e| e.0.ts)
    }

    pub fn pop(&mut self) -> Option<(Ts, E)> {
        let Reverse(e) = self.heap.pop()?;
        debug_assert!(e.ts >= self.now);
        self.now = e.ts;
        Some((e.ts, e.ev))
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_is_time_priority_insertion() {
        let mut s = Scheduler::new();
        s.push(Ts(10), 2, "t10-p2-a");
        s.push(Ts(10), 1, "t10-p1");
        s.push(Ts(5), 9, "t5");
        s.push(Ts(10), 2, "t10-p2-b");
        let order: Vec<_> = std::iter::from_fn(|| s.pop().map(|x| x.1)).collect();
        assert_eq!(order, vec!["t5", "t10-p1", "t10-p2-a", "t10-p2-b"]);
    }

    #[test]
    fn past_is_clamped() {
        let mut s = Scheduler::new();
        s.push(Ts(10), 0, 1);
        s.pop();
        s.push(Ts(3), 0, 2);
        assert_eq!(s.pop(), Some((Ts(10), 2)));
    }
}
