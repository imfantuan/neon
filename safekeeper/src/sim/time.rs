use std::{cmp::Ordering, collections::BinaryHeap};



pub struct Timing {
    /// Current world's time.
    current_time: u64,
    /// Pending timers.
    timers: BinaryHeap<Pending>,
    /// Global nonce.
    nonce: u32,
}

impl Timing {
    pub fn new() -> Timing {
        Timing {
            current_time: 0,
            timers: BinaryHeap::new(),
            nonce: 0,
        }
    }

    /// Tick-tock the global clock. Return the event ready to be processed
    /// or move the clock forward and then return the event.
    pub fn step(&mut self) -> Option<Pending> {
        if self.timers.len() == 0 {
            // no future events
            return None;
        }

        if !self.is_event_ready() {
            let next_time = self.timers.peek().unwrap().time;
            println!("Advancing time from {} to {}", self.current_time, next_time);
            self.current_time = next_time;
            assert!(self.is_event_ready());
        }

        self.timers.pop()
    }

    /// TODO: write docs
    pub fn schedule(&mut self, time: u64, event: Event) {
        self.nonce += 1;
        let nonce = self.nonce;
        self.timers.push(Pending { time, nonce, event })
    }

    /// Return true if there is a ready event.
    fn is_event_ready(&self) -> bool {
        self.timers
            .peek()
            .map_or(false, |x| x.time <= self.current_time)
    }
}

pub struct Pending {
    pub time: u64,
    pub nonce: u32,
    pub event: Event,
}

impl Pending {
    pub fn process(&self) {
        self.event.process();
    }
}

// BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
// to get that.
impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (other.time, other.nonce).partial_cmp(&(self.time, self.nonce))
    }
}

impl Ord for Pending {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.time, other.nonce).cmp(&(self.time, self.nonce))
    }
}

impl PartialEq for Pending {
    fn eq(&self, other: &Self) -> bool {
        &(other.time, other.nonce) == &(self.time, self.nonce)
    }
}

impl Eq for Pending {}

#[derive(Debug)]
pub enum Event {}

impl Event {
    fn process(&self) {
        // TODO:
    }
}
