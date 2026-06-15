//! A fixed-capacity circular (ring) buffer.
//!
//! [`RingBuffer`] stores up to a fixed number of elements. While it is still
//! filling up, pushed values are simply appended. Once it reaches capacity it
//! begins to overwrite the oldest element on every push, returning the evicted
//! value to the caller. The backing storage is allocated once at construction
//! and never grows again, which makes it well suited to N-period lookbacks and
//! other fixed-size sliding-window workloads.
//!
//! # Examples
//!
//! ```rust
//! use chapaty::ring_buffer::RingBuffer;
//!
//! let mut window = RingBuffer::new(3);
//! window.push(1);
//! window.push(2);
//! window.push(3);
//!
//! // The window is now full; the next push evicts the oldest value.
//! assert!(window.is_full());
//! assert_eq!(window.push(4), Some(1));
//! ```

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum RingState {
    /// The buffer has not yet reached capacity. Elements are strictly appended.
    /// The `buffer.len()` is both the current length and the next insertion index.
    Filling,
    /// The buffer is fully populated. New insertions overwrite the oldest data.
    /// The `cursor` is the index of the next element to be evicted.
    Full { cursor: usize },
}

impl RingState {
    fn is_full(&self) -> bool {
        matches!(self, RingState::Full { .. })
    }
}

/// A generic, fixed-capacity circular buffer for N-period lookbacks.
///
/// The buffer holds at most [`capacity`](Self::capacity) elements. Until that
/// many values have been pushed it grows like a [`Vec`]; afterwards every
/// [`push`](Self::push) overwrites the oldest element and returns it. No
/// allocation occurs after construction.
///
/// # Examples
///
/// ```rust
/// # use chapaty::ring_buffer::RingBuffer;
///
/// let mut buffer = RingBuffer::new(2);
/// assert_eq!(buffer.push("a"), None);
/// assert_eq!(buffer.push("b"), None);
/// assert_eq!(buffer.push("c"), Some("a")); // overwrites the oldest element
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingBuffer<T> {
    capacity: usize,
    buffer: SmallVec<[T; 16]>,
    state: RingState,
}

impl<T> RingBuffer<T> {
    /// Creates a new, empty `RingBuffer` that can hold exactly `capacity` elements.
    ///
    /// The backing allocation is reserved immediately, so no further allocation
    /// occurs while the buffer is in use.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0`: a zero-capacity ring buffer can never hold
    /// or evict an element and is therefore treated as a programming error.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::ring_buffer::RingBuffer;
    ///
    /// let buffer: RingBuffer<f64> = RingBuffer::new(3);
    /// assert!(buffer.is_empty());
    /// assert_eq!(buffer.capacity(), 3);
    /// ```
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "RingBuffer capacity must be strictly greater than 0. Got {capacity} <= 0."
        );
        Self {
            capacity,
            buffer: SmallVec::with_capacity(capacity),
            state: RingState::Filling,
        }
    }

    /// Returns the maximum number of elements the buffer can hold.
    ///
    /// This value is fixed at construction time and never changes.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of elements currently stored in the buffer.
    ///
    /// This grows from `0` up to [`capacity`](Self::capacity) and then stays
    /// there until the buffer is [`clear`](Self::clear)ed.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::ring_buffer::RingBuffer;
    ///
    /// let mut buffer = RingBuffer::new(2);
    /// assert_eq!(buffer.len(), 0);
    /// buffer.push(1);
    /// assert_eq!(buffer.len(), 1);
    /// buffer.push(2);
    /// buffer.push(3); // overwrites; length stays at capacity
    /// assert_eq!(buffer.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns `true` if the buffer contains no elements.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns `true` once the buffer has been filled to [`capacity`](Self::capacity).
    ///
    /// While full, every [`push`](Self::push) evicts and returns the oldest element.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::ring_buffer::RingBuffer;
    ///
    /// let mut buffer = RingBuffer::new(2);
    /// assert!(!buffer.is_full());
    /// buffer.push(1);
    /// assert!(!buffer.is_full());
    /// buffer.push(2);
    /// assert!(buffer.is_full());
    /// ```
    pub fn is_full(&self) -> bool {
        self.state.is_full()
    }

    /// Pushes `value` into the buffer, returning the evicted element if there was one.
    ///
    /// While the buffer is still filling, `value` is appended and [`None`] is
    /// returned. Once the buffer is full, `value` overwrites the oldest element
    /// and that element is returned as [`Some`]. Runs in _O_(1) and never
    /// allocates.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::ring_buffer::RingBuffer;
    ///
    /// let mut buffer = RingBuffer::new(2);
    /// assert_eq!(buffer.push(10), None);
    /// assert_eq!(buffer.push(20), None);
    /// assert_eq!(buffer.push(30), Some(10)); // evicts the oldest value
    /// ```
    pub fn push(&mut self, value: T) -> Option<T> {
        match self.state {
            RingState::Filling => {
                self.buffer.push(value);

                if self.buffer.len() == self.capacity {
                    // Transition to the full state, starting eviction at index 0.
                    self.state = RingState::Full { cursor: 0 };
                }
                None
            }
            RingState::Full { cursor } => {
                // Buffer is full: swap the incoming value in for the oldest one.
                let evicted_value = std::mem::replace(&mut self.buffer[cursor], value);

                // Advance the cursor, wrapping back to the start at capacity.
                let next_cursor = if cursor + 1 == self.capacity {
                    0
                } else {
                    cursor + 1
                };
                self.state = RingState::Full {
                    cursor: next_cursor,
                };

                Some(evicted_value)
            }
        }
    }

    /// Removes all elements, dropping them, and resets the buffer to empty.
    ///
    /// The capacity and backing allocation are retained, so refilling the
    /// buffer afterwards does not allocate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use chapaty::ring_buffer::RingBuffer;
    ///
    /// let mut buffer = RingBuffer::new(3);
    /// buffer.push(1.0);
    /// buffer.clear();
    /// assert!(buffer.is_empty());
    /// assert!(!buffer.is_full());
    /// ```
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.state = RingState::Filling;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_starts_empty() {
        let buffer: RingBuffer<i32> = RingBuffer::new(4);
        assert!(buffer.is_empty());
        assert!(!buffer.is_full());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 4);
    }

    #[test]
    #[should_panic(expected = "capacity must be strictly greater than 0")]
    fn new_zero_capacity_panics() {
        let _: RingBuffer<i32> = RingBuffer::new(0);
    }

    #[test]
    fn reports_full_exactly_at_capacity() {
        let mut buffer = RingBuffer::new(3);
        assert_eq!(buffer.push(1), None);
        assert!(!buffer.is_full());
        assert_eq!(buffer.push(2), None);
        assert!(!buffer.is_full());
        assert_eq!(buffer.push(3), None);
        assert!(buffer.is_full());
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn evicts_oldest_first_when_full() {
        let mut buffer = RingBuffer::new(3);
        for v in [1, 2, 3] {
            assert_eq!(buffer.push(v), None);
        }
        // Each push past capacity returns the oldest remaining value (FIFO).
        assert_eq!(buffer.push(4), Some(1));
        assert_eq!(buffer.push(5), Some(2));
        assert_eq!(buffer.push(6), Some(3));
        assert_eq!(buffer.push(7), Some(4));
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.capacity(), 3);
    }

    #[test]
    fn capacity_one_overwrites_on_every_push() {
        let mut buffer = RingBuffer::new(1);
        assert_eq!(buffer.push(10), None);
        assert!(buffer.is_full());
        assert_eq!(buffer.push(20), Some(10));
        assert_eq!(buffer.push(30), Some(20));
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn clear_resets_state_and_allows_refilling() {
        let mut buffer = RingBuffer::new(2);
        buffer.push(1);
        buffer.push(2);
        assert!(buffer.is_full());

        buffer.clear();
        assert!(buffer.is_empty());
        assert!(!buffer.is_full());
        assert_eq!(buffer.len(), 0);

        // Fully usable again, including the fill -> full -> evict cycle.
        assert_eq!(buffer.push(9), None);
        assert_eq!(buffer.push(8), None);
        assert_eq!(buffer.push(7), Some(9));
    }
}
