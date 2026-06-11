use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum RingState {
    /// The buffer has not yet reached capacity. Elements are strictly appended.
    /// The `count` represents both the current length and the next insertion index.
    Filling { count: usize },
    /// The buffer is fully populated. New insertions will overwrite the oldest data.
    /// The `cursor` indicates the index of the next element to be evicted.
    Full { cursor: usize },
}

impl RingState {
    fn is_full(&self) -> bool {
        matches!(self, RingState::Full { .. })
    }
}

/// A generic, zero-allocation circular buffer for N-period lookbacks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingBuffer<T> {
    capacity: usize,
    buffer: Vec<T>,
    state: RingState,
}

impl<T> RingBuffer<T> {
    /// Creates a new `RingBuffer` with an exact capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0, as a 0-capacity ring buffer cannot hold state.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut buffer: RingBuffer<f64> = RingBuffer::new(3);
    /// assert!(buffer.is_empty());
    /// ```
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "RingBuffer capacity must be strictly greater than 0"
        );
        Self {
            capacity,
            buffer: Vec::with_capacity(capacity),
            state: RingState::Filling { count: 0 },
        }
    }

    /// Returns the exact lookback capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of elements currently held in the buffer.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns `true` if the buffer contains no elements.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns `true` if the buffer has completed its first full cycle.
    pub fn is_full(&self) -> bool {
        self.state.is_full()
    }

    /// Pushes a new value into the buffer, advancing the internal state machine.
    ///
    /// If the buffer is full, the oldest value is evicted and returned as `Some(T)`.
    /// If the buffer is still filling, the value is appended and `None` is returned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut buffer = RingBuffer::new(2);
    /// assert_eq!(buffer.push(10), None);
    /// assert_eq!(buffer.push(20), None);
    /// assert_eq!(buffer.push(30), Some(10)); // Evicts oldest
    /// ```
    pub fn push(&mut self, value: T) -> Option<T> {
        match self.state {
            RingState::Filling { count } => {
                self.buffer.push(value);
                let next_count = count + 1;

                if next_count == self.capacity {
                    // Transition to Full state starting at index 0
                    self.state = RingState::Full { cursor: 0 };
                } else {
                    self.state = RingState::Filling { count: next_count };
                }
                None
            }
            RingState::Full { cursor } => {
                // Buffer is full. Safely swap the current value with the incoming one.
                let evicted_value = std::mem::replace(&mut self.buffer[cursor], value);

                // Advance cursor, wrapping around via modulo logic
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

    /// Clears the buffer, dropping all elements and resetting the state to `Filling`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut buffer = RingBuffer::new(3);
    /// buffer.push(1.0);
    /// buffer.clear();
    /// assert!(buffer.is_empty());
    /// assert!(!buffer.is_full());
    /// ```
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.state = RingState::Filling { count: 0 };
    }
}
