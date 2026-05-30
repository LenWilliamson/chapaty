use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::math::StreamingIndicator;

// ================================================================================================
// SHARED: Exponential Weighted Mean (Base Logic)
// ================================================================================================

/// Internal helper for EMA-like calculations (Standard EMA and Wilder's Smoothing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StreamingEwm {
    alpha: f64,
    current_mean: Option<f64>,
    window_size: usize,
    count: usize,
}

impl StreamingEwm {
    pub(crate) fn new(alpha: f64, window_size: usize) -> Self {
        Self {
            alpha,
            current_mean: None,
            window_size,
            count: 0,
        }
    }
}

impl StreamingIndicator for StreamingEwm {
    type Input = f64;
    type Output<'a> = Option<f64>;

    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        self.count += 1;

        match self.current_mean {
            None => {
                // First trade: initialize the mean
                self.current_mean = Some(value);
            }
            Some(prev) => {
                // Recursive update
                self.current_mean = Some(self.alpha * value + (1.0 - self.alpha) * prev);
            }
        }

        if self.count >= self.window_size {
            self.current_mean
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.current_mean = None;
        self.count = 0;
    }
}

// ================================================================================================
// SMA: Simple Moving Average
// ================================================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingSma {
    window_size: usize,
    buffer: VecDeque<f64>,
    sum: f64,
}

impl StreamingSma {
    pub fn new(window_size: u16) -> Self {
        let size = window_size as usize;
        Self {
            window_size: size,
            buffer: VecDeque::with_capacity(size),
            sum: 0.0,
        }
    }
}

impl StreamingIndicator for StreamingSma {
    type Input = f64;
    type Output<'a> = Option<f64>;

    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        self.buffer.push_back(value);
        self.sum += value;

        if self.buffer.len() > self.window_size
            && let Some(removed) = self.buffer.pop_front()
        {
            self.sum -= removed;
        }

        if self.buffer.len() >= self.window_size {
            Some(self.sum / self.buffer.len() as f64)
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.sum = 0.0;
    }
}

// ================================================================================================
// EMA: Exponential Moving Average
// ================================================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingEma {
    inner: StreamingEwm,
}

impl StreamingEma {
    pub fn new(window_size: u16) -> Self {
        // Standard EMA Alpha = 2 / (Span + 1)
        let alpha = 2.0 / (window_size as f64 + 1.0);
        Self {
            inner: StreamingEwm::new(alpha, window_size as usize),
        }
    }
}

impl StreamingIndicator for StreamingEma {
    type Input = f64;
    type Output<'a> = Option<f64>;

    fn update(&mut self, value: Self::Input) -> Self::Output<'_> {
        self.inner.update(value)
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}
