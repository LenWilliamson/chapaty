# Chapaty: Open Source Trading Strategy Backtesting in Rust

Welcome to Chapaty, an open-source project designed to set the standard for trading strategy backtesting. As this project is still in its early stages, and I am new to Rust and software engineering practices, this `README.md` serves as a repository for questions, challenges, and ideas. My goal is to seek feedback to improve both my code and my understanding.

## Questions and Challenges

Below are questions that have arisen during the development of Chapaty. For each question, I have provided context and examples to facilitate feedback. While some questions might seem trivial to experienced developers, I’ve documented them to either seek guidance or address them myself as I gain more experience. The "Expectations for Feedback" section reflects what I am currently seeking in terms of feedback.

**Table of Contents:**
1. [Why do I get the import `use chapaty::DataFrame;`?](#1-why-do-i-get-the-import-use-chapatydataframe)
2. [How to handle `impl From`?](#2-how-to-handle-impl-from)
3. [Is combining building and computation in `build_and_compute()` a good practice?](#3-is-combining-building-and-computation-in-build_and_compute-a-good-practice)
4. [How can I improve my documentation using Rustdoc?](#4-how-can-i-improve-my-documentation-using-rustdoc)

---

## 1. Why do I get the import `use chapaty::DataFrame;`?

**Context:**
- I am using the `DataFrame` struct from the `polars` crate.
- In my integration test modules, I am importing `use chapaty::DataFrame;`, although I expected the struct to be available directly from the `polars` crate.

**Question:**
- Is this import behavior expected in Rust, or is there something I might be missing in my module organization?

**Code Example:**
```rust
// tests/test_configurations/mod.rs
use chapaty::DataFrame;
use polars::{io::SerReader, prelude::CsvReadOptions};

pub fn get_expected_result(file_name: &str) -> DataFrame {
    CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(file_name.into()))
        .unwrap()
        .finish()
        .unwrap()
}
```

**Expectations for Feedback:**
- Why is this behavior occurring?
- Are there better ways to structure my modules to avoid this issue, if it is not ideal?

---

## 2. How to handle `impl From`?

**Context:**
- I understand that `impl From<A> for B` is used to convert from one type to another.
- However, I am unsure where exactly to define this implementation in my project structure.

**Question:**
- Where is the best place to define `impl From<A> for B` in a Rust project?

**Additional Context:**
- I found a discussion [here](https://www.reddit.com/r/rust/comments/1estnop/question_where_should_impl_froma_for_b_be_defined/) but would appreciate further insights or examples.

**Expectations for Feedback:**
- Best practices for organizing `From` implementations.
- Examples from larger Rust projects or open-source libraries.

---

## 3. Is combining building and computation in `build_and_compute()` a good practice?

**Context:**
- I am using the builder pattern to construct complex objects in Chapaty.
- This pattern includes a `build_and_compute()` method that both finalizes the object and performs a computation.

**Question:**
- In general, is it considered good practice to combine construction and computation in a single method like `build_and_compute()`? I have a `XyzCalculator` designed to compute the `Xyz` object, with a builder to create the `XyzCalculator`.

**Code Example:**
Here’s how I use the `TradePnLCalculator`:
```rust
// Using the TradePnLCalculator
let trade_pnl = TradePnLCalculatorBuilder::new()
        // Set parameters for the builder...
        .build_and_compute();
```
The object is constructed as follows:
```rust
impl TradePnLCalculator {
    pub fn compute(&self) -> TradePnL {
        // Computes the PnL of a trade
    }
}

impl TradePnLCalculatorBuilder {
    pub fn new() -> Self {
        // Initializes the builder for TradePnLCalculator
    }

    pub fn build(self) -> TradePnLCalculator {
        // Constructs a TradePnLCalculator
    }

    pub fn build_and_compute(self) -> TradePnL {
        self.build().compute()
    }
}
```

**Expectations for Feedback:**
- Is this approach considered good practice?
- Are there alternative patterns that might be more suitable?

---

## 4. How can I improve my documentation using Rustdoc?

**Context:**
- I aim to ensure Chapaty is well-documented and easy to use for other developers.
- I am using Rustdoc but am unsure if I am leveraging its full potential.

**Question:**
- What are some best practices or tips for writing effective Rustdoc comments?

**Expectations for Feedback:**
- Examples of well-documented Rust projects.
- Specific Rustdoc features that are commonly overlooked.

---