

# Chapaty: Open Source Trading Strategy Backtesting in Rust

Welcome to Chapaty, an open-source project designed to become the standard for trading strategy backtesting. This project is in its early stages, and I am both new to Rust and to software engineering practices. This `README.md` serves as a place to collect questions, challenges, and ideas as I build out this project. Eventually, I hope to seek feedback to improve both my code and my understanding.

## Questions and Challenges

Below are questions that have arisen during the development of Chapaty. For each question, I've provided context and examples to facilitate feedback. While some questions may seem trivial to experienced developers, I’ve documented them to either seek guidance or address them myself as I gain more experience. The "Expectations for Feedback" are a snapshot of the time when I was writing this question of what I seek to now.

### 1. Why do I get the import `use chapaty::DataFrame;`?

**Context:**
- I use the `DataFrame` struct by the `polars` module.
- When trying to use the `polars::prelude::DataFrame` in my integration test modules, I'm importing `use chapaty::DataFrame;`.
- I expected the struct to be available from the `polars` crate.

**Question:**
- Is this import behavior expected in Rust, or is there something I am missing in my module organization?

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
- Why is this behaviour happening?
- Are there better ways to structure my modules to avoid this, if it is indeed not ideal?

---

### 2. How to handle `impl From`?

**Context:**
- I have read about `impl From<A> for B` and understand that it is used to convert from one type to another.
- However, I am confused about where exactly to define this implementation in my project structure.

**Question:**
- Where is the best place to define `impl From<A> for B` in a Rust project?

**Additional Context:**
- I found a discussion [here](https://www.reddit.com/r/rust/comments/1estnop/question_where_should_impl_froma_for_b_be_defined/) but would appreciate more insights or examples.

**Expectations for Feedback:**
- Best practices for organizing `From` implementations.
- Examples from larger Rust projects or open-source libraries.

---

### 3. Is combining building and computation in `build_and_compute()` a good practice?

**Context:**
- I am using the builder pattern to construct complex objects in Chapaty.
- The pattern includes a `build_and_compute()` method that both finalizes the object and performs a computation.

**Question:**
- In "general", is it considered good practice to combine the construction and computation of an object in a single method like `build_and_compute()`? I have a `XyzCalculator` whose sole purpose is to compute the `Xyz` object, and a builder to create the `XyzCalculator`.

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

---

### 4. How can I improve my documentation using Rustdoc?

**Context:**
- I want to make sure Chapaty is well-documented and easy to use for other developers.
- I am using Rustdoc but am unsure if I am utilizing it to its full potential.

**Question:**
- What are some best practices or tips for writing effective Rustdoc comments?

**Expectations for Feedback:**
- Examples of well-documented Rust projects.
- Specific Rustdoc features that are commonly overlooked.

---