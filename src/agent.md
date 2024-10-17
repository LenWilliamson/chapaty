To model the transition from a **discrete approach** (where you evaluate trades at specific points) to a **continuous approach** (where the system constantly checks the market and strategies in real-time), you can make several architectural changes. The idea is to create a mechanism that monitors the market conditions on every OHLC candle and evaluates the strategies' conditions on an ongoing basis. You want to:
1. Continuously check for entries from multiple strategies.
2. Monitor the progress of ongoing trades (PnL, market movements).
3. Switch between strategies if conditions for a pivot are met (based on your decision rule or agent logic).
4. Exit trades when conditions are met (take profit, stop loss, or timeout).

Here’s a step-by-step breakdown of how to model this:

### 1. Track the State Continuously
You’ll need to introduce a **state** that tracks the progress of trades over time. For each OHLC candle, you check if the strategy triggers a trade, and once a trade is open, you continuously monitor the PnL and other conditions.

Create a structure to track the current state of the strategy, market, and trades:

```rust
pub struct TradeState {
    is_active: bool,
    entry_ts: Option<DateTime<Utc>>, // Timestamp of the entry
    trade: Option<Trade>,            // The active trade (if any)
    pnl: Option<TradePnL>,           // The current PnL of the trade
    strategy_name: String,           // Which strategy is active
}

impl TradeState {
    pub fn new() -> Self {
        TradeState {
            is_active: false,
            entry_ts: None,
            trade: None,
            pnl: None,
            strategy_name: String::new(),
        }
    }

    pub fn update(&mut self, trade: Option<Trade>, pnl: Option<TradePnL>, strategy_name: &str) {
        self.is_active = trade.is_some();
        self.trade = trade;
        self.pnl = pnl;
        self.strategy_name = strategy_name.to_string();
    }

    pub fn clear(&mut self) {
        self.is_active = false;
        self.entry_ts = None;
        self.trade = None;
        self.pnl = None;
        self.strategy_name.clear();
    }
}
```

### 2. Continuously Evaluate Strategies for Entry
As the market data (OHLC candles) is processed, you need to continuously check whether any of the strategies have generated an entry signal. This requires a loop over the available strategies and a way to decide when to pivot from one strategy to another.

```rust
impl Agent {
    pub fn run(&mut self, market_data: &[OhlcData]) -> PnLReportDataRow {
        let mut state = TradeState::new();  // Initialize trade state
        for candle in market_data.iter() {
            self.evaluate_candle(&candle, &mut state);  // Process each candle
            if let Some(ref trade) = state.trade {
                self.monitor_trade(&trade, &mut state);  // Continuously monitor the trade
            }
        }
        self.finalize_report(state)  // Once market data ends, finalize the report
    }

    fn evaluate_candle(&self, candle: &OhlcData, state: &mut TradeState) {
        if state.is_active {
            // Check if current trade meets exit conditions (timeout, take profit, stop loss)
            if self.check_exit_conditions(state, candle) {
                state.clear();
            }
        } else {
            // Check if any strategy has an entry signal
            for bot in &self.bots {
                let trade_signal = bot.strategy.get_trade_signal(candle);
                if trade_signal.is_some() {
                    state.update(trade_signal, None, &bot.strategy.get_name());
                    break;  // Exit loop once we get a trade signal
                }
            }
        }
    }

    fn monitor_trade(&self, trade: &Trade, state: &mut TradeState) {
        // Calculate PnL based on current market data and update trade state
        let pnl = self.calculate_pnl(trade, state);
        state.pnl = Some(pnl);
        
        // Check for pivot decision to switch strategies
        if self.should_pivot(state) {
            state.clear();  // Exit the current trade
            // Logic to switch to another strategy if needed
        }
    }

    fn check_exit_conditions(&self, state: &mut TradeState, candle: &OhlcData) -> bool {
        // Implement logic to check for exit conditions (take profit, stop loss, or timeout)
        // Return true if the trade should be exited
    }

    fn should_pivot(&self, state: &TradeState) -> bool {
        // Implement pivot logic: return true if we should switch to another strategy
    }

    fn calculate_pnl(&self, trade: &Trade, state: &TradeState) -> TradePnL {
        // Calculate current PnL based on market conditions
    }

    fn finalize_report(&self, state: TradeState) -> PnLReportDataRow {
        // Create final PnL report based on the state at the end of the market data
    }
}
```

### 3. Monitor the Trade in Real-Time
For every OHLC candle, the system will:
- **Evaluate the trade signal**: If no trade is active, check if any strategy generates an entry signal.
- **Monitor the PnL and market movements**: Once a trade is active, monitor it continuously. This involves calculating the trade’s PnL as new market data (OHLC candles) arrive.
- **Check for exit conditions**: Continuously check if the trade hits the stop loss, take profit, or end of day (timeout). If any of these conditions are met, the trade exits.

### 4. Pivot Between Strategies
You can implement **pivot logic** that allows the agent to switch between strategies during the trade.

For example:
- **Strategy B takes precedence**: If Strategy B generates an entry signal while Strategy A is active, the agent can exit the current trade and pivot to Strategy B.
  
This can be handled in the `should_pivot` function, where you decide when the conditions for switching to another strategy are met:

```rust
fn should_pivot(&self, state: &TradeState) -> bool {
    // Check if Strategy B has a signal while Strategy A is active
    for bot in &self.bots {
        if bot.strategy.get_name() == "Strategy B" {
            if bot.strategy.get_trade_signal(&state.trade).is_some() {
                return true;  // Pivot to Strategy B
            }
        }
    }
    false
}
```

### 5. Continuously Updating the PnL
The **PnL** calculation happens in real-time as new OHLC candles arrive. You need a function that updates the PnL based on how far the price has moved from the trade entry price:

```rust
fn calculate_pnl(&self, trade: &Trade, state: &TradeState) -> TradePnL {
    let current_price = self.get_current_market_price();
    let entry_price = trade.entry_price;
    let pnl = self.pnl_calculator.calculate(entry_price, current_price, &trade);
    pnl
}
```

### 6. Incorporating a Neural Network (Optional)
Once you have this continuous framework in place, you can then incorporate a **neural network** model (using a crate like `tch` for PyTorch). The NN can take the **market state** (e.g., OHLC data, volatility, momentum indicators) as input and predict whether to:
- **Stay in the current strategy** or
- **Pivot to a new strategy**.

You can train the neural network on historical data to learn these decision rules, and use it as part of the `should_pivot` logic.

---

### Summary

In this continuous approach:
- You evaluate strategies on every new OHLC candle, rather than only at specific times.
- The system constantly monitors ongoing trades, recalculates PnL, and checks if exit conditions are met.
- The agent can decide whether to pivot between strategies based on predefined rules or using a neural network.

This gives you a more dynamic and adaptable framework where the agent is always "aware" of the current market conditions and can make decisions in real-time.

---
---
---

# FSM two states

Modeling your trading system as a **Finite State Machine (FSM)** is a highly appropriate approach. In a FSM, the system transitions between different "states" based on inputs (market conditions, strategy signals) and predefined rules (strategy execution, exit criteria, pivots). This makes it well-suited for your use case where the agent transitions between different strategies and actions depending on the evolving market data.

### Key Concepts of a Finite State Machine (FSM)

In an FSM, there are a few key components:
1. **States**: Different possible configurations the system can be in. Each state represents a particular scenario (e.g., no active trade, trade from Strategy A active, trade from Strategy B active, etc.).
2. **Transitions**: These are the rules or conditions that cause the system to move from one state to another (e.g., when a strategy generates a trade signal or when a stop-loss is hit).
3. **Events/Inputs**: External inputs that influence transitions between states. In your case, these would be OHLC data, trade signals from strategies, or market conditions.
4. **Actions**: Actions that are performed when entering or leaving a state, or when transitioning between states (e.g., executing a trade, exiting a trade, pivoting between strategies).

### Example FSM for Your Trading System

#### States

1. **Idle**: No trade is currently active.
2. **Trade Active: Strategy A**: A trade is active using Strategy A.
3. **Trade Active: Strategy B**: A trade is active using Strategy B.
4. **Exiting**: The system is in the process of closing a trade due to a take-profit, stop-loss, or end-of-day condition.
5. **Pivoting**: A pivot decision is being made to switch between strategies (e.g., switching from Strategy A to Strategy B).

#### Transitions

- **Idle → Trade Active (Strategy A)**: A valid trade signal is generated by Strategy A.
- **Idle → Trade Active (Strategy B)**: A valid trade signal is generated by Strategy B.
- **Trade Active (Strategy A) → Exiting**: Exit conditions are met (take-profit, stop-loss, timeout) for Strategy A.
- **Trade Active (Strategy B) → Exiting**: Exit conditions are met for Strategy B.
- **Trade Active (Strategy A) → Pivoting**: A pivot condition is met (Strategy B issues a stronger signal while Strategy A is active).
- **Pivoting → Trade Active (Strategy B)**: Pivoting results in a transition to Strategy B.
- **Exiting → Idle**: After the trade is closed, the system goes back to Idle.

### Example Diagram

```
            +---------------------+
            |         Idle         |
            +---------------------+
                     |
           Strategy A signal --> Trade Active (Strategy A)  --+  
                     |                                        |--> Pivoting --> Trade Active (Strategy B)
           Strategy B signal --> Trade Active (Strategy B)  --+
                     |  
              Exit conditions
                     |
               +---------------+
               |    Exiting     |
               +---------------+
                     |
                  Trade closed
                     |
                   Idle
```

### Implementing the FSM in Rust

To implement the FSM, you can define an enum representing the different states and then a struct that handles the state transitions. Here's how you might start:

#### 1. Define the States

```rust
pub enum TradeState {
    Idle,
    TradeActiveStrategyA { trade: Trade },
    TradeActiveStrategyB { trade: Trade },
    Exiting { trade: Trade },
    Pivoting { from_strategy: String, to_strategy: String },
}
```

#### 2. Define the Transitions

```rust
impl TradeState {
    pub fn next(self, event: TradeEvent) -> TradeState {
        match self {
            TradeState::Idle => match event {
                TradeEvent::SignalFromStrategyA(trade) => TradeState::TradeActiveStrategyA { trade },
                TradeEvent::SignalFromStrategyB(trade) => TradeState::TradeActiveStrategyB { trade },
                _ => self,
            },
            TradeState::TradeActiveStrategyA { trade } => match event {
                TradeEvent::ExitConditionsMet => TradeState::Exiting { trade },
                TradeEvent::PivotToStrategyB => TradeState::Pivoting {
                    from_strategy: "Strategy A".to_string(),
                    to_strategy: "Strategy B".to_string(),
                },
                _ => self,
            },
            TradeState::TradeActiveStrategyB { trade } => match event {
                TradeEvent::ExitConditionsMet => TradeState::Exiting { trade },
                _ => self,
            },
            TradeState::Exiting { trade } => TradeState::Idle,
            TradeState::Pivoting { from_strategy, to_strategy } => {
                if to_strategy == "Strategy B" {
                    TradeState::TradeActiveStrategyB { trade: Trade::new() }
                } else {
                    TradeState::TradeActiveStrategyA { trade: Trade::new() }
                }
            }
        }
    }
}
```

#### 3. Define Events

You'll also need to define the **events** that trigger transitions between states:

```rust
pub enum TradeEvent {
    SignalFromStrategyA(Trade),
    SignalFromStrategyB(Trade),
    ExitConditionsMet,
    PivotToStrategyB,
    Timeout,
}
```

#### 4. Implementing the FSM in Your Agent

Now, your agent can handle state transitions in a loop over the OHLC data, using the FSM to manage which strategy to execute, when to exit, and when to pivot:

```rust
impl Agent {
    pub fn run_fsm(&mut self, market_data: &[OhlcData]) {
        let mut state = TradeState::Idle;
        
        for candle in market_data.iter() {
            match state {
                TradeState::Idle => {
                    if let Some(trade) = self.strategy_a.get_trade_signal(candle) {
                        state = state.next(TradeEvent::SignalFromStrategyA(trade));
                    } else if let Some(trade) = self.strategy_b.get_trade_signal(candle) {
                        state = state.next(TradeEvent::SignalFromStrategyB(trade));
                    }
                }
                TradeState::TradeActiveStrategyA { trade } => {
                    if self.check_exit_conditions(&trade, candle) {
                        state = state.next(TradeEvent::ExitConditionsMet);
                    } else if self.strategy_b.has_stronger_signal(candle) {
                        state = state.next(TradeEvent::PivotToStrategyB);
                    }
                }
                TradeState::TradeActiveStrategyB { trade } => {
                    if self.check_exit_conditions(&trade, candle) {
                        state = state.next(TradeEvent::ExitConditionsMet);
                    }
                }
                _ => {}
            }
        }
    }
}
```

### Advantages of Using FSM

1. **Clarity**: FSM provides a clear structure to manage the different stages of the trading process (idle, trading, exiting, pivoting). This is especially useful when handling multiple strategies and complex decision-making.
2. **Extensibility**: You can easily add new states or transitions (e.g., more complex pivoting logic) without changing the overall architecture.
3. **Deterministic**: An FSM makes it easier to reason about the system’s behavior and ensure it transitions through states predictably.
4. **Modularity**: Each state and transition is well-defined, making the system easier to test and debug.

### Extending FSM with a Neural Network

Once you have the FSM structure in place, adding a neural network (NN) decision mechanism becomes easier. Instead of using hard-coded rules for pivoting between strategies, you can train an NN model to predict the best action (e.g., continue, pivot, or exit) based on features extracted from the market data. The NN would output a probability or decision, which would trigger transitions in the FSM.

---

### Summary

- **FSM** provides a structured way to manage your trading system’s states and transitions, making it ideal for switching between strategies (A and B) and handling different phases (entry, active trade, exit).
- By defining **clear states**, **events**, and **transitions**, your system becomes more modular, extensible, and easier to understand.
- This approach can easily be extended to incorporate more sophisticated decision-making, such as using neural networks to manage strategy pivots.


---
---
---

# FSM Multiple States
If you have **n strategies** instead of just two, the Finite State Machine (FSM) approach can still be effective, but it will need some generalization. Here's how you can model this system efficiently when dealing with multiple strategies:

### Key Adaptations for Multiple Strategies

1. **State Representation**: Instead of hardcoding for "Strategy A" and "Strategy B," the states and transitions should be generalized to support any number of strategies.
   
2. **Strategy Selection**: You need a way to dynamically choose the best strategy from a set of available strategies based on the current market conditions or other factors (such as previous performance).

3. **Action-Oriented Transitions**: Rather than transitioning directly between specific strategies, you can have a decision state where the system chooses the best strategy based on a decision policy or rule (which can later be replaced with an NN).

### Generalized FSM for Multiple Strategies

#### 1. **State Definition**

Instead of states like `TradeActiveStrategyA` or `TradeActiveStrategyB`, you’ll have a more generic state for any active strategy, where you track the strategy currently in use.

```rust
pub enum TradeState {
    Idle,
    TradeActive { strategy: String, trade: Trade },
    Exiting { trade: Trade },
    Pivoting { from_strategy: String, to_strategy: String },
}
```

- **Idle**: No trade is active.
- **TradeActive**: A trade is active using a particular strategy, which is dynamically defined by its name (or ID).
- **Exiting**: In the process of closing a trade due to exit conditions.
- **Pivoting**: Transitioning from one strategy to another based on certain criteria.

#### 2. **Event Definition**

The events will also need to handle multiple strategies. You can generalize the events to signal trades or pivots from any strategy.

```rust
pub enum TradeEvent {
    SignalFromStrategy { strategy_name: String, trade: Trade },
    ExitConditionsMet,
    PivotToStrategy { new_strategy_name: String },
    Timeout,
}
```

- **SignalFromStrategy**: Indicates that a specific strategy has signaled a trade.
- **PivotToStrategy**: Indicates a need to switch from the current strategy to another strategy.

#### 3. **Handling n Strategies**

You'll maintain a **list of strategies** and check each strategy in the loop to see which ones are signaling trades. The decision of which strategy to execute can be based on a ranking system, predefined rules, or a neural network. You may also want to implement priority rules to handle cases where multiple strategies signal simultaneously.

#### 4. **Generalized FSM Logic**

Here’s an example of how to generalize the FSM to handle multiple strategies:

```rust
impl TradeState {
    pub fn next(self, event: TradeEvent) -> TradeState {
        match self {
            TradeState::Idle => match event {
                TradeEvent::SignalFromStrategy { strategy_name, trade } => {
                    TradeState::TradeActive { strategy: strategy_name, trade }
                }
                _ => self,
            },
            TradeState::TradeActive { strategy, trade } => match event {
                TradeEvent::ExitConditionsMet => TradeState::Exiting { trade },
                TradeEvent::PivotToStrategy { new_strategy_name } => TradeState::Pivoting {
                    from_strategy: strategy,
                    to_strategy: new_strategy_name,
                },
                _ => self,
            },
            TradeState::Exiting { trade } => TradeState::Idle,
            TradeState::Pivoting { from_strategy, to_strategy } => {
                // Transition to the new strategy
                TradeState::TradeActive { strategy: to_strategy, trade: Trade::new() }
            }
        }
    }
}
```

### Implementing the FSM in an Agent with Multiple Strategies

Now, you can iterate through the list of strategies to check for trade signals at each time step and handle state transitions accordingly.

```rust
impl Agent {
    pub fn run_fsm(&mut self, market_data: &[OhlcData]) {
        let mut state = TradeState::Idle;
        
        for candle in market_data.iter() {
            match state {
                TradeState::Idle => {
                    for strategy in &self.strategies {
                        if let Some(trade) = strategy.get_trade_signal(candle) {
                            state = state.next(TradeEvent::SignalFromStrategy {
                                strategy_name: strategy.get_name(),
                                trade,
                            });
                            break; // Exit loop once a trade is found
                        }
                    }
                }
                TradeState::TradeActive { ref strategy, ref trade } => {
                    if self.check_exit_conditions(trade, candle) {
                        state = state.next(TradeEvent::ExitConditionsMet);
                    } else {
                        for strategy in &self.strategies {
                            if strategy.has_better_signal(candle) {
                                state = state.next(TradeEvent::PivotToStrategy {
                                    new_strategy_name: strategy.get_name(),
                                });
                                break; // Exit loop on a pivot signal
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
}
```

### Decision Policies for Strategy Selection

Once you're handling multiple strategies, you can implement various policies to decide which strategy to execute:

#### 1. **Priority Rules**:
   - Pre-define priorities for each strategy.
   - The agent checks for signals from the highest-priority strategy first and defaults to lower-priority strategies if no signal is found.

#### 2. **Performance-Based Selection**:
   - Keep track of the historical performance of each strategy.
   - Use a moving average or performance score to select the best strategy based on recent performance.

#### 3. **Market Condition-Based Rules**:
   - Depending on the market conditions (volatility, trend, etc.), select strategies that historically perform better in those conditions.

#### 4. **Machine Learning Decision (Neural Network)**:
   - A neural network could learn to predict the best strategy based on features extracted from the market data.
   - The input to the network could be market features (price, volume, volatility) and outputs a probability distribution over the available strategies.
   
### Example: Strategy Selection with NN

When using a neural network, the logic could look something like this:

```rust
impl Agent {
    pub fn run_fsm_with_nn(&mut self, market_data: &[OhlcData]) {
        let mut state = TradeState::Idle;
        
        for candle in market_data.iter() {
            match state {
                TradeState::Idle => {
                    let strategy_name = self.nn_decision_policy(candle); // Predict best strategy
                    if let Some(trade) = self.get_strategy_by_name(strategy_name).get_trade_signal(candle) {
                        state = state.next(TradeEvent::SignalFromStrategy {
                            strategy_name,
                            trade,
                        });
                    }
                }
                TradeState::TradeActive { ref strategy, ref trade } => {
                    if self.check_exit_conditions(trade, candle) {
                        state = state.next(TradeEvent::ExitConditionsMet);
                    } else {
                        let new_strategy_name = self.nn_decision_policy(candle); // Predict if pivot needed
                        if new_strategy_name != strategy {
                            state = state.next(TradeEvent::PivotToStrategy {
                                new_strategy_name,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }
    
    fn nn_decision_policy(&self, candle: &OhlcData) -> String {
        // Use a trained neural network model to predict the best strategy
        let features = self.extract_features(candle);
        let predicted_strategy = self.nn_model.predict(features);
        predicted_strategy
    }
}
```

### Summary of FSM for n Strategies

- **State Management**: You can manage multiple strategies by defining generic states and transitions in your FSM.
- **Dynamic Strategy Selection**: At each time step, the agent can evaluate which strategy should be active based on predefined rules or a decision-making model (NN).
- **Scalable**: The FSM approach is scalable and easily allows for adding or removing strategies without significant changes to the overall system.
- **Machine Learning**: A neural network can be used to select strategies based on learned patterns in the market data, making your agent adaptive and flexible.

By designing the agent this way, you allow it to manage multiple strategies efficiently while keeping the system extensible and modular.

---
---
---

# Acceptance States

To model the accepting states "Idle" and "Exiting" in a Finite State Machine (FSM) for your trading bot system, you can treat **Idle** and **Exiting** as two critical states that represent distinct moments in the trading process. Here’s how to approach these states with the conditions you've described:

1. **Idle State**: 
   - This is the state when the agent is not engaged in any trade. It will remain idle until:
     - A valid trade signal from one of the strategies is detected.
     - The market session ends without any trade signal (indicating "No Entry").
   
2. **Exiting State**: 
   - This is the state when the agent has an active trade, but needs to exit either:
     - By meeting one of the trade's exit conditions (like Stop Loss or Take Profit).
     - If no exit conditions are met and the market session ends (indicating a "Timeout" exit).

Here’s how you can model the system to account for these two scenarios and manage state transitions.

### States and Transitions

We define states and transitions for the FSM in terms of how the market evolves over time, particularly how the agent responds to new candles or the end of the day.

#### 1. **Idle State**
- **Definition**: The agent is not in a trade.
- **Transitions**:
  - Stay in `Idle` if no trade signals occur.
  - Transition to `TradeActive` if a trade signal occurs.
  - End in a "No Entry" if the session (market day) ends and no trades have been entered.

#### 2. **TradeActive State**
- **Definition**: The agent has entered a trade using a specific strategy.
- **Transitions**:
  - Transition to `Exiting` when Stop Loss, Take Profit, or Timeout conditions are met.
  - Stay in `TradeActive` as long as the trade is open and no exit conditions are met.
  - Transition to `Exiting` if the market session ends, triggering a timeout exit.

#### 3. **Exiting State**
- **Definition**: The agent is exiting a trade.
- **Transitions**:
  - End in a completed trade (PnL is calculated).
  - Transition back to `Idle` after the trade is closed.

### Event Handling

Here are the key events that influence state transitions:

- **TradeSignal**: A strategy signals a trade (buy or sell).
- **ExitConditionsMet**: A trade hits Stop Loss, Take Profit, or another exit condition.
- **Timeout**: The market session ends, triggering an automatic trade exit.
- **EndOfDay**: No trade signal occurs and the market session ends, meaning "No Entry."

### FSM Model with Idle and Exiting States

We can modify the FSM to handle these cases explicitly by incorporating `Idle` and `Exiting` as accepting states.

```rust
pub enum TradeState {
    Idle,
    TradeActive { strategy: String, trade: Trade },
    Exiting { trade: Trade },
    Timeout,
    NoEntry,
}
```

### TradeState Transitions

- **Idle**: The agent starts in the `Idle` state.
  - **Transition to `TradeActive`**: When a strategy signals a trade.
  - **Transition to `NoEntry`**: When the market session ends without any trade signals.
  
- **TradeActive**: The agent is in an active trade.
  - **Transition to `Exiting`**: If exit conditions are met (Stop Loss, Take Profit).
  - **Transition to `Timeout`**: When the market session ends without meeting any exit conditions, causing a forced exit (timeout).
  
- **Exiting**: The trade is closing.
  - **Transition to `Idle`**: After the trade is closed, returning to idle for the next session.

### Code Implementation of FSM for Idle and Exiting

Here’s how you might model this using Rust, keeping track of state transitions:

```rust
impl TradeState {
    pub fn next(self, event: TradeEvent, market_session_ended: bool) -> TradeState {
        match self {
            // If Idle, we can either enter a trade or remain idle until the session ends.
            TradeState::Idle => match event {
                TradeEvent::SignalFromStrategy { strategy_name, trade } => {
                    TradeState::TradeActive { strategy: strategy_name, trade }
                }
                _ if market_session_ended => TradeState::NoEntry, // No trade, end of session.
                _ => self, // Remain idle.
            },
            // If a trade is active, we can either meet an exit condition or hit a timeout.
            TradeState::TradeActive { strategy, trade } => match event {
                TradeEvent::ExitConditionsMet => TradeState::Exiting { trade },
                _ if market_session_ended => TradeState::Timeout, // End of session forces timeout.
                _ => self, // Continue active trade.
            },
            // When Exiting, we return to Idle.
            TradeState::Exiting { .. } => TradeState::Idle,
            // After a timeout, we also return to Idle, but a PnL calculation should happen.
            TradeState::Timeout => TradeState::Idle,
            // If no trade was entered, we return to Idle after market ends.
            TradeState::NoEntry => TradeState::Idle,
        }
    }
}
```

### Event Management

To handle events, you'll need logic that continuously monitors the market data and session time. The events will guide the state transitions.

```rust
impl Agent {
    pub fn run_fsm(&mut self, market_data: &[OhlcData]) {
        let mut state = TradeState::Idle;
        let mut session_ended = false;
        
        for candle in market_data.iter() {
            session_ended = self.check_if_market_session_ended(candle);
            
            match state {
                // Idle: looking for trade signals
                TradeState::Idle => {
                    for strategy in &self.strategies {
                        if let Some(trade) = strategy.get_trade_signal(candle) {
                            state = state.next(TradeEvent::SignalFromStrategy {
                                strategy_name: strategy.get_name(),
                                trade,
                            }, session_ended);
                            break;
                        }
                    }
                    // Check if session ends and no entry was made
                    if session_ended {
                        state = state.next(TradeEvent::NoEvent, session_ended);
                    }
                }
                
                // TradeActive: monitoring the trade
                TradeState::TradeActive { ref strategy, ref trade } => {
                    if self.check_exit_conditions(trade, candle) {
                        state = state.next(TradeEvent::ExitConditionsMet, session_ended);
                    } else if session_ended {
                        state = state.next(TradeEvent::Timeout, session_ended);
                    }
                }

                // Exiting or Timeout states
                TradeState::Exiting { .. } | TradeState::Timeout => {
                    state = TradeState::Idle;
                }

                // Handle NoEntry state
                TradeState::NoEntry => {
                    state = TradeState::Idle; // Simply return to idle
                }
            }
        }
    }
    
    fn check_if_market_session_ended(&self, candle: &OhlcData) -> bool {
        // Logic to check if the market session has ended.
    }
    
    fn check_exit_conditions(&self, trade: &Trade, candle: &OhlcData) -> bool {
        // Logic to check if any of the exit conditions (e.g., SL, TP) are met.
    }
}
```

### Summary

- **Idle State**: The agent waits for a signal. If none is found and the market session ends, the system triggers a "No Entry" event, marking the session as idle.
- **TradeActive State**: When in a trade, the agent monitors the market. If exit conditions are met, the agent transitions to the `Exiting` state. If the market session ends without exit conditions being met, the agent transitions to the `Timeout` state.
- **Exiting State**: After exiting the trade (either by regular exit conditions or a timeout), the agent transitions back to `Idle`, ready for the next trading session.

This approach introduces clear handling for "no entry" and timeout events, ensuring that trades are exited properly at the end of the day and no unexpected behavior occurs when a session closes.

---
---
---

# State Machine

For a trading state machine, we can model the system using **type states** just like the file reading example but adapted to a trading bot's lifecycle. We will define various states that the bot can be in during its operation:

### States:
1. **Idle**: No trade has been initiated; the bot is waiting for a signal from a strategy.
2. **TradeActive**: A trade is currently active and being managed.
3. **Exiting**: The bot is in the process of exiting the current trade, either due to a stop-loss, take-profit, timeout, or manual exit.
4. **Timeout**: No more candles or the trading day has ended, forcing the trade to exit automatically.

### Transitions:
1. From **Idle** to **TradeActive** when a strategy signals a new trade.
2. From **TradeActive** to **Exiting** when a signal to exit (take-profit, stop-loss, etc.) occurs or no more data is available (timeout).
3. From **Exiting** to **Idle** once the trade is closed and no further trade signals are present.

### Type States Design for FSM in Trading Bot

```rust
use std::marker::PhantomData;

// Define all state structs
struct Idle;
struct TradeActive;
struct Exiting;
struct Timeout;

// Market data simulation and trade results (simplified)
struct OHLC {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

struct Trade {
    entry: f64,
    stop_loss: f64,
    take_profit: f64,
}

// Base struct for the trading bot with a state
struct TradingBot<State> {
    ohlc_data: Vec<OHLC>, // Simulating the OHLC candles
    trade: Option<Trade>,  // Current trade (if any)
    current_candle: usize, // Tracks the current candle being processed
    _state: PhantomData<State>,
}

impl TradingBot<Idle> {
    // Start in the Idle state
    pub fn new(ohlc_data: Vec<OHLC>) -> Self {
        Self {
            ohlc_data,
            trade: None,
            current_candle: 0,
            _state: PhantomData,
        }
    }

    // Transition from Idle to TradeActive when a signal is received
    pub fn signal_trade(self, entry: f64, stop_loss: f64, take_profit: f64) -> TradingBot<TradeActive> {
        let trade = Trade {
            entry,
            stop_loss,
            take_profit,
        };
        println!("New trade signaled: Entry at {}", entry);
        TradingBot {
            ohlc_data: self.ohlc_data,
            trade: Some(trade),
            current_candle: self.current_candle,
            _state: PhantomData,
        }
    }
}

impl TradingBot<TradeActive> {
    // In TradeActive, process each candle and monitor for exits
    pub fn on_candle(self) -> TradingBot<TradeActive> {
        if let Some(current_ohlc) = self.ohlc_data.get(self.current_candle) {
            let trade = self.trade.as_ref().unwrap();
            println!("Processing candle: {:?}", current_ohlc);

            // Check if the trade should exit (take profit or stop loss)
            if current_ohlc.low <= trade.stop_loss {
                println!("Trade hit stop loss at {}", trade.stop_loss);
                return self.exit_trade();
            }
            if current_ohlc.high >= trade.take_profit {
                println!("Trade hit take profit at {}", trade.take_profit);
                return self.exit_trade();
            }

            // If still active, advance to the next candle
            return TradingBot {
                ohlc_data: self.ohlc_data,
                trade: self.trade,
                current_candle: self.current_candle + 1,
                _state: PhantomData,
            };
        }

        // No more candles, trigger timeout exit
        self.timeout_exit()
    }

    // Transition to the Exiting state when trade is done
    fn exit_trade(self) -> TradingBot<Exiting> {
        println!("Exiting trade...");
        TradingBot {
            ohlc_data: self.ohlc_data,
            trade: self.trade,
            current_candle: self.current_candle,
            _state: PhantomData,
        }
    }

    // Transition to Timeout if the day ends without hitting stop loss or take profit
    fn timeout_exit(self) -> TradingBot<Timeout> {
        println!("Timeout: No more candles, exiting trade.");
        TradingBot {
            ohlc_data: self.ohlc_data,
            trade: self.trade,
            current_candle: self.current_candle,
            _state: PhantomData,
        }
    }
}

impl TradingBot<Exiting> {
    // After exiting the trade, transition back to Idle to wait for the next signal
    pub fn complete_exit(self) -> TradingBot<Idle> {
        println!("Trade exited. Returning to Idle state.");
        TradingBot {
            ohlc_data: self.ohlc_data,
            trade: None,
            current_candle: self.current_candle,
            _state: PhantomData,
        }
    }
}

impl TradingBot<Timeout> {
    // Handle timeout event and return to Idle
    pub fn complete_timeout(self) -> TradingBot<Idle> {
        println!("Timeout completed. Returning to Idle state.");
        TradingBot {
            ohlc_data: self.ohlc_data,
            trade: None,
            current_candle: self.current_candle,
            _state: PhantomData,
        }
    }
}

fn main() {
    let ohlc_data = vec![
        OHLC {
            open: 1.1,
            high: 1.2,
            low: 1.0,
            close: 1.15,
        },
        OHLC {
            open: 1.15,
            high: 1.25,
            low: 1.1,
            close: 1.2,
        },
        OHLC {
            open: 1.2,
            high: 1.3,
            low: 1.15,
            close: 1.25,
        },
    ];

    // Start in the Idle state
    let bot = TradingBot::<Idle>::new(ohlc_data);

    // Simulate a trade signal and process the market
    let bot = bot.signal_trade(1.15, 1.05, 1.25);

    // Simulate processing candles in a loop
    let bot = bot.on_candle().on_candle().on_candle();

    // Exiting the trade
    let bot = bot.complete_exit();

    // We are back in the Idle state, ready for the next trade
}
```

### Key Concepts:
- **Idle State**: The bot waits for a signal from any strategy.
- **TradeActive State**: Once a signal is received, the bot monitors the trade, adjusting its state based on market data (e.g., OHLC candles). If the trade hits stop loss or take profit, the bot transitions to the **Exiting** state. If no exit occurs and the candles are finished, it transitions to the **Timeout** state.
- **Exiting State**: The bot exits the trade (whether by stop-loss, take-profit, or manual exit).
- **Timeout State**: If the trade times out (e.g., at the end of the trading day), the bot exits the trade automatically.

### Finite State Machine Transitions:
1. **Idle → TradeActive**: When a strategy signals a new trade.
2. **TradeActive → Exiting**: When stop-loss, take-profit, or other exit condition is met.
3. **TradeActive → Timeout**: If no more candles are available, the trade times out.
4. **Exiting → Idle**: Once the trade has been closed, the bot returns to idle.
5. **Timeout → Idle**: If the trade timed out, the bot returns to idle.

### Benefits of This Model:
- **Compile-time Safety**: The type system ensures that only valid transitions occur. For example, you cannot read OHLC candles or perform trade actions in the **Idle** state.
- **Encapsulation of Transitions**: Each state encapsulates its behavior, making the code cleaner and ensuring that state transitions are clear and safe.
- **Scalability**: More states and transitions can be easily added (e.g., a **Paused** state or handling special exit conditions).

This type-state pattern can extend to managing more complex trading strategies with multiple interacting bots, where each bot could have its own state machine, and an overarching agent can orchestrate the state of all bots involved.
