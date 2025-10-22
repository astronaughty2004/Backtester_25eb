# Backtester

A modular, daywise backtesting framework for algorithmic trading strategies. Supports price-only data with comprehensive risk management, execution simulation, and performance reporting.

## Features

- **Modular Architecture**: Separation of concerns with pluggable components
- **Daywise P&L Tracking**: Track daily performance with optional EOD position square-off
- **Price-Only Data**: Optimized for simple price data (OHLCV optional)
- **Multi-Timeframe**: Built-in resampling and alignment
- **Risk Management**: Position sizing (fraction-based, volatility-based), leverage limits
- **Execution Simulation**: First-touch intrabar logic with slippage and commissions
- **Comprehensive Metrics**: CAGR, Sharpe, Sortino, drawdown, VaR/CVaR, trade statistics
- **Reporting**: CSV trade logs, JSON metrics, equity/drawdown plots

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

### 1. Prepare Configuration

Create a `config.yaml` file:

```yaml
data:
  file_path: "data/prices.csv"
  symbol: "AAPL"
  timestamp_col: "timestamp"
  price_col: "price"
  timeframe: "1min"

capital:
  initial_capital: 100000

execution:
  commission_pct: 0.001
  slippage_bps: 5.0

risk:
  max_position_pct: 0.2
  max_leverage: 1.0
  sizing_method: "fraction"
  sizing_value: 0.1

eod:
  square_off_positions: true
  enable_daily_pnl: true

reporting:
  generate_reports: true
  output_dir: "results"
```

### 2. Run Backtest

```bash
python -m src.backtester.cli --config config.yaml --strategy ma_cross
```

## Usage

### Command Line Interface

```bash
python -m src.backtester.cli \
  --config config.yaml \
  --strategy ma_cross \
  --log-level INFO
```

**Arguments:**
- `--config`: Path to config YAML (required)
- `--strategy`: Strategy to use (`ma_cross`, `buy_hold`)
- `--log-level`: Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- `--log-file`: Optional log file path

### Data Format

**CSV Example:**
```csv
timestamp,price
2024-01-01 09:30:00,100.50
2024-01-01 09:31:00,100.75
```

**Parquet:** Same columns as CSV

### Custom Strategies

Implement the `StrategyBase` interface:

```python
from src.backtester.strategy import StrategyBase
from src.backtester.models import Bar, Signal, SignalType

class MyStrategy(StrategyBase):
    def on_bar(self, bar: Bar):
        # Update internal state
        pass
    
    def generate_signal(self, bar: Bar) -> Signal:
        # Your logic here
        if should_buy:
            return Signal(
                timestamp=bar.timestamp,
                symbol=bar.symbol,
                signal_type=SignalType.LONG,
                target_quantity=None  # Let risk manager decide
            )
        return None
```

## Configuration Reference

### Data Section
- `file_path`: Path to data file (CSV/Parquet)
- `symbol`: Trading symbol
- `timestamp_col`: Timestamp column name
- `price_col`: Price column name
- `timeframe`: Bar timeframe (e.g., `1min`, `5min`, `1h`)
- `start_date`, `end_date`: Optional date filtering

### Risk Section
- `sizing_method`: `fraction` or `volatility`
- `sizing_value`: Fraction of capital or volatility target
- `max_position_pct`: Max position size as % of capital
- `max_leverage`: Max portfolio leverage
- `max_positions`: Max concurrent positions

### EOD Section
- `square_off_positions`: Close all positions at EOD
- `square_off_time`: Time to square off (HH:MM:SS)
- `enable_daily_pnl`: Track daily P&L

## Output

### Trade Sheet (`trade_sheet.csv`)
All fills with timestamps, prices, P&L

### Metrics (`metrics.json`, `metrics.txt`)
- Return metrics: Total return, CAGR
- Risk metrics: Volatility, Sharpe, Sortino, drawdown, VaR/CVaR
- Trade metrics: Win rate, profit factor, expectancy

### Charts
- `equity_curve.png`: Portfolio value over time
- `drawdown.png`: Drawdown visualization
- `returns_dist.png`: Returns distribution histogram

## Architecture

```
Data Layer:        loader.py, resampler.py
Execution Layer:   execution.py, order_manager.py, portfolio.py
Strategy Layer:    strategy.py, signal_manager.py
Risk Layer:        risk_manager.py
Reporting Layer:   metrics.py, reports.py
Orchestration:     engine.py, cli.py
```

## License

MIT
