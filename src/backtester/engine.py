# Bar-by-bar loop that: loads bars, calls strategy hooks, records snapshots
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from pathlib import Path

from .models import Bar, PortfolioSnapshot, Fill
from .config import BacktesterConfig
from .data.loader import DataLoader
from .data.resampler import Resampler
from .strategy import StrategyBase
from .signal_manager import SignalManager
from .order_manager import OrderManager
from .risk_manager import RiskManager
from .portfolio import Portfolio
from .execution import ExecutionEngine
from .metrics import MetricsCalculator
from .reports import ReportGenerator

logger = logging.getLogger(__name__)


class BacktestEngine:
    """Main backtest engine - orchestrates all components"""
    
    def __init__(self, config: BacktesterConfig):
        """
        Initialize backtest engine
        
        Args:
            config: Backtest configuration
        """
        self.config = config
        
        # Initialize components
        self.loader = DataLoader()
        self.resampler = Resampler()
        self.execution_engine = ExecutionEngine(
            commission_pct=config.execution.commission_pct,
            slippage_bps=config.execution.slippage_bps
        )
        self.risk_manager = RiskManager(config.risk)
        self.order_manager = OrderManager(self.execution_engine)
        self.portfolio = Portfolio(
            initial_capital=config.capital.initial_capital,
            enable_daily_pnl=config.eod.enable_daily_pnl
        )
        self.signal_manager = SignalManager()
        
        # Storage
        self.bars: List[Bar] = []
        self.snapshots: List[PortfolioSnapshot] = []
        
        # State tracking
        self.current_date: Optional[datetime.date] = None
        self.strategy: Optional[StrategyBase] = None
        
        logger.info(f"BacktestEngine initialized: {config.data.symbol}")
    
    def load_data(self) -> List[Bar]:
        """
        Load data from file
        
        Returns:
            List of bars
        """
        logger.info(f"Loading data: {self.config.data.file_path}")
        
        # Determine file type
        file_path = Path(self.config.data.file_path)
        if file_path.suffix == '.csv':
            bars = self.loader.load_csv(
                file_path=str(file_path),
                timestamp_col=self.config.data.timestamp_col,
                price_col=self.config.data.price_col,
                symbol=self.config.data.symbol,
                start_date=self.config.data.start_date,
                end_date=self.config.data.end_date
            )
        elif file_path.suffix == '.parquet':
            bars = self.loader.load_parquet(
                file_path=str(file_path),
                timestamp_col=self.config.data.timestamp_col,
                price_col=self.config.data.price_col,
                symbol=self.config.data.symbol,
                start_date=self.config.data.start_date,
                end_date=self.config.data.end_date
            )
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")
        
        # Resample if needed
        if self.config.data.timeframe != '1min':
            logger.info(f"Resampling to {self.config.data.timeframe}")
            bars = self.resampler.resample(bars, self.config.data.timeframe)
        
        logger.info(f"Data loaded: {len(bars)} bars")
        return bars
    
    def set_strategy(self, strategy: StrategyBase):
        """
        Set strategy
        
        Args:
            strategy: Strategy instance
        """
        self.strategy = strategy
        logger.info(f"Strategy set: {strategy.__class__.__name__}")
    
    def on_bar(self, bar: Bar):
        """
        Process single bar
        
        Args:
            bar: Current bar
        """
        # Update strategy state
        if self.strategy:
            self.strategy.on_bar(bar)
            
            # Get signals from strategy
            signal = self.strategy.generate_signal(bar)
            if signal:
                self.signal_manager.add_signal(signal)
        
        # Get pending signals for this timestamp
        signals = self.signal_manager.get_signals_for_timestamp(bar.timestamp)
        
        # Convert signals to orders with risk management
        for signal in signals:
            order = self.risk_manager.create_order_from_signal(
                signal=signal,
                current_price=bar.price,
                portfolio=self.portfolio
            )
            
            if order:
                self.order_manager.submit_order(order)
        
        # Process orders (check for fills)
        fills = self.order_manager.process_bar(bar)
        
        # Apply fills to portfolio
        for fill in fills:
            self.portfolio.apply_fill(fill)
            
            # Notify strategy of fill
            if self.strategy:
                self.strategy.on_fill(fill)
        
        # Update portfolio with current prices
        self.portfolio.update_prices({bar.symbol: bar.price})
        
        # Create snapshot
        snapshot = self.portfolio.create_snapshot(bar.timestamp)
        self.snapshots.append(snapshot)
    
    def on_day_start(self, date: datetime.date):
        """
        Handle start of day
        
        Args:
            date: Current date
        """
        logger.debug(f"Day start: {date}")
        
        if self.strategy:
            self.strategy.on_day_start(date)
    
    def on_day_end(self, date: datetime.date):
        """
        Handle end of day
        
        Args:
            date: Current date
        """
        logger.debug(f"Day end: {date}")
        
        # Square off positions if enabled
        if self.config.eod.square_off_positions:
            positions = list(self.portfolio.positions.values())
            for position in positions:
                logger.info(f"EOD square off: {position.symbol} qty={position.quantity}")
                # Strategy handles square-off signal generation
                if self.strategy:
                    self.strategy.on_day_end(date)
        
        # Record daily P&L
        if self.config.eod.enable_daily_pnl:
            daily_pnl = self.portfolio.get_daily_pnl()
            logger.info(f"Daily P&L: ${daily_pnl:,.2f}")
    
    def run(self) -> Dict:
        """
        Run backtest
        
        Returns:
            Dict with results
        """
        if not self.strategy:
            raise ValueError("Strategy not set. Call set_strategy() first.")
        
        logger.info("=" * 60)
        logger.info("BACKTEST START")
        logger.info("=" * 60)
        
        # Load data
        self.bars = self.load_data()
        
        if not self.bars:
            raise ValueError("No data loaded")
        
        # Initialize strategy
        self.strategy.on_start()
        
        # Bar-by-bar loop
        logger.info(f"Processing {len(self.bars)} bars...")
        
        for i, bar in enumerate(self.bars):
            # Check for day boundary
            bar_date = bar.timestamp.date()
            
            if self.current_date is None:
                # First bar
                self.current_date = bar_date
                self.on_day_start(bar_date)
            elif bar_date != self.current_date:
                # New day
                self.on_day_end(self.current_date)
                self.current_date = bar_date
                self.on_day_start(bar_date)
            
            # Process bar
            self.on_bar(bar)
            
            # Log progress
            if (i + 1) % 1000 == 0:
                logger.info(f"Processed {i + 1}/{len(self.bars)} bars")
        
        # Handle last day
        if self.current_date:
            self.on_day_end(self.current_date)
        
        # Finalize strategy
        self.strategy.on_end()
        
        logger.info("=" * 60)
        logger.info("BACKTEST END")
        logger.info("=" * 60)
        
        # Calculate metrics
        logger.info("Calculating metrics...")
        metrics_calc = MetricsCalculator()
        metrics = metrics_calc.calculate_all_metrics(
            snapshots=self.snapshots,
            fills=self.order_manager.get_all_fills(),
            initial_capital=self.config.capital.initial_capital
        )
        
        # Log summary
        logger.info(f"Total Return: {metrics['total_return']*100:.2f}%")
        logger.info(f"CAGR: {metrics['cagr']*100:.2f}%")
        logger.info(f"Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
        logger.info(f"Max Drawdown: {metrics['max_drawdown']*100:.2f}%")
        logger.info(f"Number of Trades: {metrics['num_trades']}")
        logger.info(f"Win Rate: {metrics['win_rate']*100:.2f}%")
        
        # Generate reports if enabled
        if self.config.reporting.generate_reports:
            logger.info("Generating reports...")
            report_gen = ReportGenerator(self.config.reporting.output_dir)
            report_gen.generate_all_reports(
                snapshots=self.snapshots,
                fills=self.order_manager.get_all_fills(),
                metrics=metrics
            )
        
        # Return results
        return {
            'metrics': metrics,
            'snapshots': self.snapshots,
            'fills': self.order_manager.get_all_fills(),
            'portfolio': self.portfolio
        }
