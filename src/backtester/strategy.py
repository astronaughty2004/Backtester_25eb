# StrategyBase class + DayStrategy lifecycle + example EMA strategy
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from datetime import datetime, date
import logging
import pandas as pd
import numpy as np

from .models import Bar, Signal, Fill, OrderSide, Position

logger = logging.getLogger(__name__)


class StrategyBase(ABC):
    """Base class for all trading strategies"""
    
    def __init__(self, name: str = "Strategy"):
        """
        Initialize strategy
        
        Args:
            name: Strategy name
        """
        self.name = name
        self.signals: List[Signal] = []
        self.current_positions: Dict[str, Position] = {}
        
        logger.info(f"Strategy '{name}' initialized")
    
    def preprocess(self, data: Dict[str, List[Bar]]):
        """
        Preprocess data before backtest starts
        Override to compute indicators, etc.
        
        Args:
            data: Dict mapping symbol -> list of bars
        """
        pass
    
    @abstractmethod
    def on_bar(self, bar: Bar, **kwargs) -> Optional[List[Signal]]:
        """
        Called on each bar
        
        Args:
            bar: Current bar
            **kwargs: Additional data (e.g., other timeframes)
        
        Returns:
            List of signals or None
        """
        pass
    
    def on_fill(self, fill: Fill):
        """
        Called when order is filled
        Override to react to executions
        
        Args:
            fill: Fill object
        """
        pass
    
    def on_day_start(self, date: date):
        """
        Called at start of trading day
        Override for daily setup
        
        Args:
            date: Current date
        """
        pass
    
    def on_day_end(self, date: date):
        """
        Called at end of trading day
        Override for daily cleanup
        
        Args:
            date: Current date
        """
        pass
    
    def update_positions(self, positions: Dict[str, Position]):
        """Update current positions"""
        self.current_positions = positions.copy()
    
    def has_position(self, symbol: str) -> bool:
        """Check if has open position in symbol"""
        return symbol in self.current_positions and self.current_positions[symbol].quantity != 0
    
    def get_position_quantity(self, symbol: str) -> int:
        """Get current position quantity"""
        if symbol in self.current_positions:
            return self.current_positions[symbol].quantity
        return 0


class DayStrategy(StrategyBase):
    """Strategy with explicit day lifecycle"""
    
    def __init__(self, name: str = "DayStrategy", square_off_eod: bool = True):
        """
        Initialize day strategy
        
        Args:
            name: Strategy name
            square_off_eod: Generate exit signals at EOD
        """
        super().__init__(name)
        self.square_off_eod = square_off_eod
        self.current_day: Optional[date] = None
    
    def on_day_end(self, date: date):
        """Square off all positions at EOD if enabled"""
        if self.square_off_eod:
            signals = []
            for symbol, position in self.current_positions.items():
                if position.quantity != 0:
                    # Generate exit signal
                    side = OrderSide.SELL if position.quantity > 0 else OrderSide.BUY
                    signal = Signal(
                        timestamp=datetime.combine(date, datetime.max.time()),
                        symbol=symbol,
                        side=side,
                        size=abs(position.quantity),
                        reason="EOD Square Off"
                    )
                    signals.append(signal)
                    logger.info(f"EOD square off: {symbol} {side.value} {abs(position.quantity)}")
            
            return signals
        
        return None


class MovingAverageCrossStrategy(StrategyBase):
    """Example: Simple moving average crossover strategy"""
    
    def __init__(self,
                 fast_period: int = 10,
                 slow_period: int = 20,
                 name: str = "MA_Cross"):
        """
        Initialize MA crossover strategy
        
        Args:
            fast_period: Fast MA period
            slow_period: Slow MA period
            name: Strategy name
        """
        super().__init__(name)
        self.fast_period = fast_period
        self.slow_period = slow_period
        
        # Price history for MA calculation
        self.price_history: Dict[str, List[float]] = {}
        
        logger.info(f"MA Cross Strategy: fast={fast_period}, slow={slow_period}")
    
    def on_bar(self, bar: Bar, **kwargs) -> Optional[List[Signal]]:
        """Generate signals based on MA crossover"""
        symbol = bar.symbol
        price = bar.price
        
        # Update price history
        if symbol not in self.price_history:
            self.price_history[symbol] = []
        
        self.price_history[symbol].append(price)
        
        # Keep only necessary history
        max_period = max(self.fast_period, self.slow_period)
        if len(self.price_history[symbol]) > max_period + 1:
            self.price_history[symbol] = self.price_history[symbol][-(max_period + 1):]
        
        # Need enough data
        if len(self.price_history[symbol]) < self.slow_period:
            return None
        
        # Calculate MAs
        prices = np.array(self.price_history[symbol])
        fast_ma = np.mean(prices[-self.fast_period:])
        slow_ma = np.mean(prices[-self.slow_period:])
        
        # Previous MAs for crossover detection
        if len(prices) > self.slow_period:
            prev_fast_ma = np.mean(prices[-(self.fast_period+1):-1])
            prev_slow_ma = np.mean(prices[-(self.slow_period+1):-1])
        else:
            return None
        
        # Detect crossover
        signals = []
        
        # Bullish crossover
        if prev_fast_ma <= prev_slow_ma and fast_ma > slow_ma:
            if not self.has_position(symbol) or self.get_position_quantity(symbol) <= 0:
                signal = Signal(
                    timestamp=bar.timestamp,
                    symbol=symbol,
                    side=OrderSide.BUY,
                    reason=f"MA Cross: {fast_ma:.2f} > {slow_ma:.2f}"
                )
                signals.append(signal)
                logger.info(f"BUY signal: {symbol} @ {price:.2f} (fast={fast_ma:.2f}, slow={slow_ma:.2f})")
        
        # Bearish crossover
        elif prev_fast_ma >= prev_slow_ma and fast_ma < slow_ma:
            if self.get_position_quantity(symbol) > 0:
                signal = Signal(
                    timestamp=bar.timestamp,
                    symbol=symbol,
                    side=OrderSide.SELL,
                    reason=f"MA Cross: {fast_ma:.2f} < {slow_ma:.2f}"
                )
                signals.append(signal)
                logger.info(f"SELL signal: {symbol} @ {price:.2f} (fast={fast_ma:.2f}, slow={slow_ma:.2f})")
        
        return signals if signals else None


class BuyAndHoldStrategy(StrategyBase):
    """Simple buy and hold strategy"""
    
    def __init__(self, name: str = "BuyAndHold"):
        super().__init__(name)
        self.entered = False
    
    def on_bar(self, bar: Bar, **kwargs) -> Optional[List[Signal]]:
        """Buy on first bar if not already in position"""
        if not self.entered and not self.has_position(bar.symbol):
            self.entered = True
            signal = Signal(
                timestamp=bar.timestamp,
                symbol=bar.symbol,
                side=OrderSide.BUY,
                reason="Buy and Hold Entry"
            )
            return [signal]
        
        return None
