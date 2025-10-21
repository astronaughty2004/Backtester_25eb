# sizing functions: fraction_of_capital(), vol_based_size(), exposure checks
from typing import Optional, Dict, List
import logging
from datetime import datetime, timedelta
import math

from .models import Position, Order, OrderSide
from .utils import safe_divide, clamp


logger = logging.getLogger(__name__)


class RiskManager:
    """
    Manages position sizing and risk limits
    """
    
    def __init__(self,
                 max_position_pct: float = 0.20,
                 max_portfolio_leverage: float = 1.0,
                 max_positions: Optional[int] = None,
                 min_position_size: int = 1,
                 sizing_method: str = "fraction",
                 vol_lookback: int = 20,
                 target_vol: float = 0.15):
        """
        Initialize risk manager
        
        Args:
            max_position_pct: Max position size as fraction of portfolio (0.20 = 20%)
            max_portfolio_leverage: Max total leverage allowed
            max_positions: Max number of concurrent positions
            min_position_size: Minimum position size
            sizing_method: "fraction", "fixed", or "volatility"
            vol_lookback: Lookback period for volatility calculation
            target_vol: Target volatility for vol-based sizing
        """
        self.max_position_pct = max_position_pct
        self.max_portfolio_leverage = max_portfolio_leverage
        self.max_positions = max_positions
        self.min_position_size = min_position_size
        self.sizing_method = sizing_method
        self.vol_lookback = vol_lookback
        self.target_vol = target_vol
        
        logger.info(
            f"RiskManager initialized: max_position={max_position_pct*100}%, "
            f"max_leverage={max_portfolio_leverage}, sizing={sizing_method}"
        )
    
    def calculate_position_size(self,
                                symbol: str,
                                price: float,
                                portfolio_value: float,
                                signal_size: Optional[int] = None,
                                volatility: Optional[float] = None) -> int:
        """
        Calculate position size based on sizing method
        
        Args:
            symbol: Symbol to trade
            price: Current price
            portfolio_value: Total portfolio value
            signal_size: Requested size from signal (if provided)
            volatility: Historical volatility (if using vol-based sizing)
        
        Returns:
            Position size (quantity)
        """
        # If signal provides explicit size, use it (subject to limits)
        if signal_size is not None and signal_size > 0:
            size = signal_size
        
        # Otherwise, calculate based on sizing method
        elif self.sizing_method == "fraction":
            size = self.size_by_fraction(price, portfolio_value)
        
        elif self.sizing_method == "volatility":
            if volatility is None:
                logger.warning(f"Volatility-based sizing requested but volatility not provided for {symbol}")
                size = self.size_by_fraction(price, portfolio_value)
            else:
                size = self.size_by_volatility(price, portfolio_value, volatility)
        
        elif self.sizing_method == "fixed":
            size = self.min_position_size
        
        else:
            logger.warning(f"Unknown sizing method: {self.sizing_method}, using fraction")
            size = self.size_by_fraction(price, portfolio_value)
        
        # Apply minimum size
        size = max(size, self.min_position_size)
        
        # Round to integer
        size = int(size)
        
        logger.debug(f"Calculated position size for {symbol}: {size} @ {price:.2f}")
        
        return size
    
    def size_by_fraction(self, price: float, portfolio_value: float) -> int:
        """
        Size position as fraction of portfolio value
        
        Args:
            price: Current price
            portfolio_value: Total portfolio value
        
        Returns:
            Position size (quantity)
        """
        if price <= 0 or portfolio_value <= 0:
            return self.min_position_size
        
        # Calculate max capital to allocate
        max_capital = portfolio_value * self.max_position_pct
        
        # Calculate quantity
        quantity = max_capital / price
        
        return int(quantity)
    
    def size_by_volatility(self,
                          price: float,
                          portfolio_value: float,
                          volatility: float) -> int:
        """
        Size position based on volatility targeting
        
        Position size = (target_vol * portfolio_value) / (price * volatility)
        
        Args:
            price: Current price
            portfolio_value: Total portfolio value
            volatility: Historical volatility (annualized)
        
        Returns:
            Position size (quantity)
        """
        if price <= 0 or portfolio_value <= 0 or volatility <= 0:
            return self.min_position_size
        
        # Volatility-based sizing
        target_dollar_vol = self.target_vol * portfolio_value
        position_dollar_vol = price * volatility
        
        quantity = safe_divide(target_dollar_vol, position_dollar_vol, default=self.min_position_size)
        
        # Respect max position size limit
        max_capital = portfolio_value * self.max_position_pct
        max_quantity = max_capital / price
        
        quantity = min(quantity, max_quantity)
        
        return int(quantity)
    
    def check_position_limit(self,
                            symbol: str,
                            proposed_quantity: int,
                            price: float,
                            portfolio_value: float,
                            current_positions: Dict[str, Position]) -> tuple[bool, str]:
        """
        Check if proposed position size respects risk limits
        
        Args:
            symbol: Symbol to trade
            proposed_quantity: Proposed position quantity
            price: Current price
            portfolio_value: Total portfolio value
            current_positions: Current positions dict
        
        Returns:
            (is_allowed, reason)
        """
        # Check max number of positions
        if self.max_positions is not None:
            current_count = len([p for p in current_positions.values() if p.quantity != 0])
            
            # If adding new position
            if symbol not in current_positions or current_positions[symbol].quantity == 0:
                if current_count >= self.max_positions:
                    return False, f"Max positions limit reached ({self.max_positions})"
        
        # Check position size limit
        position_value = abs(proposed_quantity) * price
        max_position_value = portfolio_value * self.max_position_pct
        
        if position_value > max_position_value:
            return False, f"Position size ${position_value:.2f} exceeds max ${max_position_value:.2f}"
        
        # Check portfolio leverage
        total_exposure = sum(abs(p.quantity * price) for p in current_positions.values())
        new_exposure = total_exposure + abs(proposed_quantity * price)
        
        # Adjust if replacing existing position
        if symbol in current_positions:
            existing_exposure = abs(current_positions[symbol].quantity * price)
            new_exposure -= existing_exposure
        
        leverage = safe_divide(new_exposure, portfolio_value, default=0.0)
        
        if leverage > self.max_portfolio_leverage:
            return False, f"Portfolio leverage {leverage:.2f} exceeds max {self.max_portfolio_leverage}"
        
        return True, "OK"
    
    def check_exposure_limit(self,
                            proposed_quantity: int,
                            price: float,
                            portfolio_value: float,
                            current_positions: Dict[str, Position]) -> tuple[bool, str]:
        """
        Check if adding this position would exceed leverage limits
        
        Args:
            proposed_quantity: Proposed position quantity
            price: Current price
            portfolio_value: Total portfolio value
            current_positions: Current positions dict
        
        Returns:
            (is_allowed, reason)
        """
        if portfolio_value <= 0:
            return False, "Portfolio value is zero or negative"
        
        # Calculate total exposure
        current_exposure = sum(abs(p.quantity * price) for p in current_positions.values())
        new_position_value = abs(proposed_quantity) * price
        total_exposure = current_exposure + new_position_value
        
        leverage = total_exposure / portfolio_value
        
        if leverage > self.max_portfolio_leverage:
            return False, f"Total exposure ${total_exposure:.2f} exceeds max leverage {self.max_portfolio_leverage}"
        
        return True, "OK"
    
    def adjust_size_for_limits(self,
                               symbol: str,
                               proposed_quantity: int,
                               price: float,
                               portfolio_value: float,
                               current_positions: Dict[str, Position]) -> int:
        """
        Adjust position size to fit within risk limits
        
        Args:
            symbol: Symbol to trade
            proposed_quantity: Proposed position quantity
            price: Current price
            portfolio_value: Total portfolio value
            current_positions: Current positions dict
        
        Returns:
            Adjusted position size
        """
        if proposed_quantity <= 0:
            return 0
        
        # Check if proposed size is allowed
        is_allowed, reason = self.check_position_limit(
            symbol, proposed_quantity, price, portfolio_value, current_positions
        )
        
        if is_allowed:
            return proposed_quantity
        
        # Calculate max allowed size
        max_position_value = portfolio_value * self.max_position_pct
        max_quantity = int(max_position_value / price)
        
        # Also check leverage constraint
        current_exposure = sum(abs(p.quantity * price) for p in current_positions.values() if p.symbol != symbol)
        available_exposure = (portfolio_value * self.max_portfolio_leverage) - current_exposure
        max_quantity_leverage = int(available_exposure / price)
        
        # Take minimum of constraints
        adjusted_quantity = min(max_quantity, max_quantity_leverage, proposed_quantity)
        adjusted_quantity = max(adjusted_quantity, 0)
        
        logger.warning(
            f"Position size adjusted for {symbol}: {proposed_quantity} -> {adjusted_quantity} "
            f"(reason: {reason})"
        )
        
        return adjusted_quantity
    
    def calculate_stop_loss(self,
                           entry_price: float,
                           side: OrderSide,
                           stop_pct: Optional[float] = None,
                           atr: Optional[float] = None,
                           atr_multiplier: float = 2.0) -> Optional[float]:
        """
        Calculate stop loss price
        
        Args:
            entry_price: Entry price
            side: Order side (BUY/SELL)
            stop_pct: Stop loss percentage (e.g., 0.02 = 2%)
            atr: Average True Range for ATR-based stops
            atr_multiplier: ATR multiplier for stop distance
        
        Returns:
            Stop loss price
        """
        if stop_pct is not None:
            # Percentage-based stop
            if side == OrderSide.BUY:
                return entry_price * (1 - stop_pct)
            else:
                return entry_price * (1 + stop_pct)
        
        elif atr is not None:
            # ATR-based stop
            stop_distance = atr * atr_multiplier
            if side == OrderSide.BUY:
                return entry_price - stop_distance
            else:
                return entry_price + stop_distance
        
        return None
    
    def calculate_take_profit(self,
                             entry_price: float,
                             side: OrderSide,
                             profit_pct: Optional[float] = None,
                             risk_reward_ratio: Optional[float] = None,
                             stop_loss: Optional[float] = None) -> Optional[float]:
        """
        Calculate take profit price
        
        Args:
            entry_price: Entry price
            side: Order side (BUY/SELL)
            profit_pct: Take profit percentage (e.g., 0.05 = 5%)
            risk_reward_ratio: Risk/reward ratio (e.g., 2.0 = 2:1)
            stop_loss: Stop loss price (for R:R calculation)
        
        Returns:
            Take profit price
        """
        if profit_pct is not None:
            # Percentage-based target
            if side == OrderSide.BUY:
                return entry_price * (1 + profit_pct)
            else:
                return entry_price * (1 - profit_pct)
        
        elif risk_reward_ratio is not None and stop_loss is not None:
            # Risk/reward based target
            risk = abs(entry_price - stop_loss)
            reward = risk * risk_reward_ratio
            
            if side == OrderSide.BUY:
                return entry_price + reward
            else:
                return entry_price - reward
        
        return None
