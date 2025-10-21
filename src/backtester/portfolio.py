# apply fills → update positions, cash, realized/unrealized P&L, snapshots
from typing import Dict, List, Optional
from datetime import datetime, date
from copy import deepcopy
import logging

from .models import Fill, Position, PortfolioSnapshot, OrderSide, Bar
from .utils import calculate_pnl, is_same_day


logger = logging.getLogger(__name__)


class Portfolio:
    """
    Tracks positions, cash, and P&L with daywise support
    """
    
    def __init__(self, 
                 initial_cash: float,
                 track_daily_pnl: bool = True,
                 square_off_eod: bool = False):
        """
        Initialize portfolio
        
        Args:
            initial_cash: Starting cash
            track_daily_pnl: Track daily P&L separately
            square_off_eod: Square off all positions at end of day
        """
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.track_daily_pnl = track_daily_pnl
        self.square_off_eod = square_off_eod
        
        # Positions
        self.positions: Dict[str, Position] = {}
        
        # P&L tracking
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.total_commission = 0.0
        self.num_trades = 0
        
        # Daily tracking
        self.current_day: Optional[date] = None
        self.daily_pnl = 0.0
        self.daily_starting_value = initial_cash
        self.previous_day_value = initial_cash
        
        # Daily P&L history
        self.daily_pnl_history: Dict[date, float] = {}
        self.daily_return_history: Dict[date, float] = {}
        
        # Snapshots
        self.snapshots: List[PortfolioSnapshot] = []
        
        # Price cache for MTM
        self.current_prices: Dict[str, float] = {}
        
        logger.info(f"Portfolio initialized with ${initial_cash:,.2f}")
    
    def apply_fill(self, fill: Fill, current_price: Optional[float] = None):
        """
        Apply fill to portfolio - update positions and cash
        
        Args:
            fill: Fill to apply
            current_price: Current market price (for MTM)
        """
        symbol = fill.symbol
        
        # Update price cache
        if current_price is None:
            current_price = fill.price
        self.current_prices[symbol] = current_price
        
        # Check for new day
        if self.track_daily_pnl:
            self._check_new_day(fill.timestamp)
        
        # Get or create position
        if symbol not in self.positions:
            self.positions[symbol] = Position(
                symbol=symbol,
                quantity=0,
                avg_price=0.0,
                opened_at=fill.timestamp,
                last_updated=fill.timestamp
            )
        
        position = self.positions[symbol]
        
        # Calculate P&L for closing trades
        fill_realized_pnl = 0.0
        
        if position.quantity != 0:
            # Check if this fill closes or reduces position
            is_closing = (
                (position.quantity > 0 and fill.side == OrderSide.SELL) or
                (position.quantity < 0 and fill.side == OrderSide.BUY)
            )
            
            if is_closing:
                # Calculate realized P&L for closing portion
                close_quantity = min(abs(fill.quantity), abs(position.quantity))
                fill_realized_pnl = calculate_pnl(
                    position.avg_price,
                    fill.price,
                    close_quantity if position.quantity > 0 else -close_quantity
                )
                
                logger.debug(
                    f"Closing {close_quantity} of {symbol} position: "
                    f"entry={position.avg_price:.2f}, exit={fill.price:.2f}, "
                    f"PnL=${fill_realized_pnl:.2f}"
                )
        
        # Update position
        self._update_position(position, fill)
        
        # Update cash
        if fill.side == OrderSide.BUY:
            # Buying: cash decreases
            self.cash -= fill.net_value
        else:
            # Selling: cash increases
            self.cash += fill.net_value
        
        # Update P&L
        self.realized_pnl += fill_realized_pnl
        self.total_commission += fill.commission
        self.num_trades += 1
        
        # Update daily P&L
        if self.track_daily_pnl:
            self.daily_pnl += fill_realized_pnl - fill.commission
        
        # Store realized PnL in fill
        fill.realized_pnl = fill_realized_pnl
        
        # Remove flat positions
        if position.quantity == 0:
            logger.debug(f"Position in {symbol} closed")
        
        logger.info(
            f"Fill applied: {symbol} {fill.side.value} {fill.quantity}@{fill.price:.2f} "
            f"(commission=${fill.commission:.2f}, realized_pnl=${fill_realized_pnl:.2f})"
        )
    
    def _update_position(self, position: Position, fill: Fill):
        """
        Update position with fill
        
        Args:
            position: Position to update
            fill: Fill to apply
        """
        old_quantity = position.quantity
        
        # Determine new quantity
        if fill.side == OrderSide.BUY:
            new_quantity = old_quantity + fill.quantity
        else:
            new_quantity = old_quantity - fill.quantity
        
        # Update average price
        if new_quantity == 0:
            # Position closed
            position.quantity = 0
            position.avg_price = 0.0
        
        elif (old_quantity > 0 and new_quantity > 0) or (old_quantity < 0 and new_quantity < 0):
            # Adding to existing position (same direction)
            old_value = abs(old_quantity) * position.avg_price
            new_value = fill.quantity * fill.price
            total_quantity = abs(new_quantity)
            
            position.avg_price = (old_value + new_value) / total_quantity
            position.quantity = new_quantity
        
        elif abs(new_quantity) < abs(old_quantity):
            # Reducing position (keep same avg price)
            position.quantity = new_quantity
        
        else:
            # Reversing position
            position.quantity = new_quantity
            position.avg_price = fill.price
            position.opened_at = fill.timestamp
        
        position.last_updated = fill.timestamp
        position.total_commission += fill.commission
    
    def update_market_prices(self, prices: Dict[str, float]):
        """
        Update current market prices for all positions
        
        Args:
            prices: Dict of symbol -> price
        """
        self.current_prices.update(prices)
        
        # Update unrealized P&L for all positions
        self.unrealized_pnl = 0.0
        
        for symbol, position in self.positions.items():
            if position.quantity != 0 and symbol in self.current_prices:
                current_price = self.current_prices[symbol]
                position.update_unrealized_pnl(current_price)
                self.unrealized_pnl += position.unrealized_pnl
    
    def update_from_bar(self, bar: Bar):
        """
        Update portfolio with bar data (for MTM)
        
        Args:
            bar: Bar data
        """
        self.current_prices[bar.symbol] = bar.close
        
        if bar.symbol in self.positions:
            position = self.positions[bar.symbol]
            if position.quantity != 0:
                position.update_unrealized_pnl(bar.close)
        
        # Recalculate total unrealized P&L
        self.unrealized_pnl = sum(
            p.unrealized_pnl for p in self.positions.values() if p.quantity != 0
        )
    
    def create_snapshot(self, timestamp: datetime) -> PortfolioSnapshot:
        """
        Create portfolio snapshot
        
        Args:
            timestamp: Snapshot timestamp
        
        Returns:
            PortfolioSnapshot object
        """
        # Calculate total value
        positions_value = sum(
            abs(p.quantity) * self.current_prices.get(p.symbol, p.avg_price)
            for p in self.positions.values()
            if p.quantity != 0
        )
        
        total_value = self.cash + self.unrealized_pnl
        
        # Calculate daily return
        daily_return = 0.0
        if self.previous_day_value > 0:
            daily_return = (total_value - self.previous_day_value) / self.previous_day_value
        
        snapshot = PortfolioSnapshot(
            timestamp=timestamp,
            cash=self.cash,
            positions=deepcopy(self.positions),
            total_value=total_value,
            realized_pnl=self.realized_pnl,
            unrealized_pnl=self.unrealized_pnl,
            daily_pnl=self.daily_pnl,
            daily_return=daily_return,
            total_commission=self.total_commission,
            num_trades=self.num_trades,
            starting_cash=self.initial_cash,
            previous_day_value=self.previous_day_value
        )
        
        self.snapshots.append(snapshot)
        
        return snapshot
    
    def _check_new_day(self, timestamp: datetime):
        """
        Check if we've moved to a new day and update daily tracking
        
        Args:
            timestamp: Current timestamp
        """
        current_date = timestamp.date()
        
        if self.current_day is None:
            # First day
            self.current_day = current_date
            self.daily_starting_value = self.get_total_value()
            return
        
        if current_date != self.current_day:
            # New day - record previous day's P&L
            end_of_day_value = self.get_total_value()
            
            self.daily_pnl_history[self.current_day] = self.daily_pnl
            
            daily_return = 0.0
            if self.daily_starting_value > 0:
                daily_return = self.daily_pnl / self.daily_starting_value
            self.daily_return_history[self.current_day] = daily_return
            
            logger.info(
                f"Day end {self.current_day}: PnL=${self.daily_pnl:,.2f}, "
                f"Return={daily_return*100:.2f}%, Value=${end_of_day_value:,.2f}"
            )
            
            # Square off EOD if enabled
            if self.square_off_eod:
                self._square_off_all_positions(timestamp)
            
            # Reset for new day
            self.current_day = current_date
            self.previous_day_value = end_of_day_value
            self.daily_starting_value = end_of_day_value
            self.daily_pnl = 0.0
    
    def _square_off_all_positions(self, timestamp: datetime):
        """
        Square off all positions (for EOD square-off)
        
        Args:
            timestamp: Square-off timestamp
        """
        if not self.positions:
            return
        
        logger.info(f"Squaring off all positions at {timestamp}")
        
        for symbol, position in list(self.positions.items()):
            if position.quantity != 0:
                # Create synthetic fill to close position
                close_price = self.current_prices.get(symbol, position.avg_price)
                close_side = OrderSide.SELL if position.quantity > 0 else OrderSide.BUY
                
                # Calculate P&L
                realized_pnl = calculate_pnl(
                    position.avg_price,
                    close_price,
                    position.quantity
                )
                
                # Update P&L
                self.realized_pnl += realized_pnl
                self.daily_pnl += realized_pnl
                
                # Update cash (no commission for EOD square-off)
                if position.quantity > 0:
                    self.cash += abs(position.quantity) * close_price
                else:
                    self.cash -= abs(position.quantity) * close_price
                
                logger.info(
                    f"EOD square-off: {symbol} {abs(position.quantity)}@{close_price:.2f} "
                    f"(entry={position.avg_price:.2f}, PnL=${realized_pnl:.2f})"
                )
                
                # Close position
                position.quantity = 0
                position.avg_price = 0.0
    
    def get_position(self, symbol: str) -> Optional[Position]:
        """Get position for symbol"""
        return self.positions.get(symbol)
    
    def get_total_value(self) -> float:
        """
        Calculate total portfolio value
        
        Returns:
            Total value including unrealized P&L
        """
        return self.cash + self.unrealized_pnl
    
    def get_equity_curve(self) -> List[tuple]:
        """
        Get equity curve from snapshots
        
        Returns:
            List of (timestamp, total_value) tuples
        """
        return [(s.timestamp, s.total_value) for s in self.snapshots]
    
    def get_daily_pnl_series(self) -> Dict[date, float]:
        """Get daily P&L history"""
        return self.daily_pnl_history.copy()
    
    def get_daily_returns_series(self) -> Dict[date, float]:
        """Get daily returns history"""
        return self.daily_return_history.copy()
    
    def get_statistics(self) -> dict:
        """
        Get portfolio statistics
        
        Returns:
            Dict with portfolio stats
        """
        total_value = self.get_total_value()
        total_pnl = self.realized_pnl + self.unrealized_pnl
        total_return = (total_value - self.initial_cash) / self.initial_cash if self.initial_cash > 0 else 0.0
        
        num_positions = len([p for p in self.positions.values() if p.quantity != 0])
        
        return {
            'initial_cash': self.initial_cash,
            'current_cash': self.cash,
            'total_value': total_value,
            'realized_pnl': self.realized_pnl,
            'unrealized_pnl': self.unrealized_pnl,
            'total_pnl': total_pnl,
            'total_return': total_return,
            'total_commission': self.total_commission,
            'num_trades': self.num_trades,
            'num_positions': num_positions,
            'num_snapshots': len(self.snapshots),
            'num_trading_days': len(self.daily_pnl_history)
        }
    
    def reset(self):
        """Reset portfolio to initial state"""
        self.cash = self.initial_cash
        self.positions.clear()
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.total_commission = 0.0
        self.num_trades = 0
        self.current_day = None
        self.daily_pnl = 0.0
        self.daily_starting_value = self.initial_cash
        self.previous_day_value = self.initial_cash
        self.daily_pnl_history.clear()
        self.daily_return_history.clear()
        self.snapshots.clear()
        self.current_prices.clear()
        
        logger.info("Portfolio reset")
