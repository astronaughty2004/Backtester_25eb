# fill simulation: first-touch intrabar, slippage, commission models
from typing import Optional, Tuple
from datetime import datetime
import logging

from .models import Order, Fill, Bar, OrderSide, OrderType, OrderStatus
from .utils import (
    generate_fill_id,
    calculate_slippage,
    calculate_commission,
    round_to_tick,
    validate_price
)


logger = logging.getLogger(__name__)


class ExecutionEngine:
    """
    Simulates realistic order execution with intrabar first-touch logic
    """
    
    def __init__(self,
                 slippage_bps: float = 0.0,
                 commission_bps: float = 0.0,
                 tick_size: float = 0.05,
                 use_first_touch: bool = True):
        """
        Initialize execution engine
        
        Args:
            slippage_bps: Slippage in basis points
            commission_bps: Commission in basis points
            tick_size: Minimum price increment
            use_first_touch: Use first-touch logic vs close-only
        """
        self.slippage_bps = slippage_bps
        self.commission_bps = commission_bps
        self.tick_size = tick_size
        self.use_first_touch = use_first_touch
        
        self.fill_counter = 0
        
        logger.info(
            f"ExecutionEngine initialized: slippage={slippage_bps}bps, "
            f"commission={commission_bps}bps, tick_size={tick_size}"
        )
    
    def simulate_fill(self,
                     order: Order,
                     bar: Bar,
                     fill_sequence: int = 0) -> Optional[Fill]:
        """
        Simulate order fill against a bar using first-touch logic
        
        Args:
            order: Order to fill
            bar: OHLC bar data
            fill_sequence: Sequence number for this fill
        
        Returns:
            Fill object if order can be filled, None otherwise
        """
        if not order.is_active:
            return None
        
        if order.symbol != bar.symbol:
            return None
        
        # Determine execution price based on order type
        execution_price = self._determine_execution_price(order, bar)
        
        if execution_price is None:
            return None
        
        # Apply slippage
        side_str = order.side.value
        fill_price = calculate_slippage(execution_price, self.slippage_bps, side_str)
        fill_price = round_to_tick(fill_price, self.tick_size)
        
        # Determine fill quantity (full fill for now, can extend for partial fills)
        fill_quantity = order.remaining_quantity
        
        # Calculate commission
        commission = calculate_commission(fill_quantity, fill_price, self.commission_bps)
        
        # Create fill
        fill = Fill(
            fill_id=generate_fill_id(order.order_id, fill_sequence),
            order_id=order.order_id,
            timestamp=bar.timestamp,
            symbol=order.symbol,
            side=order.side,
            quantity=fill_quantity,
            price=fill_price,
            commission=commission,
            slippage=self.slippage_bps,
            execution_price=execution_price,
            metadata={
                'bar_open': bar.open,
                'bar_high': bar.high,
                'bar_low': bar.low,
                'bar_close': bar.close,
            }
        )
        
        self.fill_counter += 1
        
        logger.debug(
            f"Fill generated: {fill.fill_id} - {order.symbol} {order.side.value} "
            f"{fill_quantity}@{fill_price:.2f} (exec={execution_price:.2f})"
        )
        
        return fill
    
    def _determine_execution_price(self, order: Order, bar: Bar) -> Optional[float]:
        """
        Determine execution price using first-touch logic
        
        For market orders:
        - Use bar open price
        
        For limit orders (BUY):
        - If limit_price >= bar.low, fill at min(limit_price, bar.open)
        
        For limit orders (SELL):
        - If limit_price <= bar.high, fill at max(limit_price, bar.open)
        
        For stop orders:
        - Similar logic but triggered when stop is hit
        """
        if order.order_type == OrderType.MARKET:
            return self._market_order_price(order, bar)
        
        elif order.order_type == OrderType.LIMIT:
            return self._limit_order_price(order, bar)
        
        elif order.order_type == OrderType.STOP:
            return self._stop_order_price(order, bar)
        
        elif order.order_type == OrderType.STOP_LIMIT:
            return self._stop_limit_order_price(order, bar)
        
        return None
    
    def _market_order_price(self, order: Order, bar: Bar) -> Optional[float]:
        """Market order fills at open (or close if not using first-touch)"""
        if self.use_first_touch:
            return bar.open
        else:
            return bar.close
    
    def _limit_order_price(self, order: Order, bar: Bar) -> Optional[float]:
        """
        Limit order first-touch logic
        
        BUY limit: fills if bar.low <= limit_price
        - First touch at: min(limit_price, bar.open)
        
        SELL limit: fills if bar.high >= limit_price
        - First touch at: max(limit_price, bar.open)
        """
        if order.limit_price is None:
            return None
        
        limit_price = order.limit_price
        
        if order.side == OrderSide.BUY:
            # Buy limit: need price to drop to limit or below
            if bar.low <= limit_price:
                # First touch price
                if self.use_first_touch:
                    # If open is already below limit, use open
                    # Otherwise, use limit price
                    return min(limit_price, bar.open) if bar.open <= limit_price else limit_price
                else:
                    # Use close price if available at limit
                    return min(limit_price, bar.close) if bar.close <= limit_price else None
            return None
        
        else:  # SELL
            # Sell limit: need price to rise to limit or above
            if bar.high >= limit_price:
                # First touch price
                if self.use_first_touch:
                    # If open is already above limit, use open
                    # Otherwise, use limit price
                    return max(limit_price, bar.open) if bar.open >= limit_price else limit_price
                else:
                    # Use close price if available at limit
                    return max(limit_price, bar.close) if bar.close >= limit_price else None
            return None
    
    def _stop_order_price(self, order: Order, bar: Bar) -> Optional[float]:
        """
        Stop order becomes market order when stop price is hit
        
        BUY stop: triggers when price >= stop_price
        SELL stop: triggers when price <= stop_price
        """
        if order.stop_price is None:
            return None
        
        stop_price = order.stop_price
        
        if order.side == OrderSide.BUY:
            # Buy stop: triggers when price rises to stop or above
            if bar.high >= stop_price:
                # Execute at stop price or open if gap up
                return max(stop_price, bar.open)
            return None
        
        else:  # SELL
            # Sell stop: triggers when price falls to stop or below
            if bar.low <= stop_price:
                # Execute at stop price or open if gap down
                return min(stop_price, bar.open)
            return None
    
    def _stop_limit_order_price(self, order: Order, bar: Bar) -> Optional[float]:
        """
        Stop-limit order: becomes limit order when stop is triggered
        """
        if order.stop_price is None or order.limit_price is None:
            return None
        
        stop_price = order.stop_price
        limit_price = order.limit_price
        
        # First check if stop is triggered
        stop_triggered = False
        
        if order.side == OrderSide.BUY:
            stop_triggered = bar.high >= stop_price
        else:
            stop_triggered = bar.low <= stop_price
        
        if not stop_triggered:
            return None
        
        # Now apply limit order logic
        return self._limit_order_price(order, bar)
    
    def check_stop_loss_hit(self,
                           position_side: str,
                           stop_loss: float,
                           bar: Bar) -> Tuple[bool, Optional[float]]:
        """
        Check if stop loss is hit during bar
        
        Args:
            position_side: "LONG" or "SHORT"
            stop_loss: Stop loss price
            bar: Current bar
        
        Returns:
            (is_hit, execution_price)
        """
        if position_side.upper() in ["LONG", "BUY"]:
            # Long position: stop loss below entry
            if bar.low <= stop_loss:
                # Hit during the bar
                execution_price = min(stop_loss, bar.open)
                return True, execution_price
        
        elif position_side.upper() in ["SHORT", "SELL"]:
            # Short position: stop loss above entry
            if bar.high >= stop_loss:
                # Hit during the bar
                execution_price = max(stop_loss, bar.open)
                return True, execution_price
        
        return False, None
    
    def check_take_profit_hit(self,
                             position_side: str,
                             take_profit: float,
                             bar: Bar) -> Tuple[bool, Optional[float]]:
        """
        Check if take profit is hit during bar
        
        Args:
            position_side: "LONG" or "SHORT"
            take_profit: Take profit price
            bar: Current bar
        
        Returns:
            (is_hit, execution_price)
        """
        if position_side.upper() in ["LONG", "BUY"]:
            # Long position: take profit above entry
            if bar.high >= take_profit:
                # Hit during the bar
                execution_price = max(take_profit, bar.open)
                return True, execution_price
        
        elif position_side.upper() in ["SHORT", "SELL"]:
            # Short position: take profit below entry
            if bar.low <= take_profit:
                # Hit during the bar
                execution_price = min(take_profit, bar.open)
                return True, execution_price
        
        return False, None
    
    def resolve_tp_sl_tie(self,
                         position_side: str,
                         stop_loss: float,
                         take_profit: float,
                         bar: Bar) -> Tuple[str, float]:
        """
        Resolve when both TP and SL are hit in same bar
        
        Conservative approach: assume SL is hit first (worst case)
        
        Args:
            position_side: "LONG" or "SHORT"
            stop_loss: Stop loss price
            take_profit: Take profit price
            bar: Current bar
        
        Returns:
            ("SL" or "TP", execution_price)
        """
        sl_hit, sl_price = self.check_stop_loss_hit(position_side, stop_loss, bar)
        tp_hit, tp_price = self.check_take_profit_hit(position_side, take_profit, bar)
        
        if sl_hit and tp_hit:
            # Both hit - determine which was hit first based on bar structure
            if position_side.upper() in ["LONG", "BUY"]:
                # Long: SL below, TP above
                # If bar went down first (low before high), SL hit first
                # Conservative: assume SL unless bar clearly went up first
                if bar.close > bar.open:  # Bullish bar, likely hit TP
                    return "TP", tp_price
                else:  # Bearish or uncertain, assume SL
                    return "SL", sl_price
            else:
                # Short: SL above, TP below
                if bar.close < bar.open:  # Bearish bar, likely hit TP
                    return "TP", tp_price
                else:  # Bullish or uncertain, assume SL
                    return "SL", sl_price
        
        elif sl_hit:
            return "SL", sl_price
        elif tp_hit:
            return "TP", tp_price
        
        return "NONE", 0.0
