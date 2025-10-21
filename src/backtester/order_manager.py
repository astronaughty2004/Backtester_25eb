# submit_order(), cancel_order(), order lifecycle & ID generation
from typing import Dict, List, Optional
from datetime import datetime
import logging

from .models import Order, Fill, Signal, Bar, OrderSide, OrderType, OrderStatus
from .execution import ExecutionEngine
from .utils import generate_order_id


logger = logging.getLogger(__name__)


class OrderManager:
    """
    Manages order lifecycle: submission, cancellation, and execution
    """
    
    def __init__(self, execution_engine: ExecutionEngine):
        """
        Initialize order manager
        
        Args:
            execution_engine: ExecutionEngine instance for fill simulation
        """
        self.execution_engine = execution_engine
        
        # Order tracking
        self.active_orders: Dict[str, Order] = {}
        self.completed_orders: Dict[str, Order] = {}
        self.all_orders: List[Order] = []
        
        # Fill tracking
        self.fills: List[Fill] = []
        self.fill_sequences: Dict[str, int] = {}  # order_id -> next fill sequence
        
        logger.info("OrderManager initialized")
    
    def submit_order(self,
                    symbol: str,
                    side: OrderSide,
                    quantity: int,
                    order_type: OrderType = OrderType.MARKET,
                    limit_price: Optional[float] = None,
                    stop_price: Optional[float] = None,
                    stop_loss: Optional[float] = None,
                    take_profit: Optional[float] = None,
                    timestamp: Optional[datetime] = None,
                    signal_id: Optional[str] = None,
                    metadata: Optional[dict] = None) -> Order:
        """
        Submit a new order
        
        Args:
            symbol: Symbol to trade
            side: BUY or SELL
            quantity: Order quantity
            order_type: Order type (MARKET, LIMIT, STOP, STOP_LIMIT)
            limit_price: Limit price (for LIMIT and STOP_LIMIT orders)
            stop_price: Stop price (for STOP and STOP_LIMIT orders)
            stop_loss: Stop loss price
            take_profit: Take profit price
            timestamp: Order timestamp
            signal_id: Parent signal ID
            metadata: Additional metadata
        
        Returns:
            Order object
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        # Generate order ID
        side_str = side.value if isinstance(side, OrderSide) else str(side)
        order_id = generate_order_id(symbol, timestamp, side_str)
        
        # Create order
        order = Order(
            order_id=order_id,
            timestamp=timestamp,
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            status=OrderStatus.SUBMITTED,
            limit_price=limit_price,
            stop_price=stop_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            parent_signal_id=signal_id,
            submitted_at=timestamp,
            metadata=metadata or {}
        )
        
        # Add to tracking
        self.active_orders[order_id] = order
        self.all_orders.append(order)
        self.fill_sequences[order_id] = 0
        
        logger.info(
            f"Order submitted: {order_id} - {symbol} {side_str} {quantity} "
            f"{order_type.value} @ {limit_price or stop_price or 'MARKET'}"
        )
        
        return order
    
    def submit_order_from_signal(self,
                                signal: Signal,
                                quantity: int,
                                timestamp: Optional[datetime] = None) -> Order:
        """
        Create order from signal
        
        Args:
            signal: Signal object
            quantity: Order quantity (from risk manager)
            timestamp: Order timestamp
        
        Returns:
            Order object
        """
        if timestamp is None:
            timestamp = signal.timestamp
        
        # Determine order type from signal
        if signal.price is not None:
            order_type = OrderType.LIMIT
            limit_price = signal.price
            stop_price = None
        else:
            order_type = OrderType.MARKET
            limit_price = None
            stop_price = None
        
        return self.submit_order(
            symbol=signal.symbol,
            side=signal.side,
            quantity=quantity,
            order_type=order_type,
            limit_price=limit_price,
            stop_price=stop_price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            timestamp=timestamp,
            signal_id=getattr(signal, 'signal_id', None),
            metadata=signal.metadata
        )
    
    def cancel_order(self, order_id: str, timestamp: Optional[datetime] = None) -> bool:
        """
        Cancel an active order
        
        Args:
            order_id: Order ID to cancel
            timestamp: Cancellation timestamp
        
        Returns:
            True if cancelled, False if not found or already complete
        """
        if order_id not in self.active_orders:
            logger.warning(f"Cannot cancel order {order_id}: not found in active orders")
            return False
        
        order = self.active_orders[order_id]
        
        if not order.is_active:
            logger.warning(f"Cannot cancel order {order_id}: already in terminal state {order.status}")
            return False
        
        # Update order status
        order.status = OrderStatus.CANCELLED
        
        # Move to completed
        self.completed_orders[order_id] = order
        del self.active_orders[order_id]
        
        logger.info(f"Order cancelled: {order_id}")
        
        return True
    
    def process_bar(self, bar: Bar) -> List[Fill]:
        """
        Process bar against all active orders
        
        Args:
            bar: Current bar
        
        Returns:
            List of fills generated
        """
        fills = []
        orders_to_complete = []
        
        for order_id, order in list(self.active_orders.items()):
            # Skip if different symbol
            if order.symbol != bar.symbol:
                continue
            
            # Try to fill order
            fill = self.execution_engine.simulate_fill(
                order=order,
                bar=bar,
                fill_sequence=self.fill_sequences[order_id]
            )
            
            if fill is not None:
                # Update order
                self._apply_fill_to_order(order, fill)
                
                # Increment fill sequence
                self.fill_sequences[order_id] += 1
                
                # Track fill
                fills.append(fill)
                self.fills.append(fill)
                
                # Check if order is complete
                if order.is_complete:
                    orders_to_complete.append(order_id)
        
        # Move completed orders
        for order_id in orders_to_complete:
            self.completed_orders[order_id] = self.active_orders[order_id]
            del self.active_orders[order_id]
        
        return fills
    
    def _apply_fill_to_order(self, order: Order, fill: Fill):
        """
        Update order state with fill
        
        Args:
            order: Order to update
            fill: Fill to apply
        """
        # Update filled quantity
        order.filled_quantity += fill.quantity
        
        # Update average fill price
        total_filled = order.filled_quantity
        if total_filled > 0:
            # Weighted average
            old_value = order.avg_fill_price * (total_filled - fill.quantity)
            new_value = fill.price * fill.quantity
            order.avg_fill_price = (old_value + new_value) / total_filled
        else:
            order.avg_fill_price = fill.price
        
        # Update status
        if order.filled_quantity >= order.quantity:
            order.status = OrderStatus.FILLED
            order.filled_at = fill.timestamp
        elif order.filled_quantity > 0:
            order.status = OrderStatus.PARTIAL
        
        logger.debug(
            f"Fill applied to order {order.order_id}: "
            f"{order.filled_quantity}/{order.quantity} @ {order.avg_fill_price:.2f}"
        )
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID"""
        if order_id in self.active_orders:
            return self.active_orders[order_id]
        elif order_id in self.completed_orders:
            return self.completed_orders[order_id]
        return None
    
    def get_active_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """
        Get all active orders, optionally filtered by symbol
        
        Args:
            symbol: Filter by symbol (optional)
        
        Returns:
            List of active orders
        """
        orders = list(self.active_orders.values())
        
        if symbol is not None:
            orders = [o for o in orders if o.symbol == symbol]
        
        return orders
    
    def get_fills(self, 
                 symbol: Optional[str] = None,
                 start_date: Optional[datetime] = None,
                 end_date: Optional[datetime] = None) -> List[Fill]:
        """
        Get fills with optional filters
        
        Args:
            symbol: Filter by symbol
            start_date: Filter by start date
            end_date: Filter by end date
        
        Returns:
            List of fills
        """
        fills = self.fills
        
        if symbol is not None:
            fills = [f for f in fills if f.symbol == symbol]
        
        if start_date is not None:
            fills = [f for f in fills if f.timestamp >= start_date]
        
        if end_date is not None:
            fills = [f for f in fills if f.timestamp <= end_date]
        
        return fills
    
    def get_order_statistics(self) -> dict:
        """
        Get order statistics
        
        Returns:
            Dict with order stats
        """
        total_orders = len(self.all_orders)
        active_orders = len(self.active_orders)
        completed_orders = len(self.completed_orders)
        
        filled_orders = len([o for o in self.completed_orders.values() 
                           if o.status == OrderStatus.FILLED])
        cancelled_orders = len([o for o in self.completed_orders.values() 
                              if o.status == OrderStatus.CANCELLED])
        
        total_fills = len(self.fills)
        
        return {
            'total_orders': total_orders,
            'active_orders': active_orders,
            'completed_orders': completed_orders,
            'filled_orders': filled_orders,
            'cancelled_orders': cancelled_orders,
            'total_fills': total_fills,
            'fill_rate': filled_orders / total_orders if total_orders > 0 else 0.0
        }
    
    def reset(self):
        """Reset order manager state"""
        self.active_orders.clear()
        self.completed_orders.clear()
        self.all_orders.clear()
        self.fills.clear()
        self.fill_sequences.clear()
        
        logger.info("OrderManager reset")
