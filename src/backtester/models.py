# dataclasses: Signal, Order, Fill, Position, Portfolio, Bar
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List
from enum import Enum


class OrderSide(Enum):
    """Order side: BUY or SELL"""
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    """Order types supported"""
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class OrderStatus(Enum):
    """Order lifecycle states"""
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class PositionSide(Enum):
    """Position direction"""
    LONG = "LONG"
    FLAT = "FLAT"
    SHORT = "SHORT"


@dataclass
class Bar:
    """Price bar data - works with price-only or full OHLCV"""
    timestamp: datetime
    symbol: str
    price: float  # Primary field for price-only data
    
    # Optional OHLCV fields (for full bar data)
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    
    def __post_init__(self):
        """Auto-populate OHLCV from price if not provided"""
        # If only price is provided, use it for all OHLCV
        if self.open is None:
            self.open = self.price
        if self.high is None:
            self.high = self.price
        if self.low is None:
            self.low = self.price
        if self.close is None:
            self.close = self.price
        if self.volume is None:
            self.volume = 0.0
        
        # Validate if full OHLCV provided
        if self.high < max(self.open, self.close, self.low):
            # Auto-correct instead of raising error
            self.high = max(self.open, self.close, self.low, self.high)
        if self.low > min(self.open, self.close, self.high):
            self.low = min(self.open, self.close, self.high, self.low)
    
    @classmethod
    def from_price(cls, timestamp: datetime, symbol: str, price: float):
        """Create bar from just timestamp and price"""
        return cls(timestamp=timestamp, symbol=symbol, price=price)
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create bar from dictionary"""
        return cls(
            timestamp=data['timestamp'],
            symbol=data['symbol'],
            price=data.get('price', data.get('close', 0.0)),
            open=data.get('open'),
            high=data.get('high'),
            low=data.get('low'),
            close=data.get('close'),
            volume=data.get('volume')
        )


@dataclass
class Signal:
    """Trading signal from strategy"""
    timestamp: datetime
    symbol: str
    side: OrderSide
    size: Optional[int] = None  # None = use risk manager to determine
    price: Optional[float] = None  # None = market order
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    reason: Optional[str] = None
    metadata: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Convert string side to enum if needed"""
        if isinstance(self.side, str):
            self.side = OrderSide(self.side.upper())


@dataclass
class Order:
    """Order object with lifecycle tracking"""
    order_id: str
    timestamp: datetime
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: int
    status: OrderStatus = OrderStatus.PENDING
    
    # Price fields
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    
    # Execution tracking
    filled_quantity: int = 0
    avg_fill_price: float = 0.0
    
    # Risk management
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    
    # Metadata
    parent_signal_id: Optional[str] = None
    submitted_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    metadata: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Convert strings to enums if needed"""
        if isinstance(self.side, str):
            self.side = OrderSide(self.side.upper())
        if isinstance(self.order_type, str):
            self.order_type = OrderType(self.order_type.upper())
        if isinstance(self.status, str):
            self.status = OrderStatus(self.status.upper())
    
    @property
    def remaining_quantity(self) -> int:
        """Unfilled quantity"""
        return self.quantity - self.filled_quantity
    
    @property
    def is_active(self) -> bool:
        """Check if order can still be filled"""
        return self.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIAL]
    
    @property
    def is_complete(self) -> bool:
        """Check if order is fully filled or terminal state"""
        return self.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]


@dataclass
class Fill:
    """Execution fill record"""
    fill_id: str
    order_id: str
    timestamp: datetime
    symbol: str
    side: OrderSide
    quantity: int
    price: float
    commission: float
    slippage: float  # bps
    
    # P&L tracking (for closing trades)
    realized_pnl: float = 0.0
    
    # Metadata
    execution_price: float = 0.0  # price before slippage
    metadata: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Convert string side to enum if needed"""
        if isinstance(self.side, str):
            self.side = OrderSide(self.side.upper())
        if self.execution_price == 0.0:
            self.execution_price = self.price
    
    @property
    def gross_value(self) -> float:
        """Total value excluding commission"""
        return self.quantity * self.price
    
    @property
    def net_value(self) -> float:
        """Total value including commission"""
        return self.gross_value + self.commission


@dataclass
class Position:
    """Current position in a symbol"""
    symbol: str
    quantity: int  # positive = long, negative = short
    avg_price: float
    
    # Tracking
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_commission: float = 0.0
    
    # Metadata
    opened_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    metadata: Dict = field(default_factory=dict)
    
    @property
    def side(self) -> PositionSide:
        """Position direction"""
        if self.quantity > 0:
            return PositionSide.LONG
        elif self.quantity < 0:
            return PositionSide.SHORT
        else:
            return PositionSide.FLAT
    
    @property
    def market_value(self) -> float:
        """Current market value (needs current price)"""
        # This will be computed externally with current price
        return abs(self.quantity) * self.avg_price
    
    def update_unrealized_pnl(self, current_price: float):
        """Update unrealized P&L with current market price"""
        if self.quantity != 0:
            self.unrealized_pnl = (current_price - self.avg_price) * self.quantity


@dataclass
class PortfolioSnapshot:
    """Portfolio state at a point in time"""
    timestamp: datetime
    cash: float
    positions: Dict[str, Position]
    
    # P&L tracking
    total_value: float
    realized_pnl: float  # cumulative
    unrealized_pnl: float  # current
    
    # Daily tracking
    daily_pnl: float = 0.0
    daily_return: float = 0.0
    
    # Metrics
    total_commission: float = 0.0
    num_trades: int = 0
    
    # Reference values
    starting_cash: float = 0.0
    previous_day_value: Optional[float] = None
    
    @property
    def equity(self) -> float:
        """Total equity = cash + market value of positions"""
        return self.total_value
    
    @property
    def positions_value(self) -> float:
        """Total value of all positions"""
        return sum(pos.market_value for pos in self.positions.values())
    
    @property
    def leverage(self) -> float:
        """Current leverage ratio"""
        if self.total_value <= 0:
            return 0.0
        return self.positions_value / self.total_value
    
    def get_position(self, symbol: str) -> Optional[Position]:
        """Get position for symbol"""
        return self.positions.get(symbol)
