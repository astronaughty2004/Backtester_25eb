# helpers: datetime, id gen, logging helpers
import uuid
import logging
from datetime import datetime, date, time, timedelta
from typing import Optional, Union
import hashlib


# ============================================================================
# ID Generation
# ============================================================================

def generate_order_id(symbol: str, timestamp: datetime, side: str) -> str:
    """
    Generate unique order ID
    Format: ORD_{symbol}_{timestamp}_{uuid}
    """
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S%f")
    unique_id = str(uuid.uuid4())[:8]
    return f"ORD_{symbol}_{ts_str}_{side}_{unique_id}"


def generate_fill_id(order_id: str, sequence: int = 0) -> str:
    """
    Generate unique fill ID
    Format: FILL_{order_id}_{sequence}
    """
    return f"FILL_{order_id}_{sequence}"


def generate_signal_id(symbol: str, timestamp: datetime) -> str:
    """
    Generate unique signal ID
    Format: SIG_{symbol}_{timestamp}_{uuid}
    """
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S%f")
    unique_id = str(uuid.uuid4())[:8]
    return f"SIG_{symbol}_{ts_str}_{unique_id}"


def generate_trade_id() -> str:
    """Generate simple trade ID"""
    return f"TRADE_{uuid.uuid4().hex[:12]}"


# ============================================================================
# DateTime Helpers
# ============================================================================

def is_same_day(dt1: datetime, dt2: datetime) -> bool:
    """Check if two datetimes are on the same day"""
    return dt1.date() == dt2.date()


def get_day_start(dt: datetime) -> datetime:
    """Get start of day (00:00:00) for a datetime"""
    return datetime.combine(dt.date(), time.min)


def get_day_end(dt: datetime) -> datetime:
    """Get end of day (23:59:59.999999) for a datetime"""
    return datetime.combine(dt.date(), time.max)


def is_market_hours(dt: datetime, 
                    market_open: time = time(9, 15),
                    market_close: time = time(15, 30)) -> bool:
    """
    Check if datetime is within market hours
    Default: 9:15 AM to 3:30 PM (Indian market)
    """
    dt_time = dt.time()
    return market_open <= dt_time <= market_close


def get_trading_day_boundaries(dt: datetime) -> tuple[datetime, datetime]:
    """
    Get start and end of trading day
    Returns: (day_start, day_end)
    """
    day_start = datetime.combine(dt.date(), time(9, 15))
    day_end = datetime.combine(dt.date(), time(15, 30))
    return day_start, day_end


def parse_timestamp(ts: Union[str, datetime, int, float]) -> datetime:
    """
    Parse various timestamp formats to datetime
    """
    if isinstance(ts, datetime):
        return ts
    elif isinstance(ts, (int, float)):
        # Assume Unix timestamp
        return datetime.fromtimestamp(ts)
    elif isinstance(ts, str):
        # Try common formats
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(ts, fmt)
            except ValueError:
                continue
        raise ValueError(f"Unable to parse timestamp: {ts}")
    else:
        raise TypeError(f"Unsupported timestamp type: {type(ts)}")


# ============================================================================
# Math Helpers
# ============================================================================

def calculate_slippage(price: float, slippage_bps: float, side: str) -> float:
    """
    Calculate slippage amount
    
    Args:
        price: Execution price
        slippage_bps: Slippage in basis points
        side: BUY or SELL
    
    Returns:
        Price after slippage
    """
    slippage_factor = slippage_bps / 10000.0
    
    if side.upper() in ["BUY", "LONG"]:
        # Slippage increases buy price
        return price * (1 + slippage_factor)
    else:
        # Slippage decreases sell price
        return price * (1 - slippage_factor)


def calculate_commission(quantity: int, price: float, commission_bps: float) -> float:
    """
    Calculate commission cost
    
    Args:
        quantity: Order quantity
        price: Execution price
        commission_bps: Commission in basis points
    
    Returns:
        Commission amount (always positive)
    """
    gross_value = abs(quantity) * price
    commission = gross_value * (commission_bps / 10000.0)
    return abs(commission)


def calculate_pnl(entry_price: float, exit_price: float, quantity: int) -> float:
    """
    Calculate P&L for a trade
    
    Args:
        entry_price: Entry price
        exit_price: Exit price
        quantity: Position quantity (positive for long, negative for short)
    
    Returns:
        Realized P&L
    """
    return (exit_price - entry_price) * quantity


def round_to_tick(price: float, tick_size: float = 0.05) -> float:
    """
    Round price to nearest tick size
    
    Args:
        price: Price to round
        tick_size: Minimum price increment
    
    Returns:
        Rounded price
    """
    return round(price / tick_size) * tick_size


def calculate_returns(current_value: float, previous_value: float) -> float:
    """
    Calculate simple returns
    
    Returns:
        Return as decimal (0.01 = 1%)
    """
    if previous_value == 0:
        return 0.0
    return (current_value - previous_value) / previous_value


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division with default value for zero denominator"""
    if denominator == 0:
        return default
    return numerator / denominator


# ============================================================================
# Logging Helpers
# ============================================================================

def setup_logger(name: str, 
                log_file: Optional[str] = None,
                level: int = logging.INFO,
                console: bool = True) -> logging.Logger:
    """
    Setup logger with file and console handlers
    
    Args:
        name: Logger name
        log_file: Path to log file (optional)
        level: Logging level
        console: Whether to add console handler
    
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()
    
    # Format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


# ============================================================================
# Validation Helpers
# ============================================================================

def validate_price(price: float, min_price: float = 0.01) -> bool:
    """Validate price is positive and above minimum"""
    return price >= min_price


def validate_quantity(quantity: int) -> bool:
    """Validate quantity is positive integer"""
    return isinstance(quantity, int) and quantity > 0


def clamp(value: float, min_value: float, max_value: float) -> float:
    """Clamp value between min and max"""
    return max(min_value, min(max_value, value))
