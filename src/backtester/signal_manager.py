# ingest signals (live or file), timestamp, dedupe, queue
from typing import List, Dict, Optional, Set
from datetime import datetime
from collections import deque
import pandas as pd
import logging

from .models import Signal, OrderSide
from .utils import parse_timestamp, generate_signal_id

logger = logging.getLogger(__name__)


class SignalManager:
    """Manage signal ingestion, deduplication, and queueing"""
    
    def __init__(self, dedupe_window_seconds: int = 60):
        """
        Initialize signal manager
        
        Args:
            dedupe_window_seconds: Time window for deduplication
        """
        self.dedupe_window_seconds = dedupe_window_seconds
        
        # Signal queue
        self.signal_queue: deque = deque()
        
        # All signals
        self.all_signals: List[Signal] = []
        
        # Deduplication tracking: (symbol, side, timestamp) -> signal_id
        self.recent_signals: Dict[tuple, str] = {}
        
        logger.info(f"SignalManager initialized (dedupe_window={dedupe_window_seconds}s)")
    
    def add_signal(self, signal: Signal) -> bool:
        """
        Add signal to queue
        
        Args:
            signal: Signal to add
        
        Returns:
            True if added, False if duplicate
        """
        # Generate signal ID if not present
        if not hasattr(signal, 'signal_id') or signal.signal_id is None:
            signal.signal_id = generate_signal_id(signal.symbol, signal.timestamp)
        
        # Check for duplicate
        if self._is_duplicate(signal):
            logger.debug(f"Duplicate signal ignored: {signal.symbol} {signal.side.value}")
            return False
        
        # Add to queue and tracking
        self.signal_queue.append(signal)
        self.all_signals.append(signal)
        
        # Track for deduplication
        key = (signal.symbol, signal.side.value, signal.timestamp)
        self.recent_signals[key] = signal.signal_id
        
        logger.debug(f"Signal added: {signal.symbol} {signal.side.value} @ {signal.timestamp}")
        
        return True
    
    def add_signals(self, signals: List[Signal]) -> int:
        """
        Add multiple signals
        
        Args:
            signals: List of signals
        
        Returns:
            Number of signals added
        """
        added = 0
        for signal in signals:
            if self.add_signal(signal):
                added += 1
        
        return added
    
    def get_signals_for_timestamp(self, timestamp: datetime) -> List[Signal]:
        """
        Get all signals for a specific timestamp
        
        Args:
            timestamp: Target timestamp
        
        Returns:
            List of signals
        """
        signals = []
        
        # Process signals from queue that match timestamp
        while self.signal_queue:
            signal = self.signal_queue[0]
            
            if signal.timestamp <= timestamp:
                signals.append(self.signal_queue.popleft())
            else:
                break
        
        return signals
    
    def has_pending_signals(self) -> bool:
        """Check if queue has pending signals"""
        return len(self.signal_queue) > 0
    
    def peek_next_signal_time(self) -> Optional[datetime]:
        """Get timestamp of next signal without removing it"""
        if self.signal_queue:
            return self.signal_queue[0].timestamp
        return None
    
    def load_signals_from_csv(self, file_path: str, symbol_col: str = 'symbol') -> int:
        """
        Load signals from CSV file
        
        CSV format:
        timestamp, symbol, side, price, size, stop_loss, take_profit
        
        Args:
            file_path: Path to CSV file
            symbol_col: Name of symbol column
        
        Returns:
            Number of signals loaded
        """
        logger.info(f"Loading signals from {file_path}")
        
        df = pd.read_csv(file_path)
        
        # Parse signals
        signals = []
        for _, row in df.iterrows():
            try:
                signal = Signal(
                    timestamp=parse_timestamp(row['timestamp']),
                    symbol=row.get(symbol_col, row.get('ticker', 'UNKNOWN')),
                    side=OrderSide(row['side'].upper()),
                    size=int(row['size']) if 'size' in row and pd.notna(row['size']) else None,
                    price=float(row['price']) if 'price' in row and pd.notna(row['price']) else None,
                    stop_loss=float(row['stop_loss']) if 'stop_loss' in row and pd.notna(row['stop_loss']) else None,
                    take_profit=float(row['take_profit']) if 'take_profit' in row and pd.notna(row['take_profit']) else None,
                    reason=row.get('reason', 'CSV Import')
                )
                signals.append(signal)
            except Exception as e:
                logger.warning(f"Failed to parse signal row: {e}")
        
        # Sort by timestamp
        signals.sort(key=lambda s: s.timestamp)
        
        # Add to queue
        added = self.add_signals(signals)
        
        logger.info(f"Loaded {added} signals from CSV")
        
        return added
    
    def _is_duplicate(self, signal: Signal) -> bool:
        """
        Check if signal is duplicate within time window
        
        Args:
            signal: Signal to check
        
        Returns:
            True if duplicate
        """
        # Clean old entries
        cutoff_time = signal.timestamp.timestamp() - self.dedupe_window_seconds
        
        keys_to_remove = []
        for key, sig_id in self.recent_signals.items():
            sym, side, ts = key
            if ts.timestamp() < cutoff_time:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.recent_signals[key]
        
        # Check for duplicate
        key = (signal.symbol, signal.side.value, signal.timestamp)
        
        # Also check within time window
        for recent_key in self.recent_signals.keys():
            sym, side, ts = recent_key
            if (sym == signal.symbol and 
                side == signal.side.value and
                abs((ts - signal.timestamp).total_seconds()) < self.dedupe_window_seconds):
                return True
        
        return False
    
    def get_statistics(self) -> dict:
        """Get signal statistics"""
        total_signals = len(self.all_signals)
        pending_signals = len(self.signal_queue)
        processed_signals = total_signals - pending_signals
        
        # Count by side
        buy_signals = sum(1 for s in self.all_signals if s.side == OrderSide.BUY)
        sell_signals = sum(1 for s in self.all_signals if s.side == OrderSide.SELL)
        
        return {
            'total_signals': total_signals,
            'processed_signals': processed_signals,
            'pending_signals': pending_signals,
            'buy_signals': buy_signals,
            'sell_signals': sell_signals
        }
    
    def reset(self):
        """Reset signal manager"""
        self.signal_queue.clear()
        self.all_signals.clear()
        self.recent_signals.clear()
        
        logger.info("SignalManager reset")
