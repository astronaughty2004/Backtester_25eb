# multi-timeframe alignment utilities
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import logging

from ..models import Bar

logger = logging.getLogger(__name__)


class Resampler:
    """Resample and align price data across multiple timeframes"""
    
    def __init__(self):
        """Initialize resampler"""
        pass
    
    def resample(self,
                bars: List[Bar],
                target_timeframe: str) -> List[Bar]:
        """
        Resample bars to target timeframe
        
        Args:
            bars: List of bars (any timeframe)
            target_timeframe: Target timeframe (e.g., '5T', '15T', '1H', '1D')
                             T = minutes, H = hours, D = days
        
        Returns:
            Resampled list of bars
        """
        if not bars:
            return []
        
        logger.info(f"Resampling {len(bars)} bars to {target_timeframe}")
        
        # Convert to DataFrame
        df = self._bars_to_dataframe(bars)
        
        # Set timestamp as index
        df.set_index('timestamp', inplace=True)
        
        # Resample based on available data
        if 'open' in df.columns and df['open'].notna().any():
            # Full OHLCV resampling
            resampled = df.resample(target_timeframe).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'price': 'last'
            })
        else:
            # Price-only resampling
            resampled = df.resample(target_timeframe).agg({
                'price': 'last'
            })
        
        # Drop rows with no data
        resampled = resampled.dropna(subset=['price'])
        
        # Convert back to bars
        symbol = bars[0].symbol
        resampled_bars = []
        
        for timestamp, row in resampled.iterrows():
            if 'open' in row and pd.notna(row['open']):
                bar = Bar(
                    timestamp=timestamp,
                    symbol=symbol,
                    price=row['price'],
                    open=row.get('open'),
                    high=row.get('high'),
                    low=row.get('low'),
                    close=row.get('close'),
                    volume=row.get('volume', 0.0)
                )
            else:
                bar = Bar.from_price(
                    timestamp=timestamp,
                    symbol=symbol,
                    price=row['price']
                )
            resampled_bars.append(bar)
        
        logger.info(f"Resampled to {len(resampled_bars)} bars")
        
        return resampled_bars
    
    def align_timeframes(self,
                        primary_bars: List[Bar],
                        secondary_bars: List[Bar]) -> Dict[datetime, Optional[Bar]]:
        """
        Align secondary timeframe bars to primary timeframe timestamps
        Prevents look-ahead bias by only making bars available after they complete
        
        Args:
            primary_bars: Primary (higher frequency) bars
            secondary_bars: Secondary (lower frequency) bars to align
        
        Returns:
            Dict mapping primary timestamps -> secondary bar (None if not available yet)
        """
        alignment = {}
        secondary_idx = 0
        
        for primary_bar in primary_bars:
            # Find the most recent completed secondary bar
            aligned_bar = None
            
            for i in range(secondary_idx, len(secondary_bars)):
                if secondary_bars[i].timestamp <= primary_bar.timestamp:
                    aligned_bar = secondary_bars[i]
                    secondary_idx = i
                else:
                    break
            
            alignment[primary_bar.timestamp] = aligned_bar
        
        return alignment
    
    def create_multi_timeframe_view(self,
                                   bars: List[Bar],
                                   timeframes: List[str]) -> Dict[str, List[Bar]]:
        """
        Create multiple timeframe views from single timeframe data
        
        Args:
            bars: Original bars (typically highest frequency)
            timeframes: List of target timeframes (e.g., ['5T', '15T', '1H', '1D'])
        
        Returns:
            Dict mapping timeframe -> resampled bars
        """
        views = {}
        
        for tf in timeframes:
            try:
                views[tf] = self.resample(bars, tf)
            except Exception as e:
                logger.error(f"Failed to resample to {tf}: {e}")
        
        return views
    
    def forward_fill(self,
                    bars: List[Bar],
                    frequency: str = '1T') -> List[Bar]:
        """
        Forward fill missing bars
        
        Args:
            bars: Bars with potential gaps
            frequency: Expected frequency
        
        Returns:
            Bars with gaps filled
        """
        if not bars:
            return []
        
        df = self._bars_to_dataframe(bars)
        df.set_index('timestamp', inplace=True)
        
        # Create complete time range
        time_range = pd.date_range(
            start=df.index.min(),
            end=df.index.max(),
            freq=frequency
        )
        
        # Reindex and forward fill
        df = df.reindex(time_range)
        df['price'] = df['price'].ffill()
        
        if 'close' in df.columns:
            df['close'] = df['close'].ffill()
            df['open'] = df['open'].ffill()
            df['high'] = df['high'].ffill()
            df['low'] = df['low'].ffill()
        
        df['volume'] = df['volume'].fillna(0.0)
        
        # Convert back to bars
        symbol = bars[0].symbol
        filled_bars = []
        
        for timestamp, row in df.iterrows():
            if pd.notna(row['price']):
                bar = Bar(
                    timestamp=timestamp,
                    symbol=symbol,
                    price=row['price'],
                    open=row.get('open') if pd.notna(row.get('open')) else None,
                    high=row.get('high') if pd.notna(row.get('high')) else None,
                    low=row.get('low') if pd.notna(row.get('low')) else None,
                    close=row.get('close') if pd.notna(row.get('close')) else None,
                    volume=row.get('volume', 0.0)
                )
                filled_bars.append(bar)
        
        return filled_bars
    
    def _bars_to_dataframe(self, bars: List[Bar]) -> pd.DataFrame:
        """Convert bars to DataFrame"""
        data = []
        for bar in bars:
            row = {
                'timestamp': bar.timestamp,
                'price': bar.price,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume
            }
            data.append(row)
        
        return pd.DataFrame(data)
