# CSV/Parquet loader, timestamp parsing, adjusted prices, resampling
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Optional, Union, Dict
from datetime import datetime
import logging

from ..models import Bar
from ..utils import parse_timestamp

logger = logging.getLogger(__name__)


class DataLoader:
    """Load price data from CSV or Parquet files"""
    
    def __init__(self,
                 timestamp_col: str = 'timestamp',
                 price_col: str = 'price',
                 symbol_col: str = 'symbol'):
        """
        Initialize data loader
        
        Args:
            timestamp_col: Name of timestamp column
            price_col: Name of price column
            symbol_col: Name of symbol column
        """
        self.timestamp_col = timestamp_col
        self.price_col = price_col
        self.symbol_col = symbol_col
    
    def load_csv(self,
                file_path: str,
                symbol: Optional[str] = None,
                start_date: Optional[Union[str, datetime]] = None,
                end_date: Optional[Union[str, datetime]] = None) -> List[Bar]:
        """
        Load price data from CSV file
        
        Args:
            file_path: Path to CSV file
            symbol: Symbol name (if not in file)
            start_date: Filter start date
            end_date: Filter end date
        
        Returns:
            List of Bar objects
        """
        logger.info(f"Loading data from {file_path}")
        
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found: {file_path}")
        
        # Load CSV
        df = pd.read_csv(file_path)
        
        return self._df_to_bars(df, symbol, start_date, end_date)
    
    def load_parquet(self,
                    file_path: str,
                    symbol: Optional[str] = None,
                    start_date: Optional[Union[str, datetime]] = None,
                    end_date: Optional[Union[str, datetime]] = None) -> List[Bar]:
        """
        Load price data from Parquet file
        
        Args:
            file_path: Path to Parquet file
            symbol: Symbol name (if not in file)
            start_date: Filter start date
            end_date: Filter end date
        
        Returns:
            List of Bar objects
        """
        logger.info(f"Loading data from {file_path}")
        
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found: {file_path}")
        
        # Load Parquet
        df = pd.read_parquet(file_path)
        
        return self._df_to_bars(df, symbol, start_date, end_date)
    
    def load(self, file_path: str, **kwargs) -> List[Bar]:
        """Auto-detect format and load"""
        file_path = Path(file_path)
        
        if file_path.suffix.lower() == '.parquet':
            return self.load_parquet(str(file_path), **kwargs)
        else:
            return self.load_csv(str(file_path), **kwargs)
    
    def _df_to_bars(self,
                   df: pd.DataFrame,
                   symbol: Optional[str],
                   start_date: Optional[Union[str, datetime]],
                   end_date: Optional[Union[str, datetime]]) -> List[Bar]:
        """Convert DataFrame to list of Bar objects"""
        
        # Detect columns
        timestamp_col = self._detect_column(df, ['timestamp', 'date', 'datetime', 'time'])
        price_col = self._detect_column(df, ['price', 'close', 'value'])
        symbol_col = self._detect_column(df, ['symbol', 'ticker', 'instrument'], required=False)
        
        # Parse timestamps
        df['timestamp_parsed'] = df[timestamp_col].apply(parse_timestamp)
        
        # Filter by date range
        if start_date is not None:
            start_dt = parse_timestamp(start_date) if not isinstance(start_date, datetime) else start_date
            df = df[df['timestamp_parsed'] >= start_dt]
        
        if end_date is not None:
            end_dt = parse_timestamp(end_date) if not isinstance(end_date, datetime) else end_date
            df = df[df['timestamp_parsed'] <= end_dt]
        
        # Get symbol
        if symbol_col and symbol_col in df.columns:
            symbols = df[symbol_col].unique()
            if len(symbols) > 1:
                logger.warning(f"Multiple symbols found: {symbols}. Using first symbol.")
            symbol = symbols[0]
        elif symbol is None:
            symbol = "UNKNOWN"
        
        # Check for OHLCV columns
        has_ohlcv = all(col in df.columns for col in ['open', 'high', 'low', 'close'])
        has_volume = 'volume' in df.columns
        
        # Create bars
        bars = []
        for _, row in df.iterrows():
            if has_ohlcv:
                bar = Bar(
                    timestamp=row['timestamp_parsed'],
                    symbol=symbol,
                    price=row.get('close', row.get(price_col)),
                    open=row.get('open'),
                    high=row.get('high'),
                    low=row.get('low'),
                    close=row.get('close'),
                    volume=row.get('volume', 0.0) if has_volume else 0.0
                )
            else:
                bar = Bar.from_price(
                    timestamp=row['timestamp_parsed'],
                    symbol=symbol,
                    price=row[price_col]
                )
            bars.append(bar)
        
        logger.info(f"Loaded {len(bars)} bars for {symbol}")
        
        return bars
    
    def _detect_column(self,
                      df: pd.DataFrame,
                      possible_names: List[str],
                      required: bool = True) -> Optional[str]:
        """Detect column name from list of possibilities"""
        for col in df.columns:
            if col.lower() in possible_names:
                return col
        
        if required:
            raise ValueError(f"Could not find required column. Tried: {possible_names}")
        
        return None
    
    def load_multiple_symbols(self,
                             file_paths: Dict[str, str],
                             **kwargs) -> Dict[str, List[Bar]]:
        """
        Load data for multiple symbols
        
        Args:
            file_paths: Dict mapping symbol -> file path
            **kwargs: Arguments passed to load()
        
        Returns:
            Dict mapping symbol -> list of bars
        """
        data = {}
        
        for symbol, file_path in file_paths.items():
            try:
                data[symbol] = self.load(file_path, symbol=symbol, **kwargs)
            except Exception as e:
                logger.error(f"Failed to load data for {symbol}: {e}")
        
        return data
    
    def apply_adjustments(self,
                         bars: List[Bar],
                         adjustment_factors: Optional[pd.DataFrame] = None) -> List[Bar]:
        """
        Apply price adjustments (splits, dividends)
        
        Args:
            bars: List of bars
            adjustment_factors: DataFrame with timestamp -> adjustment factor
        
        Returns:
            Adjusted bars
        """
        if adjustment_factors is None:
            return bars
        
        adjusted_bars = []
        for bar in bars:
            factor = adjustment_factors.get(bar.timestamp, 1.0)
            
            adjusted_bar = Bar(
                timestamp=bar.timestamp,
                symbol=bar.symbol,
                price=bar.price * factor,
                open=bar.open * factor if bar.open else None,
                high=bar.high * factor if bar.high else None,
                low=bar.low * factor if bar.low else None,
                close=bar.close * factor if bar.close else None,
                volume=bar.volume / factor if bar.volume else None
            )
            adjusted_bars.append(adjusted_bar)
        
        return adjusted_bars
