# compute CAGR, vol, Sharpe, max drawdown/duration, turnover, VaR
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple
from datetime import datetime, timedelta
import logging

from .models import PortfolioSnapshot, Fill

logger = logging.getLogger(__name__)


class MetricsCalculator:
    """Calculate backtest performance metrics"""
    
    def __init__(self, risk_free_rate: float = 0.02):
        """
        Initialize metrics calculator
        
        Args:
            risk_free_rate: Annual risk-free rate (default 2%)
        """
        self.risk_free_rate = risk_free_rate
    
    def calculate_all_metrics(self,
                             snapshots: List[PortfolioSnapshot],
                             fills: List[Fill],
                             initial_capital: float) -> Dict:
        """
        Calculate all metrics
        
        Args:
            snapshots: List of portfolio snapshots
            fills: List of fills
            initial_capital: Starting capital
        
        Returns:
            Dict with all metrics
        """
        if not snapshots:
            return {}
        
        # Extract equity curve
        equity_curve = [s.total_value for s in snapshots]
        timestamps = [s.timestamp for s in snapshots]
        
        # Calculate returns
        returns = self.calculate_returns(snapshots)
        daily_returns = self.calculate_daily_returns(snapshots)
        
        # Basic metrics
        total_return = self.calculate_total_return(initial_capital, equity_curve[-1])
        cagr = self.calculate_cagr(initial_capital, equity_curve[-1], timestamps[0], timestamps[-1])
        
        # Risk metrics
        volatility = self.calculate_volatility(daily_returns)
        sharpe = self.calculate_sharpe_ratio(daily_returns, self.risk_free_rate)
        sortino = self.calculate_sortino_ratio(daily_returns, self.risk_free_rate)
        
        # Drawdown metrics
        max_dd, max_dd_duration = self.calculate_max_drawdown(equity_curve, timestamps)
        calmar = cagr / abs(max_dd) if max_dd != 0 else 0.0
        
        # Trade metrics
        trade_metrics = self.calculate_trade_metrics(fills)
        
        # Risk metrics
        var_95 = self.calculate_var(daily_returns, confidence=0.95)
        cvar_95 = self.calculate_cvar(daily_returns, confidence=0.95)
        
        metrics = {
            # Return metrics
            'total_return': total_return,
            'cagr': cagr,
            'total_pnl': equity_curve[-1] - initial_capital,
            
            # Risk metrics
            'volatility': volatility,
            'sharpe_ratio': sharpe,
            'sortino_ratio': sortino,
            'max_drawdown': max_dd,
            'max_drawdown_duration_days': max_dd_duration,
            'calmar_ratio': calmar,
            'var_95': var_95,
            'cvar_95': cvar_95,
            
            # Trade metrics
            **trade_metrics,
            
            # Additional info
            'num_snapshots': len(snapshots),
            'start_date': timestamps[0].strftime('%Y-%m-%d'),
            'end_date': timestamps[-1].strftime('%Y-%m-%d'),
            'initial_capital': initial_capital,
            'final_value': equity_curve[-1]
        }
        
        return metrics
    
    def calculate_returns(self, snapshots: List[PortfolioSnapshot]) -> np.ndarray:
        """Calculate period-to-period returns"""
        if len(snapshots) < 2:
            return np.array([])
        
        values = np.array([s.total_value for s in snapshots])
        returns = np.diff(values) / values[:-1]
        
        return returns
    
    def calculate_daily_returns(self, snapshots: List[PortfolioSnapshot]) -> np.ndarray:
        """Calculate daily returns from snapshots"""
        # Group by date
        daily_values = {}
        for snapshot in snapshots:
            date = snapshot.timestamp.date()
            daily_values[date] = snapshot.total_value
        
        # Calculate daily returns
        dates = sorted(daily_values.keys())
        if len(dates) < 2:
            return np.array([])
        
        returns = []
        for i in range(1, len(dates)):
            prev_value = daily_values[dates[i-1]]
            curr_value = daily_values[dates[i]]
            ret = (curr_value - prev_value) / prev_value if prev_value > 0 else 0.0
            returns.append(ret)
        
        return np.array(returns)
    
    def calculate_total_return(self, initial_value: float, final_value: float) -> float:
        """Calculate total return"""
        if initial_value == 0:
            return 0.0
        return (final_value - initial_value) / initial_value
    
    def calculate_cagr(self,
                      initial_value: float,
                      final_value: float,
                      start_date: datetime,
                      end_date: datetime) -> float:
        """Calculate Compound Annual Growth Rate"""
        if initial_value <= 0:
            return 0.0
        
        days = (end_date - start_date).days
        if days == 0:
            return 0.0
        
        years = days / 365.25
        cagr = (final_value / initial_value) ** (1 / years) - 1
        
        return cagr
    
    def calculate_volatility(self, returns: np.ndarray, annualize: bool = True) -> float:
        """Calculate volatility (standard deviation of returns)"""
        if len(returns) == 0:
            return 0.0
        
        vol = np.std(returns, ddof=1)
        
        if annualize:
            vol = vol * np.sqrt(252)  # Annualize assuming 252 trading days
        
        return vol
    
    def calculate_sharpe_ratio(self, returns: np.ndarray, risk_free_rate: float = 0.0) -> float:
        """Calculate Sharpe ratio"""
        if len(returns) == 0:
            return 0.0
        
        excess_returns = returns - (risk_free_rate / 252)  # Daily risk-free rate
        
        if np.std(excess_returns) == 0:
            return 0.0
        
        sharpe = np.mean(excess_returns) / np.std(excess_returns, ddof=1)
        sharpe = sharpe * np.sqrt(252)  # Annualize
        
        return sharpe
    
    def calculate_sortino_ratio(self, returns: np.ndarray, risk_free_rate: float = 0.0) -> float:
        """Calculate Sortino ratio (uses downside deviation)"""
        if len(returns) == 0:
            return 0.0
        
        excess_returns = returns - (risk_free_rate / 252)
        downside_returns = excess_returns[excess_returns < 0]
        
        if len(downside_returns) == 0 or np.std(downside_returns) == 0:
            return 0.0
        
        sortino = np.mean(excess_returns) / np.std(downside_returns, ddof=1)
        sortino = sortino * np.sqrt(252)  # Annualize
        
        return sortino
    
    def calculate_max_drawdown(self,
                               equity_curve: List[float],
                               timestamps: List[datetime]) -> Tuple[float, int]:
        """
        Calculate maximum drawdown and its duration
        
        Returns:
            (max_drawdown_pct, duration_in_days)
        """
        if len(equity_curve) == 0:
            return 0.0, 0
        
        equity = np.array(equity_curve)
        running_max = np.maximum.accumulate(equity)
        drawdown = (equity - running_max) / running_max
        
        max_dd = np.min(drawdown)
        
        # Calculate duration
        max_dd_duration = 0
        current_duration = 0
        
        for i in range(len(drawdown)):
            if drawdown[i] < 0:
                if i > 0:
                    days = (timestamps[i] - timestamps[i-1]).days
                    current_duration += max(days, 1)
                else:
                    current_duration = 1
                max_dd_duration = max(max_dd_duration, current_duration)
            else:
                current_duration = 0
        
        return max_dd, max_dd_duration
    
    def calculate_trade_metrics(self, fills: List[Fill]) -> Dict:
        """Calculate trade-related metrics"""
        if not fills:
            return {
                'num_trades': 0,
                'win_rate': 0.0,
                'profit_factor': 0.0,
                'avg_win': 0.0,
                'avg_loss': 0.0,
                'expectancy': 0.0,
                'total_commission': 0.0
            }
        
        # Separate winning and losing trades
        winning_trades = [f.realized_pnl for f in fills if f.realized_pnl > 0]
        losing_trades = [f.realized_pnl for f in fills if f.realized_pnl < 0]
        
        num_trades = len([f for f in fills if f.realized_pnl != 0])
        num_wins = len(winning_trades)
        num_losses = len(losing_trades)
        
        win_rate = num_wins / num_trades if num_trades > 0 else 0.0
        
        avg_win = np.mean(winning_trades) if winning_trades else 0.0
        avg_loss = np.mean(losing_trades) if losing_trades else 0.0
        
        total_wins = sum(winning_trades) if winning_trades else 0.0
        total_losses = abs(sum(losing_trades)) if losing_trades else 0.0
        
        profit_factor = total_wins / total_losses if total_losses > 0 else 0.0
        
        expectancy = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)
        
        total_commission = sum(f.commission for f in fills)
        
        return {
            'num_trades': num_trades,
            'num_wins': num_wins,
            'num_losses': num_losses,
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'expectancy': expectancy,
            'total_commission': total_commission
        }
    
    def calculate_var(self, returns: np.ndarray, confidence: float = 0.95) -> float:
        """Calculate Value at Risk"""
        if len(returns) == 0:
            return 0.0
        
        var = np.percentile(returns, (1 - confidence) * 100)
        
        return var
    
    def calculate_cvar(self, returns: np.ndarray, confidence: float = 0.95) -> float:
        """Calculate Conditional Value at Risk (Expected Shortfall)"""
        if len(returns) == 0:
            return 0.0
        
        var = self.calculate_var(returns, confidence)
        cvar = np.mean(returns[returns <= var])
        
        return cvar if not np.isnan(cvar) else 0.0
