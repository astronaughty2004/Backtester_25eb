# export trade_sheet.csv, metrics.json/txt, basic plots
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import json
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import logging

from .models import Fill, PortfolioSnapshot
from .metrics import MetricsCalculator

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generate backtest reports and visualizations"""
    
    def __init__(self, output_dir: str = "results"):
        """
        Initialize report generator
        
        Args:
            output_dir: Directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"ReportGenerator initialized: output_dir={output_dir}")
    
    def export_trade_sheet(self, fills: List[Fill], filename: str = "trade_sheet.csv"):
        """
        Export trade sheet to CSV
        
        Args:
            fills: List of fills
            filename: Output filename
        """
        if not fills:
            logger.warning("No fills to export")
            return
        
        # Convert fills to DataFrame
        data = []
        for fill in fills:
            row = {
                'fill_id': fill.fill_id,
                'timestamp': fill.timestamp,
                'symbol': fill.symbol,
                'side': fill.side.value,
                'quantity': fill.quantity,
                'price': fill.price,
                'commission': fill.commission,
                'slippage_bps': fill.slippage,
                'realized_pnl': fill.realized_pnl,
                'gross_value': fill.gross_value,
                'net_value': fill.net_value
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        
        # Export
        output_path = self.output_dir / filename
        df.to_csv(output_path, index=False)
        
        logger.info(f"Trade sheet exported: {output_path} ({len(fills)} trades)")
    
    def export_metrics(self, metrics: Dict, filename_json: str = "metrics.json", 
                      filename_txt: str = "metrics.txt"):
        """
        Export metrics to JSON and text files
        
        Args:
            metrics: Dict of metrics
            filename_json: JSON filename
            filename_txt: Text filename
        """
        if not metrics:
            logger.warning("No metrics to export")
            return
        
        # Export JSON
        json_path = self.output_dir / filename_json
        with open(json_path, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)
        
        logger.info(f"Metrics exported (JSON): {json_path}")
        
        # Export readable text
        txt_path = self.output_dir / filename_txt
        with open(txt_path, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("BACKTEST PERFORMANCE METRICS\n")
            f.write("=" * 60 + "\n\n")
            
            f.write("RETURN METRICS:\n")
            f.write(f"  Total Return:        {metrics.get('total_return', 0)*100:>10.2f}%\n")
            f.write(f"  CAGR:                {metrics.get('cagr', 0)*100:>10.2f}%\n")
            f.write(f"  Total P&L:           ${metrics.get('total_pnl', 0):>10,.2f}\n")
            f.write("\n")
            
            f.write("RISK METRICS:\n")
            f.write(f"  Volatility (Annual): {metrics.get('volatility', 0)*100:>10.2f}%\n")
            f.write(f"  Sharpe Ratio:        {metrics.get('sharpe_ratio', 0):>10.2f}\n")
            f.write(f"  Sortino Ratio:       {metrics.get('sortino_ratio', 0):>10.2f}\n")
            f.write(f"  Max Drawdown:        {metrics.get('max_drawdown', 0)*100:>10.2f}%\n")
            f.write(f"  Max DD Duration:     {metrics.get('max_drawdown_duration_days', 0):>10.0f} days\n")
            f.write(f"  Calmar Ratio:        {metrics.get('calmar_ratio', 0):>10.2f}\n")
            f.write(f"  VaR (95%):           {metrics.get('var_95', 0)*100:>10.2f}%\n")
            f.write(f"  CVaR (95%):          {metrics.get('cvar_95', 0)*100:>10.2f}%\n")
            f.write("\n")
            
            f.write("TRADE METRICS:\n")
            f.write(f"  Number of Trades:    {metrics.get('num_trades', 0):>10.0f}\n")
            f.write(f"  Win Rate:            {metrics.get('win_rate', 0)*100:>10.2f}%\n")
            f.write(f"  Profit Factor:       {metrics.get('profit_factor', 0):>10.2f}\n")
            f.write(f"  Average Win:         ${metrics.get('avg_win', 0):>10,.2f}\n")
            f.write(f"  Average Loss:        ${metrics.get('avg_loss', 0):>10,.2f}\n")
            f.write(f"  Expectancy:          ${metrics.get('expectancy', 0):>10,.2f}\n")
            f.write(f"  Total Commission:    ${metrics.get('total_commission', 0):>10,.2f}\n")
            f.write("\n")
            
            f.write("PERIOD:\n")
            f.write(f"  Start Date:          {metrics.get('start_date', 'N/A')}\n")
            f.write(f"  End Date:            {metrics.get('end_date', 'N/A')}\n")
            f.write(f"  Initial Capital:     ${metrics.get('initial_capital', 0):>10,.2f}\n")
            f.write(f"  Final Value:         ${metrics.get('final_value', 0):>10,.2f}\n")
            f.write("\n")
        
        logger.info(f"Metrics exported (TXT): {txt_path}")
    
    def plot_equity_curve(self, snapshots: List[PortfolioSnapshot], 
                         filename: str = "equity_curve.png"):
        """
        Plot equity curve
        
        Args:
            snapshots: List of portfolio snapshots
            filename: Output filename
        """
        if not snapshots:
            logger.warning("No snapshots to plot")
            return
        
        timestamps = [s.timestamp for s in snapshots]
        equity = [s.total_value for s in snapshots]
        
        plt.figure(figsize=(12, 6))
        plt.plot(timestamps, equity, linewidth=2, label='Portfolio Value')
        plt.xlabel('Date')
        plt.ylabel('Portfolio Value ($)')
        plt.title('Equity Curve')
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        # Format x-axis
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gcf().autofmt_xdate()
        
        output_path = self.output_dir / filename
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Equity curve plotted: {output_path}")
    
    def plot_drawdown(self, snapshots: List[PortfolioSnapshot], 
                     filename: str = "drawdown.png"):
        """
        Plot drawdown chart
        
        Args:
            snapshots: List of portfolio snapshots
            filename: Output filename
        """
        if not snapshots:
            logger.warning("No snapshots to plot")
            return
        
        timestamps = [s.timestamp for s in snapshots]
        equity = [s.total_value for s in snapshots]
        
        # Calculate drawdown
        equity_array = pd.Series(equity)
        running_max = equity_array.expanding().max()
        drawdown = (equity_array - running_max) / running_max * 100
        
        plt.figure(figsize=(12, 6))
        plt.fill_between(timestamps, drawdown, 0, alpha=0.3, color='red', label='Drawdown')
        plt.plot(timestamps, drawdown, color='darkred', linewidth=1)
        plt.xlabel('Date')
        plt.ylabel('Drawdown (%)')
        plt.title('Drawdown Chart')
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        # Format x-axis
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gcf().autofmt_xdate()
        
        output_path = self.output_dir / filename
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Drawdown chart plotted: {output_path}")
    
    def plot_returns_distribution(self, snapshots: List[PortfolioSnapshot], 
                                  filename: str = "returns_dist.png"):
        """
        Plot returns distribution histogram
        
        Args:
            snapshots: List of portfolio snapshots
            filename: Output filename
        """
        if len(snapshots) < 2:
            logger.warning("Not enough snapshots for returns distribution")
            return
        
        # Calculate returns
        equity = [s.total_value for s in snapshots]
        returns = pd.Series(equity).pct_change().dropna() * 100
        
        plt.figure(figsize=(12, 6))
        plt.hist(returns, bins=50, alpha=0.7, edgecolor='black')
        plt.xlabel('Return (%)')
        plt.ylabel('Frequency')
        plt.title('Returns Distribution')
        plt.grid(True, alpha=0.3)
        
        # Add vertical line at 0
        plt.axvline(x=0, color='red', linestyle='--', linewidth=1)
        
        # Add statistics
        plt.text(0.02, 0.98, f"Mean: {returns.mean():.2f}%\nStd: {returns.std():.2f}%",
                transform=plt.gca().transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        output_path = self.output_dir / filename
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Returns distribution plotted: {output_path}")
    
    def generate_all_reports(self,
                            snapshots: List[PortfolioSnapshot],
                            fills: List[Fill],
                            metrics: Dict):
        """
        Generate all reports
        
        Args:
            snapshots: List of portfolio snapshots
            fills: List of fills
            metrics: Metrics dict
        """
        logger.info("Generating all reports...")
        
        # Export data
        self.export_trade_sheet(fills)
        self.export_metrics(metrics)
        
        # Generate plots
        self.plot_equity_curve(snapshots)
        self.plot_drawdown(snapshots)
        self.plot_returns_distribution(snapshots)
        
        logger.info(f"All reports generated in: {self.output_dir}")
