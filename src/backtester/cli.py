# CLI entrypoint: parse config, run backtest, export results
import argparse
import logging
import sys
from pathlib import Path

from .config import BacktesterConfig
from .engine import BacktestEngine
from .strategy import MovingAverageCrossStrategy, BuyAndHoldStrategy
from .utils import setup_logging


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Backtester - Modular daywise backtesting framework',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--config',
        type=str,
        required=True,
        help='Path to config YAML file'
    )
    
    parser.add_argument(
        '--strategy',
        type=str,
        default='ma_cross',
        choices=['ma_cross', 'buy_hold'],
        help='Strategy to use (default: ma_cross)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        help='Log file path (optional)'
    )
    
    return parser.parse_args()


def create_strategy(strategy_name: str):
    """
    Create strategy instance
    
    Args:
        strategy_name: Strategy name
        
    Returns:
        Strategy instance
    """
    if strategy_name == 'ma_cross':
        return MovingAverageCrossStrategy(
            fast_period=10,
            slow_period=20,
            square_off_eod=True
        )
    elif strategy_name == 'buy_hold':
        return BuyAndHoldStrategy(
            square_off_eod=True
        )
    else:
        raise ValueError(f"Unknown strategy: {strategy_name}")


def main():
    """Main entry point"""
    # Parse arguments
    args = parse_args()
    
    # Setup logging
    setup_logging(
        level=getattr(logging, args.log_level),
        log_file=args.log_file
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info("BACKTESTER")
        logger.info("=" * 60)
        
        # Load config
        logger.info(f"Loading config: {args.config}")
        config = BacktesterConfig.from_yaml(args.config)
        
        # Create engine
        logger.info("Initializing engine...")
        engine = BacktestEngine(config)
        
        # Create strategy
        logger.info(f"Creating strategy: {args.strategy}")
        strategy = create_strategy(args.strategy)
        engine.set_strategy(strategy)
        
        # Run backtest
        logger.info("Running backtest...")
        results = engine.run()
        
        # Print summary
        print("\n" + "=" * 60)
        print("BACKTEST RESULTS")
        print("=" * 60)
        
        metrics = results['metrics']
        
        print("\nRETURN METRICS:")
        print(f"  Total Return:        {metrics['total_return']*100:>10.2f}%")
        print(f"  CAGR:                {metrics['cagr']*100:>10.2f}%")
        print(f"  Total P&L:           ${metrics['total_pnl']:>10,.2f}")
        
        print("\nRISK METRICS:")
        print(f"  Volatility (Annual): {metrics['volatility']*100:>10.2f}%")
        print(f"  Sharpe Ratio:        {metrics['sharpe_ratio']:>10.2f}")
        print(f"  Sortino Ratio:       {metrics['sortino_ratio']:>10.2f}")
        print(f"  Max Drawdown:        {metrics['max_drawdown']*100:>10.2f}%")
        print(f"  Calmar Ratio:        {metrics['calmar_ratio']:>10.2f}")
        
        print("\nTRADE METRICS:")
        print(f"  Number of Trades:    {metrics['num_trades']:>10.0f}")
        print(f"  Win Rate:            {metrics['win_rate']*100:>10.2f}%")
        print(f"  Profit Factor:       {metrics['profit_factor']:>10.2f}")
        print(f"  Expectancy:          ${metrics['expectancy']:>10,.2f}")
        
        print("\nPERIOD:")
        print(f"  Start Date:          {metrics['start_date']}")
        print(f"  End Date:            {metrics['end_date']}")
        print(f"  Initial Capital:     ${metrics['initial_capital']:>10,.2f}")
        print(f"  Final Value:         ${metrics['final_value']:>10,.2f}")
        
        if config.reporting.generate_reports:
            print(f"\nReports generated in: {config.reporting.output_dir}")
        
        print("=" * 60 + "\n")
        
        logger.info("Backtest completed successfully")
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
