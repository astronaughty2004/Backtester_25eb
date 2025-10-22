# load/validate config, default values
import yaml
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class DataConfig:
    """Data source configuration"""
    price_data: str
    signal_file: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    timeframe: str = "1D"
    adjust_prices: bool = True


@dataclass
class CapitalConfig:
    """Capital configuration"""
    initial: float = 100000.0
    currency: str = "USD"


@dataclass
class ExecutionConfig:
    """Execution simulation configuration"""
    slippage_bps: float = 5.0
    commission_bps: float = 2.0
    fill_model: str = "first_touch"
    partial_fills: bool = False


@dataclass
class RiskConfig:
    """Risk management configuration"""
    max_position_pct: float = 0.20
    max_portfolio_leverage: float = 1.0
    stop_loss_pct: Optional[float] = None
    take_profit_pct: Optional[float] = None
    sizing_method: str = "fraction"
    vol_lookback: int = 20
    target_vol: float = 0.15
    max_positions: Optional[int] = None


@dataclass
class EODConfig:
    """End-of-day configuration"""
    close_all_eod: bool = False
    mtm_frequency: str = "daily"


@dataclass
class ReportingConfig:
    """Reporting configuration"""
    output_dir: str = "results"
    export_trades: bool = True
    export_metrics: bool = True
    generate_plots: bool = True
    plots: List[str] = field(default_factory=lambda: ["equity_curve", "drawdown"])


@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = "INFO"
    file: str = "backtest.log"
    console: bool = True


@dataclass
class BacktesterConfig:
    """Complete backtester configuration"""
    data: DataConfig
    capital: CapitalConfig
    execution: ExecutionConfig
    risk: RiskConfig
    eod: EODConfig
    reporting: ReportingConfig
    logging: LoggingConfig
    
    @classmethod
    def from_yaml(cls, config_path: str) -> 'BacktesterConfig':
        """Load configuration from YAML file"""
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        
        return cls.from_dict(config_dict)
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'BacktesterConfig':
        """Create config from dictionary"""
        # Data config
        data_config = DataConfig(
            price_data=config_dict['data']['price_data'],
            signal_file=config_dict['data'].get('signal_file'),
            start_date=config_dict['data'].get('start_date'),
            end_date=config_dict['data'].get('end_date'),
            timeframe=config_dict['data'].get('timeframe', '1D'),
            adjust_prices=config_dict['data'].get('adjust_prices', True)
        )
        
        # Capital config
        capital_dict = config_dict.get('capital', {})
        capital_config = CapitalConfig(
            initial=capital_dict.get('initial', 100000.0),
            currency=capital_dict.get('currency', 'USD')
        )
        
        # Execution config
        exec_dict = config_dict.get('execution', {})
        execution_config = ExecutionConfig(
            slippage_bps=exec_dict.get('slippage_bps', 5.0),
            commission_bps=exec_dict.get('commission_bps', 2.0),
            fill_model=exec_dict.get('fill_model', 'first_touch'),
            partial_fills=exec_dict.get('partial_fills', False)
        )
        
        # Risk config
        risk_dict = config_dict.get('risk', {})
        risk_config = RiskConfig(
            max_position_pct=risk_dict.get('max_position_pct', 0.20),
            max_portfolio_leverage=risk_dict.get('max_portfolio_leverage', 1.0),
            stop_loss_pct=risk_dict.get('stop_loss_pct'),
            take_profit_pct=risk_dict.get('take_profit_pct'),
            sizing_method=risk_dict.get('sizing_method', 'fraction'),
            vol_lookback=risk_dict.get('vol_lookback', 20),
            target_vol=risk_dict.get('target_vol', 0.15),
            max_positions=risk_dict.get('max_positions')
        )
        
        # EOD config
        eod_dict = config_dict.get('eod', {})
        eod_config = EODConfig(
            close_all_eod=eod_dict.get('close_all_eod', False),
            mtm_frequency=eod_dict.get('mtm_frequency', 'daily')
        )
        
        # Reporting config
        report_dict = config_dict.get('reporting', {})
        reporting_config = ReportingConfig(
            output_dir=report_dict.get('output_dir', 'results'),
            export_trades=report_dict.get('export_trades', True),
            export_metrics=report_dict.get('export_metrics', True),
            generate_plots=report_dict.get('generate_plots', True),
            plots=report_dict.get('plots', ['equity_curve', 'drawdown'])
        )
        
        # Logging config
        log_dict = config_dict.get('logging', {})
        logging_config = LoggingConfig(
            level=log_dict.get('level', 'INFO'),
            file=log_dict.get('file', 'backtest.log'),
            console=log_dict.get('console', True)
        )
        
        return cls(
            data=data_config,
            capital=capital_config,
            execution=execution_config,
            risk=risk_config,
            eod=eod_config,
            reporting=reporting_config,
            logging=logging_config
        )
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        # Validate data config
        if not self.data.price_data:
            errors.append("data.price_data is required")
        
        # Validate capital
        if self.capital.initial <= 0:
            errors.append("capital.initial must be positive")
        
        # Validate execution
        if self.execution.slippage_bps < 0:
            errors.append("execution.slippage_bps must be non-negative")
        if self.execution.commission_bps < 0:
            errors.append("execution.commission_bps must be non-negative")
        
        # Validate risk
        if not 0 < self.risk.max_position_pct <= 1:
            errors.append("risk.max_position_pct must be between 0 and 1")
        if self.risk.max_portfolio_leverage < 0:
            errors.append("risk.max_portfolio_leverage must be non-negative")
        
        return errors
    
    def __post_init__(self):
        """Validate after initialization"""
        errors = self.validate()
        if errors:
            error_msg = "\n".join([f"  - {e}" for e in errors])
            raise ValueError(f"Configuration validation failed:\n{error_msg}")
