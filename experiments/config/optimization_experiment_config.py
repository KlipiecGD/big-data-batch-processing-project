import yaml
from pathlib import Path
from typing import Dict, Any


class OptimizationConfig:
    """Configuration for Spark optimization experiments"""

    def __init__(self, config_path: str = "experiments/config/optimization_config.yaml") -> None:
        """
        Load experiment configuration from YAML file
        Args:
            config_path (str): Path to the YAML configuration file
        """
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Experiment config not found: {config_path}")
        
        with open(config_file, 'r') as f:
            self._config = yaml.safe_load(f)
    
    def get_experiment_config(self, experiment_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific experiment
        Args:
            experiment_name (str): Name of the experiment
        """
        experiments = self._config.get('experiments', {})
        
        if experiment_name not in experiments:
            available = list(experiments.keys())
            raise ValueError(
                f"Experiment '{experiment_name}' not found. "
                f"Available experiments: {available}"
            )
        
        return experiments[experiment_name]
    
    def get_all_experiments(self) -> Dict[str, Dict[str, Any]]:
        """Get all experiment configurations"""
        return self._config.get('experiments', {})
    
    def get_execution_config(self) -> Dict[str, Any]:
        """Get execution settings"""
        return self._config.get('execution', {})
    
    def get_metrics_config(self) -> list:
        """Get metrics to collect"""
        return self._config.get('metrics', [])
    
    def get_paths_config(self) -> Dict[str, str]:
        """Get paths configuration"""
        return self._config.get('paths', {})
optimization_config = OptimizationConfig()