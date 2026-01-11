import yaml
from pathlib import Path
from typing import Any, Optional


class Config:
    """Configuration class that loads and provides access to config values."""

    def __init__(self, config_path: Optional[str] = None) -> None:
        """
        Initialize configuration from YAML file.

        Args:
            config_path (str): Path to the config YAML file
        """
        # Determine the location of this file (src/config/config.py)
        current_file = Path(__file__)
        
        # Calculate Project Root: src/config/ -> src/ -> Project Root
        self.project_root = current_file.parent.parent.parent

        if config_path:
            config_file = Path(config_path)
        else:
            # Dynamically get the path relative to this python file
            config_file = current_file.parent / "config.yaml"

        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_file, "r") as f:
            self._config = yaml.safe_load(f)

    def get(self, key_path: str, default: Optional[Any] = None) -> Optional[Any]:
        """
        Get a configuration value using dot notation.

        Args:
            key_path (str): Dot-separated path to config value (e.g., "loader.document_path")
            default: Default value if key not found

        Returns:
                Configuration value or default

        """
        keys = key_path.split(".")
        value = self._config

        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
        
    @property
    def data_generation(self) -> dict:
        """Get data generation configuration."""
        return self._config.get("data_generation", {})
    
    @property
    def queries(self) -> dict:
        """Get queries configuration."""
        return self._config.get("queries", {})

    @property
    def database(self) -> dict:
        """Get database configuration."""
        return self._config.get("database", {})


config = Config()