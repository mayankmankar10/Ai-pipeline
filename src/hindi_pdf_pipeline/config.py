"""
Configuration Management Module

Handles all configuration settings for the Hindi PDF processing pipeline.
Supports environment variables, default values, and validation.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

class Config:
    """
    Configuration manager for Hindi PDF Pipeline.
    
    Loads configuration from environment variables with sensible defaults.
    Validates configuration values and provides easy access to settings.
    """
    
    def __init__(self, env_file: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            env_file: Path to .env file. If None, looks for .env in current directory.
        """
        # Load environment variables from .env file
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()
        
        # Initialize configuration values
        self._config = self._load_config()
        self._validate_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        config = {}
        
        # Google Drive API Configuration
        config['google_credentials_path'] = os.getenv(
            'GOOGLE_CREDENTIALS_PATH', 
            'credentials/service_account.json'
        )
        config['google_token_path'] = os.getenv(
            'GOOGLE_TOKEN_PATH', 
            'credentials/token.json'
        )
        config['google_scopes'] = os.getenv(
            'GOOGLE_SCOPES', 
            'https://www.googleapis.com/auth/drive'
        ).split(',')
        
        # Google Drive Folder IDs
        config['input_folder_id'] = os.getenv('INPUT_FOLDER_ID')
        config['output_folder_id'] = os.getenv('OUTPUT_FOLDER_ID')
        
        # Processing Configuration
        config['polling_interval_seconds'] = int(os.getenv('POLLING_INTERVAL_SECONDS', '60'))
        config['max_retries'] = int(os.getenv('MAX_RETRIES', '3'))
        config['retry_delay_seconds'] = int(os.getenv('RETRY_DELAY_SECONDS', '5'))
        
        # OCR Configuration
        config['tesseract_path'] = os.getenv(
            'TESSERACT_PATH', 
            'C:\\Program Files\\Tesseract-OCR\\tesseract.exe'
        )
        config['ocr_language'] = os.getenv('OCR_LANGUAGE', 'hin+eng')
        config['ocr_config'] = os.getenv('OCR_CONFIG', '--oem 3 --psm 6')
        
        # Logging Configuration
        config['log_level'] = os.getenv('LOG_LEVEL', 'INFO')
        config['log_file'] = os.getenv('LOG_FILE', 'logs/pipeline.log')
        config['max_log_size_mb'] = int(os.getenv('MAX_LOG_SIZE_MB', '10'))
        config['log_backup_count'] = int(os.getenv('LOG_BACKUP_COUNT', '5'))
        
        # CSV Output Configuration
        config['csv_encoding'] = os.getenv('CSV_ENCODING', 'utf-8-sig')
        config['csv_delimiter'] = os.getenv('CSV_DELIMITER', ',')
        
        # File Tracking
        config['tracking_db_path'] = os.getenv('TRACKING_DB_PATH', 'data/processed_files.json')
        
        # Default CSV columns for output
        config['default_csv_columns'] = [
            'original_filename',
            'hindi_name',
            'english_name', 
            'english_name_lowercase',
            'extraction_timestamp',
            'page_number',
            'confidence_score'
        ]
        
        return config
    
    def _validate_config(self) -> None:
        """Validate critical configuration values."""
        errors = []
        
        # Check for required folder IDs
        if not self._config['input_folder_id']:
            errors.append("INPUT_FOLDER_ID is required")
        
        if not self._config['output_folder_id']:
            errors.append("OUTPUT_FOLDER_ID is required")
        
        # Check if Tesseract path exists (only if on Windows)
        tesseract_path = self._config['tesseract_path']
        if os.name == 'nt' and not os.path.exists(tesseract_path):
            logging.warning(f"Tesseract not found at {tesseract_path}. OCR may not work properly.")
        
        # Validate polling interval
        if self._config['polling_interval_seconds'] < 1:
            errors.append("POLLING_INTERVAL_SECONDS must be at least 1")
        
        # Validate retry settings
        if self._config['max_retries'] < 0:
            errors.append("MAX_RETRIES must be non-negative")
        
        if self._config['retry_delay_seconds'] < 0:
            errors.append("RETRY_DELAY_SECONDS must be non-negative")
        
        if errors:
            raise ValueError(f"Configuration errors: {'; '.join(errors)}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        return self._config.get(key, default)
    
    def get_all(self) -> Dict[str, Any]:
        """Get all configuration values."""
        return self._config.copy()
    
    def update(self, updates: Dict[str, Any]) -> None:
        """
        Update configuration with new values.
        
        Args:
            updates: Dictionary of configuration updates
        """
        self._config.update(updates)
        self._validate_config()
    
    def setup_logging(self) -> None:
        """Set up logging configuration based on config values."""
        from logging.handlers import RotatingFileHandler
        import colorlog
        
        # Create logs directory if it doesn't exist
        log_file = Path(self._config['log_file'])
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure logging level
        log_level = getattr(logging, self._config['log_level'].upper(), logging.INFO)
        
        # Create formatters
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        console_formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green', 
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            }
        )
        
        # Setup root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Remove existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Add file handler with rotation
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=self._config['max_log_size_mb'] * 1024 * 1024,
            backupCount=self._config['log_backup_count'],
            encoding='utf-8'
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(log_level)
        root_logger.addHandler(file_handler)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(log_level)
        root_logger.addHandler(console_handler)
        
        logging.info("Logging configuration initialized")
    
    @property
    def google_credentials_path(self) -> str:
        """Path to Google service account credentials."""
        return self._config['google_credentials_path']
    
    @property
    def google_token_path(self) -> str:
        """Path to Google OAuth token."""
        return self._config['google_token_path']
    
    @property
    def google_scopes(self) -> List[str]:
        """Google API scopes."""
        return self._config['google_scopes']
    
    @property
    def input_folder_id(self) -> str:
        """Google Drive input folder ID."""
        return self._config['input_folder_id']
    
    @property
    def output_folder_id(self) -> str:
        """Google Drive output folder ID."""
        return self._config['output_folder_id']
    
    @property
    def polling_interval_seconds(self) -> int:
        """Polling interval in seconds."""
        return self._config['polling_interval_seconds']
    
    @property
    def max_retries(self) -> int:
        """Maximum number of retries."""
        return self._config['max_retries']
    
    @property
    def retry_delay_seconds(self) -> int:
        """Delay between retries in seconds."""
        return self._config['retry_delay_seconds']
    
    @property
    def tesseract_path(self) -> str:
        """Path to Tesseract executable."""
        return self._config['tesseract_path']
    
    @property
    def ocr_language(self) -> str:
        """OCR language configuration."""
        return self._config['ocr_language']
    
    @property
    def ocr_config(self) -> str:
        """OCR configuration parameters."""
        return self._config['ocr_config']
    
    @property
    def csv_encoding(self) -> str:
        """CSV file encoding."""
        return self._config['csv_encoding']
    
    @property
    def csv_delimiter(self) -> str:
        """CSV delimiter character."""
        return self._config['csv_delimiter']
    
    @property
    def tracking_db_path(self) -> str:
        """Path to file tracking database."""
        return self._config['tracking_db_path']
    
    @property
    def default_csv_columns(self) -> List[str]:
        """Default CSV column names."""
        return self._config['default_csv_columns']


# Global config instance
_config_instance: Optional[Config] = None

def get_config(env_file: Optional[str] = None) -> Config:
    """
    Get global configuration instance.
    
    Args:
        env_file: Path to .env file for first-time initialization
        
    Returns:
        Config instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = Config(env_file)
    return _config_instance

def reload_config(env_file: Optional[str] = None) -> Config:
    """
    Reload configuration (useful for testing).
    
    Args:
        env_file: Path to .env file
        
    Returns:
        New Config instance
    """
    global _config_instance
    _config_instance = Config(env_file)
    return _config_instance
