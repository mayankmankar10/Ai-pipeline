"""
Unit tests for configuration module.
"""

import os
import tempfile
import pytest
from unittest.mock import patch, mock_open
from pathlib import Path

from src.hindi_pdf_pipeline.config import Config, get_config, reload_config

class TestConfig:
    """Test cases for Config class."""
    
    def test_config_initialization_default(self):
        """Test config initialization with defaults."""
        config = Config()
        
        assert config.google_credentials_path == 'credentials/service_account.json'
        assert config.polling_interval_seconds == 60
        assert config.max_retries == 3
        assert config.csv_encoding == 'utf-8-sig'
        assert len(config.default_csv_columns) > 0
    
    def test_config_initialization_with_env_file(self):
        """Test config initialization with custom env file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
            f.write("POLLING_INTERVAL_SECONDS=120\n")
            f.write("MAX_RETRIES=5\n")
            f.write("INPUT_FOLDER_ID=test_input_folder\n")
            f.write("OUTPUT_FOLDER_ID=test_output_folder\n")
            env_file = f.name
        
        try:
            config = Config(env_file=env_file)
            assert config.polling_interval_seconds == 120
            assert config.max_retries == 5
            assert config.input_folder_id == "test_input_folder"
            assert config.output_folder_id == "test_output_folder"
        finally:
            os.unlink(env_file)
    
    def test_config_validation_missing_folder_ids(self):
        """Test config validation with missing required values."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
            f.write("POLLING_INTERVAL_SECONDS=60\n")
            env_file = f.name
        
        try:
            with pytest.raises(ValueError, match="INPUT_FOLDER_ID is required"):
                Config(env_file=env_file)
        finally:
            os.unlink(env_file)
    
    def test_config_validation_invalid_values(self):
        """Test config validation with invalid values."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
            f.write("INPUT_FOLDER_ID=test_input\n")
            f.write("OUTPUT_FOLDER_ID=test_output\n") 
            f.write("POLLING_INTERVAL_SECONDS=0\n")
            f.write("MAX_RETRIES=-1\n")
            env_file = f.name
        
        try:
            with pytest.raises(ValueError, match="POLLING_INTERVAL_SECONDS must be at least 1"):
                Config(env_file=env_file)
        finally:
            os.unlink(env_file)
    
    def test_config_get_method(self):
        """Test config get method."""
        config = Config()
        
        assert config.get('polling_interval_seconds') == 60
        assert config.get('nonexistent_key', 'default') == 'default'
        assert config.get('nonexistent_key') is None
    
    def test_config_update_method(self):
        """Test config update method."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
            f.write("INPUT_FOLDER_ID=test_input\n")
            f.write("OUTPUT_FOLDER_ID=test_output\n")
            env_file = f.name
        
        try:
            config = Config(env_file=env_file)
            original_interval = config.polling_interval_seconds
            
            config.update({'polling_interval_seconds': 300})
            assert config.polling_interval_seconds == 300
            assert config.get('polling_interval_seconds') == 300
        finally:
            os.unlink(env_file)
    
    def test_config_properties(self):
        """Test config property accessors."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
            f.write("INPUT_FOLDER_ID=test_input\n")
            f.write("OUTPUT_FOLDER_ID=test_output\n")
            f.write("GOOGLE_SCOPES=scope1,scope2,scope3\n")
            env_file = f.name
        
        try:
            config = Config(env_file=env_file)
            
            assert config.input_folder_id == "test_input"
            assert config.output_folder_id == "test_output"
            assert config.google_scopes == ["scope1", "scope2", "scope3"]
        finally:
            os.unlink(env_file)
    
    @patch('logging.getLogger')
    def test_setup_logging(self, mock_get_logger):
        """Test logging setup."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
            f.write("INPUT_FOLDER_ID=test_input\n")
            f.write("OUTPUT_FOLDER_ID=test_output\n")
            f.write("LOG_LEVEL=DEBUG\n")
            env_file = f.name
        
        try:
            config = Config(env_file=env_file)
            
            # Create temporary log directory
            with tempfile.TemporaryDirectory() as temp_dir:
                log_file = os.path.join(temp_dir, "test.log")
                config.update({'log_file': log_file})
                
                # Should not raise exception
                config.setup_logging()
                
                # Check that log file was created
                assert os.path.exists(log_file)
        finally:
            os.unlink(env_file)

class TestConfigGlobals:
    """Test global config functions."""
    
    def test_get_config_singleton(self):
        """Test that get_config returns singleton instance."""
        config1 = get_config()
        config2 = get_config()
        
        # Should be the same instance
        assert config1 is config2
    
    def test_reload_config(self):
        """Test config reload functionality."""
        config1 = get_config()
        config2 = reload_config()
        
        # Should be different instances after reload
        assert config1 is not config2
        
        # But subsequent calls should return same instance
        config3 = get_config()
        assert config2 is config3

class TestConfigIntegration:
    """Integration tests for config module."""
    
    def test_config_with_real_env_file(self):
        """Test config with actual .env file content."""
        env_content = """
# Google Drive Configuration
GOOGLE_CREDENTIALS_PATH=test_credentials.json
GOOGLE_TOKEN_PATH=test_token.json
INPUT_FOLDER_ID=1234567890abcdef
OUTPUT_FOLDER_ID=fedcba0987654321

# Processing Configuration  
POLLING_INTERVAL_SECONDS=90
MAX_RETRIES=2

# OCR Configuration
OCR_LANGUAGE=hin
OCR_CONFIG=--oem 1 --psm 3

# CSV Configuration
CSV_ENCODING=utf-8
CSV_DELIMITER=;
"""
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
            f.write(env_content)
            env_file = f.name
        
        try:
            config = Config(env_file=env_file)
            
            # Verify all values were loaded correctly
            assert config.google_credentials_path == "test_credentials.json"
            assert config.google_token_path == "test_token.json"
            assert config.input_folder_id == "1234567890abcdef"
            assert config.output_folder_id == "fedcba0987654321"
            assert config.polling_interval_seconds == 90
            assert config.max_retries == 2
            assert config.ocr_language == "hin"
            assert config.ocr_config == "--oem 1 --psm 3"
            assert config.csv_encoding == "utf-8"
            assert config.csv_delimiter == ";"
            
        finally:
            os.unlink(env_file)
    
    def test_config_partial_override(self):
        """Test config with partial environment variable override."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.env') as f:
            f.write("INPUT_FOLDER_ID=test_input\n")
            f.write("OUTPUT_FOLDER_ID=test_output\n")
            f.write("POLLING_INTERVAL_SECONDS=45\n")
            # Other values should use defaults
            env_file = f.name
        
        try:
            config = Config(env_file=env_file)
            
            # Overridden values
            assert config.input_folder_id == "test_input"
            assert config.output_folder_id == "test_output"
            assert config.polling_interval_seconds == 45
            
            # Default values
            assert config.max_retries == 3
            assert config.csv_encoding == "utf-8-sig"
            assert config.csv_delimiter == ","
            
        finally:
            os.unlink(env_file)

if __name__ == "__main__":
    pytest.main([__file__])
