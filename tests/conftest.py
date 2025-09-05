"""
Pytest configuration and shared fixtures for Hindi PDF Pipeline tests.
"""

import os
import tempfile
import pytest
from pathlib import Path

# Add src directory to Python path for imports
import sys
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as td:
        yield Path(td)

@pytest.fixture
def sample_env_file():
    """Create a sample .env file for testing."""
    content = """
INPUT_FOLDER_ID=test_input_folder
OUTPUT_FOLDER_ID=test_output_folder
POLLING_INTERVAL_SECONDS=30
MAX_RETRIES=2
OCR_LANGUAGE=hin+eng
CSV_ENCODING=utf-8-sig
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
        f.write(content)
        temp_file = f.name
    
    yield temp_file
    
    # Cleanup
    try:
        os.unlink(temp_file)
    except:
        pass

@pytest.fixture
def sample_hindi_text():
    """Sample Hindi text for testing."""
    return "नाम: राम कुमार शर्मा आयु: 25 वर्ष पता: दिल्ली, भारत"

@pytest.fixture
def sample_pdf_content():
    """Sample PDF-like content for testing."""
    return """
    व्यक्तिगत जानकारी
    
    नाम: राम कुमार शर्मा
    पिता का नाम: श्याम लाल शर्मा  
    माता का नाम: सीता देवी
    आयु: 25 वर्ष
    जन्म तिथि: 15/08/1998
    पता: 123, मुख्य सड़क, नई दिल्ली - 110001
    
    शिक्षा:
    - बी.ए. (दिल्ली विश्वविद्यालय)
    - एम.ए. (जवाहरलाल नेहरू विश्वविद्यालय)
    """

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment before running tests."""
    # Suppress noisy logs during testing
    import logging
    logging.getLogger('googleapiclient.discovery').setLevel(logging.ERROR)
    logging.getLogger('google.auth.transport.requests').setLevel(logging.ERROR)
    
    # Create test directories if they don't exist
    test_dirs = ['logs', 'data', 'temp_downloads']
    for dir_name in test_dirs:
        Path(dir_name).mkdir(exist_ok=True)
    
    yield
    
    # Cleanup test directories (optional)
    # You might want to keep some for debugging

# Custom markers
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "requires_credentials: marks tests that require Google Drive credentials"
    )

# Skip tests that require credentials if not available
def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle conditional skipping."""
    skip_credentials = pytest.mark.skip(reason="Google Drive credentials not available")
    
    for item in items:
        if "requires_credentials" in item.keywords:
            # Check if credentials are available
            cred_paths = [
                "credentials/service_account.json",
                "credentials/client_secret.json"
            ]
            
            if not any(os.path.exists(path) for path in cred_paths):
                item.add_marker(skip_credentials)
