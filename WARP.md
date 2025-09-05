# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

This is the **Hindi PDF Processing Pipeline** - an automated Python pipeline for processing Hindi PDFs with text extraction, transliteration, and structured data conversion. The project provides end-to-end processing from Google Drive PDFs to structured CSV output with Hindi-to-English transliteration.

## Development Commands

### Environment Setup
```powershell
# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/macOS

# Install dependencies
pip install -r requirements.txt

# Set up configuration
cp .env.template .env
# Edit .env with your Google Drive folder IDs and credentials
```

### Running the Pipeline
```powershell
# Run single processing cycle
python examples/run_pipeline.py --mode single

# Run continuously with polling
python examples/run_pipeline.py --mode continuous

# Check pipeline status
python examples/run_pipeline.py --mode status

# Reprocess failed files
python examples/run_pipeline.py --mode reprocess

# Process specific file by Google Drive ID
python examples/run_pipeline.py --mode file --file-id 1ABC123DEF456

# Generate comprehensive report
python examples/run_pipeline.py --mode report --output reports/status.json

# Enable verbose logging
python examples/run_pipeline.py --mode single --verbose
```

### Testing
```powershell
# Run all tests
pytest

# Run with coverage
pytest --cov=hindi_pdf_pipeline

# Run specific test categories
pytest -m unit          # Unit tests only
pytest -m integration   # Integration tests only
pytest -m "not slow"    # Skip slow tests

# Run tests with verbose output
pytest -v

# Run specific test file
pytest tests/test_config.py -v
```

### Development Tasks
```powershell
# Run linting (if configured)
flake8 src/ tests/

# Run type checking (if configured)  
mypy src/

# Format code (if configured)
black src/ tests/

# Install in development mode
pip install -e .
```

## Architecture Overview

The project follows a **modular pipeline architecture** with clear separation of concerns:

### Core Components

1. **HindiPDFPipeline** (`main_pipeline.py`) - Main orchestrator
   - Coordinates all processing components
   - Manages error handling and retry logic
   - Provides status monitoring and reporting
   - Handles continuous and scheduled processing

2. **GoogleDriveManager** (`drive_manager.py`) - Google Drive integration
   - Handles authentication (service account + OAuth)
   - Monitors folders for new PDF files
   - Downloads/uploads files with retry mechanisms
   - Manages file metadata and checksums

3. **PDFProcessor** (`pdf_processor.py`) - Text extraction
   - Multiple extraction methods (PyPDF2, pdfplumber, PyMuPDF)
   - OCR fallback with Tesseract for scanned documents
   - Image enhancement for better OCR results
   - Text extraction validation

4. **HindiTextProcessor** (`text_processor.py`) - Text processing
   - Hindi text cleaning and normalization
   - Devanagari to Roman transliteration using indic-transliteration
   - Name entity extraction with pattern matching
   - Structured data parsing from Hindi text

5. **CSVGenerator** (`csv_generator.py`) - Output generation
   - Flexible CSV output with customizable columns
   - Unicode text handling for Hindi/English content
   - Data validation and formatting
   - Multiple output format support

6. **FileTracker** (`file_tracker.py`) - Deduplication system
   - Persistent tracking of processed files using JSON storage
   - Duplicate detection using file IDs and MD5 hashes
   - Processing status management (pending, processing, completed, failed)
   - Cleanup of old records and retry logic

7. **Config** (`config.py`) - Configuration management
   - Environment variable loading with .env support
   - Configuration validation and defaults
   - Centralized logging setup
   - Property-based access to settings

### Data Flow

1. **Monitoring**: GoogleDriveManager polls input folder for new PDFs
2. **Download**: Files downloaded to temporary local storage
3. **Extraction**: PDFProcessor extracts text using hybrid approach (direct + OCR fallback)
4. **Processing**: HindiTextProcessor cleans text, extracts entities, and transliterates Hindi to English
5. **Generation**: CSVGenerator creates structured CSV with extracted data
6. **Upload**: Results uploaded to Google Drive output folder
7. **Tracking**: FileTracker records processing status and prevents reprocessing

### Key Design Patterns

- **Component-based architecture**: Each module has single responsibility
- **Configuration-driven**: All settings externalized via environment variables
- **Retry mechanisms**: Built-in retry logic with exponential backoff
- **Status tracking**: Comprehensive monitoring and error reporting
- **Resource management**: Proper cleanup of temporary files and resources

## Configuration Requirements

### Essential Environment Variables
```env
# Required: Google Drive folder IDs (get from Drive folder URLs)
INPUT_FOLDER_ID=your_input_folder_id_here
OUTPUT_FOLDER_ID=your_output_folder_id_here

# Required: Google API credentials
GOOGLE_CREDENTIALS_PATH=credentials/service_account.json

# Optional: Processing settings
POLLING_INTERVAL_SECONDS=60
MAX_RETRIES=3
OCR_LANGUAGE=hin+eng
```

### Google Drive API Setup
1. Create project in Google Cloud Console
2. Enable Google Drive API
3. Create service account and download JSON credentials
4. Share Drive folders with service account email
5. Set folder IDs in `.env` file

### OCR Requirements
- **Windows**: Download Tesseract from GitHub releases, install to `C:\Program Files\Tesseract-OCR\`
- **Linux**: `sudo apt-get install tesseract-ocr tesseract-ocr-hin`
- **macOS**: `brew install tesseract tesseract-lang`

## File Structure Context

```
src/hindi_pdf_pipeline/     # Main package
├── __init__.py            # Package exports
├── main_pipeline.py       # Pipeline orchestrator
├── drive_manager.py       # Google Drive operations
├── pdf_processor.py       # PDF text extraction
├── text_processor.py      # Hindi text processing
├── csv_generator.py       # CSV output generation
├── file_tracker.py        # File deduplication
└── config.py             # Configuration management

examples/                  # Usage examples
└── run_pipeline.py       # Main CLI interface

tests/                     # Test suite
├── conftest.py           # Test fixtures and configuration
├── test_config.py        # Configuration tests
└── test_text_processor.py # Text processing tests
```

## Development Guidelines

### Code Organization
- Each module handles one primary responsibility
- Configuration is centralized and environment-driven
- All components accept Config instance for consistency
- Logging is configured centrally and used throughout

### Error Handling
- Use structured exception handling with meaningful error messages
- Implement retry mechanisms for external API calls
- Log errors with context (file names, IDs, etc.)
- Fail gracefully and continue processing other files when possible

### Testing Approach
- Unit tests for individual components
- Integration tests for end-to-end workflows
- Use pytest fixtures for common test data
- Mock external dependencies (Google Drive API, OCR)
- Test both success and failure scenarios

### Hindi Text Processing Considerations
- Always handle Unicode properly (UTF-8 encoding)
- Use indic-transliteration library for Devanagari conversion
- Implement pattern matching for Hindi text structures
- Consider OCR quality and implement confidence scoring
- Filter out common Hindi stop words from entity extraction

### Google Drive Integration
- Handle API rate limits with exponential backoff
- Implement proper OAuth/service account authentication
- Cache file metadata to avoid unnecessary API calls
- Use file checksums for duplicate detection
- Handle large file downloads with progress tracking

### Performance Considerations
- Process files in batches when possible
- Use temporary directories for file operations
- Implement cleanup of temporary files
- Monitor memory usage for large PDF processing
- Use threading locks for concurrent operations

## Common Tasks

### Adding New Text Processing Rules
1. Modify pattern matching in `text_processor.py`
2. Update `_compile_patterns()` method with new regex patterns
3. Add corresponding entity extraction logic
4. Update tests in `test_text_processor.py`

### Adding New CSV Columns
1. Update `default_csv_columns` in `config.py`
2. Modify data structure in `CSVGenerator.generate_csv_with_pandas()`
3. Update corresponding tests and documentation

### Debugging Processing Issues
1. Enable verbose logging: `--verbose` flag or `LOG_LEVEL=DEBUG`
2. Check logs in `logs/pipeline.log`
3. Use status command to check pipeline state
4. Verify Google Drive folder permissions and IDs
5. Test OCR functionality with sample Hindi PDFs

### Extending for New Languages
1. Add new language support in `text_processor.py`
2. Update transliteration methods for target script
3. Add language-specific patterns and stop words
4. Update OCR language configuration
