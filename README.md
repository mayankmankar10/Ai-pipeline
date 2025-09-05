# Hindi PDF Processing Pipeline

An automated Python pipeline for processing Hindi PDFs with text extraction, transliteration, and structured data conversion.

## 🚀 Features

- **Automated Google Drive Integration**: Monitor folders for new PDF files and upload processed results
- **Multi-Method Text Extraction**: Direct PDF text extraction with OCR fallback for scanned documents
- **Hindi Text Processing**: Clean, parse, and extract structured data from Hindi text
- **Transliteration**: Convert Hindi names to English with lowercase variations
- **Deduplication**: Track processed files to avoid reprocessing using file IDs and hashes
- **CSV Generation**: Export structured data to clean CSV files with customizable columns
- **Error Handling**: Comprehensive error handling with retry mechanisms and logging
- **Monitoring**: Status tracking, reporting, and maintenance capabilities

## 📋 Prerequisites

- Python 3.8 or higher
- Google Drive API credentials (service account or OAuth)
- Tesseract OCR (for scanned PDF processing)

### System Requirements

**Windows:**
- Tesseract OCR: Download from [GitHub releases](https://github.com/UB-Mannheim/tesseract/wiki)
- Install to `C:\Program Files\Tesseract-OCR\`

**Linux/macOS:**
```bash
# Ubuntu/Debian
sudo apt-get install tesseract-ocr tesseract-ocr-hin

# macOS (Homebrew)
brew install tesseract tesseract-lang
```

## 🛠️ Installation

1. **Clone the repository:**
```bash
git clone <repository-url>
cd Ai-pipeline
```

2. **Create virtual environment:**
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Set up Google Drive API:**

   **Option A: Service Account (Recommended for automation)**
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select existing one
   - Enable Google Drive API
   - Create service account credentials
   - Download JSON key file and save as `credentials/service_account.json`
   - Share your Google Drive folders with the service account email

   **Option B: OAuth (Interactive)**
   - Create OAuth 2.0 credentials in Google Cloud Console
   - Download client secrets and save as `credentials/client_secret.json`

5. **Configure environment:**
```bash
cp .env.template .env
# Edit .env with your configuration
```

## ⚙️ Configuration

Create a `.env` file from the template and configure:

```env
# Google Drive API Configuration
GOOGLE_CREDENTIALS_PATH=credentials/service_account.json
INPUT_FOLDER_ID=your_input_folder_id_here
OUTPUT_FOLDER_ID=your_output_folder_id_here

# Processing Configuration
POLLING_INTERVAL_SECONDS=60
MAX_RETRIES=3

# OCR Configuration (Windows)
TESSERACT_PATH=C:\Program Files\Tesseract-OCR\tesseract.exe
OCR_LANGUAGE=hin+eng

# CSV Output Configuration
CSV_ENCODING=utf-8-sig
CSV_DELIMITER=,
```

### Getting Google Drive Folder IDs

1. Open Google Drive in browser
2. Navigate to desired folder
3. Copy the folder ID from the URL: `https://drive.google.com/drive/folders/FOLDER_ID_HERE`

## 🚦 Usage

### Basic Usage

Run the example script with different modes:

```bash
# Run single processing cycle
python examples/run_pipeline.py --mode single

# Run continuously with polling
python examples/run_pipeline.py --mode continuous

# Check pipeline status
python examples/run_pipeline.py --mode status

# Reprocess failed files
python examples/run_pipeline.py --mode reprocess
```

### Advanced Usage

```bash
# Process specific file by Google Drive ID
python examples/run_pipeline.py --mode file --file-id 1ABC123DEF456

# Generate comprehensive report
python examples/run_pipeline.py --mode report --output reports/status.json

# Use custom configuration
python examples/run_pipeline.py --config custom.env --mode single

# Enable verbose logging
python examples/run_pipeline.py --mode single --verbose
```

### Programmatic Usage

```python
from hindi_pdf_pipeline import HindiPDFPipeline

# Initialize pipeline
pipeline = HindiPDFPipeline()

# Run single processing cycle
results = pipeline.run_single_cycle()
print(f"Processed {results['processed']} files")

# Get pipeline status
status = pipeline.get_status()
print(f"Success rate: {status['file_tracker']['success_rate_percent']:.1f}%")

# Process specific file
success = pipeline.process_file_by_id('your_file_id_here')
```

## 📁 Project Structure

```
Ai-pipeline/
├── src/
│   └── hindi_pdf_pipeline/
│       ├── __init__.py
│       ├── config.py              # Configuration management
│       ├── drive_manager.py       # Google Drive API operations
│       ├── pdf_processor.py       # PDF text extraction
│       ├── text_processor.py      # Hindi text processing
│       ├── csv_generator.py       # CSV output generation
│       ├── file_tracker.py        # File deduplication
│       └── main_pipeline.py       # Main orchestrator
├── tests/
│   ├── conftest.py               # Test configuration
│   ├── test_config.py           # Configuration tests
│   └── test_text_processor.py   # Text processing tests
├── examples/
│   └── run_pipeline.py          # Example usage script
├── credentials/                  # API credentials (create this)
├── logs/                        # Log files
├── data/                        # Processing data
├── requirements.txt             # Python dependencies
├── .env.template               # Configuration template
└── README.md                   # This file
```

## 🔧 Components

### 1. Configuration Management (`config.py`)
- Environment variable loading with defaults
- Configuration validation
- Logging setup

### 2. Google Drive Manager (`drive_manager.py`)
- Authentication (service account + OAuth)
- File listing, downloading, uploading
- Retry logic with exponential backoff

### 3. PDF Processor (`pdf_processor.py`)
- Multiple extraction methods (PyPDF2, pdfplumber, PyMuPDF)
- OCR fallback with Tesseract
- Image enhancement for better OCR results

### 4. Hindi Text Processor (`text_processor.py`)
- Text cleaning and normalization
- Devanagari to Roman transliteration
- Name entity recognition
- Structured data extraction

### 5. CSV Generator (`csv_generator.py`)
- Flexible CSV output with customizable columns
- Unicode text handling
- Multiple output formats (CSV, Excel, JSON)

### 6. File Tracker (`file_tracker.py`)
- Persistent tracking of processed files
- Duplicate detection using file hashes
- Processing status management
- Cleanup of old records

### 7. Main Pipeline (`main_pipeline.py`)
- Orchestrates all components
- Error handling and retry logic
- Status monitoring and reporting
- Maintenance tasks

## 📊 Output Format

The pipeline generates CSV files with the following columns:

| Column | Description |
|--------|-------------|
| Original Filename | Source PDF filename |
| Hindi Name | Extracted Hindi text |
| English Name | Transliterated English text |
| English Name (Lowercase) | Lowercase version |
| Page Number | Source page number |
| Confidence Score | Extraction confidence (0-1) |
| Entity Type | Type of extracted entity |
| Extraction Timestamp | When the data was extracted |

## 🔍 Monitoring and Logging

### Log Files
- Main pipeline: `logs/pipeline.log`
- Example script: `examples/pipeline_example.log`

### Status Monitoring
```python
# Get comprehensive status
status = pipeline.get_status()

# Key metrics
print(f"Files processed: {status['pipeline']['files_processed']}")
print(f"Success rate: {status['file_tracker']['success_rate_percent']:.1f}%")
print(f"Total files tracked: {status['file_tracker']['total_files']}")
```

### Reporting
Generate detailed reports:
```bash
python examples/run_pipeline.py --mode report --output reports/status.json
```

## 🧪 Testing

Run the test suite:

```bash
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
```

### Test Structure
- `tests/conftest.py` - Test configuration and fixtures
- `tests/test_config.py` - Configuration module tests
- `tests/test_text_processor.py` - Text processing tests

## 🚨 Troubleshooting

### Common Issues

**1. Google Drive API Authentication Failed**
```
Error: Failed to load service account credentials
```
- Verify `credentials/service_account.json` exists and is valid
- Ensure service account email has access to Drive folders
- Check that Google Drive API is enabled in Google Cloud Console

**2. Tesseract OCR Not Found**
```
Error: Tesseract not found or not configured
```
- Install Tesseract OCR
- Update `TESSERACT_PATH` in `.env` file
- Test: `tesseract --version`

**3. Hindi Text Not Processing**
```
Error: No entities extracted
```
- Verify OCR language includes Hindi: `OCR_LANGUAGE=hin+eng`
- Check if PDF contains actual Hindi text vs. images
- Try different extraction methods: `direct`, `ocr`, `hybrid`

**4. CSV Encoding Issues**
```
Error: 'charmap' codec can't encode character
```
- Use `CSV_ENCODING=utf-8-sig` for Windows Excel compatibility
- For other tools, try `CSV_ENCODING=utf-8`

### Debug Mode

Enable verbose logging:
```bash
python examples/run_pipeline.py --mode single --verbose
```

Check pipeline status:
```bash
python examples/run_pipeline.py --mode status
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Run test suite: `pytest`
5. Commit changes: `git commit -m "Add feature"`
6. Push to branch: `git push origin feature-name`
7. Create Pull Request

### Development Setup

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run linting
flake8 src/ tests/

# Run type checking
mypy src/

# Format code
black src/ tests/
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- Create an issue for bugs or feature requests
- Check existing issues for solutions
- Review logs in `logs/` directory for detailed error information

## 🔄 Changelog

### Version 1.0.0
- Initial release
- Complete pipeline implementation
- Google Drive integration
- Hindi text processing and transliteration
- CSV generation with multiple formats
- Comprehensive testing suite
- Documentation and examples
