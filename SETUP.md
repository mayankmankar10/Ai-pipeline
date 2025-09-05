# Quick Setup Guide

This guide will help you get the Hindi PDF Processing Pipeline up and running quickly.

## 📋 Prerequisites Checklist

- [ ] Python 3.8+ installed
- [ ] Git installed
- [ ] Google account with access to Google Drive
- [ ] Tesseract OCR installed (for scanned PDFs)

## 🚀 Quick Start (10 minutes)

### Step 1: Download and Install

```bash
# Clone the repository
git clone <repository-url>
cd Ai-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Install Tesseract OCR

**Windows:**
1. Download from: https://github.com/UB-Mannheim/tesseract/wiki
2. Install to default location: `C:\Program Files\Tesseract-OCR\`
3. Verify: `tesseract --version`

**Linux/macOS:**
```bash
# Ubuntu/Debian
sudo apt-get install tesseract-ocr tesseract-ocr-hin

# macOS
brew install tesseract tesseract-lang

# Verify installation
tesseract --version
```

### Step 3: Set Up Google Drive API

#### Option A: Service Account (Recommended)

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing
3. Enable Google Drive API:
   - Go to "APIs & Services" > "Library"
   - Search for "Google Drive API"
   - Click "Enable"

4. Create service account:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "Service Account"
   - Fill in details and click "Create"
   - Skip role assignment (click "Continue")
   - Click "Done"

5. Generate key:
   - Click on created service account
   - Go to "Keys" tab
   - Click "Add Key" > "Create New Key" > "JSON"
   - Download and save as `credentials/service_account.json`

6. Note the service account email (looks like `name@project.iam.gserviceaccount.com`)

#### Option B: OAuth (Interactive)

1. In Google Cloud Console > "Credentials"
2. Click "Create Credentials" > "OAuth 2.0 Client ID"
3. Choose "Desktop Application"
4. Download client secrets as `credentials/client_secret.json`

### Step 4: Create Google Drive Folders

1. Create two folders in Google Drive:
   - `Hindi PDF Input` (for source PDFs)
   - `Hindi PDF Output` (for processed CSVs)

2. Share folders with service account:
   - Right-click folder > "Share"
   - Add service account email
   - Give "Editor" permissions

3. Get folder IDs:
   - Open folder in browser
   - Copy ID from URL: `https://drive.google.com/drive/folders/FOLDER_ID_HERE`

### Step 5: Configure Environment

```bash
# Copy template
cp .env.template .env

# Edit .env file
nano .env  # or use your favorite editor
```

Minimal `.env` configuration:
```env
# Required: Google Drive folder IDs
INPUT_FOLDER_ID=your_input_folder_id_here
OUTPUT_FOLDER_ID=your_output_folder_id_here

# Required: Credentials path
GOOGLE_CREDENTIALS_PATH=credentials/service_account.json

# Optional: Tesseract path (auto-detected on Linux/macOS)
TESSERACT_PATH=C:\Program Files\Tesseract-OCR\tesseract.exe
```

### Step 6: Test the Pipeline

```bash
# Test configuration
python examples/run_pipeline.py --mode status

# Upload a Hindi PDF to your input folder, then run:
python examples/run_pipeline.py --mode single
```

## 🧪 Testing Your Setup

### 1. Verify Installation
```bash
python -c "import hindi_pdf_pipeline; print('✅ Pipeline installed successfully')"
```

### 2. Test Google Drive Connection
```bash
python examples/run_pipeline.py --mode status
```
You should see folder statistics without errors.

### 3. Test with Sample PDF
1. Upload a Hindi PDF to your input Google Drive folder
2. Run: `python examples/run_pipeline.py --mode single`
3. Check your output folder for generated CSV

### 4. Run Unit Tests
```bash
pytest tests/ -v
```

## 🐛 Troubleshooting Quick Fixes

### "No module named 'hindi_pdf_pipeline'"
```bash
# Make sure you're in the right directory and virtual environment
cd Ai-pipeline
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### "INPUT_FOLDER_ID is required"
```bash
# Check your .env file
cat .env
# Make sure folder IDs are set correctly
```

### "Tesseract not found"
```bash
# Test Tesseract installation
tesseract --version

# If not found, install and update .env with correct path
```

### "Failed to authenticate with Google Drive"
```bash
# Check credentials file exists
ls credentials/
# Verify service account has folder access
```

### "No PDF files found"
```bash
# Make sure PDFs are uploaded to correct folder
# Check folder permissions for service account
```

## 📈 Next Steps

Once basic setup is working:

1. **Continuous Processing**: `python examples/run_pipeline.py --mode continuous`
2. **Monitoring**: Set up log monitoring and alerts
3. **Customization**: Modify CSV columns in configuration
4. **Scaling**: Run on server with scheduled tasks

## 🔗 Useful Links

- [Google Cloud Console](https://console.cloud.google.com/)
- [Google Drive API Documentation](https://developers.google.com/drive/api)
- [Tesseract OCR Documentation](https://github.com/tesseract-ocr/tesseract)
- [Python Virtual Environments](https://docs.python.org/3/tutorial/venv.html)

## 💡 Tips

- **Service Account Email**: Save it! You'll need it for folder sharing
- **Folder IDs**: Bookmark the folder URLs for easy access
- **Log Files**: Check `logs/pipeline.log` for detailed error messages
- **Test Data**: Start with small, clear Hindi PDFs for testing
- **Backup**: Keep backups of your credentials and configuration

## 🆘 Getting Help

1. Check the logs: `tail -f logs/pipeline.log`
2. Run with verbose mode: `--verbose`
3. Review the full [README.md](README.md) for detailed documentation
4. Create an issue with error logs and configuration (remove credentials!)

---

**🎉 Congratulations!** You should now have a working Hindi PDF processing pipeline. Upload some Hindi PDFs to your input folder and watch the magic happen!
