"""
Hindi PDF Processing Pipeline

A comprehensive Python pipeline for automated processing of Hindi PDFs,
including text extraction, transliteration, and structured data conversion.
"""

__version__ = "1.0.0"
__author__ = "Hindi PDF Pipeline Team"
__email__ = "support@hindipdfpipeline.com"

from .config import Config
from .drive_manager import GoogleDriveManager
from .pdf_processor import PDFProcessor
from .text_processor import HindiTextProcessor
from .csv_generator import CSVGenerator
from .file_tracker import FileTracker
from .main_pipeline import HindiPDFPipeline

__all__ = [
    "Config",
    "GoogleDriveManager", 
    "PDFProcessor",
    "HindiTextProcessor",
    "CSVGenerator",
    "FileTracker",
    "HindiPDFPipeline"
]
