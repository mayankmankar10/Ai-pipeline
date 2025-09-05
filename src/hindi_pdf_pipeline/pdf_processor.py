"""
PDF Processing Module

Handles PDF text extraction with multiple fallback methods:
1. Direct text extraction from PDFs with embedded text
2. OCR processing for scanned PDFs or when direct extraction fails
3. Preprocessing and image enhancement for better OCR results
"""

import os
import io
import logging
from typing import List, Dict, Tuple, Optional, Any
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

import fitz  # PyMuPDF
import pytesseract
from PIL import Image, ImageEnhance, ImageFilter
import pdf2image
import PyPDF2
import pdfplumber
import numpy as np

from .config import Config

logger = logging.getLogger(__name__)

@dataclass
class ExtractedText:
    """Represents extracted text from a PDF page."""
    page_number: int
    text: str
    confidence: float
    extraction_method: str  # 'direct', 'ocr', 'hybrid'
    word_count: int
    language_detected: Optional[str] = None

@dataclass
class PDFMetadata:
    """Represents PDF document metadata."""
    filename: str
    file_path: str
    total_pages: int
    file_size: int
    creation_date: Optional[datetime]
    modification_date: Optional[datetime]
    author: Optional[str]
    title: Optional[str]
    subject: Optional[str]

class PDFProcessor:
    """
    PDF processing class with multiple text extraction strategies.
    
    Provides robust text extraction using direct PDF text extraction,
    OCR fallback, and image preprocessing for optimal results with Hindi text.
    """
    
    def __init__(self, config: Config):
        """
        Initialize PDF processor.
        
        Args:
            config: Configuration instance
        """
        self.config = config
        self._setup_tesseract()
    
    def _setup_tesseract(self) -> None:
        """Set up Tesseract OCR configuration."""
        # Set Tesseract executable path if specified
        if self.config.tesseract_path and os.path.exists(self.config.tesseract_path):
            pytesseract.pytesseract.tesseract_cmd = self.config.tesseract_path
            logger.info(f"Tesseract configured at: {self.config.tesseract_path}")
        else:
            logger.warning("Tesseract path not found or not configured. OCR may not work properly.")
    
    def extract_metadata(self, pdf_path: str) -> PDFMetadata:
        """
        Extract metadata from PDF file.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            PDFMetadata object with extracted information
        """
        try:
            # Use PyMuPDF for metadata extraction
            doc = fitz.open(pdf_path)
            metadata = doc.metadata
            
            # Get file statistics
            file_stats = os.stat(pdf_path)
            
            return PDFMetadata(
                filename=os.path.basename(pdf_path),
                file_path=pdf_path,
                total_pages=doc.page_count,
                file_size=file_stats.st_size,
                creation_date=datetime.fromtimestamp(file_stats.st_ctime) if file_stats.st_ctime else None,
                modification_date=datetime.fromtimestamp(file_stats.st_mtime) if file_stats.st_mtime else None,
                author=metadata.get('author'),
                title=metadata.get('title'),
                subject=metadata.get('subject')
            )
        
        except Exception as e:
            logger.error(f"Error extracting metadata from {pdf_path}: {e}")
            # Return minimal metadata on error
            return PDFMetadata(
                filename=os.path.basename(pdf_path),
                file_path=pdf_path,
                total_pages=0,
                file_size=0,
                creation_date=None,
                modification_date=None,
                author=None,
                title=None,
                subject=None
            )
    
    def extract_text_direct(self, pdf_path: str) -> List[ExtractedText]:
        """
        Extract text directly from PDF using embedded text.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of ExtractedText objects for each page
        """
        extracted_pages = []
        
        # Try multiple extraction methods for best results
        methods = [
            ("pdfplumber", self._extract_with_pdfplumber),
            ("pymupdf", self._extract_with_pymupdf),
            ("pypdf2", self._extract_with_pypdf2)
        ]
        
        for method_name, extract_func in methods:
            try:
                logger.debug(f"Trying direct extraction with {method_name}")
                pages = extract_func(pdf_path)
                
                if pages and any(page.text.strip() for page in pages):
                    logger.info(f"Successfully extracted text using {method_name}")
                    return pages
                
            except Exception as e:
                logger.warning(f"Direct extraction with {method_name} failed: {e}")
                continue
        
        logger.warning(f"All direct extraction methods failed for {pdf_path}")
        return extracted_pages
    
    def _extract_with_pdfplumber(self, pdf_path: str) -> List[ExtractedText]:
        """Extract text using pdfplumber library."""
        pages = []
        
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                try:
                    text = page.extract_text() or ""
                    
                    pages.append(ExtractedText(
                        page_number=page_num,
                        text=text,
                        confidence=0.95 if text.strip() else 0.0,
                        extraction_method="direct_pdfplumber",
                        word_count=len(text.split()) if text else 0
                    ))
                    
                except Exception as e:
                    logger.warning(f"Error extracting page {page_num} with pdfplumber: {e}")
                    pages.append(ExtractedText(
                        page_number=page_num,
                        text="",
                        confidence=0.0,
                        extraction_method="direct_pdfplumber",
                        word_count=0
                    ))
        
        return pages
    
    def _extract_with_pymupdf(self, pdf_path: str) -> List[ExtractedText]:
        """Extract text using PyMuPDF library."""
        pages = []
        
        doc = fitz.open(pdf_path)
        
        for page_num in range(doc.page_count):
            try:
                page = doc[page_num]
                text = page.get_text()
                
                pages.append(ExtractedText(
                    page_number=page_num + 1,
                    text=text,
                    confidence=0.9 if text.strip() else 0.0,
                    extraction_method="direct_pymupdf",
                    word_count=len(text.split()) if text else 0
                ))
                
            except Exception as e:
                logger.warning(f"Error extracting page {page_num + 1} with PyMuPDF: {e}")
                pages.append(ExtractedText(
                    page_number=page_num + 1,
                    text="",
                    confidence=0.0,
                    extraction_method="direct_pymupdf",
                    word_count=0
                ))
        
        doc.close()
        return pages
    
    def _extract_with_pypdf2(self, pdf_path: str) -> List[ExtractedText]:
        """Extract text using PyPDF2 library."""
        pages = []
        
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            
            for page_num, page in enumerate(pdf_reader.pages, 1):
                try:
                    text = page.extract_text()
                    
                    pages.append(ExtractedText(
                        page_number=page_num,
                        text=text,
                        confidence=0.85 if text.strip() else 0.0,
                        extraction_method="direct_pypdf2",
                        word_count=len(text.split()) if text else 0
                    ))
                    
                except Exception as e:
                    logger.warning(f"Error extracting page {page_num} with PyPDF2: {e}")
                    pages.append(ExtractedText(
                        page_number=page_num,
                        text="",
                        confidence=0.0,
                        extraction_method="direct_pypdf2",
                        word_count=0
                    ))
        
        return pages
    
    def extract_text_ocr(self, pdf_path: str, enhance_images: bool = True) -> List[ExtractedText]:
        """
        Extract text using OCR from PDF pages converted to images.
        
        Args:
            pdf_path: Path to PDF file
            enhance_images: Whether to enhance images for better OCR results
            
        Returns:
            List of ExtractedText objects for each page
        """
        extracted_pages = []
        
        try:
            logger.info(f"Starting OCR extraction for {pdf_path}")
            
            # Convert PDF to images
            images = pdf2image.convert_from_path(
                pdf_path,
                dpi=300,  # High DPI for better OCR quality
                fmt='PNG'
            )
            
            for page_num, image in enumerate(images, 1):
                try:
                    # Enhance image if requested
                    if enhance_images:
                        image = self._enhance_image_for_ocr(image)
                    
                    # Perform OCR
                    ocr_result = pytesseract.image_to_data(
                        image,
                        lang=self.config.ocr_language,
                        config=self.config.ocr_config,
                        output_type=pytesseract.Output.DICT
                    )
                    
                    # Extract text and calculate confidence
                    text, confidence = self._process_ocr_result(ocr_result)
                    
                    extracted_pages.append(ExtractedText(
                        page_number=page_num,
                        text=text,
                        confidence=confidence / 100.0,  # Convert to 0-1 scale
                        extraction_method="ocr",
                        word_count=len(text.split()) if text else 0
                    ))
                    
                    logger.debug(f"OCR completed for page {page_num}: {len(text)} characters, confidence: {confidence}%")
                    
                except Exception as e:
                    logger.error(f"OCR failed for page {page_num}: {e}")
                    extracted_pages.append(ExtractedText(
                        page_number=page_num,
                        text="",
                        confidence=0.0,
                        extraction_method="ocr",
                        word_count=0
                    ))
            
            logger.info(f"OCR extraction completed for {len(images)} pages")
            
        except Exception as e:
            logger.error(f"Failed to convert PDF to images for OCR: {e}")
        
        return extracted_pages
    
    def _enhance_image_for_ocr(self, image: Image.Image) -> Image.Image:
        """
        Enhance image for better OCR results.
        
        Args:
            image: PIL Image object
            
        Returns:
            Enhanced PIL Image object
        """
        try:
            # Convert to grayscale if needed
            if image.mode != 'L':
                image = image.convert('L')
            
            # Increase contrast
            enhancer = ImageEnhance.Contrast(image)
            image = enhancer.enhance(1.5)
            
            # Increase sharpness
            enhancer = ImageEnhance.Sharpness(image)
            image = enhancer.enhance(2.0)
            
            # Apply slight blur to reduce noise
            image = image.filter(ImageFilter.GaussianBlur(radius=0.5))
            
            # Resize if too small (OCR works better on larger images)
            width, height = image.size
            if width < 1000 or height < 1000:
                scale_factor = max(1000 / width, 1000 / height)
                new_size = (int(width * scale_factor), int(height * scale_factor))
                image = image.resize(new_size, Image.LANCZOS)
            
            return image
            
        except Exception as e:
            logger.warning(f"Image enhancement failed: {e}")
            return image
    
    def _process_ocr_result(self, ocr_result: Dict) -> Tuple[str, float]:
        """
        Process OCR result to extract text and calculate confidence.
        
        Args:
            ocr_result: OCR result dictionary from pytesseract
            
        Returns:
            Tuple of (extracted_text, average_confidence)
        """
        words = []
        confidences = []
        
        for i, word in enumerate(ocr_result['text']):
            if word.strip():
                words.append(word)
                conf = int(ocr_result['conf'][i])
                if conf > 0:  # Only consider positive confidence values
                    confidences.append(conf)
        
        text = ' '.join(words)
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0
        
        return text, avg_confidence
    
    def extract_text_hybrid(self, pdf_path: str) -> List[ExtractedText]:
        """
        Extract text using hybrid approach: direct extraction with OCR fallback.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of ExtractedText objects for each page
        """
        logger.info(f"Starting hybrid text extraction for {pdf_path}")
        
        # First, try direct extraction
        direct_pages = self.extract_text_direct(pdf_path)
        
        # Identify pages that need OCR (low confidence or no text)
        ocr_page_numbers = []
        final_pages = []
        
        for page in direct_pages:
            if page.confidence < 0.5 or len(page.text.strip()) < 10:
                ocr_page_numbers.append(page.page_number)
            else:
                final_pages.append(page)
        
        # Perform OCR on pages that need it
        if ocr_page_numbers:
            logger.info(f"Performing OCR on {len(ocr_page_numbers)} pages: {ocr_page_numbers}")
            ocr_pages = self.extract_text_ocr(pdf_path)
            
            # Replace pages that needed OCR
            ocr_dict = {page.page_number: page for page in ocr_pages}
            
            for page_num in ocr_page_numbers:
                if page_num in ocr_dict:
                    ocr_page = ocr_dict[page_num]
                    ocr_page.extraction_method = "hybrid_ocr"
                    final_pages.append(ocr_page)
        
        # Sort pages by page number
        final_pages.sort(key=lambda x: x.page_number)
        
        logger.info(f"Hybrid extraction completed: {len(final_pages)} pages processed")
        return final_pages
    
    def extract_text(self, pdf_path: str, method: str = "hybrid") -> List[ExtractedText]:
        """
        Extract text from PDF using specified method.
        
        Args:
            pdf_path: Path to PDF file
            method: Extraction method ("direct", "ocr", "hybrid")
            
        Returns:
            List of ExtractedText objects for each page
        """
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        logger.info(f"Extracting text from {pdf_path} using {method} method")
        
        start_time = datetime.now()
        
        try:
            if method == "direct":
                pages = self.extract_text_direct(pdf_path)
            elif method == "ocr":
                pages = self.extract_text_ocr(pdf_path)
            elif method == "hybrid":
                pages = self.extract_text_hybrid(pdf_path)
            else:
                raise ValueError(f"Unknown extraction method: {method}")
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            total_chars = sum(len(page.text) for page in pages)
            avg_confidence = sum(page.confidence for page in pages) / len(pages) if pages else 0
            
            logger.info(
                f"Text extraction completed: {len(pages)} pages, "
                f"{total_chars} characters, avg confidence: {avg_confidence:.2f}, "
                f"processing time: {processing_time:.2f}s"
            )
            
            return pages
            
        except Exception as e:
            logger.error(f"Text extraction failed for {pdf_path}: {e}")
            raise
    
    def is_text_rich_pdf(self, pdf_path: str, sample_pages: int = 3) -> bool:
        """
        Check if PDF contains sufficient embedded text (not scanned).
        
        Args:
            pdf_path: Path to PDF file
            sample_pages: Number of pages to sample for text detection
            
        Returns:
            True if PDF appears to have embedded text, False otherwise
        """
        try:
            doc = fitz.open(pdf_path)
            total_pages = doc.page_count
            
            # Sample pages from beginning, middle, and end
            if total_pages <= sample_pages:
                pages_to_check = list(range(total_pages))
            else:
                pages_to_check = [
                    0,  # First page
                    total_pages // 2,  # Middle page
                    total_pages - 1  # Last page
                ]
            
            text_lengths = []
            for page_num in pages_to_check:
                page = doc[page_num]
                text = page.get_text().strip()
                text_lengths.append(len(text))
            
            doc.close()
            
            # Consider PDF text-rich if average page has more than 100 characters
            avg_text_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0
            is_text_rich = avg_text_length > 100
            
            logger.debug(f"PDF text richness check: avg {avg_text_length:.1f} chars/page, text-rich: {is_text_rich}")
            return is_text_rich
            
        except Exception as e:
            logger.error(f"Error checking PDF text richness: {e}")
            return False  # Assume not text-rich on error
    
    def get_optimal_extraction_method(self, pdf_path: str) -> str:
        """
        Determine optimal extraction method for a PDF.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Recommended extraction method ("direct", "ocr", "hybrid")
        """
        try:
            if self.is_text_rich_pdf(pdf_path):
                return "direct"
            else:
                return "ocr"
        except Exception as e:
            logger.error(f"Error determining optimal extraction method: {e}")
            return "hybrid"  # Safest fallback
    
    def validate_extracted_text(self, pages: List[ExtractedText]) -> Dict[str, Any]:
        """
        Validate quality of extracted text.
        
        Args:
            pages: List of ExtractedText objects
            
        Returns:
            Dictionary with validation metrics
        """
        if not pages:
            return {
                "valid": False,
                "reason": "No pages extracted",
                "total_pages": 0,
                "total_characters": 0,
                "avg_confidence": 0.0
            }
        
        total_chars = sum(len(page.text) for page in pages)
        avg_confidence = sum(page.confidence for page in pages) / len(pages)
        pages_with_text = sum(1 for page in pages if page.text.strip())
        
        # Validation criteria
        min_chars_per_page = 10
        min_confidence = 0.6
        min_pages_with_text_ratio = 0.5
        
        is_valid = (
            total_chars >= min_chars_per_page * len(pages) and
            avg_confidence >= min_confidence and
            pages_with_text / len(pages) >= min_pages_with_text_ratio
        )
        
        return {
            "valid": is_valid,
            "total_pages": len(pages),
            "pages_with_text": pages_with_text,
            "total_characters": total_chars,
            "avg_confidence": avg_confidence,
            "avg_chars_per_page": total_chars / len(pages) if pages else 0,
            "text_coverage_ratio": pages_with_text / len(pages) if pages else 0
        }
