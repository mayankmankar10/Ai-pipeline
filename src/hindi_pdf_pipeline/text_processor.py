"""
Hindi Text Processing Module

Handles Hindi text processing including:
- Text cleaning and normalization
- Transliteration from Hindi to English
- Structured data extraction from Hindi text
- Name entity recognition and processing
- Text pattern matching and parsing
"""

import re
import logging
from typing import List, Dict, Tuple, Optional, Set, Any
from dataclasses import dataclass
from datetime import datetime

# First, try to use Indic NLP Library with proper setup
try:
    from indicnlp import common
    from indicnlp.transliterate.unicode_transliterate import ItransTransliterator
    INDIC_NLP_AVAILABLE = True
except ImportError:
    # Fallback to indic-transliteration with intelligent cleanup
    from indic_transliteration import sanscript
    from indic_transliteration.sanscript import transliterate
    INDIC_NLP_AVAILABLE = False

from .config import Config

logger = logging.getLogger(__name__)

@dataclass
class ExtractedEntity:
    """Represents an extracted entity from Hindi text."""
    hindi_text: str
    english_text: str
    english_lowercase: str
    entity_type: str  # 'name', 'place', 'organization', etc.
    confidence: float
    page_number: int
    position: Tuple[int, int]  # (start, end) character positions

@dataclass
class StructuredData:
    """Represents structured data extracted from Hindi text."""
    entities: List[ExtractedEntity]
    raw_text: str
    cleaned_text: str
    page_number: int
    extraction_timestamp: datetime
    processing_method: str

class HindiTextProcessor:
    """
    Processes Hindi text for name extraction and transliteration.
    
    Provides comprehensive text processing capabilities including:
    - Hindi text cleaning and normalization
    - Devanagari to Roman transliteration
    - Name and entity extraction
    - Pattern-based data parsing
    """
    
    def __init__(self, config: Config):
        """
        Initialize Hindi text processor.
        
        Args:
            config: Configuration instance
        """
        self.config = config
        
        # Compile regex patterns for performance
        self._compile_patterns()
        
        # Initialize transliteration settings
        if INDIC_NLP_AVAILABLE:
            self.transliterator = ItransTransliterator(lang='hi')
        else:
            self.transliterator = None
        
        # Set up for fallback transliterator
        self.source_script = sanscript.DEVANAGARI
        self.target_script = sanscript.ITRANS  # Using ITRANS for better control
        
        # Patterns for cleaning unwanted trailing vowels
        self._init_cleanup_patterns()
        
        # Common Hindi words to filter out from names
        self.stop_words = {
            'का', 'की', 'के', 'में', 'से', 'को', 'और', 'या', 'है', 'हैं', 'था', 'थी', 'थे',
            'नाम', 'पिता', 'माता', 'पुत्र', 'पुत्री', 'श्री', 'श्रीमती', 'जी', 'साहब', 'महोदय',
            'द्वारा', 'तक', 'बाद', 'पहले', 'साथ', 'लिए', 'अनुसार', 'तरह', 'रूप',
            'जन्म', 'मृत्यु', 'आयु', 'उम्र', 'वर्ष', 'साल', 'महीना', 'दिन', 'तारीख',
            'गांव', 'शहर', 'जिला', 'राज्य', 'देश', 'पता', 'मकान', 'सड़क', 'इलाका'
        }
        
        # Name indicators that often precede names
        self.name_indicators = {
            'श्री', 'श्रीमती', 'कुमार', 'कुमारी', 'डॉ', 'प्रो', 'मिस्टर', 'मिसेज',
            'बेटा', 'बेटी', 'पुत्र', 'पुत्री', 'पति', 'पत्नी', 'माता', 'पिता'
        }
    
    def _init_cleanup_patterns(self) -> None:
        """Initialize patterns for cleaning unwanted transliteration artifacts."""
        
        # Common patterns that add unwanted trailing vowels
        self.cleanup_patterns = [
            # Remove trailing 'a' from common names that shouldn't have it
            (r'\b(Ram)a\b', r'\1'),       # Rama -> Ram
            (r'\b(Shyam)a\b', r'\1'),     # Shyama -> Shyam
            (r'\b(Mohan)a\b', r'\1'),     # Mohana -> Mohan
            (r'\b(Krishna)a\b', r'\1'),   # Krishnaa -> Krishna
            (r'\b(Geeta)a\b', r'\1'),     # Geetaa -> Geeta
            (r'\b(Sita)a\b', r'\1'),      # Sitaa -> Sita
            (r'\b(Radha)a\b', r'\1'),     # Radhaa -> Radha
            
            # Clean up double vowels
            (r'aa\b', 'a'),               # Remove trailing double 'a'
            (r'ee\b', 'e'),               # Remove trailing double 'e'
            (r'ii\b', 'i'),               # Remove trailing double 'i'
            (r'oo\b', 'o'),               # Remove trailing double 'o'
            (r'uu\b', 'u'),               # Remove trailing double 'u'
            
            # ITRANS specific cleanups
            (r'\^', ''),                  # Remove ITRANS markers
            (r'~', ''),                   # Remove ITRANS markers
        ]
        
        # Compile patterns for performance
        self.compiled_cleanup_patterns = [
            (re.compile(pattern, re.IGNORECASE), replacement)
            for pattern, replacement in self.cleanup_patterns
        ]
    
    def _compile_patterns(self) -> None:
        """Compile regex patterns for efficient text processing."""
        
        # Pattern for Hindi names (sequences of Devanagari characters)
        self.hindi_name_pattern = re.compile(
            r'[\u0900-\u097F]+(?:\s+[\u0900-\u097F]+)*',
            re.UNICODE
        )
        
        # Pattern for potential name contexts
        self.name_context_pattern = re.compile(
            r'(?:नाम|श्री|श्रीमती|कुमार|कुमारी)\s*:?\s*([\u0900-\u097F\s]+)',
            re.UNICODE | re.IGNORECASE
        )
        
        # Pattern for structured data (key-value pairs)
        self.structured_pattern = re.compile(
            r'([\u0900-\u097F]+)\s*:?\s*([\u0900-\u097F\s\d\-\/]+)',
            re.UNICODE
        )
        
        # Pattern for cleaning unwanted characters
        self.clean_pattern = re.compile(
            r'[^\u0900-\u097F\s\d\.\-\/\(\)]+',
            re.UNICODE
        )
        
        # Pattern for age/date information
        self.age_pattern = re.compile(
            r'(?:आयु|उम्र|वर्ष|साल)\s*:?\s*(\d+)',
            re.UNICODE | re.IGNORECASE
        )
        
        # Pattern for date formats
        self.date_pattern = re.compile(
            r'(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})',
            re.UNICODE
        )
    
    def clean_text(self, text: str) -> str:
        """
        Clean and normalize Hindi text.
        
        Args:
            text: Input Hindi text
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Remove unwanted characters but preserve Hindi, digits, and basic punctuation
        cleaned = self.clean_pattern.sub(' ', text)
        
        # Normalize whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Remove leading/trailing whitespace
        cleaned = cleaned.strip()
        
        # Remove standalone punctuation
        cleaned = re.sub(r'\s+[।,;.]+\s+', ' ', cleaned)
        
        return cleaned
    
    def transliterate_text(self, hindi_text: str) -> str:
        """
        Transliterate Hindi (Devanagari) text to English.
        
        Args:
            hindi_text: Hindi text in Devanagari script
            
        Returns:
            Transliterated English text
        """
        if not hindi_text:
            return ""
        
        try:
            # Clean text before transliteration
            clean_hindi = self.clean_text(hindi_text)
            
            if not clean_hindi:
                return ""
            
            if INDIC_NLP_AVAILABLE:
                transliterated = self.transliterator.transliterate(clean_hindi)
            else:
                # Transliterate using indic-transliteration library
                transliterated = transliterate(
                    clean_hindi,
                    self.source_script,
                    self.target_script
                )
            
            # Clean up transliteration artifacts
            transliterated = self._clean_transliteration(transliterated)
            
            return transliterated
            
        except Exception as e:
            logger.warning(f"Transliteration failed for '{hindi_text}': {e}")
            return ""
    
    def _clean_transliteration(self, text: str) -> str:
        """
        Clean up transliteration artifacts and normalize output using improved patterns.
        
        Args:
            text: Transliterated text
            
        Returns:
            Cleaned transliterated text with natural spelling
        """
        if not text:
            return ""
        
        # Remove extra spaces and normalize
        cleaned = re.sub(r'\s+', ' ', text).strip()
        
        # Apply intelligent cleanup patterns to remove unwanted vowels
        for pattern, replacement in self.compiled_cleanup_patterns:
            cleaned = pattern.sub(replacement, cleaned)
        
        # Capitalize first letter of each word for names
        if self._looks_like_name(cleaned):
            cleaned = ' '.join(word.capitalize() for word in cleaned.split())
        
        return cleaned
    
    def _looks_like_name(self, text: str) -> bool:
        """
        Heuristic to determine if text looks like a person's name.
        
        Args:
            text: Text to check
            
        Returns:
            True if text appears to be a name
        """
        if not text:
            return False
        
        words = text.split()
        
        # Names typically have 1-4 words
        if len(words) < 1 or len(words) > 4:
            return False
        
        # Each word should be reasonable length for a name part
        for word in words:
            if len(word) < 2 or len(word) > 20:
                return False
        
        # Should not contain numbers (usually)
        if re.search(r'\d', text):
            return False
        
        return True
    
    def extract_names(self, text: str, page_number: int = 1) -> List[ExtractedEntity]:
        """
        Extract names from Hindi text.
        
        Args:
            text: Hindi text to process
            page_number: Page number for tracking
            
        Returns:
            List of extracted name entities
        """
        entities = []
        
        if not text:
            return entities
        
        # Clean text first
        cleaned_text = self.clean_text(text)
        
        # Method 1: Look for names after indicators
        context_matches = self.name_context_pattern.finditer(cleaned_text)
        for match in context_matches:
            hindi_name = match.group(1).strip()
            if self._is_valid_name(hindi_name):
                entity = self._create_name_entity(
                    hindi_name, page_number, match.span(), method="context"
                )
                if entity:
                    entities.append(entity)
        
        # Method 2: Extract all potential names (sequences of Devanagari words)
        name_matches = self.hindi_name_pattern.finditer(cleaned_text)
        for match in name_matches:
            hindi_name = match.group(0).strip()
            if self._is_valid_name(hindi_name) and not self._is_stop_word_sequence(hindi_name):
                # Check if not already found by context method
                if not any(e.hindi_text == hindi_name for e in entities):
                    entity = self._create_name_entity(
                        hindi_name, page_number, match.span(), method="pattern"
                    )
                    if entity:
                        entities.append(entity)
        
        logger.debug(f"Extracted {len(entities)} name entities from page {page_number}")
        return entities
    
    def _is_valid_name(self, text: str) -> bool:
        """
        Validate if extracted text is likely a name.
        
        Args:
            text: Text to validate
            
        Returns:
            True if text appears to be a valid name
        """
        if not text or len(text.strip()) < 2:
            return False
        
        words = text.split()
        
        # Filter out very short or very long sequences
        if len(words) < 1 or len(words) > 4:
            return False
        
        # Filter out sequences that are all stop words
        if all(word in self.stop_words for word in words):
            return False
        
        # Filter out sequences with common non-name patterns
        if any(word in ['वर्ष', 'साल', 'दिन', 'महीना', 'जन्म', 'मृत्यु'] for word in words):
            return False
        
        # Names should not be all single characters
        if all(len(word) == 1 for word in words):
            return False
        
        return True
    
    def _is_stop_word_sequence(self, text: str) -> bool:
        """Check if text is entirely composed of stop words."""
        words = text.split()
        return all(word in self.stop_words for word in words)
    
    def _create_name_entity(self, hindi_text: str, page_number: int, 
                           position: Tuple[int, int], method: str) -> Optional[ExtractedEntity]:
        """
        Create a name entity with transliteration.
        
        Args:
            hindi_text: Original Hindi text
            page_number: Page number
            position: Text position
            method: Extraction method used
            
        Returns:
            ExtractedEntity or None if creation fails
        """
        try:
            # Transliterate to English
            english_text = self.transliterate_text(hindi_text)
            if not english_text:
                return None
            
            # Create lowercase version
            english_lowercase = english_text.lower()
            
            # Calculate confidence based on method and text quality
            confidence = self._calculate_name_confidence(hindi_text, method)
            
            return ExtractedEntity(
                hindi_text=hindi_text,
                english_text=english_text,
                english_lowercase=english_lowercase,
                entity_type="name",
                confidence=confidence,
                page_number=page_number,
                position=position
            )
            
        except Exception as e:
            logger.warning(f"Failed to create entity for '{hindi_text}': {e}")
            return None
    
    def _calculate_name_confidence(self, text: str, method: str) -> float:
        """
        Calculate confidence score for extracted name.
        
        Args:
            text: Hindi text
            method: Extraction method
            
        Returns:
            Confidence score between 0 and 1
        """
        base_confidence = 0.7 if method == "context" else 0.5
        
        words = text.split()
        
        # Boost confidence for reasonable name length
        if 2 <= len(words) <= 3:
            base_confidence += 0.15
        elif len(words) == 1:
            base_confidence += 0.05
        
        # Boost confidence if preceded by name indicators
        if any(indicator in text for indicator in self.name_indicators):
            base_confidence += 0.1
        
        # Reduce confidence for very short or very long words
        avg_word_length = sum(len(word) for word in words) / len(words)
        if 3 <= avg_word_length <= 8:
            base_confidence += 0.1
        elif avg_word_length < 2:
            base_confidence -= 0.2
        
        return min(1.0, max(0.1, base_confidence))
    
    def extract_structured_data(self, text: str, page_number: int = 1) -> StructuredData:
        """
        Extract structured data from Hindi text.
        
        Args:
            text: Hindi text to process
            page_number: Page number for tracking
            
        Returns:
            StructuredData object with extracted information
        """
        if not text:
            return StructuredData(
                entities=[],
                raw_text="",
                cleaned_text="",
                page_number=page_number,
                extraction_timestamp=datetime.now(),
                processing_method="empty"
            )
        
        # Clean the text
        cleaned_text = self.clean_text(text)
        
        # Extract names and other entities
        entities = self.extract_names(cleaned_text, page_number)
        
        # Extract additional structured information
        additional_entities = self._extract_additional_entities(cleaned_text, page_number)
        entities.extend(additional_entities)
        
        return StructuredData(
            entities=entities,
            raw_text=text,
            cleaned_text=cleaned_text,
            page_number=page_number,
            extraction_timestamp=datetime.now(),
            processing_method="comprehensive"
        )
    
    def _extract_additional_entities(self, text: str, page_number: int) -> List[ExtractedEntity]:
        """
        Extract additional entities like ages, dates, etc.
        
        Args:
            text: Cleaned Hindi text
            page_number: Page number
            
        Returns:
            List of additional extracted entities
        """
        entities = []
        
        # Extract age information
        age_matches = self.age_pattern.finditer(text)
        for match in age_matches:
            age_value = match.group(1)
            entities.append(ExtractedEntity(
                hindi_text=match.group(0),
                english_text=f"Age: {age_value}",
                english_lowercase=f"age: {age_value}",
                entity_type="age",
                confidence=0.9,
                page_number=page_number,
                position=match.span()
            ))
        
        # Extract date information
        date_matches = self.date_pattern.finditer(text)
        for match in date_matches:
            date_text = match.group(0)
            entities.append(ExtractedEntity(
                hindi_text=date_text,
                english_text=date_text,  # Dates remain the same
                english_lowercase=date_text.lower(),
                entity_type="date",
                confidence=0.95,
                page_number=page_number,
                position=match.span()
            ))
        
        return entities
    
    def batch_process_pages(self, pages_text: List[str]) -> List[StructuredData]:
        """
        Process multiple pages of text in batch.
        
        Args:
            pages_text: List of text content for each page
            
        Returns:
            List of StructuredData objects for each page
        """
        results = []
        
        for page_num, text in enumerate(pages_text, 1):
            try:
                structured_data = self.extract_structured_data(text, page_num)
                results.append(structured_data)
                logger.debug(f"Processed page {page_num}: {len(structured_data.entities)} entities")
            except Exception as e:
                logger.error(f"Error processing page {page_num}: {e}")
                # Add empty result for failed page
                results.append(StructuredData(
                    entities=[],
                    raw_text=text or "",
                    cleaned_text="",
                    page_number=page_num,
                    extraction_timestamp=datetime.now(),
                    processing_method="failed"
                ))
        
        total_entities = sum(len(data.entities) for data in results)
        logger.info(f"Batch processing completed: {len(results)} pages, {total_entities} entities")
        
        return results
    
    def filter_duplicates(self, entities: List[ExtractedEntity], 
                         similarity_threshold: float = 0.8) -> List[ExtractedEntity]:
        """
        Filter out duplicate entities based on text similarity.
        
        Args:
            entities: List of extracted entities
            similarity_threshold: Similarity threshold for considering duplicates
            
        Returns:
            List of unique entities
        """
        if not entities:
            return []
        
        unique_entities = []
        
        for entity in entities:
            is_duplicate = False
            
            for unique_entity in unique_entities:
                if self._calculate_similarity(entity, unique_entity) >= similarity_threshold:
                    is_duplicate = True
                    # Keep the one with higher confidence
                    if entity.confidence > unique_entity.confidence:
                        unique_entities.remove(unique_entity)
                        unique_entities.append(entity)
                    break
            
            if not is_duplicate:
                unique_entities.append(entity)
        
        logger.debug(f"Filtered duplicates: {len(entities)} -> {len(unique_entities)} entities")
        return unique_entities
    
    def _calculate_similarity(self, entity1: ExtractedEntity, entity2: ExtractedEntity) -> float:
        """
        Calculate similarity between two entities.
        
        Args:
            entity1: First entity
            entity2: Second entity
            
        Returns:
            Similarity score between 0 and 1
        """
        # Simple similarity based on English lowercase text
        text1 = entity1.english_lowercase
        text2 = entity2.english_lowercase
        
        if text1 == text2:
            return 1.0
        
        # Calculate character-level similarity
        if not text1 or not text2:
            return 0.0
        
        # Simple Jaccard similarity on character sets
        set1 = set(text1.replace(' ', ''))
        set2 = set(text2.replace(' ', ''))
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
    
    def validate_extraction(self, structured_data: StructuredData) -> Dict[str, Any]:
        """
        Validate quality of text extraction and processing.
        
        Args:
            structured_data: Structured data to validate
            
        Returns:
            Dictionary with validation metrics
        """
        metrics = {
            "valid": False,
            "entity_count": len(structured_data.entities),
            "name_count": 0,
            "avg_confidence": 0.0,
            "text_length": len(structured_data.cleaned_text),
            "processing_method": structured_data.processing_method
        }
        
        if not structured_data.entities:
            metrics["reason"] = "No entities extracted"
            return metrics
        
        # Count entities by type
        name_entities = [e for e in structured_data.entities if e.entity_type == "name"]
        metrics["name_count"] = len(name_entities)
        
        # Calculate average confidence
        if structured_data.entities:
            metrics["avg_confidence"] = sum(e.confidence for e in structured_data.entities) / len(structured_data.entities)
        
        # Validation criteria
        min_entities = 1
        min_confidence = 0.5
        min_text_length = 10
        
        is_valid = (
            metrics["entity_count"] >= min_entities and
            metrics["avg_confidence"] >= min_confidence and
            metrics["text_length"] >= min_text_length
        )
        
        metrics["valid"] = is_valid
        
        if not is_valid:
            reasons = []
            if metrics["entity_count"] < min_entities:
                reasons.append(f"Too few entities ({metrics['entity_count']})")
            if metrics["avg_confidence"] < min_confidence:
                reasons.append(f"Low confidence ({metrics['avg_confidence']:.2f})")
            if metrics["text_length"] < min_text_length:
                reasons.append(f"Text too short ({metrics['text_length']})")
            metrics["reason"] = "; ".join(reasons)
        
        return metrics
    
    def get_processing_summary(self, structured_data_list: List[StructuredData]) -> Dict[str, Any]:
        """
        Generate summary statistics for processed data.
        
        Args:
            structured_data_list: List of processed structured data
            
        Returns:
            Summary statistics dictionary
        """
        if not structured_data_list:
            return {"total_pages": 0, "total_entities": 0}
        
        total_entities = sum(len(data.entities) for data in structured_data_list)
        total_names = sum(
            len([e for e in data.entities if e.entity_type == "name"])
            for data in structured_data_list
        )
        
        # Calculate average confidence across all entities
        all_entities = []
        for data in structured_data_list:
            all_entities.extend(data.entities)
        
        avg_confidence = (
            sum(e.confidence for e in all_entities) / len(all_entities)
            if all_entities else 0.0
        )
        
        # Pages with successful extraction
        successful_pages = sum(
            1 for data in structured_data_list 
            if data.processing_method != "failed" and data.entities
        )
        
        return {
            "total_pages": len(structured_data_list),
            "successful_pages": successful_pages,
            "total_entities": total_entities,
            "total_names": total_names,
            "avg_confidence": avg_confidence,
            "processing_success_rate": successful_pages / len(structured_data_list) if structured_data_list else 0
        }
