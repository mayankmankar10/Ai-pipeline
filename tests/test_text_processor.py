"""
Unit tests for Hindi text processing module.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from src.hindi_pdf_pipeline.config import Config
from src.hindi_pdf_pipeline.text_processor import (
    HindiTextProcessor, 
    ExtractedEntity,
    StructuredData
)

class TestHindiTextProcessor:
    """Test cases for HindiTextProcessor class."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration."""
        config = Mock(spec=Config)
        return config
    
    @pytest.fixture
    def processor(self, config):
        """Create HindiTextProcessor instance for testing."""
        return HindiTextProcessor(config)
    
    def test_clean_text_basic(self, processor):
        """Test basic text cleaning functionality."""
        # Hindi text with various unwanted characters
        dirty_text = "राम  कुमार!@#$%^&*()  शर्मा   ।।। "
        
        cleaned = processor.clean_text(dirty_text)
        
        # Should remove extra symbols but keep Hindi text and spaces
        assert "राम" in cleaned
        assert "कुमार" in cleaned
        assert "शर्मा" in cleaned
        assert "!@#$%^&*()" not in cleaned
        assert cleaned.strip() == cleaned  # No leading/trailing spaces
    
    def test_clean_text_empty(self, processor):
        """Test cleaning empty or None text."""
        assert processor.clean_text("") == ""
        assert processor.clean_text(None) == ""
        assert processor.clean_text("   ") == ""
    
    def test_transliterate_text_basic(self, processor):
        """Test basic transliteration functionality."""
        hindi_text = "राम कुमार"
        
        result = processor.transliterate_text(hindi_text)
        
        # Should produce some English transliteration
        assert isinstance(result, str)
        assert len(result) > 0
        assert result != hindi_text  # Should be different from input
    
    def test_transliterate_text_empty(self, processor):
        """Test transliteration with empty input."""
        assert processor.transliterate_text("") == ""
        assert processor.transliterate_text(None) == ""
    
    def test_looks_like_name_valid(self, processor):
        """Test name detection with valid names."""
        valid_names = [
            "Ram Kumar",
            "John Doe", 
            "A B C",
            "SingleName"
        ]
        
        for name in valid_names:
            assert processor._looks_like_name(name) == True
    
    def test_looks_like_name_invalid(self, processor):
        """Test name detection with invalid names."""
        invalid_names = [
            "",  # Empty
            "A",  # Too short
            "A B C D E F G",  # Too many words
            "Ram123",  # Contains numbers
            "VeryVeryVeryLongNameThatExceedsReasonableLength"  # Too long
        ]
        
        for name in invalid_names:
            assert processor._looks_like_name(name) == False
    
    def test_is_valid_name_basic(self, processor):
        """Test name validation logic."""
        # Valid names
        assert processor._is_valid_name("राम कुमार") == True
        assert processor._is_valid_name("प्रिया शर्मा") == True
        
        # Invalid names
        assert processor._is_valid_name("") == False
        assert processor._is_valid_name("का") == False  # Stop word
        assert processor._is_valid_name("वर्ष महीना") == False  # Contains non-name words
    
    def test_extract_names_simple(self, processor):
        """Test name extraction from simple text."""
        text = "नाम: राम कुमार शर्मा"
        
        entities = processor.extract_names(text, page_number=1)
        
        assert len(entities) > 0
        assert any("राम" in entity.hindi_text for entity in entities)
        assert all(entity.entity_type == "name" for entity in entities)
        assert all(entity.page_number == 1 for entity in entities)
    
    def test_extract_names_empty_text(self, processor):
        """Test name extraction with empty text."""
        entities = processor.extract_names("", page_number=1)
        assert entities == []
        
        entities = processor.extract_names(None, page_number=1)
        assert entities == []
    
    def test_create_name_entity(self, processor):
        """Test entity creation."""
        hindi_text = "राम कुमार"
        
        with patch.object(processor, 'transliterate_text', return_value="Ram Kumar"):
            entity = processor._create_name_entity(
                hindi_text, 
                page_number=1, 
                position=(0, 10), 
                method="context"
            )
        
        assert entity is not None
        assert entity.hindi_text == hindi_text
        assert entity.english_text == "Ram Kumar"
        assert entity.english_lowercase == "ram kumar"
        assert entity.entity_type == "name"
        assert entity.page_number == 1
        assert entity.position == (0, 10)
        assert 0 < entity.confidence <= 1
    
    def test_calculate_name_confidence(self, processor):
        """Test confidence calculation."""
        # Context method should have higher confidence
        conf_context = processor._calculate_name_confidence("राम कुमार", "context")
        conf_pattern = processor._calculate_name_confidence("राम कुमार", "pattern")
        
        assert conf_context > conf_pattern
        assert 0 < conf_context <= 1
        assert 0 < conf_pattern <= 1
    
    def test_extract_structured_data(self, processor):
        """Test structured data extraction."""
        text = "नाम: राम कुमार आयु: 25 वर्ष"
        
        structured_data = processor.extract_structured_data(text, page_number=1)
        
        assert isinstance(structured_data, StructuredData)
        assert structured_data.page_number == 1
        assert structured_data.raw_text == text
        assert len(structured_data.cleaned_text) > 0
        assert isinstance(structured_data.extraction_timestamp, datetime)
        assert len(structured_data.entities) > 0
    
    def test_batch_process_pages(self, processor):
        """Test batch processing of multiple pages."""
        pages_text = [
            "राम कुमार शर्मा",
            "सीता देवी",
            "गीता पटेल"
        ]
        
        results = processor.batch_process_pages(pages_text)
        
        assert len(results) == 3
        assert all(isinstance(data, StructuredData) for data in results)
        assert results[0].page_number == 1
        assert results[1].page_number == 2
        assert results[2].page_number == 3
    
    def test_filter_duplicates_basic(self, processor):
        """Test duplicate filtering."""
        # Create test entities with some duplicates
        entities = [
            ExtractedEntity(
                hindi_text="राम कुमार",
                english_text="Ram Kumar",
                english_lowercase="ram kumar",
                entity_type="name",
                confidence=0.8,
                page_number=1,
                position=(0, 10)
            ),
            ExtractedEntity(
                hindi_text="राम कुमार",  # Duplicate
                english_text="Ram Kumar",
                english_lowercase="ram kumar",
                entity_type="name",
                confidence=0.9,  # Higher confidence
                page_number=1,
                position=(15, 25)
            ),
            ExtractedEntity(
                hindi_text="सीता देवी",
                english_text="Sita Devi",
                english_lowercase="sita devi",
                entity_type="name",
                confidence=0.7,
                page_number=1,
                position=(30, 40)
            )
        ]
        
        unique_entities = processor.filter_duplicates(entities)
        
        # Should have 2 unique entities (duplicate removed)
        assert len(unique_entities) == 2
        
        # Should keep the one with higher confidence
        ram_entities = [e for e in unique_entities if "ram" in e.english_lowercase]
        assert len(ram_entities) == 1
        assert ram_entities[0].confidence == 0.9
    
    def test_calculate_similarity(self, processor):
        """Test similarity calculation between entities."""
        entity1 = ExtractedEntity(
            hindi_text="राम",
            english_text="Ram",
            english_lowercase="ram",
            entity_type="name",
            confidence=0.8,
            page_number=1,
            position=(0, 3)
        )
        
        entity2 = ExtractedEntity(
            hindi_text="राम",
            english_text="Ram",
            english_lowercase="ram",
            entity_type="name",
            confidence=0.9,
            page_number=1,
            position=(5, 8)
        )
        
        entity3 = ExtractedEntity(
            hindi_text="सीता",
            english_text="Sita",
            english_lowercase="sita",
            entity_type="name",
            confidence=0.7,
            page_number=1,
            position=(10, 14)
        )
        
        # Identical entities should have similarity of 1.0
        assert processor._calculate_similarity(entity1, entity2) == 1.0
        
        # Different entities should have lower similarity
        assert processor._calculate_similarity(entity1, entity3) < 1.0
    
    def test_validate_extraction_valid(self, processor):
        """Test validation with good extraction results."""
        structured_data = StructuredData(
            entities=[
                ExtractedEntity(
                    hindi_text="राम कुमार",
                    english_text="Ram Kumar",
                    english_lowercase="ram kumar",
                    entity_type="name",
                    confidence=0.8,
                    page_number=1,
                    position=(0, 10)
                )
            ],
            raw_text="नाम: राम कुमार",
            cleaned_text="नाम राम कुमार",
            page_number=1,
            extraction_timestamp=datetime.now(),
            processing_method="comprehensive"
        )
        
        validation = processor.validate_extraction(structured_data)
        
        assert validation['valid'] == True
        assert validation['entity_count'] == 1
        assert validation['name_count'] == 1
        assert validation['avg_confidence'] == 0.8
    
    def test_validate_extraction_invalid(self, processor):
        """Test validation with poor extraction results."""
        structured_data = StructuredData(
            entities=[],  # No entities
            raw_text="",
            cleaned_text="",
            page_number=1,
            extraction_timestamp=datetime.now(),
            processing_method="failed"
        )
        
        validation = processor.validate_extraction(structured_data)
        
        assert validation['valid'] == False
        assert validation['entity_count'] == 0
        assert 'reason' in validation
    
    def test_get_processing_summary(self, processor):
        """Test processing summary generation."""
        structured_data_list = [
            StructuredData(
                entities=[
                    ExtractedEntity(
                        hindi_text="राम",
                        english_text="Ram",
                        english_lowercase="ram",
                        entity_type="name",
                        confidence=0.8,
                        page_number=1,
                        position=(0, 3)
                    )
                ],
                raw_text="राम",
                cleaned_text="राम",
                page_number=1,
                extraction_timestamp=datetime.now(),
                processing_method="comprehensive"
            ),
            StructuredData(
                entities=[],
                raw_text="",
                cleaned_text="",
                page_number=2,
                extraction_timestamp=datetime.now(),
                processing_method="failed"
            )
        ]
        
        summary = processor.get_processing_summary(structured_data_list)
        
        assert summary['total_pages'] == 2
        assert summary['successful_pages'] == 1
        assert summary['total_entities'] == 1
        assert summary['total_names'] == 1
        assert 0 < summary['avg_confidence'] <= 1
        assert 0 < summary['processing_success_rate'] <= 1

class TestExtractedEntity:
    """Test ExtractedEntity dataclass."""
    
    def test_entity_creation(self):
        """Test entity creation and attributes."""
        entity = ExtractedEntity(
            hindi_text="राम कुमार",
            english_text="Ram Kumar",
            english_lowercase="ram kumar",
            entity_type="name",
            confidence=0.85,
            page_number=1,
            position=(10, 20)
        )
        
        assert entity.hindi_text == "राम कुमार"
        assert entity.english_text == "Ram Kumar"
        assert entity.english_lowercase == "ram kumar"
        assert entity.entity_type == "name"
        assert entity.confidence == 0.85
        assert entity.page_number == 1
        assert entity.position == (10, 20)

class TestStructuredData:
    """Test StructuredData dataclass."""
    
    def test_structured_data_creation(self):
        """Test structured data creation."""
        timestamp = datetime.now()
        
        data = StructuredData(
            entities=[],
            raw_text="test raw text",
            cleaned_text="test cleaned text",
            page_number=1,
            extraction_timestamp=timestamp,
            processing_method="test"
        )
        
        assert data.entities == []
        assert data.raw_text == "test raw text"
        assert data.cleaned_text == "test cleaned text"
        assert data.page_number == 1
        assert data.extraction_timestamp == timestamp
        assert data.processing_method == "test"

if __name__ == "__main__":
    pytest.main([__file__])
