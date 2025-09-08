import pytest
from src.hindi_pdf_pipeline.text_processor import HindiTextProcessor
from src.hindi_pdf_pipeline.csv_generator import CSVGenerator
from src.hindi_pdf_pipeline.config import get_config
from src.hindi_pdf_pipeline.text_processor import StructuredData, ExtractedEntity
import pandas as pd
import os
import json
from datetime import datetime

@pytest.fixture
def config():
    return get_config()

@pytest.fixture
def text_processor(config):
    return HindiTextProcessor(config)

@pytest.fixture
def csv_generator(config):
    return CSVGenerator(config)

def test_transliteration_fix(text_processor):
    # Test cases for transliteration
    test_cases = {
        "राम": "Ram",
        "श्याम": "Shyam",
        "कृष्ण": "Krishna",
        "सीता": "Sita",
    }

    for hindi_name, expected_english_name in test_cases.items():
        transliterated_name = text_processor.transliterate_text(hindi_name)
        assert transliterated_name == expected_english_name

def test_csv_encoding_fix(csv_generator, tmp_path):
    # Create dummy structured data
    structured_data = [
        StructuredData(
            entities=[
                ExtractedEntity(
                    hindi_text="राम",
                    english_text="Ram",
                    english_lowercase="ram",
                    entity_type="name",
                    confidence=0.9,
                    page_number=1,
                    position=(0, 3),
                )
            ],
            raw_text="राम",
            cleaned_text="राम",
            page_number=1,
            extraction_timestamp=datetime.now(),
            processing_method="test",
        )
    ]

    # Generate CSV
    output_path = os.path.join(tmp_path, "test.csv")
    csv_generator.generate_csv_with_pandas(structured_data, output_path)

    # Check the encoding of the generated CSV
    with open(output_path, "rb") as f:
        raw_data = f.read()
        assert raw_data.startswith(b'\xef\xbb\xbf')

def test_json_encoding_fix(tmp_path):
    # Create dummy data
    data = {"name": "राम"}

    # Save to JSON
    output_path = os.path.join(tmp_path, "test.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # Read the JSON and check the content
    with open(output_path, "r", encoding="utf-8") as f:
        loaded_data = json.load(f)
        assert loaded_data["name"] == "राम"
