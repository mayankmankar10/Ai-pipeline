#!/usr/bin/env python3
"""
Component Demo Script

Demonstrates individual components of the Hindi PDF pipeline
for testing and development purposes.
"""

import sys
import tempfile
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hindi_pdf_pipeline.config import Config
from hindi_pdf_pipeline.text_processor import HindiTextProcessor
from hindi_pdf_pipeline.csv_generator import CSVGenerator

def demo_text_processing():
    """Demonstrate Hindi text processing capabilities."""
    print("🔤 Hindi Text Processing Demo")
    print("=" * 50)
    
    # Sample Hindi text
    sample_text = """
    व्यक्तिगत जानकारी
    
    नाम: राम कुमार शर्मा
    पिता का नाम: श्याम लाल शर्मा
    माता का नाम: सीता देवी शर्मा
    आयु: 25 वर्ष
    जन्म तिथि: 15/08/1998
    पता: 123, मुख्य सड़क, नई दिल्ली - 110001
    
    परिवारिक जानकारी:
    पत्नी: गीता कुमारी
    बेटा: अर्जुन कुमार
    बेटी: प्रिया शर्मा
    """
    
    # Create temp config for testing
    config = Config()
    
    # Override required settings for demo
    config._config['input_folder_id'] = 'demo'
    config._config['output_folder_id'] = 'demo'
    
    processor = HindiTextProcessor(config)
    
    print("Sample Hindi Text:")
    print("-" * 30)
    print(sample_text)
    print()
    
    # Process the text
    structured_data = processor.extract_structured_data(sample_text)
    
    print("Extracted Entities:")
    print("-" * 30)
    
    for i, entity in enumerate(structured_data.entities, 1):
        print(f"{i}. {entity.entity_type.upper()}")
        print(f"   Hindi: {entity.hindi_text}")
        print(f"   English: {entity.english_text}")
        print(f"   Lowercase: {entity.english_lowercase}")
        print(f"   Confidence: {entity.confidence:.2f}")
        print(f"   Position: {entity.position}")
        print()
    
    print(f"Total entities extracted: {len(structured_data.entities)}")
    print()
    
    return structured_data

def demo_csv_generation(structured_data_list):
    """Demonstrate CSV generation from structured data."""
    print("📊 CSV Generation Demo")
    print("=" * 50)
    
    # Create temp config
    config = Config()
    config._config['input_folder_id'] = 'demo'
    config._config['output_folder_id'] = 'demo'
    
    csv_generator = CSVGenerator(config)
    
    # Generate CSV in temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
        temp_path = temp_file.name
    
    try:
        # Generate CSV
        csv_path = csv_generator.generate_csv_with_pandas(
            structured_data_list,
            temp_path,
            filename="demo_document.pdf",
            include_metadata=True
        )
        
        print(f"CSV generated: {csv_path}")
        print()
        
        # Display CSV content
        print("CSV Content:")
        print("-" * 30)
        
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[:10]):  # Show first 10 lines
                print(f"{i+1:2d}: {line.rstrip()}")
            
            if len(lines) > 10:
                print(f"... ({len(lines) - 10} more lines)")
        
        print()
        
        # Validate CSV
        validation = csv_generator.validate_csv_output(csv_path)
        print("CSV Validation:")
        print(f"  Valid: {validation['valid']}")
        print(f"  Rows: {validation['row_count']}")
        print(f"  Columns: {validation['column_count']}")
        print(f"  File size: {validation['file_size']} bytes")
        
        if validation['errors']:
            print("  Errors:")
            for error in validation['errors']:
                print(f"    - {error}")
        
        print()
        
        return csv_path
        
    except Exception as e:
        print(f"❌ Error generating CSV: {e}")
        return None

def demo_transliteration():
    """Demonstrate Hindi to English transliteration."""
    print("🔄 Transliteration Demo")
    print("=" * 50)
    
    # Sample Hindi names
    hindi_names = [
        "राम कुमार शर्मा",
        "सीता देवी",
        "अर्जुन कुमार",
        "प्रिया शर्मा",
        "गीता पटेल",
        "मोहन लाल गुप्ता",
        "कृष्णा वर्मा",
        "राधा कुमारी"
    ]
    
    # Create processor
    config = Config()
    config._config['input_folder_id'] = 'demo'
    config._config['output_folder_id'] = 'demo'
    
    processor = HindiTextProcessor(config)
    
    print("Hindi to English Transliteration:")
    print("-" * 40)
    print(f"{'Hindi Name':<20} {'English Name':<20} {'Lowercase'}")
    print("-" * 60)
    
    for hindi_name in hindi_names:
        english_name = processor.transliterate_text(hindi_name)
        lowercase_name = english_name.lower()
        
        print(f"{hindi_name:<20} {english_name:<20} {lowercase_name}")
    
    print()

def demo_text_cleaning():
    """Demonstrate text cleaning capabilities."""
    print("🧹 Text Cleaning Demo")
    print("=" * 50)
    
    # Sample messy Hindi text
    messy_texts = [
        "राम  कुमार!@#$%^&*()  शर्मा   ।।। ",
        "नाम    :    सीता     देवी     ।।",
        "!!!   गीता   ###   पटेल   $$$ ",
        "  आयु: 25 वर्ष   ।।।",
        "<<>>  मोहन लाल {{}} गुप्ता  <<<"
    ]
    
    config = Config()
    config._config['input_folder_id'] = 'demo'
    config._config['output_folder_id'] = 'demo'
    
    processor = HindiTextProcessor(config)
    
    print("Text Cleaning Examples:")
    print("-" * 40)
    
    for i, messy_text in enumerate(messy_texts, 1):
        cleaned_text = processor.clean_text(messy_text)
        
        print(f"{i}. Original:  '{messy_text}'")
        print(f"   Cleaned:   '{cleaned_text}'")
        print()

def main():
    """Run all component demonstrations."""
    print("🚀 Hindi PDF Pipeline - Component Demo")
    print("=" * 60)
    print()
    
    try:
        # Demo 1: Text Cleaning
        demo_text_cleaning()
        
        # Demo 2: Transliteration
        demo_transliteration()
        
        # Demo 3: Text Processing
        structured_data = demo_text_processing()
        
        # Demo 4: CSV Generation
        if structured_data:
            demo_csv_generation([structured_data])
        
        print("✅ All component demos completed successfully!")
        print()
        print("💡 Next Steps:")
        print("1. Set up Google Drive API credentials")
        print("2. Configure .env file with folder IDs")
        print("3. Run the full pipeline: python examples/run_pipeline.py --mode single")
        
    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
