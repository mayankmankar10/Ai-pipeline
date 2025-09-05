import pdfplumber
import pandas as pd
import os
import re
import json
from pathlib import Path

# Import our processor class
import sys
sys.path.append('.')
from pdf_voter_processor import VoterDataProcessor

def test_on_sample_files():
    """Test the processor on a few sample files"""
    processor = VoterDataProcessor()
    
    # Test files - just pick first 2 files from each folder
    test_files = [
        r'.\Supplementary\SupplementaryOne_ULB_023_11_1.pdf',
        r'.\ULB\ULB_023_11_1 (1).pdf'
    ]
    
    all_voters = []
    
    for pdf_file in test_files:
        if os.path.exists(pdf_file):
            print(f"\n{'='*50}")
            print(f"Testing: {pdf_file}")
            print('='*50)
            
            voters = processor.process_pdf_file(pdf_file)
            
            # Add metadata
            for voter in voters:
                voter['sourceFile'] = os.path.basename(pdf_file)
                voter['nagarNigam'] = '1'
            
            all_voters.extend(voters)
            print(f"Extracted {len(voters)} voters from {pdf_file}")
            
            # Show first few records
            if voters:
                print("\nSample records:")
                for i, voter in enumerate(voters[:3]):
                    print(f"\nRecord {i+1}:")
                    for key, value in voter.items():
                        print(f"  {key}: {value}")
        else:
            print(f"File not found: {pdf_file}")
    
    print(f"\nTotal test records: {len(all_voters)}")
    
    if all_voters:
        # Save test results
        df = pd.DataFrame(all_voters)
        
        # Reorder columns to match the example format
        column_order = [
            'age', 'bodyNumber', 'district', 'fatherOrHusbandName',
            'fatherOrHusbandNameHindi', 'fatherOrHusbandNameLower',
            'gender', 'houseNo', 'locality', 'partNumber',
            'pollingCenter', 'roomNumber', 'sectionNumber', 'srNo',
            'voterName', 'voterNameHindi', 'voterNameLower', 'ward',
            'sourceFile', 'nagarNigam'
        ]
        
        # Ensure all columns exist
        for col in column_order:
            if col not in df.columns:
                df[col] = ''
        
        df = df[column_order]
        
        # Save test CSV
        df.to_csv('test_voter_data.csv', index=False, encoding='utf-8')
        print(f"Test data saved to test_voter_data.csv")
        
        # Save sample JSON
        sample_data = df.head(5).to_dict('records')
        with open('test_sample.json', 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, indent=2, ensure_ascii=False)
        print(f"Sample JSON saved to test_sample.json")
        
        print("\nFirst few records in CSV format:")
        print(df.head().to_string(index=False))

if __name__ == "__main__":
    test_on_sample_files()
