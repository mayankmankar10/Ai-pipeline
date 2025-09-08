#!/usr/bin/env python3
import pdfplumber
import re
from final_voter_processor import FinalVoterDataProcessor

def debug_pdf_issues():
    """Debug the actual issues with PDF processing"""
    processor = FinalVoterDataProcessor()
    pdf_path = "./ULB/ULB_023_11_1 (1).pdf"
    
    print("🔍 DEBUGGING PDF EXTRACTION ISSUES")
    print("=" * 60)
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Examine first few pages in detail
            for page_num in range(min(3, len(pdf.pages))):
                page = pdf.pages[page_num]
                print(f"\n--- PAGE {page_num + 1} ANALYSIS ---")
                
                # 1. Raw text extraction
                print("\n1. RAW TEXT SAMPLE:")
                raw_text = page.extract_text()
                if raw_text:
                    # Show first 500 characters of raw text
                    print(f"Raw text preview (first 500 chars):")
                    print(repr(raw_text[:500]))
                    
                    # 2. CID cleaned text
                    print("\n2. AFTER CID CLEANING:")
                    cleaned = processor.clean_cid_text(raw_text)
                    print(f"Cleaned text preview (first 500 chars):")
                    print(repr(cleaned[:500]))
                    
                    # 3. Check for Hindi text presence
                    print("\n3. HINDI TEXT CHECK:")
                    hindi_chars = re.findall(r'[\u0900-\u097F]', cleaned)
                    print(f"Found {len(hindi_chars)} Hindi characters")
                    print(f"Sample Hindi chars: {hindi_chars[:20]}")
                    
                    # 4. Look for patterns that might be voter records
                    print("\n4. PATTERN ANALYSIS:")
                    lines = cleaned.split('\n')
                    print(f"Total lines: {len(lines)}")
                    
                    # Look for lines with numbers (potential serial numbers)
                    numeric_lines = [line.strip() for line in lines if re.search(r'\d+', line) and len(line.strip()) > 10]
                    print(f"Lines with numbers (potential records): {len(numeric_lines)}")
                    
                    # Show some examples
                    print("\nSample lines with numbers:")
                    for i, line in enumerate(numeric_lines[:5]):
                        print(f"  {i+1}: {repr(line)}")
                    
                    # 5. Try to find gender markers
                    print("\n5. GENDER MARKER ANALYSIS:")
                    gender_patterns = ['पु', 'म', 'फ', 'स्त्री', 'पुरुष']
                    for pattern in gender_patterns:
                        matches = len(re.findall(pattern, cleaned))
                        print(f"  '{pattern}': {matches} matches")
                    
                    # 6. Age pattern analysis
                    print("\n6. AGE PATTERN ANALYSIS:")
                    # Look for two-digit numbers that might be ages
                    age_candidates = re.findall(r'\b(\d{2})\b', cleaned)
                    valid_ages = [age for age in age_candidates if 18 <= int(age) <= 90]
                    print(f"Potential ages found: {len(valid_ages)}")
                    print(f"Sample ages: {valid_ages[:10]}")
                    
                # 7. Table extraction
                print("\n7. TABLE EXTRACTION:")
                tables = page.extract_tables()
                print(f"Found {len(tables)} tables")
                
                if tables:
                    for t_idx, table in enumerate(tables[:1]):  # Just first table
                        print(f"\nTable {t_idx + 1} structure:")
                        print(f"  Rows: {len(table)}")
                        if table:
                            print(f"  Columns: {len(table[0]) if table[0] else 0}")
                            
                            # Show first few rows
                            for row_idx, row in enumerate(table[:3]):
                                if row:
                                    cleaned_row = []
                                    for cell in row:
                                        if cell:
                                            cleaned_cell = processor.clean_cid_text(str(cell))
                                            cleaned_row.append(cleaned_cell[:50] + "..." if len(cleaned_cell) > 50 else cleaned_cell)
                                        else:
                                            cleaned_row.append("")
                                    print(f"  Row {row_idx + 1}: {cleaned_row}")
                
                print("-" * 50)
                
                if page_num >= 2:  # Limit detailed analysis to first 3 pages
                    break
                    
    except Exception as e:
        print(f"Error during debug analysis: {e}")
        import traceback
        traceback.print_exc()

def test_voter_parsing():
    """Test voter parsing with sample data"""
    processor = FinalVoterDataProcessor()
    
    print("\n\n🧪 TESTING VOTER PARSING")
    print("=" * 60)
    
    # Test with some sample lines that might exist in the PDF
    test_lines = [
        "1 कशजचचयन राम कुमार राम प्रसाद पु 45",
        "2 ए01 सीता देवी मोहन लाल म 38",
        "3 बी02 प्रकाश गुप्ता शुक्ला जी पु 52",
        "49 कशजचचयन राजक ाुमार रामपाल सिसंह पु 46"
    ]
    
    header_info = processor.extract_header_info("")
    
    for i, test_line in enumerate(test_lines):
        print(f"\nTest {i+1}: {repr(test_line)}")
        try:
            voters = processor.extract_voters_from_line(test_line, header_info)
            if voters:
                print(f"  ✅ Extracted {len(voters)} voters:")
                for voter in voters:
                    validation = processor.validate_voter_record(voter)
                    status = "✅ Valid" if validation['valid'] else f"❌ Issues: {', '.join(validation['issues'])}"
                    print(f"    Sr.No: {voter.get('srNo')}, Name: {voter.get('voterName')} / {voter.get('voterNameHindi')}, {status}")
            else:
                print("  ❌ No voters extracted")
        except Exception as e:
            print(f"  ❌ Error: {e}")

if __name__ == "__main__":
    debug_pdf_issues()
    test_voter_parsing()
