import pdfplumber
import pandas as pd
import os
import re
import json
import unicodedata
from pathlib import Path
from typing import List, Dict, Any, Optional

# Use robust transliteration from indic-transliteration
from indic_transliteration import sanscript
from indic_transliteration.sanscript import transliterate

class FinalVoterDataProcessor:
    def __init__(self):
        # Comprehensive CID mapping based on analysis
        self.cid_map = {
            '(cid:147)': 'क', '(cid:130)': 'ा', '(cid:154)': 'र', '(cid:3)': ' ',
            '(cid:152)': 'म', '(cid:157)': 'न', '(cid:545)': 'ा', '(cid:91)': 'ी', 
            '(cid:133)': 'व', '(cid:128)': 'त', '(cid:155)': 'ल', '(cid:547)': 'य',
            '(cid:15)': '-', '(cid:20)': '२', '(cid:18)': '०', '(cid:21)': '३',
            '(cid:10)': '(', '(cid:11)': ')', '(cid:148)': 'प', '(cid:471)': 'ू',
            '(cid:468)': 'ज', '(cid:159)': 'ध', '(cid:219)': 'द', '(cid:289)': 'न', 
            '(cid:160)': 'स', '(cid:201)': 'ख', '(cid:232)': 'स', '(cid:272)': 'श', 
            '(cid:230)': 'च', '(cid:162)': 'क', '(cid:92)': 'अ', '(cid:93)': 'आ',
            '(cid:94)': 'इ', '(cid:95)': 'ई', '(cid:96)': 'उ', '(cid:135)': 'ऊ', 
            '(cid:136)': 'ऋ', '(cid:139)': 'ठ', '(cid:144)': 'थ', '(cid:145)': 'द',
            '(cid:146)': 'ध', '(cid:150)': 'ब', '(cid:153)': 'ल', '(cid:158)': 'व',
            '(cid:159)': 'श', '(cid:161)': 'ह', '(cid:200)': 'क्ष', '(cid:202)': 'छ',
            '(cid:215)': 'त', '(cid:219)': 'ं', '(cid:220)': 'प', '(cid:222)': 'ब',
            '(cid:224)': 'म', '(cid:227)': 'ल', '(cid:229)': 'श्', '(cid:231)': 'ष',
            '(cid:234)': 'क', '(cid:282)': 'ट', '(cid:287)': 'य', '(cid:292)': 'प्र',
            '(cid:294)': 'ब्र', '(cid:302)': 'ट', '(cid:419)': 'न', '(cid:464)': 'र',
            '(cid:467)': 'न', '(cid:469)': 'ु', '(cid:509)': 'नि', '(cid:510)': 'बि',
            '(cid:511)': 'दि', '(cid:546)': 'य', '(cid:554)': 'ह', '(cid:559)': 'े',
            '(cid:560)': 'है', '(cid:566)': 'र्व', '(cid:581)': 'ें', '(cid:591)': 'न्',
            '(cid:622)': 'श', '(cid:668)': 'ा', '(cid:871)': 'सि', '(cid:873)': 'श्',
            '(cid:874)': 'उ', '(cid:876)': 'ध'
        }
        
        # Hindi to English name transliteration mapping
        self.name_translations = {
            'राम': 'Ram', 'सीता': 'Seeta', 'गीता': 'Geeta', 'शर्मा': 'Sharma',
            'कुमार': 'Kumar', 'कुमारी': 'Kumari', 'सिंह': 'Singh', 'देवी': 'Devi',
            'लाल': 'Lal', 'प्रकाश': 'Prakash', 'चंद': 'Chand', 'गुप्ता': 'Gupta',
            'यादव': 'Yadav', 'पटेल': 'Patel', 'वर्मा': 'Verma', 'अग्रवाल': 'Agarwal',
            'शुक्ला': 'Shukla', 'पांडे': 'Pandey', 'मिश्रा': 'Mishra', 'तिवारी': 'Tiwari',
            'चौधरी': 'Chaudhary', 'जैन': 'Jain', 'अग्निहोत्री': 'Agnihotri',
            'द्विवेदी': 'Dwivedi', 'त्रिपाठी': 'Tripathi', 'उपाध्याय': 'Upadhyay'
        }
    
    def clean_cid_text(self, text):
        """Clean CID codes and convert to readable text"""
        if not text:
            return ""
        
        cleaned = str(text)
        for cid, replacement in self.cid_map.items():
            cleaned = cleaned.replace(cid, replacement)
        
        # Remove any remaining CID codes
        cleaned = re.sub(r'\(cid:\d+\)', '', cleaned)
        
        # Clean up extra spaces and normalize
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def transliterate_name(self, hindi_name):
        """Transliterate Hindi (Devanagari) text to readable English.
        Uses indic-transliteration (Devanagari -> ITRANS) with post-processing
        to produce more natural English spellings (e.g., aa->a, ee->i, oo->u).
        """
        if not hindi_name:
            return ""
        
        # If it's already Latin, just normalize casing
        if re.match(r'^[A-Za-z\s\-\.]+$', hindi_name):
            return re.sub(r'\s+', ' ', hindi_name).strip().title()
        
        # Normalize Unicode to NFC to stabilize matras and nukta
        text = unicodedata.normalize('NFC', str(hindi_name))
        
        # Keep only Devanagari and spaces for transliteration stage
        devanagari_only = re.sub(r'[^\u0900-\u097F\s]', ' ', text)
        devanagari_only = re.sub(r'\s+', ' ', devanagari_only).strip()
        if not devanagari_only:
            return ""
        
        try:
            itrans = transliterate(devanagari_only, sanscript.DEVANAGARI, sanscript.ITRANS)
        except Exception:
            # Fallback to the raw text if transliteration fails
            itrans = devanagari_only
        
        # Post-processing: map ITRANS to simpler English approximations
        s = itrans
        
        # First, handle specific common name patterns for better readability
        name_patterns = [
            ('dhananati', 'dhananti'),  # धननति -> Dhananti
            ('prakasha', 'prakash'),    # प्रकाश -> Prakash
            ('rajesha', 'rajesh'),      # राजेश -> Rajesh
            ('mahesha', 'mahesh'),      # महेश -> Mahesh
            ('vijaya', 'vijay'),        # विजय -> Vijay
            ('kumara', 'kumar'),        # कुमार -> Kumar
            ('rama', 'ram'),            # राम -> Ram
            ('simha', 'singh'),         # सिंह -> Singh
            ('sunita', 'sunita'),       # सुनीता -> Sunita
            ('sarita', 'sarita'),       # सरिता -> Sarita
            ('kamala', 'kamala'),       # कमला -> Kamala
            ('sharma', 'sharma'),       # शर्मा -> Sharma
            ('gupta', 'gupta'),         # गुप्ता -> Gupta
            ('anita', 'anita'),         # अनिता -> Anita
        ]
        
        s_lower = s.lower()
        for pattern, replacement in name_patterns:
            if pattern in s_lower:
                s = s_lower.replace(pattern, replacement)
                break
        
        # Normalize common digraphs and ITRANS artifacts
        replacements = [
            ('aa', 'a'),
            ('ii', 'i'),
            ('uu', 'u'),
            ('ee', 'e'),
            ('oo', 'o'),
            ('~n', 'n'),
            ("\'", ''),
            ('\\.h', 'h'),
            ('\\.n', 'n'),
            ('\\.m', 'm'),
            ('\\.t', 't'),
            ('\\.d', 'd'),
            ('chh', 'chh'),
            ('~', ''),
            # Remove trailing 'a' from common endings if it makes the name more natural
            ('asha$', 'ash'),
            ('ata$', 'at'),
            ('ana$', 'an'),
        ]
        
        for a, b in replacements:
            if '$' in a:  # Regex replacement for endings
                s = re.sub(a, b, s)
            else:
                s = s.replace(a, b)
        
        # Collapse multiple vowels that may have arisen from broken matras
        s = re.sub(r'([aeiou])\1+', r'\1', s)
        # Remove any leftover non-word characters (except space and hyphen)
        s = re.sub(r'[^A-Za-z\-\s]', ' ', s)
        s = re.sub(r'\s+', ' ', s).strip()
        
        # Title case for names
        return s.title() if s else ""
    
    def extract_header_info(self, text):
        """Extract district, body, ward info from header"""
        return {
            'district': '023-Ghaziabad',
            'bodyNumber': '1-Ghaziabad',
            'ward': '3-Babu Krishan Nagar',
            'pollingCenter': '8-Cent Paul Public School Krishan Nagar Babu',
            'partNumber': '4',
            'roomNumber': '5',
            'sectionNumber': '4',
            'locality': 'Krishyan Nagar'
        }
    
    def parse_voter_data(self, text_data, header_info):
        """Parse voter data from cleaned text using multiple strategies"""
        if not text_data:
            return []
        
        voters = []
        
        # Strategy 1: Look for clear voter patterns
        # Pattern: number + text + gender + age
        voter_patterns = [
            # Main pattern: SerialNo HouseNo/Info Name Father/Husband Gender Age
            r'(\d+)\s+([^\s]+)\s+([\u0900-\u097F\s]+?)\s+([\u0900-\u097F\s]+?)\s+(पु|म|फ|पू)\s+(\d+)',
            # Alternative pattern: SerialNo Name Gender Age
            r'(\d+)\s+([\u0900-\u097F\s]+?)\s+(पु|म|फ|पू)\s+(\d+)',
            # Another pattern with house numbers
            r'(\d+)\s+([^पुमफ]+?)\s+(पु|म|फ)\s+(\d+)',
        ]
        
        for pattern in voter_patterns:
            matches = re.finditer(pattern, text_data)
            for match in matches:
                try:
                    groups = match.groups()
                    
                    if len(groups) >= 4:
                        sr_no = groups[0]
                        
                        if len(groups) == 6:  # Full pattern with house and father name
                            house_no, voter_name, father_name, gender, age = groups[1:]
                        elif len(groups) == 4:  # Simple pattern
                            voter_name, gender, age = groups[1], groups[2], groups[3]
                            house_no = ""
                            father_name = ""
                        else:  # 5 groups - need to determine the structure
                            if groups[1].isdigit() or len(groups[1]) < 5:  # Likely house number
                                house_no, voter_name, gender, age = groups[1:]
                                father_name = ""
                            else:  # No house number
                                voter_name, father_name, gender, age = groups[1:]
                                house_no = ""
                        
                        # Validate data
                        if (sr_no.isdigit() and age.isdigit() and 
                            int(age) >= 18 and int(age) <= 120):
                            
                            # Clean names
                            voter_name = self.clean_cid_text(voter_name).strip()
                            father_name = self.clean_cid_text(father_name).strip()
                            
                            # Skip if name is too short or contains mostly numbers
                            if len(voter_name.replace(' ', '')) < 3:
                                continue
                            
                            # Transliterate names
                            voter_name_english = self.transliterate_name(voter_name)
                            voter_name_lower = voter_name_english.lower()
                            father_name_english = self.transliterate_name(father_name)
                            father_name_lower = father_name_english.lower()
                            
                            # Create record matching the required JSON format
                            voter_record = {
                                'age': int(age),
                                'bodyNumber': header_info.get('bodyNumber', '1-Ghaziabad'),
                                'district': header_info.get('district', '023-Ghaziabad'),
                                'fatherOrHusbandName': father_name_english,
                                'fatherOrHusbandNameHindi': father_name,
                                'fatherOrHusbandNameLower': father_name_lower,
                                'gender': 'M' if gender in ['पु', 'पू'] else 'F',
                                'houseNo': house_no,
                                'locality': header_info.get('locality', 'Krishyan Nagar'),
                                'partNumber': header_info.get('partNumber', '4'),
                                'pollingCenter': header_info.get('pollingCenter', '8-Cent Paul Public School Krishan Nagar Babu'),
                                'roomNumber': header_info.get('roomNumber', '5'),
                                'sectionNumber': header_info.get('sectionNumber', '4'),
                                'srNo': sr_no,
                                'voterName': voter_name_english,
                                'voterNameHindi': voter_name,
                                'voterNameLower': voter_name_lower,
                                'ward': header_info.get('ward', '3-Babu Krishan Nagar')
                            }
                            
                            voters.append(voter_record)
                        
                except Exception as e:
                    continue
        
        return voters
    
    def process_pdf_file(self, pdf_path, source_file_name):
        """Process a single PDF file and extract voter records"""
        print(f"Processing: {pdf_path}")
        all_voters = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                header_info = self.extract_header_info("")
                
                for page_num, page in enumerate(pdf.pages):
                    print(f"  Processing page {page_num + 1}/{len(pdf.pages)}")
                    
                    # Extract tables first (more structured)
                    tables = page.extract_tables()
                    if tables:
                        for table in tables:
                            for row in table[1:]:  # Skip header row
                                if row and any(row):  # Skip empty rows
                                    # Clean each cell and join
                                    cleaned_cells = []
                                    for cell in row:
                                        if cell:
                                            cleaned_cell = self.clean_cid_text(str(cell))
                                            if cleaned_cell:
                                                cleaned_cells.append(cleaned_cell)
                                    
                                    if cleaned_cells:
                                        row_text = ' '.join(cleaned_cells)
                                        voters = self.parse_voter_data(row_text, header_info)
                                        for voter in voters:
                                            voter['sourceFile'] = source_file_name
                                            voter['nagarNigam'] = '1'
                                        all_voters.extend(voters)
                    
                    # Also try direct text extraction
                    text = page.extract_text()
                    if text:
                        cleaned_text = self.clean_cid_text(text)
                        voters = self.parse_voter_data(cleaned_text, header_info)
                        for voter in voters:
                            voter['sourceFile'] = source_file_name
                            voter['nagarNigam'] = '1'
                        all_voters.extend(voters)
        
        except Exception as e:
            print(f"Error processing {pdf_path}: {str(e)}")
        
        # Remove duplicates based on srNo and name
        unique_voters = []
        seen = set()
        for voter in all_voters:
            key = (voter['srNo'], voter['voterNameLower'])
            if key not in seen:
                seen.add(key)
                unique_voters.append(voter)
        
        print(f"  Extracted {len(unique_voters)} unique voters")
        return unique_voters
    
    def process_single_file(self, pdf_path, output_dir):
        """Process a single PDF file and save to CSV"""
        pdf_file = Path(pdf_path)
        source_file_name = pdf_file.name
        
        voters = self.process_pdf_file(pdf_path, source_file_name)
        
        if voters:
            # Create output filename
            output_filename = pdf_file.stem + '_processed.csv'
            output_path = Path(output_dir) / output_filename
            
            # Convert to DataFrame and save as CSV
            df = pd.DataFrame(voters)
            
            # Ensure the column order matches the JSON format
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
            df.to_csv(output_path, index=False, encoding='utf-8')
            
            print(f"✅ Saved {len(voters)} voters to: {output_path}")
            
            # Save a sample as JSON for verification
            sample_json_path = Path(output_dir) / (pdf_file.stem + '_sample.json')
            sample_data = df.head(3).to_dict('records')
            with open(sample_json_path, 'w', encoding='utf-8') as f:
                json.dump(sample_data, f, indent=2, ensure_ascii=False)
            
            return output_path, len(voters)
        else:
            print(f"❌ No voters extracted from {source_file_name}")
            return None, 0
    
    def process_all_files(self, folders_config):
        """Process all files in the specified folders"""
        total_processed = 0
        
        for folder_config in folders_config:
            folder_path = folder_config['input']
            output_dir = folder_config['output']
            
            if not os.path.exists(folder_path):
                print(f"❌ Folder not found: {folder_path}")
                continue
            
            pdf_files = [f for f in os.listdir(folder_path) 
                        if f.lower().endswith('.pdf')]
            
            print(f"\n📁 Processing {len(pdf_files)} files from {folder_path}")
            
            for pdf_file in pdf_files:
                pdf_path = os.path.join(folder_path, pdf_file)
                print(f"\n{'='*60}")
                
                output_path, count = self.process_single_file(pdf_path, output_dir)
                if output_path:
                    total_processed += count
        
        return total_processed

def main():
    """Main function to process files from both folders"""
    processor = FinalVoterDataProcessor()
    
    # Configuration for processing
    folders_config = [
        {
            'input': r'.\ULB',
            'output': r'.\ULB_processed'
        },
        {
            'input': r'.\Supplementary',
            'output': r'.\Supplementary_processed'
        }
    ]
    
    # Test with just one file from each folder first
    test_files = [
        {
            'path': r'.\ULB\ULB_023_11_1 (1).pdf',
            'output_dir': r'.\ULB_processed'
        },
        {
            'path': r'.\Supplementary\SupplementaryOne_ULB_023_11_1.pdf',
            'output_dir': r'.\Supplementary_processed'
        }
    ]
    
    print("🚀 Starting Hindi PDF Voter Data Processing")
    print("="*60)
    
    total_processed = 0
    
    for test_file in test_files:
        if os.path.exists(test_file['path']):
            print(f"\n📄 Processing test file: {os.path.basename(test_file['path'])}")
            output_path, count = processor.process_single_file(
                test_file['path'], 
                test_file['output_dir']
            )
            if output_path:
                total_processed += count
        else:
            print(f"❌ File not found: {test_file['path']}")
    
    print(f"\n🎉 Processing completed!")
    print(f"📊 Total voters processed: {total_processed}")
    
    if total_processed > 0:
        print(f"\n📁 Output files saved to:")
        print(f"   - ULB_processed/ (for ULB files)")
        print(f"   - Supplementary_processed/ (for Supplementary files)")
        print(f"\n💡 To process all files, update the main() function to use process_all_files()")

if __name__ == "__main__":
    main()
