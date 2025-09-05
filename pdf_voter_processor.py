import pdfplumber
import pandas as pd
import os
import re
import json
from pathlib import Path

class VoterDataProcessor:
    def __init__(self):
        # Hindi to English mapping for common voter list terms
        self.hindi_english_map = {
            # Gender mappings
            'पु': 'M', 'पुरुष': 'M', 'म': 'F', 'महिला': 'F', 'फ': 'F', 'पुं': 'M',
            
            # Common Hindi names and their transliterations
            'गाजियाबाद': 'Ghaziabad', 'गाज़ियाबाद': 'Ghaziabad',
            'कृष्णन': 'Krishan', 'कृषन': 'Krishan', 'कृष्ण': 'Krishna',
            'नगर': 'Nagar', 'सरिता': 'Sarita', 'देवी': 'Devi',
            'ओम': 'Om', 'प्रकाश': 'Prakash', 'राम': 'Ram',
            'शर्मा': 'Sharma', 'गुप्ता': 'Gupta', 'सिंह': 'Singh',
            'कुमार': 'Kumar', 'कुमारी': 'Kumari',
            'बाबू': 'Babu', 'लाल': 'Lal', 'चंद': 'Chand', 'चांद': 'Chand',
            'दास': 'Das', 'गोपाल': 'Gopal', 'राज': 'Raj',
            'सुनीता': 'Sunita', 'मीरा': 'Meera', 'गीता': 'Geeta',
            'अनिता': 'Anita', 'रीता': 'Rita', 'सीता': 'Seeta',
            'विजय': 'Vijay', 'अजय': 'Ajay', 'संजय': 'Sanjay',
            'राजेश': 'Rajesh', 'सुरेश': 'Suresh', 'महेश': 'Mahesh',
            'दिनेश': 'Dinesh', 'नरेश': 'Naresh', 'उमेश': 'Umesh',
            'रमेश': 'Ramesh', 'योगेश': 'Yogesh', 'मुकेश': 'Mukesh',
            'रविकांत': 'Ravikant', 'शिवकुमार': 'Shivkumar',
            'जगदीश': 'Jagdish', 'हरीश': 'Harish', 'गिरीश': 'Girish',
            'अशोक': 'Ashok', 'विनोद': 'Vinod', 'प्रमोद': 'Pramod',
            'पंकज': 'Pankaj', 'विकास': 'Vikas', 'आकाश': 'Akash',
            'सुमित्रा': 'Sumitra', 'कमला': 'Kamala', 'शांति': 'Shanti',
            'सुधा': 'Sudha', 'उषा': 'Usha', 'माया': 'Maya',
            'सरला': 'Sarla', 'बिमला': 'Bimala', 'निर्मला': 'Nirmala',
            'संतोष': 'Santosh', 'प्रकाशी': 'Prakashi', 'सुशीला': 'Sushila',
            'कविता': 'Kavita', 'ममता': 'Mamta', 'सुमित्रा': 'Sumitra',
            'पूजा': 'Pooja', 'प्रिया': 'Priya', 'सुनीता': 'Sunita'
        }
        
        # Pattern to match voter entries
        self.voter_pattern = r'(\d+)\s+([^पुमफ]+?)\s+(पु|म|फ|पुरुष|महिला)\s+(\d+)'
        
    def clean_text(self, text):
        """Clean and normalize text"""
        if not text:
            return ""
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        return text
    
    def transliterate_name(self, hindi_name):
        """Simple transliteration of Hindi names to English"""
        if not hindi_name:
            return ""
        
        # First check if already in English
        if re.match(r'^[a-zA-Z\s\.\-]+$', hindi_name):
            return hindi_name.title()
        
        # Replace known mappings
        english_name = hindi_name
        for hindi, english in self.hindi_english_map.items():
            english_name = english_name.replace(hindi, english)
        
        # Simple character-by-character mapping for remaining characters
        char_map = {
            'अ': 'a', 'आ': 'aa', 'इ': 'i', 'ई': 'ee', 'उ': 'u', 'ऊ': 'oo',
            'ए': 'e', 'ऐ': 'ai', 'ओ': 'o', 'औ': 'au',
            'क': 'k', 'ख': 'kh', 'ग': 'g', 'घ': 'gh', 'ङ': 'ng',
            'च': 'ch', 'छ': 'chh', 'ज': 'j', 'झ': 'jh', 'ञ': 'ny',
            'ट': 't', 'ठ': 'th', 'ड': 'd', 'ढ': 'dh', 'ण': 'n',
            'त': 't', 'थ': 'th', 'द': 'd', 'ध': 'dh', 'न': 'n',
            'प': 'p', 'फ': 'ph', 'ब': 'b', 'भ': 'bh', 'म': 'm',
            'य': 'y', 'र': 'r', 'ल': 'l', 'व': 'v', 'श': 'sh',
            'ष': 'sh', 'स': 's', 'ह': 'h', 'ा': 'a', 'ि': 'i',
            'ी': 'ee', 'ु': 'u', 'ू': 'oo', 'े': 'e', 'ै': 'ai',
            'ो': 'o', 'ौ': 'au', '्': '', 'ं': 'n', 'ः': 'h'
        }
        
        for hindi_char, english_char in char_map.items():
            english_name = english_name.replace(hindi_char, english_char)
        
        # Clean up and format
        english_name = re.sub(r'[^\w\s]', '', english_name)
        english_name = re.sub(r'\s+', ' ', english_name).strip().title()
        
        return english_name if english_name else hindi_name
    
    def extract_header_info(self, text):
        """Extract district, body, ward, polling center info from header"""
        info = {
            'district': '',
            'bodyNumber': '',
            'ward': '',
            'pollingCenter': '',
            'partNumber': '',
            'roomNumber': '',
            'sectionNumber': ''
        }
        
        lines = text.split('\n')[:10]  # Check first 10 lines
        
        for line in lines:
            line = line.strip()
            
            # Extract district (जिला)
            if 'जिला' in line or 'ज़िला' in line:
                match = re.search(r'(\d{2,3}[-\s]*गाजियाबाद|गाजियाबाद)', line)
                if match:
                    info['district'] = '023-Ghaziabad'
            
            # Extract body number
            if 'निकाय' in line and 'गाजियाबाद' in line:
                info['bodyNumber'] = '1-Ghaziabad'
            
            # Extract ward
            if 'वार्ड' in line:
                match = re.search(r'(\d+[-\s]*[^0-9\s]+)', line)
                if match:
                    ward_text = match.group(1)
                    # Transliterate ward name
                    ward_parts = ward_text.split('-')
                    if len(ward_parts) >= 2:
                        ward_num = ward_parts[0]
                        ward_name = self.transliterate_name(ward_parts[1].strip())
                        info['ward'] = f"{ward_num}-{ward_name}"
            
            # Extract polling center
            if 'मतदान केंद्र' in line or 'स्कूल' in line:
                # Try to extract polling center info
                if 'स्कूल' in line:
                    school_match = re.search(r'(\d+[-\s]*[^0-9]+स्कूल[^0-9]*)', line)
                    if school_match:
                        center_text = school_match.group(1)
                        info['pollingCenter'] = self.transliterate_name(center_text)
            
            # Extract part number
            if 'भाग संख्या' in line:
                part_match = re.search(r'भाग संख्या[:\s]*(\d+)', line)
                if part_match:
                    info['partNumber'] = part_match.group(1)
        
        return info
    
    def parse_voter_entry(self, entry_text, header_info):
        """Parse individual voter entry"""
        entry_text = self.clean_text(entry_text)
        
        # Try to extract voter information using regex
        # Pattern: Serial_Number House_Number Name Father/Husband_Name Gender Age
        patterns = [
            r'(\d+)\s+([^\d\s]+(?:\s+[^\d\s]+)*?)\s+([^\d\s]+(?:\s+[^\d\s]+)*?)\s+(पु|म|फ|पुरुष|महिला)\s+(\d+)',
            r'(\d+)\s+([^पुमफ]+?)\s+(पु|म|फ)\s+(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, entry_text)
            if match:
                try:
                    if len(match.groups()) == 5:
                        sr_no, house_or_name1, name_or_father, gender, age = match.groups()
                        # Try to determine which is name and which is father's name
                        voter_name = house_or_name1.strip()
                        father_name = name_or_father.strip()
                        house_no = ""
                    else:
                        sr_no, voter_name, gender, age = match.groups()
                        father_name = ""
                        house_no = ""
                    
                    # Transliterate names
                    voter_name_english = self.transliterate_name(voter_name)
                    voter_name_lower = voter_name_english.lower()
                    father_name_english = self.transliterate_name(father_name)
                    father_name_lower = father_name_english.lower()
                    
                    # Normalize gender
                    gender_normalized = 'M' if gender in ['पु', 'पुरुष'] else 'F'
                    
                    voter_record = {
                        'srNo': sr_no,
                        'voterName': voter_name_english,
                        'voterNameHindi': voter_name,
                        'voterNameLower': voter_name_lower,
                        'fatherOrHusbandName': father_name_english,
                        'fatherOrHusbandNameHindi': father_name,
                        'fatherOrHusbandNameLower': father_name_lower,
                        'gender': gender_normalized,
                        'age': int(age),
                        'houseNo': house_no,
                        'district': header_info.get('district', '023-Ghaziabad'),
                        'bodyNumber': header_info.get('bodyNumber', '1-Ghaziabad'),
                        'ward': header_info.get('ward', '3-Babu Krishan Nagar'),
                        'pollingCenter': header_info.get('pollingCenter', '8-Cent Paul Public School Krishan Nagar Babu'),
                        'partNumber': header_info.get('partNumber', '4'),
                        'roomNumber': header_info.get('roomNumber', '5'),
                        'sectionNumber': header_info.get('sectionNumber', '4'),
                        'locality': 'Krishyan Nagar'  # Default locality
                    }
                    
                    return voter_record
                except Exception as e:
                    print(f"Error parsing voter entry: {e}")
                    continue
        
        return None
    
    def clean_cid_text(self, text):
        """Clean CID codes and convert to readable text"""
        if not text:
            return ""
        
        # Remove CID codes like (cid:XXX) and replace with approximate characters
        cid_replacements = {
            '(cid:147)': 'क', '(cid:130)': 'ा', '(cid:154)': 'र', '(cid:3)': '',
            '(cid:152)': 'म', '(cid:157)': 'न', '(cid:545)': 'ा', '(cid:91)': 'ी', 
            '(cid:133)': 'व', '(cid:128)': 'त', '(cid:155)': 'ल', '(cid:547)': 'य',
            '(cid:15)': '-', '(cid:20)': '२', '(cid:18)': '०', '(cid:21)': '३',
            '(cid:10)': '(', '(cid:11)': ')', '(cid:148)': 'प', '(cid:471)': 'ू',
            '(cid:154)': 'र', '(cid:160)': 'स', '(cid:468)': 'ज', '(cid:467)': 'न',
            '(cid:219)': 'ं', '(cid:289)': 'द', '(cid:232)': 'स', '(cid:144)': 'थ',
            '(cid:201)': 'ख', '(cid:153)': 'ल', '(cid:468)': 'ज', '(cid:224)': 'म',
            '(cid:554)': 'ह', '(cid:161)': 'ह', '(cid:227)': 'ल', '(cid:559)': 'े',
            '(cid:581)': 'ें', '(cid:95)': 'ै', '(cid:292)': 'प', '(cid:272)': 'श',
            '(cid:96)': 'ि', '(cid:222)': 'ब', '(cid:146)': 'ध', '(cid:560)': 'है',
            '(cid:92)': 'अ', '(cid:876)': 'ध', '(cid:282)': 'ट', '(cid:230)': 'च',
            '(cid:302)': 'ट', '(cid:93)': 'आ', '(cid:94)': 'इ', '(cid:95)': 'ई',
            '(cid:135)': 'उ', '(cid:136)': 'ऊ', '(cid:871)': 'सि', '(cid:668)': 'क',
            '(cid:510)': 'बि', '(cid:511)': 'दि', '(cid:509)': 'नि', '(cid:622)': 'श',
            '(cid:215)': 'त', '(cid:419)': 'न', '(cid:202)': 'छ', '(cid:231)': 'ष',
            '(cid:234)': 'क', '(cid:139)': 'ठ', '(cid:159)': 'भ', '(cid:200)': 'क्ष',
            '(cid:566)': 'र्व', '(cid:591)': 'न्', '(cid:287)': 'य', '(cid:220)': 'प्र'
        }
        
        cleaned_text = text
        for cid, replacement in cid_replacements.items():
            cleaned_text = cleaned_text.replace(cid, replacement)
        
        # Remove any remaining CID codes
        cleaned_text = re.sub(r'\(cid:\d+\)', '', cleaned_text)
        
        # Clean up extra spaces
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        return cleaned_text
    
    def process_pdf_file(self, pdf_path):
        """Process a single PDF file and extract voter records"""
        print(f"Processing: {pdf_path}")
        voters = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                header_info = {}
                
                for page_num, page in enumerate(pdf.pages):
                    # Try different text extraction methods
                    text = page.extract_text()
                    if not text:
                        # Try with layout extraction
                        text = page.extract_text(layout=True)
                    if not text:
                        continue
                    
                    # Clean CID codes
                    text = self.clean_cid_text(text)
                    
                    # Extract header info from first page
                    if page_num == 0:
                        header_info = self.extract_header_info(text)
                    
                    # Try to extract table data first
                    tables = page.extract_tables()
                    if tables:
                        for table in tables:
                            for row in table[1:]:  # Skip header row
                                if row and len(row) >= 2:
                                    # Process table row data
                                    row_text = ' '.join([str(cell) if cell else '' for cell in row])
                                    row_text = self.clean_cid_text(row_text)
                                    
                                    # Extract voter entries from row
                                    voter_entries = re.findall(r'(\d+)\s+[^\d]*?\s+(पु|म|फ)\s+(\d+)', row_text)
                                    for entry in voter_entries:
                                        try:
                                            # Parse the entry
                                            parts = row_text.split()
                                            if len(parts) >= 4:
                                                sr_no = entry[0]
                                                gender = 'M' if entry[1] in ['पु'] else 'F'
                                                age = entry[2]
                                                
                                                # Try to find name around the serial number
                                                sr_idx = None
                                                for i, part in enumerate(parts):
                                                    if part == sr_no:
                                                        sr_idx = i
                                                        break
                                                
                                                if sr_idx and sr_idx + 1 < len(parts):
                                                    # Extract name (next few words after serial number)
                                                    name_parts = []
                                                    for i in range(sr_idx + 1, min(sr_idx + 5, len(parts))):
                                                        if not re.match(r'^(\d+|पु|म|फ)$', parts[i]):
                                                            name_parts.append(parts[i])
                                                        else:
                                                            break
                                                    
                                                    voter_name = ' '.join(name_parts)
                                                    if voter_name:
                                                        voter_record = self.create_voter_record(
                                                            sr_no, voter_name, '', gender, age, header_info
                                                        )
                                                        if voter_record:
                                                            voters.append(voter_record)
                                        except:
                                            continue
                    
                    # Also process line by line as fallback
                    lines = text.split('\n')
                    current_entry = ""
                    
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                        
                        # Skip header lines
                        if any(skip_word in line for skip_word in ['Page', 'जिला', 'निकाय', 'वार्ड', 'मतदान', 'भाग', 'परिशिष्ट']):
                            continue
                        
                        # Check if this line starts a new voter entry (starts with a number)
                        if re.match(r'^\d+\s+', line):
                            # Process previous entry if exists
                            if current_entry:
                                voter = self.parse_voter_entry(current_entry, header_info)
                                if voter:
                                    voters.append(voter)
                            current_entry = line
                        else:
                            # Continue current entry
                            current_entry += " " + line
                    
                    # Process last entry
                    if current_entry:
                        voter = self.parse_voter_entry(current_entry, header_info)
                        if voter:
                            voters.append(voter)
        
        except Exception as e:
            print(f"Error processing {pdf_path}: {str(e)}")
        
        return voters
    
    def create_voter_record(self, sr_no, voter_name, father_name, gender, age, header_info):
        """Create a standardized voter record"""
        try:
            voter_name_english = self.transliterate_name(voter_name)
            voter_name_lower = voter_name_english.lower()
            father_name_english = self.transliterate_name(father_name)
            father_name_lower = father_name_english.lower()
            
            voter_record = {
                'srNo': str(sr_no),
                'voterName': voter_name_english,
                'voterNameHindi': voter_name,
                'voterNameLower': voter_name_lower,
                'fatherOrHusbandName': father_name_english,
                'fatherOrHusbandNameHindi': father_name,
                'fatherOrHusbandNameLower': father_name_lower,
                'gender': gender,
                'age': int(age) if str(age).isdigit() else 0,
                'houseNo': '',
                'district': header_info.get('district', '023-Ghaziabad'),
                'bodyNumber': header_info.get('bodyNumber', '1-Ghaziabad'),
                'ward': header_info.get('ward', '3-Babu Krishan Nagar'),
                'pollingCenter': header_info.get('pollingCenter', '8-Cent Paul Public School Krishan Nagar Babu'),
                'partNumber': header_info.get('partNumber', '4'),
                'roomNumber': header_info.get('roomNumber', '5'),
                'sectionNumber': header_info.get('sectionNumber', '4'),
                'locality': 'Krishyan Nagar'
            }
            
            return voter_record
        except Exception as e:
            print(f"Error creating voter record: {e}")
            return None
    
    def process_all_pdfs(self, folders, output_file='voter_data.csv'):
        """Process all PDF files in the given folders"""
        all_voters = []
        
        for folder in folders:
            if not os.path.exists(folder):
                print(f"Folder not found: {folder}")
                continue
            
            pdf_files = [f for f in os.listdir(folder) if f.lower().endswith('.pdf')]
            print(f"Found {len(pdf_files)} PDF files in {folder}")
            
            for pdf_file in pdf_files:
                pdf_path = os.path.join(folder, pdf_file)
                voters = self.process_pdf_file(pdf_path)
                
                # Add metadata
                for voter in voters:
                    voter['sourceFile'] = pdf_file
                    voter['nagarNigam'] = '1'  # Set to 1 for all files as requested
                
                all_voters.extend(voters)
                print(f"Extracted {len(voters)} voters from {pdf_file}")
        
        print(f"Total voters extracted: {len(all_voters)}")
        
        if all_voters:
            # Convert to DataFrame and save as CSV
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
            df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"Data saved to {output_file}")
            
            # Save a sample as JSON for verification
            sample_json = output_file.replace('.csv', '_sample.json')
            sample_data = df.head(5).to_dict('records')
            with open(sample_json, 'w', encoding='utf-8') as f:
                json.dump(sample_data, f, indent=2, ensure_ascii=False)
            print(f"Sample data saved to {sample_json}")
        
        return all_voters

def main():
    processor = VoterDataProcessor()
    
    # Folders containing PDF files
    folders = ['Supplementary', 'ULB']
    
    # Process all PDFs
    voters = processor.process_all_pdfs(folders, 'voter_data_processed.csv')
    
    print(f"Processing completed! Total records: {len(voters)}")

if __name__ == "__main__":
    main()
