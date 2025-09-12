# Hindi Voter PDF Data Extraction - Robust Version
# Overhauled parsing, cleaning, header extraction, and validation logic for maximum recall and accuracy

import pdfplumber
import pandas as pd
import os
import re
import json
import unicodedata
from pathlib import Path
from typing import List, Dict, Any, Optional

# Use Indic NLP Library for natural transliteration
from indicnlp import common
from indicnlp.transliterate.unicode_transliterate import UnicodeIndicTransliterator

try:
    common.set_resources_path("indic_nlp_resources")
except:
    pass

print("✅ Using Indic NLP Library (UnicodeIndicTransliterator) for natural transliteration")

class FinalVoterDataProcessor:
    def __init__(self):
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
        # Broken/compound character fixes
        self.broken_char_fixes = {
            'ा ु': 'ाु', 'क ा': 'का', 'र ा': 'रा', 'म ा': 'मा', 'न ा': 'ना', 'त ा': 'ता',
            'स ा': 'सा', 'ल ा': 'ला', 'प ा': 'पा', 'द ा': 'दा', 'ब ा': 'बा', 'ग ा': 'गा',
            'ज ा': 'जा', 'च ा': 'चा', 'व ा': 'वा', 'श ा': 'शा', 'ह ा': 'हा', 'य ा': 'या',
            'काु': 'काु', 'राजकाुमार': 'राजकुमार', 'राजक ाुमार': 'राजकुमार',
            'चयाम': 'श्याम', 'प्रक ाा': 'प्रकाश', 'प्रकााश': 'प्रकाश',
            'जयप्रक ाा': 'जयप्रकाश', 'जयप्रकााश': 'जयप्रकाश', 'मक ा': 'मका',
            'कउन': 'कुन', 'कुनता': 'कुन्ता', 'बागू': 'बाबू', 'कशचचयन': 'कृष्णा',
            'ठाक ाुर': 'ठाकुर', 'ठाकाुर': 'ठाकुर', 'मुक ाुल': 'मुकुल',
            'मुकाुल': 'मुकुल', 'विनित': 'विनीत', 'सिसं': 'सिंह', 'सिलं': 'सिंह',
            'श ्याम': 'श्याम', 'ग िता': 'गीता'
        }
        # Name indicators for splitting
        self.father_indicators = ['सिसंह', 'सिंह', 'कुमार', 'प्रसाद', 'लाल', 'चंद', 'देव', 'राम', 'शर्मा', 'गुप्ता', 'यादव', 'पटेल', 'वर्मा', 'अग्रवाल', 'शुक्ला', 'पांडे', 'मिश्रा', 'तिवारी', 'चौधरी', 'जैन', 'अग्निहोत्री', 'द्विवेदी', 'त्रिपाठी', 'उपाध्याय']

    def clean_cid_text(self, text):
        if not text:
            return ""
        cleaned = str(text)
        for cid, replacement in self.cid_map.items():
            cleaned = cleaned.replace(cid, replacement)
        cleaned = re.sub(r'\(cid:\d+)', '', cleaned)
        for broken, fixed in self.broken_char_fixes.items():
            cleaned = cleaned.replace(broken, fixed)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned

    def transliterate_name(self, hindi_name):
        if not hindi_name:
            return ""
        text = unicodedata.normalize('NFC', str(hindi_name))
        devanagari_only = re.sub(r'[^
0900-
097F\s]', ' ', text)
        devanagari_only = re.sub(r'\s+', ' ', devanagari_only).strip()
        if not devanagari_only:
            return ""
        try:
            itrans = UnicodeIndicTransliterator.transliterate(devanagari_only, "hi", "en")
        except Exception as e:
            print(f"Transliteration failed for '{devanagari_only}': {e}")
            itrans = devanagari_only
        s = itrans
        replacements = [
            ('aa', 'a'), ('ii', 'i'), ('ee', 'e'), ('oo', 'o'), ('uu', 'u'),
            ('~n', 'n'), ("'", ''), ('\.h', 'h'), ('\.n', 'n'), ('\.m', 'm'),
            ('\.t', 't'), ('\.d', 'd'), ('chh', 'chh'), ('~', ''),
            ('asha$', 'ash'), ('ata$', 'at'), ('ana$', 'an'),
        ]
        s_lower = s.lower()
        for a, b in replacements:
            s = re.sub(a, b, s)
        s = re.sub(r'([aeiou])\1+', r'\1', s)
        s = re.sub(r'[^A-Za-z\-\s]', ' ', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return s.title() if s else ""

    def extract_header_info(self, page_text):
        # Use regex and Hindi keywords to find headers
        info = {'district': '', 'bodyNumber': '', 'ward': '', 'pollingCenter': '', 'partNumber': '', 'roomNumber': '', 'sectionNumber': '', 'locality': ''}
        # Example header patterns:
        match = re.search(r'(\d{2,3}[- ]?[A-Za-zअ-ह]+)', page_text)
        if match:
            info['district'] = match.group(1)
        match = re.search(r'(\d+-[A-Za-zअ-ह]+)', page_text)
        if match:
            info['bodyNumber'] = match.group(1)
        match = re.search(r'(वार्ड[: ]?(\d+-?[A-Za-zअ-ह]+))', page_text)
        if match:
            info['ward'] = match.group(2)
        match = re.search(r'(केंद्र[: ]?([A-Za-zअ-ह ]+))', page_text)
        if match:
            info['pollingCenter'] = match.group(2)
        match = re.search(r'(अनुभाग[: ]?(\d+))', page_text)
        if match:
            info['sectionNumber'] = match.group(2)
        match = re.search(r'(कक्ष[: ]?(\d+))', page_text)
        if match:
            info['roomNumber'] = match.group(2)
        match = re.search(r'(भाग[: ]?(\d+))', page_text)
        if match:
            info['partNumber'] = match.group(2)
        # Locality
        match = re.search(r'(नगर[: ]?([A-Za-zअ-ह ]+))', page_text)
        if match:
            info['locality'] = match.group(2)
        # Fallbacks
        for k in info:
            if not info[k]:
                info[k] = f"UNKNOWN-{k}"
        return info

    def parse_voter_data(self, text_data, header_info):
        if not text_data:
            return []
        voters = []
        lines = [line.strip() for line in text_data.split('\n') if line.strip()]
        for line in lines:
            voter_records = self.extract_voters_from_line(line, header_info)
            voters.extend(voter_records)
        return voters

    def extract_voters_from_line(self, line, header_info):
        voters = []
        if not line or len(line) < 10:
            return voters
        line = self.clean_cid_text(line)
        # Sliding window pattern: find sequences of (Serial, HouseNo?, Name, Relative, Gender, Age)
        pattern = r'(\d{1,4})\s+([A-Za-zअ-ह0-9]{1,8})?\s*([अ-हA-Za-z ]{2,})\s+([अ-हA-Za-z ]{2,})\s+(पु|म|स्त्री|पुरुष|फ)\s+(\d{1,3})'
        matches = re.finditer(pattern, line)
        for match in matches:
            sr_no = match.group(1)
            house_no = match.group(2) if match.group(2) else ""
            name = match.group(3).strip()
            father = match.group(4).strip()
            gender = match.group(5)
            age = match.group(6)
            voter_record = self.build_voter_record(sr_no, house_no, name, father, gender, age, header_info)
            if voter_record:
                voters.append(voter_record)
        # Fallback: try less strict pattern if no matches
        if not voters:
            fallback = re.findall(r'(\d{1,4})\s+([अ-हA-Za-z ]{2,})\s+(पु|म|स्त्री|पुरुष|फ)\s+(\d{1,3})', line)
            for parts in fallback:
                sr_no, middle, gender, age = parts
                middle = self.clean_cid_text(middle)
                name, father = self.smart_split_names(middle)
                voter_record = self.build_voter_record(sr_no, "", name, father, gender, age, header_info)
                if voter_record:
                    voters.append(voter_record)
        return voters

    def smart_split_names(self, middle):
        words = middle.split()
        if len(words) < 2:
            return middle, ""
        split_idx = 1
        for i, word in enumerate(words):
            if any(ind in word for ind in self.father_indicators):
                split_idx = i
                break
        name = ' '.join(words[:split_idx]).strip()
        father = ' '.join(words[split_idx:]).strip()
        return name, father

    def build_voter_record(self, sr_no, house_no, name, father, gender, age, header_info):
        try:
            age_int = int(age)
            if age_int < 18 or age_int > 120:
                return None
            voter_name_hindi = self.clean_and_validate_name(name)
            father_name_hindi = self.clean_and_validate_name(father)
            if not voter_name_hindi or len(voter_name_hindi) < 2:
                return None
            voter_name_english = self.transliterate_name(voter_name_hindi)
            voter_name_lower = voter_name_english.lower() if voter_name_english else ""
            father_name_english = self.transliterate_name(father_name_hindi) if father_name_hindi else ""
            father_name_lower = father_name_english.lower() if father_name_english else ""
            gender_code = 'M' if gender in ['पु', 'पुरुष'] else 'F'
            record = {
                'age': age_int,
                'bodyNumber': header_info.get('bodyNumber',''),
                'district': header_info.get('district',''),
                'fatherOrHusbandName': father_name_english,
                'fatherOrHusbandNameHindi': father_name_hindi,
                'fatherOrHusbandNameLower': father_name_lower,
                'gender': gender_code,
                'houseNo': house_no,
                'locality': header_info.get('locality',''),
                'partNumber': header_info.get('partNumber',''),
                'pollingCenter': header_info.get('pollingCenter',''),
                'roomNumber': header_info.get('roomNumber',''),
                'sectionNumber': header_info.get('sectionNumber',''),
                'srNo': sr_no,
                'voterName': voter_name_english,
                'voterNameHindi': voter_name_hindi,
                'voterNameLower': voter_name_lower,
                'ward': header_info.get('ward','')
            }
            return record
        except Exception as e:
            print(f"Error building voter record: {e}")
            return None

    def clean_and_validate_name(self, name):
        if not name:
            return ""
        name = re.sub(r'[^
0900-
097F\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        non_name_words = {'पुत्र', 'पत्नी', 'पति', 'स/ो', 'डब्ल्यू/ओ', 'पिता', 'का', 'की', 'के'}
        words = name.split()
        filtered_words = [word for word in words if word not in non_name_words]
        return ' '.join(filtered_words).strip()

    def process_pdf_file(self, pdf_path, source_file_name):
        print(f"Processing: {pdf_path}")
        all_voters = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    print(f"  Processing page {page_num + 1}/{len(pdf.pages)}")
                    text = page.extract_text()
                    if not text:
                        continue
                    cleaned_text = self.clean_cid_text(text)
                    header_info = self.extract_header_info(cleaned_text)
                    voters = self.parse_voter_data(cleaned_text, header_info)
                    for voter in voters:
                        voter['sourceFile'] = source_file_name
                        voter['nagarNigam'] = '1'
                    all_voters.extend(voters)
        except Exception as e:
            print(f"Error processing {pdf_path}: {str(e)}")
        # Remove duplicates
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
        pdf_file = Path(pdf_path)
        source_file_name = pdf_file.name
        voters = self.process_pdf_file(pdf_path, source_file_name)
        if voters:
            output_filename = pdf_file.stem + '_processed.csv'
            output_path = Path(output_dir) / output_filename
            df = pd.DataFrame(voters)
            column_order = [
                'age', 'bodyNumber', 'district', 'fatherOrHusbandName', 'fatherOrHusbandNameHindi',
                'fatherOrHusbandNameLower', 'gender', 'houseNo', 'locality', 'partNumber', 'pollingCenter',
                'roomNumber', 'sectionNumber', 'srNo', 'voterName', 'voterNameHindi', 'voterNameLower', 'ward',
                'sourceFile', 'nagarNigam'
            ]
            for col in column_order:
                if col not in df.columns:
                    df[col] = ''
            df = df[column_order]
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"✅ Saved {len(voters)} voters to: {output_path}")
            return output_path, len(voters)
        else:
            print(f"❌ No voters extracted from {source_file_name}")
            return None, 0

def main():
    processor = FinalVoterDataProcessor()
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
    test_files = []
    ulb_folder = r'.\ULB'
    if os.path.exists(ulb_folder):
        ulb_files = [f for f in os.listdir(ulb_folder) if f.lower().endswith('.pdf')]
        if ulb_files:
            test_files.append({
                'path': os.path.join(ulb_folder, ulb_files[0]),
                'output_dir': r'.\ULB_processed'
            })
    supp_folder = r'.\Supplementary'
    if os.path.exists(supp_folder):
        supp_files = [f for f in os.listdir(supp_folder) if f.lower().endswith('.pdf')]
        if supp_files:
            test_files.append({
                'path': os.path.join(supp_folder, supp_files[0]),
                'output_dir': r'.\Supplementary_processed'
            })
    print("🚀 Hindi PDF Voter Data Processing - Robust Version")
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
    print(f"\n🎉 Processing completed!\n📊 Total voters processed: {total_processed}")
    if total_processed > 0:
        print(f"\n📁 Output files saved to:\n   - ULB_processed/ (for ULB files)\n   - Supplementary_processed/ (for Supplementary files)")

if __name__ == "__main__":
    main()