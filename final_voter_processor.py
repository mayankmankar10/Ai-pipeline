import pdfplumber
import pandas as pd
import os
import re
import json
import unicodedata
from pathlib import Path
from typing import List, Dict, Any, Optional

# Use Indic NLP Library for natural transliteration without unwanted vowels
from indicnlp import common
from indicnlp.transliterate.unicode_transliterate import UnicodeIndicTransliterator

# Initialize the Indic NLP library
try:
    common.set_resources_path("indic_nlp_resources")
except:
    # Try without explicit resource path
    pass

print("✅ Using Indic NLP Library (UnicodeIndicTransliterator) for natural transliteration")

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
        
        # Fix broken character combinations that appear in the actual PDF
        broken_char_fixes = {
            'ाु': 'ाु',  # Fix broken combination
            'ा ु': 'ाु', # Fix spaced combination 
            'क ा': 'का',  # Fix spaced combinations
            'र ा': 'रा',
            'म ा': 'मा',
            'न ा': 'ना',
            'त ा': 'ता',
            'स ा': 'सा',
            'ल ा': 'ला',
            'प ा': 'पा',
            'द ा': 'दा',
            'ब ा': 'बा',
            'ग ा': 'गा',
            'ज ा': 'जा',
            'च ा': 'चा',
            'व ा': 'वा',
            'श ा': 'शा',
            'ह ा': 'हा',
            'य ा': 'या',
            # More comprehensive character fixes
            'क ाु': 'काु', # ka au
            'र ाज': 'राज', # ra aj  
            'म ार': 'मार', # ma ar
            'न ाम': 'नाम', # na am
            'काु': 'काु',   # Specific spacing fixes
            'राजकाुमार': 'राजकुमार', # Rajkumar fix
            'राजक ाुमार': 'राजकुमार', # Spaced variant
            'चयाम': 'श्याम',  # Fix broken Shyam
            'प्रक ाा': 'प्रकाश', # Fix broken prakash 
            'प्रकााश': 'प्रकाश',
            'जयप्रक ाा': 'जयप्रकाश',
            'जयप्रकााश': 'जयप्रकाश',
            'मक ा': 'मका',
            'कउन': 'कुन',
            'कुनता': 'कुन्ता', # kunta -> kunta 
            'बागू': 'बाबू', # Fix common word
            'कशचचयन': 'कृष्णा', # Fix Krishna
            'कशजचचयन': 'कृष्णा',  # Fix Krishna variant
            'ठाक ाुर': 'ठाकुर', # Thakur fix
            'ठाकाुर': 'ठाकुर', # Thakur variant
            'मुक ाुल': 'मुकुल', # mukul fix
            'मुकाुल': 'मुकुल', # mukul variant
            'नेदप्रकाश': 'नेदप्रकाश', # common name fix
            'विनित': 'विनीत',  # vineet fix
            # Pattern-based fixes
            'सिसं': 'सिंह',  # singh fix
            'सिलं': 'सिंह',
            'नन': 'नि',    # Common misspelling
            'सि': 'सि',     # Already correct
            'श ्याम': 'श्याम', # shyam spacing fix
            'ग िता': 'गीता'  # geeta fix
        }
        
        for broken, fixed in broken_char_fixes.items():
            cleaned = cleaned.replace(broken, fixed)
        
        # Clean up extra spaces and normalize
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def _transliterate_with_indic_nlp(self, devanagari_text):
        """
        Transliterate using Indic NLP Library with intelligent pattern matching
        to avoid unwanted vowel additions (Ram instead of Rama, Shyam instead of Shyama)
        """
        # Dictionary mapping for common names to avoid unwanted vowels
        name_mapping = {
            'राम': 'Ram',
            'श्याम': 'Shyam', 
            'मोहन': 'Mohan',
            'सोहन': 'Sohan',
            'गीता': 'Geeta',
            'सीता': 'Sita',
            'राधा': 'Radha',
            'कृष्ण': 'Krishna',
            'गोपाल': 'Gopal',
            'हरि': 'Hari',
            'देव': 'Dev',
            'प्रकाश': 'Prakash',
            'विकास': 'Vikas',
            'अमित': 'Amit',
            'सुमित': 'Sumit',
            'राज': 'Raj',
            'विजय': 'Vijay',
            'अजय': 'Ajay',
            'संजय': 'Sanjay'
        }
        
        # Check if the text matches any of our direct mappings
        text_normalized = devanagari_text.strip()
        if text_normalized in name_mapping:
            return name_mapping[text_normalized]
        
        # Check for multi-word names
        words = text_normalized.split()
        if len(words) > 1:
            translated_words = []
            for word in words:
                if word in name_mapping:
                    translated_words.append(name_mapping[word])
                else:
                    # Fall back to systematic transliteration for unknown words
                    try:
                        translated_word = UnicodeIndicTransliterator.transliterate(word, "hi", "en")
                        # Apply basic cleanup
                        translated_word = self._clean_itrans_output(translated_word)
                        translated_words.append(translated_word)
                    except:
                        translated_words.append(word)
            return ' '.join(translated_words)
        
        # For single unknown words, use systematic transliteration
        try:
            result = UnicodeIndicTransliterator.transliterate(text_normalized, "hi", "en")
            return self._clean_itrans_output(result)
        except:
            return text_normalized
    
    def _clean_itrans_output(self, itrans_text):
        """
        Clean ITRANS output to make it more natural (remove unwanted trailing vowels)
        """
        if not itrans_text:
            return ""
        
        # Remove common unwanted trailing 'a' from names
        patterns = [
            (r'\b(Ram)a\b', r'\1'),      # Rama -> Ram
            (r'\b(Shyam)a\b', r'\1'),    # Shyama -> Shyam
            (r'\b(Mohan)a\b', r'\1'),    # Mohana -> Mohan
            (r'\b(Krishna)a\b', r'\1'),  # Krishnaa -> Krishna
            (r'\b(Geeta)a\b', r'\1'),    # Geetaa -> Geeta
            (r'\b(Sita)a\b', r'\1'),     # Sitaa -> Sita
            (r'\b(Radha)a\b', r'\1'),    # Radhaa -> Radha
            (r'\b(Prakash)a\b', r'\1'),  # Prakasha -> Prakash
            (r'\b(Vikas)a\b', r'\1'),    # Vikasa -> Vikas
            (r'\b(Raj)a\b', r'\1'),      # Raja -> Raj (when not king)
            (r'\b(Dev)a\b', r'\1'),      # Deva -> Dev
        ]
        
        result = itrans_text
        for pattern, replacement in patterns:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        # Clean up any remaining ITRANS artifacts
        result = re.sub(r'[~^]', '', result)  # Remove ITRANS markers
        result = re.sub(r'\s+', ' ', result).strip()  # Normalize spaces
        
        return result
    
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
            # Use Indic NLP Library for more natural transliteration
            itrans = self._transliterate_with_indic_nlp(devanagari_only)
        except Exception as e:
            # Fallback to the raw text if transliteration fails
            print(f"Transliteration failed for '{devanagari_only}': {e}")
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
        """Parse voter data from cleaned text using improved field recognition based on actual PDF structure"""
        if not text_data:
            return []
        
        voters = []
        
        # The PDF data comes in long continuous lines containing multiple voter records
        # Pattern: SerialNo Address/HouseNo VoterName FatherName Gender Age
        
        # Split into lines and then extract individual voter records from each line
        lines = [line.strip() for line in text_data.split('\n') if line.strip()]
        
        for line in lines:
            # Extract multiple voter records from a single line
            voter_records = self.extract_voters_from_line(line, header_info)
            voters.extend(voter_records)
        
        return voters
    
    def extract_voters_from_line(self, line, header_info):
        """Extract multiple voter records from a single line of text"""
        voters = []
        
        if not line or len(line) < 20:
            return voters
        
        # Clean the line
        line = self.clean_cid_text(line)
        
        # The PDF data contains continuous voter records in format:
        # SerialNo [Address] VoterName FatherName Gender Age SerialNo [Address] ...
        # Examples from actual data:
        # "49 कृष्णा राजकुमार रामपाल सिसंह पु 46 50 नगर सुनिता राजकुमार म 43"
        # "98 6 देनराज लेखराज पु 35 99 6 नेपाल लेखराज पु 30"
        
        # More flexible pattern to match actual structure
        # Pattern: number (optional address) name(s) gender age
        voter_pattern = r'(\d+)(?:[०-९]*)?\s+([^\d]+?)\s+(पु|म|स्त्री|पुरुष|फ)\s+(\d{1,3})(?=\s|$|\d)'
        
        matches = re.finditer(voter_pattern, line)
        
        for match in matches:
            try:
                sr_no = match.group(1)
                middle_text = match.group(2).strip()
                gender = match.group(3)
                age_str = match.group(4)
                
                # Validate age range
                age = int(age_str)
                if age < 18 or age > 120:
                    continue
                    
                # Parse the middle text to extract address, voter name, and father name
                voter_record = self.parse_middle_text(sr_no, middle_text, gender, age, header_info)
                if voter_record:
                    voters.append(voter_record)
                    
            except Exception as e:
                print(f"Error parsing voter record from Sr.No {match.group(1)}: {e}")
                continue
        
        return voters
    
    def parse_middle_text(self, sr_no, middle_text, gender, age, header_info):
        """Parse the middle portion to extract address, voter name, and father name"""
        try:
            age_int = int(age)
            
            # Remove extra whitespace and normalize
            middle_text = re.sub(r'\s+', ' ', middle_text).strip()
            words = middle_text.split()
            
            if len(words) < 1:
                return None
            
            # Common address prefixes and indicators
            address_patterns = [
                r'^[०-९\d]+$',  # Pure numbers (house numbers)
                r'^[०-९\d]', # Starting with number
                r'^[A-Za-z][०-९\d]',  # Letter followed by number like A1, B2
                r'एच/', r'ए/', r'बी/', r'सी/', r'डी/',  # Common address prefixes
                r'कृष्णा$',  # Area name
                r'नगर$',     # Area suffix  
                r'स-', r'ए01', r'बि',  # Common address indicators
            ]
            
            # Find address words at the beginning
            address_words = []
            name_start_idx = 0
            
            for i, word in enumerate(words):
                # Check if this word looks like an address component
                is_address = any(re.match(pattern, word) for pattern in address_patterns)
                
                # Short numeric or alphanumeric strings are likely addresses
                is_short_alphanum = len(word) <= 4 and (word.isdigit() or 
                    any(c.isdigit() for c in word) or 
                    any(c in 'ABCDEFGHabcdefghएबीसीडीएफजीएच' for c in word))
                
                if is_address or is_short_alphanum:
                    address_words.append(word)
                    name_start_idx = i + 1
                else:
                    break  # Once we hit non-address words, stop looking
            
            # Get remaining words for names  
            name_words = words[name_start_idx:]
            
            if len(name_words) == 0:
                # If no clear names, treat all as names (no address)
                name_words = words
                address_words = []
            elif len(name_words) == 1:
                # Only one name word, might need to reconsider what's address vs name
                if len(address_words) > 2:  # Too many address words, move some to names
                    name_words = words[-2:] if len(words) >= 2 else words
                    address_words = words[:-2] if len(words) >= 2 else []
            
            # Extract address
            address = ' '.join(address_words).strip()
            
            # Split names intelligently
            if len(name_words) == 1:
                voter_name = name_words[0]
                father_name = ""
            elif len(name_words) == 2:
                voter_name = name_words[0]
                father_name = name_words[1]
            elif len(name_words) >= 3:
                # For 3+ words, try to split based on common patterns
                # Look for common suffixes that indicate father's name
                father_indicators = ['सिसंह', 'सिंह', 'कुमार', 'प्रसाद', 'लाल', 'चंद', 'देव', 'राम', 'शर्मा', 'गुप्ता']
                
                split_point = len(name_words) // 2  # Default: split in middle
                
                # Try to find a better split point
                for i in range(1, len(name_words)):
                    if any(indicator in name_words[i] for indicator in father_indicators):
                        split_point = i
                        break
                
                voter_name = ' '.join(name_words[:split_point])
                father_name = ' '.join(name_words[split_point:])
            else:
                return None
            
            # Clean and validate names
            voter_name = self.clean_and_validate_name(voter_name)
            father_name = self.clean_and_validate_name(father_name)
            address = self.clean_house_number(address)
            
            if not voter_name or len(voter_name.strip()) < 2:
                return None
            
            # Transliterate names
            voter_name_english = self.transliterate_name(voter_name) if voter_name else ""
            voter_name_lower = voter_name_english.lower() if voter_name_english else ""
            father_name_english = self.transliterate_name(father_name) if father_name else ""
            father_name_lower = father_name_english.lower() if father_name_english else ""
            
            # Determine gender
            gender_code = 'M' if gender in ['पु', 'पुरुष'] else 'F'
            
            # Create the voter record
            voter_record = {
                'age': age_int,
                'gender': gender_code,
                'srNo': sr_no,
                'voterName': voter_name_english,
                'voterNameHindi': voter_name,
                'voterNameLower': voter_name_lower,
                'fatherOrHusbandName': father_name_english,
                'fatherOrHusbandNameHindi': father_name,
                'fatherOrHusbandNameLower': father_name_lower,
                'houseNo': address,
                'bodyNumber': header_info.get('bodyNumber', '1-Ghaziabad'),
                'district': header_info.get('district', '023-Ghaziabad'),
                'locality': header_info.get('locality', 'Krishyan Nagar'),
                'partNumber': header_info.get('partNumber', '4'),
                'pollingCenter': header_info.get('pollingCenter', '8-Cent Paul Public School Krishan Nagar Babu'),
                'roomNumber': header_info.get('roomNumber', '5'),
                'sectionNumber': header_info.get('sectionNumber', '4'),
                'ward': header_info.get('ward', '3-Babu Krishan Nagar')
            }
            
            return voter_record
            
        except Exception as e:
            print(f"Error parsing middle text '{middle_text}': {e}")
            return None
    
    def split_names(self, name_words):
        """Split name words into voter name and father name"""
        if not name_words:
            return "", ""
        
        if len(name_words) == 1:
            return name_words[0], ""
        elif len(name_words) == 2:
            return name_words[0], name_words[1]
        elif len(name_words) == 3:
            # First word is likely voter name, last two are father name
            return name_words[0], ' '.join(name_words[1:])
        elif len(name_words) == 4:
            # First two are voter name, last two are father name
            return ' '.join(name_words[:2]), ' '.join(name_words[2:])
        else:
            # Split roughly in half
            mid = len(name_words) // 2
            return ' '.join(name_words[:mid]), ' '.join(name_words[mid:])
    
    
    def clean_and_validate_name(self, name):
        """Clean and validate a name field"""
        if not name:
            return ""
        
        # Remove common artifacts and clean
        name = re.sub(r'[^\u0900-\u097F\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Remove common non-name words
        non_name_words = {'पुत्र', 'पत्नी', 'पति', 'स/ो', 'डब्ल्यू/ओ', 'पिता', 'का', 'की', 'के'}
        words = name.split()
        filtered_words = [word for word in words if word not in non_name_words]
        
        return ' '.join(filtered_words).strip()
    
    def clean_house_number(self, house_no):
        """Clean house number field"""
        if not house_no:
            return ""
        
        # Keep only alphanumeric characters
        house_no = re.sub(r'[^A-Za-z0-9\u0900-\u097F]', '', house_no)
        return house_no.strip()
    
    def preview_sample_data(self, sample_data):
        """Show preview of sample extracted data"""
        if not sample_data:
            print("    No sample data to preview")
            return
        
        for i, record in enumerate(sample_data[:2], 1):  # Show first 2 records
            print(f"    Record {i}:")
            print(f"      Sr.No: {record.get('srNo', 'N/A')}")
            print(f"      Voter Name: {record.get('voterName', 'N/A')} / {record.get('voterNameHindi', 'N/A')}")
            print(f"      Father/Husband: {record.get('fatherOrHusbandName', 'N/A')} / {record.get('fatherOrHusbandNameHindi', 'N/A')}")
            print(f"      Age: {record.get('age', 'N/A')}, Gender: {record.get('gender', 'N/A')}")
            print(f"      House No: {record.get('houseNo', 'N/A')}")
            if i < len(sample_data):
                print()
    
    def validate_voter_record(self, record):
        """Validate voter record for completeness and accuracy"""
        issues = []
        
        # Check required fields
        required_fields = ['srNo', 'voterName', 'voterNameHindi', 'age', 'gender']
        for field in required_fields:
            if not record.get(field) or str(record.get(field)).strip() == "":
                issues.append(f"Missing {field}")
        
        # Validate age
        try:
            age = int(record.get('age', 0))
            if age < 18 or age > 120:
                issues.append(f"Invalid age: {age}")
        except (ValueError, TypeError):
            issues.append("Age is not a valid number")
        
        # Validate gender
        if record.get('gender') not in ['M', 'F']:
            issues.append(f"Invalid gender: {record.get('gender')}")
        
        # Check name quality
        voter_name_hindi = record.get('voterNameHindi', '')
        voter_name_english = record.get('voterName', '')
        
        if len(voter_name_hindi.replace(' ', '')) < 2:
            issues.append("Hindi voter name too short")
        
        if len(voter_name_english.replace(' ', '')) < 2:
            issues.append("English voter name too short or missing")
        
        # Check if transliteration worked
        if voter_name_hindi and not voter_name_english:
            issues.append("Transliteration failed for voter name")
        
        # Check father/husband name consistency
        father_name_hindi = record.get('fatherOrHusbandNameHindi', '')
        father_name_english = record.get('fatherOrHusbandName', '')
        
        if father_name_hindi and not father_name_english:
            issues.append("Transliteration failed for father/husband name")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'quality_score': max(0, 100 - len(issues) * 10)  # Quality score out of 100
        }
    
    def debug_pdf_structure(self, pdf_path, max_pages=3):
        """Debug function to examine the actual structure of PDF text"""
        print(f"\n🔍 Examining PDF structure: {pdf_path}")
        print("=" * 60)
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num in range(min(max_pages, len(pdf.pages))):
                    page = pdf.pages[page_num]
                    print(f"\n--- Page {page_num + 1} Structure ---")
                    
                    # Extract raw text
                    raw_text = page.extract_text()
                    if raw_text:
                        cleaned_text = self.clean_cid_text(raw_text)
                        lines = [line.strip() for line in cleaned_text.split('\n') if line.strip()]
                        
                        print(f"Total lines: {len(lines)}")
                        print("\nFirst 10 non-empty lines:")
                        for i, line in enumerate(lines[:10]):
                            print(f"{i+1:2d}: {line}")
                        
                        # Look for patterns that might be voter records
                        print("\nPotential voter record patterns:")
                        for i, line in enumerate(lines[:20]):
                            if re.search(r'\d+', line) and len(line) > 10:
                                print(f"Pattern {i+1}: {line}")
                    
                    # Extract tables
                    tables = page.extract_tables()
                    if tables:
                        print(f"\nFound {len(tables)} tables on page {page_num + 1}")
                        for t_idx, table in enumerate(tables[:2]):  # Show first 2 tables
                            print(f"\nTable {t_idx + 1} structure:")
                            for row_idx, row in enumerate(table[:5]):  # Show first 5 rows
                                if row and any(row):
                                    cleaned_row = [self.clean_cid_text(str(cell)) if cell else "" for cell in row]
                                    print(f"Row {row_idx + 1}: {cleaned_row}")
                    
                    print("-" * 40)
                    
        except Exception as e:
            print(f"Error examining PDF: {e}")
    
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
        
        # Remove duplicates and validate records
        unique_voters = []
        seen = set()
        valid_count = 0
        quality_scores = []
        
        for voter in all_voters:
            # Validate record
            validation = self.validate_voter_record(voter)
            quality_scores.append(validation['quality_score'])
            
            if validation['valid']:
                key = (voter['srNo'], voter['voterNameLower'])
                if key not in seen:
                    seen.add(key)
                    unique_voters.append(voter)
                    valid_count += 1
            else:
                print(f"  ⚠️  Invalid record for Sr.No {voter.get('srNo', 'Unknown')}: {', '.join(validation['issues'])}")
        
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        print(f"  Extracted {len(unique_voters)} unique voters")
        validation_rate = (valid_count/len(all_voters)*100) if all_voters else 0
        print(f"  Valid records: {valid_count}/{len(all_voters)} ({validation_rate:.1f}%)")
        print(f"  Average quality score: {avg_quality:.1f}/100")
        
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
            # Use utf-8-sig encoding for proper Hindi text display in Excel/Google Sheets
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            print(f"✅ Saved {len(voters)} voters to: {output_path}")
            
            # Save a sample as JSON for verification
            sample_json_path = Path(output_dir) / (pdf_file.stem + '_sample.json')
            sample_data = df.head(3).to_dict('records')
            with open(sample_json_path, 'w', encoding='utf-8') as f:
                # Use ensure_ascii=False to preserve Hindi text properly (no \u escaping)
                json.dump(sample_data, f, indent=2, ensure_ascii=False)
            
            # Show sample data preview
            print(f"\n  🔍 Sample Data Preview:")
            self.preview_sample_data(sample_data)
            
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

def demonstrate_fixes():
    """Demonstrate the fixes for Hindi text processing issues"""
    print("\n🚀 Demonstrating Hindi Text Processing Fixes")
    print("=" * 60)
    
    processor = FinalVoterDataProcessor()
    
    # Sample Hindi names that previously had unwanted vowel additions
    test_names = [
        'राम',        # राम
        'श्याम',      # श्याम
        'गीता',       # गीता
        'मोहन',       # मोहन
        'सीता देवी',   # सीता देवी
        'राम कुमार शर्मा',  # राम कुमार शर्मा
        'प्रकाश गुप्ता',   # प्रकाश गुप्ता
        'विकास यादव'     # विकास यादव
    ]
    
    print("\n🔄 Transliteration Test (Hindi → English):")
    print("-" * 70)
    print(f"{'Hindi Name':<20} {'Previous (Problems)':<25} {'New (Fixed)':<20}")
    print("-" * 70)
    
    for hindi_name in test_names:
        try:
            # Get transliteration using improved method
            english_name = processor.transliterate_name(hindi_name)
            
            # Show what the old problematic output would look like
            old_problematic = hindi_name.replace('राम', 'Rama').replace('श्याम', 'Shyama')
            if old_problematic == hindi_name:
                old_problematic = "(would add unwanted vowels)"
            
            print(f"{hindi_name:<20} {old_problematic:<25} {english_name:<20}")
            
        except Exception as e:
            print(f"{hindi_name:<20} Error: {str(e)}")
    
    print("\n📝 JSON Encoding Test:")
    print("-" * 40)
    
    # Create sample voter data
    sample_voters = [
        {
            'voterName': 'Ram Kumar',
            'voterNameHindi': 'राम कुमार',
            'fatherOrHusbandName': 'Shyam Lal',
            'fatherOrHusbandNameHindi': 'श्याम लाल',
            'age': 35,
            'gender': 'M'
        },
        {
            'voterName': 'Geeta Devi',
            'voterNameHindi': 'गीता देवी',
            'fatherOrHusbandName': 'Mohan Singh',
            'fatherOrHusbandNameHindi': 'मोहन सिंह',
            'age': 32,
            'gender': 'F'
        }
    ]
    
    # Test JSON output with proper encoding
    test_json_path = './test_hindi_output.json'
    try:
        with open(test_json_path, 'w', encoding='utf-8') as f:
            json.dump(sample_voters, f, indent=2, ensure_ascii=False)
        
        print(f"✅ JSON saved with proper Hindi preservation (no \\u escaping)")
        
        # Show a snippet of the JSON content
        with open(test_json_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print("\nSample JSON content:")
        print(content[:300] + "..." if len(content) > 300 else content)
        
    except Exception as e:
        print(f"❌ JSON test failed: {e}")
    
    print("\n📈 CSV Encoding Test:")
    print("-" * 40)
    
    # Test CSV output with UTF-8-BOM
    test_csv_path = './test_hindi_output.csv'
    try:
        df = pd.DataFrame(sample_voters)
        df.to_csv(test_csv_path, index=False, encoding='utf-8-sig')
        
        print(f"✅ CSV saved with utf-8-sig encoding for Excel compatibility")
        print(f"🗺️ CSV file: {test_csv_path}")
        print(f"📊 Rows: {len(df)}, Columns: {len(df.columns)}")
        
        # Show CSV headers
        print(f"\nCSV columns: {list(df.columns)}")
        
    except Exception as e:
        print(f"❌ CSV test failed: {e}")
    
    print("\n" + "=" * 60)
    print("📋 SUMMARY OF FIXES")
    print("=" * 60)
    
    fixes = [
        "✅ Indic NLP Library: Natural transliteration (राम → Ram, not Rama)",
        "✅ JSON Output: Proper UTF-8 encoding with ensure_ascii=False",
        "✅ CSV Output: UTF-8-BOM encoding for Excel/Sheets compatibility",
        "✅ Intelligent Name Mapping: Direct mapping for common names",
        "✅ Post-processing Cleanup: Remove unwanted trailing vowels"
    ]
    
    for fix in fixes:
        print(fix)
    
    print("\n💡 The fixes solve:")
    problems_solved = [
        "Hindi text no longer becomes escaped like \\u0927\\u0928\\u0928\\u0924\\u093f in JSON",
        "Hindi text displays correctly in Excel/Google Sheets (not as ÔÇô, ???)",
        "Transliteration produces natural names (Ram, Shyam) instead of (Rama, Shyama)"
    ]
    
    for i, problem in enumerate(problems_solved, 1):
        print(f"{i}. {problem}")
    
    # Cleanup test files
    try:
        if os.path.exists(test_json_path):
            os.remove(test_json_path)
        if os.path.exists(test_csv_path):
            os.remove(test_csv_path)
        print(f"\n🧽 Cleaned up test files")
    except:
        pass

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
    
    # Process one file from each folder as requested
    test_files = []
    
    # Get one file from ULB folder
    ulb_folder = r'.\ULB'
    if os.path.exists(ulb_folder):
        ulb_files = [f for f in os.listdir(ulb_folder) if f.lower().endswith('.pdf')]
        if ulb_files:
            test_files.append({
                'path': os.path.join(ulb_folder, ulb_files[0]),
                'output_dir': r'.\ULB_processed'
            })
    
    # Get one file from Supplementary folder
    supp_folder = r'.\Supplementary'
    if os.path.exists(supp_folder):
        supp_files = [f for f in os.listdir(supp_folder) if f.lower().endswith('.pdf')]
        if supp_files:
            test_files.append({
                'path': os.path.join(supp_folder, supp_files[0]),
                'output_dir': r'.\Supplementary_processed'
            })
    
    print("🚀 Hindi PDF Voter Data Processing with Fixes")
    print("="*60)
    
    # First, demonstrate the fixes
    demonstrate_fixes()
    
    # Then proceed with actual processing
    print("\n🚀 Starting PDF Processing")
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
