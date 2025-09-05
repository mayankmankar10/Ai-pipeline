import pdfplumber
import os

def analyze_pdf(pdf_path):
    """Analyze a PDF file to understand its structure and content."""
    print(f"Analyzing: {pdf_path}")
    print("=" * 50)
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            print(f"Number of pages: {len(pdf.pages)}")
            
            # Analyze first few pages
            for page_num, page in enumerate(pdf.pages[:2]):  # Only first 2 pages
                print(f"\n--- Page {page_num + 1} ---")
                text = page.extract_text()
                if text:
                    lines = text.split('\n')
                    print(f"Number of lines: {len(lines)}")
                    print("First 20 lines:")
                    for i, line in enumerate(lines[:20]):
                        print(f"{i+1:2d}: {line.strip()}")
                    
                    # Check for tables
                    tables = page.extract_tables()
                    if tables:
                        print(f"\nFound {len(tables)} table(s)")
                        for i, table in enumerate(tables[:1]):  # First table only
                            print(f"Table {i+1} sample rows:")
                            for j, row in enumerate(table[:5]):  # First 5 rows
                                print(f"Row {j+1}: {row}")
                else:
                    print("No text found on this page")
    
    except Exception as e:
        print(f"Error analyzing PDF: {str(e)}")

# Analyze sample files from both folders
sample_files = [
    r".\Supplementary\SupplementaryOne_ULB_023_11_1.pdf",
    r".\ULB\ULB_023_11_1 (1).pdf"
]

for pdf_file in sample_files:
    if os.path.exists(pdf_file):
        analyze_pdf(pdf_file)
        print("\n" + "="*80 + "\n")
    else:
        print(f"File not found: {pdf_file}")
