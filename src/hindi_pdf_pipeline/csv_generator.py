"""
CSV Generation Module

Handles conversion of structured data to clean CSV format including:
- Converting extracted entities to tabular format
- Handling Unicode text properly
- Customizable column mapping and formatting
- Data validation and cleaning
- Multiple output formats and encodings
"""

import csv
import os
import logging
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
from datetime import datetime
from dataclasses import asdict
import pandas as pd

from .config import Config
from .text_processor import ExtractedEntity, StructuredData

logger = logging.getLogger(__name__)

class CSVGenerator:
    """
    Generates CSV files from structured Hindi PDF data.
    
    Provides flexible CSV generation with customizable columns,
    proper Unicode handling, and data validation.
    """
    
    def __init__(self, config: Config):
        """
        Initialize CSV generator.
        
        Args:
            config: Configuration instance
        """
        self.config = config
        
        # Default column mappings
        self.default_columns = {
            'original_filename': 'Original Filename',
            'hindi_name': 'Hindi Name',
            'english_name': 'English Name',
            'english_name_lowercase': 'English Name (Lowercase)',
            'extraction_timestamp': 'Extraction Timestamp',
            'page_number': 'Page Number',
            'confidence_score': 'Confidence Score',
            'entity_type': 'Entity Type',
            'position_start': 'Position Start',
            'position_end': 'Position End'
        }
        
        # Column order for output
        self.column_order = [
            'original_filename',
            'page_number', 
            'hindi_name',
            'english_name',
            'english_name_lowercase',
            'entity_type',
            'confidence_score',
            'position_start',
            'position_end',
            'extraction_timestamp'
        ]
    
    def generate_csv_from_structured_data(self, 
                                        structured_data_list: List[StructuredData],
                                        output_path: str,
                                        filename: str = None,
                                        include_metadata: bool = True) -> str:
        """
        Generate CSV file from structured data.
        
        Args:
            structured_data_list: List of structured data from text processing
            output_path: Output file path
            filename: Original PDF filename for metadata
            include_metadata: Whether to include processing metadata
            
        Returns:
            Path to generated CSV file
        """
        if not structured_data_list:
            logger.warning("No structured data provided for CSV generation")
            return ""
        
        # Convert structured data to tabular format
        rows = self._convert_to_rows(structured_data_list, filename, include_metadata)
        
        if not rows:
            logger.warning("No data rows generated for CSV")
            return ""
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Generate CSV
        try:
            self._write_csv_file(rows, output_path)
            logger.info(f"CSV file generated successfully: {output_path}")
            logger.info(f"Generated CSV with {len(rows)} rows")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to generate CSV file: {e}")
            raise
    
    def _convert_to_rows(self, 
                        structured_data_list: List[StructuredData],
                        filename: Optional[str] = None,
                        include_metadata: bool = True) -> List[Dict[str, Any]]:
        """
        Convert structured data to CSV rows.
        
        Args:
            structured_data_list: List of structured data
            filename: Original filename for metadata
            include_metadata: Include processing metadata
            
        Returns:
            List of row dictionaries
        """
        rows = []
        
        for structured_data in structured_data_list:
            for entity in structured_data.entities:
                row = self._create_row_from_entity(
                    entity, 
                    structured_data, 
                    filename, 
                    include_metadata
                )
                if row:
                    rows.append(row)
        
        return rows
    
    def _create_row_from_entity(self, 
                               entity: ExtractedEntity,
                               structured_data: StructuredData,
                               filename: Optional[str],
                               include_metadata: bool) -> Dict[str, Any]:
        """
        Create CSV row from extracted entity.
        
        Args:
            entity: Extracted entity
            structured_data: Parent structured data
            filename: Original filename
            include_metadata: Include metadata
            
        Returns:
            Row dictionary
        """
        row = {
            'original_filename': filename or 'unknown',
            'hindi_name': entity.hindi_text,
            'english_name': entity.english_text,
            'english_name_lowercase': entity.english_lowercase,
            'extraction_timestamp': structured_data.extraction_timestamp.isoformat(),
            'page_number': entity.page_number,
            'confidence_score': round(entity.confidence, 3),
            'entity_type': entity.entity_type,
            'position_start': entity.position[0],
            'position_end': entity.position[1]
        }
        
        # Add metadata if requested
        if include_metadata:
            row['processing_method'] = structured_data.processing_method
            row['text_length'] = len(structured_data.cleaned_text)
        
        return row
    
    def _write_csv_file(self, rows: List[Dict[str, Any]], output_path: str) -> None:
        """
        Write rows to CSV file with proper encoding.
        
        Args:
            rows: List of row dictionaries
            output_path: Output file path
        """
        if not rows:
            return
        
        # Determine columns to include
        available_columns = set(rows[0].keys()) if rows else set()
        columns_to_write = [col for col in self.column_order if col in available_columns]
        
        # Add any extra columns not in standard order
        extra_columns = available_columns - set(self.column_order)
        columns_to_write.extend(sorted(extra_columns))
        
        # Write CSV file
        with open(output_path, 'w', newline='', encoding=self.config.csv_encoding) as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=columns_to_write,
                delimiter=self.config.csv_delimiter,
                quoting=csv.QUOTE_MINIMAL
            )
            
            # Write header
            header_row = {
                col: self.default_columns.get(col, col.replace('_', ' ').title())
                for col in columns_to_write
            }
            writer.writerow(header_row)
            
            # Write data rows
            for row in rows:
                # Filter row to only include columns we're writing
                filtered_row = {col: row.get(col, '') for col in columns_to_write}
                writer.writerow(filtered_row)
    
    def generate_csv_with_pandas(self,
                                structured_data_list: List[StructuredData],
                                output_path: str,
                                filename: str = None,
                                include_metadata: bool = True) -> str:
        """
        Generate CSV using pandas for more advanced formatting.
        
        Args:
            structured_data_list: List of structured data
            output_path: Output file path
            filename: Original PDF filename
            include_metadata: Include metadata
            
        Returns:
            Path to generated CSV file
        """
        if not structured_data_list:
            logger.warning("No structured data provided for CSV generation")
            return ""
        
        # Convert to DataFrame
        rows = self._convert_to_rows(structured_data_list, filename, include_metadata)
        
        if not rows:
            logger.warning("No data rows generated for CSV")
            return ""
        
        try:
            df = pd.DataFrame(rows)
            
            # Reorder columns if they exist
            available_columns = df.columns.tolist()
            ordered_columns = [col for col in self.column_order if col in available_columns]
            extra_columns = [col for col in available_columns if col not in self.column_order]
            final_columns = ordered_columns + sorted(extra_columns)
            
            df = df[final_columns]
            
            # Apply column name mapping
            column_mapping = {
                col: self.default_columns.get(col, col.replace('_', ' ').title())
                for col in df.columns
            }
            df = df.rename(columns=column_mapping)
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save to CSV
            df.to_csv(
                output_path,
                index=False,
                encoding=self.config.csv_encoding,
                sep=self.config.csv_delimiter
            )
            
            logger.info(f"CSV file generated with pandas: {output_path}")
            logger.info(f"Generated CSV with {len(df)} rows and {len(df.columns)} columns")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to generate CSV with pandas: {e}")
            raise
    
    def generate_summary_csv(self,
                           structured_data_list: List[StructuredData],
                           output_path: str,
                           filename: str = None) -> str:
        """
        Generate summary CSV with aggregated statistics.
        
        Args:
            structured_data_list: List of structured data
            output_path: Output file path
            filename: Original PDF filename
            
        Returns:
            Path to generated summary CSV
        """
        if not structured_data_list:
            return ""
        
        # Calculate summary statistics
        summary_data = self._calculate_summary_stats(structured_data_list, filename)
        
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Write summary CSV
            with open(output_path, 'w', newline='', encoding=self.config.csv_encoding) as csvfile:
                writer = csv.writer(csvfile, delimiter=self.config.csv_delimiter)
                
                # Write header
                writer.writerow(['Metric', 'Value'])
                
                # Write summary data
                for key, value in summary_data.items():
                    writer.writerow([key.replace('_', ' ').title(), value])
            
            logger.info(f"Summary CSV generated: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to generate summary CSV: {e}")
            raise
    
    def _calculate_summary_stats(self, 
                               structured_data_list: List[StructuredData],
                               filename: Optional[str]) -> Dict[str, Any]:
        """
        Calculate summary statistics for structured data.
        
        Args:
            structured_data_list: List of structured data
            filename: Original filename
            
        Returns:
            Dictionary of summary statistics
        """
        total_pages = len(structured_data_list)
        total_entities = sum(len(data.entities) for data in structured_data_list)
        
        # Count entities by type
        entity_counts = {}
        all_entities = []
        for data in structured_data_list:
            all_entities.extend(data.entities)
        
        for entity in all_entities:
            entity_type = entity.entity_type
            entity_counts[entity_type] = entity_counts.get(entity_type, 0) + 1
        
        # Calculate confidence statistics
        confidences = [entity.confidence for entity in all_entities]
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0
        min_confidence = min(confidences) if confidences else 0
        max_confidence = max(confidences) if confidences else 0
        
        # Pages with entities
        pages_with_entities = sum(1 for data in structured_data_list if data.entities)
        
        # Processing timestamps
        timestamps = [data.extraction_timestamp for data in structured_data_list]
        earliest_processing = min(timestamps) if timestamps else None
        latest_processing = max(timestamps) if timestamps else None
        
        summary = {
            'filename': filename or 'unknown',
            'total_pages': total_pages,
            'pages_with_entities': pages_with_entities,
            'total_entities': total_entities,
            'average_confidence': round(avg_confidence, 3),
            'min_confidence': round(min_confidence, 3),
            'max_confidence': round(max_confidence, 3),
            'earliest_processing': earliest_processing.isoformat() if earliest_processing else '',
            'latest_processing': latest_processing.isoformat() if latest_processing else ''
        }
        
        # Add entity type counts
        for entity_type, count in entity_counts.items():
            summary[f'{entity_type}_count'] = count
        
        return summary
    
    def validate_csv_output(self, csv_path: str) -> Dict[str, Any]:
        """
        Validate generated CSV file.
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Validation results dictionary
        """
        validation_results = {
            'valid': False,
            'file_exists': False,
            'file_size': 0,
            'row_count': 0,
            'column_count': 0,
            'encoding_valid': False,
            'errors': []
        }
        
        try:
            # Check if file exists
            if not os.path.exists(csv_path):
                validation_results['errors'].append('File does not exist')
                return validation_results
            
            validation_results['file_exists'] = True
            validation_results['file_size'] = os.path.getsize(csv_path)
            
            # Try to read the CSV
            try:
                df = pd.read_csv(csv_path, encoding=self.config.csv_encoding)
                validation_results['row_count'] = len(df)
                validation_results['column_count'] = len(df.columns)
                validation_results['encoding_valid'] = True
                
                # Check for required columns
                required_columns = ['Hindi Name', 'English Name', 'Page Number']
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    validation_results['errors'].append(f'Missing columns: {missing_columns}')
                
                # Check for empty data
                if len(df) == 0:
                    validation_results['errors'].append('CSV is empty')
                
                # Check for Unicode content
                hindi_columns = ['Hindi Name']
                for col in hindi_columns:
                    if col in df.columns:
                        hindi_count = df[col].astype(str).str.contains(r'[\u0900-\u097F]', na=False).sum()
                        if hindi_count == 0:
                            validation_results['errors'].append(f'No Hindi text found in {col}')
                
                validation_results['valid'] = len(validation_results['errors']) == 0
                
            except UnicodeDecodeError:
                validation_results['errors'].append('Encoding error - file may not be properly encoded')
            except Exception as e:
                validation_results['errors'].append(f'Error reading CSV: {str(e)}')
            
        except Exception as e:
            validation_results['errors'].append(f'Validation error: {str(e)}')
        
        return validation_results
    
    def generate_multiple_formats(self,
                                structured_data_list: List[StructuredData],
                                output_dir: str,
                                base_filename: str,
                                formats: List[str] = None) -> Dict[str, str]:
        """
        Generate output in multiple formats.
        
        Args:
            structured_data_list: List of structured data
            output_dir: Output directory
            base_filename: Base filename (without extension)
            formats: List of formats to generate ['csv', 'excel', 'json']
            
        Returns:
            Dictionary mapping format to output path
        """
        if formats is None:
            formats = ['csv', 'excel', 'json']
        
        output_paths = {}
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate CSV
        if 'csv' in formats:
            csv_path = os.path.join(output_dir, f"{base_filename}.csv")
            try:
                self.generate_csv_with_pandas(structured_data_list, csv_path, base_filename)
                output_paths['csv'] = csv_path
            except Exception as e:
                logger.error(f"Failed to generate CSV: {e}")
        
        # Generate Excel
        if 'excel' in formats:
            excel_path = os.path.join(output_dir, f"{base_filename}.xlsx")
            try:
                self._generate_excel(structured_data_list, excel_path, base_filename)
                output_paths['excel'] = excel_path
            except Exception as e:
                logger.error(f"Failed to generate Excel: {e}")
        
        # Generate JSON
        if 'json' in formats:
            json_path = os.path.join(output_dir, f"{base_filename}.json")
            try:
                self._generate_json(structured_data_list, json_path, base_filename)
                output_paths['json'] = json_path
            except Exception as e:
                logger.error(f"Failed to generate JSON: {e}")
        
        return output_paths
    
    def _generate_excel(self,
                       structured_data_list: List[StructuredData],
                       output_path: str,
                       filename: str) -> None:
        """Generate Excel file from structured data."""
        rows = self._convert_to_rows(structured_data_list, filename, True)
        
        if not rows:
            return
        
        df = pd.DataFrame(rows)
        
        # Reorder columns
        available_columns = df.columns.tolist()
        ordered_columns = [col for col in self.column_order if col in available_columns]
        extra_columns = [col for col in available_columns if col not in self.column_order]
        final_columns = ordered_columns + sorted(extra_columns)
        df = df[final_columns]
        
        # Apply column name mapping
        column_mapping = {
            col: self.default_columns.get(col, col.replace('_', ' ').title())
            for col in df.columns
        }
        df = df.rename(columns=column_mapping)
        
        # Save to Excel
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Extracted Data', index=False)
            
            # Add summary sheet
            summary = self._calculate_summary_stats(structured_data_list, filename)
            summary_df = pd.DataFrame(list(summary.items()), columns=['Metric', 'Value'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        logger.info(f"Excel file generated: {output_path}")
    
    def _generate_json(self,
                      structured_data_list: List[StructuredData],
                      output_path: str,
                      filename: str) -> None:
        """Generate JSON file from structured data."""
        import json
        
        # Convert structured data to JSON-serializable format
        json_data = {
            'filename': filename,
            'processing_timestamp': datetime.now().isoformat(),
            'pages': []
        }
        
        for data in structured_data_list:
            page_data = {
                'page_number': data.page_number,
                'processing_method': data.processing_method,
                'extraction_timestamp': data.extraction_timestamp.isoformat(),
                'raw_text_length': len(data.raw_text),
                'cleaned_text_length': len(data.cleaned_text),
                'entities': []
            }
            
            for entity in data.entities:
                entity_data = {
                    'hindi_text': entity.hindi_text,
                    'english_text': entity.english_text,
                    'english_lowercase': entity.english_lowercase,
                    'entity_type': entity.entity_type,
                    'confidence': entity.confidence,
                    'position': entity.position
                }
                page_data['entities'].append(entity_data)
            
            json_data['pages'].append(page_data)
        
        # Save to JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"JSON file generated: {output_path}")
    
    def create_filename(self, 
                       original_filename: str,
                       suffix: str = "processed",
                       timestamp: bool = True) -> str:
        """
        Create output filename based on original filename.
        
        Args:
            original_filename: Original PDF filename
            suffix: Suffix to add
            timestamp: Whether to include timestamp
            
        Returns:
            Generated filename
        """
        base_name = Path(original_filename).stem
        
        parts = [base_name, suffix]
        
        if timestamp:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            parts.append(timestamp_str)
        
        return "_".join(parts)
