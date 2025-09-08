"""
Main Pipeline Orchestrator

Coordinates all components of the Hindi PDF processing pipeline:
- Monitors Google Drive folders for new files
- Orchestrates PDF processing workflow
- Manages error handling and retry logic
- Provides status monitoring and reporting
- Handles cleanup and maintenance tasks
"""

import os
import time
import logging
import schedule
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
import threading
from contextlib import contextmanager

from .config import Config, get_config
from .drive_manager import GoogleDriveManager
from .pdf_processor import PDFProcessor
from .text_processor import HindiTextProcessor
from .csv_generator import CSVGenerator
from .file_tracker import FileTracker, ProcessingStatus

logger = logging.getLogger(__name__)

class PipelineStatus:
    """Tracks pipeline execution status and metrics."""
    
    def __init__(self):
        self.started_at = datetime.now()
        self.last_run = None
        self.runs_count = 0
        self.files_processed = 0
        self.files_failed = 0
        self.total_processing_time = 0.0
        self.errors = []
        self.is_running = False

class HindiPDFPipeline:
    """
    Main pipeline orchestrator for Hindi PDF processing.
    
    Coordinates all components to provide an automated end-to-end
    processing pipeline with monitoring, error handling, and retry logic.
    """
    
    def __init__(self, config: Optional[Config] = None):
        """
        Initialize the Hindi PDF processing pipeline.
        
        Args:
            config: Optional configuration instance (will create default if None)
        """
        # Initialize configuration and logging
        self.config = config or get_config()
        self.config.setup_logging()
        
        logger.info("Initializing Hindi PDF Processing Pipeline")
        
        # Initialize components
        self.drive_manager = GoogleDriveManager(self.config)
        self.pdf_processor = PDFProcessor(self.config)
        self.text_processor = HindiTextProcessor(self.config)
        self.csv_generator = CSVGenerator(self.config)
        self.file_tracker = FileTracker(self.config)
        
        # Pipeline status and control
        self.status = PipelineStatus()
        self._should_stop = False
        self._processing_lock = threading.Lock()
        
        # Temporary directories
        self.temp_dir = Path("temp_downloads")
        self.temp_dir.mkdir(exist_ok=True)
        
        logger.info("Pipeline initialization completed successfully")
    
    def process_single_file(self, file_id: str, file_metadata: Dict[str, Any]) -> bool:
        """
        Process a single PDF file through the complete pipeline.
        
        Args:
            file_id: Google Drive file ID
            file_metadata: File metadata from Google Drive
            
        Returns:
            True if processing was successful, False otherwise
        """
        filename = file_metadata.get('name', 'unknown.pdf')
        
        logger.info(f"Starting processing of file: {filename} (ID: {file_id})")
        
        # Check if file should be processed
        if not self.file_tracker.should_process_file(file_id):
            logger.info(f"Skipping file {filename} - already processed or exceeded max attempts")
            return True  # Return True because it's not an error
        
        # Mark processing as started
        self.file_tracker.add_or_update_file(
            file_id, 
            filename, 
            metadata={
                'size': file_metadata.get('size'),
                'modified_time': file_metadata.get('modifiedTime')
            }
        )
        self.file_tracker.mark_processing_started(file_id)
        
        try:
            # Step 1: Download file from Google Drive
            local_file_path = self.temp_dir / filename
            logger.info(f"Downloading file from Google Drive...")
            
            success = self.drive_manager.retry_operation(
                self.drive_manager.download_file,
                file_id,
                str(local_file_path)
            )
            
            if not success:
                raise Exception("Failed to download file from Google Drive")
            
            # Step 2: Extract text from PDF
            logger.info(f"Extracting text from PDF...")
            extracted_pages = self.pdf_processor.extract_text(
                str(local_file_path),
                method="hybrid"  # Use hybrid method for best results
            )
            
            if not extracted_pages:
                raise Exception("No text could be extracted from PDF")
            
            # Validate extraction quality
            validation = self.pdf_processor.validate_extracted_text(extracted_pages)
            if not validation['valid']:
                logger.warning(f"Text extraction quality low: {validation.get('reason', 'Unknown')}")
            
            # Step 3: Process Hindi text and extract entities
            logger.info(f"Processing Hindi text and extracting entities...")
            pages_text = [page.text for page in extracted_pages]
            structured_data_list = self.text_processor.batch_process_pages(pages_text)
            
            if not structured_data_list:
                raise Exception("Failed to process Hindi text")
            
            # Filter duplicates across all pages
            all_entities = []
            for data in structured_data_list:
                all_entities.extend(data.entities)
            
            unique_entities = self.text_processor.filter_duplicates(all_entities)
            
            # Update structured data with filtered entities
            # For simplicity, put all unique entities in the first page's data
            if structured_data_list and unique_entities:
                structured_data_list[0].entities = unique_entities
                for i in range(1, len(structured_data_list)):
                    structured_data_list[i].entities = []
            
            logger.info(f"Extracted {len(unique_entities)} unique entities")
            
            # Step 4: Generate CSV output
            logger.info(f"Generating CSV output...")
            
            # Create output filename
            output_filename = self.csv_generator.create_filename(filename, "processed")
            output_path = self.temp_dir / f"{output_filename}.csv"
            
            csv_path = self.csv_generator.generate_csv_with_pandas(
                structured_data_list,
                str(output_path),
                filename,
                include_metadata=True
            )
            
            if not csv_path or not os.path.exists(csv_path):
                raise Exception("Failed to generate CSV output")
            
            # Validate CSV output
            validation_results = self.csv_generator.validate_csv_output(csv_path)
            if not validation_results['valid']:
                logger.warning(f"CSV validation issues: {validation_results.get('errors', [])}")
            
            # Step 5: Upload results to Google Drive
            logger.info(f"Uploading results to Google Drive...")
            
            uploaded_file_id = self.drive_manager.retry_operation(
                self.drive_manager.upload_file,
                csv_path,
                self.config.output_folder_id,
                f"{output_filename}.csv"
            )
            
            if not uploaded_file_id:
                raise Exception("Failed to upload results to Google Drive")
            
            # Step 6: Mark as completed
            output_files = [csv_path]
            
            # Generate summary report if requested
            summary_path = self.temp_dir / f"{output_filename}_summary.csv"
            summary_csv_path = self.csv_generator.generate_summary_csv(
                structured_data_list,
                str(summary_path),
                filename
            )
            
            if summary_csv_path:
                # Upload summary to Google Drive
                summary_file_id = self.drive_manager.retry_operation(
                    self.drive_manager.upload_file,
                    summary_csv_path,
                    self.config.output_folder_id,
                    f"{output_filename}_summary.csv"
                )
                
                if summary_file_id:
                    output_files.append(summary_csv_path)
            
            self.file_tracker.mark_processing_completed(file_id, output_files)
            
            # Clean up temporary files
            self._cleanup_temp_files([str(local_file_path), csv_path, summary_csv_path])
            
            logger.info(f"Successfully processed file: {filename}")
            self.status.files_processed += 1
            
            return True
            
        except Exception as e:
            error_msg = f"Error processing file {filename}: {str(e)}"
            logger.error(error_msg)
            
            # Mark as failed
            self.file_tracker.mark_processing_failed(file_id, error_msg)
            self.status.files_failed += 1
            self.status.errors.append({
                'timestamp': datetime.now().isoformat(),
                'file_id': file_id,
                'filename': filename,
                'error': str(e)
            })
            
            # Clean up any temporary files
            local_file_path = self.temp_dir / filename
            if local_file_path.exists():
                try:
                    local_file_path.unlink()
                except:
                    pass
            
            return False
    
    def run_single_cycle(self) -> Dict[str, Any]:
        """
        Run a single processing cycle.
        
        Returns:
            Dictionary with cycle results and statistics
        """
        with self._processing_lock:
            if self.status.is_running:
                logger.warning("Processing cycle already running, skipping")
                return {'status': 'skipped', 'reason': 'already_running'}
            
            self.status.is_running = True
        
        try:
            cycle_start = datetime.now()
            logger.info("Starting processing cycle")
            
            # Reset stale in-progress records
            reset_count = self.file_tracker.reset_stale_in_progress()
            if reset_count > 0:
                logger.info(f"Reset {reset_count} stale in-progress records")
            
            # Get list of files from Google Drive
            try:
                files = self.drive_manager.retry_operation(
                    self.drive_manager.list_files_in_folder,
                    self.config.input_folder_id,
                    "pdf"
                )
            except Exception as e:
                error_msg = f"Failed to list files from Google Drive: {e}"
                logger.error(error_msg)
                self.status.errors.append({
                    'timestamp': datetime.now().isoformat(),
                    'error': error_msg
                })
                return {'status': 'error', 'message': error_msg}
            
            if not files:
                logger.info("No PDF files found in input folder")
                return {'status': 'success', 'files_processed': 0, 'message': 'No files to process'}
            
            logger.info(f"Found {len(files)} PDF files in input folder")
            
            # For testing: Process only the first file
            if files:
                logger.info("Processing only the first file for testing purposes...")
                files = files[:1]
            
            # Process each file
            processed_count = 0
            failed_count = 0
            skipped_count = 0
            
            for file_metadata in files:
                if self._should_stop:
                    logger.info("Stop requested, ending processing cycle")
                    break
                
                file_id = file_metadata['id']
                filename = file_metadata['name']
                
                try:
                    # Check if file should be processed
                    if not self.file_tracker.should_process_file(file_id):
                        skipped_count += 1
                        logger.debug(f"Skipping file {filename} (already processed or max attempts reached)")
                        continue
                    
                    # Process the file
                    success = self.process_single_file(file_id, file_metadata)
                    
                    if success:
                        processed_count += 1
                    else:
                        failed_count += 1
                        
                except Exception as e:
                    logger.error(f"Unexpected error processing file {filename}: {e}")
                    failed_count += 1
                    self.status.errors.append({
                        'timestamp': datetime.now().isoformat(),
                        'file_id': file_id,
                        'filename': filename,
                        'error': f"Unexpected error: {str(e)}"
                    })
            
            # Update status
            cycle_time = (datetime.now() - cycle_start).total_seconds()
            self.status.last_run = cycle_start
            self.status.runs_count += 1
            self.status.total_processing_time += cycle_time
            
            logger.info(
                f"Processing cycle completed: {processed_count} processed, "
                f"{failed_count} failed, {skipped_count} skipped in {cycle_time:.2f}s"
            )
            
            return {
                'status': 'success',
                'processed': processed_count,
                'failed': failed_count,
                'skipped': skipped_count,
                'cycle_time_seconds': cycle_time,
                'total_files': len(files)
            }
            
        finally:
            self.status.is_running = False
    
    def run_continuous(self, polling_interval: Optional[int] = None) -> None:
        """
        Run the pipeline continuously with specified polling interval.
        
        Args:
            polling_interval: Seconds between polling cycles (uses config default if None)
        """
        interval = polling_interval or self.config.polling_interval_seconds
        
        logger.info(f"Starting continuous processing with {interval}s polling interval")
        logger.info("Press Ctrl+C to stop")
        
        # Schedule cleanup tasks
        schedule.every(24).hours.do(self._run_maintenance_tasks)
        
        try:
            while not self._should_stop:
                # Run processing cycle
                cycle_results = self.run_single_cycle()
                
                # Run scheduled tasks (cleanup, etc.)
                schedule.run_pending()
                
                # Log summary if there were any results
                if cycle_results.get('status') == 'success':
                    processed = cycle_results.get('processed', 0)
                    failed = cycle_results.get('failed', 0)
                    
                    if processed > 0 or failed > 0:
                        logger.info(f"Cycle summary: {processed} processed, {failed} failed")
                
                # Sleep until next cycle
                if not self._should_stop:
                    logger.debug(f"Sleeping for {interval} seconds until next cycle")
                    time.sleep(interval)
                    
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, stopping pipeline")
            self.stop()
        except Exception as e:
            logger.error(f"Unexpected error in continuous processing: {e}")
            raise
        
        logger.info("Continuous processing stopped")
    
    def run_scheduled(self) -> None:
        """
        Run the pipeline on a schedule using the schedule library.
        
        Configure scheduled runs in your calling code using the schedule library.
        """
        logger.info("Running scheduled processing cycle")
        
        try:
            cycle_results = self.run_single_cycle()
            
            # Log results
            if cycle_results.get('status') == 'success':
                processed = cycle_results.get('processed', 0)
                failed = cycle_results.get('failed', 0)
                logger.info(f"Scheduled run completed: {processed} processed, {failed} failed")
            else:
                logger.error(f"Scheduled run failed: {cycle_results.get('message', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Error in scheduled processing: {e}")
            raise
    
    def stop(self) -> None:
        """Stop the pipeline gracefully."""
        logger.info("Stopping pipeline...")
        self._should_stop = True
        
        # Wait for current processing to complete if running
        if self.status.is_running:
            logger.info("Waiting for current processing to complete...")
            while self.status.is_running:
                time.sleep(1)
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status and statistics.
        
        Returns:
            Dictionary with status information
        """
        # Get file tracker statistics
        tracker_stats = self.file_tracker.get_statistics()
        
        uptime = (datetime.now() - self.status.started_at).total_seconds()
        
        status_info = {
            'pipeline': {
                'started_at': self.status.started_at.isoformat(),
                'uptime_seconds': uptime,
                'last_run': self.status.last_run.isoformat() if self.status.last_run else None,
                'runs_count': self.status.runs_count,
                'is_running': self.status.is_running,
                'files_processed': self.status.files_processed,
                'files_failed': self.status.files_failed,
                'total_processing_time': self.status.total_processing_time,
                'avg_processing_time': (
                    self.status.total_processing_time / self.status.runs_count 
                    if self.status.runs_count > 0 else 0
                ),
                'recent_errors': self.status.errors[-10:] if self.status.errors else []
            },
            'file_tracker': tracker_stats,
            'configuration': {
                'input_folder_id': self.config.input_folder_id,
                'output_folder_id': self.config.output_folder_id,
                'polling_interval_seconds': self.config.polling_interval_seconds,
                'max_retries': self.config.max_retries,
                'ocr_language': self.config.ocr_language
            }
        }
        
        return status_info
    
    def _cleanup_temp_files(self, file_paths: List[str]) -> None:
        """Clean up temporary files."""
        for file_path in file_paths:
            try:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
                    logger.debug(f"Cleaned up temporary file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary file {file_path}: {e}")
    
    def _run_maintenance_tasks(self) -> None:
        """Run periodic maintenance tasks."""
        logger.info("Running maintenance tasks...")
        
        try:
            # Clean up old file records
            cleaned_records = self.file_tracker.cleanup_old_records(max_age_days=90)
            if cleaned_records > 0:
                logger.info(f"Cleaned up {cleaned_records} old file records")
            
            # Clean up old temporary files
            self.drive_manager.cleanup_temp_files(str(self.temp_dir), max_age_hours=24)
            
            # Reset error list if it's getting too long
            if len(self.status.errors) > 100:
                self.status.errors = self.status.errors[-50:]
                logger.info("Trimmed error history")
                
        except Exception as e:
            logger.error(f"Error during maintenance tasks: {e}")
    
    @contextmanager
    def processing_context(self, file_id: str):
        """Context manager for tracking file processing."""
        self.file_tracker.mark_processing_started(file_id)
        try:
            yield
        except Exception as e:
            self.file_tracker.mark_processing_failed(file_id, str(e))
            raise
        else:
            # Success case handled in process_single_file
            pass
    
    def process_file_by_id(self, file_id: str) -> bool:
        """
        Process a specific file by Google Drive ID.
        
        Args:
            file_id: Google Drive file ID
            
        Returns:
            True if processing was successful
        """
        try:
            # Get file metadata
            metadata = self.drive_manager.get_file_metadata(file_id)
            if not metadata:
                logger.error(f"Could not get metadata for file ID: {file_id}")
                return False
            
            return self.process_single_file(file_id, metadata)
            
        except Exception as e:
            logger.error(f"Error processing file by ID {file_id}: {e}")
            return False
    
    def reprocess_failed_files(self, max_attempts: int = None) -> Dict[str, Any]:
        """
        Reprocess files that previously failed.
        
        Args:
            max_attempts: Maximum attempts before giving up (uses config default if None)
            
        Returns:
            Dictionary with reprocessing results
        """
        max_attempts = max_attempts or self.config.max_retries
        
        logger.info("Starting reprocessing of failed files")
        
        failed_files = self.file_tracker.get_failed_files(max_attempts)
        
        if not failed_files:
            logger.info("No failed files found that can be retried")
            return {'status': 'success', 'message': 'No files to reprocess'}
        
        logger.info(f"Found {len(failed_files)} failed files to reprocess")
        
        results = {
            'status': 'success',
            'total_files': len(failed_files),
            'processed': 0,
            'failed': 0,
            'skipped': 0
        }
        
        for record in failed_files:
            if self._should_stop:
                break
            
            try:
                # Get current file metadata from Google Drive
                metadata = self.drive_manager.get_file_metadata(record.file_id)
                if not metadata:
                    logger.warning(f"Could not get metadata for file {record.file_id}, skipping")
                    results['skipped'] += 1
                    continue
                
                success = self.process_single_file(record.file_id, metadata)
                
                if success:
                    results['processed'] += 1
                else:
                    results['failed'] += 1
                    
            except Exception as e:
                logger.error(f"Error reprocessing file {record.file_id}: {e}")
                results['failed'] += 1
        
        logger.info(
            f"Reprocessing completed: {results['processed']} processed, "
            f"{results['failed']} failed, {results['skipped']} skipped"
        )
        
        return results
    
    def export_processing_report(self, output_path: str) -> str:
        """
        Export a comprehensive processing report.
        
        Args:
            output_path: Path for the report file
            
        Returns:
            Path to generated report
        """
        logger.info(f"Generating processing report: {output_path}")
        
        try:
            # Get comprehensive statistics
            status = self.get_status()
            
            # Export file records
            records_path = output_path.replace('.json', '_records.json')
            self.file_tracker.export_records(records_path)
            
            # Create comprehensive report
            report = {
                'report_generated': datetime.now().isoformat(),
                'pipeline_status': status,
                'file_records_exported_to': records_path
            }
            
            # Write main report
            import json
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Processing report generated: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating processing report: {e}")
            raise
