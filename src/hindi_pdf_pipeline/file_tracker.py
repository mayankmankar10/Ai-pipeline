"""
File Tracking Module

Handles file tracking and deduplication to avoid reprocessing files:
- Tracks processed files using IDs and hashes
- Persistent storage of processing history
- Duplicate detection and prevention
- Processing status management
- Cleanup of old records
"""

import os
import json
import hashlib
import logging
from typing import Dict, List, Optional, Set, Any
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
from enum import Enum

from .config import Config

logger = logging.getLogger(__name__)

class ProcessingStatus(Enum):
    """Enum for file processing status."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress" 
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class FileRecord:
    """Represents a tracked file record."""
    file_id: str
    filename: str
    file_hash: str
    file_size: int
    processing_status: ProcessingStatus
    first_seen: datetime
    last_processed: Optional[datetime]
    processing_attempts: int
    error_message: Optional[str]
    output_files: List[str]
    metadata: Dict[str, Any]

class FileTracker:
    """
    Tracks processed files to avoid reprocessing and manage pipeline state.
    
    Maintains a persistent database of file processing history including
    file hashes, processing status, and output file locations.
    """
    
    def __init__(self, config: Config):
        """
        Initialize file tracker.
        
        Args:
            config: Configuration instance
        """
        self.config = config
        self.db_path = Path(config.tracking_db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # In-memory cache of records for performance
        self._records: Dict[str, FileRecord] = {}
        self._loaded = False
        
        # Load existing records
        self.load_records()
    
    def load_records(self) -> None:
        """Load file records from persistent storage."""
        try:
            if self.db_path.exists():
                with open(self.db_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Convert JSON data back to FileRecord objects
                for file_id, record_data in data.items():
                    # Convert datetime strings back to datetime objects
                    record_data['first_seen'] = datetime.fromisoformat(record_data['first_seen'])
                    if record_data['last_processed']:
                        record_data['last_processed'] = datetime.fromisoformat(record_data['last_processed'])
                    
                    # Convert status string back to enum
                    record_data['processing_status'] = ProcessingStatus(record_data['processing_status'])
                    
                    self._records[file_id] = FileRecord(**record_data)
                
                logger.info(f"Loaded {len(self._records)} file records from {self.db_path}")
            else:
                logger.info("No existing file tracking database found, starting fresh")
                
            self._loaded = True
            
        except Exception as e:
            logger.error(f"Error loading file records: {e}")
            # Start with empty records if loading fails
            self._records = {}
            self._loaded = True
    
    def save_records(self) -> None:
        """Save file records to persistent storage."""
        try:
            # Convert FileRecord objects to JSON-serializable format
            data = {}
            for file_id, record in self._records.items():
                record_dict = asdict(record)
                
                # Convert datetime objects to ISO format strings
                record_dict['first_seen'] = record.first_seen.isoformat()
                if record.last_processed:
                    record_dict['last_processed'] = record.last_processed.isoformat()
                else:
                    record_dict['last_processed'] = None
                
                # Convert enum to string
                record_dict['processing_status'] = record.processing_status.value
                
                data[file_id] = record_dict
            
            # Write to file
            with open(self.db_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Saved {len(self._records)} file records to {self.db_path}")
            
        except Exception as e:
            logger.error(f"Error saving file records: {e}")
    
    def compute_file_hash(self, file_path: str) -> str:
        """
        Compute MD5 hash of a file.
        
        Args:
            file_path: Path to file
            
        Returns:
            MD5 hash string
        """
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error computing hash for {file_path}: {e}")
            return ""
    
    def is_file_processed(self, file_id: str, file_path: Optional[str] = None) -> bool:
        """
        Check if a file has already been processed successfully.
        
        Args:
            file_id: File ID (typically from Google Drive)
            file_path: Optional local file path for hash verification
            
        Returns:
            True if file has been processed successfully
        """
        if not self._loaded:
            self.load_records()
        
        if file_id not in self._records:
            return False
        
        record = self._records[file_id]
        
        # Check processing status
        if record.processing_status != ProcessingStatus.COMPLETED:
            return False
        
        # Verify file hash if local file path provided
        if file_path and os.path.exists(file_path):
            current_hash = self.compute_file_hash(file_path)
            if current_hash and current_hash != record.file_hash:
                logger.info(f"File {file_id} has changed (hash mismatch), needs reprocessing")
                return False
        
        return True
    
    def should_process_file(self, file_id: str, file_path: Optional[str] = None,
                          max_attempts: int = 3) -> bool:
        """
        Determine if a file should be processed based on history and constraints.
        
        Args:
            file_id: File ID
            file_path: Optional local file path
            max_attempts: Maximum processing attempts before giving up
            
        Returns:
            True if file should be processed
        """
        if not self._loaded:
            self.load_records()
        
        # New files should be processed
        if file_id not in self._records:
            return True
        
        record = self._records[file_id]
        
        # Already completed successfully
        if record.processing_status == ProcessingStatus.COMPLETED:
            # Check if file has changed
            if file_path and os.path.exists(file_path):
                current_hash = self.compute_file_hash(file_path)
                if current_hash and current_hash != record.file_hash:
                    logger.info(f"File {file_id} has changed, should reprocess")
                    return True
            return False
        
        # Too many failed attempts
        if (record.processing_status == ProcessingStatus.FAILED and 
            record.processing_attempts >= max_attempts):
            logger.warning(f"File {file_id} exceeded max attempts ({max_attempts}), skipping")
            return False
        
        # File is currently being processed (check for stale locks)
        if record.processing_status == ProcessingStatus.IN_PROGRESS:
            # If last update was more than 1 hour ago, assume stale
            if (record.last_processed and 
                datetime.now() - record.last_processed > timedelta(hours=1)):
                logger.warning(f"File {file_id} has stale in-progress status, will reprocess")
                return True
            else:
                logger.info(f"File {file_id} is currently being processed")
                return False
        
        # File failed but within attempt limits, can retry
        if record.processing_status == ProcessingStatus.FAILED:
            return True
        
        # Default: process the file
        return True
    
    def add_or_update_file(self, file_id: str, filename: str, file_path: Optional[str] = None,
                          metadata: Optional[Dict[str, Any]] = None) -> FileRecord:
        """
        Add a new file or update existing file record.
        
        Args:
            file_id: File ID
            filename: Original filename
            file_path: Optional local file path
            metadata: Additional metadata
            
        Returns:
            FileRecord object
        """
        if not self._loaded:
            self.load_records()
        
        current_time = datetime.now()
        file_hash = ""
        file_size = 0
        
        # Compute hash and size if file path provided
        if file_path and os.path.exists(file_path):
            file_hash = self.compute_file_hash(file_path)
            file_size = os.path.getsize(file_path)
        
        # Check if record exists
        if file_id in self._records:
            record = self._records[file_id]
            
            # Update existing record
            record.filename = filename
            if file_hash:
                record.file_hash = file_hash
            if file_size:
                record.file_size = file_size
            if metadata:
                record.metadata.update(metadata)
            
            logger.debug(f"Updated existing record for file {file_id}")
        else:
            # Create new record
            record = FileRecord(
                file_id=file_id,
                filename=filename,
                file_hash=file_hash,
                file_size=file_size,
                processing_status=ProcessingStatus.PENDING,
                first_seen=current_time,
                last_processed=None,
                processing_attempts=0,
                error_message=None,
                output_files=[],
                metadata=metadata or {}
            )
            
            self._records[file_id] = record
            logger.debug(f"Added new record for file {file_id}")
        
        # Save changes
        self.save_records()
        
        return record
    
    def mark_processing_started(self, file_id: str) -> None:
        """
        Mark a file as currently being processed.
        
        Args:
            file_id: File ID
        """
        if file_id in self._records:
            record = self._records[file_id]
            record.processing_status = ProcessingStatus.IN_PROGRESS
            record.last_processed = datetime.now()
            record.processing_attempts += 1
            record.error_message = None
            
            self.save_records()
            logger.debug(f"Marked file {file_id} as in progress (attempt {record.processing_attempts})")
    
    def mark_processing_completed(self, file_id: str, output_files: List[str] = None) -> None:
        """
        Mark a file as successfully processed.
        
        Args:
            file_id: File ID
            output_files: List of generated output file paths
        """
        if file_id in self._records:
            record = self._records[file_id]
            record.processing_status = ProcessingStatus.COMPLETED
            record.last_processed = datetime.now()
            record.error_message = None
            
            if output_files:
                record.output_files.extend(output_files)
            
            self.save_records()
            logger.info(f"Marked file {file_id} as completed")
    
    def mark_processing_failed(self, file_id: str, error_message: str) -> None:
        """
        Mark a file as failed to process.
        
        Args:
            file_id: File ID
            error_message: Error description
        """
        if file_id in self._records:
            record = self._records[file_id]
            record.processing_status = ProcessingStatus.FAILED
            record.last_processed = datetime.now()
            record.error_message = error_message
            
            self.save_records()
            logger.warning(f"Marked file {file_id} as failed: {error_message}")
    
    def get_file_record(self, file_id: str) -> Optional[FileRecord]:
        """
        Get file record by ID.
        
        Args:
            file_id: File ID
            
        Returns:
            FileRecord or None if not found
        """
        if not self._loaded:
            self.load_records()
        
        return self._records.get(file_id)
    
    def get_files_by_status(self, status: ProcessingStatus) -> List[FileRecord]:
        """
        Get all files with a specific processing status.
        
        Args:
            status: Processing status to filter by
            
        Returns:
            List of FileRecord objects
        """
        if not self._loaded:
            self.load_records()
        
        return [record for record in self._records.values() if record.processing_status == status]
    
    def get_pending_files(self) -> List[FileRecord]:
        """Get all files that are pending processing."""
        return self.get_files_by_status(ProcessingStatus.PENDING)
    
    def get_failed_files(self, max_attempts: int = 3) -> List[FileRecord]:
        """
        Get files that failed processing but can be retried.
        
        Args:
            max_attempts: Maximum attempts threshold
            
        Returns:
            List of FileRecord objects that can be retried
        """
        failed_files = self.get_files_by_status(ProcessingStatus.FAILED)
        return [f for f in failed_files if f.processing_attempts < max_attempts]
    
    def cleanup_old_records(self, max_age_days: int = 90) -> int:
        """
        Clean up old file records.
        
        Args:
            max_age_days: Maximum age of records to keep
            
        Returns:
            Number of records cleaned up
        """
        if not self._loaded:
            self.load_records()
        
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        
        # Find records to remove
        records_to_remove = []
        for file_id, record in self._records.items():
            # Only remove completed or failed records that are old
            if (record.processing_status in [ProcessingStatus.COMPLETED, ProcessingStatus.FAILED] and
                record.first_seen < cutoff_date):
                records_to_remove.append(file_id)
        
        # Remove old records
        for file_id in records_to_remove:
            del self._records[file_id]
        
        if records_to_remove:
            self.save_records()
            logger.info(f"Cleaned up {len(records_to_remove)} old file records")
        
        return len(records_to_remove)
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get processing statistics.
        
        Returns:
            Dictionary with various statistics
        """
        if not self._loaded:
            self.load_records()
        
        status_counts = {}
        for status in ProcessingStatus:
            status_counts[status.value] = len(self.get_files_by_status(status))
        
        total_files = len(self._records)
        total_attempts = sum(record.processing_attempts for record in self._records.values())
        
        # Calculate success rate
        completed = status_counts.get('completed', 0)
        success_rate = (completed / total_files * 100) if total_files > 0 else 0
        
        # Find oldest and newest records
        if self._records:
            first_seen_dates = [record.first_seen for record in self._records.values()]
            oldest_record = min(first_seen_dates)
            newest_record = max(first_seen_dates)
        else:
            oldest_record = None
            newest_record = None
        
        return {
            'total_files': total_files,
            'status_counts': status_counts,
            'total_processing_attempts': total_attempts,
            'success_rate_percent': round(success_rate, 2),
            'oldest_record': oldest_record.isoformat() if oldest_record else None,
            'newest_record': newest_record.isoformat() if newest_record else None,
            'database_path': str(self.db_path),
            'database_size_bytes': os.path.getsize(self.db_path) if self.db_path.exists() else 0
        }
    
    def reset_stale_in_progress(self, max_age_hours: int = 2) -> int:
        """
        Reset stale in-progress records to pending status.
        
        Args:
            max_age_hours: Maximum age for in-progress records
            
        Returns:
            Number of records reset
        """
        if not self._loaded:
            self.load_records()
        
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        reset_count = 0
        
        for record in self._records.values():
            if (record.processing_status == ProcessingStatus.IN_PROGRESS and
                record.last_processed and
                record.last_processed < cutoff_time):
                
                record.processing_status = ProcessingStatus.PENDING
                reset_count += 1
                logger.warning(f"Reset stale in-progress record for file {record.file_id}")
        
        if reset_count > 0:
            self.save_records()
            logger.info(f"Reset {reset_count} stale in-progress records")
        
        return reset_count
    
    def export_records(self, output_path: str, status_filter: Optional[ProcessingStatus] = None) -> None:
        """
        Export file records to JSON file.
        
        Args:
            output_path: Output file path
            status_filter: Optional status filter
        """
        if not self._loaded:
            self.load_records()
        
        # Filter records if specified
        if status_filter:
            records_to_export = {
                file_id: record for file_id, record in self._records.items()
                if record.processing_status == status_filter
            }
        else:
            records_to_export = self._records
        
        # Convert to JSON-serializable format
        export_data = {}
        for file_id, record in records_to_export.items():
            record_dict = asdict(record)
            record_dict['first_seen'] = record.first_seen.isoformat()
            if record.last_processed:
                record_dict['last_processed'] = record.last_processed.isoformat()
            else:
                record_dict['last_processed'] = None
            record_dict['processing_status'] = record.processing_status.value
            export_data[file_id] = record_dict
        
        # Write to file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported {len(export_data)} records to {output_path}")
    
    def find_duplicates(self) -> Dict[str, List[str]]:
        """
        Find duplicate files based on hash.
        
        Returns:
            Dictionary mapping hash to list of file IDs
        """
        if not self._loaded:
            self.load_records()
        
        hash_to_files = {}
        
        for file_id, record in self._records.items():
            if record.file_hash:
                if record.file_hash not in hash_to_files:
                    hash_to_files[record.file_hash] = []
                hash_to_files[record.file_hash].append(file_id)
        
        # Return only hashes with multiple files
        duplicates = {hash_val: file_list for hash_val, file_list in hash_to_files.items() 
                     if len(file_list) > 1}
        
        if duplicates:
            logger.info(f"Found {len(duplicates)} sets of duplicate files")
        
        return duplicates
