"""
Google Drive Manager Module

Handles all Google Drive API operations including:
- Authentication and authorization
- Monitoring folders for new files
- Downloading files
- Uploading processed results
- File metadata management
"""

import io
import os
import time
import hashlib
import logging
from typing import List, Dict, Optional, Tuple, Any
from pathlib import Path
from datetime import datetime

import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from .config import Config

logger = logging.getLogger(__name__)

class GoogleDriveManager:
    """
    Manages Google Drive API operations for the Hindi PDF processing pipeline.
    
    Provides methods for authentication, file monitoring, downloading, and uploading
    with proper error handling and retry mechanisms.
    """
    
    def __init__(self, config: Config):
        """
        Initialize Google Drive manager.
        
        Args:
            config: Configuration instance
        """
        self.config = config
        self.service = None
        self._authenticate()
    
    def _authenticate(self) -> None:
        """Authenticate with Google Drive API."""
        creds = None
        
        # Check if we have service account credentials
        if os.path.exists(self.config.google_credentials_path):
            logger.info("Using service account authentication")
            try:
                creds = ServiceAccountCredentials.from_service_account_file(
                    self.config.google_credentials_path,
                    scopes=self.config.google_scopes
                )
            except Exception as e:
                logger.error(f"Failed to load service account credentials: {e}")
                raise
        
        # Fall back to OAuth flow if no service account
        elif os.path.exists(self.config.google_token_path):
            logger.info("Using OAuth token authentication")
            creds = Credentials.from_authorized_user_file(
                self.config.google_token_path,
                self.config.google_scopes
            )
        
        # If there are no valid credentials available, start OAuth flow
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Refreshing OAuth credentials")
                creds.refresh(Request())
            else:
                logger.info("Starting OAuth authentication flow")
                # This requires client_secret.json file
                client_secrets_path = "credentials/client_secret.json"
                if not os.path.exists(client_secrets_path):
                    raise FileNotFoundError(
                        f"OAuth client secrets not found at {client_secrets_path}. "
                        f"Please download from Google Cloud Console or use service account authentication."
                    )
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secrets_path,
                    self.config.google_scopes
                )
                creds = flow.run_local_server(port=0)
            
            # Save the credentials for future use
            os.makedirs(os.path.dirname(self.config.google_token_path), exist_ok=True)
            with open(self.config.google_token_path, 'w') as token:
                token.write(creds.to_json())
        
        # Build the service
        try:
            self.service = build('drive', 'v3', credentials=creds)
            logger.info("Google Drive API authentication successful")
        except Exception as e:
            logger.error(f"Failed to build Google Drive service: {e}")
            raise
    
    def list_files_in_folder(self, folder_id: str, file_type: str = "pdf") -> List[Dict[str, Any]]:
        """
        List files in a Google Drive folder.
        
        Args:
            folder_id: Google Drive folder ID
            file_type: File extension to filter by (default: pdf)
            
        Returns:
            List of file metadata dictionaries
        """
        try:
            query = f"'{folder_id}' in parents and trashed=false"
            if file_type:
                query += f" and name contains '.{file_type.lower()}'"
            
            results = self.service.files().list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType, size, modifiedTime, md5Checksum)"
            ).execute()
            
            files = results.get('files', [])
            logger.info(f"Found {len(files)} {file_type} files in folder {folder_id}")
            
            return files
            
        except HttpError as e:
            logger.error(f"Error listing files in folder {folder_id}: {e}")
            raise
    
    def download_file(self, file_id: str, output_path: str) -> bool:
        """
        Download a file from Google Drive.
        
        Args:
            file_id: Google Drive file ID
            output_path: Local path to save the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get file metadata
            file_metadata = self.service.files().get(fileId=file_id).execute()
            file_name = file_metadata['name']
            
            logger.info(f"Downloading file: {file_name}")
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Download file content
            request = self.service.files().get_media(fileId=file_id)
            with open(output_path, 'wb') as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(f"Download progress: {int(status.progress() * 100)}%")
            
            logger.info(f"Successfully downloaded {file_name} to {output_path}")
            return True
            
        except HttpError as e:
            logger.error(f"Error downloading file {file_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading file {file_id}: {e}")
            return False
    
    def upload_file(self, local_path: str, folder_id: str, filename: Optional[str] = None) -> Optional[str]:
        """
        Upload a file to Google Drive.
        
        Args:
            local_path: Path to local file
            folder_id: Google Drive folder ID to upload to
            filename: Optional filename override
            
        Returns:
            File ID if successful, None otherwise
        """
        try:
            if not os.path.exists(local_path):
                logger.error(f"Local file not found: {local_path}")
                return None
            
            if not filename:
                filename = os.path.basename(local_path)
            
            logger.info(f"Uploading file: {filename}")
            
            # Determine MIME type based on file extension
            file_ext = Path(local_path).suffix.lower()
            mime_type_map = {
                '.pdf': 'application/pdf',
                '.csv': 'text/csv',
                '.txt': 'text/plain',
                '.json': 'application/json'
            }
            mime_type = mime_type_map.get(file_ext, 'application/octet-stream')
            
            # Prepare file metadata
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            
            # Upload file
            media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
            request = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            )
            
            response = None
            while response is None:
                status, response = request.next_chunk()
                if status:
                    logger.debug(f"Upload progress: {int(status.progress() * 100)}%")
            
            file_id = response.get('id')
            logger.info(f"Successfully uploaded {filename} with ID: {file_id}")
            return file_id
            
        except HttpError as e:
            logger.error(f"Error uploading file {local_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error uploading file {local_path}: {e}")
            return None
    
    def get_file_metadata(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a Google Drive file.
        
        Args:
            file_id: Google Drive file ID
            
        Returns:
            File metadata dictionary or None if error
        """
        try:
            metadata = self.service.files().get(
                fileId=file_id,
                fields="id, name, mimeType, size, modifiedTime, md5Checksum"
            ).execute()
            return metadata
        except HttpError as e:
            logger.error(f"Error getting metadata for file {file_id}: {e}")
            return None
    
    def monitor_folder_for_changes(self, folder_id: str, last_check: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Monitor a folder for new or modified files since last check.
        
        Args:
            folder_id: Google Drive folder ID to monitor
            last_check: Datetime of last check (if None, returns all files)
            
        Returns:
            List of new/modified files
        """
        try:
            files = self.list_files_in_folder(folder_id, "pdf")
            
            if last_check is None:
                return files
            
            # Filter files modified after last check
            new_files = []
            for file in files:
                modified_time = datetime.fromisoformat(
                    file['modifiedTime'].replace('Z', '+00:00')
                )
                if modified_time > last_check:
                    new_files.append(file)
            
            logger.info(f"Found {len(new_files)} new/modified files since {last_check}")
            return new_files
            
        except Exception as e:
            logger.error(f"Error monitoring folder {folder_id}: {e}")
            return []
    
    def compute_file_hash(self, file_path: str) -> str:
        """
        Compute MD5 hash of a local file.
        
        Args:
            file_path: Path to local file
            
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
    
    def is_file_changed(self, file_id: str, local_path: str) -> bool:
        """
        Check if a Google Drive file has changed compared to local file.
        
        Args:
            file_id: Google Drive file ID
            local_path: Path to local file
            
        Returns:
            True if file has changed, False otherwise
        """
        try:
            # Get remote file metadata
            remote_metadata = self.get_file_metadata(file_id)
            if not remote_metadata:
                return True  # Assume changed if can't get metadata
            
            # Check if local file exists
            if not os.path.exists(local_path):
                return True  # File is new
            
            # Compare checksums
            remote_hash = remote_metadata.get('md5Checksum', '')
            local_hash = self.compute_file_hash(local_path)
            
            return remote_hash != local_hash
            
        except Exception as e:
            logger.error(f"Error checking if file changed: {e}")
            return True  # Assume changed on error
    
    def retry_operation(self, operation, *args, max_retries: Optional[int] = None, **kwargs):
        """
        Retry a Google Drive operation with exponential backoff.
        
        Args:
            operation: Function to retry
            *args: Arguments for the operation
            max_retries: Maximum number of retries (uses config default if None)
            **kwargs: Keyword arguments for the operation
            
        Returns:
            Result of the operation
            
        Raises:
            Last exception if all retries fail
        """
        if max_retries is None:
            max_retries = self.config.max_retries
        
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except HttpError as e:
                last_exception = e
                if e.resp.status in [429, 500, 502, 503, 504]:  # Retryable errors
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * self.config.retry_delay_seconds
                        logger.warning(f"Retryable error {e.resp.status}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                raise  # Non-retryable error
            except Exception as e:
                last_exception = e
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * self.config.retry_delay_seconds
                    logger.warning(f"Error occurred, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                    continue
                raise
        
        # All retries exhausted
        logger.error(f"Operation failed after {max_retries + 1} attempts")
        raise last_exception
    
    def create_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> Optional[str]:
        """
        Create a new folder in Google Drive.
        
        Args:
            folder_name: Name of the new folder
            parent_folder_id: Parent folder ID (root if None)
            
        Returns:
            New folder ID if successful, None otherwise
        """
        try:
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            folder_id = folder.get('id')
            logger.info(f"Created folder '{folder_name}' with ID: {folder_id}")
            return folder_id
            
        except HttpError as e:
            logger.error(f"Error creating folder '{folder_name}': {e}")
            return None
    
    def batch_download_files(self, file_ids: List[str], output_dir: str) -> Dict[str, bool]:
        """
        Download multiple files in batch.
        
        Args:
            file_ids: List of Google Drive file IDs
            output_dir: Directory to save downloaded files
            
        Returns:
            Dictionary mapping file_id to success status
        """
        results = {}
        
        os.makedirs(output_dir, exist_ok=True)
        
        for file_id in file_ids:
            try:
                # Get file name
                metadata = self.get_file_metadata(file_id)
                if not metadata:
                    results[file_id] = False
                    continue
                
                filename = metadata['name']
                output_path = os.path.join(output_dir, filename)
                
                # Download with retry
                success = self.retry_operation(self.download_file, file_id, output_path)
                results[file_id] = success
                
            except Exception as e:
                logger.error(f"Error in batch download for file {file_id}: {e}")
                results[file_id] = False
        
        successful = sum(1 for success in results.values() if success)
        logger.info(f"Batch download completed: {successful}/{len(file_ids)} files successful")
        
        return results
    
    def cleanup_temp_files(self, temp_dir: str, max_age_hours: int = 24) -> None:
        """
        Clean up temporary downloaded files older than specified age.
        
        Args:
            temp_dir: Directory containing temporary files
            max_age_hours: Maximum age of files to keep in hours
        """
        try:
            if not os.path.exists(temp_dir):
                return
            
            current_time = time.time()
            max_age_seconds = max_age_hours * 3600
            cleaned_count = 0
            
            for filename in os.listdir(temp_dir):
                file_path = os.path.join(temp_dir, filename)
                if os.path.isfile(file_path):
                    file_age = current_time - os.path.getmtime(file_path)
                    if file_age > max_age_seconds:
                        os.remove(file_path)
                        cleaned_count += 1
            
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} temporary files older than {max_age_hours} hours")
                
        except Exception as e:
            logger.error(f"Error cleaning up temporary files: {e}")
