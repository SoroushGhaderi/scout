"""
Data Versioning System
======================

Provides version control for bronze layer data, enabling:
- Version history tracking
- Reprocessing of old versions
- SCD (Slowly Changing Dimension) handling

Usage:
    from src.utils.versioning import DataVersioning, VersionMetadata
    
    versioning = DataVersioning(base_path="data/fotmob/bronze")
    version = versioning.save_version(
        match_id="12345",
        date="20251114",
        data={"key": "value"},
        version_type="scrape"
    )
"""

import json
import hashlib
import shutil
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging
from .date_utils import format_date_compact_to_display

logger = logging.getLogger(__name__)


@dataclass
class VersionMetadata:
    """
    Metadata for a data version.
    
    Attributes:
        version: Version number (incremental)
        timestamp: When this version was created
        version_type: Type of version (scrape, reprocess, correction)
        checksum: SHA256 hash of the data
        file_path: Path to the versioned file
        previous_version: Previous version number (if any)
        change_reason: Reason for this version
        metadata: Additional metadata
    """
    
    version: int
    timestamp: datetime
    version_type: str  # scrape, reprocess, correction, scd_update
    checksum: str
    file_path: str
    previous_version: Optional[int] = None
    change_reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VersionMetadata':
        """Create from dictionary."""
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)


@dataclass
class SCDRecord:
    """
    Slowly Changing Dimension record (Type 2).
    
    Attributes:
        record_id: Unique identifier for the record
        effective_date: When this version became effective
        end_date: When this version ended (None for current)
        is_current: Whether this is the current version
        version: Version number
        data: The dimension data
    """
    
    record_id: str
    effective_date: datetime
    end_date: Optional[datetime]
    is_current: bool
    version: int
    data: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data['effective_date'] = self.effective_date.isoformat()
        if self.end_date:
            data['end_date'] = self.end_date.isoformat()
        return data


class DataVersioning:
    """
    Manages data versioning for bronze layer.
    
    Structure:
        data/{scraper}/bronze/
        ├── matches/
        │   └── date=YYYY-MM-DD/
        │       ├── match_12345.json          # Current version
        │       └── _versions/
        │           └── match_12345/
        │               ├── v1_match_12345.json
        │               ├── v2_match_12345.json
        │               └── versions.json     # Version metadata
    """
    
    def __init__(self, base_path: str, enable_scd: bool = False):
        """
        Initialize data versioning.
        
        Args:
            base_path: Base path for bronze storage
            enable_scd: Enable SCD Type 2 tracking
        """
        self.base_path = Path(base_path)
        self.enable_scd = enable_scd
        self.logger = logger
        
        # SCD storage directory
        if self.enable_scd:
            self.scd_dir = self.base_path.parent / "_scd"
            self.scd_dir.mkdir(parents=True, exist_ok=True)
    
    def _compute_checksum(self, data: Any) -> str:
        """Compute SHA256 checksum of data."""
        if isinstance(data, dict):
            data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        else:
            data_str = str(data)
        return hashlib.sha256(data_str.encode('utf-8')).hexdigest()
    
    def _get_versions_dir(self, date_dir: Path, match_id: str) -> Path:
        """Get versions directory for a match."""
        versions_dir = date_dir / "_versions" / f"match_{match_id}"
        versions_dir.mkdir(parents=True, exist_ok=True)
        return versions_dir
    
    def _get_version_metadata_path(self, versions_dir: Path) -> Path:
        """Get path to version metadata file."""
        return versions_dir / "versions.json"
    
    def _load_version_metadata(self, versions_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
        """Load version metadata."""
        metadata_path = self._get_version_metadata_path(versions_dir)
        
        if not metadata_path.exists():
            return {}
        
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Could not load version metadata from {metadata_path}: {e}")
            return {}
    
    def _save_version_metadata(self, versions_dir: Path, metadata: Dict[str, List[Dict[str, Any]]]):
        """Save version metadata."""
        metadata_path = self._get_version_metadata_path(versions_dir)
        
        try:
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.error(f"Could not save version metadata to {metadata_path}: {e}")
    
    def _archive_current_version(
        self,
        current_file: Path,
        versions_dir: Path,
        version: int,
        checksum: str,
        version_type: str,
        change_reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Archive current version before overwriting."""
        if not current_file.exists():
            return None
        
        # Create versioned file name
        versioned_file = versions_dir / f"v{version}_{current_file.name}"
        
        try:
            # Copy current file to versioned location
            shutil.copy2(current_file, versioned_file)
            
            # Create version metadata
            version_meta = VersionMetadata(
                version=version,
                timestamp=datetime.now(),
                version_type=version_type,
                checksum=checksum,
                file_path=str(versioned_file),
                previous_version=version - 1 if version > 1 else None,
                change_reason=change_reason,
                metadata=metadata or {}
            )
            
            # Load existing metadata
            all_metadata = self._load_version_metadata(versions_dir)
            match_id = current_file.stem.replace('match_', '')
            
            if match_id not in all_metadata:
                all_metadata[match_id] = []
            
            all_metadata[match_id].append(version_meta.to_dict())
            
            # Save updated metadata
            self._save_version_metadata(versions_dir, all_metadata)
            
            self.logger.info(f"Archived version {version} of {current_file.name} to {versioned_file}")
            return versioned_file
            
        except Exception as e:
            self.logger.error(f"Could not archive version {version} of {current_file.name}: {e}")
            return None
    
    def save_version(
        self,
        match_id: str,
        date_str: str,
        data: Dict[str, Any],
        current_file: Path,
        version_type: str = "scrape",
        change_reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        force_new_version: bool = False
    ) -> VersionMetadata:
        """
        Save a new version of data, archiving previous version if it exists.
        
        Args:
            match_id: Match identifier
            date_str: Date string (YYYY-MM-DD or YYYYMMDD)
            data: Data to save
            current_file: Path to current file (will be overwritten)
            version_type: Type of version (scrape, reprocess, correction)
            change_reason: Reason for this version
            metadata: Additional metadata
            force_new_version: Force new version even if data is identical
        
        Returns:
            VersionMetadata for the new version
        """
        # Format date
        try:
            date_formatted = format_date_compact_to_display(date_str)
        except (ValueError, IndexError):
            date_formatted = date_str
        
        date_dir = Path(current_file).parent
        versions_dir = self._get_versions_dir(date_dir, match_id)
        
        # Compute checksum of new data
        new_checksum = self._compute_checksum(data)
        
        # Load existing version metadata
        all_metadata = self._load_version_metadata(versions_dir)
        match_metadata = all_metadata.get(match_id, [])
        
        # Determine next version number
        if match_metadata:
            next_version = max(m['version'] for m in match_metadata) + 1
            last_version = match_metadata[-1]
            last_checksum = last_version.get('checksum')
            
            # Check if data has changed
            if not force_new_version and new_checksum == last_checksum:
                self.logger.debug(f"Data unchanged for {match_id}, skipping versioning")
                return VersionMetadata.from_dict(last_version)
        else:
            next_version = 1
        
        # Archive current version if it exists (only if version > 1)
        if current_file.exists() and next_version > 1:
            self._archive_current_version(
                current_file=current_file,
                versions_dir=versions_dir,
                version=next_version - 1,
                checksum=self._compute_checksum_from_file(current_file),
                version_type=version_type,
                change_reason=change_reason,
                metadata=metadata
            )
        
        # Create new version metadata
        version_meta = VersionMetadata(
            version=next_version,
            timestamp=datetime.now(),
            version_type=version_type,
            checksum=new_checksum,
            file_path=str(current_file),
            previous_version=next_version - 1 if next_version > 1 else None,
            change_reason=change_reason,
            metadata=metadata or {}
        )
        
        # Save to metadata
        if match_id not in all_metadata:
            all_metadata[match_id] = []
        all_metadata[match_id].append(version_meta.to_dict())
        self._save_version_metadata(versions_dir, all_metadata)
        
        self.logger.info(f"Created version {next_version} for {match_id}")
        return version_meta
    
    def _compute_checksum_from_file(self, file_path: Path) -> str:
        """Compute checksum of a file."""
        try:
            with open(file_path, 'rb') as f:
                return hashlib.sha256(f.read()).hexdigest()
        except Exception as e:
            self.logger.warning(f"Could not compute checksum for {file_path}: {e}")
            return ""
    
    def get_versions(self, match_id: str, date_str: str) -> List[VersionMetadata]:
        """
        Get all versions for a match.
        
        Args:
            match_id: Match identifier
            date_str: Date string
        
        Returns:
            List of VersionMetadata sorted by version number
        """
        # Format date
        try:
            date_formatted = format_date_compact_to_display(date_str)
        except (ValueError, IndexError):
            date_formatted = date_str
        
        # Find date directory
        matches_dir = self.base_path / "matches"
        date_dir = matches_dir / date_formatted
        
        if not date_dir.exists():
            return []
        
        versions_dir = self._get_versions_dir(date_dir, match_id)
        all_metadata = self._load_version_metadata(versions_dir)
        
        match_metadata = all_metadata.get(match_id, [])
        return [VersionMetadata.from_dict(m) for m in sorted(match_metadata, key=lambda x: x['version'])]
    
    def get_version_file(self, match_id: str, date_str: str, version: int) -> Optional[Path]:
        """
        Get file path for a specific version.
        
        Args:
            match_id: Match identifier
            date_str: Date string
            version: Version number
        
        Returns:
            Path to versioned file, or None if not found
        """
        # Format date
        try:
            date_formatted = format_date_compact_to_display(date_str)
        except (ValueError, IndexError):
            date_formatted = date_str
        
        matches_dir = self.base_path / "matches"
        date_dir = matches_dir / date_formatted
        
        if not date_dir.exists():
            return None
        
        versions_dir = self._get_versions_dir(date_dir, match_id)
        versioned_file = versions_dir / f"v{version}_match_{match_id}.json"
        
        if versioned_file.exists():
            return versioned_file
        
        return None
    
    def restore_version(self, match_id: str, date_str: str, version: int) -> bool:
        """
        Restore a specific version as the current version.
        
        Args:
            match_id: Match identifier
            date_str: Date string
            version: Version number to restore
        
        Returns:
            True if successful, False otherwise
        """
        version_file = self.get_version_file(match_id, date_str, version)
        
        if not version_file:
            self.logger.error(f"Version {version} not found for {match_id}")
            return False
        
        # Format date
        try:
            date_formatted = format_date_compact_to_display(date_str)
        except (ValueError, IndexError):
            date_formatted = date_str
        
        matches_dir = self.base_path / "matches"
        date_dir = matches_dir / date_formatted
        current_file = date_dir / f"match_{match_id}.json"
        
        try:
            # Archive current version first
            if current_file.exists():
                versions = self.get_versions(match_id, date_str)
                next_version = max(v.version for v in versions) + 1 if versions else 1
                self._archive_current_version(
                    current_file=current_file,
                    versions_dir=self._get_versions_dir(date_dir, match_id),
                    version=next_version - 1,
                    checksum=self._compute_checksum_from_file(current_file),
                    version_type="restore_backup",
                    change_reason=f"Backup before restoring version {version}"
                )
            
            # Copy versioned file to current location
            shutil.copy2(version_file, current_file)
            
            # Create new version metadata for restore
            self.save_version(
                match_id=match_id,
                date_str=date_str,
                data={},  # Will be loaded from file
                current_file=current_file,
                version_type="restore",
                change_reason=f"Restored from version {version}",
                force_new_version=True
            )
            
            self.logger.info(f"Restored version {version} for {match_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Could not restore version {version} for {match_id}: {e}")
            return False
    
    def create_scd_record(
        self,
        record_id: str,
        data: Dict[str, Any],
        effective_date: Optional[datetime] = None
    ) -> SCDRecord:
        """
        Create or update an SCD Type 2 record.
        
        Args:
            record_id: Unique record identifier
            data: Dimension data
            effective_date: When this version becomes effective (default: now)
        
        Returns:
            SCDRecord
        """
        if not self.enable_scd:
            raise RuntimeError("SCD is not enabled. Set enable_scd=True in DataVersioning constructor.")
        
        if effective_date is None:
            effective_date = datetime.now()
        
        scd_file = self.scd_dir / f"{record_id}.json"
        
        # Load existing SCD records
        if scd_file.exists():
            with open(scd_file, 'r', encoding='utf-8') as f:
                records_data = json.load(f)
            records = [SCDRecord(**r) for r in records_data]
        else:
            records = []
        
        # End previous current record
        for record in records:
            if record.is_current:
                record.end_date = effective_date
                record.is_current = False
        
        # Determine next version
        next_version = max((r.version for r in records), default=0) + 1
        
        # Create new record
        new_record = SCDRecord(
            record_id=record_id,
            effective_date=effective_date,
            end_date=None,
            is_current=True,
            version=next_version,
            data=data
        )
        
        records.append(new_record)
        
        # Save records
        records_data = [r.to_dict() for r in records]
        with open(scd_file, 'w', encoding='utf-8') as f:
            json.dump(records_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Created SCD record version {next_version} for {record_id}")
        return new_record
    
    def get_scd_record(
        self,
        record_id: str,
        as_of_date: Optional[datetime] = None
    ) -> Optional[SCDRecord]:
        """
        Get SCD record as of a specific date (or current if None).
        
        Args:
            record_id: Record identifier
            as_of_date: Date to query (None for current)
        
        Returns:
            SCDRecord or None if not found
        """
        if not self.enable_scd:
            raise RuntimeError("SCD is not enabled.")
        
        scd_file = self.scd_dir / f"{record_id}.json"
        
        if not scd_file.exists():
            return None
        
        with open(scd_file, 'r', encoding='utf-8') as f:
            records_data = json.load(f)
        
        records = [SCDRecord(**r) for r in records_data]
        
        if as_of_date is None:
            # Get current record
            for record in records:
                if record.is_current:
                    return record
        else:
            # Get record effective at as_of_date
            for record in records:
                if record.effective_date <= as_of_date:
                    if record.end_date is None or record.end_date >= as_of_date:
                        return record
        
        return None

