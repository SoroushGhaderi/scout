"""
Data Lineage Tracking
=====================

Tracks data flow from source through transformations to destination.
Provides audit trail and enables data provenance queries.

Usage:
    from src.utils.lineage import DataLineage, LineageTracker
    
    tracker = LineageTracker()
    lineage = tracker.record_scrape(
        source="fotmob_api",
        match_id="12345",
        date="20251114",
        file_path="data/bronze/match_12345.json"
    )
"""

import hashlib
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


@dataclass
class DataLineage:
    """
    Data lineage record tracking data flow.
    
    Attributes:
        lineage_id: Unique identifier for this lineage record
        source: Data source (e.g., "fotmob_api", "aiscore_web")
        source_id: Source-specific identifier (e.g., match_id)
        source_path: Path to source data file
        transformation: Transformation applied (e.g., "bronze_to_clickhouse")
        destination: Destination system (e.g., "clickhouse", "bronze_storage")
        destination_table: Table name in destination (if applicable)
        destination_id: Destination-specific identifier
        timestamp: When the transformation occurred
        checksum: Hash of the data for integrity verification
        metadata: Additional metadata about the transformation
        parent_lineage_ids: IDs of parent lineage records (for transformation chains)
    """
    
    lineage_id: str
    source: str
    source_id: str
    source_path: Optional[str] = None
    transformation: str = "none"
    destination: str = "bronze"
    destination_table: Optional[str] = None
    destination_id: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    checksum: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_lineage_ids: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataLineage':
        """Create from dictionary."""
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)


class LineageTracker:
    """
    Tracks data lineage throughout the pipeline.
    
    Stores lineage records in:
    - JSON files: data/{scraper}/lineage/{date}/lineage.json
    - ClickHouse: lineage table (optional)
    """
    
    def __init__(self, base_path: str = "data", enable_clickhouse: bool = False):
        """
        Initialize lineage tracker.
        
        Args:
            base_path: Base path for lineage storage
            enable_clickhouse: Whether to also store in ClickHouse
        """
        self.base_path = Path(base_path)
        self.enable_clickhouse = enable_clickhouse
        self.lineage_records: Dict[str, DataLineage] = {}
        self.logger = logger
        
    def _generate_lineage_id(self, source: str, source_id: str, transformation: str) -> str:
        """Generate unique lineage ID."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        return f"{source}_{source_id}_{transformation}_{timestamp}"
    
    def _compute_checksum(self, file_path: Path) -> Optional[str]:
        """Compute SHA256 checksum of a file."""
        try:
            if not file_path.exists():
                return None
            
            sha256 = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    sha256.update(chunk)
            return sha256.hexdigest()
        except Exception as e:
            self.logger.warning(f"Could not compute checksum for {file_path}: {e}")
            return None
    
    def _get_lineage_path(self, scraper: str, date: str) -> Path:
        """Get path to lineage file for a date."""
        # If base_path already ends with the scraper name, don't add it again
        # This allows base_path to be "data/fotmob" instead of just "data"
        if str(self.base_path).endswith(scraper):
            lineage_dir = self.base_path / "lineage" / date
        else:
            lineage_dir = self.base_path / scraper / "lineage" / date
        lineage_dir.mkdir(parents=True, exist_ok=True)
        return lineage_dir / "lineage.json"
    
    def _load_lineage(self, scraper: str, date: str) -> Dict[str, DataLineage]:
        """Load lineage records for a date."""
        lineage_path = self._get_lineage_path(scraper, date)
        
        if not lineage_path.exists():
            return {}
        
        try:
            with open(lineage_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            records = {}
            for lineage_id, lineage_data in data.items():
                records[lineage_id] = DataLineage.from_dict(lineage_data)
            
            return records
        except Exception as e:
            self.logger.error(f"Could not load lineage from {lineage_path}: {e}")
            return {}
    
    def _save_lineage(self, scraper: str, date: str, records: Dict[str, DataLineage]):
        """Save lineage records for a date."""
        lineage_path = self._get_lineage_path(scraper, date)
        
        try:
            data = {lineage_id: lineage.to_dict() for lineage_id, lineage in records.items()}
            
            with open(lineage_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            self.logger.debug(f"Saved {len(records)} lineage records to {lineage_path}")
        except Exception as e:
            self.logger.error(f"Could not save lineage to {lineage_path}: {e}")
    
    def record_scrape(
        self,
        scraper: str,
        source: str,
        source_id: str,
        date: str,
        file_path: Optional[Path] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> DataLineage:
        """
        Record data lineage for a scrape operation.
        
        Args:
            scraper: Scraper name (fotmob, aiscore)
            source: Source system (e.g., "fotmob_api", "aiscore_web")
            source_id: Source identifier (e.g., match_id)
            date: Date in YYYYMMDD format
            file_path: Path to scraped data file
            metadata: Additional metadata
        
        Returns:
            DataLineage record
        """
        checksum = None
        if file_path:
            checksum = self._compute_checksum(file_path)
        
        lineage_id = self._generate_lineage_id(source, source_id, "scrape")
        
        lineage = DataLineage(
            lineage_id=lineage_id,
            source=source,
            source_id=source_id,
            source_path=str(file_path) if file_path else None,
            transformation="scrape",
            destination="bronze",
            timestamp=datetime.now(),
            checksum=checksum,
            metadata=metadata or {}
        )
        
        # Load existing records
        records = self._load_lineage(scraper, date)
        records[lineage_id] = lineage
        self._save_lineage(scraper, date, records)
        
        self.logger.debug(f"Recorded scrape lineage: {lineage_id}")
        return lineage
    
    def record_load(
        self,
        scraper: str,
        source_id: str,
        date: str,
        destination_table: str,
        destination_id: Optional[str] = None,
        source_path: Optional[Path] = None,
        parent_lineage_ids: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> DataLineage:
        """
        Record data lineage for a load operation (bronze to ClickHouse).
        
        Args:
            scraper: Scraper name (fotmob, aiscore)
            source_id: Source identifier (e.g., match_id)
            date: Date in YYYYMMDD format
            destination_table: ClickHouse table name
            destination_id: Destination identifier (if different from source_id)
            source_path: Path to source data file
            parent_lineage_ids: IDs of parent lineage records
            metadata: Additional metadata
        
        Returns:
            DataLineage record
        """
        checksum = None
        if source_path:
            checksum = self._compute_checksum(source_path)
        
        lineage_id = self._generate_lineage_id(scraper, source_id, f"load_{destination_table}")
        
        lineage = DataLineage(
            lineage_id=lineage_id,
            source="bronze",
            source_id=source_id,
            source_path=str(source_path) if source_path else None,
            transformation=f"bronze_to_clickhouse",
            destination="clickhouse",
            destination_table=destination_table,
            destination_id=destination_id or source_id,
            timestamp=datetime.now(),
            checksum=checksum,
            metadata=metadata or {},
            parent_lineage_ids=parent_lineage_ids or []
        )
        
        # Load existing records
        records = self._load_lineage(scraper, date)
        records[lineage_id] = lineage
        self._save_lineage(scraper, date, records)
        
        self.logger.debug(f"Recorded load lineage: {lineage_id}")
        return lineage
    
    def get_lineage(self, scraper: str, date: str, source_id: Optional[str] = None) -> List[DataLineage]:
        """
        Get lineage records for a date, optionally filtered by source_id.
        
        Args:
            scraper: Scraper name
            date: Date in YYYYMMDD format
            source_id: Optional source ID to filter by
        
        Returns:
            List of DataLineage records
        """
        records = self._load_lineage(scraper, date)
        
        if source_id:
            return [lineage for lineage in records.values() if lineage.source_id == source_id]
        
        return list(records.values())
    
    def get_lineage_chain(self, scraper: str, date: str, source_id: str) -> List[DataLineage]:
        """
        Get complete lineage chain for a source_id (scrape -> load).
        
        Args:
            scraper: Scraper name
            date: Date in YYYYMMDD format
            source_id: Source identifier
        
        Returns:
            List of DataLineage records in chronological order
        """
        all_records = self._load_lineage(scraper, date)
        
        # Find all records for this source_id
        relevant_records = [
            lineage for lineage in all_records.values()
            if lineage.source_id == source_id or lineage.destination_id == source_id
        ]
        
        # Sort by timestamp
        relevant_records.sort(key=lambda x: x.timestamp)
        
        return relevant_records
    
    def verify_integrity(self, scraper: str, date: str, source_id: str) -> Dict[str, Any]:
        """
        Verify data integrity by comparing checksums.
        
        Args:
            scraper: Scraper name
            date: Date in YYYYMMDD format
            source_id: Source identifier
        
        Returns:
            Dictionary with verification results
        """
        chain = self.get_lineage_chain(scraper, date, source_id)
        
        if not chain:
            return {
                "verified": False,
                "reason": "No lineage records found",
                "source_id": source_id
            }
        
        # Check if source file still exists and matches checksum
        scrape_record = next((r for r in chain if r.transformation == "scrape"), None)
        
        if not scrape_record:
            return {
                "verified": False,
                "reason": "No scrape record found",
                "source_id": source_id
            }
        
        if not scrape_record.source_path:
            return {
                "verified": False,
                "reason": "No source path in lineage record",
                "source_id": source_id
            }
        
        source_path = Path(scrape_record.source_path)
        current_checksum = self._compute_checksum(source_path)
        
        verified = current_checksum == scrape_record.checksum
        
        return {
            "verified": verified,
            "source_id": source_id,
            "source_path": str(source_path),
            "expected_checksum": scrape_record.checksum,
            "current_checksum": current_checksum,
            "lineage_records": len(chain),
            "timestamp": datetime.now().isoformat()
        }

