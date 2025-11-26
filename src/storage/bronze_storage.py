"""Bronze layer storage for raw API responses (JSON format)."""

import json
import os
import io
import gzip
import tarfile
from pathlib import Path
from typing import Dict, Any, Optional, Set, List
from datetime import datetime

# Try to import filelock (for file locking), fall back to no locking if not available
try:
    from filelock import FileLock, Timeout
    FILE_LOCKING_AVAILABLE = True
except ImportError:
    FILE_LOCKING_AVAILABLE = False
    FileLock = None
    Timeout = None

from ..utils.logging_utils import get_logger
from ..utils.lineage import LineageTracker
from ..utils.date_utils import format_date_compact_to_display_partial


class BronzeStorage:
    """
    Store raw API responses in Bronze layer (data lake).
    
    Bronze layer contains unprocessed, raw data exactly as received from the API.
    This provides an audit trail and enables reprocessing if needed.
    
    Structure:
        data/fotmob/
            ├── matches/
            │   └── YYYYMMDD/
            │       ├── match_4193494.json
            │       └── match_4193495.json
            └── lineage/
                └── YYYYMMDD/
                    └── lineage.json
            └── daily_listings/
                └── YYYYMMDD/
                    └── matches.json
    
    Data lineage is tracked separately in: data/fotmob/lineage/{date}/lineage.json
    Daily listings track match IDs to prevent duplicate API requests.
    """
    
    def __init__(self, base_dir: str = "data/fotmob"):
        """
        Initialize Bronze storage.
        
        Args:
            base_dir: Base directory for raw data (default: data/fotmob)
        """
        self.base_dir = Path(base_dir)
        self.logger = get_logger()
        
        # Initialize lineage tracker (lineage stored in data/fotmob/lineage/)
        # Use base_dir directly so lineage goes to data/fotmob/lineage/ (not data/fotmob/fotmob/lineage/)
        self.lineage_tracker = LineageTracker(base_path=str(self.base_dir))
        
        # Create base directories with conflict checking
        if self.base_dir.exists() and self.base_dir.is_file():
            raise OSError(
                f"Cannot create directory '{self.base_dir}': A file with that name already exists. "
                f"Please remove or rename the file at {self.base_dir.absolute()}"
            )
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # Matches directory
        self.matches_dir = self.base_dir / "matches"
        if self.matches_dir.exists() and self.matches_dir.is_file():
            raise OSError(
                f"Cannot create directory '{self.matches_dir}': A file with that name already exists. "
                f"Please remove or rename the file at {self.matches_dir.absolute()}"
            )
        self.matches_dir.mkdir(parents=True, exist_ok=True)
        
        # Daily listings directory
        self.daily_listings_dir = self.base_dir / "daily_listings"
        if self.daily_listings_dir.exists() and self.daily_listings_dir.is_file():
            raise OSError(
                f"Cannot create directory '{self.daily_listings_dir}': A file with that name already exists. "
                f"Please remove or rename the file at {self.daily_listings_dir.absolute()}"
            )
        self.daily_listings_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Bronze storage initialized: {base_dir}")
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform pre-flight health checks before operations.
        
        Checks:
        - Disk space available
        - Write permissions
        - Directory structure
        - Network connectivity (basic)
        
        Returns:
            Dictionary with health check results
        """
        issues = []
        warnings = []
        checks = []
        
        # Check 1: Disk space
        try:
            import shutil
            stat = shutil.disk_usage(self.base_dir)
            free_gb = stat.free / (1024**3)
            total_gb = stat.total / (1024**3)
            used_pct = (stat.used / stat.total) * 100
            
            checks.append({
                "check": "Disk Space",
                "status": "OK" if free_gb >= 5 else "WARNING" if free_gb >= 1 else "ERROR",
                "message": f"{free_gb:.1f} GB free ({used_pct:.1f}% used)",
                "details": {
                    "free_gb": round(free_gb, 2),
                    "total_gb": round(total_gb, 2),
                    "used_pct": round(used_pct, 1)
                }
            })
            
            if free_gb < 1:
                issues.append(f"Critical: Less than 1 GB free disk space ({free_gb:.1f} GB)")
            elif free_gb < 5:
                warnings.append(f"Low disk space: {free_gb:.1f} GB free (recommend 5+ GB)")
        except Exception as e:
            checks.append({
                "check": "Disk Space",
                "status": "ERROR",
                "message": f"Failed to check: {e}"
            })
            issues.append(f"Could not check disk space: {e}")
        
        # Check 2: Write permissions
        try:
            test_file = self.base_dir / '.health_check_write_test'
            test_file.write_text('test')
            test_file.unlink()
            
            checks.append({
                "check": "Write Permissions",
                "status": "OK",
                "message": "Can write to bronze directory"
            })
        except Exception as e:
            checks.append({
                "check": "Write Permissions",
                "status": "ERROR",
                "message": f"No write permission: {e}"
            })
            issues.append(f"No write permission in {self.base_dir}: {e}")
        
        # Check 3: Directory structure
        required_dirs = [
            self.matches_dir
        ]
        
        missing_dirs = [d for d in required_dirs if not d.exists()]
        
        if missing_dirs:
            checks.append({
                "check": "Directory Structure",
                "status": "WARNING",
                "message": f"{len(missing_dirs)} directories missing (will be created)",
                "details": {"missing": [str(d) for d in missing_dirs]}
            })
            warnings.append(f"{len(missing_dirs)} directories missing")
        else:
            checks.append({
                "check": "Directory Structure",
                "status": "OK",
                "message": "All required directories exist"
            })
        
        # Check 4: Network connectivity (basic)
        try:
            import requests
            requests.get('https://www.fotmob.com', timeout=5)
            
            checks.append({
                "check": "Network Connectivity",
                "status": "OK",
                "message": "Can reach FotMob.com"
            })
        except Exception as e:
            checks.append({
                "check": "Network Connectivity",
                "status": "WARNING",
                "message": f"Cannot reach FotMob.com: {e}"
            })
            warnings.append(f"Network may be unavailable: {e}")
        
        # Check 5: File locking availability
        if FILE_LOCKING_AVAILABLE:
            checks.append({
                "check": "File Locking",
                "status": "OK",
                "message": "File locking available (filelock installed)"
            })
        else:
            checks.append({
                "check": "File Locking",
                "status": "WARNING",
                "message": "File locking NOT available (install 'filelock' package)"
            })
            warnings.append("File locking not available - concurrent access may cause issues")
        
        # Overall health
        error_count = sum(1 for c in checks if c["status"] == "ERROR")
        warning_count = sum(1 for c in checks if c["status"] == "WARNING")
        
        if error_count > 0:
            overall_status = "UNHEALTHY"
        elif warning_count > 0:
            overall_status = "WARNING"
        else:
            overall_status = "HEALTHY"
        
        return {
            "overall_status": overall_status,
            "timestamp": datetime.now().isoformat(),
            "checks": checks,
            "issues": issues,
            "warnings": warnings,
            "summary": {
                "total_checks": len(checks),
                "passed": sum(1 for c in checks if c["status"] == "OK"),
                "warnings": warning_count,
                "errors": error_count
            }
        }
    
    def save_raw_match_data(
        self, 
        match_id: str, 
        raw_data: Dict[str, Any],
        date_str: Optional[str] = None
    ) -> Path:
        """
        Save raw API response to Bronze layer.
        
        Bronze layer = One API call = One JSON file = Complete or Failed
        
        Args:
            match_id: Match ID
            raw_data: Raw API response (dict) - complete response from one API call
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            Path to saved file
        """
        try:
            # Normalize date to YYYYMMDD format
            if date_str:
                # Convert YYYY-MM-DD to YYYYMMDD if needed
                if len(date_str) == 10 and '-' in date_str:
                    date_str_normalized = date_str.replace('-', '')
                elif len(date_str) == 8 and date_str.isdigit():
                    date_str_normalized = date_str
                else:
                    raise ValueError(f"Invalid date format: {date_str}. Expected YYYYMMDD or YYYY-MM-DD")
            else:
                date_str_normalized = datetime.now().strftime("%Y%m%d")
            
            date_dir = self.matches_dir / date_str_normalized
            date_dir.mkdir(parents=True, exist_ok=True)
            
            # ATOMIC FILE WRITE: Write to temp, verify, then rename
            file_path = date_dir / f"match_{match_id}.json"
            temp_path = date_dir / f".match_{match_id}.json.tmp"
            scraped_at = datetime.now().isoformat()
            
            # Add metadata
            data_with_metadata = {
                "match_id": match_id,
                "scraped_at": scraped_at,
                "date": date_str_normalized,
                "data": raw_data
            }
            
            try:
                # Step 1: Write to temporary file
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(data_with_metadata, f, indent=2, ensure_ascii=False)
                
                # Step 2: Verify the file is valid JSON (can be read back)
                with open(temp_path, 'r', encoding='utf-8') as f:
                    json.load(f)  # Throws JSONDecodeError if corrupt
                
                # Step 3: Atomic rename (replaces existing file if present)
                # Use rename() which works reliably on Windows
                if file_path.exists():
                    file_path.unlink()  # Remove existing file first (Windows requirement)
                temp_path.rename(file_path)
                
                # Step 4: Get file size AFTER successful write
                file_size_kb = os.path.getsize(file_path) / 1024
                
                # Step 5: Record data lineage
                try:
                    self.lineage_tracker.record_scrape(
                        scraper="fotmob",
                        source="fotmob_api",
                        source_id=match_id,
                        date=date_str_normalized,
                        file_path=file_path,
                        metadata={
                            "file_size_kb": round(file_size_kb, 2),
                            "scraped_at": scraped_at
                        }
                    )
                except Exception as e:
                    self.logger.warning(f"Could not record lineage for {match_id}: {e}")
                
                self.logger.debug(f"Saved raw data for match {match_id} to {file_path} ({file_size_kb:.2f} KB)")
                return file_path
                
            except Exception as write_error:
                # Clean up temp file if it exists
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except:
                        pass
                raise write_error
            
        except Exception as e:
            # Failure tracking removed - use data lineage instead
            self.logger.error(f"Error saving raw data for match {match_id}: {e}", exc_info=True)
            raise
    
    def load_raw_match_data(
        self, 
        match_id: str,
        date_str: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Load raw API response from Bronze layer.
        Supports: tar archive, individual .json.gz, and .json files.
        
        Args:
            match_id: Match ID
            date_str: Optional date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            Raw API response dict, or None if not found
        """
        try:
            if date_str:
                # Normalize date to YYYYMMDD format
                if len(date_str) == 10 and '-' in date_str:
                    date_str_normalized = date_str.replace('-', '')
                elif len(date_str) == 8 and date_str.isdigit():
                    date_str_normalized = date_str
                else:
                    self.logger.warning(f"Invalid date format: {date_str}, trying as-is")
                    date_str_normalized = date_str
                
                date_dir = self.matches_dir / date_str_normalized
                
                # Try 1: Extract from tar archive containing .json.gz files (new format)
                archive_path = date_dir / f"{date_str_normalized}_matches.tar"
                if archive_path.exists():
                    try:
                        with tarfile.open(archive_path, 'r') as tar:
                            member_name = f"match_{match_id}.json.gz"
                            try:
                                member = tar.getmember(member_name)
                                f = tar.extractfile(member)
                                if f:
                                    # Decompress the .json.gz content
                                    with gzip.open(io.BytesIO(f.read()), 'rt', encoding='utf-8') as gz:
                                        data = json.load(gz)
                                    return data.get('data', data)
                            except KeyError:
                                # File not in archive
                                pass
                    except Exception as e:
                        self.logger.error(f"Error reading archive {archive_path}: {e}")
                
                # Try 2: Individual .json.gz file (old compressed format)
                file_path_gz = date_dir / f"match_{match_id}.json.gz"
                if file_path_gz.exists():
                    with gzip.open(file_path_gz, 'rt', encoding='utf-8') as f:
                        data = json.load(f)
                    return data.get('data', data)
                
                # Try 3: Individual .json file (uncompressed format)
                file_path = date_dir / f"match_{match_id}.json"
                if file_path.exists():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    return data.get('data', data)
                
                self.logger.warning(f"Raw data not found for match {match_id} in {date_str}")
                return None
            else:
                # Search without date - check archives and individual files
                # This is slower, so date_str should be provided when possible
                for date_dir in self.matches_dir.iterdir():
                    if not date_dir.is_dir():
                        continue
                    
                    # Try archive first (.tar containing .json.gz files)
                    archive_path = date_dir / f"{date_dir.name}_matches.tar"
                    if archive_path.exists():
                        try:
                            with tarfile.open(archive_path, 'r') as tar:
                                member_name = f"match_{match_id}.json.gz"
                                try:
                                    member = tar.getmember(member_name)
                                    f = tar.extractfile(member)
                                    if f:
                                        with gzip.open(io.BytesIO(f.read()), 'rt', encoding='utf-8') as gz:
                                            data = json.load(gz)
                                        return data.get('data', data)
                                except KeyError:
                                    continue
                        except Exception:
                            continue
                
                # Fall back to individual files
                matches_gz = list(self.matches_dir.rglob(f"match_{match_id}.json.gz"))
                matches = list(self.matches_dir.rglob(f"match_{match_id}.json"))
                
                if matches_gz:
                    with gzip.open(matches_gz[0], 'rt', encoding='utf-8') as f:
                        data = json.load(f)
                    return data.get('data', data)
                elif matches:
                    with open(matches[0], 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    return data.get('data', data)
                
                self.logger.warning(f"Raw data not found for match {match_id}")
                return None
            
        except Exception as e:
            self.logger.error(f"Error loading raw data for match {match_id}: {e}")
            return None
    
    def match_exists(self, match_id: str, date_str: Optional[str] = None) -> bool:
        """
        Check if raw data exists for a match.
        Checks archive, .json.gz, and .json files.
        
        Args:
            match_id: Match ID
            date_str: Optional date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            True if raw data exists, False otherwise
        """
        if date_str:
            # Normalize date to YYYYMMDD format
            if len(date_str) == 10 and '-' in date_str:
                date_str_normalized = date_str.replace('-', '')
            elif len(date_str) == 8 and date_str.isdigit():
                date_str_normalized = date_str
            else:
                date_str_normalized = date_str  # Try as-is
            
            date_dir = self.matches_dir / date_str_normalized
            
            # Check 1: In tar archive (containing .json.gz files)
            archive_path = date_dir / f"{date_str_normalized}_matches.tar"
            if archive_path.exists():
                try:
                    with tarfile.open(archive_path, 'r') as tar:
                        member_name = f"match_{match_id}.json.gz"
                        try:
                            tar.getmember(member_name)
                            return True
                        except KeyError:
                            pass
                except Exception:
                    pass
            
            # Check 2: Individual files
            file_path = date_dir / f"match_{match_id}.json"
            file_path_gz = date_dir / f"match_{match_id}.json.gz"
            return file_path.exists() or file_path_gz.exists()
        else:
            # Search all formats (slower without date)
            matches = list(self.matches_dir.rglob(f"match_{match_id}.json"))
            matches_gz = list(self.matches_dir.rglob(f"match_{match_id}.json.gz"))
            
            if matches or matches_gz:
                return True
            
            # Check archives (.tar containing .json.gz files)
            for date_dir in self.matches_dir.iterdir():
                if not date_dir.is_dir():
                    continue
                archive_path = date_dir / f"{date_dir.name}_matches.tar"
                if archive_path.exists():
                    try:
                        with tarfile.open(archive_path, 'r') as tar:
                            member_name = f"match_{match_id}.json.gz"
                            try:
                                tar.getmember(member_name)
                                return True
                            except KeyError:
                                continue
                    except Exception:
                        continue
            
            return False
    
    def save_daily_listing(self, date_str: str, match_ids: List[int]) -> Path:
        """
        Save daily listing of match IDs for a date with comprehensive metadata.
        Used to track games and prevent duplicate API requests.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
            match_ids: List of match IDs for the date
        
        Returns:
            Path to saved daily listing file
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            raise ValueError(f"Invalid date format: {date_str}. Expected YYYYMMDD or YYYY-MM-DD")
        
        date_dir = self.daily_listings_dir / date_str_normalized
        date_dir.mkdir(parents=True, exist_ok=True)
        
        listing_file = date_dir / "matches.json"
        
        # Gather storage statistics
        matches_date_dir = self.matches_dir / date_str_normalized
        storage_stats = self._get_storage_stats(date_str_normalized, match_ids, matches_date_dir)
        
        listing_data = {
            "date": date_str_normalized,
            "scraped_at": datetime.now().isoformat(),
            "match_ids": [int(mid) for mid in match_ids],
            "total_matches": len(match_ids),
            "storage": storage_stats
        }
        
        # Atomic write
        temp_file = date_dir / ".matches.json.tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(listing_data, f, indent=2, ensure_ascii=False)
            temp_file.replace(listing_file)
            self.logger.info(
                f"Saved daily listing for {date_str_normalized}: {len(match_ids)} matches, "
                f"{storage_stats['files_stored']} files stored ({storage_stats['total_size_mb']:.2f} MB)"
            )
            return listing_file
        except Exception as e:
            if temp_file.exists():
                temp_file.unlink()
            raise
    
    def _get_storage_stats(
        self, 
        date_str: str, 
        match_ids: List[int], 
        matches_date_dir: Path
    ) -> Dict[str, Any]:
        """
        Gather comprehensive storage statistics for a date.
        
        Args:
            date_str: Date string in YYYYMMDD format
            match_ids: List of match IDs
            matches_date_dir: Path to matches directory for the date
        
        Returns:
            Dictionary with storage statistics
        """
        stats = {
            "files_stored": 0,
            "files_missing": 0,
            "total_size_bytes": 0,
            "total_size_mb": 0.0,
            "files_in_archive": 0,
            "files_individual": 0,
            "archive_size_bytes": 0,
            "archive_size_mb": 0.0,
            "scraped_match_ids": [],
            "missing_match_ids": []
        }
        
        if not matches_date_dir.exists():
            stats["files_missing"] = len(match_ids)
            stats["missing_match_ids"] = [int(mid) for mid in match_ids]
            return stats
        
        # Check for tar archive
        archive_path = matches_date_dir / f"{date_str}_matches.tar"
        archived_match_ids = set()
        if archive_path.exists():
            try:
                stats["archive_size_bytes"] = archive_path.stat().st_size
                stats["archive_size_mb"] = stats["archive_size_bytes"] / (1024 * 1024)
                
                # Create a set of match IDs we're looking for (for faster lookup)
                match_ids_set = {int(mid) for mid in match_ids}
                
                with tarfile.open(archive_path, 'r') as tar:
                    for member in tar.getmembers():
                        if member.name.startswith("match_") and member.name.endswith(".json.gz"):
                            # Extract match ID from filename
                            match_id_str = member.name.replace("match_", "").replace(".json.gz", "")
                            try:
                                match_id = int(match_id_str)
                                # Only count if this match_id is in our list
                                if match_id in match_ids_set:
                                    archived_match_ids.add(match_id)
                                    stats["files_in_archive"] += 1
                                    stats["total_size_bytes"] += member.size
                            except ValueError:
                                pass
            except Exception as e:
                self.logger.warning(f"Error reading archive {archive_path}: {e}")
        
        # Check individual files
        for match_id in match_ids:
            match_id_int = int(match_id)
            file_path = matches_date_dir / f"match_{match_id_int}.json"
            file_path_gz = matches_date_dir / f"match_{match_id_int}.json.gz"
            
            found = False
            if match_id_int in archived_match_ids:
                found = True
                stats["scraped_match_ids"].append(match_id_int)
            elif file_path.exists():
                found = True
                stats["files_individual"] += 1
                file_size = file_path.stat().st_size
                stats["total_size_bytes"] += file_size
                stats["scraped_match_ids"].append(match_id_int)
            elif file_path_gz.exists():
                found = True
                stats["files_individual"] += 1
                file_size = file_path_gz.stat().st_size
                stats["total_size_bytes"] += file_size
                stats["scraped_match_ids"].append(match_id_int)
            
            if not found:
                stats["files_missing"] += 1
                stats["missing_match_ids"].append(match_id_int)
        
        stats["files_stored"] = stats["files_in_archive"] + stats["files_individual"]
        stats["total_size_mb"] = stats["total_size_bytes"] / (1024 * 1024)
        
        # Calculate completion percentage
        if len(match_ids) > 0:
            stats["completion_percentage"] = round(
                (stats["files_stored"] / len(match_ids)) * 100, 2
            )
        else:
            stats["completion_percentage"] = 0.0
        
        return stats
    
    def load_daily_listing(self, date_str: str) -> Optional[Dict[str, Any]]:
        """
        Load daily listing of match IDs for a date.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            Dictionary with daily listing data, or None if not found
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            self.logger.warning(f"Invalid date format: {date_str}")
            return None
        
        listing_file = self.daily_listings_dir / date_str_normalized / "matches.json"
        
        if not listing_file.exists():
            return None
        
        try:
            with open(listing_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading daily listing for {date_str_normalized}: {e}")
            return None
    
    def get_match_ids_for_date(self, date_str: str) -> List[int]:
        """
        Get list of match IDs for a date from daily listing.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            List of match IDs, or empty list if not found
        """
        listing = self.load_daily_listing(date_str)
        if listing:
            return listing.get('match_ids', [])
        return []
    
    def daily_listing_exists(self, date_str: str) -> bool:
        """
        Check if daily listing exists for a date.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            True if daily listing exists, False otherwise
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            return False
        
        listing_file = self.daily_listings_dir / date_str_normalized / "matches.json"
        return listing_file.exists()
    
    def mark_match_as_scraped(self, match_id: str, date_str: str) -> bool:
        """
        Update daily listing file to mark a match as scraped.
        Moves match_id from missing_match_ids to scraped_match_ids.
        
        Args:
            match_id: Match ID to mark as scraped
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
        
        Returns:
            True if update was successful, False otherwise
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            self.logger.warning(f"Invalid date format: {date_str}")
            return False
        
        listing_file = self.daily_listings_dir / date_str_normalized / "matches.json"
        
        if not listing_file.exists():
            self.logger.debug(f"Daily listing file not found: {listing_file}")
            return False
        
        try:
            # Load existing data
            with open(listing_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            match_id_int = int(match_id)
            
            # Initialize storage section if it doesn't exist
            if 'storage' not in data:
                data['storage'] = {}
            storage = data['storage']
            
            # Initialize lists if they don't exist
            if 'missing_match_ids' not in storage:
                storage['missing_match_ids'] = []
            if 'scraped_match_ids' not in storage:
                storage['scraped_match_ids'] = []
            
            # Remove from missing_match_ids if present
            if match_id_int in storage['missing_match_ids']:
                storage['missing_match_ids'].remove(match_id_int)
            
            # Add to scraped_match_ids if not already present
            if match_id_int not in storage['scraped_match_ids']:
                storage['scraped_match_ids'].append(match_id_int)
            
            # Recalculate storage statistics after marking match as scraped
            try:
                # Get all match IDs from the listing
                all_match_ids = data.get('match_ids', [])
                if not all_match_ids:
                    # Fallback: try to get from matches list if available
                    matches = data.get('matches', [])
                    all_match_ids = [m.get('match_id') for m in matches if m.get('match_id')]
                
                if all_match_ids:
                    # Convert to list of integers for _get_storage_stats
                    match_ids_int = [int(mid) for mid in all_match_ids]
                    matches_date_dir = self.matches_dir / date_str_normalized
                    storage_stats = self._get_storage_stats(date_str_normalized, match_ids_int, matches_date_dir)
                    
                    # Update storage statistics
                    storage.update({
                        'files_stored': storage_stats['files_stored'],
                        'files_missing': storage_stats['files_missing'],
                        'total_size_bytes': storage_stats['total_size_bytes'],
                        'total_size_mb': storage_stats['total_size_mb'],
                        'files_in_archive': storage_stats['files_in_archive'],
                        'files_individual': storage_stats['files_individual'],
                        'archive_size_bytes': storage_stats['archive_size_bytes'],
                        'archive_size_mb': storage_stats['archive_size_mb'],
                        'completion_percentage': storage_stats.get('completion_percentage', 0.0)
                    })
            except Exception as e:
                self.logger.warning(f"Could not update storage statistics: {e}")
            
            # Atomic write
            temp_file = listing_file.parent / ".matches.json.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Verify JSON is valid
            with open(temp_file, 'r', encoding='utf-8') as f:
                json.load(f)  # Will raise if invalid
            
            # Atomic rename (Windows-safe)
            if listing_file.exists():
                listing_file.unlink()
            temp_file.rename(listing_file)
            
            self.logger.debug(f"Updated daily listing: match {match_id} marked as scraped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating daily listing for match {match_id}: {e}")
            # Clean up temp file if it exists
            temp_file = listing_file.parent / ".matches.json.tmp"
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
            return False
    
    def compress_date_files(self, date_str: str, force: bool = False) -> Dict[str, Any]:
        """
        Compress all JSON files for a specific date:
        Step 1: Compress each .json to .json.gz (GZIP compression)
        Step 2: Bundle all .json.gz into ONE tar archive
        Step 3: Delete individual .json.gz files
        
        Final result: ONE tar file containing all compressed .json.gz files
        
        RESUMABLE: If archive already exists, skips compression unless force=True.
        
        Args:
            date_str: Date string in YYYYMMDD format (or YYYY-MM-DD, will be converted)
            force: If True, recompress even if archive exists (default: False)
        
        Returns:
            Dictionary with compression statistics and processing log
        """
        # Normalize date to YYYYMMDD format
        if len(date_str) == 10 and '-' in date_str:
            date_str_normalized = date_str.replace('-', '')
        elif len(date_str) == 8 and date_str.isdigit():
            date_str_normalized = date_str
        else:
            raise ValueError(f"Invalid date format: {date_str}. Expected YYYYMMDD or YYYY-MM-DD")
        
        date_str = date_str_normalized
        
        date_dir = self.matches_dir / date_str
        archive_path = date_dir / f"{date_str}_matches.tar"
        
        # RESUMABILITY: Check if already compressed
        if not force and archive_path.exists():
            archive_size_mb = archive_path.stat().st_size / (1024 * 1024)
            self.logger.info(f"Archive already exists: {archive_path} ({archive_size_mb:.2f} MB), skipping compression")
            return {
                "compressed": 0,
                "size_before_mb": 0,
                "size_after_mb": archive_size_mb,
                "saved_mb": 0,
                "saved_pct": 0,
                "archive_file": str(archive_path),
                "status": "already_compressed"
            }
        
        if not date_dir.exists():
            self.logger.warning(f"Date directory not found: {date_dir}")
            return {"compressed": 0, "size_before_mb": 0, "size_after_mb": 0, "archive_file": None, "status": "no_directory"}
        
        json_files = list(date_dir.glob("*.json"))
        existing_gz_files = list(date_dir.glob("*.json.gz"))
        
        # Calculate total size before
        all_files = json_files + existing_gz_files
        if not all_files:
            self.logger.info(f"No files to compress in {date_dir}")
            return {"compressed": 0, "size_before_mb": 0, "size_after_mb": 0, "archive_file": None, "status": "no_files"}
        
        total_before = sum(f.stat().st_size for f in all_files)
        
        try:
            # STEP 1: Compress .json files to .json.gz (if any exist)
            gz_files = list(existing_gz_files)  # Start with existing .json.gz
            
            if json_files:
                self.logger.info(f"Step 1: Compressing {len(json_files)} .json files to .json.gz...")
                for json_file in json_files:
                    # Read original
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Write compressed (compact format, no indentation)
                    gz_file = json_file.with_suffix('.json.gz')
                    with gzip.open(gz_file, 'wt', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False)
                    
                    gz_files.append(gz_file)
                    
                    # Remove original JSON file
                    json_file.unlink()
            else:
                self.logger.info(f"Step 1: Using {len(existing_gz_files)} existing .json.gz files...")
            
            self.logger.info(f"Step 2: Bundling {len(gz_files)} .json.gz files into single tar archive...")
            
            # STEP 2: Create tar archive with all .json.gz files (NO additional compression)
            archive_path = date_dir / f"{date_str}_matches.tar"
            
            with tarfile.open(archive_path, 'w') as tar:  # 'w' = no compression, just tar
                for gz_file in gz_files:
                    # Add .json.gz file to archive with just the filename
                    tar.add(gz_file, arcname=gz_file.name)
            
            # Get final archive size
            total_after = archive_path.stat().st_size
            
            # TAR VERIFICATION: Verify archive integrity BEFORE deleting originals
            self.logger.info(f"Verifying tar archive integrity...")
            try:
                with tarfile.open(archive_path, 'r') as tar:
                    # Get list of files in archive
                    tar_members = {m.name for m in tar.getmembers()}
                    expected_files = {f.name for f in gz_files}
                    
                    # Check all expected files are in tar
                    if tar_members != expected_files:
                        missing = expected_files - tar_members
                        extra = tar_members - expected_files
                        error_msg = []
                        if missing:
                            error_msg.append(f"Missing files in tar: {missing}")
                        if extra:
                            error_msg.append(f"Unexpected files in tar: {extra}")
                        raise ValueError(f"Tar archive incomplete: {'; '.join(error_msg)}")
                    
                    # Spot-check: Verify a few files can be extracted
                    import random
                    # Only verify files that still exist (in case of race conditions)
                    existing_gz_files = [f for f in gz_files if f.exists()]
                    if existing_gz_files:
                        sample_size = min(3, len(existing_gz_files))
                        sample_files = random.sample(existing_gz_files, sample_size)
                        
                        for gz_file in sample_files:
                            try:
                                member = tar.getmember(gz_file.name)
                                extracted_data = tar.extractfile(member)
                                if extracted_data is None:
                                    raise ValueError(f"Could not extract {gz_file.name} from tar")
                                # Try to read a bit to ensure it's readable
                                extracted_data.read(100)
                            except Exception as extract_error:
                                raise ValueError(f"Failed to verify {gz_file.name} in tar: {extract_error}")
                    else:
                        self.logger.warning("No existing .json.gz files found for spot-check verification")
                
                self.logger.info(f"[OK] Tar archive verified: {len(tar_members)} files intact")
                
            except Exception as verify_error:
                # Archive is bad! Delete it and keep originals
                self.logger.error(f"Tar verification failed: {verify_error}")
                if archive_path.exists():
                    archive_path.unlink()
                    self.logger.info("Deleted corrupt tar archive, kept original files")
                raise Exception(f"Tar archive verification failed: {verify_error}")
            
            # STEP 3: ONLY NOW delete individual .json.gz files (archive is verified)
            self.logger.info(f"Step 3: Cleaning up {len(gz_files)} individual .json.gz files...")
            deleted_count = 0
            for gz_file in gz_files:
                if gz_file.exists():
                    try:
                        gz_file.unlink()
                        deleted_count += 1
                    except Exception as e:
                        self.logger.warning(f"Could not delete {gz_file.name}: {e}")
                else:
                    self.logger.debug(f"File {gz_file.name} already deleted or missing, skipping")
            
            if deleted_count < len(gz_files):
                self.logger.info(f"Deleted {deleted_count} of {len(gz_files)} files (some were already missing)")
            
            # Calculate savings
            size_before_mb = total_before / (1024 * 1024)
            size_after_mb = total_after / (1024 * 1024)
            saved_mb = size_before_mb - size_after_mb
            saved_pct = ((total_before - total_after) / max(total_before, 1)) * 100
            
            self.logger.info(
                f"[OK] Archive complete for {date_str}: "
                f"{len(gz_files)} files -> {archive_path.name}, "
                f"{size_before_mb:.2f} MB -> {size_after_mb:.2f} MB "
                f"(saved {saved_mb:.2f} MB, {saved_pct:.1f}%)"
            )
            
            return {
                "compressed": len(gz_files),
                "size_before_mb": round(size_before_mb, 2),
                "size_after_mb": round(size_after_mb, 2),
                "saved_mb": round(saved_mb, 2),
                "saved_pct": round(saved_pct, 1),
                "archive_file": str(archive_path),
                "status": "completed"
            }
            
        except Exception as e:
            self.logger.error(f"Error during compression for {date_str}: {e}")
            # Clean up partial files
            archive_path = date_dir / f"{date_str}_matches.tar"
            if archive_path.exists():
                archive_path.unlink()
            return {"compressed": 0, "size_before_mb": 0, "size_after_mb": 0, "archive_file": None, "status": "error", "error": str(e)}

