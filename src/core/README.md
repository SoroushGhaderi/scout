# Core Package

The `core` package provides foundational components for the Scout project.

## Overview

This package contains interfaces, exceptions, constants, and type definitions that are used throughout the application. It establishes contracts and standards that all other modules should follow.

## Modules

### 📋 `interfaces.py`

Protocol definitions for all major components using Python's `Protocol` typing.

**Protocols Defined:**
- `StorageProtocol` - Interface for storage implementations (Bronze/Silver/Gold)
- `ScraperProtocol` - Interface for scraper implementations (FotMob/AIScore)
- `ProcessorProtocol` - Interface for data processors
- `OrchestratorProtocol` - Interface for orchestration logic
- `ConfigProtocol` - Interface for configuration classes
- `CacheProtocol` - Interface for cache implementations
- `MetricsProtocol` - Interface for metrics tracking
- `LoggerProtocol` - Interface for logging

**Usage:**
```python
from src.core import StorageProtocol, ScraperProtocol

# Type hints with protocols
def process_data(storage: StorageProtocol, scraper: ScraperProtocol):
    match_data = scraper.fetch_match_details("12345")
    storage.save_match("12345", match_data, "20241218")
```

**Benefits:**
- Structural typing (duck typing with type safety)
- Easy to swap implementations for testing
- Clear contracts for components
- Better IDE autocomplete and type checking

---

### ⚠️ `exceptions.py`

Custom exception hierarchy for the entire application.

**Exception Hierarchy:**
```
ScoutError (base)
├── ConfigurationError
├── StorageError
│   ├── StorageReadError
│   ├── StorageWriteError
│   └── StorageNotFoundError
├── ScraperError
│   ├── ScraperConnectionError
│   ├── ScraperTimeoutError
│   ├── ScraperRateLimitError
│   └── ScraperParseError
├── ProcessorError
│   └── ValidationError
├── DatabaseError
│   ├── DatabaseConnectionError
│   └── DatabaseQueryError
└── OrchestratorError
```

**Usage:**
```python
from src.core import StorageError, ScraperError

try:
    storage.save_match(match_id, data, date)
except StorageWriteError as e:
    logger.error(f"Failed to save: {e}")
    logger.debug(f"Details: {e.to_dict()}")
except StorageError as e:
    logger.error(f"Storage error: {e}")
```

**Features:**
- Structured error information with `details` dict
- `to_dict()` method for serialization
- Rich context in error messages
- Easy error categorization

---

### 🔢 `constants.py`

Project-wide constants and enumerations.

**Categories:**
- Date and time formats
- HTTP status codes
- File extensions
- Match statuses
- Scraper names
- Storage layers
- Default values
- Environment variable names
- Regex patterns
- Table names
- Error messages

**Usage:**
```python
from src.core.constants import (
    MatchStatus,
    Defaults,
    HttpStatus,
    DATE_FORMAT_COMPACT,
)

# Use constants instead of magic values
if response.status_code == HttpStatus.RATE_LIMITED:
    time.sleep(Defaults.RETRY_INITIAL_WAIT)

if match_status in MatchStatus.COMPLETED_STATUSES:
    process_completed_match(match)
```

**Benefits:**
- No magic numbers or strings
- Single source of truth
- Easy to update values
- Improved code readability

---

### 📝 `types.py`

Common type definitions and type aliases.

**Type Categories:**
- Basic aliases (`MatchID`, `DateStr`, `URL`)
- Data structures (`JSONDict`, `Headers`)
- Status types (`MatchStatusType`, `ScrapeStatus`)
- Structured types (`TeamData`, `MatchMetadata`)
- Function return types
- Type validators

**Usage:**
```python
from src.core.types import (
    MatchID,
    DateStr,
    JSONDict,
    ScraperMetrics,
    is_valid_date_str,
)

def scrape_match(match_id: MatchID, date: DateStr) -> JSONDict:
    """Type-safe function signature."""
    if not is_valid_date_str(date):
        raise ValueError(f"Invalid date: {date}")
    ...

# Structured types with TypedDict
metrics: ScraperMetrics = {
    'total_matches': 100,
    'successful_matches': 95,
    'failed_matches': 5,
    'duration_seconds': 123.45,
}
```

**Benefits:**
- Better IDE autocomplete
- Type checking with mypy
- Clear function signatures
- Structured data validation

---

## Integration Examples

### Example 1: Implementing a New Storage

```python
from src.core import StorageProtocol
from src.core.exceptions import StorageError, StorageWriteError
from src.core.types import MatchID, DateStr, JSONDict
from pathlib import Path

class RedisStorage(StorageProtocol):
    """Redis-based storage implementation."""
    
    def save_match(
        self,
        match_id: MatchID,
        data: JSONDict,
        date: DateStr
    ) -> Path:
        try:
            key = f"match:{date}:{match_id}"
            self.redis.set(key, json.dumps(data))
            return Path(f"redis://{key}")
        except Exception as e:
            raise StorageWriteError(
                f"Failed to save match {match_id}",
                details={'date': date, 'error': str(e)}
            )
    
    def load_match(
        self,
        match_id: MatchID,
        date: Optional[DateStr] = None
    ) -> Optional[JSONDict]:
        # Implementation...
        pass
    
    def match_exists(
        self,
        match_id: MatchID,
        date: Optional[DateStr] = None
    ) -> bool:
        # Implementation...
        pass
```

### Example 2: Using Constants and Types

```python
from src.core.constants import MatchStatus, Defaults, HttpStatus
from src.core.types import MatchStatusType, ScraperMetrics
from src.core.exceptions import ScraperRateLimitError

def fetch_with_retry(url: str, max_retries: int = Defaults.MAX_RETRIES):
    """Fetch URL with automatic retry."""
    for attempt in range(max_retries):
        response = requests.get(url, timeout=Defaults.HTTP_TIMEOUT)
        
        if response.status_code == HttpStatus.RATE_LIMITED:
            raise ScraperRateLimitError(
                "Rate limit exceeded",
                details={'retry_after': response.headers.get('Retry-After')}
            )
        
        if response.status_code == HttpStatus.OK:
            return response.json()
    
    return None

def filter_completed_matches(matches: list) -> list:
    """Filter for completed matches only."""
    return [
        match for match in matches
        if match['status'] in MatchStatus.COMPLETED_STATUSES
    ]
```

### Example 3: Type-Safe Configuration

```python
from src.core import ConfigProtocol, ValidationError
from src.core.types import LogLevel, ValidationResult

class MyConfig(ConfigProtocol):
    """Custom configuration with validation."""
    
    def __init__(self):
        self.log_level: LogLevel = "INFO"
        self.timeout: int = 30
    
    def validate(self) -> ValidationResult:
        """Validate configuration values."""
        errors = []
        
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level not in valid_levels:
            errors.append(f"Invalid log_level: {self.log_level}")
        
        if self.timeout <= 0:
            errors.append(f"timeout must be positive: {self.timeout}")
        
        return errors
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            'log_level': self.log_level,
            'timeout': self.timeout,
        }
```

---

## Testing with Protocols

Protocols make testing easier by allowing mock implementations:

```python
from src.core import StorageProtocol, ScraperProtocol
from src.core.types import MatchID, DateStr, JSONDict

class MockStorage(StorageProtocol):
    """Mock storage for testing."""
    
    def __init__(self):
        self.data = {}
    
    def save_match(self, match_id: MatchID, data: JSONDict, date: DateStr):
        self.data[match_id] = data
        return Path(f"/mock/{date}/{match_id}.json")
    
    def load_match(self, match_id: MatchID, date: DateStr = None):
        return self.data.get(match_id)
    
    def match_exists(self, match_id: MatchID, date: DateStr = None):
        return match_id in self.data

# Use in tests
def test_scraper():
    mock_storage = MockStorage()
    scraper = MyScraper(storage=mock_storage)
    
    scraper.scrape_match("12345", "20241218")
    assert mock_storage.match_exists("12345")
```

---

## Best Practices

1. **Always use protocols for type hints** instead of concrete classes
2. **Catch specific exceptions** instead of broad `Exception`
3. **Use constants** instead of hardcoded values
4. **Define type aliases** for complex types
5. **Validate data** using type guards from `types.py`
6. **Provide rich error context** using `details` parameter
7. **Document protocol implementations** with docstrings

---

## Migration Guide

### Before (Without Core):
```python
def save_match(match_id: str, data: dict, date: str) -> str:
    if response.status_code == 429:  # Magic number
        time.sleep(2)  # Magic number
        raise Exception("Rate limited")  # Generic exception
    
    if match_status == "Finished":  # Magic string
        process_match(data)
```

### After (With Core):
```python
from src.core import StorageProtocol, ScraperRateLimitError
from src.core.constants import HttpStatus, Defaults, MatchStatus
from src.core.types import MatchID, DateStr, JSONDict
from pathlib import Path

def save_match(
    match_id: MatchID,
    data: JSONDict,
    date: DateStr
) -> Path:
    if response.status_code == HttpStatus.RATE_LIMITED:
        time.sleep(Defaults.RETRY_INITIAL_WAIT)
        raise ScraperRateLimitError(
            "Rate limit exceeded",
            details={'match_id': match_id, 'date': date}
        )
    
    if match_status == MatchStatus.FINISHED:
        process_match(data)
```

---

## Contributing

When adding new components:

1. Define protocols in `interfaces.py` first
2. Add exceptions to appropriate category in `exceptions.py`
3. Add constants to appropriate section in `constants.py`
4. Define type aliases in `types.py` if needed
5. Update `__init__.py` exports
6. Update this README

---

## Version

Current version: **1.0.0**
