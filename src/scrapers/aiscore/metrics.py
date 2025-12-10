"""Metrics collection and monitoring."""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ScrapingMetrics:
    """Tracks scraping metrics and performance."""

    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

    pages_scraped: int = 0
    links_found: int = 0
    links_inserted: int = 0
    duplicates_prevented: int = 0
    errors_encountered: int = 0

    total_scroll_time: float = 0.0
    total_extraction_time: float = 0.0
    scroll_count: int = 0

    def duration(self) -> float:
        """Get total duration in seconds."""
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()

    def links_per_second(self) -> float:
        """Calculate throughput."""
        duration = self.duration()
        return self.links_found / duration if duration > 0 else 0

    def avg_scroll_time(self) -> float:
        """Average time per scroll."""
        return self.total_scroll_time / self.scroll_count if self.scroll_count > 0 else 0

    def success_rate(self) -> float:
        """Success rate percentage."""
        total_operations = self.links_found + self.errors_encountered
        return (self.links_found / total_operations * 100) if total_operations > 0 else 0

    def to_dict(self) -> dict:
        """Convert metrics to dictionary."""
        return {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration(),
            'pages_scraped': self.pages_scraped,
            'links_found': self.links_found,
            'links_inserted': self.links_inserted,
            'duplicates_prevented': self.duplicates_prevented,
            'errors': self.errors_encountered,
            'throughput_links_per_sec': self.links_per_second(),
            'avg_scroll_time': self.avg_scroll_time(),
            'success_rate': self.success_rate(),
            'scroll_count': self.scroll_count,
        }

    def to_json(self, indent: int = 2) -> str:
        """Export metrics as JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def save_to_file(self, filepath: str):
        """Save metrics to JSON file."""
        try:
            path = Path(filepath)
            path.parent.mkdir(parents=True, exist_ok=True)

            with open(path, 'w', encoding='utf-8') as f:
                f.write(self.to_json())

            logger.info(f"Metrics saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save metrics: {e}")

    def log_summary(self):
        """Log comprehensive summary."""
        logger.info("=" * 80)
        logger.info("SCRAPING METRICS SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Duration: {self.duration():.2f}s")
        logger.info(f"Pages scraped: {self.pages_scraped}")
        logger.info(f"Links found: {self.links_found}")
        logger.info(f"Links inserted: {self.links_inserted}")
        logger.info(f"Duplicates prevented: {self.duplicates_prevented}")
        logger.info(f"Errors: {self.errors_encountered}")
        logger.info(f"Throughput: {self.links_per_second():.2f} links/sec")
        logger.info(f"Success rate: {self.success_rate():.1f}%")
        logger.info(f"Avg scroll time: {self.avg_scroll_time():.3f}s")
        logger.info("=" * 80)


class MetricsContext:
    """Context manager for metrics collection."""

    def __init__(self, export_path: Optional[str] = None):
        self.metrics = ScrapingMetrics()
        self.export_path = export_path

    def __enter__(self) -> ScrapingMetrics:
        return self.metrics

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.metrics.end_time = datetime.now()
        self.metrics.log_summary()

        if self.export_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.export_path}/metrics_{timestamp}.json"
            self.metrics.save_to_file(filename)

        return False
