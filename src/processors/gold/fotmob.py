"""FotMob gold aggregation definitions."""

from pathlib import Path
from typing import List


class FotMobGoldProcessor:
    """Resolve gold SQL aggregation files for FotMob."""

    def __init__(self, sql_dir: Path):
        self.sql_dir = Path(sql_dir)

    def sql_files(self) -> List[Path]:
        """Return ordered SQL files for gold aggregations."""
        return sorted(
            path for path in self.sql_dir.glob("*.sql") if not path.name.startswith("scenario_")
        )
