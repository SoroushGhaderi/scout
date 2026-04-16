"""FotMob gold aggregation definitions."""

from pathlib import Path
from typing import List


class FotMobGoldProcessor:
    """Resolve gold SQL aggregation files for FotMob."""

    def __init__(self, sql_dir: Path):
        self.sql_dir = Path(sql_dir)

    def sql_files(self) -> List[Path]:
        """Return ordered non-DDL SQL files for gold load processing."""
        return sorted(
            path
            for path in self.sql_dir.glob("*.sql")
            if not path.name.startswith("scenario_")
            and "_create_" not in path.name
        )
