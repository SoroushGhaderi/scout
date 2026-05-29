"""FotMob gold aggregation definitions."""

from pathlib import Path
from typing import List


class FotMobGoldProcessor:
    """Resolve gold SQL aggregation files for FotMob."""

    def __init__(self, sql_dir: Path):
        self.sql_dir = Path(sql_dir)

    def sql_files(self) -> List[Path]:
        """Return ordered non-DDL SQL files for gold load processing."""
        def _is_ddl(path: Path) -> bool:
            name = path.name.lower()
            return (
                name.startswith("create_")
                or name.startswith("00_")
                or name.startswith("01_")
                or "_create_" in name
            )

        return sorted(
            path
            for path in self.sql_dir.glob("*.sql")
            if not path.name.startswith("scenario_")
            and not _is_ddl(path)
        )
