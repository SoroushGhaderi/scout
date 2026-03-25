"""FotMob silver transformation definitions."""

from pathlib import Path
from typing import List


class FotMobSilverProcessor:
    """Resolve silver SQL transformation files for FotMob."""

    def __init__(self, sql_dir: Path):
        self.sql_dir = Path(sql_dir)

    def sql_files(self) -> List[Path]:
        """Return ordered SQL files for silver transformations."""
        return sorted(self.sql_dir.glob("*.sql"))
