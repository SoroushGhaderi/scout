"""Layered entrypoint for full medallion pipeline orchestration."""

from pathlib import Path
import runpy


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    runpy.run_path(str(root / "scripts" / "pipeline.py"), run_name="__main__")

