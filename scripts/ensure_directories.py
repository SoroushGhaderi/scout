"""Ensure required directories exist.

This script creates the necessary directory structure for the Scout project.
It's optional since the pipeline scripts create directories automatically,
but can be useful for pre-initialization or troubleshooting.

Usage:
    python scripts/ensure_directories.py
"""

from pathlib import Path
import sys


def ensure_directories() -> None:
    """Create required directories if they don't exist."""
    
    # Get project root (parent of scripts directory)
    project_root = Path(__file__).parent.parent
    
    # Required directories
    directories = [
        project_root / "data",
        project_root / "data" / "fotmob",
        project_root / "data" / "fotmob" / "daily",
        project_root / "data" / "fotmob" / "matches",
        project_root / "data" / "aiscore",
        project_root / "data" / "aiscore" / "daily_listings",
        project_root / "data" / "aiscore" / "matches",
        project_root / "logs",
    ]
    
    print("Creating required directories...")
    print(f"Project root: {project_root}")
    print()
    
    created = []
    existed = []
    
    for directory in directories:
        if directory.exists():
            existed.append(directory)
            print(f"  ✓ {directory.relative_to(project_root)} (already exists)")
        else:
            directory.mkdir(parents=True, exist_ok=True)
            created.append(directory)
            print(f"  + {directory.relative_to(project_root)} (created)")
    
    print()
    print("=" * 60)
    print(f"Summary:")
    print(f"  Created: {len(created)} directories")
    print(f"  Existed: {len(existed)} directories")
    print(f"  Total:   {len(directories)} directories")
    print("=" * 60)
    
    if created:
        print()
        print("✓ Directory structure initialized successfully!")
    else:
        print()
        print("✓ All directories already exist.")


def main() -> int:
    """Main execution function."""
    try:
        ensure_directories()
        return 0
    except Exception as e:
        print(f"Error creating directories: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
