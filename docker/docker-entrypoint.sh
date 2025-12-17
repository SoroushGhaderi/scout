#!/bin/bash
# Docker entrypoint script for the scraper container

set -e

# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse to be ready..."
until python -c "import requests; requests.get('http://clickhouse:8123/ping')" 2>/dev/null; do
  echo "ClickHouse is unavailable - sleeping"
  sleep 2
done

echo "ClickHouse is ready!"

# Check and create databases/tables if they don't exist
echo "Checking ClickHouse databases..."
python scripts/setup_clickhouse.py
if [ $? -eq 0 ]; then
  echo "✓ Database check completed"
else
  echo "⚠ Warning: Database check/creation may have failed. Check logs for details."
fi

# Execute the command passed to the container
exec "$@"
