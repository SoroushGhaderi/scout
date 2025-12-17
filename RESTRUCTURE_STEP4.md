# Project Restructuring - Step 4: Move Docker Files

**Date:** December 18, 2025  
**Status:** ✅ COMPLETED

## Summary

Moved all Docker-related files from the project root to a dedicated `docker/` directory for better organization and cleaner project structure.

## Changes Made

### 1. Created New Docker Directory

- **New Directory:** `docker/`
- Contains all Docker-related configuration files

### 2. Moved Docker Files (3 files)

#### Dockerfile
```
OLD: ./Dockerfile
NEW: docker/Dockerfile
```

**Path Updates:**
- `COPY requirements.txt .` → `COPY ../requirements.txt .`
- `COPY src/ ./src/` → `COPY --chown=appuser:appuser ../src/ ./src/`
- `COPY scripts/ ./scripts/` → `COPY --chown=appuser:appuser ../scripts/ ./scripts/`
- `COPY config/ ./config/` → `COPY --chown=appuser:appuser ../config/ ./config/`
- `COPY setup.py ./` → `COPY --chown=appuser:appuser ../setup.py ./`
- `COPY pytest.ini ./` → `COPY --chown=appuser:appuser ../pytest.ini ./`

#### docker-compose.yml
```
OLD: ./docker-compose.yml
NEW: docker/docker-compose.yml
```

**Path Updates:**
- `context: .` → `context: ..` (build context)
- `dockerfile: Dockerfile` → `dockerfile: docker/Dockerfile`
- All volume mounts updated:
  - `./data:/app/data` → `../data:/app/data`
  - `./logs:/app/logs` → `../logs:/app/logs`
  - `./config:/app/config:ro` → `../config:/app/config:ro`
  - `./scripts:/app/scripts` → `../scripts:/app/scripts`
  - `./src:/app/src` → `../src:/app/src`
  - `./clickhouse:/app/clickhouse:ro` → `../clickhouse:/app/clickhouse:ro`
  - `./crontab:/etc/cron.d/scraper-cron:ro` → `../crontab:/etc/cron.d/scraper-cron:ro`
- Environment file: `.env` → `../.env`

#### docker-entrypoint.sh
```
OLD: ./docker-entrypoint.sh
NEW: docker/docker-entrypoint.sh
```

**No path changes needed** (uses runtime paths inside container)

### 3. Updated Makefile

Added `COMPOSE_FILE` variable and updated all docker-compose commands:

```makefile
# OLD
docker-compose up -d

# NEW
COMPOSE_FILE = -f docker/docker-compose.yml
docker-compose $(COMPOSE_FILE) up -d
```

**Updated Commands:**
- `build`, `up`, `down`, `restart`
- `logs`, `logs-scraper`, `logs-clickhouse`
- `shell`, `init-dirs`, `clickhouse-client`
- `rebuild`, `clean`, `clean-all`
- `scrape-fotmob`, `scrape-aiscore`
- `load-fotmob`, `load-aiscore`
- `optimize-tables`, `status`, `health`
- `dev`, `test`

### 4. Updated Documentation (2 files)

#### README.md

Updated all docker-compose commands to use `-f docker/docker-compose.yml`:

```bash
# OLD
docker-compose up -d

# NEW
docker-compose -f docker/docker-compose.yml up -d
# Or use: make up
```

**Updated Sections:**
- Quick Start
- Installation
- Usage (Pipeline, Individual Scrapers)
- ClickHouse Access
- Table Optimization
- Project Structure (added docker/ directory)
- Troubleshooting
- Monitoring

#### DEVELOPMENT.md

Updated docker-compose references:

```bash
# OLD
docker-compose exec scraper python scripts/pipeline.py 20251208

# NEW
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208
```

**Updated Sections:**
- ClickHouse Optimization
- Common Commands
- Testing (Health Checks)

## Architecture Improvement

### Before (SCATTERED)

```
scout/
├── Dockerfile              ❌  # Docker files in root
├── docker-compose.yml      ❌  # Mixed with project files
├── docker-entrypoint.sh    ❌
├── README.md
├── requirements.txt
├── pyproject.toml
├── Makefile
├── src/
├── scripts/
└── ...
```

### After (ORGANIZED)

```
scout/
├── docker/                 ✅  # All Docker files together
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── docker-entrypoint.sh
├── README.md
├── requirements.txt
├── pyproject.toml
├── Makefile
├── src/
├── scripts/
└── ...
```

## Why This Matters

1. **Organization**: Docker files are grouped in a dedicated directory
2. **Cleaner Root**: Project root is less cluttered
3. **Separation**: Infrastructure configuration is separate from application code
4. **Consistency**: Follows common project structure patterns (like `scripts/`, `src/`)
5. **Maintainability**: Easier to find and manage Docker-related files
6. **Scalability**: Room for additional Docker configs (e.g., `docker-compose.override.yml`, `docker-compose.prod.yml`)

## Benefits

- ✅ Cleaner project root directory
- ✅ Better separation of concerns (infrastructure vs. application)
- ✅ Follows industry best practices
- ✅ Easier to manage multiple Docker configurations
- ✅ More intuitive project structure
- ✅ Makefile provides convenience commands (users don't need to type full paths)

## Usage Examples

### With docker-compose (explicit path)

```bash
# Start services
docker-compose -f docker/docker-compose.yml up -d

# Execute command
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208

# View logs
docker-compose -f docker/docker-compose.yml logs -f scraper

# Stop services
docker-compose -f docker/docker-compose.yml down
```

### With Make (recommended)

```bash
# Start services
make up

# View logs
make logs-scraper

# Open shell
make shell

# Stop services
make down

# Rebuild and restart
make rebuild
```

## Testing

All docker-compose commands work correctly:

```bash
# Build images
make build

# Start services
make up

# Check status
make status

# Execute script
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py

# Stop services
make down
```

## Migration Notes

**No breaking changes for users who use the Makefile:**
- All `make` commands work as before
- No changes needed to existing workflows

**Users calling docker-compose directly must:**
- Add `-f docker/docker-compose.yml` flag
- Or set `COMPOSE_FILE=docker/docker-compose.yml` environment variable
- Or create a shell alias: `alias dc='docker-compose -f docker/docker-compose.yml'`

**Recommended approach:**
```bash
# Add to ~/.bashrc or ~/.zshrc
export COMPOSE_FILE=docker/docker-compose.yml
```

## Future Enhancements

This structure now supports:

1. **Multiple environments:**
   ```
   docker/
   ├── Dockerfile
   ├── docker-compose.yml           # Base configuration
   ├── docker-compose.dev.yml       # Development overrides
   ├── docker-compose.prod.yml      # Production configuration
   └── docker-compose.test.yml      # Testing environment
   ```

2. **Additional Docker files:**
   ```
   docker/
   ├── Dockerfile
   ├── Dockerfile.dev               # Development image
   ├── Dockerfile.prod              # Production image
   ├── .dockerignore                # Docker ignore patterns
   └── entrypoints/                 # Multiple entrypoint scripts
       ├── scraper-entrypoint.sh
       ├── scheduler-entrypoint.sh
       └── worker-entrypoint.sh
   ```

3. **Docker documentation:**
   ```
   docker/
   ├── README.md                    # Docker-specific documentation
   ├── DEPLOYMENT.md                # Deployment guide
   └── TROUBLESHOOTING.md          # Docker troubleshooting
   ```

## Git Status

```
 M Makefile
 M README.md
 M DEVELOPMENT.md
 D Dockerfile
 D docker-compose.yml
 D docker-entrypoint.sh
?? docker/Dockerfile
?? docker/docker-compose.yml
?? docker/docker-entrypoint.sh
?? RESTRUCTURE_STEP4.md
```

## Commit Message (Suggested)

```
refactor(docker): Move Docker files to dedicated directory

- Move Dockerfile to docker/Dockerfile (update COPY paths to use ../)
- Move docker-compose.yml to docker/docker-compose.yml (update context and volumes)
- Move docker-entrypoint.sh to docker/docker-entrypoint.sh
- Update Makefile with COMPOSE_FILE variable for all commands
- Update README.md with new docker-compose paths (show both direct and make usage)
- Update DEVELOPMENT.md docker-compose references

This improves project organization by grouping all Docker-related files
in a dedicated directory, making the root less cluttered and following
industry best practices for project structure.

For users:
- Using make: No changes needed (works as before)
- Using docker-compose directly: Add -f docker/docker-compose.yml flag
```

---

## Related Steps

**Previous Steps:**
- [Step 1: Move AIScore Storage](RESTRUCTURE_STEP1.md) ✅
- [Step 3: Create Core Module](RESTRUCTURE_STEP3.md) ✅

**Next Steps:**
- **Step 5:** Move notebooks to `notebooks/` directory
- **Step 6:** Consolidate config files
- **Step 7:** Consider renaming `src/` to `src/scout/` for package clarity

---

**Scout - Project Restructuring Step 4** - Docker Organization Complete
