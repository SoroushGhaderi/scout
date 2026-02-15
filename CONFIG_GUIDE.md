# Configuration Guide

Scout uses a two-layer configuration system that separates application settings from environment-specific and sensitive data.

## Configuration Architecture

### Layer 1: config.yaml (Primary)
Contains **all application-specific settings** that define how scrapers behave:
- Request timeouts and delays
- Scraping behavior settings
- Storage paths and configuration
- Browser settings
- Selectors and validation rules
- Retry policies
- Logging and metrics configuration

**Location:** `config.yaml` in project root

**Best for:**
- Application logic and feature configuration
- Settings that are the same across all environments
- Easy collaboration and version control

### Layer 2: .env (Sensitive & Environment-Specific)
Contains **only sensitive data and environment-specific values**:
- Database credentials
- API authentication tokens
- Email/SMTP credentials
- Environment-specific overrides

**Location:** `.env` in project root (not tracked in git)

**Best for:**
- Credentials and secrets
- Environment-specific values (dev, staging, production)
- Configuration that varies by deployment

## How Configuration Loading Works

### FotMobConfig
```python
config = FotMobConfig()
```

Loading priority:
1. Load defaults from `config.yaml` under `fotmob:` section
2. Apply .env overrides using `FOTMOB_*` variables
3. Initialize directories and validate

### AIScoreConfig
```python
config = AIScoreConfig()
```

Loading priority:
1. Load defaults from `config.yaml` under `aiscore:` section
2. Apply .env overrides using `AISCORE_*` variables
3. Initialize directories and validate

## Configuration File Structure

### config.yaml
```yaml
# Common settings
logging:
  level: INFO
  file: logs/scraper.log
  dir: logs

# FotMob scraper configuration
fotmob:
  api:
    base_url: https://www.fotmob.com/api/data
    x_mas_token: set_via_env
  request:
    timeout: 30
    delay_min: 2.0
    delay_max: 4.0
  storage:
    bronze_path: data/fotmob
    enabled: true
  # ... more settings

# AIScore scraper configuration
aiscore:
  scraping:
    base_url: https://www.aiscore.com
    filter_by_leagues: true
    allowed_leagues:
      - Premier League
      - La Liga
      # ... more leagues
  browser:
    headless: true
    window_size: "1920x1080"
  # ... more settings
```

### .env
```bash
# Database credentials
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_USER=fotmob_user
CLICKHOUSE_PASSWORD=secure_password

# API tokens (sensitive)
FOTMOB_X_MAS_TOKEN=your_token_here

# SMTP credentials
ALERT_SMTP_USER=email@example.com
ALERT_SMTP_PASSWORD=email_app_password

# Configuration file path (optional)
CONFIG_FILE_PATH=config.yaml
```

## Overriding Configuration

### Via .env Variables
Override any config.yaml setting using environment variables:

**Format:** `{SCRAPER}_{CONFIG_PATH_UPPERCASE}`

**Examples:**
```bash
# Override FotMob request timeout
FOTMOB_REQUEST_TIMEOUT=60

# Override AIScore headless mode
AISCORE_HEADLESS=false

# Override AIScore allowed leagues (comma-separated)
AISCORE_ALLOWED_LEAGUES=Premier League,La Liga,Bundesliga
```

### Via Code
```python
from config import FotMobConfig

config = FotMobConfig()
config.request.timeout = 60  # Override after loading
```

## Configuration Precedence

For any setting:
1. **Highest:** Code-level overrides (set after config instantiation)
2. **Medium:** .env file variables (FOTMOB_*, AISCORE_*)
3. **Lowest:** config.yaml defaults

## Best Practices

### ✅ DO:
- Keep application settings in `config.yaml`
- Keep credentials in `.env` (never commit to git)
- Use .env for environment-specific values
- Document new configuration options in `config.yaml`
- Update config.yaml when adding new features

### ❌ DON'T:
- Put credentials in `config.yaml`
- Put application logic in `.env`
- Commit `.env` to version control
- Hardcode values in Python code when they should be configurable

## Adding New Configuration

1. Add setting to `config.yaml` under appropriate section
2. Update corresponding dataclass in `config/*.py`
3. Update `_load_config()` method to load from YAML
4. Add .env override in `_apply_env_overrides()` if needed

**Example:**
```yaml
# In config.yaml
fotmob:
  request:
    custom_setting: 123
```

```python
# In config/fotmob.py
from dataclasses import dataclass

@dataclass
class RequestConfig:
    timeout: int = 30
    custom_setting: int = 123  # Add new field

# In _load_config()
request_config = yaml_fotmob.get('request', {})
self.request = RequestConfig(
    timeout=request_config.get('timeout', 30),
    custom_setting=request_config.get('custom_setting', 123),
)

# In _apply_env_overrides()
if os.getenv('FOTMOB_REQUEST_CUSTOM_SETTING'):
    self.request.custom_setting = int(os.getenv('FOTMOB_REQUEST_CUSTOM_SETTING'))
```

## Configuration Validation

Configuration is validated during initialization. Validation errors include:
- Invalid log levels
- Missing required paths
- Invalid retry settings

Check logs for validation warnings during startup.

## Troubleshooting

### config.yaml not found
- Ensure `config.yaml` exists in project root
- Check `CONFIG_FILE_PATH` environment variable if custom location

### Settings not loading
- Verify YAML syntax in `config.yaml`
- Check variable names in .env match expected format
- Review logs for parsing errors

### .env overrides not working
- Verify variable name format: `{SCRAPER}_{SETTING_PATH_UPPERCASE}`
- Ensure .env file is loaded (check `python-dotenv`)
- Note: Some complex settings may not support .env overrides

## Configuration Files in Git

### Tracked:
- ✅ `config.yaml` - Application defaults

### Ignored (.gitignore):
- ❌ `.env` - Never commit secrets
- ❌ `.env.local` - Local environment overrides
- ❌ `.env.*.local` - Environment-specific secrets
