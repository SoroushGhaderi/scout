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

## Telegram Daily Metrics & Alerts

Scout uses **Telegram** for daily scraping reports with metrics and emojis.

### Setup Instructions

#### 1. Create a Telegram Bot
```bash
# 1. Open Telegram and message @BotFather
# 2. Type /newbot
# 3. Choose a name and username
# 4. You'll get: "Use this token to access the HTTP API"
# 5. Copy your token
```

#### 2. Get Your Chat ID
```bash
# 1. Forward the bot message to yourself or a group
# 2. Visit: https://api.telegram.org/bot[YOUR_TOKEN]/getUpdates
# 3. Replace [YOUR_TOKEN] with your actual token
# 4. Look for "chat": {"id": 123456789}  (this is your chat_id)
```

#### 3. Configure Environment
Add to `.env`:
```bash
TELEGRAM_BOT_TOKEN=8431175588:AAENww0dKW50wEgMLU9iRcyvGH4A7bHgsto
TELEGRAM_CHAT_ID=your_chat_id_here
```

### Using Telegram Reports

#### FotMob Daily Report
```python
from src.utils.metrics_alerts import send_daily_report

send_daily_report(
    scraper='fotmob',
    date='20260215',
    matches_scraped=150,
    errors=2,
    skipped=3,
    duration_seconds=3600,
    cache_hits=45
)
```

**Sample Output:**
```
⚽ FotMob Daily Report - 20260215

✨ Matches Scraped: 150
📈 Success Rate: 98.7% ✅
❌ Errors: 2
⏭️ Skipped: 3
⏱️ Duration: 1.0h
💨 Cache Hits: 45

✅ All matches scraped successfully!
```

#### AIScore Daily Report
```python
send_daily_report(
    scraper='aiscore',
    date='20260215',
    matches_scraped=120,
    odds_scraped=118,
    errors=1,
    skipped=1,
    duration_seconds=5400
)
```

**Sample Output:**
```
⚽ AIScore Daily Report - 20260215

✨ Matches Found: 120
💰 Odds Scraped: 118
📈 Success Rate: 98.3% ✅
❌ Errors: 1
⏭️ Skipped: 1
⏱️ Duration: 1.5h

ℹ️ Status: Completed with issues
```

### Emoji Legend

**Status:**
- ✅ Success
- ❌ Error
- ⚠️ Warning
- ℹ️ Info

**Metrics:**
- ⚽ Matches
- ✨ Matches Scraped
- 📈 Success Rate
- ⏱️ Duration
- 💨 Cache Hits
- 💰 Odds

### Integration in Pipeline

Add to your scraping script (e.g., `scripts/pipeline.py`):

```python
import time
from src.utils.metrics_alerts import send_daily_report

# Start scraping
start_time = time.time()
fotmob_start = start_time

# ... FotMob scraping ...
fotmob_duration = time.time() - fotmob_start
fotmob_matches = 150  # Your actual count

# Send FotMob report
send_daily_report(
    scraper='fotmob',
    matches_scraped=fotmob_matches,
    errors=2,
    duration_seconds=fotmob_duration
)

# ... AIScore scraping ...
aiscore_start = time.time()
# ... AIScore scraping logic ...
aiscore_duration = time.time() - aiscore_start
aiscore_matches = 120  # Your actual count

# Send AIScore report
send_daily_report(
    scraper='aiscore',
    matches_scraped=aiscore_matches,
    odds_scraped=118,
    duration_seconds=aiscore_duration
)

print("✅ Daily reports sent to Telegram!")
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
