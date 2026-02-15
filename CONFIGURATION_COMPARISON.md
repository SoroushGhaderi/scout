# Configuration Architecture: Before vs After

## BEFORE Refactoring ❌

```
.env (143 lines)
├── Common Settings
│   ├── LOG_LEVEL
│   ├── METRICS_ENABLED
│   └── LOG_FILE
├── ClickHouse
│   ├── CLICKHOUSE_HOST
│   ├── CLICKHOUSE_PASSWORD
│   └── ... (all DB settings)
├── FotMob Settings (40 lines)
│   ├── FOTMOB_X_MAS_TOKEN (sensitive ✓)
│   ├── FOTMOB_REQUEST_TIMEOUT ✗
│   ├── FOTMOB_DELAY_MIN ✗
│   ├── FOTMOB_MAX_WORKERS ✗
│   ├── FOTMOB_FILTER_BY_STATUS ✗
│   ├── FOTMOB_ALLOWED_MATCH_STATUSES ✗
│   ├── FOTMOB_BRONZE_PATH ✗
│   ├── FOTMOB_STORAGE_ENABLED ✗
│   ├── FOTMOB_RETRY_MAX_ATTEMPTS ✗
│   ├── FOTMOB_DATA_QUALITY_ENABLED ✗
│   └── ... (more application settings)
├── AIScore Settings (50 lines)
│   ├── AISCORE_BASE_URL ✗
│   ├── AISCORE_FILTER_BY_LEAGUES ✗
│   ├── AISCORE_ALLOWED_LEAGUES (long string) ✗
│   ├── AISCORE_SCROLL_INCREMENT ✗
│   ├── AISCORE_TIMEOUT_PAGE_LOAD ✗
│   ├── AISCORE_HEADLESS ✗
│   ├── AISCORE_BROWSER_WINDOW_SIZE ✗
│   ├── AISCORE_SELECTOR_MATCH_CONTAINER ✗
│   ├── AISCORE_VALIDATION_EXCLUDED_PATHS ✗
│   └── ... (more settings)
└── Email Config
    ├── ALERT_SMTP_HOST
    ├── ALERT_SMTP_PASSWORD (sensitive ✓)
    └── ...

Problems:
❌ Mixed sensitive and non-sensitive data
❌ Hard to read and maintain
❌ Application settings in environment file
❌ No separation of concerns
❌ All in one file (143 lines)
❌ Difficult to manage across deployments
```

## AFTER Refactoring ✅

```
config.yaml (296 lines - TRACKED IN GIT)
├── Common Settings
│   ├── logging
│   │   ├── level: INFO
│   │   ├── format: ...
│   │   └── file: logs/scraper.log
│   └── metrics
│       ├── enabled: true
│       └── export_path: metrics
│
├── FotMob Configuration (ORGANIZED HIERARCHY)
│   ├── api:
│   │   ├── base_url: https://www.fotmob.com/api/data
│   │   └── user_agents: [list]
│   ├── request:
│   │   ├── timeout: 30
│   │   ├── delay_min: 2.0
│   │   └── delay_max: 4.0
│   ├── scraping:
│   │   ├── max_workers: 2
│   │   ├── enable_parallel: true
│   │   ├── filter_by_status: true
│   │   └── allowed_match_statuses: [...]
│   ├── storage:
│   │   ├── bronze_path: data/fotmob
│   │   └── enabled: true
│   ├── retry:
│   │   ├── max_attempts: 3
│   │   ├── initial_wait: 2.0
│   │   └── max_wait: 10.0
│   └── data_quality:
│       ├── enabled: true
│       └── fail_on_issues: false
│
├── AIScore Configuration (ORGANIZED HIERARCHY)
│   ├── scraping:
│   │   ├── base_url: https://www.aiscore.com
│   │   ├── filter_by_leagues: true
│   │   ├── allowed_leagues: [95 leagues...]
│   │   └── extract_team_names: false
│   ├── scroll:
│   │   ├── increment: 500
│   │   ├── pause: 0.3
│   │   └── max_no_change: 8
│   ├── timeouts:
│   │   ├── page_load: 30
│   │   ├── element_wait: 10
│   │   └── cloudflare_max: 15
│   ├── browser:
│   │   ├── headless: true
│   │   ├── window_size: "1920x1080"
│   │   ├── block_images: true
│   │   └── user_agent: Mozilla/5.0...
│   ├── selectors:
│   │   ├── match_container: .match-container
│   │   ├── all_tab: .changeTabBox .changeItem
│   │   └── match_link: "a[href*='/match']"
│   ├── validation:
│   │   ├── excluded_paths: [/h2h, /statistics, ...]
│   │   └── required_pattern: /match
│   ├── retry:
│   │   ├── max_attempts: 3
│   │   ├── initial_wait: 2.0
│   │   └── status_codes: [429, 500, 502, ...]
│   └── storage:
│       ├── bronze_path: data/aiscore
│       └── enabled: true
│
└── Comments explaining each section

Benefits:
✅ Clear hierarchy and organization
✅ Easy to read and understand
✅ Tracked in git (history/diffs)
✅ Environment-agnostic defaults
✅ Ready for version control
✅ 296 lines of ONLY application config
```

```
.env (51 lines - NOT TRACKED IN GIT)
├── Database Credentials
│   ├── CLICKHOUSE_HOST=clickhouse
│   ├── CLICKHOUSE_PORT=8123
│   ├── CLICKHOUSE_USER=fotmob_user
│   └── CLICKHOUSE_PASSWORD=secure_pass
│
├── API Tokens (Sensitive)
│   └── FOTMOB_X_MAS_TOKEN=eyJib2R5Ijp7...
│
├── Email Credentials (Sensitive)
│   ├── ALERT_SMTP_HOST=smtp.gmail.com
│   ├── ALERT_SMTP_PORT=587
│   ├── ALERT_SMTP_USER=email@example.com
│   └── ALERT_SMTP_PASSWORD=secure_password
│
└── Configuration Path
    └── CONFIG_FILE_PATH=config.yaml

Benefits:
✅ ONLY sensitive data
✅ NOT in git history (safe)
✅ Clean and focused (51 lines vs 143)
✅ Environment-specific values only
✅ Easy to inject per deployment
✅ Clear what needs secrets
```

## Configuration Loading Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Application Startup                                             │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│ FotMobConfig() or AIScoreConfig()                              │
│ Instantiation                                                   │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
        ┌────────────────────┐
        │ _load_config()     │
        │ Priority: LOWEST   │
        │                    │
        │ Load from:         │
        │ config.yaml        │
        │                    │
        │ Fallback to        │
        │ hardcoded defaults │
        └────────────────────┘
                 │
                 ▼
     ┌────────────────────────┐
     │ _apply_env_overrides() │
     │ Priority: MEDIUM       │
     │                        │
     │ Override with:         │
     │ FOTMOB_* or AISCORE_*  │
     │ variables from .env    │
     └────────────────────────┘
                 │
                 ▼
        ┌────────────────────┐
        │ Python Code        │
        │ Priority: HIGHEST  │
        │                    │
        │ Can further        │
        │ override settings  │
        │ if needed          │
        └────────────────────┘
                 │
                 ▼
    ┌─────────────────────────┐
    │ Fully Configured        │
    │ TotMobConfig object     │
    │ Ready to use in scrapers│
    └─────────────────────────┘
```

## Configuration Example Scenarios

### Scenario 1: Default Configuration
```
Input:
  config.yaml: FotMob max_workers = 2
  .env: (no FOTMOB_MAX_WORKERS override)

Result:
  config.scraping.max_workers = 2
```

### Scenario 2: Environment Override
```
Input:
  config.yaml: FotMob max_workers = 2
  .env: FOTMOB_MAX_WORKERS=4

Result:
  config.scraping.max_workers = 4
```

### Scenario 3: Code Override
```
Input:
  config.yaml: FotMob max_workers = 2
  .env: FOTMOB_MAX_WORKERS=4
  Code: config.scraping.max_workers = 8

Result:
  config.scraping.max_workers = 8 (highest priority)
```

## Real-World Example

### Updating Browser Size for AIScore
```yaml
# In config.yaml (tracked in git)
aiscore:
  browser:
    window_size: "1920x1080"
```

### If you need different size just for this deployment:
```bash
# In .env (not in git, environment-specific)
AISCORE_BROWSER_WINDOW_SIZE=1024x768
```

### Or override in code:
```python
from config import AIScoreConfig

config = AIScoreConfig()
config.browser.window_size = "2560x1440"  # Highest priority
```

## Summary

| Aspect | Before | After |
|--------|--------|-------|
| **File Count** | 1 large file (143 lines) | 2 focused files (296 + 51 lines) |
| **Separation** | ❌ Mixed concerns | ✅ Clear separation |
| **Git Tracking** | ❌ Secrets in git | ✅ Only app config in git |
| **Organization** | ❌ Flat list | ✅ Hierarchical structure |
| **Maintainability** | ❌ Hard to navigate | ✅ Easy to find settings |
| **Security** | ❌ Risk of leaking secrets | ✅ Secrets isolated |
| **Deployment** | ❌ Complex .env management | ✅ Simple .env injection |
| **Documentation** | ❌ Limited | ✅ Comprehensive guides |
