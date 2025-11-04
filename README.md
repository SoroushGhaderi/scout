# Tennis Match Scraper - Database Only

Simple scraper that saves all data to SQLite database.

## 📁 Files

Only 3 files needed:

1. **`scrape_links_to_db.py`** - Collect match links
2. **`scrape_details_from_db.py`** - Scrape match details  
3. **`query_links_db.py`** - View database statistics

## 🚀 Quick Start

### Step 1: Install Dependencies

```bash
pip install selenium webdriver-manager beautifulsoup4
```

### Step 2: Collect Match Links

Run with date argument:

```bash
# Scrape specific date (default: headless/invisible browser)
python scrape_links_to_db.py 20251102

# Or with YYYY-MM-DD format
python scrape_links_to_db.py 2025-11-02

# Scrape entire month (all days in November 2025)
python scrape_links_to_db.py 202511 --month

# Visible browser mode (use if Cloudflare blocks)
python scrape_links_to_db.py 20251102 --visible
```

This saves all match URLs to `data/tennis_matches.db`

### Step 3: Scrape Match Details

```bash
python scrape_details_from_db.py
```

This scrapes details for all unscraped links and saves to database.

### Step 4: View Statistics

```bash
python query_links_db.py
```

Shows scraping progress and statistics.

## 📊 Database

All data saved to: `data/tennis_matches.db`

**Tables:**
- `match_links` - All match URLs with scraping status
- `match_details` - Complete match information

## ⚙️ Configuration

### scrape_links_to_db.py
```bash
# Run with date argument (default: headless/invisible browser)
python scrape_links_to_db.py 20251102

# Options:
# --month    Scrape entire month (date should be YYYYMM, e.g., 202511)
# --visible  Run browser in visible mode (default: headless)

# Examples:
python scrape_links_to_db.py 20251102           # Single date
python scrape_links_to_db.py 202511 --month     # Entire month
python scrape_links_to_db.py 202511 --month --visible  # Month with visible browser
```

### scrape_details_from_db.py
```python
LIMIT = None  # None = all, or set number like 10
DELAY = 3  # Seconds between requests
MAX_ATTEMPTS = 3  # Skip after 3 failures
```

## 💡 Usage Examples

### Collect links for yesterday
```python
# Edit scrape_links_to_db.py
from datetime import datetime, timedelta
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
dates_to_scrape = [yesterday]
```

### Scrape first 10 links only
```python
# Edit scrape_details_from_db.py
LIMIT = 10
```

### View unscraped links
```python
from query_links_db import get_unscraped_links

links = get_unscraped_links(limit=10)
for link in links:
    print(link[1])  # Print match URL
```

## 🎯 Complete Workflow

```
1. python scrape_links_to_db.py      → Collect links → Database
2. python scrape_details_from_db.py  → Scrape details → Database  
3. python query_links_db.py          → View statistics
```

That's it! All data in database, no CSV files.
