# 🎾 What to Run

## ✅ Only 3 Files to Run

### 1️⃣ Collect Links
```bash
python scrape_links_to_db.py <date>
```
**What it does:** Finds all match links for the specified date and saves to database

**Examples:**
```bash
# Scrape single date (default: headless/invisible browser)
python scrape_links_to_db.py 20251102

# Scrape with date format YYYY-MM-DD
python scrape_links_to_db.py 2025-11-02

# Scrape entire month (all days in November 2025)
python scrape_links_to_db.py 202511 --month

# Scrape entire month with visible browser
python scrape_links_to_db.py 202511 --month --visible

# Run in visible mode (use if Cloudflare blocks)
python scrape_links_to_db.py 20251102 --visible
```

---

### 2️⃣ Scrape Details
```bash
python scrape_details_from_db.py
```
**What it does:** Scrapes match details for all unscraped links

**Note:** Only scrapes links that haven't been scraped yet. Safe to run multiple times.

---

### 3️⃣ View Statistics
```bash
python query_links_db.py
```
**What it does:** Shows database statistics and progress

---

## 📊 Complete Workflow

```
Step 1: python scrape_links_to_db.py
         ↓
         Saves links to database

Step 2: python scrape_details_from_db.py
         ↓
         Scrapes details, saves to database

Step 3: python query_links_db.py
         ↓
         View progress and statistics
```

---

## 🔧 First Time Setup

1. Install dependencies:
   ```bash
   pip install selenium webdriver-manager beautifulsoup4
   ```

2. Run with date argument:
   ```bash
   python scrape_links_to_db.py 20251102
   ```

3. Scrape details:
   ```bash
   python scrape_details_from_db.py
   ```

---

## 📁 Database Location

All data saved to: **`data/tennis_matches.db`**

You can query it directly with SQL or use `query_links_db.py`

---

## ❓ Which File Should I Run?

- **Want to collect NEW links?** → `scrape_links_to_db.py`
- **Want to scrape details?** → `scrape_details_from_db.py`
- **Want to see progress?** → `query_links_db.py`

**That's it! Simple!** 🎉

