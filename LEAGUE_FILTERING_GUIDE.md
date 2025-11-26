# League-Based Filtering Guide

## Overview

The scraper now supports **league-based filtering** instead of country-based filtering. This allows you to target specific leagues (e.g., "Premier League", "La Liga", "Champions League") rather than filtering by country.

## Why League-Based Filtering?

**Benefits:**
- ✅ **More precise** - Target exact leagues you care about
- ✅ **Cross-country leagues** - Capture Champions League, Europa League, etc.
- ✅ **Better control** - Exclude lower-tier leagues from the same country
- ✅ **Flexible** - Mix leagues from different countries easily

**Example:**
- **Before (Country):** "England" → Gets ALL English leagues (Premier League, Championship, League One, etc.)
- **After (League):** "Premier League" → Gets ONLY Premier League matches

## Step-by-Step Guide

### Step 1: Scrape 30 Days of Links (All Matches)

First, scrape 30 days with NO filtering to collect all possible leagues:

```bash
# Temporarily disable filtering in .env
AISCORE_FILTER_BY_COUNTRIES=false
AISCORE_FILTER_BY_LEAGUES=false

# Scrape 30 days (links only - fast!)
python scripts/scrape_aiscore.py --month 202511 --links-only
```

This will scrape all match links and save league information for each match.

### Step 2: Analyze Leagues

Run the league analysis script to discover what leagues are available:

```bash
python scripts/analyze_leagues.py --days 30
```

**Output:**
```
================================================================================
TOP 50 LEAGUES BY MATCH COUNT
================================================================================

Rank   League Name                              Matches    Countries
------ ---------------------------------------- ---------- ------------------------------
1      Premier League                           300        England
2      La Liga                                  280        Spain
3      Serie A                                  270        Italy
4      Bundesliga                               260        Germany
5      Ligue 1                                  250        France
6      Champions League                         120        Europe
7      Europa League                            90         Europe
8      Championship                             290        England
...
```

**Files Created:**
- `analysis_reports/league_analysis_TIMESTAMP.json` - Full detailed analysis
- `analysis_reports/league_config_template_TIMESTAMP.json` - Suggested config

### Step 3: Choose Your Leagues

Open the generated template file and review suggested leagues:

```json
{
  "scraping": {
    "filter_by_leagues": true,
    "allowed_leagues": [
      "Bundesliga",
      "Champions League",
      "Championship",
      "Europa League",
      "La Liga",
      "Ligue 1",
      "Premier League",
      "Serie A",
      ...
    ]
  }
}
```

### Step 4: Configure League Filtering

Add to your `.env` file:

```bash
# Enable league-based filtering
AISCORE_FILTER_BY_LEAGUES=true

# Disable country filtering (league filtering takes priority)
AISCORE_FILTER_BY_COUNTRIES=false

# List of allowed leagues (comma-separated)
AISCORE_ALLOWED_LEAGUES=Premier League,La Liga,Serie A,Bundesliga,Ligue 1,Champions League,Europa League,Europa Conference League
```

**Important Notes:**
- League names are **case-insensitive** ("Premier League" = "premier league")
- Use **exact names** as they appear in the analysis
- **Comma-separated** list (no quotes needed in .env)
- If `AISCORE_FILTER_BY_LEAGUES=true`, country filtering is ignored

### Step 5: Test the Configuration

Scrape a single day to verify:

```bash
python scripts/scrape_aiscore.py 20251126 --links-only
```

Check the output - you should only see matches from your configured leagues.

## Configuration Priority

The filtering logic works in this priority order:

1. **League filtering** (if `AISCORE_FILTER_BY_LEAGUES=true`)
   - Uses `AISCORE_ALLOWED_LEAGUES`
   - Ignores country settings

2. **Country filtering** (if `AISCORE_FILTER_BY_COUNTRIES=true` and league filtering disabled)
   - Uses `AISCORE_ALLOWED_COUNTRIES`
   - Original behavior

3. **Importance filtering** (if `AISCORE_FILTER_BY_IMPORTANCE=true` and others disabled)
   - Uses star/bookmark indicators
   - Legacy method

4. **No filtering** (all filters disabled)
   - Scrapes ALL matches

## Example Configurations

### Top 5 European Leagues Only

```bash
AISCORE_FILTER_BY_LEAGUES=true
AISCORE_ALLOWED_LEAGUES=Premier League,La Liga,Serie A,Bundesliga,Ligue 1
```

### European Competitions + Top Leagues

```bash
AISCORE_FILTER_BY_LEAGUES=true
AISCORE_ALLOWED_LEAGUES=Premier League,La Liga,Serie A,Bundesliga,Ligue 1,Champions League,Europa League,Europa Conference League
```

### English Football (All Tiers)

```bash
AISCORE_FILTER_BY_LEAGUES=true
AISCORE_ALLOWED_LEAGUES=Premier League,Championship,League One,League Two,FA Cup,EFL Cup
```

### World Cups + Top Leagues

```bash
AISCORE_FILTER_BY_LEAGUES=true
AISCORE_ALLOWED_LEAGUES=World Cup,European Championship,Copa America,Premier League,La Liga,Serie A,Bundesliga,Ligue 1
```

## Advanced: Periodic League Analysis

Run analysis monthly to discover new leagues:

```bash
# Analyze current month
python scripts/analyze_leagues.py --start-date 20251101 --days 30

# See what's trending
python scripts/analyze_leagues.py --days 7 --top-n 100
```

## Troubleshooting

### No Matches Found

**Problem:** Scraper finds 0 matches

**Solutions:**
1. Verify league names match exactly (check analysis output)
2. Try with one known league first: `AISCORE_ALLOWED_LEAGUES=Premier League`
3. Check if filtering is enabled: `AISCORE_FILTER_BY_LEAGUES=true`

### League Names Don't Match

**Problem:** League appears in analysis but doesn't work in config

**Solution:**
- Copy exact name from `league_analysis_*.json` file
- Check for special characters or extra spaces
- League names are normalized (case-insensitive) but must be exact

### Want to Go Back to Country Filtering

**Solution:**
```bash
# Disable league filtering
AISCORE_FILTER_BY_LEAGUES=false

# Re-enable country filtering
AISCORE_FILTER_BY_COUNTRIES=true
```

## Performance Impact

League-based filtering is **slightly faster** than country filtering because:
- ✅ More precise matching (fewer false positives)
- ✅ Skip entire country's worth of irrelevant leagues
- ✅ Reduced DOM processing for unwanted leagues

**Estimated savings:** ~5-10% faster link scraping for targeted league lists.

## Migration from Country to League Filtering

### Step-by-Step Migration

1. **Run analysis on existing data** (no new scraping needed):
   ```bash
   python scripts/analyze_leagues.py --days 30
   ```

2. **Map your countries to leagues**:
   - Old: `England, Spain, Germany`
   - New: Check analysis for all leagues in those countries
   - Choose specific leagues you want

3. **Update .env file**:
   ```bash
   # OLD
   AISCORE_FILTER_BY_COUNTRIES=true
   AISCORE_ALLOWED_COUNTRIES=England,Spain,Germany

   # NEW
   AISCORE_FILTER_BY_LEAGUES=true
   AISCORE_ALLOWED_LEAGUES=Premier League,La Liga,Bundesliga,Champions League
   ```

4. **Test with one day**:
   ```bash
   python scripts/scrape_aiscore.py 20251126 --links-only
   ```

5. **Compare results**:
   - Check `data/aiscore/daily_listings/20251126/matches.json`
   - Verify only desired leagues are present

## Files Modified

### New Files:
- `scripts/analyze_leagues.py` - League analysis tool
- `LEAGUE_FILTERING_GUIDE.md` - This guide

### Modified Files:
- `src/config/aiscore_config.py` - Added league filtering config
- `scripts/aiscore_scripts/scrape_links.py` - Added league filtering logic

## Summary

League-based filtering gives you **surgical precision** in choosing which matches to scrape:

| Metric | Country Filtering | League Filtering |
|--------|------------------|------------------|
| Precision | Medium | High |
| Control | Coarse (whole country) | Fine (specific leagues) |
| Cross-border leagues | ❌ Difficult | ✅ Easy |
| Performance | Baseline | 5-10% faster |
| Setup complexity | Simple | Medium (requires analysis) |

**Recommended:** Use league filtering for production scraping after running initial analysis.

---

**Next Steps:**
1. Run `python scripts/analyze_leagues.py` on your existing data
2. Review the generated `league_analysis_*.json`
3. Update your `.env` with desired leagues
4. Test with a single day scrape
5. Deploy to production

Happy scraping! 🚀
