-- Create tables for AIScore data warehouse
-- This script creates all tables for AIScore match and odds data
-- Run this AFTER creating the databases (00_create_databases_fotmob_and_aiscore.sql)
-- Note: Database prefix (aiscore.) will be added automatically by setup script

-- 1. Matches table - Main match information
-- Using ReplacingMergeTree to automatically handle duplicates based on ORDER BY key
-- Duplicates are removed during merge operations (use FINAL in SELECT for immediate deduplication)
CREATE TABLE IF NOT EXISTS matches (
    match_id String,
    match_url Nullable(String),
    game_date Date,
    scrape_date Date,
    scrape_timestamp DateTime,
    scrape_status Nullable(String),
    scrape_duration Nullable(Float32),
    home_team Nullable(String),
    away_team Nullable(String),
    match_result Nullable(String),
    league Nullable(String),
    country Nullable(String),
    odds_1x2_count Nullable(Int32),
    odds_asian_handicap_count Nullable(Int32),
    odds_over_under_goals_count Nullable(Int32),
    odds_over_under_corners_count Nullable(Int32),
    total_odds_count Nullable(Int32),
    links_scraping_complete UInt8,
    links_scraping_completed_at Nullable(DateTime),
    odds_scraping_complete UInt8,
    odds_scraping_completed_at Nullable(DateTime),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (game_date, match_id)
PARTITION BY toYYYYMM(game_date);

-- 2. 1X2 Odds table
-- Using ReplacingMergeTree to automatically handle duplicates based on ORDER BY key
-- Duplicates are removed during merge operations (use FINAL in SELECT for immediate deduplication)
-- Note: ORDER BY uses nullable columns, so we enable allow_nullable_key setting
CREATE TABLE IF NOT EXISTS odds_1x2 (
    match_id String,
    match_url Nullable(String),
    game_date Date,
    scrape_date Date,
    bookmaker Nullable(String),
    home_odds Nullable(Float32),
    draw_odds Nullable(Float32),
    away_odds Nullable(Float32),
    scraped_at DateTime,
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (game_date, match_id, scraped_at, bookmaker)
PARTITION BY toYYYYMM(game_date)
SETTINGS allow_nullable_key = 1;

-- 3. Asian Handicap Odds table
-- Using ReplacingMergeTree to automatically handle duplicates based on ORDER BY key
-- Duplicates are removed during merge operations (use FINAL in SELECT for immediate deduplication)
-- Note: ORDER BY uses nullable columns, so we enable allow_nullable_key setting
CREATE TABLE IF NOT EXISTS odds_asian_handicap (
    match_id String,
    match_url Nullable(String),
    game_date Date,
    scrape_date Date,
    match_time Nullable(String),
    moment_result Nullable(String),
    home_handicap Nullable(String),  -- Changed to String to support formats like "-0/0.5", "+0.5"
    home_odds Nullable(Float32),
    away_handicap Nullable(String),  -- Changed to String to support formats like "+0/0.5", "-0.5"
    away_odds Nullable(Float32),
    scraped_at DateTime,
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (game_date, match_id, scraped_at, home_handicap, away_handicap)
PARTITION BY toYYYYMM(game_date)
SETTINGS allow_nullable_key = 1;

-- 4. Over/Under Odds table
-- Using ReplacingMergeTree to automatically handle duplicates based on ORDER BY key
-- Duplicates are removed during merge operations (use FINAL in SELECT for immediate deduplication)
-- Note: ORDER BY uses nullable columns, so we enable allow_nullable_key setting
CREATE TABLE IF NOT EXISTS odds_over_under (
    match_id String,
    match_url Nullable(String),
    game_date Date,
    scrape_date Date,
    bookmaker Nullable(String),
    total_line Nullable(Float32),
    over_odds Nullable(Float32),
    under_odds Nullable(Float32),
    market_type Nullable(String),
    scraped_at DateTime,
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (game_date, match_id, scraped_at, bookmaker, total_line, market_type)
PARTITION BY toYYYYMM(game_date)
SETTINGS allow_nullable_key = 1;

-- 5. Daily Listings table - Summary of daily scraping
-- Using ReplacingMergeTree to automatically handle duplicates based on ORDER BY key
-- Duplicates are removed during merge operations (use FINAL in SELECT for immediate deduplication)
CREATE TABLE IF NOT EXISTS daily_listings (
    scrape_date Date,
    total_matches Int32,
    links_scraping_complete UInt8,
    links_scraping_completed_at Nullable(DateTime),
    odds_scraping_complete UInt8,
    odds_scraping_completed_at Nullable(DateTime),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (scrape_date)
PARTITION BY toYYYYMM(scrape_date);

