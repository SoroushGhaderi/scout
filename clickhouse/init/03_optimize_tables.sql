-- ============================================================================
-- OPTIMIZE TABLES
-- ============================================================================
-- This script optimizes all tables to remove duplicates and merge data parts.
-- Run this after data loading is complete.
--
-- Usage:
--   clickhouse-client --queries-file=03_optimize_tables.sql
--   or
--   cat 03_optimize_tables.sql | clickhouse-client
--
-- ============================================================================

-- ============================================================================
-- FotMob Tables Optimization
-- ============================================================================

OPTIMIZE TABLE fotmob.general FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.timeline FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.venue FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.player FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.shotmap FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.goal FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.cards FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.red_card FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.period FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.momentum FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.starters FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.substitutes FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.coaches FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.team_form FINAL DEDUPLICATE;

-- ============================================================================
-- AIScore Tables Optimization
-- ============================================================================

OPTIMIZE TABLE aiscore.matches FINAL DEDUPLICATE;
OPTIMIZE TABLE aiscore.odds_1x2 FINAL DEDUPLICATE;
OPTIMIZE TABLE aiscore.odds_asian_handicap FINAL DEDUPLICATE;
OPTIMIZE TABLE aiscore.odds_over_under FINAL DEDUPLICATE;
OPTIMIZE TABLE aiscore.daily_listings FINAL DEDUPLICATE;

