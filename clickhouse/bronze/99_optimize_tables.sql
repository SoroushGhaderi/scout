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

OPTIMIZE TABLE bronze.general FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.timeline FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.venue FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.player FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.shotmap FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.goal FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.cards FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.red_card FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.period FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.momentum FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.starters FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.substitutes FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.coaches FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.team_form FINAL DEDUPLICATE;
OPTIMIZE TABLE bronze.match_index FINAL DEDUPLICATE;
