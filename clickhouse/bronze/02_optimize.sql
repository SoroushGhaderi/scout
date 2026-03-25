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

OPTIMIZE TABLE fotmob.bronze_general FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_timeline FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_venue FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_player FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_shotmap FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_goal FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_cards FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_red_card FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_period FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_momentum FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_starters FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_substitutes FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_coaches FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.bronze_team_form FINAL DEDUPLICATE;
