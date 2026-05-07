---
signal_id: sig_player_discipline_cards_persistent_offender
status: active
entity: player
family: discipline
subfamily: cards
grain: match_player
headline: "Persistent Offender"
trigger: "player commits >= 5 fouls while receiving 0 cards in the same match"
row_identity:
  - match_id
  - triggered_player_id
  - triggered_team_id
asset_paths:
  table: gold.sig_player_discipline_cards_persistent_offender
  sql: clickhouse/gold/signal/sig_player_discipline_cards_persistent_offender.sql
  runner: scripts/gold/signal/runners/sig_player_discipline_cards_persistent_offender.py
---
# sig_player_discipline_cards_persistent_offender

## Purpose

Flags players who repeatedly foul (5 or more fouls) but finish the match without a card, surfacing persistent infringement profiles that escaped booking.

## Tactical And Statistical Logic

- Trigger condition:
  - `triggered_player_fouls_committed >= 5`
  - `triggered_player_total_cards = 0`
- Player foul volume is sourced from `silver.player_match_stat.fouls_committed`.
- Player card counts are sourced from `silver.card` at `match_id + player_id` grain.
- Team/opponent discipline context is sourced from `silver.period_stat` (`period = 'All'`) using symmetric `triggered_team_*` and `opponent_*` fields for fouls and cards.
- Possession context is included to distinguish foul-heavy low-possession defending from foul-heavy high-possession pressing patterns.

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_player_discipline_cards_persistent_offender.sql`
- Runner: `scripts/gold/signal/runners/sig_player_discipline_cards_persistent_offender.py`
- Target table: `gold.sig_player_discipline_cards_persistent_offender`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_player_discipline_cards_persistent_offender.py
```

## Output Schema

| Column Name | Description | Reason |
|---|---|---|
| `match_id` | Unique match identifier | Football developer: stable join key for player, team, and match-level diagnostics |
| `match_date` | Match calendar date | Football developer: enables temporal trend analysis on discipline behavior |
| `home_team_id` | Home team ID | Football developer: preserves bilateral match context |
| `home_team_name` | Home team name | Football developer: readable contextual labeling |
| `away_team_id` | Away team ID | Football developer: preserves bilateral match context |
| `away_team_name` | Away team name | Football developer: readable contextual labeling |
| `home_score` | Home full-time goals | Football developer: outcome context for interpreting foul tolerance |
| `away_score` | Away full-time goals | Football developer: outcome context for interpreting foul tolerance |
| `triggered_side` | Side of triggered player (`home` or `away`) | Football developer: canonical side orientation for side-aware aggregation |
| `triggered_player_id` | Triggered player ID | Football developer: player-level identity key for feature joins |
| `triggered_player_name` | Triggered player name | Football developer: readable player attribution |
| `triggered_team_id` | Team ID of triggered player | Football developer: links player behavior to team tactical context |
| `triggered_team_name` | Team name of triggered player | Football developer: readable team attribution |
| `opponent_team_id` | Opponent team ID | Football developer: matchup identity for bilateral comparisons |
| `opponent_team_name` | Opponent team name | Football developer: readable matchup attribution |
| `trigger_threshold_fouls_committed` | Configured foul threshold for trigger | Football developer: makes trigger boundary explicit in row-level outputs |
| `trigger_threshold_total_cards` | Configured card threshold for trigger | Football developer: makes the no-card gate explicit in row-level outputs |
| `triggered_player_fouls_committed` | Fouls committed by triggered player | Football developer: core trigger metric representing persistent infringement |
| `triggered_player_total_cards` | Total cards received by triggered player in match | Football developer: validates no-card trigger gate at row level |
| `triggered_player_yellow_cards` | Yellow-card count for triggered player | Football developer: disciplinary detail for QA and edge-case audits |
| `triggered_player_red_cards` | Red-card count for triggered player | Football developer: severe-discipline detail for QA and edge-case audits |
| `triggered_player_minutes_played` | Minutes played by triggered player | Football developer: exposure context for interpreting foul totals |
| `triggered_player_was_fouled` | Times triggered player was fouled | Football developer: physical duel context around foul-heavy behavior |
| `foul_count_above_threshold` | Fouls committed above trigger threshold (`fouls - 5`) | Football developer: severity measure beyond binary trigger |
| `triggered_team_fouls` | Fouls committed by triggered player's team | Football developer: team-level infringement environment around player behavior |
| `opponent_fouls` | Fouls committed by opponent team | Football developer: bilateral infringement comparator |
| `triggered_team_total_cards` | Total cards (yellow+red) for triggered player's team | Football developer: team discipline context for referee strictness and game tone |
| `opponent_total_cards` | Total cards (yellow+red) for opponent team | Football developer: bilateral discipline comparator |
| `triggered_team_yellow_cards` | Triggered-team yellow-card count | Football developer: card-color decomposition for discipline profiling |
| `opponent_yellow_cards` | Opponent yellow-card count | Football developer: bilateral card-color decomposition |
| `triggered_team_red_cards` | Triggered-team red-card count | Football developer: high-impact discipline context around foul patterns |
| `opponent_red_cards` | Opponent red-card count | Football developer: bilateral high-impact discipline comparator |
| `triggered_team_possession_pct` | Possession percentage of triggered side | Football developer: style/context signal for whether fouls happened in proactive or reactive phases |
| `opponent_possession_pct` | Possession percentage of opponent side | Football developer: bilateral possession comparator for discipline interpretation |
