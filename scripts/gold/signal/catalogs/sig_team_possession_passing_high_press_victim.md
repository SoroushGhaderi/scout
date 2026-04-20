# sig_team_possession_passing_high_press_victim

## Purpose

Triggers when a team's pass accuracy drops below 70% as a proxy for defensive-third breakdown under high press.

## Tactical And Statistical Logic

- Trigger condition: `accurate_passes / pass_attempts < 0.70` for home or away in full-match totals (`period = 'All'`).
- If both teams are below threshold, `triggered_side = both` and team-specific trigger identifiers are set to `NULL`.
- Enrichment fields provide context through pass volume, pass accuracy delta, and own-half pass share.

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_team_possession_passing_high_press_victim.sql`
- Runner: `scripts/gold/signal/runners/sig_team_possession_passing_high_press_victim.py`
- Target table: `gold.sig_team_possession_passing_high_press_victim`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_team_possession_passing_high_press_victim.py
```

## Output Schema

| Column Name | Description | Reason |
|---|---|---|
| `match_id` | Unique match identifier | Identifier |
| `match_date` | Date the match was played | Identifier |
| `home_team_id` | Home team ID | Identifier |
| `home_team_name` | Home team name | Identifier |
| `away_team_id` | Away team ID | Identifier |
| `away_team_name` | Away team name | Identifier |
| `home_score` | Full-time home goals | Identifier |
| `away_score` | Full-time away goals | Identifier |
| `triggered_side` | Which side triggered the signal (`home`, `away`, `both`) | Signal |
| `triggered_team_id` | Team ID of the triggered side | Signal |
| `triggered_team_name` | Team name of the triggered side | Signal |
| `triggered_pass_accuracy_pct` | Pass accuracy (%) of the triggered team — the core signal value | Signal |
| `pass_accuracy_home_pct` | Home team pass accuracy (%) across full match | Enrichment |
| `pass_accuracy_away_pct` | Away team pass accuracy (%) across full match | Enrichment |
| `pass_accuracy_delta_pct` | Absolute accuracy gap between home and away — large delta implies one team imposed the press | Enrichment |
| `pass_attempts_home` | Total passes attempted by home team — low volume + low accuracy = severe press domination | Enrichment |
| `pass_attempts_away` | Total passes attempted by away team — same rationale | Enrichment |
| `own_half_pass_share_home_pct` | Share of home passes played in own half — high value indicates team was pinned back | Enrichment |
| `own_half_pass_share_away_pct` | Share of away passes played in own half — same rationale | Enrichment |
