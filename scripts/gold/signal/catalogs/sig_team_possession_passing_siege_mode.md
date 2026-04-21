# sig_team_possession_passing_siege_mode

## Purpose

Triggers when a team sustains more than 80% full-match possession, indicating total territorial siege and opponent suppression.

## Tactical And Statistical Logic

- Trigger condition: `ball_possession_home > 80` (home trigger) or `ball_possession_away > 80` (away trigger) for `period = 'All'`.
- Signal is side-specific (`home` / `away`) and includes opponent identity for contextual analysis.
- Enrichment captures whether possession dominance is productive (xG, shots, box touches, corners, progression, and pass quality).

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_team_possession_passing_siege_mode.sql`
- Runner: `scripts/gold/signal/runners/sig_team_possession_passing_siege_mode.py`
- Target table: `gold.sig_team_possession_passing_siege_mode`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_team_possession_passing_siege_mode.py
```

## Output Schema

| Column Name | Description | Reason |
|---|---|---|
| `match_id` | Unique match identifier | Identifier |
| `match_date` | Date the match was played | Identifier |
| `home_team_id` | ID of the home team | Identifier |
| `home_team_name` | Name of the home team | Identifier |
| `away_team_id` | ID of the away team | Identifier |
| `away_team_name` | Name of the away team | Identifier |
| `home_score` | Goals scored by the home team | Identifier |
| `away_score` | Goals scored by the away team | Identifier |
| `triggered_side` | Which side (`home` / `away`) fired the signal | Signal |
| `triggered_team_id` | ID of the team that exceeded 80% possession | Signal |
| `triggered_team_name` | Name of the team that exceeded 80% possession | Signal |
| `opponent_team_id` | ID of the opposition team | Context |
| `opponent_team_name` | Name of the opposition team | Context |
| `triggered_team_possession_pct` | Full-match possession % of the triggered team | Signal |
| `opponent_possession_pct` | Full-match possession % of the opposition | Context |
| `possession_delta` | Difference in possession % between triggered team and opponent | Signal |
| `triggered_team_pass_attempts` | Total pass attempts by triggered team — volume of the siege | Enrichment |
| `opponent_pass_attempts` | Total pass attempts by opponent — how suppressed were they | Enrichment |
| `triggered_team_accurate_passes` | Accurate passes completed by triggered team | Enrichment |
| `opponent_accurate_passes` | Accurate passes completed by opponent | Enrichment |
| `triggered_team_pass_acc_pct` | Pass accuracy % of triggered team — quality of circulation | Enrichment |
| `opponent_pass_acc_pct` | Pass accuracy % of opponent — disruption level under pressure | Enrichment |
| `triggered_team_opp_half_passes` | Passes completed in the opponent's half — forward intent | Enrichment |
| `opponent_opp_half_passes` | Opponent passes in triggered team's half — counter-threat check | Enrichment |
| `triggered_team_touches_opp_box` | Touches inside the opponent's box — final-third penetration | Enrichment |
| `opponent_touches_opp_box` | Opponent touches in triggered team's box — defensive exposure | Enrichment |
| `triggered_team_corners` | Corners won by triggered team — set-piece volume from dominance | Enrichment |
| `opponent_corners` | Corners won by opponent | Enrichment |
| `triggered_team_shots` | Total shots by triggered team — shot volume from possession | Enrichment |
| `opponent_shots` | Total shots by opponent | Enrichment |
| `triggered_team_shots_on_target` | Shots on target by triggered team — accuracy of attack | Enrichment |
| `triggered_team_big_chances` | Big chances created by triggered team — quality of threat | Enrichment |
| `opponent_big_chances` | Big chances created by opponent — danger despite low possession | Enrichment |
| `triggered_team_xg` | Expected goals for triggered team — shot quality generated | Enrichment |
| `opponent_xg` | Expected goals for opponent — threat via low-possession tactics | Enrichment |
| `xg_delta` | xG difference (triggered − opponent) — whether dominance was productive | Enrichment |
