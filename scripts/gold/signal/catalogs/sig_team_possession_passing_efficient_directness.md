# sig_team_possession_passing_efficient_directness

## Purpose

Triggers when a team generates high shot volume with low possession, indicating efficient directness and transition-led threat creation.

## Tactical And Statistical Logic

- Trigger condition: `ball_possession < 35` and `total_shots > 5` at full match (`period = 'All'`) for home or away.
- Captures direct, transition-oriented football where threat generation is disproportionate to possession share.
- Enrichment layers differentiate clinical counter-attacking from noisy low-possession shot production.

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_team_possession_passing_efficient_directness.sql`
- Runner: `scripts/gold/signal/runners/sig_team_possession_passing_efficient_directness.py`
- Target table: `gold.sig_team_possession_passing_efficient_directness`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_team_possession_passing_efficient_directness.py
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
| `triggered_team_id` | ID of the team with <35% possession and >5 shots | Signal |
| `triggered_team_name` | Name of the triggered team | Signal |
| `opponent_team_id` | ID of the opposition team | Context |
| `opponent_team_name` | Name of the opposition team | Context |
| `triggered_team_possession_pct` | Full-match possession % of triggered team — primary signal condition | Signal |
| `opponent_possession_pct` | Full-match possession % of opponent — dominance baseline | Signal |
| `triggered_team_total_shots` | Total shots by triggered team — primary signal condition | Signal |
| `opponent_total_shots` | Total shots by opponent | Signal |
| `triggered_team_shots_on_target` | Shots on target by triggered team — directness accuracy | Enrichment |
| `opponent_shots_on_target` | Shots on target by opponent | Enrichment |
| `triggered_team_shots_inside_box` | Shots from inside the box — proximity and danger of attempts | Enrichment |
| `opponent_shots_inside_box` | Opponent shots from inside the box | Enrichment |
| `triggered_team_big_chances` | Big chances created by triggered team — quality of transition openings | Enrichment |
| `opponent_big_chances` | Big chances created by opponent | Enrichment |
| `triggered_team_big_chances_missed` | Big chances missed by triggered team — clinical efficiency check | Enrichment |
| `opponent_big_chances_missed` | Big chances missed by opponent | Enrichment |
| `triggered_team_xg` | Total xG for triggered team — expected value of shots generated | Enrichment |
| `opponent_xg` | Total xG for opponent | Enrichment |
| `triggered_team_xg_open_play` | Open-play xG for triggered team — transition-sourced threat only | Enrichment |
| `opponent_xg_open_play` | Open-play xG for opponent | Enrichment |
| `triggered_team_xg_on_target` | xG on target for triggered team — quality of shots that tested keeper | Enrichment |
| `opponent_xg_on_target` | xG on target for opponent | Enrichment |
| `xg_delta` | xG difference (triggered − opponent) — net expected threat balance | Enrichment |
| `triggered_team_pass_attempts` | Pass attempts by triggered team — low volume confirms direct style | Enrichment |
| `opponent_pass_attempts` | Pass attempts by opponent — confirms possession dominance | Enrichment |
| `triggered_team_accurate_passes` | Accurate passes by triggered team | Enrichment |
| `opponent_accurate_passes` | Accurate passes by opponent | Enrichment |
| `triggered_team_pass_acc_pct` | Pass accuracy % of triggered team — quality of limited circulation | Enrichment |
| `opponent_pass_acc_pct` | Pass accuracy % of opponent | Enrichment |
| `triggered_team_long_ball_attempts` | Long ball attempts by triggered team — key directness mechanism | Enrichment |
| `opponent_long_ball_attempts` | Long ball attempts by opponent | Enrichment |
| `triggered_team_accurate_long_balls` | Accurate long balls by triggered team — vertical progression success rate | Enrichment |
| `opponent_accurate_long_balls` | Accurate long balls by opponent | Enrichment |
| `triggered_team_touches_opp_box` | Touches in opponent's box by triggered team — final-third penetration despite low possession | Enrichment |
| `opponent_touches_opp_box` | Opponent touches in triggered team's box | Enrichment |
| `triggered_team_opp_half_passes` | Triggered team passes in opponent's half — forward pitch occupation | Enrichment |
| `opponent_opp_half_passes` | Opponent passes in triggered team's half | Enrichment |
| `triggered_team_interceptions` | Interceptions by triggered team — ball-winning to fuel transitions | Enrichment |
| `opponent_interceptions` | Interceptions by opponent | Enrichment |
| `triggered_team_tackles_won` | Tackles won by triggered team — ground-level ball recovery | Enrichment |
| `opponent_tackles_won` | Tackles won by opponent | Enrichment |
| `triggered_team_clearances` | Clearances by triggered team — defensive resilience under possession pressure | Enrichment |
| `opponent_clearances` | Clearances by opponent | Enrichment |
| `triggered_team_cross_attempts` | Cross attempts by triggered team — wide channel usage in attack | Enrichment |
| `opponent_cross_attempts` | Cross attempts by opponent | Enrichment |
| `triggered_team_accurate_crosses` | Accurate crosses by triggered team — delivery quality from wide | Enrichment |
| `opponent_accurate_crosses` | Accurate crosses by opponent | Enrichment |
| `triggered_team_corners` | Corners won by triggered team — set-piece volume as transition by-product | Enrichment |
| `opponent_corners` | Corners won by opponent | Enrichment |
| `triggered_team_fouls` | Fouls committed by triggered team — aggression in ball-winning | Enrichment |
| `opponent_fouls` | Fouls committed by opponent — disruption of triggered team's transitions | Enrichment |
