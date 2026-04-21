# sig_team_possession_passing_low_block_frustration

## Purpose

Triggers when a team records extreme cross volume in one match, indicating wide-overload behaviour caused by blocked central access.

## Tactical And Statistical Logic

- Trigger condition: `cross_attempts > 40` on full-match stats (`period = 'All'`) for home or away.
- This flags low-block frustration patterns where central progression is denied and attacks are repeatedly rerouted wide.
- Enrichment fields validate whether crossing pressure converted into threat and whether opponent defensive block behaviours were present.

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_team_possession_passing_low_block_frustration.sql`
- Runner: `scripts/gold/signal/runners/sig_team_possession_passing_low_block_frustration.py`
- Target table: `gold.sig_team_possession_passing_low_block_frustration`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_team_possession_passing_low_block_frustration.py
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
| `triggered_team_id` | ID of the team that attempted >40 crosses | Signal |
| `triggered_team_name` | Name of the triggered team | Signal |
| `opponent_team_id` | ID of the opposition team | Context |
| `opponent_team_name` | Name of the opposition team | Context |
| `triggered_team_cross_attempts` | Cross attempts by triggered team — the primary signal condition | Signal |
| `opponent_cross_attempts` | Cross attempts by opponent | Signal |
| `triggered_team_accurate_crosses` | Accurate crosses by triggered team — delivery success within the overload | Enrichment |
| `opponent_accurate_crosses` | Accurate crosses by opponent | Enrichment |
| `triggered_team_cross_acc_pct` | Cross accuracy % of triggered team — quality of wide delivery under frustration | Enrichment |
| `opponent_cross_acc_pct` | Cross accuracy % of opponent | Enrichment |
| `triggered_team_touches_opp_box` | Touches in opponent's box by triggered team — central penetration despite wide overload | Enrichment |
| `opponent_touches_opp_box` | Opponent touches in triggered team's box | Enrichment |
| `triggered_team_opp_half_passes` | Passes in opponent's half by triggered team — confirms sustained territorial pressure | Enrichment |
| `opponent_opp_half_passes` | Opponent passes in triggered team's half | Enrichment |
| `triggered_team_dribbles_succeeded` | Successful dribbles by triggered team — attempts to beat the block 1v1 through the middle | Enrichment |
| `opponent_dribbles_succeeded` | Successful dribbles by opponent | Enrichment |
| `triggered_team_dribble_attempts` | Dribble attempts by triggered team — frequency of individual central carry attempts | Enrichment |
| `opponent_dribble_attempts` | Dribble attempts by opponent | Enrichment |
| `triggered_team_possession_pct` | Full-match possession % of triggered team — confirms attacking dominance framing | Enrichment |
| `opponent_possession_pct` | Full-match possession % of opponent | Enrichment |
| `triggered_team_pass_attempts` | Total pass attempts by triggered team — volume of circulation before switching wide | Enrichment |
| `opponent_pass_attempts` | Total pass attempts by opponent | Enrichment |
| `triggered_team_accurate_passes` | Accurate passes by triggered team | Enrichment |
| `opponent_accurate_passes` | Accurate passes by opponent | Enrichment |
| `triggered_team_pass_acc_pct` | Pass accuracy % of triggered team | Enrichment |
| `opponent_pass_acc_pct` | Pass accuracy % of opponent | Enrichment |
| `triggered_team_total_shots` | Total shots by triggered team — did crossing yield attempts at all? | Enrichment |
| `opponent_total_shots` | Total shots by opponent | Enrichment |
| `triggered_team_shots_on_target` | Shots on target by triggered team — quality of chances from wide delivery | Enrichment |
| `opponent_shots_on_target` | Shots on target by opponent | Enrichment |
| `triggered_team_shots_inside_box` | Shots from inside the box by triggered team — crossing converts to close-range chances | Enrichment |
| `opponent_shots_inside_box` | Shots from inside the box by opponent | Enrichment |
| `triggered_team_shots_outside_box` | Shots from outside the box by triggered team — speculative attempts when block holds firm | Enrichment |
| `opponent_shots_outside_box` | Shots from outside the box by opponent | Enrichment |
| `triggered_team_big_chances` | Big chances created by triggered team — whether crossing ever broke the block decisively | Enrichment |
| `opponent_big_chances` | Big chances created by opponent | Enrichment |
| `triggered_team_big_chances_missed` | Big chances missed by triggered team — wastefulness under frustration | Enrichment |
| `opponent_big_chances_missed` | Big chances missed by opponent | Enrichment |
| `triggered_team_xg` | Total xG for triggered team — expected value of all attempts generated | Enrichment |
| `opponent_xg` | Total xG for opponent | Enrichment |
| `triggered_team_xg_set_play` | Set-play xG for triggered team — crosses often lead to set-play-adjacent situations | Enrichment |
| `opponent_xg_set_play` | Set-play xG for opponent | Enrichment |
| `triggered_team_xg_open_play` | Open-play xG for triggered team — transition-sourced threat only | Enrichment |
| `opponent_xg_open_play` | Open-play xG for opponent | Enrichment |
| `xg_delta` | xG difference (triggered − opponent) — net expected threat balance despite wide overload | Enrichment |
| `triggered_team_clearances` | Clearances by triggered team | Enrichment |
| `opponent_clearances` | Clearances by opponent — volume of headed/last-ditch defending of crosses | Enrichment |
| `triggered_team_interceptions` | Interceptions by triggered team | Enrichment |
| `opponent_interceptions` | Interceptions by opponent — active disruption of build-up before it reaches wide areas | Enrichment |
| `triggered_team_shot_blocks` | Shot blocks by triggered team | Enrichment |
| `opponent_shot_blocks` | Shot blocks by opponent — physical suppression of attempts from cross-derived positions | Enrichment |
| `triggered_team_aerials_won` | Aerial duels won by triggered team — success in contesting crossed balls | Enrichment |
| `opponent_aerials_won` | Aerial duels won by opponent — defensive dominance in the air against crosses | Enrichment |
| `triggered_team_aerial_attempts` | Aerial duel attempts by triggered team | Enrichment |
| `opponent_aerial_attempts` | Aerial duel attempts by opponent | Enrichment |
| `triggered_team_corners` | Corners won by triggered team — natural by-product of sustained wide pressure | Enrichment |
| `opponent_corners` | Corners won by opponent | Enrichment |
| `triggered_team_fouls` | Fouls committed by triggered team — aggression in trying to win the ball back quickly | Enrichment |
| `opponent_fouls` | Fouls committed by opponent — how cynically the block was maintained | Enrichment |
