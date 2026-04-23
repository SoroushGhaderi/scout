# sig_team_possession_passing_second_half_possession_collapse

## Purpose

Triggers when a team’s possession drops by more than 20 percentage points from first half to second half.

## Tactical And Statistical Logic

- Trigger condition: `(second_half_possession - first_half_possession) < -20` for home or away.
- Built from half-level pivots and enriched with bilateral pass-volume, pass-accuracy, territorial progression, and xG-half splits.
- Designed to distinguish tactical game-state surrender from forced territorial collapse.

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_team_possession_passing_second_half_possession_collapse.sql`
- Runner: `scripts/gold/signal/runners/sig_team_possession_passing_second_half_possession_collapse.py`
- Target table: `gold.sig_team_possession_passing_second_half_possession_collapse`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_team_possession_passing_second_half_possession_collapse.py
```

## Output Schema

| Column Name | Description | Reason |
|---|---|---|
| `match_id` | Unique match identifier | Football developer: anchors joins across match, team, and downstream feature tables — stable match/team reference field |
| `match_date` | Calendar date of the match | Football developer: anchors joins across match, team, and downstream feature tables — stable match/team reference field |
| `home_team_id` | Home team numeric ID | Football developer: anchors joins across match, team, and downstream feature tables — stable match/team reference field |
| `home_team_name` | Home team display name | Football developer: anchors joins across match, team, and downstream feature tables — stable match/team reference field |
| `away_team_id` | Away team numeric ID | Football developer: anchors joins across match, team, and downstream feature tables — stable match/team reference field |
| `away_team_name` | Away team display name | Football developer: anchors joins across match, team, and downstream feature tables — stable match/team reference field |
| `home_score` | Full-time goals scored by home team | Football developer: anchors joins across match, team, and downstream feature tables / outcome context |
| `away_score` | Full-time goals scored by away team | Football developer: anchors joins across match, team, and downstream feature tables / outcome context |
| `triggered_side` | Whether the triggered team was `'home'` or `'away'` | Football developer: this is the direct trigger metric used to classify the tactical pattern — row orientation label |
| `triggered_team_id` | Numeric ID of the team that experienced the possession drop | Football developer: this is the direct trigger metric used to classify the tactical pattern — core trigger field or direct signal context |
| `triggered_team_name` | Display name of the triggered team | Football developer: this is the direct trigger metric used to classify the tactical pattern — core trigger field or direct signal context |
| `opponent_team_id` | Numeric ID of the opposing team | Football developer: provides side/opponent orientation so tactical readings are not misattributed — opponent or orientation field for bilateral interpretation |
| `opponent_team_name` | Display name of the opposing team | Football developer: provides side/opponent orientation so tactical readings are not misattributed — opponent or orientation field for bilateral interpretation |
| `possession_drop_pp` | Percentage-point change in possession: 2H − 1H for the triggered team (always < −20) | Football developer: **Core signal value** |
| `triggered_team_poss_fh` | Triggered team's ball possession % in the first half | Football developer: this is the direct trigger metric used to classify the tactical pattern — baseline possession |
| `triggered_team_poss_sh` | Triggered team's ball possession % in the second half | Football developer: this is the direct trigger metric used to classify the tactical pattern — dropped possession |
| `opponent_poss_fh` | Opponent's ball possession % in the first half | Football developer: this is the direct trigger metric used to classify the tactical pattern (symmetric pair) |
| `opponent_poss_sh` | Opponent's ball possession % in the second half | Football developer: this is the direct trigger metric used to classify the tactical pattern (symmetric pair) |
| `possession_swing_delta` | Net possession dominance swing: (2H gap) − (1H gap) between the two teams | Football developer: adds diagnostic football context to explain why the trigger fired — bilateral shift magnitude |
| `triggered_team_pass_att_fh` | Total pass attempts by triggered team in the first half | Football developer: adds diagnostic football context to explain why the trigger fired — volume proxy for possession style |
| `triggered_team_pass_att_sh` | Total pass attempts by triggered team in the second half | Football developer: adds diagnostic football context to explain why the trigger fired — volume collapse detector |
| `opponent_pass_att_fh` | Opponent's total pass attempts in the first half | Football developer: adds diagnostic football context to explain why the trigger fired (symmetric pair) |
| `opponent_pass_att_sh` | Opponent's total pass attempts in the second half | Football developer: adds diagnostic football context to explain why the trigger fired (symmetric pair) |
| `triggered_team_pass_acc_fh` | Triggered team's pass accuracy % in the first half | Football developer: adds diagnostic football context to explain why the trigger fired — quality of possession, 1H baseline |
| `triggered_team_pass_acc_sh` | Triggered team's pass accuracy % in the second half | Football developer: adds diagnostic football context to explain why the trigger fired — did accuracy also decline? |
| `opponent_pass_acc_fh` | Opponent's pass accuracy % in the first half | Football developer: adds diagnostic football context to explain why the trigger fired (symmetric pair) |
| `opponent_pass_acc_sh` | Opponent's pass accuracy % in the second half | Football developer: adds diagnostic football context to explain why the trigger fired (symmetric pair) |
| `triggered_team_pass_acc_delta` | Change in pass accuracy % from 1H → 2H for triggered team | Football developer: adds diagnostic football context to explain why the trigger fired — distinguishes intentional low-block from forced error |
| `opponent_pass_acc_delta` | Change in pass accuracy % from 1H → 2H for opponent | Football developer: adds diagnostic football context to explain why the trigger fired (symmetric pair) |
| `triggered_team_opp_half_passes_fh` | Triggered team passes completed in the opponent's half, 1H | Football developer: adds diagnostic football context to explain why the trigger fired — territorial progression, 1H |
| `triggered_team_opp_half_passes_sh` | Triggered team passes completed in the opponent's half, 2H | Football developer: adds diagnostic football context to explain why the trigger fired — did they retreat spatially? |
| `opponent_opp_half_passes_fh` | Opponent passes in the triggered team's half, 1H | Football developer: adds diagnostic football context to explain why the trigger fired (symmetric pair) |
| `opponent_opp_half_passes_sh` | Opponent passes in the triggered team's half, 2H | Football developer: adds diagnostic football context to explain why the trigger fired (symmetric pair) |
| `triggered_team_xg_fh` | xG generated by triggered team in the first half | Football developer: adds diagnostic football context to explain why the trigger fired — was the 1H possession productive? |
| `triggered_team_xg_sh` | xG generated by triggered team in the second half | Football developer: adds diagnostic football context to explain why the trigger fired — did the drop translate to fewer chances? |
| `opponent_xg_fh` | xG generated by opponent in the first half | Football developer: adds diagnostic football context to explain why the trigger fired (symmetric pair) |
| `opponent_xg_sh` | xG generated by opponent in the second half | Football developer: adds diagnostic football context to explain why the trigger fired (symmetric pair) |
| `xg_swing_delta` | Net xG dominance swing: (2H gap) − (1H gap) | Football developer: adds diagnostic football context to explain why the trigger fired — was the possession drop punished in expected-goals terms? |
