# sig_team_possession_passing_long_ball_desperation

## Purpose

Triggers when the losing side attempts an extreme long-ball volume (`>60`), indicating desperation-driven direct play.

## Tactical And Statistical Logic

- Trigger condition: match is not drawn and the losing team has `long_ball_attempts > 60`.
- Triggered/opponent columns are dynamically resolved so the triggered side is always the losing team that met the threshold.
- Enrichment quantifies precision, tactical share, aerial effectiveness, and chance output of the direct route.

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_team_possession_passing_long_ball_desperation.sql`
- Runner: `scripts/gold/signal/runners/sig_team_possession_passing_long_ball_desperation.py`
- Target table: `gold.sig_team_possession_passing_long_ball_desperation`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_team_possession_passing_long_ball_desperation.py
```

## Output Schema

| Column Name | Description | Reason |
|---|---|---|
| `match_id` | Unique match identifier | Identifier |
| `match_date` | Calendar date of the match | Identifier |
| `home_team_id` | Numeric ID of the home team | Identifier |
| `home_team_name` | Display name of the home team | Identifier |
| `away_team_id` | Numeric ID of the away team | Identifier |
| `away_team_name` | Display name of the away team | Identifier |
| `home_score` | Home team final goals scored | Identifier |
| `away_score` | Away team final goals scored | Identifier |
| `score_margin_home_perspective` | Home score minus away score; negative value means the home team was losing | Context — quantifies the size of the deficit that motivated long-ball escalation |
| `triggered_team_id` | Numeric ID of the losing team that exceeded 60 long-ball attempts | Identifier |
| `triggered_team_name` | Display name of the triggered team | Identifier |
| `opponent_team_id` | Numeric ID of the winning opponent | Identifier |
| `opponent_team_name` | Display name of the opposing team | Identifier |
| `triggered_team_long_ball_attempts` | Total long-ball attempts by the triggered (losing) team | Signal — core measured signal value |
| `opponent_long_ball_attempts` | Total long-ball attempts by the winning opponent | Signal — symmetric pair; distinguishes whether direct play was match-wide or one-sided |
| `long_ball_attempts_delta` | Home long-ball attempts minus away long-ball attempts | Signal — bilateral net direct-play volume imbalance |
| `triggered_team_accurate_long_balls` | Accurate long balls completed by the triggered team | Enrichment — tests whether desperation volume retained any directional precision |
| `opponent_accurate_long_balls` | Accurate long balls completed by the opponent | Enrichment — symmetric pair; opponent's own directional long-ball quality |
| `triggered_team_long_ball_accuracy_pct` | Accurate long balls as a percentage of total long-ball attempts for the triggered team | Enrichment — low accuracy confirms panic distribution rather than deliberate direct play |
| `opponent_long_ball_accuracy_pct` | Accurate long balls as a percentage of total long-ball attempts for the opponent | Enrichment — symmetric pair; opponent's long-ball precision in contrast |
| `triggered_team_long_ball_share_pct` | Long-ball attempts as a percentage of total pass attempts for the triggered team | Enrichment — measures the magnitude of tactical shift away from short build-up play |
| `opponent_long_ball_share_pct` | Long-ball attempts as a percentage of total pass attempts for the opponent | Enrichment — symmetric pair; reveals whether opponent also played direct or held possession |
| `triggered_team_pass_accuracy_pct` | Overall pass completion rate of the triggered team | Enrichment — low overall accuracy alongside high long-ball volume confirms full build-up collapse |
| `opponent_pass_accuracy_pct` | Overall pass completion rate of the opponent | Enrichment — symmetric pair; high opponent accuracy reinforces the possession asymmetry |
| `triggered_team_possession_pct` | Ball possession percentage of the triggered team | Enrichment — losing teams with low possession are structurally forced into direct play |
| `opponent_possession_pct` | Ball possession percentage of the opponent | Enrichment — symmetric pair; high opponent possession is often the root cause of long-ball desperation |
| `triggered_team_aerials_won` | Aerial duels won by the triggered team | Enrichment — quantifies whether the long-ball route actually won the second ball |
| `opponent_aerials_won` | Aerial duels won by the opponent | Enrichment — symmetric pair; opponent aerial dominance nullifies the long-ball route |
| `triggered_team_aerial_success_pct` | Aerial duel win rate of the triggered team as a percentage | Enrichment — low win rate exposes the long-ball route as structurally ineffective |
| `opponent_aerial_success_pct` | Aerial duel win rate of the opponent as a percentage | Enrichment — symmetric pair; high opponent aerial rate confirms physical dominance in the air |
| `triggered_team_xg` | Total expected goals generated by the triggered team | Enrichment — tests whether desperation long-ball volume still manufactured genuine chances |
| `opponent_xg` | Total expected goals generated by the opponent | Enrichment — symmetric pair; measures the xG cushion the winning side built |
| `xg_delta` | Home xG minus away xG across the full match | Enrichment — bilateral net attacking threat differential independent of scoreline |
| `triggered_team_total_shots` | Total shots attempted by the triggered team | Enrichment — shot volume generated despite a direct and increasingly chaotic approach |
| `opponent_total_shots` | Total shots attempted by the opponent | Enrichment — symmetric pair; opponent's attacking output from a position of control |
| `triggered_team_clearances_conceded` | Clearances made by the opponent to repel the triggered team's long-ball delivery | Enrichment — high clearance count by the opponent confirms the long-ball route was absorbed rather than broken down |
| `opponent_clearances_conceded` | Clearances made by the triggered team in their own defensive phase | Enrichment — symmetric pair; triggered team's defensive exposure while committing bodies forward |
