# sig_team_possession_passing_high_tempo_passing

## Purpose

Triggers when either side reaches a peak half passing tempo of `>= 6.5` passes per minute (half passes ÷ 45), indicating unusually high-possession circulation speed.

## Tactical And Statistical Logic

- Signal name: `sig_team_possession_passing_high_tempo_passing`.
- Trigger condition: either side peaks at `>= 6.5` passes per minute in one half (`FirstHalf`/`SecondHalf`).
- Measurement is symmetric for home and away and preserves bilateral context fields for downstream style classification.

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_team_possession_passing_high_tempo_passing.sql`
- Runner: `scripts/gold/signal/runners/sig_team_possession_passing_high_tempo_passing.py`
- Target table: `gold.sig_team_possession_passing_high_tempo_passing`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_team_possession_passing_high_tempo_passing.py
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
| `home_score` | Full-time goals scored by home team | Identifier / result context |
| `away_score` | Full-time goals scored by away team | Identifier / result context |
| `home_possession_h1` | Home team ball possession % in first half | Context — tempo only means something relative to how long a team holds the ball |
| `home_possession_h2` | Home team ball possession % in second half | Context — half-by-half possession shift reveals game state influence |
| `away_possession_h1` | Away team ball possession % in first half | Context — symmetric pair to `home_possession_h1` |
| `away_possession_h2` | Away team ball possession % in second half | Context — symmetric pair to `home_possession_h2` |
| `home_passes_h1` | Total passes by home team in first half | Signal source — raw numerator for passes-per-min proxy |
| `home_passes_h2` | Total passes by home team in second half | Signal source — detects half-level tempo shift |
| `away_passes_h1` | Total passes by away team in first half | Signal — symmetric pair to `home_passes_h1` |
| `away_passes_h2` | Total passes by away team in second half | Signal — symmetric pair to `home_passes_h2` |
| `home_passes_per_min_h1` | Home passes ÷ 45 for first half | Signal — passes-per-minute proxy, H1 |
| `home_passes_per_min_h2` | Home passes ÷ 45 for second half | Signal — passes-per-minute proxy, H2 |
| `away_passes_per_min_h1` | Away passes ÷ 45 for first half | Signal — symmetric pair to `home_passes_per_min_h1` |
| `away_passes_per_min_h2` | Away passes ÷ 45 for second half | Signal — symmetric pair to `home_passes_per_min_h2` |
| `home_peak_passes_per_min` | Higher of H1/H2 passes-per-min for home team | Signal — single trigger value used in HAVING and ranking |
| `away_peak_passes_per_min` | Higher of H1/H2 passes-per-min for away team | Signal — symmetric pair to `home_peak_passes_per_min` |
| `home_accurate_passes_total` | Successful passes by home team across both halves | Enrichment — volume without quality is misleading; accurate passes confirm circulation intent |
| `away_accurate_passes_total` | Successful passes by away team across both halves | Enrichment — symmetric pair |
| `home_pass_attempts_total` | Total pass attempts by home team across both halves | Enrichment — denominator for accuracy; exposes high-volume low-accuracy spam |
| `away_pass_attempts_total` | Total pass attempts by away team across both halves | Enrichment — symmetric pair |
| `home_pass_accuracy_pct` | Accurate ÷ attempted passes % for home team | Enrichment — distinguishes high-quality tempo from high-risk direct play |
| `away_pass_accuracy_pct` | Accurate ÷ attempted passes % for away team | Enrichment — symmetric pair |
| `pass_accuracy_delta_home_minus_away` | Home pass accuracy % minus away pass accuracy % | Enrichment — bilateral net; positive = home dominates circulation quality |
| `home_opposition_half_passes` | Home passes played in the opposition's half | Enrichment — high tempo in the final third signals aggressive press-and-circulate; distinguishes deep build-up from advanced possession |
| `away_opposition_half_passes` | Away passes played in the opposition's half | Enrichment — symmetric pair |
| `home_own_half_passes` | Home passes played in own half | Enrichment — high own-half share with high tempo = safety-first build-up rather than progressive intent |
| `away_own_half_passes` | Away passes played in own half | Enrichment — symmetric pair |
| `home_opp_half_pass_pct` | % of home passes played in opposition half | Enrichment — progressive territory ratio; cross-reference with tempo to classify style archetype |
| `away_opp_half_pass_pct` | % of away passes played in opposition half | Enrichment — symmetric pair |
| `triggered_team_side` | Which side(s) fired the signal: `'home'`, `'away'`, or `'both'` | Signal — routes downstream analysis to the correct team for asymmetric comparisons |
