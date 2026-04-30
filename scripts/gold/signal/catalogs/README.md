# Gold Signal Catalogs

Each active Gold signal has a catalog file in this directory. The index is structured from per-signal metadata so engineers and analysts can quickly scan signal ownership, grain, and status.

## Metadata Contract (Pulse-Oriented)

Catalogs now use a richer YAML block focused on Pulse consumption and analytical traceability:

- `taxonomy`: classification dimensions used by Pulse navigation and filtering.
- `pulse`: UI-facing metadata (`headline`, `default_surface`, narrative template, and user value tags).
- `trigger`: machine-readable trigger expression and scope.
- `identity`: deduplication identity and required output identity fields.
- `asset_binding`: convention-based asset resolution to avoid repeating identical paths/tables in every file.
- `quality`: QA expectations and downstream impact tags.

Asset resolution is now convention-based:

- Target table: `gold.{signal_id}`
- SQL path: `clickhouse/gold/signal/{signal_id}.sql`
- Runner path: `scripts/gold/signal/runners/{signal_id}.py`

Use `asset_binding.overrides` only when a signal breaks the default convention.

| Signal ID | Entity | Family | Subfamily | Grain | Status | Catalog |
|---|---|---|---|---|---|---|
| `sig_match_possession_passing_momentum_swing` | team | possession | passing | `match_team` | active | [sig_match_possession_passing_momentum_swing.md](sig_match_possession_passing_momentum_swing.md) |
| `sig_match_possession_passing_possession_stalemate` | team | possession | passing | `match_team` | active | [sig_match_possession_passing_possession_stalemate.md](sig_match_possession_passing_possession_stalemate.md) |
| `sig_match_possession_passing_unproductive_game` | team | possession | passing | `match_team` | active | [sig_match_possession_passing_unproductive_game.md](sig_match_possession_passing_unproductive_game.md) |
| `sig_team_possession_passing_aerial_reliance` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_aerial_reliance.md](sig_team_possession_passing_aerial_reliance.md) |
| `sig_team_possession_passing_death_by_passes` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_death_by_passes.md](sig_team_possession_passing_death_by_passes.md) |
| `sig_team_possession_passing_efficient_directness` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_efficient_directness.md](sig_team_possession_passing_efficient_directness.md) |
| `sig_team_possession_passing_failed_penetration` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_failed_penetration.md](sig_team_possession_passing_failed_penetration.md) |
| `sig_team_possession_passing_final_third_efficiency` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_final_third_efficiency.md](sig_team_possession_passing_final_third_efficiency.md) |
| `sig_team_possession_passing_high_press_victim` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_high_press_victim.md](sig_team_possession_passing_high_press_victim.md) |
| `sig_team_possession_passing_high_tempo_passing` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_high_tempo_passing.md](sig_team_possession_passing_high_tempo_passing.md) |
| `sig_team_possession_passing_keeper_involved` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_keeper_involved.md](sig_team_possession_passing_keeper_involved.md) |
| `sig_team_possession_passing_long_ball_desperation` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_long_ball_desperation.md](sig_team_possession_passing_long_ball_desperation.md) |
| `sig_team_possession_passing_low_block_frustration` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_low_block_frustration.md](sig_team_possession_passing_low_block_frustration.md) |
| `sig_team_possession_passing_possession_without_purpose` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_possession_without_purpose.md](sig_team_possession_passing_possession_without_purpose.md) |
| `sig_team_possession_passing_press_resistance` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_press_resistance.md](sig_team_possession_passing_press_resistance.md) |
| `sig_team_possession_passing_second_half_possession_collapse` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_second_half_possession_collapse.md](sig_team_possession_passing_second_half_possession_collapse.md) |
| `sig_team_possession_passing_siege_mode` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_siege_mode.md](sig_team_possession_passing_siege_mode.md) |
| `sig_team_possession_passing_sterile_dominance` | team | possession | passing | `match_team` | active | [sig_team_possession_passing_sterile_dominance.md](sig_team_possession_passing_sterile_dominance.md) |
| `sig_player_possession_passing_back_pass_heavy` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_back_pass_heavy.md](sig_player_possession_passing_back_pass_heavy.md) |
| `sig_player_possession_passing_box_penetrator` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_box_penetrator.md](sig_player_possession_passing_box_penetrator.md) |
| `sig_player_possession_passing_creative_hub` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_creative_hub.md](sig_player_possession_passing_creative_hub.md) |
| `sig_player_possession_passing_corner_specialist` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_corner_specialist.md](sig_player_possession_passing_corner_specialist.md) |
| `sig_player_possession_passing_deadball_creator` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_deadball_creator.md](sig_player_possession_passing_deadball_creator.md) |
| `sig_player_possession_passing_cross_heavy` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_cross_heavy.md](sig_player_possession_passing_cross_heavy.md) |
| `sig_player_possession_passing_dribble_threat` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_dribble_threat.md](sig_player_possession_passing_dribble_threat.md) |
| `sig_player_possession_passing_high_turnover_risk` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_high_turnover_risk.md](sig_player_possession_passing_high_turnover_risk.md) |
| `sig_player_possession_passing_high_value_provider` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_high_value_provider.md](sig_player_possession_passing_high_value_provider.md) |
| `sig_player_possession_passing_impact_sub_passing` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_impact_sub_passing.md](sig_player_possession_passing_impact_sub_passing.md) |
| `sig_player_possession_passing_isolated_target` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_isolated_target.md](sig_player_possession_passing_isolated_target.md) |
| `sig_player_possession_passing_long_ball_specialist` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_long_ball_specialist.md](sig_player_possession_passing_long_ball_specialist.md) |
| `sig_player_possession_passing_midfield_general` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_midfield_general.md](sig_player_possession_passing_midfield_general.md) |
| `sig_player_possession_passing_overloaded_possession` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_overloaded_possession.md](sig_player_possession_passing_overloaded_possession.md) |
| `sig_player_possession_passing_recycling_specialist` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_recycling_specialist.md](sig_player_possession_passing_recycling_specialist.md) |
| `sig_player_possession_passing_safe_outlet` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_safe_outlet.md](sig_player_possession_passing_safe_outlet.md) |
| `sig_player_possession_passing_switch_expert` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_switch_expert.md](sig_player_possession_passing_switch_expert.md) |
| `sig_player_possession_passing_under_pressure_expert` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_under_pressure_expert.md](sig_player_possession_passing_under_pressure_expert.md) |
| `sig_player_possession_passing_unsuccessful_crosser` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_unsuccessful_crosser.md](sig_player_possession_passing_unsuccessful_crosser.md) |
| `sig_player_possession_passing_vertical_threat` | player | possession | passing | `match_player` | active | [sig_player_possession_passing_vertical_threat.md](sig_player_possession_passing_vertical_threat.md) |

Each per-signal catalog includes:

1. YAML metadata block
2. Purpose
3. Tactical and statistical logic
4. Technical assets (convention-based unless overridden)
5. Execution command
6. Output schema table with column description and reason
