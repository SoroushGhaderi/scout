# Scripts Layout

## Canonical Entry Points

Use these for all new documentation, automation, and daily runs:

- `scripts/bronze/scrape_fotmob.py`
- `scripts/bronze/load_clickhouse.py`
- `scripts/bronze/drop_tables.py`
- `scripts/bronze/setup_clickhouse.py`
- `scripts/silver/process.py`
- `scripts/silver/drop_tables.py`
- `scripts/silver/setup_clickhouse.py`
- `scripts/gold/process.py`
- `scripts/gold/drop_tables.py`
- `scripts/gold/setup_clickhouse.py`
- `scripts/orchestration/pipeline.py`
- `scripts/orchestration/setup_clickhouse.py`

## Operational Utility Scripts

- `scripts/ensure_directories.py`
- `scripts/health_check.py`
- `scripts/refresh_turnstile.py`
- `scripts/check_logging_style.py`

## Scenario Scripts

These `scripts/gold/scenario_*.py` runners are discovered and executed by `scripts/gold/process.py`.

- `scripts/gold/scenario_demolition.py`
- `scripts/gold/scenario_defensive_shutdown_win.py`
- `scripts/gold/scenario_underdog_heist.py`
- `scripts/gold/scenario_dead_ball_dominance.py`
- `scripts/gold/scenario_low_block_heist.py`
- `scripts/gold/scenario_tactical_stalemate.py`
- `scripts/gold/scenario_great_escape.py`
- `scripts/gold/scenario_one_man_army.py`
- `scripts/gold/scenario_last_gasp.py`
- `scripts/gold/scenario_shot_stopper.py`
- `scripts/gold/scenario_war_zone.py`
- `scripts/gold/scenario_clinical_finisher.py`
- `scripts/gold/scenario_russian_roulette.py`
- `scripts/gold/scenario_efficiency_machine.py`
- `scripts/gold/scenario_away_day_masterclass.py`
- `scripts/gold/scenario_key_pass_king.py`
- `scripts/gold/scenario_wildcard.py`
- `scripts/gold/scenario_lead_by_example.py`
- `scripts/gold/scenario_young_gun.py`
- `scripts/gold/scenario_second_half_warriors.py`
- `scripts/gold/scenario_big_chance_killer.py`
- `scripts/gold/scenario_ten_men_stand.py`
- `scripts/gold/scenario_progressive_powerhouse.py`
- `scripts/gold/scenario_sterile_control.py`
- `scripts/gold/scenario_defensive_masterclass.py`
- `scripts/gold/scenario_metronome.py`
- `scripts/gold/scenario_high_intensity_engine.py`
- `scripts/gold/scenario_box_to_box_general.py`
- `scripts/gold/scenario_against_the_grain.py`
- `scripts/gold/scenario_unpunished_aggression.py`
- `scripts/gold/scenario_pressing_masterclass.py`
- `scripts/gold/scenario_elite_shot_stopper.py`
- `scripts/gold/scenario_hollow_dominance.py`
- `scripts/gold/scenario_touchline_terror.py`
- `scripts/gold/scenario_line_breaker.py`
- `scripts/gold/scenario_basketball_match.py`
- `scripts/gold/scenario_lightning_rod.py`
- `scripts/gold/scenario_human_shield.py`
- `scripts/gold/scenario_golden_touch.py`
- `scripts/gold/scenario_black_hole.py`
- `scripts/gold/scenario_high_line_trap.py`
- `scripts/gold/scenario_ghost_poacher.py`
- `scripts/gold/scenario_route_one_masterclass.py`
- `scripts/gold/scenario_total_suffocation.py`
- `scripts/gold/scenario_territorial_suffocation.py`
- `scripts/gold/scenario_clinical_pivot.py`
- `scripts/gold/scenario_chaos_engine.py`
- `scripts/gold/scenario_tired_legs.py`
- `scripts/gold/scenarios_catalog.md`
