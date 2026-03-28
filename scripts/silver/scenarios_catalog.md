# Silver Scenarios Catalog

This file is the shared documentation for scenario-style jobs in the silver layer.
Add each new scenario as a separate section below.

## Shared DDL For Scenario Tables

- Table DDL file: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/create_tables.sql`

## scenario_demolition

### Purpose

Find finished matches with a 3+ goal winning margin for quick analysis of dominant results.

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_demolition.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_demolition.py`
- Target table: `fotmob.silver_scenario_demolition`

### Run

```bash
python3 scripts/silver/scenario_demolition.py
```

## scenario_defensive_shutdown_win

### Purpose

Find finished matches where the winner allowed less than 0.3 xG (strong defensive dominance).

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_defensive_shutdown_win.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_defensive_shutdown_win.py`
- Target table: `fotmob.silver_scenario_defensive_shutdown_win`

### Run

```bash
python3 scripts/silver/scenario_defensive_shutdown_win.py
```

## scenario_underdog_heist

### Purpose

Find finished matches where the winner had less than 1.0 xG (an underdog-style win).

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_underdog_heist.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_underdog_heist.py`
- Target table: `fotmob.silver_scenario_underdog_heist`

### Run

```bash
python3 scripts/silver/scenario_underdog_heist.py
```

## scenario_dead_ball_dominance

### Purpose

Find finished wins where the winning side scored at least two goals from dead-ball situations.

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_dead_ball_dominance.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_dead_ball_dominance.py`
- Target table: `fotmob.silver_scenario_dead_ball_dominance`

### Run

```bash
python3 scripts/silver/scenario_dead_ball_dominance.py
```

## scenario_low_block_heist

### Purpose

Find finished matches where the winner had less than 35% possession.

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_low_block_heist.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_low_block_heist.py`
- Target table: `fotmob.silver_scenario_low_block_heist`

### Run

```bash
python3 scripts/silver/scenario_low_block_heist.py
```

## scenario_tactical_stalemate

### Purpose

Find finished matches with very low combined xG, including draws.

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_tactical_stalemate.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_tactical_stalemate.py`
- Target table: `fotmob.silver_scenario_tactical_stalemate`

### Run

```bash
python3 scripts/silver/scenario_tactical_stalemate.py
```

## scenario_great_escape

### Purpose

Find finished wins where the winning side was losing at minute 60.

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_great_escape.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_great_escape.py`
- Target table: `fotmob.silver_scenario_great_escape`

### Run

```bash
python3 scripts/silver/scenario_great_escape.py
```

## scenario_one_man_army

### Purpose

Find standout individual performances with at least two goals or two assists.

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_one_man_army.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_one_man_army.py`
- Target table: `fotmob.silver_scenario_one_man_army`

### Run

```bash
python3 scripts/silver/scenario_one_man_army.py
```

## scenario_last_gasp

### Purpose

Find finished wins where the deciding goal came late (85+) from draw/losing state.

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_last_gasp.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_last_gasp.py`
- Target table: `fotmob.silver_scenario_last_gasp`

### Run

```bash
python3 scripts/silver/scenario_last_gasp.py
```

## scenario_shot_stopper

### Purpose

Find goalkeepers with standout shot-stopping impact (xG saved >= 1.5).

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_shot_stopper.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_shot_stopper.py`
- Target table: `fotmob.silver_scenario_shot_stopper`

### Run

```bash
python3 scripts/silver/scenario_shot_stopper.py
```

## scenario_war_zone

### Purpose

Find finished high-intensity matches with extreme fouls/cards profiles.

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_war_zone.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_war_zone.py`
- Target table: `fotmob.silver_scenario_war_zone`

### Run

```bash
python3 scripts/silver/scenario_war_zone.py
```

## Template For Future Scenarios

- Scenario name: `scenario_<name>`
- Purpose: short business reason
- SQL file: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_<name>.sql`
- Python file: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_<name>.py`
- Optional target table: `fotmob.silver_scenario_<name>`
- Run command: `python3 scripts/silver/scenario_<name>.py`
