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

## scenario_win_with_low_xg_conceded

### Purpose

Find finished matches where the winner allowed less than 0.3 xG (strong defensive dominance).

### Files

- SQL: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_win_with_low_xg_conceded.sql`
- Python runner: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_win_with_low_xg_conceded.py`
- Target table: `fotmob.silver_scenario_win_with_low_xg_conceded`

### Run

```bash
python3 scripts/silver/scenario_win_with_low_xg_conceded.py
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

## Template For Future Scenarios

- Scenario name: `scenario_<name>`
- Purpose: short business reason
- SQL file: `/Users/soroush/Desktop/Projects/scout/clickhouse/silver/scenario_<name>.sql`
- Python file: `/Users/soroush/Desktop/Projects/scout/scripts/silver/scenario_<name>.py`
- Optional target table: `fotmob.silver_scenario_<name>`
- Run command: `python3 scripts/silver/scenario_<name>.py`
