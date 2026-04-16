# Gold Signals Catalog

This file is the shared documentation for signal-style jobs in the gold layer.
Each signal is documented with tactical rationale, threshold justification, and data integrity notes.

---

## Shared DDL For Signal Tables

- Table DDL file: `clickhouse/gold/02_create_signal_tables.sql`

---

## 📡 Signal: Team Possession-Passing Total Dominance (`signal_team_possession_passing_total_dominance`)

### 🎯 Purpose
Identifies matches where one side combines overwhelming possession control with a substantial accurate-passing edge, capturing fixtures defined by sustained territorial and circulation dominance.

### 🧠 Tactical & Statistical Logic

- **Dual-Control Requirement (Possession + Passing):** Possession alone can be sterile, and passing totals alone can be inflated by game state. Combining both conditions isolates matches where control was both territorial and technically sustained.

- **Asymmetric Thresholding:** The signal requires meaningful separation rather than parity-level superiority, so minor statistical edges are excluded. This avoids false positives in balanced matches and keeps the output focused on clear structural dominance.

- **Match-Level Integrity:** The query operates on completed match-level aggregates (not partial intervals), ensuring triggered rows reflect full-match control patterns rather than temporary in-game phases.

### 📂 Technical Assets
- **SQL Transformation:** `clickhouse/gold/signal/signal_team_possession_passing_total_dominance.sql`
- **Python Runner:** `scripts/gold/signal/signal_team_possession_passing_total_dominance.py`
- **Target Table:** `gold.signal_team_possession_passing_total_dominance`

### 🚀 Execution
```bash
python3 scripts/gold/signal/signal_team_possession_passing_total_dominance.py
```

---

## 📡 Signal: Team Possession-Passing Possession Without Purpose (`signal_team_possession_passing_possession_without_purpose`)

### 🎯 Purpose
Identifies teams that monopolize possession but produce very little direct attacking threat, surfacing sterile control profiles where ball retention does not translate into end-product.

### 🧠 Tactical & Statistical Logic

- **Control Without Penetration:** Possession above 65% indicates sustained control, but fewer than 2 shots on target marks poor chance conversion from that control.

- **Full-Time Signal Integrity:** The trigger is evaluated only on finished matches and full-match period aggregates (`period = 'All'`), preventing partial-phase false positives.

- **Side-Aware Attribution:** The query assigns `triggered_side`, `triggered_team_id`, and `triggered_team_name` so downstream consumers can attribute sterile domination to the correct team.

### 📂 Technical Assets
- **SQL Transformation:** `clickhouse/gold/signal/signal_team_possession_passing_possession_without_purpose.sql`
- **Python Runner:** `scripts/gold/signal/signal_team_possession_passing_possession_without_purpose.py`
- **Target Table:** `gold.signal_team_possession_passing_possession_without_purpose`

### 🚀 Execution
```bash
python3 scripts/gold/signal/signal_team_possession_passing_possession_without_purpose.py
```

---

## Reference Template For New Signals

Use the following structure when adding a new signal entry:

```markdown
## 📡 Signal: <Title> (`signal_<name>`)

### 🎯 Purpose
<What this signal captures and why it matters>

### 🧠 Tactical & Statistical Logic
- <Threshold/condition rationale #1>
- <Threshold/condition rationale #2>
- <Data integrity/reproducibility rationale>

### 📂 Technical Assets
- **SQL Transformation:** `clickhouse/gold/signal/signal_<name>.sql`
- **Python Runner:** `scripts/gold/signal/signal_<name>.py`
- **Target Table:** `gold.signal_<name>`

### 🚀 Execution
```bash
python3 scripts/gold/signal/signal_<name>.py
```
```
