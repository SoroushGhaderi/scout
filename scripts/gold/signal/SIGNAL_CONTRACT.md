# Gold Signal Contract Index

The original monolithic contract has been split into two purpose-specific contracts:

1. Core creative contract (Codex High):
   - `scripts/gold/signal/SIGNAL_CORE_CONTRACT.md`
2. Routine implementation contract (Codex Low):
   - `scripts/gold/signal/SIGNAL_EXECUTION_CONTRACT.md`

## Which Contract To Use

Use `SIGNAL_CORE_CONTRACT.md` when the task requires:

- writing or refining signal SQL logic
- designing analyst query outputs
- producing or improving signal catalogs

Use `SIGNAL_EXECUTION_CONTRACT.md` when the task requires:

- creating/updating signal tables
- writing/updating Python runners
- wiring bulk execution and release checks

Both contracts are normative and complementary; a complete signal package must satisfy both.

## Completion and Git Commit Rule

For each signal, complete all required parts first, then commit the signal package to Git as one standard per-signal commit.
This commit step is mandatory: once a signal package is complete, the author MUST create that per-signal commit before moving to unrelated work.

Required completion scope before commit:

- core logic and analyst output requirements (`SIGNAL_CORE_CONTRACT.md`)
- execution wiring, runner updates, and release checks (`SIGNAL_EXECUTION_CONTRACT.md`)

Commit policy:

- create the commit only after the full signal package is complete
- do not create partial per-signal commits before completion
