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

## Completion and Commit Rule

For each signal, commit changes only after all required parts are complete and aligned with the standard structure across both contracts:

- core logic and analyst output requirements (`SIGNAL_CORE_CONTRACT.md`)
- execution wiring, runner updates, and release checks (`SIGNAL_EXECUTION_CONTRACT.md`)

Do not create partial per-signal commits before the full signal package is complete in this structure.
