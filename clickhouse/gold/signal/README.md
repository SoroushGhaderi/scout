# Gold Signal Query Contexts

Signals are tactical/statistical triggers with focused single-condition logic.

- `signal/`: signal SQL definitions executed by `scripts/gold/signal/runners/sig_*.py` runners.
- The bulk loader also supports legacy `signal_*.py` runner names during migration.

All signal runner scripts in `scripts/gold/signal/` resolve SQL files recursively, so nested folder layouts are supported.
