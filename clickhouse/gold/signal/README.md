# Gold Signal Query Contexts

Signals are tactical/statistical triggers with focused single-condition logic.

- `clickhouse/gold/signal/sig_*.sql`: signal SQL definitions executed by `scripts/gold/signal/runners/sig_*.py` runners.
- The bulk loader also supports legacy `signal_*.py` runner names during migration.

All signal runner scripts in `scripts/gold/signal/runners/` resolve SQL files from this directory.
