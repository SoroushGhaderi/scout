# Gold Signal Query Contexts

Signals are tactical/statistical triggers with focused single-condition logic.

- `signal/`: signal SQL definitions executed by `scripts/gold/signal/runners/signal_*.py` runners.

All signal runner scripts in `scripts/gold/signal/` resolve SQL files recursively, so nested folder layouts are supported.
