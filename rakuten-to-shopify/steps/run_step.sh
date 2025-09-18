#!/bin/bash
# Quick step runner script
#
# Usage:
#   ./steps/run_step.sh 01 input.csv        # Run step 01
#   ./steps/run_step.sh 03 --from-step 02   # Run step 03 from step 02 output
#   ./steps/run_step.sh --list              # List all steps

cd "$(dirname "$0")/.."
uv run python steps/step_runner.py "$@"