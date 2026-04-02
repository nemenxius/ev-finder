#!/bin/zsh
set -euo pipefail

cd /Users/bmartins/Documents/EVFinder

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

/usr/bin/python3 /Users/bmartins/Documents/EVFinder/value_bet_alerts.py
