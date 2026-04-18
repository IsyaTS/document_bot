#!/usr/bin/env bash
set -euo pipefail

cd /opt/aidar/document_bot

if tmux has-session -t document_bot 2>/dev/null; then
  echo "Bot is already running in tmux session document_bot"
  exit 0
fi

mkdir -p generated data
touch bot.log
tmux new-session -d -s document_bot 'cd /opt/aidar/document_bot && .venv/bin/python -u main.py >> bot.log 2>&1'
echo "Bot started in tmux session document_bot"
