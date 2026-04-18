#!/usr/bin/env bash
set -euo pipefail

cd /opt/aidar/document_bot

if tmux has-session -t document_bot 2>/dev/null; then
  echo "Bot is running"
  tail -20 bot.log
else
  echo "Bot is not running"
  if [ -f bot.log ]; then
    tail -20 bot.log
  fi
fi
