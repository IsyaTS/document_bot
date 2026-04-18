#!/usr/bin/env bash
set -euo pipefail

cd /opt/aidar/document_bot

if tmux has-session -t document_bot 2>/dev/null; then
  tmux kill-session -t document_bot
  echo "Bot stopped"
else
  echo "Bot is not running"
fi
