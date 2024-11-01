#!/bin/sh

while true; do

  nohup uv run python3 streaming >/dev/null 2>&1 &
  nohup uv run python3 general_tasks >/dev/null 2>&1 &
  nohup bash sync_with_remote.sh >/dev/null 2>&1 &
done &