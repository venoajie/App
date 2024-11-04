#!/bin/bash
# https://lovethepenguin.com/linux-check-if-a-file-or-directory-exists-e00cfa672249

while true; do
    pgrep -f 'nohup uv run python3 app' >/dev/null && echo 'OK' || (nohup uv run python3 app>/dev/null 2>&1 & echo 'Restart'); 
    pgrep -f 'nohup uv run python3 streaming' >/dev/null && echo 'OK' || (nohup uv run python3 astreamingpp>/dev/null 2>&1 & echo 'Restart'); 
    pgrep -f 'nohup uv run python3 general_tasks' >/dev/null && echo 'OK' || (nohup uv run python3 general_tasks>/dev/null 2>&1 & echo 'Restart'); 
    pgrep -f 'sh sync_with_remote.sh' >/dev/null && echo 'OK' || (htop& echo 'Restart'); 

    sleep 5;

done

