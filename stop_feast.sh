kill $(lsof -t -i:8008)
tmux kill-session -t feast_ui
