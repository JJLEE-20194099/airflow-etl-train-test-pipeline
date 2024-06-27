kill $(lsof -t -i:2001)
tmux kill-session -t hcm_cat
tmux kill-session -t hcm_lgbm
tmux kill-session -t hcm_xgb
tmux kill-session -t hn_cat
tmux kill-session -t hn_lgbm
tmux kill-session -t hn_xgb