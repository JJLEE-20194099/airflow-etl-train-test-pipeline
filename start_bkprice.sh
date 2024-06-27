tmux new-session -d -s hn_cat 'celery -A utils.train_func worker -c 1 -Q hn_cat --loglevel=INFO' & \
tmux new-session -d -s hn_lgbm 'celery -A utils.train_func worker -c 1 -Q hn_lgbm --loglevel=INFO' & \
tmux new-session -d -s hn_xgb 'celery -A utils.train_func worker -c 1 -Q hn_xgb --loglevel=INFO' & \
tmux new-session -d -s hcm_cat 'celery -A utils.train_func worker -c 1 -Q hcm_cat --loglevel=INFO' & \
tmux new-session -d -s hcm_lgbm 'celery -A utils.train_func worker -c 1 -Q hcm_lgbm --loglevel=INFO' & \
tmux new-session -d -s hcm_xgb 'celery -A utils.train_func worker -c 1 -Q hcm_xgb --loglevel=INFO' & \
uvicorn main:app --port 2001 --reload


