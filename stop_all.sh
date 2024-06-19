tmux kill-session -t feast_worker
tmux kill-session -t airflow_worker
tmux kill-session -t mlflow_worker
sh stop_feast.sh
sh stop_mlflow.sh
sh stop_airflow.sh