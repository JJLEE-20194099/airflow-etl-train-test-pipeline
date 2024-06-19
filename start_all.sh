tmux new-session -d -s feast_worker 'sh start_feast.sh'
tmux new-session -d -s mlflow_worker 'sh start_mlflow.sh'
tmux new-session -d -s airflow_worker 'sh start_airflow.sh'
