tmux new-session -d -s airflowscheduler 'airflow scheduler'
airflow webserver -p 8080 -D

