tmux new-session -d -s crawl_worker 'cd /home/long/long/datn-crawlbot && sh restart_crawl.sh'
tmux new-session -d -s broker_worker 'cd /home/long/long/datn-crawlbot && sh restart_broker.sh'
tmux new-session -d -s feast_worker 'cd /home/long/long/datn-crawlbot && sh restart_feast.sh'
tmux new-session -d -s mlflow_worker 'cd /home/long/long/datn-crawlbot && sh restart_mlflow.sh'
tmux new-session -d -s airflow_worker 'cd /home/long/long/datn-crawlbot && sh restart_airflow.sh'
sh restart_bkprice.sh
