kill $(lsof -t -i:5000)
fuser -k 5000/tcp