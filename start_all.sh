#!/bin/bash

SESSION="aml"

tmux new-session -d -s $SESSION

# 1. Docker (infra)
tmux rename-window -t $SESSION:0 'docker'
tmux send-keys -t $SESSION 'cd ~/projects/aml-tx-detection && sudo docker start neo4j && sudo docker start redpanda' C-m

# 2. Graph Builder
tmux new-window -t $SESSION -n 'graph'
tmux send-keys -t $SESSION 'cd ~/projects/aml-tx-detection/services/graph-builder && source ../../venv/bin/activate && export PYTHONPATH=$(pwd) && python app/consumer.py' C-m

# 3. Ingestion
tmux new-window -t $SESSION -n 'ingestion'
tmux send-keys -t $SESSION 'cd ~/projects/aml-tx-detection/services/ingestion-service && source ../../venv/bin/activate && uvicorn app.main:app --reload --port 8000' C-m

# 4. Feature
tmux new-window -t $SESSION -n 'feature'
tmux send-keys -t $SESSION 'cd ~/projects/aml-tx-detection/services/feature-service && source ../../venv/bin/activate && uvicorn app.main:app --reload --port 8002' C-m

# 5. Alert
tmux new-window -t $SESSION -n 'alert'
tmux send-keys -t $SESSION 'cd ~/projects/aml-tx-detection/services/alert-service && source ../../venv/bin/activate && uvicorn app.main:app --reload --port 8001' C-m

tmux attach-session -t $SESSION
