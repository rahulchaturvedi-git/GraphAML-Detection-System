#!/bin/bash

PROJECT_ROOT="$HOME/projects/aml-tx-detection"
VENV="$PROJECT_ROOT/venv/bin/activate"

gnome-terminal --title="Infrastructure" -- bash -c "cd $PROJECT_ROOT/infra/kafka && docker compose up; exec bash"

gnome-terminal --title="Ingestion" -- bash -c "source $VENV && cd $PROJECT_ROOT/services/ingestion-service && uvicorn app.main:app --reload --port 8000; exec bash"

gnome-terminal --title="Graph Builder" -- bash -c "source $VENV && cd $PROJECT_ROOT/services/graph-builder && python3 -m app.consumer; exec bash"

gnome-terminal --title="Project" -- bash -c "source $VENV && cd $PROJECT_ROOT; exec bash"