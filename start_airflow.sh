#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Set Airflow home
export AIRFLOW_HOME=$(pwd)

echo "Starting Airflow services..."
echo "Airflow Home: $AIRFLOW_HOME"

# Start webserver in background
echo "Starting Airflow webserver on port 8080..."
nohup airflow webserver --port 8084 > webserver.log 2>&1 &
WEBSERVER_PID=$!
echo "Webserver PID: $WEBSERVER_PID"

# Start scheduler in background  
echo "Starting Airflow scheduler..."
nohup airflow scheduler > scheduler.log 2>&1 &
SCHEDULER_PID=$!
echo "Scheduler PID: $SCHEDULER_PID"

# Save PIDs for later cleanup
echo $WEBSERVER_PID > webserver.pid
echo $SCHEDULER_PID > scheduler.pid

echo ""
echo "Airflow is starting up..."
echo "Web UI will be available at: http://localhost:8084"
echo "Login with: admin / admin"
echo ""
echo "To stop Airflow services, run: ./stop_airflow.sh"
echo "To view logs: tail -f webserver.log scheduler.log" 