#!/bin/bash

echo "Stopping Airflow services..."

# Stop webserver
if [ -f webserver.pid ]; then
    WEBSERVER_PID=$(cat webserver.pid)
    echo "Stopping webserver (PID: $WEBSERVER_PID)..."
    kill $WEBSERVER_PID 2>/dev/null
    rm webserver.pid
else
    echo "No webserver PID file found"
fi

# Stop scheduler
if [ -f scheduler.pid ]; then
    SCHEDULER_PID=$(cat scheduler.pid)
    echo "Stopping scheduler (PID: $SCHEDULER_PID)..."
    kill $SCHEDULER_PID 2>/dev/null
    rm scheduler.pid
else
    echo "No scheduler PID file found"
fi

# Kill any remaining airflow processes
echo "Cleaning up any remaining Airflow processes..."
pkill -f "airflow webserver" 2>/dev/null
pkill -f "airflow scheduler" 2>/dev/null

echo "Airflow services stopped." 