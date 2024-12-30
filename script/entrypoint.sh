#!/bin/bash
set -e

# Install requirements if the file exists
if [ -f "/opt/airflow/requirements.txt" ]; then
    echo "Installing Python dependencies..."
    pip install --no-cache-dir -r /opt/airflow/requirements.txt || true
fi

# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db init

# Create admin user if it doesn't already exist
echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@airscholar.com \
    --password admin || echo "Admin user already exists."

# Run database upgrade
echo "Upgrading Airflow database..."
airflow db upgrade

# Start Airflow webserver
echo "Starting Airflow webserver..."
exec airflow webserver

#### in order to make it executable in terminal: chmod +x script/entrypoint.sh
