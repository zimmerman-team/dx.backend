#!/bin/bash

# Check if an argument is provided
if [ $# -ne 1 ]; then
  echo "Usage: $0 [dev|test|staging|prod]"
  exit 1
fi

# Extract the first argument provided
MODE="$1"

# Check the value of the provided argument and run the appropriate command
if [ "$MODE" = "dev" ]; then
  flask run --port 4004
elif [ "$MODE" = "prod" ]; then
  gunicorn -w 8 app:app -b 0.0.0.0:4004 --daemon --access-logfile ./logging/access.txt --error-logfile ./logging/error.txt --timeout 600
elif [ "$MODE" = "staging" ]; then
  gunicorn -w 8 app:app -b 0.0.0.0:4004 --daemon --access-logfile ./logging/access.txt --error-logfile ./logging/error.txt --timeout 600
elif [ "$MODE" = "test" ]; then
  gunicorn -w 8 app:app -b 0.0.0.0:4004 --daemon --access-logfile ./logging/access.txt --error-logfile ./logging/error.txt --timeout 600
if [ "$MODE" = "docker" ]; then
  docker run . -p 4004:4004
else
  echo "Invalid mode. Use 'dev', 'test', 'staging' or 'prod'."
  exit 1
fi
