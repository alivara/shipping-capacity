#!/bin/bash

mkdir -p media/tmp/ logs/

echo ":: Db migrations..."
alembic upgrade head
echo ":: Db migrations done."

# Start server
echo ":: Starting server..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload --log-level debug
