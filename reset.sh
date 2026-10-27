#!/bin/bash
set -e

echo "=== 1. Resetting MySQL Database Tables ==="
docker compose exec storage python drop_tables.py
docker compose exec storage python create_tables.py

echo "=== 2. Resetting Processing stats.json to Baseline ==="
cat << 'JSON' > ~/ACIT3855/Processing/stats.json
{
  "num_activity_events": 0,
  "total_step_count": 0,
  "avg_steps_per_event": 0.0,
  "avg_activity_heart_rate": 0.0,
  "last_updated_activity": "2026-01-01T00:00:00.000000",
  "num_exercise_events": 0,
  "total_exercise_duration": 0,
  "avg_exercise_duration": 0.0,
  "avg_exercise_heart_rate": 0.0,
  "last_updated_exercise": "2026-01-01T00:00:00.000000"
}
JSON

echo "=== 3. Restarting Processing Container ==="
docker compose restart processing

echo "=== Verification ==="
curl -s http://localhost:8100/stats
echo ""
echo "System successfully reset to clean 0 baseline!"
