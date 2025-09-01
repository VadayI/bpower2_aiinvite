#!/bin/sh
set -e

# Jeśli używasz SQLite (DATABASE_URL zaczyna się od "sqlite://"), skipujemy czekanie
if echo "${DATABASE_URL:-}" | grep -q '^sqlite:'; then
  echo "Używasz SQLite, pomijam czekanie na bazę."
else
  echo "Czekam na bazę…"
  until pg_isready -h "${DB_HOST:-db}" -p "${DB_PORT:-5432}" -U "$DB_USER"; do
    sleep 1
  done
fi

echo "Uruchamiam migracje"
python manage.py migrate --noinput

echo "Zbieram statyczne pliki"
python manage.py collectstatic --noinput

echo "Start Gunicorna"
exec gunicorn AIInvite.wsgi:application \
  --bind 0.0.0.0:8000 \
  --workers 4 \
  --timeout 180 \
  --graceful-timeout 30
