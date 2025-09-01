FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # domyślne dla pg_isready
    DB_HOST=db \
    DB_PORT=5432

WORKDIR /app

# Zależności systemowe
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential libpq-dev postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Zależności Pythona
COPY requirements.txt /app/
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt gunicorn whitenoise django-environ

# Kod aplikacji
COPY . /app/

# Entry point
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
