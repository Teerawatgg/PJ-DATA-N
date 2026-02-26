FROM python:3.11-slim

WORKDIR /app

# system deps (optional but safe for psycopg2-binary)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py /app/app.py
COPY templates /app/templates

ENV PORT=5000
EXPOSE 5000

# gunicorn for production
CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app"]