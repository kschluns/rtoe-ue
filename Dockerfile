FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir poetry==1.8.3

# Copy dependency files first for Docker layer caching
COPY pyproject.toml poetry.lock* /app/

# Install dependencies (no dev deps)
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --only main

# Copy app
COPY runner.py /app/runner.py

CMD ["python", "-m", "rtoe_ue.runner"]
