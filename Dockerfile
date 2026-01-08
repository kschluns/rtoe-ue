FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /app

# Create Dagster home
RUN mkdir -p $DAGSTER_HOME

# Copy dependency files first for Docker layer caching
COPY pyproject.toml poetry.lock* /app/

# Copy application code (src layout)
COPY src /app/src

# Install runtime dependencies from pyproject.toml (PEP 621)
# This builds/install the package and installs deps.
RUN pip install --no-cache-dir .

# Copy Dagster instance config
COPY dagster/dagster.yaml $DAGSTER_HOME/dagster.yaml

# Default command (still override in ECS)
CMD ["dagster", "--help"]

