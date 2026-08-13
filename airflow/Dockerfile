# Multi-stage Dockerfile using uv for fast, reliable Airflow & dbt environment
FROM python:3.11-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv from official binary image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock ./

# Sync project dependencies using uv
RUN uv sync --frozen --no-cache

# Place venv binaries in PATH
ENV PATH="/app/.venv/bin:$PATH"
ENV AIRFLOW_HOME="/app/airflow"

# Copy repository contents
COPY . .

# Expose Airflow Webserver port
EXPOSE 8080

CMD ["airflow", "standalone"]
