# Multi-stage build for optimized image size
# Stage 1: Build dependencies
FROM python:3.12-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install UV for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml README.md ./

# Install dependencies using UV (much faster than pip)
# Install ALL dependencies including all loader dependencies
# This ensures optional dependencies don't cause import errors
RUN uv pip install --system --no-cache \
    pandas>=2.3.1 \
    pyarrow>=20.0.0 \
    typer>=0.15.2 \
    adbc-driver-manager>=1.5.0 \
    adbc-driver-postgresql>=1.5.0 \
    protobuf>=4.21.0 \
    base58>=2.1.1 \
    eth-hash[pysha3]>=0.7.1 \
    eth-utils>=5.2.0 \
    google-cloud-bigquery>=3.30.0 \
    google-cloud-storage>=3.1.0 \
    arro3-core>=0.5.1 \
    arro3-compute>=0.5.1 \
    psycopg2-binary>=2.9.0 \
    redis>=4.5.0 \
    deltalake>=1.0.2 \
    pyiceberg[sql-sqlite]>=0.10.0 \
    pydantic>=2.0,<2.12 \
    snowflake-connector-python>=4.0.0 \
    snowpipe-streaming>=1.0.0 \
    lmdb>=1.4.0

# Stage 2: Runtime image
FROM python:3.12-slim

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -m -u 1000 amp && \
    mkdir -p /app /data && \
    chown -R amp:amp /app /data

# Set working directory
WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

# Copy UV from builder for package installation
COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv

# Copy application code
COPY --chown=amp:amp src/ ./src/
COPY --chown=amp:amp apps/ ./apps/
COPY --chown=amp:amp data/ ./data/
COPY --chown=amp:amp pyproject.toml README.md ./

# Install the amp package in the system Python (NOT editable for Docker)
RUN uv pip install --system --no-cache .

# Switch to non-root user
USER amp

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command - run ERC20 loader
# Can be overridden with docker run arguments
ENTRYPOINT ["python", "apps/test_erc20_labeled_parallel.py"]
CMD ["--blocks", "100000", "--workers", "8", "--flush-interval", "0.5"]
