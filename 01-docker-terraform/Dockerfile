FROM python:3.11-slim as builder

WORKDIR /build

# WHY: gcc needed to compile psycopg2, postgresql-client for healthcheck
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (Docker layer caching optimization)
# WHY: If requirements.txt unchanged, this layer is cached
COPY requirements.txt .

# WHY: Virtual env isolates dependencies from system Python
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# WHY: --no-cache-dir reduces image size (no pip cache needed)
RUN pip install --no-cache-dir -r requirements.txt

# ═══════════════════════════════════════════════════════════
# Stage 2: Runtime - Minimal image with only what's needed
# ═══════════════════════════════════════════════════════════
# WHY: Fresh slim image without build tools (much smaller)
FROM python:3.11-slim

WORKDIR /app

# WHY: postgresql-client needed for healthcheck and manual debugging
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# WHY: Copy only venv from builder (excludes gcc and build artifacts)
COPY --from=builder /opt/venv /opt/venv

# Copy application code
COPY pipeline/ ./pipeline/

# WHY: .env.example as default (override with --env-file at runtime)
COPY .env.example ./.env

# Environment variables
# PYTHONUNBUFFERED: Force stdout/stderr to be unbuffered (better logging)
# PYTHONDONTWRITEBYTECODE: Don't create .pyc files (cleaner image)
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# WHY: Pre-create data directory for volume mount
RUN mkdir -p /app/data

# WHY: Health check validates DB connection (Kubernetes/Docker Swarm ready)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from pipeline.database import Database; db = Database(); db.connect(); db.close()" || exit 1

# WHY: ENTRYPOINT (not CMD) - harder to override accidentally
ENTRYPOINT ["python", "-m", "pipeline.ingest_pipeline"]

# ═══════════════════════════════════════════════════════════
# 📋 BUILD & RUN INSTRUCTIONS
# ═══════════════════════════════════════════════════════════

# Build image:
# docker build -t nyc-taxi-pipeline:latest -f Dockerfile .

# Run with docker-compose database:
# docker run --rm \
#   --network 01-docker-terraform_default \
#   -e POSTGRES_HOST=postgres \
#   -e POSTGRES_PORT=5432 \
#   -v $(pwd)/data:/app/data \
#   nyc-taxi-pipeline:latest

# Run with custom .env:
# docker run --rm \
#   --env-file .env \
#   -v $(pwd)/data:/app/data \
#   nyc-taxi-pipeline:latest

# Run with CLI command (override entrypoint):
# docker run --rm \
#   --entrypoint python \
#   --env-file .env \
#   nyc-taxi-pipeline:latest \
#   -m pipeline.cli status

# ═══════════════════════════════════════════════════════════
# 🎯 IMAGE SIZE COMPARISON
# ═══════════════════════════════════════════════════════════
# Single-stage:  ~450 MB (includes gcc, build tools)
# Multi-stage:   ~200 MB (only runtime dependencies)
# Savings:       ~250 MB (55% smaller!)
# ═══════════════════════════════════════════════════════════
