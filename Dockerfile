# =============================================================================
# STRAT-015: S17 VWAP Premium Fade SHORT Scanner
# =============================================================================
FROM python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

RUN apt-get update && apt-get install -y gcc git && rm -rf /var/lib/apt/lists/*

# Install shared lib from public repo
RUN uv pip install --system --no-cache git+https://github.com/algojj/sc-shared.git

# Install requirements
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# =============================================================================
FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy all Python files
COPY *.py ./

EXPOSE 8037

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import asyncio; print('healthy')"

CMD ["python", "scanner_service.py"]
