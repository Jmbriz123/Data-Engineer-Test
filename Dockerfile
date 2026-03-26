# ──────────────────────────────────────────────────────────────────────────────
# Springer Capital — Referral Pipeline Docker Image
# ──────────────────────────────────────────────────────────────────────────────
# Build : docker build -t springer-referral-pipeline .
# Run   : docker run --rm \
#           -v "$(pwd)/data":/app/data:ro \
#           -v "$(pwd)/output":/app/output \
#           -v "$(pwd)/profiling":/app/profiling \
#           springer-referral-pipeline
# ──────────────────────────────────────────────────────────────────────────────

# Use the official slim Python 3.12 image as base
FROM python:3.12-slim

# Set working directory inside the container
WORKDIR /app

# Copy dependency manifest first (layer-cache optimisation)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY pipeline.py          .
COPY profiling_script.py  .

# Declare mount-point volumes so Docker Desktop shows them
VOLUME ["/app/data", "/app/output", "/app/profiling"]

# Environment variables (overridable at runtime)
ENV DATA_DIR=/app/data
ENV OUTPUT_DIR=/app/output
ENV PROFILING_DIR=/app/profiling

# Default command: run profiling then pipeline
CMD ["sh", "-c", "python profiling_script.py && python pipeline.py"]
