# Stage 1: Build stage with full development tools
FROM python:3.9-slim as builder

# Set working directory
WORKDIR /build

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Create virtual environment and install dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip and install dependencies
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Production stage with minimal footprint
FROM python:3.9-slim as production

# Set metadata
LABEL maintainer="AI-Autoscaler Team"
LABEL description="AI-driven Kubernetes Horizontal Pod Autoscaler with BiLSTM predictions"
LABEL version="1.0.0"

# Create non-root user for security
RUN groupadd -r autoscaler && useradd -r -g autoscaler autoscaler

# Set working directory
WORKDIR /app

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY --chown=autoscaler:autoscaler . .

# Set Python path
ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED="1"
ENV PYTHONDONTWRITEBYTECODE="1"

# Switch to non-root user
USER autoscaler

# Default command
CMD ["python", "main.py"]