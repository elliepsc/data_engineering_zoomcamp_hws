# Dockerfile for Module 4 - Analytics Engineering with dbt

FROM python:3.11-slim

# Set working directory
WORKDIR /workspace

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    wget \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for dbt
RUN mkdir -p /root/.dbt

# Set environment variables
ENV DBT_PROFILES_DIR=/workspace
ENV PYTHONUNBUFFERED=1

# Expose port for dbt docs (optional): changed to avoid conflict with Airflow. was 8080 previously
EXPOSE 38080

# Default command
CMD ["/bin/bash"]
