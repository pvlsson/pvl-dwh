FROM python:3.11-slim

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /workspace

# Install Python requirements
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt
