# Lightweight Python base
FROM python:3.11-slim

# Prevent Python from writing .pyc & enable unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install build deps and Scrapy system packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libxml2-dev \
    libxslt-dev \
    libz-dev \
    libffi-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# run as non-root user
RUN groupadd -r scrapy && useradd -r -g scrapy scrapyuser
USER scrapyuser

# Default command
CMD ["scrapy", "list"]
