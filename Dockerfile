FROM python:3.11-slim

# Install system dependencies (git is often required)
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy source code into container
COPY . /app

# Install your fork in editable mode with dev dependencies
RUN pip install --no-cache-dir -e ".[dev]"

# Install server dependencies
RUN pip install --no-cache-dir -r server/requirements.txt

# Create directory for session storage
RUN mkdir -p /tmp/aider-sessions

# Default command: run the HTTP server
CMD ["python", "server/main.py"]
