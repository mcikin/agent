FROM python:3.11-slim
RUN apt-get update && apt-get install -y git curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml ./
# Install dependencies from pyproject.toml
RUN pip install --no-cache-dir -e .
COPY app ./app
ENV WORKDIR=/srv/aider
# Install aider (your fork) into the image; override via build-arg if needed
ARG AIDER_PIP=aider-chat
RUN pip install --no-cache-dir ${AIDER_PIP}
# Default binary path; can be overridden by env
ENV AIDER_BIN=/usr/local/bin/aider
# Ensure litellm is available
RUN python -c "import litellm; print('litellm imported successfully'); print('litellm location:', litellm.__file__)"
RUN mkdir -p $WORKDIR && useradd -m worker && chown -R worker:worker $WORKDIR /app
USER worker
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8080"]


