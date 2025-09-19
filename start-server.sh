#!/bin/bash

echo "🚀 Starting Aider HTTP Server..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  No .env file found. Creating from template..."
    cp ENV_TEMPLATE .env
    echo "📝 Please edit .env with your actual configuration:"
    echo "   - Supabase URL and Service Role Key"
    echo "   - Self-hosted LLM endpoint"
    echo "   - Model name"
    echo ""
    echo "Then run this script again."
    exit 1
fi

# Set default UID/GID if not set
if [ -z "$UID" ]; then
    export UID=$(id -u)
fi
if [ -z "$GID" ]; then
    export GID=$(id -g)
fi

echo "📋 Configuration:"
echo "   UID/GID: $UID:$GID"
echo "   Port: 5005"
echo "   Work Dir: /tmp/aider-sessions"

# Build and start the container
echo "🔨 Building Docker image..."
docker compose build

echo "🏃 Starting server..."
docker compose up

echo "✅ Server should be running at http://localhost:5005"
echo "🔍 Health check: http://localhost:5005/health"
echo "📖 API docs: http://localhost:5005/docs" 