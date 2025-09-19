# Aider HTTP Server

This setup transforms Aider into an HTTP API server that:

- 🌐 **Exposes OpenAI-compatible endpoints** for your Flutter app
- 📁 **Integrates with Supabase Storage** for file management
- 🤖 **Connects to LLMs** (OpenRouter, self-hosted, OpenAI)
- 🔄 **Auto-syncs file changes** between Supabase and local sessions

## 🏗️ Architecture

```
Flutter App → HTTP API (port 5005) → Aider Engine → LLM Provider
                ↓
         Supabase Storage (files)
```

## 🚀 Quick Start (OpenRouter Testing)

### 1. Prerequisites

- Docker & Docker Compose
- OpenRouter API key (get free one at https://openrouter.ai)
- Supabase project with storage bucket

### 2. Get OpenRouter API Key

1. Go to https://openrouter.ai
2. Sign up and get your API key: `sk-or-xxxxxxx`
3. You get free credits to test!

### 3. Setup Configuration

```bash
# Copy environment template
cp ENV_TEMPLATE .env

# Edit .env - only these 3 lines are required for testing:
AIDER_OPENAI_API_KEY=sk-or-your-openrouter-key-here
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
```

### 4. Start Server

```bash
./start-server.sh
```

The server will be available at:

- **API**: http://localhost:5005
- **Health Check**: http://localhost:5005/health
- **Docs**: http://localhost:5005/docs

## ⚙️ Configuration

### OpenRouter Setup (Default)

Just get your API key from https://openrouter.ai and set:

```bash
AIDER_OPENAI_API_BASE=https://openrouter.ai/api/v1
AIDER_OPENAI_API_KEY=sk-or-your-key-here
AIDER_MODEL=openrouter/qwen/qwq-32b  # Powerful reasoning model
```

**Good models for testing:**

- `openrouter/qwen/qwq-32b` - Powerful reasoning, great for complex DAG creation
- `openrouter/openai/gpt-4o-mini` - Fast, cheap, great for coding
- `openrouter/anthropic/claude-3.7-sonnet` - High quality
- `openrouter/deepseek/deepseek-chat` - Very cheap

### Supabase Setup

1. Create a Supabase project at https://supabase.com
2. Go to **Storage** → Create bucket named `code-files`
3. Get your credentials from **Settings** → **API**:
   ```bash
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_SERVICE_ROLE_KEY=eyJhbG...your-service-role-key
   SUPABASE_BUCKET=code-files
   ```

### Self-hosted LLM Options (Later)

Once OpenRouter testing works, you can switch to self-hosted:

#### Option A: Ollama

```bash
# Install Ollama
curl -fsSL https://ollama.ai/install.sh | sh

# Pull a model
ollama pull llama3.2:latest

# Start with larger context
OLLAMA_CONTEXT_LENGTH=8192 ollama serve
```

**Config:**

```bash
AIDER_OPENAI_API_BASE=http://localhost:11434/v1
AIDER_OPENAI_API_KEY=dummy-key
AIDER_MODEL=llama3.2:latest
```

#### Option B: LM Studio

1. Download LM Studio from https://lmstudio.ai
2. Load a model and start the server
3. Use default port 1234

**Config:**

```bash
AIDER_OPENAI_API_BASE=http://localhost:1234/v1
AIDER_OPENAI_API_KEY=dummy-key
AIDER_MODEL=your-loaded-model-name
```

## 📱 Flutter Integration

Update your Flutter app's `.env`:

```bash
AIDER_URL=http://localhost:5005
AIDER_CHAT_PATH=/v1/chat/completions
AIDER_API_KEY=  # Leave empty for now
```

**Model Selection:**

```dart
final agent = AgentService.fromEnv();
// Use any OpenRouter model:
agent.currentModel = 'openrouter/qwen/qwq-32b';
// Or let it use the default from your server config
agent.currentModel = 'aider';  // Uses server default
```

## 🧪 Testing

### 1. Test Health Check

```bash
curl http://localhost:5005/health
```

Should return:

```json
{
  "status": "healthy",
  "version": "0.xx.x",
  "supabase_connected": true,
  "llm_configured": true,
  "llm_provider": "OpenRouter"
}
```

### 2. Test Chat Completion

```bash
curl -X POST http://localhost:5005/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "openrouter/qwen/qwq-32b",
    "messages": [{"role": "user", "content": "Hello! Can you help me code?"}],
    "stream": true
  }'
```

## 🔄 File Workflow

1. **Upload files** to your Supabase `code-files` bucket
2. **Chat with Aider** via your Flutter app
3. **Files are auto-downloaded** to session directory
4. **Aider processes** the request with OpenRouter
5. **Modified files** are auto-uploaded back to Supabase

## 🐛 Troubleshooting

### Server Won't Start

```bash
# Check logs
docker-compose logs

# Check port availability
lsof -i :5005
```

### Health Check Shows Issues

```bash
# Check your .env file
cat .env

# Test OpenRouter directly
curl -H "Authorization: Bearer sk-or-your-key" \
  "https://openrouter.ai/api/v1/models"
```

### Flutter Connection Issues

- **Android Emulator**: Use `http://10.0.2.2:5005` instead of `localhost`
- **Network**: Ensure Docker port is exposed: `docker ps`

## 🔧 Development

### Running without Docker

```bash
cd server
pip install -r requirements.txt
python main.py
```

### Switch Between Providers

Just update your `.env` file and restart:

```bash
# OpenRouter
AIDER_OPENAI_API_BASE=https://openrouter.ai/api/v1

# Self-hosted Ollama
AIDER_OPENAI_API_BASE=http://localhost:11434/v1

# OpenAI Direct
AIDER_OPENAI_API_BASE=https://api.openai.com/v1
```

## 📄 License

Same as the main Aider project.
