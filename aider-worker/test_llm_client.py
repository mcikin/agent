#!/usr/bin/env python3
"""
Test script to verify LLM client setup
"""
import os
import sys
import asyncio

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

try:
    import litellm
    print("✅ litellm imported successfully")
    try:
        version = getattr(litellm, '__version__', 'unknown')
        print(f"   Version: {version}")
    except:
        print("   Version: (not available)")
    print(f"   Location: {litellm.__file__}")
except ImportError as e:
    print(f"❌ litellm import failed: {e}")
    sys.exit(1)

# Import our LLM client function
from main import get_llm_client, LLM_AVAILABLE

print(f"\nLLM_AVAILABLE: {LLM_AVAILABLE}")

if LLM_AVAILABLE:
    print("\n🔍 Testing LLM client creation...")

    # Test environment variables
    api_base = os.getenv("AIDER_OPENAI_API_BASE", "https://openrouter.ai/api/v1")
    api_key = os.getenv("AIDER_OPENAI_API_KEY", "")
    openrouter_key = os.getenv("OPENROUTER_API_KEY", "")
    model = os.getenv("AIDER_MODEL", "openrouter/qwen/qwq-32b")

    print(f"📋 Environment variables:")
    print(f"   AIDER_OPENAI_API_BASE: {api_base}")
    print(f"   AIDER_OPENAI_API_KEY: {'***' + api_key[-4:] if api_key else 'Not set'}")
    print(f"   OPENROUTER_API_KEY: {'***' + openrouter_key[-4:] if openrouter_key else 'Not set'}")
    print(f"   AIDER_MODEL: {model}")

    # Test LLM client creation
    try:
        client = get_llm_client()
        if client:
            print(f"✅ LLM client created successfully: {client}")
            print(f"   Model: {client.model_name}")
        else:
            print("❌ LLM client creation returned None")
            print("   This could be due to:")
            print("   - Missing API key")
            print("   - Network connectivity issues")
            print("   - Model configuration problems")
    except Exception as e:
        print(f"❌ LLM client creation failed with exception: {e}")
        import traceback
        traceback.print_exc()
else:
    print("❌ LLM not available - litellm not imported")

print("\n🔧 Recommended fixes if LLM client fails:")
print("1. Ensure OPENROUTER_API_KEY or AIDER_OPENAI_API_KEY is set")
print("2. Check network connectivity to OpenRouter")
print("3. Verify AIDER_MODEL is set to a valid OpenRouter model")
print("4. Rebuild container: docker-compose build --no-cache aider-worker")
print("5. Check logs for detailed error messages")
