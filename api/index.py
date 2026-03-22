"""
Vercel serverless entrypoint — wraps the FastAPI app for Vercel Python runtime.
"""
from backend.main import app

# Vercel expects the ASGI app as `app` or `handler`
# With @vercel/python runtime, export the FastAPI app directly
