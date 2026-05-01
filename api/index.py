"""Vercel entrypoint — re-exports the Flask app from the parent server.py.

Vercel's @vercel/python builder looks for a `app` variable.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server import app  # noqa: E402
