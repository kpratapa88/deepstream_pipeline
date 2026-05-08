"""
Root conftest.py — ensures the workspace root is on sys.path so that
`from backend.xxx import ...` works when running pytest from the project root.
"""
import sys
import os

# Add workspace root to path (pytest usually does this, but explicit is safer)
sys.path.insert(0, os.path.dirname(__file__))
