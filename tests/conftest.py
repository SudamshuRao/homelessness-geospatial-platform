# tests/conftest.py
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    # put repo root at front so local packages are found first
    sys.path.insert(0, str(ROOT))
