from __future__ import annotations

import sys
from pathlib import Path


# Ensure repository root is importable so implicit namespace packages like `modules/`
# work reliably under pytest on Windows.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

