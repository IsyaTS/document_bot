from __future__ import annotations

import sys
from pathlib import Path

import uvicorn

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from platform_core.settings import load_platform_settings
from platform_runtime import create_app


def main() -> None:
    settings = load_platform_settings()
    uvicorn.run(create_app(), host=settings.api_host, port=settings.api_port)


if __name__ == "__main__":
    main()
