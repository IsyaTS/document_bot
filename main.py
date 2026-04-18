from __future__ import annotations

import asyncio
import logging

from app.bot import run_bot
from app.config import load_settings


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = load_settings()
    logging.info("Starting document bot")
    asyncio.run(run_bot(settings))


if __name__ == "__main__":
    main()
