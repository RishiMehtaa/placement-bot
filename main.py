"""
main.py
-------
Entry point for the WhatsApp Placement Intelligence System.
This file is expanded in each subsequent phase.
Currently: verifies environment and logger are working correctly.
"""

from utils.logger import get_logger
from config.settings import get_settings

logger = get_logger(__name__)


def main() -> None:
    settings = get_settings()
    logger.info(
        "placement_bot_startup",
        env=settings.env,
        log_level=settings.log_level,
        api_port=settings.api_port,
        queue_backend=settings.queue_backend,
        target_group_jid=settings.target_group_jid,
    )
    logger.info("phase_0_complete", message="Environment loaded successfully.")


if __name__ == "__main__":
    main()