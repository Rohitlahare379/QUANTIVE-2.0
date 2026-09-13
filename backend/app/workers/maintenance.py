"""Small durable-work scheduler for the data foundation.

The scheduler performs no market-data or database work itself.  It only wakes
Dramatiq actors, leaving all durable state transitions to the lease-aware workers.
"""

import asyncio
import logging

from app.core.config import settings
from app.workers.config import broker  # Ensure `.send()` targets Redis.
from app.workers.cagg_refresh import process_cagg_refresh
from app.workers.gap_repair import scan_gaps_and_schedule
from app.workers.historical_merge import process_historical_merge
from app.workers.export import enqueue_export_recovery
from app.workers.live_strategy import enqueue_live_strategy_evaluation
from app.workers.strategy_comparison import enqueue_strategy_comparison_production
from app.workers.strategy_monitoring import enqueue_strategy_monitoring

logger = logging.getLogger(__name__)


def enqueue_maintenance_cycle() -> None:
    """Wake every durable data-maintenance workflow once."""
    scan_gaps_and_schedule.send()
    process_historical_merge.send()
    process_cagg_refresh.send()
    enqueue_export_recovery()
    enqueue_live_strategy_evaluation()
    enqueue_strategy_comparison_production()
    enqueue_strategy_monitoring()


async def run_maintenance_loop(interval_seconds: float | None = None) -> None:
    interval = interval_seconds or settings.MAINTENANCE_INTERVAL_SECONDS
    while True:
        try:
            enqueue_maintenance_cycle()
        except Exception:
            logger.exception("Unable to enqueue Quantive maintenance cycle")
        await asyncio.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    asyncio.run(run_maintenance_loop())
