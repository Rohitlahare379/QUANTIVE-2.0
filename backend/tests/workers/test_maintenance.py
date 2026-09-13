from unittest.mock import AsyncMock, patch

import pytest

from app.workers.maintenance import enqueue_maintenance_cycle


def test_maintenance_cycle_wakes_all_durable_data_workflows():
    with patch("app.workers.maintenance.scan_gaps_and_schedule.send") as scan_send, \
         patch("app.workers.maintenance.process_historical_merge.send") as merge_send, \
         patch("app.workers.maintenance.process_cagg_refresh.send") as cagg_send, \
         patch("app.workers.maintenance.enqueue_export_recovery") as export_recovery_enqueue, \
         patch("app.workers.maintenance.enqueue_live_strategy_evaluation") as live_strategy_enqueue, \
         patch("app.workers.maintenance.enqueue_strategy_comparison_production") as strategy_comparison_enqueue, \
         patch("app.workers.maintenance.enqueue_strategy_monitoring") as strategy_monitoring_enqueue:
        enqueue_maintenance_cycle()

    scan_send.assert_called_once_with()
    merge_send.assert_called_once_with()
    cagg_send.assert_called_once_with()
    export_recovery_enqueue.assert_called_once_with()
    live_strategy_enqueue.assert_called_once_with()
    strategy_comparison_enqueue.assert_called_once_with()
    strategy_monitoring_enqueue.assert_called_once_with()
