# Dead Letter Queue Recovery Runbook

## Overview
Jobs enter the Dead Letter Queue (DLQ) when they permanently fail (e.g., throwing a `PermanentError` or exhausting the `max_retries` limit). Because Quantive depends on highly available continuous streaming data, a buildup in the DLQ indicates that specific assets have stopped receiving data and the `sync_ranges` metadata is drifting from reality.

## Common Causes

### 1. Asset Delisted from Exchange
**Symptom:** DLQ `actor_name` shows `sync_historical_data` repeatedly failing with `BinanceAPIException (Invalid Symbol)`.
**Why:** Binance has permanently removed the trading pair.

### 2. Upstream Network Outage
**Symptom:** Massive uniform DLQ spikes across all assets simultaneously.
**Why:** Cloudflare outage, AWS partition, or Binance complete systems failure exceeding the 1-hour Dramatiq TimeLimit.

### 3. Database Schema / Constraint Violation
**Symptom:** DLQ jobs failing with `IntegrityError` or `DataError`.
**Why:** A recent deployment introduced a TimescaleDB schema change or a bug in `Raw1mCandle` serialization.

---

## Recovery Procedure

### Action 1: Introspect the DLQ
Determine what the error was.
```bash
docker exec -it quantive-worker redis-cli lrange dramatiq:default.DQ 0 0
```
Inspect the `traceback` and `actor_name` fields in the JSON.

### Action 2: Requeue Jobs (Temporary Failures)
If the failure was due to a temporary network blip or an accidental DB lock that has now been resolved, requeue the jobs back to the main Dramatiq exchange:
```bash
docker exec -it quantive-worker dramatiq requeue app.workers.tasks
```
Wait 5 minutes and check if `dlq_job_count` reaches 0 on the Grafana dashboard.

### Action 3: Asset Deactivation Workflow (Permanent Failures)
If the exchange has delisted the asset, or the asset is fundamentally broken, you must prevent the background crons from endlessly spawning new jobs for it.

1. **Update `asset_registry`**
   ```sql
   UPDATE asset_registry SET is_active = false WHERE symbol = 'BAD_ASSET_USDT';
   ```
2. **Purge the DLQ**
   If the existing DLQ jobs are garbage because the asset is dead, flush the queue:
   ```bash
   docker exec -it quantive-worker redis-cli del dramatiq:default.DQ
   ```

## Escalation
If `dlq_job_count` > 1000 and the cause is unknown, escalate to the Principal Data Integrity Engineer immediately.
