# Low-Level Design (LLD): Event-Driven Orchestration Framework

## 1. Overview

This document defines the LLD for an event-driven orchestration framework in an Azure-based data platform using:

- Azure Databricks
- Azure Data Factory (ADF)
- ADLS Gen2
- Confluent Kafka (CDC where available)
- Delta Lake

The platform follows Medallion architecture (Bronze/Silver/Gold) and uses metadata-driven orchestration from Git.

## 2. Design Principles

- Event-driven orchestration only (no schedule dependency chaining)
- Decoupled dependency evaluation and execution
- Dependency engine does not trigger jobs directly
- Dependency engine creates ADLS trigger files
- ADF/Databricks consume storage/file triggers
- Idempotent and replayable behavior end-to-end

## 3. High-Level Flow

```text
Producers -> event_log (Delta) -> Dependency Engine -> ADLS _READY trigger file
                                                     -> ADF storage trigger
                                                     -> Databricks file consumer
```

## 4. Event Producers

1. Control table changes (EOD completion, batch stages)
2. Bronze readiness conditions
3. ADF pipeline completion events
4. Databricks job completion events (Silver/Gold/DQ)
5. Polling-based ingestion (for non-CDC systems)

## 5. Event Store Design (`orchestration.event_log`)

### 5.1 Schema

| Column | Type | Description |
|---|---|---|
| event_id | STRING | Deterministic idempotency key |
| event_type | STRING | Event category |
| event_source | STRING | KAFKA / ADF / DATABRICKS / POLLER |
| source_system | STRING | FINACLE / FLEXCUBE / etc. |
| source_object | STRING | Job/table/pipeline/stage |
| business_date | DATE | Logical processing date |
| event_ts | TIMESTAMP | Original event time |
| ingest_ts | TIMESTAMP | Ingestion timestamp |
| status | STRING | SUCCESS / FAILED / PARTIAL |
| event_version | INT | Event contract version |
| correlation_id | STRING | Trace identifier |
| run_id | STRING | Upstream execution id |
| partition_date | DATE | Physical partition key |
| payload | STRING | JSON payload |
| payload_hash | STRING | Hash of payload JSON |
| producer_id | STRING | Service principal/app id |
| replay_flag | BOOLEAN | Replay marker |
| replay_batch_id | STRING | Replay batch identifier |
| dedupe_key | STRING | Optional business dedupe key |
| created_by | STRING | Creator identity |
| created_ts | TIMESTAMP | Creation timestamp |

### 5.2 Partitioning and Performance

- Partition by `partition_date`
- Optimize and Z-Order by `(business_date, event_type, source_system, source_object)`
- Compact small files on active partitions

### 5.3 Idempotency

- `event_id` is deterministic hash of key business attributes
- Upsert with Delta `MERGE` on `event_id`
- Duplicate events are ignored safely

### 5.4 Example DDL

```sql
CREATE TABLE IF NOT EXISTS orchestration.event_log (
  event_id STRING,
  event_type STRING,
  event_source STRING,
  source_system STRING,
  source_object STRING,
  business_date DATE,
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,
  status STRING,
  event_version INT,
  correlation_id STRING,
  run_id STRING,
  partition_date DATE,
  payload STRING,
  payload_hash STRING,
  producer_id STRING,
  replay_flag BOOLEAN,
  replay_batch_id STRING,
  dedupe_key STRING,
  created_by STRING,
  created_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (partition_date);
```

## 6. Event Ingestion Patterns

## 6.1 CDC (Kafka -> Databricks -> Delta)

```text
Source DB -> Kafka topic -> Databricks structured streaming -> event_log MERGE
```

```python
from pyspark.sql import functions as F

events = (
  spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "<server>")
  .option("subscribe", "cdc.events")
  .load()
  .selectExpr("CAST(value AS STRING) AS raw_json")
)

normalized = (
  events.select(
    F.get_json_object("raw_json", "$.source_system").alias("source_system"),
    F.get_json_object("raw_json", "$.source_object").alias("source_object"),
    F.get_json_object("raw_json", "$.business_date").cast("date").alias("business_date"),
    F.get_json_object("raw_json", "$.event_ts").cast("timestamp").alias("event_ts"),
    F.lit("CONTROL_STATUS_CHANGED").alias("event_type"),
    F.lit("KAFKA").alias("event_source"),
    F.lit("SUCCESS").alias("status"),
    F.col("raw_json").alias("payload")
  )
  .withColumn("event_id", F.sha2(F.concat_ws("|", "event_type", "source_system", "source_object", F.col("business_date")), 256))
  .withColumn("payload_hash", F.sha2("payload", 256))
  .withColumn("ingest_ts", F.current_timestamp())
  .withColumn("partition_date", F.to_date("event_ts"))
)
```

## 6.2 Polling-based ingestion

```text
ADF/Databricks poller -> source control table -> transitions to COMPLETED -> event_log MERGE
```

## 6.3 Databricks post-run event

```text
Databricks job success -> write DATABRICKS_JOB_COMPLETED into event_log
```

## 6.4 ADF completion event

```text
ADF final activity -> emit completion event file/API call -> event normalization -> event_log
```

## 7. Dependency Engine (Databricks)

## 7.1 Responsibilities

- Read active dependency metadata from Git-deployed config
- Evaluate readiness from `event_log`
- Support `ALL` and `ANY` dependency strategies
- Handle duplicates/late events
- Create idempotent trigger files in ADLS

## 7.2 Processing Model

Recommended: micro-batch Databricks job (frequent interval or event-invoked run).

Reason:
- simpler replay/backfill controls
- SQL-centric dependency evaluation
- lower operational overhead vs continuous streaming

## 7.3 Processing Flow

```text
Load config -> load relevant event window -> evaluate rules -> check existing trigger
-> write _READY.json -> write trigger_audit
```

## 7.4 Sample SQL (ALL strategy)

```sql
WITH required AS (
  SELECT 'daily_finance_ingest' AS job_name, 'EOD_COMPLETE' AS event_type, 'FINACLE' AS source_system
  UNION ALL
  SELECT 'daily_finance_ingest', 'EOD_COMPLETE', 'FLEXCUBE'
),
available AS (
  SELECT DISTINCT event_type, source_system
  FROM orchestration.event_log
  WHERE business_date = DATE('2026-06-23') AND status = 'SUCCESS'
)
SELECT
  r.job_name,
  COUNT(*) AS required_count,
  COUNT(a.event_type) AS matched_count,
  COUNT(*) = COUNT(a.event_type) AS is_ready
FROM required r
LEFT JOIN available a
  ON r.event_type = a.event_type
 AND r.source_system = a.source_system
GROUP BY r.job_name;
```

## 8. Git-Based Configuration

## 8.1 Location

```text
/config/orchestration/jobs/{job_name}.yaml
```

## 8.2 YAML Schema

```yaml
job_name: string
job_type: ADF|DATABRICKS
active: true|false
business_date_offset: integer
dependency_strategy: ALL|ANY
required_events:
  - event_type: string
    source_system: string
    source_object: string|null
    status: string
    business_date_offset: integer
downstream:
  trigger_type: ADLS_FILE
  trigger_path: string
  pipeline_name: string|null
  databricks_job_name: string|null
metadata:
  owner: string
  sla_minutes: integer
```

## 8.3 Example

```yaml
job_name: daily_finance_ingest
job_type: ADF
active: true
business_date_offset: 0
dependency_strategy: ALL
required_events:
  - event_type: EOD_COMPLETE
    source_system: FINACLE
    source_object: EOD
    status: SUCCESS
    business_date_offset: 0
  - event_type: EOD_COMPLETE
    source_system: FLEXCUBE
    source_object: EOD
    status: SUCCESS
    business_date_offset: 0
downstream:
  trigger_type: ADLS_FILE
  trigger_path: /triggers/daily_finance_ingest/{business_date}/_READY.json
  pipeline_name: adf_ingest_finance
  databricks_job_name: null
metadata:
  owner: data-platform
  sla_minutes: 30
```

## 9. Trigger File Generation (Critical)

## 9.1 Convention

```text
/triggers/{job_name}/{business_date}/_READY.json
```

## 9.2 Payload

```json
{
  "trigger_id": "sha256(job_name|business_date|config_version)",
  "job_name": "daily_finance_ingest",
  "job_type": "ADF",
  "business_date": "2026-06-23",
  "dependency_strategy": "ALL",
  "config_version": "git:9f3ab1c",
  "trigger_status": "READY",
  "generated_ts": "2026-06-24T01:10:00Z",
  "correlation_id": "dep-20260624-011000-001",
  "required_events": []
}
```

## 9.3 Idempotency

- Check trigger audit + ADLS file existence before write
- Write once per `job_name + business_date`
- Never overwrite `_READY.json` unless explicit replay mode

## 9.4 Python example

```python
import json
from datetime import datetime, timezone

job_name = "daily_finance_ingest"
business_date = "2026-06-23"
path = f"abfss://orchestration@<storage>.dfs.core.windows.net/triggers/{job_name}/{business_date}/_READY.json"

payload = {
  "trigger_id": "sha256-value",
  "job_name": job_name,
  "business_date": business_date,
  "trigger_status": "READY",
  "generated_ts": datetime.now(timezone.utc).isoformat()
}

dbutils.fs.put(path, json.dumps(payload), overwrite=False)
```

## 10. Trigger Consumption

## 10.1 ADF

- Configure storage event trigger on `/triggers/*/*/_READY.json`
- Pipeline steps: Lookup JSON -> extract parameters -> execute
- Prevent duplicate processing via run audit keyed by `job_name + business_date`

## 10.2 Databricks

- Option A: Auto Loader streaming consumption (recommended for scale)
- Option B: periodic batch scan (simple, low volume)
- Maintain checkpoint and dedupe by trigger_id

## 11. Multi-Condition Join Logic

Supports rules like `FINACLE EOD + FLEXCUBE EOD`.

```sql
WITH required AS (
  SELECT stack(2,
    'EOD_COMPLETE', 'FINACLE',
    'EOD_COMPLETE', 'FLEXCUBE'
  ) AS (event_type, source_system)
),
available AS (
  SELECT DISTINCT event_type, source_system
  FROM orchestration.event_log
  WHERE business_date = DATE('2026-06-23') AND status = 'SUCCESS'
)
SELECT COUNT(*) = COUNT(a.event_type) AS ready
FROM required r
LEFT JOIN available a
  ON r.event_type = a.event_type
 AND r.source_system = a.source_system;
```

## 12. Error Handling and Edge Cases

- Missing events: keep WAITING state, raise SLA alert
- Late arrivals: reevaluate open windows, then trigger when satisfied
- Partial completion: require configured status policy
- Duplicate events: ignored via deterministic `event_id`
- Trigger exists: skip regeneration, upsert audit if needed

## 13. Observability and Monitoring

Use Delta audit tables:

1. `event_processing_audit`
2. `trigger_audit`
3. `job_run_audit`

Track:
- event latency
- dependency wait time
- trigger creation latency
- consumer start delay
- end-to-end completion SLA

Alert on:
- ingestion failures
- waiting jobs beyond SLA
- trigger creation failures
- consumer failures

## 14. Security and Governance

- ADLS ACL/RBAC least privilege by component role
- Unity Catalog governance for orchestration tables
- Separate service principals for producers/engine/consumers
- Avoid sensitive payload content; mask if unavoidable

## 15. Performance and Scalability

- Partition event_log by date and optimize regularly
- Restrict evaluation to configurable lookback horizon
- Precompute active dependency sets for faster evaluation
- Scale producers independently from dependency engine

## 16. End-to-End Example

```text
FINACLE EOD COMPLETE -> event_log
FLEXCUBE EOD COMPLETE -> event_log
Dependency Engine evaluates ALL condition -> READY
Write /triggers/daily_finance_ingest/2026-06-23/_READY.json
ADF storage trigger starts Bronze ingestion
ADF completion event written to event_log
Dependency Engine evaluates Silver dependency -> READY
Write /triggers/silver_finance_curated/2026-06-23/_READY.json
Databricks Silver job starts and completes -> completion event -> downstream continuation
```

---

This LLD is production-oriented and supports replayability, idempotency, decoupled orchestration, and Git-driven governance for enterprise data platforms.
