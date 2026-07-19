# Decision tables

Reference for each decision domain. Consult when deriving a design choice from workload answers.

## Engine

Ask, don't default. If user picks Spark or Flink, proceed. If undecided:

**Flink candidate:** append-only workloads with sub-5-minute visibility target AND continuous streaming source.

**Spark default:** everything else.

For mutable workloads at 5-minute visibility, Spark handles cleanly — Flink's advantage doesn't apply.

## Writer

Derived from source + pipeline_shape (see question-flow.md Q2.9).

### Kafka source (special case)

**Default: HoodieStreamer.** Rationale (surface these in dialogue if user asks why):
- Schema registry integration (Confluent + custom).
- Format support built-in (AvroKafkaSource, JsonKafkaSource, ProtoKafkaSource).
- Exactly-once from Kafka (checkpoint stored in Hudi commits).
- Kafka meta fields propagation.
- Error table for dead-letter routing.
- Continuous mode: ingestion + async compaction + async clustering in one Spark job.
- Transformer chain (SQL-based, custom-class, chained) handles most enrichment and CDC-mapping.

**Reach for Spark DataSource for Kafka only when:**
- Multi-source complexity (multiple Kafka topics + JDBC lookup + multi-table writes).
- ML DataFrame-native library work.
- One-off backfills where a HoodieStreamer job feels heavier than needed.

### Non-Kafka sources (DFS, JDBC, another Hudi table, S3/GCS events, Kinesis, Pulsar)

HoodieStreamer and DataSource are co-equal defaults. Choose based on pipeline_shape:

```
if pipeline_shape == "config-driven":
  → HoodieStreamer
  mode = "continuous" if continuous ingest declared else "run-once"

elif pipeline_shape == "custom code":
  → Spark DataSource

elif pipeline_shape == "SQL-centric":
  → Spark SQL

elif pipeline_shape == "streaming with primitives":
  Ask: writeStream sink vs forEachBatch
  - forEachBatch → Spark DataSource
  - writeStream sink → Ask: stateful primitives needed?
    - Yes → Spark Structured Streaming
    - No → nudge toward HoodieStreamer
```

### Popularity as battle-tested signal

HoodieStreamer + Spark DataSource are the two most-deployed Hudi writer paths — battle-tested, no tuning knobs required, just works out of the box.

Spark SQL: niche, only when user has SQL-only requirement.
Spark Structured Streaming (writeStream sink): rare, only when company-wide streaming framework OR genuine stateful primitives.

For first-time users on Kafka: HoodieStreamer is the safest first Hudi table. For non-Kafka sources: either HoodieStreamer or DataSource is fine.

## Table type

Derived from mutability + experience + update distribution (for mutable).

| Signals | Derived table type + compaction |
|---|---|
| Immutable | COW (silent, no dialogue) |
| Mutable + first-time / fire-and-forget | COW |
| Mutable + some experience | MOR + inline compaction |
| Mutable + experienced + writer is HoodieStreamer continuous | MOR + async (free) |
| Mutable + experienced + writer is DataSource/SQL/Structured Streaming | MOR + async via advanced deployment (standalone compactor) |
| Mutable + some experience + writer is HoodieStreamer continuous | MOR + async (upgrade for free — bonus from writer choice) |

**Provisional in Round 1, refined at §8.3 checkpoint** if writer selection unlocks free async upgrade.

Tradeoff table (in ADR):

|                    | Copy-on-Write (CoW)                     | Merge-on-Read (MoR)                                                              |
| ------------------ | --------------------------------------- | -------------------------------------------------------------------------------- |
| **Write cost**     | High — rewrites whole base files        | Low — appends log blocks                                                         |
| **Read latency**   | Low — reads are plain parquet           | Snapshot reads merge base + logs; read-optimized reads skip logs; compaction periodically brings MoR in line with CoW |
| **Ops surface**    | Minimal — no compaction to run          | Compaction runs as an ongoing service                                            |
| **Typical fit**    | Batch BI, reference tables, first-time users | Streaming upserts, CDC ingestion, experienced operators                     |

### Handling workload-vs-experience tension

If mutability + update distribution point toward MOR but user picked fire-and-forget:

> "Your workload signals point toward MOR (mutable + uniform updates at scale = high write amp for COW), but you picked fire-and-forget which typically means COW. Three reconciliations:
> - (a) Accept COW; ADR flags concrete revisit conditions if write amp materializes.
> - (b) Step up to MOR with inline compaction. Slightly more per-batch latency, no separate service to deploy.
> - (c) Keep the workload smaller and rely on the Operations Agent to flag if COW hits a wall.
>
> Which matches your priorities?"

## Index

Derived from six signals: engine, mutability, partitioning, partition-column stability, projected table size, key characteristics.

### Decision table

| Index | Write cost | Storage | Scope | Best when | Engine (1.2.0) |
|---|---|---|---|---|---|
| **SIMPLE / Global SIMPLE** | O(files listed) per commit | Minimal | Partition or Global | Small tables (<~100M rows), random updates | Spark |
| **BLOOM / Global BLOOM** | Range prune + bloom check | Bloom filters in MDT | Partition or Global | Sub-1-2TB + monotonic keys | Spark |
| **Partitioned RLI** | O(1) via MDT hash-shard | ~few % of record count in MDT | Partition (uniqueness within partition) | Any real scale, partition-stable | Spark + Flink |
| **Global RLI** | O(1) via MDT hash-shard | ~few % of record count in MDT | Global (table-wide uniqueness) | Any scale, unpartitioned or partition-unstable | Spark + Flink |
| **BUCKET** | O(1) via bucket hash | No MDT partition | Partition-scoped only | Bounded key cardinality + balanced partition sizes | Spark + Flink (Flink dominant) |

### Decision pseudocode

```
if immutable:
  → SIMPLE (index cost irrelevant; no tagging happens)

elif engine == "flink":
  if unpartitioned or partition-unstable:
    → GLOBAL_RLI (added in 1.2.0)
  elif partition-stable + key_cardinality_bounded + partition_sizes_balanced:
    → BUCKET
  else:
    → PARTITIONED_RLI

elif engine == "spark":
  if unpartitioned:
    if key_cardinality_bounded_and_stable:
      → BUCKET
    else:
      → GLOBAL_RLI
  elif partitioned + partition-unstable:
    → GLOBAL_RLI (or GLOBAL_BLOOM if sub-1-2TB + monotonic + cost-sensitive)
  elif partitioned + partition-stable:
    if projected_table_size < ~1-2TB:
      if monotonic_keys: → BLOOM
      elif key_cardinality_bounded_and_stable + partition_sizes_balanced: → BUCKET
      else: → SIMPLE (or PARTITIONED_RLI)
    else (projected >= ~1-2TB):
      if key_cardinality_bounded_and_stable + partition_sizes_balanced: → BUCKET
      else: → PARTITIONED_RLI
```

### BUCKET when to prefer over RLI

- Bounded and predictable key cardinality.
- Partition sizes roughly balanced.
- Writer latency tight, MDT record_index sync cost matters.
- Smaller MDT footprint desired.

### BUCKET fails when

- Key cardinality unbounded or growing (any new-record-generating workload — trips, events, orders, logs).
- Skewed partition sizes → recommend RLI/Partitioned RLI (do NOT recommend CONSISTENT_HASHING at design time; niche escape hatch).

### BLOOM caveats

- Effective sub-1-2TB only.
- `hoodie.bloom.index.use.metadata=true` is experimental at 1.2.0 — do NOT recommend at design time.

### Async-buildable framing

Most index decisions are no longer durable at table creation. RECORD_INDEX (both variants), BLOOM (experimental), col stats, secondary index, expression index — all buildable async on live tables using HoodieIndexer, no rewrite needed.

**BUCKET is the durable exception.** Bucket count fixed at creation.

Design implication: for smaller mutable tables, recommend lighter index (SIMPLE) with ADR note that RLI can be added later without rewrite. Avoid over-engineering.

## Partitioning

Query-alignment-first, not size-first.

### Rule engine flow

1. If consumer reads filter on natural low-cardinality dimension → partition by that dimension.
2. If consumer reads filter on time (recent-N-day scans, incremental) → partition by date.
3. If consumer reads are scan-heavy or point-lookup (no partition-aligned filter) → consider unpartitioned (subject to size threshold).

### Projected partition count guardrails

Formula: `projected_partition_count = cardinality(partition_column) × time_buckets_across_retention`

For date-only: `cardinality = 1`, `time_buckets = retention_days` (or months).
For composite `<business_dim>/<date>`: multiply.

- **Green: < 10K partitions** — proceed.
- **Yellow: 10K – 50K** — warn (see warnings.md → PROJECTED_PARTITION_COUNT_YELLOW).
- **Red: > 50K** — reject (see warnings.md → PROJECTED_PARTITION_COUNT_RED).

### Time granularity default

- **Daily** — default. Recent-N-day read patterns align.
- **Monthly** — when daily pushes into yellow/red.
- **Hourly** — rarely recommended. Only when volume >~10GB/hour and consumers explicitly need hourly pruning.

### Immutable raw layer

Default to **ingestion-time partitioning**, not event-time. Raw layer consumers ask "give me new data in the last N hours" — ingestion-time question. Raw doesn't apply business logic.

Override to event time if:
- User explicitly names event-time-filtered downstream reads as dominant.
- Raw layer is unusual with strong event-time semantic upstream.

### Unpartitioned viability

Viable when both hold:
- Total table stays under ~500GB at 2-3 year horizon.
- Consumer read pattern is point-lookup / join / full-scan (not filtered on natural partition dimension).

For point-lookup-dominated workloads with growing key set (like unpartitioned DIM tables), unpartitioned + Global RLI works up to larger sizes (~2TB+) because RLI keeps lookup cost bounded.

Above threshold → partition, even if no natural business filter. Fallback: partition by date-derived column with daily granularity.

## Small-files posture (immutable only)

Three postures — user picks (see question-flow.md Q2.8).

### Recommendation prose adapts to two axes

**Partition cardinality:**
- Low-card (date-only) → any posture viable.
- High-card (composite business dim) → posture (c) recommended.

**Future-consumers axis:**
- Closed universe (all silver consumers exist today) → any posture viable.
- Open universe (new silver pipelines may spin up 6+ months later) + terabytes → (b) or (c) required; (a) becomes warning.

### Matrix

| Scenario | Recommended posture |
|---|---|
| Low-cardinality partition + closed-universe + <500GB | (a) or (b) viable |
| Low-cardinality partition + open-universe or terabytes | (b) — clustering handles async |
| High-cardinality partition | (c) — every batch fans across many partitions; inline small-file handling per file group pays off |

## Retention

Time-travel + incremental lookback window. NOT record lifetime.

### Cleaner policy selection

- Continuous ingest → `KEEP_LATEST_BY_HOURS`.
- Scheduled batch → `KEEP_LATEST_COMMITS`.
- **NEVER `KEEP_LATEST_FILE_VERSIONS`** — operates at file-group level, savepoint interaction awkward, archival can't make progress cleanly.

### Commit-cadence-aware retention default

Timeline latency degrades past ~5K entries; practical target ~1000.

Formula (COW baseline):
```
base_entries_per_commit = 6  # 3 ingestion + 3 cleaner
if MOR + async compaction: adjust += 3 / compaction_cadence_commits  # typically +0.6
if async clustering: adjust += 3 / clustering_cadence_commits  # typically +0.6
entries_per_commit = base_entries_per_commit + adjust

commits_per_day = 1440 / commit_cadence_minutes
timeline_entries_per_day = commits_per_day * entries_per_commit
```

### Safe defaults by commit cadence (COW baseline, cleaner retained = 500)

| Commit cadence | Safe max retention | Wall-clock lookback |
|---|---|---|
| 5 min | ~500 commits | ~1.7 days |
| 10 min | ~500 commits | ~3.5 days |
| 15 min | ~500 commits | ~5 days |
| 30 min | ~500 commits | ~10 days |
| 60 min | ~500 commits | ~20 days |

### Sub-5-minute cadence

If computed safe retention < 1 day (e.g., 1-min cadence):

> "At 1-minute cadence, safe retention drops below 1 day. As a best practice, stabilize a 5-minute cadence pipeline first before attempting sub-5-min ingest."

Not a hard block — user can proceed.

## Cleaner + archival config (inline autopilot)

Emit silently. No user question about cadence.

```
hoodie.clean.automatic=true
hoodie.clean.async.enabled=false
hoodie.clean.policy=<KEEP_LATEST_BY_HOURS or KEEP_LATEST_COMMITS>
hoodie.clean.hours.retained OR hoodie.clean.commits.retained=<derived>

hoodie.archive.automatic=true
hoodie.archive.async=false
hoodie.keep.min.commits=2 * cleaner.commits.retained
hoodie.keep.max.commits=keep.min.commits + max(4, cleaner.commits.retained * 0.4)
hoodie.commits.archival.batch=10
```

**Archival bucketization:** archival bucketizes by instant type. Two buckets: ingestion commits and table-service commits. Each has its own min/max threshold. 2x ratio holds because per-bucket accounting keeps combined active timeline bounded.

## Compaction (MOR only)

Derived from writer + experience.

| Writer | Compaction mode |
|---|---|
| HoodieStreamer continuous | Async in-process, automatic. **No config emitted.** |
| Spark Structured Streaming (writeStream sink) | Inline default; async via `hoodie.datasource.compaction.async.enable=true` if experienced. |
| Spark DataSource | Inline default. Async requires standalone `HoodieCompactor` (advanced deployment). |
| Spark SQL | Same as DataSource. |

For inline:
```
hoodie.compact.inline=true
hoodie.compact.inline.max.delta.commits=5
hoodie.compact.inline.trigger.strategy=NUM_COMMITS
```

### Compaction target IO trap

`hoodie.compaction.target.io` defaults to **500GB per round**. At TB-scale MOR, file groups accumulate uncompacted → log files grow forever → read latency degrades.

If projected size ≥ 1TB with MOR → surface ADR flag: "Bump `hoodie.compaction.target.io` to 2-5TB."

## Clustering

Off by default. Fires only when user asks or when workload signals strongly suggest benefit.

**When Architect surfaces clustering:**
- Immutable + small-files posture (b) — clustering is on the path by choice.
- MOR + async services + workload signals suggest fragmentation over time.

**When enabled:**
```
hoodie.clustering.async.enabled=true
hoodie.clustering.async.max.commits=5
hoodie.clustering.plan.strategy.small.file.limit=300MB
hoodie.clustering.plan.strategy.target.file.max.bytes=1GB
hoodie.table.services.incremental.enabled=true  # 1.2.0 win
```

## Meta-fields

For mutable: silent default — keep all meta fields. No user question.

For immutable + record size ≤1KB: prompt (see question-flow.md Q2.7).

Rule engine mapping:

| Record size | Incremental / CDC needed? | Recommendation |
|---|---|---|
| >1KB or unknown | any | Keep all meta fields |
| 200B–1KB | Yes | Keep all meta fields (needed for incremental) |
| 200B–1KB | No | Offer selective (`_hoodie_commit_time` only) |
| <200B | Yes | Keep all meta fields; ADR notes storage cost |
| <200B | No | Offer disable-entirely OR selective |

### Incremental relation nuance

Disabling meta fields loses Hudi's native efficient incremental relation (uses `_hoodie_commit_time` + timeline metadata to identify touched partitions/file groups). **Falls back to snapshot-read + commit-time filter (like Delta/Iceberg). Still functional**, just slower at scale.

Selective mode preserves the native fast path (still populates `_hoodie_commit_time`).

**Framing:** don't tell users they're "giving up a lot." Present each option's actual cost concretely.

### Mutual exclusion with auto-gen

Auto-gen keys require `_hoodie_record_key` materialized. Two coherent immutable presets:
- User-provided natural key + disable meta fields entirely → max storage saving.
- Auto-gen key + keep meta fields (or selective) → efficient ingest, no stable identity.

## Read behavior

Rule engine mapping from consumer-read-pattern answer to Hudi query type:

| Consumer behavior | Hudi query type |
|---|---|
| Bulk analytical | Snapshot |
| Targeted lookups on record key | Snapshot with RLI-driven file skipping |
| Targeted lookups on non-key column | Snapshot with secondary index (surfaces as ADR flag) |
| Streaming / incremental | Incremental query |
| Read-optimized-tolerant + latency-sensitive on MOR | Read-optimized query |
| Change data capture | CDC query |

Query type is derived, not asked.

## Key generator

| Answer shape | Key generator |
|---|---|
| Single field | `SimpleKeyGenerator` |
| Multi-field (composite) | `ComplexKeyGenerator` |
| Auto-gen (immutable only) | No key generator, no `recordkey.field` |
| Timestamp-derived partition | `TimestampBasedKeyGenerator` |
| Mixed (business + timestamp) | `CustomKeyGenerator` |
| Unpartitioned | `NonpartitionedKeyGenerator` |

Auto-detection: date-string partition columns + natural business record key → `SimpleKeyGenerator`; no explicit `TimestampBasedKeyGenerator` question needed.
