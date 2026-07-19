# Config templates

Grouped `hoodie.*` properties emitted per decision. Consult when generating the final config bundle.

## Grouping (per §9.3 of the proposal)

1. **Durable table properties** — set at creation, cannot change without rewrite.
2. **Writer properties** — writer-side runtime config.
3. **Reader properties** — reader-side config (per query engine — different keys per engine).
4. **Platform-managed properties** — MDT, target Hudi version, other fixed platform standards.
5. **Workload-dependent tuning variables** — cadences, shuffle parallelism, target sizes.

## Durable table properties

### Table type
```
hoodie.table.type=COPY_ON_WRITE   # or MERGE_ON_READ
```

### Record key
For SimpleKeyGenerator (single field):
```
hoodie.datasource.write.recordkey.field=<column>
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.SimpleKeyGenerator
```

For ComplexKeyGenerator (composite):
```
hoodie.datasource.write.recordkey.field=<col1>,<col2>
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.ComplexKeyGenerator
```

For auto-gen (immutable only) — omit `recordkey.field` and keygenerator entirely.

For TimestampBasedKeyGenerator (timestamp-derived partition):
```
hoodie.datasource.write.recordkey.field=<column>
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.TimestampBasedKeyGenerator
hoodie.keygen.timebased.timestamp.type=<UNIX_TIMESTAMP|DATE_STRING|MIXED|EPOCHMILLISECONDS|SCALAR>
hoodie.keygen.timebased.output.dateformat=<format>
hoodie.keygen.timebased.timezone=UTC
```

For CustomKeyGenerator (mixed):
```
hoodie.datasource.write.recordkey.field=<column>
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.CustomKeyGenerator
```
Additional config for CustomKeyGenerator partition-path spec: `<field1:type1,field2:type2>` where type is SIMPLE or TIMESTAMP.

For NonpartitionedKeyGenerator (unpartitioned):
```
hoodie.datasource.write.recordkey.field=<column>
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator
hoodie.datasource.write.partitionpath.field=
```

### Partition path
```
hoodie.datasource.write.partitionpath.field=<column>
# Empty string for unpartitioned
```

For Hive-style partitioning (column=value folder naming):
```
hoodie.datasource.write.hive_style_partitioning=true
```

### Meta fields
For keep all (default):
```
hoodie.populate.meta.fields=true
```

For selective (Hudi 1.x, `_hoodie_commit_time` only):
```
hoodie.populate.meta.fields=true
# Additional config to nullify other meta fields — 1.x-specific, verify exact key at 1.2.0
```

For disable entirely:
```
hoodie.populate.meta.fields=false
```

## Writer properties

### Operation (per §7.7.5 mapping)
```
hoodie.datasource.write.operation=<upsert|insert|bulk_insert|delete|insert_overwrite|insert_overwrite_table>
```

### Ordering / precombine
```
hoodie.table.ordering.fields=<column>
# Used for resolving record precedence when multiple versions of a key exist in a batch
```

### Small-file handling
Default inline for insert/upsert (mutable). For immutable + posture (a):
```
hoodie.datasource.write.operation=bulk_insert
hoodie.parquet.small.file.limit=0   # disable small-file handling
```

For immutable + posture (b):
```
hoodie.datasource.write.operation=bulk_insert
# Add async clustering — see clustering section
```

For immutable + posture (c):
```
hoodie.datasource.write.operation=insert
hoodie.parquet.small.file.limit=104857600   # 100MB, default
hoodie.parquet.max.file.size=125829120       # 120MB, default
```

### Bulk-insert sort mode
```
hoodie.bulkinsert.sort.mode=<NONE|GLOBAL_SORT|PARTITION_SORT|PARTITION_PATH_REPARTITION|PARTITION_PATH_REPARTITION_AND_SORT>
```

## Reader properties (per engine)

**Reader-side MDT is per-engine — do not assume readers inherit writer MDT config.**

Spark:
```
hoodie.metadata.enable=true
hoodie.enable.data.skipping=true
```

Flink:
```
metadata.enabled=true
read.data.skipping.enabled=true
```

Presto:
```
hudi.metadata-table-enabled=true
```

Athena:
```
hudi.metadata-listing-enabled=true
```

Emit reader config per query engine named in the workload.

## Platform-managed properties

Always emit, don't ask:
```
hoodie.metadata.enable=true                              # MDT on
hoodie.metadata.index.column.stats.enable=false          # col stats + partition stats off (coupled)
hoodie.metadata.index.bloom.filter.enable=false          # experimental in 1.2.0
```

Target Hudi 1.2.0 (implied by dependency version, not a runtime config).

## Cleaner + archival (inline autopilot)

```
hoodie.clean.automatic=true
hoodie.clean.async.enabled=false
hoodie.clean.policy=<KEEP_LATEST_BY_HOURS or KEEP_LATEST_COMMITS>
hoodie.clean.hours.retained=<derived>          # if KEEP_LATEST_BY_HOURS
hoodie.clean.commits.retained=<derived>        # if KEEP_LATEST_COMMITS

hoodie.archive.automatic=true
hoodie.archive.async=false
hoodie.keep.min.commits=<2 * cleaner.commits.retained>
hoodie.keep.max.commits=<keep.min.commits + max(4, cleaner.commits.retained * 0.4)>
hoodie.commits.archival.batch=10
```

**Do NOT emit:** `hoodie.clean.fileversions.retained` — file-versions policy not recommended.

## Index

### SIMPLE
```
hoodie.index.type=SIMPLE
```

### Global SIMPLE
```
hoodie.index.type=GLOBAL_SIMPLE
hoodie.simple.index.update.partition.path=true
```

### BLOOM
```
hoodie.index.type=BLOOM
hoodie.bloom.index.prune.by.ranges=true
# Do NOT set hoodie.bloom.index.use.metadata=true — experimental at 1.2.0
```

### Global BLOOM
```
hoodie.index.type=GLOBAL_BLOOM
hoodie.bloom.index.update.partition.path=true
```

### Record Level Index (partitioned)
```
hoodie.index.type=RECORD_LEVEL_INDEX
hoodie.metadata.record.level.index.enable=true
hoodie.metadata.record.index.max.filegroup.count=10
hoodie.metadata.record.index.min.filegroup.count=1
```

### Global Record Level Index
```
hoodie.index.type=GLOBAL_RECORD_LEVEL_INDEX
hoodie.metadata.global.record.level.index.enable=true
hoodie.metadata.record.index.max.filegroup.count=10000
hoodie.metadata.record.index.min.filegroup.count=10
```

### BUCKET (SIMPLE)
```
hoodie.index.type=BUCKET
hoodie.index.bucket.engine=SIMPLE
hoodie.bucket.index.num.buckets=<derived>
```

### BUCKET (CONSISTENT_HASHING — MOR only)
Not recommended at design time. Escape hatch for skewed-partition BUCKET workloads.

## Compaction (MOR only)

### Inline (default for DataSource/SQL)
```
hoodie.compact.inline=true
hoodie.compact.inline.max.delta.commits=5
hoodie.compact.inline.trigger.strategy=NUM_COMMITS
```

### Async via HoodieStreamer continuous
No config emitted. On by default; disable via `--disable-compaction` CLI flag (not recommended).

### Async via Spark Structured Streaming
```
hoodie.datasource.compaction.async.enable=true
```

### Compaction target IO trap
For projected table size ≥ 1TB + MOR:
```
hoodie.compaction.target.io=2199023255552   # 2TB, adjust based on scale
# Default is 500GB — insufficient at TB+ scale
```

Include as explicit tuning knob in ADR with rationale.

### Compaction selection strategy
Default (LogFileSizeBasedCompactionStrategy):
```
hoodie.compaction.strategy=org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy
```

## Clustering (off by default)

Only emit when enabled (immutable + posture (b), or explicit user request).

### Inline
```
hoodie.clustering.inline=true
hoodie.clustering.inline.max.commits=4
```

### Async
```
hoodie.clustering.async.enabled=true
hoodie.clustering.async.max.commits=5
```

### Plan strategy
```
hoodie.clustering.plan.strategy.class=org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy
hoodie.clustering.plan.strategy.small.file.limit=314572800    # 300MB
hoodie.clustering.plan.strategy.target.file.max.bytes=1073741824  # 1GB
```

### Execution strategy
Default (SparkSortAndSizeExecutionStrategy):
```
hoodie.clustering.execution.strategy.class=org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy
```

### Sort columns (if layout optimization enabled)
```
hoodie.clustering.plan.strategy.sort.columns=<column>
hoodie.layout.optimize.strategy=LINEAR    # or ZORDER or HILBERT
```

### Incremental table services (1.2.0 default, keep on)
```
hoodie.table.services.incremental.enabled=true
```

## Concurrency (V1 = SINGLE_WRITER)

```
hoodie.write.concurrency.mode=SINGLE_WRITER
```

Multi-writer configs (OCC, lock providers, LAZY failed writes policy) deferred to V2+ per §7.6.

## Sample bundles per archetype

### Immutable event stream (EVENT-shape) — Kafka source, small records
```
# Table
hoodie.table.type=COPY_ON_WRITE
hoodie.datasource.write.recordkey.field=            # empty (auto-gen)
hoodie.datasource.write.partitionpath.field=event_ingest_date
hoodie.populate.meta.fields=false                    # or selective
hoodie.datasource.write.hive_style_partitioning=true

# Writer
hoodie.datasource.write.operation=bulk_insert       # posture (b) — see clustering
hoodie.bulkinsert.sort.mode=NONE

# Index
hoodie.index.type=SIMPLE

# Services
hoodie.clean.automatic=true
hoodie.clean.policy=KEEP_LATEST_BY_HOURS
hoodie.clean.hours.retained=48

hoodie.archive.automatic=true
hoodie.keep.min.commits=1000
hoodie.keep.max.commits=1200

hoodie.clustering.async.enabled=true                 # posture (b)
hoodie.clustering.async.max.commits=5

# Platform
hoodie.metadata.enable=true
hoodie.metadata.index.column.stats.enable=false

# Concurrency
hoodie.write.concurrency.mode=SINGLE_WRITER
```

### Mutable dimension table (DIM-shape) — Kafka CDC source, unpartitioned, uniform updates
```
# Table
hoodie.table.type=MERGE_ON_READ                     # experience = some
hoodie.datasource.write.recordkey.field=customer_id
hoodie.datasource.write.partitionpath.field=       # empty (unpartitioned)
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator
hoodie.populate.meta.fields=true                    # mutable default

# Writer
hoodie.datasource.write.operation=upsert
hoodie.table.ordering.fields=updated_at

# Index
hoodie.index.type=GLOBAL_RECORD_LEVEL_INDEX
hoodie.metadata.global.record.level.index.enable=true

# Services
hoodie.compact.inline=true
hoodie.compact.inline.max.delta.commits=5

hoodie.clean.automatic=true
hoodie.clean.policy=KEEP_LATEST_BY_HOURS
hoodie.clean.hours.retained=48

hoodie.archive.automatic=true
hoodie.keep.min.commits=1000
hoodie.keep.max.commits=1200

# Platform
hoodie.metadata.enable=true

# Concurrency
hoodie.write.concurrency.mode=SINGLE_WRITER
```

### Mutable fact table (FACT-shape) — Kafka CDC source, date-partitioned, recent-heavy updates, TB-scale
```
# Table
hoodie.table.type=MERGE_ON_READ                     # experienced → async via HoodieStreamer
hoodie.datasource.write.recordkey.field=trip_id
hoodie.datasource.write.partitionpath.field=trip_date
hoodie.populate.meta.fields=true                    # mutable default

# Writer
hoodie.datasource.write.operation=upsert
hoodie.table.ordering.fields=updated_at

# Index
hoodie.index.type=RECORD_LEVEL_INDEX
hoodie.metadata.record.level.index.enable=true

# Services
# No compaction config — HoodieStreamer continuous handles async automatically

# COMPACTION TARGET IO TRAP — bump for TB scale
hoodie.compaction.target.io=2199023255552            # 2TB

hoodie.clean.automatic=true
hoodie.clean.policy=KEEP_LATEST_BY_HOURS
hoodie.clean.hours.retained=48

hoodie.archive.automatic=true
hoodie.keep.min.commits=1000
hoodie.keep.max.commits=1200

# Platform
hoodie.metadata.enable=true

# Concurrency
hoodie.write.concurrency.mode=SINGLE_WRITER
```
