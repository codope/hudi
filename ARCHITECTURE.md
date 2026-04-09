# ARCHITECTURE.md — Apache Hudi System Architecture

## What is Apache Hudi?

Apache Hudi is an open data lakehouse platform that brings database-like functionality — ACID transactions, upserts, deletes, indexes, and incremental processing — to cloud object storage (S3, GCS, ADLS, HDFS) while keeping data in open file formats (Parquet, ORC, HFile). It supports both streaming and batch workloads across multiple compute engines (Spark, Flink, Presto, Trino, Hive).

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Query Engines (Readers)                            │
│   Spark SQL │ Presto │ Trino │ Hive │ Flink │ Custom (Java client)         │
└──────┬──────┴────┬───┴───┬───┴──┬───┴───┬───┴──────────────────────────────┘
       │           │       │      │       │
       ▼           ▼       ▼      ▼       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       Hudi Read Path                                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                      │
│  │  Snapshot     │  │ Incremental  │  │ Read-Optimized│                     │
│  │  Queries      │  │ Queries      │  │ Queries (CoW) │                     │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                      │
│         │                 │                  │                               │
│         ▼                 ▼                  ▼                               │
│  ┌──────────────────────────────────────────────────┐                       │
│  │          File System View / File Index            │                      │
│  │    (resolves file slices for a given instant)     │                      │
│  └──────────────────────┬───────────────────────────┘                       │
│                         │                                                   │
│  ┌──────────────────────▼───────────────────────────┐                       │
│  │        Metadata Table (Indexes)                   │                      │
│  │  Files │ ColumnStats │ BloomFilter │ RecordIndex  │                      │
│  └───────────────────────────────────────────────────┘                       │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────────┐
│                        Storage Layer                                        │
│                                                                             │
│   <base_path>/                                                              │
│   ├── .hoodie/              ← Timeline + Metadata Table                     │
│   │   ├── hoodie.properties ← Table config                                 │
│   │   ├── timeline/         ← Active & archived instants                   │
│   │   └── metadata/         ← Internal MoR table for indexes               │
│   └── <partitions>/         ← Data files (base + log)                      │
│       ├── *.parquet         ← Base files (columnar)                        │
│       └── *.log.*           ← Log files (row-level deltas)                 │
│                                                                             │
│   Cloud Storage: S3 │ GCS │ ADLS │ HDFS │ Local FS                         │
└─────────────────────────────────────────────────────────────────────────────┘
                               ▲
┌──────────────────────────────┴──────────────────────────────────────────────┐
│                        Hudi Write Path                                      │
│  ┌──────────────────────────────────────────────────┐                       │
│  │           Write Client (per engine)               │                      │
│  │  SparkRDDWriteClient │ HoodieFlinkWriteClient │   │                      │
│  │  HoodieJavaWriteClient                            │                      │
│  └──────────┬────────────────────────────────────────┘                      │
│             │                                                               │
│  ┌──────────▼───────────┐  ┌─────────────────────────┐                     │
│  │    HoodieTable       │  │   HoodieIndex            │                     │
│  │  (CoW / MoR)         │  │ (Bloom/Bucket/Simple/    │                     │
│  │                      │  │  Record Index)            │                     │
│  └──────────┬───────────┘  └────────────┬────────────┘                     │
│             │                           │                                   │
│  ┌──────────▼───────────────────────────▼────────────┐                     │
│  │          Action Executors                          │                     │
│  │  Commit │ DeltaCommit │ Compact │ Cluster │ Clean  │                     │
│  └──────────┬────────────────────────────────────────┘                     │
│             │                                                               │
│  ┌──────────▼───────────┐  ┌─────────────────────────┐                     │
│  │   Write Handles      │  │ Conflict Resolution      │                     │
│  │  Create│Merge│Append  │  │ (OCC with distributed   │                     │
│  │                      │  │  locks)                   │                     │
│  └──────────────────────┘  └─────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────────────────┘
                               ▲
┌──────────────────────────────┴──────────────────────────────────────────────┐
│                     Ingestion Layer                                          │
│  Spark DataSource │ Flink DataStream │ DeltaStreamer │ Kafka Connect         │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Concepts

### Timeline

The timeline is the backbone of Hudi's transactional model. Every mutation to a table creates a **timeline instant** — a record of an action with begin and completion timestamps.

**Action types:**
- `commit` — Produces new base files (CoW writes)
- `deltacommit` — Produces log files (MoR writes)
- `replacecommit` — Atomically replaces file groups (clustering, insert overwrite)
- `compaction` — Merges log files into base files
- `logcompaction` — Consolidates multiple log files into one
- `clean` — Removes obsolete file versions
- `indexing` — Async index building
- `rollback` / `savepoint` / `restore` — Recovery operations

**State machine:** REQUESTED → INFLIGHT → COMPLETED

Timestamps are monotonically increasing across all processes. Completed instants carry both begin and completion times, enabling deterministic ordering for concurrent operations.

The active timeline lives in `.hoodie/timeline/`. Completed instants are archived to `.hoodie/timeline/history/` using an LSM-based storage format (manifests + Parquet files).

### Table Types

#### Copy-on-Write (CoW)
All data lives in base files (Parquet). Every write produces entirely new versions of affected files. Optimized for **read-heavy** workloads — queries read columnar files directly with zero merge overhead.

```
Write: base_v1.parquet → (update) → base_v2.parquet  (full rewrite)
Read:  Scan base_v2.parquet directly
```

#### Merge-on-Read (MoR)
Writes append to log files (row-oriented Hudi log format). Reads merge base files with log files at query time. Optimized for **write-heavy** workloads — writes are fast (append-only), reads pay merge cost.

```
Write: base_v1.parquet + delta_1.log + delta_2.log  (append only)
Read:  Merge base_v1.parquet + delta_1.log + delta_2.log at query time
Compaction: base_v1 + logs → base_v2.parquet  (periodic background merge)
```

MoR also supports a **read-optimized** query that reads only base files (stale but fast).

### File Groups and File Slices

Data is organized into **file groups**, each identified by a unique file ID. A file group belongs to a single partition.

A **file slice** is a snapshot of a file group at a point in time:
- One optional **base file** (`<fileId>_<writeToken>_<beginTime>.parquet`)
- Zero or more **log files** (`.<fileId>_<requestedTime>.log.<version>_<writeToken>`)

Records with the same record key always map to the same file group (1:1 mapping), which is fundamental to Hudi's efficiency — updates only touch one file group.

### Query Types

| Query Type | Description | Table Types |
|------------|-------------|-------------|
| **Snapshot** | Latest committed state of all records | CoW, MoR |
| **Time Travel** | State at a specific point in time | CoW, MoR |
| **Incremental** | Only records changed between two instants | CoW, MoR |
| **CDC** | Before/after images of modifications | MoR |
| **Read-Optimized** | Only base files (fast but potentially stale) | MoR |

### Indexing

Indexes map record keys to file groups, enabling efficient upserts (knowing which file to update) and point lookups.

**Index types:**
| Index | Scope | Mechanism | Best For |
|-------|-------|-----------|----------|
| Bloom | Partition | Bloom filters in base file footers | Large batch workloads |
| Simple | Partition/Global | Brute-force key lookup | Small datasets |
| Bucket | Partition | Hash-based bucketing (consistent hashing) | High-throughput streaming |
| Record Index | Global | Metadata table partition mapping keys → file groups | Large tables with frequent upserts |
| HBase | Global | External HBase table | Legacy, being replaced by Record Index |

**Metadata-backed indexes** (stored in `.hoodie/metadata/`):
- **Files index** — Partition → file listing (replaces expensive `listFiles()` on cloud storage)
- **Column stats** — Per-file min/max/null counts for data skipping
- **Bloom filters** — Serialized bloom filters for key presence checks
- **Record index** — Record key → file group mapping
- **Secondary indexes** — SQL-style indexes on non-key columns
- **Expression indexes** — Indexes on computed expressions (e.g., `year(ts)`)

## Data Flow

### Write Path

```
1. Initialize: Acquire begin timestamp → create timeline instant (REQUESTED)

2. Index Lookup: For each incoming record, look up the index to find
   the target file group (or assign a new one for inserts)

3. Write Data:
   ├── CoW: Create new base files via HoodieCreateHandle / HoodieMergeHandle
   └── MoR: Append to log files via HoodieAppendHandle

4. Commit (within distributed lock):
   a. Check for conflicts with concurrent writers
   b. Update metadata table (file listing, indexes)
   c. Write completion time → instant moves to COMPLETED
   d. Optionally schedule table services (compaction, clustering, cleaning)

5. Release lock
```

### Conflict Resolution (Optimistic Concurrency Control)

Hudi uses optimistic concurrency control with pluggable lock providers (ZooKeeper, DynamoDB, Hive metastore, in-process JVM lock).

During commit, within the lock critical section:
1. Find all instants completed after the writer's snapshot time
2. Check for overlapping file group writes → abort if found
3. Check for concurrent compaction/clustering on same file groups → abort if found
4. Optionally compare record keys of concurrent writes → abort on overlap
5. If no conflicts: finalize commit

**Conflict resolution strategies:**
- `SimpleConcurrentFileWritesConflictResolutionStrategy` — File-level conflict detection
- `PreferWriterConflictResolutionStrategy` — Prefers ingestion writers over table services; can abort clustering if writes are pending

### Read Path

```
1. Select snapshot time (latest or specific instant for time travel)

2. Resolve file system view:
   a. List all file groups from metadata table (files index)
   b. Filter out replaced file groups (from replacecommit)
   c. For each file group, construct file slice:
      - Base file with greatest begin time ≤ snapshot time
      - Log files with completion time ≤ snapshot time

3. Apply index-based pruning:
   a. Partition pruning (partition path filters)
   b. Data skipping via column stats (min/max filtering)
   c. Bloom filter checks for key-based lookups

4. Read and merge:
   ├── CoW: Read base files directly (no merge needed)
   └── MoR: Read base file + apply log file deltas using RecordMerger
```

### Table Services

These background operations maintain table health:

#### Compaction (MoR only)
Merges accumulated log files into base files, converting row-level deltas into optimized columnar format. Reduces read amplification at the cost of write amplification.

```
Schedule: Identify file groups with enough log files → create compaction plan
Execute:  For each file group: read base + logs → merge → write new base file
```

#### Log Compaction (MoR only)
Minor compaction that consolidates multiple small log files into a single larger log file without producing a new base file. Reduces read amplification with minimal write amplification.

#### Clustering
Reorganizes data layout for better query performance (e.g., sorting by frequently-queried columns, resizing files to target size).

```
Schedule: Select file groups by size/age → group into clustering units
Execute:  Read groups → sort/partition by clustering key → write new file groups
Finalize: Atomic replacecommit swaps old → new file groups
```

Clustering strategies: `SizeBasedClusteringPlanStrategy`, `StreamCopyClusteringPlanStrategy`, `ConsistentBucketClusteringPlanStrategy`

#### Cleaning
Removes file versions that are no longer needed by any active reader. Retention policies:
- `keep_latest_commits` — Keep files reachable from the last N commits
- `keep_latest_file_versions` — Keep the last N versions of each file

## Engine Integration Architecture

### Spark Integration

```
┌─────────────────────────────────────────────────┐
│              Spark Application                   │
│  spark.read.format("hudi").load(path)           │
│  spark.sql("SELECT * FROM hudi_table")          │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│        HoodieSparkSessionExtension               │
│  (SQL parser, custom rules, stored procedures)   │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│           DefaultSource (DataSource V1/V2)        │
│  ┌──────────────┐  ┌─────────────────────────┐  │
│  │HoodieBaseRel │  │ DML Commands            │  │
│  │(reads)       │  │ (Insert/Update/Delete/  │  │
│  │              │  │  Merge Into)            │  │
│  └──────┬───────┘  └──────────┬──────────────┘  │
│         │                     │                  │
│  ┌──────▼───────┐  ┌─────────▼──────────────┐  │
│  │HoodieFileIdx │  │ SparkRDDWriteClient     │  │
│  │(pruning)     │  │ (bulk insert/upsert/    │  │
│  │              │  │  delete/insert overwrite)│  │
│  └──────────────┘  └────────────────────────┘  │
└─────────────────────────────────────────────────┘
                     │
          Version-Specific Adapters
    ┌────────┬───────┼────────┬────────┐
    ▼        ▼       ▼        ▼        ▼
  3.3.x   3.4.x   3.5.x   4.0.x   (future)
```

Spark integration supports 50+ stored procedures (call via `CALL <procedure>(...)`) for operations like compaction, clustering, cleaning, metadata management, etc.

### Flink Integration

```
┌─────────────────────────────────────────────────┐
│              Flink Application                    │
│  (DataStream API / Table API / SQL)              │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│         HoodieSink (Sink V2)                     │
│  ┌───────────────────────────────────────────┐  │
│  │  Pipeline: Partition → Buffer → Write →   │  │
│  │  Commit (with checkpointing)              │  │
│  └───────────────────────────────────────────┘  │
│  Write modes: Stream, BulkInsert, Bucket,       │
│  Append, Bootstrap                               │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│       StreamWriteOperator                        │
│  → HoodieFlinkWriteClient                        │
│  → Checkpoint-based commit coordination          │
│  → Async compaction/clustering operators         │
└─────────────────────────────────────────────────┘
```

Flink integration uses checkpoint barriers for commit coordination — each checkpoint boundary triggers a commit of buffered writes.

### Utilities / DeltaStreamer

```
Source (Kafka, DFS, JDBC, S3, etc.)
  → SchemaProvider (Registry, File, Hive, etc.)
    → Transformer (SQL-based, optional)
      → Write Client (Spark-based)
        → Post-write: Sync (Hive, Glue) + Checkpoint
```

DeltaStreamer (`HoodieDeltaStreamer`) and the newer Streamer framework provide continuous ingestion from various sources. Multi-table support via `HoodieMultiTableDeltaStreamer`.

### Metastore Sync

```
HoodieSyncTool (abstract)
  ├── HiveSyncTool        → Hive Metastore (HMS)
  ├── GlueSyncTool        → AWS Glue Data Catalog
  ├── DataHubSyncTool     → DataHub metadata platform
  └── AdbSyncTool         → Alibaba ADB
```

After writes, sync tools update external catalogs with partition information, schema changes, and table properties so query engines can discover Hudi tables.

## Metadata Table Architecture

The metadata table is an internal Hudi MoR table stored at `.hoodie/metadata/`. It replaces expensive cloud storage `list` operations with indexed lookups.

**Partitions (each is a separate index):**

| Partition | Key | Value | Purpose |
|-----------|-----|-------|---------|
| `files` | Partition path | File name → metadata map | File listing without `listFiles()` |
| `column_stats` | Hash(col, partition, file) | Min/max/null counts | Data skipping |
| `bloom_filters` | Hash(partition, file) | Serialized bloom filter | Key presence check |
| `record_index` | Record key | File group + instant info | O(1) key-to-file lookup |
| `secondary_index_<name>` | `<secondary-key>$<primary-key>` | isDeleted flag | SQL-style secondary index |
| `expr_index_<name>` | Expression result | File group info | Function-based index |

The metadata table is updated transactionally as part of each commit. It uses the same MoR format as user tables, with compaction to maintain read performance.

## Concurrency Model

### Writers vs Writers
Optimistic concurrency control. Writers proceed independently and check for conflicts only at commit time (within a distributed lock). Conflicts are detected at the file-group level.

### Writers vs Readers
Snapshot isolation via MVCC. Readers pick a snapshot time and see a consistent view. Writers create new file versions without modifying existing ones. Readers and writers never contend.

### Writers vs Table Services
- **Compaction:** Non-blocking. Runs concurrently with writers. Excludes file groups being clustered.
- **Clustering:** May be aborted if concurrent writers touched the same file groups.
- **Cleaning:** Never conflicts (only removes files no longer readable at any active snapshot).
- **Indexing:** Async. Lock acquired only for timeline updates, not during index building.

### Lock Providers
Pluggable via `LockProvider` interface:
- Apache ZooKeeper
- Amazon DynamoDB
- Apache Hive Metastore
- JVM in-process (single-writer scenarios)

## Record Merging

When reading MoR tables or resolving conflicts, records must be merged. Hudi provides two APIs:

### HoodieRecordPayload (Legacy)
Interface with `preCombine()` and `combineAndGetUpdateValue()` methods. Implementations: `OverwriteWithLatestAvroPayload`, `PartialUpdateAvroPayload`, various Debezium payloads.

### HoodieRecordMerger (Current, 1.0+)
Stateless merger with strategies:
- **EVENT_TIME_BASED** — Latest record by a user-specified event time field
- **COMMIT_TIME_BASED** — Latest record by commit timestamp
- **CUSTOM** — User-defined merge logic
- **PAYLOAD_BASED** — Delegates to `HoodieRecordPayload`

Engine-specific implementations: `DefaultSparkRecordMerger`, `HoodieFlinkRecordMerger`.

## Meta Fields

Every Hudi record carries five meta fields:

| Field | Purpose |
|-------|---------|
| `_hoodie_commit_time` | Commit timestamp for record-level history |
| `_hoodie_commit_seqno` | Unique sequence number within a commit |
| `_hoodie_record_key` | Materialized record key |
| `_hoodie_partition_path` | Partition the record belongs to |
| `_hoodie_file_name` | File containing this record version |

These fields enable incremental queries, record-level time travel, and debugging.

## Log File Format

Hudi's native log format is a sequence of blocks:

```
┌──────────────┐
│  Magic (#HUDI#) │  6 bytes
├──────────────┤
│  Block Length │  8 bytes
├──────────────┤
│  Version     │  4 bytes
├──────────────┤
│  Block Type  │  4 bytes (Data/Delete/Command/CDC)
├──────────────┤
│  Header      │  Variable (metadata key-value pairs)
├──────────────┤
│  Content     │  Variable (Avro/Parquet/HFile encoded records)
├──────────────┤
│  Footer      │  Variable
├──────────────┤
│  Total Length │  8 bytes (verification)
└──────────────┘
```

**Block types:**
- **Avro Block** — Row-oriented records (default for MoR writes)
- **Parquet Block** — Columnar records
- **HFile Block** — Sorted key-value pairs (efficient range scans)
- **Delete Block** — Tombstones for deleted records
- **Command Block** — Reader instructions (e.g., `ROLLBACK_PREVIOUS_BLOCK`)
- **CDC Block** — Change data capture with before/after images

## Key Design Decisions

1. **Single file group per record key** — Guarantees that upserts/deletes touch exactly one file group, enabling efficient conflict detection and index maintenance.

2. **Log-structured storage** — MoR tables use append-only log files for writes, amortizing write cost. Compaction converts logs to columnar format in the background.

3. **Self-managing metadata** — The metadata table eliminates expensive cloud storage operations (listing, stat calls) and enables sophisticated indexing without external systems.

4. **Engine-agnostic core** — Write/read logic in `hudi-client-common` and `hudi-common` is engine-independent. Engine modules only provide implementations of abstract interfaces (`HoodieEngineContext`, `HoodieData`, engine-specific table types).

5. **Timeline as source of truth** — All state transitions are recorded on the timeline. Recovery, time travel, incremental queries, and garbage collection all derive from the timeline.

6. **Pluggable everything** — Index types, merge strategies, conflict resolution, lock providers, storage backends, schema providers, and ingestion sources are all pluggable via configuration.
