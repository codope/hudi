# AGENTS.md — Guidance for AI Agents Working on Apache Hudi

## Before You Start

1. **Read CLAUDE.md** for build commands, repository layout, and coding conventions.
2. **Identify the affected module(s)** — changes rarely touch just one module. A feature may need changes in `hudi-common` (models/config), `hudi-client-common` (logic), and one or more engine modules (Spark/Flink/Java).
3. **Understand the table version** — Hudi is currently on Table Version 8 (the 1.0 format). Timeline, metadata table, and file layout follow the 1.0 spec.
4. **Check for existing RFCs** — the `rfc/` directory contains design documents for major features. Read the relevant RFC before making architectural changes.

## Module Dependency Rules

```
hudi-io  (lowest level — storage primitives, protobuf)
  ↑
hudi-common  (timeline, metadata, schema, config, models)
  ↑
hudi-hadoop-common  (Hadoop FS compatibility)
  ↑
hudi-client-common  (engine-agnostic write/read logic, indexing, actions)
  ↑
hudi-spark-client / hudi-flink-client / hudi-java-client  (engine-specific clients)
  ↑
hudi-spark-datasource / hudi-flink-datasource  (user-facing datasource APIs)
  ↑
hudi-utilities / hudi-sync / hudi-kafka-connect  (tools and integrations)
```

**Key rules:**
- `hudi-common` and `hudi-io` must NOT depend on any engine (Spark, Flink, Hadoop MR).
- `hudi-client-common` must NOT depend on Spark or Flink — only on `hudi-common` and `hudi-io`.
- Engine-specific code belongs in the corresponding engine module.
- No Scala imports in Java packages (enforced by `style/import-control.xml`).

## How to Add a New Feature

### New Configuration Property
1. Add `ConfigProperty<T>` in the appropriate config class (e.g., `HoodieWriteConfig`, `HoodieCompactionConfig`).
2. Include `.key()`, `.defaultValue()`, `.sinceVersion()`, `.withDocumentation()`.
3. Wire it through the builder pattern if using `HoodieWriteConfig.Builder`.
4. Add to `HoodieConfig` subclass's `PROPERTY_MAP` if applicable.

### New Index Type
1. Implement `HoodieIndex<I, O>` in `hudi-client-common`.
2. Register in `HoodieIndexFactory`.
3. Add engine-specific implementations in spark/flink/java client modules.
4. Add metadata table partition type if it's a metadata-backed index.

### New Table Action
1. Create an executor extending `BaseActionExecutor` in `hudi-client-common/.../table/action/`.
2. Wire it into `HoodieTable` (the abstract base) and each engine's table implementation.
3. If the action has REQUESTED/INFLIGHT/COMPLETED states, add the action type to `HoodieTimeline`.

### New Spark SQL Command / Stored Procedure
1. Stored procedures go in `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/procedures/`.
2. Register in `HoodieProcedures`.
3. SQL commands go in `hudi-spark-datasource/hudi-spark3-common/src/main/scala/org/apache/spark/sql/hudi/command/`.

### New Source for Utilities/Streamer
1. Extend `Source` in `hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/`.
2. Implement `fetchNewDataInAvroFormat()` or `fetchNewDataInRowFormat()`.
3. Add corresponding config class in `hudi-utilities/.../config/`.
4. Add schema provider if the source needs custom schema resolution.

### New Sync Tool
1. Extend `HoodieSyncTool` in `hudi-sync/hudi-sync-common`.
2. Implement `HoodieSyncClient` for the target metastore.
3. Create a new sub-module under `hudi-sync/` if it has external dependencies.

## Testing Guidelines

- **Unit tests** go alongside the source in the same module's `src/test/java/` or `src/test/scala/`.
- **Naming:** `<ClassName>Test.java` for the primary test class.
- **Framework:** JUnit 5 (`@Test`, `@BeforeEach`, `@ParameterizedTest`). Use Mockito for mocking.
- **Shared test utilities** live in `hudi-tests-common`.
- **Tag functional tests** with `@Tag("functional")` so they run under `-Pfunctional-tests`.
- **Run tests for your module:** `mvn test -pl hudi-client/hudi-client-common -Punit-tests`
- **Spark/Flink tests** often need the engine profile: `mvn test -pl hudi-spark-datasource/hudi-spark -Dspark3.5 -Punit-tests`

## Common Pitfalls

1. **Forgetting engine modules** — If you add a method to `HoodieTable` or `BaseHoodieWriteClient`, you must implement/override it in all three engine table types (Spark, Flink, Java).
2. **Breaking the import boundary** — Java code must not import Scala classes. `style/import-control.xml` will catch this at build time.
3. **Config without documentation** — Every `ConfigProperty` should have `.withDocumentation()` and `.sinceVersion()`.
4. **Hardcoded Spark version assumptions** — Use the adapter pattern. Version-specific Spark code goes in the corresponding `hudi-spark3.X.x` module, not in `hudi-spark-common`.
5. **Not handling both table types** — Many features need different behavior for CopyOnWrite vs MergeOnRead. Check both code paths.
6. **Checkstyle violations** — Run `mvn checkstyle:check -pl <module>` before submitting. Max line length is 200 chars, no tabs, no wildcard imports.
7. **Missing license headers** — All new files need the Apache License 2.0 header.
8. **Generic type parameters** — Core classes use `<T, I, K, O>` generics. T=record type, I=input collection, K=key collection, O=output/write status. Maintain consistency.

## Key Entry Points by Use Case

| If you're working on... | Start here |
|------------------------|------------|
| Write path / upsert / insert | `BaseHoodieWriteClient`, `BaseCommitActionExecutor` |
| Read path / queries | `HoodieBaseRelation.scala` (Spark), `HoodieTableInputFormat` (MR) |
| Compaction | `RunCompactionActionExecutor`, `HoodieCompactor` |
| Clustering | `ClusteringPlanActionExecutor`, `ClusteringExecutionStrategy` |
| Cleaning | `CleanActionExecutor`, `CleanPlanActionExecutor` |
| Timeline | `HoodieTimeline`, `HoodieActiveTimeline`, `HoodieInstant` |
| Metadata table | `HoodieTableMetadata`, `HoodieTableMetadataWriter` |
| Schema evolution | `InternalSchema`, `TableSchemaResolver` |
| Index | `HoodieIndex`, `HoodieIndexFactory` |
| Conflict resolution | `PreferWriterConflictResolutionStrategy`, `TransactionManager` |
| Spark SQL extensions | `HoodieSparkSessionExtension.scala` |
| Flink streaming write | `StreamWriteOperator`, `HoodieFlinkWriteClient` |
| DeltaStreamer/Streamer | `HoodieDeltaStreamer`, `DeltaSync` |
| Hive sync | `HiveSyncTool`, `HoodieHiveSyncClient` |
| Key generation | `AvroKeyGenerator`, `SimpleKeyGenerator`, `CustomKeyGenerator` |
| File I/O | `HoodieWriteHandle` hierarchy, `HoodieStorage` |

## File Layout on Storage (for context)

```
<base_path>/
├── .hoodie/                          # Metadata directory
│   ├── hoodie.properties             # Table config (name, type, version, keys)
│   ├── timeline/                     # Active timeline (instants)
│   │   └── history/                  # Archived timeline (LSM-based)
│   └── metadata/                     # Metadata table (MoR format)
│       ├── files/                    # File listing index
│       ├── column_stats/             # Column statistics index
│       ├── record_index/             # Record-to-file-group mapping
│       ├── bloom_filters/            # Bloom filter index
│       ├── secondary_index_<name>/   # Secondary indexes
│       └── expr_index_<name>/        # Expression indexes
├── <partition_path>/                 # Data partitions
│   ├── <file_id>_<token>_<time>.parquet   # Base files
│   └── .<file_id>_<time>.log.<ver>_<token> # Log files (MoR)
```

## Useful References

- **Tech specs (1.0):** https://hudi.apache.org/learn/tech-specs-1point0
- **RFC directory:** `rfc/` in the repo root
- **Hudi docs:** https://hudi.apache.org/docs/overview
- **Style config:** `style/checkstyle.xml`, `style/import-control.xml`
