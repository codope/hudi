# Question flow — Round 1, 2, 3

Read this file when you're about to ask questions. Each round lists questions in order with conditional gating.

## Round 1 — Source, engine, mutability, distribution, experience (all tiers)

Round 1 is the minimum to specify a PROTOTYPING design. For EXPLORATION, replace hard questions with explanations where possible.

### Q1.1 — Engine

> "Which processing engine are you planning to use — Spark, Flink, or undecided?"

- Spark or Flink → proceed to Q1.2.
- Undecided → present Spark vs Flink tradeoff (see decision-tables.md → engine). Recommend based on workload signals.

### Q1.2 — Source

> "Where does the data come from?"

Typical answers: Kafka, DFS (files), JDBC (database), another Hudi table, S3/GCS events, Kinesis, Pulsar, custom.

Store this — Kafka triggers HoodieStreamer as writer default (see decision-tables.md → writer).

### Q1.3 — Continuous vs periodic

> "Is data ingested continuously (24/7 stream), or in periodic batches (scheduled jobs)?"

### Q1.4 — Mutability

> "Is the workload mutable or immutable? (Mutable = records get updated after insert. Immutable = records only ever append, never change.)"

### Q1.5 — Update distribution (MUTABLE ONLY — skip for immutable)

> "When updates arrive, which parts of the table do they touch?
>
> - **Uniformly across the whole table** — any record can be updated at any time (e.g., customer, vendor, product tables).
> - **Concentrated on recent data** — most updates hit records from the last few days, with a tail of stragglers (e.g., trips, orders, sessions).
> - **Unsure.**"

If "recent-concentrated," ask a follow-up in Round 2 about tail length (see Q2.3).

### Q1.6 — Experience with Hudi

> "Are you experienced managing and operating Hudi tables?
>
> - **First-time / want fire-and-forget** — one job to run, simple ops.
> - **Some experience** — want MOR benefits without ops complexity (inline compaction is fine).
> - **Experienced operator** — comfortable with standalone async compaction executors, MVCC concurrent services, tuning."
>
> Briefly explain the COW vs MOR tradeoff during the question:
> - COW: rewrites base files on updates. Simple ops, no compaction to run, but write cost grows with update frequency.
> - MOR: appends updates to logs, compacts periodically. Lower write cost, but compaction becomes a critical service to keep read latency in check.

Derives table type + compaction posture (see decision-tables.md → table-type).

## Round 1 outputs

- Engine (user-answered).
- Table type PROVISIONAL (derived from experience + mutability + distribution).
- Compaction posture PROVISIONAL.
- Writer will be derived in Round 2.

**For EXPLORATION and PROTOTYPING tiers: Round 1 is the entire conversation.** Skip to output (ADR + config bundle).

## Round 2 — Reads, layout, identity, writer (PRODUCTIONIZING_INITIAL, PRODUCTION_AT_SCALE)

### Q2.1 — Consumer read pattern (three-axis question)

> "How will downstream consumers use this table? A few things to help me understand:
>
> - **General shape** — do they run bulk analytical queries (scan large portions of the table), targeted lookups (look up specific records), or streaming/incremental consumers that only need what changed?
> - **Latency sensitivity** — do reads need to be fast, or is some read latency acceptable if it buys cheaper writes?
> - **Time travel or incremental query needs** — do any consumers need to query the table as-of a past commit, or read only changes since their last checkpoint?"

Rule engine maps answers to Hudi query types internally. See decision-tables.md → read-behavior.

### Q2.2 — Table size

> "What's the current table size, and what do you expect over the next 2-3 years?"

Used for partitioning threshold, unpartitioned viability, index selection, retention limits.

### Q2.3 — Retention lookback

> "What's the maximum lookback window you'd need for time-travel or incremental queries? For example, do downstream consumers need to query the table as-of a past commit, or read only changes since their last checkpoint (last day, last week, last month)?"

- "No time-travel or incremental needs" → default to safe max for commit cadence.
- Otherwise → clamp user-desired to safe max (surface the clamp explicitly). See decision-tables.md → retention.

**If Round 1 update distribution was "recent-concentrated", also ask:**

> "You said updates concentrate on recent data. Roughly how far back do late-arriving updates trickle in — hours, days, weeks, or longer?"

Cross-check tail vs retention (see warnings.md → UPDATE_TAIL_VS_RETENTION).

### Q2.4 — Partitioning

Lead with explanation, then delegation-friendly ask:

> "Partitioning splits the table into subdirectories keyed by a column value. It gives Hudi two things:
> - **Fast reads for queries that filter on the partition column.** Instead of scanning the whole table, Hudi reads only the matching partitions. On multi-TB tables this can be orders of magnitude faster.
> - **Manageable file layout at scale.** As tables grow into TBs, unpartitioned tables produce very large or very many files that stress writers, readers, and downstream jobs like silver bootstrap.
>
> Do you have a partition column in mind (a date-like column, a business dimension), or would you like to skip partitioning for now?"

Follow-up phrasing depends on answer:

**User names a column:**
> "Is it a date-like column, or a business dimension? Roughly how many distinct values will it have across the retention window?"

Rule engine computes projected partition count and runs Vice 1/2/3 checks. See warnings.md.

**User says "you pick":**
Architect defaults to date-based (ingestion time for raw/immutable; event time only if user has strong event-time semantic downstream). Daily granularity. Report the choice with rationale.

**User says "no partitioning / unpartitioned":**
> "That's fine as long as the table stays manageable. Two concerns to flag now, based on what you've told me:
> - If your projected table size at 2-3 years crosses ~500GB and this table feeds downstream silver pipelines that will ever need to bootstrap from it, unpartitioned makes that bootstrap job much harder (small files, whole-table snapshot reads in one commit).
> - If this is a raw layer that only feeds append-only silver consumers who are always caught up, unpartitioned is more forgiving.
>
> Given your workload, my read is: [surface recommendation]. Proceed unpartitioned, or reconsider?"

### Q2.5 — Partition-column stability (PARTITIONED ONLY)

> "Can the partition column's value change for a given record across updates, or is it stable once inserted? Does the source data always contain the correct partition value for each record at update time?"

- Stable + source has correct value → partition-scoped index viable.
- Value can change OR source doesn't always have partition value → global-scope index required.

### Q2.6 — Record key (concept-anchored)

> "Hudi is rooted in database design, so it treats a **primary key** — Hudi calls it a record key — as first-class. A record key is a column (or combination of columns) that uniquely identifies a record.
>
> The record key powers a lot of what Hudi does: upserts, index lookups, compaction, concurrency control, dedup, change tracking.
>
> When a new record arrives with a key that already exists, Hudi updates the existing record. Different key → new record.
>
> Which column (or columns) uniquely identify a record in your workload?"

**For IMMUTABLE workloads, add auto-gen alternative:**

> "For append-only workloads with no downstream identity requirements, Hudi can also auto-generate keys efficiently (roughly 3-10x lighter than UUIDs).
>
> - **Auto-generate keys for me** — Hudi creates keys internally, no key config needed.
> - **I have a natural key column** — event_id, session_id, transaction_id, etc.
>
> Which fits?"

Auto-gen incompatible with disabling meta fields (see Q2.7).

Answer routing:
- Single field → SimpleKeyGenerator.
- Multi-field → ComplexKeyGenerator.
- Auto-gen (immutable only) → no key generator, no `recordkey.field` config.

### Q2.7 — Meta-fields prompt (IMMUTABLE + record size below 1KB ONLY)

Skip entirely for mutable workloads (silently keep all meta fields).

Skip for immutable + record size >1KB (rounding error).

For immutable + ≤1KB:

> "Hudi adds 5 meta fields to every record. They enable incremental queries, uniqueness checks, and other features. Meta-fields add roughly 50-100 bytes per record — on records above ~1KB they're rounding error, but on smaller records they can add meaningful storage overhead.
>
> Do you know roughly how big each of your records is?
> - **Above ~1KB / not sure** → keep all meta fields (default, safest).
> - **Around 200B–1KB** → your storage saving from disabling is small but real. Options: keep all (safest), or selective mode (Hudi 1.x, `_hoodie_commit_time` only).
> - **Below ~200B** → meta-fields overhead is significant (25%+ of record size). Options:
>   - Keep all meta fields — safest, all features work.
>   - Selective mode (`_hoodie_commit_time` only) — saves ~40%, incremental relation still works efficiently.
>   - Disable meta fields entirely — saves ~50%, incremental falls back to slower snapshot-read + filter pattern (still functional, just less efficient), CDC unavailable.
>
> Storage saving at scale: at 10B records × 200B, disabling entirely saves ~50-100GB total. If incremental queries and CDC aren't needed, that's real. If either might be needed later, keep them.
>
> Which trade fits?"

**Important:** don't tell users they're "giving up a lot" — position "disable entirely" as still functional (falls back to what Delta/Iceberg do natively), just slower without Hudi's native fast path. Selective is the balanced middle.

### Q2.8 — Small-files posture (IMMUTABLE ONLY — after partitioning resolved)

Skip for mutable (inline small-file handling via `insert`/`upsert` is Hudi default; no user question).

> "How should Hudi handle small files?
>
> - **Don't worry about small files** — fastest ingest. Uses `bulk_insert`, no clustering. Files stay whatever size the batch produces.
> - **Handle small files without slowing ingestion** — `bulk_insert` + async clustering. Writer stays fast; a separate service compacts small files in the background.
> - **Keep files well-sized even if ingestion takes longer** — `insert` inline. Slight per-batch latency cost, no separate clustering service to run."

Recommendation prose adapts based on:
- Partition cardinality (low-card date vs high-card business dim).
- Future-consumers axis (closed universe = fixed consumers; open universe = future silver pipelines may bootstrap-read).

See decision-tables.md → small-files-posture.

### Q2.9 — Pipeline shape

> "How is the pipeline expressed?
>
> - **Config-driven ingestion** — property-file-driven source → transform → Hudi wiring. Most common shape.
> - **Custom application code** — Scala/Java/Python that reads sources, transforms with DataFrame ops, writes to Hudi. Includes streaming-source consumers using forEachBatch.
> - **SQL-centric** — you write INSERT/MERGE/UPDATE/DELETE statements.
> - **True streaming-sink writes** — you use `writeStream.format('hudi')` directly, and you need stateful streaming primitives (windows, watermarks, joins across streams)."

**Structured Streaming disambiguation follow-up (if user picks "streaming"):**

> "Do you use `writeStream.format('hudi')` as the actual sink, or do you consume from a stream and call `.write.format('hudi')` inside a forEachBatch callback?"

- forEachBatch → route to DataSource path.
- writeStream sink → true Structured Streaming. Ask about stateful primitives:

> "Do you need stateful stream operations (windows, watermarks, joins with another stream)?"

- Yes → Structured Streaming.
- No → nudge toward HoodieStreamer.

**Kafka source override note:** If source is Kafka, default is HoodieStreamer regardless of user's pipeline_shape answer — surface HoodieStreamer's Kafka-specific advantages (schema registry, exactly-once, error routing, in-process async services). Only route to DataSource for Kafka when the pipeline has multi-source complexity, ML DataFrame-native library work, or one-off backfills.

**First-time user nudge:** if experience is EXPLORATION/PROTOTYPING and pipeline_shape is SQL-centric or streaming-with-primitives:

> "For a first Hudi table, HoodieStreamer or Spark DataSource are the two most-deployed paths. Spark SQL / Structured Streaming work but have smaller production footprints. Do you want to reconsider, or proceed with the specialized writer?"

## §8.3 — Derived-fact synthesis checkpoint

**Fires between Round 2 and Round 3.** Echo computed facts back to user:

```
Confirmed workload: <categorical facts from Rounds 1-2>

Derived:
- Steady-state table size: <computed>
- Projected partition count: <computed>
- Files/day at target file size: <computed>
- Small-file risk: <assessment>

Tensions surfaced (if any):
- <update-tail vs retention window>
- <partition-column stability vs cross-partition update>
- <three-concurrent-services warning>
- <table-type refinement if writer selection unlocks free async>

Please confirm before I generate the full config.
```

User confirms before Round 3 fires.

**Refinement moment:** if writer selection landed on HoodieStreamer continuous mode AND table type is MOR, the async-compaction becomes free regardless of experience level. State this to the user as a "here's a bonus" upgrade at the checkpoint.

## Round 3 — Scale, concurrency, index (PRODUCTION_AT_SCALE ONLY)

### Q3.1 — Writers

> "Single or multiple independent writers?"

V1 assumes single writer. Multi-writer deferred to future rubric — if user says multiple, note it as an ADR flag and proceed assuming single for now.

### Q3.2 — Index

Derived from decision-tables.md → index. Architect presents recommendation with rationale from the decision table:

> "For your workload — <mutable/immutable, partitioned/unpartitioned, partition-stable, projected size, key characteristics> — I recommend `<index type>` because <one reason>.
>
> [Tradeoff table showing options]
>
> Confirm, or override?"

### Q3.3 — Derived services confirmation

Architect states what services will run:
- Cleaner + archival: inline autopilot, config emitted per retention answer.
- Compaction: derived from writer + table type (see decision-tables.md → compaction).
- Clustering: off by default; only surface as recommendation if immutable + small-files posture (b).

Ask user to confirm.

## Retired from V1 dialogue

- Ops-appetite question (three-way hands-off/standard/tuned) — retired. Experience question replaced it.
- Visibility-interval as first-class Round 1 question — retired. Moves to ADR-level fact (sizing note).
- Compaction cadence question at design time — retired. Emitted silently at default.
- Cleaner cadence question — retired. Autopilot.
- Col-stats decision at design time — retired. Operations Agent territory.
- Clustering push — clustering is off by default; only surfaces on strong workload signals.
- Point-lookup column follow-up (record key vs other column) — retired. Secondary index becomes ADR flag.

## Question ordering rationale

Reads first (Q2.1) → physical layout (Q2.2-2.5) → identity (Q2.6-2.7) → writer (Q2.9). Writer selection benefits from knowing the workload profile, so it comes last.

Q2.7 meta-fields prompt fires immediately after Q2.6 record key when applicable — they're coupled by the auto-gen / virtual-keys mutual exclusion.

Q2.8 small-files posture fires only for immutable, only after partitioning is resolved (recommendation depends on partition cardinality).
