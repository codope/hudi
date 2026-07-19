# Rule engine warnings

Named warnings that fire on specific workload signals. Consult when driving the flow — surface warnings at the right point in the conversation, not all at once at the end.

Format for each: name, trigger condition, message template, when it fires.

## Partitioning warnings

### VICE_1_PARTITION_MISALIGNMENT

**Triggered when:** user chooses a partition column that doesn't match consumer read filters.

**Message:**
> "You partitioned by `<column>`, but you said consumers filter primarily on `<other_column>`. Partition pruning won't help these queries. Options: (i) repartition by the filtered column, (ii) enable column stats + data skipping (deferred to Operations Agent), (iii) keep the current partitioning if there are other filters you didn't mention."

**When fires:** immediately after user names a partition column in Q2.4, once read pattern (Q2.1) is known.

### VICE_2_OVERLY_GRANULAR_PARTITIONING

**Triggered when:** `partition_size = table_size / partition_count < 100MB` at projected steady state.

**Message:**
> "At ~`<partition_size>`MB per partition, average partition well below healthy 100MB+ range. Options: coarsen granularity, drop composite dimension, or drop partitioning entirely if total table stays under ~500GB."

**When fires:** after Q2.4 + Q2.2 (table size known + partition count projected).

### VICE_3_PARTITION_EVOLUTION

**Triggered when:** user mentions "start hourly, coarsen later" or similar evolution plan.

**Message:**
> "Don't. Queries spanning evolution boundary hit mixed file sizes and see straggler tasks. Pick the coarsest scheme that meets your freshness needs upfront, even if current volume seems to justify finer grain."

**When fires:** during Q2.4 partitioning discussion.

### HIGH_CARDINALITY_PARTITION_TRAP

**Triggered when:** partition col looks like business ID (column name contains `_id`, `_uuid`, or matches known-ID patterns).

**Message:**
> "You're proposing `<column>` as the partition column. This looks like a business ID with potentially high cardinality. Concerns:
> - At 100K+ partitions, Hudi's in-memory file system view grows large enough to cause elevated read and write latencies due to memory pressure.
> - Even if queries filter on `<column>` (pruning to one partition), writer-side and reader-side metadata handling is expensive.
> - Composite `<business_id>/<date>` patterns compound this rapidly.
>
> Alternatives:
> - Partition by date and rely on data-skipping / secondary index for `<column>`-based reads.
> - Bucket index on `<column>` if lookups on this column are dominant — provides direct file-group routing without partition-count blowup.
> - Keep the partition choice if cardinality stays bounded and you have strong reason — validate against the projected-count guardrails."

**When fires:** immediately after user names a partition column in Q2.4.

### PROJECTED_PARTITION_COUNT_YELLOW

**Triggered when:** projected partition count in 10K-50K band.

**Message:**
> "Warning: at ~`<count>` partitions, MDT `files` partition grows large, listing operations get expensive. Consider coarsening granularity or dropping composite dimension."

**When fires:** at §8.3 derived-fact synthesis checkpoint.

### PROJECTED_PARTITION_COUNT_RED

**Triggered when:** projected partition count > 50K.

**Message:**
> "Reject: ~`<count>` partitions at projected steady state. This is a known failure mode — 270K partitions with 3MB per partition is documented as disaster territory. Coarsen granularity or drop composite dimension before proceeding."

**When fires:** during Q2.4 partitioning discussion. Blocks proceeding until user changes partitioning.

## Design tension warnings

### WORKLOAD_EXPERIENCE_TENSION

**Triggered when:** mutable + uniform-distribution updates + large projected table AND user picks fire-and-forget experience.

**Message:**
> "Your workload signals point toward MOR (mutable + uniform updates at scale = high write amp for COW), but you picked fire-and-forget which typically means COW. Three reconciliations:
> - (a) Accept COW; ADR flags concrete revisit conditions if write amp materializes.
> - (b) Step up to MOR with inline compaction. Slightly more per-batch latency, no separate service to deploy.
> - (c) Keep the workload smaller and rely on the Operations Agent to flag if COW hits a wall.
>
> Which matches your priorities?"

**When fires:** after Q1.6 (experience) when Q1.4 + Q1.5 + Q2.2 signals are available. Ask user to reconcile.

### UPDATE_TAIL_VS_RETENTION

**Triggered when:** update-tail estimate (from Q2.3 follow-up) > retention window (Q2.3 main answer).

**Message:**
> "Your update pattern has a tail extending ~`<tail>`, but your retention window is `<retention>`. Late-arriving updates land correctly on current records — Hudi handles that fine. But downstream consumers with incremental checkpoints older than `<retention>` cannot reconcile against intermediate historical states.
>
> Options:
> - (a) Widen retention (if commit cadence allows).
> - (b) Keep retention and expect consumers to check in more frequently.
> - (c) Reduce commit cadence to allow wider safe retention."

**When fires:** at §8.3 derived-fact synthesis checkpoint.

## Config trap warnings

### RETENTION_CLAMP

**Triggered when:** `user_desired_lookback > safe_max_retention` (computed from commit cadence).

**Message:**
> "You asked for `<desired>` of lookback, but at `<commit_cadence>`-minute commit cadence that would push the active timeline past its healthy range and degrade latency. Clamping to `<safe_max>`. To widen retention, either reduce commit cadence (e.g., 15-min instead of 5-min → 7 days safe) or accept the shorter window."

**When fires:** during Q2.3 retention question, immediately when user's desired value exceeds safe max.

### SUB_5_MIN_CADENCE_UNSTABLE

**Triggered when:** commit cadence < 5 min AND computed safe retention < 1 day.

**Message:**
> "At `<cadence>`-minute cadence, safe retention drops below 1 day (~`<computed>`). As a best practice, first stabilize a 5-minute cadence pipeline before attempting sub-5-minute commits. If sub-minute is a hard requirement, expect a very tight retention window and plan operational monitoring accordingly."

**When fires:** during Q2.3 retention question. Not a hard block.

### COMPACTION_TARGET_IO_TRAP

**Triggered when:** MOR + projected table size ≥ 1TB.

**Message:**
> "For MOR at TB-scale, `hoodie.compaction.target.io` defaults to 500GB per compaction round. At this scale, file groups accumulate uncompacted → log files grow forever → read latency degrades. Community has seen production workloads hit this. Bump to 2-5TB in the config bundle."

**When fires:** after table type + projected size are known. Add to ADR as explicit tuning knob.

### THREE_CONCURRENT_SERVICES

**Triggered when:** writer is HoodieStreamer continuous AND table type is MOR AND clustering enabled.

**Message:**
> "Three concurrent services in one Spark job: ingestion + async compaction + async clustering. Default 1:1:1 resource split works for balanced workloads. If ingestion falls behind, shift weight toward `--delta-sync-scheduling-weight`. If compaction backlog grows, shift toward `--compact-scheduling-weight`. Operations Agent territory."

**When fires:** at §8.3 derived-fact synthesis checkpoint.

### WRITER_COMPACTION_MISMATCH

**Triggered when:** user wants MOR async compaction AND writer that doesn't support async in-process (DataSource or SQL).

**Message:**
> "Your writer choice (`<writer>`) means async compaction requires deploying a separate `HoodieCompactor` job — an advanced deployment pattern. Alternative: switch writer to HoodieStreamer continuous mode, and get async compaction for free in-process. Which fits?"

**When fires:** after Q2.9 (pipeline shape → writer) is known, only if MOR + experienced signals were captured earlier.

## Growing set

More warnings will emerge during Path A implementation. When you encounter a workload where the current warnings don't fire but something feels wrong, flag it explicitly — that's the signal for a new warning to add.
