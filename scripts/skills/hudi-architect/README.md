# hudi-architect (Milestone 1 preview)

An OSS conversational design advisor for Apache Hudi tables. Packaged as a Claude Code Skill for early review.

## What it does

Turns workload requirements into a validated Hudi table architecture through a tiered conversational flow:

1. **Tier gate** — figures out whether you're exploring, prototyping, productionizing at modest scale, or going all-in at TB/PB.
2. **Rounds 1-3** — asks workload questions (not Hudi jargon questions) gated by tier.
3. **Output** — produces an Architecture Decision Record (ADR) with tradeoff tables, durability warnings, config bundle, and measurable revisit conditions.

Target Hudi version: **1.2.0**.

## How to invoke

### As a Claude Code Skill

Copy this directory into your Claude Code skills location:

```bash
# User-level (available in every project)
cp -r hudi-architect ~/.claude/skills/

# Or project-level (available only in this project)
cp -r hudi-architect .claude/skills/
```

Then in Claude Code:

```
/hudi-architect
```

Claude will drive the design flow.

### As a reference

Even without invoking as a Skill, the files in `references/` are readable design references:

- `question-flow.md` — round-by-round question list with conditional gating.
- `decision-tables.md` — derivation tables for each design domain (engine, writer, table type, index, partitioning, retention, services, meta-fields, record key).
- `warnings.md` — rule-engine warnings and when they fire.
- `config-templates.md` — `hoodie.*` property templates per decision + sample bundles for three workload archetypes.
- `adr-template.md` — the structure of the ADR output.

The Skill itself is defined in `SKILL.md`.

## What to look for during review

This is **Milestone 1 of a longer arc** — meant to be shareable and playable, not final. Things worth stress-testing:

1. **Walk a real workload through the flow.** Pick a Hudi table you know (existing or planned) and run `/hudi-architect`. Note where the questions don't fit, where the tradeoffs feel wrong, where a decision surprises you.
2. **Check the warnings fire when they should.** Try configurations that should trigger Vice 1/2/3, the high-cardinality-partition trap, or the compaction-target-IO trap. Do the warnings surface at the right moment?
3. **Try the tier gate at all four levels.** The `EXPLORATION` mode should feel like a Hudi tutor, not a design advisor. The `PRODUCTION_AT_SCALE` mode should feel rigorous. If either feels wrong, that's a signal.
4. **Read the ADR output.** Are the revisit conditions actually measurable? Do the durability tables cover the one-way decisions relevant to your workload?

## What's out of scope in Milestone 1

- Multi-writer / concurrency (OCC, NBCC, lock providers) — deferred to a later pipeline-modeling rubric.
- Benchmarking / scale-characterization — different flow shape, future revision.
- CONSISTENT_HASHING bucket recommendations at design time.
- Partial-update MERGE nudging (Spark SQL only).
- Version-awareness across Hudi releases (V1 pins to 1.2.0).
- Session persistence for tier upgrades.

## Longer arc

- **M1 (this)** — Skill-shaped shareable version for colleague/community review.
- **M2** — Sort out pending items (concurrency, benchmarking, session persistence, etc.).
- **M3** — Feedback incorporation from reviewers.
- **M4** — Integration into Hudi's Agentic Lakehouse (`hudi-agent-gateway`) — see [discussion #19264](https://github.com/apache/hudi/discussions/19264).

## Feedback welcome

Play with the Skill, note what breaks or feels off, and share back. Every walkthrough that surfaces a gap makes the design engine sharper before it becomes real code.

Full proposal design document lives in the parent directory as `hudi_architect_agent_proposal_chatgpt.md`.
