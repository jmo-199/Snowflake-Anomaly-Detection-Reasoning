# Anomaly Detection + LLM-Powered Root Cause Analysis Pipeline

A production-grade Snowflake pipeline that detects volumetric anomalies in customer and provider interaction data, then uses Cortex (Claude 3.5 Sonnet) to generate verified root cause analyses with hallucination guardrails.

This README is the authoritative specification for the system. Claude Code should treat this as the design document and follow it exactly.

---

## Project Goal

Detect statistically improbable spikes in interaction volume across four streams (provider chats, provider calls, member chats, member calls) and produce trustworthy LLM-generated root cause analyses that explain what changed and why — with multiple layers of hallucination defense.

The system runs daily, examines yesterday's data, flags anomalies at 99% confidence, and produces structured JSON RCAs validated against deterministic guardrails.

---

## Design Philosophy

The LLM is a **constrained narrator**, never a source of truth.

- All numerical claims (resolution rates, sentiment percentages, sub-category shifts) are pre-computed in SQL before the LLM runs.
- The LLM explains pre-computed numbers; it never estimates them.
- Every LLM output is structured JSON conforming to a strict schema.
- Citations to specific transcript IDs are validated programmatically (Python set difference), not by another LLM.
- An independent critic LLM provides a second judgment.
- Verification requires both gates (programmatic citation check AND critic verdict) to pass.

This design principle is non-negotiable. Any change that gives the LLM more authority over numerical or factual claims violates the architecture.

---

## Architecture Overview

The pipeline has seven phases:

| Phase | Component | Purpose |
|-------|-----------|---------|
| 0 | Verification queries | Pre-flight checks before deployment |
| 1 | Radar views | 365-day baseline + qualified training subset |
| 2 | Prompt config table | Versioned DRAFT and CRITIC prompts |
| 3 | RCA memory table | Storage for verified RCAs with idempotency |
| 4 | ML detector training | Snowflake ML Anomaly Detection model |
| 5 | Daily audit procedure | Production Snowpark Python stored procedure |
| 6 | Scheduled task | Daily 4 AM ET execution (created suspended) |
| 7 | Backfill procedure | Same logic, takes date parameter for testing |

---

## Repository Layout

```
/sql
  phase_0_verification.sql      # Pre-flight schema and data checks
  phase_1_radar.sql             # Baseline + training views
  phase_2_prompts.sql           # AI_PROMPT_CONFIG table + DRAFT/CRITIC inserts
  phase_3_memory.sql            # ANOMALY_RCA_MEMORY table
  phase_4_train.sql             # Train TOPIC_SPIKE_DETECTOR
  phase_6_schedule.sql          # CREATE TASK (suspended)

/procedures
  phase_5_daily_audit.sql       # PRC_DAILY_FORENSIC_AUDIT
  phase_7_backfill_audit.sql    # PRC_BACKFILL_FORENSIC_AUDIT

/tests
  verification_queries.sql      # Run after each phase to validate
  backfill_test_cases.sql       # Known historical dates to test against

/docs
  architecture.md               # Design choices and tradeoffs
  math_foundations.md           # Statistical principles explained
  runbook.md                    # Deployment sequence
```

---

## Data Model

### Source tables (four interaction streams)

All four tables share the same column schema:

| Column | Type | Notes |
|--------|------|-------|
| TRANSCRIPT_ID | STRING | Unique per interaction |
| LOB | STRING | Line of business (e.g., Commercial, Medicaid, Medicare) |
| MAIN_TOPIC_CATEGORY | STRING | Top-level topic |
| INTERACTION_TYPE | STRING | Channel identifier (must distinguish provider vs member) |
| CONVERSATION_DATE | DATE or TIMESTAMP | When the interaction occurred |

Source table names (placeholders to be replaced before deployment):
- `TABLE_PROVIDER_CHATS`
- `TABLE_PROVIDER_CALLS`
- `TABLE_MEMBER_CHATS`
- `TABLE_MEMBER_CALLS`

### Deep table (for RCA evidence)

A unified table containing the full interaction record with summaries:

| Column | Type | Notes |
|--------|------|-------|
| TRANSCRIPT_ID | STRING | Joinable to source streams |
| LOB | STRING | |
| MAIN_TOPIC_CATEGORY | STRING | |
| MAIN_TOPIC_SUB_CAT | STRING | Sub-category, used for topic drift detection |
| INTERACTION_TYPE | STRING | |
| CONVERSATION_DATE | DATE or TIMESTAMP | |
| AGENT_RESOLUTION | STRING | Values: 'Resolved' or 'Not Resolved' |
| MEMBER_SENTIMENT | STRING | Values: 'Positive', 'Neutral', 'Negative' |
| CALL_SUMMARY | STRING | Plain-text summary, not JSON |

Placeholder name: `TABLE_UNIFIED_DEEP`.

---

## Phase Specifications

### Phase 1: The Radar

Build two views:

**`V_HISTORICAL_RADAR`** — daily call counts across all four streams, unioned, grouped by topic + LOB + interaction type + date. Window: trailing 365 days. Filter out NULL dimensions.

**`V_HISTORICAL_RADAR_TRAINING`** — same data, filtered to series with ≥30 days of history (prevents noisy forecasts on sparse series).

Composite series key: `MAIN_TOPIC_CATEGORY || '|' || LOB || '|' || INTERACTION_TYPE` aliased as `SERIES_KEY`. The pipe character is the separator because it does not naturally appear in business data values.

### Phase 2: Prompt Config

Create `AI_PROMPT_CONFIG` table with columns: `VERSION_ID`, `PROMPT_TYPE` ('DRAFT' or 'CRITIC'), `INSTRUCTIONS`, `VERSION_DATE`.

Insert two prompts (version 'MVP_V4'):

**DRAFT prompt** instructs the LLM to:
- Treat pre-computed quantitative deltas as ground truth
- Output strict JSON with fields: `root_cause`, `driving_lobs`, `driving_segments`, `resolution_shift`, `sentiment_shift`, `topic_drift`, `evidence_transcript_ids`
- Cite real transcript IDs from the provided sample only
- Quote actual delta numbers in resolution_shift and sentiment_shift fields
- Never output prose, markdown fences, or freeform text

**CRITIC prompt** instructs the LLM to:
- Audit the draft against the deltas and the transcript sample
- Output strict JSON with fields: `confidence_score` (0-100), `evidence_valid`, `missing_evidence`, `verdict` ('VERIFIED' or 'UNCONFIRMED')
- Mark VERIFIED only if every claim is supported by evidence
- Set confidence below 60 if evidence is weak or numbers contradict the deltas

### Phase 3: Memory Table

Create `ANOMALY_RCA_MEMORY` with columns including `SERIES_KEY`, `EVENT_DATE`, `DRAFT_RCA_JSON` (VARIANT), `CRITIC_REVIEW_JSON` (VARIANT), `FINAL_STATUS`, `CONFIDENCE_SCORE`, and a unique constraint on `(SERIES_KEY, EVENT_DATE)` for idempotency.

### Phase 4: Train the Detector

Use `CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION` with:
- `INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_HISTORICAL_RADAR_TRAINING')`
- `SERIES_COLNAME => 'SERIES_KEY'`
- `TIMESTAMP_COLNAME => 'CONVERSATION_DATE'`
- `TARGET_COLNAME => 'CALL_COUNT'`
- `LABEL_COLNAME => ''`

Model name: `TOPIC_SPIKE_DETECTOR`.

### Phase 5: Daily Forensic Audit

Stored procedure `PRC_DAILY_FORENSIC_AUDIT()`, language Python, runtime 3.10, packages `snowflake-snowpark-python` and `pandas`.

Logic flow:

1. Load DRAFT and CRITIC prompts from `AI_PROMPT_CONFIG`
2. Run `DETECT_ANOMALIES` filtered to yesterday in ET timezone with 99% prediction interval
3. Exit early if no anomalies
4. For each anomaly:
   a. Split `SERIES_KEY` into `topic`, `lob`, `interaction_type`
   b. Skip if already processed (check `ANOMALY_RCA_MEMORY`)
   c. Compute quantitative deltas in SQL: resolution rate, sentiment distribution, sub-category drift between spike day and 6-day baseline
   d. Pull 15-row transcript sample, prioritized by spike-day and unresolved
   e. Build the valid_ids set from the actual sample
   f. Send DRAFT prompt + deltas + sample to Cortex
   g. Safe-parse JSON; on parse failure, log as ERROR and continue
   h. Programmatic citation check: claimed_ids minus valid_ids must be empty
   i. Send DRAFT + deltas + sample + citation check result to CRITIC
   j. Mark VERIFIED only if `critic_verdict == 'VERIFIED' AND citations_valid`
   k. Insert into `ANOMALY_RCA_MEMORY`
5. Return summary string

### Phase 6: Schedule

Create `TSK_DAILY_FORENSIC_AUDIT` with cron `0 4 * * * America/New_York`. **Create suspended.** Only RESUME after Phase 7 backfill testing validates output quality.

### Phase 7: Backfill

Procedure `PRC_BACKFILL_FORENSIC_AUDIT(TARGET_DATE DATE)` — identical to Phase 5 but takes a date parameter instead of computing yesterday. Used for historical testing before enabling the daily task.

---

## Non-Negotiable Implementation Constraints

These constraints are load-bearing for correctness and must not be violated:

### Parameter binding
All data values must be bound as `?` parameters in `session.sql()` calls, never f-string interpolated. Table names defined as Python constants may be f-stringed since they are not user-controlled.

```python
# CORRECT
session.sql("SELECT * FROM t WHERE topic = ?", params=[topic])

# WRONG
session.sql(f"SELECT * FROM t WHERE topic = '{topic}'")
```

### Safe JSON parsing
Every LLM output must be parsed defensively. If the output cannot be parsed as JSON, the run is logged as ERROR — never as success. Implement a `_safe_json_parse` helper that strips optional markdown fences and falls back to None on parse failure.

### Programmatic citation validation
The hallucinated_ids set is computed in Python as `claimed_ids - valid_ids`. This is deterministic set theory, not LLM judgment. Citations are valid only if the difference set is empty AND the claimed set is non-empty.

### Two-gate verification
An RCA is marked VERIFIED only if BOTH conditions hold:
- The CRITIC LLM returns `verdict: 'VERIFIED'`
- The programmatic citation check passes (`hallucinated_ids` is empty and `claimed_ids` is non-empty)

If either fails, the status is UNCONFIRMED.

### Idempotency
Before any expensive work runs for a given anomaly, query `ANOMALY_RCA_MEMORY` for `(SERIES_KEY, EVENT_DATE)`. If a row exists, skip the entire processing for that anomaly. The unique constraint provides a second layer of protection.

### Timezone correctness
"Yesterday" is computed as:
```sql
DATEADD('day', -1, CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP())::DATE)
```
Never rely on server-local time.

### Composite series key
The series key concatenates topic, LOB, and interaction type with `|` as separator. Both directions of the operation (concatenation in SQL, splitting in Python) must use the same separator. Python split must use `split('|', 2)` to limit to 3 pieces.

---

## Configuration Variables

These are placeholder values that must be replaced before deployment. Claude Code should ASK for these during the clarifying-questions step:

| Variable | Where it appears | Example value |
|----------|------------------|---------------|
| Provider chats table name | Phase 1 | `TABLE_PROVIDER_CHATS` |
| Provider calls table name | Phase 1 | `TABLE_PROVIDER_CALLS` |
| Member chats table name | Phase 1 | `TABLE_MEMBER_CHATS` |
| Member calls table name | Phase 1 | `TABLE_MEMBER_CALLS` |
| Deep table name | Phases 5, 7 | `TABLE_UNIFIED_DEEP` |
| Cortex model identifier | Phases 5, 7 | `claude-3-5-sonnet` |
| Warehouse name | Phase 6 | `YOUR_WH` |
| AGENT_RESOLUTION values | Phases 5, 7 | `'Resolved'` / `'Not Resolved'` |
| MEMBER_SENTIMENT values | Phases 5, 7 | `'Positive'` / `'Neutral'` / `'Negative'` |

---

## Mathematical Foundations (for docs/math_foundations.md)

The system rests on five distinct mathematical frameworks, each handling a layer the others can't:

1. **Statistical Process Control (SPC) / time-series forecasting** — the detector learns expected values per series and constructs prediction intervals around forecasts. Anomalies are values outside the 99% interval. Foundation: Shewhart control charts, gradient-boosted forecasting with seasonality features.

2. **Seasonal-trend decomposition** — the forecaster implicitly learns trend, weekly seasonality, and residual variance per series. Each series gets its own learned baseline that adapts to its natural volatility.

3. **Distribution comparison / divergence** — topic drift is measured by comparing the sub-category distribution on the spike day against the 6-day baseline. Current implementation uses a top-5 set difference heuristic; could be formalized with KL or Jensen-Shannon divergence in the future.

4. **Set theory** — the citation check is a deterministic set difference: `H = C \ V` where C is claimed IDs and V is valid IDs. This is the only fully deterministic component and serves as the strongest hallucination defense.

5. **Conjunctive Bayesian-style verification** — the final verdict requires two independent gates (citation check AND critic verdict) to pass. Failure modes are uncorrelated, so the conjunction is stronger than either alone.

The unifying principle: use the LLM only for problems that don't have rigorous mathematical solutions. Statistical detection, rate computation, and citation validation are mathematical problems. Narrative synthesis is the only place LLMs are genuinely needed.

---

## Hallucination Defense (for docs/architecture.md)

When asked "how does this prevent hallucinations," the answer has four parts:

1. **Pre-computation of all numerical claims.** Resolution rates, sentiment percentages, baseline-to-spike deltas are all computed in SQL before the LLM runs. The LLM narrates ground-truth numbers; it never estimates them.

2. **Programmatic citation validation.** The LLM must cite specific transcript IDs as evidence. Claimed IDs are validated against actual data via Python set difference — deterministic, not probabilistic. Hallucinated IDs are caught with certainty.

3. **Structured JSON output with strict schema.** No freeform prose. Each field has a defined purpose. Parse failures are caught and logged as errors, never silently accepted.

4. **Independent critic layer.** A second LLM call audits the draft against the same data, with the citation check result also provided. The system requires both the critic's verdict and the programmatic check to pass before marking VERIFIED.

The system never claims the LLM is reliable. It claims the architecture is reliable because the LLM is constrained.

---

## Deployment Sequence (for docs/runbook.md)

Execute in this order. Do not skip steps. Each gate must clear before proceeding.

1. **Phase 0** — Run all verification queries. Confirm Cortex enabled, source table schemas match, INTERACTION_TYPE values distinguish streams, AGENT_RESOLUTION/MEMBER_SENTIMENT string values match the code.

2. **Phase 1** — Create both views. Inspect top series volumes and qualifying series count. If the qualifying series count is 0 or unreasonably low, debug data coverage before proceeding.

3. **Phase 2** — Create AI_PROMPT_CONFIG and insert prompts. Verify both rows exist.

4. **Phase 3** — Create ANOMALY_RCA_MEMORY. Confirm the unique constraint exists.

5. **Phase 4** — Train TOPIC_SPIKE_DETECTOR. This may take several minutes depending on series count.

6. **Phase 7** — Create the backfill procedure. Run `DETECT_ANOMALIES` over the last 60 days as a precision/recall sanity check. Pick a known-anomalous historical date and run the backfill procedure against it. Read the resulting RCAs critically. Verify cited transcript IDs exist. Repeat for 3–5 different dates.

7. **Phase 5** — Create the daily procedure. Run it manually once with `CALL PRC_DAILY_FORENSIC_AUDIT()`. Inspect the result.

8. **Phase 6** — Only after the above 7 steps look healthy: `ALTER TASK TSK_DAILY_FORENSIC_AUDIT RESUME` to enable scheduled execution.

---

## What I Need From Claude Code

In this order:

1. Read this README and ask any clarifying questions before writing code. Specifically clarify the configuration variables in the table above.

2. After my answers, scaffold the directory structure described in Repository Layout.

3. Build phase by phase. Stop after each phase and show me the file before proceeding to the next.

4. After all phases are written, generate `docs/architecture.md`, `docs/math_foundations.md`, and `docs/runbook.md`.

5. Generate `tests/verification_queries.sql` with the Phase 0 checks plus per-phase post-deployment verifications.

Do NOT proceed past step 1 until I answer the clarifying questions.

Do NOT deviate from the architecture in this README. If something seems suboptimal, raise it as a question rather than silently changing it.

Treat this README as the source of truth. If implementation details elsewhere conflict with this document, this document wins.
