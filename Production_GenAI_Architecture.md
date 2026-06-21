# Production-Grade GenAI Executive Summary Platform
### Snowflake Cortex Analyst + OpenAI + LangChain on top of QlikSense/QlikView reporting

---

## 1. Problem Statement

| Scenario | Trigger | Challenge |
|---|---|---|
| **A. Scheduled summary** | QlikSense/QlikView report data refreshes daily in Snowflake | The underlying result sets behind a report can be millions of rows; you cannot stuff that into an LLM context window |
| **B. Ad-hoc Q&A** | User asks a free-text question ("How did APAC revenue trend last quarter vs plan?") | You must (1) turn NL → SQL against the right tables, (2) execute it safely, (3) summarize a result that may itself be huge |

Both scenarios share the same backend: Snowflake data, a semantic layer that knows the business vocabulary, and an LLM narrative layer. The two scenarios differ only in **what triggers SQL generation** (a fixed report definition vs. Cortex Analyst's NL→SQL). Design one platform that serves both.

The two genuinely hard engineering problems are:
1. **Summarizing data that doesn't fit in a context window**, addressed in §4.
2. **Knowing what SQL/semantics sit behind existing Qlik objects so Cortex Analyst can reproduce them**, addressed in §7–§8.

---

## 2. Reference Architecture

```
                         ┌───────────────────────────────────────────┐
                         │              SNOWFLAKE (system of record)  │
                         │                                            │
   Daily ETL ──────────▶ │  Base tables ─▶ Dynamic Tables / MVs        │
   (Qlik script /        │  (pre-aggregated facts, KPIs, deltas)       │
    dbt / Snowpipe)      │        │                                    │
                         │        ▼                                    │
                         │  Cortex Analyst Semantic Model / Semantic   │
                         │  View  (facts, dims, metrics, synonyms,     │
                         │  verified queries)                          │
                         │        │                                    │
                         │  Cortex Functions (SUMMARIZE / COMPLETE /   │
                         │  AGGREGATE) for in-warehouse pre-digestion  │
                         └────────┼──────────────────────────────────┘
                                  │ REST (Cortex Analyst /
                                  │ Cortex Search) + JDBC/Snowpark
                                  ▼
                  ┌────────────────────────────────────┐
                  │     Orchestration Service (FastAPI) │
                  │     LangChain / LangGraph agents     │
                  │  ┌────────────┐  ┌─────────────────┐│
                  │  │ NL→SQL tool│  │ Map-Reduce       ││
                  │  │ (Cortex    │  │ Summarization    ││
                  │  │ Analyst)   │  │ Chain            ││
                  │  └────────────┘  └─────────────────┘│
                  │  Cache (Redis / Snowflake table)     │
                  │  Guardrails, eval, audit log          │
                  └───────────────┬──────────────────────┘
                                  │
                     ┌────────────┴─────────────┐
                     ▼                           ▼
              OpenAI (GPT-4o/4.1)          Snowflake-hosted Cortex
              narrative polishing          LLM (Llama/Mistral) for
              (only on already-            anything touching raw/
              aggregated, reviewed          sensitive rows
              text — see §9.1)
                     │
                     ▼
        Exec summary store (Snowflake table) ──▶ Qlik (NL object / extension),
                                                  Slack, email, chat UI
```

**Component responsibilities**

| Layer | Tool | Job |
|---|---|---|
| Storage / compute | Snowflake | Single source of truth, all heavy aggregation, governance (RBAC, masking, row-access policies) |
| Semantic layer | Cortex Analyst semantic model / native Semantic View | Maps business vocabulary → SQL; the only thing allowed to generate SQL from NL |
| Pre-digestion | Dynamic Tables, stored procs, Cortex `SUMMARIZE`/`COMPLETE` | Convert "huge" into "small" *before* anything leaves Snowflake |
| Orchestration | LangChain/LangGraph + FastAPI | Routes scenario A vs B, runs map-reduce summarization, calls Cortex Analyst REST API, calls OpenAI, applies caching/guardrails |
| Narrative generation | OpenAI GPT-4o (or Cortex `COMPLETE`) | Turns structured facts into exec prose |
| Delivery | Qlik extension / Streamlit-in-Snowflake / Slack / email | Surfaces the summary back where users already look |

---

## 3. The Two Scenarios in Detail

### 3a. Scenario A — Scheduled (daily refresh) summary

1. ETL/Qlik reload completes → Snowflake `STREAM` on the target fact table fires, or a `TASK` is scheduled to run 15 min after the known ETL window.
2. A `TASK` (or external orchestrator: Airflow/Snowflake Tasks/Step Functions) runs a stored procedure that refreshes **Dynamic Tables** holding: current-period KPIs, period-over-period deltas, top/bottom-N movers, anomaly flags (z-score or `% change > threshold`).
3. This pre-aggregated, small (\<10K rows, usually \<500) result is handed to the **map-reduce summarization chain** (§4.3).
4. Output written to an `EXEC_SUMMARY` table keyed by `report_id, business_date`, plus pushed to wherever users read it.
5. Idempotent + cached: if the data fingerprint (hash of the aggregate inputs) hasn't changed since the last run, skip regeneration.

### 3b. Scenario B — Ad-hoc NL question

1. User asks a question in a chat UI.
2. LangChain agent calls **Cortex Analyst** (`POST /api/v2/cortex/analyst/message`) with the question + the relevant semantic model.
3. Cortex Analyst returns generated SQL (and an optional interpretation/confidence). The agent **always shows the SQL** to the user/log for auditability before/while executing it.
4. SQL executes against Snowflake under the **calling user's own role** (so existing row-access/masking policies apply — never run as a service account with elevated privileges).
5. If the result set is small (a single KPI, a short table) → send straight to OpenAI for a one-paragraph answer.
6. If the result set is large → route through the same **map-reduce summarization chain** as scenario A.
7. Response + the SQL used + a link to "open in Qlik"/"open in Snowsight" is returned. Log the (question, SQL, row count, summary) tuple for evaluation and for Cortex Analyst's **verified-query** feedback loop.

Both scenarios converge on the same downstream chain — build it once.

---

## 4. Summarizing Datasets That Don't Fit in Context

This is the central scaling problem. The rule: **never pipe raw rows into an LLM.** Layer the defenses below; most requests should be fully handled by layer 1.

### 4.1 Layer 1 — Push aggregation into Snowflake (handles ~90% of cases)
- Cortex Analyst's generated SQL should itself contain `GROUP BY`, `QUALIFY`/`TOP N`, window functions for deltas, not `SELECT *`. Encode this in the semantic model's **metrics** (pre-defined aggregations) so the LLM is steered toward producing aggregate queries rather than row dumps.
- For scenario A, never query the raw fact table directly — query a **Dynamic Table** that already holds daily/weekly grain aggregates, refreshed incrementally (cheap) instead of recomputed (expensive) on every run.
- Compute the "interesting" facts in SQL, not in the LLM: period-over-period % change, contribution analysis, rank changes, outlier detection (`STDDEV`/z-score). The LLM should receive **facts**, not raw numbers to derive facts from — LLMs are unreliable at arithmetic over large tables and reliable at turning a row of "Revenue +12% QoQ, driven by APAC (+30%), partially offset by EMEA (-8%)" into prose.

### 4.2 Layer 2 — In-warehouse LLM pre-digestion
- For unavoidably large *text* fields (support tickets, comments, free-text survey data) feeding into a summary, use **Cortex `SUMMARIZE`** or `COMPLETE` inside a SQL/Snowpark UDF to chunk-summarize at the warehouse layer. Data never leaves Snowflake for this step, and it's parallelized by the Snowflake compute engine, not by your application.
- Example pattern:
  ```sql
  CREATE OR REPLACE TABLE chunk_summaries AS
  SELECT region, SNOWFLAKE.CORTEX.SUMMARIZE(LISTAGG(comment_text, ' ')) AS region_summary
  FROM support_tickets
  WHERE ticket_date = CURRENT_DATE - 1
  GROUP BY region;
  ```
  This is itself a map step — Snowflake does the "map" over partitions, your chain does the "reduce."

### 4.3 Layer 3 — Map-reduce / hierarchical summarization (LangChain)
When layer 1 still leaves you with, say, 200 rows of regional/product-level detail that's too much for one tight paragraph but needed for fidelity:

- **Map**: summarize each logical group (region, product line, business unit) independently — these calls can run **in parallel** (`asyncio.gather` or LangChain's `RunnableParallel`).
- **Reduce**: combine the group-level summaries into one executive narrative, with the LLM instructed to highlight outliers and ignore line items immaterial to the headline story.
- Use LangChain's `load_summarize_chain(chain_type="map_reduce")` as a starting point, but for production replace it with an explicit **LCEL/LangGraph** graph so you control: chunking strategy, parallelism limits, retries per chunk, and a hard token budget per call.
- **Refine vs. map-reduce**: map-reduce is preferred here over `refine` (sequential refinement) because it parallelizes and is robust to one chunk failing; refine is more accurate for narrative *flow* but serializes and is slower/costlier at scale.

```python
# Simplified LCEL map-reduce sketch
map_prompt = ChatPromptTemplate.from_template(
    "Summarize this group's performance in 2 sentences, only mentioning "
    "statistically notable changes:\n{group_facts}"
)
reduce_prompt = ChatPromptTemplate.from_template(
    "Combine these group summaries into one executive paragraph. "
    "Lead with the single most important finding:\n{group_summaries}"
)

map_chain = map_prompt | llm | StrOutputParser()
group_summaries = await map_chain.abatch(
    [{"group_facts": g} for g in grouped_facts], config={"max_concurrency": 8}
)
final = (reduce_prompt | llm | StrOutputParser()).invoke(
    {"group_summaries": "\n".join(group_summaries)}
)
```

### 4.4 Layer 4 — Caching & fingerprinting
- Hash the aggregate inputs (not the raw table) that feed a summary. If the hash matches yesterday's, skip regeneration and reuse the cached narrative (with a "data unchanged" note). This matters because daily refreshes often don't materially move every report.
- Cache at two levels: (1) Cortex Analyst NL→SQL cache (same question → same SQL, skip regeneration), (2) summary cache (same aggregate fingerprint → same narrative).

### 4.5 Layer 5 — Token budgeting & guardrails
- Hard cap rows/tokens passed to any single LLM call; if a generated query would return more than N rows, the agent should **rewrite the request as an aggregate query** rather than truncate raw rows (truncation silently produces wrong/misleading summaries).
- Add a "data sufficiency" check: if Cortex Analyst's SQL touches a fact table without any `GROUP BY` and row count > threshold, reject and force re-planning before it reaches the LLM.

---

## 5. Why a hybrid OpenAI + Cortex LLM design (not just one)

| Use | Recommended engine | Why |
|---|---|---|
| NL → SQL | **Cortex Analyst** (not OpenAI directly) | It's grounded in your semantic model, returns auditable SQL, and keeps schema/data fully inside Snowflake's trust boundary |
| In-warehouse chunk summarization of raw/sensitive text | **Cortex `SUMMARIZE`/`COMPLETE`** | Data never leaves Snowflake — important if tickets/comments contain PII |
| Final executive narrative polishing (on already-aggregated, reviewed, non-sensitive facts) | **OpenAI GPT-4o/4.1** | Best-in-class prose quality, instruction following, tone consistency for an "executive summary" audience |

This hybrid pattern minimizes data leaving Snowflake — only small, aggregated, already-reviewed structured facts ever reach OpenAI. If your compliance posture requires zero external egress, swap the final step to Cortex `COMPLETE` (Llama/Mistral hosted in Snowflake) at some cost to prose quality.

---

## 6. Tech Stack Summary

- **Snowflake**: tables, Dynamic Tables, Cortex Analyst, Cortex Search (optional RAG over business glossaries/prior commentary), Cortex Functions, Snowpark (Python UDFs/stored procs), Streams/Tasks.
- **LangChain / LangGraph**: agent orchestration, map-reduce chains, tool-calling wrapper around the Cortex Analyst REST API, memory for multi-turn Q&A.
- **OpenAI**: final narrative generation (Responses/Chat Completions API); consider Azure OpenAI if you need a private network boundary and enterprise data-use guarantees.
- **FastAPI**: stateless API layer; horizontally scalable, fronts both scenario A's batch trigger and scenario B's chat endpoint.
- **Redis or a Snowflake `RESULT_CACHE` table**: caching layer for §4.4.
- **LangSmith (or comparable tracing)**: every chain run traced — generated SQL, row counts, prompts, tokens, latency — essential for debugging "why did the summary say that."
- **Git**: semantic model YAML and verified-query sets are code — version, review, and CI-test them like any other artifact.

---

## 7. Extracting the Queries Behind Existing Qlik Reports

You don't need to reverse-engineer reports by reading dashboards. Qlik stores the **load script** (which contains the actual SQL run against Snowflake to build the data model) as a first-class, retrievable artifact — extract it programmatically rather than re-deriving it by hand.

### 7.1 QlikSense (Enterprise or SaaS)

1. **Engine API (live, real-time)**: open a WebSocket session to the Qlik Engine, open the app, and call `Doc.GetScript()` — returns the full load script text as a string. This is what the IDE's script editor itself uses.
2. **REST API (simpler, scriptable)**: `GET /api/v1/apps/{appId}/data/script` returns the script for a given app. This is the easiest path to automate.
3. **Qlik CLI**: `qlik app script get --app-id <appId> > script.qvs` wraps the REST call — good for a scheduled extraction job.
4. **Data model cross-reference**: call `GetTablesAndKeys` (Engine API) or use the Data Model Viewer export to get the resulting table/field/association graph. Cross-referencing this against the script confirms join keys and grain, which you'll need for the semantic view in §8.
5. **Chart-level measures/dimensions**: walk each sheet's objects via `GetAllInfos` + `GetLayout` on chart objects to pull the actual expressions used (e.g., `Sum(SalesAmount)`, `Count(DISTINCT OrderID)`). These map almost 1:1 onto Cortex Analyst **measures** — this is the fastest way to seed your metric definitions correctly rather than guessing from raw SQL.

### 7.2 QlikView (legacy)

1. **COM Automation API** (Windows-only, classic approach): `Document.GetScript()` via the QlikView Server/Desktop COM object — same idea as Qlik Sense's Engine API call.
2. **QMS API** (QlikView Management Service) can enumerate documents and trigger script export jobs at scale across a QVW estate.
3. Several open-source `.qvw` parsers exist that can pull the script/data-model XML directly out of the binary file without needing a live QlikView Server session — useful if you only have file access, not server API access.

### 7.3 Parsing the script to recover the underlying SQL

Qlik load scripts pull from Snowflake via an ODBC/JDBC connection, typically in the form:
```
LIB CONNECT TO 'Snowflake_Connection';
SQL SELECT o.order_id, o.order_date, c.region, SUM(o.amount) AS revenue
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
GROUP BY 1,2,3;
```
- Write a small parser (regex or a proper SQL-aware tokenizer) that scans script text for `SQL SELECT ... ;` blocks and the surrounding `LOAD`/`RESIDENT`/`JOIN` statements that further transform the data *inside* Qlik (these post-SQL transforms matter — they're business logic that Snowflake doesn't know about until you replicate it).
- Run this extraction **nightly** over every app, diff against the previous day's script, and store results in a git-backed repo. A script diff is your early-warning signal that a report's logic changed and the semantic model needs review — this is the dynamic part of "how do we keep this in sync."

---

## 8. From Extracted Queries to Snowflake Semantic Views

Two options exist today; pick based on where you want governance to live.

| Option | What it is | When to use |
|---|---|---|
| **Cortex Analyst semantic model (YAML)** | A YAML file (stored as a stage file or in the Analyst config) describing tables, dimensions, measures, synonyms, verified queries | Faster to iterate, easiest to version in git, scoped per-use-case |
| **Native Snowflake Semantic View** (`CREATE SEMANTIC VIEW`) | A first-class Snowflake schema object with `TABLES`, `RELATIONSHIPS`, `FACTS`, `DIMENSIONS`, `METRICS` clauses | Better for org-wide governance — grants/RBAC apply like any other Snowflake object, reusable by Cortex Analyst *and* plain SQL consumers |

Recommendation: **author in YAML for fast iteration during the build-out, then promote stable, validated definitions into native Semantic View objects** for production governance. Cortex Analyst can point at either.

### 8.1 Step-by-step build process

1. **Inventory**: from §7, you have, per Qlik app: the SQL pulled from Snowflake + the post-load transforms + the chart-level measures/dimensions actually used.
2. **Identify grain & star schema**: Qlik's associative model is usually already close to a star schema (Qlik actively encourages denormalized facts + dimension tables). Map each Qlik fact table → a Snowflake fact table; each Qlik dimension table → a Snowflake dimension table. Record the join keys you confirmed via `GetTablesAndKeys`.
3. **Define relationships** between fact and dimension tables (primary key / foreign key) in the semantic model.
4. **Define dimensions** with business-friendly names and **synonyms** pulled from Qlik field captions/labels (these are exactly the vocabulary users already use when they ask questions — reuse it instead of inventing new terms).
5. **Define metrics** directly from the chart expressions you extracted (`Sum(SalesAmount)` → `metric total_sales: SUM(sales_amount)`), not by guessing — this guarantees Cortex Analyst's numbers match what users already see in the Qlik chart, which is your acceptance criterion.
6. **Seed verified queries**: for every commonly-used Qlik chart/sheet, write down the NL question a user would ask to get that same number, paired with the exact SQL that reproduces it. Load these as Cortex Analyst's **verified query repository** — this is what materially improves NL→SQL accuracy over time, more than prompt tweaking does.
7. **Validate by parity testing**: run the same question through Cortex Analyst and compare the result against the live Qlik object's value, for every chart you're migrating. Automate this as a regression suite — re-run whenever the nightly script-diff (§7.3) shows a change.

### 8.2 Example semantic model skeleton (YAML)

```yaml
name: sales_performance
description: Sales facts reverse-engineered from Qlik app "Regional Sales Dashboard"
tables:
  - name: fact_orders
    base_table: { database: ANALYTICS, schema: SALES, table: FACT_ORDERS }
    primary_key: { columns: [order_id] }
  - name: dim_customer
    base_table: { database: ANALYTICS, schema: SALES, table: DIM_CUSTOMER }
    primary_key: { columns: [customer_id] }

relationships:
  - name: orders_to_customer
    left_table: fact_orders
    right_table: dim_customer
    relationship_columns:
      - { left_column: customer_id, right_column: customer_id }

dimensions:
  - name: region
    table: dim_customer
    expr: region
    synonyms: ["territory", "geo"]

facts:
  - name: order_amount
    table: fact_orders
    expr: amount

metrics:
  - name: total_revenue
    expr: SUM(fact_orders.amount)
    synonyms: ["revenue", "sales"]

verified_queries:
  - question: "What was total revenue by region last quarter?"
    sql: |
      SELECT d.region, SUM(f.amount) AS total_revenue
      FROM fact_orders f JOIN dim_customer d ON f.customer_id = d.customer_id
      WHERE f.order_date >= DATEADD(quarter, -1, CURRENT_DATE())
      GROUP BY d.region;
```

### 8.3 Promoting to a native Semantic View (once validated)

```sql
CREATE OR REPLACE SEMANTIC VIEW sales_performance_sv
  TABLES (
    fact_orders AS analytics.sales.fact_orders PRIMARY KEY (order_id),
    dim_customer AS analytics.sales.dim_customer PRIMARY KEY (customer_id)
  )
  RELATIONSHIPS (
    orders_to_customer AS fact_orders (customer_id) REFERENCES dim_customer (customer_id)
  )
  FACTS ( fact_orders.order_amount AS amount )
  DIMENSIONS ( dim_customer.region AS region WITH SYNONYMS ('territory','geo') )
  METRICS ( total_revenue AS SUM(fact_orders.order_amount) );
```

Grant access the same way you would on any table/view; Cortex Analyst respects the caller's role when querying through it, so existing row-access/masking policies on the base tables remain enforced.

---

## 9. Production Hardening

### 9.1 Security & data governance
- Treat "what reaches OpenAI" as a controlled boundary: only pre-aggregated, non-row-level facts cross it. Add a pre-send filter that rejects payloads containing raw PII columns even if a chain bug tries to forward them.
- Enforce Snowflake RBAC/row-access/masking policies on the **base tables**, not just at the semantic-model layer — Cortex Analyst-generated SQL runs as the requesting user, so policy enforcement is inherited for free, but only if you don't run summarization jobs under an overly-privileged service role.
- Log every (user, question, generated SQL, rows returned, summary) tuple for audit — this is also your evaluation dataset.

### 9.2 Cost control
- Cache aggressively (§4.4). Most daily summaries are unchanged from the day before in large swaths of a business.
- Use a cheaper model for the map step (per-group summaries) and reserve GPT-4o-class for the reduce/final-narrative step.
- Set per-request and daily token budgets per team/report; alert on anomalies (a runaway agent loop calling Cortex Analyst repeatedly is the most common cost blowup).

### 9.3 Observability & evaluation
- Trace every chain run (LangSmith or equivalent): prompts, generated SQL, latency, token cost.
- Build a golden set of (question → expected SQL/expected KPI value) pairs from §8.1 step 7 and re-run it on every semantic-model change as a CI gate — treat the semantic model like code with tests.
- Track Cortex Analyst's own accuracy metrics and feed corrected SQL back in as new verified queries — this is a closed feedback loop, not a one-time setup.

### 9.4 Reliability
- Map-step failures (one group's summarization call fails) should not fail the whole reduce step — retry the failed chunk, and if it still fails, drop it with an explicit "data unavailable for X" note rather than silently omitting it.
- Guardrail check before delivery: does the narrative's stated numbers match the source aggregate (a simple regex/number-extraction cross-check against the SQL result) — catches LLM number hallucination before it reaches an executive.

### 9.5 CI/CD
- Semantic model YAML / Semantic View DDL lives in git, deployed via Snowflake CLI or schemachange, with the golden-question regression suite (§9.3) as a required check before merge.
- Script-diff job from §7.3 opens a PR/ticket automatically when a source Qlik app's script changes, so semantic model drift is caught proactively instead of when a user reports a wrong number.

---

## 10. Suggested Rollout Phases

1. **Phase 0 — Extraction tooling**: build the nightly Qlik script-extraction job (§7) and get it storing diffs in git for your top 5–10 reports.
2. **Phase 1 — Semantic model v1**: hand-build YAML semantic models for those reports from the extracted scripts + chart expressions; validate via parity testing against live Qlik numbers.
3. **Phase 2 — Scenario A (batch)**: stand up Dynamic Tables + the map-reduce chain for one report's daily exec summary end-to-end; get the caching/fingerprinting working.
4. **Phase 3 — Scenario B (ad hoc)**: expose the chat interface backed by Cortex Analyst for the same reports; add guardrails and the SQL-disclosure audit trail.
5. **Phase 4 — Scale out**: extend to the rest of the report estate, promote stable semantic models to native Semantic Views, add CI gates and the golden-question regression suite.
6. **Phase 5 — Optimize**: tune model selection (map vs. reduce engines), tighten cost/caching, and formalize the verified-query feedback loop as an ongoing process, not a project.

---

## 11. Open Decisions to Resolve Early

- **Egress policy**: is sending any aggregated text to OpenAI acceptable under your data-governance policy, or must everything stay inside Snowflake (forcing Cortex `COMPLETE` end-to-end)? This single decision changes §5 significantly.
- **QlikSense vs. QlikView mix**: confirm which API path (Engine/REST vs. COM/QMS) is available in your environment — this is often an infra/licensing question, not just technical.
- **Ownership of the semantic model**: who signs off when a verified query or metric definition changes — this needs an owner before Phase 4, since it becomes the single source of truth multiple chat experiences depend on.
