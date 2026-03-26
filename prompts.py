import json
from typing import Any


def prompt_compact_normalization(document_text: str, context: dict[str, Any]) -> str:
    return f"""
You are an ETL compact-export normalization engine.

Return STRICT JSON only.

GOAL:
Normalize a small, metadata-rich ETL pseudocode export into a clean technical graph payload.

IMPORTANT:
- Treat STAGE_LIST, LINK_FLOW, and EXECUTION_FLOW as first-class structural evidence.
- Use explicit STAGE blocks only as supporting detail when present.
- Preserve the original stage identities in members[].
- Prefer factual structure over naming heuristics.
- Do not invent edges that are not supported by LINK_FLOW or EXECUTION_FLOW.
- Build the TECHNICAL graph only. A deterministic graph builder will derive the high-level graph later.

NODE KINDS:
- source
- lookup
- integration
- core_transform
- validation
- store
- target
- exception

EDGE TYPES:
- main
- lookup
- exception
- secondary

CLUSTER:
- primary
- secondary

NORMALIZATION RULES:
- Sources are extraction/origin stages.
- Lookups are dependency/reference stages feeding transforms.
- Stores are intermediate persistence stages and hashed-file outputs used as storage.
- Targets are final persisted outputs.
- Transforms with business logic should be core_transform or integration or validation.
- If a node clearly does not reach the primary target and represents a side branch, mark cluster = secondary.
- Keep labels clean and readable, but keep members[] as original names.
- Node ids must be normalized, stable, and must not start with STAGE_.

OUTPUT:
{{
  "nodes": [
    {{
      "id": "normalized_id",
      "raw_name": "original stage name",
      "stage_type": "Transformer|HashedFile|OracleConnector|...",
      "kind": "source|lookup|integration|core_transform|validation|store|target|exception",
      "label": "clean readable label",
      "business_label": "optional clean business label",
      "cluster": "primary|secondary",
      "members": ["original stage name"]
    }}
  ],
  "edges": [
    {{
      "from": "normalized_id",
      "to": "normalized_id",
      "type": "main|lookup|exception|secondary",
      "evidence": "short factual evidence"
    }}
  ],
  "notes": ["optional short notes"]
}}

Deterministic compact-export hints:
{json.dumps({
    "compact_export_profile": context.get("compact_export_profile", {}),
    "stage_catalog": context.get("stage_catalog", []),
    "link_flow_hints": context.get("link_flow_hints", []),
    "execution_flow_hints": context.get("execution_flow_hints", {}),
    "target_table_hints": context.get("target_table_hints", []),
    "annotation_hints": context.get("annotation_hints", []),
}, indent=2)}

Pseudocode:
{document_text[:16000]}
"""


def prompt_phase1(context: dict[str, Any], chunk: dict[str, Any]) -> str:
    return f"""
You are an ETL signal extraction engine.

Return STRICT JSON only.

PHASE 1: SIGNAL EXTRACTION

GOAL:
Extract ONLY what exists. No interpretation.

----------------------------------------
EXTRACT ONLY:
----------------------------------------
- name (original)
- kind:
  source | transform | lookup | store | target | exception
- stage_type (optional)
- tables (if present)
- primacy_score: 0 to 1
- canonical_hint: best business-facing dataset or stage label when obvious

----------------------------------------
DO NOT:
----------------------------------------
- assign roles (integration/core/etc.)
- merge signals
- deduplicate across chunks
- infer pipeline
- classify based on position
- create groups
- decide main flow vs secondary flow

----------------------------------------
CLASSIFICATION RULES
----------------------------------------

CLASSIFICATION PRIORITY RULE (VERY IMPORTANT)

When determining kind:
1. FIRST use chunk metadata:
   - chunk.kind
   - chunk.block_type
   - chunk.stage_name
   - chunk.object_type
   - chunk.connector_direction
   - chunk.wrapper_hint
2. ONLY use chunk text if metadata is ambiguous
3. NEVER classify based on unrelated stage names mentioned in the same chunk

Example:
If stage_name = STAGE_SEQUENTIAL_FILE_*
-> classify as source EVEN IF transformer is mentioned in text

OracleConnector -> source
SequentialFile -> source
Transformer -> transform

HashedFile:
- read-only -> lookup
- write -> store
- unclear -> lookup

Target connectors -> target
File writes -> exception

DATASTAGE EXPORT OBJECT RULES

- OracleConnector with SELECT / source-table extraction remains source even if SQL is rich
- OracleConnector with TRUNCATE / INSERT / target-table loading is target
- CustomStage is often a wrapper; only classify it as transform if real business logic is explicit
- CustomOutput is often a target-side wrapper or output record; do not promote it into a business node unless persistence / loading is explicit
- HashedOutput may be a store only when persistence is explicit; otherwise treat cautiously as wrapper/output metadata
- transactionManager and connector wrapper records are operational plumbing, not business core nodes

SOURCE LOCK RULE

If metadata indicates the object is structurally a source/extractor:
- keep kind = source
- do NOT upgrade it to transform just because SQL contains joins, CASE logic, or derived columns
- rich SQL may increase primacy_score, but must not change a structural source into a transform

Primacy scoring:
- High primacy when strongly tied to target tables, annotations, detailed schemas, or rich SQL logic
- Low primacy when stage is generic scaffold, wrapper, passthrough, or operational plumbing
- Use primacy only as extraction metadata, not structure inference

----------------------------------------
STRICT FILTERING
----------------------------------------

Ignore:
- DSLink*, Lnk_* (internal link records, not real stages)
- debug, peek, rowgenerator, columngenerator, sample stages
- runtime ops (OPEN, CLOSE, FETCH)
- generic connector wrappers with no business identity and no column definitions
- generic names such as SOURCE_DATA_INPUT, MAIN_DATA_OUTPUT, TARGET_OUTPUT, NEXT_PROCESSING_STAGE when they only wrap movement without column or table context

DO NOT ignore:
- Load_* stages (these are real business stages like exception file writers)
- Stages containing "log", "sort", "copy" in their body text (these are normal ETL metadata fields)
- Any stage with SQL, column definitions, target tables, or transformation logic

CANONICAL SIGNAL RULE

When duplicate-like signals appear in the same chunk:
- keep the more specific business-bearing stage or dataset
- prefer signals with real table names, detailed SQL, or detailed column definitions
- demote generic wrappers by lowering primacy_score instead of inventing new groups

METADATA PRIORITY RULE

Use chunk metadata to guide extraction:
- If has_sql = true:
  -> strongly favor extracting this as a SOURCE candidate
- If sql_complexity > 3:
  -> increase primacy_score significantly
- If signal_strength is high:
  -> prefer this signal over others in the same chunk
- If block_type == "stage" and contains SQL:
  -> this is likely a PRIMARY SOURCE

LINK BLOCK RULE

If chunk.block_type == "link":
- Extract ONLY connections if explicitly defined
- Do NOT create new signals unless a real dataset/stage is referenced
- Assign LOW primacy_score to any extracted signal

----------------------------------------
OUTPUT
----------------------------------------

{{
  "chunk_id": "{chunk["chunk_id"]}",
  "signals": [
    {{
      "name": "original name",
      "kind": "source|transform|lookup|store|target|exception",
      "stage_type": "string",
      "tables": ["string"],
      "canonical_hint": "string",
      "primacy_score": 0.0
    }}
  ]
}}

Chunk metadata:
{json.dumps({
    "chunk_id": chunk.get("chunk_id"),
    "stage_name": chunk.get("stage_name"),
    "business_name": chunk.get("business_name"),
    "block_name": chunk.get("block_name"),
    "block_type": chunk.get("block_type"),
    "object_type": chunk.get("object_type"),
    "connector_direction": chunk.get("connector_direction"),
    "wrapper_hint": chunk.get("wrapper_hint"),
    "target_tables": chunk.get("target_tables", []),
    "input_links": chunk.get("input_links", []),
    "output_links": chunk.get("output_links", []),
    "is_structural": chunk.get("is_structural"),
    "chunk_index": chunk.get("chunk_index"),
    "total_chunks_in_stage": chunk.get("total_chunks_in_stage"),
    "kind": chunk.get("kind"),
    "tables": chunk.get("tables", []),
    "has_sql": chunk.get("has_sql"),
    "sql_complexity": chunk.get("sql_complexity"),
    "signal_strength": chunk.get("signal_strength"),
    "semantic": chunk.get("semantic", {}),
    "start_token": chunk.get("start_token"),
    "end_token": chunk.get("end_token"),
}, indent=2)}

Hints:
{json.dumps({
    "stage_name_hints": context["stage_name_hints"][:20],
    "target_table_hints": context.get("target_table_hints", [])[:10],
    "annotation_hints": context.get("annotation_hints", [])[:10],
    "stage_primacy_hints": context.get("stage_primacy_hints", {}),
    "entity_map": context.get("entity_map", {}),
}, indent=2)}

Chunk:
{chunk["text"]}
"""


def prompt_phase15(document_text: str, phase1: dict[str, Any], context: dict[str, Any]) -> str:
    return f"""
You are an ETL system reconstruction engine.

Return STRICT JSON only.

PHASE 1.5: GLOBAL PIPELINE UNDERSTANDING

Your job is NOT to extract components.
Your job is to understand the entire pipeline FIRST.

----------------------------------------
INPUT
----------------------------------------
- Full pseudocode
- Phase 1 signals (raw extracted nodes)

----------------------------------------
GOAL
----------------------------------------
Build a COMPLETE MENTAL MODEL of the ETL pipeline.

DO NOT output graph.
DO NOT assign final roles.

----------------------------------------
YOU MUST
----------------------------------------

STEP 1: Identify dominant pipeline
- Where does data START?
- Where does it END (main target)?
- What is the MAIN path (ignore side flows initially)

STEP 2: Build main flow chain
Example:
Sources -> Integration -> Transform -> Store -> Target

STEP 3: Identify:
- Side flows (exception, lookup, secondary pipelines)
- Lookups vs real sources
- Stores vs transient stages

STEP 4: Identify strongest source
Use:
- SQL richness
- joins
- column definitions
- connection type
- table references

STEP 5: Identify core transform
- where most business logic exists
- where enrichment happens
- where lookup is used

STEP 6: For metadata-rich compact exports
- if compact_export_profile.is_metadata_rich_compact_export == true, treat STAGE_LIST, LINK_FLOW, and EXECUTION_FLOW as first-class evidence
- use deterministic link_flow_hints as structural truth candidates
- use stage_catalog to recover important nodes even when explicit STAGE: blocks are sparse
- use execution_flow_hints to infer primary processing, lookup dependencies, storage, and target loading

----------------------------------------
IMPORTANT RULES
----------------------------------------

- Ignore stage names if misleading
- Prefer data movement over naming
- SQL > stage labels
- If table appears in SQL, prioritize it over stage name
- joins imply pre-integration
- sequential transformers may be ONE logical stage

STRUCTURAL ROLE GUARDS

- Distinguish business nodes from technical wrappers
- CustomStage, CustomOutput, transactionManager, connector child records, and output-record wrappers are not automatically business nodes
- Prefer real source/transform/store/target stages over wrapper records when both appear
- If a node is structurally a source connector or extractor, it must remain a source even if its SQL is rich
- Rich SQL may mark a source as preintegrated_source, but must not make it the core_transform by itself
- A wrapper/output record should not become store_node or target_node unless the text explicitly shows persisted loading semantics
- If a downstream `Tfm_...`, `Transformer`, or transform-like stage exists, strongest_source and core_transform should usually NOT be the same node
- If a target table hint exists, prefer that persisted table/stage as target_node over a CustomOutput wrapper

SQL DOMINANCE RULE

If a source contains:
- 3+ joins
- multiple derived columns
- CASE logic

Then:
- Treat it as PRE_INTEGRATED_SOURCE
- Downstream transform is NOT integration unless it also converges other real primary sources
- Favor the downstream heavy-business-logic transformer as the core_transform candidate

----------------------------------------
OUTPUT
----------------------------------------

{{
  "dominant_pipeline": [
    "source_node",
    "integration_node",
    "core_transform_node",
    "store_node",
    "target_node"
  ],
  "strongest_source": "node_id",
  "strongest_source_evidence": "brief evidence",
  "core_transform": "node_id",
  "core_transform_evidence": "brief evidence",
  "store_node": "node_id",
  "target_node": "node_id",
  "preintegrated_sources": ["node_id"],
  "side_flows": [
    {{
      "type": "exception|lookup|secondary",
      "nodes": ["node_ids"],
      "reason": "brief factual explanation"
    }}
  ],
  "reasoning": "short explanation of pipeline structure"
}}

Global hints:
{json.dumps({
    "job_name_hints": context.get("job_name_hints", [])[:10],
    "compact_export_profile": context.get("compact_export_profile", {}),
    "stage_catalog": context.get("stage_catalog", [])[:25],
    "link_flow_hints": context.get("link_flow_hints", [])[:30],
    "execution_flow_hints": context.get("execution_flow_hints", {}),
    "target_table_hints": context.get("target_table_hints", [])[:10],
    "annotation_hints": context.get("annotation_hints", [])[:10],
    "dominant_table_hints": context.get("dominant_table_hints", [])[:20],
    "document_sql_profile": context.get("document_sql_profile", {}),
    "stage_sql_hints": context.get("stage_sql_hints", {}),
    "stage_primacy_hints": context.get("stage_primacy_hints", {}),
    "entity_map": context.get("entity_map", {}),
}, indent=2)}

Phase 1 signals:
{json.dumps(phase1, indent=2)}

Pseudocode (first window):
{document_text[:9000]}

Pseudocode (second window):
{document_text[9000:18000]}
"""


def prompt_phase2(document_text: str, phase1: dict[str, Any], phase15: dict[str, Any], context: dict[str, Any]) -> str:
    return f"""
You are an ETL graph reconstruction engine.

Return STRICT JSON only.

PHASE 2: BUILD FULL DATA FLOW GRAPH

========================================
STEP 0 — LOCKED ANCHOR (NO RE-EVALUATION)
========================================

You are given a precomputed dominant pipeline from Phase 1.5.
This is GROUND TRUTH.

----------------------------------------
LOCKED NODES (DO NOT MODIFY)
----------------------------------------

- canonical_source
- core_transform
- store_node
- target_node

----------------------------------------
STRICT RULES (MANDATORY)
----------------------------------------

- DO NOT re-identify source
- DO NOT re-identify core transform
- DO NOT re-identify store
- DO NOT re-identify target
- DO NOT score candidates
- DO NOT compare alternatives
- DO NOT override Phase 1.5 even if conflicting evidence appears

If pseudocode suggests a different structure:
-> IGNORE it
-> Attach nodes relative to the given anchor

----------------------------------------
YOUR ROLE
----------------------------------------

You are NOT discovering the pipeline.
You are ATTACHING nodes to an existing pipeline.

----------------------------------------
INPUT ANCHOR (FROM PHASE 1.5)
----------------------------------------

canonical_source = {json.dumps(phase15.get("strongest_source"))}
core_transform   = {json.dumps(phase15.get("core_transform"))}
store_node       = {json.dumps(phase15.get("store_node"))}
target_node      = {json.dumps(phase15.get("target_node"))}

----------------------------------------
REQUIRED SPINE
----------------------------------------

You MUST enforce this chain:

canonical_source -> core_transform -> store_node -> target_node

Even if intermediate stages exist, they must be mapped into this chain,
not replace it.

STORE RESOLUTION RULE:
If Phase 1.5 defines a store_node:
- that node MUST be used as anchor_load_node
- no transform can replace it
- any transform feeding target must be upstream of store
  OR treated as a passthrough wrapper

TRANSFORM NEAR TARGET RULE:
If a transform is directly connected to target
BUT a store_node exists from Phase 1.5:
- that transform is NOT the anchor_load_node
- it must be treated as upstream transform OR passthrough wrapper
- store remains the only valid load node

ATTACHMENT STRATEGY:
For each node:
1. If node == canonical_source/core_transform/store_node/target_node:
   -> place directly in spine
2. Else:
   -> find the closest anchor node it connects to
   -> attach as upstream, downstream, or side flow

DO NOT build the path from scratch.
DO NOT recompute the chain.

========================================
STEP 1 — SHARED LINK CONFLICT RESOLUTION
========================================

A shared link conflict occurs when multiple transform nodes declare
the same upstream input link name.

For each conflict, score each competing transformer:
- has a LOOKUP_LINK in addition to INPUT_LINK              (+3)
- has explicit exception routing (exception output pin)    (+3)
- has detailed STAGE_VARIABLES or transformation logic     (+2)
- has detailed COLUMN_DEFINITIONS or COLUMN_MAPPINGS       (+2)
- its id matches the locked core_transform                 (+5)

Winner takes the shared link.
All other competing nodes are demoted: wire them as secondary branches
that receive a copy of the upstream data, NOT as primary consumers.

========================================
STEP 2 — SECONDARY FLOW ISOLATION
========================================

A node or sub-chain is a SECONDARY FLOW when:
- it never reaches the locked target_node
- OR its output is a sequential file, error file, or non-primary table
- OR it receives from the same source as the anchor chain but leads
  to a different terminal node

Lookup nodes used by the locked core_transform are NOT secondary.
They are primary side dependencies and should stay attached to the primary flow.

Mark each node with:  "flow": "primary" | "secondary" | "exception"

Rules:
- Do NOT suppress secondary flows from the graph
- DO mark them clearly so downstream phases can distinguish them
- A node receiving from a high-primacy embedded-join source
  that is NOT the canonical core transformer is secondary by default
  unless its output path reaches the primary target

========================================
STEP 3 — FULL GRAPH CONSTRUCTION
========================================

Now wire all nodes using actual link declarations in the pseudocode.

For metadata-rich compact exports:
- if compact_export_profile.is_metadata_rich_compact_export == true, treat link_flow_hints as first-class structural evidence
- preserve the directed paths given by LINK_FLOW unless Phase 1.5 locked anchors require a minimal attachment adjustment
- use stage_catalog to keep important stages visible even if explicit STAGE blocks are sparse
- use execution_flow_hints as supporting evidence for classifying source, lookup, store, target, and secondary branches

Rules:
- preserve all steps
- preserve sequence
- preserve branching and merging
- do not promote source/lookup nodes into transforms
- use the locked Phase 1.5 chain as the spine
- secondary and exception flows attach to the spine as branches

SIDE FLOW WIRING:
- lookup -> transform edges use type: "lookup"
- transform -> exception edges use type: "exception"
- all anchor chain edges use type: "main"
- all secondary branch edges use type: "secondary"

========================================
OUTPUT
========================================

{{
  "anchor_chain": {{
    "canonical_source": "node_id",
    "canonical_core_transformer": "node_id",
    "anchor_load_node": "node_id",
    "primary_target": "node_id",
    "anchor_score_notes": "brief rationale"
  }},
  "nodes": [
    {{
      "id": "component_id",
      "type": "source|transform|lookup|store|target|exception",
      "name": "display name",
      "members": ["original node names"],
      "flow": "primary|secondary|exception",
      "reason": "factual evidence only"
    }}
  ],
  "edges": [
    {{
      "from": "node_id",
      "to": "node_id",
      "type": "main|lookup|exception|secondary",
      "reason": "factual flow evidence only"
    }}
  ],
  "side_flows": [
    {{
      "type": "lookup|exception|reject|secondary",
      "from": "node_id",
      "to": "node_id",
      "reason": "factual side-flow evidence only"
    }}
  ]
}}

Phase 1.5 global pipeline understanding:
{json.dumps(phase15, indent=2)}

Phase 1 signals:
{json.dumps(phase1, indent=2)}

Deterministic metadata-rich export hints:
{json.dumps({
    "compact_export_profile": context.get("compact_export_profile", {}),
    "stage_catalog": context.get("stage_catalog", [])[:25],
    "link_flow_hints": context.get("link_flow_hints", [])[:30],
    "execution_flow_hints": context.get("execution_flow_hints", {}),
}, indent=2)}

Pseudocode (first window):
{document_text[:8000]}

Pseudocode (second window — check for transformer and target detail):
{document_text[8000:16000]}
"""


def prompt_phase3(phase2: dict[str, Any], context: dict[str, Any]) -> str:
    return f"""
You are an ETL lineage pattern detection engine.

Return STRICT JSON only.

PHASE 3: PATTERN DETECTION AND ROLE ASSIGNMENT

You are given the Phase 2 graph which includes an anchor_chain field.
The anchor_chain identifies the pre-resolved dominant path.
Your job is to detect structural patterns WITHOUT overriding that anchor.

DO NOT modify the graph.
DO NOT remove nodes.
DO NOT compress yet.
DO NOT invent replacement ids.

========================================
STEP 0 — ACCEPT THE ANCHOR CHAIN
========================================

Read phase2.anchor_chain directly.

Assign these roles immediately without further evaluation:
- anchor_chain.canonical_source          -> role: source (primary)
- anchor_chain.canonical_core_transformer -> role: core_transform (locked)
- anchor_chain.anchor_load_node          -> role: store (if hashed file) OR passthrough (if transform)
- anchor_chain.primary_target            -> role: target (locked)

These four roles are LOCKED. No pattern detection step may override them.

When you encounter any pattern that would reclassify a locked node,
SKIP the reclassification and note the conflict in the pattern description instead.

========================================
STEP 1 — SECONDARY FLOW CLASSIFICATION
========================================

All nodes marked flow: "secondary" in Phase 2 are evaluated separately.

For secondary nodes:
- detect their own patterns (integration, chain, validation, etc.)
- assign roles prefixed with "secondary_" to make them distinguishable
  e.g. "secondary_integration", "secondary_core_transform"
- do NOT include secondary node roles in the primary chain computation

This prevents a high-edge-count secondary node (such as a transformer
receiving 9 source inputs for a parallel sub-flow) from competing with
the locked core_transform in Step 0.

========================================
STEP 2 — ANALYZE PRIMARY GRAPH STRUCTURE
========================================

Operate ONLY on nodes where flow == "primary" or flow == "exception".

From edges among these nodes:

1. Identify:
   - multiple incoming edges (convergence)
   - multiple outgoing edges (divergence)
   - linear chains
   - branching paths

2. Identify:
   - all source -> target paths within primary flow
   - longest transformation chain within primary flow
   - convergence points
   - divergence points

========================================
STEP 3 — DETECT PATTERNS (PRIMARY FLOW ONLY)
========================================

Apply each pattern only to primary-flow nodes.
Skip any pattern whose winning node would conflict with a locked role.

----------------------------------------
PATTERN 1: MANY -> ONE (INTEGRATION)
----------------------------------------

If a PRIMARY-flow transform has multiple incoming edges from other primary nodes:
-> classify as: "integration"

CONFLICT GUARD: if the candidate is the locked core_transform, do NOT
reclassify it as integration. Instead emit the pattern but mark it
"blocked_by_lock": true and assign integration to the next-best candidate.

----------------------------------------
PATTERN 2: SEQUENTIAL TRANSFORMS
----------------------------------------

1. Compute all paths within PRIMARY flow from sources to target
2. PRIMARY CHAIN = path that passes through the locked core_transform
3. All other paths = secondary chains (mark primary_chain: false)
4. The primary chain is not computed by longest-path — it is anchored
   to the locked core_transform and traced outward both directions

----------------------------------------
PATTERN 3: CORE TRANSFORM
----------------------------------------

Already locked from Step 0.
Emit this pattern with the locked node id.
Do not re-evaluate candidates.

----------------------------------------
PATTERN 4: VALIDATION / FILTER
----------------------------------------

A validation candidate within the PRIMARY chain:
- is closer to target or store than the locked core_transform
- sends data to exception
- appears AFTER the locked core_transform in the primary chain

----------------------------------------
PATTERN 5: STORE PATTERN
----------------------------------------

A node is STORE only if:
- it represents a persistent dataset (hashed file / table)
- it is NOT a transform type
- it appears as a storage entity in Phase 2 nodes

NEVER reclassify a transform as store.

----------------------------------------
PATTERN 6: EXCEPTION FLOW
----------------------------------------

If edges exist: transform -> exception (in primary or exception flow)
-> classify: "exception_flow"
These may originate from the locked core_transform.

----------------------------------------
PATTERN 7: LOOKUP ENRICHMENT
----------------------------------------

Attach lookup to the node in the primary chain that:
- is AFTER integration (if any)
- has an explicit lookup input edge in Phase 2

If the locked core_transform has a lookup edge, attach the lookup to it.

----------------------------------------
PATTERN 8: BRANCHING
----------------------------------------

If a primary-flow node has multiple outgoing primary edges:
-> classify: "branching"

----------------------------------------
PATTERN 10: EMBEDDED_JOIN_SOURCE
----------------------------------------

If a source node has rich multi-join SQL (3+ JOINs):
-> classify: "embedded_join_source"

CONFLICT RESOLUTION AGAINST PATTERN 1:
An embedded_join_source SUPPRESSES Pattern 1 (integration) for any
transform that receives ONLY from that source and no other primary sources.

Rationale: if the only reason a transform looks like "integration" is
because a pre-joined source feeds it, the integration already happened
inside the source SQL. The downstream transform is a core_transform,
not an integration node.

This suppression does NOT apply if the transform also receives from
other distinct primary sources (true post-source convergence).

========================================
STEP 4 — ASSIGN ROLES (PRIMARY FLOW)
========================================

Locked roles from Step 0 are already assigned.
For remaining primary nodes, assign ONE role:

- integration
- core_transform
- validation
- passthrough

PRIORITY when conflicts occur:
1. locked assignments (never overridden)
2. core_transform
3. store
4. validation
5. integration (suppressed when embedded_join_source applies)
6. lookup_enrichment
7. sequential_chain
8. branching

Passthrough nodes MUST NOT influence chain or core detection.

========================================
STEP 5 — SUMMARIZE
========================================

{{
  "anchor_chain_confirmed": {{
    "canonical_source": "node_id",
    "canonical_core_transformer": "node_id",
    "anchor_load_node": "node_id",
    "primary_target": "node_id"
  }},
  "patterns": [
    {{
      "type": "integration|sequential_chain|core_transform|validation|store|exception_flow|lookup_enrichment|branching|merging|embedded_join_source",
      "nodes": ["list of node ids"],
      "blocked_by_lock": false,
      "description": "what this pattern represents"
    }}
  ],
  "node_roles": {{
    "node_id": "integration|core_transform|validation|passthrough|store|source|target|exception|lookup|secondary_integration|secondary_core_transform"
  }},
  "chains": [
    {{
      "nodes": ["node1", "node2", "node3"],
      "type": "main_chain",
      "length": 3,
      "primary_chain": true
    }}
  ],
  "lookup_usage": [
    {{
      "lookup": "node_id",
      "used_by": "node_id"
    }}
  ],
  "exception_flows": [
    {{
      "from": "node_id",
      "to": "node_id"
    }}
  ]
}}

Phase 2 graph:
{json.dumps(phase2, indent=2)}

Target hints for cross-check:
{json.dumps(context.get("target_table_hints", []))}
"""
def prompt_phase4(phase2: dict[str, Any], phase3: dict[str, Any]) -> str:
    return f"""
You are an ETL lineage compression engine.

Return STRICT JSON only.

PHASE 4: SEMANTIC COMPRESSION

========================================
STEP 0 — READ LOCKED ANCHOR
========================================

Read phase3.anchor_chain_confirmed directly.

These four nodes have permanently assigned roles.
Do NOT reclassify them. Do NOT merge them with other nodes.
Do NOT omit them.

  canonical_source          -> kind: source (primary)
  canonical_core_transformer -> kind: core_transform
  anchor_load_node           -> kind: store  (if hashed file type)
                                    passthrough (if transform type — omit from output)
  primary_target             -> kind: target

========================================
STEP 1 — CONSUME ROLES DIRECTLY FROM PHASE 3
========================================

Use phase3.node_roles as the authoritative role map.
Do NOT re-derive roles from structure.

ROLE MAPPING TO KIND:
  source                    -> kind: source
  core_transform            -> kind: core_transform
  integration               -> kind: integration
  validation                -> kind: validation
  store                     -> kind: store
  target                    -> kind: target
  exception                 -> kind: exception
  lookup                    -> kind: lookup
  passthrough               -> OMIT from normalized_components
  secondary_*               -> OMIT from normalized_components
                               ADD to isolated_sub_flows instead

========================================
STEP 2 — CONSUME FLOW MARKERS FROM PHASE 2
========================================

Use phase2 node flow markers as a second filter AFTER role mapping:
  flow: "primary"   -> include in normalized_components
  flow: "secondary" -> OMIT from normalized_components, add to isolated_sub_flows
  flow: "exception" -> include in normalized_components as kind: exception

If role mapping (Step 1) and flow marker (Step 2) conflict:
  - trust Phase 3 role for kind assignment
  - trust Phase 2 flow marker for include/exclude decision
  - when both say exclude, exclude
  - when one says include and one says exclude, EXCLUDE and add to isolated_sub_flows

========================================
STEP 3 — COMPRESSION RULES
========================================

A. DO NOT mix kinds:
   source != transform, transform != store, store != lookup

B. LAYER PRESERVATION:
   Only emit layers that are evidenced by phase3.node_roles.
   Do not invent an integration layer if phase3 has no integration role.
   Do not invent a validation layer if phase3 has no validation role.
   Emit exactly the layers that exist.

C. MEMBER NAMES:
   Always include original node names in members[].
   Never invent or shorten original names inside members.

D. SOURCE GROUPING:
   If multiple source nodes feed the same core_transform or integration node,
   group them as kind: source_group only when they share the same flow: "primary".
   Secondary sources are excluded.

E. LOOKUP GROUPING:
   If multiple lookup nodes are used by the same transform,
   group them as kind: lookup_group.
   Preserve all original names in members.

F. PASSTHROUGH OMISSION:
   Nodes with role: passthrough are silently removed.
   Do not mention them in normalized_components.
   Do not add them to isolated_sub_flows.

G. ISOLATED SUB-FLOW RULE:
   Any node where:
   - phase3.node_roles has a secondary_ prefix
   - OR phase2 flow == "secondary"
   is an isolated sub-flow.
   Add a descriptive note per sub-flow chain (not per node).
   Group nodes of the same secondary chain into one note.

TRANSFORM MERGE RULE (CRITICAL)

If multiple transform nodes:
- are connected sequentially (A -> B)
- no branching exists between them
- both belong to primary flow

Then:
- merge them into ONE core_transform component
- combine their members
- assign one business-facing name

Example:
TRANSFORMER_VOR_PROCESSING -> VEHICLE_SK
becomes one core_transform component such as:
"Vehicle Offroad Processing"

========================================
OUTPUT
========================================

{{
  "normalized_components": [
    {{
      "id": "short_uppercase_id",
      "name": "Business-friendly label",
      "kind": "source|source_group|lookup|lookup_group|integration|core_transform|validation|store|target|exception",
      "members": ["ORIGINAL node names from phase2"],
      "reason": "one-line compression rationale"
    }}
  ],
  "isolated_sub_flows": [
    {{
      "label": "short description of what this sub-flow does",
      "members": ["original node names"],
      "terminal": "terminal node name or file path"
    }}
  ]
}}

Phase 3 (roles and anchor):
{json.dumps(phase3, indent=2)}

Phase 2 (nodes with flow markers):
{json.dumps({
    "anchor_chain": phase2.get("anchor_chain"),
    "nodes": phase2.get("nodes", [])
}, indent=2)}
"""


def prompt_phase34(phase2: dict[str, Any], context: dict[str, Any], diagram_intent: dict[str, Any]) -> str:
    return f"""
You are an ETL lineage structure and compression engine.

Return STRICT JSON only.

PHASE 3+4: PATTERN DETECTION, ROLE ASSIGNMENT, AND SEMANTIC COMPRESSION

You are given the Phase 2 graph which includes an anchor_chain field.
The anchor_chain identifies the pre-resolved dominant path.
Your job is to detect structural patterns, assign roles, and then compress
the graph without overriding the anchor.

DO NOT modify the anchor chain.
DO NOT invent replacement ids outside normalized_components.id.

========================================
STEP 0 - ACCEPT THE ANCHOR CHAIN
========================================

Read phase2.anchor_chain directly.

Assign these roles immediately without further evaluation:
- anchor_chain.canonical_source           -> role: source (primary)
- anchor_chain.canonical_core_transformer -> role: core_transform (locked)
- anchor_chain.anchor_load_node           -> role: store (if hashed file) OR passthrough (if transform)
- anchor_chain.primary_target             -> role: target (locked)

These four roles are LOCKED. No later step may override them.

========================================
STEP 1 - SECONDARY FLOW CLASSIFICATION
========================================

All nodes marked flow: "secondary" in Phase 2 are evaluated separately.

For secondary nodes:
- detect their own patterns
- assign roles prefixed with "secondary_"
- do NOT include them in normalized_components
- include them in isolated_sub_flows instead

========================================
STEP 2 - DETECT PRIMARY PATTERNS
========================================

Operate ONLY on nodes where flow == "primary" or flow == "exception".

Detect:
- integration
- sequential_chain
- core_transform
- validation
- store
- exception_flow
- lookup_enrichment
- branching
- embedded_join_source

Rules:
- do not override locked anchor roles
- if a node conflicts with a locked role, emit the pattern and mark blocked_by_lock: true
- lookup attached to the locked core_transform remains a lookup, not secondary

========================================
STEP 3 - ASSIGN NODE ROLES
========================================

Assign one authoritative role per node:
- integration
- core_transform
- validation
- passthrough
- store
- source
- target
- exception
- lookup
- secondary_integration
- secondary_core_transform

========================================
STEP 4 - COMPRESS USING THOSE ROLES
========================================

Use the assigned node_roles as the authoritative role map.
Do NOT re-derive roles from structure a second time.

Compression rules:
- source -> kind: source
- core_transform -> kind: core_transform
- integration -> kind: integration
- validation -> kind: validation
- store -> kind: store
- target -> kind: target
- exception -> kind: exception
- lookup -> kind: lookup
- passthrough -> omit
- secondary_* -> omit from normalized_components and add to isolated_sub_flows

Include/exclude decision:
- flow: "primary" -> include in normalized_components
- flow: "secondary" -> exclude and add to isolated_sub_flows
- flow: "exception" -> include as exception

Compression behavior:
- do not mix kinds
- only emit layers evidenced by node_roles
- preserve original names in members[]
- group primary sources when appropriate into source_group
- group lookup nodes when appropriate into lookup_group
- omit passthrough nodes silently
- merge sequential primary transforms into one core_transform when no branching exists
- prefer cleaner business-facing component names from diagram_intent.rename_candidates
- do not use raw stage prefixes when a clearer name is available
- for metadata-rich compact exports, use STAGE_LIST + LINK_FLOW + EXECUTION_FLOW as first-class evidence when stage blocks are sparse

========================================
OUTPUT
========================================

{{
  "phase3": {{
    "anchor_chain_confirmed": {{
      "canonical_source": "node_id",
      "canonical_core_transformer": "node_id",
      "anchor_load_node": "node_id",
      "primary_target": "node_id"
    }},
    "patterns": [
      {{
        "type": "integration|sequential_chain|core_transform|validation|store|exception_flow|lookup_enrichment|branching|merging|embedded_join_source",
        "nodes": ["list of node ids"],
        "blocked_by_lock": false,
        "description": "what this pattern represents"
      }}
    ],
    "node_roles": {{
      "node_id": "integration|core_transform|validation|passthrough|store|source|target|exception|lookup|secondary_integration|secondary_core_transform"
    }},
    "chains": [
      {{
        "nodes": ["node1", "node2", "node3"],
        "type": "main_chain",
        "length": 3,
        "primary_chain": true
      }}
    ],
    "lookup_usage": [
      {{
        "lookup": "node_id",
        "used_by": "node_id"
      }}
    ],
    "exception_flows": [
      {{
        "from": "node_id",
        "to": "node_id"
      }}
    ]
  }},
  "phase4": {{
    "normalized_components": [
      {{
        "id": "short_uppercase_id",
        "name": "Business-friendly label",
        "kind": "source|source_group|lookup|lookup_group|integration|core_transform|validation|store|target|exception",
        "members": ["ORIGINAL node names from phase2"],
        "reason": "one-line compression rationale"
      }}
    ],
    "isolated_sub_flows": [
      {{
        "label": "short description of what this sub-flow does",
        "members": ["original node names"],
        "terminal": "terminal node name or file path"
      }}
    ]
  }}
}}

Phase 2 graph:
{json.dumps(phase2, indent=2)}

Target hints for cross-check:
{json.dumps(context.get("target_table_hints", []))}

Diagram intent:
{json.dumps(diagram_intent, indent=2)}

Deterministic metadata-rich export hints:
{json.dumps({
    "compact_export_profile": context.get("compact_export_profile", {}),
    "stage_catalog": context.get("stage_catalog", [])[:25],
    "link_flow_hints": context.get("link_flow_hints", [])[:30],
    "execution_flow_hints": context.get("execution_flow_hints", {}),
}, indent=2)}
"""


def prompt_phase5(phase3: dict[str, Any], phase4: dict[str, Any], diagram_intent: dict[str, Any]) -> str:
    return f"""
You are an ETL model builder.

Return STRICT JSON only.

PHASE 5: BUILD TWO MODELS

You build a HIGH-LEVEL model and a TECHNICAL model from Phase 4 components.
Both models must use the same anchor spine.

========================================
STEP 0 — READ THE SPINE
========================================

The spine is the ordered primary path from Phase 3:
  phase3.anchor_chain_confirmed:
    canonical_source -> canonical_core_transformer -> anchor_load_node -> primary_target

All nodes in both models are organized relative to this spine.

========================================
HIGH-LEVEL MODEL RULES
========================================

Purpose: business-facing, minimal, clean.

INCLUDE:
- source_group (or single source if only one primary source)
- lookup_group (or single lookup if only one)
- integration (if present in phase4)
- core_transform
- store (if present in phase4)
- target
- exception (if present in phase4)

EXCLUDE:
- individual source members (show the group label only)
- individual lookup members (show the group label only)
- passthrough nodes (already omitted in phase4)
- secondary flows entirely
- validation nodes UNLESS they have an exception output
  (if validation produces an exception file it is worth showing)

GROUPING:
- Use phase4 source_group and lookup_group directly
- If phase4 has no group, create a group label from the members

NOTES:
- Add one note per isolated_sub_flow from phase4
- Format: "Secondary flow: [label] -> [terminal]"

NAMING RULE:
- Prefer clean business-facing names from diagram_intent.rename_candidates
- Avoid raw prefixes such as STAGE_, TFM_, HF_, ORA_, SRC_ in display labels
- Keep members[] raw and unchanged for traceability

NODE REDUCTION RULE (CRITICAL)

High-level model MUST reduce node count.

Rules:
- Only include:
  source_group (or single source)
  lookup_group (single node only)
  ONE core_transform
  store (if exists)
  target
  exception (if exists)
- DO NOT include:
  intermediate transforms
  validation nodes
  routing nodes
  sequential transforms

Target node count:
4 to 6 nodes maximum.
If more nodes exist, you MUST merge them.

========================================
TECHNICAL MODEL RULES
========================================

Purpose: engineering-facing, complete, traceable.

INCLUDE:
- ALL primary-flow nodes from phase4.normalized_components
- individual source nodes (expanded, not grouped)
- individual lookup nodes (expanded, not grouped)
- validation nodes always
- exception nodes always

SECONDARY FLOWS:
- Include secondary flow nodes as a separate named cluster
- Prefix their ids with "SEC_" in the technical model
- They appear as a side block, not inline with the spine

ORIGINAL NAMES:
- Every node must include its original name(s) from members[]
- Do NOT rename or shorten original names

DISPLAY LABEL RULE:
- technical model names should still be readable
- preserve engineering specificity, but remove noisy connector prefixes when possible
- if a normalized component already has a clean phase4 name, reuse it

NODE EXPANSION RULE (CRITICAL)

Technical model MUST expand transformations.

Rules:
- Split core_transform into logical steps if semantics imply:
  derivation
  validation
  routing
- Each logical step becomes a separate technical node
- Lookup nodes must be individual, not grouped
- Exception nodes should preserve rule context when available

========================================
OUTPUT
========================================

{{
  "high_level_model": {{
    "spine": ["ordered list of node ids on primary path"],
    "nodes": [
      {{
        "id": "node_id",
        "kind": "source_group|lookup_group|integration|core_transform|store|target|exception",
        "name": "display label",
        "members": ["original names"]
      }}
    ],
    "lookup_edges": [
      {{"from": "lookup_id", "to": "transform_id"}}
    ],
    "exception_edges": [
      {{"from": "transform_id", "to": "exception_id"}}
    ],
    "notes": ["Secondary flow: label -> terminal"]
  }},
  "technical_model": {{
    "spine": ["ordered list of node ids on primary path"],
    "nodes": [
      {{
        "id": "node_id",
        "kind": "source|lookup|integration|core_transform|validation|store|target|exception",
        "name": "display label",
        "members": ["original names"],
        "cluster": "primary|secondary"
      }}
    ],
    "lookup_edges": [
      {{"from": "lookup_id", "to": "transform_id"}}
    ],
    "exception_edges": [
      {{"from": "transform_id", "to": "exception_id"}}
    ],
    "secondary_edges": [
      {{"from": "node_id", "to": "node_id"}}
    ],
    "notes": []
  }}
}}

Phase 4 components:
{json.dumps(phase4, indent=2)}

Phase 3 anchor and chains (for spine ordering):
{json.dumps({
    "anchor_chain_confirmed": phase3.get("anchor_chain_confirmed"),
    "chains": phase3.get("chains", [])
}, indent=2)}

Diagram intent:
{json.dumps(diagram_intent, indent=2)}
"""


def prompt_phase6(document_text: str, phase3: dict[str, Any], phase5: dict[str, Any]) -> str:
    return f"""
You are an ETL transformation semantics engine.

Return STRICT JSON only.

PHASE 6: TRANSFORMATION SEMANTICS

========================================
STEP 0 — IDENTIFY ANALYSIS TARGETS
========================================

Only analyze nodes that are:
1. Listed in phase5.technical_model.nodes
2. Have kind: core_transform, integration, or validation
3. Have cluster: "primary"

Do NOT analyze:
- source, lookup, store, target, exception nodes
- secondary cluster nodes
- nodes not present in phase5

The locked canonical_core_transformer from phase3 is your highest-priority target.
Analyze it first and most thoroughly.

========================================
STEP 1 — EXTRACT SEMANTICS
========================================

For each analysis target, locate its business logic in the pseudocode.

Use BOTH pseudocode windows:
Window A (early logic):
{document_text[:7000]}

Window B (transformation detail):
{document_text[7000:14000]}

For each node, identify which of these semantic categories apply.
A node may have multiple categories.

STANDARDIZATION:
  Evidence: UPPER_CASE, TRIM, date format conversion, type casting,
            COALESCE with default values, NULL handling with fixed defaults

DERIVATION:
  Evidence: CASE WHEN expressions, calculated fields, NVL with expressions,
            fields built from other fields (not just passed through)

LOOKUP:
  Evidence: explicit JOIN to dimension table, LOOKUP() call,
            surrogate key resolution (e.g. vehicle_sk from registration_no)

VALIDATION:
  Evidence: IF condition THEN REJECT, constraint checks, flag fields,
            mandatory field checks, range checks, format checks

FILTER:
  Evidence: WHERE clause, IF condition with no-output branch,
            records routed to exception based on flag value

ROUTING:
  Evidence: svException flag, multiple OUTPUT TO links from same node,
            CASE-based output selection, records split to different targets

========================================
STEP 2 — EXTRACT BUSINESS RULES
========================================

For each node, extract 2-5 specific human-readable rules.
Rules must be concrete, not generic.

BAD: "Applies business rules"
GOOD: "Sets SUPP_SK = -99 when garage name is null"
GOOD: "Rejects records where VEHICLE_SK lookup fails"
GOOD: "Routes to exception file when svException = -1"

========================================
OUTPUT
========================================

{{
  "transform_semantics": [
    {{
      "node_id": "phase5 node id",
      "original_name": "original stage name",
      "business_label": "short human label (3-6 words)",
      "semantics": ["VALIDATION", "DERIVATION"],
      "rules": [
        "concrete rule 1",
        "concrete rule 2"
      ],
      "rule_summary": "1-2 line summary",
      "exception_condition": "condition text or empty string"
    }}
  ]
}}

Phase 5 models (for node list):
{json.dumps({
    "technical_nodes": [
        n for n in phase5.get("technical_model", {}).get("nodes", [])
        if n.get("kind") in ("core_transform", "integration", "validation")
        and n.get("cluster") == "primary"
    ]
}, indent=2)}

Phase 3 anchor (canonical_core_transformer for priority):
{json.dumps(phase3.get("anchor_chain_confirmed", {}), indent=2)}
"""


def prompt_phase7(phase2: dict[str, Any], phase3: dict[str, Any],
                  phase4: dict[str, Any], phase5: dict[str, Any], phase6: dict[str, Any],
                  diagram_intent: dict[str, Any]) -> str:
    return f"""
You are an ETL graph assembly engine.

Return STRICT JSON only.

PHASE 7: BUILD GRAPH

========================================
SPINE ANCHOR
========================================

The primary spine is fixed:
{json.dumps(phase3.get("anchor_chain_confirmed", {}), indent=2)}

All graph construction is relative to this spine.

========================================
ID NORMALIZATION RULE (CRITICAL)
========================================

You are given phase4.normalized_components which define the canonical ids.

You MUST:
1. Build an ID_MAP:
   - for each normalized component
   - map every member raw name -> component.id
2. Normalize ALL node ids and ALL edge endpoints using ID_MAP
3. If multiple raw nodes map to the same normalized id:
   - collapse them into one node
   - merge members
   - merge duplicate edges
4. RAW ids such as STAGE_* are allowed ONLY inside members
5. FINAL graph node ids and edge endpoints MUST use normalized ids only

STRICT RULE:
If a final node id starts with "STAGE_" it is wrong.

========================================
EDGE SOURCE RULES
========================================

You have two edge sources. Use them differently:

phase2.edges — structural truth, but contains mixed flow types.
  Use ONLY edges where type == "main" or type == "lookup" or type == "exception"
  for the high_level_graph.
  Use ALL edge types for the technical_graph, but tag secondary edges separately.

phase5 nodes — clean compressed model.
  Use phase5.high_level_model.nodes for the high_level_graph.
  Use phase5.technical_model.nodes for the technical_graph.

DO NOT re-derive edges from node names.
DO NOT invent edges not present in phase2.

========================================
HIGH-LEVEL GRAPH RULES
========================================

Nodes: use phase5.high_level_model.nodes exactly.
Edges: use only phase2 edges where type == "main", filtered to nodes present
       in the high-level model. If a phase2 edge references a node not in
       the high-level model, skip it.
Lookup edges: from phase5.high_level_model.lookup_edges
Exception edges: from phase5.high_level_model.exception_edges
Secondary flows: do NOT appear as nodes or edges.
               Represent them as a graph-level note only.

========================================
TECHNICAL GRAPH RULES
========================================

Nodes: use phase5.technical_model.nodes exactly.
       Nodes with cluster == "secondary" are included but visually separated.
Edges:
  - main edges: phase2 edges where type == "main"
  - lookup edges: phase2 edges where type == "lookup"
  - exception edges: phase2 edges where type == "exception"
  - secondary edges: phase2 edges where type == "secondary"
    (connect only to nodes with cluster == "secondary")

Semantic labels: inject from phase6.transform_semantics.
  Match on node_id. Add business_label, semantics, rules, rule_summary,
  and exception_condition to the node.

DIAGRAM CLEANLINESS RULE:
- prefer clean business-facing labels over raw stage ids
- keep members[] as the traceable originals
- if phase5 label is still noisy, use diagram_intent.rename_candidates or phase4 component name
- high_level_graph should look presentation-ready, not like a raw export dump

========================================
OUTPUT
========================================

{{
  "high_level_graph": {{
    "nodes": [
      {{
        "id": "node_id",
        "kind": "source_group|lookup_group|integration|core_transform|store|target|exception",
        "label": "display label",
        "members": ["original names"]
      }}
    ],
    "edges": [
      {{
        "from": "node_id",
        "to": "node_id",
        "type": "main|lookup|exception"
      }}
    ],
    "notes": ["secondary flow summaries from phase5"]
  }},
  "technical_graph": {{
    "nodes": [
      {{
        "id": "node_id",
        "kind": "source|lookup|integration|core_transform|validation|store|target|exception",
        "label": "display label",
        "members": ["original names"],
        "cluster": "primary|secondary",
        "business_label": "from phase6 or null",
        "semantics": ["from phase6 or []"]
      }}
    ],
    "edges": [
      {{
        "from": "node_id",
        "to": "node_id",
        "type": "main|lookup|exception|secondary"
      }}
    ]
  }}
}}

Phase 4 normalized components:
{json.dumps(phase4, indent=2)}

Phase 5 models:
{json.dumps(phase5, indent=2)}

Phase 2 edges (filtered reference):
{json.dumps({"edges": phase2.get("edges", []), "anchor_chain": phase2.get("anchor_chain")}, indent=2)}

Phase 6 semantics:
{json.dumps(phase6, indent=2)}

Diagram intent:
{json.dumps(diagram_intent, indent=2)}
"""
