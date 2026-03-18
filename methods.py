import os
import json
import uuid
import re
import shlex
import hashlib
from collections import defaultdict, deque
from pathlib import Path
from dotenv import load_dotenv
from xml.sax.saxutils import escape
import networkx as nx
try:
    from langchain_openai import AzureChatOpenAI
except ImportError:
    AzureChatOpenAI = None

try:
    from sql_metadata import Parser
except ImportError:
    Parser = None

try:
    import graphviz
except ImportError:
    graphviz = None

from .parsers import parse_alteryx_jobs, parse_datastage_jobs, parse_informatica_jobs

load_dotenv()

SERVICE_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = str((SERVICE_ROOT / "data").resolve())
UPLOAD_BASE_PATH = str((SERVICE_ROOT / "data" / "uploads").resolve())
OUTPUT_BASE_PATH = str((SERVICE_ROOT / "data" / "output").resolve())
LOG_BASE_PATH = str((SERVICE_ROOT / "data" / "logs").resolve())
_SEMANTIC_LLM = None


"""
**************************************
Lineage Agent Helper Methods
**************************************
"""


# -----------------------------------------------------
# Utility
# -----------------------------------------------------

def write_json(path, data):
    """
    Write JSON safely while ensuring directory exists.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def read_json(path):
    """
    Read JSON safely. Returns empty list if file not found.
    """
    if not path or not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def require_existing_file(path, label):
    """
    Validate that an expected input file exists.
    """

    if not path:
        raise FileNotFoundError(f"Missing required {label}.")

    if not os.path.exists(path):
        raise FileNotFoundError(f"{label} not found: {path}")

    return path


def normalize_lineage_options(options=None):
    """
    Normalize lineage rendering and semantic-enrichment options with stable defaults.
    """

    defaults = {
        "diagram_mode": "logical",
        "include_sql_sources": False,
        "collapse_intermediate_datasets": True,
        "include_lookup_nodes": True,
        "llm_role_classification": False,
        "llm_business_labels": False,
        "llm_sql_grouping": False,
    }

    if not options:
        return defaults

    normalized = defaults.copy()

    for key, value in options.items():
        if value is not None:
            normalized[key] = value

    normalized["diagram_mode"] = str(normalized["diagram_mode"]).lower()

    return normalized


def _safe_source_key(value):
    """
    Normalize a source file identifier for grouping.
    """

    return os.path.basename(value or "lineage")


def _graphs_payload(entries):
    """
    Wrap a list of per-source graph entries in a stable envelope.
    """

    return {"graphs": entries}


def _is_graphs_payload(payload):
    """
    Return True when payload already contains per-source graph entries.
    """

    return isinstance(payload, dict) and isinstance(payload.get("graphs"), list)


def _payload_graphs(payload):
    """
    Read a per-source graph envelope and return its entries.
    """

    if _is_graphs_payload(payload):
        return payload.get("graphs", [])

    return []


def _json_string(data):
    """
    Serialize structured data consistently for tool outputs that expect strings.
    """

    return json.dumps(data, indent=2)


def _lineage_complexity(node_count, edge_count):
    """
    Classify lineage size to drive abstraction and routing heuristics.
    """

    if node_count <= 8 and edge_count <= 8:
        return "small"

    if node_count <= 18 and edge_count <= 22:
        return "medium"

    return "large"


def _display_label(node):
    """
    Build a render label that preserves the original node name and appends semantic context.
    """

    base_label = str(node.get("label") or node.get("id") or "").strip()
    details = [str(item).strip() for item in node.get("details", []) if str(item).strip()]

    if not details:
        return base_label

    return "\n".join([base_label] + details)


def _semantic_detail_lines(node_type, stage_meta=None):
    """
    Return short semantic detail lines for a node without replacing the original label.
    """

    stage_meta = stage_meta or {}
    detail_map = {
        "SOURCE": ["Source table"],
        "SOURCE_GROUP": ["Grouped upstream sources"],
        "EXTRACT": ["Source extract"],
        "TRANSFORM": ["Transformation"],
        "DECISION": ["Conditional / multi-output transform"],
        "LOOKUP": ["Lookup store"],
        "STORE": ["Persistent store"],
        "DB_TARGET": ["Final target"],
        "FILE_TARGET": ["File target"],
        "TARGET": ["Target"],
        "EXCEPTION": ["Exception / reject output"],
        "DATASET": ["Intermediate dataset"],
    }

    details = list(detail_map.get(node_type, []))
    constraints = stage_meta.get("constraints", [])
    for item in constraints[:2]:
        expression = item.get("expression", "").strip()
        if expression:
            details.append(f"Condition: {expression[:80]}")

    return details


def _make_node(node_id, node_type, stage_meta=None, label=None, details=None, **extra):
    """
    Create a lineage node with stable label and semantic metadata.
    """

    node = {
        "id": node_id,
        "type": node_type,
        "label": label or node_id,
        "details": list(details or _semantic_detail_lines(node_type, stage_meta)),
    }
    node.update(extra)
    return node


def build_lineage_diagram_filename(source_files, options=None, extension=".drawio"):
    """
    Build a deterministic diagram filename for the same inputs and options.
    """

    normalized_options = normalize_lineage_options(options)
    safe_sources = sorted(source_files or ["lineage"])
    base_name = "_".join(os.path.splitext(os.path.basename(item))[0] for item in safe_sources[:3])
    base_name = re.sub(r'[^A-Za-z0-9._-]+', "_", base_name).strip("._") or "lineage"
    fingerprint_payload = {
        "source_files": safe_sources,
        "options": normalized_options,
    }
    fingerprint = hashlib.md5(
        json.dumps(fingerprint_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()[:10]

    return f"{base_name}_{fingerprint}{extension}"


def _normalize_arrow_text(text):
    """
    Normalize corrupted arrow characters from source files.
    """

    if not text:
        return ""

    return (
        text.replace("â†", "←")
        .replace("â†’", "→")
        .replace("\u2190", "←")
        .replace("\u2192", "→")
    )


def _detect_pseudocode_type(text):
    """
    Detect the dominant pseudocode family for parser dispatch.
    """

    sample = str(text or "")

    if any(marker in sample for marker in (
        "WORKFLOW_DEFINITION:",
        "SOURCE_QUALIFIER:",
        "TARGET_DEFINITION:",
        "SESSION_DEFINITION:",
        "SOURCE_DEFINITION ",
        "TRANSFORMATION ",
        "Input Link:",
        "Output Link:",
        "DATA_FLOW_CONNECTIONS:",
    )):
        return "informatica"

    if re.search(r"//\s*---\s*.*\(ID:\s*\d+,\s*[^)]+\)\s*\[Lines\s*\d+-\d+\]", sample):
        return "alteryx"

    if any(marker in sample for marker in ("StageType:", "Constraint (", "Input: â† dataset_", "Output: â†’ dataset_")):
        return "datastage"

    return "datastage"


def _split_jobs_for_parser(source_name, text, parser_type):
    """
    Split source text into parser-specific job records.
    """

    normalized = _normalize_arrow_text(text)

    if parser_type != "datastage":
        return [{
            "source_file": source_name,
            "job_index": 1,
            "parser_type": parser_type,
            "content": normalized.strip(),
        }]

    jobs = []
    segments = re.split(r"//\s*=+", normalized)

    for index, seg in enumerate(segments, start=1):
        if len(seg.strip()) <= 50:
            continue
        jobs.append({
            "source_file": source_name,
            "job_index": index,
            "parser_type": parser_type,
            "content": seg.strip(),
        })

    if jobs:
        return jobs

    return [{
        "source_file": source_name,
        "job_index": 1,
        "parser_type": parser_type,
        "content": normalized.strip(),
    }]


def _extract_stage_header(block):
    """
    Parse stage header metadata from a pseudocode block.
    Accepts headers both with and without line numbers.
    """

    match = re.search(
        r'//\s*---\s*\[(.*?)\s*:\s*(.*?)\](?:\s*\[Lines\s*(\d+)-(\d+)\])?',
        block
    )

    if not match:
        return None

    return {
        "kind": match.group(1).strip(),
        "stage": match.group(2).strip(),
        "line_start": int(match.group(3)) if match.group(3) else 0,
        "line_end": int(match.group(4)) if match.group(4) else 0,
    }

def _extract_link_entries(block, direction):
    """
    Parse Input/Output entries with dataset id, alias, and link name.
    """

    pattern = (
        rf'{direction}:\s*[←→]\s*(dataset_\d+)'
        rf'(?:\s*\(([^)]+)\))?'
        rf'(?:\s*\(Link:\s*([^)]+)\))?'
    )

    entries = []

    for dataset_id, alias, link_name in re.findall(pattern, block):
        entries.append({
            "dataset_id": dataset_id.strip(),
            "alias": (alias or "").strip(),
            "link_name": (link_name or "").strip(),
        })

    return entries


def _extract_stage_type(block):
    """
    Extract StageType value from a pseudocode block.
    """

    match = re.search(r'StageType:\s*([^\r\n]+)', block)
    return match.group(1).strip() if match else ""


def _extract_sql_block(block):
    """
    Extract SQL payload from a pseudocode block.
    """

    match = re.search(
        r'SQL:\s*(.*?)(?=\n\s*(?:Input|Output|StageType|Transformations|Constraint|Link File|UnixFormat|PipeStage|UnicodeBOM|UnicodeSwapped|WithFilter):|\Z)',
        block,
        re.S
    )
    return match.group(1).strip() if match else ""


def _extract_constraints(block):
    """
    Extract constraint expressions from a pseudocode block.
    """

    constraints = []

    for link_name, expression in re.findall(r'Constraint\s*\(([^)]+)\):\s*([^\r\n]+)', block):
        constraints.append({
            "link_name": link_name.strip(),
            "expression": expression.strip(),
        })

    return constraints


def _deduplicate_edges(edges):
    """
    Remove duplicate edges while preserving order.
    """

    deduped = []
    seen = set()

    for edge in edges:
        key = (
            edge.get("source", ""),
            edge.get("target", ""),
            edge.get("kind", ""),
        )

        if key in seen:
            continue

        seen.add(key)
        deduped.append(edge)

    return deduped


def _is_lookup_stage(stage_meta):
    """
    Determine whether a stage behaves as a lookup/helper node.
    """

    if not stage_meta:
        return False

    stage_type = (stage_meta.get("stage_type") or "").lower()
    stage_kind = (stage_meta.get("stage_kind") or "").lower()

    return "hashedfile" in stage_type or "hashedfile" in stage_kind


def _classify_semantic_stage(stage_meta, inbound_count, outbound_count, used_as_lookup):
    """
    Classify a stage node for semantic lineage rendering.
    """

    stage_name = stage_meta.get("stage", "")
    stage_type = (stage_meta.get("stage_type") or "").lower()
    stage_kind = (stage_meta.get("stage_kind") or "").lower()
    output_count = len(stage_meta.get("outputs", []))

    if stage_type == "source":
        return "SOURCE"

    if stage_type == "target":
        return "TARGET"

    if stage_name.lower().startswith("sq_"):
        return "EXTRACT"

    if "source_qualifier" in stage_type or "source_qualifier" in stage_kind:
        return "EXTRACT"

    if "seqfile" in stage_type or "seqfile" in stage_kind:
        return "FILE_TARGET"

    if "hashedfile" in stage_type or "hashedfile" in stage_kind:
        return "LOOKUP" if used_as_lookup else "STORE"

    if "oracleconnector" in stage_type:
        if inbound_count == 0:
            return "EXTRACT"
        if outbound_count == 0:
            return "DB_TARGET"
        return "TRANSFORM"

    if "exception" in stage_name.lower() and outbound_count == 0:
        return "FILE_TARGET"

    if ("transformer" in stage_type or "transformer" in stage_kind) and output_count > 1:
        return "DECISION"

    return "TRANSFORM"


def _edge_kind_from_input(entry, target_stage=None, source_stage=None):
    """
    Classify relationship label from an input link.
    """

    alias = (entry.get("alias") or "").lower()
    link_name = (entry.get("link_name") or "").lower()
    target_stage_type = (target_stage.get("stage_type") or "").lower() if target_stage else ""
    target_stage_kind = (target_stage.get("stage_kind") or "").lower() if target_stage else ""
    source_is_lookup = _is_lookup_stage(source_stage)

    if any(token in alias or token in link_name for token in ("reject", "error", "err_", "rej_")):
        return "exception"

    if "hashedfile" in target_stage_type or "hashedfile" in target_stage_kind:
        return "input"

    if source_is_lookup:
        return "lookup"

    if "lookup" in alias or "lookup" in link_name:
        return "lookup"

    return "input"


def _should_use_llm_semantics(options):
    """
    Return True when any semantic-enrichment option requires an LLM hint pass.
    """

    return any(
        options.get(key)
        for key in ("llm_role_classification", "llm_business_labels", "llm_sql_grouping")
    )


def _get_semantic_llm():
    """
    Create a cached Azure OpenAI client for semantic enrichment.
    """

    global _SEMANTIC_LLM

    if _SEMANTIC_LLM is not None:
        return _SEMANTIC_LLM

    if AzureChatOpenAI is None:
        return None

    deployment = os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_NAME")
    version = os.getenv("AZURE_OPENAI_API_VERSION")
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")

    if not all([deployment, version, endpoint, api_key]):
        return None

    _SEMANTIC_LLM = AzureChatOpenAI(
        azure_deployment=deployment,
        openai_api_version=version,
        azure_endpoint=endpoint,
        api_key=api_key,
        temperature=0.0,
    )
    return _SEMANTIC_LLM


def _extract_json_object(text):
    """
    Extract the first JSON object from a model response.
    """

    if not text:
        return {}

    text = text.strip()

    try:
        return json.loads(text)
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")

    if start == -1 or end == -1 or end <= start:
        return {}

    try:
        return json.loads(text[start:end + 1])
    except Exception:
        return {}


def _generate_llm_semantic_hints(stages, edges, tables, options):
    """
    Ask the LLM for batched stage-role, business-label, and SQL-grouping hints.
    """

    if not _should_use_llm_semantics(options):
        return {}

    llm = _get_semantic_llm()
    if llm is None:
        return {}

    payload = {
        "stages": [
            {
                "stage": stage.get("stage"),
                "stage_kind": stage.get("stage_kind"),
                "stage_type": stage.get("stage_type"),
                "inputs": [item.get("alias") or item.get("dataset_id") for item in stage.get("inputs", [])],
                "outputs": [item.get("alias") or item.get("dataset_id") for item in stage.get("outputs", [])],
                "constraints": [item.get("expression") for item in stage.get("constraints", [])],
                "has_sql": bool(stage.get("sql")),
            }
            for stage in stages
        ],
        "edges": edges,
        "sql_tables": [table.get("table") for table in tables],
        "options": {
            "llm_role_classification": bool(options.get("llm_role_classification")),
            "llm_business_labels": bool(options.get("llm_business_labels")),
            "llm_sql_grouping": bool(options.get("llm_sql_grouping")),
        },
    }

    prompt = f"""
You are enriching ETL lineage semantics.
Return JSON only.

Tasks:
1. If llm_role_classification is true, classify stages into one of:
   SOURCE, EXTRACT, LOOKUP, STORE, TRANSFORM, DECISION, DB_TARGET, FILE_TARGET.
2. If llm_business_labels is true, provide a short business label.
3. If llm_sql_grouping is true, group SQL tables by target stage when it improves readability.
4. If constraints indicate filtering or branching, provide a short condition summary.

Rules:
- Preserve original stage and table names externally. Do not rename them.
- Only return stages or tables that appear in the input payload.
- Keep business labels short.
- Keep condition summaries short.
- Return valid JSON matching this structure:
{{
  "stages": [
    {{
      "stage": "stage_name",
      "role": "TRANSFORM",
      "business_label": "Short label",
      "condition_summary": "Optional short condition"
    }}
  ],
  "sql_groups": [
    {{
      "target_stage": "stage_name",
      "group_name": "Upstream Sources",
      "tables": ["SCHEMA.TABLE_A", "SCHEMA.TABLE_B"]
    }}
  ]
}}

Input:
{json.dumps(payload, indent=2)}
"""

    try:
        response = llm.invoke(prompt)
        content = response.content if hasattr(response, "content") else str(response)
        return _extract_json_object(content)
    except Exception:
        return {}


def _validate_llm_semantic_hints(hints, stages, tables, options):
    """
    Validate semantic hints against parsed stages and extracted SQL tables.
    """

    if not isinstance(hints, dict):
        return {"stages": {}, "sql_groups": []}

    stage_names = {stage.get("stage") for stage in stages}
    table_names = {table.get("table") for table in tables}
    valid_roles = {"SOURCE", "EXTRACT", "LOOKUP", "STORE", "TRANSFORM", "DECISION", "DB_TARGET", "FILE_TARGET"}
    stage_hints = {}

    for item in hints.get("stages", []):
        if not isinstance(item, dict):
            continue
        stage_name = item.get("stage")
        if stage_name not in stage_names:
            continue

        normalized = {}
        role = str(item.get("role") or "").strip().upper()
        if options.get("llm_role_classification") and role in valid_roles:
            normalized["role"] = role

        business_label = str(item.get("business_label") or "").strip()
        if options.get("llm_business_labels") and business_label:
            normalized["business_label"] = business_label[:80]

        condition_summary = str(item.get("condition_summary") or "").strip()
        if condition_summary:
            normalized["condition_summary"] = condition_summary[:120]

        if normalized:
            stage_hints[stage_name] = normalized

    sql_groups = []
    if options.get("llm_sql_grouping"):
        for item in hints.get("sql_groups", []):
            if not isinstance(item, dict):
                continue
            target_stage = item.get("target_stage")
            if target_stage not in stage_names:
                continue
            group_name = str(item.get("group_name") or "Upstream Sources").strip()[:80]
            tables_in_group = [table for table in item.get("tables", []) if table in table_names]
            if len(tables_in_group) < 2:
                continue
            sql_groups.append({
                "target_stage": target_stage,
                "group_name": group_name or "Upstream Sources",
                "tables": sorted(set(tables_in_group), key=str.lower),
            })

    return {"stages": stage_hints, "sql_groups": sql_groups}


_ALTERYX_GENERIC_KINDS = {
    "Formula",
    "Filter",
    "Sort",
    "AutoField",
    "DataCleansing",
    "Select",
    "Sample",
    "Unique",
}

_ALTERYX_SOURCE_KINDS = {"DbFileInput", "TextInput", "InputData", "DynamicInput"}
_ALTERYX_SINK_KINDS = {"DbFileOutput", "BrowseV2", "OutputData", "Render"}


def _alteryx_branch_key(stage_name):
    """
    Infer a coarse business branch hint from an Alteryx stage name.
    """

    tokens = re.findall(r"[A-Za-z]+", str(stage_name or ""))
    stop_words = {
        "browse",
        "data",
        "db",
        "xls",
        "csv",
        "sp",
        "summary",
        "analytics",
        "calculate",
        "clean",
        "sort",
        "filter",
        "join",
        "value",
        "score",
        "profit",
        "margin",
        "field",
        "high",
        "performance",
        "stock",
        "status",
        "segment",
        "category",
    }

    for token in tokens:
        lowered = token.lower()
        if lowered not in stop_words:
            return lowered

    return None


def _alteryx_connect(stages_by_name, source_name, target_name, edge_counter):
    """
    Add a synthetic connection between two Alteryx stages if not already present.
    """

    source_stage = stages_by_name.get(source_name)
    target_stage = stages_by_name.get(target_name)

    if not source_stage or not target_stage or source_name == target_name:
        return edge_counter

    existing = {item.get("alias") for item in source_stage.get("outputs", [])}
    if target_name in existing:
        return edge_counter

    dataset_id = f"dataset_alt_resolved_{edge_counter}"
    source_stage.setdefault("outputs", []).append({
        "dataset_id": dataset_id,
        "alias": target_name,
        "link_name": "",
    })
    target_stage.setdefault("inputs", []).append({
        "dataset_id": dataset_id,
        "alias": source_name,
        "link_name": "",
    })

    return edge_counter + 1


def _alteryx_generic_candidates(stages):
    """
    Return generic Alteryx tools that still have incomplete connectivity.
    """

    return [
        stage for stage in stages
        if stage.get("stage_kind") in _ALTERYX_GENERIC_KINDS
        and (not stage.get("inputs") or not stage.get("outputs"))
    ]


def _generate_alteryx_disambiguation_hints(stages, unresolved_stages, job_text):
    """
    Ask the LLM to resolve only ambiguous connector-less Alteryx generic tools.
    """

    llm = _get_semantic_llm()
    if llm is None or not unresolved_stages:
        return {}

    ordered = sorted(stages, key=lambda item: (item.get("line_start", 0), item.get("stage", "")))
    payload = {
        "workflow_family": "alteryx",
        "ordered_stages": [
            {
                "stage": stage.get("stage"),
                "stage_kind": stage.get("stage_kind"),
                "line_start": stage.get("line_start"),
                "inputs": [item.get("alias") for item in stage.get("inputs", [])],
                "outputs": [item.get("alias") for item in stage.get("outputs", [])],
            }
            for stage in ordered
        ],
        "ambiguous_stages": [
            {
                "stage": stage.get("stage"),
                "stage_kind": stage.get("stage_kind"),
                "line_start": stage.get("line_start"),
                "inputs": [item.get("alias") for item in stage.get("inputs", [])],
                "outputs": [item.get("alias") for item in stage.get("outputs", [])],
            }
            for stage in unresolved_stages
        ],
        "job_excerpt": str(job_text or "")[:12000],
    }

    prompt = f"""
You are resolving ambiguous connector-less Alteryx lineage.
Return JSON only.

Task:
- For each ambiguous stage, suggest at most one predecessor and at most one successor.
- Use only stage names that already exist in the payload.
- Prefer local line-order consistency and business branch consistency.
- Do not invent stages.

Return this shape:
{{
  "assignments": [
    {{
      "stage": "Calculate Value Score",
      "predecessor": "Clean Customer Data",
      "successor": "Join Customer Data"
    }}
  ]
}}

Input:
{json.dumps(payload, indent=2)}
"""

    try:
        response = llm.invoke(prompt)
        content = response.content if hasattr(response, "content") else str(response)
        return _extract_json_object(content)
    except Exception:
        return {}


def _apply_alteryx_disambiguation_hints(stages, hints):
    """
    Validate and apply targeted Alteryx connection hints.
    """

    if not isinstance(hints, dict):
        return stages

    stages_by_name = {stage.get("stage"): stage for stage in stages}
    edge_counter = 1 + sum(len(stage.get("outputs", [])) for stage in stages)

    for item in hints.get("assignments", []):
        if not isinstance(item, dict):
            continue

        stage_name = item.get("stage")
        predecessor = item.get("predecessor")
        successor = item.get("successor")

        if stage_name not in stages_by_name:
            continue

        if predecessor in stages_by_name:
            edge_counter = _alteryx_connect(stages_by_name, predecessor, stage_name, edge_counter)
        if successor in stages_by_name:
            edge_counter = _alteryx_connect(stages_by_name, stage_name, successor, edge_counter)

    return list(stages_by_name.values())


def _resolve_alteryx_ambiguous_steps(stages, job_text=""):
    """
    Apply a deterministic pass first, then optional LLM help for unresolved generic Alteryx steps.
    """

    if not stages:
        return stages

    ordered = sorted(stages, key=lambda item: (item.get("line_start", 0), item.get("stage", "")))
    branch_by_stage = {}
    active_branch = None

    for stage in ordered:
        branch = _alteryx_branch_key(stage.get("stage"))
        if branch:
            active_branch = branch
        branch_by_stage[stage.get("stage")] = active_branch

    active_branch = None
    for stage in reversed(ordered):
        stage_name = stage.get("stage")
        if branch_by_stage.get(stage_name):
            active_branch = branch_by_stage[stage_name]
        elif active_branch:
            branch_by_stage[stage_name] = active_branch

    stages_by_name = {stage.get("stage"): stage for stage in ordered}
    edge_counter = 1 + sum(len(stage.get("outputs", [])) for stage in ordered)

    for stage in ordered:
        stage_name = stage.get("stage")
        stage_kind = stage.get("stage_kind")
        if stage_kind not in _ALTERYX_GENERIC_KINDS:
            continue

        branch = branch_by_stage.get(stage_name)
        line_start = stage.get("line_start", 0)

        if stage.get("inputs") and not stage.get("outputs"):
            for candidate in ordered:
                if candidate.get("line_start", 0) <= line_start:
                    continue
                candidate_name = candidate.get("stage")
                candidate_kind = candidate.get("stage_kind")
                candidate_branch = branch_by_stage.get(candidate_name)
                if branch and candidate_branch and candidate_branch != branch:
                    continue
                if candidate_kind in _ALTERYX_SOURCE_KINDS:
                    continue
                edge_counter = _alteryx_connect(stages_by_name, stage_name, candidate_name, edge_counter)
                break

        if stage.get("outputs") and not stage.get("inputs"):
            for candidate in reversed(ordered):
                if candidate.get("line_start", 0) >= line_start:
                    continue
                candidate_name = candidate.get("stage")
                candidate_kind = candidate.get("stage_kind")
                candidate_branch = branch_by_stage.get(candidate_name)
                if branch and candidate_branch and candidate_branch != branch:
                    continue
                if candidate_kind in _ALTERYX_SINK_KINDS:
                    continue
                edge_counter = _alteryx_connect(stages_by_name, candidate_name, stage_name, edge_counter)
                break

        if not stage.get("inputs") and not stage.get("outputs"):
            predecessor = None
            successor = None

            for candidate in reversed(ordered):
                if candidate.get("line_start", 0) >= line_start:
                    continue
                candidate_name = candidate.get("stage")
                candidate_kind = candidate.get("stage_kind")
                candidate_branch = branch_by_stage.get(candidate_name)
                if branch and candidate_branch and candidate_branch != branch:
                    continue
                if candidate_kind in _ALTERYX_SINK_KINDS:
                    continue
                predecessor = candidate_name
                break

            for candidate in ordered:
                if candidate.get("line_start", 0) <= line_start:
                    continue
                candidate_name = candidate.get("stage")
                candidate_kind = candidate.get("stage_kind")
                candidate_branch = branch_by_stage.get(candidate_name)
                if branch and candidate_branch and candidate_branch != branch:
                    continue
                if candidate_kind in _ALTERYX_SOURCE_KINDS:
                    continue
                successor = candidate_name
                break

            if predecessor:
                edge_counter = _alteryx_connect(stages_by_name, predecessor, stage_name, edge_counter)
            if successor:
                edge_counter = _alteryx_connect(stages_by_name, stage_name, successor, edge_counter)

    resolved = list(stages_by_name.values())
    unresolved = _alteryx_generic_candidates(resolved)
    if unresolved:
        resolved = _apply_alteryx_disambiguation_hints(
            resolved,
            _generate_alteryx_disambiguation_hints(resolved, unresolved, job_text),
        )

    return sorted(resolved, key=lambda item: (item.get("line_start", 0), item.get("stage", "")))


def _apply_stage_semantic_hints(node, stage_hint):
    """
    Append validated semantic hints to a node without losing its original label.
    """

    if not stage_hint:
        return node

    details = list(node.get("details", []))

    business_label = stage_hint.get("business_label")
    if business_label:
        details.append(f"Business: {business_label}")

    condition_summary = stage_hint.get("condition_summary")
    if condition_summary:
        details.append(f"Rule: {condition_summary}")

    node["details"] = details
    if stage_hint.get("role"):
        node["type"] = stage_hint["role"]

    return node


def _collapse_semantic_nodes(edges, nodes, removable_types):
    """
    Collapse helper nodes while preserving lineage flow.
    """

    node_type_map = {node["id"]: node["type"] for node in nodes}
    incoming = defaultdict(list)
    outgoing = defaultdict(list)

    for edge in edges:
        incoming[edge["target"]].append(edge)
        outgoing[edge["source"]].append(edge)

    collapsed_edges = []
    removed_nodes = set()

    for node_id, node_type in node_type_map.items():
        if node_type not in removable_types:
            continue

        parents = incoming.get(node_id, [])
        children = outgoing.get(node_id, [])

        if not parents or not children:
            continue

        removed_nodes.add(node_id)

        for parent in parents:
            for child in children:
                edge_kind = "lookup" if node_type == "LOOKUP" else child.get("kind", parent.get("kind", "flow"))
                collapsed_edges.append({
                    "source": parent["source"],
                    "target": child["target"],
                    "kind": edge_kind,
                })

    passthrough_edges = [
        edge for edge in edges
        if edge["source"] not in removed_nodes and edge["target"] not in removed_nodes
    ]

    final_nodes = [node for node in nodes if node["id"] not in removed_nodes]

    return final_nodes, _deduplicate_edges(passthrough_edges + collapsed_edges)


def _build_dataset_graph(stages, tables, options):
    """
    Build a technical graph that keeps intermediate dataset nodes visible.
    """

    nodes = {}
    edges = []
    dataset_aliases = {}
    dataset_consumers = defaultdict(list)
    stage_inbound = defaultdict(int)
    stage_outbound = defaultdict(int)
    table_targets = defaultdict(set)

    for stage in stages:
        stage_name = stage["stage"]

        for output in stage.get("outputs", []):
            dataset_id = output.get("dataset_id")
            alias = output.get("alias") or dataset_id
            if dataset_id:
                dataset_aliases[dataset_id] = alias

        for inp in stage.get("inputs", []):
            dataset_id = inp.get("dataset_id")
            if dataset_id:
                dataset_consumers[dataset_id].append({
                    "stage": stage_name,
                    "kind": _edge_kind_from_input(inp, stage, None),
                })

    for stage in stages:
        stage_name = stage["stage"]
        nodes.setdefault(stage_name, _make_node(stage_name, "TRANSFORM", stage_meta=stage))

        for output in stage.get("outputs", []):
            dataset_id = output.get("dataset_id")
            alias = output.get("alias") or dataset_id
            dataset_node = alias or dataset_id

            if not dataset_id or not dataset_node:
                continue

            nodes.setdefault(dataset_node, _make_node(dataset_node, "DATASET"))
            edges.append({
                "source": stage_name,
                "target": dataset_node,
                "kind": "flow",
            })
            stage_outbound[stage_name] += 1

        for inp in stage.get("inputs", []):
            dataset_id = inp.get("dataset_id")
            dataset_node = dataset_aliases.get(dataset_id) or inp.get("alias") or dataset_id

            if not dataset_id or not dataset_node:
                continue

            nodes.setdefault(dataset_node, _make_node(dataset_node, "DATASET"))
            edges.append({
                "source": dataset_node,
                "target": stage_name,
                "kind": _edge_kind_from_input(inp, stage, None),
            })
            stage_inbound[stage_name] += 1

    for table in tables:
        table_name = table.get("table")
        stage_name = table.get("stage")
        if not table_name or not stage_name:
            continue
        if stage_inbound.get(stage_name, 0) > 0 and not options.get("include_sql_sources"):
            continue
        nodes.setdefault(table_name, _make_node(table_name, "SOURCE"))
        table_targets[table_name].add(stage_name)

    for table_name, stage_names in table_targets.items():
        for stage_name in sorted(stage_names):
            edges.append({
                "source": table_name,
                "target": stage_name,
                "kind": "sql_source",
            })

    for stage in stages:
        stage_name = stage["stage"]
        stage_type = _classify_semantic_stage(
            stage,
            stage_inbound.get(stage_name, 0),
            stage_outbound.get(stage_name, 0),
            used_as_lookup=any(edge["target"] == stage_name and edge["kind"] == "lookup" for edge in edges),
        )
        nodes[stage_name]["type"] = stage_type
        nodes[stage_name]["details"] = _semantic_detail_lines(stage_type, stage)

    return {
        "nodes": list(nodes.values()),
        "edges": _deduplicate_edges(edges),
    }


def _aggregate_parallel_sources(nodes, edges, semantic_hints=None, min_group_size=3):
    """
    Group many SQL source tables feeding the same downstream stage into one source node.
    """

    node_map = {node["id"]: dict(node) for node in nodes}
    incoming = defaultdict(list)
    outgoing = defaultdict(list)

    for edge in edges:
        incoming[edge["target"]].append(edge)
        outgoing[edge["source"]].append(edge)

    groups = defaultdict(list)
    semantic_hints = semantic_hints or {}

    for item in semantic_hints.get("sql_groups", []):
        target_stage = item.get("target_stage")
        for table_name in item.get("tables", []):
            if table_name in node_map:
                groups[target_stage].append(table_name)

    for node_id, node in node_map.items():
        if node.get("type") != "SOURCE":
            continue

        if incoming.get(node_id):
            continue

        source_edges = outgoing.get(node_id, [])
        if not source_edges:
            continue

        kinds = {edge.get("kind") for edge in source_edges}
        targets = {edge.get("target") for edge in source_edges}

        if kinds != {"sql_source"} or len(targets) != 1:
            continue

        if not semantic_hints.get("sql_groups"):
            groups[next(iter(targets))].append(node_id)

    nodes_to_remove = set()
    replacement_edges = []

    for target_id, member_ids in groups.items():
        if len(member_ids) < min_group_size:
            continue

        sorted_members = sorted(member_ids, key=str.lower)
        group_id = f"__source_group__{re.sub(r'[^A-Za-z0-9_]+', '_', target_id).strip('_').lower() or 'target'}"
        explicit_group = next(
            (item for item in semantic_hints.get("sql_groups", []) if item.get("target_stage") == target_id),
            None,
        )
        group_label = (explicit_group or {}).get("group_name") or "Upstream Sources"
        detail_lines = [f"{len(sorted_members)} source tables"] + sorted_members

        node_map[group_id] = _make_node(
            group_id,
            "SOURCE_GROUP",
            label=group_label,
            details=detail_lines,
            members=sorted_members,
        )
        replacement_edges.append({
            "source": group_id,
            "target": target_id,
            "kind": "sql_source",
        })
        nodes_to_remove.update(sorted_members)

    if not nodes_to_remove:
        return nodes, edges

    final_nodes = [
        node for node_id, node in node_map.items()
        if node_id not in nodes_to_remove
    ]
    final_edges = [
        edge for edge in edges
        if edge["source"] not in nodes_to_remove and edge["target"] not in nodes_to_remove
    ]
    final_edges.extend(replacement_edges)

    return final_nodes, _deduplicate_edges(final_edges)


def _build_semantic_graph(stages, stage_edges, tables, options, semantic_hints=None):
    """
    Build a clean stage-level graph for logical lineage diagrams.
    """

    nodes = {}
    edges = []
    stage_map = {stage["stage"]: stage for stage in stages}
    stage_hints = (semantic_hints or {}).get("stages", {})
    inbound_counts = defaultdict(int)
    outbound_counts = defaultdict(int)
    lookup_stages = set()

    for edge in stage_edges:
        source_name = edge.get("source")
        target_name = edge.get("target")

        if not source_name or not target_name:
            continue

        inbound_counts[target_name] += 1
        outbound_counts[source_name] += 1

        if edge.get("kind") == "lookup":
            lookup_stages.add(source_name)

        nodes.setdefault(source_name, _make_node(source_name, "SOURCE", stage_meta=stage_map.get(source_name)))
        nodes.setdefault(target_name, _make_node(target_name, "TRANSFORM", stage_meta=stage_map.get(target_name)))
        edges.append({
            "source": source_name,
            "target": target_name,
            "kind": edge.get("kind", "flow"),
        })

    for stage_name, stage in stage_map.items():
        node_type = _classify_semantic_stage(
            stage,
            inbound_counts.get(stage_name, 0),
            outbound_counts.get(stage_name, 0),
            stage_name in lookup_stages,
        )
        if stage_hints.get(stage_name, {}).get("role"):
            node_type = stage_hints[stage_name]["role"]
        nodes[stage_name] = _apply_stage_semantic_hints(
            _make_node(stage_name, node_type, stage_meta=stage),
            stage_hints.get(stage_name),
        )

    complexity = _lineage_complexity(len(stage_map), len(stage_edges))
    add_sql_sources = options.get("include_sql_sources")
    table_targets = defaultdict(set)

    if add_sql_sources:
        for table in tables:
            table_name = table.get("table")
            stage_name = table.get("stage")

            if not table_name or not stage_name or stage_name not in stage_map:
                continue

            if inbound_counts.get(stage_name, 0) > 0 and not options.get("include_sql_sources"):
                continue

            table_targets[table_name].add(stage_name)

        for table_name, stage_names in table_targets.items():
            nodes[table_name] = _make_node(table_name, "SOURCE")
            for stage_name in sorted(stage_names):
                edges.append({
                    "source": table_name,
                    "target": stage_name,
                    "kind": "sql_source",
                })

    if not options.get("include_lookup_nodes"):
        node_values, edge_values = _collapse_semantic_nodes(
            edges,
            list(nodes.values()),
            {"LOOKUP"},
        )
        nodes = {node["id"]: node for node in node_values}
        edges = edge_values

    if options.get("diagram_mode") == "logical":
        node_values, edge_values = _aggregate_parallel_sources(list(nodes.values()), edges, semantic_hints=semantic_hints)
        nodes = {node["id"]: node for node in node_values}
        edges = edge_values

    return {
        "nodes": list(nodes.values()),
        "edges": _deduplicate_edges(edges),
        "complexity": complexity,
        "sql_sources": sorted(table_targets.keys()),
    }


def _build_nx_graph(node_ids, edges, allowed_kinds=None):
    """
    Build a directed lineage graph using NetworkX.
    """

    graph = nx.DiGraph()
    graph.add_nodes_from(node_ids)
    filtered = []

    for edge in edges:
        if allowed_kinds is not None and edge.get("kind") not in allowed_kinds:
            continue
        source = edge.get("source")
        target = edge.get("target")
        if not source or not target:
            continue
        graph.add_edge(source, target)
        filtered.append(edge)

    return graph, filtered


def _longest_path(node_ids, edges):
    """
    Compute a stable longest-path spine using NetworkX.
    """

    if not node_ids:
        return []

    graph, _ = _build_nx_graph(node_ids, edges)

    if graph.number_of_edges() == 0:
        return sorted(node_ids)[:1]

    if nx.is_directed_acyclic_graph(graph):
        return list(nx.algorithms.dag.dag_longest_path(graph))

    condensed = nx.condensation(graph)
    component_path = list(nx.algorithms.dag.dag_longest_path(condensed))
    component_members = defaultdict(list)

    for node_id, component_id in condensed.graph.get("mapping", {}).items():
        component_members[component_id].append(node_id)

    path = [
        sorted(component_members.get(component_id, []), key=str.lower)[0]
        for component_id in component_path
        if component_members.get(component_id)
    ]

    return path or sorted(node_ids)[:1]


def _dag_layers(node_ids, edges):
    """
    Assign left-to-right ranks using NetworkX longest-path layering.
    """

    graph, _ = _build_nx_graph(node_ids, edges)
    rank = {node_id: 0 for node_id in node_ids}

    if nx.is_directed_acyclic_graph(graph):
        for node_id in nx.topological_sort(graph):
            parent_ranks = [rank[parent] for parent in graph.predecessors(node_id)]
            rank[node_id] = (max(parent_ranks) + 1) if parent_ranks else 0
    else:
        condensed = nx.condensation(graph)
        component_rank = {}
        for component_id in nx.topological_sort(condensed):
            parents = list(condensed.predecessors(component_id))
            component_rank[component_id] = (max(component_rank[parent] for parent in parents) + 1) if parents else 0
        mapping = condensed.graph.get("mapping", {})
        for node_id in node_ids:
            rank[node_id] = component_rank.get(mapping.get(node_id), 0)

    children = {node_id: sorted(graph.successors(node_id)) for node_id in graph.nodes}
    parents = {node_id: sorted(graph.predecessors(node_id)) for node_id in graph.nodes}

    return rank, children, parents


def _graphviz_plain_layout(nodes, edges, sizes, annotations=None):
    """
    Ask Graphviz for node positions and return draw.io-friendly coordinates.
    
    Uses safe identifiers (node_0, node_1, etc.) to avoid parsing issues with
    special characters in node IDs, then maps results back to original IDs.
    """

    if graphviz is None:
        raise RuntimeError("Graphviz Python package is not installed.")

    dot = graphviz.Digraph(engine="dot")
    lane_priority = {
        "source": 0,
        "lookup": 1,
        "support": 2,
        "main": 3,
        "branch": 4,
    }
    node_ids = [node["id"] for node in nodes]
    rank_map, _, parents = _dag_layers(node_ids, edges)
    annotation_map = annotations or {}
    complexity = _lineage_complexity(len(nodes), len(edges))
    ranksep = {
        "small": "1.8",
        "medium": "2.2",
        "large": "2.7",
    }.get(complexity, "2.2")
    nodesep = {
        "small": "0.9",
        "medium": "1.2",
        "large": "1.5",
    }.get(complexity, "1.2")
    dot.attr(
        rankdir="LR",
        ranksep=ranksep,
        nodesep=nodesep,
        splines="ortho",
        pad="0.4",
        margin="0.25",
        concentrate="true",
        outputorder="edgesfirst",
    )
    dot.attr("node", shape="box", fixedsize="false")

    # Create mapping from safe IDs to original IDs
    safe_id_map = {}
    id_to_safe = {}
    
    for index, node in enumerate(nodes):
        original_id = node["id"]
        safe_id = f"node_{index}"
        safe_id_map[safe_id] = original_id
        id_to_safe[original_id] = safe_id
        
        node_size = sizes.get(original_id, {"width": 220, "height": 70})
        dot.node(
            safe_id,
            label=_display_label(node),
            width=f"{node_size['width'] / 72:.3f}",
            height=f"{node_size['height'] / 72:.3f}",
        )

    # Build explicit layered ranks and stable intra-rank ordering to reduce crossings.
    rank_groups = defaultdict(list)
    for node in nodes:
        rank_groups[rank_map.get(node["id"], 0)].append(node["id"])

    def _rank_sort_key(node_id):
        parent_positions = sorted(rank_map.get(parent, 0) for parent in parents.get(node_id, []))
        parent_hint = sum(parent_positions) / len(parent_positions) if parent_positions else -1
        lane = annotation_map.get(node_id, {}).get("lane", "main")
        node_type = next((item.get("type", "") for item in nodes if item["id"] == node_id), "")
        return (
            lane_priority.get(lane, 9),
            parent_hint,
            node_type,
            node_id.lower(),
        )

    for rank, node_group in sorted(rank_groups.items()):
        ordered_ids = sorted(node_group, key=_rank_sort_key)
        with dot.subgraph(name=f"rank_{rank}") as sub:
            sub.attr(rank="same")
            for node_id in ordered_ids:
                sub.node(id_to_safe[node_id])

        # Preserve vertical ordering within a layer using invisible edges.
        for source_id, target_id in zip(ordered_ids, ordered_ids[1:]):
            dot.edge(
                id_to_safe[source_id],
                id_to_safe[target_id],
                style="invis",
                weight="20",
            )

    for edge in edges:
        source_safe_id = id_to_safe.get(edge["source"], edge["source"])
        target_safe_id = id_to_safe.get(edge["target"], edge["target"])
        edge_attrs = {
            "weight": "6",
        }
        if edge.get("kind") in {"lookup", "sql_source"}:
            edge_attrs["weight"] = "2"
            edge_attrs["minlen"] = "1"
        elif edge.get("kind") == "exception":
            edge_attrs["minlen"] = "2"
        else:
            edge_attrs["minlen"] = "2"
        dot.edge(source_safe_id, target_safe_id, **edge_attrs)

    try:
        plain = dot.pipe(format="plain").decode("utf-8")
    except Exception as exc:
        raise RuntimeError(
            "Graphviz layout failed. Ensure the `dot` executable is installed and available on PATH."
        ) from exc

    positions = {}
    graph_height = 0.0

    for line in plain.splitlines():
        if not line.strip():
            continue
            
        parts = line.split()
        if not parts:
            continue

        if parts[0] == "graph" and len(parts) >= 4:
            try:
                graph_height = float(parts[3])
            except (ValueError, IndexError):
                continue
                
        elif parts[0] == "node" and len(parts) >= 6:
            try:
                safe_id = parts[1]
                original_id = safe_id_map.get(safe_id, safe_id)
                
                x_center = float(parts[2]) * 72
                y_center = float(parts[3]) * 72
                width = float(parts[4]) * 72
                height = float(parts[5]) * 72
                
                positions[original_id] = {
                    "x": int(round(x_center - (width / 2))),
                    "y": int(round((graph_height * 72) - y_center - (height / 2))),
                }
            except (ValueError, IndexError) as e:
                raise ValueError(
                    f"Failed to parse Graphviz output line: '{line}'. "
                    f"Parts: {parts}. Error: {e}"
                ) from e

    if not positions:
        raise RuntimeError("Graphviz returned no node positions.")

    return positions


def _spread_missing_or_colliding_positions(nodes, edges, sizes, positions, annotations=None):
    """
    Apply a minimal repair when Graphviz leaves nodes unplaced or stacked.
    """

    row_gap = 160
    column_tolerance = 80
    lane_priority = {
        "source": 0,
        "lookup": 1,
        "support": 2,
        "main": 3,
        "branch": 4,
    }
    annotation_map = annotations or {}
    graph, _ = _build_nx_graph([node["id"] for node in nodes], edges)
    grouped = defaultdict(list)

    for node in nodes:
        node_id = node["id"]
        if node_id in positions:
            column_key = round(positions[node_id]["x"] / column_tolerance)
            grouped[column_key].append(node_id)

    for column_key, node_ids in grouped.items():
        if len(node_ids) <= 1:
            continue

        ordered_ids = sorted(
            node_ids,
            key=lambda item: (
                lane_priority.get(annotation_map.get(item, {}).get("lane", "main"), 9),
                -graph.out_degree(item),
                graph.in_degree(item),
                item.lower(),
            )
        )

        top_y = min(positions[item]["y"] for item in ordered_ids)
        for offset, node_id in enumerate(ordered_ids):
            positions[node_id]["y"] = top_y + (offset * row_gap)

    if len(positions) == len(nodes):
        return positions

    node_ids = [node["id"] for node in nodes]
    rank_map, _, _ = _dag_layers(node_ids, edges)
    x_gap = 340
    base_x = 40
    base_y = 40
    next_y_by_rank = defaultdict(lambda: base_y)

    for node_id in sorted(node_ids, key=lambda item: (rank_map.get(item, 0), item.lower())):
        if node_id in positions:
            rank = rank_map.get(node_id, 0)
            next_y_by_rank[rank] = max(next_y_by_rank[rank], positions[node_id]["y"] + row_gap)
            continue
        rank = rank_map.get(node_id, 0)
        positions[node_id] = {
            "x": base_x + (rank * x_gap),
            "y": next_y_by_rank[rank],
        }
        next_y_by_rank[rank] += row_gap

    return positions


def _generate_layout_single(graph_entry):
    """
    Generate layout for one lineage graph using Graphviz.
    """

    nodes = graph_entry.get("nodes", [])
    edges = graph_entry.get("edges", [])
    options = normalize_lineage_options(graph_entry.get("options", {}))
    annotations = graph_entry.get("annotations", {})
    complexity = graph_entry.get("complexity") or _lineage_complexity(len(nodes), len(edges))
    positions = {}
    sizes = {}
    node_ids = [node["id"] for node in nodes]

    for node_id in node_ids:
        node = next((item for item in nodes if item["id"] == node_id), {"id": node_id})
        width, height = _node_box_size(_display_label(node))
        sizes[node_id] = {"width": width, "height": height}

    positions = _graphviz_plain_layout(nodes, edges, sizes, annotations=annotations)
    positions = _spread_missing_or_colliding_positions(nodes, edges, sizes, positions, annotations=annotations)

    return {
        "source_file": _safe_source_key(graph_entry.get("source_file")),
        "nodes": nodes,
        "positions": positions,
        "sizes": sizes,
        "edges": edges,
        "options": options,
        "annotations": annotations,
        "complexity": complexity,
        "semantic_hints_applied": graph_entry.get("semantic_hints_applied", False),
    }


def _node_box_size(label):
    """
    Compute a width that keeps long labels inside the node box.
    """

    lines = [line for line in str(label or "").splitlines() if line] or [""]
    max_length = max(len(line) for line in lines)
    line_count = len(lines)
    width = max(220, min(420, 160 + (max_length * 5)))
    height = max(70, min(240, 40 + (line_count * 22)))
    return width, height


def _edge_style(kind, route="", complexity="medium"):
    """
    Return draw.io edge style tuned for logical lineage.
    """

    base = "edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;jettySize=auto;orthogonalLoop=1;"

    if route == "vertical":
        return base + "elbow=vertical;"

    if route == "side":
        return base + "elbow=horizontal;"

    if kind == "lookup":
        return base + "elbow=vertical;dashed=1;dashPattern=6 4;"

    if kind == "sql_source":
        return base + "elbow=vertical;dashed=1;dashPattern=2 4;"

    if kind == "exception":
        return base + "elbow=vertical;"

    return base


def _ordered_nodes_for_render(nodes, positions, annotations):
    """
    Order nodes deterministically for cleaner XML output.
    """

    lane_priority = {
        "source": 0,
        "lookup": 1,
        "support": 2,
        "main": 3,
        "branch": 4,
    }

    return sorted(
        nodes,
        key=lambda node: (
            lane_priority.get(annotations.get(node["id"], {}).get("lane", "main"), 9),
            positions.get(node["id"], {}).get("x", 0),
            positions.get(node["id"], {}).get("y", 0),
            node["id"].lower(),
        )
    )


def _edge_waypoints(edge, positions, sizes, annotations, complexity="medium"):
    """
    Create explicit waypoints only for large diagrams that benefit from a bend.
    """

    if complexity == "small":
        return []

    source_pos = positions.get(edge["source"])
    target_pos = positions.get(edge["target"])
    source_size = sizes.get(edge["source"], {"width": 220, "height": 70})
    target_size = sizes.get(edge["target"], {"width": 220, "height": 70})

    if not source_pos or not target_pos:
        return []

    source_center_x = source_pos["x"] + (source_size["width"] / 2)
    source_center_y = source_pos["y"] + (source_size["height"] / 2)
    target_center_x = target_pos["x"] + (target_size["width"] / 2)
    target_center_y = target_pos["y"] + (target_size["height"] / 2)

    if edge.get("kind") in {"lookup", "sql_source", "exception"}:
        bend_y = max(source_center_y, target_center_y) + 40
        return [
            {"x": source_center_x, "y": bend_y},
            {"x": target_center_x, "y": bend_y},
        ]

    return []


def _derive_logical_lineage(nodes, edges, options):
    """
    Derive a logical graph with lightweight annotations and Graphviz-driven layout.
    """

    node_ids = [node["id"] for node in nodes]
    node_types = {node["id"]: node["type"] for node in nodes}
    complexity = _lineage_complexity(len(nodes), len(edges))
    non_lookup_edges = [edge for edge in edges if edge.get("kind") not in {"lookup", "sql_source"}]
    main_path = _longest_path(node_ids, non_lookup_edges)
    if len(main_path) < 2:
        main_path = _longest_path(node_ids, edges)
    if not main_path:
        main_path = sorted(node_ids)

    main_set = set(main_path)
    graph, all_edges = _build_nx_graph(node_ids, edges)

    annotations = {}

    for node in nodes:
        node_id = node["id"]
        node_type = node_types.get(node_id, "DATASET")
        is_main = node_id in main_set
        has_incoming = graph.in_degree(node_id) > 0
        has_outgoing = graph.out_degree(node_id) > 0
        lane = "support"
        role = "main" if is_main else "support"

        if node_type in {"SOURCE", "SOURCE_GROUP", "EXTRACT"}:
            lane = "source"
        elif node_type in {"LOOKUP", "STORE"}:
            lane = "lookup"
        elif node_type in {"FILE_TARGET", "DB_TARGET", "TARGET", "EXCEPTION"} and has_incoming and not is_main:
            lane = "branch"
            role = "branch"
        elif is_main:
            lane = "main"
        elif has_incoming and not has_outgoing:
            lane = "branch"
            role = "branch"

        annotations[node_id] = {
            "role": role,
            "lane": lane,
        }

    logical_edges = []

    for edge in all_edges:
        logical_edges.append({
            "source": edge["source"],
            "target": edge["target"],
            "kind": edge.get("kind", "input"),
        })

    return {
        "nodes": nodes,
        "edges": _deduplicate_edges(logical_edges),
        "main_path": main_path,
        "annotations": annotations,
        "options": options,
        "complexity": complexity,
    }


def _derive_technical_lineage(nodes, edges, options):
    """
    Preserve the technical dataset-level graph and add only lightweight render annotations.
    """

    node_ids = [node["id"] for node in nodes]
    node_types = {node["id"]: node["type"] for node in nodes}
    complexity = _lineage_complexity(len(nodes), len(edges))
    non_lookup_edges = [edge for edge in edges if edge.get("kind") not in {"lookup", "sql_source"}]
    main_path = _longest_path(node_ids, non_lookup_edges)
    if not main_path:
        main_path = _longest_path(node_ids, edges)
    if not main_path:
        main_path = sorted(node_ids)

    main_set = set(main_path)
    annotations = {}

    for node in nodes:
        node_id = node["id"]
        node_type = node_types.get(node_id, "DATASET")

        if node_type == "SOURCE":
            lane = "source"
            role = "support"
        elif node_type in {"LOOKUP", "STORE"}:
            lane = "lookup"
            role = "support"
        elif node_type in {"FILE_TARGET", "DB_TARGET", "TARGET", "EXCEPTION"}:
            lane = "branch"
            role = "branch"
        elif node_id in main_set:
            lane = "main"
            role = "main"
        else:
            lane = "support"
            role = "support"

        annotations[node_id] = {
            "role": role,
            "lane": lane,
        }

    return {
        "nodes": nodes,
        "edges": _deduplicate_edges(edges),
        "main_path": main_path,
        "annotations": annotations,
        "options": options,
        "complexity": complexity,
    }


def _style_for_node_type(node_type):
    """
    Return draw.io style per node category.
    """

    base = "whiteSpace=wrap;html=1;strokeColor=#666666;"

    styles = {
        "SOURCE": base + "shape=cylinder3;boundedLbl=1;size=15;fillColor=#fff2cc;",
        "SOURCE_GROUP": base + "shape=cylinder3;boundedLbl=1;size=18;fillColor=#fff2cc;",
        "EXTRACT": base + "rounded=1;fillColor=#f8cecc;",
        "TRANSFORM": base + "rounded=1;fillColor=#f8cecc;",
        "DECISION": base + "shape=rhombus;perimeter=rhombusPerimeter;fillColor=#f8cecc;",
        "DATASET": base + "shape=document;boundedLbl=1;fillColor=#dae8fc;",
        "LOOKUP": base + "shape=document;boundedLbl=1;fillColor=#dae8fc;",
        "STORE": base + "shape=document;boundedLbl=1;fillColor=#dae8fc;",
        "DB_TARGET": base + "shape=cylinder3;boundedLbl=1;size=15;fillColor=#d5e8d4;",
        "FILE_TARGET": base + "shape=document;boundedLbl=1;fillColor=#d5e8d4;",
        "TARGET": base + "shape=document;boundedLbl=1;fillColor=#d5e8d4;",
        "EXCEPTION": base + "shape=document;boundedLbl=1;fillColor=#ffe6cc;",
    }

    return styles.get(node_type, base + "rounded=1;fillColor=#f5f5f5;")


# -----------------------------------------------------
# State Tracking
# -----------------------------------------------------

def save_lineage_state(session_id, user_id, state):
    """
    Save lineage pipeline state for debugging and recovery.

    State is merged with existing state instead of overwriting.
    """

    state_dir = os.path.join(
        OUTPUT_BASE_PATH,
        "lineage_state",
        user_id
    )

    os.makedirs(state_dir, exist_ok=True)

    state_path = os.path.join(state_dir, f"{session_id}.json")

    existing = {}

    if os.path.exists(state_path):
        existing = read_json(state_path)

    existing.update(state)

    write_json(state_path, existing)


def load_lineage_state(user_id, session_id):
    """
    Load lineage state for a given session.
    """

    state_dir = os.path.join(
        OUTPUT_BASE_PATH,
        "lineage_state",
        user_id
    )

    state_path = os.path.join(state_dir, f"{session_id}.json")

    if os.path.exists(state_path):
        return read_json(state_path)

    return {}


def get_previous_state(user_id, key):
    """
    Retrieve latest value for a specific key from lineage state files.
    Used by router when pipeline tools skip inputs.
    """

    state_dir = os.path.join(
        OUTPUT_BASE_PATH,
        "lineage_state",
        user_id
    )

    if not os.path.exists(state_dir):
        return None

    files = sorted(os.listdir(state_dir), reverse=True)

    for f in files:

        state = read_json(os.path.join(state_dir, f))

        if key in state:
            return state[key]

    return None


# -----------------------------------------------------
# File Utilities
# -----------------------------------------------------

def get_or_create_file(agent_name: str, tool_name: str):
    """
    Create a log file for the tool if it doesn't exist.

    Returns the path to the log file.
    """

    log_dir = os.path.join(
        OUTPUT_BASE_PATH,
        "logs",
        agent_name
    )

    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f"{tool_name}.log")

    if not os.path.exists(log_file):
        with open(log_file, "w") as f:
            f.write("")

    return log_file


def list_files_in_directory(directory_path: str):
    """
    Safely list files in a directory.

    Returns only file names (not directories).
    """

    if not directory_path:
        return []

    if not os.path.exists(directory_path):
        return []

    files = []

    for f in os.listdir(directory_path):

        full_path = os.path.join(directory_path, f)

        if os.path.isfile(full_path):
            files.append(f)

    return files


def get_lineage_upload_dir(user_id: str, category: str) -> str:
    """
    Build a lineage upload directory for a user and category.
    """

    return os.path.join(UPLOAD_BASE_PATH, "lineage", user_id, category)


def get_lineage_output_dir(user_id: str, category: str) -> str:
    """
    Build a lineage output directory for a user and category.
    """

    return os.path.join(OUTPUT_BASE_PATH, "lineage", user_id, category)


def resolve_lineage_artifact(user_id: str, category: str, filename: str, base_type: str = "upload"):
    """
    Resolve a lineage artifact filename to an absolute path.
    """

    if not filename:
        return None

    if os.path.isabs(filename):
        return filename

    base_dir = (
        get_lineage_upload_dir(user_id, category)
        if base_type == "upload"
        else get_lineage_output_dir(user_id, category)
    )

    return os.path.join(base_dir, filename)


# -----------------------------------------------------
# Read ETL Sources
# -----------------------------------------------------

def read_etl_sources(job_files, output_path):
    """
    Read ETL pseudocode files and store contents into JSON.
    """

    contents = []

    for file in job_files:

        if not os.path.exists(file):
            continue

        with open(file, "r", encoding="utf-8") as f:

            contents.append({
                "file": os.path.basename(file),
                "content": f.read()
            })

    write_json(output_path, contents)

    return output_path


# -----------------------------------------------------
# Job Boundary Detection
# -----------------------------------------------------

def detect_job_boundaries(source_file, output_path):
    """
    Detect ETL job boundaries and parser family inside pseudocode.
    """

    source_file = require_existing_file(source_file, "lineage source file")
    data = read_json(source_file)

    jobs = []

    for entry in data:
        source_name = _safe_source_key(entry.get("file"))
        text = entry.get("content", "")
        parser_type = _detect_pseudocode_type(text)
        jobs.extend(_split_jobs_for_parser(source_name, text, parser_type))

    write_json(output_path, jobs)

    return output_path


# -----------------------------------------------------
# Parse ETL Stages
# -----------------------------------------------------
def parse_stages_chunked(job_file, output_path):
    """
    Extract stage metadata from pseudocode jobs via tool-specific parsers.
    """

    job_file = require_existing_file(job_file, "job boundaries file")
    jobs = read_json(job_file)

    normalized_jobs = []
    for job in jobs:
        if isinstance(job, dict):
            normalized_jobs.append({
                "source_file": _safe_source_key(job.get("source_file")),
                "job_index": job.get("job_index", 1),
                "parser_type": job.get("parser_type") or _detect_pseudocode_type(job.get("content", "")),
                "content": job.get("content", ""),
            })
        else:
            normalized_jobs.append({
                "source_file": "lineage",
                "job_index": 1,
                "parser_type": _detect_pseudocode_type(job),
                "content": str(job),
            })

    jobs_by_type = defaultdict(list)
    for job in normalized_jobs:
        jobs_by_type[job["parser_type"]].append(job)

    stages = []
    if jobs_by_type.get("datastage"):
        stages.extend(parse_datastage_jobs(jobs_by_type["datastage"]))
    if jobs_by_type.get("informatica"):
        stages.extend(parse_informatica_jobs(jobs_by_type["informatica"]))
    if jobs_by_type.get("powercenter"):
        # Backward compatibility for job-boundary artifacts created before the Informatica rename.
        stages.extend(parse_informatica_jobs(jobs_by_type["powercenter"]))
    if jobs_by_type.get("alteryx"):
        for job in jobs_by_type["alteryx"]:
            alteryx_stages = parse_alteryx_jobs([job])
            stages.extend(_resolve_alteryx_ambiguous_steps(alteryx_stages, job.get("content", "")))

    write_json(output_path, stages)

    return output_path


# -----------------------------------------------------
# Dataset Link Extraction
# -----------------------------------------------------

def extract_dataset_links(parsed_stage_file, output_path):
    """
    Build dataset flow relationships between ETL stages.
    Supports multiple dataset producers.
    """

    parsed_stage_file = require_existing_file(parsed_stage_file, "parsed stage file")
    stages = read_json(parsed_stage_file)

    stages_by_source = defaultdict(list)

    for stage in stages:
        stages_by_source[_safe_source_key(stage.get("source_file"))].append(stage)

    graph_entries = []

    for source_file, source_stages in sorted(stages_by_source.items()):
        edges = []
        producers = {}
        stage_map = {stage["stage"]: stage for stage in source_stages}

        for stage in source_stages:
            for output in stage.get("outputs", []):
                dataset_id = output.get("dataset_id")

                if not dataset_id:
                    continue

                producers.setdefault(dataset_id, []).append({
                    "stage": stage["stage"],
                    "alias": output.get("alias") or stage["stage"],
                })

        for stage in source_stages:
            for inp in stage.get("inputs", []):
                dataset_id = inp.get("dataset_id")

                if not dataset_id:
                    continue

                producer_list = producers.get(dataset_id, [])

                for producer in producer_list:
                    source_stage = stage_map.get(producer["stage"])

                    edges.append({
                        "source": producer["stage"],
                        "target": stage["stage"],
                        "kind": _edge_kind_from_input(
                            inp,
                            stage_map.get(stage["stage"]),
                            source_stage
                        ),
                        "dataset_id": dataset_id,
                        "link_name": inp.get("link_name", "")
                    })

        graph_entries.append({
            "source_file": source_file,
            "edges": _deduplicate_edges(edges),
            "stages": source_stages,
        })

    payload = _graphs_payload(graph_entries)

    write_json(output_path, payload)

    return output_path


# -----------------------------------------------------
# SQL Metadata Extraction
# -----------------------------------------------------

def extract_sql_metadata(parsed_stage_file, output_path):
    """
    Extract SQL tables using sql-metadata parser with fallback.
    """

    parsed_stage_file = require_existing_file(parsed_stage_file, "parsed stage file")
    stages = read_json(parsed_stage_file)

    tables = []
    seen = set()

    for stage in stages:
        source_file = _safe_source_key(stage.get("source_file"))

        for sql in stage.get("sql", []):

            extracted = []

            if Parser is not None:

                try:
                    parser = Parser(sql)
                    extracted = parser.tables
                except Exception:
                    extracted = []

            # fallback regex
            if not extracted:
                extracted = re.findall(
                    r'(?:FROM|JOIN)\s+([A-Za-z0-9_.]+)',
                    sql,
                    re.I
                )

            for table in extracted:

                key = (source_file, table.lower(), stage["stage"])

                if key in seen:
                    continue

                seen.add(key)

                tables.append({
                    "source_file": source_file,
                    "table": table,
                    "stage": stage["stage"]
                })

    write_json(output_path, tables)

    return output_path


# -----------------------------------------------------
# Normalize Lineage Graph
# -----------------------------------------------------

def normalize_lineage_graph(dataset_file, sql_file, output_path, options=None):
    """
    Convert raw dataset links and SQL tables into normalized graph.
    """

    dataset_file = require_existing_file(dataset_file, "dataset links file")
    sql_file = require_existing_file(sql_file, "sql metadata file")
    options = normalize_lineage_options(options)

    dataset_payload = read_json(dataset_file)
    tables = read_json(sql_file)

    if _is_graphs_payload(dataset_payload):
        dataset_graphs = _payload_graphs(dataset_payload)
    elif isinstance(dataset_payload, dict):
        dataset_graphs = [{
            "source_file": "lineage",
            "edges": dataset_payload.get("edges", []),
            "stages": dataset_payload.get("stages", []),
        }]
    else:
        dataset_graphs = [{
            "source_file": "lineage",
            "edges": dataset_payload,
            "stages": [],
        }]

    tables_by_source = defaultdict(list)
    for table in tables:
        tables_by_source[_safe_source_key(table.get("source_file"))].append(table)

    normalized_graphs = []

    for graph_entry in dataset_graphs:
        source_file = _safe_source_key(graph_entry.get("source_file"))
        edges = graph_entry.get("edges", [])
        stages = graph_entry.get("stages", [])
        source_tables = tables_by_source.get(source_file, [])
        semantic_hints = {}

        if _should_use_llm_semantics(options):
            semantic_hints = _validate_llm_semantic_hints(
                _generate_llm_semantic_hints(stages, edges, source_tables, options),
                stages,
                source_tables,
                options,
            )

        if options.get("diagram_mode") == "technical" and not options.get("collapse_intermediate_datasets"):
            graph_payload = _build_dataset_graph(stages, source_tables, options)
            complexity = _lineage_complexity(
                len(graph_payload.get("nodes", [])),
                len(graph_payload.get("edges", [])),
            )
            sql_sources = [table.get("table") for table in source_tables]
        else:
            graph_payload = _build_semantic_graph(stages, edges, source_tables, options, semantic_hints=semantic_hints)
            complexity = graph_payload.get("complexity", _lineage_complexity(len(stages), len(edges)))
            sql_sources = graph_payload.get("sql_sources", [])

        normalized_graphs.append({
            "source_file": source_file,
            "nodes": graph_payload.get("nodes", []),
            "edges": graph_payload.get("edges", []),
            "sql_sources": sql_sources,
            "options": options,
            "complexity": complexity,
            "semantic_hints_applied": bool(semantic_hints),
        })

    normalized = _graphs_payload(normalized_graphs)

    write_json(output_path, normalized)

    return output_path


# -----------------------------------------------------
# Build Lineage Graph
# -----------------------------------------------------

def build_lineage_graph_bottom_up(graph_file, output_path):
    """
    Build lineage graph paths from normalized graph.
    """

    graph_file = require_existing_file(graph_file, "normalized graph file")
    graph = read_json(graph_file)

    graph_entries = _payload_graphs(graph) if _is_graphs_payload(graph) else [graph]
    lineage_entries = []

    for graph_entry in graph_entries:
        options = normalize_lineage_options(graph_entry.get("options", {}))

        if options.get("diagram_mode") == "technical" and not options.get("collapse_intermediate_datasets"):
            rendered_graph = _derive_technical_lineage(
                graph_entry.get("nodes", []),
                graph_entry.get("edges", []),
                options,
            )
        else:
            rendered_graph = _derive_logical_lineage(
                graph_entry.get("nodes", []),
                graph_entry.get("edges", []),
                options,
            )

        lineage_entries.append({
            "source_file": _safe_source_key(graph_entry.get("source_file")),
            "paths": [rendered_graph.get("main_path", [])] if rendered_graph.get("main_path") else [],
            "edges": rendered_graph.get("edges", []),
            "nodes": rendered_graph.get("nodes", []),
            "annotations": rendered_graph.get("annotations", {}),
            "sql_sources": graph_entry.get("sql_sources", []),
            "options": graph_entry.get("options", {}),
            "complexity": rendered_graph.get("complexity", graph_entry.get("complexity")),
            "semantic_hints_applied": graph_entry.get("semantic_hints_applied", False),
        })

    lineage = _graphs_payload(lineage_entries)

    write_json(output_path, lineage)

    return output_path


# -----------------------------------------------------
# Layout Generation
# -----------------------------------------------------

def generate_layout(graph_file, output_path):
    """
    Generate node layout coordinates for visualization.
    """

    graph_file = require_existing_file(graph_file, "lineage graph file")
    graph = read_json(graph_file)

    graph_entries = _payload_graphs(graph) if _is_graphs_payload(graph) else [graph]
    layout = _graphs_payload([
        _generate_layout_single(graph_entry)
        for graph_entry in graph_entries
    ])

    write_json(output_path, layout)

    return output_path


# -----------------------------------------------------
# Draw.io XML Generation
# -----------------------------------------------------

def generate_drawio_xml(layout_file):
    """
    Convert the finalized lineage layout JSON into draw.io XML format.
    """

    layout_file = require_existing_file(layout_file, "layout file")
    layout = read_json(layout_file)

    layout_entries = _payload_graphs(layout) if _is_graphs_payload(layout) else [layout]
    diagrams = []

    for layout_entry in layout_entries:
        nodes = layout_entry.get("nodes")
        pos = layout_entry.get("positions")
        sizes = layout_entry.get("sizes", {})
        edges = layout_entry.get("edges")
        annotations = layout_entry.get("annotations", {})
        complexity = layout_entry.get("complexity", "medium")

        if nodes is None or pos is None or edges is None:
            raise ValueError(f"Invalid layout file format: {layout_file}")

        xml = []

        xml.append('<mxfile host="app.diagrams.net" version="29.6.1">')
        xml.append('  <diagram name="Logical ETL Lineage" id="0">')
        xml.append('    <mxGraphModel dx="1895" dy="958" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="827" pageHeight="1169" math="0" shadow="0">')
        xml.append("      <root>")
        xml.append('        <mxCell id="0"/>')
        xml.append('        <mxCell id="1" parent="0"/>')
        xml.append("")
        xml.append("        <!-- Nodes -->")

        id_map = {}
        ordered_nodes = _ordered_nodes_for_render(nodes, pos, annotations)

        for index, node in enumerate(ordered_nodes, start=1):
            node_id = f"n{index}"
            id_map[node["id"]] = node_id

            geometry = pos.get(node["id"], {"x": 0, "y": 0})
            render_label = _display_label(node)
            label = escape(render_label, {'"': "&quot;"})
            style = _style_for_node_type(node.get("type")).replace(
                "whiteSpace=wrap;html=1;",
                "whiteSpace=wrap;html=1;overflow=hidden;align=center;verticalAlign=middle;spacing=6;"
            )
            node_size = sizes.get(node["id"], {})
            width = node_size.get("width") or _node_box_size(render_label)[0]
            height = node_size.get("height") or _node_box_size(render_label)[1]

            xml.append(
                f'        <mxCell id="{node_id}" value="{label}" '
                f'style="{style}" vertex="1" parent="1">'
            )
            xml.append(
                f'          <mxGeometry x="{geometry["x"]}" y="{geometry["y"]}" width="{width}" height="{height}" as="geometry"/>'
            )
            xml.append("        </mxCell>")

        xml.append("")
        xml.append("        <!-- Edges -->")

        for index, edge in enumerate(edges, start=1):
            source_id = id_map.get(edge["source"], "")
            target_id = id_map.get(edge["target"], "")
            value = ""
            route = edge.get("route", "")
            style = _edge_style(edge.get("kind", ""), route, complexity)
            waypoints = _edge_waypoints(edge, pos, sizes, annotations, complexity)

            xml.append(
                f'        <mxCell id="e{index}" edge="1" parent="1" '
                f'source="{source_id}" target="{target_id}" value="{value}" style="{style}">'
            )
            if waypoints:
                xml.append('          <mxGeometry relative="1" as="geometry">')
                xml.append('            <Array as="points">')
                for point in waypoints:
                    xml.append(
                        f'              <mxPoint x="{point["x"]}" y="{point["y"]}"/>'
                    )
                xml.append('            </Array>')
                xml.append('          </mxGeometry>')
            else:
                xml.append('          <mxGeometry relative="1" as="geometry"/>')
            xml.append("        </mxCell>")

        xml.append("      </root>")
        xml.append("    </mxGraphModel>")
        xml.append("  </diagram>")
        xml.append("</mxfile>")

        diagrams.append({
            "source_file": _safe_source_key(layout_entry.get("source_file")),
            "drawio_xml": "\n".join(xml),
        })

    if len(diagrams) == 1:
        return diagrams[0]["drawio_xml"]

    return _json_string({"diagrams": diagrams})


# -----------------------------------------------------
# Store XML
# -----------------------------------------------------

def store_lineage_xml(xml, directory, filename):
    """
    Store generated draw.io XML lineage diagram.
    """

    if filename is None:
        filename = f"lineage_{uuid.uuid4().hex}.drawio"

    os.makedirs(directory, exist_ok=True)

    path = os.path.join(directory, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(xml)

    return path
