import os
import json
import uuid
import re
from collections import defaultdict, deque
from pathlib import Path
from dotenv import load_dotenv
from xml.sax.saxutils import escape

try:
    from sql_metadata import Parser
except ImportError:
    Parser = None

load_dotenv()

SERVICE_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = str((SERVICE_ROOT / "data").resolve())
UPLOAD_BASE_PATH = str((SERVICE_ROOT / "data" / "uploads").resolve())
OUTPUT_BASE_PATH = str((SERVICE_ROOT / "data" / "output").resolve())
LOG_BASE_PATH = str((SERVICE_ROOT / "data" / "logs").resolve())


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


def _canonical_stage_block(block):
    """
    Restore the stage block prefix after regex splitting.
    """

    block = block.strip()

    if not block:
        return ""

    if block.startswith("// --- ["):
        return block

    return f"// --- [{block}"


def _extract_stage_header(block):
    """
    Parse stage header metadata from a pseudocode block.
    """

    match = re.search(
        r'// --- \[(.*?)\s*:\s*(.*?)\]\s*\[Lines\s*(\d+)-(\d+)\]\s*---',
        block
    )

    if not match:
        return None

    return {
        "kind": match.group(1).strip(),
        "stage": match.group(2).strip(),
        "line_start": int(match.group(3)),
        "line_end": int(match.group(4)),
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


def _stage_lookup_hint(stage_name, stage_type, stage_kind, edge_kind):
    """
    Detect lookup-style stages.
    """

    if edge_kind == "lookup":
        return True

    stage_name = (stage_name or "").lower()
    stage_type = (stage_type or "").lower()
    stage_kind = (stage_kind or "").lower()

    return "lookup" in stage_name or "hashedfile" in stage_type or "hashedfile" in stage_kind


def _classify_node(node_name, stage_meta, inbound_kinds, outbound_kinds):
    """
    Assign a visual category to a lineage node.
    """

    if not stage_meta:
        return "DATASET"

    stage_name = stage_meta.get("stage", "")
    stage_type = (stage_meta.get("stage_type") or "").lower()
    stage_kind = (stage_meta.get("stage_kind") or "").lower()

    if "exception" in stage_name.lower():
        return "EXCEPTION"

    if _stage_lookup_hint(stage_name, stage_type, stage_kind, "lookup" if "lookup" in outbound_kinds else ""):
        if stage_meta.get("used_as_lookup"):
            return "LOOKUP"

    if "transformer" in stage_type or "transformer" in stage_kind:
        return "TRANSFORM"

    if "oracleconnector" in stage_type:
        return "SOURCE" if not inbound_kinds else "TARGET"

    if "seqfile" in stage_type or "seqfile" in stage_kind:
        return "TARGET"

    if "hashedfile" in stage_type or "hashedfile" in stage_kind:
        return "LOOKUP" if stage_meta.get("used_as_lookup") else "DATASET"

    return "TRANSFORM"


def _edge_kind_from_input(entry):
    """
    Classify relationship label from an input link.
    """

    alias = (entry.get("alias") or "").lower()
    link_name = (entry.get("link_name") or "").lower()

    if "exception" in alias or "exception" in link_name:
        return "exception"

    if link_name.startswith("lk") or "lookup" in alias or "lookup" in link_name:
        return "lookup"

    return "input"


def _style_for_node_type(node_type):
    """
    Return draw.io style per node category.
    """

    base = "rounded=1;whiteSpace=wrap;html=1;strokeColor=#666666;"

    styles = {
        "SOURCE": base + "fillColor=#fff2cc;",
        "TRANSFORM": base + "fillColor=#f8cecc;",
        "DATASET": base + "fillColor=#dae8fc;",
        "LOOKUP": base + "fillColor=#dae8fc;",
        "TARGET": base + "fillColor=#d5e8d4;",
        "EXCEPTION": base + "fillColor=#ffe6cc;",
    }

    return styles.get(node_type, base + "fillColor=#f5f5f5;")


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
    Detect independent ETL pipelines inside pseudocode.
    """

    source_file = require_existing_file(source_file, "lineage source file")
    data = read_json(source_file)

    jobs = []

    for entry in data:

        text = entry.get("content", "")
        text = _normalize_arrow_text(text)

        segments = re.split(r'//\s*=+', text)

        for seg in segments:

            if len(seg.strip()) > 50:
                jobs.append(seg.strip())

    write_json(output_path, jobs)

    return output_path


# -----------------------------------------------------
# Parse ETL Stages
# -----------------------------------------------------

def parse_stages_chunked(job_file, output_path):
    """
    Extract stage metadata from pseudocode jobs.
    """

    job_file = require_existing_file(job_file, "job boundaries file")
    jobs = read_json(job_file)

    stages = []

    for job in jobs:

        job = _normalize_arrow_text(job)
        stage_blocks = re.split(r'// --- \[', job)

        for block in stage_blocks:

            normalized_block = _canonical_stage_block(block)

            if not normalized_block:
                continue

            header = _extract_stage_header(normalized_block)

            if not header:
                continue

            inputs = _extract_link_entries(normalized_block, "Input")
            outputs = _extract_link_entries(normalized_block, "Output")
            sql = _extract_sql_block(normalized_block)

            stages.append({
                "stage": header["stage"],
                "type": "TRANSFORM",
                "stage_kind": header["kind"],
                "stage_type": _extract_stage_type(normalized_block),
                "line_start": header["line_start"],
                "line_end": header["line_end"],
                "inputs": inputs,
                "outputs": outputs,
                "sql": [sql] if sql else []
            })

    write_json(output_path, stages)

    return output_path


# -----------------------------------------------------
# Dataset Link Extraction
# -----------------------------------------------------

def extract_dataset_links(parsed_stage_file, output_path):
    """
    Build dataset flow relationships between ETL stages.
    """

    parsed_stage_file = require_existing_file(parsed_stage_file, "parsed stage file")
    stages = read_json(parsed_stage_file)

    edges = []
    producers = {}

    for stage in stages:

        for output in stage.get("outputs", []):
            dataset_id = output.get("dataset_id")

            if not dataset_id:
                continue

            producers[dataset_id] = {
                "stage": stage["stage"],
                "alias": output.get("alias") or stage["stage"],
            }

    for stage in stages:

        for inp in stage.get("inputs", []):
            dataset_id = inp.get("dataset_id")
            producer = producers.get(dataset_id)

            source_name = (
                producer.get("alias")
                if producer
                else inp.get("alias")
            )

            if not source_name:
                continue

            edges.append({
                "source": source_name,
                "target": stage["stage"],
                "kind": _edge_kind_from_input(inp),
                "dataset_id": dataset_id,
                "link_name": inp.get("link_name", "")
            })

    payload = {
        "edges": _deduplicate_edges(edges),
        "stages": stages
    }

    write_json(output_path, payload)

    return output_path


# -----------------------------------------------------
# SQL Metadata Extraction
# -----------------------------------------------------

def extract_sql_metadata(parsed_stage_file, output_path):
    """
    Extract SQL tables using sql-metadata parser.
    """

    parsed_stage_file = require_existing_file(parsed_stage_file, "parsed stage file")
    stages = read_json(parsed_stage_file)

    tables = []
    seen = set()

    if Parser is None:
        write_json(output_path, tables)
        return output_path

    for stage in stages:

        for sql in stage.get("sql", []):

            try:

                parser = Parser(sql)

                for table in parser.tables:
                    key = (table.lower(), stage["stage"])

                    if key in seen:
                        continue

                    seen.add(key)

                    tables.append({
                        "table": table,
                        "stage": stage["stage"]
                    })

            except Exception:
                continue

    write_json(output_path, tables)

    return output_path


# -----------------------------------------------------
# Normalize Lineage Graph
# -----------------------------------------------------

def normalize_lineage_graph(dataset_file, sql_file, output_path):
    """
    Convert raw dataset links and SQL tables into normalized graph.
    """

    dataset_file = require_existing_file(dataset_file, "dataset links file")
    sql_file = require_existing_file(sql_file, "sql metadata file")

    dataset_payload = read_json(dataset_file)
    tables = read_json(sql_file)

    if isinstance(dataset_payload, dict):
        edges = dataset_payload.get("edges", [])
        stages = dataset_payload.get("stages", [])
    else:
        edges = dataset_payload
        stages = []

    stage_map = {stage["stage"]: stage for stage in stages}
    inbound_kinds = defaultdict(set)
    outbound_kinds = defaultdict(set)
    lookup_sources = set()

    for edge in edges:
        inbound_kinds[edge["target"]].add(edge.get("kind", ""))
        outbound_kinds[edge["source"]].add(edge.get("kind", ""))

        if edge.get("kind") == "lookup":
            lookup_sources.add(edge["source"])

    nodes = {}

    for edge in edges:

        for node_name in (edge["source"], edge["target"]):
            if node_name in nodes:
                continue

            stage_meta = dict(stage_map.get(node_name, {}))

            if stage_meta:
                stage_meta["used_as_lookup"] = node_name in lookup_sources

            nodes[node_name] = _classify_node(
                node_name,
                stage_meta,
                inbound_kinds.get(node_name, set()),
                outbound_kinds.get(node_name, set())
            )

    normalized = {
        "nodes": [{"id": k, "type": v} for k, v in nodes.items()],
        "edges": edges,
        "sql_sources": tables
    }

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

    edges = graph.get("edges", [])
    nodes = graph.get("nodes", [])

    adjacency = defaultdict(list)
    indegree = {node["id"]: 0 for node in nodes}

    for edge in edges:

        adjacency[edge["source"]].append(edge["target"])
        indegree[edge["target"]] = indegree.get(edge["target"], 0) + 1
        indegree.setdefault(edge["source"], 0)

    queue = deque(sorted(node_id for node_id, degree in indegree.items() if degree == 0))
    ordered = []

    while queue:
        node_id = queue.popleft()
        ordered.append(node_id)

        for child in sorted(adjacency.get(node_id, [])):
            indegree[child] -= 1

            if indegree[child] == 0:
                queue.append(child)

    lineage = {
        "paths": [ordered] if ordered else [],
        "edges": edges,
        "nodes": nodes,
        "sql_sources": graph.get("sql_sources", [])
    }

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

    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    positions = {}
    node_types = {node["id"]: node["type"] for node in nodes}
    children = defaultdict(list)
    indegree = {node["id"]: 0 for node in nodes}

    for edge in edges:

        children[edge["source"]].append(edge["target"])
        indegree[edge["target"]] = indegree.get(edge["target"], 0) + 1
        indegree.setdefault(edge["source"], 0)

    queue = deque(sorted(node_id for node_id, degree in indegree.items() if degree == 0))
    levels = {node_id: 0 for node_id in queue}

    while queue:
        node_id = queue.popleft()

        for child in children.get(node_id, []):
            levels[child] = max(levels.get(child, 0), levels.get(node_id, 0) + 1)
            indegree[child] -= 1

            if indegree[child] == 0:
                queue.append(child)

    for node in nodes:
        levels.setdefault(node["id"], 0)

    type_priority = {
        "SOURCE": 0,
        "LOOKUP": 1,
        "TRANSFORM": 2,
        "DATASET": 3,
        "TARGET": 4,
        "EXCEPTION": 5
    }

    layer_map = defaultdict(list)

    for node in nodes:
        layer_map[levels[node["id"]]].append(node["id"])

    for layer, layer_nodes in layer_map.items():
        layer_nodes.sort(key=lambda node_id: (type_priority.get(node_types.get(node_id), 99), node_id.lower()))

        for index, node_id in enumerate(layer_nodes):
            positions[node_id] = {
                "x": 50 + (layer * 300),
                "y": 120 + (index * 180)
            }

    layout = {
        "nodes": nodes,
        "positions": positions,
        "edges": edges
    }

    write_json(output_path, layout)

    return output_path


# -----------------------------------------------------
# Draw.io XML Generation
# -----------------------------------------------------

def generate_drawio_xml(layout_file):
    """
    Convert lineage layout JSON into draw.io XML format.
    """

    layout_file = require_existing_file(layout_file, "layout file")
    layout = read_json(layout_file)

    nodes = layout.get("nodes")
    pos = layout.get("positions")
    edges = layout.get("edges")

    if nodes is None or pos is None or edges is None:
        raise ValueError(f"Invalid layout file format: {layout_file}")

    xml = []

    xml.append("<mxfile>")
    xml.append('  <diagram name="ETL Lineage">')
    xml.append("    <mxGraphModel>")
    xml.append("      <root>")
    xml.append('        <mxCell id="0"/>')
    xml.append('        <mxCell id="1" parent="0"/>')
    xml.append("")
    xml.append("        <!-- Nodes -->")

    id_map = {}

    for index, node in enumerate(nodes, start=1):

        node_id = f"n{index}"
        id_map[node["id"]] = node_id

        geometry = pos.get(node["id"], {"x": 0, "y": 0})
        label = escape(node["id"])
        style = _style_for_node_type(node.get("type"))

        xml.append(
            f'        <mxCell id="{node_id}" value="{label}" '
            f'style="{style}" vertex="1" parent="1">'
        )
        xml.append(
            f'          <mxGeometry x="{geometry["x"]}" y="{geometry["y"]}" width="220" height="80" as="geometry"/>'
        )
        xml.append("        </mxCell>")

    xml.append("")
    xml.append("        <!-- Edges -->")

    for index, edge in enumerate(edges, start=1):
        source_id = id_map.get(edge["source"], "")
        target_id = id_map.get(edge["target"], "")
        value = escape(edge.get("kind", ""))

        xml.append(
            f'        <mxCell id="e{index}" edge="1" parent="1" '
            f'source="{source_id}" target="{target_id}" value="{value}">'
        )
        xml.append('          <mxGeometry relative="1" as="geometry"/>')
        xml.append("        </mxCell>")

    xml.append("      </root>")
    xml.append("    </mxGraphModel>")
    xml.append("  </diagram>")
    xml.append("</mxfile>")

    return "\n".join(xml)


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
