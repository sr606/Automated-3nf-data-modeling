import os
import json
import uuid
import re
import hashlib
from collections import defaultdict, deque
from pathlib import Path
from dotenv import load_dotenv
from xml.sax.saxutils import escape

try:
    from sql_metadata import Parser
except ImportError:
    Parser = None

try:
    import graphviz
except ImportError:
    graphviz = None

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


def normalize_lineage_options(options=None):
    """
    Normalize lineage rendering options with stable defaults.
    """

    defaults = {
        "diagram_mode": "logical",
        "include_sql_sources": False,
        "collapse_intermediate_datasets": True,
        "include_lookup_nodes": True,
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

    stage_type = (stage_type or "").lower()
    stage_kind = (stage_kind or "").lower()

    if "hashedfile" in stage_type or "hashedfile" in stage_kind:
        return True

    if edge_kind == "lookup":
        return True

    return False


def _is_lookup_stage(stage_meta):
    """
    Determine whether a stage behaves as a lookup/helper node.
    """

    if not stage_meta:
        return False

    stage_type = (stage_meta.get("stage_type") or "").lower()
    stage_kind = (stage_meta.get("stage_kind") or "").lower()

    return "hashedfile" in stage_type or "hashedfile" in stage_kind


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


def _classify_semantic_stage(stage_meta, inbound_count, outbound_count, used_as_lookup):
    """
    Classify a stage node for semantic lineage rendering.
    """

    stage_name = stage_meta.get("stage", "")
    stage_type = (stage_meta.get("stage_type") or "").lower()
    stage_kind = (stage_meta.get("stage_kind") or "").lower()
    output_count = len(stage_meta.get("outputs", []))

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


def _collapse_lookup_nodes(edges, nodes):
    """
    Collapse lookup helper nodes into direct lookup edges.
    """

    node_type_map = {node["id"]: node["type"] for node in nodes}
    incoming = defaultdict(list)
    outgoing = defaultdict(list)

    for edge in edges:
        incoming[edge["target"]].append(edge)
        outgoing[edge["source"]].append(edge)

    collapsed_edges = []
    removed_lookup_nodes = set()

    for node_id, node_type in node_type_map.items():
        if node_type != "LOOKUP":
            continue

        parents = incoming.get(node_id, [])
        children = outgoing.get(node_id, [])

        if not parents or not children:
            continue

        removed_lookup_nodes.add(node_id)

        for parent in parents:
            for child in children:
                collapsed_edges.append({
                    "source": parent["source"],
                    "target": child["target"],
                    "kind": "lookup"
                })

    passthrough_edges = [
        edge for edge in edges
        if edge["source"] not in removed_lookup_nodes and edge["target"] not in removed_lookup_nodes
    ]

    final_nodes = [node for node in nodes if node["id"] not in removed_lookup_nodes]

    return final_nodes, _deduplicate_edges(passthrough_edges + collapsed_edges)


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
        nodes.setdefault(stage_name, {
            "id": stage_name,
            "type": "TRANSFORM",
        })

        for output in stage.get("outputs", []):
            dataset_id = output.get("dataset_id")
            alias = output.get("alias") or dataset_id
            dataset_node = alias or dataset_id

            if not dataset_id or not dataset_node:
                continue

            nodes.setdefault(dataset_node, {
                "id": dataset_node,
                "type": "DATASET",
            })
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

            nodes.setdefault(dataset_node, {
                "id": dataset_node,
                "type": "DATASET",
            })
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
        nodes.setdefault(table_name, {"id": table_name, "type": "SOURCE"})
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

    return {
        "nodes": list(nodes.values()),
        "edges": _deduplicate_edges(edges),
    }


def _build_semantic_graph(stages, stage_edges, tables, options):
    """
    Build a clean stage-level graph for logical lineage diagrams.
    """

    nodes = {}
    edges = []
    stage_map = {stage["stage"]: stage for stage in stages}
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

        nodes.setdefault(source_name, {"id": source_name, "type": "SOURCE"})
        nodes.setdefault(target_name, {"id": target_name, "type": "TRANSFORM"})
        edges.append({
            "source": source_name,
            "target": target_name,
            "kind": edge.get("kind", "flow"),
        })

    for stage_name, stage in stage_map.items():
        nodes[stage_name] = {
            "id": stage_name,
            "type": _classify_semantic_stage(
                stage,
                inbound_counts.get(stage_name, 0),
                outbound_counts.get(stage_name, 0),
                stage_name in lookup_stages,
            ),
        }

    complexity = _lineage_complexity(len(stage_map), len(stage_edges))
    add_sql_sources = options.get("include_sql_sources") or complexity == "small"
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
            nodes[table_name] = {"id": table_name, "type": "SOURCE"}
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

    return {
        "nodes": list(nodes.values()),
        "edges": _deduplicate_edges(edges),
        "complexity": complexity,
        "sql_sources": sorted(table_targets.keys()),
    }


def _build_edge_maps(edges, allowed_kinds=None):
    """
    Build adjacency maps from edge list.
    """

    children = defaultdict(list)
    parents = defaultdict(list)
    filtered = []

    for edge in edges:
        if allowed_kinds is not None and edge.get("kind") not in allowed_kinds:
            continue

        filtered.append(edge)
        children[edge["source"]].append(edge["target"])
        parents[edge["target"]].append(edge["source"])

    return filtered, children, parents


def _topological_sort(node_ids, edges):
    """
    Return a stable topological ordering when possible.
    """

    _, children, _ = _build_edge_maps(edges)
    indegree = {node_id: 0 for node_id in node_ids}

    for edge in edges:
        indegree[edge["target"]] = indegree.get(edge["target"], 0) + 1
        indegree.setdefault(edge["source"], 0)

    queue = deque(sorted(node_id for node_id, degree in indegree.items() if degree == 0))
    order = []

    while queue:
        node_id = queue.popleft()
        order.append(node_id)

        for child in sorted(children.get(node_id, [])):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    for node_id in sorted(node_ids):
        if node_id not in order:
            order.append(node_id)

    return order


def _longest_path(node_ids, edges):
    """
    Compute a longest path on a DAG-like lineage graph.
    """

    if not node_ids:
        return []

    order = _topological_sort(node_ids, edges)
    _, _, parents = _build_edge_maps(edges)
    distance = {node_id: 0 for node_id in node_ids}
    previous = {}

    for node_id in order:
        for parent in parents.get(node_id, []):
            candidate = distance[parent] + 1
            if candidate > distance[node_id]:
                distance[node_id] = candidate
                previous[node_id] = parent

    end_node = max(order, key=lambda node_id: distance.get(node_id, 0))
    path = [end_node]

    while end_node in previous:
        end_node = previous[end_node]
        path.append(end_node)

    path.reverse()
    return path


def _dag_layers(node_ids, edges):
    """
    Assign a left-to-right layer to each node using longest-path ranking.
    """

    order = _topological_sort(node_ids, edges)
    _, children, parents = _build_edge_maps(edges)
    rank = {node_id: 0 for node_id in node_ids}

    for node_id in order:
        parent_ranks = [rank[parent] for parent in parents.get(node_id, [])]
        if parent_ranks:
            rank[node_id] = max(parent_ranks) + 1

    return rank, children, parents


def _node_sort_weight(node):
    """
    Stable tie-breaker for deterministic layer ordering.
    """

    priority = {
        "SOURCE": 0,
        "EXTRACT": 1,
        "LOOKUP": 2,
        "STORE": 3,
        "TRANSFORM": 4,
        "TARGET": 5,
        "EXCEPTION": 6,
        "DATASET": 7,
    }

    return (priority.get(node.get("type", ""), 99), node["id"].lower())


def _graphviz_plain_layout(nodes, edges, sizes):
    """
    Ask Graphviz for node positions and return draw.io-friendly coordinates.
    """

    if graphviz is None:
        raise RuntimeError("Graphviz Python package is not installed.")

    dot = graphviz.Digraph(engine="dot")
    dot.attr(rankdir="LR", nodesep="0.6", ranksep="1.1", splines="line")

    for node in nodes:
        node_id = node["id"]
        node_size = sizes.get(node_id, {"width": 220, "height": 70})
        dot.node(
            node_id,
            label=node_id,
            width=f"{node_size['width'] / 72:.3f}",
            height=f"{node_size['height'] / 72:.3f}",
            shape="box",
            fixedsize="false",
        )

    for edge in edges:
        dot.edge(edge["source"], edge["target"])

    try:
        plain = dot.pipe(format="plain").decode("utf-8")
    except Exception as exc:
        raise RuntimeError(
            "Graphviz layout failed. Ensure the `dot` executable is installed and available on PATH."
        ) from exc

    positions = {}
    graph_height = 0.0

    for line in plain.splitlines():
        parts = line.split()
        if not parts:
            continue

        if parts[0] == "graph" and len(parts) >= 4:
            graph_height = float(parts[3])
        elif parts[0] == "node" and len(parts) >= 6:
            node_id = parts[1]
            x_center = float(parts[2]) * 72
            y_center = float(parts[3]) * 72
            width = float(parts[4]) * 72
            height = float(parts[5]) * 72
            positions[node_id] = {
                "x": int(round(x_center - (width / 2))),
                "y": int(round((graph_height * 72) - y_center - (height / 2))),
            }

    if not positions:
        raise RuntimeError("Graphviz returned no node positions.")

    return positions


def _spread_missing_or_colliding_positions(nodes, edges, sizes, positions):
    """
    Distribute nodes that Graphviz left unplaced or placed on identical coordinates.
    """

    node_ids = [node["id"] for node in nodes]
    layer_rank, _, _ = _dag_layers(node_ids, edges)
    by_layer = defaultdict(list)

    for node in nodes:
        by_layer[layer_rank.get(node["id"], 0)].append(node["id"])

    x_gap = 340
    base_x = 40
    base_y = 40
    row_gap = 130

    for layer_index, layer_nodes in by_layer.items():
        layer_x = base_x + (layer_index * x_gap)
        placed = [positions[node_id] for node_id in layer_nodes if node_id in positions]

        if not placed:
            current_y = base_y
            for node_id in sorted(layer_nodes):
                positions[node_id] = {"x": layer_x, "y": current_y}
                current_y += row_gap
            continue

        collision_groups = defaultdict(list)
        for node_id in layer_nodes:
            if node_id not in positions:
                continue
            key = (positions[node_id]["x"], positions[node_id]["y"])
            collision_groups[key].append(node_id)

        for (x_value, y_value), node_group in collision_groups.items():
            if len(node_group) <= 1:
                continue
            for offset, node_id in enumerate(sorted(node_group)):
                positions[node_id] = {
                    "x": x_value,
                    "y": y_value + (offset * row_gap),
                }

        max_y = max(item["y"] for item in positions.values()) if positions else base_y
        for node_id in sorted(layer_nodes):
            if node_id in positions:
                continue
            max_y += row_gap
            positions[node_id] = {"x": layer_x, "y": max_y}

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
        width, height = _node_box_size(node_id)
        sizes[node_id] = {"width": width, "height": height}

    positions = _graphviz_plain_layout(nodes, edges, sizes)
    positions = _spread_missing_or_colliding_positions(nodes, edges, sizes, positions)

    return {
        "source_file": _safe_source_key(graph_entry.get("source_file")),
        "nodes": nodes,
        "positions": positions,
        "sizes": sizes,
        "edges": edges,
        "options": options,
        "annotations": annotations,
        "complexity": complexity,
    }


def _shortest_distance(start_nodes, children):
    """
    Compute shortest distance from a set of start nodes.
    """

    queue = deque()
    distance = {}

    for node_id in start_nodes:
        distance[node_id] = 0
        queue.append(node_id)

    while queue:
        node_id = queue.popleft()

        for child in children.get(node_id, []):
            if child in distance:
                continue

            distance[child] = distance[node_id] + 1
            queue.append(child)

    return distance


def _nearest_anchor(node_id, anchors, children):
    """
    Find nearest anchor reachable from a node.
    """

    queue = deque([(node_id, 0)])
    visited = {node_id}

    while queue:
        current, depth = queue.popleft()

        if current in anchors:
            return current, depth

        for child in children.get(current, []):
            if child in visited:
                continue
            visited.add(child)
            queue.append((child, depth + 1))

    return None, None


def _nearest_upstream_anchor(node_id, anchors, parents):
    """
    Find nearest upstream anchor reachable from a node.
    """

    queue = deque([(node_id, 0)])
    visited = {node_id}

    while queue:
        current, depth = queue.popleft()

        if current in anchors and current != node_id:
            return current, depth

        for parent in parents.get(current, []):
            if parent in visited:
                continue
            visited.add(parent)
            queue.append((parent, depth + 1))

    return None, None


def _node_box_size(label):
    """
    Compute a width that keeps long labels inside the node box.
    """

    length = len(label or "")
    width = max(220, min(320, 160 + (length * 4)))
    height = 70 if length <= 28 else 80
    return width, height


def _edge_style(kind, route="", complexity="medium"):
    """
    Return draw.io edge style tuned for logical lineage.
    """

    if complexity in {"small", "medium"}:
        base = "edgeStyle=none;rounded=1;html=1;"

        if kind == "lookup":
            return base + "dashed=1;dashPattern=6 4;"

        if kind == "sql_source":
            return base + "dashed=1;dashPattern=2 4;"

        return base

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
            lane_priority.get(annotations.get(node["id"], {}).get("lane", "support"), 9),
            positions.get(node["id"], {}).get("x", 0),
            positions.get(node["id"], {}).get("y", 0),
            node["id"].lower(),
        )
    )


def _edge_waypoints(edge, positions, sizes, annotations, complexity="medium"):
    """
    Create explicit waypoints for support and branch edges so they avoid node boxes.
    """

    if complexity in {"small", "medium"} and edge.get("kind") != "exception":
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
    route = edge.get("route", "")

    if route == "vertical":
        if abs(source_center_y - target_center_y) < 20:
            return []
        mid_x = source_center_x if source_center_x < target_center_x else target_center_x
        bend_x = mid_x + 30
        return [
            {"x": bend_x, "y": source_center_y},
            {"x": bend_x, "y": target_center_y},
        ]

    if route == "side":
        if abs(source_center_x - target_center_x) < 20:
            return []
        bend_y = source_center_y - 40 if source_center_y <= target_center_y else source_center_y + 40
        return [
            {"x": source_center_x, "y": bend_y},
            {"x": target_center_x, "y": bend_y},
        ]

    if edge.get("kind") == "exception":
        bend_y = max(source_center_y, target_center_y) + 40
        return [
            {"x": source_center_x, "y": bend_y},
            {"x": target_center_x, "y": bend_y},
        ]

    return []


def _derive_logical_lineage(nodes, edges, options):
    """
    Derive a logical graph with a main spine and supporting lanes.
    """

    node_ids = [node["id"] for node in nodes]
    node_types = {node["id"]: node["type"] for node in nodes}
    complexity = _lineage_complexity(len(nodes), len(edges))
    non_lookup_edges = [edge for edge in edges if edge.get("kind") not in {"lookup", "sql_source"}]
    main_path = _longest_path(node_ids, non_lookup_edges)
    if len(main_path) < 2:
        main_path = _longest_path(node_ids, edges)
    if len(main_path) < 2:
        main_path = node_ids[:]
        
        
    if not main_path:
        main_path = _longest_path(node_ids, edges)

    main_set = set(main_path)
    all_edges, all_children, all_parents = _build_edge_maps(edges)
    _, main_children, main_parents = _build_edge_maps(non_lookup_edges)
    from_main = _shortest_distance(main_path, main_children)
    to_main = _shortest_distance(list(reversed(main_path)), main_parents)

    annotations = {}

    for node in nodes:
        node_id = node["id"]
        node_type = node_types.get(node_id, "DATASET")

        annotations[node_id] = {
            "role": "main" if node_id in main_set else "support",
            "anchor": node_id if node_id in main_set else None,
            "distance": 0,
            "lane": "main" if node_id in main_set else "support",
        }

        if node_id in main_set:
            continue

        downstream_anchor, downstream_distance = _nearest_anchor(node_id, main_set, all_children)
        upstream_anchor, upstream_distance = _nearest_upstream_anchor(node_id, main_set, all_parents)

        if node_type in {"SOURCE", "EXTRACT"}:
            annotations[node_id]["role"] = "support"
            annotations[node_id]["lane"] = "source"
            annotations[node_id]["anchor"] = downstream_anchor or upstream_anchor or (main_path[0] if main_path else node_id)
            annotations[node_id]["distance"] = downstream_distance or upstream_distance or 1
            continue

        if node_type in {"LOOKUP", "STORE"}:
            annotations[node_id]["role"] = "support"
            annotations[node_id]["lane"] = "lookup"
            annotations[node_id]["anchor"] = downstream_anchor or upstream_anchor or (main_path[0] if main_path else node_id)
            annotations[node_id]["distance"] = downstream_distance or upstream_distance or 1
            continue

        if upstream_anchor is not None and (
            downstream_anchor is None or
            from_main.get(node_id, 10**6) <= to_main.get(node_id, 10**6)
        ) and node_type not in {"SOURCE", "LOOKUP", "STORE", "EXTRACT"}:
            annotations[node_id]["role"] = "branch"
            annotations[node_id]["lane"] = "branch"
            annotations[node_id]["anchor"] = upstream_anchor
            annotations[node_id]["distance"] = upstream_distance or 1
        else:
            annotations[node_id]["role"] = "support"
            annotations[node_id]["lane"] = "support"
            annotations[node_id]["anchor"] = downstream_anchor or upstream_anchor or (main_path[0] if main_path else node_id)
            annotations[node_id]["distance"] = downstream_distance or upstream_distance or 1

        if node_type == "TARGET" and upstream_anchor is not None:
            annotations[node_id]["role"] = "branch"
            annotations[node_id]["lane"] = "branch"
            annotations[node_id]["anchor"] = upstream_anchor
            annotations[node_id]["distance"] = upstream_distance or 1

    logical_edges = []

    for edge in all_edges:
        source_role = annotations.get(edge["source"], {}).get("role")
        target_role = annotations.get(edge["target"], {}).get("role")
        route = ""

        if edge.get("kind") in {"lookup", "sql_source"}:
            route = "vertical"
        elif complexity == "small" and annotations.get(edge["source"], {}).get("lane") == "source":
            route = "side"
        elif source_role in {"support"} or target_role in {"support", "branch"}:
            route = "vertical"

        logical_edges.append({
            "source": edge["source"],
            "target": edge["target"],
            "kind": edge.get("kind", "input"),
            "route": route,
        })

    return {
        "nodes": nodes,
        "edges": _deduplicate_edges(logical_edges),
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
    Detect independent ETL pipelines inside pseudocode.
    """

    source_file = require_existing_file(source_file, "lineage source file")
    data = read_json(source_file)

    jobs = []

    for entry in data:
        source_name = _safe_source_key(entry.get("file"))

        text = entry.get("content", "")
        text = _normalize_arrow_text(text)

        segments = re.split(r'//\s*=+', text)

        for index, seg in enumerate(segments, start=1):

            if len(seg.strip()) > 50:
                jobs.append({
                    "source_file": source_name,
                    "job_index": index,
                    "content": seg.strip(),
                })

    write_json(output_path, jobs)

    return output_path


# -----------------------------------------------------
# Parse ETL Stages
# -----------------------------------------------------
def parse_stages_chunked(job_file, output_path):
    """
    Extract stage metadata from pseudocode jobs.
    Robust parser tolerant to formatting differences.
    """

    job_file = require_existing_file(job_file, "job boundaries file")
    jobs = read_json(job_file)

    stages = []

    for job in jobs:
        if isinstance(job, dict):
            source_file = _safe_source_key(job.get("source_file"))
            job_text = job.get("content", "")
        else:
            source_file = "lineage"
            job_text = job

        job_text = _normalize_arrow_text(job_text)

        # detect stage blocks safely
        stage_blocks = re.findall(
            r'(//\s*---\s*\[.*?\](?:.*?))(?=//\s*---\s*\[|\Z)',
            job_text,
            re.S
        )

        for block in stage_blocks:

            header = _extract_stage_header(block)

            if not header:
                continue

            inputs = _extract_link_entries(block, "Input")
            outputs = _extract_link_entries(block, "Output")
            sql = _extract_sql_block(block)

            stages.append({
                "source_file": source_file,
                "stage": header["stage"],
                "type": "TRANSFORM",
                "stage_kind": header["kind"],
                "stage_type": _extract_stage_type(block),
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

        if options.get("diagram_mode") == "technical" and not options.get("collapse_intermediate_datasets"):
            graph_payload = _build_dataset_graph(stages, source_tables, options)
            complexity = _lineage_complexity(
                len(graph_payload.get("nodes", [])),
                len(graph_payload.get("edges", [])),
            )
            sql_sources = [table.get("table") for table in source_tables]
        else:
            graph_payload = _build_semantic_graph(stages, edges, source_tables, options)
            complexity = graph_payload.get("complexity", _lineage_complexity(len(stages), len(edges)))
            sql_sources = graph_payload.get("sql_sources", [])

        normalized_graphs.append({
            "source_file": source_file,
            "nodes": graph_payload.get("nodes", []),
            "edges": graph_payload.get("edges", []),
            "sql_sources": sql_sources,
            "options": options,
            "complexity": complexity,
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
        logical_graph = _derive_logical_lineage(
            graph_entry.get("nodes", []),
            graph_entry.get("edges", []),
            normalize_lineage_options(graph_entry.get("options", {}))
        )

        lineage_entries.append({
            "source_file": _safe_source_key(graph_entry.get("source_file")),
            "paths": [logical_graph.get("main_path", [])] if logical_graph.get("main_path") else [],
            "edges": logical_graph.get("edges", []),
            "nodes": logical_graph.get("nodes", []),
            "annotations": logical_graph.get("annotations", {}),
            "sql_sources": graph_entry.get("sql_sources", []),
            "options": graph_entry.get("options", {}),
            "complexity": logical_graph.get("complexity", graph_entry.get("complexity")),
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
    Convert lineage layout JSON into draw.io XML format.
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
            label = escape(node["id"])
            style = _style_for_node_type(node.get("type")).replace(
                "whiteSpace=wrap;html=1;",
                "whiteSpace=wrap;html=1;overflow=hidden;align=center;verticalAlign=middle;spacing=6;"
            )
            node_size = sizes.get(node["id"], {})
            width = node_size.get("width") or _node_box_size(node["id"])[0]
            height = node_size.get("height") or _node_box_size(node["id"])[1]

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
