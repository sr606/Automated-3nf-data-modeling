import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from lineage_agent.brain.prompts import (
    prompt_phase1,
    prompt_phase15,
    prompt_phase2,
    prompt_phase3,
    prompt_phase4,
    prompt_phase5,
    prompt_phase6,
    prompt_phase7,
)
from lineage_agent.brain.preprocessing import build_preprocessed_context
from lineage_agent.brain.utils import (
    DEFAULT_MAX_CHUNK_CHARS,
    extract_json,
    read_text,
    render_dot,
    safe_name,
    timestamp,
    write_json,
    write_text,
)

try:
    from langchain_openai import AzureChatOpenAI
except ImportError:
    AzureChatOpenAI = None


load_dotenv()


@dataclass
class PhaseSpec:
    key: str
    title: str
    goal: str
    output_name: str
    description: str


@dataclass
class AgentConfig:
    input_path: str
    output_dir: str
    model_override: str = ""
    max_chunk_chars: int = DEFAULT_MAX_CHUNK_CHARS


@dataclass
class AgentContext:
    input_file: str
    document_text: str
    preprocessed_context: dict[str, Any]
    run_id: str
    run_dir: str


@dataclass
class PhaseResult:
    """Typed result for phase executions."""
    phase_key: str
    data: dict[str, Any]
    prompt: str = ""
    retry_prompt: str = ""


@dataclass
class AgentState:
    phase_outputs: dict[str, Any] = field(default_factory=dict)
    prompts: dict[str, str] = field(default_factory=dict)
    architecture: dict[str, Any] = field(default_factory=dict)
    
    def record_phase(self, phase_key: str, data: dict[str, Any]) -> None:
        """Record phase output with structured typing."""
        self.phase_outputs[phase_key] = data
    
    def record_prompt(self, phase_key: str, prompt: str, is_retry: bool = False) -> None:
        """Record prompt with optional retry tracking."""
        suffix = "_retry" if is_retry else ""
        self.prompts[f"{phase_key}{suffix}"] = prompt


PHASE_SPECS = [
    PhaseSpec("phase1_signal_extraction", "Signal Extraction", "Identify what exists without connecting or simplifying.", "chunk_signals", "Extract raw chunk-level component signals only."),
    PhaseSpec("phase15_global_reconstruction", "Global Reconstruction", "Understand the dominant ETL pipeline before graph wiring.", "global_understanding", "Build a global-first mental model of the primary path and side flows."),
    PhaseSpec("phase2_flow_reconstruction", "Flow Reconstruction", "Build the full mental pipeline before any compression.", "graph", "Reconstruct the end-to-end graph and side flows."),
    PhaseSpec("phase3_pattern_detection", "Pattern Detection", "Infer structural meaning from topology rather than names.", "patterns", "Detect integration, lookup enrichment, core transform, and load patterns."),
    PhaseSpec("phase4_semantic_compression", "Semantic Compression", "Remove noise while keeping pipeline meaning intact.", "normalized_components", "Compress sequential transforms and group related components logically."),
    PhaseSpec("phase5_build_models", "Model Building", "Produce both high-level and technical representations.", "models", "Create a grouped business view and a detailed technical view."),
    PhaseSpec("phase6_transform_semantics", "Transformation Semantics", "Attach business-meaningful transform semantics.", "transform_semantics", "Infer standardization, derivation, lookup, validation, null handling, and routing."),
    PhaseSpec("phase7_graph_construction", "Graph Construction", "Convert the models into explicit nodes and edges.", "graphs", "Build high-level and technical graph payloads with typed edges."),
    PhaseSpec("phase8_graphviz_generation", "Graphviz Rendering", "Render both views into Graphviz DOT.", "dot", "Emit styled DOT for high-level and technical diagrams."),
]


class GraphNormalizer:
    """Encapsulates graph normalization logic with common helpers."""
    
    @staticmethod
    def build_valid_node_ids(nodes: list[dict[str, Any]]) -> set[str]:
        """Extract all valid node IDs from a list of nodes."""
        return {str(n.get("id") or "").strip() for n in nodes if isinstance(n, dict) and str(n.get("id") or "").strip()}
    
    @staticmethod
    def validate_edge_endpoints(edge: dict[str, Any], valid_ids: set[str]) -> bool:
        """Check if edge endpoints reference valid nodes."""
        from_id = str(edge.get("from") or "").strip()
        to_id = str(edge.get("to") or "").strip()
        return bool(from_id and to_id and from_id in valid_ids and to_id in valid_ids and from_id != to_id)
    
    @staticmethod
    def fix_exception_flow_direction(edges: list[dict[str, Any]], exception_ids: set[str], transform_ids: set[str]) -> list[dict[str, Any]]:
        """Reverse exception edges pointing wrong direction (exception→transform to transform→exception)."""
        fixed_edges = []
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            edge = dict(edge)  # Copy to avoid mutation
            from_id = str(edge.get("from") or "").strip()
            to_id = str(edge.get("to") or "").strip()
            edge_type = str(edge.get("type") or "").strip()
            
            # If exception edge points wrong way, reverse it
            if edge_type == "exception" and from_id in exception_ids and to_id in transform_ids:
                # Wrong direction, reverse it
                edge["from"] = to_id
                edge["to"] = from_id
                edge["reason"] = "Edge direction corrected: exceptions flow FROM transform TO exception handler."
            
            fixed_edges.append(edge)
        return fixed_edges


class LayeredLineageAgent:
    def __init__(self, config: AgentConfig):
        self.config = config
        self.llm = self._get_llm(config.model_override)

    def run(self) -> dict[str, Any]:
        context = self._build_agent_context()
        state = AgentState(architecture=self._architecture_payload())

        state.record_phase("phase1_signal_extraction", self._run_phase1(context, state))
        state.record_phase("phase15_global_reconstruction", self._run_phase15(context, state))
        state.record_phase("phase2_flow_reconstruction", self._run_phase2(context, state))
        state.record_phase("phase3_pattern_detection", self._run_phase3(context, state))
        state.record_phase("phase4_semantic_compression", self._run_phase4(context, state))
        state.record_phase("phase5_build_models", self._run_phase5(context, state))
        state.record_phase("phase6_transform_semantics", self._run_phase6(context, state))
        state.record_phase("phase7_graph_construction", self._run_phase7(state))
        state.record_phase("phase8_graphviz_generation", self._run_phase8(state))

        # Write DOT files only (no JSON artifacts)
        phase8 = state.phase_outputs["phase8_graphviz_generation"]
        run_dir = Path(context.run_dir)
        high_level_dot_path = run_dir / "high_level.dot"
        technical_dot_path = run_dir / "technical.dot"
        write_text(high_level_dot_path, phase8.get("high_level_dot", ""))
        write_text(technical_dot_path, phase8.get("technical_dot", ""))

        pdf_outputs = {
            "high_level": render_dot(high_level_dot_path),
            "technical": render_dot(technical_dot_path),
        }

        return self._build_result(context, pdf_outputs)

    def _build_agent_context(self) -> AgentContext:
        input_file = Path(self.config.input_path)
        document_text = read_text(input_file)
        preprocessed_context = build_preprocessed_context(document_text, self.config.max_chunk_chars)
        run_id = f"{safe_name(input_file.stem)}_{timestamp()}"
        run_dir = Path(self.config.output_dir) / run_id
        return AgentContext(
            input_file=str(input_file),
            document_text=document_text,
            preprocessed_context=preprocessed_context,
            run_id=run_id,
            run_dir=str(run_dir),
        )

    def _architecture_payload(self) -> dict[str, Any]:
        return {
            "agent_name": "LayeredLineageAgent",
            "strategy": {
                "core_principles": [
                    "Chunk awareness",
                    "Progressive abstraction",
                    "Forced reasoning order",
                    "Lossy compression with structure preservation",
                ],
                "reasoning_order": [
                    "Find all pieces",
                    "Understand the full pipeline globally",
                    "Build full pipeline mentally",
                    "Detect structural patterns",
                    "Compress structure",
                    "Build two views",
                    "Add transform semantics",
                    "Construct graph",
                    "Render diagram",
                ],
            },
            "phases": [asdict(item) for item in PHASE_SPECS],
        }

    def _run_phase1(self, context: AgentContext, state: AgentState) -> dict[str, Any]:
        chunk_signals = []
        for chunk in context.preprocessed_context["chunks"]:
            prompt = prompt_phase1(context.preprocessed_context, chunk)
            state.record_prompt(f"phase1_{chunk['chunk_id']}", prompt)
            chunk_signals.append(self._invoke_json(prompt))
        
        consolidated_signals = self._consolidate_phase1_signals(chunk_signals)
        authoritative_inventory = self._build_authoritative_inventory(consolidated_signals)
        return {
            "phase": "signal_extraction",
            "goal": "Identify what exists without structure.",
            "job_name_hints": context.preprocessed_context["job_name_hints"],
            "chunk_signals": chunk_signals,
            "consolidated_signals": consolidated_signals,
            "authoritative_inventory": authoritative_inventory,
        }

    def _run_phase2(self, context: AgentContext, state: AgentState) -> dict[str, Any]:
        phase15 = state.phase_outputs["phase15_global_reconstruction"]
        base_prompt = prompt_phase2(
            context.document_text,
            state.phase_outputs["phase1_signal_extraction"],
            phase15,
        )
        prompt = (
            "PHASE 2 OVERRIDE - USE GLOBAL CONTEXT AS TRUTH\n\n"
            "You are given Phase 1.5 global pipeline understanding.\n"
            "Treat its dominant_pipeline as ground truth.\n"
            "Do not override strongest_source, core_transform, store_node, or target_node unless Phase 1.5 left that field blank.\n"
            "Your job in Phase 2 is to attach remaining nodes correctly to this backbone.\n\n"
            + base_prompt
        )
        state.record_prompt("phase2", prompt)
        phase2 = self._invoke_json(prompt)
        
        violations = self._phase2_anchor_violations(phase2, phase15)
        if violations:
            retry_prompt = (
                prompt
                + "\n\nPHASE 2 AUTO-RETRY - PRIOR RESPONSE VIOLATED LOCKED ANCHOR\n"
                + "You must correct these violations and return JSON again.\n"
                + "\n".join(f"- {item}" for item in violations)
                + "\n\nMANDATORY RETRY RULES:\n"
                + "- anchor_chain.canonical_source must equal strongest_source when provided\n"
                + "- anchor_chain.canonical_core_transformer must equal core_transform when provided\n"
                + "- anchor_chain.anchor_load_node must equal store_node when provided\n"
                + "- anchor_chain.primary_target must equal target_node when provided\n"
                + "- main edges must preserve the locked anchor order\n"
                + "- if you cannot find explicit evidence, still preserve the locked anchor exactly\n"
            )
            state.record_prompt("phase2", retry_prompt, is_retry=True)
            phase2 = self._invoke_json(retry_prompt)
        
        return phase2

    def _run_phase15(self, context: AgentContext, state: AgentState) -> dict[str, Any]:
        prompt = prompt_phase15(
            context.document_text,
            state.phase_outputs["phase1_signal_extraction"],
            context.preprocessed_context,
        )
        state.record_prompt("phase15", prompt)
        phase15 = self._invoke_json(prompt)
        return self._reconcile_phase15_output(
            phase15,
            state.phase_outputs["phase1_signal_extraction"],
            context.preprocessed_context,
        )

    def _run_phase3(self, context: AgentContext, state: AgentState) -> dict[str, Any]:
        prompt = prompt_phase3(
            state.phase_outputs["phase2_flow_reconstruction"],
            context.preprocessed_context,
        )
        state.record_prompt("phase3", prompt)
        return self._invoke_json(prompt)

    def _run_phase4(self, context: AgentContext, state: AgentState) -> dict[str, Any]:
        prompt = prompt_phase4(
            state.phase_outputs["phase2_flow_reconstruction"],
            state.phase_outputs["phase3_pattern_detection"],
        )
        state.record_prompt("phase4", prompt)
        return self._invoke_json(prompt)

    def _run_phase5(self, context: AgentContext, state: AgentState) -> dict[str, Any]:
        prompt = prompt_phase5(
            state.phase_outputs["phase3_pattern_detection"],
            state.phase_outputs["phase4_semantic_compression"],
        )
        state.record_prompt("phase5", prompt)
        return self._invoke_json(prompt)

    def _run_phase6(self, context: AgentContext, state: AgentState) -> dict[str, Any]:
        prompt = prompt_phase6(
            context.document_text,
            state.phase_outputs["phase3_pattern_detection"],
            state.phase_outputs["phase5_build_models"],
        )
        state.record_prompt("phase6", prompt)
        return self._invoke_json(prompt)

    def _run_phase7(self, state: AgentState) -> dict[str, Any]:
        prompt = prompt_phase7(
            state.phase_outputs["phase2_flow_reconstruction"],
            state.phase_outputs["phase3_pattern_detection"],
            state.phase_outputs["phase4_semantic_compression"],
            state.phase_outputs["phase5_build_models"],
            state.phase_outputs["phase6_transform_semantics"],
        )
        state.record_prompt("phase7", prompt)
        phase7 = self._invoke_json(prompt)
        return self._normalize_graph_payload(
            phase7,
            state.phase_outputs["phase15_global_reconstruction"],
            state.phase_outputs["phase1_signal_extraction"],
            state.phase_outputs["phase4_semantic_compression"],
            state.phase_outputs["phase5_build_models"],
            state.phase_outputs["phase6_transform_semantics"],
        )

    def _run_phase8(self, state: AgentState) -> dict[str, Any]:
        state.record_prompt("phase8", "Deterministic Python DOT renderer used. LLM rendering skipped.")
        phase8 = self._build_dot_payload(state.phase_outputs["phase7_graph_construction"])
        return self._sanitize_dot_payload(phase8)

    def _normalize_graph_payload(
        self,
        phase7: dict[str, Any],
        phase15: dict[str, Any],
        phase1: dict[str, Any],
        phase4: dict[str, Any],
        phase5: dict[str, Any],
        phase6: dict[str, Any],
    ) -> dict[str, Any]:
        result = dict(phase7 or {})
        id_map = self._build_graph_id_map(phase4, phase5, phase7)
        locked_source = self._resolve_graph_id(str(phase15.get("strongest_source") or "").strip(), id_map)
        locked_core = self._resolve_graph_id(str(phase15.get("core_transform") or "").strip(), id_map)
        locked_store = self._resolve_graph_id(str(phase15.get("store_node") or "").strip(), id_map)
        locked_target = self._resolve_graph_id(str(phase15.get("target_node") or "").strip(), id_map)
        confidence_index = self._build_confidence_index(phase1)
        technical = dict(result.get("technical_graph") or {})
        high_level = dict(result.get("high_level_graph") or {})

        technical_nodes = self._normalize_graph_nodes(technical.get("nodes", []), id_map)
        high_level_nodes = self._normalize_graph_nodes(high_level.get("nodes", []), id_map)

        technical_nodes = self._upsert_graph_node(technical_nodes, locked_source, "source", "primary")
        technical_nodes = self._upsert_graph_node(technical_nodes, locked_core, "core_transform", "primary")
        technical_nodes = self._upsert_graph_node(technical_nodes, locked_store, "store", "primary")
        technical_nodes = self._upsert_graph_node(technical_nodes, locked_target, "target", "primary")

        high_level_nodes = self._upsert_graph_node(high_level_nodes, locked_source, "source_group", None)
        high_level_nodes = self._upsert_graph_node(high_level_nodes, locked_core, "core_transform", None)
        high_level_nodes = self._upsert_graph_node(high_level_nodes, locked_store, "store", None)
        high_level_nodes = self._upsert_graph_node(high_level_nodes, locked_target, "target", None)

        for node in technical_nodes:
            if str(node.get("kind") or "") == "lookup":
                node["cluster"] = "primary"
            node["confidence_score"] = self._node_confidence(node, confidence_index, phase15)
        technical["nodes"] = technical_nodes
        for node in high_level_nodes:
            node["confidence_score"] = self._node_confidence(node, confidence_index, phase15)
        high_level["nodes"] = high_level_nodes

        technical_edges = self._normalize_graph_edges(technical.get("edges", []), technical_nodes, locked_store, locked_target, id_map)
        high_level_edges = self._normalize_graph_edges(high_level.get("edges", []), high_level_nodes, locked_store, locked_target, id_map)

        technical_edges = self._upsert_edge(technical_edges, locked_source, locked_core, "main", "Locked anchor spine from Phase 1.5.")
        high_level_edges = self._upsert_edge(high_level_edges, locked_source, locked_core, "main", "Locked anchor spine from Phase 1.5.")
        if locked_store and locked_store != locked_core:
            technical_edges = self._upsert_edge(technical_edges, locked_core, locked_store, "main", "Locked anchor spine from Phase 1.5.")
            high_level_edges = self._upsert_edge(high_level_edges, locked_core, locked_store, "main", "Locked anchor spine from Phase 1.5.")
        technical_edges = self._upsert_edge(
            technical_edges,
            locked_store if locked_store else locked_core,
            locked_target,
            "main",
            "Locked anchor spine from Phase 1.5.",
        )
        high_level_edges = self._upsert_edge(
            high_level_edges,
            locked_store if locked_store else locked_core,
            locked_target,
            "main",
            "Locked anchor spine from Phase 1.5.",
        )

        technical["edges"] = technical_edges
        high_level["edges"] = [edge for edge in high_level_edges if edge.get("type") != "secondary"]

        spine = [node_id for node_id in [locked_source, locked_core, locked_store, locked_target] if node_id]
        technical["spine"] = spine
        high_level["spine"] = spine

        technical = self._merge_transform_chains(technical)
        high_level = self._merge_transform_chains(high_level)
        technical = self._prune_secondary_artifacts(technical)
        high_level = self._prune_secondary_artifacts(high_level)
        high_level = self._canonicalize_core_nodes(high_level)
        self._validate_single_high_level_core(high_level)
        technical = self._expand_technical_graph(technical, phase6, id_map)
        high_level = self._reduce_high_level_graph(high_level)
        
        # Fix exception edge directions: they should flow OUT from transforms TO exception handlers
        technical = self._fix_exception_edge_directions(technical)
        high_level = self._fix_exception_edge_directions(high_level)

        result["technical_graph"] = technical
        result["high_level_graph"] = high_level
        result["id_map"] = dict(sorted(id_map.items()))
        return result

    def _reconcile_phase15_output(
        self,
        phase15: dict[str, Any],
        phase1: dict[str, Any],
        preprocessed_context: dict[str, Any],
    ) -> dict[str, Any]:
        result = dict(phase15 or {})
        strongest_source = str(result.get("strongest_source") or "").strip()
        core_transform = str(result.get("core_transform") or "").strip()
        store_node = str(result.get("store_node") or "").strip()
        target_node = str(result.get("target_node") or "").strip()

        wrapper_names, source_names, store_names, lookup_names = self._phase15_role_hints(phase1, preprocessed_context)
        transform_candidate = self._select_phase15_transform_candidate(
            preprocessed_context,
            wrapper_names,
            source_names,
            store_names,
            lookup_names,
            strongest_source,
        )

        if strongest_source and strongest_source == core_transform and transform_candidate:
            core_transform = transform_candidate
            result["core_transform"] = core_transform
            result["core_transform_evidence"] = (
                f"Reconciled from downstream transform candidate {core_transform} "
                f"because the strongest source remains the structural source."
            )

        if self._is_wrapper_like_node(store_node, wrapper_names):
            store_node = ""
            result["store_node"] = ""

        target_hints = [str(item or "").strip() for item in preprocessed_context.get("target_table_hints", []) if str(item or "").strip()]
        if (
            not target_node
            or target_node == store_node
            or target_node == strongest_source
            or target_node == core_transform
            or self._is_wrapper_like_node(target_node, wrapper_names)
        ) and target_hints:
            target_node = target_hints[0]
            result["target_node"] = target_node

        dominant_pipeline = [str(item or "").strip() for item in result.get("dominant_pipeline", []) if str(item or "").strip()]
        rebuilt_pipeline: list[str] = []
        for candidate in [strongest_source, core_transform, store_node, target_node]:
            if candidate and candidate not in rebuilt_pipeline:
                rebuilt_pipeline.append(candidate)
        if rebuilt_pipeline:
            result["dominant_pipeline"] = rebuilt_pipeline

        if strongest_source and strongest_source not in [str(item or "").strip() for item in result.get("preintegrated_sources", [])]:
            preintegrated = [str(item or "").strip() for item in result.get("preintegrated_sources", []) if str(item or "").strip()]
            if strongest_source in source_names and strongest_source == str(phase15.get("strongest_source") or "").strip():
                preintegrated.append(strongest_source)
                result["preintegrated_sources"] = self._merge_string_lists([], preintegrated)

        return result

    def _phase15_role_hints(
        self,
        phase1: dict[str, Any],
        preprocessed_context: dict[str, Any],
    ) -> tuple[set[str], set[str], set[str], set[str]]:
        wrapper_names: set[str] = set()
        source_names: set[str] = set()
        store_names: set[str] = set()
        lookup_names: set[str] = set()

        for chunk in preprocessed_context.get("chunks", []) or []:
            if not isinstance(chunk, dict):
                continue
            names = [
                str(chunk.get("stage_name") or "").strip(),
                str(chunk.get("block_name") or "").strip(),
                *[str(item or "").strip() for item in chunk.get("stage_names", []) or []],
            ]
            normalized = {item.upper() for item in names if item}
            object_type = str(chunk.get("object_type") or "").upper()
            wrapper_hint = str(chunk.get("wrapper_hint") or "").strip().lower()
            kind = str(chunk.get("kind") or "").strip().lower()

            if wrapper_hint == "wrapper" or "CUSTOMOUTPUT" in object_type or "TRANSACTIONMANAGER" in " ".join(normalized):
                wrapper_names |= normalized
            if kind == "source":
                source_names |= normalized
            if kind == "store":
                store_names |= normalized
            if kind == "lookup":
                lookup_names |= normalized

        inventory = dict(phase1.get("authoritative_inventory") or {})
        for item in inventory.get("sources", []) or []:
            source_names |= self._phase15_inventory_names(item)
        for item in inventory.get("stores", []) or []:
            store_names |= self._phase15_inventory_names(item)
        for item in inventory.get("lookups", []) or []:
            lookup_names |= self._phase15_inventory_names(item)

        return wrapper_names, source_names, store_names, lookup_names

    def _phase15_inventory_names(self, item: dict[str, Any]) -> set[str]:
        names = {
            str(item.get("id") or "").strip(),
            str(item.get("name") or "").strip(),
            str(item.get("original_name") or "").strip(),
            str(item.get("canonical_hint") or "").strip(),
            *[str(alias or "").strip() for alias in item.get("aliases", []) or []],
        }
        return {name.upper() for name in names if name}

    def _select_phase15_transform_candidate(
        self,
        preprocessed_context: dict[str, Any],
        wrapper_names: set[str],
        source_names: set[str],
        store_names: set[str],
        lookup_names: set[str],
        strongest_source: str,
    ) -> str:
        candidates: list[tuple[int, str]] = []
        strongest_source_key = strongest_source.upper()
        for name in preprocessed_context.get("stage_name_hints", []) or []:
            candidate = str(name or "").strip()
            if not candidate:
                continue
            key = candidate.upper()
            if key == strongest_source_key:
                continue
            if key in wrapper_names or key in store_names or key in lookup_names:
                continue

            score = 0
            if re.search(r"(^TFM[_ ]|TRANSFORM|TRANSFORMER|LOADRECORDS)", key):
                score += 8
            if "LOADRECORDS" in key:
                score += 4
            if re.search(r"(TRXOUTPUT|MERGED TRANSFORMER OUTPUT|ADDITIONAL TRANSFORMER OUTPUT)", key):
                score += 4
            if "OUTPUT" in key:
                score -= 6
            if "CUSTOMSTAGE" in key or "CUSTOMOUTPUT" in key or "ORACLECONNECTOR" in key:
                score -= 3
            if key in source_names:
                score -= 4

            if score > 0:
                candidates.append((score, candidate))

        if not candidates:
            return ""

        candidates.sort(key=lambda item: (-item[0], item[1]))
        return candidates[0][1]

    def _is_wrapper_like_node(self, node_name: str, wrapper_names: set[str]) -> bool:
        value = str(node_name or "").strip()
        if not value:
            return False
        upper = value.upper()
        return (
            upper in wrapper_names
            or "CUSTOMOUTPUT" in upper
            or "TRANSACTIONMANAGER" in upper
            or upper.endswith("P2")
        )

    def _merge_transform_chains(self, graph: dict[str, Any]) -> dict[str, Any]:
        result = dict(graph or {})
        nodes = [dict(node) for node in result.get("nodes", []) if isinstance(node, dict)]
        edges = [dict(edge) for edge in result.get("edges", []) if isinstance(edge, dict)]
        spine = [str(item or "").strip() for item in result.get("spine", []) if str(item or "").strip()]
        transform_kinds = {"transform", "integration", "core_transform", "validation"}
        node_by_id = {str(node.get("id") or "").strip(): node for node in nodes if str(node.get("id") or "").strip()}
        merge_pairs: list[tuple[str, str]] = []

        for edge in edges:
            from_id = str(edge.get("from") or "").strip()
            to_id = str(edge.get("to") or "").strip()
            if str(edge.get("type") or "").strip() != "main":
                continue
            left = node_by_id.get(from_id)
            right = node_by_id.get(to_id)
            if not left or not right:
                continue
            if str(left.get("kind") or "") not in transform_kinds:
                continue
            if str(right.get("kind") or "") not in transform_kinds:
                continue
            if left.get("cluster") == "secondary" or right.get("cluster") == "secondary":
                continue
            merge_pairs.append((from_id, to_id))

        if not merge_pairs:
            return result

        merge_map: dict[str, str] = {}
        for keep_id, drop_id in merge_pairs:
            resolved_keep = merge_map.get(keep_id, keep_id)
            resolved_drop = merge_map.get(drop_id, drop_id)
            for key, value in list(merge_map.items()):
                if value == resolved_drop:
                    merge_map[key] = resolved_keep
            merge_map[resolved_drop] = resolved_keep

        if not merge_map:
            return result

        merged_nodes: dict[str, dict[str, Any]] = {}
        for node in nodes:
            original_id = str(node.get("id") or "").strip()
            final_id = merge_map.get(original_id, original_id)
            current = merged_nodes.get(final_id)
            if current is None:
                current = dict(node)
                current["id"] = final_id
                merged_nodes[final_id] = current
            else:
                current["members"] = self._merge_string_lists(current.get("members", []), node.get("members", []))
                if original_id != final_id:
                    current["members"] = self._merge_string_lists(current.get("members", []), [original_id])
                current["semantics"] = self._merge_string_lists(current.get("semantics", []), node.get("semantics", []))
                if node.get("business_label") and not current.get("business_label"):
                    current["business_label"] = node.get("business_label")
                current["confidence_score"] = max(float(current.get("confidence_score", 0.0) or 0.0), float(node.get("confidence_score", 0.0) or 0.0))
            if original_id != final_id:
                current["members"] = self._merge_string_lists(current.get("members", []), [original_id])
            current["kind"] = self._merged_kind(str(current.get("kind") or ""), str(node.get("kind") or ""))
            if node.get("cluster") == "primary":
                current["cluster"] = "primary"

        merged_edges: list[dict[str, Any]] = []
        for edge in edges:
            from_id = merge_map.get(str(edge.get("from") or "").strip(), str(edge.get("from") or "").strip())
            to_id = merge_map.get(str(edge.get("to") or "").strip(), str(edge.get("to") or "").strip())
            if not from_id or not to_id or from_id == to_id:
                continue
            merged_edges = self._upsert_edge(
                merged_edges,
                from_id,
                to_id,
                str(edge.get("type") or "").strip(),
                str(edge.get("reason") or "Merged graph edge."),
            )

        merged_spine: list[str] = []
        for item in spine:
            resolved = merge_map.get(item, item)
            if resolved and resolved not in merged_spine:
                merged_spine.append(resolved)

        result["nodes"] = list(merged_nodes.values())
        result["edges"] = merged_edges
        result["spine"] = merged_spine
        return result

    def _canonicalize_core_nodes(self, graph: dict[str, Any]) -> dict[str, Any]:
        result = dict(graph or {})
        nodes = [dict(node) for node in result.get("nodes", []) if isinstance(node, dict)]
        edges = [dict(edge) for edge in result.get("edges", []) if isinstance(edge, dict)]
        spine = [str(item or "").strip() for item in result.get("spine", []) if str(item or "").strip()]

        core_id = ""
        core_node = None
        for node_id in spine:
            node = next((item for item in nodes if str(item.get("id") or "").strip() == node_id), None)
            if node and str(node.get("kind") or "") == "core_transform":
                core_id = node_id
                core_node = node
                break

        if not core_id or core_node is None:
            return result

        core_members = {str(item or "").strip() for item in core_node.get("members", []) or [] if str(item or "").strip()}
        duplicate_ids: list[str] = []
        for node in nodes:
            node_id = str(node.get("id") or "").strip()
            if not node_id or node_id == core_id:
                continue
            if str(node.get("kind") or "") != "core_transform":
                continue
            node_members = {str(item or "").strip() for item in node.get("members", []) or [] if str(item or "").strip()}
            if core_members & node_members:
                duplicate_ids.append(node_id)
                core_members |= node_members

        if not duplicate_ids:
            return result

        core_node["members"] = self._merge_string_lists(list(core_members), [core_id])

        rewritten_edges: list[dict[str, Any]] = []
        for edge in edges:
            from_id = str(edge.get("from") or "").strip()
            to_id = str(edge.get("to") or "").strip()
            if from_id in duplicate_ids:
                from_id = core_id
            if to_id in duplicate_ids:
                to_id = core_id
            rewritten_edges = self._upsert_edge(
                rewritten_edges,
                from_id,
                to_id,
                str(edge.get("type") or "").strip(),
                str(edge.get("reason") or "Canonicalized high-level edge."),
            )

        result["nodes"] = [node for node in nodes if str(node.get("id") or "").strip() not in duplicate_ids]
        result["edges"] = rewritten_edges
        result["spine"] = [core_id if item in duplicate_ids else item for item in spine]
        deduped_spine: list[str] = []
        for item in result["spine"]:
            if item and item not in deduped_spine:
                deduped_spine.append(item)
        result["spine"] = deduped_spine
        return result

    def _prune_secondary_artifacts(self, graph: dict[str, Any]) -> dict[str, Any]:
        result = dict(graph or {})
        nodes = [dict(node) for node in result.get("nodes", []) if isinstance(node, dict)]
        primary_nodes = [node for node in nodes if str(node.get("cluster") or "primary") != "secondary"]
        valid_ids = {str(node.get("id") or "").strip() for node in primary_nodes if str(node.get("id") or "").strip()}

        pruned_edges: list[dict[str, Any]] = []
        for edge in result.get("edges", []) or []:
            if not isinstance(edge, dict):
                continue
            if str(edge.get("type") or "").strip() == "secondary":
                continue
            from_id = str(edge.get("from") or "").strip()
            to_id = str(edge.get("to") or "").strip()
            if from_id in valid_ids and to_id in valid_ids:
                pruned_edges.append(dict(edge))

        result["nodes"] = primary_nodes
        result["edges"] = pruned_edges
        result["notes"] = []
        result["spine"] = [item for item in result.get("spine", []) if str(item or "").strip() in valid_ids]
        return result

    def _validate_single_high_level_core(self, graph: dict[str, Any]) -> None:
        nodes = [node for node in graph.get("nodes", []) if isinstance(node, dict)]
        core_nodes = [node for node in nodes if str(node.get("kind") or "") == "core_transform"]
        if len(core_nodes) > 1:
            raise RuntimeError("Multiple core transforms detected after canonicalization.")

    def _merged_kind(self, left: str, right: str) -> str:
        priority = ["core_transform", "integration", "validation", "transform", "store", "target", "lookup", "exception", "source", "source_group", "lookup_group"]
        candidates = [str(left or "").strip(), str(right or "").strip()]
        for kind in priority:
            if kind in candidates:
                return kind
        return candidates[0] if candidates and candidates[0] else candidates[1] if len(candidates) > 1 else ""

    def _expand_technical_graph(self, graph: dict[str, Any], phase6: dict[str, Any], id_map: dict[str, str]) -> dict[str, Any]:
        result = dict(graph or {})
        nodes = [dict(node) for node in result.get("nodes", []) if isinstance(node, dict)]
        semantics_map: dict[str, dict[str, Any]] = {}
        for item in phase6.get("transform_semantics", []) or []:
            if not isinstance(item, dict):
                continue
            candidates = [
                self._resolve_graph_id(str(item.get("node_id") or "").strip(), id_map),
                self._resolve_graph_id(str(item.get("original_name") or "").strip(), id_map),
            ]
            for candidate in candidates:
                if candidate:
                    semantics_map[candidate] = dict(item)

        for node in nodes:
            node_id = str(node.get("id") or "").strip()
            semantic = semantics_map.get(node_id, {})
            if not semantic:
                for member in node.get("members", []) or []:
                    resolved_member = self._resolve_graph_id(str(member or "").strip(), id_map)
                    if resolved_member and resolved_member in semantics_map:
                        semantic = semantics_map[resolved_member]
                        break
            if (
                str(node.get("cluster") or "primary") != "primary"
                or str(node.get("kind") or "") not in {"core_transform", "integration", "validation"}
            ):
                continue
            if not semantic:
                continue
            node["semantics"] = self._merge_string_lists(node.get("semantics", []), semantic.get("semantics", []))
            node["rules"] = self._merge_string_lists(node.get("rules", []), semantic.get("rules", []))
            node["rule_summary"] = str(semantic.get("rule_summary") or node.get("rule_summary") or "").strip()
            node["exception_condition"] = str(semantic.get("exception_condition") or node.get("exception_condition") or "").strip()
            business_label = str(semantic.get("business_label") or "").strip()
            if business_label:
                node["business_label"] = business_label

        result["nodes"] = nodes
        return result

    def _reduce_high_level_graph(self, graph: dict[str, Any]) -> dict[str, Any]:
        result = dict(graph or {})
        nodes = [dict(node) for node in result.get("nodes", []) if isinstance(node, dict)]
        edges = [dict(edge) for edge in result.get("edges", []) if isinstance(edge, dict)]
        spine = [str(item or "").strip() for item in result.get("spine", []) if str(item or "").strip()]
        keep_ids = set(spine)
        lookup_id = next((str(node.get("id") or "").strip() for node in nodes if str(node.get("kind") or "") in {"lookup", "lookup_group"}), "")
        exception_ids = [str(node.get("id") or "").strip() for node in nodes if str(node.get("kind") or "") == "exception"]
        if lookup_id:
            keep_ids.add(lookup_id)
        keep_ids.update(item for item in exception_ids if item)

        reduced_nodes = [node for node in nodes if str(node.get("id") or "").strip() in keep_ids]
        reduced_edges: list[dict[str, Any]] = []
        for edge in edges:
            from_id = str(edge.get("from") or "").strip()
            to_id = str(edge.get("to") or "").strip()
            if from_id not in keep_ids or to_id not in keep_ids:
                continue
            reduced_edges = self._upsert_edge(
                reduced_edges,
                from_id,
                to_id,
                str(edge.get("type") or "").strip(),
                str(edge.get("reason") or "Reduced high-level edge."),
            )

        result["nodes"] = reduced_nodes
        result["edges"] = reduced_edges
        result["spine"] = [item for item in spine if item in keep_ids]
        return result

    def _fix_exception_edge_directions(self, graph: dict[str, Any]) -> dict[str, Any]:
        """Fix exception edge directions: they must flow FROM transforms TO exception handlers, not vice versa."""
        result = dict(graph or {})
        nodes = result.get("nodes", [])
        edges = result.get("edges", [])
        
        # Build node type map
        node_kinds = {node.get("id"): str(node.get("kind") or "").strip() 
                     for node in nodes if isinstance(node, dict) and node.get("id")}
        
        # Find all exception nodes
        exception_ids = {node_id for node_id, kind in node_kinds.items() if kind == "exception"}
        transform_ids = {node_id for node_id, kind in node_kinds.items() 
                        if kind in {"core_transform", "integration", "validation", "transform"}}
        
        # Use GraphNormalizer helper to fix edges
        result["edges"] = GraphNormalizer.fix_exception_flow_direction(edges or [], exception_ids, transform_ids)
        return result

    def _build_dot_payload(self, phase7: dict[str, Any]) -> dict[str, Any]:
        return {
            "high_level_dot": self._build_dot_graph(dict(phase7.get("high_level_graph") or {}), technical=False),
            "technical_dot": self._build_dot_graph(dict(phase7.get("technical_graph") or {}), technical=True),
        }

    def _build_dot_graph(self, graph: dict[str, Any], technical: bool) -> str:
        graph_name = "technical" if technical else "high_level"
        lines = [f"digraph {graph_name} {{", "  rankdir=LR;"]
        notes = [str(item or "").strip() for item in graph.get("notes", []) if str(item or "").strip()]
        # Only include secondary flow notes in high-level diagram, not technical
        if notes and not technical:
            lines.append(f"  // Secondary flows: {' | '.join(notes)}")

        primary_nodes = [node for node in graph.get("nodes", []) if isinstance(node, dict) and str(node.get("cluster") or "primary") != "secondary"]
        secondary_nodes = [node for node in graph.get("nodes", []) if isinstance(node, dict) and str(node.get("cluster") or "") == "secondary"]

        lines.extend(self._dot_cluster("cluster_primary", "Primary Flow", primary_nodes, dashed=False, technical=technical))
        # Only include secondary nodes in high-level diagram, not technical
        if not technical and secondary_nodes:
            lines.extend(self._dot_cluster("cluster_secondary", "Secondary Flows", secondary_nodes, dashed=True, technical=technical))

        for edge in graph.get("edges", []):
            if not isinstance(edge, dict):
                continue
            from_id = str(edge.get("from") or "").strip()
            to_id = str(edge.get("to") or "").strip()
            if not from_id or not to_id or from_id == to_id:
                continue
            edge_type = str(edge.get("type") or "main").strip()
            style = self._dot_edge_style(edge_type)
            attrs = [f'color="{style["color"]}"', f'style={style["style"]}', f'penwidth={style["penwidth"]}']
            if style["label"]:
                attrs.append(f'label="{style["label"]}"')
            lines.append(f"  {from_id} -> {to_id} [{', '.join(attrs)}];")

        lines.append("}")
        return "\n".join(lines)

    def _dot_cluster(self, cluster_id: str, label: str, nodes: list[dict[str, Any]], dashed: bool, technical: bool) -> list[str]:
        lines = [f"  subgraph {cluster_id} {{", '    node [fontname="Arial", fontsize=11, margin="0.2,0.1"];', f'    label="{label}";']
        if dashed:
            lines.append("    style=dashed;")
        for node in nodes:
            lines.append(f"    {str(node.get('id') or '').strip()} {self._dot_node_attrs(node, technical)};")
        lines.append("  }")
        return lines

    def _dot_node_attrs(self, node: dict[str, Any], technical: bool) -> str:
        kind = str(node.get("kind") or "").strip()
        style = self._dot_node_style(kind, str(node.get("cluster") or "primary") == "secondary")
        label = self._dot_node_label(node, technical)
        
        # Clean and deduplicate table names for display
        members = node.get("members", []) or []
        clean_members = []
        seen = set()
        
        for member in members:
            if not member:
                continue
            member_str = str(member).strip()
            if member_str and member_str not in seen:
                clean_members.append(member_str)
                seen.add(member_str)
        
        # Show top 2 clean unique table names
        display_members = clean_members[:2] if clean_members else []
        tooltip = self._dot_escape_tooltip("\n".join(display_members) if display_members else "")
        
        attrs = [
            f'shape={style["shape"]}',
            f'fillcolor="{style["fillcolor"]}"',
            f'fontcolor="{style["fontcolor"]}"',
            f'style="{style["style"]}"' if "," in style["style"] else f'style={style["style"]}',
            f"label={label}",
            f'tooltip="{tooltip}"',
        ]
        return "[" + ", ".join(attrs) + "]"

    def _dot_node_style(self, kind: str, secondary: bool) -> dict[str, str]:
        if secondary:
            return {"shape": "box", "fillcolor": "#F5F5F5", "fontcolor": "#9E9E9E", "style": "filled,dashed"}
        styles = {
            "source": {"shape": "folder", "fillcolor": "#C8E6C9", "fontcolor": "#1B5E20", "style": "filled"},
            "source_group": {"shape": "folder", "fillcolor": "#C8E6C9", "fontcolor": "#1B5E20", "style": "filled"},
            "lookup": {"shape": "note", "fillcolor": "#BBDEFB", "fontcolor": "#0D47A1", "style": "filled"},
            "lookup_group": {"shape": "note", "fillcolor": "#BBDEFB", "fontcolor": "#0D47A1", "style": "filled"},
            "integration": {"shape": "box", "fillcolor": "#FFE0B2", "fontcolor": "#E65100", "style": "filled,bold"},
            "core_transform": {"shape": "box", "fillcolor": "#FFE0B2", "fontcolor": "#E65100", "style": "filled,bold,rounded"},
            "validation": {"shape": "box", "fillcolor": "#FFF9C4", "fontcolor": "#F57F17", "style": "filled"},
            "store": {"shape": "box", "fillcolor": "#B2EBF2", "fontcolor": "#006064", "style": "filled"},
            "target": {"shape": "cylinder", "fillcolor": "#C8E6C9", "fontcolor": "#1B5E20", "style": "filled,bold"},
            "exception": {"shape": "note", "fillcolor": "#FFCDD2", "fontcolor": "#B71C1C", "style": "filled"},
        }
        return styles.get(kind, {"shape": "box", "fillcolor": "#F5F5F5", "fontcolor": "#424242", "style": "filled"})

    def _dot_edge_style(self, edge_type: str) -> dict[str, str]:
        styles = {
            "main": {"color": "#37474F", "style": "solid", "penwidth": "2.0", "label": ""},
            "lookup": {"color": "#1565C0", "style": "dashed", "penwidth": "1.5", "label": "lookup"},
            "exception": {"color": "#C62828", "style": "dashed", "penwidth": "1.5", "label": "exception"},
            "secondary": {"color": "#9E9E9E", "style": "dashed", "penwidth": "1.0", "label": ""},
        }
        return styles.get(edge_type, styles["main"])

    def _dot_node_label(self, node: dict[str, Any], technical: bool) -> str:
        title = self._dot_escape_html(str(node.get("label") or node.get("name") or self._display_name(str(node.get("id") or ""))))
        if technical:
            business = self._dot_escape_html(str(node.get("business_label") or node.get("name") or "").strip())
            semantics = self._dot_escape_html(", ".join(str(item or "") for item in node.get("semantics", []) or [] if str(item or "")))
            rules = self._dot_escape_html(" | ".join(str(item or "") for item in node.get("rules", []) or [] if str(item or "")))
            exception_condition = self._dot_escape_html(str(node.get("exception_condition") or "").strip())
            parts = [f"<B>{title}</B>"]
            if business:
                parts.append(f"<I>{business}</I>")
            if semantics:
                parts.append(f'<FONT POINT-SIZE="9">{semantics}</FONT>')
            if rules:
                parts.append(f'<FONT POINT-SIZE="9">Rule: {rules}</FONT>')
            if exception_condition:
                parts.append(f'<FONT POINT-SIZE="9">Exception: {exception_condition}</FONT>')
            return "<" + "<BR/>".join(parts) + ">"
        # Show only the primary (first) table name in the label for clean display
        members_list = node.get("members", []) or []
        primary_member = self._dot_escape_html(str(members_list[0] or "")) if members_list else ""
        parts = [f"<B>{title}</B>"]
        if primary_member:
            parts.append(f'<FONT POINT-SIZE="9">{primary_member}</FONT>')
        return "<" + "<BR/>".join(parts) + ">"

    def _dot_escape_html(self, value: str) -> str:
        text = str(value or "")
        text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return text

    def _dot_escape_tooltip(self, value: str) -> str:
        return str(value or "").replace("\\", "\\\\").replace('"', '\\"')

    def _sanitize_dot_payload(self, phase8: dict[str, Any]) -> dict[str, Any]:
        result = dict(phase8 or {})
        for key in ("high_level_dot", "technical_dot"):
            result[key] = self._sanitize_dot_text(str(result.get(key, "") or ""))
        return result

    def _sanitize_dot_text(self, dot: str) -> str:
        text = str(dot or "")
        def _tooltip_replacer(match: re.Match[str]) -> str:
            tooltip = match.group(1).replace(chr(13), "").replace(chr(10), "\\n")
            return f'tooltip="{tooltip}"'

        text = re.sub(r'tooltip="([^"]*)"', _tooltip_replacer, text, flags=re.S)
        lines = text.splitlines()
        cleaned: list[str] = []
        for line in lines:
            stripped = line.strip()
            edge_match = re.match(r"([A-Za-z0-9_]+)\s*->\s*([A-Za-z0-9_]+)", stripped)
            if edge_match and edge_match.group(1) == edge_match.group(2):
                continue
            cleaned.append(line)
        return "\n".join(cleaned)

    def _phase2_anchor_violations(self, phase2: dict[str, Any], phase15: dict[str, Any]) -> list[str]:
        violations: list[str] = []
        anchor = dict(phase2.get("anchor_chain") or {})
        locked_pairs = [
            ("canonical_source", str(phase15.get("strongest_source") or "").strip(), "canonical_source"),
            ("canonical_core_transformer", str(phase15.get("core_transform") or "").strip(), "canonical_core_transformer"),
            ("anchor_load_node", str(phase15.get("store_node") or "").strip(), "anchor_load_node"),
            ("primary_target", str(phase15.get("target_node") or "").strip(), "primary_target"),
        ]
        for anchor_key, locked_value, label in locked_pairs:
            actual_value = str(anchor.get(anchor_key) or "").strip()
            if locked_value and actual_value != locked_value:
                violations.append(f"{label} must be {locked_value!r} but was {actual_value!r}")

        if not violations:
            edges = [edge for edge in phase2.get("edges", []) if isinstance(edge, dict) and str(edge.get("type") or "") == "main"]
            main_pairs = {(str(edge.get("from") or "").strip(), str(edge.get("to") or "").strip()) for edge in edges}
            expected_chain = [
                str(phase15.get("strongest_source") or "").strip(),
                str(phase15.get("core_transform") or "").strip(),
                str(phase15.get("store_node") or "").strip(),
                str(phase15.get("target_node") or "").strip(),
            ]
            expected_chain = [node_id for node_id in expected_chain if node_id]
            for from_id, to_id in zip(expected_chain, expected_chain[1:]):
                if (from_id, to_id) not in main_pairs:
                    violations.append(f"missing locked main edge {from_id!r} -> {to_id!r}")
        return violations

    def _build_confidence_index(self, phase1: dict[str, Any]) -> dict[str, float]:
        index: dict[str, float] = {}
        for signal in phase1.get("consolidated_signals", []):
            if not isinstance(signal, dict):
                continue
            canonical = str(signal.get("canonical_name") or "").strip().upper()
            original = str(signal.get("original_name") or signal.get("name") or "").strip().upper()
            primacy = float(signal.get("primacy_score", 0.0) or 0.0)
            confidence = float(signal.get("confidence", 0.0) or 0.0)
            occurrences = int(signal.get("occurrences", 1) or 1)
            derived = max(confidence, min(1.0, max(0.0, primacy) * 0.75 + min(occurrences, 4) * 0.06 + 0.2))
            for key in [canonical, original]:
                if key:
                    index[key] = max(index.get(key, 0.0), round(derived, 2))
            for alias in signal.get("aliases", []) or []:
                alias_key = str(alias or "").strip().upper()
                if alias_key:
                    index[alias_key] = max(index.get(alias_key, 0.0), round(derived - 0.05, 2))
        return index

    def _node_confidence(self, node: dict[str, Any], confidence_index: dict[str, float], phase15: dict[str, Any]) -> float:
        keys = [str(node.get("id") or "").strip().upper()]
        keys.extend(str(item or "").strip().upper() for item in node.get("members", []) or [])
        base = 0.35
        for key in keys:
            if key:
                base = max(base, confidence_index.get(key, 0.0))
        locked_nodes = {
            str(phase15.get("strongest_source") or "").strip().upper(),
            str(phase15.get("core_transform") or "").strip().upper(),
            str(phase15.get("store_node") or "").strip().upper(),
            str(phase15.get("target_node") or "").strip().upper(),
        }
        if str(node.get("id") or "").strip().upper() in locked_nodes:
            base += 0.15
        if str(node.get("kind") or "") in {"source", "source_group", "core_transform", "store", "target"}:
            base += 0.05
        return round(min(1.0, max(0.0, base)), 2)

    def _build_graph_id_map(self, phase4: dict[str, Any], phase5: dict[str, Any], phase7: dict[str, Any]) -> dict[str, str]:
        id_map: dict[str, str] = {}
        sources = []
        sources.extend(phase4.get("normalized_components", []) or [])
        sources.extend(phase5.get("high_level_model", {}).get("nodes", []) or [])
        sources.extend(
            item for item in (phase5.get("technical_model", {}).get("nodes", []) or [])
            if isinstance(item, dict) and not str(item.get("id") or "").strip().upper().startswith("STAGE_")
        )
        sources.extend(
            item for item in (phase7.get("high_level_graph", {}).get("nodes", []) or [])
            if isinstance(item, dict) and not str(item.get("id") or "").strip().upper().startswith("STAGE_")
        )

        for item in sources:
            if not isinstance(item, dict):
                continue
            canonical_id = str(item.get("id") or "").strip()
            members = [str(member or "").strip() for member in item.get("members", []) or [] if str(member or "").strip()]
            if not canonical_id and members:
                canonical_id = self._semantic_id_from_raw(members[0])
            if not canonical_id:
                continue
            if canonical_id.upper().startswith("STAGE_") and members:
                canonical_id = self._semantic_id_from_raw(members[0])
            id_map.setdefault(canonical_id, canonical_id)
            for member in members:
                id_map.setdefault(member, canonical_id)
                stripped = self._strip_stage_prefix(member)
                if stripped:
                    id_map.setdefault(stripped, canonical_id)
        return id_map

    def _upsert_edge(self, edges: list[dict[str, Any]], from_id: str, to_id: str, edge_type: str, reason: str) -> list[dict[str, Any]]:
        if not from_id or not to_id or from_id == to_id:
            return edges
        for edge in edges:
            if str(edge.get("from") or "").strip() == from_id and str(edge.get("to") or "").strip() == to_id and str(edge.get("type") or "").strip() == edge_type:
                edge["reason"] = reason
                return edges
        edges.append({"from": from_id, "to": to_id, "type": edge_type, "reason": reason})
        return edges

    def _node_type(self, nodes: list[dict[str, Any]], node_id: str) -> str:
        for node in nodes:
            if str(node.get("id") or "").strip() == node_id:
                return str(node.get("type") or node.get("kind") or "").strip()
        return ""

    def _upsert_graph_node(self, nodes: list[dict[str, Any]], node_id: str, kind: str, cluster: str | None) -> list[dict[str, Any]]:
        if not node_id:
            return nodes
        for node in nodes:
            if str(node.get("id") or "").strip() == node_id:
                node["kind"] = kind
                node["label"] = node.get("label") or node.get("name") or self._display_name(node_id)
                node["name"] = node.get("name") or node.get("label") or self._display_name(node_id)
                members = node.get("members", [])
                if node_id not in members:
                    node["members"] = [node_id] + [item for item in members if item != node_id]
                if cluster is not None:
                    node["cluster"] = cluster
                return nodes
        item = {
            "id": node_id,
            "kind": kind,
            "label": self._display_name(node_id),
            "name": self._display_name(node_id),
            "members": [node_id],
        }
        if cluster is not None:
            item["cluster"] = cluster
        nodes.append(item)
        return nodes

    def _normalize_graph_nodes(self, nodes: Any, id_map: dict[str, str]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for raw_node in nodes or []:
            if not isinstance(raw_node, dict):
                continue
            node = dict(raw_node)
            node_id = self._resolve_graph_id(str(node.get("id") or "").strip(), id_map)
            if not node_id:
                continue
            merged_members = []
            for member in node.get("members", []) or []:
                member_text = str(member or "").strip()
                if member_text:
                    merged_members.append(member_text)
            raw_id = str(raw_node.get("id") or "").strip()
            if raw_id and raw_id not in merged_members and raw_id != node_id:
                merged_members.insert(0, raw_id)
            label = str(node.get("label") or node.get("name") or self._display_name(node_id)).strip()
            existing = next((item for item in normalized if str(item.get("id") or "").strip() == node_id), None)
            if existing is None:
                item = {
                    "id": node_id,
                    "kind": str(node.get("kind") or "").strip(),
                    "label": label or self._display_name(node_id),
                    "name": str(node.get("name") or label or self._display_name(node_id)).strip(),
                    "members": self._merge_string_lists([], merged_members),
                }
                if node.get("cluster") is not None:
                    item["cluster"] = node.get("cluster")
                if node.get("business_label") is not None:
                    item["business_label"] = node.get("business_label")
                if node.get("semantics") is not None:
                    item["semantics"] = list(node.get("semantics") or [])
                if node.get("rules") is not None:
                    item["rules"] = list(node.get("rules") or [])
                if node.get("rule_summary") is not None:
                    item["rule_summary"] = node.get("rule_summary")
                if node.get("exception_condition") is not None:
                    item["exception_condition"] = node.get("exception_condition")
                normalized.append(item)
                continue
            if node.get("kind") and not existing.get("kind"):
                existing["kind"] = node.get("kind")
            if len(str(label or "")) > len(str(existing.get("label") or "")):
                existing["label"] = label
            if len(str(node.get("name") or "")) > len(str(existing.get("name") or "")):
                existing["name"] = str(node.get("name") or "").strip()
            existing["members"] = self._merge_string_lists(existing.get("members", []), merged_members)
            if node.get("cluster") == "primary" or not existing.get("cluster"):
                existing["cluster"] = node.get("cluster")
            if node.get("business_label") and not existing.get("business_label"):
                existing["business_label"] = node.get("business_label")
            if node.get("semantics"):
                existing["semantics"] = self._merge_string_lists(existing.get("semantics", []), node.get("semantics", []))
            if node.get("rules"):
                existing["rules"] = self._merge_string_lists(existing.get("rules", []), node.get("rules", []))
            if node.get("rule_summary") and not existing.get("rule_summary"):
                existing["rule_summary"] = node.get("rule_summary")
            if node.get("exception_condition") and not existing.get("exception_condition"):
                existing["exception_condition"] = node.get("exception_condition")
        return normalized

    def _normalize_graph_edges(self, edges: Any, nodes: list[dict[str, Any]], locked_store: str, locked_target: str, id_map: dict[str, str]) -> list[dict[str, Any]]:
        # Build set of valid node IDs to prevent dangling edges
        valid_node_ids = {node.get("id") for node in nodes if isinstance(node, dict) and node.get("id")}
        
        normalized: list[dict[str, Any]] = []
        for raw_edge in edges or []:
            if not isinstance(raw_edge, dict):
                continue
            edge = dict(raw_edge)
            from_id = self._resolve_graph_id(str(edge.get("from") or "").strip(), id_map)
            to_id = self._resolve_graph_id(str(edge.get("to") or "").strip(), id_map)
            edge_type = str(edge.get("type") or "").strip()
            if not from_id or not to_id or from_id == to_id:
                continue
            
            # Skip edges to non-existent nodes (dangling edges)
            if to_id not in valid_node_ids:
                continue
                
            if locked_store and locked_target and edge_type == "main" and to_id == locked_target and from_id != locked_store:
                edge["to"] = locked_store
            if edge_type == "secondary" and self._node_type(nodes, from_id) == "lookup":
                edge["type"] = "lookup"
            normalized = self._upsert_edge(normalized, from_id, to_id, str(edge.get("type") or ""), str(edge.get("reason") or "Normalized graph edge."))
        return normalized

    def _resolve_graph_id(self, node_id: str, id_map: dict[str, str]) -> str:
        text = str(node_id or "").strip()
        if not text:
            return ""
        if text in id_map:
            return id_map[text]
        stripped = self._strip_stage_prefix(text)
        if stripped in id_map:
            return id_map[stripped]
        if text.upper().startswith("STAGE_"):
            return self._semantic_id_from_raw(text)
        return text

    def _strip_stage_prefix(self, node_id: str) -> str:
        text = str(node_id or "").strip()
        if text.upper().startswith("STAGE_"):
            return text[6:]
        return text

    def _semantic_id_from_raw(self, node_id: str) -> str:
        text = self._strip_stage_prefix(node_id)
        text = re.sub(r"[^A-Za-z0-9_]+", "_", text).strip("_")
        return text.upper()

    def _display_name(self, node_id: str) -> str:
        return self._strip_stage_prefix(str(node_id or "")).replace("_", " ").strip().title() or "Unnamed Node"

    def _invoke_json(self, prompt: str) -> dict[str, Any]:
        response = self.llm.invoke(prompt)
        content = response.content if hasattr(response, "content") else str(response)
        payload = extract_json(content)
        if not isinstance(payload, dict):
            raise RuntimeError("LLM did not return a valid JSON object.")
        return payload

    def _consolidate_phase1_signals(self, chunk_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for chunk_payload in chunk_signals:
            for signal in chunk_payload.get("signals", []):
                if not isinstance(signal, dict):
                    continue
                name = str(signal.get("name") or "").strip()
                if not name:
                    continue
                cleaned_operations = self._filter_noise_operations(signal.get("operations", []))
                if not self._should_keep_signal(name, signal, cleaned_operations):
                    continue
                key = self._signal_identity(name, signal)
                if key not in merged:
                    merged[key] = {
                        "name": name,
                        "original_name": name,
                        "canonical_name": self._canonical_name(name, signal),
                        "canonical_hint": str(signal.get("canonical_hint") or "").strip(),
                        "business_name": "",
                        "aliases": [],
                        "kind": str(signal.get("kind") or "").strip(),
                        "stage_type": str(signal.get("stage_type") or "").strip(),
                        "tables": [],
                        "operations": [],
                        "hints": [],
                        "primary_role": str(signal.get("primary_role") or "").strip(),
                        "secondary_roles": [],
                        "confidence": float(signal.get("confidence", 0.0) or 0.0),
                        "primacy_score": float(signal.get("primacy_score", 0.0) or 0.0),
                        "occurrences": 0,
                    }
                target = merged[key]
                target["aliases"] = self._merge_string_lists(target["aliases"], [name])
                target["kind"] = self._prefer_kind(target["kind"], str(signal.get("kind") or "").strip())
                target["stage_type"] = target["stage_type"] or str(signal.get("stage_type") or "").strip()
                if not target.get("canonical_hint"):
                    target["canonical_hint"] = str(signal.get("canonical_hint") or "").strip()
                target["tables"] = self._merge_string_lists(target["tables"], signal.get("tables", []))
                target["operations"] = self._merge_string_lists(target["operations"], cleaned_operations)
                target["hints"] = self._merge_string_lists(target["hints"], signal.get("hints", []))
                target["primary_role"], target["secondary_roles"] = self._merge_roles(
                    target["primary_role"],
                    target["secondary_roles"],
                    str(signal.get("primary_role") or "").strip(),
                    signal.get("secondary_roles", []),
                )
                target["confidence"] = max(target["confidence"], float(signal.get("confidence", 0.0) or 0.0))
                target["primacy_score"] = max(target["primacy_score"], float(signal.get("primacy_score", 0.0) or 0.0))
                target["occurrences"] += 1
        consolidated = [self._normalize_consolidated_signal(item) for item in merged.values()]
        consolidated = self._collapse_exception_signals(consolidated)
        consolidated = self._filter_target_signals(consolidated)
        return sorted(consolidated, key=lambda item: (-item["occurrences"], item["canonical_name"].lower()))

    def _signal_identity(self, name: str, signal: dict[str, Any]) -> str:
        return self._entity_merge_key(self._canonical_name(name, signal), signal)

    def _merge_string_lists(self, current: list[str], incoming: Any) -> list[str]:
        results = list(current)
        for item in incoming or []:
            value = str(item or "").strip()
            if value and value not in results:
                results.append(value)
        return results

    def _prefer_kind(self, left: str, right: str) -> str:
        rank = {
            "source": 1,
            "transform": 2,
            "lookup": 3,
            "store": 4,
            "target": 5,
            "exception": 6,
            "ambiguous": 99,
        }
        left_value = rank.get(str(left or "").strip(), 50)
        right_value = rank.get(str(right or "").strip(), 50)
        return left if left_value <= right_value else right

    def _canonical_name(self, name: str, signal: dict[str, Any]) -> str:
        tables = [str(item or "").strip() for item in signal.get("tables", []) if str(item or "").strip()]
        if tables:
            table_name = tables[0].split(".")[-1]
            return self._normalize_entity_label(table_name, signal)
        text = str(name or "").strip().upper()
        text = re.sub(r"^STAGE_", "", text)
        text = re.sub(r"^(LOAD_|READ_|WRITE_|EXTRACT_|FETCH_|INSERT_|OUTPUT_)", "", text)
        text = re.sub(r"(_SOURCE|_TARGET|_OUTPUT|_CONNECTOR|_STAGE)$", "", text)
        return self._normalize_entity_label(text or str(name or "").strip().upper(), signal)

    def _normalize_entity_label(self, value: str, signal: dict[str, Any]) -> str:
        text = str(value or "").strip().upper()
        text = re.sub(r"^[A-Z0-9]+[._]", "", text) if text.count(".") >= 2 else text
        text = re.sub(r"^(EXTRACT_|FETCH_|READ_|WRITE_|LOAD_|INSERT_|SELECT_|SOURCE_|TARGET_)", "", text)
        text = re.sub(r"^(TRANSFORMED_|PROCESSED_)", "", text)
        text = re.sub(r"(_SOURCE|_TARGET|_OUTPUT|_CONNECTOR|_RESULT|_RECORDS?)$", "", text)
        stage_type = str(signal.get("stage_type") or "").upper()
        if "SEQUENTIAL" not in stage_type and not text.endswith("_FILE"):
            text = re.sub(r"_DATA$", "", text)
        text = re.sub(r"__+", "_", text).strip("_")
        return text or "UNNAMED_ENTITY"

    def _entity_merge_key(self, canonical_name: str, signal: dict[str, Any]) -> str:
        key = str(canonical_name or "").upper()
        stage_type = str(signal.get("stage_type") or "").upper()
        if "SEQUENTIAL" not in stage_type and "HASHEDFILE" not in stage_type:
            key = re.sub(r"_DATA$", "", key)
        key = re.sub(r"^(EXTRACT_|FETCH_|READ_|WRITE_|LOAD_|INSERT_)", "", key)
        key = re.sub(r"__+", "_", key).strip("_")
        return key

    def _business_name(self, canonical_name: str, signal: dict[str, Any]) -> str:
        base = str(canonical_name or "").strip().upper()
        kind = str(signal.get("kind") or "").strip()
        special = {
            "LNK_VEHICLE_OFF_ROAD": "Vehicle Off Road Integration",
            "TRANSFORMER_DATA_PROCESSING": "Vehicle Off Road Data Processing",
            "DATA_TRANSFORMATION": "Business Rules Transformation",
            "DATA_QUALITY_CHECK": "Data Quality Validation",
            "EXCEPTION_OUTPUT": "Exception Output",
            "HF_FACT_VOR": "VOR Fact Store",
            "HF_FACT_VOR_DATA": "VOR Fact Store",
            "STG_VEHICLE_OFF_ROAD_FACT": "Vehicle Off Road Staging Target",
            "VOR_FACT_TABLE": "Vehicle Off Road Final Target",
            "VOR_VEHICLE_OFF_ROAD": "Vehicle Off Road Source",
        }
        if base in special:
            return special[base]
        label = base.replace("_", " ").strip().title()
        if kind == "source" and not label.endswith("Source"):
            return f"{label} Source"
        if kind == "lookup" and not label.endswith("Lookup"):
            return f"{label} Lookup"
        if kind == "store" and not label.endswith("Store"):
            return f"{label} Store"
        if kind == "target" and "Target" not in label:
            return f"{label} Target"
        if kind == "exception" and "Exception" not in label:
            return f"{label} Exception"
        if kind == "transform" and all(token not in label for token in ("Transform", "Validation", "Integration", "Routing")):
            return f"{label} Transform"
        return label

    def _normalize_consolidated_signal(self, signal: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(signal)
        normalized["aliases"] = self._merge_string_lists([], normalized.get("aliases", []))
        normalized["tables"] = self._merge_string_lists([], normalized.get("tables", []))
        normalized["operations"] = self._merge_string_lists([], normalized.get("operations", []))
        normalized["hints"] = self._merge_string_lists([], normalized.get("hints", []))

        canonical_name = str(normalized.get("canonical_name") or normalized.get("name") or "").strip().upper()
        stage_type = str(normalized.get("stage_type") or "").strip().upper()
        operations = [str(item or "").strip().upper() for item in normalized.get("operations", [])]
        hints = [str(item or "").strip().upper() for item in normalized.get("hints", [])]
        sample = " ".join(
            [
                canonical_name,
                str(normalized.get("name") or "").upper(),
                stage_type,
                " ".join(str(item or "").upper() for item in normalized.get("tables", [])),
                " ".join(operations),
                " ".join(hints),
            ]
        )

        resolved_kind = str(normalized.get("kind") or "").strip() or "ambiguous"
        semantic_type = "dataset"
        if any(token in stage_type for token in ("TRANSFORMER", "MODIFY", "SWITCH", "JOIN", "AGG", "REM", "COPY")):
            semantic_type = "transform"
        elif any(token in stage_type for token in ("SEQFILE", "SEQUENTIALFILE", "ORACLECONNECTOR", "CONNECTOR", "HASHEDFILE", "LOOKUP")):
            semantic_type = "dataset"

        if "EXCEPTION" in sample or "REJECT" in sample or "ERROR_LOG" in sample:
            resolved_kind = "exception"
        elif any(token in sample for token in ("TARGET_SCHEMA", "STG_VEHICLE_OFF_ROAD_FACT", "LOAD_TO_STAGING_TABLE", "INSERT_TO_STAGING_TABLE")):
            resolved_kind = "target"
        elif "HASHEDFILE" in stage_type or canonical_name.startswith("HF_"):
            lookup_markers = ("LOOKUP", "ENRICH", "RESOLVE_", "READ_", "CACHE_", "INDEX", "KEY_BY", "DIMENSION")
            store_markers = ("WRITE_", "STORE_", "INSERT", "OUTPUT", "PERSIST")
            has_lookup_behavior = any(token in sample for token in lookup_markers) or any("LOAD_LOOKUP" in token for token in operations)
            has_store_behavior = any(token in sample for token in store_markers)
            if any(token.startswith("LOAD_") for token in operations) and not has_lookup_behavior:
                has_store_behavior = True
            if has_store_behavior and not has_lookup_behavior:
                resolved_kind = "store"
            elif has_lookup_behavior and not has_store_behavior:
                resolved_kind = "lookup"
            elif has_lookup_behavior and has_store_behavior:
                resolved_kind = "store"
        elif "LOOKUP" in sample and resolved_kind not in ("store", "target"):
            resolved_kind = "lookup"
        elif "TRANSFORM" in sample or "JOIN" in sample or "REMDUP" in sample or "REMOVE_DUPLICATES" in sample:
            resolved_kind = "transform"
        elif "SEQUENTIALFILE" in stage_type or "ORACLECONNECTOR" in stage_type or "CONNECTOR" in stage_type:
            if resolved_kind not in ("target", "exception"):
                resolved_kind = "source"

        explicit_source_evidence = any(
            token in sample for token in ("EXTRACT_", "READ_", "SOURCE", "SELECT", "ORACLECONNECTOR", "SEQUENTIALFILE")
        )
        explicit_lookup_evidence = any(
            token in sample for token in ("LEFT JOIN", "LEFT OUTER JOIN", "LOOKUP", "ENRICH", "RESOLVE_")
        )
        if explicit_source_evidence and resolved_kind == "lookup":
            if not any(token in sample for token in ("DIMENSION_TABLE", "LOOKUPSTAGE", "HASHEDFILE")):
                resolved_kind = "source"

        if resolved_kind == "exception" and semantic_type == "transform":
            resolved_kind = "transform"
            normalized["secondary_roles"] = self._merge_string_lists(normalized.get("secondary_roles", []), ["validation", "routing"])
        if semantic_type == "dataset" and resolved_kind == "transform":
            if "LOOKUP" in sample or "JOIN" in " ".join(operations):
                resolved_kind = "lookup"
            elif any(token in sample for token in ("DIM", "TABLE", "FILE", "OWNER.", "SCHEMA.")):
                resolved_kind = "source"

        generic_name = str(normalized.get("name") or "").strip().upper()
        if generic_name in ("TARGET", "ERROR_HANDLING", "JOB_COMPLETION_HANDLER"):
            if resolved_kind == "target" and normalized.get("tables"):
                pass
            elif resolved_kind == "exception":
                normalized["secondary_roles"] = self._merge_string_lists(normalized.get("secondary_roles", []), ["routing"])
            elif resolved_kind == "source":
                resolved_kind = "transform"
                normalized["secondary_roles"] = self._merge_string_lists(normalized.get("secondary_roles", []), ["routing"])

        normalized["kind"] = resolved_kind
        normalized["semantic_type"] = semantic_type
        normalized["canonical_name"] = self._entity_merge_key(canonical_name, normalized)
        normalized["canonical_hint"] = str(normalized.get("canonical_hint") or normalized["canonical_name"]).strip()
        normalized["business_name"] = self._business_name(normalized["canonical_name"], normalized)
        normalized["aliases"] = [
            alias for alias in normalized["aliases"]
            if alias and self._entity_merge_key(alias, normalized) != normalized["canonical_name"]
        ]

        primary_role = str(normalized.get("primary_role") or "").strip()
        if resolved_kind == "lookup":
            primary_role = "lookup_enrichment"
            normalized["secondary_roles"] = [role for role in normalized.get("secondary_roles", []) if role != "integration"]
        elif resolved_kind == "store":
            if "lookup_enrichment" in normalized.get("secondary_roles", []) and primary_role in ("lookup_enrichment", ""):
                primary_role = "routing"
            elif not primary_role:
                primary_role = "routing"
        elif resolved_kind == "target" and not primary_role:
            primary_role = "routing"
        elif resolved_kind == "exception":
            primary_role = "validation"
        elif resolved_kind == "source" and not primary_role:
            primary_role = "integration"
        normalized["primary_role"] = primary_role
        normalized["secondary_roles"] = [
            role for role in normalized.get("secondary_roles", [])
            if role and role != primary_role
        ]
        return normalized

    def _filter_noise_operations(self, operations: Any) -> list[str]:
        filtered = []
        for item in operations or []:
            value = str(item or "").strip()
            upper = value.upper()
            if not value:
                continue
            if upper.startswith(("CLOSE_", "OPEN_", "FETCH_", "INIT_")):
                continue
            filtered.append(value)
        return filtered

    def _should_keep_signal(self, name: str, signal: dict[str, Any], operations: list[str]) -> bool:
        text = " ".join(
            [
                str(name or ""),
                str(signal.get("kind") or ""),
                str(signal.get("stage_type") or ""),
                " ".join(str(item or "") for item in signal.get("tables", [])),
                " ".join(operations),
            ]
        ).upper()
        if not operations and any(token in text for token in ("CLOSE_", "OPEN_", "FETCH_", "INIT_")):
            return False
        if "REMOVE_DUPLICATES" in text or "REMDUP" in text:
            signal["kind"] = "transform"
        if "STG_VEHICLE_OFF_ROAD_FACT" in text and any(token in text for token in ("LOAD", "INSERT", "TARGET", "TRUNCATE")):
            signal["kind"] = "target"
        if "EXCEPTION_SOURCE" in text and "READ_" in text:
            signal["kind"] = "exception"
        return True

    def _merge_roles(self, existing_primary: str, existing_secondary: list[str], new_primary: str, new_secondary: Any) -> tuple[str, list[str]]:
        roles = []
        for value in [existing_primary, new_primary]:
            text = str(value or "").strip()
            if text and text not in roles:
                roles.append(text)
        for value in existing_secondary or []:
            text = str(value or "").strip()
            if text and text not in roles:
                roles.append(text)
        for value in new_secondary or []:
            text = str(value or "").strip()
            if text and text not in roles:
                roles.append(text)
        if not roles:
            return "", []
        role_rank = {"integration": 1, "core": 2, "lookup_enrichment": 3, "cleanse": 4, "validation": 5, "routing": 6}
        primary = sorted(roles, key=lambda item: role_rank.get(item, 99))[0]
        secondary = [item for item in roles if item != primary]
        return primary, secondary

    def _collapse_exception_signals(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        exception_bucket = None
        results = []
        for signal in signals:
            if signal.get("kind") != "exception":
                results.append(signal)
                continue
            sample = " ".join(
                [
                    str(signal.get("name", "")),
                    str(signal.get("stage_type", "")),
                    " ".join(signal.get("tables", [])),
                    " ".join(signal.get("operations", [])),
                ]
            ).upper()
            is_output = any(token in sample for token in ("OUTPUT", "REJECT", "ERROR_LOG", "EXCEPTION_FILE", "WRITE_", "SEQFILE", "FILE_WRITER"))
            if not is_output:
                transform_copy = dict(signal)
                transform_copy["kind"] = "transform"
                transform_copy["primary_role"] = "validation"
                transform_copy["secondary_roles"] = self._merge_string_lists(transform_copy.get("secondary_roles", []), ["routing"])
                results.append(transform_copy)
                continue
            if exception_bucket is None:
                exception_bucket = {
                    "name": "Exception Output",
                    "original_name": "Exception Output",
                    "canonical_name": "EXCEPTION_OUTPUT",
                    "canonical_hint": "EXCEPTION_OUTPUT",
                    "business_name": "Exception Output",
                    "aliases": [],
                    "kind": "exception",
                    "stage_type": "ExceptionOutput",
                    "semantic_type": "dataset",
                    "tables": [],
                    "operations": [],
                    "hints": [],
                    "primary_role": "validation",
                    "secondary_roles": [],
                    "confidence": 0.0,
                    "primacy_score": 0.0,
                    "occurrences": 0,
                }
            exception_bucket["aliases"] = self._merge_string_lists(exception_bucket["aliases"], signal.get("aliases", []) or [signal.get("name", "")])
            exception_bucket["tables"] = self._merge_string_lists(exception_bucket["tables"], signal.get("tables", []))
            exception_bucket["operations"] = self._merge_string_lists(exception_bucket["operations"], signal.get("operations", []))
            exception_bucket["hints"] = self._merge_string_lists(exception_bucket["hints"], signal.get("hints", []))
            exception_bucket["confidence"] = max(exception_bucket["confidence"], float(signal.get("confidence", 0.0) or 0.0))
            exception_bucket["primacy_score"] = max(exception_bucket["primacy_score"], float(signal.get("primacy_score", 0.0) or 0.0))
            exception_bucket["occurrences"] += int(signal.get("occurrences", 1) or 1)
        if exception_bucket:
            results.append(exception_bucket)
        return results

    def _filter_target_signals(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        targets = [signal for signal in signals if signal.get("kind") == "target"]
        non_targets = [signal for signal in signals if signal.get("kind") != "target"]
        if not targets:
            return signals
        preferred_targets = []
        for signal in targets:
            sample = " ".join(
                [
                    signal.get("name", ""),
                    signal.get("stage_type", ""),
                    " ".join(signal.get("tables", [])),
                    " ".join(signal.get("operations", [])),
                ]
            ).upper()
            if "ORACLE" in sample or "TARGET_SCHEMA" in sample or "STG_VEHICLE_OFF_ROAD_FACT" in sample:
                preferred_targets.append(signal)
        if not preferred_targets:
            preferred_targets = targets[:1]
        return non_targets + preferred_targets

    def _build_authoritative_inventory(self, signals: list[dict[str, Any]]) -> dict[str, Any]:
        inventory: dict[str, list[dict[str, Any]]] = {
            "sources": [],
            "transforms": [],
            "lookups": [],
            "stores": [],
            "targets": [],
            "exceptions": [],
        }
        plural_map = {
            "source": "sources",
            "transform": "transforms",
            "lookup": "lookups",
            "store": "stores",
            "target": "targets",
            "exception": "exceptions",
        }
        for signal in signals:
            bucket = plural_map.get(str(signal.get("kind") or "").strip())
            if not bucket:
                continue
            inventory[bucket].append(
                {
                    "id": signal.get("canonical_name"),
                    "name": signal.get("canonical_name"),
                    "business_name": signal.get("business_name"),
                    "original_name": signal.get("original_name"),
                    "canonical_hint": signal.get("canonical_hint"),
                    "confidence": signal.get("confidence"),
                    "primacy_score": signal.get("primacy_score"),
                    "aliases": signal.get("aliases", []),
                    "stage_type": signal.get("stage_type"),
                    "semantic_type": signal.get("semantic_type"),
                    "tables": signal.get("tables", []),
                    "primary_role": signal.get("primary_role"),
                    "secondary_roles": signal.get("secondary_roles", []),
                }
            )
        return inventory


    def _build_result(self, context: AgentContext, pdf_outputs: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
        run_dir = Path(context.run_dir)
        high_level_dot_path = run_dir / "high_level.dot"
        technical_dot_path = run_dir / "technical.dot"
        pdf_outputs = pdf_outputs or {}
        high_level_pdf = dict(pdf_outputs.get("high_level") or {})
        technical_pdf = dict(pdf_outputs.get("technical") or {})
        warnings = [
            warning
            for warning in [
                str(high_level_pdf.get("warning") or "").strip(),
                str(technical_pdf.get("warning") or "").strip(),
            ]
            if warning
        ]
        return {
            "agent_name": "LayeredLineageAgent",
            "run_id": context.run_id,
            "input_file": context.input_file,
            "run_dir": context.run_dir,
            "output_dir": context.run_dir,
            "chunk_count": len(context.preprocessed_context["chunks"]),
            "job_name_hints": context.preprocessed_context["job_name_hints"],
            "dot_files": [str(high_level_dot_path), str(technical_dot_path)],
            "pdf_files": [
                path
                for path in [
                    str(high_level_pdf.get("pdf_path") or "").strip(),
                    str(technical_pdf.get("pdf_path") or "").strip(),
                ]
                if path
            ],
            "pdf_generated": bool(high_level_pdf.get("pdf_generated")) or bool(technical_pdf.get("pdf_generated")),
            "warnings": warnings,
        }

    def _get_llm(self, model_override: str):
        if AzureChatOpenAI is None:
            raise RuntimeError("langchain-openai is not installed.")
        deployment = (
            os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
            or os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_NAME")
            or os.getenv("AZURE_OPENAI_MODEL_DEPLOYMENT")
        )
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        api_key = os.getenv("AZURE_OPENAI_API_KEY")
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        if not all([deployment, endpoint, api_key, api_version]):
            raise RuntimeError("Missing Azure OpenAI configuration in .env.")
        kwargs = {
            "azure_deployment": deployment,
            "azure_endpoint": endpoint,
            "api_key": api_key,
            "openai_api_version": api_version,
            "temperature": float(os.getenv("LINEAGE_LLM_TEMPERATURE", "0")),
        }
        if model_override:
            kwargs["model"] = model_override
        return AzureChatOpenAI(**kwargs)


def run_pipeline(input_path: str, output_dir: str, model_override: str = "") -> dict[str, Any]:
    config = AgentConfig(input_path=input_path, output_dir=output_dir, model_override=model_override)
    return LayeredLineageAgent(config).run()
