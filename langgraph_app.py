"""
LangGraph Application
Implements the workflow for automated 3NF normalization using LangGraph
"""

from typing import TypedDict, Dict, Any, List
from langgraph.graph import StateGraph, END
from pathlib import Path

from metadata_extractor import MetadataExtractor
from auto_profiler import AutoProfiler
from fk_detector import ForeignKeyDetector
from normalizer import Normalizer
from sql_generator import SQLGenerator
from schema_validator import SchemaValidator


# Define the state structure
class NormalizationState(TypedDict):
    """State that flows through the LangGraph workflow"""
    input_folder: str
    output_folder: str
    sql_output_folder: str
    erd_output_folder: str
    
    # Data collected during workflow
    metadata: Dict[str, Any]
    profiles: Dict[str, Any]
    foreign_keys: List[Dict[str, Any]]
    normalized_tables: Dict[str, Any]
    sql_script_path: str
    erd_path: str
    
    # Status and logs
    status: str
    errors: List[str]
    logs: List[str]


class NormalizationWorkflow:
    """LangGraph workflow for automated 3NF normalization"""
    
    def __init__(self):
        self.graph = self._build_graph()
        
    def load_files_node(self, state: NormalizationState) -> NormalizationState:
        """Node 1: Load all CSV/JSON files from input folder"""
        print("\n" + "="*70)
        print("NODE 1: LOADING FILES")
        print("="*70)
        
        try:
            input_folder = state['input_folder']
            
            # Count files
            input_path = Path(input_folder)
            csv_files = list(input_path.glob('*.csv'))
            json_files = list(input_path.glob('*.json'))
            total_files = len(csv_files) + len(json_files)
            
            state['logs'].append(f"Found {total_files} files ({len(csv_files)} CSV, {len(json_files)} JSON)")
            print(f"Found {total_files} files to process")
            
            state['status'] = 'files_loaded'
            
        except Exception as e:
            error_msg = f"Error loading files: {str(e)}"
            state['errors'].append(error_msg)
            state['status'] = 'error'
            print(f"❌ {error_msg}")
        
        return state
    
    def extract_metadata_node(self, state: NormalizationState) -> NormalizationState:
        """Node 2: Extract metadata from all files"""
        print("\n" + "="*70)
        print("NODE 2: EXTRACTING METADATA")
        print("="*70)
        
        try:
            extractor = MetadataExtractor()
            metadata = extractor.extract_all_metadata(state['input_folder'])
            
            state['metadata'] = metadata
            state['logs'].append(f"Extracted metadata from {len(metadata)} tables")
            print(extractor.get_metadata_summary())
            
            state['status'] = 'metadata_extracted'
            
        except Exception as e:
            error_msg = f"Error extracting metadata: {str(e)}"
            state['errors'].append(error_msg)
            state['status'] = 'error'
            print(f"❌ {error_msg}")
        
        return state
    
    def profile_node(self, state: NormalizationState) -> NormalizationState:
        """Node 3: Profile data for dependencies and normalization issues"""
        print("\n" + "="*70)
        print("NODE 3: PROFILING DATA")
        print("="*70)
        
        try:
            profiler = AutoProfiler(state['metadata'])
            profiles = profiler.profile_all_tables()
            
            state['profiles'] = profiles
            state['logs'].append(f"Profiled {len(profiles)} tables")
            print(profiler.get_profile_summary())
            
            state['status'] = 'profiled'
            
        except Exception as e:
            error_msg = f"Error profiling data: {str(e)}"
            state['errors'].append(error_msg)
            state['status'] = 'error'
            print(f"❌ {error_msg}")
        
        return state
    
    def detect_primary_keys_node(self, state: NormalizationState) -> NormalizationState:
        """Node 4: Detect primary keys (already done in profiling, this is for explicit tracking)"""
        print("\n" + "="*70)
        print("NODE 4: PRIMARY KEY DETECTION")
        print("="*70)
        
        try:
            pk_count = 0
            for table_name, profile in state['profiles'].items():
                if profile.get('primary_key'):
                    pk = profile['primary_key']
                    pk_str = ', '.join(pk)
                    print(f"  ✓ {table_name}: PK = ({pk_str})")
                    pk_count += 1
                else:
                    print(f"  ⚠ {table_name}: No natural PK (will use surrogate key)")
            
            state['logs'].append(f"Detected {pk_count} natural primary keys")
            state['status'] = 'primary_keys_detected'
            
        except Exception as e:
            error_msg = f"Error detecting primary keys: {str(e)}"
            state['errors'].append(error_msg)
            state['status'] = 'error'
            print(f"❌ {error_msg}")
        
        return state
    
    def detect_foreign_keys_node(self, state: NormalizationState) -> NormalizationState:
        """Node 5: Detect foreign key relationships"""
        print("\n" + "="*70)
        print("NODE 5: FOREIGN KEY DETECTION")
        print("="*70)
        
        try:
            fk_detector = ForeignKeyDetector(state['metadata'], state['profiles'])
            foreign_keys = fk_detector.detect_all_foreign_keys()
            
            state['foreign_keys'] = foreign_keys
            state['logs'].append(f"Detected {len(foreign_keys)} foreign key relationships")
            print(fk_detector.get_fk_summary())
            
            state['status'] = 'foreign_keys_detected'
            
        except Exception as e:
            error_msg = f"Error detecting foreign keys: {str(e)}"
            state['errors'].append(error_msg)
            state['status'] = 'error'
            print(f"❌ {error_msg}")
        
        return state
    
    def normalize_3nf_node(self, state: NormalizationState) -> NormalizationState:
        """Node 6: Perform 3NF normalization"""
        print("\n" + "="*70)
        print("NODE 6: 3NF NORMALIZATION")
        print("="*70)
        
        try:
            normalizer = Normalizer(state['metadata'], state['profiles'], state['foreign_keys'])
            normalized_tables = normalizer.normalize_all_tables()
            
            state['normalized_tables'] = normalized_tables
            state['logs'].append(f"Normalized into {len(normalized_tables)} tables")
            print(normalizer.get_normalization_summary())
            
            # Export normalized tables
            normalizer.export_normalized_tables(state['output_folder'])
            state['logs'].append(f"Exported normalized tables to {state['output_folder']}")
            
            state['status'] = 'normalized'
            
        except Exception as e:
            error_msg = f"Error normalizing tables: {str(e)}"
            state['errors'].append(error_msg)
            state['status'] = 'error'
            print(f"❌ {error_msg}")
        
        return state
    
    def generate_sql_node(self, state: NormalizationState) -> NormalizationState:
        """Node 7: Validate normalized schema"""
        print("\n" + "="*70)
        print("NODE 7: SCHEMA VALIDATION")
        print("="*70)
        
        try:
            validator = SchemaValidator(
                state['normalized_tables'],
                state['profiles'],
                state['foreign_keys']
            )
            
            validation_results = validator.validate_all()
            
            # Store validation results
            state['logs'].append(f"Schema validation: {'PASSED' if validation_results['is_valid'] else 'FAILED'}")
            
            if not validation_results['is_valid']:
                for error in validation_results['errors']:
                    state['errors'].append(error)
                
                # Continue anyway but log warnings
                print("\n⚠ Schema has validation issues, but continuing with SQL generation...")
            
            state['status'] = 'schema_validated'
            
        except Exception as e:
            error_msg = f"Error validating schema: {str(e)}"
            state['errors'].append(error_msg)
            state['status'] = 'error'
            print(f"❌ {error_msg}")
        
        return state
    
    def generate_sql_node2(self, state: NormalizationState) -> NormalizationState:
        """Node 8: Generate SQL DDL scripts"""
        print("\n" + "="*70)
        print("NODE 8: SQL GENERATION")
        print("="*70)
        
        try:
            sql_gen = SQLGenerator(
                state['normalized_tables'],
                state['metadata'],
                state['profiles'],
                state['foreign_keys']
            )
            
            sql_script_path = sql_gen.generate_all_sql(state['sql_output_folder'])
            
            state['sql_script_path'] = sql_script_path
            state['logs'].append(f"Generated SQL script: {sql_script_path}")
            
            state['status'] = 'sql_generated'
            
        except Exception as e:
            error_msg = f"Error generating SQL: {str(e)}"
            state['errors'].append(error_msg)
            state['status'] = 'error'
            print(f"❌ {error_msg}")
        
        return state
    
    def validate_sql_node(self, state: NormalizationState) -> NormalizationState:
        """Node 9: Validate SQL syntax (basic validation)"""
        print("\n" + "="*70)
        print("NODE 9: SQL VALIDATION")
        print("="*70)
        
        try:
            # Basic syntax validation
            sql_path = Path(state['sql_script_path'])
            
            if not sql_path.exists():
                raise FileNotFoundError(f"SQL script not found: {sql_path}")
            
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Basic checks
            checks = {
                'CREATE TABLE statements': 'CREATE TABLE' in sql_content,
                'PRIMARY KEY constraints': 'PRIMARY KEY' in sql_content,
                'Proper semicolons': ';' in sql_content,
                'COMMIT statement': 'COMMIT' in sql_content,
            }
            
            print("\nValidation Results:")
            all_passed = True
            for check_name, passed in checks.items():
                status = "✓" if passed else "❌"
                print(f"  {status} {check_name}")
                if not passed:
                    all_passed = False
            
            if all_passed:
                state['logs'].append("SQL validation passed")
                print("\n✓ SQL script passed basic validation")
            else:
                state['logs'].append("SQL validation completed with warnings")
                print("\n⚠ SQL script has potential issues")
            
            state['status'] = 'sql_validated'
            
        except Exception as e:
            error_msg = f"Error validating SQL: {str(e)}"
            state['errors'].append(error_msg)
            state['status'] = 'error'
            print(f"❌ {error_msg}")
        
        return state
    
    def export_outputs_node(self, state: NormalizationState) -> NormalizationState:
        """Node 10: Export ERD and final outputs"""
        print("\n" + "="*70)
        print("NODE 10: EXPORTING OUTPUTS")
        print("="*70)
        
        try:
            # Generate ERD using utility module
            from utils import ERDGenerator
            
            erd_gen = ERDGenerator(
                state['normalized_tables'],
                state['profiles'],
                state['foreign_keys']
            )
            
            erd_path = erd_gen.generate_erd(state['erd_output_folder'])
            state['erd_path'] = erd_path
            state['logs'].append(f"Generated ERD: {erd_path}")
            
            state['status'] = 'completed'
            print("\n✓ All outputs exported successfully")
            
        except Exception as e:
            # ERD generation is optional, don't fail the entire workflow
            error_msg = f"Warning: Could not generate ERD: {str(e)}"
            state['logs'].append(error_msg)
            print(f"⚠ {error_msg}")
            state['status'] = 'completed'
        
        return state
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow"""
        # Create workflow graph
        workflow = StateGraph(NormalizationState)
        
        # Add nodes
        workflow.add_node("load_files", self.load_files_node)
        workflow.add_node("extract_metadata", self.extract_metadata_node)
        workflow.add_node("profile", self.profile_node)
        workflow.add_node("detect_primary_keys", self.detect_primary_keys_node)
        workflow.add_node("detect_foreign_keys", self.detect_foreign_keys_node)
        workflow.add_node("normalize_3nf", self.normalize_3nf_node)
        workflow.add_node("validate_schema", self.generate_sql_node)
        workflow.add_node("generate_sql", self.generate_sql_node2)
        workflow.add_node("validate_sql", self.validate_sql_node)
        workflow.add_node("export_outputs", self.export_outputs_node)
        
        # Define edges (workflow flow)
        workflow.set_entry_point("load_files")
        workflow.add_edge("load_files", "extract_metadata")
        workflow.add_edge("extract_metadata", "profile")
        workflow.add_edge("profile", "detect_primary_keys")
        workflow.add_edge("detect_primary_keys", "detect_foreign_keys")
        workflow.add_edge("detect_foreign_keys", "normalize_3nf")
        workflow.add_edge("normalize_3nf", "validate_schema")
        workflow.add_edge("validate_schema", "generate_sql")
        workflow.add_edge("generate_sql", "validate_sql")
        workflow.add_edge("validate_sql", "export_outputs")
        workflow.add_edge("export_outputs", END)
        
        # Compile the graph
        return workflow.compile()
    
    def run(self, input_folder: str, output_folder: str = "./normalized_output",
            sql_output_folder: str = "./sql_output", 
            erd_output_folder: str = "./erd") -> NormalizationState:
        """
        Execute the complete normalization workflow
        """
        print("\n" + "="*70)
        print("AUTOMATED 3NF DATA MODELING WORKFLOW")
        print("="*70)
        
        # Initialize state
        initial_state = NormalizationState(
            input_folder=input_folder,
            output_folder=output_folder,
            sql_output_folder=sql_output_folder,
            erd_output_folder=erd_output_folder,
            metadata={},
            profiles={},
            foreign_keys=[],
            normalized_tables={},
            sql_script_path="",
            erd_path="",
            status="initializing",
            errors=[],
            logs=[]
        )
        
        # Run the workflow
        final_state = self.graph.invoke(initial_state)
        
        # Print summary
        print("\n" + "="*70)
        print("WORKFLOW SUMMARY")
        print("="*70)
        print(f"Status: {final_state['status']}")
        print(f"\nLogs:")
        for log in final_state['logs']:
            print(f"  • {log}")
        
        if final_state['errors']:
            print(f"\nErrors:")
            for error in final_state['errors']:
                print(f"  ❌ {error}")
        
        print("\n" + "="*70)
        print("OUTPUTS")
        print("="*70)
        print(f"  Normalized CSV/JSON: {output_folder}")
        print(f"  SQL Script: {final_state.get('sql_script_path', 'N/A')}")
        print(f"  ERD Diagram: {final_state.get('erd_path', 'N/A')}")
        print("="*70)
        
        return final_state


if __name__ == "__main__":
    # Test the workflow
    workflow = NormalizationWorkflow()
    result = workflow.run(
        input_folder="./input_files",
        output_folder="./normalized_output",
        sql_output_folder="./sql_output",
        erd_output_folder="./erd"
    )
