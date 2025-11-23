"""
Foreign Key Detector Module
Detects foreign key relationships between tables using:
- Name similarity
- Value overlap
- Cardinality patterns
- Hierarchical patterns
- Metadata matching
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Set
from difflib import SequenceMatcher
import re


class ForeignKeyDetector:
    """Detect foreign key relationships across tables based on functional dependencies"""
    
    def __init__(self, metadata: Dict[str, Any], profiles: Dict[str, Any]):
        self.metadata = metadata
        self.profiles = profiles
        self.foreign_keys = []
        self.detected_relationships = set()  # Track to avoid circular FKs
        
    def detect_hierarchical_pattern(self, table_name: str, col_name: str) -> bool:
        """
        Detect if column represents a hierarchical relationship (self-referencing FK)
        """
        # Common patterns for hierarchical relationships
        hierarchical_patterns = [
            r'parent_(.+)',
            r'(.+)_parent',
            r'manager_(.+)',
            r'supervisor_(.+)',
            r'chief_(.+)',
            r'head_(.+)',
        ]
        
        col_lower = col_name.lower()
        table_lower = table_name.lower()
        
        for pattern in hierarchical_patterns:
            if re.match(pattern, col_lower):
                # Check if the referenced entity matches the table
                match = re.match(pattern, col_lower)
                if match and match.group(1) in table_lower:
                    return True
        
        return False
    
    def _would_create_circular_dependency(self, table1: str, table2: str) -> bool:
        """
        Check if adding FK from table1 to table2 would create a circular dependency
        Uses DFS to detect cycles in the dependency graph
        """
        # Build current dependency graph
        graph = {}
        for rel in self.detected_relationships:
            if rel not in graph:
                graph[rel] = []
        
        for fk_table, pk_table in self.detected_relationships:
            if fk_table not in graph:
                graph[fk_table] = []
            graph[fk_table].append(pk_table)
        
        # Add proposed edge
        if table1 not in graph:
            graph[table1] = []
        graph[table1].append(table2)
        
        # DFS to detect cycle
        def has_cycle_dfs(node: str, visited: set, rec_stack: set) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            if node in graph:
                for neighbor in graph[node]:
                    if neighbor not in visited:
                        if has_cycle_dfs(neighbor, visited, rec_stack):
                            return True
                    elif neighbor in rec_stack:
                        return True
            
            rec_stack.remove(node)
            return False
        
        # Check for cycles starting from table1
        visited = set()
        return has_cycle_dfs(table1, visited, set())
    
    def is_valid_foreign_key(self, child_table: str, fk_col: str, 
                            parent_table: str, pk_col: str) -> Dict[str, Any]:
        """
        Determine if fk_col in child_table is a valid foreign key to pk_col in parent_table.
        
        STRICT RULES (ALL must be TRUE):
        1) parent_table has a valid primary key assigned
        2) fk_col values ⊆ pk_col values (subset relationship)
        3) parent_table has fewer rows than child_table (cardinality check)
        4) fk_col is NOT the primary key of child_table
        5) No ambiguity (fk_col matches only ONE table's PK)
        6) No circular dependencies
        7) pk_col must be the actual PK of parent_table
        8) fk_col must not be a non-identity descriptive/transactional column
        """
        result = {
            'is_valid': False,
            'reasons': [],
            'is_pk_reference': False
        }
        
        df_child = self.metadata[child_table]['dataframe']
        df_parent = self.metadata[parent_table]['dataframe']
        
        # RULE 1: Parent table must have a valid primary key
        if parent_table not in self.profiles or not self.profiles[parent_table].get('primary_key'):
            result['reasons'].append(f'Parent table {parent_table} has no primary key assigned')
            return result
        
        parent_pk = self.profiles[parent_table]['primary_key']
        
        # RULE 7: pk_col must be THE primary key of parent table
        if pk_col not in parent_pk:
            result['reasons'].append(f'{pk_col} is not the primary key of {parent_table}')
            return result
        
        result['is_pk_reference'] = True
        
        # RULE 4: fk_col must NOT be the primary key of child_table
        if child_table in self.profiles and self.profiles[child_table].get('primary_key'):
            child_pk = self.profiles[child_table]['primary_key']
            if fk_col in child_pk:
                result['reasons'].append(f'{fk_col} is the primary key of {child_table} (PK ≠ FK rule)')
                return result
        
        # RULE 8: fk_col must have identifier semantics
        # Only columns with _id, _key, _code, _ref patterns can be FKs
        col_lower = fk_col.lower()
        has_id_pattern = ('_id' in col_lower or col_lower.endswith('id') or 
                          '_key' in col_lower or '_code' in col_lower or '_ref' in col_lower)
        
        if not has_id_pattern:
            result['reasons'].append(f'{fk_col} does not match identifier pattern (*_id, *_key, *_code, *_ref)')
            return result
        
        # RULE 2: fk_col values must be subset of pk_col values
        fk_values = set(str(v) for v in df_child[fk_col].dropna().unique())
        pk_values = set(str(v) for v in df_parent[pk_col].dropna().unique())
        
        if not fk_values:
            result['reasons'].append(f'{fk_col} has no non-null values')
            return result
        
        # Check subset relationship
        values_not_in_parent = fk_values - pk_values
        subset_coverage = (len(fk_values) - len(values_not_in_parent)) / len(fk_values)
        
        # Require 100% subset coverage for valid FK
        if subset_coverage < 1.0:
            result['reasons'].append(
                f'{fk_col} values not subset of {pk_col} values '
                f'({len(values_not_in_parent)} of {len(fk_values)} values missing in parent)'
            )
            return result
        
        # RULE 3: Parent table must have fewer rows than child table (or equal for 1:1)
        parent_row_count = len(df_parent)
        child_row_count = len(df_child)
        
        if parent_row_count > child_row_count:
            result['reasons'].append(
                f'Parent {parent_table} ({parent_row_count} rows) has more rows than '
                f'child {child_table} ({child_row_count} rows) - invalid cardinality'
            )
            return result
        
        # RULE 6: Check for circular dependencies
        if self._would_create_circular_dependency(child_table, parent_table):
            result['reasons'].append(f'Would create circular dependency: {child_table} -> {parent_table}')
            return result
        
        # All rules passed - valid FK
        result['is_valid'] = True
        result['reasons'] = [
            f'FK values ({len(fk_values)}) subset of PK values ({len(pk_values)})',
            f'Parent rows ({parent_row_count}) <= Child rows ({child_row_count})',
            f'{fk_col} is not PK of {child_table}',
            f'{pk_col} is PK of {parent_table}',
            f'{fk_col} has identifier pattern',
            'No circular dependencies'
        ]
        
        return result
    
    def _are_datatypes_compatible(self, dtype1: str, dtype2: str) -> bool:
        """Check if two Oracle datatypes are compatible for FK relationship"""
        # Extract base types
        base1 = dtype1.split('(')[0].upper()
        base2 = dtype2.split('(')[0].upper()
        
        # Compatible type groups
        compatible_groups = [
            {'NUMBER', 'INTEGER', 'INT'},
            {'VARCHAR2', 'VARCHAR', 'CHAR', 'NVARCHAR2'},
            {'DATE', 'TIMESTAMP'},
        ]
        
        if base1 == base2:
            return True
        
        for group in compatible_groups:
            if base1 in group and base2 in group:
                return True
        
        return False
    
    def detect_all_foreign_keys(self) -> List[Dict[str, Any]]:
        """
        Detect all foreign key relationships using strict parent-child rules.
        
        Algorithm:
        for each table B (potential child):
            for each non-PK column X in B:
                for each table A (potential parent):
                    if X values ⊆ A.PK values AND row_count(A) < row_count(B):
                        if exactly ONE such A exists:
                            validate circular dependency
                            if OK: add FK B.X → A.PK
        """
        detected_fks = []
        table_names = list(self.metadata.keys())
        
        print(f"\nDetecting foreign keys across {len(table_names)} tables using strict parent-child rules...")
        
        # Phase 1: For each table, identify non-PK columns
        for child_table in table_names:
            df_child = self.metadata[child_table]['dataframe']
            child_cols = df_child.columns.tolist()
            
            # Get child table's PK (if any)
            child_pk = []
            if child_table in self.profiles and self.profiles[child_table].get('primary_key'):
                child_pk = list(self.profiles[child_table]['primary_key'])
            
            # Phase 2: For each non-PK column, find potential parent tables
            for fk_col in child_cols:
                # Skip if this column is the PK
                if fk_col in child_pk:
                    continue
                
                # Phase 3: Test against all potential parent tables
                valid_parents = []
                
                for parent_table in table_names:
                    # Skip self-references for now (handle separately)
                    if parent_table == child_table:
                        continue
                    
                    # Get parent table's PK
                    if parent_table not in self.profiles or not self.profiles[parent_table].get('primary_key'):
                        continue
                    
                    parent_pk = list(self.profiles[parent_table]['primary_key'])
                    
                    # Check each PK column of parent
                    for pk_col in parent_pk:
                        # Validate FK relationship
                        validation = self.is_valid_foreign_key(
                            child_table, fk_col, parent_table, pk_col
                        )
                        
                        if validation['is_valid']:
                            valid_parents.append({
                                'parent_table': parent_table,
                                'pk_col': pk_col,
                                'reasons': validation['reasons']
                            })
                
                # Phase 4: RULE 5 - Must have exactly ONE valid parent (no ambiguity)
                # If multiple parents found, use column name similarity to break tie
                if len(valid_parents) == 1:
                    parent = valid_parents[0]
                    
                    # Add to detected FKs
                    detected_fks.append({
                        'fk_table': child_table,
                        'fk_column': fk_col,
                        'pk_table': parent['parent_table'],
                        'pk_column': parent['pk_col'],
                        'reasons': parent['reasons'],
                        'relationship_type': 'parent-child'
                    })
                    
                    # Track relationship for circular dependency detection
                    self.detected_relationships.add((child_table, parent['parent_table']))
                    
                    print(f"  [+] FK detected: {child_table}.{fk_col} -> {parent['parent_table']}.{parent['pk_col']}")
                
                elif len(valid_parents) > 1:
                    # Ambiguous - multiple potential parents
                    # Try to resolve using column name similarity
                    fk_col_base = fk_col.lower().replace('_id', '').replace('_key', '').replace('_code', '').replace('_ref', '')
                    
                    best_match = None
                    best_score = 0
                    
                    for parent in valid_parents:
                        parent_table_name = parent['parent_table'].lower()
                        
                        # Exact match with table name
                        if fk_col_base == parent_table_name or parent_table_name in fk_col_base or fk_col_base in parent_table_name:
                            if len(parent_table_name) > best_score:
                                best_match = parent
                                best_score = len(parent_table_name)
                    
                    if best_match and best_score > 0:
                        # Resolved ambiguity with name matching
                        detected_fks.append({
                            'fk_table': child_table,
                            'fk_column': fk_col,
                            'pk_table': best_match['parent_table'],
                            'pk_column': best_match['pk_col'],
                            'reasons': best_match['reasons'] + [f'Resolved ambiguity using column name match ({fk_col} -> {best_match["parent_table"]})'],
                            'relationship_type': 'parent-child'
                        })
                        
                        # Track relationship
                        self.detected_relationships.add((child_table, best_match['parent_table']))
                        
                        print(f"  [+] FK detected: {child_table}.{fk_col} -> {best_match['parent_table']}.{best_match['pk_col']} (ambiguity resolved by name)")
                    else:
                        # Still ambiguous - skip
                        parent_tables = [p['parent_table'] for p in valid_parents]
                        print(f"  [!] Ambiguous FK: {child_table}.{fk_col} could reference {parent_tables} - skipped")
        
        # Phase 5: Handle self-referencing FKs (hierarchical relationships)
        for table_name in table_names:
            df = self.metadata[table_name]['dataframe']
            cols = df.columns.tolist()
            
            # Get table's PK
            if table_name not in self.profiles or not self.profiles[table_name].get('primary_key'):
                continue
            
            pk = list(self.profiles[table_name]['primary_key'])
            if len(pk) != 1:
                continue  # Only handle single-column PKs for self-references
            
            pk_col = pk[0]
            
            for col in cols:
                if col == pk_col:
                    continue
                
                # Check if this column references the same table's PK
                if self.detect_hierarchical_pattern(table_name, col):
                    validation = self.is_valid_foreign_key(table_name, col, table_name, pk_col)
                    
                    # For self-references, relax row count requirement
                    if not validation['is_valid']:
                        # Check if it's just the row count issue
                        df_values = set(str(v) for v in df[col].dropna().unique())
                        pk_values = set(str(v) for v in df[pk_col].dropna().unique())
                        
                        if df_values.issubset(pk_values) and col not in pk:
                            detected_fks.append({
                                'fk_table': table_name,
                                'fk_column': col,
                                'pk_table': table_name,
                                'pk_column': pk_col,
                                'reasons': ['Self-referencing FK (hierarchical)', f'{col} values ⊆ {pk_col} values'],
                                'relationship_type': 'self-referencing'
                            })
                            print(f"  [+] Self-FK detected: {table_name}.{col} -> {table_name}.{pk_col}")
        
        self.foreign_keys = detected_fks
        print(f"\n[+] Detected {len(detected_fks)} valid foreign keys")
        return detected_fks
    
    def get_fk_summary(self) -> str:
        """Get summary of detected foreign keys"""
        if not self.foreign_keys:
            return "No foreign keys detected"
        
        summary = []
        summary.append(f"\n=== DETECTED FOREIGN KEYS ({len(self.foreign_keys)}) ===\n")
        
        for fk in self.foreign_keys:
            summary.append(f"\n{fk['fk_table']}.{fk['fk_column']} -> {fk['pk_table']}.{fk['pk_column']}")
            summary.append(f"  Type: {fk['relationship_type']}")
            summary.append(f"  Reasons:")
            for reason in fk['reasons']:
                summary.append(f"    - {reason}")
        
        return "\n".join(summary)


if __name__ == "__main__":
    from metadata_extractor import MetadataExtractor
    from auto_profiler import AutoProfiler
    
    # Test FK detector
    extractor = MetadataExtractor()
    metadata = extractor.extract_all_metadata("./input_files")
    
    profiler = AutoProfiler(metadata)
    profiles = profiler.profile_all_tables()
    
    fk_detector = ForeignKeyDetector(metadata, profiles)
    foreign_keys = fk_detector.detect_all_foreign_keys()
    
    print(fk_detector.get_fk_summary())
