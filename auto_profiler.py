"""
Auto Profiler Module
Profiles data to detect:
- Functional dependencies
- Partial dependencies
- Transitive dependencies
- Candidate keys
- Composite key patterns
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Set, Tuple, Any
from itertools import combinations
from collections import defaultdict


class AutoProfiler:
    """Profile data for normalization analysis"""
    
    def __init__(self, metadata: Dict[str, Any]):
        self.metadata = metadata
        self.dependencies = {}
        self.candidate_keys = {}
        
    def find_functional_dependencies(self, df: pd.DataFrame, table_name: str, 
                                     max_determinant_size: int = 3) -> Dict[str, List[str]]:
        """
        Find functional dependencies: X -> Y
        Where X determines Y (if X value is known, Y value is determined)
        """
        dependencies = defaultdict(list)
        columns = df.columns.tolist()
        
        print(f"  Analyzing functional dependencies for {table_name}...")
        
        # Single column dependencies
        for det_col in columns:
            for dep_col in columns:
                if det_col == dep_col:
                    continue
                
                if self._is_functional_dependency(df, [det_col], dep_col):
                    dependencies[det_col].append(dep_col)
        
        # Check composite key dependencies (up to max_determinant_size columns)
        if len(df) > 1 and len(columns) > 2:
            for size in range(2, min(max_determinant_size + 1, len(columns))):
                for det_cols in combinations(columns, size):
                    det_key = "+".join(det_cols)
                    
                    # Check if this combination uniquely identifies rows
                    if df.duplicated(subset=list(det_cols), keep=False).sum() == 0:
                        # This is a candidate key
                        for dep_col in columns:
                            if dep_col not in det_cols:
                                dependencies[det_key].append(dep_col)
        
        return dict(dependencies)
    
    def _is_functional_dependency(self, df: pd.DataFrame, determinants: List[str], 
                                  dependent: str, threshold: float = 0.99) -> bool:
        """
        Check if determinants -> dependent with high confidence
        """
        if len(df) < 2:
            return False
        
        # Group by determinants and check if dependent is constant
        grouped = df.groupby(determinants, dropna=False)[dependent].nunique()
        
        # If all groups have exactly 1 value for dependent, it's a FD
        if (grouped == 1).all():
            return True
        
        # Allow some tolerance for data quality issues
        if (grouped == 1).sum() / len(grouped) >= threshold:
            return True
        
        return False
    
    def detect_partial_dependencies(self, df: pd.DataFrame, table_name: str,
                                   composite_key: List[str]) -> Dict[str, List[str]]:
        """
        Detect partial dependencies (violates 2NF)
        A column depends on only part of the composite key
        """
        if len(composite_key) < 2:
            return {}
        
        partial_deps = {}
        columns = [col for col in df.columns if col not in composite_key]
        
        print(f"  Checking for partial dependencies in {table_name}...")
        
        # Check each subset of the composite key
        for size in range(1, len(composite_key)):
            for subset in combinations(composite_key, size):
                subset_key = "+".join(subset)
                
                for col in columns:
                    if self._is_functional_dependency(df, list(subset), col):
                        # This column depends on a partial key
                        if subset_key not in partial_deps:
                            partial_deps[subset_key] = []
                        partial_deps[subset_key].append(col)
        
        return partial_deps
    
    def detect_transitive_dependencies(self, df: pd.DataFrame, table_name: str,
                                      primary_key: List[str]) -> List[Tuple[str, str, List[str]]]:
        """
        Detect transitive dependencies (violates 3NF)
        PK -> A -> B (where A is not a candidate key)
        ONLY flag genuine transitive dependencies, not just repeated categorical values
        """
        transitive_deps = []
        columns = [col for col in df.columns if col not in primary_key]
        
        print(f"  Checking for transitive dependencies in {table_name}...")
        
        # Find all FDs from primary key
        pk_key = "+".join(primary_key)
        pk_dependent_cols = []
        
        for col in columns:
            if self._is_functional_dependency(df, primary_key, col):
                pk_dependent_cols.append(col)
        
        # Check if any non-key column determines other columns
        for intermediate_col in pk_dependent_cols:
            for target_col in columns:
                if intermediate_col == target_col:
                    continue
                
                if self._is_functional_dependency(df, [intermediate_col], target_col):
                    # Verify this is a true transitive dependency
                    if not self._is_direct_dependency(df, primary_key, target_col):
                        # Check if intermediate_col represents a genuine entity
                        if self._represents_genuine_entity(df, intermediate_col, [target_col]):
                            # Collect all attributes transitively dependent via this intermediate
                            dependent_attrs = [target_col]
                            for other_col in columns:
                                if other_col != intermediate_col and other_col != target_col:
                                    if self._is_functional_dependency(df, [intermediate_col], other_col):
                                        if not self._is_direct_dependency(df, primary_key, other_col):
                                            if other_col not in dependent_attrs:
                                                dependent_attrs.append(other_col)
                            
                            # Only create entry if we haven't already recorded this intermediate
                            existing = [t for t in transitive_deps if t[1] == intermediate_col]
                            if not existing:
                                transitive_deps.append((pk_key, intermediate_col, dependent_attrs))
                                break  # Don't duplicate entries for same intermediate
        
        return transitive_deps
    
    def _is_direct_dependency(self, df: pd.DataFrame, determinants: List[str], 
                             dependent: str) -> bool:
        """
        Check if dependent is directly determined by determinants
        """
        if len(df) < 2:
            return True
        
        # Group by determinants and check if dependent is constant
        grouped = df.groupby(determinants, dropna=False)[dependent].nunique()
        
        # If all groups have exactly 1 value for dependent, it's a direct FD
        return (grouped == 1).all()
    
    def _represents_genuine_entity(self, df: pd.DataFrame, col: str, 
                                   dependent_cols: List[str]) -> bool:
        """
        Determine if column represents a genuine business entity
        Not just categorical/lookup values
        """
        # Single dependent column that's just a name/description is not an entity
        if len(dependent_cols) == 1:
            dep_col = dependent_cols[0]
            name_indicators = ['name', 'description', 'label', 'title', 'desc']
            if any(ind in dep_col.lower() for ind in name_indicators):
                if col.replace('_id', '').replace('_code', '') in dep_col.lower():
                    return False
        
        # Check uniqueness - if very low, likely categorical
        unique_count = df[col].nunique()
        total_count = len(df)
        uniqueness_ratio = unique_count / total_count if total_count > 0 else 0
        
        # If less than 10 unique values or less than 5% uniqueness, probably not entity
        if unique_count < 10 or uniqueness_ratio < 0.05:
            return False
        
        # Check for entity indicators
        entity_indicators = ['supplier', 'vendor', 'manufacturer', 'customer', 'employee',
                           'product', 'order', 'invoice', 'contract', 'location', 'company']
        
        col_lower = col.lower()
        has_entity_indicator = any(ind in col_lower for ind in entity_indicators)
        
        # Simple categorical values (state, status, category alone) are not entities
        simple_categorical = ['state', 'status', 'type', 'category', 'level', 'grade', 
                             'class', 'rank', 'priority', 'severity']
        
        is_simple_categorical = any(cat in col_lower for cat in simple_categorical)
        
        if is_simple_categorical and not has_entity_indicator:
            return False
        
        # Entity must have multiple meaningful attributes
        if has_entity_indicator and len(dependent_cols) >= 2:
            return True
        
        # Has contact/address information suggests real entity
        contact_indicators = ['email', 'phone', 'address', 'contact', 'website', 'city', 'zip']
        has_contact = any(any(ind in dep.lower() for ind in contact_indicators) 
                         for dep in dependent_cols)
        
        if has_contact:
            return True
        
        # Default: only if 3+ diverse attributes
        return len(dependent_cols) >= 3
    
    def find_candidate_keys(self, df: pd.DataFrame, table_name: str,
                           max_key_size: int = 3) -> List[Tuple[str, ...]]:
        """
        Find all candidate keys (columns or combinations that uniquely identify rows)
        CRITICAL: Must have identity semantics - uniqueness alone is not sufficient
        """
        if len(df) == 0:
            return []
        
        candidate_keys = []
        columns = df.columns.tolist()
        
        # Blacklist of attributes that should NOT be candidate keys
        blacklist_patterns = [
            'email', 'phone', 'price', 'amount', 'cost', 'total', 'quantity',
            'date', 'timestamp', 'name', 'description', 'address', 'status',
            'city', 'state', 'zip', 'country', 'supplier', 'category'
        ]
        
        # Identity semantic patterns that SHOULD be candidate keys
        identity_patterns = ['_id', '_key', '_code', '_ref', '_number', 'sys_id', 'uuid', 'guid']
        
        print(f"  Finding candidate keys for {table_name}...")
        
        # Check single columns first - MUST have identity semantics
        for col in columns:
            col_lower = col.lower()
            
            # Skip blacklisted attributes (even if unique)
            if any(pattern in col_lower for pattern in blacklist_patterns):
                continue
            
            # Check for identity semantics
            has_identity = any(pattern in col_lower for pattern in identity_patterns)
            if not has_identity:
                # Check if starts/ends with identity indicator
                has_identity = (col_lower.startswith('id') or col_lower.endswith('id') or
                              col_lower.startswith('key') or col_lower.endswith('key') or
                              col_lower.startswith('code') or col_lower.endswith('code'))
            
            # Only consider columns with identity semantics
            if has_identity:
                if df[col].nunique() == len(df) and df[col].notna().all():
                    candidate_keys.append((col,))
        
        # If no single column key found, check combinations (at least one must be identity column)
        if not candidate_keys and len(columns) > 1:
            for size in range(2, min(max_key_size + 1, len(columns) + 1)):
                for cols in combinations(columns, size):
                    # At least one column must have identity semantics
                    has_identity_col = False
                    for col in cols:
                        col_lower = col.lower()
                        if any(pattern in col_lower for pattern in identity_patterns):
                            has_identity_col = True
                            break
                    
                    if not has_identity_col:
                        continue
                    
                    # Check if combination is unique
                    if not df.duplicated(subset=list(cols), keep=False).any():
                        candidate_keys.append(cols)
                        
                        # If we found keys of this size, don't look for larger keys
                        if len(candidate_keys) >= 3:
                            break
                
                if candidate_keys:
                    break
        
        return candidate_keys
    
    def analyze_column_relationships(self, df: pd.DataFrame, col1: str, col2: str) -> Dict[str, Any]:
        """
        Analyze the relationship between two columns
        """
        # Value overlap analysis
        unique1 = set(df[col1].dropna().unique())
        unique2 = set(df[col2].dropna().unique())
        
        # Convert to strings for comparison
        unique1_str = {str(v) for v in unique1}
        unique2_str = {str(v) for v in unique2}
        
        overlap = len(unique1_str & unique2_str)
        overlap_ratio = overlap / min(len(unique1_str), len(unique2_str)) if min(len(unique1_str), len(unique2_str)) > 0 else 0
        
        # Cardinality relationship
        if len(unique1) == 0 or len(unique2) == 0:
            cardinality = "unknown"
        elif len(unique1) > len(unique2) * 5:
            cardinality = "many-to-one"  # col1 is FK to col2
        elif len(unique2) > len(unique1) * 5:
            cardinality = "one-to-many"  # col2 is FK to col1
        else:
            cardinality = "many-to-many"
        
        return {
            'overlap_count': overlap,
            'overlap_ratio': overlap_ratio,
            'cardinality': cardinality,
            'unique1': len(unique1),
            'unique2': len(unique2)
        }
    
    def profile_all_tables(self) -> Dict[str, Any]:
        """
        Profile all tables for dependencies and keys
        """
        profiles = {}
        
        for table_name, meta in self.metadata.items():
            print(f"\nProfiling table: {table_name}")
            df = meta['dataframe']
            
            # Find candidate keys
            candidate_keys = self.find_candidate_keys(df, table_name)
            
            # Find functional dependencies
            func_deps = self.find_functional_dependencies(df, table_name)
            
            # If we have candidate keys, check for partial and transitive dependencies
            partial_deps = {}
            transitive_deps = []
            
            if candidate_keys:
                # Use the first candidate key (or the smallest one)
                primary_key = min(candidate_keys, key=len)
                
                if len(primary_key) > 1:
                    partial_deps = self.detect_partial_dependencies(df, table_name, list(primary_key))
                
                transitive_deps = self.detect_transitive_dependencies(df, table_name, list(primary_key))
            else:
                primary_key = None
            
            profiles[table_name] = {
                'candidate_keys': candidate_keys,
                'primary_key': primary_key,
                'functional_dependencies': func_deps,
                'partial_dependencies': partial_deps,
                'transitive_dependencies': transitive_deps,
                'violates_2nf': len(partial_deps) > 0,
                'violates_3nf': len(transitive_deps) > 0
            }
        
        self.dependencies = profiles
        return profiles
    
    def get_profile_summary(self) -> str:
        """Get a summary of profiling results"""
        if not self.dependencies:
            return "No profiling done yet"
        
        summary = []
        summary.append("=== PROFILING SUMMARY ===\n")
        
        for table_name, profile in self.dependencies.items():
            summary.append(f"\nTable: {table_name}")
            
            # Candidate keys
            if profile['candidate_keys']:
                summary.append(f"  Candidate Keys:")
                for key in profile['candidate_keys'][:3]:  # Show first 3
                    key_str = ", ".join(key)
                    summary.append(f"    - ({key_str})")
            else:
                summary.append(f"  ⚠ No natural candidate key found - will need surrogate key")
            
            # Primary key
            if profile['primary_key']:
                pk_str = ", ".join(profile['primary_key'])
                summary.append(f"  Selected PK: ({pk_str})")
            
            # Normalization violations
            if profile['violates_2nf']:
                summary.append(f"  ⚠ VIOLATES 2NF - Partial dependencies detected")
                for key, deps in profile['partial_dependencies'].items():
                    summary.append(f"      {key} -> {', '.join(deps)}")
            
            if profile['violates_3nf']:
                summary.append(f"  ⚠ VIOLATES 3NF - Transitive dependencies detected")
                for pk, inter, targets in profile['transitive_dependencies']:
                    summary.append(f"      {pk} -> {inter} -> {', '.join(targets)}")
            
            if not profile['violates_2nf'] and not profile['violates_3nf']:
                summary.append(f"  ✓ Already in 3NF")
        
        return "\n".join(summary)


if __name__ == "__main__":
    from metadata_extractor import MetadataExtractor
    
    # Test profiler
    extractor = MetadataExtractor()
    metadata = extractor.extract_all_metadata("./input_files")
    
    profiler = AutoProfiler(metadata)
    profiles = profiler.profile_all_tables()
    
    print(profiler.get_profile_summary())
