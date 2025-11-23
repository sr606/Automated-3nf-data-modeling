"""
Normalizer Module
Implements automated 3NF normalization:
- Checks and enforces 1NF (atomic values, no repeating groups)
- Checks and enforces 2NF (no partial dependencies)
- Checks and enforces 3NF (no transitive dependencies)
- Generates normalized table structures
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Set
import re


class Normalizer:
    """Automated 3NF normalization engine"""
    
    # Attributes that should NEVER be primary keys
    PK_BLACKLIST_PATTERNS = [
        # Transactional attributes
        'email', 'phone', 'mobile', 'fax', 'contact',
        # Monetary/numeric transactional
        'price', 'amount', 'cost', 'total', 'subtotal', 'tax', 'discount',
        'quantity', 'qty', 'count', 'balance', 'payment', 'salary', 'wage',
        # Temporal (except when combined with entity ID)
        'date', 'time', 'timestamp', 'created', 'updated', 'modified',
        # Descriptive attributes
        'name', 'description', 'desc', 'title', 'label', 'comment', 'note',
        # Categorical attributes
        'status', 'state', 'type', 'category', 'class', 'level', 'priority',
        # Location attributes (non-unique)
        'address', 'street', 'city', 'zip', 'postal', 'country',
        # Product/item attributes
        'product', 'item', 'sku', 'barcode', 'isbn',
        # Other non-unique attributes
        'supplier', 'vendor', 'manufacturer', 'brand', 'model',
        'order_date', 'ship_date', 'delivery_date', 'due_date'
    ]
    
    def __init__(self, metadata: Dict[str, Any], profiles: Dict[str, Any], 
                 foreign_keys: List[Dict[str, Any]]):
        self.metadata = metadata
        self.profiles = profiles
        self.foreign_keys = foreign_keys
        self.normalized_tables = {}
        self.normalization_log = []
        
    def enforce_1nf(self, table_name: str, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Enforce First Normal Form:
        - Atomic values only
        - No repeating groups
        - No multivalued columns
        """
        result_tables = {table_name: df.copy()}
        columns_meta = self.metadata[table_name]['columns']
        
        # Find multivalued columns
        multivalued_cols = [col for col, meta in columns_meta.items() 
                           if meta.get('is_multivalued', False)]
        
        if not multivalued_cols:
            self.normalization_log.append(f"✓ {table_name}: Already in 1NF")
            return result_tables
        
        # Split multivalued columns into separate tables
        for col in multivalued_cols:
            self.normalization_log.append(f"⚙ {table_name}: Splitting multivalued column '{col}'")
            
            # Determine primary key for original table
            pk = self._get_or_create_primary_key(table_name, df)
            
            # Create new table for the multivalued attribute
            new_table_name = f"{table_name}_{col}"
            
            # Explode the multivalued column
            new_df = self._explode_multivalued_column(df, col, pk)
            
            if new_df is not None and len(new_df) > 0:
                result_tables[new_table_name] = new_df
                
                # Remove the column from original table
                result_tables[table_name] = result_tables[table_name].drop(columns=[col])
                
                self.normalization_log.append(f"  → Created table '{new_table_name}' with columns: {pk} + {col}_value")
        
        return result_tables
    
    def _explode_multivalued_column(self, df: pd.DataFrame, col: str, 
                                    pk: List[str]) -> pd.DataFrame:
        """
        Split multivalued column into separate rows
        """
        # Common delimiters
        delimiters = [',', ';', '|', '/', ':']
        
        # Detect the most common delimiter
        sample = df[col].dropna().head(100).astype(str)
        delimiter = None
        max_count = 0
        
        for delim in delimiters:
            count = sample.str.contains(f'\\{delim}', regex=True).sum()
            if count > max_count:
                max_count = count
                delimiter = delim
        
        if delimiter is None:
            return None
        
        # Create new dataframe
        rows = []
        for idx, row in df.iterrows():
            pk_values = {p: row[p] for p in pk}
            col_value = str(row[col]) if pd.notna(row[col]) else ""
            
            if delimiter in col_value:
                values = [v.strip() for v in col_value.split(delimiter)]
                for value in values:
                    if value:
                        new_row = pk_values.copy()
                        new_row[f'{col}_value'] = value
                        rows.append(new_row)
            elif col_value:
                new_row = pk_values.copy()
                new_row[f'{col}_value'] = col_value
                rows.append(new_row)
        
        if rows:
            return pd.DataFrame(rows)
        return None
    
    def enforce_2nf(self, table_name: str, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Enforce Second Normal Form:
        - Must be in 1NF
        - No partial dependencies (applies only to tables with composite keys)
        """
        result_tables = {table_name: df.copy()}
        
        # Get profile for this table
        if table_name not in self.profiles:
            self.normalization_log.append(f"⚠ {table_name}: No profile found, skipping 2NF check")
            return result_tables
        
        profile = self.profiles[table_name]
        partial_deps = profile.get('partial_dependencies', {})
        
        if not partial_deps:
            self.normalization_log.append(f"✓ {table_name}: Already in 2NF")
            return result_tables
        
        # Handle partial dependencies
        self.normalization_log.append(f"⚙ {table_name}: Resolving partial dependencies")
        
        primary_key = profile.get('primary_key')
        if not primary_key or len(primary_key) < 2:
            self.normalization_log.append(f"  No composite key, skipping 2NF")
            return result_tables
        
        # For each partial dependency, create a new table
        for partial_key, dependent_cols in partial_deps.items():
            key_cols = partial_key.split('+')
            new_table_name = f"{table_name}_{'_'.join(key_cols)}"
            
            # Columns for new table: partial key + dependent columns
            new_table_cols = key_cols + dependent_cols
            
            # Create new table
            new_df = df[new_table_cols].drop_duplicates().reset_index(drop=True)
            result_tables[new_table_name] = new_df
            
            # Remove dependent columns from original table
            result_tables[table_name] = result_tables[table_name].drop(columns=dependent_cols)
            
            self.normalization_log.append(f"  → Created table '{new_table_name}' for partial dependency")
            self.normalization_log.append(f"     PK: ({', '.join(key_cols)})")
            self.normalization_log.append(f"     Attributes: {', '.join(dependent_cols)}")
        
        return result_tables
    
    def enforce_3nf(self, table_name: str, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Enforce Third Normal Form with generalized semantic rules:
        - Must be in 2NF
        - No transitive dependencies
        - ONLY create new tables based on functional dependencies and semantic analysis
        - NOT based on value repetition frequency
        """
        result_tables = {table_name: df.copy()}
        
        # Get profile for this table
        if table_name not in self.profiles:
            self.normalization_log.append(f"⚠ {table_name}: No profile found, skipping 3NF check")
            return result_tables
        
        profile = self.profiles[table_name]
        transitive_deps = profile.get('transitive_dependencies', [])
        
        if not transitive_deps:
            self.normalization_log.append(f"✓ {table_name}: Already in 3NF")
            return result_tables
        
        # Handle transitive dependencies with semantic validation
        self.normalization_log.append(f"⚙ {table_name}: Analyzing transitive dependencies with semantic rules")
        
        # For each transitive dependency, apply semantic rules
        for pk_key, intermediate_col, target_cols in transitive_deps:
            # Rule 1: Verify this is a true transitive dependency (not direct)
            if not self._verify_functional_dependency_chain(df, pk_key, intermediate_col, target_cols):
                self.normalization_log.append(f"  → Skipping '{intermediate_col}': Not a true transitive dependency")
                continue
            
            # Rule 2: Check for multi-row pattern (event/history/child table)
            multi_row_info = self._detect_multi_row_pattern(df, table_name, intermediate_col)
            if multi_row_info['is_multi_row']:
                pattern = multi_row_info['pattern_type']
                new_table_name = f"{table_name}_{pattern}"
                self.normalization_log.append(f"  → '{intermediate_col}' shows multi-row pattern: {pattern}")
                self.normalization_log.append(f"     Evidence: {'; '.join(multi_row_info['evidence'])}")
                # Create event/child table
                self._create_related_table(result_tables, table_name, new_table_name, 
                                          df, intermediate_col, target_cols)
                continue
            
            # Rule 3: Semantic entity detection - is this a genuine business entity?
            entity_info = self._detect_semantic_entity(df, intermediate_col, target_cols)
            
            if not entity_info['is_entity']:
                self.normalization_log.append(f"  → Keeping '{intermediate_col}' in main table")
                self.normalization_log.append(f"     Reason: {'; '.join(entity_info['reasons'])}")
                continue
            
            # Entity detected - create reference table
            new_table_name = self._generate_entity_table_name(table_name, intermediate_col, 
                                                              entity_info['entity_type'])
            self.normalization_log.append(f"  → Creating entity table for '{intermediate_col}'")
            self.normalization_log.append(f"     Confidence: {entity_info['confidence']:.0%}, Type: {entity_info['entity_type']}")
            self.normalization_log.append(f"     Reason: {'; '.join(entity_info['reasons'])}")
            
            self._create_related_table(result_tables, table_name, new_table_name, 
                                      df, intermediate_col, target_cols)
        
        return result_tables
    
    def _generate_entity_table_name(self, parent_table: str, key_col: str, 
                                    entity_type: str) -> str:
        """
        Generate appropriate table name based on entity type
        """
        # Clean up column name for table naming
        base_name = key_col.replace('_id', '').replace('_code', '').replace('_key', '')
        
        # Avoid redundant naming
        if base_name in parent_table:
            return base_name
        
        return base_name
    
    def _create_related_table(self, result_tables: Dict[str, pd.DataFrame], 
                             parent_table: str, new_table_name: str,
                             df: pd.DataFrame, key_col: str, attribute_cols: List[str]):
        """
        Create a related table and update parent table
        """
        # Columns for new table: key column + attribute columns
        new_table_cols = [key_col] + [col for col in attribute_cols if col in df.columns]
        
        if len(new_table_cols) < 2:
            return
        
        # Create entity table with unique combinations
        new_df = df[new_table_cols].drop_duplicates().reset_index(drop=True)
        result_tables[new_table_name] = new_df
        
        # Remove attribute columns from parent (keep key as FK)
        cols_to_remove = [col for col in attribute_cols if col in result_tables[parent_table].columns]
        if cols_to_remove:
            result_tables[parent_table] = result_tables[parent_table].drop(columns=cols_to_remove)
        
        self.normalization_log.append(f"     → Created table '{new_table_name}' with {len(new_df)} rows")
        self.normalization_log.append(f"     → Columns: {', '.join(new_table_cols)}")
    
    def _verify_functional_dependency_chain(self, df: pd.DataFrame, pk_key: str,
                                           intermediate_col: str, target_cols: List[str]) -> bool:
        """
        Verify true transitive dependency: PK -> intermediate -> target
        NOT just: PK -> target (direct dependency)
        Uses functional dependency testing on actual data
        """
        if len(df) < 2:
            return False
        
        # Get PK columns
        pk_cols = pk_key.split('+') if '+' in pk_key else [pk_key]
        
        # Validate columns exist
        if not all(col in df.columns for col in pk_cols):
            return False
        if intermediate_col not in df.columns:
            return False
        
        # Test 1: PK -> intermediate (must be functional dependency)
        grouped_pk_inter = df.groupby(pk_cols)[intermediate_col].nunique()
        if not (grouped_pk_inter == 1).all():
            # Intermediate varies for same PK - not a functional dependency
            return False
        
        # Test 2: For each target, verify intermediate -> target
        has_valid_target = False
        for target_col in target_cols:
            if target_col not in df.columns:
                continue
            
            # Check intermediate -> target is FD
            grouped_inter_target = df.groupby(intermediate_col)[target_col].nunique()
            if (grouped_inter_target == 1).all():
                # This is valid: intermediate determines target
                # This is the KEY test for transitive dependency
                
                # Additional validation: Check if this is purely coincidental
                # If intermediate values are reused across different PK values,
                # it's a genuine intermediate entity, not coincidental
                pk_per_inter = df.groupby(intermediate_col)[pk_cols].nunique()
                max_pks_per_inter = pk_per_inter.max().max() if isinstance(pk_per_inter, pd.DataFrame) else pk_per_inter.max()
                
                if max_pks_per_inter > 1:
                    # Intermediate value appears with multiple PK values
                    # This indicates a genuine entity (e.g., same product in multiple orders)
                    has_valid_target = True
                else:
                    # Each intermediate appears with only one PK
                    # Could be 1:1 or coincidental - check if it adds semantic value
                    # If uniqueness of intermediate is lower than PK, it's meaningful
                    inter_unique = df[intermediate_col].nunique()
                    pk_unique = len(df) if isinstance(pk_cols, list) and len(pk_cols) == 1 else df[pk_cols].drop_duplicates().shape[0]
                    
                    if inter_unique < pk_unique * 0.9:  # At least 10% reduction in cardinality
                        has_valid_target = True
        
        return has_valid_target
    
    def _detect_semantic_entity(self, df: pd.DataFrame, intermediate_col: str, 
                                dependent_cols: List[str]) -> Dict[str, Any]:
        """
        Generalized semantic entity detection - works on any dataset
        Determines if intermediate column represents a genuine business entity
        Based on: attribute count, diversity, cardinality, semantic clustering
        """
        result = {
            'is_entity': False,
            'confidence': 0.0,
            'reasons': [],
            'entity_type': 'unknown'
        }
        
        # Rule 1: Must have at least one dependent attribute
        if len(dependent_cols) == 0:
            result['reasons'].append('No dependent attributes')
            return result
        
        # Get cardinality metrics
        unique_count = df[intermediate_col].nunique()
        total_count = len(df)
        uniqueness_ratio = unique_count / total_count if total_count > 0 else 0
        
        # Rule 2: Analyze cardinality - too few unique values suggests categorical, not entity
        # Use dynamic threshold based on dataset size
        min_unique_threshold = max(10, total_count * 0.01)  # At least 1% or 10 whichever is higher
        min_ratio_threshold = 0.02  # At least 2% diversity
        
        if unique_count < min_unique_threshold or uniqueness_ratio < min_ratio_threshold:
            result['reasons'].append(f'Low cardinality: {unique_count} unique ({uniqueness_ratio:.1%})')
            return result
        
        # Rule 3: Check attribute diversity - are dependent attrs truly diverse?
        diverse_attrs = []
        for dep_col in dependent_cols:
            if dep_col not in df.columns:
                continue
            
            # Check if this attribute varies across intermediate values
            grouped = df.groupby(intermediate_col)[dep_col].nunique()
            # If attribute is constant per intermediate value, it's a true dependency
            if (grouped == 1).all():
                diverse_attrs.append(dep_col)
        
        if len(diverse_attrs) == 0:
            result['reasons'].append('No stable functional dependencies found')
            return result
        
        # Rule 4: Analyze semantic clustering - do attributes belong together?
        # Check for common name patterns/suffixes
        attr_tokens = set()
        for col in [intermediate_col] + diverse_attrs:
            # Extract semantic tokens from column name
            tokens = re.findall(r'[a-z]+', col.lower())
            attr_tokens.update(tokens)
        
        # Remove common words
        common_words = {'id', 'code', 'name', 'desc', 'description', 'number', 'num', 
                       'value', 'text', 'data', 'info', 'type', 'status'}
        semantic_tokens = attr_tokens - common_words
        
        # Rule 5: Calculate entity confidence score
        confidence = 0.0
        
        # Factor 1: Number of diverse attributes (more = higher confidence)
        if len(diverse_attrs) == 1:
            confidence += 0.1
        elif len(diverse_attrs) == 2:
            confidence += 0.3
        elif len(diverse_attrs) >= 3:
            confidence += 0.5
        
        # Factor 2: Uniqueness ratio (moderate = entity, too low = category, too high = transaction)
        if 0.05 <= uniqueness_ratio <= 0.7:
            confidence += 0.2
        elif 0.02 <= uniqueness_ratio <= 0.9:
            confidence += 0.1
        
        # Factor 3: Semantic clustering (shared tokens suggest cohesive entity)
        if len(semantic_tokens) >= 1:
            confidence += 0.2
        
        # Factor 4: Contact/structural attributes (strong entity indicator)
        structural_indicators = ['email', 'phone', 'address', 'street', 'city', 'state', 'zip',
                                'postal', 'country', 'website', 'url', 'contact', 'fax']
        has_structural = any(any(ind in col.lower() for ind in structural_indicators)
                            for col in diverse_attrs)
        if has_structural:
            confidence += 0.3
            result['reasons'].append('Has contact/address attributes')
        
        # Rule 6: Threshold - require minimum confidence
        if confidence >= 0.4:  # 40% confidence threshold
            result['is_entity'] = True
            result['confidence'] = confidence
            result['reasons'].append(f'{len(diverse_attrs)} diverse attributes')
            result['reasons'].append(f'Cardinality: {unique_count} unique ({uniqueness_ratio:.1%})')
            
            # Infer entity type from cardinality and attribute patterns
            if uniqueness_ratio > 0.5:
                result['entity_type'] = 'master_entity'  # High diversity, likely master data
            elif has_structural:
                result['entity_type'] = 'reference_entity'  # Has contact info
            else:
                result['entity_type'] = 'lookup_entity'  # Lower diversity, structured reference
        else:
            result['reasons'].append(f'Low confidence score: {confidence:.2f}')
        
        return result
    
    def _detect_multi_row_pattern(self, df: pd.DataFrame, table_name: str, 
                                  potential_pk_col: str) -> Dict[str, Any]:
        """
        Detect if table has multi-row patterns for same identifier
        (event tables, history tables, transaction details)
        Works generically without hardcoded column names
        """
        result = {
            'is_multi_row': False,
            'pattern_type': None,
            'evidence': []
        }
        
        # Check if potential_pk_col has duplicates
        if potential_pk_col not in df.columns:
            return result
        
        duplicate_count = df[potential_pk_col].duplicated().sum()
        if duplicate_count == 0:
            return result
        
        # Has duplicates - analyze pattern
        result['is_multi_row'] = True
        result['evidence'].append(f'{duplicate_count} duplicate values in {potential_pk_col}')
        
        # Pattern 1: Event/History pattern - look for temporal columns
        temporal_indicators = ['date', 'time', 'timestamp', 'created', 'updated', 
                              'modified', 'occurred', 'logged', 'recorded']
        temporal_cols = [col for col in df.columns 
                        if any(ind in col.lower() for ind in temporal_indicators)]
        
        if temporal_cols:
            result['pattern_type'] = 'event_history'
            result['evidence'].append(f'Temporal columns: {", ".join(temporal_cols)}')
            return result
        
        # Pattern 2: Status change pattern - look for status/state columns
        status_indicators = ['status', 'state', 'stage', 'phase', 'step', 'condition']
        status_cols = [col for col in df.columns
                      if any(ind in col.lower() for ind in status_indicators)]
        
        if status_cols:
            # Check if status values vary for same ID
            for status_col in status_cols:
                status_variety = df.groupby(potential_pk_col)[status_col].nunique()
                if (status_variety > 1).any():
                    result['pattern_type'] = 'status_history'
                    result['evidence'].append(f'Status changes in {status_col}')
                    return result
        
        # Pattern 3: Line item pattern - parent-child with item details
        item_indicators = ['item', 'line', 'detail', 'entry', 'component', 'part']
        has_item_indicator = any(ind in table_name.lower() for ind in item_indicators)
        
        if has_item_indicator:
            result['pattern_type'] = 'line_items'
            result['evidence'].append('Table name suggests line items')
            return result
        
        # Pattern 4: Sequence pattern - look for sequence/order columns
        sequence_indicators = ['seq', 'sequence', 'order', 'position', 'index', 'number', 'rank']
        sequence_cols = [col for col in df.columns
                        if any(ind in col.lower() for ind in sequence_indicators)]
        
        if sequence_cols:
            result['pattern_type'] = 'sequenced_children'
            result['evidence'].append(f'Sequence columns: {", ".join(sequence_cols)}')
            return result
        
        # Default: Generic child relationship
        result['pattern_type'] = 'child_records'
        result['evidence'].append('Multiple rows per identifier (generic 1-to-many)')
        
        return result
    
    def _verify_transitive_dependency(self, df: pd.DataFrame, pk_key: str,
                                     intermediate_col: str, target_cols: List[str]) -> bool:
        """
        Verify this is a true transitive dependency, not just a direct dependency
        PK -> intermediate -> target (not PK -> target directly)
        """
        if len(df) < 2:
            return False
        
        # Get PK columns
        pk_cols = pk_key.split('+') if '+' in pk_key else [pk_key]
        
        # Check if all PK columns exist
        if not all(col in df.columns for col in pk_cols):
            return False
        
        # Check if target depends on PK directly (if so, not transitive)
        for target_col in target_cols:
            if target_col not in df.columns:
                continue
            
            # Group by PK and check if target has only one value per PK
            grouped = df.groupby(pk_cols)[target_col].nunique()
            
            # If target varies within same PK, it's not a functional dependency at all
            if (grouped > 1).any():
                continue
            
            # Check if target varies within same intermediate value
            grouped_by_inter = df.groupby(intermediate_col)[target_col].nunique()
            
            # If target is constant per intermediate, it's dependent on intermediate
            if (grouped_by_inter == 1).all():
                return True
        
        return False
    
    def _validate_attribute_placement(self, df: pd.DataFrame, table_name: str,
                                     pk_cols: List[str], attribute: str) -> Dict[str, Any]:
        """
        Validate if attribute belongs in this table based on functional dependency
        NOT based on value duplication frequency
        """
        result = {
            'belongs_here': True,
            'reason': 'Functionally dependent on primary key',
            'alternative_key': None
        }
        
        if not pk_cols or attribute not in df.columns:
            return result
        
        # Test functional dependency: PK -> attribute
        try:
            grouped = df.groupby(pk_cols)[attribute].nunique()
            
            # If attribute is constant per PK, it belongs here
            if (grouped == 1).all():
                return result
            
            # Attribute varies per PK - investigate
            result['belongs_here'] = False
            result['reason'] = 'Attribute not functionally determined by PK'
            
            # Check if there's another column that better determines this attribute
            for col in df.columns:
                if col == attribute or col in pk_cols:
                    continue
                
                grouped_alt = df.groupby(col)[attribute].nunique()
                if (grouped_alt == 1).all():
                    result['alternative_key'] = col
                    result['reason'] = f'Functionally dependent on {col}, not PK'
                    break
            
        except Exception as e:
            # If grouping fails, assume it belongs
            result['belongs_here'] = True
            result['reason'] = f'Unable to test dependency: {str(e)}'
        
        return result
    
    def _has_identity_semantics(self, col_name: str) -> Dict[str, Any]:
        """
        Check if column name contains semantic identity patterns
        Identity columns are those that represent unique entity identifiers
        """
        col_lower = col_name.lower()
        
        # Strong identity indicators (high confidence) - check anywhere in name
        # These include underscore versions which are unambiguous
        strong_identity_patterns = [
            '_id', '_key', '_code', '_ref', '_number', 'sys_id',
            'uuid', 'guid'
        ]
        
        # Check strong patterns first
        for pattern in strong_identity_patterns:
            if pattern in col_lower:
                return {
                    'has_identity': True,
                    'confidence': 'high',
                    'reason': f'Contains identity pattern: {pattern}'
                }
        
        # Moderate identity indicators - check at word boundaries
        # These can appear at start, end, or as complete words separated by underscore
        moderate_identity_patterns = ['id', 'key', 'code', 'ref', 'number']
        
        # Split by underscore to check word boundaries
        parts = col_lower.split('_')
        for pattern in moderate_identity_patterns:
            if pattern in parts:
                return {
                    'has_identity': True,
                    'confidence': 'moderate',
                    'reason': f'Contains identity word: {pattern}'
                }
        
        # Also check if starts or ends with pattern (for camelCase)
        for pattern in moderate_identity_patterns:
            if col_lower.startswith(pattern) or col_lower.endswith(pattern):
                return {
                    'has_identity': True,
                    'confidence': 'moderate',
                    'reason': f'Starts/ends with identity pattern: {pattern}'
                }
        
        return {
            'has_identity': False,
            'confidence': 'none',
            'reason': 'No identity semantic patterns found'
        }
    
    def _is_suitable_for_primary_key(self, col_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate if column is suitable as primary key
        CRITICAL: Semantic identity must be detected BEFORE checking uniqueness
        Returns: {'is_suitable': bool, 'reason': str}
        """
        col_lower = col_name.lower()
        
        # STEP 1: Check if column has identity semantics
        identity_check = self._has_identity_semantics(col_name)
        
        if not identity_check['has_identity']:
            return {
                'is_suitable': False,
                'reason': 'No identity semantics - not a valid PK candidate'
            }
        
        # STEP 2: Check blacklist patterns (disqualifiers)
        # BUT: If column has identity semantics, blacklist should be more lenient
        # Only reject if the blacklisted pattern is NOT part of an identity compound
        # e.g., 'product' alone is bad, but 'product_code' or 'product_id' is good
        if identity_check['confidence'] != 'high':
            # For moderate confidence identity, apply blacklist strictly
            for pattern in self.PK_BLACKLIST_PATTERNS:
                if pattern in col_lower:
                    return {
                        'is_suitable': False,
                        'reason': f'Blacklisted attribute type: {pattern} (not an identity column)'
                    }
        # For high-confidence identity (has _id, _code, _key, etc.), skip blacklist check
        # These are unambiguous identity columns regardless of prefix
        
        # STEP 3: Verify uniqueness (validation after identity confirmed)
        unique_count = df[col_name].nunique()
        total_count = len(df)
        
        if unique_count < total_count:
            return {
                'is_suitable': False,
                'reason': f'Not unique: {unique_count}/{total_count} unique values'
            }
        
        # STEP 4: Check for nulls
        if df[col_name].isna().any():
            return {
                'is_suitable': False,
                'reason': 'Contains NULL values'
            }
        
        return {
            'is_suitable': True,
            'reason': f'Identity column ({identity_check["confidence"]} confidence), unique, non-null'
        }
    
    def _determine_best_primary_key(self, df: pd.DataFrame, table_name: str,
                                   candidate_keys: List[List[str]] = None) -> Dict[str, Any]:
        """
        Determine best primary key with semantic identity priority:
        1. MUST have identity semantics (id, _id, code, key, ref, number)
        2. NEVER use transactional/descriptive attributes even if unique
        3. NEVER use foreign keys as primary keys
        4. Generate surrogate when no identity column exists
        """
        result = {
            'key_type': 'surrogate',  # Default
            'key_columns': [],
            'reason': ''
        }
        
        # Check for natural key candidates with identity semantics
        natural_candidates = []
        
        for col in df.columns:
            # CHILD TABLE RULE: Skip if it's a repeating foreign key
            if self._is_repeating_foreign_key(table_name, col, df):
                continue
            
            # Skip if it's a foreign key
            if self._is_foreign_key_column(table_name, [col]):
                continue
            
            # CRITICAL: Check identity semantics first
            identity_check = self._has_identity_semantics(col)
            
            if not identity_check['has_identity']:
                # Not an identity column - skip even if unique
                continue
            
            # Validate suitability (checks blacklist, uniqueness, nulls)
            suitability = self._is_suitable_for_primary_key(col, df)
            
            if suitability['is_suitable']:
                # Calculate score based on column characteristics
                score = 100
                
                # Boost score for high-confidence identity patterns
                if identity_check['confidence'] == 'high':
                    score += 20
                elif identity_check['confidence'] == 'moderate':
                    score += 10
                
                # Prefer shorter names
                if len(col) > 20:
                    score -= 5
                
                # Prefer columns with standard identity suffixes
                col_lower = col.lower()
                if col_lower.endswith('_id'):
                    score += 15
                elif col_lower.endswith('_key') or col_lower.endswith('_code'):
                    score += 10
                
                natural_candidates.append({
                    'columns': [col],
                    'score': score,
                    'reason': suitability['reason'],
                    'identity_confidence': identity_check['confidence']
                })
        
        # Check provided candidate keys (must have at least one identity column)
        if candidate_keys:
            for key_cols in candidate_keys:
                # Skip if any column is a FK
                if self._is_foreign_key_column(table_name, key_cols):
                    continue
                
                # At least one column must have identity semantics
                has_identity = False
                for col in key_cols:
                    if self._has_identity_semantics(col)['has_identity']:
                        has_identity = True
                        break
                
                if not has_identity:
                    # Skip composite keys without identity columns
                    continue
                
                # Check if combination is unique
                if not df.duplicated(subset=key_cols, keep=False).any():
                    score = 85 - len(key_cols) * 5  # Prefer smaller keys
                    key_str = ", ".join(key_cols)
                    natural_candidates.append({
                        'columns': key_cols,
                        'score': score,
                        'reason': f'Composite key with identity semantics: {key_str}',
                        'identity_confidence': 'moderate'
                    })
        
        # Select best natural key if available
        if natural_candidates:
            best = max(natural_candidates, key=lambda x: x['score'])
            result['key_type'] = 'natural'
            result['key_columns'] = best['columns']
            result['reason'] = best['reason']
            return result
        
        # Generate surrogate key (no identity column found)
        surrogate_name = f"{table_name}_id"
        counter = 1
        while surrogate_name in df.columns:
            surrogate_name = f"{table_name}_id_{counter}"
            counter += 1
        
        result['key_type'] = 'surrogate'
        result['key_columns'] = [surrogate_name]
        result['reason'] = 'No identity column found - generated surrogate key'
        
        return result
    
    def _get_or_create_primary_key(self, table_name: str, df: pd.DataFrame) -> List[str]:
        """
        Get existing primary key or create surrogate key
        """
        if table_name in self.profiles and self.profiles[table_name].get('primary_key'):
            return list(self.profiles[table_name]['primary_key'])
        
        # Create surrogate key
        surrogate_key = f"{table_name}_id"
        
        # Make sure the column name doesn't already exist
        counter = 1
        while surrogate_key in df.columns:
            surrogate_key = f"{table_name}_id_{counter}"
            counter += 1
        
        return [surrogate_key]
    
    def _move_fk_dependent_attributes(self, tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        FACT/DIMENSION SPLIT RULE:
        For each table, check if any non-key attributes functionally depend on a FK
        rather than on the table's PK. If so, move those attributes to the referenced table.
        """
        result = {name: df.copy() for name, df in tables.items()}
        
        # Track which attributes have been moved to avoid duplicates
        moved_attributes = {}
        
        for table_name, df in tables.items():
            # Get this table's FKs
            table_fks = [fk for fk in self.foreign_keys if fk['fk_table'] == table_name]
            
            if not table_fks:
                continue
            
            # Get this table's PK
            table_pk = None
            if table_name in self.profiles and self.profiles[table_name].get('primary_key'):
                table_pk = list(self.profiles[table_name]['primary_key'])
            
            if not table_pk:
                continue
            
            # For each FK, check if any attributes depend on it rather than on the PK
            for fk in table_fks:
                fk_col = fk['fk_column']
                parent_table = fk['pk_table']
                
                if fk_col not in df.columns:
                    continue
                
                # Check each non-key column
                cols_to_move = []
                for col in df.columns:
                    # Skip key columns
                    if col in table_pk or col == fk_col:
                        continue
                    
                    # Check if column has identity semantics (skip identity columns)
                    if self._has_identity_semantics(col)['has_identity']:
                        continue
                    
                    # Test functional dependency: FK -> attribute
                    # If attribute is constant per FK value, it depends on FK, not PK
                    try:
                        fk_grouped = df.groupby(fk_col)[col].nunique()
                        if (fk_grouped == 1).all():
                            # Attribute is determined by FK
                            # Check if it's NOT determined by PK (fact attribute)
                            pk_grouped = df.groupby(table_pk)[col].nunique()
                            if not (pk_grouped == 1).all():
                                # Attribute varies within PK but constant per FK
                                # This shouldn't happen in normalized data, but handle it
                                continue
                            
                            # Attribute depends on FK - should be in parent table
                            cols_to_move.append(col)
                    except Exception:
                        continue
                
                # Move columns to parent table
                if cols_to_move and parent_table in result:
                    self.normalization_log.append(
                        f"  → Moving FK-dependent attributes from '{table_name}' to '{parent_table}': {', '.join(cols_to_move)}"
                    )
                    
                    # Get unique combinations of FK + attributes to move
                    parent_df = result[parent_table]
                    parent_pk_col = fk['pk_column']
                    
                    # Create mapping from FK to attribute values
                    fk_attr_data = df[[fk_col] + cols_to_move].drop_duplicates()
                    
                    # Merge into parent table (update existing rows)
                    for col in cols_to_move:
                        if col not in parent_df.columns:
                            # Add column to parent
                            parent_df = parent_df.merge(
                                fk_attr_data[[fk_col, col]].drop_duplicates(),
                                left_on=parent_pk_col,
                                right_on=fk_col,
                                how='left'
                            )
                            # Drop duplicate FK column
                            if fk_col in parent_df.columns and fk_col != parent_pk_col:
                                parent_df = parent_df.drop(columns=[fk_col])
                    
                    result[parent_table] = parent_df
                    
                    # Remove columns from child table
                    result[table_name] = result[table_name].drop(columns=cols_to_move)
                    
                    # Track moved attributes
                    for col in cols_to_move:
                        moved_attributes[col] = parent_table
        
        return result
    
    def add_surrogate_keys(self, tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Add keys to tables using smart selection:
        - Prefer natural keys when unique
        - Generate surrogate when needed
        - NEVER use foreign key as primary key
        - Apply CHILD TABLE RULE for repeating FKs
        - FIX (1): If surrogate <table>_id exists, use it as PK when FK columns repeat
        """
        result = {}
        
        for table_name, df in tables.items():
            # CHILD TABLE PK RULE FIX (1):
            # Check if table already has a surrogate key column (e.g., employees_skills_id)
            surrogate_col = f"{table_name}_id"
            has_surrogate = surrogate_col in df.columns
            
            # Check if table has any repeating FK columns
            has_repeating_fk = False
            for col in df.columns:
                if self._is_repeating_foreign_key(table_name, col, df):
                    has_repeating_fk = True
                    break
            
            # If table has a surrogate and a repeating FK, force surrogate as PK
            if has_surrogate and has_repeating_fk:
                result[table_name] = df
                # Update profile to use surrogate as PK
                if table_name not in self.profiles:
                    self.profiles[table_name] = {}
                self.profiles[table_name]['primary_key'] = [surrogate_col]
                
                self.normalization_log.append(f"  → Using surrogate key '{surrogate_col}' as PK for '{table_name}'")
                self.normalization_log.append(f"     Reason: Child table with repeating FK - surrogate must be PK")
                continue
            
            # Determine best primary key
            key_info = self._determine_best_primary_key(df, table_name)
            
            if key_info['key_type'] == 'natural':
                # Use natural key as-is
                result[table_name] = df
                self.normalization_log.append(f"  → Using natural key for '{table_name}': {', '.join(key_info['key_columns'])}")
                self.normalization_log.append(f"     Reason: {key_info['reason']}")
            
            else:
                # Add surrogate key
                surrogate_key = key_info['key_columns'][0]
                df_with_key = df.copy()
                df_with_key.insert(0, surrogate_key, range(1, len(df) + 1))
                result[table_name] = df_with_key
                
                # Update profile
                if table_name not in self.profiles:
                    self.profiles[table_name] = {}
                self.profiles[table_name]['primary_key'] = [surrogate_key]
                
                self.normalization_log.append(f"  → Added surrogate key '{surrogate_key}' to '{table_name}'")
                self.normalization_log.append(f"     Reason: {key_info['reason']}")
        
        return result
    
    def _is_foreign_key_column(self, table_name: str, pk_cols: List[str]) -> bool:
        """
        Check if any of the PK columns are actually foreign keys
        """
        for fk in self.foreign_keys:
            if fk['fk_table'] == table_name:
                if fk['fk_column'] in pk_cols:
                    return True
        return False
    
    def _is_repeating_foreign_key(self, table_name: str, col: str, df: pd.DataFrame) -> bool:
        """
        Detect if a column is a foreign key that has repeating values (child table pattern).
        RULE: If a column appears as an FK and has duplicate values, it cannot be the PK.
        FIX (1): Also check if column name suggests FK pattern (<entity>_id)
        """
        # Check if this column is a foreign key
        is_fk = False
        for fk in self.foreign_keys:
            if fk['fk_table'] == table_name and fk['fk_column'] == col:
                is_fk = True
                break
        
        # FIX (1): Also treat columns ending in _id as potential FKs
        if not is_fk:
            col_lower = col.lower()
            if col_lower.endswith('_id') and not col_lower == f"{table_name}_id".lower():
                # This looks like a FK column (not the table's own surrogate)
                is_fk = True
        
        if not is_fk:
            return False
        
        # Check if values repeat
        if col in df.columns:
            has_duplicates = df[col].duplicated().any()
            return has_duplicates
        
        return False
    
    def _is_child_or_event_table(self, table_name: str) -> bool:
        """
        Detect if this is a child table or event table that needs surrogate key
        """
        # Check for child/event indicators in table name
        child_indicators = ['_items', '_events', '_history', '_log', '_details', 
                           '_lines', '_entries', '_transactions']
        
        for indicator in child_indicators:
            if indicator in table_name.lower():
                return True
        
        return False
    
    def _infer_missing_foreign_keys(self, tables: Dict[str, pd.DataFrame]) -> None:
        """
        FIX (2): AUTO-GENERATE MISSING FKs for <entity>_id columns.
        
        After PK selection is finalized, infer foreign keys ONLY for columns that
        are named <entity>_id or end with "_id".
        
        A column B.x should become an FK to table A only if:
        • A has a primary key PK_A
        • column name x matches the PK naming pattern
        • values_of(B.x) are a subset of values_of(A.PK_A)
        • x is not B's own PK
        • No circular FK
        """
        inferred_fks = []
        
        # Build a map of table -> PK for fast lookup
        table_pks = {}
        for table_name in tables.keys():
            if table_name in self.profiles and self.profiles[table_name].get('primary_key'):
                pk = self.profiles[table_name]['primary_key']
                if len(pk) == 1:  # Only single-column PKs
                    table_pks[table_name] = pk[0]
        
        # For each table (child)
        for child_table, child_df in tables.items():
            child_pk = self.profiles.get(child_table, {}).get('primary_key', [])
            
            # For each column in child table
            for col in child_df.columns:
                col_lower = col.lower()
                
                # Must end with _id
                if not col_lower.endswith('_id'):
                    continue
                
                # Must not be the child table's own PK
                if col in child_pk:
                    continue
                
                # Check if FK already exists for this column
                existing_fk = False
                for fk in self.foreign_keys:
                    if fk['fk_table'] == child_table and fk['fk_column'] == col:
                        existing_fk = True
                        break
                
                if existing_fk:
                    continue
                
                # Extract entity name from column (e.g., customer_id -> customer)
                # Try to find parent table
                entity_base = col_lower.replace('_id', '')
                
                # Try multiple naming conventions
                potential_parent_tables = [
                    entity_base,  # customer
                    entity_base + 's',  # customers
                    entity_base + 'es',  # addresses (if entity ends in 's')
                ]
                
                # Find matching parent table with PK
                parent_table = None
                parent_pk_col = None
                
                for parent_name in potential_parent_tables:
                    if parent_name in table_pks:
                        parent_table = parent_name
                        parent_pk_col = table_pks[parent_name]
                        break
                
                if not parent_table:
                    continue
                
                # Prevent self-reference (unless intentional like manager_id -> employees)
                if parent_table == child_table:
                    continue
                
                # Check if would create circular dependency
                if self._would_create_circular_fk(child_table, parent_table):
                    continue
                
                # Validate subset relationship: child FK values ⊆ parent PK values
                parent_df = tables[parent_table]
                
                child_values = set(str(v) for v in child_df[col].dropna().unique())
                parent_values = set(str(v) for v in parent_df[parent_pk_col].dropna().unique())
                
                if not child_values:
                    continue
                
                # Check if subset (all child values exist in parent)
                if child_values.issubset(parent_values):
                    # Valid FK relationship found
                    fk_entry = {
                        'fk_table': child_table,
                        'fk_column': col,
                        'pk_table': parent_table,
                        'pk_column': parent_pk_col,
                        'relationship': 'inferred_id_pattern'
                    }
                    
                    inferred_fks.append(fk_entry)
                    self.foreign_keys.append(fk_entry)
                    
                    self.normalization_log.append(
                        f"  → Inferred FK: {child_table}.{col} -> {parent_table}.{parent_pk_col}"
                    )
        
        if inferred_fks:
            self.normalization_log.append(f"  ✓ Inferred {len(inferred_fks)} missing foreign keys based on <entity>_id pattern")
    
    def _would_create_circular_fk(self, table1: str, table2: str) -> bool:
        """
        Check if adding FK from table1 to table2 would create a circular dependency.
        """
        # Build dependency graph
        graph = {}
        for fk in self.foreign_keys:
            child = fk['fk_table']
            parent = fk['pk_table']
            if child not in graph:
                graph[child] = []
            graph[child].append(parent)
        
        # Add proposed edge
        if table1 not in graph:
            graph[table1] = []
        if table2 not in graph[table1]:
            graph[table1].append(table2)
        
        # DFS to detect cycle
        def has_cycle(node, visited, rec_stack):
            visited.add(node)
            rec_stack.add(node)
            
            if node in graph:
                for neighbor in graph[node]:
                    if neighbor not in visited:
                        if has_cycle(neighbor, visited, rec_stack):
                            return True
                    elif neighbor in rec_stack:
                        return True
            
            rec_stack.remove(node)
            return False
        
        return has_cycle(table1, set(), set())
    
    def normalize_table(self, table_name: str) -> Dict[str, pd.DataFrame]:
        """
        Normalize a single table to 3NF
        Driven by functional dependencies and business semantics
        """
        self.normalization_log.append(f"\n{'='*60}")
        self.normalization_log.append(f"NORMALIZING TABLE: {table_name}")
        self.normalization_log.append(f"{'='*60}")
        
        df = self.metadata[table_name]['dataframe'].copy()
        
        # Step 0: Check for business entity patterns
        self._detect_business_entities(table_name, df)
        
        # Step 1: Enforce 1NF (multi-valued attributes only)
        tables = self.enforce_1nf(table_name, df)
        
        # Step 2: Enforce 2NF on each table (partial dependencies)
        normalized_2nf = {}
        for tname, tdf in tables.items():
            result = self.enforce_2nf(tname, tdf)
            normalized_2nf.update(result)
        
        # Step 3: Enforce 3NF on each table (transitive dependencies with entity validation)
        normalized_3nf = {}
        for tname, tdf in normalized_2nf.items():
            result = self.enforce_3nf(tname, tdf)
            normalized_3nf.update(result)
        
        # Step 3.5: Apply FACT/DIMENSION SPLIT - move FK-dependent attributes to parent tables
        fact_dimension_split = self._move_fk_dependent_attributes(normalized_3nf)
        
        # Step 4: Add surrogate keys where needed (never FK as PK, child table rule)
        final_tables = self.add_surrogate_keys(fact_dimension_split)
        
        # Step 4.5: AUTO-GENERATE MISSING FKs for <entity>_id columns (FIX 2)
        self._infer_missing_foreign_keys(final_tables)
        
        # Step 5: Preserve all attributes (verification)
        self._verify_attribute_preservation(table_name, df, final_tables)
        
        return final_tables
    
    def _detect_business_entities(self, table_name: str, df: pd.DataFrame):
        """
        Analyze table structure to provide normalization guidance
        Uses generalized pattern matching without hardcoded domain rules
        """
        # Analyze column patterns
        col_patterns = {
            'identifiers': [],
            'descriptive': [],
            'temporal': [],
            'quantitative': [],
            'categorical': []
        }
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Identify different types of columns
            if any(x in col_lower for x in ['id', 'key', 'code', 'number']):
                col_patterns['identifiers'].append(col)
            elif any(x in col_lower for x in ['name', 'description', 'title', 'label']):
                col_patterns['descriptive'].append(col)
            elif any(x in col_lower for x in ['date', 'time', 'timestamp', 'created', 'updated']):
                col_patterns['temporal'].append(col)
            elif any(x in col_lower for x in ['amount', 'quantity', 'price', 'total', 'count']):
                col_patterns['quantitative'].append(col)
            elif df[col].nunique() < 50:  # Low cardinality suggests categorical
                col_patterns['categorical'].append(col)
        
        # Infer table type from patterns
        if len(col_patterns['identifiers']) > 2:
            self.normalization_log.append(f"  → Table has {len(col_patterns['identifiers'])} identifier columns")
            self.normalization_log.append(f"     Likely represents relationships between entities")
        
        if col_patterns['temporal']:
            self.normalization_log.append(f"  → Temporal columns found: {', '.join(col_patterns['temporal'][:3])}")
            self.normalization_log.append(f"     May contain event/history data")
        
        if col_patterns['quantitative']:
            self.normalization_log.append(f"  → Quantitative columns found: {', '.join(col_patterns['quantitative'][:3])}")
            self.normalization_log.append(f"     May represent transactional/fact data")
    
    def _verify_attribute_preservation(self, original_table: str, original_df: pd.DataFrame,
                                      final_tables: Dict[str, pd.DataFrame]):
        """
        Verify that all original attributes are preserved in final tables
        """
        original_cols = set(original_df.columns)
        final_cols = set()
        
        for table_name, df in final_tables.items():
            if table_name.startswith(original_table):
                final_cols.update(df.columns)
        
        # Remove surrogate keys from comparison
        surrogate_keys = {col for col in final_cols if col.endswith('_id') and col not in original_cols}
        final_cols -= surrogate_keys
        
        # Check for missing columns
        missing_cols = original_cols - final_cols
        if missing_cols:
            self.normalization_log.append(f"  ⚠ WARNING: Attributes not preserved: {missing_cols}")
        else:
            self.normalization_log.append(f"  ✓ All original attributes preserved")
    
    def normalize_all_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Normalize all tables to 3NF
        """
        all_normalized = {}
        
        for table_name in self.metadata.keys():
            normalized = self.normalize_table(table_name)
            all_normalized.update(normalized)
        
        self.normalized_tables = all_normalized
        return all_normalized
    
    def get_normalization_summary(self) -> str:
        """Get summary of normalization process"""
        summary = []
        summary.append("\n" + "="*70)
        summary.append("NORMALIZATION SUMMARY")
        summary.append("="*70)
        
        summary.extend(self.normalization_log)
        
        summary.append("\n" + "="*70)
        summary.append(f"FINAL NORMALIZED TABLES: {len(self.normalized_tables)}")
        summary.append("="*70)
        
        for table_name, df in self.normalized_tables.items():
            summary.append(f"\n{table_name}:")
            summary.append(f"  Columns: {', '.join(df.columns.tolist())}")
            summary.append(f"  Rows: {len(df)}")
        
        return "\n".join(summary)
    
    def export_normalized_tables(self, output_dir: str):
        """
        Export normalized tables to CSV and JSON
        """
        from pathlib import Path
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for table_name, df in self.normalized_tables.items():
            # Export as CSV
            csv_path = output_path / f"{table_name}.csv"
            df.to_csv(csv_path, index=False)
            
            # Export as JSON
            json_path = output_path / f"{table_name}.json"
            df.to_json(json_path, orient='records', indent=2)
            
            print(f"Exported: {table_name} ({len(df)} rows)")


if __name__ == "__main__":
    from metadata_extractor import MetadataExtractor
    from auto_profiler import AutoProfiler
    from fk_detector import ForeignKeyDetector
    
    # Test normalizer
    extractor = MetadataExtractor()
    metadata = extractor.extract_all_metadata("./input_files")
    
    profiler = AutoProfiler(metadata)
    profiles = profiler.profile_all_tables()
    
    fk_detector = ForeignKeyDetector(metadata, profiles)
    foreign_keys = fk_detector.detect_all_foreign_keys()
    
    normalizer = Normalizer(metadata, profiles, foreign_keys)
    normalized = normalizer.normalize_all_tables()
    
    print(normalizer.get_normalization_summary())
