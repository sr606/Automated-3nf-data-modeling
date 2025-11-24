"""
SQL Generator Module
Generates Oracle-compatible SQL DDL scripts:
- CREATE TABLE statements
- Primary key constraints
- Foreign key constraints
- Indexes
- Proper datatypes
- Reserved keyword handling
"""

import pandas as pd
from typing import Dict, List, Any, Set
from pathlib import Path
import re


class SQLGenerator:
    """Generate Oracle SQL DDL scripts"""
    
    # Oracle reserved keywords
    ORACLE_RESERVED_WORDS = {
        'ACCESS', 'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'AUDIT', 'BETWEEN',
        'BY', 'CHAR', 'CHECK', 'CLUSTER', 'COLUMN', 'COMMENT', 'COMPRESS', 'CONNECT',
        'CREATE', 'CURRENT', 'DATE', 'DECIMAL', 'DEFAULT', 'DELETE', 'DESC', 'DISTINCT',
        'DROP', 'ELSE', 'EXCLUSIVE', 'EXISTS', 'FILE', 'FLOAT', 'FOR', 'FROM', 'GRANT',
        'GROUP', 'HAVING', 'IDENTIFIED', 'IMMEDIATE', 'IN', 'INCREMENT', 'INDEX', 'INITIAL',
        'INSERT', 'INTEGER', 'INTERSECT', 'INTO', 'IS', 'LEVEL', 'LIKE', 'LOCK', 'LONG',
        'MAXEXTENTS', 'MINUS', 'MLSLABEL', 'MODE', 'MODIFY', 'NOAUDIT', 'NOCOMPRESS',
        'NOT', 'NOWAIT', 'NULL', 'NUMBER', 'OF', 'OFFLINE', 'ON', 'ONLINE', 'OPTION',
        'OR', 'ORDER', 'PCTFREE', 'PRIOR', 'PRIVILEGES', 'PUBLIC', 'RAW', 'RENAME',
        'RESOURCE', 'REVOKE', 'ROW', 'ROWID', 'ROWNUM', 'ROWS', 'SELECT', 'SESSION',
        'SET', 'SHARE', 'SIZE', 'SMALLINT', 'START', 'SUCCESSFUL', 'SYNONYM', 'SYSDATE',
        'TABLE', 'THEN', 'TO', 'TRIGGER', 'UID', 'UNION', 'UNIQUE', 'UPDATE', 'USER',
        'VALIDATE', 'VALUES', 'VARCHAR', 'VARCHAR2', 'VIEW', 'WHENEVER', 'WHERE', 'WITH',
        'TIMESTAMP', 'INTERVAL', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
        'TIMEZONE', 'PARTITION', 'SUBPARTITION', 'ENABLE', 'DISABLE', 'SEQUENCE',
        'TYPE', 'BODY', 'PACKAGE', 'PROCEDURE', 'FUNCTION', 'CURSOR', 'EXCEPTION'
    }
    
    def __init__(self, normalized_tables: Dict[str, pd.DataFrame], 
                 metadata: Dict[str, Any], profiles: Dict[str, Any],
                 foreign_keys: List[Dict[str, Any]]):
        self.normalized_tables = normalized_tables
        self.metadata = metadata
        self.profiles = profiles
        self.foreign_keys = foreign_keys
        self.table_schemas = {}
        self.generated_tables = set()  # Track generated tables to prevent duplicates
        
    def sanitize_identifier(self, name: str) -> str:
        """
        Sanitize SQL identifier to avoid reserved keywords and invalid characters.
        Enforces Oracle's 30-character limit with hash suffix for uniqueness.
        """
        # Replace spaces and special characters with underscores
        sanitized = re.sub(r'[^A-Za-z0-9_]', '_', name)
        
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Ensure it starts with a letter
        if not sanitized or not sanitized[0].isalpha():
            sanitized = 'col_' + sanitized
        
        # Convert to uppercase for reserved word check
        name_upper = sanitized.upper()
        
        # If it's a reserved word, add suffix
        if name_upper in self.ORACLE_RESERVED_WORDS:
            sanitized = f"{sanitized}_col"
        
        # Enforce 30-character limit with hash suffix for uniqueness
        if len(sanitized) > 30:
            # Use hash of original name for uniqueness
            import hashlib
            hash_suffix = hashlib.md5(name.encode()).hexdigest()[:4]
            # Truncate to 25 chars and add 5-char suffix (_XXXX)
            sanitized = sanitized[:25] + '_' + hash_suffix
        
        return sanitized
    
    def infer_oracle_datatype(self, df: pd.DataFrame, col: str, 
                             metadata_type: str = None) -> str:
        """
        Infer Oracle datatype from DataFrame column
        """
        if metadata_type:
            return metadata_type
        
        series = df[col]
        
        # Handle empty or all-null series
        if series.isna().all():
            return "VARCHAR2(4000)"
        
        # Check if numeric
        if pd.api.types.is_numeric_dtype(series):
            if pd.api.types.is_integer_dtype(series):
                max_val = series.max()
                if pd.notna(max_val):
                    if max_val <= 2147483647:
                        return "NUMBER(10)"
                    else:
                        return "NUMBER(19)"
                return "NUMBER(10)"
            else:
                return "NUMBER(15,2)"
        
        # Check if datetime
        if pd.api.types.is_datetime64_any_dtype(series):
            return "TIMESTAMP"
        
        # Check if boolean
        unique_values = set(str(v).lower() for v in series.dropna().unique())
        if unique_values.issubset({'true', 'false', '0', '1', 't', 'f', 'yes', 'no'}):
            return "CHAR(1)"
        
        # Default to VARCHAR2
        max_length = series.astype(str).str.len().max()
        if pd.isna(max_length) or max_length < 1:
            max_length = 255
        else:
            max_length = min(int(max_length * 1.5), 4000)
        
        return f"VARCHAR2({max_length})"
    
    def _validate_pk_not_fk(self, table_name: str, pk_columns: List[str]) -> bool:
        """
        Validate that primary key columns are not foreign keys
        """
        for fk in self.foreign_keys:
            if fk['fk_table'] == table_name:
                if fk['fk_column'] in pk_columns:
                    return False
        return True
    
    def get_primary_key_columns(self, table_name: str, df: pd.DataFrame) -> List[str]:
        """
        Determine primary key columns for a table.
        
        RULES (PRIORITY ORDER):
        1) ALWAYS prefer artificial surrogate key (<table>_id or *_id that's not an FK)
        2) NEVER use FK columns as PK
        3) NEVER use business columns (name, category, status, tag, quantity, price) as PK
        4) For tag/attribute/multivalue tables: use their own surrogate PK, not parent FK
        5) For associative tables (many-to-many): if no surrogate, use composite of all FKs
        6) Never force or create columns - only use what exists in metadata
        """
        # PRIORITY 1: Look for artificial surrogate key ending with _id
        # This handles tag tables, attribute tables, and child tables
        for col in df.columns:
            if col.endswith('_id'):
                # Must not be a foreign key to another table
                if not self._is_foreign_key_in_table(table_name, [col]):
                    # Verify it's unique
                    if df[col].nunique() == len(df) and df[col].notna().all():
                        return [col]
                    # Even if not unique now, this is the intended PK from normalization
                    # Check if this matches the table name pattern
                    col_base = col.replace('_id', '').lower()
                    table_base = table_name.lower().replace('_', '')
                    if col_base in table_base or table_base in col_base:
                        return [col]
        
        # PRIORITY 2: Check profile data for existing PK (if already determined)
        original_table = None
        for orig_name in self.metadata.keys():
            if table_name == orig_name or table_name.startswith(orig_name):
                original_table = orig_name
                break
        
        if original_table and original_table in self.profiles:
            pk = self.profiles[original_table].get('primary_key')
            if pk:
                # Check if PK columns exist in current dataframe
                pk_cols = [col for col in pk if col in df.columns]
                if pk_cols:
                    # Verify these are not foreign keys
                    if self._validate_pk_not_fk(table_name, pk_cols):
                        # Verify these are not business columns
                        if not self._are_business_columns(pk_cols):
                            # Verify uniqueness
                            if not df.duplicated(subset=pk_cols, keep=False).any():
                                return pk_cols
                            else:
                                print(f"  WARNING: PK {pk_cols} for {table_name} is not unique, checking for surrogate key")
        
        # PRIORITY 3: For associative/link tables - composite key of all FKs
        # (only if no surrogate key exists)
        fk_cols_in_table = []
        for fk in self.foreign_keys:
            if fk['fk_table'] == table_name and fk['fk_column'] in df.columns:
                fk_cols_in_table.append(fk['fk_column'])
        
        # If table has multiple FKs and few other columns, it's likely associative
        if len(fk_cols_in_table) >= 2 and len(df.columns) - len(fk_cols_in_table) <= 2:
            # Check if FK combination is unique
            if not df.duplicated(subset=fk_cols_in_table, keep=False).any():
                return fk_cols_in_table
        
        # PRIORITY 4: Look for any column with _id, _key, _code suffix that's unique and not FK
        for col in df.columns:
            col_lower = col.lower()
            if any(col_lower.endswith(suffix) for suffix in ['_id', '_key', '_code', '_ref']):
                if not self._is_foreign_key_in_table(table_name, [col]):
                    if df[col].nunique() == len(df) and df[col].notna().all():
                        return [col]
        
        # PRIORITY 5: First non-FK, non-business column that's unique
        for col in df.columns:
            if not self._is_foreign_key_in_table(table_name, [col]):
                if not self._are_business_columns([col]):
                    if df[col].nunique() == len(df) and df[col].notna().all():
                        return [col]
        
        # FALLBACK: Use first column that's not an FK
        for col in df.columns:
            if not self._is_foreign_key_in_table(table_name, [col]):
                return [col]
        
        # Last resort: use first column
        return [df.columns[0]] if len(df.columns) > 0 else []
    
    def _are_business_columns(self, columns: List[str]) -> bool:
        """
        Check if columns are business/descriptive attributes that should never be PKs.
        Business columns: name, title, description, category, status, type, tag, 
                         quantity, price, amount, date (as descriptor), etc.
        """
        business_patterns = [
            'name', 'title', 'description', 'desc', 'label',
            'category', 'status', 'state', 'type', 'kind',
            'tag', 'skill', 'attribute', 'feature', 'property',
            'quantity', 'qty', 'amount', 'price', 'cost', 'value',
            'address', 'email', 'phone', 'contact',
            'note', 'comment', 'remark', 'message'
        ]
        
        for col in columns:
            col_lower = col.lower()
            # Check if column name contains any business pattern
            for pattern in business_patterns:
                if pattern in col_lower and not col_lower.endswith('_id'):
                    return True
        
        return False
    
    def _is_foreign_key_in_table(self, table_name: str, columns: List[str]) -> bool:
        """
        Check if any of the columns are foreign keys in this table
        """
        for fk in self.foreign_keys:
            # Check if FK is in this table
            if fk['fk_table'] == table_name or table_name.startswith(fk['fk_table']):
                if fk['fk_column'] in columns:
                    return True
        
        return False
    
    def _would_create_circular_fk(self, fk_table: str, pk_table: str) -> bool:
        """
        Check if adding FK from fk_table to pk_table would create circular dependency.
        
        RULE 4: Prevent circular FK chains for execution safety.
        Example: A->B, B->C, C->A would be circular
        
        Args:
            fk_table: Table that would have the FK
            pk_table: Table being referenced
            
        Returns:
            True if circular dependency would be created
        """
        # Build dependency graph from existing FKs
        dependencies = {}
        for fk in self.foreign_keys:
            child = fk['fk_table']
            parent = fk['pk_table']
            if child not in dependencies:
                dependencies[child] = set()
            dependencies[child].add(parent)
        
        # Check if pk_table already depends on fk_table (directly or transitively)
        def has_path(start: str, target: str, visited: set = None) -> bool:
            if visited is None:
                visited = set()
            
            if start == target:
                return True
            
            if start in visited:
                return False
            
            visited.add(start)
            
            if start in dependencies:
                for parent in dependencies[start]:
                    if has_path(parent, target, visited):
                        return True
            
            return False
        
        # If pk_table depends on fk_table, adding FK would create cycle
        return has_path(pk_table, fk_table)
    
    def generate_create_table_script(self, table_name: str, df: pd.DataFrame) -> str:
        """
        Generate CREATE TABLE script for a normalized table.
        Ensures all columns and constraints are inside the table definition.
        Prevents duplicate CREATE TABLE statements.
        """
        sanitized_table_name = self.sanitize_identifier(table_name)
        
        # Check if table already generated
        if sanitized_table_name in self.generated_tables:
            return f"-- Table {sanitized_table_name} already defined\n"
        
        self.generated_tables.add(sanitized_table_name)
        
        # Get column definitions
        column_defs = []
        pk_columns = self.get_primary_key_columns(table_name, df)
        
        for col in df.columns:
            sanitized_col = self.sanitize_identifier(col)
            
            # Get datatype from metadata if available
            datatype = None
            for orig_table, meta in self.metadata.items():
                if col in meta['columns']:
                    datatype = meta['columns'][col]['datatype']
                    break
            
            if not datatype:
                datatype = self.infer_oracle_datatype(df, col)
            
            # Determine if NOT NULL
            null_ratio = df[col].isna().sum() / len(df) if len(df) > 0 else 0
            is_pk = col in pk_columns
            
            null_constraint = " NOT NULL" if (is_pk or null_ratio < 0.05) else ""
            
            column_defs.append(f"    {sanitized_col} {datatype}{null_constraint}")
        
        # Add primary key constraint INSIDE table definition
        if pk_columns:
            sanitized_pk_cols = [self.sanitize_identifier(col) for col in pk_columns]
            # Enforce 30-char limit for constraint name
            constraint_name = f"pk_{sanitized_table_name}"
            if len(constraint_name) > 30:
                import hashlib
                hash_suffix = hashlib.md5(table_name.encode()).hexdigest()[:4]
                constraint_name = f"pk_{sanitized_table_name[:22]}_{hash_suffix}"
            pk_constraint = f"    CONSTRAINT {constraint_name} PRIMARY KEY ({', '.join(sanitized_pk_cols)})"
            column_defs.append(pk_constraint)
        
        # Build CREATE TABLE statement - columns INSIDE parentheses
        sql = f"CREATE TABLE {sanitized_table_name} (\n"
        sql += ",\n".join(column_defs)
        sql += "\n);"
        
        # Store schema info
        self.table_schemas[table_name] = {
            'sanitized_name': sanitized_table_name,
            'columns': {col: self.sanitize_identifier(col) for col in df.columns},
            'primary_key': pk_columns
        }
        
        return sql
    
    def _is_valid_fk_target(self, pk_table: str, pk_column: str, fk_table: str = None) -> bool:
        """
        Validate that the referenced column is a PRIMARY KEY or UNIQUE KEY in the target table.
        
        FK acceptance rules:
          1) B.x → A.y  is valid only if  y ∈ PrimaryKey(A) OR y ∈ UniqueColumns(A)
          2) For composite PKs, the FK must reference ALL columns or the column must be separately UNIQUE
          3) If parent PK contains columns not in child table, skip FK (never force columns)
        
        Args:
            pk_table: Target table name
            pk_column: Referenced column name (single column)
            fk_table: Source table name (optional, for composite PK validation)
            
        Returns:
            True if pk_column is a complete PK or UNIQUE in pk_table, False otherwise
        """
        # Check if we have profile information for this table
        if pk_table not in self.profiles:
            return False
        
        profile = self.profiles[pk_table]
        
        # Check if column is the COMPLETE primary key (single-column PK)
        if profile.get('primary_key'):
            pk = profile['primary_key']
            
            # Single-column PK - always valid
            if len(pk) == 1 and pk_column in pk:
                return True
            
            # Composite PK - must check if column is separately UNIQUE
            elif len(pk) > 1 and pk_column in pk:
                # For composite PKs, check if this column alone is UNIQUE (in candidate_keys)
                if profile.get('candidate_keys'):
                    for candidate_key in profile['candidate_keys']:
                        if len(candidate_key) == 1 and pk_column in candidate_key:
                            return True
                
                # RULE 2: For composite PKs, we should reference ALL columns
                # If only one column is referenced, it must be UNIQUE (checked above)
                # Since it's not UNIQUE alone, reject this FK
                return False
        
        # Check if column is in any single-column candidate key (unique constraint)
        if profile.get('candidate_keys'):
            for candidate_key in profile['candidate_keys']:
                # Single-column unique constraint
                if len(candidate_key) == 1 and pk_column in candidate_key:
                    return True
        
        return False
    
    def _can_reference_composite_pk(self, fk_table: str, pk_table: str, pk_columns: List[str]) -> bool:
        """
        Check if child table can reference a composite PK.
        
        RULE: Child must have ALL columns of the composite PK, or we skip the FK.
        Never force or add columns to satisfy FK constraints.
        
        Args:
            fk_table: Child table name
            pk_table: Parent table name  
            pk_columns: All columns in parent's composite PK
            
        Returns:
            True if child has all required columns, False otherwise
        """
        if fk_table not in self.normalized_tables:
            return False
        
        child_df = self.normalized_tables[fk_table]
        
        # Check if ALL composite PK columns exist in child
        for col in pk_columns:
            if col not in child_df.columns:
                return False
        
        return True
    
    def generate_foreign_key_constraints(self) -> List[str]:
        """
        Generate ALTER TABLE statements for foreign key constraints.
        Only generates FKs where the referenced column is a PRIMARY KEY or UNIQUE KEY.
        """
        constraints = []
        constraint_counter = 1
        skipped_fks = []
        
        for fk in self.foreign_keys:
            fk_table = fk['fk_table']
            fk_column = fk['fk_column']
            pk_table = fk['pk_table']
            pk_column = fk['pk_column']
            
            # Check if both tables exist in normalized tables
            fk_table_exists = False
            pk_table_exists = False
            
            actual_fk_table = None
            actual_pk_table = None
            
            # Find actual table names (might be renamed during normalization)
            for table_name in self.normalized_tables.keys():
                if table_name == fk_table or table_name.startswith(fk_table):
                    # Check if FK column exists
                    if fk_column in self.normalized_tables[table_name].columns:
                        fk_table_exists = True
                        actual_fk_table = table_name
                        break
            
            for table_name in self.normalized_tables.keys():
                if table_name == pk_table or table_name.startswith(pk_table):
                    # Check if PK column exists
                    if pk_column in self.normalized_tables[table_name].columns:
                        pk_table_exists = True
                        actual_pk_table = table_name
                        break
            
            if not (fk_table_exists and pk_table_exists):
                continue
            
            # Get sanitized names
            if actual_fk_table in self.table_schemas:
                sanitized_fk_table = self.table_schemas[actual_fk_table]['sanitized_name']
                sanitized_fk_column = self.table_schemas[actual_fk_table]['columns'].get(fk_column)
            else:
                sanitized_fk_table = self.sanitize_identifier(actual_fk_table)
                sanitized_fk_column = self.sanitize_identifier(fk_column)
            
            if actual_pk_table in self.table_schemas:
                sanitized_pk_table = self.table_schemas[actual_pk_table]['sanitized_name']
                sanitized_pk_column = self.table_schemas[actual_pk_table]['columns'].get(pk_column)
            else:
                sanitized_pk_table = self.sanitize_identifier(actual_pk_table)
                sanitized_pk_column = self.sanitize_identifier(pk_column)
            
            if not (sanitized_fk_column and sanitized_pk_column):
                continue
            
            # CRITICAL: Validate that referenced column is PK or UNIQUE in target table
            # RULE 2: For composite PKs, validate all columns are present
            if not self._is_valid_fk_target(actual_pk_table, pk_column, actual_fk_table):
                # Check if this is a composite PK issue
                if actual_pk_table in self.profiles:
                    parent_pk = self.profiles[actual_pk_table].get('primary_key', [])
                    if len(parent_pk) > 1 and pk_column in parent_pk:
                        # Composite PK - check if we can reference all columns
                        if not self._can_reference_composite_pk(actual_fk_table, actual_pk_table, parent_pk):
                            skipped_fks.append({
                                'fk': f"{actual_fk_table}.{fk_column} -> {actual_pk_table}.{pk_column}",
                                'reason': f"Composite PK {parent_pk} in {actual_pk_table} - child missing required columns"
                            })
                        else:
                            skipped_fks.append({
                                'fk': f"{actual_fk_table}.{fk_column} -> {actual_pk_table}.{pk_column}",
                                'reason': f"{pk_column} is part of composite PK {parent_pk} but not UNIQUE alone in {actual_pk_table}"
                            })
                    else:
                        skipped_fks.append({
                            'fk': f"{actual_fk_table}.{fk_column} -> {actual_pk_table}.{pk_column}",
                            'reason': f"{pk_column} is not a PRIMARY KEY or UNIQUE KEY in {actual_pk_table}"
                        })
                else:
                    skipped_fks.append({
                        'fk': f"{actual_fk_table}.{fk_column} -> {actual_pk_table}.{pk_column}",
                        'reason': f"No profile data for {actual_pk_table}"
                    })
                continue
            
            # RULE 4: Check for circular dependencies (execution safety)
            if self._would_create_circular_fk(actual_fk_table, actual_pk_table):
                skipped_fks.append({
                    'fk': f"{actual_fk_table}.{fk_column} -> {actual_pk_table}.{pk_column}",
                    'reason': f"Would create circular FK dependency"
                })
                continue
            
            # Generate constraint name with 30-char limit
            import hashlib
            constraint_name = f"fk_{sanitized_fk_table}_{constraint_counter}"
            if len(constraint_name) > 30:
                # Create unique constraint name with hash
                hash_suffix = hashlib.md5(f"{actual_fk_table}_{actual_pk_table}_{constraint_counter}".encode()).hexdigest()[:4]
                constraint_name = f"fk_{sanitized_fk_table[:20]}_{hash_suffix}"
                # Ensure still under 30 chars
                if len(constraint_name) > 30:
                    constraint_name = f"fk_{constraint_counter}_{hash_suffix}"
            
            # Generate ALTER TABLE statement
            sql = f"ALTER TABLE {sanitized_fk_table}\n"
            sql += f"    ADD CONSTRAINT {constraint_name}\n"
            sql += f"    FOREIGN KEY ({sanitized_fk_column})\n"
            sql += f"    REFERENCES {sanitized_pk_table}({sanitized_pk_column});"
            
            constraints.append(sql)
            constraint_counter += 1
        
        # Report skipped FKs
        if skipped_fks:
            print(f"\n[!] Skipped {len(skipped_fks)} foreign key(s) - referenced column not PK/UNIQUE:")
            for skipped in skipped_fks:
                print(f"    - {skipped['fk']}: {skipped['reason']}")
        
        return constraints
    
    def generate_indexes(self) -> List[str]:
        """
        Generate CREATE INDEX statements for foreign keys and frequently queried columns
        """
        indexes = []
        index_counter = 1
        
        # Create indexes on foreign key columns
        for fk in self.foreign_keys:
            fk_table = fk['fk_table']
            fk_column = fk['fk_column']
            
            # Find actual table name
            actual_fk_table = None
            for table_name in self.normalized_tables.keys():
                if table_name == fk_table or table_name.startswith(fk_table):
                    if fk_column in self.normalized_tables[table_name].columns:
                        actual_fk_table = table_name
                        break
            
            if not actual_fk_table:
                continue
            
            if actual_fk_table in self.table_schemas:
                sanitized_table = self.table_schemas[actual_fk_table]['sanitized_name']
                sanitized_column = self.table_schemas[actual_fk_table]['columns'].get(fk_column)
            else:
                sanitized_table = self.sanitize_identifier(actual_fk_table)
                sanitized_column = self.sanitize_identifier(fk_column)
            
            if not sanitized_column:
                continue
            
            # Generate index name with 30-char limit
            import hashlib
            index_name = f"idx_{sanitized_table}_{index_counter}"
            if len(index_name) > 30:
                # Create unique index name with hash
                hash_suffix = hashlib.md5(f"{actual_fk_table}_{fk_column}_{index_counter}".encode()).hexdigest()[:4]
                index_name = f"idx_{sanitized_table[:21]}_{hash_suffix}"
                # Ensure still under 30 chars
                if len(index_name) > 30:
                    index_name = f"idx_{index_counter}_{hash_suffix}"
            
            sql = f"CREATE INDEX {index_name} ON {sanitized_table}({sanitized_column});"
            indexes.append(sql)
            index_counter += 1
        
        return indexes
    
    def generate_all_sql(self, output_dir: str):
        """
        Generate all SQL scripts and save to files
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        all_sql = []
        
        # Header
        all_sql.append("-- =====================================================")
        all_sql.append("-- Auto-generated 3NF Normalized Database Schema")
        all_sql.append("-- Generated by: Automated 3NF Data Modeling System")
        all_sql.append("-- =====================================================")
        all_sql.append("")
        
        # Drop tables (in reverse order to avoid FK constraints)
        all_sql.append("-- Drop existing tables (if any)")
        all_sql.append("-- Execute these statements if you need to recreate the schema")
        all_sql.append("")
        
        for table_name in reversed(list(self.normalized_tables.keys())):
            sanitized_name = self.sanitize_identifier(table_name)
            all_sql.append(f"-- DROP TABLE {sanitized_name} CASCADE CONSTRAINTS;")
        all_sql.append("")
        
        # Create tables
        all_sql.append("-- =====================================================")
        all_sql.append("-- CREATE TABLE statements")
        all_sql.append("-- =====================================================")
        all_sql.append("")
        
        for table_name, df in self.normalized_tables.items():
            all_sql.append(f"-- Table: {table_name}")
            all_sql.append(f"-- Rows: {len(df)}")
            create_sql = self.generate_create_table_script(table_name, df)
            all_sql.append(create_sql)
            all_sql.append("")
        
        # Foreign key constraints
        all_sql.append("-- =====================================================")
        all_sql.append("-- FOREIGN KEY constraints")
        all_sql.append("-- =====================================================")
        all_sql.append("")
        
        fk_constraints = self.generate_foreign_key_constraints()
        for constraint in fk_constraints:
            all_sql.append(constraint)
            all_sql.append("")
        
        # Indexes
        all_sql.append("-- =====================================================")
        all_sql.append("-- CREATE INDEX statements")
        all_sql.append("-- =====================================================")
        all_sql.append("")
        
        indexes = self.generate_indexes()
        for index in indexes:
            all_sql.append(index)
            all_sql.append("")
        
        # Commit
        all_sql.append("-- =====================================================")
        all_sql.append("COMMIT;")
        all_sql.append("-- =====================================================")
        
        # Save to file
        sql_content = "\n".join(all_sql)
        output_file = output_path / "normalized_schema.sql"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(sql_content)
        
        print(f"\n✓ SQL script generated: {output_file}")
        print(f"  - Tables: {len(self.normalized_tables)}")
        print(f"  - Foreign Keys: {len(fk_constraints)}")
        print(f"  - Indexes: {len(indexes)}")
        
        return str(output_file)
    
    def generate_complete_schema(self) -> str:
        """
        Generate complete SQL schema as a string (for testing/export).
        Returns the full DDL script without saving to file.
        """
        all_sql = []
        
        # Header
        all_sql.append("-- =====================================================")
        all_sql.append("-- Auto-generated 3NF Normalized Database Schema")
        all_sql.append("-- Generated by: Automated 3NF Data Modeling System")
        all_sql.append("-- =====================================================")
        all_sql.append("")
        
        # Drop tables (in reverse order to avoid FK constraints)
        all_sql.append("-- Drop existing tables (if any)")
        all_sql.append("-- Execute these statements if you need to recreate the schema")
        all_sql.append("")
        
        for table_name in reversed(list(self.normalized_tables.keys())):
            sanitized_name = self.sanitize_identifier(table_name)
            all_sql.append(f"-- DROP TABLE {sanitized_name} CASCADE CONSTRAINTS;")
        all_sql.append("")
        
        # Create tables
        all_sql.append("-- =====================================================")
        all_sql.append("-- CREATE TABLE statements")
        all_sql.append("-- =====================================================")
        all_sql.append("")
        
        for table_name, df in self.normalized_tables.items():
            all_sql.append(f"-- Table: {table_name}")
            all_sql.append(f"-- Rows: {len(df)}")
            create_sql = self.generate_create_table_script(table_name, df)
            all_sql.append(create_sql)
            all_sql.append("")
        
        # Foreign key constraints
        all_sql.append("-- =====================================================")
        all_sql.append("-- FOREIGN KEY constraints")
        all_sql.append("-- =====================================================")
        all_sql.append("")
        
        fk_constraints = self.generate_foreign_key_constraints()
        for constraint in fk_constraints:
            all_sql.append(constraint)
            all_sql.append("")
        
        # Indexes
        all_sql.append("-- =====================================================")
        all_sql.append("-- CREATE INDEX statements")
        all_sql.append("-- =====================================================")
        all_sql.append("")
        
        indexes = self.generate_indexes()
        for index in indexes:
            all_sql.append(index)
            all_sql.append("")
        
        # Commit
        all_sql.append("-- =====================================================")
        all_sql.append("COMMIT;")
        all_sql.append("-- =====================================================")
        
        return "\n".join(all_sql)


if __name__ == "__main__":
    from metadata_extractor import MetadataExtractor
    from auto_profiler import AutoProfiler
    from fk_detector import ForeignKeyDetector
    from normalizer import Normalizer
    
    # Test SQL generator
    extractor = MetadataExtractor()
    metadata = extractor.extract_all_metadata("./input_files")
    
    profiler = AutoProfiler(metadata)
    profiles = profiler.profile_all_tables()
    
    fk_detector = ForeignKeyDetector(metadata, profiles)
    foreign_keys = fk_detector.detect_all_foreign_keys()
    
    normalizer = Normalizer(metadata, profiles, foreign_keys)
    normalized = normalizer.normalize_all_tables()
    
    sql_gen = SQLGenerator(normalized, metadata, profiles, foreign_keys)
    sql_gen.generate_all_sql("./sql_output")
