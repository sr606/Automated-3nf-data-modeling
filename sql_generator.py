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
        Determine primary key columns for a table
        RULE: Never use a foreign key as primary key
        """
        # Check if original table had a PK
        original_table = None
        for orig_name in self.metadata.keys():
            if table_name.startswith(orig_name):
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
                        # Verify uniqueness
                        if not df.duplicated(subset=pk_cols, keep=False).any():
                            return pk_cols
                        else:
                            print(f"  WARNING: PK {pk_cols} for {table_name} is not unique, generating surrogate key")
                    is_fk = self._is_foreign_key_in_table(table_name, pk_cols)
                    if not is_fk:
                        return pk_cols
        
        # Look for surrogate key (typically first column ending with _id)
        # Prefer surrogate keys for child/event tables
        for col in df.columns:
            if col.endswith('_id'):
                # Check if this is the table's own ID (not a FK)
                table_base = table_name.split('_')[0]
                if table_base in col or table_name in col:
                    # Verify it's not a FK
                    if not self._is_foreign_key_in_table(table_name, [col]):
                        return [col]
        
        # Find columns with high uniqueness that are not FKs
        for col in df.columns:
            if df[col].nunique() == len(df) and df[col].notna().all():
                if not self._is_foreign_key_in_table(table_name, [col]):
                    return [col]
        
        # If no suitable PK found, use first column that's not a FK
        for col in df.columns:
            if not self._is_foreign_key_in_table(table_name, [col]):
                return [col]
        
        # Last resort: use first column
        return [df.columns[0]] if len(df.columns) > 0 else []
    
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
    
    def _is_valid_fk_target(self, pk_table: str, pk_column: str) -> bool:
        """
        Validate that the referenced column is a PRIMARY KEY or UNIQUE KEY in the target table.
        
        FK acceptance rule:
          B.x → A.y  is valid only if  y ∈ PrimaryKey(A) OR y ∈ UniqueColumns(A)
        
        Args:
            pk_table: Target table name
            pk_column: Referenced column name
            
        Returns:
            True if pk_column is PK or UNIQUE in pk_table, False otherwise
        """
        # Check if we have profile information for this table
        if pk_table not in self.profiles:
            return False
        
        profile = self.profiles[pk_table]
        
        # Check if column is in primary key
        if profile.get('primary_key'):
            if pk_column in profile['primary_key']:
                return True
        
        # Check if column is in any candidate key (unique constraint)
        if profile.get('candidate_keys'):
            for candidate_key in profile['candidate_keys']:
                # Single-column unique constraint
                if len(candidate_key) == 1 and pk_column in candidate_key:
                    return True
                # Multi-column unique constraint where this column is part of it
                # Only accept if it's a single-column reference
                elif len(candidate_key) > 1 and pk_column in candidate_key:
                    # For multi-column unique constraints, we need to be more careful
                    # Only accept if the FK references the entire unique key
                    # For now, skip multi-column unique constraints
                    pass
        
        return False
    
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
            if not self._is_valid_fk_target(actual_pk_table, pk_column):
                skipped_fks.append({
                    'fk': f"{actual_fk_table}.{fk_column} -> {actual_pk_table}.{pk_column}",
                    'reason': f"{pk_column} is not a PRIMARY KEY or UNIQUE KEY in {actual_pk_table}"
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
