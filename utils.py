"""
Utility Functions Module
Provides helper utilities for:
- SQL reserved keyword sanitization
- Surrogate key generation
- Datatype normalization
- ERD generation
- Null handling strategies
"""

import graphviz
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd


class ERDGenerator:
    """Generate Entity-Relationship Diagrams"""
    
    def __init__(self, normalized_tables: Dict[str, pd.DataFrame],
                 profiles: Dict[str, Any], foreign_keys: List[Dict[str, Any]]):
        self.normalized_tables = normalized_tables
        self.profiles = profiles
        self.foreign_keys = foreign_keys
        
    def generate_erd(self, output_folder: str) -> str:
        """
        Generate ERD using Graphviz
        """
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create digraph
        dot = graphviz.Digraph(comment='3NF Normalized Database Schema')
        dot.attr(rankdir='TB', splines='ortho')
        dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightblue')
        
        # Add tables as nodes
        for table_name, df in self.normalized_tables.items():
            # Build table structure
            label = f"<<TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0'>"
            label += f"<TR><TD BGCOLOR='darkblue'><FONT COLOR='white'><B>{table_name}</B></FONT></TD></TR>"
            
            # Find primary key columns
            pk_cols = self._get_primary_key_columns(table_name, df)
            
            # Add columns
            for col in df.columns:
                is_pk = col in pk_cols
                icon = "🔑 " if is_pk else ""
                
                label += f"<TR><TD ALIGN='LEFT'>{icon}{col}</TD></TR>"
            
            label += "</TABLE>>"
            
            dot.node(table_name, label=label)
        
        # Add foreign key relationships as edges
        for fk in self.foreign_keys:
            fk_table = fk['fk_table']
            pk_table = fk['pk_table']
            
            # Find actual table names in normalized tables
            actual_fk_table = None
            actual_pk_table = None
            
            for table_name in self.normalized_tables.keys():
                if table_name == fk_table or table_name.startswith(fk_table):
                    if fk['fk_column'] in self.normalized_tables[table_name].columns:
                        actual_fk_table = table_name
                
                if table_name == pk_table or table_name.startswith(pk_table):
                    if fk['pk_column'] in self.normalized_tables[table_name].columns:
                        actual_pk_table = table_name
            
            if actual_fk_table and actual_pk_table:
                # Add edge with label
                label = f"{fk['fk_column']} → {fk['pk_column']}"
                dot.edge(actual_fk_table, actual_pk_table, label=label, 
                        color='darkgreen', fontsize='10')
        
        # Generate output files
        output_file = output_path / "normalized_erd"
        
        try:
            # Try to render as PNG
            dot.render(str(output_file), format='png', cleanup=True)
            print(f"✓ ERD generated: {output_file}.png")
            return str(output_file) + ".png"
        except Exception as e:
            # Fall back to saving just the DOT file
            dot_file = str(output_file) + ".dot"
            with open(dot_file, 'w') as f:
                f.write(dot.source)
            print(f"⚠ Could not render PNG (Graphviz not installed?)")
            print(f"  Saved DOT file: {dot_file}")
            print(f"  You can visualize it at: https://dreampuf.github.io/GraphvizOnline/")
            return dot_file
    
    def generate_mermaid_erd(self, output_folder: str) -> str:
        """
        Generate ERD using Mermaid syntax (text-based)
        """
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)
        
        mermaid = ["erDiagram"]
        
        # Add tables
        for table_name, df in self.normalized_tables.items():
            pk_cols = self._get_primary_key_columns(table_name, df)
            
            mermaid.append(f"    {table_name} {{")
            
            for col in df.columns:
                is_pk = col in pk_cols
                key_marker = "PK" if is_pk else ""
                datatype = self._infer_simple_type(df[col])
                
                mermaid.append(f"        {datatype} {col} {key_marker}".rstrip())
            
            mermaid.append("    }")
        
        # Add relationships
        for fk in self.foreign_keys:
            fk_table = fk['fk_table']
            pk_table = fk['pk_table']
            
            # Find actual table names
            actual_fk_table = None
            actual_pk_table = None
            
            for table_name in self.normalized_tables.keys():
                if table_name == fk_table or table_name.startswith(fk_table):
                    if fk['fk_column'] in self.normalized_tables[table_name].columns:
                        actual_fk_table = table_name
                
                if table_name == pk_table or table_name.startswith(pk_table):
                    if fk['pk_column'] in self.normalized_tables[table_name].columns:
                        actual_pk_table = table_name
            
            if actual_fk_table and actual_pk_table:
                # Many-to-one relationship
                mermaid.append(f"    {actual_pk_table} ||--o{{ {actual_fk_table} : has")
        
        # Save to file
        output_file = output_path / "normalized_erd.mmd"
        with open(output_file, 'w') as f:
            f.write('\n'.join(mermaid))
        
        print(f"✓ Mermaid ERD generated: {output_file}")
        print(f"  You can visualize it at: https://mermaid.live/")
        
        return str(output_file)
    
    def _get_primary_key_columns(self, table_name: str, df: pd.DataFrame) -> List[str]:
        """Get primary key columns for a table"""
        # Check profiles
        for orig_name in self.profiles.keys():
            if table_name.startswith(orig_name):
                pk = self.profiles[orig_name].get('primary_key')
                if pk:
                    pk_cols = [col for col in pk if col in df.columns]
                    if pk_cols:
                        return pk_cols
        
        # Look for surrogate key
        for col in df.columns:
            if col.endswith('_id'):
                return [col]
        
        return []
    
    def _infer_simple_type(self, series: pd.Series) -> str:
        """Infer simple type for Mermaid diagram"""
        if pd.api.types.is_integer_dtype(series):
            return "int"
        elif pd.api.types.is_float_dtype(series):
            return "float"
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "date"
        else:
            return "string"


class KeywordSanitizer:
    """Sanitize SQL identifiers"""
    
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
        'VALIDATE', 'VALUES', 'VARCHAR', 'VARCHAR2', 'VIEW', 'WHENEVER', 'WHERE', 'WITH'
    }
    
    @staticmethod
    def is_reserved(word: str) -> bool:
        """Check if word is a reserved keyword"""
        return word.upper() in KeywordSanitizer.ORACLE_RESERVED_WORDS
    
    @staticmethod
    def sanitize(name: str, suffix: str = "_col") -> str:
        """Sanitize identifier by adding suffix if reserved"""
        if KeywordSanitizer.is_reserved(name):
            return f"{name}{suffix}"
        return name


class SurrogateKeyGenerator:
    """Generate surrogate keys"""
    
    @staticmethod
    def generate_key_name(table_name: str, existing_columns: List[str]) -> str:
        """Generate a surrogate key name"""
        base_name = f"{table_name}_id"
        
        # Ensure uniqueness
        counter = 1
        key_name = base_name
        while key_name in existing_columns:
            key_name = f"{base_name}_{counter}"
            counter += 1
        
        return key_name
    
    @staticmethod
    def add_surrogate_key(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Add surrogate key to dataframe"""
        key_name = SurrogateKeyGenerator.generate_key_name(table_name, df.columns.tolist())
        
        df_with_key = df.copy()
        df_with_key.insert(0, key_name, range(1, len(df) + 1))
        
        return df_with_key


class DatatypeMapper:
    """Map between different datatype systems"""
    
    @staticmethod
    def pandas_to_oracle(pandas_dtype) -> str:
        """Map pandas dtype to Oracle datatype"""
        if pd.api.types.is_integer_dtype(pandas_dtype):
            return "NUMBER(10)"
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return "NUMBER(15,2)"
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            return "TIMESTAMP"
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            return "CHAR(1)"
        else:
            return "VARCHAR2(4000)"
    
    @staticmethod
    def normalize_datatype(value: Any) -> str:
        """Normalize a value to appropriate datatype"""
        if isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, bool):
            return "BOOLEAN"
        elif pd.isna(value):
            return "NULL"
        else:
            return "STRING"


class NullHandler:
    """Strategies for handling null values"""
    
    @staticmethod
    def get_null_strategy(null_ratio: float) -> str:
        """Determine null handling strategy based on null ratio"""
        if null_ratio < 0.01:
            return "NOT NULL"
        elif null_ratio < 0.1:
            return "NULLABLE - Consider default value"
        elif null_ratio < 0.5:
            return "NULLABLE - Common"
        else:
            return "NULLABLE - High null ratio, consider redesign"
    
    @staticmethod
    def suggest_default_value(series: pd.Series) -> Any:
        """Suggest a default value for a column"""
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return None
        
        # For numeric, use median
        if pd.api.types.is_numeric_dtype(series):
            return non_null.median()
        
        # For categorical, use mode
        mode_value = non_null.mode()
        if len(mode_value) > 0:
            return mode_value[0]
        
        return None


if __name__ == "__main__":
    # Test utilities
    print("Testing Keyword Sanitizer:")
    print(f"  'SELECT' is reserved: {KeywordSanitizer.is_reserved('SELECT')}")
    print(f"  'SELECT' sanitized: {KeywordSanitizer.sanitize('SELECT')}")
    print(f"  'customer_name' is reserved: {KeywordSanitizer.is_reserved('customer_name')}")
    
    print("\nTesting Surrogate Key Generator:")
    test_df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
    print(f"  Generated key name: {SurrogateKeyGenerator.generate_key_name('users', test_df.columns.tolist())}")
    
    print("\nTesting Datatype Mapper:")
    print(f"  int64 -> Oracle: {DatatypeMapper.pandas_to_oracle('int64')}")
    print(f"  object -> Oracle: {DatatypeMapper.pandas_to_oracle('object')}")
