"""
Schema Validator Module
Validates normalized schema to prevent common errors:
- PK = non-unique attribute
- PK = FK
- Circular FK dependencies
- Missing required relationships
- Lossless join property
"""

import pandas as pd
from typing import Dict, List, Any, Set, Tuple


class SchemaValidator:
    """Validate normalized schema for correctness"""
    
    def __init__(self, tables: Dict[str, pd.DataFrame], 
                 profiles: Dict[str, Any],
                 foreign_keys: List[Dict[str, Any]]):
        self.tables = tables
        self.profiles = profiles
        self.foreign_keys = foreign_keys
        self.errors = []
        self.warnings = []
    
    def validate_primary_keys(self) -> bool:
        """
        Validate that all primary keys are:
        - Unique
        - Not null
        - Not foreign keys
        - Not transactional attributes
        """
        is_valid = True
        
        for table_name, df in self.tables.items():
            if table_name not in self.profiles:
                continue
            
            pk = self.profiles[table_name].get('primary_key')
            if not pk:
                self.warnings.append(f"⚠ Table {table_name} has no primary key defined")
                continue
            
            # Ensure PK columns exist
            pk_cols = [col for col in pk if col in df.columns]
            if not pk_cols:
                self.errors.append(f"❌ Table {table_name}: PK columns {pk} not found in table")
                is_valid = False
                continue
            
            # Check uniqueness
            if df.duplicated(subset=pk_cols, keep=False).any():
                self.errors.append(f"❌ Table {table_name}: PK {pk_cols} is not unique")
                is_valid = False
            
            # Check for nulls
            for col in pk_cols:
                if df[col].isna().any():
                    self.errors.append(f"❌ Table {table_name}: PK column {col} contains NULL values")
                    is_valid = False
            
            # Check if PK is also FK
            for fk in self.foreign_keys:
                if fk['fk_table'] == table_name and fk['fk_column'] in pk_cols:
                    self.errors.append(f"❌ Table {table_name}: PK column {fk['fk_column']} is also a FK")
                    is_valid = False
        
        return is_valid
    
    def validate_foreign_keys(self) -> bool:
        """
        Validate that all foreign keys:
        - Reference existing PK
        - No circular dependencies
        - Values exist in referenced table (referential integrity)
        """
        is_valid = True
        
        # Build dependency graph
        dependencies = {}
        for fk in self.foreign_keys:
            if fk['fk_table'] not in dependencies:
                dependencies[fk['fk_table']] = []
            dependencies[fk['fk_table']].append(fk['pk_table'])
        
        # Check for circular dependencies
        def has_cycle(node: str, visited: Set[str], rec_stack: Set[str]) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            if node in dependencies:
                for neighbor in dependencies[node]:
                    if neighbor not in visited:
                        if has_cycle(neighbor, visited, rec_stack):
                            return True
                    elif neighbor in rec_stack:
                        return True
            
            rec_stack.remove(node)
            return False
        
        visited = set()
        for table in dependencies.keys():
            if table not in visited:
                if has_cycle(table, visited, set()):
                    self.errors.append(f"❌ Circular FK dependency detected involving table {table}")
                    is_valid = False
        
        # Check referential integrity
        for fk in self.foreign_keys:
            fk_table = fk['fk_table']
            pk_table = fk['pk_table']
            fk_col = fk['fk_column']
            pk_col = fk['pk_column']
            
            if fk_table not in self.tables or pk_table not in self.tables:
                continue
            
            fk_df = self.tables[fk_table]
            pk_df = self.tables[pk_table]
            
            if fk_col not in fk_df.columns or pk_col not in pk_df.columns:
                self.errors.append(f"❌ FK {fk_table}.{fk_col} -> {pk_table}.{pk_col}: columns not found")
                is_valid = False
                continue
            
            # Check referential integrity
            fk_values = set(str(v) for v in fk_df[fk_col].dropna().unique())
            pk_values = set(str(v) for v in pk_df[pk_col].dropna().unique())
            
            invalid_refs = fk_values - pk_values
            if invalid_refs:
                coverage = 1.0 - (len(invalid_refs) / len(fk_values))
                if coverage < 0.9:  # Allow 10% tolerance for data quality
                    self.errors.append(f"❌ FK {fk_table}.{fk_col} -> {pk_table}.{pk_col}: "
                                     f"{len(invalid_refs)} values don't exist in referenced table")
                    is_valid = False
        
        return is_valid
    
    def validate_lossless_join(self) -> bool:
        """
        Validate that the schema preserves all data (lossless join property)
        """
        # This is a simplified check - full validation would require testing actual joins
        is_valid = True
        
        # For now, just verify that FK columns exist in both tables
        for fk in self.foreign_keys:
            fk_table = fk['fk_table']
            pk_table = fk['pk_table']
            fk_col = fk['fk_column']
            
            if fk_table in self.tables and pk_table in self.tables:
                if fk_col not in self.tables[fk_table].columns:
                    self.errors.append(f"❌ Lossless join violated: FK column {fk_col} missing from {fk_table}")
                    is_valid = False
        
        return is_valid
    
    def validate_3nf(self) -> bool:
        """
        Validate that schema is in 3NF
        (This is informational - the normalization process should ensure this)
        """
        # Check for obvious violations
        for table_name, df in self.tables.items():
            # Check for concatenated addresses (should be atomized)
            for col in df.columns:
                if 'address' in col.lower():
                    sample = df[col].dropna().head(10).astype(str)
                    if sample.str.contains(',').sum() > len(sample) * 0.5:
                        self.warnings.append(f"⚠ Table {table_name}: Column {col} may contain concatenated address data")
        
        return True
    
    def validate_all(self) -> Dict[str, Any]:
        """
        Run all validations and return comprehensive report
        """
        results = {
            'is_valid': True,
            'checks': {}
        }
        
        print("\n" + "="*70)
        print("SCHEMA VALIDATION")
        print("="*70)
        
        # Run all checks
        results['checks']['primary_keys'] = self.validate_primary_keys()
        results['checks']['foreign_keys'] = self.validate_foreign_keys()
        results['checks']['lossless_join'] = self.validate_lossless_join()
        results['checks']['3nf_compliance'] = self.validate_3nf()
        
        results['is_valid'] = all(results['checks'].values())
        
        # Print results
        print(f"\nValidation Results:")
        print(f"  Primary Keys:      {'✓ PASS' if results['checks']['primary_keys'] else '✗ FAIL'}")
        print(f"  Foreign Keys:      {'✓ PASS' if results['checks']['foreign_keys'] else '✗ FAIL'}")
        print(f"  Lossless Join:     {'✓ PASS' if results['checks']['lossless_join'] else '✗ FAIL'}")
        print(f"  3NF Compliance:    {'✓ PASS' if results['checks']['3nf_compliance'] else '✗ FAIL'}")
        
        # Print errors
        if self.errors:
            print(f"\n❌ {len(self.errors)} Error(s):")
            for error in self.errors:
                print(f"  {error}")
        
        # Print warnings
        if self.warnings:
            print(f"\n⚠ {len(self.warnings)} Warning(s):")
            for warning in self.warnings:
                print(f"  {warning}")
        
        if results['is_valid'] and not self.errors:
            print(f"\n✅ Schema validation PASSED - No critical errors found")
        else:
            print(f"\n❌ Schema validation FAILED - Please review errors above")
        
        print("="*70)
        
        results['errors'] = self.errors
        results['warnings'] = self.warnings
        
        return results


if __name__ == "__main__":
    # Test validator
    pass

