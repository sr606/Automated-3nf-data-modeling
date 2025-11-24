"""Test composite PK scenario"""
import pandas as pd
from sql_generator import SQLGenerator

def test_composite_pk():
    print("="*80)
    print("COMPOSITE PK TEST - FK VALIDATION")
    print("="*80)
    
    # Categories table with composite PK (like the actual 10_categories)
    categories_df = pd.DataFrame({
        'category_id': [1, 2, 3],
        'category_name': ['Electronics', 'Books', 'Clothing'],
        'description': ['Electronic items', 'Books and media', 'Apparel']
    })
    
    # Products table referencing category_id
    products_df = pd.DataFrame({
        'product_id': [101, 102, 103],
        'product_name': ['Laptop', 'Novel', 'Shirt'],
        'category_id': [1, 2, 3]
    })
    
    normalized_tables = {
        '10_categories': categories_df,
        '01_products': products_df
    }
    
    metadata = {
        '10_categories': {'dataframe': categories_df, 'columns': {}},
        '01_products': {'dataframe': products_df, 'columns': {}}
    }
    
    # Test Case 1: Composite PK without UNIQUE on single column
    print("\n[TEST 1] Composite PK - column not separately UNIQUE")
    profiles_composite = {
        '10_categories': {
            'primary_key': ('category_id', 'category_name'),  # Composite PK
            'candidate_keys': [
                ('category_id', 'category_name'),
                ('category_id', 'description')
            ]
            # Note: category_id is NOT unique alone
        },
        '01_products': {
            'primary_key': ['product_id'],
            'candidate_keys': [['product_id']]
        }
    }
    
    # FK references category_id (part of composite PK, but not UNIQUE alone)
    foreign_keys = [{
        'fk_table': '01_products',
        'fk_column': 'category_id',
        'pk_table': '10_categories',
        'pk_column': 'category_id'
    }]
    
    generator = SQLGenerator(normalized_tables, metadata, profiles_composite, foreign_keys)
    
    is_valid = generator._is_valid_fk_target('10_categories', 'category_id')
    print(f"  10_categories.category_id (part of composite PK, not UNIQUE): {is_valid}")
    assert is_valid == False, "Should REJECT FK to composite PK column without UNIQUE"
    print("  ✓ PASS: FK to composite PK column (without UNIQUE) REJECTED")
    
    constraints = generator.generate_foreign_key_constraints()
    print(f"  Generated constraints: {len(constraints)}")
    assert len(constraints) == 0, "Should generate 0 constraints"
    print("  ✓ PASS: No FK constraint generated (would cause ORA-02270)")
    
    # Test Case 2: Composite PK with column also having UNIQUE
    print("\n[TEST 2] Composite PK - column also has UNIQUE constraint")
    profiles_unique = {
        '10_categories': {
            'primary_key': ('category_id', 'category_name'),  # Composite PK
            'candidate_keys': [
                ('category_id', 'category_name'),
                ('category_id',),  # category_id is ALSO uniquely identifying
                ('category_id', 'description')
            ]
        },
        '01_products': {
            'primary_key': ['product_id'],
            'candidate_keys': [['product_id']]
        }
    }
    
    generator2 = SQLGenerator(normalized_tables, metadata, profiles_unique, foreign_keys)
    
    is_valid = generator2._is_valid_fk_target('10_categories', 'category_id')
    print(f"  10_categories.category_id (part of composite PK, also UNIQUE): {is_valid}")
    assert is_valid == True, "Should ACCEPT FK to column that is both in composite PK and UNIQUE"
    print("  ✓ PASS: FK to composite PK column (with UNIQUE) ACCEPTED")
    
    constraints = generator2.generate_foreign_key_constraints()
    print(f"  Generated constraints: {len(constraints)}")
    assert len(constraints) == 1, "Should generate 1 constraint"
    print("  ✓ PASS: FK constraint generated (valid in Oracle)")
    
    # Test Case 3: Single-column PK (always valid)
    print("\n[TEST 3] Single-column PK (always valid)")
    profiles_single = {
        '10_categories': {
            'primary_key': ['category_id'],  # Single-column PK
            'candidate_keys': [['category_id']]
        },
        '01_products': {
            'primary_key': ['product_id'],
            'candidate_keys': [['product_id']]
        }
    }
    
    generator3 = SQLGenerator(normalized_tables, metadata, profiles_single, foreign_keys)
    
    is_valid = generator3._is_valid_fk_target('10_categories', 'category_id')
    print(f"  10_categories.category_id (single-column PK): {is_valid}")
    assert is_valid == True, "Should ACCEPT FK to single-column PK"
    print("  ✓ PASS: FK to single-column PK ACCEPTED")
    
    constraints = generator3.generate_foreign_key_constraints()
    print(f"  Generated constraints: {len(constraints)}")
    assert len(constraints) == 1, "Should generate 1 constraint"
    print("  ✓ PASS: FK constraint generated")
    
    print("\n" + "="*80)
    print("ALL COMPOSITE PK TESTS PASSED!")
    print("="*80)
    print("\nSummary:")
    print("  ✓ Composite PK column (not UNIQUE alone): REJECTED → Prevents ORA-02270")
    print("  ✓ Composite PK column (also UNIQUE): ACCEPTED → Valid in Oracle")
    print("  ✓ Single-column PK: ACCEPTED → Always valid")
    print("\nThis fix prevents the ORA-02270 error!")

if __name__ == "__main__":
    test_composite_pk()
