"""
Comprehensive test for all 5 structural rules
"""
import pandas as pd
from sql_generator import SQLGenerator

def test_all_structural_rules():
    print("="*80)
    print("COMPREHENSIVE STRUCTURAL RULES TEST")
    print("="*80)
    
    # RULE 1: Child table PK rule - prefer artificial surrogate keys
    print("\n[RULE 1] Child Table PK Rule - Artificial Surrogate Keys")
    
    # Tag table with surrogate key
    tags_df = pd.DataFrame({
        'product_tags_id': [1, 2, 3, 4],  # Artificial surrogate
        'product_id': [101, 101, 102, 103],  # FK (repeating)
        'tag': ['electronics', 'sale', 'new', 'featured']
    })
    
    normalized_tables = {'product_tags': tags_df}
    metadata = {'product_tags': {'dataframe': tags_df, 'columns': {}}}
    profiles = {
        'product_tags': {
            'primary_key': ['product_tags_id'],  # Should use surrogate
            'candidate_keys': [['product_tags_id']]
        }
    }
    foreign_keys = []
    
    generator = SQLGenerator(normalized_tables, metadata, profiles, foreign_keys)
    pk = generator.get_primary_key_columns('product_tags', tags_df)
    
    print(f"  Tag table columns: {list(tags_df.columns)}")
    print(f"  Selected PK: {pk}")
    assert pk == ['product_tags_id'], "Should use artificial surrogate key, not FK"
    print("  ✓ PASS: Artificial surrogate key used as PK (not FK)")
    
    # RULE 1b: Never use business columns as PK
    print("\n[RULE 1b] Never Use Business Columns as PK")
    
    products_df = pd.DataFrame({
        'product_id': [1, 2, 3],
        'product_name': ['Laptop', 'Mouse', 'Keyboard'],  # Business column
        'category': ['Electronics', 'Accessories', 'Accessories'],
        'price': [999.99, 29.99, 79.99]
    })
    
    generator2 = SQLGenerator(
        {'products': products_df},
        {'products': {'dataframe': products_df, 'columns': {}}},
        {'products': {'primary_key': ['product_id'], 'candidate_keys': [['product_id']]}},
        []
    )
    pk = generator2.get_primary_key_columns('products', products_df)
    
    print(f"  Product columns: {list(products_df.columns)}")
    print(f"  Selected PK: {pk}")
    assert 'product_name' not in pk and 'category' not in pk and 'price' not in pk
    assert pk == ['product_id']
    print("  ✓ PASS: Surrogate key used, business columns avoided")
    
    # RULE 2: Composite PK/FK correctness
    print("\n[RULE 2] Composite PK/FK Correctness")
    
    # Parent with composite PK
    categories_df = pd.DataFrame({
        'category_id': [1, 2, 3],
        'category_name': ['Electronics', 'Books', 'Clothing'],
        'description': ['Electronic items', 'Books', 'Apparel']
    })
    
    # Child with only category_id (missing category_name)
    products2_df = pd.DataFrame({
        'product_id': [101, 102],
        'product_name': ['Laptop', 'Novel'],
        'category_id': [1, 2]  # Only partial composite PK reference
    })
    
    profiles_composite = {
        'categories': {
            'primary_key': ('category_id', 'category_name'),  # Composite
            'candidate_keys': [('category_id', 'category_name')]
        },
        'products': {
            'primary_key': ['product_id'],
            'candidate_keys': [['product_id']]
        }
    }
    
    fks = [{
        'fk_table': 'products',
        'fk_column': 'category_id',
        'pk_table': 'categories',
        'pk_column': 'category_id'
    }]
    
    generator3 = SQLGenerator(
        {'categories': categories_df, 'products': products2_df},
        {'categories': {'dataframe': categories_df, 'columns': {}},
         'products': {'dataframe': products2_df, 'columns': {}}},
        profiles_composite,
        fks
    )
    
    # Check if can reference composite PK
    can_ref = generator3._can_reference_composite_pk('products', 'categories', 
                                                      ['category_id', 'category_name'])
    print(f"  Parent PK: ('category_id', 'category_name')")
    print(f"  Child has both columns: {can_ref}")
    assert can_ref == False, "Should detect missing column"
    print("  ✓ PASS: Detected child missing composite PK columns")
    
    constraints = generator3.generate_foreign_key_constraints()
    print(f"  FK constraints generated: {len(constraints)}")
    assert len(constraints) == 0, "Should skip FK when composite PK incomplete"
    print("  ✓ PASS: FK skipped when child missing composite PK columns")
    
    # RULE 3: Tag/attribute tables must use own surrogate PK
    print("\n[RULE 3] Tag/Attribute Tables - Own Surrogate PK")
    
    skills_df = pd.DataFrame({
        'employee_skills_id': [1, 2, 3, 4, 5],  # Own surrogate
        'employee_id': [10, 10, 10, 20, 20],  # FK (repeating)
        'skill_name': ['Python', 'SQL', 'Java', 'Python', 'C++']
    })
    
    profiles_skills = {
        'employee_skills': {
            'primary_key': ['employee_skills_id'],
            'candidate_keys': [['employee_skills_id']]
        }
    }
    
    generator4 = SQLGenerator(
        {'employee_skills': skills_df},
        {'employee_skills': {'dataframe': skills_df, 'columns': {}}},
        profiles_skills,
        []
    )
    
    pk = generator4.get_primary_key_columns('employee_skills', skills_df)
    print(f"  Skills table columns: {list(skills_df.columns)}")
    print(f"  Selected PK: {pk}")
    assert pk == ['employee_skills_id'], "Should use own surrogate, not parent FK"
    assert 'employee_id' not in pk, "Should NOT use parent FK as PK"
    print("  ✓ PASS: Tag table uses own surrogate PK, not parent FK")
    
    # RULE 4: Circular dependency detection
    print("\n[RULE 4] Execution Safety - Circular Dependency Detection")
    
    # Setup: A->B, B->C
    fks_circular = [
        {'fk_table': 'A', 'fk_column': 'b_id', 'pk_table': 'B', 'pk_column': 'b_id'},
        {'fk_table': 'B', 'fk_column': 'c_id', 'pk_table': 'C', 'pk_column': 'c_id'}
    ]
    
    generator5 = SQLGenerator({}, {}, {}, fks_circular)
    
    # Try to add C->A (would create cycle: A->B->C->A)
    is_circular = generator5._would_create_circular_fk('C', 'A')
    print(f"  Existing: A->B, B->C")
    print(f"  Adding C->A would create cycle: {is_circular}")
    assert is_circular == True, "Should detect circular dependency"
    print("  ✓ PASS: Circular dependency detected")
    
    # Non-circular case
    is_circular2 = generator5._would_create_circular_fk('C', 'D')
    print(f"  Adding C->D would create cycle: {is_circular2}")
    assert is_circular2 == False, "Should not flag non-circular"
    print("  ✓ PASS: Non-circular dependency allowed")
    
    # RULE 5: No auto-modification of columns
    print("\n[RULE 5] No Auto-Modification - Metadata-Driven Only")
    
    # Table with various column types - generator should not modify
    data_df = pd.DataFrame({
        'order_id': [1, 2, 3],
        'customer_name': ['Alice Long Name Here', 'Bob', 'Charlie'],  # Long values
        'total_amount': [100.50, 200.75, 50.25],
        'order_date': ['2024-01-01', '2024-01-02', '2024-01-03']
    })
    
    generator6 = SQLGenerator(
        {'orders': data_df},
        {'orders': {'dataframe': data_df, 'columns': {}}},
        {'orders': {'primary_key': ['order_id'], 'candidate_keys': [['order_id']]}},
        []
    )
    
    # Generate CREATE TABLE
    create_sql = generator6.generate_create_table_script('orders', data_df)
    
    print(f"  Original columns: {list(data_df.columns)}")
    print(f"  CREATE TABLE generated")
    
    # Verify no column renaming or combining
    for col in data_df.columns:
        sanitized = generator6.sanitize_identifier(col)
        assert sanitized in create_sql, f"Column {col} should be preserved (as {sanitized})"
    
    print("  ✓ PASS: All columns preserved without modification")
    print("  ✓ PASS: No auto-shortening or renaming applied")
    
    # RULE 1c: Associative tables - composite of FKs if no surrogate
    print("\n[RULE 1c] Associative Tables - Composite FK as PK")
    
    enrollments_df = pd.DataFrame({
        'student_id': [1, 1, 2, 3],
        'course_id': [101, 102, 101, 103],
        'enrollment_date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-03']
    })
    
    profiles_assoc = {
        'enrollments': {
            'primary_key': ['student_id', 'course_id'],
            'candidate_keys': [['student_id', 'course_id']]
        }
    }
    
    fks_assoc = [
        {'fk_table': 'enrollments', 'fk_column': 'student_id', 'pk_table': 'students', 'pk_column': 'student_id'},
        {'fk_table': 'enrollments', 'fk_column': 'course_id', 'pk_table': 'courses', 'pk_column': 'course_id'}
    ]
    
    generator7 = SQLGenerator(
        {'enrollments': enrollments_df},
        {'enrollments': {'dataframe': enrollments_df, 'columns': {}}},
        profiles_assoc,
        fks_assoc
    )
    
    pk = generator7.get_primary_key_columns('enrollments', enrollments_df)
    print(f"  Associative table (no surrogate): {list(enrollments_df.columns)}")
    print(f"  Selected PK: {pk}")
    # Should use composite of FKs since no surrogate exists
    assert len(pk) == 2, "Should use composite key"
    assert set(pk) == {'student_id', 'course_id'}
    print("  ✓ PASS: Associative table uses composite of FKs (no surrogate)")
    
    print("\n" + "="*80)
    print("ALL 5 STRUCTURAL RULES VERIFIED!")
    print("="*80)
    print("\nSummary:")
    print("  ✓ RULE 1: Child tables use artificial surrogate keys (never FK or business cols)")
    print("  ✓ RULE 2: Composite PK/FK correctness validated (all columns required)")
    print("  ✓ RULE 3: Tag/attribute tables use own surrogate PK")
    print("  ✓ RULE 4: Circular dependencies detected and prevented")
    print("  ✓ RULE 5: No auto-modification of columns (metadata-driven)")
    print("  ✓ RULE 1c: Associative tables use composite FK when no surrogate")

if __name__ == "__main__":
    test_all_structural_rules()
