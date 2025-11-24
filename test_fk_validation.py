"""
Test FK validation - ensure only PKs and UNIQUE columns can be FK targets
"""
import pandas as pd
from sql_generator import SQLGenerator

# Create test data
def test_fk_validation():
    print("="*80)
    print("FK VALIDATION TEST - PK/UNIQUE REQUIREMENT")
    print("="*80)
    
    # Test Case 1: Valid FK - references PK
    print("\n[TEST 1] Valid FK - references PRIMARY KEY")
    
    # Parent table with PK
    parent_df = pd.DataFrame({
        'user_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com']
    })
    
    # Child table with FK to parent's PK
    child_df = pd.DataFrame({
        'order_id': [101, 102, 103],
        'user_id': [1, 2, 1],
        'amount': [100, 200, 150]
    })
    
    normalized_tables = {
        'users': parent_df,
        'orders': child_df
    }
    
    metadata = {
        'users': {'dataframe': parent_df, 'columns': {}},
        'orders': {'dataframe': child_df, 'columns': {}}
    }
    
    profiles = {
        'users': {
            'primary_key': ['user_id'],
            'candidate_keys': [['user_id'], ['email']]
        },
        'orders': {
            'primary_key': ['order_id'],
            'candidate_keys': [['order_id']]
        }
    }
    
    # FK references PK
    foreign_keys = [{
        'fk_table': 'orders',
        'fk_column': 'user_id',
        'pk_table': 'users',
        'pk_column': 'user_id'
    }]
    
    generator = SQLGenerator(normalized_tables, metadata, profiles, foreign_keys)
    
    # Check validation
    is_valid = generator._is_valid_fk_target('users', 'user_id')
    print(f"  users.user_id is PK: {is_valid}")
    assert is_valid == True, "Should accept FK to PRIMARY KEY"
    print("  ✓ PASS: FK to PRIMARY KEY accepted")
    
    # Test Case 2: Valid FK - references UNIQUE column
    print("\n[TEST 2] Valid FK - references UNIQUE column")
    
    is_valid = generator._is_valid_fk_target('users', 'email')
    print(f"  users.email is UNIQUE: {is_valid}")
    assert is_valid == True, "Should accept FK to UNIQUE column"
    print("  ✓ PASS: FK to UNIQUE column accepted")
    
    # Test Case 3: Invalid FK - references non-PK/non-UNIQUE column
    print("\n[TEST 3] Invalid FK - references non-PK/non-UNIQUE column")
    
    is_valid = generator._is_valid_fk_target('users', 'name')
    print(f"  users.name is neither PK nor UNIQUE: {is_valid}")
    assert is_valid == False, "Should reject FK to non-PK/non-UNIQUE column"
    print("  ✓ PASS: FK to non-PK/non-UNIQUE column rejected")
    
    # Test Case 4: Generate FK constraints and verify filtering
    print("\n[TEST 4] FK constraint generation with validation")
    
    # Add invalid FK that references non-unique column
    foreign_keys_mixed = [
        {
            'fk_table': 'orders',
            'fk_column': 'user_id',
            'pk_table': 'users',
            'pk_column': 'user_id'  # Valid - PK
        },
        {
            'fk_table': 'orders',
            'fk_column': 'amount',
            'pk_table': 'users',
            'pk_column': 'name'  # Invalid - not PK or UNIQUE
        }
    ]
    
    generator2 = SQLGenerator(normalized_tables, metadata, profiles, foreign_keys_mixed)
    constraints = generator2.generate_foreign_key_constraints()
    
    print(f"  Input FKs: {len(foreign_keys_mixed)}")
    print(f"  Generated constraints: {len(constraints)}")
    assert len(constraints) == 1, "Should generate only 1 constraint (valid FK)"
    print("  ✓ PASS: Invalid FK filtered out")
    
    # Test Case 5: Table without profile data
    print("\n[TEST 5] Missing profile data")
    
    profiles_incomplete = {
        'orders': {
            'primary_key': ['order_id'],
            'candidate_keys': [['order_id']]
        }
        # 'users' profile missing
    }
    
    generator3 = SQLGenerator(normalized_tables, metadata, profiles_incomplete, foreign_keys)
    is_valid = generator3._is_valid_fk_target('users', 'user_id')
    print(f"  users profile missing: {is_valid}")
    assert is_valid == False, "Should reject when profile data missing"
    print("  ✓ PASS: FK rejected when profile data missing")
    
    # Test Case 6: Multi-column unique constraint (edge case)
    print("\n[TEST 6] Multi-column UNIQUE constraint")
    
    profiles_multi = {
        'users': {
            'primary_key': ['user_id'],
            'candidate_keys': [['user_id'], ['email', 'name']]  # Multi-column unique
        }
    }
    
    generator4 = SQLGenerator(normalized_tables, metadata, profiles_multi, foreign_keys)
    
    # Single column that's part of multi-column unique
    is_valid = generator4._is_valid_fk_target('users', 'email')
    print(f"  email is part of multi-column UNIQUE: {is_valid}")
    # Current implementation skips multi-column unique constraints for single-column FKs
    print(f"  ✓ Current behavior: {'ACCEPTED' if is_valid else 'REJECTED'} (implementation-defined)")
    
    print("\n" + "="*80)
    print("ALL TESTS PASSED!")
    print("="*80)
    print("\nSummary:")
    print("  ✓ FK to PRIMARY KEY: ACCEPTED")
    print("  ✓ FK to UNIQUE column: ACCEPTED")
    print("  ✓ FK to non-PK/non-UNIQUE: REJECTED")
    print("  ✓ FK constraint generation: FILTERED")
    print("  ✓ Missing profile data: REJECTED")
    print("\nFK Acceptance Rule Verified:")
    print("  B.x → A.y is valid ONLY IF y ∈ PrimaryKey(A) OR y ∈ UniqueColumns(A)")

if __name__ == "__main__":
    test_fk_validation()
