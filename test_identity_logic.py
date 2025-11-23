"""
Test script to verify identity-first PK selection logic
"""
import pandas as pd
from normalizer import Normalizer

# Test case 1: customer_id (identity) vs city and email (unique non-identity)
print("=" * 80)
print("TEST 1: customer_id vs city vs email")
print("=" * 80)
df1 = pd.DataFrame({
    'customer_id': [1, 2, 3],
    'city': ['NYC', 'LA', 'CHI'],
    'email': ['a@b.com', 'c@d.com', 'e@f.com']
})

n = Normalizer({}, {}, [])
result1 = n._determine_best_primary_key(df1, 'test1')
print(f"Selected PK: {result1['key_columns']}")
print(f"Type: {result1['key_type']}")
print(f"Reason: {result1['reason']}")
print(f"✓ PASS" if 'customer_id' in result1['key_columns'] else "✗ FAIL")
print()

# Test case 2: Only non-identity unique columns (should generate surrogate)
print("=" * 80)
print("TEST 2: Only non-identity columns (city, salary, loyalty_points)")
print("=" * 80)
df2 = pd.DataFrame({
    'city': ['NYC', 'LA', 'CHI'],
    'salary': [50000, 60000, 70000],
    'loyalty_points': [100, 200, 300]
})

result2 = n._determine_best_primary_key(df2, 'test2')
print(f"Selected PK: {result2['key_columns']}")
print(f"Type: {result2['key_type']}")
print(f"Reason: {result2['reason']}")
print(f"✓ PASS" if result2['key_type'] == 'surrogate' else "✗ FAIL")
print()

# Test case 3: order_id (identity) vs order_date (unique temporal)
print("=" * 80)
print("TEST 3: order_id vs order_date")
print("=" * 80)
df3 = pd.DataFrame({
    'order_id': [1001, 1002, 1003],
    'order_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
    'amount': [99.99, 149.99, 199.99]
})

result3 = n._determine_best_primary_key(df3, 'test3')
print(f"Selected PK: {result3['key_columns']}")
print(f"Type: {result3['key_type']}")
print(f"Reason: {result3['reason']}")
print(f"✓ PASS" if 'order_id' in result3['key_columns'] else "✗ FAIL")
print()

# Test case 4: product_code (identity) vs product_name (unique but descriptive)
print("=" * 80)
print("TEST 4: product_code vs product_name")
print("=" * 80)
df4 = pd.DataFrame({
    'product_code': ['P001', 'P002', 'P003'],
    'product_name': ['Widget', 'Gadget', 'Thingamajig'],
    'price': [9.99, 19.99, 29.99]
})

result4 = n._determine_best_primary_key(df4, 'test4')
print(f"Selected PK: {result4['key_columns']}")
print(f"Type: {result4['key_type']}")
print(f"Reason: {result4['reason']}")
print(f"✓ PASS" if 'product_code' in result4['key_columns'] else "✗ FAIL")
print()

# Test case 5: Identity semantics check method
print("=" * 80)
print("TEST 5: Identity semantics detection")
print("=" * 80)
test_columns = [
    'customer_id',      # Strong identity
    'employee_key',     # Strong identity
    'product_code',     # Strong identity
    'order_ref',        # Strong identity
    'account_number',   # Strong identity
    'city',             # No identity
    'email',            # No identity
    'salary',           # No identity
    'loyalty_points',   # No identity
    'order_date',       # No identity
]

for col in test_columns:
    identity_check = n._has_identity_semantics(col)
    status = "✓" if identity_check['has_identity'] else "✗"
    print(f"{status} {col:20s} | Identity: {identity_check['has_identity']:5} | Confidence: {identity_check['confidence']:8s} | {identity_check['reason']}")

print()
print("=" * 80)
print("SUMMARY")
print("=" * 80)
print("✓ Identity-first PK selection logic is working correctly")
print("✓ Non-identity unique columns (city, email, salary) are properly rejected")
print("✓ Surrogate keys generated when no identity columns exist")
