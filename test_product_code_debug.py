"""Debug why product_code is not selected as PK"""
import pandas as pd
from normalizer import Normalizer

df = pd.DataFrame({
    'product_code': ['P001', 'P002', 'P003'],
    'product_name': ['Widget', 'Gadget', 'Thingamajig'],
    'price': [9.99, 19.99, 29.99]
})

n = Normalizer({}, {}, [])

print("=" * 80)
print("CHECKING EACH COLUMN")
print("=" * 80)

for col in df.columns:
    print(f"\nColumn: {col}")
    print(f"  Data: {df[col].tolist()}")
    print(f"  Unique count: {df[col].nunique()}")
    print(f"  Total count: {len(df)}")
    print(f"  Is unique: {df[col].nunique() == len(df)}")
    print(f"  Has nulls: {df[col].isna().any()}")
    
    identity_check = n._has_identity_semantics(col)
    print(f"  Identity check: {identity_check}")
    
    suitable_check = n._is_suitable_for_primary_key(col, df)
    print(f"  Suitable check: {suitable_check}")

print("\n" + "=" * 80)
print("CALLING _determine_best_primary_key")
print("=" * 80)

result = n._determine_best_primary_key(df, 'product_test')
print(f"\nResult:")
print(f"  Key columns: {result['key_columns']}")
print(f"  Key type: {result['key_type']}")
print(f"  Reason: {result['reason']}")
