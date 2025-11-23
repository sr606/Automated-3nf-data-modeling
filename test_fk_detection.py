"""Test the new FK detection logic"""
from metadata_extractor import MetadataExtractor
from auto_profiler import AutoProfiler
from fk_detector import ForeignKeyDetector

# Extract metadata
extractor = MetadataExtractor()
metadata = extractor.extract_all_metadata("./input_files")

# Profile tables (detect PKs)
profiler = AutoProfiler(metadata)
profiles = profiler.profile_all_tables()

print("\n" + "="*70)
print("PRIMARY KEYS DETECTED")
print("="*70)
for table_name, profile in profiles.items():
    pk = profile.get('primary_key', 'None')
    print(f"  {table_name}: PK = {pk}")

# Detect FKs with new logic
fk_detector = ForeignKeyDetector(metadata, profiles)
foreign_keys = fk_detector.detect_all_foreign_keys()

print("\n" + "="*70)
print(f"FOREIGN KEYS DETECTED: {len(foreign_keys)}")
print("="*70)

for fk in foreign_keys:
    print(f"\n{fk['fk_table']}.{fk['fk_column']} -> {fk['pk_table']}.{fk['pk_column']}")
    print(f"  Type: {fk['relationship_type']}")
    print(f"  Reasons:")
    for reason in fk['reasons']:
        print(f"    - {reason}")

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70)
