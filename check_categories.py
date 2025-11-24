"""Check the 10_categories table profile and normalization"""
from metadata_extractor import MetadataExtractor
from auto_profiler import AutoProfiler
from normalizer import Normalizer
from fk_detector import ForeignKeyDetector

# Extract metadata
extractor = MetadataExtractor()
metadata = extractor.extract_all_metadata('./test_data_50')

# Profile
profiler = AutoProfiler(metadata)
profiles = profiler.profile_all_tables()

# Check 10_categories
if '10_categories' in profiles:
    cat_profile = profiles['10_categories']
    print("="*80)
    print("10_categories - BEFORE NORMALIZATION")
    print("="*80)
    print(f"Primary Key: {cat_profile.get('primary_key')}")
    print(f"Candidate Keys: {cat_profile.get('candidate_keys')}")
    print(f"Columns: {list(metadata['10_categories']['dataframe'].columns)}")
    print()

# Detect FKs
fk_detector = ForeignKeyDetector(metadata, profiles)
foreign_keys = fk_detector.detect_all_foreign_keys()

# Check FKs referencing 10_categories
print("\n" + "="*80)
print("FOREIGN KEYS REFERENCING 10_categories")
print("="*80)
for fk in foreign_keys:
    if fk['pk_table'] == '10_categories':
        print(f"{fk['fk_table']}.{fk['fk_column']} -> {fk['pk_table']}.{fk['pk_column']}")

# Normalize
normalizer = Normalizer(metadata, profiles, foreign_keys)
normalized_tables = normalizer.normalize_all_tables()

# Check normalized categories tables
print("\n" + "="*80)
print("NORMALIZED TABLES - AFTER NORMALIZATION")
print("="*80)
for table_name in sorted(normalized_tables.keys()):
    if '10_categories' in table_name or 'categories' in table_name.lower():
        df = normalized_tables[table_name]
        print(f"\nTable: {table_name}")
        print(f"  Columns: {list(df.columns)}")
        print(f"  Rows: {len(df)}")
        
        # Check if there's a profile for this normalized table
        if table_name in profiles:
            norm_profile = profiles[table_name]
            print(f"  Profile PK: {norm_profile.get('primary_key')}")
        else:
            print(f"  Profile: NOT FOUND (using original table profile)")
