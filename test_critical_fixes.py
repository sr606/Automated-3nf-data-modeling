"""
Test Script for Critical Bug Fixes
Tests that all critical issues are prevented
"""

import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from metadata_extractor import MetadataExtractor
from auto_profiler import AutoProfiler
from fk_detector import ForeignKeyDetector
from normalizer import Normalizer
from schema_validator import SchemaValidator


def create_test_data():
    """Create realistic test data"""
    
    customers = pd.DataFrame({
        'customer_id': [1, 2, 3, 4, 5],
        'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Wilson'],
        'email': ['john@example.com', 'jane@example.com', 'bob@example.com', 'alice@example.com', 'charlie@example.com'],
        'phone': ['555-1234', '555-5678', '555-9012', '555-3456', '555-7890'],
        'address': ['123 Main St, NYC', '456 Oak Ave, LA', '789 Pine Rd, Chicago', '321 Elm St, Boston', '654 Maple Dr, Seattle'],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Boston', 'Seattle'],
        'zip_code': ['10001', '90001', '60601', '02101', '98101']
    })
    
    orders = pd.DataFrame({
        'order_id': [101, 102, 103, 104, 105],
        'customer_id': [1, 2, 1, 3, 2],
        'order_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19'],
        'total_amount': [150.00, 250.00, 75.00, 300.00, 125.00],
        'status': ['completed', 'shipped', 'completed', 'processing', 'shipped']
    })
    
    order_items = pd.DataFrame({
        'order_item_id': [1001, 1002, 1003, 1004, 1005, 1006],
        'order_id': [101, 101, 102, 103, 104, 105],
        'product_id': [201, 202, 203, 201, 204, 203],
        'product_name': ['Widget A', 'Widget B', 'Widget C', 'Widget A', 'Widget D', 'Widget C'],
        'product_category': ['Electronics', 'Electronics', 'Home', 'Electronics', 'Home', 'Home'],
        'supplier': ['Supplier X', 'Supplier Y', 'Supplier Z', 'Supplier X', 'Supplier Z', 'Supplier Z'],
        'quantity': [2, 1, 3, 1, 2, 1],
        'price': [50.00, 100.00, 75.00, 50.00, 150.00, 75.00],
        'discount': [5.00, 0.00, 10.00, 5.00, 0.00, 0.00]
    })
    
    products = pd.DataFrame({
        'product_id': [201, 202, 203, 204],
        'product_name': ['Widget A', 'Widget B', 'Widget C', 'Widget D'],
        'category': ['Electronics', 'Electronics', 'Home', 'Home'],
        'supplier_name': ['Supplier X', 'Supplier Y', 'Supplier Z', 'Supplier Z'],
        'base_price': [50.00, 100.00, 75.00, 150.00]
    })
    
    return {
        'customers': customers,
        'orders': orders,
        'order_items': order_items,
        'products': products
    }


def test_pk_blacklist():
    """Test: Ensure PK is never assigned to blacklisted attributes"""
    print("\n" + "="*70)
    print("TEST 1: PK NOT ASSIGNED TO BLACKLISTED ATTRIBUTES")
    print("="*70)
    
    data = create_test_data()
    
    temp_dir = Path('./temp_test')
    temp_dir.mkdir(exist_ok=True)
    
    for table_name, df in data.items():
        df.to_csv(temp_dir / f"{table_name}.csv", index=False)
    
    extractor = MetadataExtractor()
    metadata = extractor.extract_all_metadata(str(temp_dir))
    
    profiler = AutoProfiler(metadata)
    profiles = profiler.profile_all_tables()
    
    blacklist_patterns = ['email', 'phone', 'price', 'amount', 'quantity', 'date', 'category', 'supplier', 'status']
    
    passed = True
    for table_name, profile in profiles.items():
        pk = profile.get('primary_key', [])
        for pk_col in pk:
            pk_lower = pk_col.lower()
            for pattern in blacklist_patterns:
                if pattern in pk_lower:
                    print(f"  [FAIL] Table {table_name} has blacklisted attribute '{pk_col}' as PK")
                    passed = False
                    break
    
    if passed:
        print("  [PASS] No blacklisted attributes used as PKs")
    
    import shutil
    shutil.rmtree(temp_dir)
    
    return passed


def test_pk_not_fk():
    """Test: Ensure PK is never also a FK"""
    print("\n" + "="*70)
    print("TEST 2: PK NOT EQUAL TO FK")
    print("="*70)
    
    data = create_test_data()
    
    temp_dir = Path('./temp_test')
    temp_dir.mkdir(exist_ok=True)
    
    for table_name, df in data.items():
        df.to_csv(temp_dir / f"{table_name}.csv", index=False)
    
    extractor = MetadataExtractor()
    metadata = extractor.extract_all_metadata(str(temp_dir))
    
    profiler = AutoProfiler(metadata)
    profiles = profiler.profile_all_tables()
    
    fk_detector = ForeignKeyDetector(metadata, profiles)
    foreign_keys = fk_detector.detect_all_foreign_keys(threshold=50.0)
    
    print(f"\nDetected {len(foreign_keys)} foreign keys:")
    for fk in foreign_keys:
        print(f"  {fk['fk_table']}.{fk['fk_column']} -> {fk['pk_table']}.{fk['pk_column']} (score: {fk['score']:.1f})")
    
    passed = True
    for table_name, profile in profiles.items():
        pk = profile.get('primary_key', [])
        for fk in foreign_keys:
            if fk['fk_table'] == table_name and fk['fk_column'] in pk:
                print(f"  [FAIL] Table {table_name} has PK '{fk['fk_column']}' that is also FK")
                passed = False
    
    if passed:
        print("  [PASS] No PKs are also FKs")
    
    import shutil
    shutil.rmtree(temp_dir)
    
    return passed


def test_no_circular_fks():
    """Test: Ensure no circular FK dependencies"""
    print("\n" + "="*70)
    print("TEST 3: NO CIRCULAR FK DEPENDENCIES")
    print("="*70)
    
    data = create_test_data()
    
    temp_dir = Path('./temp_test')
    temp_dir.mkdir(exist_ok=True)
    
    for table_name, df in data.items():
        df.to_csv(temp_dir / f"{table_name}.csv", index=False)
    
    extractor = MetadataExtractor()
    metadata = extractor.extract_all_metadata(str(temp_dir))
    
    profiler = AutoProfiler(metadata)
    profiles = profiler.profile_all_tables()
    
    fk_detector = ForeignKeyDetector(metadata, profiles)
    foreign_keys = fk_detector.detect_all_foreign_keys(threshold=50.0)
    
    dependencies = {}
    for fk in foreign_keys:
        if fk['fk_table'] not in dependencies:
            dependencies[fk['fk_table']] = []
        dependencies[fk['fk_table']].append(fk['pk_table'])
    
    def has_cycle(node, visited, rec_stack):
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
    passed = True
    for table in dependencies.keys():
        if table not in visited:
            if has_cycle(table, visited, set()):
                print(f"  [FAIL] Circular dependency detected involving {table}")
                passed = False
    
    if passed:
        print("  [PASS] No circular FK dependencies")
    
    import shutil
    shutil.rmtree(temp_dir)
    
    return passed


def test_full_validation():
    """Test: Run full schema validation"""
    print("\n" + "="*70)
    print("TEST 4: FULL SCHEMA VALIDATION")
    print("="*70)
    
    data = create_test_data()
    
    temp_dir = Path('./temp_test')
    temp_dir.mkdir(exist_ok=True)
    
    for table_name, df in data.items():
        df.to_csv(temp_dir / f"{table_name}.csv", index=False)
    
    extractor = MetadataExtractor()
    metadata = extractor.extract_all_metadata(str(temp_dir))
    
    profiler = AutoProfiler(metadata)
    profiles = profiler.profile_all_tables()
    
    fk_detector = ForeignKeyDetector(metadata, profiles)
    foreign_keys = fk_detector.detect_all_foreign_keys(threshold=50.0)
    
    normalizer = Normalizer(metadata, profiles, foreign_keys)
    normalized_tables = normalizer.normalize_all_tables()
    
    validator = SchemaValidator(normalized_tables, profiles, foreign_keys)
    results = validator.validate_all()
    
    passed = results['is_valid']
    
    import shutil
    shutil.rmtree(temp_dir)
    
    return passed


def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("RUNNING CRITICAL BUG FIX TESTS")
    print("="*70)
    
    tests = [
        ("PK Blacklist", test_pk_blacklist),
        ("PK Not FK", test_pk_not_fk),
        ("No Circular FKs", test_no_circular_fks),
        ("Full Validation", test_full_validation)
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n  [ERROR] in {test_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            results[test_name] = False
    
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    for test_name, passed in results.items():
        status = "[PASS]" if passed else "[FAIL]"
        print(f"  {status}: {test_name}")
    
    total_passed = sum(results.values())
    total_tests = len(results)
    
    print(f"\n  Total: {total_passed}/{total_tests} tests passed")
    
    if total_passed == total_tests:
        print("\n  [SUCCESS] ALL TESTS PASSED - Critical fixes validated!")
    else:
        print("\n  [ATTENTION] SOME TESTS FAILED - Please review errors above")
    
    print("="*70)
    
    return 0 if total_passed == total_tests else 1


if __name__ == "__main__":
    sys.exit(main())
