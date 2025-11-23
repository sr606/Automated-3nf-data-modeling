"""
Test Runner
Quick tests to verify the system components

Usage:
    python test_system.py
"""

import sys
from pathlib import Path


def test_imports():
    """Test that all required modules can be imported"""
    print("Testing imports...")
    
    try:
        import pandas
        print("  ✓ pandas")
    except ImportError as e:
        print(f"  ❌ pandas: {e}")
        return False
    
    try:
        from langgraph.graph import StateGraph
        print("  ✓ langgraph")
    except ImportError as e:
        print(f"  ❌ langgraph: {e}")
        return False
    
    try:
        import numpy
        print("  ✓ numpy")
    except ImportError as e:
        print(f"  ❌ numpy: {e}")
        return False
    
    try:
        import graphviz
        print("  ✓ graphviz (Python bindings)")
    except ImportError as e:
        print(f"  ⚠  graphviz (optional): {e}")
    
    return True


def test_modules():
    """Test that all project modules can be imported"""
    print("\nTesting project modules...")
    
    modules = [
        'metadata_extractor',
        'auto_profiler',
        'fk_detector',
        'normalizer',
        'sql_generator',
        'utils',
        'langgraph_app'
    ]
    
    all_ok = True
    for module in modules:
        try:
            __import__(module)
            print(f"  ✓ {module}")
        except Exception as e:
            print(f"  ❌ {module}: {e}")
            all_ok = False
    
    return all_ok


def test_directories():
    """Test that all required directories exist"""
    print("\nTesting directory structure...")
    
    required_dirs = [
        'input_files',
        'normalized_output',
        'sql_output',
        'erd'
    ]
    
    all_ok = True
    for dir_name in required_dirs:
        dir_path = Path(dir_name)
        if dir_path.exists():
            print(f"  ✓ {dir_name}/")
        else:
            print(f"  ❌ {dir_name}/ (missing)")
            all_ok = False
    
    return all_ok


def test_input_files():
    """Test that sample input files exist"""
    print("\nTesting input files...")
    
    input_path = Path('input_files')
    csv_files = list(input_path.glob('*.csv'))
    json_files = list(input_path.glob('*.json'))
    
    print(f"  Found {len(csv_files)} CSV files")
    print(f"  Found {len(json_files)} JSON files")
    
    if len(csv_files) + len(json_files) == 0:
        print("  ⚠  No input files found (add files to test)")
        return False
    
    return True


def test_basic_functionality():
    """Test basic functionality of key modules"""
    print("\nTesting basic functionality...")
    
    try:
        from metadata_extractor import MetadataExtractor
        extractor = MetadataExtractor()
        print("  ✓ MetadataExtractor instantiation")
    except Exception as e:
        print(f"  ❌ MetadataExtractor: {e}")
        return False
    
    try:
        from utils import KeywordSanitizer
        result = KeywordSanitizer.sanitize('SELECT')
        if result == 'SELECT_col':
            print("  ✓ KeywordSanitizer")
        else:
            print(f"  ❌ KeywordSanitizer: unexpected result '{result}'")
            return False
    except Exception as e:
        print(f"  ❌ KeywordSanitizer: {e}")
        return False
    
    try:
        from langgraph_app import NormalizationWorkflow
        workflow = NormalizationWorkflow()
        print("  ✓ NormalizationWorkflow instantiation")
    except Exception as e:
        print(f"  ❌ NormalizationWorkflow: {e}")
        return False
    
    return True


def main():
    """Run all tests"""
    print("="*70)
    print("SYSTEM VERIFICATION TEST")
    print("="*70)
    
    tests = [
        ("Dependencies", test_imports),
        ("Project Modules", test_modules),
        ("Directory Structure", test_directories),
        ("Input Files", test_input_files),
        ("Basic Functionality", test_basic_functionality)
    ]
    
    results = []
    for test_name, test_func in tests:
        result = test_func()
        results.append((test_name, result))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    all_passed = True
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status} - {test_name}")
        if not result:
            all_passed = False
    
    print("="*70)
    
    if all_passed:
        print("\n✅ All tests passed! System is ready to use.")
        print("\nRun the system with: python main.py")
        return 0
    else:
        print("\n⚠️  Some tests failed. Please fix the issues above.")
        print("\nRefer to SETUP.md for installation instructions.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
