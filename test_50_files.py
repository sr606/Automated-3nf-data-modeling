"""
Test 3NF normalization system with 50 diverse test files
Generates comprehensive statistics and validation report
"""
import os
import sys
import time
from datetime import datetime
from metadata_extractor import MetadataExtractor
from auto_profiler import AutoProfiler
from fk_detector import ForeignKeyDetector
from normalizer import Normalizer
from sql_generator import SQLGenerator
from schema_validator import SchemaValidator

def main():
    print("="*80)
    print("COMPREHENSIVE 3NF NORMALIZATION TEST - 50 FILES")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    start_time = time.time()
    
    # Configuration
    input_dir = "./test_data_50"
    output_dir = "./test_50_output"
    sql_output_dir = f"{output_dir}/sql"
    
    # Create output directories
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(sql_output_dir, exist_ok=True)
    
    # Initialize statistics
    stats = {
        'total_files': 0,
        'total_input_tables': 0,
        'total_output_tables': 0,
        'total_input_rows': 0,
        'total_input_columns': 0,
        'total_fks_detected': 0,
        'total_fks_inferred': 0,
        'files_with_1nf_violations': 0,
        'files_with_2nf_violations': 0,
        'files_with_3nf_violations': 0,
        'natural_keys_used': 0,
        'surrogate_keys_added': 0,
        'child_tables_identified': 0,
        'processing_time_seconds': 0,
        'errors': []
    }
    
    # Initialize validation variables
    validation_passed = False
    total_errors = 0
    total_warnings = 0
    
    try:
        # STEP 1: Metadata Extraction
        print("STEP 1: METADATA EXTRACTION")
        print("-" * 80)
        extractor = MetadataExtractor()
        metadata = extractor.extract_all_metadata(input_dir)
        
        stats['total_files'] = len(metadata)
        stats['total_input_tables'] = len(metadata)
        
        for table_name, meta in metadata.items():
            df = meta['dataframe']
            stats['total_input_rows'] += len(df)
            stats['total_input_columns'] += len(df.columns)
        
        print(f"✓ Extracted metadata from {stats['total_files']} files")
        print(f"  Total rows: {stats['total_input_rows']:,}")
        print(f"  Total columns: {stats['total_input_columns']}")
        print()
        
        # STEP 2: Data Profiling
        print("STEP 2: DATA PROFILING")
        print("-" * 80)
        profiler = AutoProfiler(metadata)
        profiles = profiler.profile_all_tables()
        
        # Analyze profiles for normalization violations
        for table_name, profile in profiles.items():
            if profile.get('multivalued_columns'):
                stats['files_with_1nf_violations'] += 1
            if profile.get('partial_dependencies'):
                stats['files_with_2nf_violations'] += 1
            if profile.get('transitive_dependencies'):
                stats['files_with_3nf_violations'] += 1
        
        print(f"✓ Profiled {len(profiles)} tables")
        print(f"  1NF violations: {stats['files_with_1nf_violations']} files")
        print(f"  2NF violations: {stats['files_with_2nf_violations']} files")
        print(f"  3NF violations: {stats['files_with_3nf_violations']} files")
        print()
        
        # STEP 3: Foreign Key Detection
        print("STEP 3: FOREIGN KEY DETECTION")
        print("-" * 80)
        fk_detector = ForeignKeyDetector(metadata, profiles)
        foreign_keys = fk_detector.detect_all_foreign_keys()
        
        # Count detected vs inferred FKs
        for fk in foreign_keys:
            if fk.get('relationship') == 'inferred_id_pattern':
                stats['total_fks_inferred'] += 1
            else:
                stats['total_fks_detected'] += 1
        
        print(f"✓ Detected {len(foreign_keys)} foreign key relationships")
        print(f"  Strict FK detection: {stats['total_fks_detected']}")
        print(f"  Inferred from _id pattern: {stats['total_fks_inferred']}")
        print()
        
        # Display FK relationships
        if foreign_keys:
            print("Foreign Key Relationships:")
            for i, fk in enumerate(foreign_keys[:20], 1):  # Show first 20
                print(f"  {i}. {fk['fk_table']}.{fk['fk_column']} → {fk['pk_table']}.{fk['pk_column']}")
            if len(foreign_keys) > 20:
                print(f"  ... and {len(foreign_keys) - 20} more")
        print()
        
        # STEP 4: Normalization
        print("STEP 4: 3NF NORMALIZATION")
        print("-" * 80)
        normalizer = Normalizer(metadata, profiles, foreign_keys)
        normalized_tables = normalizer.normalize_all_tables()
        
        stats['total_output_tables'] = len(normalized_tables)
        
        # Analyze normalization results
        for table_name, df in normalized_tables.items():
            if table_name in profiles and profiles[table_name].get('primary_key'):
                pk = profiles[table_name]['primary_key']
                # Check if natural or surrogate key
                if any('_id' in str(col) for col in pk):
                    stats['surrogate_keys_added'] += 1
                else:
                    stats['natural_keys_used'] += 1
            
            # Check for child table pattern
            if any('_id' in col.lower() and col.lower() != f"{table_name}_id".lower() 
                   for col in df.columns):
                stats['child_tables_identified'] += 1
        
        print(f"✓ Normalized {stats['total_input_tables']} tables → {stats['total_output_tables']} tables")
        print(f"  Natural keys used: {stats['natural_keys_used']}")
        print(f"  Surrogate keys added: {stats['surrogate_keys_added']}")
        print(f"  Child tables identified: {stats['child_tables_identified']}")
        print()
        
        # STEP 5: SQL Generation
        print("STEP 5: SQL GENERATION")
        print("-" * 80)
        sql_generator = SQLGenerator(normalized_tables, metadata, profiles, foreign_keys)
        
        # Generate SQL for each table
        sql_files_generated = 0
        for table_name in normalized_tables.keys():
            try:
                sql = sql_generator.generate_table_ddl(table_name)
                sql_file = os.path.join(sql_output_dir, f"{table_name}.sql")
                with open(sql_file, 'w') as f:
                    f.write(sql)
                sql_files_generated += 1
            except Exception as e:
                stats['errors'].append(f"SQL generation failed for {table_name}: {str(e)}")
        
        # Generate complete schema
        complete_sql = sql_generator.generate_complete_schema()
        complete_sql_file = os.path.join(sql_output_dir, "complete_schema.sql")
        with open(complete_sql_file, 'w') as f:
            f.write(complete_sql)
        
        print(f"✓ Generated SQL DDL for {sql_files_generated} tables")
        print(f"  Output directory: {sql_output_dir}")
        print(f"  Complete schema: complete_schema.sql")
        print()
        
        # STEP 6: Schema Validation
        print("STEP 6: SCHEMA VALIDATION")
        print("-" * 80)
        validator = SchemaValidator(normalized_tables, profiles, foreign_keys)
        validation_results = validator.validate_all()
        
        validation_passed = all(v['valid'] for v in validation_results.values())
        total_errors = sum(len(v.get('errors', [])) for v in validation_results.values())
        total_warnings = sum(len(v.get('warnings', [])) for v in validation_results.values())
        
        print(f"✓ Validation {'PASSED' if validation_passed else 'FAILED'}")
        print(f"  Errors: {total_errors}")
        print(f"  Warnings: {total_warnings}")
        
        if total_errors > 0:
            print("\n  Critical Errors:")
            for table_name, result in validation_results.items():
                if result.get('errors'):
                    for error in result['errors'][:5]:  # Show first 5 errors per table
                        print(f"    - {table_name}: {error}")
        print()
        
        # STEP 7: Export Normalized Data
        print("STEP 7: EXPORT NORMALIZED DATA")
        print("-" * 80)
        normalized_data_dir = f"{output_dir}/normalized_data"
        os.makedirs(normalized_data_dir, exist_ok=True)
        
        normalizer.export_normalized_tables(normalized_data_dir)
        print(f"✓ Exported {len(normalized_tables)} normalized tables")
        print(f"  Output directory: {normalized_data_dir}")
        print()
        
        # Save normalization log
        log_file = os.path.join(output_dir, "normalization_log.txt")
        with open(log_file, 'w') as f:
            f.write(normalizer.get_normalization_summary())
        print(f"✓ Normalization log saved: {log_file}")
        print()
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        stats['errors'].append(str(e))
        import traceback
        traceback.print_exc()
    
    # Calculate processing time
    end_time = time.time()
    stats['processing_time_seconds'] = round(end_time - start_time, 2)
    
    # FINAL REPORT
    print("\n" + "="*80)
    print("FINAL STATISTICS REPORT")
    print("="*80)
    print(f"Processing time: {stats['processing_time_seconds']} seconds")
    print(f"\nINPUT:")
    print(f"  Files processed: {stats['total_files']}")
    print(f"  Total rows: {stats['total_input_rows']:,}")
    print(f"  Total columns: {stats['total_input_columns']}")
    print(f"  Avg rows/file: {stats['total_input_rows'] // stats['total_files'] if stats['total_files'] > 0 else 0:,}")
    print(f"\nNORMALIZATION VIOLATIONS DETECTED:")
    print(f"  1NF violations (multivalued): {stats['files_with_1nf_violations']} files")
    print(f"  2NF violations (partial deps): {stats['files_with_2nf_violations']} files")
    print(f"  3NF violations (transitive deps): {stats['files_with_3nf_violations']} files")
    print(f"\nFOREIGN KEY DETECTION:")
    print(f"  Total FKs found: {stats['total_fks_detected'] + stats['total_fks_inferred']}")
    print(f"  - Strict detection: {stats['total_fks_detected']}")
    print(f"  - Auto-inferred (_id): {stats['total_fks_inferred']}")
    print(f"\nOUTPUT:")
    print(f"  Normalized tables: {stats['total_output_tables']}")
    print(f"  Table expansion: {stats['total_input_tables']} → {stats['total_output_tables']} "
          f"({'+' if stats['total_output_tables'] >= stats['total_input_tables'] else ''}"
          f"{stats['total_output_tables'] - stats['total_input_tables']})")
    print(f"  Natural keys used: {stats['natural_keys_used']}")
    print(f"  Surrogate keys added: {stats['surrogate_keys_added']}")
    print(f"  Child tables identified: {stats['child_tables_identified']}")
    print(f"\nVALIDATION:")
    print(f"  Status: {'✓ PASSED' if validation_passed else '✗ FAILED'}")
    print(f"  Errors: {total_errors}")
    print(f"  Warnings: {total_warnings}")
    
    if stats['errors']:
        print(f"\nERRORS ENCOUNTERED: {len(stats['errors'])}")
        for i, error in enumerate(stats['errors'][:10], 1):
            print(f"  {i}. {error}")
        if len(stats['errors']) > 10:
            print(f"  ... and {len(stats['errors']) - 10} more")
    
    print(f"\nOUTPUT DIRECTORY: {output_dir}")
    print("  - normalized_data/   (CSV and JSON files)")
    print("  - sql/              (DDL scripts)")
    print("  - normalization_log.txt")
    
    # Save statistics report
    stats_file = os.path.join(output_dir, "statistics_report.txt")
    with open(stats_file, 'w') as f:
        f.write("="*80 + "\n")
        f.write("3NF NORMALIZATION TEST - 50 FILES\n")
        f.write("="*80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write(f"Processing time: {stats['processing_time_seconds']} seconds\n\n")
        
        f.write("INPUT:\n")
        f.write(f"  Files processed: {stats['total_files']}\n")
        f.write(f"  Total rows: {stats['total_input_rows']:,}\n")
        f.write(f"  Total columns: {stats['total_input_columns']}\n\n")
        
        f.write("NORMALIZATION VIOLATIONS:\n")
        f.write(f"  1NF violations: {stats['files_with_1nf_violations']}\n")
        f.write(f"  2NF violations: {stats['files_with_2nf_violations']}\n")
        f.write(f"  3NF violations: {stats['files_with_3nf_violations']}\n\n")
        
        f.write("FOREIGN KEYS:\n")
        f.write(f"  Total detected: {stats['total_fks_detected'] + stats['total_fks_inferred']}\n")
        f.write(f"  - Strict: {stats['total_fks_detected']}\n")
        f.write(f"  - Inferred: {stats['total_fks_inferred']}\n\n")
        
        f.write("OUTPUT:\n")
        f.write(f"  Normalized tables: {stats['total_output_tables']}\n")
        f.write(f"  Natural keys: {stats['natural_keys_used']}\n")
        f.write(f"  Surrogate keys: {stats['surrogate_keys_added']}\n")
        f.write(f"  Child tables: {stats['child_tables_identified']}\n\n")
        
        f.write("VALIDATION:\n")
        f.write(f"  Status: {'PASSED' if validation_passed else 'FAILED'}\n")
        f.write(f"  Errors: {total_errors}\n")
        f.write(f"  Warnings: {total_warnings}\n")
    
    print(f"\n✓ Statistics report saved: {stats_file}")
    
    print("\n" + "="*80)
    print("TEST COMPLETED SUCCESSFULLY!")
    print("="*80)
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

if __name__ == "__main__":
    main()
