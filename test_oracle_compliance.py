"""
Test Oracle SQL compliance of the SQL generator
Tests all 5 compliance requirements
"""
import os
import re
from metadata_extractor import MetadataExtractor
from auto_profiler import AutoProfiler
from fk_detector import ForeignKeyDetector
from normalizer import Normalizer
from sql_generator import SQLGenerator

def test_oracle_compliance():
    print("="*80)
    print("ORACLE SQL COMPLIANCE TEST")
    print("="*80)
    
    # Use first 5 files from test_data_50
    input_dir = "./test_data_50"
    output_dir = "./test_oracle_output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Extract metadata
    print("\n[1/5] Extracting metadata...")
    extractor = MetadataExtractor()
    metadata = extractor.extract_all_metadata(input_dir)
    print(f"    Found {len(metadata)} files")
    
    # Step 2: Profile data
    print("\n[2/5] Profiling data...")
    profiler = AutoProfiler(metadata)
    profiles = profiler.profile_all_tables()
    
    # Step 3: Detect foreign keys
    print("\n[3/5] Detecting foreign keys...")
    fk_detector = ForeignKeyDetector(metadata, profiles)
    foreign_keys = fk_detector.detect_all_foreign_keys()
    print(f"    Detected {len(foreign_keys)} foreign keys")
    
    # Step 4: Normalize to 3NF
    print("\n[4/5] Normalizing to 3NF...")
    normalizer = Normalizer(metadata, profiles, foreign_keys)
    normalized_tables = normalizer.normalize_all_tables()
    print(f"    Generated {len(normalized_tables)} normalized tables")
    
    # Step 5: Generate SQL and test compliance
    print("\n[5/5] Generating SQL and checking Oracle compliance...")
    sql_generator = SQLGenerator(normalized_tables, metadata, profiles, foreign_keys)
    
    # Generate complete schema
    complete_sql = sql_generator.generate_complete_schema()
    
    # Save SQL
    sql_file = os.path.join(output_dir, "test_schema.sql")
    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write(complete_sql)
    
    print(f"\n    SQL generated: {sql_file}")
    
    # Run compliance checks
    print("\n" + "="*80)
    print("ORACLE COMPLIANCE CHECKS")
    print("="*80)
    
    issues = []
    
    # Check 1: No duplicate CREATE TABLE statements
    print("\n[CHECK 1] Duplicate CREATE TABLE prevention")
    create_table_pattern = re.compile(r'^CREATE TABLE (\w+)', re.IGNORECASE | re.MULTILINE)
    table_names = create_table_pattern.findall(complete_sql)
    duplicates = [name for name in table_names if table_names.count(name) > 1]
    if duplicates:
        issues.append(f"  FAIL: Duplicate CREATE TABLE for: {set(duplicates)}")
        print(f"  FAIL: Found {len(set(duplicates))} duplicate tables")
    else:
        print(f"  PASS: No duplicate CREATE TABLE statements ({len(table_names)} unique tables)")
    
    # Check 2: All identifiers <= 30 characters
    print("\n[CHECK 2] Oracle 30-character identifier limit")
    identifier_pattern = re.compile(r'\b([A-Za-z_][A-Za-z0-9_]*)\b')
    identifiers = identifier_pattern.findall(complete_sql)
    
    # Filter out SQL keywords
    sql_keywords = {'CREATE', 'TABLE', 'ALTER', 'ADD', 'CONSTRAINT', 'PRIMARY', 'KEY', 
                    'FOREIGN', 'REFERENCES', 'INDEX', 'ON', 'VARCHAR2', 'NUMBER', 
                    'TIMESTAMP', 'CHAR', 'NOT', 'NULL', 'COMMIT', 'DROP', 'CASCADE',
                    'CONSTRAINTS', 'Rows', 'Auto', 'generated', 'Generated', 'by',
                    'Automated', 'Data', 'Modeling', 'System'}
    
    long_identifiers = [id for id in identifiers if len(id) > 30 and id not in sql_keywords]
    if long_identifiers:
        issues.append(f"  FAIL: {len(long_identifiers)} identifiers exceed 30 chars")
        print(f"  FAIL: Found {len(long_identifiers)} identifiers exceeding 30 characters:")
        for id in long_identifiers[:5]:  # Show first 5
            print(f"    - {id} ({len(id)} chars)")
    else:
        print(f"  PASS: All identifiers <= 30 characters")
    
    # Check 3: No spaces or invalid characters in identifiers
    print("\n[CHECK 3] No spaces/special characters in identifiers")
    # Extract table/column names from CREATE TABLE statements
    table_block_pattern = re.compile(r'CREATE TABLE (\w+) \((.*?)\);', re.DOTALL | re.IGNORECASE)
    table_blocks = table_block_pattern.findall(complete_sql)
    
    invalid_chars = []
    for table_name, columns_block in table_blocks:
        # Check table name
        if re.search(r'[^A-Za-z0-9_]', table_name):
            invalid_chars.append(f"Table: {table_name}")
        
        # Check column names
        column_lines = [line.strip() for line in columns_block.split('\n') if line.strip() and not line.strip().startswith('CONSTRAINT')]
        for line in column_lines:
            parts = line.split()
            if parts:
                col_name = parts[0].rstrip(',')
                if re.search(r'[^A-Za-z0-9_]', col_name):
                    invalid_chars.append(f"Column: {col_name} in {table_name}")
    
    if invalid_chars:
        issues.append(f"  FAIL: {len(invalid_chars)} identifiers with invalid characters")
        print(f"  FAIL: Found {len(invalid_chars)} identifiers with invalid characters:")
        for item in invalid_chars[:5]:
            print(f"    - {item}")
    else:
        print(f"  PASS: All identifiers use valid characters (A-Z, 0-9, _)")
    
    # Check 4: Column lists inside CREATE TABLE parentheses
    print("\n[CHECK 4] Column lists inside CREATE TABLE parentheses")
    malformed_creates = []
    for match in create_table_pattern.finditer(complete_sql):
        table_name = match.group(1)
        start_pos = match.end()
        # Find opening parenthesis
        next_chars = complete_sql[start_pos:start_pos+10].strip()
        if not next_chars.startswith('('):
            malformed_creates.append(f"{table_name}: next_chars='{next_chars}'")
    
    if malformed_creates:
        issues.append(f"  FAIL: {len(malformed_creates)} tables with columns outside parentheses")
        print(f"  FAIL: {len(malformed_creates)} CREATE TABLE statements missing parentheses")
        for item in malformed_creates:
            print(f"    - {item}")
    else:
        print(f"  PASS: All CREATE TABLE statements have columns inside parentheses")
    
    # Check 5: PRIMARY KEY constraints inside table definition
    print("\n[CHECK 5] PRIMARY KEY constraints inside table definition")
    # This is a bit complex - we'll check that PK appears before the closing ); of CREATE TABLE
    pk_outside = []
    for table_name, columns_block in table_blocks:
        if 'PRIMARY KEY' in columns_block.upper():
            # PK is inside - good
            pass
        else:
            # Check if PK appears after this CREATE TABLE
            table_end = complete_sql.find(f'CREATE TABLE {table_name}')
            next_create = complete_sql.find('CREATE TABLE', table_end + 10)
            if next_create == -1:
                next_create = len(complete_sql)
            section = complete_sql[table_end:next_create]
            if 'PRIMARY KEY' in section.upper() and 'ALTER TABLE' in section.upper():
                pk_outside.append(table_name)
    
    if pk_outside:
        issues.append(f"  FAIL: {len(pk_outside)} tables with PK outside table definition")
        print(f"  FAIL: {len(pk_outside)} tables have PK constraints outside CREATE TABLE")
    else:
        print(f"  PASS: All PRIMARY KEY constraints inside table definitions")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    if issues:
        print(f"\nFAILED: {len(issues)} compliance issues found:")
        for issue in issues:
            print(issue)
        return False
    else:
        print("\nPASS: All Oracle SQL compliance checks passed!")
        print(f"\n  Tables generated: {len(table_names)}")
        print(f"  Foreign keys: {len(foreign_keys)}")
        print(f"  SQL output: {sql_file}")
        print(f"\nSQL schema is Oracle-compliant and ready for deployment!")
        return True

if __name__ == "__main__":
    success = test_oracle_compliance()
    exit(0 if success else 1)
