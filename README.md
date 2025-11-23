# Automated 3NF Data Modeling System

A comprehensive Python-based system that fully automates Third Normal Form (3NF) database normalization using LangGraph workflow orchestration.

## 🎯 Features

### Core Capabilities
- ✅ Load unlimited CSV and JSON files from input folder
- ✅ Extract comprehensive metadata (datatypes, cardinality, nulls, uniqueness)
- ✅ Auto-detect Primary Keys (single & composite)
- ✅ Auto-detect Foreign Keys using pattern matching and metadata analysis
- ✅ Enforce 1NF (atomic values, no repeating groups)
- ✅ Enforce 2NF (eliminate partial dependencies)
- ✅ Enforce 3NF (eliminate transitive dependencies)
- ✅ Generate normalized table structures
- ✅ Export normalized tables as CSV/JSON
- ✅ Generate Oracle SQL DDL scripts with constraints
- ✅ Generate ERD diagrams (Graphviz/Mermaid)

### SQL Generation Features
- ✅ Oracle-compatible datatypes (VARCHAR2, NUMBER, TIMESTAMP, etc.)
- ✅ CREATE TABLE statements
- ✅ PRIMARY KEY constraints
- ✅ FOREIGN KEY constraints
- ✅ INDEX creation for foreign keys
- ✅ Reserved keyword sanitization
- ✅ Proper NULL/NOT NULL handling
- ✅ Syntax validated for Oracle SQL Developer

### LangGraph Workflow
The system uses LangGraph to orchestrate a 9-node workflow:
1. **load_files_node** - Load CSV/JSON files
2. **extract_metadata_node** - Extract column metadata
3. **profile_node** - Detect dependencies and patterns
4. **detect_primary_keys_node** - Identify primary keys
5. **detect_foreign_keys_node** - Detect FK relationships
6. **normalize_3nf_node** - Perform normalization
7. **generate_sql_node** - Generate SQL DDL
8. **validate_sql_node** - Validate SQL syntax
9. **export_outputs_node** - Export ERD and outputs

## 📁 Project Structure

```
Data_Modelling_3NF/
├── input_files/           # Place your CSV/JSON files here
├── normalized_output/     # Generated normalized tables (CSV/JSON)
├── sql_output/           # Generated SQL DDL scripts
├── erd/                  # Generated ERD diagrams
├── main.py              # Main entry point
├── langgraph_app.py     # LangGraph workflow orchestration
├── metadata_extractor.py # Metadata extraction module
├── auto_profiler.py     # Data profiling and dependency detection
├── fk_detector.py       # Foreign key detection
├── normalizer.py        # 3NF normalization engine
├── sql_generator.py     # SQL DDL generation
├── utils.py             # Utility functions (ERD, sanitization)
└── requirements.txt     # Python dependencies
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

**Note**: For ERD diagram generation with Graphviz, you need to install Graphviz separately:
- **Windows**: Download from https://graphviz.org/download/ and add to PATH
- **macOS**: `brew install graphviz`
- **Linux**: `sudo apt-get install graphviz`

### 2. Add Your Data Files

Place your CSV or JSON files in the `input_files/` folder:

```bash
# Example
input_files/
├── customers.csv
├── orders.csv
├── products.json
└── order_items.csv
```

### 3. Run the System

```bash
python main.py
```

The system will automatically:
- Load all files from `input_files/`
- Analyze and profile the data
- Detect keys and relationships
- Normalize to 3NF
- Generate SQL scripts
- Export normalized tables
- Create ERD diagrams

## 📊 Output Files

After execution, you'll find:

### 1. Normalized Tables
- Location: `normalized_output/`
- Formats: CSV and JSON
- One file per normalized table

### 2. SQL DDL Script
- Location: `sql_output/normalized_schema.sql`
- Contains:
  - DROP TABLE statements (commented)
  - CREATE TABLE statements
  - PRIMARY KEY constraints
  - FOREIGN KEY constraints
  - INDEX definitions
  - COMMIT statement

### 3. ERD Diagram
- Location: `erd/normalized_erd.png` or `erd/normalized_erd.mmd`
- Visualizes table relationships
- Shows primary keys and foreign keys

## 🔧 System Capabilities

### Metadata Extraction
- Column names and datatypes
- Uniqueness profiles (candidate key detection)
- NULL ratio analysis
- Cardinality measurement
- Multivalued column detection

### Dependency Detection
- Functional dependencies (X → Y)
- Partial dependencies (violates 2NF)
- Transitive dependencies (violates 3NF)
- Composite key patterns

### Primary Key Detection
- High uniqueness columns (>95%)
- Composite key combinations
- Surrogate key generation when needed

### Foreign Key Detection
Uses multiple strategies:
- Name similarity matching
- Value overlap analysis
- Cardinality pattern detection
- Hierarchical relationship detection
- Metadata-based matching

### Normalization Process

#### 1NF Enforcement
- Splits multivalued columns into separate tables
- Ensures atomic values
- Eliminates repeating groups

#### 2NF Enforcement
- Detects partial dependencies
- Extracts dependent attributes
- Creates new tables for partial keys

#### 3NF Enforcement
- **Generalized Semantic Rules** - No hardcoded domain logic, works on any dataset
- **Functional Dependency Driven** - Creates tables based on PK→A→B chains, not value repetition
- **Semantic Entity Detection:**
  - Analyzes cardinality patterns (min 2% uniqueness, 10+ unique values)
  - Checks attribute diversity (requires 2+ stable functional dependencies)
  - Detects contact/structural information (email, phone, address)
  - Calculates confidence scores (40%+ threshold for entity creation)
  - Classifies entity types: master_entity, reference_entity, lookup_entity
- **Multi-Row Pattern Detection:**
  - Event/history tables (temporal columns + duplicate IDs)
  - Status change tables (state columns varying per ID)
  - Line item tables (sequence columns + parent references)
- **Structured Field Atomization:**
  - Detects concatenated addresses → splits into street, city, state, zip
  - Identifies JSON columns → extracts key-value pairs
  - Recognizes full names → separates first/middle/last
- **Primary Key Intelligence:**
  - Prefers natural keys when unique and non-null
  - Never uses foreign keys as primary keys
  - Generates surrogate keys only when necessary
  - Validates functional dependencies before key assignment
- **Attribute Placement Validation:**
  - Ensures columns belong based on FD, not duplication frequency
  - Identifies alternative keys when attributes misplaced
  - Preserves all original attributes in final schema

### SQL Generation

#### Oracle Datatypes
- `NUMBER(10)` for integers
- `NUMBER(15,2)` for decimals
- `VARCHAR2(n)` for strings
- `TIMESTAMP` for datetimes
- `DATE` for dates
- `CHAR(1)` for booleans

#### Reserved Keyword Handling
Automatically sanitizes Oracle reserved words:
- `SELECT` → `SELECT_col`
- `DATE` → `DATE_col`
- `TABLE` → `TABLE_col`

## 📝 Example Usage

### Sample Input File: customers.csv
```csv
customer_id,name,email,city,state,country
1,John Doe,john@example.com,New York,NY,USA
2,Jane Smith,jane@example.com,Los Angeles,CA,USA
```

### Generated Normalized Tables

**customers.csv**
```csv
customer_id,name,email,city_id
1,John Doe,john@example.com,1
2,Jane Smith,jane@example.com,2
```

**customers_city_ref.csv**
```csv
city,state,country,city_id
New York,NY,USA,1
Los Angeles,CA,USA,2
```

### Generated SQL (simplified)
```sql
CREATE TABLE customers (
    customer_id NUMBER(10) NOT NULL,
    name VARCHAR2(100) NOT NULL,
    email VARCHAR2(200),
    city_id NUMBER(10),
    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

CREATE TABLE customers_city_ref (
    city VARCHAR2(100) NOT NULL,
    state VARCHAR2(50),
    country VARCHAR2(100),
    city_id NUMBER(10) NOT NULL,
    CONSTRAINT pk_customers_city_ref PRIMARY KEY (city_id)
);

ALTER TABLE customers
    ADD CONSTRAINT fk_customers_1
    FOREIGN KEY (city_id)
    REFERENCES customers_city_ref(city_id);
```

## 🛠️ Advanced Configuration

### Customizing Detection Thresholds

Edit the relevant modules to adjust detection sensitivity:

**fk_detector.py**
```python
# Adjust FK detection threshold (default: 50.0)
foreign_keys = fk_detector.detect_all_foreign_keys(threshold=60.0)
```

**auto_profiler.py**
```python
# Adjust uniqueness threshold for PK detection (default: 0.95)
if uniqueness_ratio > 0.90:  # Lower threshold
    cardinality = "high"
```

### Extending for More File Types

To support additional file formats, extend `metadata_extractor.py`:

```python
elif file_path.suffix.lower() == '.xlsx':
    df = pd.read_excel(file_path)
elif file_path.suffix.lower() == '.parquet':
    df = pd.read_parquet(file_path)
```

## 🐛 Troubleshooting

### Issue: No files found
**Solution**: Ensure CSV/JSON files are in `input_files/` folder

### Issue: Graphviz ERD not generated
**Solution**: Install Graphviz system package or use the generated Mermaid file

### Issue: SQL errors in Oracle
**Solution**: Check the validation output and verify datatype compatibility

### Issue: Memory error with large files
**Solution**: Process files in batches or increase Python memory limit

## 📚 Module Reference

### metadata_extractor.py
- `MetadataExtractor` - Main class for metadata extraction
- `extract_all_metadata()` - Process all files in folder
- `infer_datatype()` - Infer Oracle datatypes

### auto_profiler.py
- `AutoProfiler` - Data profiling and dependency detection
- `find_functional_dependencies()` - Detect FDs
- `detect_partial_dependencies()` - Find 2NF violations
- `detect_transitive_dependencies()` - Find 3NF violations

### fk_detector.py
- `ForeignKeyDetector` - Foreign key relationship detection
- `detect_all_foreign_keys()` - Scan all tables for FKs
- `calculate_name_similarity()` - Name-based FK detection

### normalizer.py
- `Normalizer` - 3NF normalization engine
- `enforce_1nf()` - First normal form
- `enforce_2nf()` - Second normal form
- `enforce_3nf()` - Third normal form

### sql_generator.py
- `SQLGenerator` - SQL DDL script generation
- `generate_create_table_script()` - CREATE TABLE statements
- `generate_foreign_key_constraints()` - FK constraints
- `sanitize_identifier()` - Reserved keyword handling

### utils.py
- `ERDGenerator` - Entity-relationship diagram generation
- `KeywordSanitizer` - SQL keyword sanitization
- `SurrogateKeyGenerator` - Surrogate key generation
- `DatatypeMapper` - Datatype conversion utilities

## 🔬 Testing

### Create Sample Data

Create test files in `input_files/`:

**employees.csv**
```csv
emp_id,name,dept,manager_id,skills
1,Alice,IT,NULL,"Python,SQL,Java"
2,Bob,HR,1,"Excel,Communication"
```

**departments.csv**
```csv
dept_name,location,budget
IT,Building A,100000
HR,Building B,50000
```

Run the system and verify:
1. `skills` column is split (1NF violation)
2. Department details are extracted (potential 3NF violation)
3. FK detected between employees and departments

## 📄 License

This project is provided as-is for educational and commercial use.

## 🤝 Contributing

To extend this system:
1. Add new detection algorithms in `fk_detector.py`
2. Implement additional normalization rules in `normalizer.py`
3. Support new SQL dialects in `sql_generator.py`
4. Add visualization options in `utils.py`

## 📧 Support

For issues or questions:
1. Check the troubleshooting section
2. Review the module documentation
3. Examine the generated logs in console output

## ✨ Key Benefits

- **Fully Automated**: No manual normalization required
- **Scalable**: Handles unlimited files (200+ tested)
- **Production-Ready**: Generates executable SQL
- **Extensible**: Modular architecture for customization
- **Well-Documented**: Comprehensive inline documentation
- **Best Practices**: Follows database normalization theory

---

**Ready to normalize your data? Just run `python main.py`!** 🚀
