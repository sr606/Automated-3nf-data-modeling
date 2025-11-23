# 📖 QUICK REFERENCE GUIDE

## One-Command Execution

```powershell
python main.py
```

## File Structure Quick View

```
Data_Modelling_3NF/
│
├── 📥 INPUT
│   └── input_files/           # Your CSV/JSON files go here
│
├── 📤 OUTPUTS
│   ├── normalized_output/     # Normalized tables (CSV + JSON)
│   ├── sql_output/           # SQL DDL scripts
│   └── erd/                  # Entity-Relationship Diagrams
│
├── 🔧 CORE MODULES
│   ├── main.py               # ⭐ Start here
│   ├── langgraph_app.py      # Workflow orchestration
│   ├── metadata_extractor.py # Data analysis
│   ├── auto_profiler.py      # Dependency detection
│   ├── fk_detector.py        # Foreign key detection
│   ├── normalizer.py         # 3NF normalization
│   ├── sql_generator.py      # SQL generation
│   └── utils.py              # Helper functions
│
└── 📄 DOCUMENTATION
    ├── README.md             # Full documentation
    ├── SETUP.md              # Installation guide
    └── requirements.txt      # Dependencies
```

## Common Commands

### Install Dependencies
```powershell
pip install -r requirements.txt
```

### Run the System
```powershell
python main.py
```

### Test Individual Modules
```powershell
# Test metadata extraction
python metadata_extractor.py

# Test profiling
python auto_profiler.py

# Test FK detection
python fk_detector.py

# Test normalization
python normalizer.py

# Test SQL generation
python sql_generator.py
```

### View Outputs
```powershell
# List normalized tables
dir normalized_output

# View SQL script
type sql_output\normalized_schema.sql

# Open ERD
start erd\normalized_erd.png
```

### Clean Outputs
```powershell
# Clear all outputs
Remove-Item normalized_output\* -Force
Remove-Item sql_output\* -Force
Remove-Item erd\* -Force
```

## Quick Data Format Examples

### CSV Format
```csv
id,name,category
1,Product A,Electronics
2,Product B,Furniture
```

### JSON Format
```json
[
  {"id": 1, "name": "Product A", "category": "Electronics"},
  {"id": 2, "name": "Product B", "category": "Furniture"}
]
```

## Normal Form Checks

### ✅ 1NF (First Normal Form)
- **Rule**: Atomic values only, no repeating groups
- **Violation Example**: Column with "Python,Java,SQL"
- **Fix**: Split into separate table

### ✅ 2NF (Second Normal Form)
- **Rule**: No partial dependencies
- **Applies to**: Tables with composite primary keys
- **Violation Example**: `(OrderID, ProductID) → CustomerName`
- **Fix**: CustomerName depends only on OrderID

### ✅ 3NF (Third Normal Form)
- **Rule**: No transitive dependencies
- **Violation Example**: `EmployeeID → DepartmentID → DepartmentName`
- **Fix**: Extract department info to separate table

## Key Detection Logic

### Primary Key Detection
```
High Uniqueness (>95%) + No Nulls = Candidate Key
```

### Foreign Key Detection
```
Name Similarity + Value Overlap + Cardinality Pattern = FK Score
Score > 50 = Foreign Key Relationship
```

## SQL Generation Rules

### Oracle Datatypes Map
```
Integer        → NUMBER(10)
Decimal        → NUMBER(15,2)
String         → VARCHAR2(n)
Date           → DATE
Datetime       → TIMESTAMP
Boolean        → CHAR(1)
```

### Reserved Word Handling
```
Reserved Word → Add Suffix
Example: SELECT → SELECT_col
         DATE   → DATE_col
```

## Workflow Nodes

```
1. load_files          → Load CSV/JSON files
2. extract_metadata    → Extract column info
3. profile            → Detect dependencies
4. detect_primary_keys → Find PKs
5. detect_foreign_keys → Find FKs
6. normalize_3nf      → Perform normalization
7. generate_sql       → Create DDL scripts
8. validate_sql       → Check SQL syntax
9. export_outputs     → Generate ERD & outputs
```

## Output Files Explained

### Normalized Tables
- **Format**: CSV + JSON
- **Purpose**: Use for data import
- **Location**: `normalized_output/`

### SQL Script
- **Format**: .sql file
- **Purpose**: Execute in Oracle DB
- **Location**: `sql_output/normalized_schema.sql`
- **Contains**:
  - CREATE TABLE statements
  - PRIMARY KEY constraints
  - FOREIGN KEY constraints
  - INDEX definitions

### ERD Diagram
- **Format**: PNG (if Graphviz installed) or MermaidJS
- **Purpose**: Visualize schema
- **Location**: `erd/normalized_erd.png` or `erd/normalized_erd.mmd`

## Common Use Cases

### Use Case 1: Normalize Legacy Data
```
1. Export legacy data to CSV
2. Place in input_files/
3. Run: python main.py
4. Import SQL script to new database
```

### Use Case 2: Analyze Existing Schema
```
1. Export tables as CSV
2. Run system to detect issues
3. Review normalization log
4. Apply suggested improvements
```

### Use Case 3: Database Migration
```
1. Export from source DB
2. Normalize with system
3. Generate target SQL
4. Import to target DB
```

### Use Case 4: Data Quality Check
```
1. Load data files
2. Review metadata extraction
3. Check for multivalued columns
4. Verify relationships
```

## Troubleshooting Quick Fixes

| Problem | Quick Fix |
|---------|-----------|
| No files found | Check `input_files/` folder |
| Import error | `pip install -r requirements.txt` |
| SQL errors | Review validation output |
| No ERD PNG | Install Graphviz or use .mmd file |
| Memory error | Process fewer files |

## Performance Guidelines

| File Count | Expected Time | Memory Usage |
|------------|---------------|--------------|
| 1-10 files | < 1 minute | < 500 MB |
| 11-50 files | 1-5 minutes | 500 MB - 2 GB |
| 51-200 files | 5-20 minutes | 2-8 GB |

## Key Module Functions

### metadata_extractor.py
```python
extract_all_metadata(folder) → Dict
infer_datatype(series) → str
detect_multivalued(series) → bool
```

### auto_profiler.py
```python
profile_all_tables() → Dict
find_functional_dependencies() → Dict
detect_partial_dependencies() → Dict
detect_transitive_dependencies() → List
```

### fk_detector.py
```python
detect_all_foreign_keys(threshold) → List
calculate_name_similarity() → float
calculate_value_overlap() → Dict
```

### normalizer.py
```python
normalize_all_tables() → Dict
enforce_1nf() → Dict
enforce_2nf() → Dict
enforce_3nf() → Dict
```

### sql_generator.py
```python
generate_all_sql(output_dir) → str
sanitize_identifier(name) → str
generate_create_table_script() → str
generate_foreign_key_constraints() → List
```

## Customization Points

### Adjust FK Detection Sensitivity
```python
# In fk_detector.py
foreign_keys = fk_detector.detect_all_foreign_keys(threshold=40.0)  # Lower = more FKs
```

### Change Datatype Mappings
```python
# In metadata_extractor.py, infer_datatype method
# Add custom logic
```

### Add Custom Normalization Rules
```python
# In normalizer.py
# Add new methods following existing patterns
```

### Modify SQL Dialect
```python
# In sql_generator.py
# Change ORACLE_RESERVED_WORDS
# Modify datatype mappings
```

## Validation Checklist

### Before Running
- [ ] Input files in correct folder
- [ ] Files are valid CSV/JSON
- [ ] Dependencies installed

### After Running
- [ ] Check console for errors
- [ ] Verify normalized table count
- [ ] Test SQL script in Oracle
- [ ] Review ERD for correctness
- [ ] Validate FK relationships

## Support Resources

### Online Tools
- **Mermaid ERD Viewer**: https://mermaid.live/
- **Graphviz Online**: https://dreampuf.github.io/GraphvizOnline/
- **CSV Validator**: https://csvlint.io/

### Documentation
- **Full README**: `README.md`
- **Setup Guide**: `SETUP.md`
- **This Guide**: `QUICKREF.md`

---

## 🎯 Most Common Workflow

```powershell
# 1. Add your data files
Copy-Item "your_data\*.csv" input_files\

# 2. Run the system
python main.py

# 3. Check results
dir normalized_output
type sql_output\normalized_schema.sql
start erd\normalized_erd.png

# 4. Import to Oracle
# Open Oracle SQL Developer → Execute normalized_schema.sql
```

---

## 🎯 Enhanced Normalization Rules (Generalized)

### When to Create a New Table

✅ **YES - Create table when:**
1. **True transitive dependency:** PK → A → B (verified via FD tests)
2. **Multi-row pattern:** Same ID with timestamps/status changes
3. **Semantic entity:** Confidence ≥ 40%, has 2+ diverse attributes
4. **Multivalued attribute:** Arrays, JSON, delimited values
5. **Structured field:** Concatenated addresses, full names

❌ **NO - Keep in main table when:**
1. **Low cardinality:** < 10 unique values or < 2% uniqueness
2. **Simple descriptor:** Only 1 attribute (just a name/label)
3. **Direct dependency:** PK directly determines target (no intermediate needed)
4. **Low confidence:** Entity confidence < 40%
5. **Categorical value:** Status codes, state codes, simple types

### Configuration Thresholds

```python
# Entity Detection
MIN_UNIQUE_VALUES = max(10, rows * 0.01)  # Dynamic
MIN_UNIQUENESS_RATIO = 0.02  # 2%
CONFIDENCE_THRESHOLD = 0.40  # 40%

# Key Selection
NATURAL_KEY_PREFERRED = Always (when unique & not FK)
SURROGATE_KEY_GENERATED = Only when no natural key exists
FOREIGN_KEY_AS_PK = NEVER
```

### Common Scenarios

**Scenario 1: Extract Supplier Entity** ✅
- Data: supplier_id + name + email + phone
- Confidence: 80% (has contact info, 3+ attributes)
- Decision: CREATE suppliers table

**Scenario 2: Keep State Code** ❌
- Data: state_code + state_name
- Confidence: 10% (low cardinality, 1 attribute)
- Decision: KEEP in main table

**Scenario 3: Detect Event Table** ✅
- Data: order_id (duplicates) + status + date
- Pattern: event_history (temporal columns)
- Decision: CREATE order_status_history table

---

**That's it! Simple, powerful, automated.** 🚀
