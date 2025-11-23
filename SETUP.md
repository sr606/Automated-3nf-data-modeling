# 🚀 SETUP AND INSTALLATION GUIDE

## Prerequisites

- **Python 3.10 or higher**
- **pip** (Python package installer)
- **Git** (optional, for cloning)
- **Oracle SQL Developer** (for testing generated SQL)

## Step-by-Step Setup

### Step 1: Verify Python Installation

Open PowerShell or Command Prompt and verify Python is installed:

```powershell
python --version
```

Should show Python 3.10 or higher.

### Step 2: Navigate to Project Directory

```powershell
cd c:\Data_Modelling_3NF
```

### Step 3: Install Python Dependencies

```powershell
pip install -r requirements.txt
```

This will install:
- pandas (data manipulation)
- langgraph (workflow orchestration)
- langchain & langchain-core (graph framework)
- numpy (numerical operations)
- graphviz (ERD generation - Python bindings)
- matplotlib (visualization)
- openpyxl (Excel support)
- python-dateutil (date parsing)
- Jinja2 (templating)
- pydantic (data validation)

**Expected output:**
```
Successfully installed pandas-2.x.x langgraph-0.x.x ...
```

### Step 4: Install Graphviz (Optional - for PNG ERDs)

**For Windows:**
1. Download installer from: https://graphviz.org/download/
2. Run the installer
3. Add Graphviz to your PATH:
   - Right-click "This PC" → Properties
   - Advanced System Settings → Environment Variables
   - Edit PATH, add: `C:\Program Files\Graphviz\bin`
4. Verify installation:
   ```powershell
   dot -V
   ```

**Note**: If you skip this step, the system will generate `.mmd` (Mermaid) and `.dot` files instead of PNG images. These can be visualized online.

### Step 5: Verify Directory Structure

Ensure you have these folders:
```
c:\Data_Modelling_3NF\
├── input_files/        ✓ Created
├── normalized_output/  ✓ Created
├── sql_output/         ✓ Created
├── erd/                ✓ Created
```

### Step 6: Verify Sample Data Files

Check that sample data files are present:
```powershell
dir input_files
```

Should show:
- customers.csv
- orders.csv
- order_items.csv
- products.json
- employees.csv

### Step 7: Run the System

```powershell
python main.py
```

**Expected Output:**
```
╔═══════════════════════════════════════════════════════════════════╗
║         AUTOMATED 3NF DATA MODELING SYSTEM                       ║
╚═══════════════════════════════════════════════════════════════════╝

📁 Found 5 files to process:
   • CSV files: 4
   • JSON files: 1

🚀 Starting normalization workflow...

======================================================================
NODE 1: LOADING FILES
======================================================================
Found 5 files to process
...
```

### Step 8: Check Generated Outputs

After successful execution:

**1. Normalized Tables:**
```powershell
dir normalized_output
```

**2. SQL Script:**
```powershell
type sql_output\normalized_schema.sql
```

**3. ERD Diagram:**
```powershell
start erd\normalized_erd.png
```

## 🧪 Testing the SQL Script

### Option 1: Oracle SQL Developer

1. Open Oracle SQL Developer
2. Connect to your Oracle database
3. Open the generated SQL file: `sql_output\normalized_schema.sql`
4. Execute the script (F5)
5. Verify tables are created without errors

### Option 2: SQL*Plus

```powershell
sqlplus username/password@database
```

```sql
@sql_output\normalized_schema.sql
```

## 📊 Testing with Your Own Data

### Step 1: Clear Sample Data

```powershell
Remove-Item input_files\*.csv
Remove-Item input_files\*.json
```

### Step 2: Add Your Files

Copy your CSV or JSON files to `input_files/`:

```powershell
Copy-Item "C:\path\to\your\data\*.csv" input_files\
```

**Supported formats:**
- CSV (comma-separated values)
- JSON (array of objects)

**CSV Example:**
```csv
id,name,email
1,John,john@example.com
2,Jane,jane@example.com
```

**JSON Example:**
```json
[
  {"id": 1, "name": "John", "email": "john@example.com"},
  {"id": 2, "name": "Jane", "email": "jane@example.com"}
]
```

### Step 3: Run Again

```powershell
python main.py
```

## 🔧 Troubleshooting

### Problem: "No module named 'langgraph'"

**Solution:**
```powershell
pip install langgraph --upgrade
```

### Problem: "No module named 'pandas'"

**Solution:**
```powershell
pip install pandas
```

### Problem: "No CSV or JSON files found"

**Solution:**
- Verify files are in `c:\Data_Modelling_3NF\input_files\`
- Check file extensions are `.csv` or `.json`
- Ensure files are not empty

### Problem: "Graphviz not found"

**Solution:**
- Install Graphviz system package (see Step 4)
- Or ignore - system will generate `.mmd` files instead
- Visualize at: https://mermaid.live/

### Problem: "SQL errors in Oracle"

**Solution:**
1. Check column names for reserved words
2. Verify datatypes are compatible
3. Review the validation output in console
4. Manually adjust the SQL script if needed

### Problem: "Memory error with large files"

**Solution:**
```powershell
# Process fewer files at once
# Or increase Python memory
python -Xmx4g main.py
```

## 🎯 Performance Tips

### For Large Datasets (50+ files)

1. **Process in batches**: Move files to `input_files/` in groups of 20-30

2. **Increase memory**: Edit Python settings if needed

3. **Disable ERD for speed**: Comment out ERD generation in `langgraph_app.py`:
   ```python
   # In export_outputs_node method
   # state['erd_path'] = erd_gen.generate_erd(...)
   ```

### For Complex Schemas

1. **Adjust FK threshold**: Lower threshold finds more relationships:
   ```python
   # In langgraph_app.py, detect_foreign_keys_node
   foreign_keys = fk_detector.detect_all_foreign_keys(threshold=40.0)
   ```

2. **Review normalization log**: Check console output for warnings

## 📝 Configuration Options

### Custom Datatypes

Edit `metadata_extractor.py` to customize Oracle datatypes:

```python
def infer_datatype(self, series: pd.Series) -> str:
    # Add custom logic
    if series.name == 'postal_code':
        return "VARCHAR2(10)"
    # ... existing code
```

### Custom Reserved Words

Add more reserved words in `sql_generator.py`:

```python
ORACLE_RESERVED_WORDS = {
    'ACCESS', 'ADD', ...,
    'CUSTOM_KEYWORD'  # Add your keywords
}
```

### Custom Normalization Rules

Extend `normalizer.py` with custom rules:

```python
def custom_normalization_rule(self, df):
    # Your custom logic
    pass
```

## 🔐 Security Notes

- **No credentials required**: System works with local files only
- **No network access**: All processing is local
- **Data privacy**: Your data never leaves your machine

## 📚 Additional Resources

### Documentation
- LangGraph: https://langchain-ai.github.io/langgraph/
- Pandas: https://pandas.pydata.org/docs/
- Graphviz: https://graphviz.org/documentation/

### Database Theory
- 3NF: https://en.wikipedia.org/wiki/Third_normal_form
- Oracle SQL: https://docs.oracle.com/en/database/

### Visualization
- Mermaid: https://mermaid.live/
- Graphviz Online: https://dreampuf.github.io/GraphvizOnline/

## ✅ Verification Checklist

Before running on production data:

- [ ] Python 3.10+ installed
- [ ] All dependencies installed (`pip list`)
- [ ] Sample data runs successfully
- [ ] SQL script executes in Oracle without errors
- [ ] Output folders contain expected files
- [ ] ERD diagram renders correctly
- [ ] Normalized tables are valid

## 🎓 Learning Resources

### Understanding the Workflow

1. **Start with**: `main.py` - Entry point
2. **Then explore**: `langgraph_app.py` - Workflow orchestration
3. **Deep dive**: Individual modules based on your interest

### Key Concepts

- **LangGraph**: Directed graph for workflow execution
- **Metadata Extraction**: Understanding your data structure
- **Normalization**: Eliminating redundancy
- **SQL Generation**: Creating executable database scripts

---

**Need Help?** Check the README.md or review the inline code documentation.

**Ready to Go?** Just run: `python main.py` 🚀
