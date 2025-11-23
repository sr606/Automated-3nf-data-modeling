# 3NF Normalization Engine - Generalized Enhancements

## Overview

The 3NF normalization engine has been comprehensively enhanced to work **dynamically on any dataset** without hardcoded domain-specific rules. All normalization decisions are now driven by **functional dependencies**, **semantic relationships**, and **statistical analysis** rather than value repetition patterns.

---

## Core Enhancements

### 1. Structured Field Detection & Atomization

**Location:** `metadata_extractor.py::detect_structured_field()`

**Purpose:** Automatically detect and prepare for atomization of concatenated/structured data that violates 1NF.

**Capabilities:**
- **Address Detection:** Identifies address patterns (street numbers, city, state, ZIP codes)
- **JSON Detection:** Recognizes JSON objects and extracts component keys
- **Full Name Detection:** Identifies concatenated names and suggests splitting into first/middle/last
- **Pattern Recognition:** Uses regex patterns to detect structure in text fields

**Algorithm:**
```
For each column:
  1. Check column name for structure indicators (address, name, location)
  2. Sample data and apply pattern matching
  3. Detect common structures (addresses, JSON, names)
  4. Return structure type and detected components
```

**Output:**
```python
{
    'is_structured': True,
    'structure_type': 'address',  # or 'json', 'full_name'
    'detected_components': ['street', 'city', 'state', 'zip_code']
}
```

---

### 2. Semantic Entity Detection

**Location:** `normalizer.py::_detect_semantic_entity()`

**Purpose:** Determine if an intermediate column represents a genuine business entity worthy of extraction, using **only data-driven analysis** without domain knowledge.

**Analysis Factors:**

| Factor | Weight | Threshold | Purpose |
|--------|--------|-----------|---------|
| Attribute Count | 0.1-0.5 | 1-3+ | More attributes = higher entity confidence |
| Uniqueness Ratio | 0.1-0.2 | 2-70% | Moderate uniqueness suggests master data |
| Semantic Clustering | 0.2 | 1+ shared tokens | Column names share semantic meaning |
| Structural Attributes | 0.3 | Has contact info | Email/phone/address indicates real entity |

**Decision Rules:**
- ❌ **Reject** if: uniqueness < 2%, unique count < 10, no stable FDs
- ❌ **Reject** if: confidence score < 40%
- ✅ **Accept** if: confidence ≥ 40% with 2+ diverse attributes

**Entity Classification:**
- **master_entity:** High uniqueness (>50%), many attributes
- **reference_entity:** Has contact/structural information
- **lookup_entity:** Moderate diversity, structured reference data

**Example Output:**
```python
{
    'is_entity': True,
    'confidence': 0.65,
    'entity_type': 'reference_entity',
    'reasons': ['Has contact/address attributes', '3 diverse attributes', 
                'Cardinality: 127 unique (15.2%)']
}
```

---

### 3. Multi-Row Pattern Detection

**Location:** `normalizer.py::_detect_multi_row_pattern()`

**Purpose:** Identify when same identifier appears multiple times, indicating event/history/child table relationships.

**Detection Patterns:**

| Pattern | Indicators | Example |
|---------|-----------|---------|
| event_history | Temporal columns (date, timestamp, created) | Order status changes |
| status_history | Status/state columns varying per ID | Ticket state transitions |
| line_items | Table name contains item/line/detail | Order line items |
| sequenced_children | Sequence/position/rank columns | Product components |
| child_records | Generic multi-row (fallback) | Any 1-to-many relationship |

**Algorithm:**
```
1. Check if potential_pk_col has duplicates
2. If yes:
   a. Look for temporal columns → event_history
   b. Look for varying status columns → status_history
   c. Check table name for item indicators → line_items
   d. Look for sequence columns → sequenced_children
   e. Default → child_records
```

**Output:**
```python
{
    'is_multi_row': True,
    'pattern_type': 'event_history',
    'evidence': ['142 duplicate values in order_id', 
                 'Temporal columns: order_date, shipped_date']
}
```

---

### 4. Functional Dependency Chain Verification

**Location:** `normalizer.py::_verify_functional_dependency_chain()`

**Purpose:** Ensure true transitive dependency exists (PK→A→B) rather than just direct dependency (PK→B).

**Test Sequence:**
```
Test 1: Verify PK → intermediate
  - Group by PK, count unique intermediate values
  - Must be 1:1 (intermediate determined by PK)

Test 2: Verify intermediate → target
  - Group by intermediate, count unique target values
  - Must be 1:1 (target determined by intermediate)

Test 3: Ensure NOT direct PK → target
  - Check if PK directly determines target
  - If yes, verify intermediate carries additional information
  - Intermediate must vary across PKs for true transitivity
```

**Rejection Criteria:**
- ❌ Intermediate varies for same PK (not a functional dependency)
- ❌ Target varies for same intermediate (no dependency)
- ❌ Direct PK→target exists AND intermediate doesn't add info

---

### 5. Attribute Placement Validation

**Location:** `normalizer.py::_validate_attribute_placement()`

**Purpose:** Verify attributes belong in tables based on **functional dependencies**, not duplication frequency.

**Validation Process:**
```
For each attribute:
  1. Test FD: Does PK → attribute hold?
  2. Group by PK, check if attribute constant per group
  3. If YES → attribute belongs here
  4. If NO → search for alternative key that determines attribute
  5. Report findings with reasons
```

**Output:**
```python
{
    'belongs_here': False,
    'reason': 'Functionally dependent on supplier_id, not PK',
    'alternative_key': 'supplier_id'
}
```

---

### 6. Natural vs Surrogate Key Selection

**Location:** `normalizer.py::_determine_best_primary_key()`

**Purpose:** Choose optimal primary key using intelligent scoring system.

**Key Selection Logic:**

| Candidate Type | Score | Conditions |
|---------------|-------|-----------|
| Single natural key | 100 | Unique, non-null, NOT a foreign key |
| 2-column composite | 85 | Unique combination, NOT containing FKs |
| 3-column composite | 80 | Unique combination, NOT containing FKs |
| Surrogate key | 0 | No natural candidates found |

**Rules:**
- ❌ **NEVER** use foreign key as primary key
- ✅ Prefer single-column natural keys
- ✅ Prefer smaller composite keys over larger
- ✅ Generate surrogate when no natural key exists

**Output:**
```python
{
    'key_type': 'natural',  # or 'surrogate'
    'key_columns': ['email'],
    'reason': 'email is unique and non-null'
}
```

---

## Normalization Workflow

### Updated Process Flow

```
┌─────────────────────────────────────────────────┐
│ Step 0: Analyze Table Structure                │
│ - Detect column patterns (identifiers,         │
│   temporal, quantitative, categorical)          │
│ - Infer table type (transaction, master, etc)  │
└─────────────────┬───────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────┐
│ Step 1: Enforce 1NF                            │
│ - Split multivalued columns                     │
│ - Atomize structured fields (addresses, JSON)  │
└─────────────────┬───────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────┐
│ Step 2: Enforce 2NF                            │
│ - Resolve partial dependencies                  │
│ - Extract attributes dependent on part of key  │
└─────────────────┬───────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────┐
│ Step 3: Enforce 3NF (Enhanced)                 │
│ FOR each transitive dependency:                 │
│   1. Verify functional dependency chain         │
│   2. Check for multi-row pattern                │
│   3. Run semantic entity detection              │
│   4. Apply confidence threshold (40%)           │
│   5. Create table ONLY if justified             │
└─────────────────┬───────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────┐
│ Step 4: Smart Key Assignment                   │
│ - Score natural key candidates                  │
│ - Validate: NOT foreign keys                    │
│ - Add surrogate keys when needed                │
└─────────────────┬───────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────┐
│ Step 5: Attribute Preservation Check           │
│ - Verify all original columns present           │
│ - Report any missing attributes                 │
└─────────────────────────────────────────────────┘
```

---

## Key Principles

### ✅ DO Create New Tables When:

1. **True Transitive Dependency Exists**
   - PK → intermediate → target chain verified
   - Intermediate adds semantic information
   - Not just a direct PK → target relationship

2. **Multi-Row Pattern Detected**
   - Same ID appears multiple times
   - With temporal data → event table
   - With status changes → history table
   - With sequence → child table

3. **Semantic Entity Identified**
   - Confidence score ≥ 40%
   - 2+ diverse functional dependencies
   - Contact/structural information present
   - Moderate uniqueness (2-70%)

4. **Multivalued Attribute Found**
   - Arrays, lists, JSON objects
   - Delimited values (comma, semicolon)
   - Violates atomicity requirement

5. **Structured Field Detected**
   - Concatenated addresses
   - Full names (first + last)
   - JSON objects with extractable keys

### ❌ DO NOT Create New Tables When:

1. **Low Cardinality Categorical Values**
   - < 10 unique values
   - < 2% uniqueness ratio
   - Examples: state codes, status flags, categories

2. **Simple Descriptive Attributes**
   - Single dependent attribute
   - Just a label/name for ID
   - No additional semantic information

3. **Direct Functional Dependencies**
   - PK directly determines target
   - No intermediate attribute needed
   - Would create unnecessary indirection

4. **Value Repetition Without FD**
   - Values repeat but don't follow FD pattern
   - Coincidental duplication
   - Not a stable relationship

5. **Low Confidence Entity**
   - Confidence score < 40%
   - Insufficient attribute diversity
   - No structural indicators

---

## Configuration & Thresholds

All thresholds are **data-driven** and adapt to dataset size:

```python
# Entity Detection
MIN_UNIQUE_THRESHOLD = max(10, total_rows * 0.01)  # At least 1% or 10
MIN_UNIQUENESS_RATIO = 0.02  # 2% minimum diversity
ENTITY_CONFIDENCE_THRESHOLD = 0.40  # 40% confidence required

# Attribute Scoring
ATTRIBUTE_COUNT_WEIGHTS = {
    1: 0.1,   # Single attribute
    2: 0.3,   # Two attributes
    3+: 0.5   # Three or more attributes
}

UNIQUENESS_OPTIMAL_RANGE = (0.05, 0.7)  # 5-70% ideal for entities
STRUCTURAL_ATTRIBUTE_BOOST = 0.3  # Contact info adds 30%
SEMANTIC_CLUSTER_BOOST = 0.2  # Shared tokens add 20%

# Key Selection
NATURAL_KEY_SINGLE_SCORE = 100
NATURAL_KEY_COMPOSITE_BASE = 90
COMPOSITE_KEY_SIZE_PENALTY = 5  # -5 per additional column
```

---

## Benefits

### 1. **Domain Agnostic**
- Works on healthcare, finance, retail, manufacturing, etc.
- No hardcoded entity types (customer, product, order)
- Pure statistical and semantic analysis

### 2. **Prevents Over-Normalization**
- No unnecessary reference tables for state codes
- No extraction of simple category labels
- Only creates tables with genuine semantic value

### 3. **Functional Dependency Driven**
- All decisions based on PK→A→B analysis
- Not influenced by value repetition frequency
- Follows theoretical normalization principles

### 4. **Intelligent Key Management**
- Prefers natural keys when available
- Never misuses foreign keys as primary keys
- Minimizes surrogate key proliferation

### 5. **Attribute Preservation**
- Guarantees all original columns in final schema
- Tracks attribute placement across tables
- Reports any missing attributes

### 6. **Transparent Reasoning**
- Logs confidence scores and evidence
- Explains why entities created or rejected
- Shows functional dependency chains

---

## Example Transformations

### Example 1: Supplier Entity (Accepted)

**Input:**
| order_id | supplier_id | supplier_name | supplier_email | supplier_phone |
|----------|-------------|---------------|----------------|----------------|
| 1        | S101        | Acme Corp     | info@acme.com  | 555-1234       |
| 2        | S102        | Best Inc      | sales@best.com | 555-5678       |

**Analysis:**
- Uniqueness: 100% (2/2 unique supplier_ids)
- Attributes: 3 dependent (name, email, phone)
- Has contact info: YES
- Confidence: 0.80 (80%)

**Decision:** ✅ CREATE supplier table
**Reason:** High confidence (80%), structural attributes, 3 diverse attributes

---

### Example 2: State Code (Rejected)

**Input:**
| customer_id | state_code | state_name |
|-------------|-----------|------------|
| 1           | CA        | California |
| 2           | TX        | Texas      |
| 3           | CA        | California |

**Analysis:**
- Uniqueness: 3.3% (2/60 unique state codes)
- Attributes: 1 dependent (state_name)
- Has contact info: NO
- Confidence: 0.10 (10%)

**Decision:** ❌ KEEP in main table
**Reason:** Low confidence (10%), low cardinality, single descriptive attribute

---

## Testing Recommendations

To test the enhanced normalization:

```python
# Test Case 1: Entity with contact info (should extract)
df = pd.DataFrame({
    'order_id': [1, 2, 3],
    'supplier_id': ['S1', 'S2', 'S3'],
    'supplier_name': ['Acme', 'Best', 'Corp'],
    'supplier_email': ['a@', 'b@', 'c@'],
    'supplier_phone': ['111', '222', '333']
})

# Test Case 2: Simple category (should NOT extract)
df = pd.DataFrame({
    'product_id': range(100),
    'category': ['Electronics'] * 50 + ['Clothing'] * 50,
    'category_desc': ['Tech items'] * 50 + ['Apparel'] * 50
})

# Test Case 3: Event table (should detect multi-row pattern)
df = pd.DataFrame({
    'order_id': [1, 1, 2, 2],
    'status': ['Pending', 'Shipped', 'Pending', 'Delivered'],
    'status_date': ['2024-01-01', '2024-01-05', '2024-01-02', '2024-01-06']
})
```

---

## Future Enhancements

Potential areas for further improvement:

1. **Machine Learning Integration:** Train classifier on normalized datasets
2. **Domain Ontologies:** Optional semantic enrichment using ontologies
3. **User Feedback Loop:** Learn from user corrections to improve detection
4. **Composite Entity Detection:** Recognize when multiple columns form single entity
5. **Temporal Dependency Analysis:** Detect time-varying attributes (SCD Type 2)

---

## Summary

The enhanced normalization engine is now **fully generalized** and **data-driven**:

- ✅ No hardcoded domain knowledge
- ✅ Functional dependency verification
- ✅ Semantic entity detection with confidence scoring
- ✅ Multi-row pattern recognition
- ✅ Structured field atomization
- ✅ Intelligent natural/surrogate key selection
- ✅ Attribute placement validation
- ✅ Complete attribute preservation

**Result:** A normalization system that works on **any dataset** while making **intelligent, justified decisions** based on **database theory** and **statistical analysis**.
