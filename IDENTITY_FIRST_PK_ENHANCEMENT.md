# Identity-First Primary Key Selection Enhancement

## Overview
This document describes the critical enhancement made to the primary key selection logic to ensure that **semantic identity takes precedence over statistical uniqueness**.

## Problem Statement
Previously, the system could select high-cardinality non-identity columns as primary keys simply because they were unique in the dataset. Examples:
- `city` (if all customers from different cities)
- `loyalty_points` (if all unique values)
- `email` (unique but transactional)
- `salary` (unique but non-identity)

**This violates a fundamental database design principle:** Primary keys must represent semantic identity, not just statistical uniqueness.

## Solution

### New Validation Order
**OLD (FLAWED):**
```
1. Check blacklist patterns
2. Verify uniqueness
3. Check for nulls
→ If all pass, consider as PK candidate
```

**NEW (CORRECT):**
```
1. Check identity semantics (REQUIRED)
2. Check blacklist patterns
3. Verify uniqueness (validation only)
4. Check for nulls
→ Column MUST have identity semantics to be considered
```

### Implementation

#### 1. Identity Semantics Detection Method
```python
def _has_identity_semantics(self, col_name: str) -> Dict[str, Any]:
    """
    Check if column name contains semantic identity patterns
    """
    col_lower = col_name.lower()
    
    # Strong identity indicators (high confidence)
    strong_identity_patterns = [
        '_id', '_key', '_code', '_ref', '_number', 'sys_id',
        'uuid', 'guid'
    ]
    
    # Moderate identity indicators (at word boundaries)
    moderate_identity_patterns = ['id', 'key', 'code', 'ref', 'number']
    
    # Check strong patterns anywhere in name
    for pattern in strong_identity_patterns:
        if pattern in col_lower:
            return {
                'has_identity': True,
                'confidence': 'high',
                'reason': f'Contains identity pattern: {pattern}'
            }
    
    # Check moderate patterns at word boundaries
    parts = col_lower.split('_')
    for pattern in moderate_identity_patterns:
        if pattern in parts:
            return {
                'has_identity': True,
                'confidence': 'moderate',
                'reason': f'Contains identity word: {pattern}'
            }
    
    return {
        'has_identity': False,
        'confidence': 'none',
        'reason': 'No identity semantic patterns found'
    }
```

#### 2. Updated PK Suitability Check
```python
def _is_suitable_for_primary_key(self, col_name: str, df: pd.DataFrame) -> Dict[str, Any]:
    """
    STEP 1: Check identity semantics FIRST
    """
    identity_check = self._has_identity_semantics(col_name)
    
    if not identity_check['has_identity']:
        return {
            'is_suitable': False,
            'reason': 'No identity semantics - not a valid PK candidate'
        }
    
    """
    STEP 2: Check blacklist
    High-confidence identity columns bypass blacklist
    (e.g., 'product_code' is valid despite containing 'product')
    """
    if identity_check['confidence'] != 'high':
        for pattern in self.PK_BLACKLIST_PATTERNS:
            if pattern in col_lower:
                return {
                    'is_suitable': False,
                    'reason': f'Blacklisted attribute type: {pattern}'
                }
    
    # STEP 3 & 4: Verify uniqueness and check nulls
    # ... (validation only, not qualification)
```

#### 3. Updated Candidate Key Detection
```python
# In auto_profiler.py find_candidate_keys()
identity_patterns = [
    '_id', '_key', '_code', '_ref', '_number', 
    'sys_id', 'uuid', 'guid'
]

# Single column candidates
for col in df.columns:
    col_lower = col.lower()
    
    # MUST have identity semantics
    has_identity = any(pattern in col_lower for pattern in identity_patterns)
    if not has_identity:
        # Check word boundaries
        parts = col_lower.split('_')
        has_identity = any(p in ['id', 'key', 'code', 'ref', 'number'] for p in parts)
    
    # Only consider if has identity
    if has_identity:
        if df[col].nunique() == len(df) and df[col].notna().all():
            candidate_keys.append((col,))
```

## Test Results

### Test 1: Identity Column vs Non-Identity Unique Columns
**Data:**
- `customer_id`: [1, 2, 3] ✓ Identity
- `city`: ['NYC', 'LA', 'CHI'] ✗ Non-identity (unique)
- `email`: ['a@b.com', 'c@d.com', 'e@f.com'] ✗ Non-identity (unique)

**Result:** ✅ Selected `customer_id`
**Reason:** "Identity column (high confidence), unique, non-null"

### Test 2: No Identity Columns (Surrogate Key Generation)
**Data:**
- `city`: ['NYC', 'LA', 'CHI']
- `salary`: [50000, 60000, 70000]
- `loyalty_points`: [100, 200, 300]

**Result:** ✅ Generated surrogate key `test2_id`
**Reason:** "No identity column found - generated surrogate key"

### Test 3: Identity Column vs Temporal Unique Column
**Data:**
- `order_id`: [1001, 1002, 1003] ✓ Identity
- `order_date`: ['2024-01-01', '2024-01-02', '2024-01-03'] ✗ Temporal
- `amount`: [99.99, 149.99, 199.99] ✗ Monetary

**Result:** ✅ Selected `order_id`

### Test 4: Composite Name with Identity Pattern
**Data:**
- `product_code`: ['P001', 'P002', 'P003'] ✓ Identity (has `_code`)
- `product_name`: ['Widget', 'Gadget', 'Thingamajig'] ✗ Descriptive
- `price`: [9.99, 19.99, 29.99] ✗ Monetary

**Result:** ✅ Selected `product_code`
**Reason:** High-confidence identity pattern bypasses 'product' blacklist

### Test 5: Identity Semantics Detection
| Column | Identity? | Confidence | Reason |
|--------|-----------|------------|--------|
| customer_id | ✓ | high | Contains pattern: _id |
| employee_key | ✓ | high | Contains pattern: _key |
| product_code | ✓ | high | Contains pattern: _code |
| order_ref | ✓ | high | Contains pattern: _ref |
| account_number | ✓ | high | Contains pattern: _number |
| city | ✗ | none | No identity patterns found |
| email | ✗ | none | No identity patterns found |
| salary | ✗ | none | No identity patterns found |
| loyalty_points | ✗ | none | No identity patterns found |
| order_date | ✗ | none | No identity patterns found |

### Full System Test
**Input:** 5 files (customers, employees, orders, order_items, products)

**Primary Keys Detected:**
- ✅ `customers.customer_id` (identity pattern: _id)
- ✅ `employees.employee_id` (identity pattern: _id)
- ✅ `orders.order_id` (identity pattern: _id)
- ✅ `order_items.item_id` (identity pattern: _id)
- ✅ `products.product_id` (identity pattern: _id)

**Surrogate Keys Generated:**
- ✅ `employees_skills_id` (no identity columns in exploded multivalued table)
- ✅ `orders_shipping_address_id` (no identity columns in exploded multivalued table)
- ✅ `products_tags_id` (no identity columns in exploded multivalued table)

**Non-Identity Unique Columns REJECTED:**
- ✗ `city` (descriptive location)
- ✗ `loyalty_points` (numeric transactional)
- ✗ `salary` (numeric transactional)
- ✗ `email` (contact information)
- ✗ `phone` (contact information)

## Key Benefits

1. **Semantic Correctness:** PKs now represent true entity identity, not statistical accidents
2. **Prevents Data Modeling Errors:** No more using `city`, `email`, `salary` as PKs
3. **Future-Proof:** Works for any dataset without domain-specific rules
4. **Clear Failure Mode:** System generates surrogate key when no identity columns exist
5. **Composite Key Support:** At least one column in composite PK must have identity semantics

## Design Principles

### ✅ DO
- Require identity patterns: `_id`, `_key`, `_code`, `_ref`, `_number`
- Check identity BEFORE checking uniqueness
- Generate surrogate keys when no identity columns exist
- Allow high-confidence identity columns to bypass blacklist

### ❌ DON'T
- Use uniqueness as the primary qualification for PK selection
- Select descriptive attributes (name, title, description) as PKs
- Select transactional attributes (price, amount, quantity) as PKs
- Select categorical attributes (status, type, category) as PKs
- Select location attributes (city, state, country) as PKs

## Files Modified

1. **normalizer.py**
   - Added `_has_identity_semantics()` method
   - Updated `_is_suitable_for_primary_key()` with identity-first logic
   - Updated `_determine_best_primary_key()` to skip non-identity columns

2. **auto_profiler.py**
   - Updated `find_candidate_keys()` to require identity semantics
   - Added identity pattern checks for single and composite keys
   - Expanded blacklist to include more non-identity attributes

## Impact

This enhancement addresses the critical requirement: **"Uniqueness alone is not enough to qualify a PK candidate. PK detection must prioritize semantic identity over statistical uniqueness."**

The system now correctly distinguishes between:
- **Identity columns** (customer_id, product_code, order_ref) → Valid PK candidates
- **Unique non-identity columns** (city, email, loyalty_points) → Not PK candidates

This ensures the generated data models follow proper database design principles and represent true entity relationships, not accidental data distributions.
