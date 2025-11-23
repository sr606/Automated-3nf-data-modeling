"""
Metadata Extractor Module
Extracts comprehensive metadata from CSV/JSON files including:
- Column names and datatypes
- Uniqueness profiles
- Null ratios
- Cardinality
- Value distributions
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple
import re


class MetadataExtractor:
    """Extract metadata from data files"""
    
    def __init__(self):
        self.metadata = {}
        
    def load_file(self, file_path: str) -> pd.DataFrame:
        """Load CSV or JSON file into DataFrame"""
        file_path = Path(file_path)
        
        try:
            if file_path.suffix.lower() == '.csv':
                # Try different encodings
                for encoding in ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    df = pd.read_csv(file_path)
                    
            elif file_path.suffix.lower() == '.json':
                df = pd.read_json(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_path.suffix}")
                
            return df
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            return None
    
    def infer_datatype(self, series: pd.Series) -> str:
        """Infer the most appropriate Oracle datatype"""
        # Handle empty or all-null series
        if series.isna().all():
            return "VARCHAR2(4000)"
        
        # Drop nulls for type inference
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            return "VARCHAR2(4000)"
        
        # Check if numeric
        if pd.api.types.is_numeric_dtype(series):
            if pd.api.types.is_integer_dtype(series):
                max_val = series.max()
                if pd.notna(max_val):
                    if max_val <= 2147483647:
                        return "NUMBER(10)"
                    else:
                        return "NUMBER(19)"
                return "NUMBER(10)"
            else:
                return "NUMBER(15,2)"
        
        # Check if datetime
        if pd.api.types.is_datetime64_any_dtype(series):
            return "TIMESTAMP"
        
        # Try to parse as date
        try:
            pd.to_datetime(non_null_series.head(100), errors='raise')
            return "DATE"
        except:
            pass
        
        # Check if boolean
        unique_values = set(str(v).lower() for v in non_null_series.unique())
        if unique_values.issubset({'true', 'false', '0', '1', 't', 'f', 'yes', 'no', 'y', 'n'}):
            return "CHAR(1)"
        
        # Default to VARCHAR2 with appropriate length
        max_length = series.astype(str).str.len().max()
        if pd.isna(max_length) or max_length < 1:
            max_length = 255
        else:
            max_length = min(int(max_length * 1.5), 4000)  # Add 50% buffer, max 4000
        
        return f"VARCHAR2({max_length})"
    
    def detect_multivalued(self, series: pd.Series) -> bool:
        """Detect if column contains multivalued data (violates 1NF)"""
        if series.isna().all():
            return False
        
        sample = series.dropna().head(100).astype(str)
        
        # Check for common delimiters
        delimiters = [',', ';', '|', '/', ':', '\n', '\t']
        for delimiter in delimiters:
            if sample.str.contains(f'\\{delimiter}', regex=True, na=False).sum() > len(sample) * 0.3:
                return True
        
        # Check for array-like patterns
        if sample.str.contains(r'[\[\{].*[\]\}]', regex=True, na=False).sum() > len(sample) * 0.3:
            return True
        
        return False
    
    def detect_structured_field(self, series: pd.Series, col_name: str) -> Dict[str, Any]:
        """Detect if column contains structured data (address, JSON, concatenated fields)"""
        result = {
            'is_structured': False,
            'structure_type': None,
            'detected_components': []
        }
        
        if series.isna().all():
            return result
        
        sample = series.dropna().head(50).astype(str)
        
        # Address detection - look for common address patterns
        address_indicators = ['address', 'addr', 'location', 'street']
        has_address_name = any(ind in col_name.lower() for ind in address_indicators)
        
        if has_address_name:
            # Check for typical address components
            street_pattern = r'\d+\s+[A-Za-z]+'  # Number followed by street name
            city_pattern = r',\s*[A-Z][a-z]+'
            state_pattern = r'\b[A-Z]{2}\b'  # Two-letter state codes
            zip_pattern = r'\b\d{5}(-\d{4})?\b'
            
            has_street = sample.str.contains(street_pattern, regex=True, na=False).sum() > len(sample) * 0.5
            has_city = sample.str.contains(city_pattern, regex=True, na=False).sum() > len(sample) * 0.3
            has_state = sample.str.contains(state_pattern, regex=True, na=False).sum() > len(sample) * 0.3
            has_zip = sample.str.contains(zip_pattern, regex=True, na=False).sum() > len(sample) * 0.3
            
            if has_street or (has_city and (has_state or has_zip)):
                result['is_structured'] = True
                result['structure_type'] = 'address'
                components = []
                if has_street: components.append('street')
                if has_city: components.append('city')
                if has_state: components.append('state')
                if has_zip: components.append('zip_code')
                result['detected_components'] = components
                return result
        
        # JSON detection
        try:
            json_count = 0
            for val in sample.head(10):
                try:
                    json.loads(val)
                    json_count += 1
                except:
                    pass
            
            if json_count > len(sample.head(10)) * 0.7:
                result['is_structured'] = True
                result['structure_type'] = 'json'
                # Try to detect JSON keys from first valid entry
                for val in sample:
                    try:
                        obj = json.loads(val)
                        if isinstance(obj, dict):
                            result['detected_components'] = list(obj.keys())
                            break
                    except:
                        pass
                return result
        except:
            pass
        
        # Concatenated name detection (FirstName LastName in one field)
        name_indicators = ['name', 'fullname', 'full_name']
        has_name_indicator = any(ind in col_name.lower() for ind in name_indicators)
        
        if has_name_indicator:
            # Check for space-separated values that look like names
            space_count = sample.str.count(' ').mean()
            if space_count >= 1 and space_count <= 3:  # Typical for first + last or first + middle + last
                result['is_structured'] = True
                result['structure_type'] = 'full_name'
                result['detected_components'] = ['first_name', 'last_name']
                if space_count > 1.5:
                    result['detected_components'].insert(1, 'middle_name')
                return result
        
        return result
    
    def extract_column_metadata(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Extract detailed metadata for each column"""
        columns_meta = {}
        
        for col in df.columns:
            series = df[col]
            total_count = len(series)
            null_count = series.isna().sum()
            non_null_count = total_count - null_count
            
            # Basic statistics
            unique_count = series.nunique()
            uniqueness_ratio = unique_count / total_count if total_count > 0 else 0
            null_ratio = null_count / total_count if total_count > 0 else 0
            
            # Infer datatype
            datatype = self.infer_datatype(series)
            
            # Detect multivalued
            is_multivalued = self.detect_multivalued(series)
            
            # Detect structured fields
            structured_info = self.detect_structured_field(series, col)
            
            # Get sample values
            sample_values = series.dropna().head(5).tolist()
            
            # Cardinality classification
            if uniqueness_ratio > 0.95:
                cardinality = "high"  # Potential PK
            elif uniqueness_ratio > 0.5:
                cardinality = "medium"
            elif unique_count < 50:
                cardinality = "low"  # Potential categorical
            else:
                cardinality = "medium"
            
            columns_meta[col] = {
                'datatype': datatype,
                'null_count': int(null_count),
                'null_ratio': float(null_ratio),
                'unique_count': int(unique_count),
                'uniqueness_ratio': float(uniqueness_ratio),
                'cardinality': cardinality,
                'is_multivalued': is_multivalued,
                'structured_info': structured_info,
                'sample_values': [str(v) for v in sample_values],
                'total_count': int(total_count),
                'non_null_count': int(non_null_count)
            }
        
        return columns_meta
    
    def extract_table_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract complete metadata for a table"""
        df = self.load_file(file_path)
        
        if df is None:
            return None
        
        file_path = Path(file_path)
        table_name = file_path.stem
        
        # Extract column metadata
        columns_meta = self.extract_column_metadata(df, table_name)
        
        # Table-level metadata
        table_meta = {
            'table_name': table_name,
            'source_file': str(file_path),
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': columns_meta,
            'dataframe': df  # Store for later processing
        }
        
        return table_meta
    
    def extract_all_metadata(self, input_folder: str) -> Dict[str, Any]:
        """Extract metadata from all files in folder"""
        input_path = Path(input_folder)
        
        all_metadata = {}
        
        # Find all CSV and JSON files
        file_patterns = ['*.csv', '*.json']
        files = []
        for pattern in file_patterns:
            files.extend(input_path.glob(pattern))
        
        print(f"Found {len(files)} files to process")
        
        for file_path in files:
            print(f"Extracting metadata from: {file_path.name}")
            metadata = self.extract_table_metadata(str(file_path))
            
            if metadata:
                table_name = metadata['table_name']
                all_metadata[table_name] = metadata
        
        self.metadata = all_metadata
        return all_metadata
    
    def get_metadata_summary(self) -> str:
        """Get a summary of extracted metadata"""
        if not self.metadata:
            return "No metadata extracted yet"
        
        summary = []
        summary.append(f"Total tables: {len(self.metadata)}")
        summary.append("\nTable summaries:")
        
        for table_name, meta in self.metadata.items():
            summary.append(f"\n  {table_name}:")
            summary.append(f"    Rows: {meta['row_count']}")
            summary.append(f"    Columns: {meta['column_count']}")
            
            # Count potential issues
            multivalued = sum(1 for col in meta['columns'].values() if col['is_multivalued'])
            if multivalued > 0:
                summary.append(f"    ⚠ Multivalued columns: {multivalued} (violates 1NF)")
        
        return "\n".join(summary)


if __name__ == "__main__":
    # Test the extractor
    extractor = MetadataExtractor()
    metadata = extractor.extract_all_metadata("./input_files")
    print(extractor.get_metadata_summary())
