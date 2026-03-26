import pandas as pd
import io
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
import fitz  # PyMuPDF
from vertexai.generative_models import Part
from agents import infer_document_metadata, generate_data_summary, infer_document_metadata_vision
# ── Supported structured extensions ────────────────────────
_STRUCTURED_EXTENSIONS = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".json": "json",
}

# ── Name-based keyword patterns for semantic type detection ──
_ID_PATTERNS = re.compile(
    r"(^id$|_id$|^id_|ID$|_ID$|_key$|_code$|number$|_num$|_no$|^key_|^pk_|^fk_|^ref_)",
    re.IGNORECASE,
)
_DATE_PATTERNS = re.compile(
    r"(date|time|timestamp|_dt$|_ts$|created|updated|modified|period|year|month|day|quarter|fiscal)",
    re.IGNORECASE,
)
_AMOUNT_PATTERNS = re.compile(
    r"(amount|price|cost|revenue|total|sum|fee|tax|discount|salary|wage|balance|bill|spend|budget|payment|charge|rate)",
    re.IGNORECASE,
)
_NAME_PATTERNS = re.compile(
    r"(name|title|label|description|desc$|comment|note|address|email|phone|city|state|country|street)",
    re.IGNORECASE,
)
_CATEGORY_PATTERNS = re.compile(
    r"(category|type|status|level|tier|group|class|segment|region|department|channel|mode|method|kind|gender|brand|vendor|supplier|product)",
    re.IGNORECASE,
)
_QUANTITY_PATTERNS = re.compile(
    r"(quantity|qty|count|units|volume|weight|size|length|width|height|stock|inventory)",
    re.IGNORECASE,
)
_PERCENTAGE_PATTERNS = re.compile(
    r"(percent|pct|ratio|rate|margin|share|proportion|_%$)",
    re.IGNORECASE,
)
_BOOLEAN_PATTERNS = re.compile(
    r"(^is_|^has_|^can_|^flag|^active|^enabled|^deleted|^verified|^approved)",
    re.IGNORECASE,
)


def _infer_column_type_local(col_name: str, series: pd.Series, samples: list) -> dict:
    """Deterministic column type inference using pandas dtype + column name patterns + value analysis.

    Returns the same schema as the old LLM-based infer_column_metadata:
    {"predicted_type": ..., "predicted_description": ..., "confidence": ...}
    """
    dtype = series.dtype
    dtype_str = str(dtype)
    n_unique = series.nunique()
    n_total = series.dropna().shape[0]
    unique_ratio = n_unique / n_total if n_total > 0 else 0.0

    # ── 1. Boolean detection (dtype or name) ──
    if dtype == "bool" or _BOOLEAN_PATTERNS.search(col_name):
        if dtype == "bool":
            return {"predicted_type": "boolean", "predicted_description": f"Boolean flag: {col_name}", "confidence": 0.95}
        # Name-based boolean, check if values look boolean
        unique_vals = set(str(v).lower().strip() for v in samples)
        bool_vals = {"true", "false", "yes", "no", "1", "0", "y", "n", "t", "f"}
        if unique_vals.issubset(bool_vals):
            return {"predicted_type": "boolean", "predicted_description": f"Boolean flag: {col_name}", "confidence": 0.90}

    # ── 2. Date/time detection ──
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return {"predicted_type": "date", "predicted_description": f"Timestamp/date field: {col_name}", "confidence": 0.95}
    if _DATE_PATTERNS.search(col_name):
        # Try parsing samples as dates
        parsed = 0
        for s in samples[:6]:
            try:
                pd.to_datetime(s)
                parsed += 1
            except (ValueError, TypeError):
                pass
        if parsed >= len(samples) * 0.5:
            return {"predicted_type": "date", "predicted_description": f"Date/time field: {col_name}", "confidence": 0.85}
        # Name matches but values don't parse — still likely date-related
        return {"predicted_type": "date", "predicted_description": f"Likely date/time field: {col_name}", "confidence": 0.65}

    # ── 3. ID detection ──
    # Only classify as ID if the name matches ID patterns AND does NOT also match
    # a value-type pattern (amount, quantity, percentage).  This prevents columns
    # like "AmountPaid" (ends with "ID"-like suffix) from being misclassified.
    if _ID_PATTERNS.search(col_name):
        is_also_amount = _AMOUNT_PATTERNS.search(col_name)
        is_also_quantity = _QUANTITY_PATTERNS.search(col_name)
        is_also_percentage = _PERCENTAGE_PATTERNS.search(col_name)
        if not (is_also_amount or is_also_quantity or is_also_percentage):
            conf = 0.90 if unique_ratio > 0.8 else 0.75
            return {"predicted_type": "id", "predicted_description": f"Identifier/key field: {col_name}", "confidence": conf}
        # If it matches both ID and a value pattern, fall through to numeric/string checks
        # which will classify it correctly based on dtype and the value pattern.

    # ── 4. Numeric types ──
    if pd.api.types.is_numeric_dtype(dtype):
        # Percentage
        if _PERCENTAGE_PATTERNS.search(col_name):
            return {"predicted_type": "percentage", "predicted_description": f"Percentage/ratio metric: {col_name}", "confidence": 0.85}
        # Amount/currency
        if _AMOUNT_PATTERNS.search(col_name):
            return {"predicted_type": "amount", "predicted_description": f"Monetary/amount value: {col_name}", "confidence": 0.90}
        # Quantity
        if _QUANTITY_PATTERNS.search(col_name):
            return {"predicted_type": "quantity", "predicted_description": f"Quantity/count metric: {col_name}", "confidence": 0.85}
        # Low cardinality numeric = possibly categorical (encoded)
        if n_unique <= 10 and n_total > 20:
            return {"predicted_type": "category", "predicted_description": f"Low-cardinality numeric (possibly encoded category): {col_name}", "confidence": 0.60}
        # Generic numeric
        return {"predicted_type": "numeric", "predicted_description": f"Numeric metric: {col_name}", "confidence": 0.75}

    # ── 5. String/object types ──
    if dtype == "object" or pd.api.types.is_string_dtype(dtype):
        # Check if values look like dates even though dtype is string
        parsed_dates = 0
        for s in samples[:6]:
            try:
                pd.to_datetime(s)
                parsed_dates += 1
            except (ValueError, TypeError):
                pass
        if parsed_dates >= len(samples) * 0.7 and len(samples) >= 2:
            return {"predicted_type": "date", "predicted_description": f"Date stored as text: {col_name}", "confidence": 0.80}

        # Check for currency strings like "$1,200.00"
        currency_count = sum(1 for s in samples if re.match(r"^[\$\u20ac\u00a3\u00a5\u20b9]?\s*[\d,]+\.?\d*$", str(s).strip()))
        if currency_count >= len(samples) * 0.7 and _AMOUNT_PATTERNS.search(col_name):
            return {"predicted_type": "amount", "predicted_description": f"Monetary amount (text format): {col_name}", "confidence": 0.80}

        # Name patterns
        if _NAME_PATTERNS.search(col_name):
            return {"predicted_type": "name", "predicted_description": f"Name/text descriptor: {col_name}", "confidence": 0.85}

        # Category patterns or low cardinality
        if _CATEGORY_PATTERNS.search(col_name):
            return {"predicted_type": "category", "predicted_description": f"Categorical field: {col_name}", "confidence": 0.85}

        if unique_ratio < 0.5 and n_total > 5:
            return {"predicted_type": "category", "predicted_description": f"Low-cardinality text (likely categorical): {col_name}", "confidence": 0.70}

        if unique_ratio > 0.9:
            return {"predicted_type": "name", "predicted_description": f"High-cardinality text field: {col_name}", "confidence": 0.65}

        # Fallback for strings
        return {"predicted_type": "text", "predicted_description": f"Text field: {col_name}", "confidence": 0.50}

    # ── 6. Fallback ──
    return {"predicted_type": "unknown", "predicted_description": f"Could not determine type for: {col_name}", "confidence": 0.0}


def _build_issue_stack(df: pd.DataFrame, columns_metadata: list) -> list:
    """Detect data quality issues deterministically from the sample data.

    Returns a list of issue dicts that can be fed to the LLM summary call
    for richer context-aware summary generation.
    """
    issues = []
    total_rows = len(df)

    for col_meta in columns_metadata:
        col_name = col_meta["name"]
        series = df[col_name] if col_name in df.columns else None
        if series is None:
            continue

        # ── High missing ratio ──
        missing_ratio = col_meta.get("missing_ratio", 0.0)
        if missing_ratio > 0.3:
            issues.append({
                "type": "high_missing_values",
                "column": col_name,
                "severity": "high" if missing_ratio > 0.5 else "medium",
                "detail": f"Column '{col_name}' has {missing_ratio:.0%} missing values in sample",
            })

        # ── Constant column (all same value) ──
        if series.nunique() <= 1 and series.dropna().shape[0] > 0:
            issues.append({
                "type": "constant_column",
                "column": col_name,
                "severity": "low",
                "detail": f"Column '{col_name}' has only 1 unique value — may be useless for analysis",
            })

        # ── Potential duplicates in ID columns ──
        if col_meta.get("predicted_type") == "id":
            dup_count = series.dropna().duplicated().sum()
            if dup_count > 0:
                issues.append({
                    "type": "duplicate_ids",
                    "column": col_name,
                    "severity": "high",
                    "detail": f"ID column '{col_name}' has {dup_count} duplicate values in sample — may not be a true primary key",
                })

        # ── Mixed types in object columns ──
        if series.dtype == "object":
            non_null = series.dropna()
            if len(non_null) > 0:
                numeric_count = sum(1 for v in non_null if _is_numeric_string(str(v)))
                if 0 < numeric_count < len(non_null):
                    issues.append({
                        "type": "mixed_types",
                        "column": col_name,
                        "severity": "medium",
                        "detail": f"Column '{col_name}' has mixed numeric/text values ({numeric_count}/{len(non_null)} numeric)",
                    })

        # ── Negative values in amount/quantity columns ──
        if col_meta.get("predicted_type") in ("amount", "quantity") and pd.api.types.is_numeric_dtype(series):
            neg_count = (series.dropna() < 0).sum()
            if neg_count > 0:
                issues.append({
                    "type": "negative_values",
                    "column": col_name,
                    "severity": "medium",
                    "detail": f"Column '{col_name}' has {neg_count} negative values — may indicate returns/adjustments or data error",
                })

        # ── Outlier detection for numeric columns ──
        if pd.api.types.is_numeric_dtype(series) and series.dropna().shape[0] >= 5:
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            if iqr > 0:
                outliers = ((series < q1 - 3 * iqr) | (series > q3 + 3 * iqr)).sum()
                if outliers > 0:
                    issues.append({
                        "type": "outliers",
                        "column": col_name,
                        "severity": "low",
                        "detail": f"Column '{col_name}' has {outliers} extreme outliers (beyond 3x IQR) in sample",
                    })

    # ── Dataset-level issues ──

    # Multiple date columns
    date_cols = [m["name"] for m in columns_metadata if m.get("predicted_type") == "date"]
    if len(date_cols) > 1:
        issues.append({
            "type": "multiple_date_columns",
            "column": ", ".join(date_cols),
            "severity": "medium",
            "detail": f"Multiple date columns detected ({', '.join(date_cols)}) — unclear which is primary time dimension",
        })

    # Multiple amount columns
    amount_cols = [m["name"] for m in columns_metadata if m.get("predicted_type") in ("amount", "numeric")]
    if len(amount_cols) > 1:
        issues.append({
            "type": "multiple_amount_columns",
            "column": ", ".join(amount_cols),
            "severity": "low",
            "detail": f"Multiple numeric/amount columns detected ({', '.join(amount_cols)}) — may need disambiguation",
        })

    # Multiple ID columns
    id_cols = [m["name"] for m in columns_metadata if m.get("predicted_type") == "id"]
    if len(id_cols) > 1:
        issues.append({
            "type": "multiple_id_columns",
            "column": ", ".join(id_cols),
            "severity": "medium",
            "detail": f"Multiple ID columns detected ({', '.join(id_cols)}) — unclear which is primary key vs foreign key",
        })

    # No clear primary key
    if not id_cols:
        issues.append({
            "type": "no_primary_key",
            "column": "",
            "severity": "low",
            "detail": "No ID/key column detected — may need to identify a unique row identifier",
        })

    return issues


def _is_numeric_string(s: str) -> bool:
    """Check if a string looks like a number."""
    try:
        float(s.replace(",", "").replace("$", "").replace("€", "").replace("£", ""))
        return True
    except (ValueError, AttributeError):
        return False


def extract_structured_metadata(file_path: str) -> dict:
    """Single entry point for all structured data metadata extraction.

    Handles CSV, Excel, and JSON files. Never loads the full dataset —
    only reads headers + a small sample (up to 10 rows) for inference.

    This function is stateless and safe to run concurrently across files.

    Returns:
        {
            "columns": [...],            # per-column metadata (PRD spec)
            "data_summary": { ... },     # holistic dataset context profile
            "source_type": "csv"|"excel"|"json",
            "column_count": 5,
            "sample_row_count": 10,
            "edge_cases": { ... }        # detected quality signals for downstream
        }
    """
    ext = os.path.splitext(file_path)[1].lower()
    source_type = _STRUCTURED_EXTENSIONS.get(ext)

    if not source_type:
        raise ValueError(
            f"Unsupported file type '{ext}'. "
            f"Supported: {list(_STRUCTURED_EXTENSIONS.keys())}"
        )

    # Step 1: Sample the file — never full load
    df = _sample_file(file_path, source_type)

    # Step 2: Handle edge case — empty or unreadable file
    if df is None:
        return {
            "columns": [],
            "data_summary": {},
            "source_type": source_type,
            "column_count": 0,
            "sample_row_count": 0,
            "edge_cases": {"is_empty": True, "read_error": True, "has_headers": False},
        }

    if df.empty or len(df.columns) == 0:
        return {
            "columns": [],
            "data_summary": {},
            "source_type": source_type,
            "column_count": 0,
            "sample_row_count": 0,
            "edge_cases": {"is_empty": True, "read_error": False, "has_headers": len(df.columns) > 0},
        }

    # Step 3: Extract per-column metadata with stats + deterministic inference (NO LLM)
    columns_metadata = _extract_column_metadata(df)

    # Step 4: Build issue_stack — deterministic data quality analysis
    issue_stack = _build_issue_stack(df, columns_metadata)
    if issue_stack:
        print(f"[STAGE] METADATA | [VALIDATION] Detected {len(issue_stack)} data quality issues")
        for iss in issue_stack:
            print(f"[STAGE] METADATA | [VALIDATION]   {iss['severity'].upper()}: {iss['detail']}")

    # Step 5: Generate holistic dataset context profile (audit perimeter) — SINGLE LLM CALL
    # Now enriched with column metadata + issue_stack for better context
    column_headers = list(df.columns)
    sample_rows = df.head(5).to_dict(orient="records")
    data_types = {col: str(dtype) for col, dtype in df.dtypes.items()}

    # Build column metadata summary for the LLM
    column_metadata_summary = [
        {
            "name": cm["name"],
            "predicted_type": cm["predicted_type"],
            "predicted_description": cm["predicted_description"],
            "confidence": cm["confidence"],
            "missing_ratio": cm["missing_ratio"],
            "unique_ratio": cm["unique_ratio"],
        }
        for cm in columns_metadata
    ]

    data_summary = generate_data_summary(
        column_headers=column_headers,
        sample_rows=sample_rows,
        data_types=data_types,
        source_type=source_type,
        column_metadata=column_metadata_summary,
        issue_stack=issue_stack,
    )

    # Step 6: Detect edge cases — record only, never fix
    edge_cases = _detect_edge_cases(df, columns_metadata, data_summary)

    return {
        "columns": columns_metadata,
        "data_summary": data_summary,
        "source_type": source_type,
        "column_count": len(df.columns),
        "sample_row_count": len(df),
        "edge_cases": edge_cases,
    }


def _sample_file(file_path: str, source_type: str, max_rows: int = 10) -> pd.DataFrame:
    """Read only headers + first N rows from a file. Never loads full data.

    Handles encoding issues, corrupt rows, and empty files gracefully.
    Returns None if the file cannot be read at all.
    """
    if source_type == "csv":
        return _sample_csv(file_path, max_rows)
    elif source_type == "excel":
        return _sample_excel(file_path, max_rows)
    elif source_type == "json":
        return _sample_json(file_path, max_rows)
    return None


def _sample_csv(file_path: str, max_rows: int) -> pd.DataFrame:
    """Sample CSV with encoding fallback and error resilience.

    Tries utf-8 → cp1252 → latin-1. Uses nrows to avoid full scan.
    on_bad_lines='skip' handles corrupt rows without crashing.
    """
    encodings = ["utf-8", "cp1252", "latin-1"]
    for enc in encodings:
        try:
            return pd.read_csv(
                file_path,
                nrows=max_rows,
                encoding=enc,
                on_bad_lines="skip",
            )
        except UnicodeDecodeError:
            continue
        except pd.errors.EmptyDataError:
            # File exists but is empty or has no parseable content
            return pd.DataFrame()
        except Exception:
            # Catch-all for malformed CSVs — try next encoding
            continue
    # All encodings failed
    return None


def _sample_excel(file_path: str, max_rows: int) -> pd.DataFrame:
    """Sample Excel with nrows. Only reads first sheet."""
    try:
        return pd.read_excel(file_path, nrows=max_rows)
    except Exception:
        # Corrupt or password-protected Excel
        return None


def _sample_json(file_path: str, max_rows: int) -> pd.DataFrame:
    """Sample JSON — handles arrays, single objects, and nested structures.

    For large JSON arrays, reads only the first max_rows records.
    Uses ijson-style incremental reading would be ideal for GB-scale,
    but for the metadata sampling use case, json.load on first N is sufficient
    since we only need a few records.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            # Read first chunk to determine structure without loading entire file
            raw = f.read(10 * 1024 * 1024)  # Read up to 10MB max for sampling
    except (UnicodeDecodeError, OSError):
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # File might be truncated from partial read — try to parse as array
        # by finding the last complete object
        try:
            last_brace = raw.rfind("}")
            if last_brace > 0:
                truncated = raw[:last_brace + 1] + "]"
                if not truncated.startswith("["):
                    truncated = "[" + truncated
                data = json.loads(truncated)
            else:
                return None
        except json.JSONDecodeError:
            return None

    if isinstance(data, list) and len(data) > 0:
        return pd.DataFrame(data[:max_rows])
    elif isinstance(data, dict):
        return pd.json_normalize(data).head(max_rows)
    return pd.DataFrame()


def _extract_column_metadata(df: pd.DataFrame) -> list:
    """Extract per-column metadata: stats from sample + deterministic type inference.

    NO LLM calls — uses pandas dtype, column name patterns, and value analysis.
    """
    total_rows = len(df)
    results = []

    for col in df.columns:
        series = df[col]
        samples = series.dropna().astype(str).tolist()[:6]

        null_count = series.isna().sum()
        missing_ratio = round(null_count / total_rows, 2) if total_rows > 0 else 0.0
        non_null_count = total_rows - null_count
        unique_count = series.nunique()
        unique_ratio = round(unique_count / non_null_count, 2) if non_null_count > 0 else 0.0

        # Deterministic inference — no LLM call
        print(f"[FUNCTION] Entering _infer_column_type_local | column={col}")
        inferred = _infer_column_type_local(col, series, samples)
        print(f"[STAGE] METADATA | [FUNCTION] Inferred type={inferred.get('predicted_type')} for '{col}' (confidence={inferred.get('confidence')})")
        print(f"[FUNCTION] Exiting _infer_column_type_local | column={col}")

        results.append({
            "name": col,
            "samples": samples,
            "predicted_type": inferred.get("predicted_type", "unknown"),
            "predicted_description": inferred.get("predicted_description", ""),
            "confidence": inferred.get("confidence", 0.0),
            "detected_dtype": str(series.dtype),
            "missing_ratio": missing_ratio,
            "unique_ratio": unique_ratio,
            "semantic_info": inferred,
        })

    return results


def _detect_edge_cases(df: pd.DataFrame, columns_metadata: list, data_summary: dict) -> dict:
    """Detect and record edge case signals in metadata.

    This function ONLY observes and records — it never fixes, mutates, or stops the pipeline.
    Downstream engines (Schema Intelligence, Clarification, Planning) use these flags.
    """
    edge_cases = {
        "is_empty": False,
        "read_error": False,
        "has_headers": True,
        "candidate_groups": [],
        "join_risk": False,
        "semantic_conflicts": [],
    }

    # ── Header detection ──────────────────────────────────────
    # If column names look auto-generated (0, 1, 2... or Unnamed), likely no headers
    auto_headers = sum(
        1 for col in df.columns
        if str(col).isdigit() or str(col).startswith("Unnamed")
    )
    if auto_headers > len(df.columns) * 0.5:
        edge_cases["has_headers"] = False

    # ── Candidate groups: columns with similar names/types ────
    # Group by predicted_type to find multiple-candidate situations
    type_groups = {}
    for col_meta in columns_metadata:
        ptype = col_meta.get("predicted_type", "unknown")
        if ptype != "unknown":
            type_groups.setdefault(ptype, []).append(col_meta["name"])

    for ptype, cols in type_groups.items():
        if len(cols) > 1 and ptype in ("date", "datetime", "temporal", "amount", "currency",
                                        "numeric", "id", "identifier", "key"):
            edge_cases["candidate_groups"].append({
                "type": ptype,
                "columns": cols,
                "description": f"Multiple {ptype} columns detected — may need disambiguation",
            })

    # ── Join risk: multiple ID-like columns with high uniqueness ──
    id_columns = [
        col_meta for col_meta in columns_metadata
        if col_meta.get("unique_ratio", 0) > 0.8
        and col_meta.get("predicted_type", "") in ("id", "identifier", "key", "primary_key", "foreign_key")
    ]
    if len(id_columns) >= 2:
        edge_cases["join_risk"] = True

    # ── Semantic conflicts from data_summary ambiguities ──────
    ambiguities = data_summary.get("ambiguities", [])
    if ambiguities:
        for amb in ambiguities:
            edge_cases["semantic_conflicts"].append({
                "type": amb.get("type", "unknown"),
                "columns": amb.get("columns", []),
                "description": amb.get("description", ""),
            })

    return edge_cases


# ── Legacy wrappers (kept for backward compatibility) ──────
# These are used by main.py's _extract_metadata for the upload flow.
# They delegate to the new unified function internally.

def process_csv_stream(sample_stream: io.BytesIO):
    """Read CSV stream (first few rows only) and extract metadata.
    Used when file is already on disk and we have a stream of head bytes.
    """
    try:
        df = pd.read_csv(sample_stream, encoding="utf-8", on_bad_lines="skip")
    except UnicodeDecodeError:
        sample_stream.seek(0)
        df = pd.read_csv(sample_stream, encoding="cp1252", on_bad_lines="skip")
    except pd.errors.EmptyDataError:
        return {"columns": [], "data_summary": {}}

    return _build_result_from_df(df, source_type="csv")


def process_excel_file(file_obj):
    """Read Excel file (first 10 rows only) and extract metadata."""
    try:
        df = pd.read_excel(file_obj, nrows=10)
    except Exception:
        return {"columns": [], "data_summary": {}}
    return _build_result_from_df(df, source_type="excel")


def process_json_file(path: str):
    """Read JSON file (first 10 records) and extract metadata."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return {"columns": [], "data_summary": {}}

    if isinstance(data, list) and len(data) > 0:
        df = pd.DataFrame(data[:10])
    elif isinstance(data, dict):
        df = pd.json_normalize(data).head(10)
    else:
        return {"columns": [], "data_summary": {}}

    return _build_result_from_df(df, source_type="json")


def _build_result_from_df(df: pd.DataFrame, source_type: str) -> dict:
    """Shared logic: extract column metadata + data summary from a sampled DataFrame."""
    if df is None or df.empty:
        return {"columns": [], "data_summary": {}}

    columns_metadata = _extract_column_metadata(df)
    issue_stack = _build_issue_stack(df, columns_metadata)

    column_headers = list(df.columns)
    sample_rows = df.head(5).to_dict(orient="records")
    data_types = {col: str(dtype) for col, dtype in df.dtypes.items()}

    column_metadata_summary = [
        {
            "name": cm["name"],
            "predicted_type": cm["predicted_type"],
            "predicted_description": cm["predicted_description"],
            "confidence": cm["confidence"],
            "missing_ratio": cm["missing_ratio"],
            "unique_ratio": cm["unique_ratio"],
        }
        for cm in columns_metadata
    ]

    data_summary = generate_data_summary(
        column_headers=column_headers,
        sample_rows=sample_rows,
        data_types=data_types,
        source_type=source_type,
        column_metadata=column_metadata_summary,
        issue_stack=issue_stack,
    )

    return {
        "columns": columns_metadata,
        "data_summary": data_summary,
    }


# ── Unstructured data processing (PDF, images) ────────────
# Standardized to return the same top-level schema as structured data:
# { "columns": [...], "data_summary": {...}, "source_type": ..., "edge_cases": {...} }

def _get_sample_page_numbers(pdf_path: str, max_pages: int = 5) -> list:
    """Determine the pages to extract. Takes all if <= 5, otherwise a deterministic sample of 5."""
    import random
    import hashlib
    try:
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        doc.close()

        if total_pages == 0:
            return []

        if total_pages <= max_pages:
            return list(range(total_pages))

        with open(pdf_path, 'rb') as f:
            pdf_hash = hashlib.md5(f.read()).hexdigest()
            seed = int(pdf_hash, 16) % (2**31)

        random.seed(seed)
        return sorted(random.sample(range(total_pages), max_pages))
    except Exception as e:
        print(f"[PDF] Error getting sample pages: {e}")
        return []


def _get_pdf_sample_images(pdf_path: str, sample_pages: list) -> list:
    """Extract selected sample pages from PDF as Gemini Image Parts."""
    try:
        doc = fitz.open(pdf_path)
        parts = []
        for idx in sample_pages:
            page = doc.load_page(idx)
            zoom = 140 / 72.0
            matrix = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            png_bytes = pix.tobytes("png")
            parts.append(Part.from_data(data=png_bytes, mime_type="image/png"))
        doc.close()
        return parts
    except Exception as e:
        print(f"[PDF VISION] Error rendering PDF: {e}")
        return []


def _extract_tables_task(path: str, sample_pages: list) -> list:
    """Worker task: Extracts tables using pdfplumber on selected pages."""
    table_dfs = []
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            for idx in sample_pages:
                if idx < len(pdf.pages):
                    page = pdf.pages[idx]
                    for table in page.extract_tables():
                        if not table or len(table) < 2:
                            continue
                        header = [re.sub(r'\s+', '_', str(h).strip().lower()) if h else f"col_{i}" for i, h in enumerate(table[0])]
                        try:
                            tdf = pd.DataFrame(table[1:], columns=header)
                            tdf.dropna(how="all", inplace=True)
                            tdf = tdf[tdf.apply(lambda r: r.astype(str).str.strip().ne("").any(), axis=1)]
                            if not tdf.empty:
                                table_dfs.append(tdf)
                        except Exception:
                            continue
    except Exception as e:
        print(f"[PDF] Table extraction error: {e}")
    return table_dfs


def _vision_summary_task(path: str, sample_pages: list) -> dict:
    """Worker task: Combines pymupdf4llm text and Gemini Vision on selected pages."""
    print(f"[PDF] Generating document summary via Vision & pymupdf4llm (Pages: {sample_pages})...")

    image_parts = _get_pdf_sample_images(path, sample_pages)

    markdown_text = ""
    try:
        import pymupdf4llm
        markdown_text = pymupdf4llm.to_markdown(path, pages=sample_pages)
    except ImportError:
        print("[PDF] pymupdf4llm not installed. Skipping markdown extraction.")
    except Exception as e:
        print(f"[PDF] pymupdf4llm extraction error: {e}. Falling back to fitz text extraction.")
        try:
            doc = fitz.open(path)
            parts = []
            for idx in sample_pages:
                if idx < len(doc):
                    parts.append(doc.load_page(idx).get_text("text"))
            doc.close()
            markdown_text = "\n\n".join(parts)
        except Exception as fe:
            print(f"[PDF] fitz fallback extraction error: {fe}")

    contents = []
    if markdown_text:
        contents.append(f"--- EXTRACTED TEXT (Pages {sample_pages}) ---\n{markdown_text}\n--- END TEXT ---")
    contents.extend(image_parts)

    if contents:
        return infer_document_metadata_vision(contents)
    return {}


def process_pdf_file(path: str) -> dict:
    """Hybrid PDF ingestion: Concurrent Table Extraction + Vision & Markdown Understanding."""
    edge_cases = {
        "is_empty": False,
        "read_error": False,
        "has_headers": True,  # Set to True to avoid the false alarm UI warning
        "ocr_confidence": None,
    }

    # Centralize sampling: Both tasks will use these exact same pages
    sample_pages = _get_sample_page_numbers(path, max_pages=5)

    if not sample_pages:
        edge_cases["is_empty"] = True
        return {"columns": [], "data_summary": {}, "source_type": "pdf",
                "column_count": 0, "sample_row_count": 0, "edge_cases": edge_cases}

    # ── 1. RUN CONCURRENTLY: Table Extraction & Vision API Call ──
    print(f"[PDF] Starting concurrent extraction for {os.path.basename(path)}")
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_tables = executor.submit(_extract_tables_task, path, sample_pages)
        future_vision = executor.submit(_vision_summary_task, path, sample_pages)

        table_dfs = future_tables.result()
        doc_summary = future_vision.result()

    # ── 2. Process Table Results ──
    final_df = pd.DataFrame()
    if table_dfs:
        col_sets = [frozenset(t.columns) for t in table_dfs]
        if len(set(col_sets)) == 1:
            final_df = pd.concat(table_dfs, ignore_index=True)
        else:
            final_df = max(table_dfs, key=len)

    # ── 3. Process Vision Results ──
    if doc_summary:
        edge_cases["ocr_confidence"] = "high" # Vision models don't rely on OCR text layers
    else:
        edge_cases["is_empty"] = True if final_df.empty else False

    # ── 4. Combine Table Columns with Vision Fields ──
    result = _build_result_from_df(final_df, source_type="pdf") if not final_df.empty else {}
    pdf_columns = result.get("columns", [])

    # Merge Vision-detected unstructured fields into the official schema
    if doc_summary and "detected_fields" in doc_summary:
        existing_col_names = {c["name"].lower() for c in pdf_columns}
        raw_fields = doc_summary["detected_fields"]

        # Flatten: {"header_fields": [...], "line_item_fields": [...]} → flat list with section tag
        if isinstance(raw_fields, dict):
            field_list = []
            for section, items in raw_fields.items():
                if isinstance(items, list):
                    for item in items:
                        field_list.append((item, section))
        elif isinstance(raw_fields, list):
            field_list = [(f, None) for f in raw_fields]
        else:
            field_list = []

        for field_obj, section_tag in field_list:
            if isinstance(field_obj, dict):
                field_name = field_obj.get("name", "")
                ptype = field_obj.get("type", "unknown")
                desc = field_obj.get("description", "")
                sample = field_obj.get("sample_value", "")
                field_conf = field_obj.get("confidence", doc_summary.get("confidence", 0.8))
            else:
                field_name = str(field_obj).strip()
                ptype = "unknown"
                desc = ""
                sample = ""
                field_conf = doc_summary.get("confidence", 0.8)

            if not desc:
                section_label = section_tag.replace("_", " ").title() if section_tag else "Vision API"
                desc = f"Detected via {section_label}"

            samples = [sample] if sample else ["(Extracted via Vision)"]

            if field_name and field_name.lower() not in existing_col_names:
                pdf_columns.append({
                    "name": field_name,
                    "samples": samples,
                    "predicted_type": ptype,
                    "predicted_description": desc,
                    "confidence": field_conf,
                    "detected_dtype": "object",
                    "missing_ratio": 0.0,
                    "unique_ratio": 1.0,
                    "semantic_info": {"predicted_type": ptype}
                })

    return {
        "columns": pdf_columns,
        "data_summary": doc_summary,  
        "source_type": "pdf",
        "column_count": len(pdf_columns),
        "sample_row_count": len(final_df) if not final_df.empty else 0,
        "edge_cases": edge_cases,
    }
def process_sql_source(connection_config: dict) -> dict:
    """Fetch metadata from a SQL database connection.

    Returns STANDARDIZED format. For multi-table sources, columns from all tables
    are flattened with table prefix (table.column) for downstream compatibility.

    Also returns raw `tables` list for callers that need per-table structure.
    """
    import sqlalchemy

    edge_cases = {
        "is_empty": False,
        "read_error": False,
        "has_headers": True,  # SQL always has schema
        "join_risk": False,
    }

    try:
        engine = sqlalchemy.create_engine(connection_config["connection_string"])
        inspector = sqlalchemy.inspect(engine)
    except Exception:
        edge_cases["read_error"] = True
        return {
            "columns": [],
            "data_summary": {},
            "source_type": "sql",
            "column_count": 0,
            "sample_row_count": 0,
            "edge_cases": edge_cases,
            "tables": [],
        }

    table_names = inspector.get_table_names(
        schema=connection_config.get("schema", None)
    )

    if not table_names:
        engine.dispose()
        edge_cases["is_empty"] = True
        return {
            "columns": [],
            "data_summary": {},
            "source_type": "sql",
            "column_count": 0,
            "sample_row_count": 0,
            "edge_cases": edge_cases,
            "tables": [],
        }

    tables_metadata = []
    all_columns = []

    for table_name in table_names:
        table_cols = []
        for col in inspector.get_columns(table_name):
            col_meta = {
                "name": col["name"],
                "samples": [],
                "predicted_type": _sql_type_to_semantic(str(col["type"])),
                "predicted_description": col.get("comment", "") or f"Column from table {table_name}",
                "confidence": 0.7,  # Schema-derived, not sample-inferred
                "detected_dtype": str(col["type"]),
                "missing_ratio": 0.0,  # Unknown without sampling
                "unique_ratio": 0.0,
                "semantic_info": {
                    "source_table": table_name,
                    "nullable": col.get("nullable", True),
                    "sql_type": str(col["type"]),
                },
            }
            table_cols.append(col_meta)
            all_columns.append(col_meta)

        tables_metadata.append({
            "table": table_name,
            "columns": table_cols,
        })

    engine.dispose()

    # Detect join risk: multiple tables = potential joins
    if len(table_names) > 1:
        edge_cases["join_risk"] = True

    return {
        "columns": all_columns,
        "data_summary": {},
        "source_type": "sql",
        "column_count": len(all_columns),
        "sample_row_count": 0,
        "edge_cases": edge_cases,
        "tables": tables_metadata,
    }



def preextract_pdf_structured(pdf_path: str, output_dir: str):
    """Extract ALL pages from a PDF into a single analysis-ready DataFrame.

    Each row = one table row from a page, enriched with that page's header/footer KV fields.
    Pages with no table contribute one summary row (KV fields only, table cols blank).

    Columns in the output (in order):
      - page          : page number (1-indexed) — links every row back to its source page
      - [kv_fields]   : Key-Value fields discovered in page text (e.g. invoice_number,
                        vendor, date, subtotal, gst_18_percent, total).
                        Normalised to snake_case. All accessible as df["col"].
      - [table_cols]  : Table column names from pdfplumber (e.g. Item, Qty, Unit Price, Total).
                        Union of all column names seen across all pages.

    Key benefit: header fields (invoice_number, vendor, subtotal, GST, total) are REAL
    DataFrame columns, not locked in raw text. Code can do df["invoice_number"] directly.

    Returns:
        (csv_path: str, df: pd.DataFrame)  on success
        (None, None)                        if nothing could be extracted
    """
    import pdfplumber

    page_records = []     # list of (page_num, kv_dict, table_rows_list)
    all_kv_keys: list = []   # ordered union — first seen wins order
    kv_key_set: set = set()
    all_table_cols: list = []  # ordered union of table column names
    table_col_set: set = set()

    # ── Process every page ──────────────────────────────────────────────────
    try:
        # Open fitz once for the fallback text extraction across all pages
        _fitz_doc = None
        try:
            _fitz_doc = fitz.open(pdf_path)
        except Exception:
            pass

        with pdfplumber.open(pdf_path) as pdf:
            n_pages = len(pdf.pages)
            for page_idx, page in enumerate(pdf.pages):
                page_num = page_idx + 1

                # ── Text extraction (pdfplumber first, fitz fallback) ────────
                raw_text = ""
                try:
                    raw_text = page.extract_text() or ""
                except Exception:
                    pass
                if not raw_text.strip() and _fitz_doc is not None:
                    try:
                        raw_text = _fitz_doc.load_page(page_idx).get_text("text") or ""
                    except Exception:
                        pass

                # ── Key-Value extraction from page text ──────────────────────
                # Matches lines like:  "INVOICE #INV-2026-001"  (header line)
                #                      "Vendor: ABC Traders Pvt Ltd"
                #                      "GST (18%): 4244"
                kv: dict = {}
                for line in raw_text.splitlines():
                    line = line.strip()
                    m = re.match(
                        r'^([A-Za-z][A-Za-z0-9 #\(\)\/\-_\.%]*?)\s*:\s*(.+)$', line
                    )
                    if m:
                        raw_key = m.group(1).strip()
                        val = m.group(2).strip()
                        col = re.sub(r'[^a-zA-Z0-9]+', '_', raw_key).strip('_').lower()
                        if col and val and len(col) <= 40:
                            kv[col] = val
                            if col not in kv_key_set:
                                kv_key_set.add(col)
                                all_kv_keys.append(col)

                # ── Table extraction ─────────────────────────────────────────
                table_rows: list = []
                try:
                    for raw_table in (page.extract_tables() or []):
                        if not raw_table or len(raw_table) < 2:
                            continue
                        headers = [
                            str(h).strip() if h else f"col_{i}"
                            for i, h in enumerate(raw_table[0])
                        ]
                        for h in headers:
                            if h not in table_col_set:
                                table_col_set.add(h)
                                all_table_cols.append(h)
                        for raw_row in raw_table[1:]:
                            row_dict = {
                                headers[i]: (str(c).strip() if c is not None else "")
                                for i, c in enumerate(raw_row)
                                if i < len(headers)
                            }
                            if any(v for v in row_dict.values()):
                                table_rows.append(row_dict)
                except Exception as te:
                    print(f"[PDF STRUCTURED] Table page {page_num}: {te}")

                page_records.append((page_num, kv, table_rows))

        if _fitz_doc is not None:
            try:
                _fitz_doc.close()
            except Exception:
                pass

    except Exception as e:
        if _fitz_doc is not None:
            try:
                _fitz_doc.close()
            except Exception:
                pass
        print(f"[PDF STRUCTURED] Extraction failed: {e}")
        return None, None

    if not page_records:
        print(f"[PDF STRUCTURED] No pages extracted from {pdf_path}")
        return None, None

    # ── Build merged rows ────────────────────────────────────────────────────
    # Each table row gets the KV metadata from its page prepended.
    # Pages with no table contribute one summary row with empty table cols.
    final_rows = []
    for page_num, kv, table_rows in page_records:
        # Base row: page number + all KV fields (blank if not on this page)
        base = {"page": page_num}
        for k in all_kv_keys:
            base[k] = kv.get(k, "")

        if table_rows:
            for trow in table_rows:
                row = dict(base)
                for c in all_table_cols:
                    row[c] = trow.get(c, "")
                final_rows.append(row)
        else:
            # Page with only header/footer text — add one summary row
            row = dict(base)
            for c in all_table_cols:
                row[c] = ""
            final_rows.append(row)

    df = pd.DataFrame(final_rows)

    # ── Column order: page → KV fields → table columns ───────────────────────
    ordered_cols = ["page"] + all_kv_keys + all_table_cols
    df = df[[c for c in ordered_cols if c in df.columns]]

    # Drop table columns that are entirely empty (extraction artefacts)
    non_empty_table_cols = [
        c for c in all_table_cols
        if c in df.columns and df[c].astype(str).str.strip().ne("").any()
    ]
    final_cols = ["page"] + all_kv_keys + non_empty_table_cols
    df = df[[c for c in final_cols if c in df.columns]]

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    csv_path = os.path.join(output_dir, f"{base_name}_structured.csv")
    df.to_csv(csv_path, index=False)
    print(
        f"[PDF STRUCTURED] {n_pages} pages → {len(df)} rows × {len(df.columns)} cols "
        f"({len(all_kv_keys)} KV + {len(non_empty_table_cols)} table) → {csv_path}"
    )
    return csv_path, df


def _sql_type_to_semantic(sql_type: str) -> str:
    """Map SQL type strings to semantic type categories."""
    sql_upper = sql_type.upper()
    if any(t in sql_upper for t in ("INT", "BIGINT", "SMALLINT", "SERIAL")):
        return "integer"
    if any(t in sql_upper for t in ("DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "MONEY")):
        return "numeric"
    if any(t in sql_upper for t in ("DATE", "TIME", "TIMESTAMP")):
        return "temporal"
    if any(t in sql_upper for t in ("BOOL",)):
        return "boolean"
    if any(t in sql_upper for t in ("VARCHAR", "TEXT", "CHAR", "NVARCHAR")):
        return "text"
    return "unknown"
