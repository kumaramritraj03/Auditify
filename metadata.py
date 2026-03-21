import pandas as pd
import io
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from agents import infer_column_metadata, infer_document_metadata, generate_data_summary


# ── Supported structured extensions ────────────────────────
_STRUCTURED_EXTENSIONS = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".json": "json",
}

# Max threads for parallel LLM calls (per-column metadata inference)
_MAX_LLM_THREADS = 6


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

    # Step 3: Extract per-column metadata with stats + LLM inference (PARALLEL)
    columns_metadata = _extract_column_metadata(df)

    # Step 4: Generate holistic dataset context profile (audit perimeter)
    column_headers = list(df.columns)
    sample_rows = df.head(5).to_dict(orient="records")
    data_types = {col: str(dtype) for col, dtype in df.dtypes.items()}

    data_summary = generate_data_summary(
        column_headers=column_headers,
        sample_rows=sample_rows,
        data_types=data_types,
        source_type=source_type,
    )

    # Step 5: Detect edge cases — record only, never fix
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
    """Extract per-column metadata: stats from sample + LLM semantic inference.

    LLM calls run in PARALLEL across columns using ThreadPoolExecutor.
    Stats are computed locally from the sample (never full data).
    """
    total_rows = len(df)

    def _process_column(col):
        series = df[col]
        samples = series.dropna().astype(str).tolist()[:6]

        null_count = series.isna().sum()
        missing_ratio = round(null_count / total_rows, 2) if total_rows > 0 else 0.0
        non_null_count = total_rows - null_count
        unique_count = series.nunique()
        unique_ratio = round(unique_count / non_null_count, 2) if non_null_count > 0 else 0.0

        # LLM semantic inference — runs in thread pool
        llm_output = infer_column_metadata(col, samples)

        return {
            "name": col,
            "samples": samples,
            "predicted_type": llm_output.get("predicted_type", "unknown"),
            "predicted_description": llm_output.get("predicted_description", ""),
            "confidence": llm_output.get("confidence", 0.0),
            "detected_dtype": str(series.dtype),
            "missing_ratio": missing_ratio,
            "unique_ratio": unique_ratio,
            "semantic_info": llm_output,
        }

    # Parallel LLM calls — each column independently inferred
    columns = list(df.columns)
    with ThreadPoolExecutor(max_workers=_MAX_LLM_THREADS) as executor:
        futures = {executor.submit(_process_column, col): col for col in columns}
        results = {}
        for future in as_completed(futures):
            col_name = futures[future]
            try:
                results[col_name] = future.result()
            except Exception:
                # If one column fails, record it with defaults — don't crash pipeline
                results[col_name] = {
                    "name": col_name,
                    "samples": [],
                    "predicted_type": "unknown",
                    "predicted_description": "Inference failed",
                    "confidence": 0.0,
                    "detected_dtype": str(df[col_name].dtype),
                    "missing_ratio": 0.0,
                    "unique_ratio": 0.0,
                    "semantic_info": {},
                }

    # Preserve original column order
    return [results[col] for col in columns]


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

    column_headers = list(df.columns)
    sample_rows = df.head(5).to_dict(orient="records")
    data_types = {col: str(dtype) for col, dtype in df.dtypes.items()}

    data_summary = generate_data_summary(
        column_headers=column_headers,
        sample_rows=sample_rows,
        data_types=data_types,
        source_type=source_type,
    )

    return {
        "columns": columns_metadata,
        "data_summary": data_summary,
    }


# ── Unstructured data processing (PDF, images) ────────────
# Standardized to return the same top-level schema as structured data:
# { "columns": [...], "data_summary": {...}, "source_type": ..., "edge_cases": {...} }

def process_pdf_file(path: str) -> dict:
    """Extract metadata from PDF via text extraction + LLM.

    Returns STANDARDIZED format matching structured data output:
    {
        "columns": [{unified column-like metadata for detected fields}],
        "data_summary": {},
        "source_type": "pdf",
        "edge_cases": {is_empty, read_error, ocr_confidence, ...},
        "document_info": {document_type, summary, detected_fields, confidence}
    }
    """
    edge_cases = {
        "is_empty": False,
        "read_error": False,
        "has_headers": False,
        "ocr_confidence": None,
    }

    try:
        import fitz  # PyMuPDF
    except ImportError:
        edge_cases["read_error"] = True
        return {
            "columns": [],
            "data_summary": {},
            "source_type": "pdf",
            "column_count": 0,
            "sample_row_count": 0,
            "edge_cases": edge_cases,
            "document_info": {
                "document_type": "pdf",
                "summary": "PDF processing requires PyMuPDF. Install: pip install PyMuPDF",
                "detected_fields": [],
                "confidence": 0.0,
                "error": "missing_dependency",
            },
        }

    try:
        doc = fitz.open(path)
    except Exception:
        edge_cases["read_error"] = True
        return {
            "columns": [],
            "data_summary": {},
            "source_type": "pdf",
            "column_count": 0,
            "sample_row_count": 0,
            "edge_cases": edge_cases,
            "document_info": {
                "document_type": "pdf",
                "summary": "Failed to open PDF. File may be corrupt or password-protected.",
                "detected_fields": [],
                "confidence": 0.0,
            },
        }

    total_pages = len(doc)

    if total_pages == 0:
        doc.close()
        edge_cases["is_empty"] = True
        return {
            "columns": [],
            "data_summary": {},
            "source_type": "pdf",
            "column_count": 0,
            "sample_row_count": 0,
            "edge_cases": edge_cases,
            "document_info": {
                "document_type": "pdf",
                "summary": "Empty PDF — no pages found.",
                "detected_fields": [],
                "confidence": 0.0,
            },
        }

    # Sample 5-6 random pages per PRD
    import random
    if total_pages <= 6:
        sample_pages = list(range(total_pages))
    else:
        sample_pages = sorted(random.sample(range(total_pages), 6))

    extracted_text = ""
    for page_num in sample_pages:
        page = doc[page_num]
        extracted_text += f"\n--- Page {page_num + 1} ---\n"
        extracted_text += page.get_text()

    doc.close()

    if not extracted_text.strip():
        edge_cases["ocr_confidence"] = "low"
        return {
            "columns": [],
            "data_summary": {},
            "source_type": "pdf",
            "column_count": 0,
            "sample_row_count": 0,
            "edge_cases": edge_cases,
            "document_info": {
                "document_type": "pdf",
                "summary": "No text extracted. Document may be scanned/image-based. OCR required.",
                "detected_fields": [],
                "confidence": 0.0,
            },
        }

    # Assess OCR confidence from text quality
    text_len = len(extracted_text.strip())
    if text_len < 100:
        edge_cases["ocr_confidence"] = "low"
    elif text_len < 500:
        edge_cases["ocr_confidence"] = "medium"
    else:
        edge_cases["ocr_confidence"] = "high"

    # Send to LLM for field detection
    llm_result = infer_document_metadata(extracted_text[:4000])

    # Build standardized columns from detected fields
    detected_fields = llm_result.get("detected_fields", [])
    columns = []
    for field in detected_fields:
        columns.append({
            "name": field,
            "samples": [],
            "predicted_type": "document_field",
            "predicted_description": f"Field detected from PDF: {field}",
            "confidence": llm_result.get("confidence", 0.0),
            "detected_dtype": "text",
            "missing_ratio": 0.0,
            "unique_ratio": 0.0,
            "semantic_info": {},
        })

    return {
        "columns": columns,
        "data_summary": {},
        "source_type": "pdf",
        "column_count": len(columns),
        "sample_row_count": 0,
        "edge_cases": edge_cases,
        "document_info": llm_result,
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
