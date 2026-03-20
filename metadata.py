import pandas as pd
import io
from agents import infer_column_metadata

def extract_metadata_from_df(df):
    """Helper function to run LLM metadata extraction on a dataframe"""
    metadata = []
    for col in df.columns:
        # Get up to 6 valid samples, converted to string
        samples = df[col].dropna().astype(str).tolist()[:6]
        
        # Call Vertex AI
        llm_output = infer_column_metadata(col, samples)

        metadata.append({
            "name": col,
            "samples": samples,
            "semantic_info": llm_output
        })
    return metadata

def process_csv_stream(sample_stream: io.BytesIO):
    """Safely read CSV streams with fallback encoding for Windows files"""
    try:
        # Try standard UTF-8 first
        df = pd.read_csv(sample_stream, encoding='utf-8')
    except UnicodeDecodeError:
        # Fallback to standard Windows Excel CSV encoding
        sample_stream.seek(0)
        df = pd.read_csv(sample_stream, encoding='cp1252')
        
    return extract_metadata_from_df(df)

def process_excel_file(file_obj):
    """Read Excel files safely, stopping after 6 rows"""
    # Excel files cannot be stream-sliced line-by-line, but pandas can 
    # stop reading after parsing the headers and first 6 rows.
    df = pd.read_excel(file_obj, nrows=6)
    return extract_metadata_from_df(df)