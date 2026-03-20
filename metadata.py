import pandas as pd
from agents import infer_column_metadata


def process_structured_file(file_path: str):
    df = pd.read_csv(file_path, nrows=6)

    metadata = []
    indx=10
    for col in df.columns:
        samples = df[col][:indx].astype(str).tolist()

        llm_output = infer_column_metadata(col, samples)

        metadata.append({
            "name": col,
            "samples": samples,
            "semantic_info": llm_output
        })

    return metadata