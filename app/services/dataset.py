import pandas as pd
import uuid
from app.db.database import redis_client

def process_upload(file_obj, filename: str) -> tuple[str, str, dict, list]:
    file_ext = filename.split('.')[-1]
    chat_id = str(uuid.uuid4())
    
    if file_ext in ['xlsx', 'xls']:
        df = pd.read_excel(file_obj)
    else:
        df = pd.read_csv(file_obj)
    
    initial_shape = df.shape
    duplicates_count = int(df.duplicated().sum())
    df.drop_duplicates(inplace=True)
    
    missing_count = int(df.isna().sum().sum())
    df.ffill(inplace=True)
    df.bfill(inplace=True)
    
    final_shape = df.shape
    
    redis_client.set(f"dataset:{chat_id}", df.to_json(orient='split'))
    
    stats = {
        "initial_rows": initial_shape[0],
        "duplicates_removed": duplicates_count,
        "missing_values_filled": missing_count,
        "final_rows": final_shape[0],
        "final_columns": final_shape[1]
    }
    
    return chat_id, filename, stats, list(df.columns)
