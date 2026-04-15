import pandas as pd

def validate_data(df, required_columns):
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    print("Validation Passed: All required columns present.")
    return df

def normalize_data(df):
    df['diagnosis'] = df['diagnosis'].fillna('Unknown')
    df['claim_amount'] = pd.to_numeric(df['claim_amount'], errors='coerce').fillna(0.0)
    
    df['diagnosis'] = df['diagnosis'].str.upper()
    df['patient_name'] = df['patient_name'].str.title()
    
    return df

def aggregate_data(df):
    summary = df.groupby('diagnosis')['claim_amount'].sum().reset_index()
    summary = summary.rename(columns={'claim_amount': 'total_claim_amount'})
    return summary
