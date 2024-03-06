import pandas as pd

def compute_age(birthdate: pd.Series) -> pd.Series:
    from datetime import timedelta
    
    return (pd.to_datetime('today') - pd.to_datetime(birthdate)) / timedelta(days=365)