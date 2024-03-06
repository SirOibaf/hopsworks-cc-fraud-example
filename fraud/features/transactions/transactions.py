import pandas as pd

def transaction_abroad(issuer_country: pd.Series, transaction_country: pd.Series) -> pd.Series:
    return (issuer_country != transaction_country).replace({True: 1, False: 0})

def update_datetime(datetime: pd.Series) -> pd.Series:
    from datetime import timedelta
    return datetime + timedelta(days=530)
