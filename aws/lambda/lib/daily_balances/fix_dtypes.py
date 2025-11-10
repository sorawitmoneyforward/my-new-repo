import pandas as pd


def fix_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    closing_balance, diff_closing_balanceをdecimalからfloatに変換
    """
    df["closing_balance"] = df["closing_balance"].astype(float)
    df["diff_closing_balance"] = df["diff_closing_balance"].astype(float)
    return df
