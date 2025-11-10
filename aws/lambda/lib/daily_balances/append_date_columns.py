import pandas as pd


def append_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    df_ = df.copy()
    df_["date"] = pd.to_datetime(df["date"])  # date列 を .dt メソッドを適用可能にするために型変換
    df_["year"] = df_["date"].dt.year  # year カラムを作る
    df_["month"] = df_["date"].dt.month  # month カラムを作る
    return df_
