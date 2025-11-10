import pandas as pd

from .append_date_columns import append_date_columns
from .append_total_count import append_total_count
from .fix_dtypes import fix_dtypes


def preprocess_daily_balances(df_balances: pd.DataFrame) -> pd.DataFrame:
    """
    日次残高データの前処理
    """
    return append_total_count(append_date_columns(fix_dtypes(df_balances))).query("is_data_point == True")
