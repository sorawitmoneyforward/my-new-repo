import pandas as pd


def append_total_count(df: pd.DataFrame) -> pd.DataFrame:
    """
    plus, minusを区別しない明細数を計算する
    BQ側でdaily_balancesから取得する方法を考えたが、size_in_a_monthなどvader_filled_にしかないカラムもあるので一旦Colabの実装を踏襲してdfで処理
    """
    return df.assign(record_count=df["plus_record_count"] + df["minus_record_count"])
