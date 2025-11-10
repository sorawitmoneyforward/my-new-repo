import datetime

import pandas as pd


def has_enough_data(df_balances: pd.DataFrame, date: datetime.date) -> bool:
    """指定した日付の月と前月において、残高データが5件以上あるかどうかを判定

    Args:
        df_balances: 日次残高データ(company_id, date, closing_balance, diff_closing_balance,
          plus_record_count, minus_record_count, plus_record_count_in_month, minus_record_count_in_month)

    Returns:
        bool: 当月または前月に5件以上ある場合はTrue、それ以外はFalse
    """
    count_by_month = df_balances.groupby(["year", "month"]).size().reset_index().rename(columns={0: "count"})
    last_month = datetime.date(date.year, date.month, 1) - datetime.timedelta(days=1)  # noqa: F841
    return not count_by_month.query(
        "((year == @date.year & month == @date.month) | "
        "(year == @last_month.year & month == @last_month.month)) & "
        "(count >= 5)"
    ).empty
