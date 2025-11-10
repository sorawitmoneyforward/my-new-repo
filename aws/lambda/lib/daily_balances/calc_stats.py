import warnings

import numpy as np
import pandas as pd


def calc_stats(df_balances: pd.DataFrame) -> pd.DataFrame:
    """審査モデルの特徴量となる日次残高データの統計量を取得

    日次残高データがない場合はNoneを返す

    Args:
        df_balances: 日次残高データ(
            office_id, date, closing_balance, diff_closing_balance, plus_record_count, minus_record_count,
              plus_record_count_in_month, minus_record_count_in_month
            )

    Returns:
        pd.DataFrame: 左から月次統計量、全期間統計量、月次データポイント数, 全期間データポイント数, 月次明細数、全期間明細数数を結合したもの
    """
    df_features = (
        monthly_stats(df_balances)
        .merge(all_stats(df_balances), on=["office_id"], how="left")
        .merge(
            data_points(df_balances),
            on=["office_id", "year", "month"],
            how="left",
        )
        .merge(
            record_count(df_balances),
            on=["office_id", "year", "month"],
            how="left",
        )
    )

    return df_features


def data_points(df: pd.DataFrame) -> pd.DataFrame:
    """データポイント統計量を計算
    Args:
        df: 日次残高データ(
        office_id, date, closing_balance, diff_closing_balance,
          plus_record_count, minus_record_count, record_count, is_data_point
        )

    Returns:
        pd.DataFrame: office_id, 月次データポイント数の統計量(size_in_a_month),
          全期間データポイント数の統計量(all_size_in_a_month_{count|mean|std|min|25%|50%|75%|max})
    """
    df_size_in_a_month = (
        df.groupby(["office_id", "year", "month"])["is_data_point"]
        .sum()
        .rename("size_in_a_month")
        .reset_index()
    )
    return df_size_in_a_month.merge(
        df_size_in_a_month[["office_id", "size_in_a_month"]]
        .groupby("office_id")
        .describe()
        .add_prefix("all_size_in_a_month_")["all_size_in_a_month_size_in_a_month"],
        on="office_id",
        how="left",
    )


def record_count(df: pd.DataFrame) -> pd.DataFrame:
    """明細数統計量を計算
    Args:
        df: 日次残高データ(office_id, date, closing_balance, diff_closing_balance,
          plus_record_count, minus_record_count, record_count, is_data_point)
    Returns:
        pd.DataFrame: office_id, year, month, 月次明細数の統計量{plus_record|minus_record|record}_count_in_month,
          全期間明細数の統計量{plus_record|minus_record|record}_count
    """
    df_in_month = df[["office_id", "year", "month"]].drop_duplicates()
    df_all = df[["office_id"]].drop_duplicates()

    # Calculate record_count_in_month by summing record_count grouped by month
    df["record_count_in_month"] = df.groupby(["office_id", "year", "month"])[
        "record_count"
    ].transform("sum")
    df["plus_record_count_in_month"] = df.groupby(["office_id", "year", "month"])[
        "plus_record_count"
    ].transform("sum")
    df["minus_record_count_in_month"] = df.groupby(["office_id", "year", "month"])[
        "minus_record_count"
    ].transform("sum")

    for kind in ["plus_record", "minus_record", "record"]:
        df_in_month = df_in_month.merge(
            df[
                ["office_id", "year", "month", f"{kind}_count_in_month"]
            ].drop_duplicates(),
            on=["office_id", "year", "month"],
            how="left",
        ).fillna({f"{kind}_count_in_a_month": 0})
        df_all = df_all.merge(
            df[["office_id", f"{kind}_count"]]
            .groupby("office_id")
            .describe()
            .add_prefix(f"all_{kind}_cnt_")[f"all_{kind}_cnt_{kind}_count"],
            on=["office_id"],
            how="left",
        )

    return df_in_month.merge(df_all, on="office_id", how="left")


def monthly_stats(df_daily_bal_and_diff: pd.DataFrame) -> pd.DataFrame:
    """月次の要約統計量を計算
    Args:
        df_daily_bal_and_diff: 日次残高データ(office_id, date, closing_balance, diff_closing_balance,
          plus_record_count, minus_record_count,
          plus_record_count_in_month, minus_record_count_in_month)
    Returns:
        pd.DataFrame: office_id, year, month, 月次統計量
    """
    df_monthly_bal_and_diff = (
        df_daily_bal_and_diff.groupby(["office_id", "year", "month"])
        .agg(
            {
                "closing_balance": [
                    "min",
                    lambda x: np.nanpercentile(x, q=25),
                    "median",
                    "mean",
                    lambda x: np.nanpercentile(x, q=75),
                    "max",
                    lambda x: np.max(x) - np.min(x),
                ],
                "diff_closing_balance": [
                    "min",
                    lambda x: np.nanpercentile(x, q=25),
                    "median",
                    "mean",
                    lambda x: np.nanpercentile(x, q=75),
                    "max",
                    lambda x: np.max(x) - np.min(x),
                ],
            }
        )
        .reset_index()
    )

    df_monthly_bal_and_diff.columns = [
        "office_id",
        "year",
        "month",
        "monthly_min_balance",
        "monthly_q25_balance",
        "monthly_q50_balance",
        "monthly_mean_balance",
        "monthly_q75_balance",
        "monthly_max_balance",
        "monthly_max_sub_min_balance",
        "monthly_min_diff_balance",
        "monthly_q25_diff_balance",
        "monthly_q50_diff_balance",
        "monthly_mean_diff_balance",
        "monthly_q75_diff_balance",
        "monthly_max_diff_balance",
        "monthly_max_sub_min_diff_balance",
    ]
    df_cash = pd.DataFrame()
    for t in range(1, 12):
        df_cash = pd.concat([df_cash, shifted_df(df_monthly_bal_and_diff, t)], axis=1)

    df_monthly_bal_and_diff_all_features = pd.concat(
        [df_monthly_bal_and_diff, df_cash], axis=1
    )
    warnings.simplefilter("ignore")
    for stat_type in [
        "min",
        "q25",
        "q50",
        "mean",
        "q75",
        "max",
        "max_sub_min",
        "min_diff",
        "q25_diff",
        "q50_diff",
        "mean_diff",
        "q75_diff",
        "max_diff",
        "max_sub_min_diff",
    ]:
        element_list = ["monthly_" + stat_type + "_balance"]

        for t in range(2, 13):
            element_list.append("B" + str(t - 1) + "_monthly_" + stat_type + "_balance")
            # 種類ごとに、tヶ月の 累積和(cum)、標準偏差(sd) を算出する
            df_monthly_bal_and_diff_all_features[
                "cum_"
                + str(t)
                + "_B0_B"
                + str(t - 1)
                + "_monthly_"
                + stat_type
                + "_balance"
            ] = df_monthly_bal_and_diff_all_features[element_list].sum(
                axis=1, skipna=False
            )
            df_monthly_bal_and_diff_all_features[
                "sd_"
                + str(t)
                + "_B0_B"
                + str(t - 1)
                + "_monthly_"
                + stat_type
                + "_balance"
            ] = df_monthly_bal_and_diff_all_features[element_list].std(
                axis=1, skipna=False
            )
    warnings.simplefilter("default")

    return df_monthly_bal_and_diff_all_features


def shifted_df(df: pd.DataFrame, t: int) -> pd.DataFrame:
    """tヶ月前に遡ってカラムを追加する関数

    Args:
        df: 日次残高データ(office_id, year, month, monthly_min_balance, monthly_q25_balance,
          monthly_q50_balance, monthly_mean_balance, monthly_q75_balance, monthly_max_balance,
          monthly_max_sub_min_balance, monthly_min_diff_balance, monthly_q25_diff_balance,
          monthly_q50_diff_balance, monthly_mean_diff_balance, monthly_q75_diff_balance,
          monthly_max_diff_balance, monthly_max_sub_min_diff_balance)
        t: 何ヶ月前か
    Returns:
        pd.DataFrame: office_id, tヶ月前の統計量
    """
    df_shifted = df.copy().drop(["office_id", "year", "month"], axis=1)
    df_shifted.columns = [
        "B" + str(t) + "_monthly_min_balance",
        "B" + str(t) + "_monthly_q25_balance",
        "B" + str(t) + "_monthly_q50_balance",
        "B" + str(t) + "_monthly_mean_balance",
        "B" + str(t) + "_monthly_q75_balance",
        "B" + str(t) + "_monthly_max_balance",
        "B" + str(t) + "_monthly_max_sub_min_balance",
        "B" + str(t) + "_monthly_min_diff_balance",
        "B" + str(t) + "_monthly_q25_diff_balance",
        "B" + str(t) + "_monthly_q50_diff_balance",
        "B" + str(t) + "_monthly_mean_diff_balance",
        "B" + str(t) + "_monthly_q75_diff_balance",
        "B" + str(t) + "_monthly_max_diff_balance",
        "B" + str(t) + "_monthly_max_sub_min_diff_balance",
    ]

    return (
        pd.concat([df["office_id"], df_shifted], axis=1).groupby(["office_id"]).shift(t)
    )


def all_stats(df_daily_bal_and_diff: pd.DataFrame) -> pd.DataFrame:
    """全期間の要約統計量を計算

    Args:
        df_daily_bal_and_diff: 日次残高データ(office_id, date, closing_balance, diff_closing_balance,
          plus_record_count, minus_record_count, plus_record_count_in_month, minus_record_count_in_month)
    Returns:
        pd.DataFrame: office_id, 全期間統計量
    """
    df_all_bal_and_diff = (
        df_daily_bal_and_diff.groupby(["office_id"])
        .agg(
            {
                "closing_balance": [
                    "min",
                    lambda x: np.nanpercentile(x, q=25),
                    "median",
                    "mean",
                    lambda x: np.nanpercentile(x, q=75),
                    "max",
                    lambda x: np.max(x) - np.min(x),
                ],
                "diff_closing_balance": [
                    "min",
                    lambda x: np.nanpercentile(x, q=25),
                    "median",
                    "mean",
                    lambda x: np.nanpercentile(x, q=75),
                    "max",
                    lambda x: np.max(x) - np.min(x),
                ],
            }
        )
        .reset_index()
    )

    df_all_bal_and_diff.columns = [
        "office_id",
        "all_min_balance",
        "all_q25_balance",
        "all_q50_balance",
        "all_mean_balance",
        "all_q75_balance",
        "all_max_balance",
        "all_max_sub_min_balance",
        "all_min_diff_balance",
        "all_q25_diff_balance",
        "all_q50_diff_balance",
        "all_mean_diff_balance",
        "all_q75_diff_balance",
        "all_max_diff_balance",
        "all_max_sub_min_diff_balance",
    ]

    return df_all_bal_and_diff
