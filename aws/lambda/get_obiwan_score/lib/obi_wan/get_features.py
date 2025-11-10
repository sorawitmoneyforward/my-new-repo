import datetime as dt
from typing import Optional

import pandas as pd

from lib.daily_balances import UseUnderBt, calc_stats, has_enough_data, preprocess_daily_balances

from .feature_list import feature_list
from .preprocess_obi_wan_features import preprocess_obi_wan_features

# モデル評価の結果、B0(=当月)だけを使うことにした
# https://colab.research.google.com/drive/1o_M6P_Eqke58LSMfsO_vvgZwZaZZCZ3e#scrollTo=Mo3KW_EWs4CJ
BACKWARD_OPE_MONTH = 0


def get_features(
    desired_amount: int,
    date: dt.date,
    df_obi_wan_features: pd.DataFrame,
    df_balances_v2: pd.DataFrame,
    df_balances: pd.DataFrame,
) -> Optional[pd.DataFrame]:
    """
    特徴量データを取得

    Args:
        desired_amount: 希望融資額
        date: 審査日
        df_obi_wan_features: 特徴量データ(lead_id + Feature_0...Feature_47)
        df_balances: 日次残高データ(lead_daily_balances_v2)

    Returns:
        pd.DataFrame: 特徴量データ
    """
    if df_obi_wan_features.empty:
        # logger.info("features data is empty")
        return None

    if df_balances_v2.empty:
        # logger.info("the company has no balances")
        return None

    if df_balances.empty:
        # logger.info("the company has no balances")
        return None

    df_balances_v2 = preprocess_daily_balances(df_balances_v2)

    if not has_enough_data(df_balances_v2, date):
        return None

    # 前月までの月次平均明細数を計算してfeature10とする
    feature10 = (
        df_balances.groupby(["year", "month"])["record_count"]
        .sum()
        .reset_index()
        .sort_values(["year", "month"], ascending=False)
        .iloc[1:]["record_count"]  # 当月を除外して前月以前のデータを取得
        .mean()
    )
    # feature10を先頭に追加
    df_obi_wan_features.insert(0, "Feature_10", feature10)

    df = append_div_desired_amount(
        pd.merge(
            preprocess_obi_wan_features(df_obi_wan_features),
            calc_stats(df_balances_v2),
            how="cross",
        ),
        desired_amount,
    )
    X = UseUnderBt(use_under_Bt_remove_over_Bt_plus_one=BACKWARD_OPE_MONTH).fit_transform(df[feature_list])
    for i in feature_list:
        X = X.fillna({i: -9999})

    if X.empty:
        # logger.info("the stats data is likely to be invalid due to outliers")
        return None

    return X


def append_div_desired_amount(df: pd.DataFrame, lead_desired_amount: float) -> pd.DataFrame:
    """
    全カラムをそれぞれdesired_amountで割った値を追加

    Args:
        df: データフレーム
        lead_desired_amount: 希望融資額

    Returns:
        pd.DataFrame: 全カラムをそれぞれdesired_amountで割った値を追加したデータフレーム
    """
    return pd.concat(
        [
            df,
            df.drop(["office_id", "year", "month"], axis=1)
            .add_suffix("_DivLeadDesiredAmount")
            .div(lead_desired_amount, axis=0),
        ],
        axis=1,
    )
