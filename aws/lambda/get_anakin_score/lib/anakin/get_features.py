import pandas as pd

from lib.daily_balances import calc_stats, preprocess_daily_balances
from lib.utils.transformer import UseUnderBt

from .delete_col_list import delete_col_list

# 過去に遡って集約統計量を使う月数
# モデル評価の結果、B0(=当月)だけを使うことにした
# https://colab.research.google.com/drive/1yn9UrCX3vljTQrMXBp-rhQaaWb7BQCDN#scrollTo=RE7Jf-qxM01Y
BACKWARD_OPE_MONTH = 0


def get_features(df_balances: pd.DataFrame, desired_amount: int) -> pd.DataFrame:
    if df_balances.empty:
        return None

    df_balances = preprocess_daily_balances(df_balances)
    stats_df = calc_stats(df_balances)

    if stats_df is None:
        return None

    # 最後にlead_desired_amountを左にくっつけつつ、全カラムをdesired_amountで割った値を追加
    df_features = pd.merge(
        pd.DataFrame(
            {
                "office_id": [df_balances.iloc[0]["office_id"]],
                "lead_desired_amount": [desired_amount],
            }
        ),
        append_div_desired_amount(stats_df, desired_amount),
        on=["office_id"],
        how="left",
    )

    return UseUnderBt(use_under_Bt_remove_over_Bt_plus_one=BACKWARD_OPE_MONTH).fit_transform(
        df_features.drop(["office_id", "year"] + delete_col_list, axis=1)
    )


# 全カラムをそれぞれdesired_amountで割った値を追加
def append_div_desired_amount(df: pd.DataFrame, lead_desired_amount: float) -> pd.DataFrame:
    return pd.concat(
        [
            df,
            df.drop(["office_id", "year", "month"], axis=1)
            .add_suffix("_DivLeadDesiredAmount")
            .div(lead_desired_amount, axis=0),
        ],
        axis=1,
    )
