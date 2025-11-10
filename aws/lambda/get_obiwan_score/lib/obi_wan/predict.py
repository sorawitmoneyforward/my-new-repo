import datetime as dt
from typing import Optional

import pandas as pd

from lib.obi_wan.get_features import get_features
from lib.obi_wan import MODEL_VERSION
from lib.utils.s3_client import load_pickle


def predict(
    desired_amount: int,
    date: dt.date,
    df_obi_wan_features: pd.DataFrame,
    df_balances_v2: pd.DataFrame,
    df_balances: pd.DataFrame,
) -> Optional[float]:
    """
    Obi-Wan v3スコア算出
    Args:
        company_id: 企業ID
        date: 審査日
        desired_amount: 希望融資額
        df_obi_wan_features: Obi-Wan特徴量データ(lead_id
          + https://www.notion.so/bizforward/6d3e82b63d4b431e8949d257a5f3f5e9?pvs=4#ec1c35e0a2fb48e49e39f70cec87474e)
        df_balances: 日次残高データ(lead_daily_balances_v2)
    Returns:
        Obi-Wan v3スコア
    """
    X = get_features(desired_amount, date, df_obi_wan_features, df_balances_v2, df_balances)
    if X is None:
        return None

    model = load_pickle(f"models/obi-wan/{MODEL_VERSION}.pkl")

    return model.predict_proba(X)[:, 1][-1]
