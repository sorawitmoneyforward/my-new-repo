from typing import Any

import pandas as pd

from .get_features import get_features


class AnakinPredictor:
    def __init__(self, model: Any) -> None:
        self.model = model

    def predict(self, daily_balances_df: pd.DataFrame, desired_amount: int) -> float | None:
        """
        calculate anakin score for a specific office_id.
        """
        X = get_features(daily_balances_df, desired_amount)

        if X.empty:
            return None

        return self.model.predict_proba(X)[:, 0][-1]
