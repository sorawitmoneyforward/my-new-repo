import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from lib.obi_wan.get_features import get_features
from lib.utils.s3_client import load_pickle

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def set_local_s3(monkeypatch):
    monkeypatch.setenv("LOCAL_S3_DIR", str(TEST_DATA_DIR))


@pytest.fixture
def test_cases():
    return pd.read_csv("tests/data/test_cases.csv")


def test_get_features(test_cases):
    for test_case in test_cases.itertuples():
        company_id = test_case.company_id
        processed_at = dt.datetime.strptime(test_case.new_first_examination_date, "%Y-%m-%d")
        date_str = processed_at.strftime("%Y%m%d")
        df_obi_wan_features = pd.read_csv(f"tests/data/input/features/obi_wan_features_{company_id}_{date_str}.csv")
        df_balances_v2 = load_pickle(f"data/obi-wan.daily_balances/{processed_at.date()}/{company_id}.pkl")
        df_balances = load_pickle(f"data/vader.daily_balances_original/{processed_at.date()}/{company_id}.pkl")
        X = get_features(
            test_case.lead_desired_amount,
            processed_at.date(),
            df_obi_wan_features,
            df_balances_v2,
            df_balances,
        )
        X_expected = pd.read_csv(f"tests/data/expected_output/features/X_{company_id}_{date_str}.csv")

        print(f"company_id: {company_id}, processed_at: {processed_at.date()}")
        pd.testing.assert_frame_equal(X, X_expected, check_dtype=False)
