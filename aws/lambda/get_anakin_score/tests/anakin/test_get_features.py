import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from lib.anakin.get_features import get_features
from lib.utils.s3_client import load_pickle

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def set_local_s3(monkeypatch):
    monkeypatch.setenv("LOCAL_S3_DIR", str(TEST_DATA_DIR))


@pytest.fixture
def test_cases():
    return pd.read_csv("tests/anakin/data/test_cases.csv")


def test_get_features(test_cases: pd.DataFrame) -> None:
    for test_case in test_cases.itertuples():
        company_id = test_case.company_id
        processed_at = dt.datetime.strptime(test_case.first_examination_date, "%Y-%m-%d")
        df_balances = load_pickle(f"data/obi-wan.daily_balances/{processed_at.date()}/{company_id}.pkl")
        result = get_features(df_balances, desired_amount=test_case.lead_desired_amount)
        X_expected = pd.read_csv(f"tests/anakin/data/expected_output/X_{company_id}_{processed_at.date()}.csv")
        print(f"company_id: {company_id}, processed_at: {processed_at.date()}")
        pd.testing.assert_frame_equal(result, X_expected, check_dtype=False)


def test_get_features_empty_data() -> None:
    empty_df = pd.DataFrame(
        columns=[
            "office_id",
            "month",
            "date",
            "closing_balance",
            "plus_record_count",
            "minus_record_count",
            "is_data_point",
            "diff_closing_balance",
            "latest_month",
        ]
    )
    result = get_features(empty_df, desired_amount=1000000)
    assert result is None
