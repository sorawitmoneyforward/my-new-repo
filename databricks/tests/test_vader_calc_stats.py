import os
# Ensure env vars before importing modules that initialize AWS/S3
os.environ.setdefault("BUCKET_NAME", "mf-credit-scoring-local")
os.environ.setdefault("ENV", "localhost")
import pickle  # noqa: E402
from pathlib import Path  # noqa: E402
import pytest  # noqa: E402
import pandas as pd  # noqa: E402
from src.vader_calc_stats import calculation  # noqa: E402

CURRENT_DIR = Path(__file__).parent


@pytest.fixture(autouse=True)
def set_bucket_name(monkeypatch):
    monkeypatch.setenv("BUCKET_NAME", "mf-credit-scoring-local")
    monkeypatch.setenv("ENV", "localhost")


def test_vader_calc_stats_case_1():
    input_file_path = CURRENT_DIR / "data/input/test_data_vader_calc_stats_case_1.csv"
    expected_output_file_path = CURRENT_DIR / "data/expected_output/test_data_vader_calc_stats_case_1.pkl"
    df = pd.read_csv(input_file_path)
    calculated_stats_df = calculation(df)
    with open(expected_output_file_path, "rb") as f:
        expected_calculated_stats_df = pickle.load(f)

    pd.testing.assert_frame_equal(calculated_stats_df, expected_calculated_stats_df)

    return


def test_vader_calc_stats_case_2():
    input_file_path = CURRENT_DIR / "data/input/test_data_vader_calc_stats_case_2.csv"
    expected_output_file_path = CURRENT_DIR / "data/expected_output/test_data_vader_calc_stats_case_2.pkl"
    df = pd.read_csv(input_file_path)
    calculated_stats_df = calculation(df)

    with open(expected_output_file_path, "rb") as f:
        expected_calculated_stats_df = pickle.load(f)

    pd.testing.assert_frame_equal(calculated_stats_df, expected_calculated_stats_df)

    return


def test_vader_calc_stats_case_3():
    """
    外挿の除外により空の結果を返す
    """
    input_file_path = CURRENT_DIR / "data/input/test_data_vader_calc_stats_case_3.csv"
    df = pd.read_csv(input_file_path)
    calculated_stats_df = calculation(df)

    assert calculated_stats_df.empty
