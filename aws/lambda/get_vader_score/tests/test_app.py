import pytest
from pathlib import Path
from app import lambda_handler

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def set_local_s3(monkeypatch):
    monkeypatch.setenv("LOCAL_S3_DIR", str(TEST_DATA_DIR))


def test_lambda_handler_success():
    event = {"pathParameters": {"office_id": 1}}
    response = lambda_handler(event, None)
    body = response["body"]
    assert response["statusCode"] == 200
    assert "vader_score" in body
    assert "last_month_year" in body
    assert "created_at" in body


@pytest.mark.parametrize("bad_id", ["abc", "-1", None])
def test_lambda_handler_bad_param(bad_id):
    event = {"pathParameters": {"office_id": bad_id}}
    response = lambda_handler(event, None)
    assert response["statusCode"] == 400


def test_lambda_handler_not_found():
    event = {"pathParameters": {"office_id": 999}}
    response = lambda_handler(event, None)
    assert response["statusCode"] == 404
