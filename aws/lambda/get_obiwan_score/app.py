import datetime as dt
import json
import os
from typing import Optional

import pandas as pd
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.parser import ValidationError, parse
from botocore.exceptions import ClientError
from pydantic import BaseModel

from lib.obi_wan.predict import predict
from lib.obi_wan import MODEL_VERSION
from lib.utils.s3_client import load_pickle

logger = Logger(service="get_obiwan_score")


def is_local():
    return os.environ.get("LOCAL_S3_DIR") is not None


class GetObiWanScoreEvent(BaseModel):
    office_id: int
    desired_amount: int
    date: Optional[str] = None
    feature14: float
    feature15: float
    feature17: float
    feature26: float
    feature31: float
    feature35: float
    feature45: float
    feature46: float
    feature47: float


def lambda_handler(raw_event, context):
    params = {}
    if raw_event.get("pathParameters"):
        params.update(raw_event["pathParameters"])
    if raw_event.get("queryStringParameters"):
        params.update(raw_event["queryStringParameters"])

    try:
        event = parse(
            event=params,
            model=GetObiWanScoreEvent,
        )
    except ValidationError as e:
        return {"statusCode": 400, "body": f"Invalid parameters: {e}"}

    try:
        office_id = event.office_id
        desired_amount = event.desired_amount
        date = dt.datetime.strptime(event.date, "%Y-%m-%d") if event.date is not None else dt.datetime.today()
        features = [
            event.feature14,
            event.feature15,
            event.feature17,
            event.feature26,
            event.feature31,
            event.feature35,
            event.feature45,
            event.feature46,
            event.feature47,
        ]

        daily_balances_v2_key = (
            f"data/obi-wan.daily_balances/latest/{office_id}.pkl"
            if event.date is None
            else f"data/obi-wan.daily_balances/{event.date}/{office_id}.pkl"
        )
        daily_balances_key = (
            f"data/vader.daily_balances_original/latest/{office_id}.pkl"
            if event.date is None
            else f"data/vader.daily_balances_original/{event.date}/{office_id}.pkl"
        )
        try:
            df_balances_v2 = load_pickle(daily_balances_v2_key)
            df_balances = load_pickle(daily_balances_key)
            df_obi_wan_features = pd.DataFrame(
                [features],
                columns=[f"Feature_{i}" for i in [14, 15, 17, 26, 31, 35, 45, 46, 47]],
            )
            score = predict(
                desired_amount,
                date.date(),
                df_obi_wan_features,
                df_balances_v2,
                df_balances,
            )
            return {
                "statusCode": 200,
                "body": json.dumps({"score": score, "model": MODEL_VERSION}),
                "headers": {"Content-Type": "application/json"},
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return {
                    "statusCode": 404,
                    "body": json.dumps({"error": "No object found for office_id {}".format(office_id)}),
                    "headers": {"Content-Type": "application/json"},
                }
        except FileNotFoundError:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "No object found for office_id {}".format(office_id)}),
                "headers": {"Content-Type": "application/json"},
            }
    except Exception as e:
        # Log the unexpected error with traceback
        import traceback

        print("Unexpected error: {}".format(e))
        print("Traceback:")
        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"}),
            "headers": {"Content-Type": "application/json"},
        }
