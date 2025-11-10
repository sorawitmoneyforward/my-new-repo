import json
from typing import Optional

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.parser import ValidationError, parse
from pydantic import BaseModel

from lib.anakin.predict import AnakinPredictor
from lib.utils.s3_client import load_pickle

logger = Logger(service="get_anakin_score")

MODEL_VERSION = "anakin_anakin_v17_B1_Scoring=recall_TrainSS=188_TestSS=46"
model = load_pickle(f"models/anakin/{MODEL_VERSION}.pkl")


class GetAnakinScoreEvent(BaseModel):
    office_id: int
    desired_amount: int
    date: Optional[str] = None


def lambda_handler(raw_event, context):
    params = {}
    if raw_event.get("pathParameters"):
        params.update(raw_event["pathParameters"])
    if raw_event.get("queryStringParameters"):
        params.update(raw_event["queryStringParameters"])

    logger.info(f"params: {params}")

    try:
        event = parse(
            event=params,
            model=GetAnakinScoreEvent,
        )
    except ValidationError as e:
        return {"statusCode": 400, "body": f"Invalid parameters: {e}"}

    logger.info(f"event: {event}")

    daily_balances_key = (
        f"data/obi-wan.daily_balances/latest/{event.office_id}.pkl"
        if event.date is None
        else f"data/obi-wan.daily_balances/{event.date}/{event.office_id}.pkl"
    )
    try:
        logger.info(f"Loading daily balances from S3: {daily_balances_key}")
        df_balances = load_pickle(daily_balances_key)
    except Exception as e:
        logger.error(f"Error loading daily balances: {e}")
        return {"statusCode": 500, "body": f"Error loading daily balances: {e}"}

    try:
        score = AnakinPredictor(model).predict(df_balances, event.desired_amount)

        return {
            "statusCode": 200,
            "body": json.dumps({"score": score, "model": MODEL_VERSION}),
            "headers": {"Content-Type": "application/json"},
        }
    except Exception as e:
        # Log the unexpected error
        print("Unexpected error: {}".format(str(e)))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"}),
            "headers": {"Content-Type": "application/json"},
        }
