import json
import os
from pathlib import Path

import boto3
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S3_BUCKET")
S3 = boto3.client("s3")

logger = Logger()


def load_json(key: str) -> dict:
    local_s3_dir = os.environ.get("LOCAL_S3_DIR")
    if local_s3_dir:
        path = Path(local_s3_dir) / key
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        resp = S3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.load(resp["Body"])


def lambda_handler(event, context):
    try:
        path_parameters = event.get("pathParameters", {})
        office_id = path_parameters.get("office_id")
        if not office_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "office_id is required"}),
                "headers": {"Content-Type": "application/json"},
            }
        try:
            office_id = int(office_id)
            if office_id < 0:
                raise ValueError("office_id must be positive")
        except (ValueError, TypeError):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "office_id must be an positive integer"}),
                "headers": {"Content-Type": "application/json"},
            }
        key = f"data/vader.scores/latest/{office_id}.json"
        try:
            result = load_json(key)
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "vader_score": result["vader_scores"]["b1"],
                        "last_month_year": result["process_info"]["last_month_year"],
                        "created_at": result["process_info"]["processed_at"],
                    }
                ),
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
        print("Unexpected error: {}".format(str(e)))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"}),
            "headers": {"Content-Type": "application/json"},
        }
