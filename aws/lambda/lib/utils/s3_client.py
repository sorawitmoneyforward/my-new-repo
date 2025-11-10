import os
import pickle
from io import BytesIO
from pathlib import Path

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET")
S3 = boto3.client("s3")


def load_pickle(key: str):
    local_s3_dir = os.environ.get("LOCAL_S3_DIR")
    if local_s3_dir:
        path = Path(local_s3_dir) / key
        with open(path, "rb") as f:
            return pickle.load(f)
    else:
        resp = S3.get_object(Bucket=S3_BUCKET, Key=key)
        buffer = BytesIO(resp["Body"].read())
        return pickle.load(buffer)
