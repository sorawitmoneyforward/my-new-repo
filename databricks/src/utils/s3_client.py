import boto3
from typing import Any
from io import BytesIO
import pickle
import json


class S3Client:
    def __init__(self, bucket_name: str) -> None:
        self.s3_client = boto3.client("s3")
        self.bucket_name = bucket_name

    def get_object_keys(self, prefix) -> list[str]:
        """
        Retrieve all object keys from the specified S3 prefix.
        """
        paginator = self.s3_client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            if "Contents" in page:
                keys.extend([obj["Key"] for obj in page["Contents"]])
        return keys

    def upload_pickle_to_s3(self, data: Any, key: str) -> None:
        """
        Upload a Python object to S3 as a pickle file.
        """
        buffer = BytesIO()
        pickle.dump(data, buffer)
        buffer.seek(0)
        self.s3_client.upload_fileobj(buffer, self.bucket_name, key)

    def download_pickle_from_s3(self, key: str) -> Any:
        """
        Download a pickle object from S3.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            buffer: BytesIO = BytesIO(response["Body"].read())
            return pickle.load(buffer)
        except Exception as e:
            print(f"Error downloading pickle object from S3: {e}")
            raise

    def upload_json_to_s3(self, data: Any, key: str) -> None:
        """
        Upload a json data to S3.
        """
        json_data = json.dumps(
            data,
            ensure_ascii=False,
        )
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=json_data,
            ContentType="application/json",
        )

    def download_json_from_s3(self, key: str) -> Any:
        """
        Download a json file from S3.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            json_data = response["Body"].read()
            json_obj = json.loads(json_data)
            return json_obj
        except Exception as e:
            print(f"Error downloading JSON file from S3: {e}")
            raise
