import os
import sys
from typing import Optional
from pyspark.sql import SparkSession, DataFrame

import_path = os.path.join(os.getcwd(), "./src")
if import_path not in sys.path:
    sys.path.insert(0, import_path)

from config.dataset_config import (  # noqa: E402
    get_bucket_name,
    get_environment,
    get_resolved_sql,
)
from utils.s3_client import S3Client  # noqa: E402
from utils.logger import JobExecutionLogger  # noqa: E402


S3_CLIENT = S3Client(bucket_name=get_bucket_name())


class SqlTemplateExecutor:
    """SQL template execution with environment-specific resolution"""

    def __init__(self, query: str):
        self.query = query

    def execute(self) -> Optional[DataFrame]:
        spark = SparkSession.getActiveSession()
        if spark is None:
            app_name = "exec_spark_sql"
            spark = SparkSession.builder.appName(app_name).getOrCreate()

        return spark.sql(self.query)


if __name__ == "__main__":
    logger = JobExecutionLogger("exec_spark_sql")
    environment = get_environment()
    template_name = sys.argv[1]

    # Determine file paths relative to this script's location
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # __file__ not available in Databricks exec context
        script_dir = os.getcwd()

    config_path = os.path.join(script_dir, "config", "dataset_config.yaml")
    template_dir = os.path.normpath(os.path.join(script_dir, "..", "datamarts"))

    try:
        SqlTemplateExecutor(
            query=get_resolved_sql(
                template_name,
                environment,
                config_path,
                template_dir,
            )
        ).execute()
        logger.mark_success()

    except Exception as e:
        logger.mark_failure(e)

    finally:
        # 成功・失敗に関わらず、実行ログをS3にアップロード
        log_data = logger.create_log_data()
        s3_key = f"logs/databricks_jobs/{logger.log_file_name()}"
        S3_CLIENT.upload_json_to_s3(log_data, s3_key)

        # 失敗時は例外を再発生させる
        if logger.status == "failed":
            raise
