import os
import sys
from functools import partial
import pandas as pd
from itertools import groupby
from operator import itemgetter
from typing import Iterator

from pyspark.sql import SparkSession, Row

import_path = os.path.join(os.getcwd(), "./src")
if import_path not in sys.path:
    sys.path.insert(0, import_path)

from utils.const import (  # noqa: E402
    OBI_WAN_DAILY_BALANCE_PATH,
    OBI_WAN_PROCESS_INFO_PATH,
    VADER_DAILY_BALANCE_ORIGINAL_PATH,
    VADER_DAILY_BALANCE_PATH,
    VADER_PROCESS_INFO_ORIGINAL_PATH,
    VADER_PROCESS_INFO_PATH,
)
from config.dataset_config import (  # noqa: E402
    get_bucket_name,
    get_environment,
    get_resolved_sql,
    is_daily_balances_v2,
)
from utils.s3_client import S3Client  # noqa: E402
from utils.logger import JobExecutionLogger  # noqa: E402
from utils.timezone import get_now_jst  # noqa: E402


S3_CLIENT = S3Client(bucket_name=get_bucket_name())


class FetchDailyBalanceExecutor:

    default_partition_size = 1024

    def __init__(self, query: str, s3_dataframe_path: str, s3_info_path: str):
        self.processed_at = get_now_jst()
        self.query = query
        self.bucket_name = get_bucket_name()  # driver側で一度だけ取得
        self.s3_base_path = {
            "dataframe": s3_dataframe_path,
            "info": s3_info_path,
        }

    def _create_info(self, df: pd.DataFrame) -> dict:
        last_year = int(df["year"].max())
        last_month = int(df[df["year"] == last_year]["month"].max())
        return {
            "last_month_year": {"year": last_year, "month": last_month},
            "processed_at": str(self.processed_at.date()),
        }

    def _flush_office(
        self,
        s3_client: S3Client,
        office_id: str,
        df: pd.DataFrame,
        info: dict
    ) -> None:
        date_str = str(self.processed_at.date())
        s3_client.upload_pickle_to_s3(
            data=df,
            key=f"{self.s3_base_path['dataframe']}{date_str}/{office_id}.pkl",
        )
        s3_client.upload_pickle_to_s3(
            data=df,
            key=f"{self.s3_base_path['dataframe']}latest/{office_id}.pkl",
        )
        s3_client.upload_json_to_s3(
            data=info,
            key=f"{self.s3_base_path['info']}{date_str}/{office_id}.json",
        )
        s3_client.upload_json_to_s3(
            data=info,
            key=f"{self.s3_base_path['info']}latest/{office_id}.json",
        )

    def _save_partition_data(
        self,
        partition_iterator: Iterator[Row],
        bucket_name: str
    ) -> None:
        s3_client = S3Client(bucket_name)

        rows = [row.asDict(recursive=False) for row in partition_iterator]
        for office_id, group in groupby(rows, key=itemgetter("office_id")):
            raw_buffer = list(group)
            if not raw_buffer:
                continue
            df = pd.DataFrame(raw_buffer)
            info = self._create_info(df)
            self._flush_office(s3_client, office_id, df, info)

    def execute(self, partition_size: int | None = None):
        partition_size = partition_size or self.default_partition_size
        spark = SparkSession.getActiveSession()
        if spark is None:
            app_name = f"fetch_daily_balances_{self.processed_at}"
            spark = SparkSession.builder.appName(app_name).getOrCreate()

        # NOTE: P0対策
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", str(partition_size))

        df = spark.sql(self.query)
        df = df.repartition(partition_size, "office_id")
        df = df.sortWithinPartitions("office_id", "date")
        df.foreachPartition(
            partial(
                self._save_partition_data,
                bucket_name=self.bucket_name
            )
        )


if __name__ == "__main__":
    logger = JobExecutionLogger("fetch_daily_balances")
    environment = get_environment()

    # Determine file paths relative to this script's location
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # __file__ not available in Databricks exec context
        script_dir = os.getcwd()

    config_path = os.path.join(script_dir, "config", "dataset_config.yaml")
    template_dir = os.path.join(script_dir, "sql_templates")

    try:
        if is_daily_balances_v2():
            # daily_balances_v2
            # luke, anakin, obi-wanで使用中
            FetchDailyBalanceExecutor(
                query=get_resolved_sql(
                    "fetch_daily_balances_v2.sql.j2",
                    environment,
                    config_path,
                    template_dir,
                ),
                s3_dataframe_path=OBI_WAN_DAILY_BALANCE_PATH,
                s3_info_path=OBI_WAN_PROCESS_INFO_PATH,
            ).execute()
        else:
            # datapoint>=5でフィルタしたdaily_balances
            # vaderで使用中
            FetchDailyBalanceExecutor(
                query=get_resolved_sql(
                    "fetch_vader_daily_balances.sql.j2",
                    environment,
                    config_path,
                    template_dir,
                ),
                s3_dataframe_path=VADER_DAILY_BALANCE_PATH,
                s3_info_path=VADER_PROCESS_INFO_PATH,
            ).execute()
            # datapoint>=5でフィルタしていないdaily_balances
            # obi-wan特徴量である月次平均明細数(=feature10)でのみ使用中
            FetchDailyBalanceExecutor(
                query=get_resolved_sql(
                    "fetch_daily_balances.sql.j2",
                    environment,
                    config_path,
                    template_dir,
                ),
                s3_dataframe_path=VADER_DAILY_BALANCE_ORIGINAL_PATH,
                s3_info_path=VADER_PROCESS_INFO_ORIGINAL_PATH,
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
