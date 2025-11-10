import datetime as dt
import traceback
import pytz


JST = pytz.timezone('Asia/Tokyo')


class JobExecutionLogger:
    """Databricks job実行ログの管理クラス"""

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.start_time = dt.datetime.now(JST)
        self.end_time = None
        self.status = "failed"  # デフォルトは失敗
        self.error_message = None
        self.error_traceback = None

    def mark_success(self):
        """実行成功をマーク"""
        self.status = "success"
        self.end_time = dt.datetime.now(JST)

    def mark_failure(self, error: Exception):
        """実行失敗をマーク"""
        self.status = "failed"
        self.end_time = dt.datetime.now(JST)
        self.error_message = str(error)
        self.error_traceback = traceback.format_exc()

    def log_file_name(self) -> str:
        """ログファイル名を作成"""
        timestamp_str = self.start_time.strftime("%Y%m%d_%H%M%S_%f")[:-3]  # ミリ秒まで
        return f"{self.job_name}_{timestamp_str}.json"

    def create_log_data(self) -> dict:
        """実行ログをJSON形式で作成"""
        if self.end_time is None:
            self.end_time = dt.datetime.now(JST)

        log_data = {
            "platform": "databricks",
            "job_type": "databricks_job",
            "execution_type": "spark_sql",
            "job_name": self.job_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": (self.end_time - self.start_time).total_seconds(),
            "status": self.status,
            "timestamp": dt.datetime.now(JST).isoformat(),
            "databricks_info": {
                "app_name": f"{self.job_name}-{dt.datetime.now(JST).date()}",
                "execution_environment": "databricks_spark"
            }
        }

        if self.status == "failed" and self.error_message:
            log_data["error"] = {
                "message": self.error_message,
                "traceback": self.error_traceback
            }

        return log_data
