import os
import sys
import datetime as dt
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn import preprocessing
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import pandas as pd

import_path = os.path.join(os.getcwd(), "../src")
if import_path not in sys.path:
    sys.path.insert(0, import_path)

from utils.s3_client import S3Client  # noqa: E402
from utils.const import VADER_MODEL_PATH, VADER_STATS_PATH, VADER_SCORE_PATH, VADER_PROCESS_INFO_PATH  # noqa: E402
from config.dataset_config import get_bucket_name  # noqa: E402
from utils.logger import JobExecutionLogger  # noqa: E402
from utils.timezone import get_now_jst  # noqa: E402

try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


# FIXME
S3_CLIENT = S3Client(get_bucket_name())


class Remove_TimeVariable(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # 直接の書き換えが起きないようにcopy
        _X = X.copy()

        __X = _X.drop(_X.filter(like="year", axis=1).columns, axis=1)

        return __X


class LabelEncoder(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # 直接の書き換えが起きないようにcopy
        _X = X.copy()

        # ラベルエンコーディング:
        #   https://stackoverflow.com/questions/30384995/randomforestclassfier-fit-valueerror-could-not-convert-string-to-float
        le = preprocessing.LabelEncoder()
        for c in _X.columns:
            if _X[c].dtype == object:
                _X[c] = le.fit_transform(_X[c])
            else:
                pass

        return _X


class ConvertArray(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # 直接の書き換えが起きないようにcopy
        _X = X.copy()

        __X = _X.values

        return __X


class UseUnderBt(BaseEstimator, TransformerMixin):
    def __init__(self, use_under_Bt_remove_over_Bt_plus_one=0):
        self.use_under_Bt_remove_over_Bt_plus_one = use_under_Bt_remove_over_Bt_plus_one

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # 直接の書き換えが起きないようにcopy
        _X = X.copy()

        tmp_list = ["B" + str(i + 1) + "_monthly_" for i in range(self.use_under_Bt_remove_over_Bt_plus_one, 12 - 1, 1)]
        tmp_list.extend(["cum_" + str(i + 2) for i in range(self.use_under_Bt_remove_over_Bt_plus_one, 12 - 1, 1)])
        tmp_list.extend(["sd_" + str(i + 2) for i in range(self.use_under_Bt_remove_over_Bt_plus_one, 12 - 1, 1)])
        # print(tmp_list)

        for i in tmp_list:
            _X = _X.drop(_X.filter(like=i, axis=1).columns, axis=1)  # Btに応じて、カラムを削除

        __X = _X.dropna()  # NAが入っているレコードを削除

        return __X


class VaderPredictor:
    def __init__(self, models: list[Any]) -> None:
        self.models = models

    def predict(self, stats_df: pd.DataFrame) -> dict[str, float]:
        """
        calculate vader score for a specific office_id.
        """
        # Calculate Vader Scores
        vader_scores: dict[str, float] = {}
        for t in range(0, 12):
            cleaned_df = UseUnderBt(use_under_Bt_remove_over_Bt_plus_one=t).fit_transform(
                stats_df,
            )
            if cleaned_df.empty:
                vader_scores[f"b{t+1}"] = vader_scores[f"b{t}"]
            else:
                vader_scores[f"b{t+1}"] = self.models[t].predict_proba(cleaned_df)[:, 1][-1]
        return vader_scores


def get_office_ids(processed_at: dt.date):
    prefix = f"{VADER_STATS_PATH}{processed_at}/"
    keys = S3_CLIENT.get_object_keys(prefix)
    return [key.split("/")[-1].split(".")[0] for key in keys if key.endswith(".pkl")]


def get_stats_by_office_id(office_id: int, processed_at: dt.date) -> pd.DataFrame:
    key = f"{VADER_STATS_PATH}{processed_at}/{office_id}.pkl"
    return S3_CLIENT.download_pickle_from_s3(key)


def get_process_info_by_office_id(office_id: int, processed_at: dt.date) -> pd.DataFrame:
    key = f"{VADER_PROCESS_INFO_PATH}{processed_at}/{office_id}.json"
    return S3_CLIENT.download_json_from_s3(key)


def get_vader_models(version: str):
    models = []
    for n in range(12):
        key = f"{VADER_MODEL_PATH}m_BRF_B{n}_OutOfYear={version}__Pro.pkl"
        models.append(S3_CLIENT.download_pickle_from_s3(key))
    return models


def upload_vader_scores_by_office_id(processed_at: str, office_id: int, result: dict) -> pd.DataFrame:
    S3_CLIENT.upload_json_to_s3(data=result, key=f"{VADER_SCORE_PATH}{processed_at}/{office_id}.json")
    S3_CLIENT.upload_json_to_s3(data=result, key=f"{VADER_SCORE_PATH}latest/{office_id}.json")


class VaderPredictExecutor:

    default_partition_size = 1024

    def __init__(self):
        self.mode = sys.argv[2] if len(sys.argv) > 2 else "sequential"
        self.processed_at = get_now_jst()
        self.bucket_name = get_bucket_name()
        self.vader_models = None  # 遅延読み込み

    def execute(self):
        if self.mode == "spark" and SPARK_AVAILABLE:
            print("Running in optimized Spark mode")
            self.spark_mode()
        elif self.mode == "parallel":
            print("Running in parallel mode")
            self.parallel_mode()
        elif self.mode == "sequential":
            print("Running in sequential mode")
            self.sequential_mode()
        else:
            print(f"Unknown mode: {self.mode} or Spark not available. Running in sequential mode")
            self.sequential_mode()

    def _load_vader_models(self):
        """モデルの遅延読み込み"""
        if self.vader_models is None:
            self.vader_models = get_vader_models(version="2023_model_v72")
        return self.vader_models

    def spark_mode(self, partition_size: int | None = None):
        """Sparkを使用したパーティション化による並列処理"""
        partition_size = partition_size or self.default_partition_size
        try:
            spark = SparkSession.builder.appName(
                f"vader_predict_{self.processed_at}"
            ).getOrCreate()

            # 共通のデータ準備
            office_paths_data = self._prepare_office_paths_data()
            print(f"Total office_ids: {len(office_paths_data)}")

            # モデルをメインプロセスで読み込み
            print("Loading Vader models in main process...")
            vader_models = self._load_vader_models()
            print("Vader models loaded successfully")

            # office_idリストをSparkDataFrameに変換
            office_df = spark.createDataFrame(office_paths_data)

            num_partitions = min(partition_size, len(office_paths_data))
            office_df = office_df.repartition(num_partitions, "office_id")

            print(f"Repartitioned DataFrame to {office_df.rdd.getNumPartitions()} partitions.")

            # 各パーティションで並列処理を実行
            office_df.foreachPartition(
                partial(self._process_partition_static, bucket_name=self.bucket_name, vader_models=vader_models)
            )

            print("Successfully processed all office_ids in Spark mode")

        except Exception as e:
            print(f"Error in Spark mode: {e}")
            raise

    def parallel_mode(self):
        """ThreadPoolExecutorを使用した並列処理"""
        # 共通のデータ準備
        office_paths_data = self._prepare_office_paths_data()
        print(f"Total office_ids: {len(office_paths_data)}")

        # バッチに分割（1バッチ50件程度）
        office_batches = self._create_office_batches(office_paths_data, batch_size=50)
        print(f"Created {len(office_batches)} batches")

        # ThreadPoolExecutorで並列実行（最大10スレッド）
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i, batch in enumerate(office_batches):
                future = executor.submit(self._process_office_batch, batch)
                futures.append((future, i))

            # 進捗確認
            completed = 0
            for future, batch_idx in as_completed([f[0] for f in futures]):
                try:
                    future.result()
                    completed += 1
                    print(f"Completed batch {batch_idx + 1}/{len(office_batches)} ({completed}/{len(office_batches)})")
                except Exception as e:
                    print(f"Error in batch {batch_idx}: {str(e)}")

        print("All batches completed!")

    def sequential_mode(self):
        """元の逐次処理モード"""
        # 共通のデータ準備
        office_paths_data = self._prepare_office_paths_data()

        for i, office_data in enumerate(office_paths_data):
            try:
                self._process_single_office_data(office_data)
                print(f"success: office_id = {office_data['office_id']}, progress: {i+1}/{len(office_paths_data)}")
            except Exception as e:
                print(f"Error processing office_id {office_data['office_id']}: {str(e)}")

    def _prepare_office_paths_data(self) -> list[dict[str, str]]:
        """office_idと入力・出力ファイルパスのマッピングを作成する共通メソッド"""
        office_ids = get_office_ids(self.processed_at.date())
        return [
            {
                "office_id": office_id,
                "stats_file_path": f"{VADER_STATS_PATH}{self.processed_at.date()}/{office_id}.pkl",
                "process_info_file_path": f"{VADER_PROCESS_INFO_PATH}{self.processed_at.date()}/{office_id}.json",
                "output_file_path": f"{VADER_SCORE_PATH}{self.processed_at.date()}/{office_id}.json",
                "output_latest_path": f"{VADER_SCORE_PATH}latest/{office_id}.json"
            }
            for office_id in office_ids
        ]

    @staticmethod
    def _process_partition_static(partition_iterator, bucket_name, vader_models):
        """Sparkパーティション内での処理（静的メソッド）"""
        from utils.s3_client import S3Client

        s3_client = S3Client(bucket_name)

        # メインプロセスで読み込んだモデルを使用
        predictor = VaderPredictor(vader_models)

        rows = list(partition_iterator)
        if not rows:
            return

        # パーティション内の各office_idを処理
        for row in rows:
            try:
                office_id = row['office_id']
                stats_file_path = row['stats_file_path']
                process_info_file_path = row['process_info_file_path']
                output_file_path = row['output_file_path']
                output_latest_path = row['output_latest_path']

                # stats データを読み込み
                df = s3_client.download_pickle_from_s3(stats_file_path)

                if df.empty:
                    print(f"lack of datapoint: office_id = {office_id}")
                    continue

                # 予測実行
                vader_scores = predictor.predict(df)

                # プロセス情報を読み込み
                process_info = s3_client.download_json_from_s3(process_info_file_path)

                # 結果を作成
                result = {
                    "vader_scores": vader_scores,
                    "process_info": process_info,
                }

                # 結果をアップロード（通常パスとlatestパス）
                s3_client.upload_json_to_s3(result, output_file_path)
                s3_client.upload_json_to_s3(result, output_latest_path)

                print(f"Successfully processed office_id: {office_id}")

            except Exception as e:
                print(f"Error processing office_id {row['office_id']}: {str(e)}")
                # Sparkモードでは例外を再発生させない（他のパーティションに影響しないように）
                continue

    def _process_partition(self, partition_iterator, bucket_name):
        """Sparkパーティション内での処理（インスタンスメソッド版）"""
        vader_models = self._load_vader_models()
        return self._process_partition_static(partition_iterator, bucket_name, vader_models)

    def _process_single_office_data(self, office_data: dict[str, str]):
        """単一office_idの処理（逐次処理用）- 共通ビジネスロジック"""
        s3_client = S3Client(self.bucket_name)
        models = self._load_vader_models()
        predictor = VaderPredictor(models)
        self._process_single_office_data_with_client_and_predictor(office_data, s3_client, predictor)

    def _process_single_office_data_with_client_and_predictor(
        self,
        office_data: dict[str, str],
        s3_client: S3Client,
        predictor: VaderPredictor
    ):
        """単一office_idの処理（S3ClientとPredictorを受け取る版）- 共通ビジネスロジック"""
        office_id = office_data["office_id"]
        stats_file_path = office_data["stats_file_path"]
        process_info_file_path = office_data["process_info_file_path"]
        output_file_path = office_data["output_file_path"]
        output_latest_path = office_data["output_latest_path"]

        # stats データを読み込み
        df = s3_client.download_pickle_from_s3(stats_file_path)

        if df.empty:
            print(f"lack of datapoint: office_id = {office_id}")
            return

        # 予測実行
        vader_scores = predictor.predict(df)

        # プロセス情報を読み込み
        process_info = s3_client.download_json_from_s3(process_info_file_path)

        # 結果を作成
        result = {
            "vader_scores": vader_scores,
            "process_info": process_info,
        }

        # 結果をアップロード（通常パスとlatestパス）
        s3_client.upload_json_to_s3(result, output_file_path)
        s3_client.upload_json_to_s3(result, output_latest_path)

        print(f"Successfully processed office_id: {office_id}")

    def _process_office_batch(self, office_data_batch):
        """バッチ単位でのoffice_id処理"""
        s3_client = S3Client(self.bucket_name)
        models = self._load_vader_models()
        predictor = VaderPredictor(models)

        for office_data in office_data_batch:
            try:
                self._process_single_office_data_with_client_and_predictor(office_data, s3_client, predictor)
            except Exception as e:
                print(f"Error processing office_id {office_data['office_id']}: {str(e)}")

    def _create_office_batches(self, office_paths_data, batch_size: int = 50):
        """office_paths_dataリストをバッチに分割"""
        return [
            office_paths_data[i: i + batch_size]
            for i in range(0, len(office_paths_data), batch_size)
        ]


def main():
    executor = VaderPredictExecutor()
    executor.execute()


if __name__ == "__main__":
    logger = JobExecutionLogger("vader_predict")
    try:
        main()
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
