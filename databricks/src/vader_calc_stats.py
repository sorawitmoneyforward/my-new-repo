import os
import sys
import datetime as dt
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import pandas as pd

import_path = os.path.join(os.getcwd(), "./src")
if import_path not in sys.path:
    sys.path.insert(0, import_path)

from utils.s3_client import S3Client  # noqa: E402
from utils.const import VADER_DAILY_BALANCE_PATH, VADER_STATS_PATH  # noqa: E402
from config.dataset_config import get_bucket_name  # noqa: E402
from utils.logger import JobExecutionLogger  # noqa: E402
from utils.timezone import get_now_jst  # noqa: E402

try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


# FIXME
S3_CLIENT = S3Client(bucket_name=get_bucket_name())

# Columns for Stats DataFrame in MFC
CROW_MFC_DF_COLS = [
    "monthly_mean_balance",
    "monthly_max_balance",
    "monthly_min_balance",
    "monthly_max_sub_min_balance",
    "monthly_q25_balance",
    "monthly_q50_balance",
    "monthly_q75_balance",
    "monthly_mean_diff_balance",
    "monthly_max_diff_balance",
    "monthly_min_diff_balance",
    "monthly_max_sub_min_diff_balance",
    "monthly_q25_diff_balance",
    "monthly_q50_diff_balance",
    "monthly_q75_diff_balance",
    "B1_monthly_mean_balance",
    "B2_monthly_mean_balance",
    "B1_monthly_max_balance",
    "B2_monthly_max_balance",
    "B1_monthly_min_balance",
    "B2_monthly_min_balance",
    "B1_monthly_max_sub_min_balance",
    "B2_monthly_max_sub_min_balance",
    "B1_monthly_q25_balance",
    "B2_monthly_q25_balance",
    "B1_monthly_q50_balance",
    "B2_monthly_q50_balance",
    "B1_monthly_q75_balance",
    "B2_monthly_q75_balance",
    "B1_monthly_mean_diff_balance",
    "B2_monthly_mean_diff_balance",
    "B1_monthly_max_diff_balance",
    "B2_monthly_max_diff_balance",
    "B1_monthly_min_diff_balance",
    "B2_monthly_min_diff_balance",
    "B1_monthly_max_sub_min_diff_balance",
    "B2_monthly_max_sub_min_diff_balance",
    "B1_monthly_q25_diff_balance",
    "B2_monthly_q25_diff_balance",
    "B1_monthly_q50_diff_balance",
    "B2_monthly_q50_diff_balance",
    "B1_monthly_q75_diff_balance",
    "B2_monthly_q75_diff_balance",
    "cum_2_B0_B1_monthly_mean_balance",
    "cum_3_B0_B2_monthly_mean_balance",
    "sd_2_B0_B1_monthly_mean_balance",
    "sd_3_B0_B2_monthly_mean_balance",
    "cum_2_B0_B1_monthly_max_balance",
    "cum_3_B0_B2_monthly_max_balance",
    "sd_2_B0_B1_monthly_max_balance",
    "sd_3_B0_B2_monthly_max_balance",
    "cum_2_B0_B1_monthly_min_balance",
    "cum_3_B0_B2_monthly_min_balance",
    "sd_2_B0_B1_monthly_min_balance",
    "sd_3_B0_B2_monthly_min_balance",
    "cum_2_B0_B1_monthly_max_sub_min_balance",
    "cum_3_B0_B2_monthly_max_sub_min_balance",
    "sd_2_B0_B1_monthly_max_sub_min_balance",
    "sd_3_B0_B2_monthly_max_sub_min_balance",
    "cum_2_B0_B1_monthly_q25_balance",
    "cum_3_B0_B2_monthly_q25_balance",
    "sd_2_B0_B1_monthly_q25_balance",
    "sd_3_B0_B2_monthly_q25_balance",
    "cum_2_B0_B1_monthly_q50_balance",
    "cum_3_B0_B2_monthly_q50_balance",
    "sd_2_B0_B1_monthly_q50_balance",
    "sd_3_B0_B2_monthly_q50_balance",
    "cum_2_B0_B1_monthly_q75_balance",
    "cum_3_B0_B2_monthly_q75_balance",
    "sd_2_B0_B1_monthly_q75_balance",
    "sd_3_B0_B2_monthly_q75_balance",
    "cum_2_B0_B1_monthly_mean_diff_balance",
    "cum_3_B0_B2_monthly_mean_diff_balance",
    "sd_2_B0_B1_monthly_mean_diff_balance",
    "sd_3_B0_B2_monthly_mean_diff_balance",
    "cum_2_B0_B1_monthly_max_diff_balance",
    "cum_3_B0_B2_monthly_max_diff_balance",
    "sd_2_B0_B1_monthly_max_diff_balance",
    "sd_3_B0_B2_monthly_max_diff_balance",
    "cum_2_B0_B1_monthly_min_diff_balance",
    "cum_3_B0_B2_monthly_min_diff_balance",
    "sd_2_B0_B1_monthly_min_diff_balance",
    "sd_3_B0_B2_monthly_min_diff_balance",
    "cum_2_B0_B1_monthly_max_sub_min_diff_balance",
    "cum_3_B0_B2_monthly_max_sub_min_diff_balance",
    "sd_2_B0_B1_monthly_max_sub_min_diff_balance",
    "sd_3_B0_B2_monthly_max_sub_min_diff_balance",
    "cum_2_B0_B1_monthly_q25_diff_balance",
    "cum_3_B0_B2_monthly_q25_diff_balance",
    "sd_2_B0_B1_monthly_q25_diff_balance",
    "sd_3_B0_B2_monthly_q25_diff_balance",
    "cum_2_B0_B1_monthly_q50_diff_balance",
    "cum_3_B0_B2_monthly_q50_diff_balance",
    "sd_2_B0_B1_monthly_q50_diff_balance",
    "sd_3_B0_B2_monthly_q50_diff_balance",
    "cum_2_B0_B1_monthly_q75_diff_balance",
    "cum_3_B0_B2_monthly_q75_diff_balance",
    "sd_2_B0_B1_monthly_q75_diff_balance",
    "sd_3_B0_B2_monthly_q75_diff_balance",
    "B3_monthly_mean_balance",
    "B4_monthly_mean_balance",
    "B5_monthly_mean_balance",
    "B6_monthly_mean_balance",
    "B7_monthly_mean_balance",
    "B8_monthly_mean_balance",
    "B9_monthly_mean_balance",
    "B10_monthly_mean_balance",
    "B11_monthly_mean_balance",
    "B3_monthly_max_balance",
    "B4_monthly_max_balance",
    "B5_monthly_max_balance",
    "B6_monthly_max_balance",
    "B7_monthly_max_balance",
    "B8_monthly_max_balance",
    "B9_monthly_max_balance",
    "B10_monthly_max_balance",
    "B11_monthly_max_balance",
    "B3_monthly_min_balance",
    "B4_monthly_min_balance",
    "B5_monthly_min_balance",
    "B6_monthly_min_balance",
    "B7_monthly_min_balance",
    "B8_monthly_min_balance",
    "B9_monthly_min_balance",
    "B10_monthly_min_balance",
    "B11_monthly_min_balance",
    "B3_monthly_max_sub_min_balance",
    "B4_monthly_max_sub_min_balance",
    "B5_monthly_max_sub_min_balance",
    "B6_monthly_max_sub_min_balance",
    "B7_monthly_max_sub_min_balance",
    "B8_monthly_max_sub_min_balance",
    "B9_monthly_max_sub_min_balance",
    "B10_monthly_max_sub_min_balance",
    "B11_monthly_max_sub_min_balance",
    "B3_monthly_q25_balance",
    "B4_monthly_q25_balance",
    "B5_monthly_q25_balance",
    "B6_monthly_q25_balance",
    "B7_monthly_q25_balance",
    "B8_monthly_q25_balance",
    "B9_monthly_q25_balance",
    "B10_monthly_q25_balance",
    "B11_monthly_q25_balance",
    "B3_monthly_q50_balance",
    "B4_monthly_q50_balance",
    "B5_monthly_q50_balance",
    "B6_monthly_q50_balance",
    "B7_monthly_q50_balance",
    "B8_monthly_q50_balance",
    "B9_monthly_q50_balance",
    "B10_monthly_q50_balance",
    "B11_monthly_q50_balance",
    "B3_monthly_q75_balance",
    "B4_monthly_q75_balance",
    "B5_monthly_q75_balance",
    "B6_monthly_q75_balance",
    "B7_monthly_q75_balance",
    "B8_monthly_q75_balance",
    "B9_monthly_q75_balance",
    "B10_monthly_q75_balance",
    "B11_monthly_q75_balance",
    "B3_monthly_mean_diff_balance",
    "B4_monthly_mean_diff_balance",
    "B5_monthly_mean_diff_balance",
    "B6_monthly_mean_diff_balance",
    "B7_monthly_mean_diff_balance",
    "B8_monthly_mean_diff_balance",
    "B9_monthly_mean_diff_balance",
    "B10_monthly_mean_diff_balance",
    "B11_monthly_mean_diff_balance",
    "B3_monthly_max_diff_balance",
    "B4_monthly_max_diff_balance",
    "B5_monthly_max_diff_balance",
    "B6_monthly_max_diff_balance",
    "B7_monthly_max_diff_balance",
    "B8_monthly_max_diff_balance",
    "B9_monthly_max_diff_balance",
    "B10_monthly_max_diff_balance",
    "B11_monthly_max_diff_balance",
    "B3_monthly_min_diff_balance",
    "B4_monthly_min_diff_balance",
    "B5_monthly_min_diff_balance",
    "B6_monthly_min_diff_balance",
    "B7_monthly_min_diff_balance",
    "B8_monthly_min_diff_balance",
    "B9_monthly_min_diff_balance",
    "B10_monthly_min_diff_balance",
    "B11_monthly_min_diff_balance",
    "B3_monthly_max_sub_min_diff_balance",
    "B4_monthly_max_sub_min_diff_balance",
    "B5_monthly_max_sub_min_diff_balance",
    "B6_monthly_max_sub_min_diff_balance",
    "B7_monthly_max_sub_min_diff_balance",
    "B8_monthly_max_sub_min_diff_balance",
    "B9_monthly_max_sub_min_diff_balance",
    "B10_monthly_max_sub_min_diff_balance",
    "B11_monthly_max_sub_min_diff_balance",
    "B3_monthly_q25_diff_balance",
    "B4_monthly_q25_diff_balance",
    "B5_monthly_q25_diff_balance",
    "B6_monthly_q25_diff_balance",
    "B7_monthly_q25_diff_balance",
    "B8_monthly_q25_diff_balance",
    "B9_monthly_q25_diff_balance",
    "B10_monthly_q25_diff_balance",
    "B11_monthly_q25_diff_balance",
    "B3_monthly_q50_diff_balance",
    "B4_monthly_q50_diff_balance",
    "B5_monthly_q50_diff_balance",
    "B6_monthly_q50_diff_balance",
    "B7_monthly_q50_diff_balance",
    "B8_monthly_q50_diff_balance",
    "B9_monthly_q50_diff_balance",
    "B10_monthly_q50_diff_balance",
    "B11_monthly_q50_diff_balance",
    "B3_monthly_q75_diff_balance",
    "B4_monthly_q75_diff_balance",
    "B5_monthly_q75_diff_balance",
    "B6_monthly_q75_diff_balance",
    "B7_monthly_q75_diff_balance",
    "B8_monthly_q75_diff_balance",
    "B9_monthly_q75_diff_balance",
    "B10_monthly_q75_diff_balance",
    "B11_monthly_q75_diff_balance",
    "cum_4_B0_B3_monthly_mean_balance",
    "cum_5_B0_B4_monthly_mean_balance",
    "cum_6_B0_B5_monthly_mean_balance",
    "cum_7_B0_B6_monthly_mean_balance",
    "cum_8_B0_B7_monthly_mean_balance",
    "cum_9_B0_B8_monthly_mean_balance",
    "cum_10_B0_B9_monthly_mean_balance",
    "cum_11_B0_B10_monthly_mean_balance",
    "cum_12_B0_B11_monthly_mean_balance",
    "sd_4_B0_B3_monthly_mean_balance",
    "sd_5_B0_B4_monthly_mean_balance",
    "sd_6_B0_B5_monthly_mean_balance",
    "sd_7_B0_B6_monthly_mean_balance",
    "sd_8_B0_B7_monthly_mean_balance",
    "sd_9_B0_B8_monthly_mean_balance",
    "sd_10_B0_B9_monthly_mean_balance",
    "sd_11_B0_B10_monthly_mean_balance",
    "sd_12_B0_B11_monthly_mean_balance",
    "cum_4_B0_B3_monthly_max_balance",
    "cum_5_B0_B4_monthly_max_balance",
    "cum_6_B0_B5_monthly_max_balance",
    "cum_7_B0_B6_monthly_max_balance",
    "cum_8_B0_B7_monthly_max_balance",
    "cum_9_B0_B8_monthly_max_balance",
    "cum_10_B0_B9_monthly_max_balance",
    "cum_11_B0_B10_monthly_max_balance",
    "cum_12_B0_B11_monthly_max_balance",
    "sd_4_B0_B3_monthly_max_balance",
    "sd_5_B0_B4_monthly_max_balance",
    "sd_6_B0_B5_monthly_max_balance",
    "sd_7_B0_B6_monthly_max_balance",
    "sd_8_B0_B7_monthly_max_balance",
    "sd_9_B0_B8_monthly_max_balance",
    "sd_10_B0_B9_monthly_max_balance",
    "sd_11_B0_B10_monthly_max_balance",
    "sd_12_B0_B11_monthly_max_balance",
    "cum_4_B0_B3_monthly_min_balance",
    "cum_5_B0_B4_monthly_min_balance",
    "cum_6_B0_B5_monthly_min_balance",
    "cum_7_B0_B6_monthly_min_balance",
    "cum_8_B0_B7_monthly_min_balance",
    "cum_9_B0_B8_monthly_min_balance",
    "cum_10_B0_B9_monthly_min_balance",
    "cum_11_B0_B10_monthly_min_balance",
    "cum_12_B0_B11_monthly_min_balance",
    "sd_4_B0_B3_monthly_min_balance",
    "sd_5_B0_B4_monthly_min_balance",
    "sd_6_B0_B5_monthly_min_balance",
    "sd_7_B0_B6_monthly_min_balance",
    "sd_8_B0_B7_monthly_min_balance",
    "sd_9_B0_B8_monthly_min_balance",
    "sd_10_B0_B9_monthly_min_balance",
    "sd_11_B0_B10_monthly_min_balance",
    "sd_12_B0_B11_monthly_min_balance",
    "cum_4_B0_B3_monthly_max_sub_min_balance",
    "cum_5_B0_B4_monthly_max_sub_min_balance",
    "cum_6_B0_B5_monthly_max_sub_min_balance",
    "cum_7_B0_B6_monthly_max_sub_min_balance",
    "cum_8_B0_B7_monthly_max_sub_min_balance",
    "cum_9_B0_B8_monthly_max_sub_min_balance",
    "cum_10_B0_B9_monthly_max_sub_min_balance",
    "cum_11_B0_B10_monthly_max_sub_min_balance",
    "cum_12_B0_B11_monthly_max_sub_min_balance",
    "sd_4_B0_B3_monthly_max_sub_min_balance",
    "sd_5_B0_B4_monthly_max_sub_min_balance",
    "sd_6_B0_B5_monthly_max_sub_min_balance",
    "sd_7_B0_B6_monthly_max_sub_min_balance",
    "sd_8_B0_B7_monthly_max_sub_min_balance",
    "sd_9_B0_B8_monthly_max_sub_min_balance",
    "sd_10_B0_B9_monthly_max_sub_min_balance",
    "sd_11_B0_B10_monthly_max_sub_min_balance",
    "sd_12_B0_B11_monthly_max_sub_min_balance",
    "cum_4_B0_B3_monthly_q25_balance",
    "cum_5_B0_B4_monthly_q25_balance",
    "cum_6_B0_B5_monthly_q25_balance",
    "cum_7_B0_B6_monthly_q25_balance",
    "cum_8_B0_B7_monthly_q25_balance",
    "cum_9_B0_B8_monthly_q25_balance",
    "cum_10_B0_B9_monthly_q25_balance",
    "cum_11_B0_B10_monthly_q25_balance",
    "cum_12_B0_B11_monthly_q25_balance",
    "sd_4_B0_B3_monthly_q25_balance",
    "sd_5_B0_B4_monthly_q25_balance",
    "sd_6_B0_B5_monthly_q25_balance",
    "sd_7_B0_B6_monthly_q25_balance",
    "sd_8_B0_B7_monthly_q25_balance",
    "sd_9_B0_B8_monthly_q25_balance",
    "sd_10_B0_B9_monthly_q25_balance",
    "sd_11_B0_B10_monthly_q25_balance",
    "sd_12_B0_B11_monthly_q25_balance",
    "cum_4_B0_B3_monthly_q50_balance",
    "cum_5_B0_B4_monthly_q50_balance",
    "cum_6_B0_B5_monthly_q50_balance",
    "cum_7_B0_B6_monthly_q50_balance",
    "cum_8_B0_B7_monthly_q50_balance",
    "cum_9_B0_B8_monthly_q50_balance",
    "cum_10_B0_B9_monthly_q50_balance",
    "cum_11_B0_B10_monthly_q50_balance",
    "cum_12_B0_B11_monthly_q50_balance",
    "sd_4_B0_B3_monthly_q50_balance",
    "sd_5_B0_B4_monthly_q50_balance",
    "sd_6_B0_B5_monthly_q50_balance",
    "sd_7_B0_B6_monthly_q50_balance",
    "sd_8_B0_B7_monthly_q50_balance",
    "sd_9_B0_B8_monthly_q50_balance",
    "sd_10_B0_B9_monthly_q50_balance",
    "sd_11_B0_B10_monthly_q50_balance",
    "sd_12_B0_B11_monthly_q50_balance",
    "cum_4_B0_B3_monthly_q75_balance",
    "cum_5_B0_B4_monthly_q75_balance",
    "cum_6_B0_B5_monthly_q75_balance",
    "cum_7_B0_B6_monthly_q75_balance",
    "cum_8_B0_B7_monthly_q75_balance",
    "cum_9_B0_B8_monthly_q75_balance",
    "cum_10_B0_B9_monthly_q75_balance",
    "cum_11_B0_B10_monthly_q75_balance",
    "cum_12_B0_B11_monthly_q75_balance",
    "sd_4_B0_B3_monthly_q75_balance",
    "sd_5_B0_B4_monthly_q75_balance",
    "sd_6_B0_B5_monthly_q75_balance",
    "sd_7_B0_B6_monthly_q75_balance",
    "sd_8_B0_B7_monthly_q75_balance",
    "sd_9_B0_B8_monthly_q75_balance",
    "sd_10_B0_B9_monthly_q75_balance",
    "sd_11_B0_B10_monthly_q75_balance",
    "sd_12_B0_B11_monthly_q75_balance",
    "cum_4_B0_B3_monthly_mean_diff_balance",
    "cum_5_B0_B4_monthly_mean_diff_balance",
    "cum_6_B0_B5_monthly_mean_diff_balance",
    "cum_7_B0_B6_monthly_mean_diff_balance",
    "cum_8_B0_B7_monthly_mean_diff_balance",
    "cum_9_B0_B8_monthly_mean_diff_balance",
    "cum_10_B0_B9_monthly_mean_diff_balance",
    "cum_11_B0_B10_monthly_mean_diff_balance",
    "cum_12_B0_B11_monthly_mean_diff_balance",
    "sd_4_B0_B3_monthly_mean_diff_balance",
    "sd_5_B0_B4_monthly_mean_diff_balance",
    "sd_6_B0_B5_monthly_mean_diff_balance",
    "sd_7_B0_B6_monthly_mean_diff_balance",
    "sd_8_B0_B7_monthly_mean_diff_balance",
    "sd_9_B0_B8_monthly_mean_diff_balance",
    "sd_10_B0_B9_monthly_mean_diff_balance",
    "sd_11_B0_B10_monthly_mean_diff_balance",
    "sd_12_B0_B11_monthly_mean_diff_balance",
    "cum_4_B0_B3_monthly_max_diff_balance",
    "cum_5_B0_B4_monthly_max_diff_balance",
    "cum_6_B0_B5_monthly_max_diff_balance",
    "cum_7_B0_B6_monthly_max_diff_balance",
    "cum_8_B0_B7_monthly_max_diff_balance",
    "cum_9_B0_B8_monthly_max_diff_balance",
    "cum_10_B0_B9_monthly_max_diff_balance",
    "cum_11_B0_B10_monthly_max_diff_balance",
    "cum_12_B0_B11_monthly_max_diff_balance",
    "sd_4_B0_B3_monthly_max_diff_balance",
    "sd_5_B0_B4_monthly_max_diff_balance",
    "sd_6_B0_B5_monthly_max_diff_balance",
    "sd_7_B0_B6_monthly_max_diff_balance",
    "sd_8_B0_B7_monthly_max_diff_balance",
    "sd_9_B0_B8_monthly_max_diff_balance",
    "sd_10_B0_B9_monthly_max_diff_balance",
    "sd_11_B0_B10_monthly_max_diff_balance",
    "sd_12_B0_B11_monthly_max_diff_balance",
    "cum_4_B0_B3_monthly_min_diff_balance",
    "cum_5_B0_B4_monthly_min_diff_balance",
    "cum_6_B0_B5_monthly_min_diff_balance",
    "cum_7_B0_B6_monthly_min_diff_balance",
    "cum_8_B0_B7_monthly_min_diff_balance",
    "cum_9_B0_B8_monthly_min_diff_balance",
    "cum_10_B0_B9_monthly_min_diff_balance",
    "cum_11_B0_B10_monthly_min_diff_balance",
    "cum_12_B0_B11_monthly_min_diff_balance",
    "sd_4_B0_B3_monthly_min_diff_balance",
    "sd_5_B0_B4_monthly_min_diff_balance",
    "sd_6_B0_B5_monthly_min_diff_balance",
    "sd_7_B0_B6_monthly_min_diff_balance",
    "sd_8_B0_B7_monthly_min_diff_balance",
    "sd_9_B0_B8_monthly_min_diff_balance",
    "sd_10_B0_B9_monthly_min_diff_balance",
    "sd_11_B0_B10_monthly_min_diff_balance",
    "sd_12_B0_B11_monthly_min_diff_balance",
    "cum_4_B0_B3_monthly_max_sub_min_diff_balance",
    "cum_5_B0_B4_monthly_max_sub_min_diff_balance",
    "cum_6_B0_B5_monthly_max_sub_min_diff_balance",
    "cum_7_B0_B6_monthly_max_sub_min_diff_balance",
    "cum_8_B0_B7_monthly_max_sub_min_diff_balance",
    "cum_9_B0_B8_monthly_max_sub_min_diff_balance",
    "cum_10_B0_B9_monthly_max_sub_min_diff_balance",
    "cum_11_B0_B10_monthly_max_sub_min_diff_balance",
    "cum_12_B0_B11_monthly_max_sub_min_diff_balance",
    "sd_4_B0_B3_monthly_max_sub_min_diff_balance",
    "sd_5_B0_B4_monthly_max_sub_min_diff_balance",
    "sd_6_B0_B5_monthly_max_sub_min_diff_balance",
    "sd_7_B0_B6_monthly_max_sub_min_diff_balance",
    "sd_8_B0_B7_monthly_max_sub_min_diff_balance",
    "sd_9_B0_B8_monthly_max_sub_min_diff_balance",
    "sd_10_B0_B9_monthly_max_sub_min_diff_balance",
    "sd_11_B0_B10_monthly_max_sub_min_diff_balance",
    "sd_12_B0_B11_monthly_max_sub_min_diff_balance",
    "cum_4_B0_B3_monthly_q25_diff_balance",
    "cum_5_B0_B4_monthly_q25_diff_balance",
    "cum_6_B0_B5_monthly_q25_diff_balance",
    "cum_7_B0_B6_monthly_q25_diff_balance",
    "cum_8_B0_B7_monthly_q25_diff_balance",
    "cum_9_B0_B8_monthly_q25_diff_balance",
    "cum_10_B0_B9_monthly_q25_diff_balance",
    "cum_11_B0_B10_monthly_q25_diff_balance",
    "cum_12_B0_B11_monthly_q25_diff_balance",
    "sd_4_B0_B3_monthly_q25_diff_balance",
    "sd_5_B0_B4_monthly_q25_diff_balance",
    "sd_6_B0_B5_monthly_q25_diff_balance",
    "sd_7_B0_B6_monthly_q25_diff_balance",
    "sd_8_B0_B7_monthly_q25_diff_balance",
    "sd_9_B0_B8_monthly_q25_diff_balance",
    "sd_10_B0_B9_monthly_q25_diff_balance",
    "sd_11_B0_B10_monthly_q25_diff_balance",
    "sd_12_B0_B11_monthly_q25_diff_balance",
    "cum_4_B0_B3_monthly_q50_diff_balance",
    "cum_5_B0_B4_monthly_q50_diff_balance",
    "cum_6_B0_B5_monthly_q50_diff_balance",
    "cum_7_B0_B6_monthly_q50_diff_balance",
    "cum_8_B0_B7_monthly_q50_diff_balance",
    "cum_9_B0_B8_monthly_q50_diff_balance",
    "cum_10_B0_B9_monthly_q50_diff_balance",
    "cum_11_B0_B10_monthly_q50_diff_balance",
    "cum_12_B0_B11_monthly_q50_diff_balance",
    "sd_4_B0_B3_monthly_q50_diff_balance",
    "sd_5_B0_B4_monthly_q50_diff_balance",
    "sd_6_B0_B5_monthly_q50_diff_balance",
    "sd_7_B0_B6_monthly_q50_diff_balance",
    "sd_8_B0_B7_monthly_q50_diff_balance",
    "sd_9_B0_B8_monthly_q50_diff_balance",
    "sd_10_B0_B9_monthly_q50_diff_balance",
    "sd_11_B0_B10_monthly_q50_diff_balance",
    "sd_12_B0_B11_monthly_q50_diff_balance",
    "cum_4_B0_B3_monthly_q75_diff_balance",
    "cum_5_B0_B4_monthly_q75_diff_balance",
    "cum_6_B0_B5_monthly_q75_diff_balance",
    "cum_7_B0_B6_monthly_q75_diff_balance",
    "cum_8_B0_B7_monthly_q75_diff_balance",
    "cum_9_B0_B8_monthly_q75_diff_balance",
    "cum_10_B0_B9_monthly_q75_diff_balance",
    "cum_11_B0_B10_monthly_q75_diff_balance",
    "cum_12_B0_B11_monthly_q75_diff_balance",
    "sd_4_B0_B3_monthly_q75_diff_balance",
    "sd_5_B0_B4_monthly_q75_diff_balance",
    "sd_6_B0_B5_monthly_q75_diff_balance",
    "sd_7_B0_B6_monthly_q75_diff_balance",
    "sd_8_B0_B7_monthly_q75_diff_balance",
    "sd_9_B0_B8_monthly_q75_diff_balance",
    "sd_10_B0_B9_monthly_q75_diff_balance",
    "sd_11_B0_B10_monthly_q75_diff_balance",
    "sd_12_B0_B11_monthly_q75_diff_balance",
]


def _exclusion_criteria_for_model():
    try:
        file_path = os.path.join(os.path.dirname(__file__), "./resources/FCmodel_Input_Scope_in_MFC_v72.csv")
    except NameError:
        file_path = os.path.join(os.getcwd(), "./resources/FCmodel_Input_Scope_in_MFC_v72.csv")
    df = pd.read_csv(file_path)
    df = df.iloc[[3, 7], :]
    df.index = ["InputMin", "InputMax"]
    df = df.drop(
        ["Unnamed: 0", "office_id", "FC_flag_monthly", "Leak_FC_flag_monthly"],
        axis=1,
    ).reindex(CROW_MFC_DF_COLS, axis=1)
    return df


EXCLUSION_CRITERIA_FOR_MODEL = _exclusion_criteria_for_model()


def get_office_ids(processed_at: dt.date):
    prefix = f"{VADER_DAILY_BALANCE_PATH}{processed_at}/"
    keys = S3_CLIENT.get_object_keys(prefix)
    return [int(key.split("/")[-1].split(".")[0]) for key in keys if key.endswith(".pkl")]


def get_daily_balance_by_office_id(office_id: int, processed_at: dt.date) -> pd.DataFrame:
    return S3_CLIENT.download_pickle_from_s3(f"{VADER_DAILY_BALANCE_PATH}{processed_at}/{office_id}.pkl")


def uplaod_stats_by_office_id(processed_at: dt.date, office_id: int, df: pd.DataFrame) -> None:
    S3_CLIENT.upload_pickle_to_s3(
        df,
        f"{VADER_STATS_PATH}{processed_at}/{office_id}.pkl",
    )


def calculation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate monthly statistics for daily balance and its difference,
    and generate additional features for machine learning models.
    Args:
        df (pd.DataFrame): year, month, closing_balance, diff_closing_balance
    Returns:
        pd.DataFrame: Monthly statistics and additional features.
    """
    return MonthlyBalanceStats(df).calculate_monthly_stats()


class MonthlyBalanceStats:
    STATS_TYPES = [
        "min",
        "q25",
        "q50",
        "mean",
        "q75",
        "max",
        "max_sub_min",
        "min_diff",
        "q25_diff",
        "q50_diff",
        "mean_diff",
        "q75_diff",
        "max_diff",
        "max_sub_min_diff",
    ]

    def __init__(self, df_daily_bal_and_diff: pd.DataFrame):
        self.df_daily_bal_and_diff = df_daily_bal_and_diff
        self.df_monthly_bal_and_diff = pd.DataFrame()
        self.df_monthly_bal_and_diff_all_features = pd.DataFrame()

    def calculate_monthly_stats(self) -> pd.DataFrame:
        self._calculate_aggregated_stats()
        self._rename_columns()
        self._generate_shifted_features()
        self._generate_cumulative_features()
        return self._clean_df(
            self.df_monthly_bal_and_diff_all_features
        )

    def _calculate_aggregated_stats(self):
        self.df_monthly_bal_and_diff = (
            self.df_daily_bal_and_diff.groupby(["year", "month"])
            .agg(
                {
                    "closing_balance": self._get_stat_functions(),
                    "diff_closing_balance": self._get_stat_functions(),
                }
            )
            .reset_index()
        )

    def _get_stat_functions(self):
        return [
            "min",
            lambda x: np.nanpercentile(x, q=25),
            "median",
            "mean",
            lambda x: np.nanpercentile(x, q=75),
            "max",
            lambda x: np.max(x) - np.min(x),
        ]

    def _rename_columns(self):
        self.df_monthly_bal_and_diff.columns = ["year", "month"] + [
            f"monthly_{stats_type}_balance" for stats_type in self.STATS_TYPES
        ]

    def _generate_shifted_features(self):
        df_cash = pd.DataFrame()
        # Generate shifted features for the past 12 months
        for t in range(1, 12):
            df_cash = pd.concat([df_cash, self._shifted_df(t)], axis=1)
        self.df_monthly_bal_and_diff_all_features = pd.concat([self.df_monthly_bal_and_diff, df_cash], axis=1)

    def _shifted_df(self, t: int) -> pd.DataFrame:
        df_copy = self.df_monthly_bal_and_diff.copy()
        shifted_cols = df_copy.drop(["year", "month"], axis=1)
        shifted_cols.columns = [f"B{t}_{col}" for col in shifted_cols.columns]
        return pd.concat([shifted_cols], axis=1).shift(t)

    def _generate_cumulative_features(self):
        self.df_monthly_bal_and_diff_all_features = self.df_monthly_bal_and_diff_all_features.copy()
        new_features = []
        for stats_type in self.STATS_TYPES:
            element_list = [f"monthly_{stats_type}_balance"]
            for t in range(2, 13):
                element_list.append(f"B{t-1}_monthly_{stats_type}_balance")
                cum_sum_name = f"cum_{t}_B0_B{t - 1}_monthly_{stats_type}_balance"
                sd_name = f"sd_{t}_B0_B{t - 1}_monthly_{stats_type}_balance"

                new_features.append(
                    self.df_monthly_bal_and_diff_all_features[element_list]
                    .sum(axis=1, skipna=False)
                    .rename(cum_sum_name)
                )
                new_features.append(
                    self.df_monthly_bal_and_diff_all_features[element_list].std(axis=1, skipna=False).rename(sd_name)
                )
        self.df_monthly_bal_and_diff_all_features = pd.concat(
            [self.df_monthly_bal_and_diff_all_features] + new_features, axis=1
        )

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in EXCLUSION_CRITERIA_FOR_MODEL.columns.to_list():
            min_val = EXCLUSION_CRITERIA_FOR_MODEL.loc['InputMin', col]
            max_val = EXCLUSION_CRITERIA_FOR_MODEL.loc['InputMax', col]

            # Keep rows where value is NaN or between min and max
            mask = df[col].isna() | df[col].between(min_val, max_val)
            df = df.loc[mask]

            if df.empty:
                # print(EXCLUSION_CRITERIA_FOR_MODEL[[col]])
                break
        return df


class VaderStatsExecutor:

    default_partition_size = 1024

    def __init__(self):
        self.mode = sys.argv[2] if len(sys.argv) > 2 else "sequential"
        self.processed_at = get_now_jst()
        self.bucket_name = get_bucket_name()

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

    def spark_mode(self, partition_size: int | None = None):
        """Sparkを使用したパーティション化による並列処理"""
        partition_size = partition_size or self.default_partition_size
        try:
            spark = SparkSession.builder.appName(
                f"vader_calc_stats_{self.processed_at}"
            ).getOrCreate()

            # 共通のデータ準備
            office_paths_data = self._prepare_office_paths_data()
            print(f"Total office_ids: {len(office_paths_data)}")

            # office_idリストをSparkDataFrameに変換
            office_df = spark.createDataFrame(office_paths_data)

            num_partitions = min(partition_size, len(office_paths_data))
            office_df = office_df.repartition(num_partitions, "office_id")

            print(f"Repartitioned DataFrame to {office_df.rdd.getNumPartitions()} partitions.")

            # 各パーティションで並列処理を実行
            office_df.foreachPartition(
                partial(VaderStatsExecutor._process_partition_static, bucket_name=self.bucket_name)
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

    @staticmethod
    def _process_partition_static(partition_iterator, bucket_name):
        """Sparkパーティション内での処理（静的メソッド）"""
        from utils.s3_client import S3Client
        s3_client = S3Client(bucket_name)

        rows = list(partition_iterator)
        if not rows:
            return

        # パーティション内の各office_idを処理
        for row in rows:
            try:
                office_id = row['office_id']
                input_file_path = row['input_file_path']
                output_file_path = row['output_file_path']

                df = s3_client.download_pickle_from_s3(input_file_path)

                if df.empty:
                    print(f"lack of datapoint: office_id = {office_id}")
                    continue

                calculated_stats_df = calculation(df)
                s3_client.upload_pickle_to_s3(calculated_stats_df, output_file_path)
                print(f"Successfully processed office_id: {office_id}")

            except Exception as e:
                print(f"Error processing office_id {row['office_id']}: {str(e)}")
                # Sparkモードでは例外を再発生させない（他のパーティションに影響しないように）
                continue

    def _process_partition(self, partition_iterator, bucket_name):
        """Sparkパーティション内での処理（インスタンスメソッド版）"""
        return self._process_partition_static(partition_iterator, bucket_name)

    def _prepare_office_paths_data(self) -> list[dict[str, str]]:
        """office_idと入力・出力ファイルパスのマッピングを作成する共通メソッド"""
        office_ids = get_office_ids(self.processed_at.date())
        return [
            {
                "office_id": office_id,
                "input_file_path": f"{VADER_DAILY_BALANCE_PATH}{self.processed_at.date()}/{office_id}.pkl",
                "output_file_path": f"{VADER_STATS_PATH}{self.processed_at.date()}/{office_id}.pkl"
            }
            for office_id in office_ids
        ]

    def _process_single_office_data(self, office_data: dict[str, str]):
        """単一office_idの処理（逐次処理用）- 共通ビジネスロジック"""
        s3_client = S3Client(self.bucket_name)
        self._process_single_office_data_with_client(office_data, s3_client)

    def _process_single_office_data_with_client(self, office_data: dict[str, str], s3_client: S3Client):
        """単一office_idの処理（S3Clientを受け取る版）- 共通ビジネスロジック"""
        office_id = office_data["office_id"]
        input_file_path = office_data["input_file_path"]
        output_file_path = office_data["output_file_path"]

        df = s3_client.download_pickle_from_s3(input_file_path)

        if df.empty:
            print(f"lack of datapoint: office_id = {office_id}")
            return

        calculated_stats_df = calculation(df)
        s3_client.upload_pickle_to_s3(calculated_stats_df, output_file_path)
        print(f"Successfully processed office_id: {office_id}")

    def _process_office_batch(self, office_data_batch):
        """バッチ単位でのoffice_id処理"""
        s3_client = S3Client(self.bucket_name)

        for office_data in office_data_batch:
            try:
                self._process_single_office_data_with_client(office_data, s3_client)
            except Exception as e:
                print(f"Error processing office_id {office_data['office_id']}: {str(e)}")

    def _create_office_batches(self, office_paths_data, batch_size: int = 50):
        """office_paths_dataリストをバッチに分割"""
        return [
            office_paths_data[i: i + batch_size]
            for i in range(0, len(office_paths_data), batch_size)
        ]


def main():
    executor = VaderStatsExecutor()
    executor.execute()


if __name__ == "__main__":
    logger = JobExecutionLogger("vader_calc_stats")
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
