import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F
import argparse

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


def process_bronze_table(snapshot_date_str, bronze_lms_directory, spark):
    # prepare arguments
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    # connect to source back end - IRL connect to back end source system
    csv_file_path = "data/lms_loan_daily.csv"
    csv_file_clickstream = 'data/feature_clickstream.csv'
    csv_file_attributes = 'data/feature_attributes.csv'
    csv_file_financials = 'data/feature_financials.csv'

    # load data - IRL ingest from back end source system
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True).filter(col('snapshot_date') == lit(snapshot_date))
    df_clickstream = spark.read.csv(csv_file_clickstream, header=True, inferSchema=True).filter(col('snapshot_date') == lit(snapshot_date))
    df_financials  = spark.read.csv(csv_file_financials, header=True, inferSchema=True).filter(col('snapshot_date') == lit(snapshot_date))
    df_attributes  = spark.read.csv(csv_file_attributes, header=True, inferSchema=True).filter(col('snapshot_date') == lit(snapshot_date))
    
    print(f"[{snapshot_date_str}] rows : "
          f"lms={df.count()}, clickstream={df_clickstream.count()}, "
          f"financials={df_financials.count()}, attributes={df_attributes.count()}")

    sub = snapshot_date_str.replace('-', '_')
    os.makedirs(bronze_lms_directory, exist_ok=True)
    
    df.write.mode("overwrite").option("header","true").csv(os.path.join(bronze_lms_directory, f"bronze_loan_daily_{sub}"))
    base_dir = os.path.dirname(os.path.dirname(bronze_lms_directory)) or "datamart/bronze"
    out_click = os.path.join(base_dir, "clickstream", f"snapshot_date={sub}")
    out_fin   = os.path.join(base_dir, "financials",  f"snapshot_date={sub}")
    out_attr  = os.path.join(base_dir, "attributes",  f"snapshot_date={sub}")

    return df
