from pyspark.sql import SparkSession
import sys
import json
import os
import pandas as pd
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, ArrayType

spark = SparkSession.builder.appName("Dynamic_spark_json").getOrCreate()

dt = sys.argv[1]

file_path = f'/home/joo/data/movies/year={dt}/data.parquet'

if os.path.isfile(file_path):
    print(f"데이터 이미 존재 : {file_path}")
else:
    jdf = spark.read.option("multiline","true").json(f"/home/joo/data/movies/year={dt}/airflow_data.json")
    ## companys, directors 값이 다중으로 들어가 있는 경우 찾기 위해 count 컬럼 추가
    from pyspark.sql.functions import explode, col, size, explode_outer
    ccdf = jdf.withColumn("company_count", size("companys")).withColumn("directors_count", size("directors"))

    # companys를 여러개의  company로 펼치기(companys 배열을 행으로 확장)
    edf = ccdf.withColumn("company", explode_outer("companys"))

    # 전 단계에 이어서 diresctors를 여러개의 director로 다시한번  펼치기
    eedf = edf.withColumn("director", explode_outer("directors"))
    eedf.write.mode("append").parquet(file_path) # parquet형식으로 저장
