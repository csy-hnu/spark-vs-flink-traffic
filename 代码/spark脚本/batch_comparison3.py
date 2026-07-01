# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col

# 1. 初始化 SparkSession
spark = SparkSession.builder \
    .appName("BatchComparison3_DailyTotalTraffic") \
    .getOrCreate()

# 2. 读取 HDFS 上的预处理数据
input_path = "hdfs://localhost:9000/user/hadoop/traffic_data/traffic_cleaned.csv"
df = spark.read.option("header", "true").csv(input_path)

# 3. 字段类型转换
df = df.withColumn("traffic", col("traffic").cast("int"))
df = df.withColumn("hour", col("hour").cast("int"))
df = df.withColumn("is_holiday", col("is_holiday").cast("int"))

print("数据加载完成，共有 {} 条记录".format(df.count()))

# 4. 选出非节假日的一天和节假日的一天
selected_dates = ["2012-10-10", "2012-12-25"]

# 筛选出这两天的数据，按日期分组计算总流量
result_df = df.filter(col("date").isin(selected_dates)) \
    .groupBy("date", "is_holiday") \
    .agg(spark_sum("traffic").alias("total_daily_traffic")) \
    .orderBy("date")

print("选中两天的全天总流量：")
result_df.show(truncate=False)

# 5. 将结果写入 HDFS
output_path = "hdfs://localhost:9000/user/hadoop/batch_output/comparison3_daily_total"
result_df.write.mode("overwrite").option("header", "true").csv(output_path)

print("结果已写入 HDFS：{}".format(output_path))

# 6. 停止 SparkSession
spark.stop()

