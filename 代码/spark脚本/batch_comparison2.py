# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round, col

# 1. 初始化 SparkSession
spark = SparkSession.builder \
    .appName("BatchComparison2_DailyAverageTraffic") \
    .getOrCreate()

# 2. 读取 HDFS 上的预处理数据
input_path = "hdfs://localhost:9000/user/hadoop/traffic_data/traffic_cleaned.csv"
df = spark.read.option("header", "true").csv(input_path)

# 3. 字段类型转换
df = df.withColumn("traffic", col("traffic").cast("int"))
df = df.withColumn("hour", col("hour").cast("int"))
df = df.withColumn("is_holiday", col("is_holiday").cast("int"))

print("数据加载完成，共有 {} 条记录".format(df.count()))

# 4. 按日期分组，计算每天的平均流量
result_df = df.groupBy("date", "is_holiday") \
    .agg(round(avg("traffic"), 0).alias("avg_daily_traffic")) \
    .orderBy("date")

print("日均流量计算结果：")
result_df.show(50, truncate=False)

# 5. 将结果写入 HDFS
output_path = "hdfs://localhost:9000/user/hadoop/batch_output/comparison2_daily_average"
result_df.write.mode("overwrite").option("header", "true").csv(output_path)

print("结果已写入 HDFS：{}".format(output_path))

# 6. 停止 SparkSession
spark.stop()

