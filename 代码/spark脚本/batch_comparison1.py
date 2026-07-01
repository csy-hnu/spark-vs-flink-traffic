# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round, col

# 1. 初始化 SparkSession
spark = SparkSession.builder \
    .appName("BatchComparison1_HourlyPeakDetection") \
    .getOrCreate()

# 2. 读取 HDFS 上的预处理数据
input_path = "hdfs://localhost:9000/user/hadoop/traffic_data/traffic_cleaned.csv"
df = spark.read.option("header", "true").csv(input_path)

# 3. 先打印原始列名，确认字段名称
print("原始列名：")
print(df.columns)

# 4. 字段类型转换
df = df.withColumn("traffic", col("traffic").cast("int"))
df = df.withColumn("hour", col("hour").cast("int"))
df = df.withColumn("is_holiday", col("is_holiday").cast("int"))

print("数据加载完成，共有 {} 条记录".format(df.count()))
print("数据 Schema:")
df.printSchema()


# 5. 按 is_holiday 和 hour 分组，计算每个小时的平均流量
result_df = df.groupBy("is_holiday", "hour") \
    .agg(round(avg("traffic"), 0).alias("avg_traffic")) \
    .orderBy("is_holiday", "hour")

print("小时级峰值检测结果（48条基准值）：")
result_df.show(48, truncate=False)

# 6. 将结果写入 HDFS
output_path = "hdfs://localhost:9000/user/hadoop/batch_output/comparison1_hourly_baseline"
result_df.write.mode("overwrite").option("header", "true").csv(output_path)

print("结果已写入 HDFS：{}".format(output_path))

# 7. 停止 SparkSession
spark.stop()

