# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# 1. 初始化 SparkSession
spark = SparkSession.builder \
    .appName("ImportResultsToMySQL") \
    .getOrCreate()

# 2. MySQL 连接配置
mysql_url = "jdbc:mysql://localhost:3306/traffic_db?useSSL=false&serverTimezone=UTC"
mysql_properties = {
    "user": "root",
    "password": "hadoop",
    "driver": "com.mysql.jdbc.Driver"
}

# 3. 导入对比一结果：小时级峰值检测
print("正在导入对比一结果...")
df1 = spark.read.option("header", "true") \
    .csv("hdfs://localhost:9000/user/hadoop/batch_output/comparison1_hourly_baseline")
df1.write.mode("overwrite").jdbc(url=mysql_url, table="batch_hourly_baseline", properties=mysql_properties)
print("对比一导入完成，共 {} 条记录".format(df1.count()))

# 4. 导入对比二结果：日均流量
print("正在导入对比二结果...")
df2 = spark.read.option("header", "true") \
    .csv("hdfs://localhost:9000/user/hadoop/batch_output/comparison2_daily_average")
df2.write.mode("overwrite").jdbc(url=mysql_url, table="batch_daily_average", properties=mysql_properties)
print("对比二导入完成，共 {} 条记录".format(df2.count()))

# 5. 导入对比三结果：累计流量
print("正在导入对比三结果...")
df3 = spark.read.option("header", "true") \
    .csv("hdfs://localhost:9000/user/hadoop/batch_output/comparison3_daily_total")
df3.write.mode("overwrite").jdbc(url=mysql_url, table="batch_daily_total", properties=mysql_properties)
print("对比三导入完成，共 {} 条记录".format(df3.count()))

print("所有结果已成功导入 MySQL！")

# 6. 停止 SparkSession
spark.stop()

