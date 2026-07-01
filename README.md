# 项目名称：基于Spark与Flink的交通流量批流对比系统的设计与实现

## 一、文件清单

1. `preprocess.py` — 数据预处理脚本
2. `batch_comparison1.py` — Spark对比一：小时级峰值检测
3. `batch_comparison2.py` — Spark对比二：日均流量统计
4. `batch_comparison3.py` — Spark对比三：累计流量曲线
5. `import_to_mysql.py` — Spark结果导入MySQL
6. `pom.xml` — Flink Maven配置文件
7. `flink_comparison1.java` — Flink对比一：实时告警
8. `flink_comparison2.java` — Flink对比二：滚动24小时均值
9. `flink_comparison3.java` — Flink对比三：累计流量曲线
10. `mysql_queries.txt` — MySQL查询语句汇总

## 二、部署步骤

### 2.1 上传代码至虚拟机

（1）在章鱼平台虚拟机中创建 flinkapp 目录及子目录：

```bash
mkdir -p /home/hadoop/flinkapp/src/main/java
```

（2）将本地代码文件夹中的文件逐一上传至虚拟机对应位置：

| 文件 | 目标路径 |
|------|----------|
| `preprocess.py` | `/home/hadoop/preprocess.py` |
| `batch_comparison1.py` | `/home/hadoop/batch_comparison1.py` |
| `batch_comparison2.py` | `/home/hadoop/batch_comparison2.py` |
| `batch_comparison3.py` | `/home/hadoop/batch_comparison3.py` |
| `import_to_mysql.py` | `/home/hadoop/import_to_mysql.py` |
| `mysql_queries.txt` | `/home/hadoop/mysql_queries.txt` |
| `pom.xml` | `/home/hadoop/flinkapp/pom.xml` |
| `flink_comparison1.java` | `/home/hadoop/flinkapp/src/main/java/flink_comparison1.java` |
| `flink_comparison2.java` | `/home/hadoop/flinkapp/src/main/java/flink_comparison2.java` |
| `flink_comparison3.java` | `/home/hadoop/flinkapp/src/main/java/flink_comparison3.java` |

### 2.2 上传数据至HDFS

```bash
# 上传原始数据
# Metro_Interstate_Traffic_Volume 上传至 /home/hadoop/Metro_Interstate_Traffic_Volume.csv
```

### 2.3 准备MySQL数据库

```bash
# （1）登录MySQL（密码：hadoop）
mysql -u root -p

# （2）执行 /home/hadoop/mysql_queries.txt 中的建表语句
```

## 三、运行命令

### 3.1 数据预处理

```bash
cd /home/hadoop
python3 preprocess.py
```

### 3.2 Spark批处理作业

```bash
cd /home/hadoop
/usr/local/spark/bin/spark-submit batch_comparison1.py
/usr/local/spark/bin/spark-submit batch_comparison2.py
/usr/local/spark/bin/spark-submit batch_comparison3.py
```

### 3.3 批处理结果导入MySQL

```bash
cd /home/hadoop
/usr/local/spark/bin/spark-submit --jars /usr/share/java/mysql-connector-java.jar import_to_mysql.py
```

### 3.4 Flink流处理作业

```bash
# （1）启动Flink集群
/usr/local/flink/bin/start-cluster.sh
jps   # 验证启动成功

# （2）编译打包
cd /home/hadoop/flinkapp
mvn clean package -DskipTests

# （3）提交作业
cd /home/hadoop/flinkapp
/usr/local/flink/bin/flink run -c flink_comparison1 target/flink-traffic-comparison-1.0.jar
/usr/local/flink/bin/flink run -c flink_comparison2 target/flink-traffic-comparison-1.0.jar
/usr/local/flink/bin/flink run -c flink_comparison3 target/flink-traffic-comparison-1.0.jar
```

## 四、结果验证

### 4.1 查看HDFS输出

```bash
hdfs dfs -cat /user/hadoop/batch_output/comparison1_hourly_baseline/part-*.csv
hdfs dfs -cat /user/hadoop/batch_output/comparison2_daily_average/part-*.csv | head -10
hdfs dfs -cat /user/hadoop/batch_output/comparison3_daily_total/part-*.csv
```

### 4.2 MySQL验证查询

登录MySQL后执行 `/home/hadoop/mysql_queries.txt` 中的验证语句。

### 4.3 数据导出与可视化

```bash
# 导出6张表为CSV（在虚拟机终端执行）
mysql -u root -p traffic_db -e "SELECT * FROM batch_hourly_baseline" | sed 's/\t/,/g' > batch_hourly_baseline.csv
mysql -u root -p traffic_db -e "SELECT * FROM batch_daily_average" | sed 's/\t/,/g' > batch_daily_average.csv
mysql -u root -p traffic_db -e "SELECT * FROM batch_daily_total" | sed 's/\t/,/g' > batch_daily_total.csv
mysql -u root -p traffic_db -e "SELECT * FROM flink_alert_result" | sed 's/\t/,/g' > flink_alert_result.csv
mysql -u root -p traffic_db -e "SELECT * FROM flink_cumulative_result" | sed 's/\t/,/g' > flink_cumulative_result.csv
mysql -u root -p traffic_db -e "SELECT * FROM flink_rolling_avg_result" | sed 's/\t/,/g' > flink_rolling_avg_result.csv
```

将导出的CSV文件下载到本地，使用PyCharm运行 `visualize.py` 生成四张核心图表。

## 五、目录结构总览

```
/home/hadoop/
├── preprocess.py
├── batch_comparison1.py
├── batch_comparison2.py
├── batch_comparison3.py
├── import_to_mysql.py
├── mysql_queries.txt
├── traffic_cleaned.csv          （运行preprocess.py后生成）
├── flinkapp/
│   ├── pom.xml
│   └── src/main/java/
│       ├── flink_comparison1.java
│       ├── flink_comparison2.java
│       └── flink_comparison3.java
└── 导出CSV文件（运行后生成）/
    ├── batch_hourly_baseline.csv
    ├── batch_daily_average.csv
    ├── batch_daily_total.csv
    ├── flink_alert_result.csv
    ├── flink_cumulative_result.csv
    └── flink_rolling_avg_result.csv
```
