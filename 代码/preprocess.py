# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

# 1. 读取原始数据
input_file = "/home/hadoop/Metro_Interstate_Traffic_Volume.csv"
output_file = "/home/hadoop/traffic_cleaned.csv"

df = pd.read_csv(input_file)

print("原始数据行数: {}".format(len(df)))
print("原始字段: {}".format(list(df.columns)))

# 2. 字段筛选
df = df[["date_time", "traffic_volume", "holiday"]]

# 3. 时间格式转换
df["date_time"] = pd.to_datetime(df["date_time"])
df["date"] = df["date_time"].dt.date

# 4. 节假日标记：按天统一处理
daily_holiday = df.groupby("date")["holiday"].apply(
    lambda x: "None" if all(v == "None" for v in x) else x[x != "None"].iloc[0]
).reset_index()
daily_holiday.columns = ["date", "holiday"]

# 合并回原数据
df = df.merge(daily_holiday, on="date", suffixes=("_orig", ""))
df.drop("holiday_orig", axis=1, inplace=True)

# 5. 节假日转 0/1
df["is_holiday"] = df["holiday"].apply(lambda x: 0 if x == "None" else 1)
df.drop("holiday", axis=1, inplace=True)

# 6. 去重处理
df.drop_duplicates(inplace=True)
df = df.groupby(["date_time", "is_holiday"], as_index=False)["traffic_volume"].mean()

# 7. 提取 hour
df["hour"] = df["date_time"].dt.hour
df["date"] = df["date_time"].dt.date

# 8. 时间序列完整性检查与插值补全
start_time = df["date_time"].min()
end_time = df["date_time"].max()

full_time_range = pd.date_range(start=start_time, end=end_time, freq="H")
full_df = pd.DataFrame({"date_time": full_time_range})
full_df["date"] = full_df["date_time"].dt.date
full_df["hour"] = full_df["date_time"].dt.hour

df_full = full_df.merge(df, on=["date_time", "date", "hour"], how="left")

# 对缺失的 traffic 进行线性插值
df_full["traffic_volume"] = df_full["traffic_volume"].interpolate(method="linear")

# 9. 补全 is_holiday 字段
daily_holiday_map = df.groupby("date")["is_holiday"].first().to_dict()
df_full["is_holiday"] = df_full["date"].map(daily_holiday_map)

# 10. 【新增】删除 is_holiday 仍为空的记录（孤立日期无法补全）
before_count = len(df_full)
df_full = df_full.dropna(subset=["is_holiday"])
after_count = len(df_full)
print("删除 is_holiday 为空的记录：{} -> {}，删除了 {} 条".format(before_count, after_count, before_count - after_count))

# 11. 删除第一天所有记录（无法补全）
first_date = df_full["date"].min()
df_full = df_full[df_full["date"] != first_date]

# 12. 调整字段顺序并重命名
df_final = df_full[["date_time", "traffic_volume", "is_holiday", "hour", "date"]]
df_final.rename(columns={"traffic_volume": "traffic"}, inplace=True)

# 13. 对 traffic 字段四舍五入取整
df_final["traffic"] = df_final["traffic"].round(0).astype(int)

# 14. 重置索引
df_final.reset_index(drop=True, inplace=True)

# 15. 输出清洗后的数据
df_final.to_csv(output_file, index=False)

print("清洗后数据行数: {}".format(len(df_final)))
print("保留字段: {}".format(list(df_final.columns)))
print("前10行预览:")
print(df_final.head(10))
print("数据已保存至: {}".format(output_file))

