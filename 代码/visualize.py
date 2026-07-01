# visualize_core_4charts.py
# 4 Core Charts - Correct Order

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# ==================== Global Settings ====================
plt.rcParams['font.sans-serif'] = ['Arial', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
sns.set_style("whitegrid")

# Color definitions
COLOR1 = '#2E86AB'  # Blue - Non-holiday / Spark
COLOR2 = '#A23B72'  # Purple - Holiday / Flink
COLOR3 = '#F18F01'  # Orange - Alert
COLOR4 = '#6A994E'  # Green - Normal
COLOR5 = '#C73E1D'  # Red - LOW_ALERT
print("=" * 60)
print("Generating 4 Core Charts (Display Only)...")
print("Close each chart window to see the next one")
print("=" * 60)

# ==================== Load Data ====================
print("\nLoading data...")
df_baseline = pd.read_csv('batch_hourly_baseline.csv')
df_alerts = pd.read_csv('flink_alert_result.csv')
df_cumulative = pd.read_csv('flink_cumulative_result.csv')
df_rolling = pd.read_csv('flink_rolling_avg_result.csv')
df_daily_avg = pd.read_csv('batch_daily_average.csv')

print(f"  ✓ batch_hourly_baseline: {len(df_baseline)} records")
print(f"  ✓ flink_alert_result: {len(df_alerts)} records")
print(f"  ✓ flink_cumulative_result: {len(df_cumulative)} records")
print(f"  ✓ flink_rolling_avg_result: {len(df_rolling)} records")

# ==================== Chart 1: Holiday vs Non-Holiday Difference ====================
print("\n[Chart 1] Holiday vs Non-Holiday Traffic Difference...")

pivot_df = df_baseline.pivot(index='hour', columns='is_holiday', values='avg_traffic')
pivot_df.columns = ['Non-Holiday', 'Holiday']
pivot_df['Difference'] = pivot_df['Non-Holiday'] - pivot_df['Holiday']
pivot_df['Diff_Percent'] = (pivot_df['Difference'] / pivot_df['Non-Holiday']) * 100

plt.figure(figsize=(14, 6))

colors = [COLOR1 if x >= 0 else COLOR2 for x in pivot_df['Difference']]
bars = plt.bar(pivot_df.index, pivot_df['Difference'], color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)

plt.xlabel('Hour', fontsize=12)
plt.ylabel('Traffic Difference (Non-Holiday - Holiday)', fontsize=12)
plt.title('Chart 1: Holiday Impact on Traffic Volume (Comparison 1)', fontsize=14, fontweight='bold')
plt.xticks(range(0, 24, 2))
plt.grid(True, alpha=0.3, axis='y')

for bar, (hour, diff) in zip(bars, pivot_df['Difference'].items()):
    if abs(diff) > 500:
        plt.annotate(f'{int(diff)}', xy=(hour, diff),
                     xytext=(hour, diff + 80 if diff > 0 else diff - 80),
                     ha='center', fontsize=8, fontweight='bold')

plt.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
plt.tight_layout()
plt.show()
print("  ✓ Chart 1 displayed")

# ==================== Chart 2: Alert Analysis (Fixed - Split by Holiday) ====================
print("\n[Chart 2] Alert Analysis...")

fig, axes = plt.subplots(1, 3, figsize=(20, 6))  # 3个子图

# Left: Pie chart (overall)
alert_counts = df_alerts['alert_status'].value_counts()
colors_pie = [COLOR3, COLOR5, COLOR4]
explode = (0.05, 0.05, 0)
wedges, texts, autotexts = axes[0].pie(alert_counts.values, labels=alert_counts.index,
                                     autopct='%1.1f%%', colors=colors_pie, explode=explode,
                                     startangle=90, textprops={'fontsize': 11})
for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontweight('bold')
axes[0].set_title('(a) Overall Alert Distribution', fontsize=12, fontweight='bold')

# Middle: Heatmap for Non-Holiday (is_holiday = 0)
df_non_holiday = df_alerts[df_alerts['is_holiday'] == 0]
hourly_non = df_non_holiday.groupby(['hour', 'alert_status']).size().unstack(fill_value=0)
for col in ['HIGH_ALERT', 'LOW_ALERT', 'NORMAL']:
    if col not in hourly_non.columns:
        hourly_non[col] = 0
hourly_non = hourly_non[['HIGH_ALERT', 'LOW_ALERT', 'NORMAL']]

sns.heatmap(hourly_non.T, cmap='YlOrRd', annot=False, fmt='d',
            xticklabels=range(0, 24), cbar_kws={'label': 'Alert Count'}, ax=axes[1],
            annot_kws={'size': 8})
axes[1].set_xlabel('Hour', fontsize=11)
axes[1].set_ylabel('Alert Status', fontsize=11)
axes[1].set_title('(b) Non-Holiday Alert Distribution', fontsize=12, fontweight='bold')
axes[1].set_xticklabels(range(0, 24), rotation=45, ha='right', fontsize=9)

# Right: Heatmap for Holiday (is_holiday = 1)
df_holiday = df_alerts[df_alerts['is_holiday'] == 1]
hourly_hol = df_holiday.groupby(['hour', 'alert_status']).size().unstack(fill_value=0)
for col in ['HIGH_ALERT', 'LOW_ALERT', 'NORMAL']:
    if col not in hourly_hol.columns:
        hourly_hol[col] = 0
hourly_hol = hourly_hol[['HIGH_ALERT', 'LOW_ALERT', 'NORMAL']]

sns.heatmap(hourly_hol.T, cmap='YlOrRd', annot=False, fmt='d',
            xticklabels=range(0, 24), cbar_kws={'label': 'Alert Count'}, ax=axes[2],
            annot_kws={'size': 8})
axes[2].set_xlabel('Hour', fontsize=11)
axes[2].set_ylabel('Alert Status', fontsize=11)
axes[2].set_title('(c) Holiday Alert Distribution', fontsize=12, fontweight='bold')
axes[2].set_xticklabels(range(0, 24), rotation=45, ha='right', fontsize=9)

plt.suptitle('Chart 2: Alert Analysis - Separated by Holiday (Comparison 1)', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# ==================== Chart 3: Flink Rolling Average Convergence ====================
print("\n[Chart 3] Flink Rolling Average Convergence...")

target_date = '2012-10-10'
df_rolling['date'] = pd.to_datetime(df_rolling['current_time']).dt.strftime('%Y-%m-%d')
df_rolling_target = df_rolling[df_rolling['date'] == target_date].reset_index(drop=True)

spark_daily_avg = df_daily_avg[df_daily_avg['date'] == target_date]['avg_daily_traffic'].values[0]

plt.figure(figsize=(14, 6))
plt.plot(range(len(df_rolling_target)), df_rolling_target['avg_traffic'],
         marker='.', linewidth=1.8, color=COLOR3, label='Flink Rolling Average (24h window)', markersize=5)
plt.axhline(y=spark_daily_avg, color=COLOR1, linestyle='--', linewidth=2.5,
            label=f'Spark Exact Value (Batch) = {spark_daily_avg:.0f}')

# Highlight convergence area
convergence_idx = 20
plt.axvspan(convergence_idx - 3, convergence_idx + 5, alpha=0.1, color=COLOR1)
plt.annotate('Convergence Zone', xy=(convergence_idx, spark_daily_avg),
             xytext=(convergence_idx - 8, spark_daily_avg + 300),
             arrowprops=dict(arrowstyle='->', color='gray', lw=1),
             fontsize=10, fontweight='bold')

plt.xlabel('Time Sequence (Hourly Update)', fontsize=12)
plt.ylabel('Average Traffic', fontsize=12)
plt.title(f'Chart 3: Flink Rolling Average Convergence (Comparison 2 - {target_date})', fontsize=14, fontweight='bold')
plt.legend(fontsize=11, loc='lower right')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
print("  ✓ Chart 3 displayed")

# ==================== Chart 4: Cumulative Traffic Curve ====================
print("\n[Chart 4] Cumulative Traffic Curve...")

plt.figure(figsize=(14, 6))

for date, label, color in [('2012-10-10', 'Non-Holiday (2012-10-10)', COLOR1),
                            ('2012-12-25', 'Holiday (2012-12-25)', COLOR2)]:
    data = df_cumulative[df_cumulative['date'] == date]
    plt.plot(data['hour'], data['cumulative_traffic'], marker='o', label=label,
             color=color, linewidth=2.5, markersize=6)
    # Annotate start and end points
    start_val = data['cumulative_traffic'].iloc[0]
    end_val = data['cumulative_traffic'].iloc[-1]
    plt.annotate(f'Start: {start_val}', xy=(0, start_val), xytext=(0, start_val - 800),
                 fontsize=9, ha='center', fontweight='bold', color=color)
    plt.annotate(f'End: {end_val}', xy=(23, end_val), xytext=(21.5, end_val + 800),
                 fontsize=9, ha='center', fontweight='bold', color=color)

plt.xlabel('Hour', fontsize=12)
plt.ylabel('Cumulative Traffic', fontsize=12)
plt.title('Chart 4: Cumulative Traffic Growth Curve (Comparison 3)', fontsize=14, fontweight='bold')
plt.legend(fontsize=11)
plt.xticks(range(0, 24, 2))
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
print("  ✓ Chart 4 displayed")

# ==================== Summary ====================
print("\n" + "=" * 60)
print("✅ All 4 core charts displayed successfully!")
print("=" * 60)
print("\nChart List (Correct Order):")
print("  1. Chart 1: Holiday vs Non-Holiday Traffic Difference (Comparison 1)")
print("  2. Chart 2: Alert Analysis - Pie Chart + Heatmap (Comparison 1)")
print("  3. Chart 3: Flink Rolling Average Convergence (Comparison 2)")
print("  4. Chart 4: Cumulative Traffic Growth Curve (Comparison 3)")
print("\n💡 Close each chart window to view the next one")
print("=" * 60)