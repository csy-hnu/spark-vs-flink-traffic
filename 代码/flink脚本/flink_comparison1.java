import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class flink_comparison1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        System.out.println("Flink 对比一：小时级峰值检测启动...");

        // 注册 MySQL 基准数据表
        tEnv.executeSql(
            "CREATE TABLE hourly_baseline (" +
            "  is_holiday INT," +
            "  `hour` INT," +
            "  avg_traffic DOUBLE" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = 'jdbc:mysql://localhost:3306/traffic_db?useSSL=false&serverTimezone=UTC'," +
            "  'table-name' = 'batch_hourly_baseline'," +
            "  'username' = 'root'," +
            "  'password' = 'hadoop'," +
            "  'driver' = 'com.mysql.cj.jdbc.Driver'" +
            ")"
        );
        System.out.println("MySQL 基准表注册成功");

        // 注册 HDFS 流量数据表
        tEnv.executeSql(
            "CREATE TABLE traffic_source (" +
            "  date_time STRING," +
            "  traffic INT," +
            "  is_holiday INT," +
            "  `hour` INT," +
            "  `date` STRING" +
            ") WITH (" +
            "  'connector' = 'filesystem'," +
            "  'path' = 'hdfs://localhost:9000/user/hadoop/traffic_data/traffic_cleaned.csv'," +
            "  'format' = 'csv'," +
            "  'csv.field-delimiter' = ','," +
            "  'csv.ignore-parse-errors' = 'true'" +
            ")"
        );
        System.out.println("HDFS 流量表注册成功");

        // 注册 MySQL 结果表
        tEnv.executeSql(
            "CREATE TABLE alert_result (" +
            "  date_time STRING," +
            "  traffic INT," +
            "  baseline DOUBLE," +
            "  is_holiday INT," +
            "  `hour` INT," +
            "  alert_status STRING" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = 'jdbc:mysql://localhost:3306/traffic_db?useSSL=false&serverTimezone=UTC'," +
            "  'table-name' = 'flink_alert_result'," +
            "  'username' = 'root'," +
            "  'password' = 'hadoop'," +
            "  'driver' = 'com.mysql.cj.jdbc.Driver'" +
            ")"
        );
        System.out.println("MySQL 结果表注册成功");

        // 执行 JOIN 和检测
        System.out.println("开始执行 JOIN 和告警检测...");
        TableResult result = tEnv.executeSql(
            "INSERT INTO alert_result " +
            "SELECT " +
            "  t.date_time," +
            "  t.traffic," +
            "  b.avg_traffic AS baseline," +
            "  t.is_holiday," +
            "  t.`hour`," +
            "  CASE " +
            "    WHEN t.traffic > b.avg_traffic * 1.5 THEN 'HIGH_ALERT'" +
            "    WHEN t.traffic < b.avg_traffic * 0.5 THEN 'LOW_ALERT'" +
            "    ELSE 'NORMAL'" +
            "  END AS alert_status " +
            "FROM traffic_source t " +
            "JOIN hourly_baseline b " +
            "ON t.is_holiday = b.is_holiday AND t.`hour` = b.`hour`"
        );

        result.await();
        System.out.println("Flink 对比一：小时级峰值检测完成！");
    }
}
