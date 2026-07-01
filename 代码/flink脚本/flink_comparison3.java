import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.text.SimpleDateFormat;
import java.util.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class flink_comparison3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("Flink 对比三：累计流量曲线启动...");

        // 目标日期
        Set<String> targetDates = new HashSet<>(Arrays.asList("2012-10-10", "2012-12-25"));

        DataStream<String> rawStream = env.readTextFile(
            "hdfs://localhost:9000/user/hadoop/traffic_data/traffic_cleaned.csv"
        );

        DataStream<CumulativeResult> results = rawStream
            .filter(new org.apache.flink.api.common.functions.FilterFunction<String>() {
                private boolean isFirstLine = true;

                @Override
                public boolean filter(String line) throws Exception {
                    if (isFirstLine) {
                        isFirstLine = false;
                        return false;
                    }
                    return true;
                }
            })
            .map(line -> {
                String[] fields = line.split(",");
                String dateStr = fields[0].substring(0, 10);
                int hour = Integer.parseInt(fields[3].trim());
                int traffic = Integer.parseInt(fields[1].trim());
                int isHoliday = Integer.parseInt(fields[2].trim());

                return new TrafficRecord(dateStr, hour, traffic, isHoliday);
            })
            .filter(record -> targetDates.contains(record.date))
            .keyBy(record -> record.date + "_" + record.isHoliday)
            .process(new CumulativeProcessFunction());

        results.addSink(
            JdbcSink.sink(
                "INSERT INTO `flink_cumulative_result` (`date`, `hour`, `cumulative_traffic`, `is_holiday`) VALUES (?, ?, ?, ?)",
                new JdbcStatementBuilder<CumulativeResult>() {
                    @Override
                    public void accept(PreparedStatement ps, CumulativeResult result) throws SQLException {
                        ps.setString(1, result.date);
                        ps.setInt(2, result.hour);
                        ps.setLong(3, result.cumulativeTraffic);
                        ps.setInt(4, result.isHoliday);
                    }
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(100)
                    .withBatchIntervalMs(200)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl("jdbc:mysql://localhost:3306/traffic_db?useSSL=false&serverTimezone=UTC")
                    .withDriverName("com.mysql.cj.jdbc.Driver")
                    .withUsername("root")
                    .withPassword("hadoop")
                    .build()
            )
        );

        env.execute("Flink Comparison 3: Cumulative Traffic Curve");
        System.out.println("Flink 对比三：累计流量曲线完成！");
    }

    public static class TrafficRecord {
        public String date;
        public int hour;
        public int traffic;
        public int isHoliday;

        public TrafficRecord() {}

        public TrafficRecord(String date, int hour, int traffic, int isHoliday) {
            this.date = date;
            this.hour = hour;
            this.traffic = traffic;
            this.isHoliday = isHoliday;
        }
    }

    public static class CumulativeResult {
        public String date;
        public int hour;
        public long cumulativeTraffic;
        public int isHoliday;
    }

    public static class CumulativeProcessFunction 
        extends KeyedProcessFunction<String, TrafficRecord, CumulativeResult> {

        private transient ListState<TrafficRecord> bufferState;
        private static final long serialVersionUID = 1L;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<TrafficRecord> descriptor = new ListStateDescriptor<>(
                "bufferState",
                Types.POJO(TrafficRecord.class)
            );
            bufferState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(TrafficRecord record, Context ctx, Collector<CumulativeResult> out) throws Exception {
            // 1. 将当前记录加入缓存
            bufferState.add(record);

            // 2. 只处理第23小时（最后一个小时）的数据
            if (record.hour == 23) {
                // 3. 获取所有记录
                List<TrafficRecord> allRecords = new ArrayList<>();
                for (TrafficRecord r : bufferState.get()) {
                    allRecords.add(r);
                }

                // 4. 按小时排序
                allRecords.sort((a, b) -> Integer.compare(a.hour, b.hour));

                // 5. 去重：只保留每个小时的最新一条记录
                Map<Integer, TrafficRecord> latestPerHour = new LinkedHashMap<>();
                for (TrafficRecord r : allRecords) {
                    latestPerHour.put(r.hour, r);
                }

                // 6. 计算累计值并输出
                long cumulative = 0;
                for (Map.Entry<Integer, TrafficRecord> entry : latestPerHour.entrySet()) {
                    cumulative += entry.getValue().traffic;
                    
                    CumulativeResult result = new CumulativeResult();
                    result.date = record.date;
                    result.hour = entry.getKey();
                    result.cumulativeTraffic = cumulative;
                    result.isHoliday = record.isHoliday;
                    out.collect(result);
                }

                // 7. 清空缓存，避免下次重复输出
                bufferState.clear();
            }
        }
    }
}
