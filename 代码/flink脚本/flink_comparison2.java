import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class flink_comparison2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("Flink 对比二：滚动24小时平均流量启动...");

        DataStream<String> rawStream = env.readTextFile(
            "hdfs://localhost:9000/user/hadoop/traffic_data/traffic_cleaned.csv"
        );

        DataStream<AvgResult> results = rawStream
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
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                TrafficRecord record = new TrafficRecord();
                record.dateTime = fields[0];
                record.traffic = Integer.parseInt(fields[1].trim());
                record.isHoliday = Integer.parseInt(fields[2].trim());
                record.hour = Integer.parseInt(fields[3].trim());
                record.timestamp = sdf.parse(fields[0]).getTime();
                return record;
            })
            .keyBy(r -> 1)
            .process(new GlobalRolling24HourAverage());

        results.addSink(
            JdbcSink.sink(
                "INSERT INTO `flink_rolling_avg_result` (`current_time`, `window_start`, `avg_traffic`, `data_count`) VALUES (?, ?, ?, ?)",
                new JdbcStatementBuilder<AvgResult>() {
                    @Override
                    public void accept(PreparedStatement ps, AvgResult result) throws SQLException {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        ps.setString(1, sdf.format(new Date(result.currentTime)));
                        ps.setString(2, sdf.format(new Date(result.windowStart)));
                        ps.setDouble(3, result.avgTraffic);
                        ps.setInt(4, result.dataCount);
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

        env.execute("Flink Comparison 2: Rolling 24-Hour Average");
        System.out.println("Flink 对比二：滚动24小时平均流量完成！");
    }

    public static class TrafficRecord {
        public String dateTime;
        public int traffic;
        public int isHoliday;
        public int hour;
        public long timestamp;
    }

    public static class AvgResult {
        public long currentTime;
        public long windowStart;
        public double avgTraffic;
        public int dataCount;
    }

    public static class GlobalRolling24HourAverage 
        extends KeyedProcessFunction<Integer, TrafficRecord, AvgResult> {

        private transient ListState<TrafficRecord> bufferState;
        private static final long WINDOW_SIZE_MS = 24 * 60 * 60 * 1000L;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<TrafficRecord> descriptor = new ListStateDescriptor<>(
                "bufferState",
                TypeInformation.of(TrafficRecord.class)
            );
            bufferState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(
                TrafficRecord record,
                Context ctx,
                Collector<AvgResult> out) throws Exception {

            bufferState.add(record);
            long currentTime = record.timestamp;

            List<TrafficRecord> allRecords = new ArrayList<>();
            for (TrafficRecord r : bufferState.get()) {
                if (currentTime - r.timestamp <= WINDOW_SIZE_MS) {
                    allRecords.add(r);
                }
            }

            allRecords.sort((a, b) -> Long.compare(b.timestamp, a.timestamp));

            if (allRecords.size() >= 24) {
                List<TrafficRecord> validRecords = allRecords.subList(0, 24);

                double sum = 0;
                for (TrafficRecord r : validRecords) {
                    sum += r.traffic;
                }
                // 保留两位小数
                double avgTraffic = Math.round(sum / 24.0 * 100.0) / 100.0;

                AvgResult result = new AvgResult();
                result.currentTime = currentTime;
                result.windowStart = validRecords.get(23).timestamp;
                result.avgTraffic = avgTraffic;
                result.dataCount = 24;
                out.collect(result);
            }
        }
    }
}
