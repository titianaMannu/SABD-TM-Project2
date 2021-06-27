import kafka_utils.KafkaClusterConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import queries.query1.Query1;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;

public class FlinkMain {
    //todo change this parameters every names
    private static String CONSUMER_GROUP_ID = "single-flink-consumer";

    public static void main(String[] args) {

        // setup flink environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // add the source and handle watermarks
        Properties props = KafkaClusterConfig.getFlinkSourceProperties(CONSUMER_GROUP_ID);

        DataStream<Tuple2<Long, String>> stream = environment
                .addSource(new FlinkKafkaConsumer<>(KafkaClusterConfig.FLINK_TOPIC, new SimpleStringSchema(), props))
                // extract event timestamp and set it as key
                .flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<Long, String>> collector) {
                        String[] info = s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm", Locale.US);
                        try {
                            collector.collect(new Tuple2<>(format.parse(info[7]).getTime(), s));
                        } catch (ParseException ignored) {
                        }
                    }
                })
                // assign timestamp to every tuple to enable watermarking system
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Long, String> tuple) {
                        // specify event time
                        // kafka's auto-watermarks generation is only related to offset not to event time
                        return tuple.f0;
                    }
                })
                .name("stream-source");

        Query1.buildTopology(stream);

        try {
            System.out.println(environment.getExecutionPlan());
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
