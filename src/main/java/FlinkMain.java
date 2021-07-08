import kafka_utils.KafkaConfigurations;
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
import queries.operators.QueryOperators;
import queries.query1.Query1;
import queries.query2.Query2;
import utils.ShipInfo;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;

/**
 * Initialization of the Flink DS Application
 */
public class FlinkMain {

    private static String CONSUMER_GROUP = "single_flink_consumer";

    public static void main(String[] args) {

        // setup flink environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //Use Event Time
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // add the source and handle watermarks
        Properties properties = KafkaConfigurations.getSourceProperties(CONSUMER_GROUP);

        DataStream<Tuple2<Long, String>> streamSource = environment
                //Use Kafka as DataStream source system
                .addSource(new FlinkKafkaConsumer<>(KafkaConfigurations.FLINK_TOPIC, new SimpleStringSchema(), properties))
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
                // assign timestamp to enable watermarking system
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Long, String> tuple) {
                        // specify event time
                        // kafka's auto-watermarks generation is only related to offset not to event time
                        //event time must be specified explicitly
                        return tuple.f0;
                    }
                })
                .name("stream-source-extractor");

        // parsing tuples to obtain the needed information; ignoring all malformed lines.
        DataStream<ShipInfo> stream =   streamSource.flatMap(QueryOperators.parseInputFunction())
                .name("stream-query-parser");

        //query 1 topology builder
        Query1.buildTopology(stream);
        //query 2 topology builder
        Query2.buildTopology(stream);

        try {
            //print on stdout the execution plan to obtain the DSP-DAG (json-format)
            System.out.println(environment.getExecutionPlan());
            //trigger the execution of the DSP application
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
