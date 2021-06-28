package queries.query1;

import kafka_utils.FlinkStringToKafkaSerializer;
import kafka_utils.KafkaClusterConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import queries.operators.QueryOperators;
import queries.windows.MonthlyTumblingEventTimeWindow;
import queries.windows.WeeklyTumblingEventTimeWindow;
import utils.ConfStrings;
import utils.ShipInfo;


public class Query1 {

    public static void buildTopology(DataStream<Tuple2<Long, String>> source) {
        // parse tuples to obtain the needed information and ignoring all malformed lines;
        DataStream<ShipInfo> stream = source
                .flatMap(QueryOperators.parseInputFunction())
                .name("stream-query1-decoder")
                .filter(QueryOperators.filterRegion())
                .name("filter-per-western-sea"); // only western mediterranean

        DataStream<ShipInfo> areaKeyStreamed = stream
                .map(QueryOperators.computeCellId()).name("compute-cell-id");

        //weekly window
        areaKeyStreamed.keyBy(ShipInfo::getCellId) // stream keyed by cellID
                .window(new WeeklyTumblingEventTimeWindow(7, 4, WindowStagger.ALIGNED)) //weekly tumbling window
                .aggregate(new ShipAvgAggregator(), new ShipAvgProcessWindow())
                .name("query1-weekly-window-avg")
                .map(QueryOperators.ExportQuery1OutcomeToString())
                // write the output string to the correct topic in kafka
                .addSink(new FlinkKafkaProducer<>(ConfStrings.FLINK_QUERY1_WEEKLY_OUT_TOPIC.getString(),
                        new FlinkStringToKafkaSerializer(ConfStrings.FLINK_QUERY1_WEEKLY_OUT_TOPIC.getString()),
                        KafkaClusterConfig.getFlinkSinkProperties("producer" +
                                ConfStrings.FLINK_QUERY1_WEEKLY_OUT_TOPIC.getString()),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query1-weekly-avg-sink");

        //monthly window
        areaKeyStreamed.keyBy(ShipInfo::getCellId) // stream keyed by cellID
                .window(new MonthlyTumblingEventTimeWindow(1, 0, WindowStagger.ALIGNED)) //monthly tumbling window
                .aggregate(new ShipAvgAggregator(), new ShipAvgProcessWindow())
                .name("query1-monthly-window-avg")
                .map(QueryOperators.ExportQuery1OutcomeToString())
                // write the output string to the correct topic in kafka
                .addSink(new FlinkKafkaProducer<>(ConfStrings.FLINK_QUERY1_MONTHLY_OUT_TOPIC.getString(),
                        new FlinkStringToKafkaSerializer(ConfStrings.FLINK_QUERY1_MONTHLY_OUT_TOPIC.getString()),
                        KafkaClusterConfig.getFlinkSinkProperties("producer" + ConfStrings.FLINK_QUERY1_MONTHLY_OUT_TOPIC.getString()),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query1-monthly-avg-sink");
    }

}
