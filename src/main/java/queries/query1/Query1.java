package queries.query1;

import benchmarking.BenchmarkSink;
import kafka_utils.FlinkToKafkaSerializer;
import kafka_utils.KafkaConfigurations;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import queries.operators.QueryOperators;
import queries.windows.MonthlyTumblingEventTimeWindow;
import queries.windows.WeeklyTumblingEventTimeWindow;
import utils.ConfStrings;
import utils.ShipInfo;

/**
 * this class build the topology for the query 1
 */
public class Query1 {

    public static void buildTopology(DataStream<ShipInfo> source) {

        DataStream<ShipInfo> stream = source
                // only western mediterranean sea
                .filter(QueryOperators.filterRegion())
                .name("filter-per-western-sea");

        //weekly window
        DataStream<String> outputStreamWeekly = stream.keyBy(ShipInfo::getCellId) // stream keyed by cellID
                //weekly tumbling window
                .window(new WeeklyTumblingEventTimeWindow(7, 0, WindowStagger.ALIGNED))
                //ProcessWindowFunction with Incremental Aggregation
                .aggregate(new ShipAvgAggregator(), new ShipAvgProcessWindow())
                .name("query1-weekly-window-avg")
                //map window outcome to a proper string
                .map(QueryOperators.ExportQuery1OutcomeToString())
                .name("stream-query1-weekly-mapToString");

        // write the output string to the correct topic in kafka
        outputStreamWeekly.addSink(new FlinkKafkaProducer<>(ConfStrings.FLINK_QUERY1_WEEKLY_OUT_TOPIC.getString(),
                new FlinkToKafkaSerializer(ConfStrings.FLINK_QUERY1_WEEKLY_OUT_TOPIC.getString()),
                KafkaConfigurations.getFlinkSinkProperties("producer" +
                        ConfStrings.FLINK_QUERY1_WEEKLY_OUT_TOPIC.getString()),
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query1-weekly-avg-sink");
        //sink for Experimental benchmarking
        outputStreamWeekly.addSink(new BenchmarkSink("query1-weekly-avg")).name("query1-weekly-bench-sink").setParallelism(1);

        //monthly window
        DataStream<String> outputStreamMonthly = stream.keyBy(ShipInfo::getCellId) // stream keyed by cellID
                //monthly tumbling window
                .window(new MonthlyTumblingEventTimeWindow(1, 0, WindowStagger.ALIGNED))
                //ProcessWindowFunction with Incremental Aggregation
                .aggregate(new ShipAvgAggregator(), new ShipAvgProcessWindow())
                .name("query1-monthly-window-avg")
                //map window outcome to a proper string
                .map(QueryOperators.ExportQuery1OutcomeToString())
                .name("stream-query1-monthly-mapToString");

        // write the output string to the correct topic in kafka
        outputStreamMonthly.addSink(new FlinkKafkaProducer<>(ConfStrings.FLINK_QUERY1_MONTHLY_OUT_TOPIC.getString(),
                new FlinkToKafkaSerializer(ConfStrings.FLINK_QUERY1_MONTHLY_OUT_TOPIC.getString()),
                KafkaConfigurations.getFlinkSinkProperties("producer" + ConfStrings.FLINK_QUERY1_MONTHLY_OUT_TOPIC.getString()),
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query1-monthly-avg-sink");

        //sink for Experimental benchmarking
        outputStreamMonthly.addSink(new BenchmarkSink("query1-monthly-avg")).name("query1-monthly-bench-sink").setParallelism(1);
    }

}
