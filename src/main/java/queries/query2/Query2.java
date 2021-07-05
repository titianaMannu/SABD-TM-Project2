package queries.query2;

import kafka_utils.FlinkToKafkaSerializer;
import kafka_utils.KafkaConfigurations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import queries.operators.QueryOperators;
import queries.query2.window1.SeaCellAggregator;
import queries.query2.window1.SeaCellOutcome;
import queries.query2.window1.SeaCellProcessWindowFunction;
import queries.query2.window2.TripRankAggregator;
import queries.query2.window2.TripRankProcessWindowFunction;
import queries.windows.MonthlyTumblingEventTimeWindow;
import queries.windows.WeeklyTumblingEventTimeWindow;
import utils.ConfStrings;
import utils.ShipInfo;

import static org.apache.flink.streaming.api.windowing.assigners.WindowStagger.ALIGNED;

/**
 * this class build the topology for the query 2
 */
public class Query2 {
    public static void buildTopology(DataStream<ShipInfo> stream) {

        /* key by sea and cellId; notes that sea information is redundant because it is a consequence of the cellId
           but it is specified to be easily recovered later on. */
        stream.keyBy(new KeySelector<ShipInfo, String>() {
            @Override
            public String getKey(ShipInfo shipInfo) throws Exception {
                // Sea:CellId
                return shipInfo.getSeaType().name() + ":" + shipInfo.getCellId();
            }
        })
                //weekly tumbling window
                //first window to compute different trips per cellId and hour_range (am, pm)
                .window(new WeeklyTumblingEventTimeWindow(7, 0, ALIGNED))
                //ProcessWindowFunction with Incremental Aggregation
                .aggregate(new SeaCellAggregator(), new SeaCellProcessWindowFunction())
                .name("stream-query2-weekly-counter-window")
                //key by sea easily recovered from the outcome of the first window
                .keyBy(new KeySelector<SeaCellOutcome, String>() {
                    @Override
                    public String getKey(SeaCellOutcome seaCellOutcome) throws Exception {
                        return seaCellOutcome.getSea().name();
                    }
                })
                //weekly tumbling window
                // this second window takes in input the outcome of the first one
                .window(new WeeklyTumblingEventTimeWindow(7, 0, ALIGNED))
                // Used to do ranking
                .aggregate(new TripRankAggregator(), new TripRankProcessWindowFunction())
                .name("stream-query2-weekly-rank-window")
                //map window outcome to a proper string
                .map(QueryOperators.ExportQuery2OutcomeToString())
                .name("stream-query2-weekly-mapToString")
                // write the output string to the correct topic in kafka
                .addSink(new FlinkKafkaProducer<>(ConfStrings.FLINK_QUERY2_WEEKLY_OUT_TOPIC.getString(),
                        new FlinkToKafkaSerializer(ConfStrings.FLINK_QUERY2_WEEKLY_OUT_TOPIC.getString()),
                        KafkaConfigurations.getFlinkSinkProperties("producer" + ConfStrings.FLINK_QUERY2_WEEKLY_OUT_TOPIC.getString()),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query2-weekly-rank-sink");

        //monthly window
        stream.keyBy(new KeySelector<ShipInfo, String>() {
            @Override
            public String getKey(ShipInfo shipInfo) throws Exception {
                return shipInfo.getSeaType().name() + ":" + shipInfo.getCellId();
            }
        })
                //first window to compute different trips per cellId and hour_range (am, pm)
                .window(new MonthlyTumblingEventTimeWindow(1, 0, ALIGNED))
                //ProcessWindowFunction with Incremental Aggregation
                .aggregate(new SeaCellAggregator(), new SeaCellProcessWindowFunction())
                .name("stream-query2-monthly-counter-window")
                //key by sea easily recovered from the outcome of the first window
                .keyBy(new KeySelector<SeaCellOutcome, String>() {
                    @Override
                    public String getKey(SeaCellOutcome seaCellOutcome) throws Exception {
                        return seaCellOutcome.getSea().name();
                    }
                })
                //monthly tumbling window
                // this second window takes in input the outcome of the first one
                .window(new MonthlyTumblingEventTimeWindow(1, 0, ALIGNED))
                // Used to do ranking
                .aggregate(new TripRankAggregator(), new TripRankProcessWindowFunction())
                .name("stream-query2-monthly-rank-window")
                //map window outcome to a proper string
                .map(QueryOperators.ExportQuery2OutcomeToString())
                .name("stream-query2-monthly-mapToString")
                // write the output string to the correct topic in kafka
                .addSink(new FlinkKafkaProducer<>(ConfStrings.FLINK_QUERY2_MONTHLY_OUT_TOPIC.getString(),
                        new FlinkToKafkaSerializer(ConfStrings.FLINK_QUERY2_MONTHLY_OUT_TOPIC.getString()),
                        KafkaConfigurations.getFlinkSinkProperties("producer" + ConfStrings.FLINK_QUERY2_MONTHLY_OUT_TOPIC.getString()),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query2-monthly-rank-sink");
    }
}
