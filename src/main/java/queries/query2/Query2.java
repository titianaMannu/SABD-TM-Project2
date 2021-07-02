package queries.query2;

import kafka_utils.FlinkStringToKafkaSerializer;
import kafka_utils.KafkaConfigurations;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import queries.operators.QueryOperators;
import queries.query2.window1.SeaCellAggregator;
import queries.query2.window1.SeaCellOutcome;
import queries.query2.window1.SeaCellProcessWindowFunction;
import queries.query2.window2.TripRankAggregator;
import queries.query2.window2.TripRankProcessWindowFunction;
import queries.windows.WeeklyTumblingEventTimeWindow;
import utils.ConfStrings;
import utils.ShipInfo;

import static org.apache.flink.streaming.api.windowing.assigners.WindowStagger.ALIGNED;

public class Query2 {
    public static void buildTopology(DataStream<Tuple2<Long, String>> source) {
        // parse tuples to obtain the needed information and ignoring all malformed lines;
        DataStream<ShipInfo> ship_sea_stream = source
                .flatMap(QueryOperators.parseInputFunction())
                .name("stream-query2-decoder");

      /*  ship_sea_stream.keyBy(ShipInfo::getSeaType)
                .window(new WeeklyTumblingEventTimeWindow(7, 0, WindowStagger.ALIGNED))
                .aggregate(new ShipRankAggregator(), new ShipRankProcessWindow())
                .name("query2-weekly-window-ranking");*/


        ship_sea_stream.keyBy(new KeySelector<ShipInfo,  String>() {

            @Override
            public  String getKey(ShipInfo shipInfo) throws Exception {
                return shipInfo.getSeaType().name() + ":" + shipInfo.getCellId();
            }
        })
                .window(new WeeklyTumblingEventTimeWindow(7, 0, ALIGNED))
                .aggregate(new SeaCellAggregator(), new SeaCellProcessWindowFunction())
                .name("stream-query2-weekly-counter-window")
                .keyBy(new KeySelector<SeaCellOutcome, String>() {
                    @Override
                    public String  getKey(SeaCellOutcome seaCellOutcome) throws Exception {
                        return seaCellOutcome.getSea().name();
                    }
                }).window(new WeeklyTumblingEventTimeWindow(7, 0, ALIGNED))
                .aggregate(new TripRankAggregator(), new TripRankProcessWindowFunction())
                .name("stream-query2-weekly-rank-window")
                .map(QueryOperators.ExportQuery2OutcomeToString())
                .name("stream-query2-weekly-mapToString")
                // write the output string to the correct topic in kafka
                .addSink(new FlinkKafkaProducer<>(ConfStrings.FLINK_QUERY2_WEEKLY_OUT_TOPIC.getString(),
                        new FlinkStringToKafkaSerializer(ConfStrings.FLINK_QUERY2_WEEKLY_OUT_TOPIC.getString()),
                        KafkaConfigurations.getFlinkSinkProperties("producer" + ConfStrings.FLINK_QUERY2_WEEKLY_OUT_TOPIC.getString()),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query2-weekly-rank-sink");

    }
}
