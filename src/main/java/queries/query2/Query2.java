package queries.query2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import queries.operators.QueryOperators;
import queries.windows.WeeklyTumblingEventTimeWindow;
import utils.ShipInfo;

public class Query2 {
    public static void buildTopology(DataStream<Tuple2<Long, String>> source) {
        // parse tuples to obtain the needed information and ignoring all malformed lines;
        DataStream<ShipInfo> ship_sea_stream = source
                .flatMap(QueryOperators.parseInputFunction())
                .name("stream-query2-decoder")
                .map(QueryOperators.computeCellId()).name("compute-cell-id");

        ship_sea_stream.keyBy(ShipInfo::getSeaType)
                .window(new WeeklyTumblingEventTimeWindow(7, 0, WindowStagger.ALIGNED))
                .aggregate(new ShipRankAggregator(), new ShipRankProcessWindow())
                .name("query2-weekly-window-ranking");

    }
}
