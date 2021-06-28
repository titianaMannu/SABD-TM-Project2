package queries.query2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.SeaType;

import java.util.Date;

public class ShipRankProcessWindow extends ProcessWindowFunction<ShipRankOut,ShipRankOut, SeaType, TimeWindow> {
    @Override
    public void process(SeaType s, Context context, Iterable<ShipRankOut> iterable, Collector<ShipRankOut> collector) throws Exception {
        ShipRankOut rankOut = iterable.iterator().next();
        rankOut.setStartWindowDate(new Date(context.window().getStart()));
        rankOut.setSea(s.name());
        collector.collect(rankOut);
    }
}
