package queries.query2.window2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.SeaType;

import java.util.Date;

public class TripRankProcessWindowFunction extends ProcessWindowFunction<TripRankOutcome, TripRankOutcome, String, TimeWindow> {
    @Override
    public void process(String seaType, Context context, Iterable<TripRankOutcome> iterable, Collector<TripRankOutcome> collector) throws Exception {
        TripRankOutcome tripRankOutcome = iterable.iterator().next();
        tripRankOutcome.setStartWindowDate(new Date(context.window().getStart()));
        tripRankOutcome.setSea(SeaType.valueOf(seaType));
        collector.collect(tripRankOutcome);
    }
}
