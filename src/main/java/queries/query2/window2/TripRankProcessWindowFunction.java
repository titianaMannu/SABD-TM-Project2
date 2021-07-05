package queries.query2.window2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.SeaType;

import java.util.Date;

/**
 * Used for implementing ProcessWindowFunction with Incremental Aggregation
 */
public class TripRankProcessWindowFunction extends ProcessWindowFunction<TripRankOutcome, TripRankOutcome, String, TimeWindow> {
    @Override
    public void process(String seaType, Context context, Iterable<TripRankOutcome> iterable, Collector<TripRankOutcome> collector) throws Exception {
        TripRankOutcome tripRankOutcome = iterable.iterator().next();
        // set start window date
        tripRankOutcome.setStartWindowDate(new Date(context.window().getStart()));
        //set sea
        tripRankOutcome.setSea(SeaType.valueOf(seaType));
        //collect outcomes
        collector.collect(tripRankOutcome);
    }
}
