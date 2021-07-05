package queries.query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * Used for implementing ProcessWindowFunction with Incremental Aggregation that
 * allows to incrementally compute windows while having access to the
 * additional window meta information of the ProcessWindowFunction.
 */
public class ShipAvgProcessWindow extends ProcessWindowFunction<ShipAvgOutcome, ShipAvgOutcome, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<ShipAvgOutcome> iterable, Collector<ShipAvgOutcome> collector) throws Exception {
        ShipAvgOutcome outcome = iterable.iterator().next();
        //setting start date
        outcome.setStartWindowDate(new Date(context.window().getStart()));
        //setting cellId
        outcome.setCellId(key);
        //collects outcomes
        collector.collect(outcome);
    }
}
