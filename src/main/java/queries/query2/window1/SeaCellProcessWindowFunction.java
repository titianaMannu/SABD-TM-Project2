package queries.query2.window1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.SeaType;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Used for implementing ProcessWindowFunction with Incremental Aggregation
 */
public class SeaCellProcessWindowFunction extends ProcessWindowFunction<SeaCellOutcome, SeaCellOutcome,  String, TimeWindow> {
    @Override
    public void process( String key, Context context, Iterable<SeaCellOutcome> iterable, Collector<SeaCellOutcome> collector) throws Exception {
        SeaCellOutcome seaCellOutcome = iterable.iterator().next();
        //set start window date
        seaCellOutcome.setStartWindowDate(new Date(context.window().getStart()));
        List<String> keys = Arrays.asList(key.split(":"));
        //set sea
        seaCellOutcome.setSea(SeaType.valueOf(keys.get(0)));
        //set cellId
        seaCellOutcome.setCellId(keys.get(1));
        //collect outcomes
        collector.collect(seaCellOutcome);
    }
}
