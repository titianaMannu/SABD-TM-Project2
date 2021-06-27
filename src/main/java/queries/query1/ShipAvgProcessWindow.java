package queries.query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class ShipAvgProcessWindow extends ProcessWindowFunction<ShipAvgOut, ShipAvgOut, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<ShipAvgOut> iterable, Collector<ShipAvgOut> collector) throws Exception {
        ShipAvgOut outcome = iterable.iterator().next();
        outcome.setStartWindowDate(new Date(context.window().getStart()));
        outcome.setCellId(key);
        System.out.println(key);
        collector.collect(outcome);
    }
}
