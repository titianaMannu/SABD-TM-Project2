package queries.query2.window1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.SeaType;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class SeaCellProcessWindowFunction extends ProcessWindowFunction<SeaCellOutcome, SeaCellOutcome,  String, TimeWindow> {
    @Override
    public void process( String key, Context context, Iterable<SeaCellOutcome> iterable, Collector<SeaCellOutcome> collector) throws Exception {
        SeaCellOutcome seaCellOutcome = iterable.iterator().next();
        seaCellOutcome.setStartWindowDate(new Date(context.window().getStart()));
        List<String> keys = Arrays.asList(key.split(":"));
        seaCellOutcome.setSea(SeaType.valueOf(keys.get(0)));
        seaCellOutcome.setCellId(keys.get(1));
        collector.collect(seaCellOutcome);
    }


}
