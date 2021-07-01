package queries.query2.window1;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.SeaType;

import java.util.Date;

public class SeaCellProcessWindowFunction extends ProcessWindowFunction<SeaCellOutcome, SeaCellOutcome, Tuple2<SeaType, String>, TimeWindow> {
    @Override
    public void process(Tuple2<SeaType, String> tuple2, Context context, Iterable<SeaCellOutcome> iterable, Collector<SeaCellOutcome> collector) throws Exception {
        SeaCellOutcome seaCellOutcome = iterable.iterator().next();
        seaCellOutcome.setStartWindowDate(new Date(context.window().getStart()));
        seaCellOutcome.setSea(tuple2.f0);
        seaCellOutcome.setCellId(tuple2.f1);
        collector.collect(seaCellOutcome);
    }


}
