package benchmarking;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Class used for flink benchmark evaluations.
 * Usage:  ".addSink(new BenchmarkSink(context))"
 */
public class BenchmarkSink implements SinkFunction<String> {

    //type of query and window
    private final String context;

    public BenchmarkSink(String context) {
        this.context = context;
    }

    @Override
    public void invoke(String value, Context context) {
        // stats counter
        SynchronizedTupleCounter.incrementCounter(this.context);
    }

}





