package benchmarking;

import static org.apache.commons.math3.util.Precision.round;

/**
 * This class is used to take statistics for benchmarking purposes
 */

public class SynchronizedTupleCounter {
    // tuples counter
    private static long counter = 0L;
    // first output time
    private static long startTime;

    /**
     * Called when a new observation occurred.
     * Used to compute mean throughput and latency.
     *
     * @param context type of query and window
     */
    public static synchronized void incrementCounter(String context) {
        if (counter == 0L) {
            //set starting time
            startTime = System.currentTimeMillis();
            System.out.println("Initialized with success !");
        }
        // increment counter everytime that a new observation occurred
        counter++;
        // current time - start time
        double totalLatency = System.currentTimeMillis() - startTime;

        // mean throughput tuple / timeunit (millis)
        double throughput = counter / totalLatency;
        //mean latency : latency per tuple
        double latency = totalLatency / counter;
        // prints mean throughput and latency so far evaluated
        System.out.println(context + "::" + " Mean throughput: " + round(throughput,5) + " tuple/ms\t\t" + "Mean latency: " +
                round(latency,3) + " ms");
    }
}