package queries.query2.window2;

import org.apache.flink.api.common.functions.AggregateFunction;
import queries.query2.window1.SeaCellOutcome;

/**
 * Aggregate Function used for the second window to do ranking based on the total different
 * trips per cellId computed in the previous window.
 */
public class TripRankAggregator implements AggregateFunction<SeaCellOutcome, TripRankAccum, TripRankOutcome> {
    @Override
    public TripRankAccum createAccumulator() {
        return new TripRankAccum();
    }

    /**
     * Add totals trips of a cellId stored in the seaCellOutcome to the accumulator
     *
     * @param seaCellOutcome contains info to be aggregated
     * @param tripRankAccum  accumulator
     * @return Updated accumulator
     */
    @Override
    public TripRankAccum add(SeaCellOutcome seaCellOutcome, TripRankAccum tripRankAccum) {
        tripRankAccum.add(seaCellOutcome);
        return tripRankAccum;
    }

    /**
     * Obtain the ranking by sorting in descending order the accumulated values
     * and then take(3)
     * @param tripRankAccum contains accumulated info
     * @return outcome container for the ranking
     */
    @Override
    public TripRankOutcome getResult(TripRankAccum tripRankAccum) {
        TripRankOutcome tripRankOutcome = new TripRankOutcome();
        // Sort in descending order
        tripRankAccum.getAmTotalPerCellId().sort((o1, o2) -> o2.f1.compareTo(o1.f1));
        tripRankAccum.getPmTotalPerCellId().sort((o1, o2) -> o2.f1.compareTo(o1.f1));
        //ranking degree = 3
        long rankDegree = 3L;
        try {
            //add the first 3 cells (id)
            for (int i = 0; i < rankDegree; i++) {
                tripRankOutcome.addAmCell(tripRankAccum.getAmTotalPerCellId().get(i).f0);
                tripRankOutcome.addPmCell(tripRankAccum.getPmTotalPerCellId().get(i).f0);
            }
        } catch (IndexOutOfBoundsException e) {
            // too few cells < 3
            return tripRankOutcome;
        }
        return tripRankOutcome;
    }

    /**
     * This function is called in order to merge two accumulators
     * @param acc1 to be merged
     * @param acc2 to be merged
     * @return merged accumulator
     */
    @Override
    public TripRankAccum merge(TripRankAccum acc1, TripRankAccum acc2) {
        acc1.addAM(acc2.getAmTotalPerCellId());
        acc1.addPM(acc2.getPmTotalPerCellId());
        return acc2;
    }
}
