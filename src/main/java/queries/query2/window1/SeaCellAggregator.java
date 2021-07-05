package queries.query2.window1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.ShipInfo;

import static utils.ConfStrings.ANTE_MERIDIAN;
import static utils.ConfStrings.POST_MERIDIAN;

/**
 * Aggregate Function used for the first window to compute the total different trips per cellId
 */
public class SeaCellAggregator implements AggregateFunction<ShipInfo, SeaCellAccumulator, SeaCellOutcome> {

    @Override
    public SeaCellAccumulator createAccumulator() {
        return new SeaCellAccumulator();
    }

    /**
     * Add an element to the window accumulator
     *
     * @param shipInfo           contains info to be aggregated
     * @param seaCellAccumulator container for the needed info
     * @return Updated Accumulator
     */
    @Override
    public SeaCellAccumulator add(ShipInfo shipInfo, SeaCellAccumulator seaCellAccumulator) {
        seaCellAccumulator.add(shipInfo);
        return seaCellAccumulator;
    }

    /**
     * Add the total-trip value accumulated to the correct hour-range of the outcome object
     *
     * @param seaCellAccumulator container for the needed info
     * @return a container Object for results
     */
    @Override
    public SeaCellOutcome getResult(SeaCellAccumulator seaCellAccumulator) {
        SeaCellOutcome outcome = new SeaCellOutcome();
        // add total trips  for ante meridian
        outcome.addTotal(ANTE_MERIDIAN.getString(), seaCellAccumulator.getAmTrips().size());
        // add total trips for post meridian
        outcome.addTotal(POST_MERIDIAN.getString(), seaCellAccumulator.getPmTrips().size());
        return outcome;
    }

    /**
     * This function is called in order to merge two accumulators
     *
     * @param acc1 1st accumulator to be merged
     * @param acc2 2nd accumulator to be merged
     * @return merged accumulator
     */
    @Override
    public SeaCellAccumulator merge(SeaCellAccumulator acc1, SeaCellAccumulator acc2) {
        acc1.mergeAM(acc2.getAmTrips());
        acc1.mergePM(acc2.getPmTrips());
        return acc1;
    }
}
