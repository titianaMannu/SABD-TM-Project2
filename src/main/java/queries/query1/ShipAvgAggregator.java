package queries.query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.ShipInfo;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Aggregate Function used to compute the statistics for query-1
 */
public class ShipAvgAggregator implements AggregateFunction<ShipInfo, ShipAvgAccum, ShipAvgOutcome> {

    //default constructor
    public ShipAvgAggregator() {
    }

    @Override
    public ShipAvgAccum createAccumulator() {
        return new ShipAvgAccum();
    }

    /**
     * Add an element to the window accumulator
     *
     * @param shipInfo     contains info to be aggregated
     * @param shipAvgAccum container for the needed info
     * @return Updated Accumulator
     */
    public ShipAvgAccum add(ShipInfo shipInfo, ShipAvgAccum shipAvgAccum) {
        shipAvgAccum.add(shipInfo.getShip_type(), new HashSet<>(Arrays.asList(shipInfo.getTimestamp())), 1);
        return shipAvgAccum;
    }

    /**
     * This function compute result witch is stored  into a custom object (outcome)
     *
     * @param shipAvgAccum container for the needed info
     * @return a container Object for results
     */
    @Override
    public ShipAvgOutcome getResult(ShipAvgAccum shipAvgAccum) {
        ShipAvgOutcome outcome = new ShipAvgOutcome();
        shipAvgAccum.getShipStatMap().forEach((k, v) -> outcome.addAvg(k, v.getAvg()));
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
    public ShipAvgAccum merge(ShipAvgAccum acc1, ShipAvgAccum acc2) {
        acc2.getShipStatMap().forEach((k, v) -> acc1.add(k, v.getDays(), v.getShip_total()));
        return acc1;
    }
}
