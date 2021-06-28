package queries.query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.ShipInfo;
import java.util.Date;
import java.util.Arrays;
import java.util.HashSet;


public class ShipAvgAggregator implements AggregateFunction<ShipInfo, ShipAvgAccum, ShipAvgOut> {

    public ShipAvgAggregator() {
    }

    @Override
    public ShipAvgAccum createAccumulator() {
        return new ShipAvgAccum();
    }


    public ShipAvgAccum add(ShipInfo shipInfo, ShipAvgAccum shipAvgAccum) {
        shipAvgAccum.add(shipInfo.getShip_type(), new HashSet<Date>(Arrays.asList(shipInfo.getTimestamp())), 1);
        return shipAvgAccum;
    }

    @Override
    public ShipAvgOut getResult(ShipAvgAccum shipAvgAccum) {
        ShipAvgOut outcome = new ShipAvgOut();
        shipAvgAccum.getStateMap().forEach((k, v) -> outcome.addAvg(k, v.getAvg()));
        return outcome;
    }

    @Override
    public ShipAvgAccum merge(ShipAvgAccum acc1, ShipAvgAccum acc2) {
        acc2.getStateMap().forEach((k, v) -> acc1.add(k, v.getDays(), v.getShip_total()));
        return acc1;
    }
}
