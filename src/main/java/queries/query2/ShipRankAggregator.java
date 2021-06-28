package queries.query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.ShipInfo;

public class ShipRankAggregator implements AggregateFunction<ShipInfo, ShipRankAccum, ShipRankOut> {

    @Override
    public ShipRankAccum createAccumulator() {
        return new ShipRankAccum();
    }

    @Override
    public ShipRankAccum add(ShipInfo shipInfo, ShipRankAccum shipRankAccum) {
        shipRankAccum.add(shipInfo);
        return shipRankAccum;
    }

    @Override
    public ShipRankOut getResult(ShipRankAccum shipRankAccum) {
       ShipRankOut outcome = new ShipRankOut();
       shipRankAccum.getHourStateMap().forEach((k, v) -> outcome.addRank(k, v.getRank()));
       return outcome;
    }

    @Override
    public ShipRankAccum merge(ShipRankAccum acc1, ShipRankAccum acc2) {
        acc2.getHourStateMap().forEach(acc1::mergeStat);
        return acc1;
    }
}
