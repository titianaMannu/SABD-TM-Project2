package queries.query2.window1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.ShipInfo;

import static utils.ConfStrings.ANTE_MERIDIAN;
import static utils.ConfStrings.POST_MERIDIAN;

public class SeaCellAggregator implements AggregateFunction<ShipInfo, SeaCellAccumulator, SeaCellOutcome> {

    @Override
    public SeaCellAccumulator createAccumulator() {
        return new SeaCellAccumulator();
    }

    @Override
    public SeaCellAccumulator add(ShipInfo shipInfo, SeaCellAccumulator seaCellAccumulator) {
        seaCellAccumulator.add(shipInfo);
        return seaCellAccumulator;
    }

    @Override
    public SeaCellOutcome getResult(SeaCellAccumulator seaCellAccumulator) {
        SeaCellOutcome outcome =new SeaCellOutcome();
        outcome.addTotal(ANTE_MERIDIAN.getString(), seaCellAccumulator.getAmTrips().size());
        outcome.addTotal(POST_MERIDIAN.getString(), seaCellAccumulator.getPmTrips().size());
        return outcome;
    }

    @Override
    public SeaCellAccumulator merge(SeaCellAccumulator acc1, SeaCellAccumulator acc2) {
        acc1.mergeAM(acc2.getAmTrips());
        acc1.mergePM(acc2.getPmTrips());
        return acc1;
    }
}
