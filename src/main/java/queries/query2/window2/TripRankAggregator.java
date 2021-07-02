package queries.query2.window2;

import org.apache.flink.api.common.functions.AggregateFunction;
import queries.query2.window1.SeaCellOutcome;

public class TripRankAggregator implements AggregateFunction<SeaCellOutcome, TripRankAccum, TripRankOutcome> {
    @Override
    public TripRankAccum createAccumulator() {
        return new TripRankAccum();
    }

    @Override
    public TripRankAccum add(SeaCellOutcome seaCellOutcome, TripRankAccum tripRankAccum) {
        tripRankAccum.add(seaCellOutcome);
        return tripRankAccum;
    }

    @Override
    public TripRankOutcome getResult(TripRankAccum tripRankAccum) {
        TripRankOutcome tripRankOutcome = new TripRankOutcome();
        // Sort in descending order
        tripRankAccum.getAmTotalPerCellId().sort((o1, o2) -> o2.f1.compareTo(o1.f1));
        tripRankAccum.getPmTotalPerCellId().sort((o1, o2) -> o2.f1.compareTo(o1.f1));
        long rankDegree = 3L;
        try {
            for (int i = 0; i < rankDegree; i++) {
                tripRankOutcome.addAmTrip(tripRankAccum.getAmTotalPerCellId().get(i).f0);
                tripRankOutcome.addPmTrip(tripRankAccum.getPmTotalPerCellId().get(i).f0);
            }
        }catch(IndexOutOfBoundsException e){
            // too many trips
            return tripRankOutcome;
        }
        return tripRankOutcome;
    }

    @Override
    public TripRankAccum merge(TripRankAccum acc1, TripRankAccum acc2) {
        acc1.addAM(acc2.getAmTotalPerCellId());
        acc1.addPM(acc2.getPmTotalPerCellId());
        return acc2;
    }
}
