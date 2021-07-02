package queries.query2.window2;

import org.apache.flink.api.java.tuple.Tuple2;
import queries.query2.window1.SeaCellOutcome;

import java.util.ArrayList;
import java.util.List;

public class TripRankAccum {
    //fixed seaType thanks to keyBy

    //{(cell_id, total)}
    private final List<Tuple2<String, Integer>> amTotalPerCellId;
    private final List<Tuple2<String, Integer>> pmTotalPerCellId;

    public TripRankAccum() {
        this.amTotalPerCellId = new ArrayList<>();
        this.pmTotalPerCellId = new ArrayList<>();
    }

    public void add(SeaCellOutcome seaCellOutcome) {
        this.amTotalPerCellId.add(new Tuple2<>(seaCellOutcome.getCellId(), seaCellOutcome.getAmTrips()));
        this.pmTotalPerCellId.add(new Tuple2<>(seaCellOutcome.getCellId(), seaCellOutcome.getPmTrips()));
    }

    public void addAM(List<Tuple2<String, Integer>> am) {
        this.amTotalPerCellId.addAll(am);
    }

    public void addPM(List<Tuple2<String, Integer>> pm) {
        this.pmTotalPerCellId.addAll(pm);
    }

    public List<Tuple2<String, Integer>> getAmTotalPerCellId() {
        return amTotalPerCellId;
    }

    public List<Tuple2<String, Integer>> getPmTotalPerCellId() {
        return pmTotalPerCellId;
    }
}
