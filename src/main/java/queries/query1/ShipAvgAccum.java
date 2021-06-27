package queries.query1;

import utils.ShipType;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class ShipAvgAccum {

    private final HashMap<ShipType, ShipTypeAnalysis> stateMap;

    public ShipAvgAccum() {
        this.stateMap = new HashMap<>();
    }

    public void add(ShipType type, HashSet<Date> timestamp, int counter) {
        ShipTypeAnalysis analysis = this.stateMap.get(type);
        if (analysis == null) {
            this.stateMap.put(type, new ShipTypeAnalysis(timestamp, counter));
        } else {
            this.stateMap.computeIfPresent(type, (k, v) -> v.addShip(counter, timestamp));
        }
    }

    public HashMap<ShipType, ShipTypeAnalysis> getStateMap() {
        return stateMap;
    }
}
