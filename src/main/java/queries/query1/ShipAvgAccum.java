package queries.query1;

import utils.ShipType;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

public class ShipAvgAccum {

    private final HashMap<ShipType, ShipTypeAvgStat> stateMap;

    public ShipAvgAccum() {
        this.stateMap = new HashMap<>();
    }

    public void add(ShipType type, HashSet<Date> timestamp, int counter) {
        ShipTypeAvgStat analysis = this.stateMap.get(type);
        if (analysis == null) {
            this.stateMap.put(type, new ShipTypeAvgStat(timestamp, counter));
        } else {
            this.stateMap.computeIfPresent(type, (k, v) -> v.addShip(counter, timestamp));
        }
    }

    public HashMap<ShipType, ShipTypeAvgStat> getStateMap() {
        return stateMap;
    }
}
