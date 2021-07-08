package queries.query1;

import utils.ShipType;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Accumulator used to compute avg statistics
 */
public class ShipAvgAccum {

    // hash map collecting statistics per ship type
    private final HashMap<ShipType, ShipTypeAvgStat> ShipStatMap;

    public ShipAvgAccum() {
        this.ShipStatMap = new HashMap<>();
    }

    /**
     * Add a new element if the ship type is not present into the map or properly
     * updates the existing one
     *
     * @param type      ShipType
     * @param timestamp HashSet Containing dates. It is used to compute daily avg considering the right number of days
     * @param counter   value to add
     */
    public void add(ShipType type, HashSet<Date> timestamp, int counter) {
        ShipTypeAvgStat analysis = this.ShipStatMap.get(type);
        if (analysis == null) {
            this.ShipStatMap.put(type, new ShipTypeAvgStat(timestamp, counter));
        } else {
            this.ShipStatMap.computeIfPresent(type, (k, v) -> v.addShip(counter, timestamp));
        }
    }

    /**
     * Get collected statistics
     *
     * @return Hashmap: ShipType, ShipTypeAvgStat
     */
    public HashMap<ShipType, ShipTypeAvgStat> getShipStatMap() {
        return ShipStatMap;
    }
}
