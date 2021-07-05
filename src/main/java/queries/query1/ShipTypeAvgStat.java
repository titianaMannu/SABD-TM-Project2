package queries.query1;

import java.util.Date;
import java.util.HashSet;

import static org.apache.commons.math3.util.Precision.round;

/**
 * Class used to collect statistics related to a shipType and a cellId
 */
public class ShipTypeAvgStat {
    private int ship_total;
    //collect the days to compute daily avg later on
    // HashSet doesn't allow duplicates by default
    private final HashSet<Date> days;

    public ShipTypeAvgStat(HashSet<Date> days, int counter) {
        this.days = new HashSet<>(days);
        //add new days and ignore those that have already been added to the set
        this.days.addAll(days);
        this.ship_total = counter;
    }

    public ShipTypeAvgStat addShip(int counter, HashSet<Date> out_days) {
        ship_total += counter;
        //add new days and ignore those that have already been added to the set
        this.days.addAll(out_days);
        return this;
    }

    public Double getAvg() {
        //compute the daily avg
        return round(this.ship_total / (double) (this.days.size()), 3);
    }

    public int getShip_total() {
        return ship_total;
    }

    public HashSet<Date> getDays() {
        return days;
    }
}
