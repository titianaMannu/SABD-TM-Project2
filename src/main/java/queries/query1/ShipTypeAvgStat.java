package queries.query1;

import java.util.Date;
import java.util.HashSet;
import static org.apache.commons.math3.util.Precision.round;

public class ShipTypeAvgStat {
    private int ship_total;
    private final HashSet<Date> days;

    public ShipTypeAvgStat(HashSet<Date> days, int counter) {
        this.days = new HashSet<>(days);
        this.days.addAll(days);
        this.ship_total = counter;
    }

    public ShipTypeAvgStat addShip(int counter, HashSet<Date> out_days) {
        ship_total += counter;
        this.days.addAll(out_days);
        return this;
    }

    public Double getAvg() {
        return round(this.ship_total / (double) (this.days.size()), 3);
    }

    public int getShip_total() {
        return ship_total;
    }

    public HashSet<Date> getDays() {
        return days;
    }
}
