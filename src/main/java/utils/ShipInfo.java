package utils;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;

public class ShipInfo implements Serializable {

    private final String ship_id; // hexadecimal format
    private final ShipType ship_type;
    private final Double LON; // longitude
    private final Double LAT; // latitude
    private final Date timestamp; // event time
    private final String trip_id; // ship_id + departure_time + arrival_time
    private  String cellId;

    public ShipInfo(String ship_id, Integer ship_type, Double LON, Double LAT, String timestamp, String trip_id) throws ParseException {

        DateFormat format = new SimpleDateFormat("dd/MM/yyyy");

        this.ship_id = ship_id;
        this.ship_type = ShipType.valueOf(ship_type);
        this.LON = LON;
        this.LAT = LAT;
        this.timestamp =  format.parse(timestamp);
        this.trip_id = trip_id;
    }

    public String getShip_id() {
        return ship_id;
    }

    public ShipType getShip_type() {
        return ship_type;
    }

    public Double getLON() {
        return LON;
    }

    public Double getLAT() {
        return LAT;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getTrip_id() {
        return trip_id;
    }

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    @Override
    public String toString() {
        return "ShipInfo{" +
                "ship_id='" + ship_id + '\'' +
                ", ship_type=" + ship_type +
                ", LON=" + LON +
                ", LAT=" + LAT +
                ", timestamp=" + timestamp +
                ", trip_id='" + trip_id + '\'' +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShipInfo shipInfo = (ShipInfo) o;
        return Objects.equals(trip_id, shipInfo.trip_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trip_id);
    }


}
