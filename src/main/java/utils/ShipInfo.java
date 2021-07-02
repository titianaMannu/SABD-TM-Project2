package utils;

import queries.operators.Navigator;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

public class ShipInfo implements Serializable {

    private final String ship_id; // hexadecimal format
    private final ShipType ship_type;
    private final Double LON; // longitude
    private final Double LAT; // latitude
    private final Date timestamp; // event time
    private final String trip_id; // ship_id + departure_time + arrival_time
    private final String cellId;
    private final SeaType seaType;
    private final String hour_range;

    public ShipInfo(String ship_id, Integer ship_type, Double LON, Double LAT, String timestamp, String trip_id) throws ParseException {
        //set a hour category
        if (isPostMeridian(timestamp)){
            this.hour_range = ConfStrings.POST_MERIDIAN.getString();
        }else {
            this.hour_range = ConfStrings.ANTE_MERIDIAN.getString();
        }

        DateFormat format = new SimpleDateFormat("dd/MM/yyyy");
        this.ship_id = ship_id;
        this.ship_type = ShipType.valueOf(ship_type);
        this.LON = LON;
        this.LAT = LAT;
        this.timestamp =  format.parse(timestamp);
        this.trip_id = trip_id;
        this.seaType = Navigator.findSea(LON);
        this.cellId =  Navigator.findCellID(LAT, LON);
    }

    public boolean isWesternMediterranean(){
        return this.seaType.equals(SeaType.WESTERN_MEDITERRANEAN_SEA);
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

    public SeaType getSeaType() {
        return seaType;
    }

    public String getHour_range() {
        return hour_range;
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


    private boolean isPostMeridian(String timestamp) throws ParseException {

        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm");
        Date dateWithHours = format.parse(timestamp);
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(dateWithHours);
        cal1.set(Calendar.HOUR_OF_DAY, 11);
        cal1.set(Calendar.MINUTE, 59);
        Date endDate = cal1.getTime();
        return dateWithHours.after(endDate);
    }

}
