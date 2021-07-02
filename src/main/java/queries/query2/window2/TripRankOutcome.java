package queries.query2.window2;

import utils.SeaType;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TripRankOutcome {
    private String startWindowDate;
    private SeaType sea;
    private final List<String> amTripRank;
    private final List<String> pmTripRank;

    public TripRankOutcome() {
        this.amTripRank = new ArrayList<>();
        this.pmTripRank = new ArrayList<>();
    }

    public String getStartWindowDate() {
        return startWindowDate;
    }

    public void setStartWindowDate(Date startWindowDate) throws ParseException {
        DateFormat formater = new SimpleDateFormat("dd/MM/yyyy HH:mm");
        String sDate = formater.format(startWindowDate);
        this.startWindowDate = sDate;
    }

    public SeaType getSea() {
        return sea;
    }

    public void setSea(SeaType sea) {
        this.sea = sea;
    }


    public List<String> getAmTripRank() {
        return amTripRank;
    }

    public void addAmTrip(String tripId) {
        this.amTripRank.add(tripId);
    }

    public List<String> getPmTripRank() {
        return pmTripRank;
    }

    public void addPmTrip(String tripId) {
        this.pmTripRank.add(tripId);
    }
}
