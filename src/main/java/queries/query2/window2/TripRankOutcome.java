package queries.query2.window2;

import utils.SeaType;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TripRankOutcome {
    private Date startWindowDate;
    private SeaType sea;
    private List<String> amTripRank;
    private List<String> pmTripRank;

    public TripRankOutcome() {
        this.amTripRank = new ArrayList<>();
        this.pmTripRank = new ArrayList<>();
    }

    public Date getStartWindowDate() {
        return startWindowDate;
    }

    public void setStartWindowDate(Date startWindowDate) {
        this.startWindowDate = startWindowDate;
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
