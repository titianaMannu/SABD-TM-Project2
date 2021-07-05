package queries.query2.window1;

import utils.SeaType;

import java.util.Date;

import static utils.ConfStrings.ANTE_MERIDIAN;

/**
 * Class that contains first window  processing outcome
 */
public class SeaCellOutcome {
    private Date startWindowDate;
    private SeaType sea;
    private String cellId;
    private int amTrips; //total different trips (ante meridian)
    private int pmTrips; // total different trips (post meridian)

    public SeaCellOutcome() {
        this.pmTrips = 0;
        this.pmTrips = 0;
    }

    /**
     * Add the total trips counted based on its hour range
     *
     * @param hour_range am, pm
     * @param val        total
     */
    public void addTotal(String hour_range, Integer val) {
        if (hour_range.equals(ANTE_MERIDIAN.getString())) {
            this.amTrips += val;
        } else {
            this.pmTrips += val;
        }
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

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    public Integer getAmTrips() {
        return amTrips;
    }

    public Integer getPmTrips() {
        return pmTrips;
    }
}
