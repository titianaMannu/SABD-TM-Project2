package queries.query2.window1;

import utils.ConfStrings;
import utils.ShipInfo;

import java.util.HashSet;

/**
 * This is an accumulator for the 1st window
 * Collects all different trips per different hour_range
 */
public class SeaCellAccumulator {
    //HashSet for ante meridian trips; duplicated not allowed
    private final HashSet<String> amTrips;
    //HashSet for post meridian trips; duplicated not allowed
    private final HashSet<String> pmTrips;

    public SeaCellAccumulator() {
        this.amTrips = new HashSet<>();
        this.pmTrips = new HashSet<>();
    }

    public HashSet<String> getAmTrips() {
        return amTrips;
    }

    public HashSet<String> getPmTrips() {
        return pmTrips;
    }

    /**
     * This function add a trip to the correct HashSet if not present
     *
     * @param shipInfo contains info to be accumulated
     */
    public void add(ShipInfo shipInfo) {
        if (shipInfo.getHour_range().equals(ConfStrings.ANTE_MERIDIAN.getString())) {
            // add ante meridian trip if not present
            this.amTrips.add(shipInfo.getTrip_id());
        } else {
            //add post meridian trip if not present
            this.pmTrips.add(shipInfo.getTrip_id());
        }
    }

    public void mergeAM(HashSet<String> amToMerge) {
        this.amTrips.addAll(amToMerge);
    }

    public void mergePM(HashSet<String> pmToMerge) {
        this.pmTrips.addAll(pmToMerge);
    }
}
