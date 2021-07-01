package queries.query2.window1;

import utils.ConfStrings;
import utils.ShipInfo;

import java.util.HashSet;

public class SeaCellAccumulator {
  //  private final HashMap<String, HashSet<String>> tripPerHourMap;
    private final HashSet<String> amTrips;
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

    public void add(ShipInfo shipInfo) {
        if (shipInfo.getHour_range().equals(ConfStrings.ANTE_MERIDIAM.getString())){
            this.amTrips.add(shipInfo.getTrip_id());
        }else{
            this.pmTrips.add(shipInfo.getTrip_id());
        }
    }

    public void mergeAM(HashSet<String> amToMerge){
        this.amTrips.addAll(amToMerge);
    }

    public void mergePM(HashSet<String> pmToMerge){
        this.pmTrips.addAll(pmToMerge);
    }
}
