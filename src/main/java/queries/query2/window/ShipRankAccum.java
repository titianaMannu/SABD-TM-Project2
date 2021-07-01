package queries.query2.window;

import utils.ShipInfo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;


public class ShipRankAccum {
    private final HashMap<String, ShipRankStat> hourStateMap;

    public ShipRankAccum() {
        this.hourStateMap = new HashMap<>();
    }


    public void add(ShipInfo shipInfo){
        hourStateMap.merge(shipInfo.getHour_range(), new ShipRankStat(shipInfo), (o, n) -> {
            o.addTrip(shipInfo.getCellId(), new HashSet<>(Arrays.asList(shipInfo.getTrip_id())));
            return o;
                });
    }

    public void mergeStat(String key, ShipRankStat shipRankStat){
        this.getHourStateMap().merge(key, shipRankStat, ShipRankStat::merge);

    }

    public HashMap<String, ShipRankStat> getHourStateMap() {
        return hourStateMap;
    }
}
