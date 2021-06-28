package queries.query2;

import scala.Tuple2;
import utils.ShipInfo;

import java.util.*;

public class ShipRankStat {
    //unaware of hour category

    // hashmap<cellId, HashSet<tripId>>
    private final HashMap<String, HashSet<String>> tripPerCellMap;

    public ShipRankStat() {
        this.tripPerCellMap = new HashMap<>();
    }

    public ShipRankStat(ShipInfo shipInfo){
        this();
        addTrip(shipInfo.getCellId(), new HashSet<>(Arrays.asList(shipInfo.getTrip_id())));
    }

    public ShipRankStat(String cellId, HashSet<String> trips){
        this();
        addTrip(cellId, trips);
    }

    public void addTrip(String cellId, HashSet<String> trips){
        tripPerCellMap.merge(cellId, trips, (old, newVal) -> {
            old.addAll(newVal);
            return old;
        });

    }

    public ShipRankStat merge(ShipRankStat outShipRankStat){
        for (String cellId : outShipRankStat.getTripPerCellMap().keySet()){
            addTrip(cellId, outShipRankStat.getTripPerCellMap().get(cellId));
        }
        return this;
    }

    public HashMap<String, HashSet<String>> getTripPerCellMap() {
        return tripPerCellMap;
    }

    public List<String> getRank(){
        List<String> resultRank = new ArrayList<>();
        //  list creation from elements of the HashMap
        List<Tuple2<String, Integer>> counterTripPerCellList = new ArrayList<>();
        for (Map.Entry<String, HashSet<String>> entry : this.tripPerCellMap.entrySet()){
            counterTripPerCellList.add(new Tuple2<>(entry.getKey(), entry.getValue().size()));
        }

        // Sort in descending order
        counterTripPerCellList.sort((o1, o2) -> o2._2().compareTo(o1._2()));
        long rankDegree = 3L;
        for (int i = 0; i< rankDegree; i++){
            resultRank.add(counterTripPerCellList.get(i)._1());
        }

        return resultRank;

    }


}
