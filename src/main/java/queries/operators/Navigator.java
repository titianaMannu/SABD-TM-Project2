package queries.operators;


import utils.SeaType;
import utils.ShipInfo;

public class Navigator {
    private static final Double initLatitude = 32.0;
    private static final Double endLatitude = 45.0;
    private static final Double initLongitude = -6.0;
    private static final Double endLongitude = 37.0;
    private static final Double stepLat = (endLatitude - initLatitude) / 10.0;
    private static final Double stepLon = (endLongitude - initLongitude) / 40.0;

    public static void findCellID(ShipInfo shipInfo) {
        char label = 'A';
        Double i = initLatitude;
        int LATPosition = 0;
        while (i < endLatitude) {

            if (shipInfo.getLAT() >= i && shipInfo.getLAT() < i + stepLat) {
                break;
            }
            LATPosition++;
            i += stepLat;
        }


        Double j = initLongitude;
        int LONPosition = 1;
        while (j < endLongitude) {
            if (shipInfo.getLON() >= j && shipInfo.getLON() < j + stepLon) {
                break;
            }
            LONPosition++;
            j += stepLon;
        }

        label += LATPosition;
        String cellID = "" + label + LONPosition;
        //setting cell id
        shipInfo.setCellId(cellID);

    }

    public static void  setSea(ShipInfo shipInfo){
        //set sea type
        if(shipInfo.isWesternMediterranean()){
            shipInfo.setSeaType(SeaType.WESTERN_MEDITERRANEAN_SEA);
        }else {
            shipInfo.setSeaType(SeaType.EAST_MEDITERRANEAN_SEA);
        }
    }
}
