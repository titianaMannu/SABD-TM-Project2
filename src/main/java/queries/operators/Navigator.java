package queries.operators;


import utils.SeaType;

public class Navigator {
    private static final double[] westernSea = {-6.0, 11.734}; // western mediterranean
    private static final Double initLatitude = 32.0;
    private static final Double endLatitude = 45.0;
    private static final Double initLongitude = -6.0;
    private static final Double endLongitude = 37.0;
    private static final Double stepLat = (endLatitude - initLatitude) / 10.0;
    private static final Double stepLon = (endLongitude - initLongitude) / 40.0;

    public static String findCellID(Double latitude, Double longitude) {
        char label = 'A';
        int LATPosition = (int) ((latitude - initLatitude)/stepLat);
        int LONPosition = (int) (1 + (longitude - initLongitude)/stepLon);
        label += LATPosition;

        return "" + label + LONPosition;
    }

    public static SeaType findSea(Double longitude){
        //set sea type
        if(isWesternMediterranean(longitude)){
            return SeaType.WESTERN_MEDITERRANEAN_SEA;
        }else {
            return SeaType.EAST_MEDITERRANEAN_SEA;
        }
    }

    public  static boolean isWesternMediterranean(Double longitude) {
        return longitude >= westernSea[0] && longitude <= westernSea[1];
    }
}
