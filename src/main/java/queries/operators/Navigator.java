package queries.operators;


import utils.SeaType;

/**
 * This class contains functions to identify cellId and Sea buy using geographic coordinates
 */
public class Navigator {
    private static final double[] westernSea = {-6.0, 11.733}; // western mediterranean
    private static final Double initLatitude = 32.0;
    private static final Double endLatitude = 45.0;
    private static final Double initLongitude = -6.0;
    private static final Double endLongitude = 37.0;
    private static final Double stepLat = (endLatitude - initLatitude) / 10.0;
    private static final Double stepLon = (endLongitude - initLongitude) / 40.0;

    /**
     * Used to compute the correct cellId
     *
     * @param latitude  input latitude
     * @param longitude input longitude
     * @return proper cellID e.g. A1
     */
    public static String findCellID(Double latitude, Double longitude) {
        char label = 'A';
        //computing correct position
        int LATPosition = (int) ((latitude - initLatitude) / stepLat);
        int LONPosition = (int) (1 + ((longitude - initLongitude) / stepLon));

        label += LATPosition;
        return "" + label + LONPosition;
    }

    /**
     * Use sicily channel to logically separate western mediterranean to east mediterranean
     *
     * @param longitude input longitude
     * @return SeaType
     */
    public static SeaType findSea(Double longitude) {
        //set sea type
        if (isWesternMediterranean(longitude)) {
            return SeaType.WESTERN_MEDITERRANEAN_SEA;
        } else {
            return SeaType.EAST_MEDITERRANEAN_SEA;
        }
    }

    /**
     * @param longitude input longitude
     * @return true if is western mediterranean
     */
    public static boolean isWesternMediterranean(Double longitude) {
        return longitude >= westernSea[0] && longitude <= westernSea[1];
    }
}
