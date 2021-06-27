package queries.query1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import scala.Serializable;

import utils.ShipInfo;
import utils.ShipType;

import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;

public class Query1Operators implements Serializable {

    private static final double[] westernSea = {-6.0, 11.734}; // western mediterranean
    private static final Double initLatitude = 32.0;
    private static final Double endLatitude = 45.0;
    private static final Double initLongitude = -6.0;
    private static final Double endLongitude = 37.0;
    private static final Double stepLat = (endLatitude - initLatitude) / 10.0;
    private static final Double stepLon = (endLongitude - initLongitude) / 40.0;

    public static FlatMapFunction<Tuple2<Long, String>, ShipInfo> parseInputFunction() {
        return new FlatMapFunction<Tuple2<Long, String>, ShipInfo>() {
            @Override
            public void flatMap(Tuple2<Long, String> tuple, Collector<ShipInfo> collector) throws Exception {
                ShipInfo data;
                String[] info = tuple.f1.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                try {
                    data = new ShipInfo(info[0], Integer.valueOf(info[1]), Double.valueOf(info[3]), Double.valueOf(info[4]), info[7], info[10]);
                    collector.collect(data);
                } catch (NumberFormatException | ParseException ignored) {
                    //todo change this send a log message
                }
            }
        };
    }

    /**
     * @return true if the ship is located in western mediterranean
     */
    public static FilterFunction<ShipInfo> filterRegion() {
        return new FilterFunction<ShipInfo>() {
            @Override
            public boolean filter(ShipInfo shipInfo) throws Exception {
                return isWesternMediterranean(shipInfo);
            }
        };
    }

    public static boolean isWesternMediterranean(ShipInfo shipInfo) {
        return shipInfo.getLON() >= westernSea[0] && shipInfo.getLON() <= westernSea[1];
    }

    public static MapFunction<ShipInfo,  ShipInfo> computeCellId() {
        return new MapFunction<ShipInfo,  ShipInfo>() {
            @Override
            public ShipInfo map(ShipInfo shipInfo) throws Exception {
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
                shipInfo.setCellId(cellID);
                return  shipInfo;
            }
        };
    }

    public static MapFunction<ShipAvgOut, String> ExportOutcomeToString() {
        return new MapFunction<ShipAvgOut, String>() {
            @Override
            public String map(ShipAvgOut shipAvgOut) {
                StringBuilder builder = new StringBuilder();
                builder.append(shipAvgOut.getStartWindowDate()).append(",");
                builder.append(shipAvgOut.getCellId());
               // shipAvgOut.getResultMap().forEach((k, v) -> builder.append(",").append(k).append(",").append(v));

                ShipType[] types = ShipType.values();
                Arrays.sort(types);
                HashMap<ShipType, Double> resultMap = shipAvgOut.getResultMap();
                for (ShipType t : types){
                    builder.append(",");
                    Double s = resultMap.get(t);
                    if (s == null){
                        builder.append(t.name()).append(",").append("0");
                    }else {
                        builder.append(t.name()).append(",").append(s);
                    }
                }
                return builder.toString();
            }
        };
    }


    //todo leva questa roba!!!!!
  public static Tuple2<String, ShipInfo> test(ShipInfo shipInfo){

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
        String cellID = "" + label  + LONPosition;
        return new Tuple2<String, ShipInfo>(cellID, shipInfo);

    }


    public static void main(String[] args) throws ParseException {
        ShipInfo info = new ShipInfo("0xc35c9ebbf48cbb5857a868ce441824d0b2ff783a",99,  -5.9, 35.9109,"10/03/2015 12:15", "0xc35c9_10-03-15 12:xx - 10-03-15 13:26" );
        System.out.printf(test(info).toString());
                //0xc35c9ebbf48cbb5857a868ce441824d0b2ff783a	99	8.2	14.56034	35.8109	109	511	10/03/2015 12:15	MARSAXLOKK		0xc35c9_10-03-15 12:xx - 10-03-15 13:26
    }
}

