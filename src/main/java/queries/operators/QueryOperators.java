package queries.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import queries.query1.ShipAvgOut;
import scala.Serializable;

import utils.ShipInfo;
import utils.ShipType;

import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;

public class QueryOperators implements Serializable {

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
                return shipInfo.isWesternMediterranean();
            }
        };
    }



    public static MapFunction<ShipInfo,  ShipInfo> computeCellId() {
        return new MapFunction<ShipInfo,  ShipInfo>() {
            @Override
            public ShipInfo map(ShipInfo shipInfo) throws Exception {
                 Navigator.findCellID(shipInfo);
                 Navigator.setSea(shipInfo);
                 return shipInfo;
            }
        };
    }

    public static MapFunction<ShipAvgOut, String> ExportQuery1OutcomeToString() {
        return new MapFunction<ShipAvgOut, String>() {
            @Override
            public String map(ShipAvgOut shipAvgOut) {
                StringBuilder builder = new StringBuilder();
                builder.append(shipAvgOut.getStartWindowDate()).append(",");
                builder.append(shipAvgOut.getCellId());

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

}

