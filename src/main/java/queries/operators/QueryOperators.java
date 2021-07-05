package queries.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import queries.query1.ShipAvgOutcome;
import queries.query2.window2.TripRankOutcome;
import scala.Serializable;
import utils.ConfStrings;
import utils.ShipInfo;
import utils.ShipType;

import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;

public class QueryOperators implements Serializable {

    /**
     * Function used to parse source stream
     */
    public static FlatMapFunction<Tuple2<Long, String>, ShipInfo> parseInputFunction() {
        return new FlatMapFunction<Tuple2<Long, String>, ShipInfo>() {
            @Override
            public void flatMap(Tuple2<Long, String> tuple, Collector<ShipInfo> collector) throws Exception {
                ShipInfo data;
                String[] info = tuple.f1.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                try {
                    //creation of ShipInfo
                    data = new ShipInfo(info[0], Integer.valueOf(info[1]), Double.valueOf(info[3]), Double.valueOf(info[4]), info[7], info[10]);
                    collector.collect(data);
                } catch (NumberFormatException | ParseException e) {
                    e.printStackTrace();
                    //just print the error then goes on with his job
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


    /**
     * Scope - Query 1
     * Used to extract an output string from the outcome
     */
    public static MapFunction<ShipAvgOutcome, String> ExportQuery1OutcomeToString() {
        return new MapFunction<ShipAvgOutcome, String>() {
            @Override
            public String map(ShipAvgOutcome shipAvgOutcome) {
                StringBuilder builder = new StringBuilder();
                builder.append(shipAvgOutcome.getStartWindowDate()).append(",");
                builder.append(shipAvgOutcome.getCellId());

                ShipType[] types = ShipType.values();
                Arrays.sort(types);
                HashMap<ShipType, Double> resultMap = shipAvgOutcome.getResultMap();
                for (ShipType t : types) {
                    builder.append(",");
                    Double s = resultMap.get(t);
                    if (s == null) {
                        builder.append(t.name()).append(",").append("0");
                    } else {
                        builder.append(t.name()).append(",").append(s);
                    }
                }
                return builder.toString();
            }
        };
    }

    /**
     * Scope - Query 2
     * Used to extract an output string from the outcome
     */
    public static MapFunction<TripRankOutcome, String> ExportQuery2OutcomeToString() {
        return new MapFunction<TripRankOutcome, String>() {

            @Override
            public String map(TripRankOutcome tripRankOutcome) throws Exception {
                StringBuilder builder = new StringBuilder();
                builder.append(tripRankOutcome.getStartWindowDate()).append(",")
                        .append(tripRankOutcome.getSea().name()).append(",");

                builder.append(ConfStrings.ANTE_MERIDIAN.getString()).append(", [ ");
                tripRankOutcome.getAmCellTripRank().forEach(cellID -> builder.append(cellID).append(" "));
                builder.append("],");

                builder.append(ConfStrings.POST_MERIDIAN.getString()).append(", [ ");
                tripRankOutcome.getPmCellTripRank().forEach(cellID -> builder.append(cellID).append(" "));
                builder.append("]");

                return builder.toString();
            }
        };
    }
}

