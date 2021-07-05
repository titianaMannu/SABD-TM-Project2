package queries.query1;

import utils.ShipType;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Class that contains first query's window processing outcome
 */
public class ShipAvgOutcome {
    private Date startWindowDate;
    private String cellId;
    // <k= shipType, v = avg>
    private final HashMap<ShipType, Double> resultMap = new HashMap<>();

    //default constructor
    public ShipAvgOutcome() {
    }

    /**
     * Add a computed value to the hash map
     *
     * @param shipType ShipType
     * @param avg      Double, ship-type avg per cellId
     */
    public void addAvg(ShipType shipType, Double avg) {
        this.resultMap.put(shipType, avg);
    }


    public String getStartWindowDate() {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyyy");
        return dateFormatter.format(startWindowDate);
    }

    public String getCellId() {
        return cellId;
    }

    public HashMap<ShipType, Double> getResultMap() {
        return resultMap;
    }

    public void setStartWindowDate(Date startWindowDate) {
        this.startWindowDate = startWindowDate;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }
}
