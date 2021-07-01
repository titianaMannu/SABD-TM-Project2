package queries.query2.window;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class ShipRankOut {
    private Date startWindowDate;
    private String sea;
    private final HashMap<String, List<String>> resultRank = new HashMap<>();

    public ShipRankOut() {
    }

    public void addRank(String hour, List<String > rank){
        this.resultRank.put(hour, rank);
    }

    public String getStartWindowDate() {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyyy");
        return dateFormatter.format(startWindowDate);
    }

    public void setStartWindowDate(Date startWindowDate) {
        this.startWindowDate = startWindowDate;
    }

    public String getSea() {
        return sea;
    }

    public HashMap<String, List<String>> getResultRank() {
        return resultRank;
    }

    public void setSea(String sea) {
        this.sea = sea;
    }
}
