package queries.query2.window2;

import utils.SeaType;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Class that contains second window processing outcome
 */
public class TripRankOutcome {
    private String startWindowDate;
    private SeaType sea;
    private final List<String> amCellTripRank; // am (cellsId) rank
    private final List<String> pmCellTripRank; // pm (cellsId) rank

    public TripRankOutcome() {
        this.amCellTripRank = new ArrayList<>();
        this.pmCellTripRank = new ArrayList<>();
    }

    public String getStartWindowDate() {
        return startWindowDate;
    }

    public void setStartWindowDate(Date startWindowDate) throws ParseException {
        DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
        String sDate = formatter.format(startWindowDate);
        this.startWindowDate = sDate;
    }

    public SeaType getSea() {
        return sea;
    }

    public void setSea(SeaType sea) {
        this.sea = sea;
    }


    public List<String> getAmCellTripRank() {
        return amCellTripRank;
    }

    /**
     * add a cellId to the outcome rank
     *
     * @param cellId string representing a cell
     */
    public void addAmCell(String cellId) {
        this.amCellTripRank.add(cellId);
    }

    public List<String> getPmCellTripRank() {
        return pmCellTripRank;
    }

    /**
     * add a cellId to the outcome rank
     *
     * @param cellId string representing a cell
     */
    public void addPmCell(String cellId) {
        this.pmCellTripRank.add(cellId);
    }
}
