package utils;

public enum ConfStrings {
    CSV_HEADER_QUERY1("TIMESTAMP,CELL_ID,SHIP_TYPE1,AVG1_1,SHIP_TYPE2,AVG2,SHIP_TYPE3,AVG3,SHIP_TYPE4,AVG4"),
    CSV_HEADER_QUERY2("TIMESTAMP,SEA,SLOT-A,RANK-A,SLOT-P,RANK-P"),
    QUERY1_CSV_WEEKLY_OUT_PATH("Results/query1_weekly.csv"),
    QUERY1_CSV_MONTHLY_OUT_PATH("Results/query1_monthly.csv"),
    QUERY2_CSV_WEEKLY_OUT_PATH("Results/query2_weekly.csv"),
    QUERY2_CSV_MONTHLY_OUT_PATH("Results/query2_monthly.csv"),
    FLINK_QUERY1_WEEKLY_OUT_TOPIC("flink-output-topic-query1-weekly"),
    FLINK_QUERY1_MONTHLY_OUT_TOPIC("flink-output-topic-query1-monthly"),
    FLINK_QUERY2_WEEKLY_OUT_TOPIC( "flink-output-topic-query2-weekly"),
    FLINK_QUERY2_MONTHLY_OUT_TOPIC("flink-output-topic-query2-monthly"),
    KAFKA_BROKER1("localhost:9092"),
    KAFKA_BROKER2("localhost:9093"),
    KAFKA_BROKER3("localhost:9094"),
    POST_MERIDIAN("pm"),
    ANTE_MERIDIAN("am"),
    RESULTS_DIR("Results"),
    PATH_DATASET_SOURCE("data/dataset.csv"),
    PATH_FAKE_DATASET("data/fake_dataset.CSV");


    private final String string;

    ConfStrings(String first) {
        this.string = first;
    }



    public String getString() {
        return string;
    }
}
