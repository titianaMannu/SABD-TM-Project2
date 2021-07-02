import kafka_utils.KafkaCustomProducer;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Class used to start a producer that reads from file and send tuples to Kafka topics
 */
@SuppressWarnings("BusyWait")
public class ProducerRunner {

    private static final String CSV_PATH = "data/dataset.csv";
    private static final Long SLEEP = 10L;
    //we need a fake dataset to close the last session on the real-one
    private static final String[] PATHS = {"data/ridotto_ancora.csv", "data/fake_dataset.CSV"};


    public static void main(String[] args) {

        // create producer
        KafkaCustomProducer producer = new KafkaCustomProducer();

        String line;
        Long eventTime;
        for (String path : PATHS) {
            try {
                // read from file
                FileReader file = new FileReader(path);
                BufferedReader bufferedReader = new BufferedReader(file);
                DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm", Locale.US);
                while ((line = bufferedReader.readLine()) != null) {
                    try {
                        // produce tuples simulating a data stream processing source
                        String[] info = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                        eventTime = format.parse(info[7]).getTime();
                        producer.produce(eventTime, line, eventTime);
                        // for real data stream processing source simulation
                        Thread.sleep(SLEEP);
                    } catch (ParseException | InterruptedException ignored) {
                    }
                }


                bufferedReader.close();
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        producer.close();
        System.out.println("Producer process completed");
    }

}
