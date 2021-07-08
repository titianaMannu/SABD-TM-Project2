import kafka_utils.KafkaCustomProducer;
import utils.ConfStrings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * This class is used to start a producer that reads from a file and send tuples to a Kafka topics
 */
@SuppressWarnings("BusyWait")
public class ProducerRunner {

    private static final Long SLEEP = 10L;
    //we need a fake dataset to close the last session on the real-one
    private static final String[] PATHS = {ConfStrings.PATH_DATASET_SOURCE.getString(), ConfStrings.PATH_FAKE_DATASET.getString()};


    public static void main(String[] args) {
        // create producer
        KafkaCustomProducer producer = new KafkaCustomProducer();
        String line;
        for (String path : PATHS) {
            try {
                // read from file
                FileReader file = new FileReader(path);
                BufferedReader bufferedReader = new BufferedReader(file);
                while ((line = bufferedReader.readLine()) != null) {
                    try {
                        // simulating a data stream processing source
                        producer.produce(line);
                        // for real data stream processing source simulation
                        Thread.sleep(SLEEP);
                    } catch (InterruptedException ignored) {
                        //ignore
                    }
                }


                bufferedReader.close();
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        producer.close();
        System.out.println("Producer job done!");
    }

}
