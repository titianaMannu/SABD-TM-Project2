
import kafka_utils.KafkaConfigurations;
import kafka_utils.KafkaCustomConsumer;
import utils.OutputUtils;


import java.util.ArrayList;
import java.util.Scanner;

/**
 * Class used to launch consumers for Flink and Kafka Streams output
 */
public class ConsumersRunner {

    public static void main(String[] args) {

        //cleaning result directory to store data results
        OutputUtils.cleanResultsFolder();
        System.out.println("Result directory prepared");

        // create a consumer structure to allow stopping
        ArrayList<KafkaCustomConsumer> consumers = new ArrayList<>();

        int id = 0;
        // launch Flink topics consumers
        for (int i = 0; i < KafkaConfigurations.FLINK_TOPICS.length; i++) {
            KafkaCustomConsumer consumer = new KafkaCustomConsumer(id,
                    KafkaConfigurations.FLINK_TOPICS[i],
                    OutputUtils.FLINK_OUTPUT_FILES[i], OutputUtils.FLINK_HEADERS[i]);
            consumers.add(consumer);
            new Thread(consumer).start();
            id++;
        }


        System.out.println("\u001B[36m" + "Enter something to stop consumers" + "\u001B[0m");
        Scanner scanner = new Scanner(System.in);
        // wait for the user to digit something
        scanner.next();
        System.out.println("Sending shutdown signal to consumers");
        // stop consumers
        for (KafkaCustomConsumer consumer : consumers) {
            consumer.stop();
        }
        System.out.flush();
        System.out.println("Signal sent, closing...");
    }
}
