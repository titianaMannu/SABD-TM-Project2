
import kafka_utils.KafkaConfigurations;
import kafka_utils.KafkaCustomConsumer;
import utils.FolderUtils;


import java.util.ArrayList;
import java.util.Scanner;

/**
 * Class used to launch consumers
 */
public class ConsumersRunner {

    public static void main(String[] args) {

        //cleaning result directory
        FolderUtils.cleanResultsFolder();
        System.out.println("Result directory cleaned");

        // create consumers
        ArrayList<KafkaCustomConsumer> consumers = new ArrayList<>();

        int id = 0;
        // launch consumers
        for (int i = 0; i < KafkaConfigurations.FLINK_TOPICS.length; i++) {
            KafkaCustomConsumer consumer = new KafkaCustomConsumer(id,
                    KafkaConfigurations.FLINK_TOPICS[i],
                    FolderUtils.FLINK_OUTPUT_FILES[i], FolderUtils.FLINK_HEADERS[i]);
            consumers.add(consumer);
            new Thread(consumer).start();
            id++;
        }


        System.out.println("\u001B[36m" + "Enter something to stop consumers" + "\u001B[0m");
        Scanner scanner = new Scanner(System.in);
        // wait for the user to digit something
        scanner.next();
        System.out.println("Sending a signal to stop consumers");
        // stop consumers
        for (KafkaCustomConsumer consumer : consumers) {
            consumer.stop();
        }
        System.out.flush();
        System.out.println("Signal sent, closing...");
    }
}
