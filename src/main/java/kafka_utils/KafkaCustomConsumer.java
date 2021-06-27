package kafka_utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

/**
 * Class used to create consumers, subscribe them to topics and run them
 */
public class KafkaCustomConsumer implements Runnable {
    private final static int POLL_WAIT_TIME = 1000;
    private final static int CYCLE_INTERVAL_TIME = 5000;
    private String CONSUMER_GROUP_ID = "-topics-consumers";
    private final Consumer<String, String> consumer;
    private final int id;
    private final String topic;
    private final String path;
    private boolean running = true;
    private final String CSVHeader;

    /**
     * Create a new consumer using the properties
     *
     * @return the created consumer
     */
    private Consumer<String, String> createConsumer() {
        Properties props = KafkaClusterConfig.getKafkaCustomConsumerProperties(CONSUMER_GROUP_ID);
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }

    /**
     * Subscribe a consumer to a topic
     *
     * @param consumer to be subscribe
     * @param topic    chosen
     */
    private static void subscribeToTopic(Consumer<String, String> consumer, String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    /**
     * Create a parametric consumer based on the arguments
     *
     * @param id    consumer's id
     * @param topic name
     * @param path  where to store result if it's a flink consumer, null if it isn't
     */
    public KafkaCustomConsumer(int id, String topic, @Nullable String path, @Nullable String header) {
        CONSUMER_GROUP_ID = "flink" + CONSUMER_GROUP_ID;
        this.path = Objects.requireNonNull(path);
        this.id = id;
        this.topic = topic;
        this.CSVHeader = Objects.requireNonNull(header);

        // create the consumer
        consumer = createConsumer();

        // subscribe the consumer to the topic
        subscribeToTopic(consumer, topic);
    }

    @Override
    public void run() {
        runFlinkConsumer();
    }

    /**
     * Create a consumer that write flink queries' result to the csv in Results directory
     */
    @SuppressWarnings({"BusyWait", "ResultOfMethodCallIgnored"})
    private void runFlinkConsumer() {
        System.out.println("Flink Consumer " + CONSUMER_GROUP_ID + "-ID" + id + " running...");
        FileWriter writer;
        BufferedWriter bw;
        try {
            while (running) {
                Thread.sleep(CYCLE_INTERVAL_TIME);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_WAIT_TIME));

                if (!records.isEmpty()) {
                    File file = new File(path);

                    if (!file.isFile()) {
                        // creates the file if it does not exist
                        file.createNewFile();
                        writer = new FileWriter(file, true);
                        bw = new BufferedWriter(writer);
                        //append the header to the new file
                        bw.append(CSVHeader).append("\n");
                        bw.close();
                        writer.close();
                    }

                    writer = new FileWriter(file, true);
                    bw = new BufferedWriter(writer);
                    for (ConsumerRecord<String, String> record : records) {
                        bw.append(record.value());
                        bw.append("\n");
                    }

                    // close both buffered writer and file writer
                    bw.close();
                    writer.close();
                }
            }

        } catch (InterruptedException ignored) {
            // ignored
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Could not export result to " + path);
        } finally {
            // close consumer
            consumer.close();
            System.out.println("Flink Consumer " + CONSUMER_GROUP_ID + "-ID" + id + " stopped");
        }
    }

    public void stop() {
        this.running = false;
    }
}
