package kafka_utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Usage:
 * 1. consumer creation
 * 2. subscription to a topic
 * 3. run consumers
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
     * used for consumer creation
     *
     * @return an instance of  consumer
     */
    private Consumer<String, String> createConsumer() {
        Properties properties = KafkaConfigurations.getKafkaCustomConsumerProperties(CONSUMER_GROUP_ID);
        return new KafkaConsumer<>(properties);
    }

    /**
     * Subscribe a consumer to a topic
     *
     * @param consumer to be subscribe
     * @param topic    topic of interest
     */
    private static void subscribeToTopic(Consumer<String, String> consumer, String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    /**
     * @param id     consumer's id
     * @param topic  name of the topic
     * @param path   destination path for the results
     * @param header csv header
     */
    public KafkaCustomConsumer(int id, String topic, String path, String header) {
        CONSUMER_GROUP_ID = "flink" + CONSUMER_GROUP_ID;
        this.path = path;
        this.id = id;
        this.topic = topic;
        this.CSVHeader = header;

        // consumer creation
        consumer = createConsumer();

        // consumer subscription to a topic
        subscribeToTopic(consumer, topic);
    }

    @Override
    public void run() {
        runFlinkConsumer();
    }

    /**
     * Create a consumer that writes results to the csv in the output path
     */
    @SuppressWarnings({"BusyWait", "ResultOfMethodCallIgnored"})
    private void runFlinkConsumer() {
        System.out.println("Consumer: " + CONSUMER_GROUP_ID + "-ID" + id + " is running...");
        FileWriter writer;
        BufferedWriter bw;
        try {
            while (running) {
                Thread.sleep(CYCLE_INTERVAL_TIME);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_WAIT_TIME));

                if (!records.isEmpty()) {
                    //there are records consumed
                    File file = new File(path);
                    if (!file.isFile()) {
                        // creation of the file if it does not exist
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
                        //append all records to the reader
                        bw.append(record.value()).append("\n");
                    }

                    // close both buffered writer and file writer
                    bw.close();
                    writer.close();
                }
            }

        } catch (InterruptedException ignored) {
            //not print exception
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Unable to write results to:: " + path);
        } finally {
            // closing consumer
            consumer.close();
            System.out.println("Consumer: " + CONSUMER_GROUP_ID + "-ID" + id + " stop.");
        }
    }

    public void stop() {
        this.running = false;
    }
}
