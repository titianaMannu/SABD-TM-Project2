package kafka_utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.ConfStrings;

import java.util.Properties;

/**
 * Class for Kafka properties
 */
public class KafkaConfigurations {

    public static final String FLINK_TOPIC = "flink-topic";
    //all the topics
    public static final String[] FLINK_TOPICS = {
            ConfStrings.FLINK_QUERY1_WEEKLY_OUT_TOPIC.getString(),
            ConfStrings.FLINK_QUERY1_MONTHLY_OUT_TOPIC.getString(),
            ConfStrings.FLINK_QUERY2_WEEKLY_OUT_TOPIC.getString(),
            ConfStrings.FLINK_QUERY2_MONTHLY_OUT_TOPIC.getString()
    };


    // if there isn't an offset for the queue starts from the first record
    private static final String CONSUMER_FIRST_OFFSET = "earliest";
    // for exactly once semantic
    private static final boolean ENABLE_PRODUCER_EXACTLY_ONCE = true;
    private static final String ENABLE_CONSUMER_EXACTLY_ONCE = "read_committed";


    // kafka servers
    public static final String KAFKA_BROKERS = ConfStrings.KAFKA_BROKER1.getString() + "," + ConfStrings.KAFKA_BROKER2.getString() + "," + ConfStrings.KAFKA_BROKER3.getString();

    /**
     * @param consumerGroupId id of consumer group
     * @return created properties
     */
    public static Properties getSourceProperties(String consumerGroupId) {
        Properties properties = new Properties();

        // specify brokers
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // set consumer group id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        // start reading from the beginning of a partition if there isn't an offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_FIRST_OFFSET);
        // exactly once semantic
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ENABLE_CONSUMER_EXACTLY_ONCE);

        // key and value deserializers
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return properties;
    }

    /**
     * @param producerId producer's id
     * @return created properties
     */
    public static Properties getFlinkSinkProperties(String producerId) {
        Properties properties = new Properties();

        //  brokers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // producer id
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        // exactly once semantic
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_PRODUCER_EXACTLY_ONCE);

        return properties;
    }

    /**
     * @param consumerGroupId consumer group-id
     * @return created properties
     */
    public static Properties getKafkaCustomConsumerProperties(String consumerGroupId) {
        Properties properties = new Properties();

        // kafka brokers
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfigurations.KAFKA_BROKERS);
        // set consumer group id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        // start reading from thr beginning of a partition it there isn't an offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConfigurations.CONSUMER_FIRST_OFFSET);
        // exactly once semantic
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ENABLE_CONSUMER_EXACTLY_ONCE);

        // key and value deserializers
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return properties;
    }

    /**
     * @param producerId producer id
     * @return created properties
     */
    public static Properties getKafkaCustomProducerProperties(String producerId) {
        Properties properties = new Properties();

        // kafka brokers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // producer id
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        // exactly once semantic
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_PRODUCER_EXACTLY_ONCE);

        // key and value serializers
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
