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
 * Class with all the topics' name and the getter for all the kind of properties
 */
public class KafkaClusterConfig {
    // topics
    public static final String FLINK_TOPIC = "flink-topic";
    public static final String[] FLINK_TOPICS = {
            ConfStrings.FLINK_QUERY1_WEEKLY_OUT_TOPIC.getString(),
            ConfStrings.FLINK_QUERY1_MONTHLY_OUT_TOPIC.getString(),
            ConfStrings.FLINK_QUERY2_WEEKLY_OUT_TOPIC.getString(),
            ConfStrings.FLINK_QUERY2_MONTHLY_OUT_TOPIC.getString()
    };


    // if consumer has no offset for the queue starts from the first record
    private static final String CONSUMER_FIRST_OFFSET = "earliest";
    // for exactly once production
    private static final boolean ENABLE_PRODUCER_EXACTLY_ONCE = true;
    private static final String ENABLE_CONSUMER_EXACTLY_ONCE = "read_committed";


    // kafka servers
    public static final String KAFKA_BROKERS = ConfStrings.KAFKA_BROKER1.getString() + "," + ConfStrings.KAFKA_BROKER2.getString() + "," + ConfStrings.KAFKA_BROKER3.getString();

    /**
     * Creates properties for a Kafka Consumer representing the Flink stream source
     *
     * @param consumerGroupId id of consumer group
     * @return created properties
     */
    public static Properties getFlinkSourceProperties(String consumerGroupId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // set consumer group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        // start reading from beginning of partition if no offset was created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_FIRST_OFFSET);
        // exactly once semantic
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ENABLE_CONSUMER_EXACTLY_ONCE);

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

    /**
     * Creates properties for a Kafka Producer representing the one Flink processing sink
     *
     * @param producerId producer's id
     * @return created properties
     */
    public static Properties getFlinkSinkProperties(String producerId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // set producer id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        // exactly once semantic
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_PRODUCER_EXACTLY_ONCE);

        return props;
    }

    /**
     * Creates properties for a Kafka Consumer representing one output subscriber
     *
     * @param consumerGroupId id of consumer group
     * @return created properties
     */
    public static Properties getKafkaCustomConsumerProperties(String consumerGroupId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.KAFKA_BROKERS);
        // set consumer group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        // start reading from beginning of partition if no offset was created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaClusterConfig.CONSUMER_FIRST_OFFSET);
        // exactly once semantic
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ENABLE_CONSUMER_EXACTLY_ONCE);

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

    /**
     * Creates properties for a Kafka Producer representing the entire stream processing source
     *
     * @param producerId producer's id
     * @return created properties
     */
    public static Properties getKafkaSingleProducerProperties(String producerId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // set producer id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        // exactly once semantic
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_PRODUCER_EXACTLY_ONCE);

        // key and value serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }
}
