package kafka_utils;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * Class used to serialize output strings to publish on Kafka
 */
public class FlinkToKafkaSerializer implements KafkaSerializationSchema<String> {
    //output topic
    private final String topic;

    public FlinkToKafkaSerializer(String topic) {
        super();
        this.topic = topic;
    }

    /**
     * Converts the string to a Kafka-record
     *
     * @param value string to publish
     * @return kafka-compliant record
     */
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String value, @Nullable Long aLong) {
        return new ProducerRecord<>(topic, value.getBytes(StandardCharsets.UTF_8));
    }
}
