package kafka_utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Class used to create the producer that will produce tuples as a data stream in a kafka topic
 */
public class KafkaSingleProducer {
	private static final String PRODUCER_ID = "single-producer";
	private final Producer<Long, String> producer;

	/**
	 * Default constructor that sets the producer
	 */
	public KafkaSingleProducer() {
		producer = createProducer();
	}

	/**
	 * Create a new kafka producer
	 * @return the created kafka producer
	 */
	private static Producer<Long, String> createProducer() {
		// get the producer properties
		Properties props = KafkaClusterConfig.getKafkaSingleProducerProperties(PRODUCER_ID);
		return new KafkaProducer<>(props);
	}

	/**
	 * Function that publish a message to both the flink's and kafka streams' topic
	 * @param key needed to set the key in the kafka streams topic for a correctly process
	 * @param value line to be send
	 * @param timestamp event time
	 */
	public void produce(Long key, String value, Long timestamp) {
		// no need to put timestamp as key for Flink
		final ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaClusterConfig.FLINK_TOPIC, null,
				value);
		//send the records
		producer.send(record);
	}

	/**
	 * Flush and close the producer
	 */
	public void close() {
		producer.flush();
		producer.close();
	}
}
