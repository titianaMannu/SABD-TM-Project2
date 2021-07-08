package kafka_utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 *  Used to create the producer that will produce tuples as a data stream to a kafka topic
 */
public class KafkaCustomProducer {
	private static final String PRODUCER_ID = "single-flink";
	private final Producer<Long, String> producer;

	/**
	 * Default constructor: initialize the producer
	 */
	public KafkaCustomProducer() {
		producer = createProducer();
	}

	/**
	 * kafka producer creation
	 * @return instance of a  kafka producer
	 */
	private static Producer<Long, String> createProducer() {
		// retrieve producer properties
		Properties props = KafkaConfigurations.getKafkaCustomProducerProperties(PRODUCER_ID);
		return new KafkaProducer<>(props);
	}

	/**
	 * publish a message to a flink stream topic
	 * @param value string message to send
	 */
	public void produce(String value) {
		// produce a record for Flink
		final ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaConfigurations.FLINK_TOPIC, null,
				value);
		//send the record
		producer.send(record);
	}

	/**
	 * Flush all records and close the flink producer
	 */
	public void close() {
		producer.flush();
		producer.close();
	}
}
