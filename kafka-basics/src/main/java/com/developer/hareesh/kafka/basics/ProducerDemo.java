package com.developer.hareesh.kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getName());
		String bootStrapServers = PropertiesUtil.getProperty("bootStrapServer");
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> kafkaRecord = new KafkaProducer<String, String>(properties);

		String topicName = "first_topic";

		for (int i =6; i < 10; i++) {
			String message = "Message Description :" + Integer.toString(i);
			// The key helps in message to the same partition. Example: If a key_1 goes for first time to partition 1 , then after wards all the messages 
			// having key key_1 will go to partition 1. 
			String key = "key_"+Integer.toString(i);
			final ProducerRecord<String, String> producer = new ProducerRecord<String, String>(topicName,key, message);

			kafkaRecord.send(producer, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						logger.info(" record sent successfully: \n" + "Topic: " + metadata.topic() + "\n"
								+ "Partition: " + metadata.partition() + "\n" + "OffSet" + metadata.offset());
					} else {
						logger.error("Error in sending the record " + producer, exception);
					}
				}
			});
		}

		kafkaRecord.flush();
		kafkaRecord.close();
	}

}
