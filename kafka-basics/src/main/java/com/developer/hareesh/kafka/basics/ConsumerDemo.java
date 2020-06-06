package com.developer.hareesh.kafka.basics;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootStrapServers = PropertiesUtil.getProperty("bootStrapServer");;
        String topicName = "first_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-application-tw");

        @SuppressWarnings("resource")
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
        while (true) {
            kafkaConsumer.poll(Duration.ofMillis(100)).forEach((record) -> {
                logger.info("key: " + record.key() +
                        " Topic: " + record.topic() +
                        " Value: " + record.value() +
                        " Partition: " + record.partition()
                        + "TimeStamp: " + record.timestamp());
            });
        }

    }

}
