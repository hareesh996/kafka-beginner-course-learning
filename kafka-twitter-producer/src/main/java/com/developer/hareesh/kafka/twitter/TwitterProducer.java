package com.developer.hareesh.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.developer.hareesh.kafka.basics.PropertiesUtil;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

    final static Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey = PropertiesUtil.getProperty("consumerKey");
    String consumerSecret = PropertiesUtil.getProperty("consumerSecret");
    String token = PropertiesUtil.getProperty("token");
    String secrete = PropertiesUtil.getProperty("secrete");

    String topicName = "twitter_tweet";
    String bootStrapServers = PropertiesUtil.getProperty("bootStrapServers");

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        /**
         * Set up your blocking queues: Be sure to size these properly based on expected
         * TPS of your stream
         */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        // get the twitter client
        final Client twitterClient = this.createTwitterClient(msgQueue);
        twitterClient.connect();

        final KafkaProducer<String, String> kafkaTweetProducer = this.createKafkaTweetProducer();

        Thread shutDownHookThread = new Thread(new Runnable() {
            public void run() {
                twitterClient.stop();
                TwitterProducer.logger.info("Stopped the twitter client");
                kafkaTweetProducer.close();
                TwitterProducer.logger.info("Stopped the twitter producer");
            }
        });

        // close the twitter client and kafka producer on shutdown.
        Runtime.getRuntime().addShutdownHook(shutDownHookThread);

        while (!twitterClient.isDone()) {
            try {
                String tweet = msgQueue.poll(5, TimeUnit.SECONDS);
                if (tweet != null) {
                    sendTweetToKafka(kafkaTweetProducer, tweet);
                }
            } catch (InterruptedException e) {
                TwitterProducer.logger.info("An error thrown by the twitter client", e);
                twitterClient.stop();
            }

        }
    }

    private void sendTweetToKafka(KafkaProducer<String, String> kafkaTweetProducer, final String tweet) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(this.topicName, tweet);
        kafkaTweetProducer.send(producerRecord, new Callback() {

            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    TwitterProducer.logger.error("An error throw while sending the record to kafka", exception);
                } else {
                    TwitterProducer.logger.info(tweet, ": --> is successfully sent to the kafka");
                }

            }
        });
    }

    /*
     * Create the twitter client
     */
    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /**
         * Declare the host you want to connect to, the endpoint, and authentication
         * (basic auth or oauth)
         */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("india", "bitcoins");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(this.consumerKey, this.consumerSecret, this.token, this.secrete);

        ClientBuilder builder = new ClientBuilder().name(TwitterProducer.class.getName()) // optional: mainly for the logs
                .hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();

    }

    public KafkaProducer<String, String> createKafkaTweetProducer() {

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // This configuration is equivalent to the below three configuration. ACKS, max inflit requests, retry config
        // To make the producer safe
        kafkaProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        kafkaProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // This should be 1 in case kafka <1.1
        kafkaProperties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); // to be in max

        kafkaProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "60"); // put a delay so that messages can be sent as a batch instead of as single message
        kafkaProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(60 * 1024)); // keeping a batch size to particular level so that it reduces number of compressions
        kafkaProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // compress the json config

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);
        return kafkaProducer;
    }

}
