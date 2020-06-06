import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreams {
    public static void main(String[] args) {
        String bootStrapServers = "127.0.0.1:9092";

        // setup the stream config properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"twitter_signed_used");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> kStream = streamsBuilder.stream("twitter_tweet");
        // filtering tweets whose followers count is more than 10000 into a new topic called twitter_signed_tweet.
        kStream.filter((key, value) -> extractNumberOfFollowers(value) > 0)
                .to("twitter_signed_tweet");

        org.apache.kafka.streams.KafkaStreams kafkaStreams = new org.apache.kafka.streams.KafkaStreams(streamsBuilder.build(),properties);
        kafkaStreams.start();
    }

    private static Integer extractNumberOfFollowers(String v) {
        try {
            return JsonParser.parseString(v)
                    .getAsJsonObject()
                    .get("user").getAsJsonObject()
                    .get("followers_count").getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
