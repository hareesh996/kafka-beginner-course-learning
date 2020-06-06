import com.developer.hareesh.kafka.basics.PropertiesUtil;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TwitterConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());

    public RestHighLevelClient getElasticClient() {

        String hostName = PropertiesUtil.getProperty("elasticSearchHostName");
        String userName = PropertiesUtil.getProperty("userName");
        String password = PropertiesUtil.getProperty("pwd");

        BasicCredentialsProvider elasticCredentials = new BasicCredentialsProvider();

        elasticCredentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostName, 443, "https")
        ).setHttpClientConfigCallback(httpAsyncClientBuilder -> {
            httpAsyncClientBuilder.setDefaultCredentialsProvider(elasticCredentials);
            return httpAsyncClientBuilder;
        });
        RestHighLevelClient elasticRestClient = new RestHighLevelClient(builder);

        return elasticRestClient;
    }

    public KafkaConsumer<String, String> getKafkaConsumer(String topicName) {

        String bootStrapServers = PropertiesUtil.getProperty("bootStrapServers");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-application-tw");

        KafkaConsumer<String, String> twitterKafkaConsumer = new KafkaConsumer<String, String>(properties);
        twitterKafkaConsumer.subscribe(Arrays.asList(topicName));
        return twitterKafkaConsumer;
    }

    public static void main(String[] args) throws IOException {
        TwitterConsumer twConsumer = new TwitterConsumer();
        RestHighLevelClient elasticRestClient = twConsumer.getElasticClient();
        KafkaConsumer<String, String> twitterConsumer = twConsumer.getKafkaConsumer("twitter_tweet");
        BulkRequest bulkRequest = new BulkRequest();
        int noRecords = 0;
        while(true){
            ConsumerRecords<String, String> records = twitterConsumer.poll(Duration.ofMillis(100));
            if(records.count()  == 0 ){
                if(noRecords > 20){
                    break;
                }
                noRecords++;
                continue;
            }
            for (ConsumerRecord<String, String> record : records) {
                bulkRequest.add(new IndexRequest("twitter").source(record.value(), XContentType.JSON));
            }
        }
        BulkResponse indexResponse = elasticRestClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        twitterConsumer.commitSync();
        TwitterConsumer.logger.info("The twitter index id : " + Arrays.deepToString(indexResponse.getItems()));
        elasticRestClient.close();

    }

}
