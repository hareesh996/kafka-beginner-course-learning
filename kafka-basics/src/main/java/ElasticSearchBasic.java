import com.developer.hareesh.kafka.basics.PropertiesUtil;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchBasic {

    private final static Logger logger = LoggerFactory.getLogger(ElasticSearchBasic.class.getName());

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


    public static void main(String[] args) {
        ElasticSearchBasic elasticSearchBasic = new ElasticSearchBasic();
        RestHighLevelClient elasticSearchClient =  elasticSearchBasic.getElasticClient();

        IndexRequest indexRequest = new IndexRequest("twitter")
                                    .source("{\"foo\":\"bar\"}", XContentType.JSON);
        elasticSearchClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                ElasticSearchBasic.logger.info(indexResponse.getId());
                try {
                    elasticSearchClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(Exception e) {
                ElasticSearchBasic.logger.error("error for having index"+indexRequest.getDescription(),e);
                try {
                    elasticSearchClient.close();
                } catch (IOException ec) {
                    ec.printStackTrace();
                }
            }
        });
    }

}
