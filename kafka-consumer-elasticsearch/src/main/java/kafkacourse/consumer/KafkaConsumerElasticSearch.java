package kafkacourse.consumer;

import com.google.gson.JsonParser;
import kafkacourse.consumer.util.ElasticSearchConstant;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerElasticSearch {

    public static Logger logger = LoggerFactory.getLogger(KafkaConsumerElasticSearch.class);
    public static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        // poll
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for(ConsumerRecord<String, String> record : records){

                String id = extractIdFromTweet(record.value());

                // insert data to elastic-search
                String jsonString = record.value();
                IndexRequest indexRequest = new IndexRequest("twitter", "tweet", id)// id make consumer idempotent
                    .source(jsonString, XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info("id:" + indexResponse.getId());

                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

//        client.close();

    }

    public static RestHighLevelClient createClient(){

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(ElasticSearchConstant.userName, ElasticSearchConstant.password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(ElasticSearchConstant.hostname,443, "https")).setHttpClientConfigCallback(
            new RestClientBuilder.HttpClientConfigCallback(){
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder){
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            }
        );

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrap_servers = "127.0.0.1:9092";
        String groupID = "elastic-search-demo";
        // consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // subscribe
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    private static String extractIdFromTweet(String json){
        return jsonParser.parse(json).getAsJsonObject().get("id_str").getAsString();
    }
}
