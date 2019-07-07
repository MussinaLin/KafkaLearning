package mussina.kafka.course;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    private static JsonParser jsonParser = new JsonParser();
    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filterStream = inputTopic.filter(
            (k,v)-> extractFollowerinTweet(v) > 10000// filter for tweets which has more than 10000 follower
        );
        filterStream.to("important_tweets");

        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(
            streamsBuilder.build(),
            properties
        );

        //start stream application
        kafkaStreams.start();
    }

    private static int extractFollowerinTweet(String tweetJson){
        try {
            return jsonParser.parse(tweetJson).getAsJsonObject().get("user")
                .getAsJsonObject().get("followers_count").getAsInt();
        }catch (NullPointerException e){
            return 0;
        }
    }


}
