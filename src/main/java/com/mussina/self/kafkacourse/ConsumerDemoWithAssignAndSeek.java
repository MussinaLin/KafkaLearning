package com.mussina.self.kafkacourse;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithAssignAndSeek {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithAssignAndSeek.class);

    public static void main(String[] args) {
        String bootstrap_servers = "127.0.0.1:9092";
//        String groupID = "f";
        // consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe
//        consumer.subscribe(Collections.singleton("first_topic"));

        // assign
        TopicPartition topicPartition = new TopicPartition("first_topic", 0);
        consumer.assign(Arrays.asList(topicPartition));

        // seek
        consumer.seek(topicPartition, 10);

        boolean isNeedRead = true;
        int numOfRead = 0, numOfReadNum = 7;
        // poll
        while (isNeedRead){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for(ConsumerRecord<String, String> record : records){
                logger.info(String.format("topic:%s partition:%s offset:%s key:%s value:%s ",
                    record.topic(), record.partition(),record.offset(), record.key(), record.value()));
                numOfRead++;
                if(numOfRead >= numOfReadNum){
                    isNeedRead = false;
                    break;
                }
            }
        }
    }

}
