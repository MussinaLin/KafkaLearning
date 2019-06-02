package com.mussina.self.kafkacourse;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClients {

    Logger logger = LoggerFactory.getLogger(AdminClients.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient client = AdminClient.create(props);


        DescribeTopicsResult result = client.describeTopics(Arrays.asList("first_topic"));
        Map<String, KafkaFuture<TopicDescription>> values = result.values();
        KafkaFuture<TopicDescription> topicDescription = values.get("first_topic");
        int partitions = topicDescription.get().partitions().size();
        System.out.println(partitions);
        System.out.println(topicDescription.get().name());
        System.out.println(topicDescription.get().partitions().get(0).replicas().size());
        System.out.println(topicDescription.get().partitions().get(0).replicas().get(0));
    }

    public Properties getTopicProperties(final ZkUtils connection, final String topicName) {
        try {
            return AdminUtils.fetchEntityConfig(connection, ConfigType.Topic(), topicName);
        } catch (IllegalArgumentException | KafkaException e) {
            throw new TopicOperationException(topicName, e.getMessage(), e, this.getClass());
        }
    }


}
