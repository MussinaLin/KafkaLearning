package com.mussina.self.kafkacourse;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        String bootstrap_servers = "127.0.0.1:9092";
        String groupID = "g";
        String topic = "first_topic";
        // consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        CountDownLatch latch = new CountDownLatch(1);
        Runnable runner = new ConsumerThread(properties, topic, latch);

        Thread myThread = new Thread(runner);
        myThread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("caught shutdown hook.");
            ((ConsumerThread) runner).shutdown();
            try {
                logger.info("await latch..");
                latch.await();
                logger.info("end await latch..");
            } catch (InterruptedException e) {
                logger.error("Application interrupt...",e);
            }finally {
                logger.info("Application is closing...");
            }
            }
            )
        );



    }
}

class ConsumerThread implements Runnable{

    private KafkaConsumer<String, String> consumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
    private CountDownLatch latch;


    public ConsumerThread(Properties properties, String topic, CountDownLatch latch){
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("first_topic"));
        this.latch = latch;
    }

    @Override public void run() {
        try{
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for(ConsumerRecord<String, String> record : records){
                    logger.info(String.format("topic:%s partition:%s offset:%s key:%s value:%s ",
                        record.topic(), record.partition(),record.offset(), record.key(), record.value()));
                }
            }
        }catch (WakeupException e){
            logger.info("receive shutdown signal...");
        }finally {
            consumer.close();
            // tell main code we are done.
            latch.countDown();
        }
    }

    public void shutdown(){
        logger.info("ConsumerThread shutdown...");
        consumer.wakeup();
    }
}
