package kafkacourse;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        String bootstrap_servers = "127.0.0.1:9092";
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i =0; i<10; i++){
            // create ProducerRecord
            ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic", "hello kafka producer " + i);

            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info(String.format("produce succ. topic:%s partition:%s offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()));
                    }else{
                        logger.error("Produce error:", e);
                    }
                }
            });
        }


        //flush and close
        logger.info("close");
        producer.close();
    }
}
