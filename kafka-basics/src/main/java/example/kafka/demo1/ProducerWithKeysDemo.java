package example.kafka.demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeysDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerWithKeysDemo.class);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i=0; i<10; i++) {
            String topic = "first_topic";
            String value = "Hello World " + Integer.valueOf(i);
            String key = "id_" + Integer.valueOf(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key = " + key);
            // id_1, id_6  => partition 0
            // id_0, id_3, id_8 => Partition 1
            // id_2, id_4, id_5, id_7, id_9 => partition 2
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        logger.info("received metadata. \n" +
                                "Topic = " + recordMetadata.topic() + "\n" +
                                "Partition = " + recordMetadata.partition() + "\n" +
                                "Offset = " + recordMetadata.offset() + "\n" +
                                "Timestamp = " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing. ", e);
                        e.printStackTrace();
                    }
                }
            }).get();
        }

        producer.flush();
        producer.close();
    }

}
