package org.example;

import com.example.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Hello world!
 */


public class ProducerUserApp {
    public static void main(String[] args) throws InterruptedException {
        final int NUMBER_OF_RECORD = 1000000;
        final int MIN = 1;
        final int MAX = 500;
        final String topicName = "users";

        final Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        configs.put("schema.registry.url", "http://localhost:8081");
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 10);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 5000);
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        try (KafkaProducer<String, User> kafkaProducer = new KafkaProducer<>(configs)) {
//            for (int i = 0; i < NUMBER_OF_RECORD; i++) {
                User user = User.newBuilder()
                        .setName("John Doe")
                        .setAge(30)
                        .build();
                String key = "" + user.getName();
                ProducerRecord<String, User> producerRecord = new ProducerRecord<>(topicName, key, user);
                System.out.println("Produced message: (" + key + ", " + user + ")");
                kafkaProducer.send(producerRecord, ProducerUserApp::callBack);
                Thread.sleep(1000);
            }
//        }
    }

    private static void callBack(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            System.out.println("Error occurs : " + e.getMessage());
        } else {
            System.out.println("ack --> offset : " + recordMetadata.offset());
        }
    }
}
