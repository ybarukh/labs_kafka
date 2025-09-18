package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * Hello world!
 */


public class ProducerApp {
    public static void main(String[] args) throws InterruptedException {
        final int NUMBER_OF_RECORD = 1000000;
        final int MIN = 1;
        final int MAX = 500;
        final String topicName = "vehicle-count";

        final Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 10);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 5000);
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        int counter = 0;
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(configs)) {
            for (int i = 0; i < NUMBER_OF_RECORD; i++) {
                String key = "sensor-" + i;
//                int value = new Random().nextInt(MAX - MIN) + MIN;
                int value = ++counter;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, "" +value);
                System.out.println("Produced message: (" + key + ", " + value + ")");
                kafkaProducer.send(producerRecord, ProducerApp::callBack);
                Thread.sleep(1000);
            }
        }
    }

    private static void callBack(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            System.out.println("Error occurs : " + e.getMessage());
        } else {
            System.out.println("ack --> offset : " + recordMetadata.offset());
        }
    }
}
