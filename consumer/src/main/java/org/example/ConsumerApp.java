package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Hello world!
 */
public class ConsumerApp {
    public static void main(String[] args) {
        String topicName = "vehicle-count";
        String groupId = "iot-consumer-group";
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 5000000);
        configs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 5000);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configs)) {
            kafkaConsumer.subscribe(List.of(topicName));

            while (true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println("Consumed message: (" + record.key() + ", " + record.value() + ")");
                }
            }
        }
    }
}
