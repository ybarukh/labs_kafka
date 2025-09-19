package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;

/**
 * Hello world!
 */
public class StreamApp {
    private static final Logger logger = LoggerFactory.getLogger(StreamApp.class);
    public static void main(String[] args) throws InterruptedException {
        final Properties properties = new Properties();
        final int CONGESTION_THRESHOLD = 300;
        final String topicIn = "vehicle-count2";
        final String topicOut = "congestion-alerts";
        logger.info("Start StreamApp");

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "traffic-monitoring");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> vehicleCountStreams = streamsBuilder.stream(topicIn);
        KStream<String, String> congestionAlertsStreams = vehicleCountStreams.filter((key, value) -> {
            int vehicleCount = Integer.parseInt(value);
           return vehicleCount > CONGESTION_THRESHOLD;
        });
        congestionAlertsStreams.foreach((key, value) -> System.out.println("ALERT Street : " + key + ", COUNT vehicle : " + value));
        congestionAlertsStreams.to(topicOut);


        Topology topology = streamsBuilder.build();
        logger.info("Topology topology created {}", topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
            kafkaStreams.setUncaughtExceptionHandler((throwable) -> {
                logger.error("UncaughtException {}", throwable.getMessage(), throwable);
                return SHUTDOWN_CLIENT;
            });

        try {
            kafkaStreams.start();
            Thread.currentThread().join();
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } catch (StreamsException e) {
            throw new RuntimeException(e);
        } finally {
            kafkaStreams.close();
        }


    }
}