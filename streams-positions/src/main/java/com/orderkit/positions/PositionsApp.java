package com.orderkit.positions;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

/** Runs the topology against a real cluster. Tests use TopologyTestDriver instead. */
public final class PositionsApp {
    private PositionsApp() {}

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-positions");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092"));
        // Store updates, output records and input offsets commit atomically, so a crash cannot
        // leave the seen-store and the positions topic disagreeing.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        KafkaStreams streams = new KafkaStreams(PositionsTopology.build(), props);
        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            done.countDown();
        }));
        streams.start();
        done.await();
    }
}
