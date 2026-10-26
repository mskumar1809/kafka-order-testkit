package com.orderkit.positions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs the real topology in-process with TopologyTestDriver: no broker, deterministic, fast. */
class PositionsTopologyTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path stateDir; // fresh state stores per test

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> accepted;
    private TestOutputTopic<String, String> positions;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "positions-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(PositionsTopology.build(), props);
        accepted = driver.createInputTopic(PositionsTopology.INPUT_TOPIC, new StringSerializer(), new StringSerializer());
        positions = driver.createOutputTopic(PositionsTopology.OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void buysAndSellsNetToASignedPosition() {
        accepted.pipeInput("ACC-1", TestEvents.accepted("ACC-1", "AAPL", "BUY", 100));
        accepted.pipeInput("ACC-1", TestEvents.accepted("ACC-1", "AAPL", "SELL", 30));
        accepted.pipeInput("ACC-1", TestEvents.accepted("ACC-1", "AAPL", "SELL", 120));

        assertThat(positions.readKeyValuesToList())
                .extracting(kv -> kv.key, kv -> netQty(kv.value))
                .containsExactly(
                        tuple("ACC-1|AAPL", 100L),
                        tuple("ACC-1|AAPL", 70L),
                        tuple("ACC-1|AAPL", -50L)); // can go short
    }

    @Test
    void accountsAndSymbolsAreTrackedIndependently() {
        accepted.pipeInput("ACC-1", TestEvents.accepted("ACC-1", "AAPL", "BUY", 10));
        accepted.pipeInput("ACC-1", TestEvents.accepted("ACC-1", "MSFT", "SELL", 5));
        accepted.pipeInput("ACC-2", TestEvents.accepted("ACC-2", "AAPL", "BUY", 7));
        accepted.pipeInput("ACC-1", TestEvents.accepted("ACC-1", "AAPL", "BUY", 1));

        Map<String, String> latest = positions.readKeyValuesToMap();
        assertThat(latest).hasSize(3);
        assertThat(netQty(latest.get("ACC-1|AAPL"))).isEqualTo(11L);
        assertThat(netQty(latest.get("ACC-1|MSFT"))).isEqualTo(-5L);
        assertThat(netQty(latest.get("ACC-2|AAPL"))).isEqualTo(7L);
    }

    @Test
    void duplicateOrderIdIsCountedOnce() {
        // The Python processor is at-least-once on output: the same order_id can arrive twice.
        String event = TestEvents.accepted(UUID.randomUUID().toString(), "ACC-1", "AAPL", "BUY", 100);
        accepted.pipeInput("ACC-1", event);
        accepted.pipeInput("ACC-1", event);

        List<String> emitted = positions.readValuesToList();
        assertThat(emitted).hasSize(1); // the duplicate is dropped, not re-emitted
        assertThat(netQty(emitted.get(0))).isEqualTo(100L);
        assertThat(driver.<String, Long>getKeyValueStore(PositionsTopology.POSITIONS_STORE).get("ACC-1|AAPL"))
                .isEqualTo(100L);
    }

    @Test
    void malformedEventIsSkippedAndProcessingContinues() {
        accepted.pipeInput("ACC-1", "{not json");
        accepted.pipeInput("ACC-1", "{\"order_id\":\"x\",\"side\":\"HOLD\"}");
        accepted.pipeInput("ACC-1", TestEvents.accepted("ACC-1", "AAPL", "BUY", 5));

        assertThat(positions.readValuesToList()).extracting(PositionsTopologyTest::netQty).containsExactly(5L);
    }

    private static long netQty(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            return node.get("net_qty").asLong();
        } catch (Exception e) {
            throw new AssertionError("bad output JSON: " + json, e);
        }
    }
}
