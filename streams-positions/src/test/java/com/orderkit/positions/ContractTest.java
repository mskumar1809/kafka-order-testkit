package com.orderkit.positions;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Consumer side of the contract. contracts/order-accepted.schema.json is shared with the Python
 * producer, whose tests validate its real output against the same file. If either side drifts,
 * one of the two suites fails.
 */
class ContractTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static JsonSchema schema;

    @BeforeAll
    static void loadSchema() throws IOException {
        // "format" (uuid, date-time) is only an annotation in draft 2020-12 unless assertions are enabled.
        SchemaValidatorsConfig config = SchemaValidatorsConfig.builder().formatAssertionsEnabled(true).build();
        try (InputStream in = Files.newInputStream(TestEvents.CONTRACTS_DIR.resolve("order-accepted.schema.json"))) {
            schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in, config);
        }
    }

    @Test
    void sharedExampleSatisfiesTheContract() {
        assertThat(validate(TestEvents.example())).isEmpty();
    }

    @Test
    void eventsUsedByTheTopologyTestsSatisfyTheContract() throws IOException {
        for (String side : new String[] {"BUY", "SELL"}) {
            JsonNode event = MAPPER.readTree(TestEvents.accepted("ACC-9", "MSFT", side, 3));
            assertThat(validate(event)).as(side).isEmpty();
        }
    }

    static Stream<Arguments> violations() {
        return Stream.of(
                Arguments.of("missing order_id", (Consumer<ObjectNode>) e -> e.remove("order_id")),
                Arguments.of("order_id not a uuid", (Consumer<ObjectNode>) e -> e.put("order_id", "abc")),
                Arguments.of("unknown side", (Consumer<ObjectNode>) e -> e.put("side", "HOLD")),
                Arguments.of("zero qty", (Consumer<ObjectNode>) e -> e.put("qty", 0)),
                Arguments.of("fractional qty", (Consumer<ObjectNode>) e -> e.put("qty", 1.5)),
                Arguments.of("wrong status", (Consumer<ObjectNode>) e -> e.put("status", "REJECTED")),
                Arguments.of("bad timestamp", (Consumer<ObjectNode>) e -> e.put("ts", "yesterday")),
                Arguments.of("undeclared field", (Consumer<ObjectNode>) e -> e.put("venue", "XNAS")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("violations")
    void schemaRejectsContractViolations(String description, Consumer<ObjectNode> mutate) {
        ObjectNode event = TestEvents.example();
        mutate.accept(event);
        assertThat(validate(event)).as(description).isNotEmpty();
    }

    private static Set<ValidationMessage> validate(JsonNode event) {
        return schema.validate(event);
    }
}
