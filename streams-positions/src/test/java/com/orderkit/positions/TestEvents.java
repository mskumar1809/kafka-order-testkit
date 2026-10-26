package com.orderkit.positions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.UUID;

/** Builds orders.accepted events from the shared contract example, so test inputs are contract-shaped. */
final class TestEvents {
    static final Path CONTRACTS_DIR = Path.of(System.getProperty("contracts.dir", "../contracts"));
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TestEvents() {}

    static ObjectNode example() {
        try {
            return (ObjectNode) MAPPER.readTree(CONTRACTS_DIR.resolve("examples/order-accepted.json").toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String accepted(String orderId, String account, String symbol, String side, long qty) {
        ObjectNode event = example();
        BigDecimal price = event.get("price").decimalValue();
        event.put("order_id", orderId)
             .put("account", account)
             .put("symbol", symbol)
             .put("side", side)
             .put("qty", qty)
             .put("notional", price.multiply(BigDecimal.valueOf(qty)));
        return event.toString();
    }

    static String accepted(String account, String symbol, String side, long qty) {
        return accepted(UUID.randomUUID().toString(), account, symbol, side, qty);
    }
}
