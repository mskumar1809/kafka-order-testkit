package com.orderkit.positions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The fields of an orders.accepted event this service needs. Parsing is a tolerant reader:
 * unknown fields are ignored; only the fields used here are required.
 */
record AcceptedOrder(String orderId, String account, String symbol, String side, long qty) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static AcceptedOrder parse(String json) {
        JsonNode node;
        try {
            node = MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("not JSON", e);
        }
        AcceptedOrder order = new AcceptedOrder(
                text(node, "order_id"), text(node, "account"), text(node, "symbol"),
                text(node, "side"), node.path("qty").asLong(0));
        if (!order.side.equals("BUY") && !order.side.equals("SELL")) {
            throw new IllegalArgumentException("unknown side " + order.side);
        }
        if (order.qty <= 0) {
            throw new IllegalArgumentException("qty must be positive");
        }
        return order;
    }

    /** BUY adds to the position, SELL subtracts. */
    long signedQty() {
        return side.equals("BUY") ? qty : -qty;
    }

    String positionKey() {
        return account + "|" + symbol;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual()) {
            throw new IllegalArgumentException("missing field " + field);
        }
        return value.asText();
    }
}
