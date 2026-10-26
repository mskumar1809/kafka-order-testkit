package com.orderkit.positions;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

/**
 * orders.accepted (key = account) -> positions (key = "account|symbol", value = net signed qty).
 *
 * <p>No repartition is needed: the input is keyed by account, so every order for an account --
 * and therefore every order for an account|symbol pair, and every copy of a given order_id --
 * lands in the same partition and is handled by the same task and the same local state stores.
 */
public final class PositionsTopology {
    public static final String INPUT_TOPIC = "orders.accepted";
    public static final String OUTPUT_TOPIC = "positions";
    /** order_id -> timestamp first seen. Used to drop duplicate deliveries. */
    public static final String SEEN_STORE = "seen-order-ids";
    /** "account|symbol" -> net signed quantity. */
    public static final String POSITIONS_STORE = "positions";

    private PositionsTopology() {}

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(SEEN_STORE), Serdes.String(), Serdes.Long()));
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(POSITIONS_STORE), Serdes.String(), Serdes.Long()));

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
               .process(PositionProcessor::new, SEEN_STORE, POSITIONS_STORE)
               .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }
}
