package dev.practice.cdc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
final class Projector {
    private final ObjectMapper mapper;
    private final JdbcTemplate sink;
    private final MeterRegistry metrics;

    Projector(ObjectMapper mapper, JdbcTemplate sink, MeterRegistry metrics) {
        this.mapper = mapper;
        this.sink = sink;
        this.metrics = metrics;
    }

    // One autocommitted statement per record. A crash after this statement can cause redelivery.
    @KafkaListener(topics = "${CDC_TOPIC:orders.public.orders}")
    void accept(ConsumerRecord<String, String> record) throws JsonProcessingException {
        if (record.value() == null) return; // Kafka compaction tombstone follows the delete envelope.
        var change = OrderChange.parse(mapper.readTree(record.value()));
        int changed = sink.update("""
                INSERT INTO order_view (id, revision, status, deleted) VALUES (?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE
                SET revision = excluded.revision, status = excluded.status, deleted = excluded.deleted
                WHERE (order_view.revision, order_view.deleted) < (excluded.revision, excluded.deleted)
                """, change.id(), change.version(), change.status(), change.deleted());
        metrics.counter("cdc.changes", "result", changed == 1 ? "applied" : "replayed").increment();
    }
}
