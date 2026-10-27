package com.eventguard.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "processed_events", uniqueConstraints = @UniqueConstraint(name = "uk_processed_event_id", columnNames = "event_id"))
public class ProcessedEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "event_id", nullable = false, length = 120)
    private String eventId;
    @Column(name = "entity_id", nullable = false, length = 120)
    private String entityId;
    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;
    @Column(nullable = false, length = 30)
    private String status;
    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;
    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
    @Column(name = "payload_json", columnDefinition = "text", nullable = false)
    private String payloadJson;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId, String entityId, String eventType, String status, Instant occurredAt,
            String payloadJson) {
        this.eventId = eventId;
        this.entityId = entityId;
        this.eventType = eventType;
        this.status = status;
        this.occurredAt = occurredAt;
        this.processedAt = Instant.now();
        this.payloadJson = payloadJson;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getStatus() {
        return status;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public String getPayloadJson() {
        return payloadJson;
    }
}
