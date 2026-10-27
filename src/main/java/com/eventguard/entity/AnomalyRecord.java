package com.eventguard.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "anomaly_records")
public class AnomalyRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "event_id", nullable = false)
    private String eventId;
    @Column(nullable = false)
    private String anomalyType;
    @Column(nullable = false, length = 1000)
    private String description;
    @Column(nullable = false)
    private Instant detectedAt = Instant.now();

    protected AnomalyRecord() {
    }

    public AnomalyRecord(String eventId, String anomalyType, String description) {
        this.eventId = eventId;
        this.anomalyType = anomalyType;
        this.description = description;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getAnomalyType() {
        return anomalyType;
    }

    public String getDescription() {
        return description;
    }

    public Instant getDetectedAt() {
        return detectedAt;
    }
}
