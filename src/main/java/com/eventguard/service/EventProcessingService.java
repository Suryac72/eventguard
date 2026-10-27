package com.eventguard.service;

import com.eventguard.config.KafkaTopicNames;
import com.eventguard.dto.BusinessEventRequest;
import com.eventguard.entity.AnomalyRecord;
import com.eventguard.entity.ProcessedEvent;
import com.eventguard.repository.AnomalyRecordRepository;
import com.eventguard.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Duration;
import java.time.Instant;

@Service
public class EventProcessingService {
    private final ProcessedEventRepository processedRepository;
    private final AnomalyRecordRepository anomalyRepository;
    private final EventPublisher publisher;
    private final ObjectMapper mapper;
    public EventProcessingService(ProcessedEventRepository processedRepository, AnomalyRecordRepository anomalyRepository, EventPublisher publisher, ObjectMapper mapper) {
        this.processedRepository=processedRepository; this.anomalyRepository=anomalyRepository; this.publisher=publisher; this.mapper=mapper;
    }

    @Transactional
    public void process(String raw) throws Exception {
        BusinessEventRequest event = mapper.readValue(raw, BusinessEventRequest.class);
        if (event.eventId() == null || event.eventId().isBlank() || event.entityId() == null || event.entityId().isBlank())
            throw new IllegalArgumentException("eventId and entityId are required");

        if (processedRepository.findByEventId(event.eventId()).isPresent()) {
            anomalyRepository.save(new AnomalyRecord(event.eventId(), "DUPLICATE_EVENT", "Event ID has already been processed"));
            publisher.publishRaw(KafkaTopicNames.ANOMALY_EVENTS, event.entityId(), mapper.writeValueAsString(event));
            return;
        }
        String anomalyType = null;
        String description = null;
        if (event.occurredAt() != null && Duration.between(event.occurredAt(), Instant.now()).toHours() > 24) {
            anomalyType = "LATE_EVENT"; description = "Event arrived more than 24 hours after its occurredAt timestamp";
        }
        try {
            processedRepository.saveAndFlush(new ProcessedEvent(event.eventId(), event.entityId(), event.eventType(), anomalyType == null ? "PROCESSED" : "ANOMALOUS", event.occurredAt(), raw));
        } catch (DataIntegrityViolationException duplicateRace) {
            anomalyRepository.save(new AnomalyRecord(event.eventId(), "DUPLICATE_EVENT", "Concurrent delivery of an already processed event"));
            publisher.publishRaw(KafkaTopicNames.ANOMALY_EVENTS, event.entityId(), raw);
            return;
        }
        if (anomalyType != null) {
            anomalyRepository.save(new AnomalyRecord(event.eventId(), anomalyType, description));
            publisher.publishRaw(KafkaTopicNames.ANOMALY_EVENTS, event.entityId(), raw);
        } else {
            publisher.publishRaw(KafkaTopicNames.VALIDATED_EVENTS, event.entityId(), raw);
        }
    }
}
