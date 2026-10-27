package com.eventguard.service;

import com.eventguard.config.KafkaTopicNames;
import com.eventguard.dto.BusinessEventRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventPublisher {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    public EventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) { this.kafkaTemplate=kafkaTemplate; this.objectMapper=objectMapper; }
    public void publish(BusinessEventRequest event) {
        try { kafkaTemplate.send(KafkaTopicNames.BUSINESS_EVENTS, event.entityId(), objectMapper.writeValueAsString(event)); }
        catch (JsonProcessingException ex) { throw new IllegalArgumentException("Could not serialize event", ex); }
    }
    public void publishRaw(String topic, String key, String payload) { kafkaTemplate.send(topic, key, payload); }
}
