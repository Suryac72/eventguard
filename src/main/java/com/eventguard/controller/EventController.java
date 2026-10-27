package com.eventguard.controller;

import com.eventguard.dto.BusinessEventRequest;
import com.eventguard.dto.EventAcceptedResponse;
import com.eventguard.repository.ProcessedEventRepository;
import com.eventguard.service.EventPublisher;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {
    private final EventPublisher publisher;
    private final ProcessedEventRepository repository;

    public EventController(EventPublisher publisher, ProcessedEventRepository repository) {
        this.publisher = publisher;
        this.repository = repository;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public EventAcceptedResponse submit(@Valid @RequestBody BusinessEventRequest request) {
        publisher.publish(request);
        return new EventAcceptedResponse(request.eventId(), "ACCEPTED",
                "Event submitted to Kafka for asynchronous processing");
    }

    @GetMapping("/{eventId}")
    public Map<String, Object> status(@PathVariable String eventId) {
        return repository.findByEventId(eventId)
                .<Map<String, Object>>map(e -> Map.of("eventId", e.getEventId(), "entityId", e.getEntityId(),
                        "eventType", e.getEventType(), "status", e.getStatus(), "occurredAt",
                        e.getOccurredAt().toString(), "processedAt", e.getProcessedAt().toString()))
                .orElseGet(() -> Map.of("eventId", eventId, "status", "NOT_PROCESSED_YET"));
    }
}
