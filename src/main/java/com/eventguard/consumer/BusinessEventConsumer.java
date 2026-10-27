package com.eventguard.consumer;

import com.eventguard.config.KafkaTopicNames;
import com.eventguard.service.EventProcessingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BusinessEventConsumer {
    private final EventProcessingService processingService;

    public BusinessEventConsumer(EventProcessingService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(topics = KafkaTopicNames.BUSINESS_EVENTS, containerFactory = "kafkaListenerContainerFactory")
    public void consume(String payload) throws Exception {
        processingService.process(payload);
    }
}
