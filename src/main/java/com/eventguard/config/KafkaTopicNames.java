package com.eventguard.config;

public final class KafkaTopicNames {
    private KafkaTopicNames() {
    }

    public static final String BUSINESS_EVENTS = "business-events";
    public static final String VALIDATED_EVENTS = "validated-events";
    public static final String ANOMALY_EVENTS = "anomaly-events";
    public static final String PROCESSING_RETRY = "processing-retry";
    public static final String PROCESSING_DLQ = "processing-dlq";
}
