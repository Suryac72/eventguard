
package com.eventguard.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

        @Value("${spring.kafka.bootstrap-servers}")
        private String bootstrapServers;

        @Value("${spring.kafka.properties.security.protocol:SASL_SSL}")
        private String securityProtocol;

        @Value("${spring.kafka.properties.ssl.endpoint.identification.algorithm:https}")
        private String endpointAlgorithm;

        @Value("${spring.kafka.properties.ssl.truststore.type:PKCS12}")
        private String truststoreType;

        @Value("${spring.kafka.properties.ssl.truststore.location:}")
        private String truststoreLocation;

        @Value("${spring.kafka.properties.ssl.truststore.password:}")
        private String truststorePassword;

        @Value("${spring.kafka.properties.sasl.mechanism:SCRAM-SHA-512}")
        private String saslMechanism;

        @Value("${spring.kafka.properties.sasl.username:}")
        private String saslUsername;

        @Value("${spring.kafka.properties.sasl.password:}")
        private String saslPassword;

        @Value("${spring.kafka.consumer.group-id:eventguard-processor}")
        private String groupId;

        /**
         * Kafka producer configuration.
         */
        @Bean
        public ProducerFactory<String, String> producerFactory() {
                Map<String, Object> properties = new HashMap<>();

                properties.put(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers);

                properties.put(
                                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class);

                properties.put(
                                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class);

                properties.put(ProducerConfig.ACKS_CONFIG, "all");
                properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
                properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

                configureSecurity(properties);

                return new DefaultKafkaProducerFactory<>(properties);
        }

        /**
         * Kafka template used to publish events.
         */
        @Bean
        public KafkaTemplate<String, String> kafkaTemplate(
                        ProducerFactory<String, String> producerFactory) {

                return new KafkaTemplate<>(producerFactory);
        }

        /**
         * Kafka consumer configuration.
         */
        @Bean
        public ConsumerFactory<String, String> consumerFactory() {
                Map<String, Object> properties = new HashMap<>();

                properties.put(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers);

                properties.put(
                                ConsumerConfig.GROUP_ID_CONFIG,
                                groupId);

                properties.put(
                                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                StringDeserializer.class);

                properties.put(
                                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                StringDeserializer.class);

                properties.put(
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                "earliest");

                properties.put(
                                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                                false);

                configureSecurity(properties);

                return new DefaultKafkaConsumerFactory<>(properties);
        }

        /**
         * Configure TLS and SASL authentication for Aiven Kafka.
         */
        private void configureSecurity(Map<String, Object> properties) {

                properties.put(
                                "security.protocol",
                                securityProtocol);

                if (securityProtocol.endsWith("SSL")) {
                        properties.put(
                                        SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                                        endpointAlgorithm);

                        // Use a custom truststore only when a location is supplied.
                        // Otherwise, Kafka uses the JVM's default trust configuration.
                        if (truststoreLocation != null
                                        && !truststoreLocation.isBlank()) {

                                properties.put(
                                                SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                                                truststoreType);

                                properties.put(
                                                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                                                truststoreLocation);

                                if (truststorePassword != null
                                                && !truststorePassword.isBlank()) {

                                        properties.put(
                                                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                                                        truststorePassword);
                                }
                        }
                }

                configureSasl(properties);
        }

        /**
         * Configure SASL authentication.
         */
        private void configureSasl(Map<String, Object> properties) {

                if (!securityProtocol.startsWith("SASL_")) {
                        return;
                }

                if (saslUsername == null || saslUsername.isBlank()
                                || saslPassword == null || saslPassword.isBlank()) {

                        throw new IllegalStateException(
                                        "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD "
                                                        + "are required when SASL is enabled.");
                }

                String loginModule = switch (saslMechanism) {
                        case "PLAIN" ->
                                "org.apache.kafka.common.security.plain.PlainLoginModule";

                        case "SCRAM-SHA-256", "SCRAM-SHA-512" ->
                                "org.apache.kafka.common.security.scram.ScramLoginModule";

                        default -> throw new IllegalStateException(
                                        "Unsupported Kafka SASL mechanism: " + saslMechanism);
                };

                String jaasConfig = loginModule
                                + " required username=\""
                                + escapeJaasValue(saslUsername)
                                + "\" password=\""
                                + escapeJaasValue(saslPassword)
                                + "\";";

                properties.put(
                                SaslConfigs.SASL_MECHANISM,
                                saslMechanism);

                properties.put(
                                SaslConfigs.SASL_JAAS_CONFIG,
                                jaasConfig);
        }

        /**
         * Escape special characters in JAAS string values.
         */
        private String escapeJaasValue(String value) {
                return value
                                .replace("\\", "\\\\")
                                .replace("\"", "\\\"");
        }

        /**
         * Listener container configuration with retry and dead-letter handling.
         */
        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
                        ConsumerFactory<String, String> consumerFactory,
                        KafkaTemplate<String, String> kafkaTemplate) {

                ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();

                factory.setConsumerFactory(consumerFactory);

                DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                                kafkaTemplate,
                                (record, exception) -> new org.apache.kafka.common.TopicPartition(
                                                KafkaTopicNames.PROCESSING_DLQ,
                                                record.partition()));

                DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                                recoverer,
                                new FixedBackOff(1000L, 2L));

                factory.setCommonErrorHandler(errorHandler);

                return factory;
        }

        /**
         * Business events topic.
         */
        @Bean
        public NewTopic businessEvents() {
                return TopicBuilder
                                .name(KafkaTopicNames.BUSINESS_EVENTS)
                                .partitions(2)
                                .replicas(1)
                                .build();
        }

        /**
         * Validated events topic.
         */
        @Bean
        public NewTopic validatedEvents() {
                return TopicBuilder
                                .name(KafkaTopicNames.VALIDATED_EVENTS)
                                .partitions(2)
                                .replicas(1)
                                .build();
        }

        /**
         * Anomaly events topic.
         */
        @Bean
        public NewTopic anomalyEvents() {
                return TopicBuilder
                                .name(KafkaTopicNames.ANOMALY_EVENTS)
                                .partitions(2)
                                .replicas(1)
                                .build();
        }

        /**
         * Processing retry topic.
         */
        @Bean
        public NewTopic processingRetry() {
                return TopicBuilder
                                .name(KafkaTopicNames.PROCESSING_RETRY)
                                .partitions(2)
                                .replicas(1)
                                .build();
        }

        /**
         * Processing dead-letter topic.
         */
        @Bean
        public NewTopic processingDlq() {
                return TopicBuilder
                                .name(KafkaTopicNames.PROCESSING_DLQ)
                                .partitions(2)
                                .replicas(1)
                                .build();
        }
}
