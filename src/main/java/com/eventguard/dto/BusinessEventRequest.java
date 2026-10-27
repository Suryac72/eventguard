package com.eventguard.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.time.Instant;

public record BusinessEventRequest(
                @NotBlank String eventId,
                @NotBlank String entityId,
                @NotBlank String eventType,
                @NotNull @Positive BigDecimal amount,
                @NotNull Instant occurredAt,
                String source) {
}
