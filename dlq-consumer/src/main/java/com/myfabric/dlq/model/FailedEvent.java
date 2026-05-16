package com.myfabric.dlq.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.UUID;

@Document(collection = "failed_events")
public record FailedEvent(
        @Id String id,
        String profileId,
        String topic,
        int partition,
        long offset,
        String exceptionClass,
        String exceptionMessage,
        String rawPayload,
        Instant receivedAt,
        String status
) {
    public static FailedEvent from(String profileId, String topic, int partition, long offset,
                                   String exceptionClass, String exceptionMessage, String rawPayload) {
        return new FailedEvent(
                UUID.randomUUID().toString(),
                profileId, topic, partition, offset,
                exceptionClass, exceptionMessage, rawPayload,
                Instant.now(), "PENDING_REVIEW"
        );
    }

    public FailedEvent withStatus(String newStatus) {
        return new FailedEvent(id, profileId, topic, partition, offset,
                exceptionClass, exceptionMessage, rawPayload, receivedAt, newStatus);
    }
}
