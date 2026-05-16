package com.myfabric.consumer.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;

@Document(collection = "profiles")
public record Profile(
        @Id String id,
        String firstName,
        String lastName,
        String email,
        String phoneNumber,
        String department,
        String mergedFromId,
        Map<String, String> metadata,
        Instant processedAt,
        int schemaVersion,
        String lastEventType
) {}
